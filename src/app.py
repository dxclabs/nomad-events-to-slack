import http.client
import json
import logging
import os
import queue
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import consul
import nomad
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

COLOR_OOM = "#d72b3f"
COLOR_DEFAULT = "#36a64f"
CONNECT_TIMEOUT = 10
QUEUE_POLL_SECS = 2.0

# "Started" signals a restart cycle is complete; "Killed"/"Not Restarting" are
# unconditionally terminal.  "Terminated" is handled separately with a debounce
# so that a Terminated→Restarting→Started cycle isn't reported mid-flight.
TERMINAL_EVENT_TYPES = frozenset({"Started", "Killed", "Not Restarting"})

# How long to wait after a "Terminated" event before treating it as terminal.
# This gives Nomad time to emit the follow-up "Restarting" event for jobs that
# are restarted by a template change or scheduler decision.
TERMINATED_DEBOUNCE_SECS = 30


def clear_input_list(in_list: list[str]) -> None:
    while "" in in_list:
        in_list.remove("")
    while "None" in in_list:
        in_list.remove("None")


def get_stored_index(c_consul: consul.Consul, key: str) -> int:
    _, data = c_consul.kv.get(key)
    if data and data["Value"]:
        try:
            return int(data["Value"].decode("utf-8").strip())
        except ValueError:
            logger.warning("Consul key %s has unreadable value, resetting to 0", key)
            return 0
    return 0


def save_index(c_consul: consul.Consul, key: str, index: int) -> None:
    c_consul.kv.put(key, str(index))


def _nomad_headers() -> dict[str, str]:
    token = os.getenv("NOMAD_TOKEN", "")
    return {"X-Nomad-Token": token} if token else {}


def _make_nomad_client() -> nomad.Nomad:
    addr = os.getenv("NOMAD_ADDR", "http://127.0.0.1:4646")
    token = os.getenv("NOMAD_TOKEN", "")
    parsed = urlparse(addr)
    return nomad.Nomad(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 4646,
        token=token or None,
        secure=(parsed.scheme == "https"),
        timeout=CONNECT_TIMEOUT,
    )


def get_current_nomad_index() -> int:
    """Get the current Nomad event index via X-Nomad-Index response header."""
    nomad_addr = os.getenv("NOMAD_ADDR", "http://127.0.0.1:4646")
    resp = requests.get(
        f"{nomad_addr}/v1/jobs",
        headers=_nomad_headers(),
        timeout=CONNECT_TIMEOUT,
    )
    resp.raise_for_status()
    return int(resp.headers.get("X-Nomad-Index", 0))


def _empty_job_record() -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "events": [],
        "seen_keys": set(),
        "last_reported_at": now,
        "last_reported_index": 0,  # highest event_index included in last report
        "max_index": 0,  # highest event_index seen in current accumulation
        "last_state": None,
        "last_updated_at": now,
        "start_index": None,
        "meta": {
            "oom_kill_count": 0,
            "restart_count": 0,
            "download_attempts": 0,
            "health_check_failures": 0,
        },
    }


def _update_meta(
    meta: dict[str, int], event_type: str, event_details: dict[str, str]
) -> None:
    if event_type == "Terminated" and event_details.get("oom_killed") == "true":
        meta["oom_kill_count"] += 1
    elif event_type == "Restarting":
        meta["restart_count"] += 1
    elif event_type == "Downloading Artifacts":
        meta["download_attempts"] += 1


def format_job_events_to_slack(
    job_id: str,
    events: list[dict[str, Any]],
    meta: dict[str, int],
    *,
    partial: bool = False,
) -> str:
    has_oom = meta["oom_kill_count"] > 0
    color = COLOR_OOM if has_oom else os.getenv("EVENTS_COLOR", COLOR_DEFAULT)
    n = len(events)

    prefix = ""
    if has_oom:
        prefix += "💀 "
    if partial:
        prefix += "⏳ "
    header = f"{prefix}{job_id} — {n} event{'s' if n != 1 else ''}"

    event_lines = []
    for e in events:
        oom_flag = " 💀" if e["EventDetails"].get("oom_killed") == "true" else ""
        line = f"• *{e['TaskName']}* — {e['EventType']}{oom_flag}"
        if e["EventDisplayMessage"]:
            line += f"\n  _{e['EventDisplayMessage']}_"
        event_lines.append(line)

    nodes = sorted({e["NodeName"] for e in events})
    earliest_time = min(e["Time"] for e in events).strftime("%Y-%m-%d %H:%M:%S")

    context_elements: list[dict[str, Any]] = [
        {"type": "mrkdwn", "text": f"*Nodes:* {', '.join(nodes)}"},
        {"type": "mrkdwn", "text": f"*Time:* {earliest_time}"},
    ]

    meta_parts = []
    if meta["restart_count"]:
        meta_parts.append(f"restarts: {meta['restart_count']}")
    if meta["oom_kill_count"]:
        meta_parts.append(f"OOM kills: {meta['oom_kill_count']}")
    if meta["download_attempts"]:
        meta_parts.append(f"dl attempts: {meta['download_attempts']}")
    if meta["health_check_failures"]:
        meta_parts.append(f"health failures: {meta['health_check_failures']}")
    if meta_parts:
        context_elements.append({"type": "mrkdwn", "text": " | ".join(meta_parts)})

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header, "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(event_lines)},
        },
        {
            "type": "context",
            "elements": context_elements,
        },
    ]

    return json.dumps(
        {"text": header, "attachments": [{"color": color, "blocks": blocks}]}
    )


def post_message_to_slack(hook_url: str, message: str) -> bool:
    logger.debug("Posting message to %s:\n%s", hook_url, message)
    if hook_url == "":
        return False
    headers = {"Content-type": "application/json"}
    connection = http.client.HTTPSConnection("hooks.slack.com")
    connection.request(
        "POST", hook_url.replace("https://hooks.slack.com", ""), message, headers
    )
    if connection.getresponse().read().decode() == "ok":
        return True
    raise ConnectionError


def _report_and_purge(
    job_id: str,
    job: dict[str, Any],
    slack_hook_url: str,
    *,
    partial: bool = False,
) -> None:
    if not job["events"]:
        return
    label = "partial/stale" if partial else "terminal"
    logger.info("Reporting %s job %s (%d events)", label, job_id, len(job["events"]))
    try:
        post_message_to_slack(
            slack_hook_url,
            format_job_events_to_slack(
                job_id, job["events"], job["meta"], partial=partial
            ),
        )
    except (ConnectionError, OSError) as e:
        logger.exception("Failed to post Slack message for %s: %s", job_id, e)

    now = datetime.now(UTC)
    job["last_reported_index"] = job["max_index"]
    job["events"] = []
    job["seen_keys"] = set()
    job["last_reported_at"] = now
    job["max_index"] = 0
    job["last_state"] = None
    job["start_index"] = None
    job["meta"] = {
        "oom_kill_count": 0,
        "restart_count": 0,
        "download_attempts": 0,
        "health_check_failures": 0,
    }


def main() -> None:  # noqa: C901
    if os.getenv("NOMAD_EVENTS_TO_SLACK_DEBUG", "false") == "true":
        logger.setLevel(logging.DEBUG)
        logging.basicConfig(level=logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO)

    logger.info("Start APP. Reading ENV...")
    use_consul = os.getenv("USE_CONSUL", "false")
    consul_key = str(os.getenv("CONSUL_KEY", "nomad/nomad-events-to-slack"))
    node_names = str(os.getenv("NODE_NAMES", "")).split(",")
    job_ids = str(os.getenv("JOB_IDS", "")).split(",")
    event_types = str(os.getenv("EVENT_TYPES", "")).split(",")
    event_message_filters = str(os.getenv("EVENT_MESSAGE_FILTERS", "")).split(",")
    slack_web_hook_url = os.getenv("SLACK_WEB_HOOK_URL", "")
    max_job_age_secs = int(os.getenv("MAX_JOB_AGE_SECS", "3600"))

    if slack_web_hook_url == "":
        logger.error(
            "ENV SLACK_WEB_HOOK_URL not set. Set it to non-empty string and try again"
        )
        raise RuntimeError from None

    clear_input_list(node_names)
    clear_input_list(job_ids)
    clear_input_list(event_types)
    clear_input_list(event_message_filters)

    my_consul = consul.Consul() if use_consul == "true" else None

    stored_index = 0
    if use_consul == "true" and my_consul:
        try:
            stored_index = get_stored_index(my_consul, consul_key)
            if stored_index:
                logger.info("Resuming from Consul index %s", stored_index)
        except consul.base.ConsulException as e:
            logger.exception("Failed to read index from Consul: %s", e)
            raise RuntimeError from None

    if stored_index == 0:
        try:
            stored_index = get_current_nomad_index()
            logger.info(
                "No stored index — starting from current Nomad index %s", stored_index
            )
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Could not fetch current Nomad index, starting from 0: %s", e
            )

    logger.info("ENV is ok. Starting event stream (max_job_age=%ss).", max_job_age_secs)

    # Per-job event accumulator: job_id → record dict
    job_events: dict[str, dict[str, Any]] = {}

    # python-nomad manages the stream thread and reconnects on ConnectionError
    nomad_client = _make_nomad_client()
    stream_thread, stream_exit, event_queue = nomad_client.event.stream.get_stream(
        index=stored_index,
        topic="Allocation",
        timeout=None,  # no read timeout — keep connection open indefinitely
    )
    stream_thread.daemon = True
    stream_thread.start()

    try:
        while True:
            # Poll with a short timeout so stale-job checks still run during
            # quiet periods when no events are arriving.
            try:
                batch = event_queue.get(timeout=QUEUE_POLL_SECS)
            except queue.Empty:
                batch = None
            else:
                event_queue.task_done()

            if batch is not None:
                for stream_event in batch.get("Events") or []:
                    event_index = int(stream_event.get("Index") or 0)

                    # Skip snapshot events replayed at or below our stored cursor
                    if event_index <= stored_index:
                        continue

                    alloc = stream_event.get("Payload", {}).get("Allocation", {})
                    if not alloc:
                        continue

                    job_id = alloc.get("JobID", "")
                    if not job_id:
                        continue

                    # Skip events at or below the index already covered by the
                    # last report for this job — guards against Nomad re-emitting
                    # stale allocation state updates after a terminal event.
                    if (
                        job_id in job_events
                        and event_index <= job_events[job_id]["last_reported_index"]
                    ):
                        continue

                    # Optional filters
                    if node_names and alloc.get("NodeName") not in node_names:
                        continue
                    if job_ids and job_id not in job_ids:
                        continue

                    for task_name, state in (alloc.get("TaskStates") or {}).items():
                        task_event_list = state.get("Events") or []
                        if not task_event_list:
                            continue

                        latest = task_event_list[-1]
                        event_type = latest["Type"]

                        if event_types and event_type not in event_types:
                            continue
                        if (
                            event_message_filters
                            and latest.get("Message") not in event_message_filters
                        ):
                            continue

                        event_time = datetime.fromtimestamp(
                            float(latest["Time"]) / 1_000_000_000, tz=UTC
                        )

                        # Discard events older than the accumulation window
                        if (
                            datetime.now(UTC) - event_time
                        ).total_seconds() > max_job_age_secs:
                            continue

                        dedup_key = (alloc["ID"], task_name, event_type, latest["Time"])
                        task_event = {
                            "AllocationID": alloc["ID"],
                            "NodeName": alloc.get("NodeName", ""),
                            "JobID": job_id,
                            "JobType": alloc.get("JobType", ""),
                            "TaskGroup": alloc.get("TaskGroup", ""),
                            "TaskName": task_name,
                            "Time": event_time,
                            "EventType": event_type,
                            "EventMessage": latest.get("Message", ""),
                            "EventDisplayMessage": latest.get("DisplayMessage", ""),
                            "EventDetails": latest.get("Details", {}),
                        }

                        if job_id not in job_events:
                            rec = _empty_job_record()
                            rec["events"] = [task_event]
                            rec["seen_keys"] = {dedup_key}
                            rec["last_state"] = event_type
                            rec["last_updated_at"] = event_time
                            rec["start_index"] = event_index
                            rec["max_index"] = event_index
                            job_events[job_id] = rec
                            _update_meta(
                                rec["meta"], event_type, latest.get("Details", {})
                            )
                            logger.debug(
                                "New job tracked: %s at index %s", job_id, event_index
                            )
                        elif dedup_key not in job_events[job_id]["seen_keys"]:
                            job = job_events[job_id]
                            job["events"].append(task_event)
                            job["seen_keys"].add(dedup_key)
                            job["last_state"] = event_type
                            if event_time > job["last_updated_at"]:
                                job["last_updated_at"] = event_time
                            if event_index > job["max_index"]:
                                job["max_index"] = event_index
                            _update_meta(
                                job["meta"], event_type, latest.get("Details", {})
                            )

            # Report any job that has reached a terminal state or gone stale
            now = datetime.now(UTC)
            for job_id in list(job_events.keys()):
                job = job_events[job_id]
                if not job["events"]:
                    continue
                age_secs = (now - job["last_updated_at"]).total_seconds()
                is_terminal = job["last_state"] in TERMINAL_EVENT_TYPES
                # Debounce "Terminated": wait to see if "Restarting" follows
                # before treating it as terminal (e.g. template-change restart).
                if not is_terminal and job["last_state"] == "Terminated":
                    is_terminal = age_secs >= TERMINATED_DEBOUNCE_SECS
                is_stale = age_secs > max_job_age_secs
                if is_terminal or is_stale:
                    _report_and_purge(
                        job_id,
                        job,
                        slack_web_hook_url,
                        partial=is_stale and not is_terminal,
                    )

            # Advance the stored Consul index to the lowest start_index of any
            # job still accumulating events, so a restart resumes from there.
            active_indices = [
                j["start_index"]
                for j in job_events.values()
                if j["start_index"] is not None
            ]
            if active_indices:
                lowest = min(active_indices)
                if lowest > stored_index:
                    stored_index = lowest
                    logger.debug("Advancing stored index to %s", stored_index)
                    if use_consul == "true" and my_consul:
                        try:
                            save_index(my_consul, consul_key, stored_index)
                        except consul.base.ConsulException as e:
                            logger.exception("Failed to save index to Consul: %s", e)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        stream_exit.set()
        stream_thread.join(timeout=5)
        if use_consul == "true" and my_consul and stored_index:
            try:
                save_index(my_consul, consul_key, stored_index)
                logger.info("Saved index %s to Consul on exit", stored_index)
            except consul.base.ConsulException as e:
                logger.exception("Failed to save index to Consul on exit: %s", e)


if __name__ == "__main__":
    main()
