import http.client
import json
import logging
import logging.handlers
import os
import queue
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import consul
import nomad
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Separate logger for raw Nomad stream events — file-only, never console.
# Configured (or left as a no-op NullHandler) in main() based on debug flag.
stream_logger = logging.getLogger("nomad.stream")
stream_logger.propagate = False
stream_logger.addHandler(logging.NullHandler())

COLOR_OOM = "#d72b3f"
COLOR_DEFAULT = "#36a64f"
CONNECT_TIMEOUT = 10
QUEUE_POLL_SECS = 2.0

# Terminal event types keyed by Nomad job type.
# service/system: "Started" ends a restart cycle; "Terminated" is debounced so
#   a Terminated→Restarting→Started sequence isn't reported mid-flight.
# batch/sysbatch: "Terminated" is the expected end state; "Started" is
#   mid-sequence (Received→Task Setup→Started→Terminated) and must not trigger.
TERMINAL_EVENT_TYPES: dict[str, frozenset[str]] = {
    "service": frozenset({"Started", "Killed", "Not Restarting"}),
    "system": frozenset({"Started", "Killed", "Not Restarting"}),
    "batch": frozenset({"Terminated", "Killed", "Not Restarting"}),
    "sysbatch": frozenset({"Terminated", "Killed", "Not Restarting"}),
}
# Fallback for unknown/unset job type — conservative superset.
_DEFAULT_TERMINAL_EVENT_TYPES = frozenset(
    {"Started", "Terminated", "Killed", "Not Restarting"}
)

# How long to wait after a "Terminated" event before treating it as terminal
# for service/system jobs. Gives Nomad time to emit the follow-up "Restarting"
# event for template-change restarts.
TERMINATED_DEBOUNCE_SECS = 30

# Termination reporting rules keyed by job_type, then job_id or "default".
#
#   report:  "always"   — send a Slack message on every terminal event
#            "abnormal" — only send when something went wrong (OOM, unhealthy,
#                         non-zero exit, Killed, Not Restarting)
#            "never"    — suppress all terminal reports
#   partial: "always"   — send a message when a job is evicted as stale
#            "never"    — suppress stale/partial reports
#
# Per-job overrides sit alongside "default", e.g.:
#   "batch": {
#       "my-nightly-job": {"report": "always", "partial": "never"},
#       "default": ...,
#   }
#
# TODO: in future, batch jobs with a known cron schedule could report on
#       missed runs by comparing last_reported_at against the expected cadence.
_TerminationRule = dict[str, str]
TERMINATION_RULES: dict[str, dict[str, _TerminationRule]] = {
    "service": {"default": {"report": "always", "partial": "always"}},
    "system": {"default": {"report": "always", "partial": "always"}},
    "batch": {"default": {"report": "abnormal", "partial": "always"}},
    "sysbatch": {"default": {"report": "abnormal", "partial": "always"}},
}
_DEFAULT_TERMINATION_RULE: _TerminationRule = {"report": "always", "partial": "always"}


def _get_termination_rule(job_type: str, job_id: str) -> _TerminationRule:
    type_rules = TERMINATION_RULES.get(job_type, {})
    return (
        type_rules.get(job_id) or type_rules.get("default") or _DEFAULT_TERMINATION_RULE
    )


def _is_abnormal(job: dict[str, Any]) -> bool:
    """Return True if the allocation ended in an unexpected/unhealthy state."""
    if job["meta"]["oom_kill_count"] > 0:
        return True
    if job["deployment_healthy"] is False:
        return True
    if job["last_state"] in {"Killed", "Not Restarting"}:
        return True
    for e in job["events"]:
        if (
            e["EventType"] == "Terminated"
            and e["EventDetails"].get("exit_code", "0") != "0"
        ):
            return True
    return False


def _get_job_type(job_id: str) -> str:
    """Infer job type from JobID.

    Nomad allocation events don't include a JobType field, so we derive it
    from the JobID where we can. Periodic jobs have the form:
      <parent-job-id>/periodic-<unix-timestamp>

    Returns 'batch' for periodic jobs, '' otherwise (unknown — a future
    implementation could query the Nomad Job API to resolve service/system).
    """
    if "/periodic-" in job_id:
        return "batch"
    return ""


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
        "alloc_name": "",  # human-readable, e.g. "my-job.web[1]"
        "job_id": "",  # JobID, used for termination rule lookup
        "job_type": None,  # "service", "batch", "system", "sysbatch"
        "last_state": None,
        "last_updated_at": now,
        "start_index": None,
        # None = waiting for health check, True = healthy, False = unhealthy
        "deployment_healthy": None,
        # task_name → TaskStates[task].State ("pending", "running", "dead")
        "task_states": {},
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

    _health_label = {
        True: " ✅ healthy",
        False: " ❌ unhealthy",
        None: " ⏳ awaiting health check",
    }

    event_lines = []
    for e in events:
        oom_flag = " 💀" if e["EventDetails"].get("oom_killed") == "true" else ""
        health_flag = (
            _health_label[e["DeploymentHealthy"]]
            if e["EventType"] == "Started" and e["DeploymentHealthy"] is not None
            else ""
        )
        line = f"• *{e['TaskName']}* — {e['EventType']}{oom_flag}{health_flag}"
        if e["EventDisplayMessage"]:
            line += f"\n  _{e['EventDisplayMessage']}_"
        event_lines.append(line)

    nodes = sorted({e["NodeName"] for e in events})
    earliest_time = min(e["Time"] for e in events).strftime("%Y-%m-%d %H:%M:%S UTC")

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
        _log_dir = Path(__file__).parent.parent / "logs"
        _log_dir.mkdir(exist_ok=True)
        _stream_log = _log_dir / "stream.log"
        _fh = logging.handlers.RotatingFileHandler(
            _stream_log,
            maxBytes=100_000,
            backupCount=3,
        )
        _fh.setLevel(logging.DEBUG)
        _fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        stream_logger.addHandler(_fh)
        stream_logger.setLevel(logging.DEBUG)
        logger.debug("Raw event stream log: %s", _stream_log)
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

    # Per-allocation event accumulator: alloc_id → record dict
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
                stream_logger.debug(json.dumps(batch, default=str))
                for stream_event in batch.get("Events") or []:
                    event_index = int(stream_event.get("Index") or 0)

                    # Skip snapshot events replayed at or below our stored cursor
                    if event_index <= stored_index:
                        continue

                    alloc = stream_event.get("Payload", {}).get("Allocation", {})
                    if not alloc:
                        continue

                    job_id = alloc.get("JobID", "")
                    alloc_id = alloc.get("ID", "")
                    if not job_id or not alloc_id:
                        continue

                    # Skip events at or below the index already covered by the
                    # last report for this allocation — guards against Nomad
                    # re-emitting stale allocation state after a terminal event.
                    if (
                        alloc_id in job_events
                        and event_index <= job_events[alloc_id]["last_reported_index"]
                    ):
                        continue

                    # Optional filters
                    if node_names and alloc.get("NodeName") not in node_names:
                        continue
                    if job_ids and job_id not in job_ids:
                        continue

                    # Human-readable allocation name, e.g. "my-job.web[1]"
                    alloc_name = alloc.get("Name") or job_id

                    job_type = _get_job_type(job_id)
                    # None = waiting, True = healthy, False = unhealthy
                    deployment_healthy = (alloc.get("DeploymentStatus") or {}).get(
                        "Healthy"
                    )

                    for task_name, state in (alloc.get("TaskStates") or {}).items():
                        task_event_list = state.get("Events") or []
                        if not task_event_list:
                            continue

                        task_state = state.get("State", "")
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

                        dedup_key = (
                            alloc["ID"],
                            task_name,
                            event_type,
                            latest["Time"],
                            deployment_healthy,
                        )
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
                            "DeploymentHealthy": deployment_healthy,
                        }

                        if alloc_id not in job_events:
                            rec = _empty_job_record()
                            rec["events"] = [task_event]
                            rec["seen_keys"] = {dedup_key}
                            rec["alloc_name"] = alloc_name
                            rec["job_id"] = job_id
                            rec["job_type"] = job_type
                            rec["last_state"] = event_type
                            rec["last_updated_at"] = event_time
                            rec["start_index"] = event_index
                            rec["max_index"] = event_index
                            rec["deployment_healthy"] = deployment_healthy
                            rec["task_states"] = {task_name: task_state}
                            job_events[alloc_id] = rec
                            _update_meta(
                                rec["meta"], event_type, latest.get("Details", {})
                            )
                            logger.debug(
                                "New alloc tracked: %s (%s) index=%s"
                                " | state=%s deployment_healthy=%s task_states=%s",
                                alloc_name,
                                job_type,
                                event_index,
                                event_type,
                                deployment_healthy,
                                rec["task_states"],
                            )
                        elif dedup_key not in job_events[alloc_id]["seen_keys"]:
                            job = job_events[alloc_id]
                            job["events"].append(task_event)
                            job["seen_keys"].add(dedup_key)
                            job["last_state"] = event_type
                            # Update fields that may not have been set on first event
                            if alloc_name and not job["alloc_name"]:
                                job["alloc_name"] = alloc_name
                            if job_id and not job["job_id"]:
                                job["job_id"] = job_id
                            if job_type and not job["job_type"]:
                                job["job_type"] = job_type
                            job["deployment_healthy"] = deployment_healthy
                            job["task_states"][task_name] = task_state
                            if event_time > job["last_updated_at"]:
                                job["last_updated_at"] = event_time
                            if event_index > job["max_index"]:
                                job["max_index"] = event_index
                            _update_meta(
                                job["meta"], event_type, latest.get("Details", {})
                            )
                            logger.debug(
                                "%s (%s) index=%s"
                                " | state=%s deployment_healthy=%s task_states=%s",
                                job["alloc_name"],
                                job["job_type"],
                                event_index,
                                event_type,
                                deployment_healthy,
                                job["task_states"],
                            )

            # Report any allocation that has reached a terminal state or gone stale
            now = datetime.now(UTC)
            for alloc_id in list(job_events.keys()):
                job = job_events[alloc_id]
                if not job["events"]:
                    continue
                age_secs = (now - job["last_updated_at"]).total_seconds()
                terminal_types = TERMINAL_EVENT_TYPES.get(
                    job["job_type"] or "", _DEFAULT_TERMINAL_EVENT_TYPES
                )
                is_terminal = job["last_state"] in terminal_types
                # For service/system jobs "Started" is only terminal once the
                # health check has resolved. When deployment_healthy is None
                # (still awaiting checks), keep accumulating so the healthy/
                # unhealthy event can be grouped into the same message.
                if (
                    is_terminal
                    and job["last_state"] == "Started"
                    and job["deployment_healthy"] is None
                ):
                    is_terminal = False
                # Debounce "Terminated" for service/system jobs: wait to see if
                # "Restarting" follows before reporting (template-change restart).
                # batch/sysbatch already have "Terminated" in their terminal set
                # so this branch is never reached for them.
                if not is_terminal and job["last_state"] == "Terminated":
                    is_terminal = age_secs >= TERMINATED_DEBOUNCE_SECS
                # deployment_healthy=False means Nomad has marked the allocation
                # unhealthy and will take no further action — report immediately.
                # Use `is False` explicitly: None means still waiting for checks.
                if not is_terminal and job["deployment_healthy"] is False:
                    is_terminal = True
                is_stale = age_secs > max_job_age_secs
                if is_terminal or is_stale:
                    rule = _get_termination_rule(job["job_type"] or "", job["job_id"])
                    abnormal = _is_abnormal(job)
                    logger.debug(
                        "%s %s — job_type=%s rule=%s abnormal=%s is_terminal=%s"
                        " is_stale=%s",
                        job["alloc_name"] or alloc_id,
                        job["last_state"],
                        job["job_type"],
                        rule,
                        abnormal,
                        is_terminal,
                        is_stale,
                    )
                    if is_terminal and (
                        rule["report"] == "always"
                        or (rule["report"] == "abnormal" and abnormal)
                    ):
                        _report_and_purge(
                            job["alloc_name"] or alloc_id,
                            job,
                            slack_web_hook_url,
                        )
                    elif is_stale and rule["partial"] == "always":
                        _report_and_purge(
                            job["alloc_name"] or alloc_id,
                            job,
                            slack_web_hook_url,
                            partial=True,
                        )
                    elif is_terminal or is_stale:
                        # Rule says suppress — still purge so the accumulator
                        # doesn't stall index advancement or re-report later.
                        reason = (
                            f"report={rule['report']}"
                            if is_terminal
                            else f"partial={rule['partial']}"
                        )
                        logger.debug(
                            "Not reporting %s %s — suppressed by termination rule (%s)",
                            job["alloc_name"] or alloc_id,
                            job["last_state"],
                            reason,
                        )
                        _report_and_purge(
                            job["alloc_name"] or alloc_id,
                            job,
                            slack_hook_url="",  # empty → post_message_to_slack no-ops
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
