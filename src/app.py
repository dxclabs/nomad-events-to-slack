import http.client
import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any

import consul
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

COLOR_OOM = "#d72b3f"
COLOR_DEFAULT = "#36a64f"
CONNECT_TIMEOUT = 10


def clear_input_list(in_list: list[str]) -> None:
    while "" in in_list:
        in_list.remove("")
    while "None" in in_list:
        in_list.remove("None")


def get_stored_index(c_consul: consul.Consul, key: str) -> int:
    _, data = c_consul.kv.get(key)
    if data and data["Value"]:
        try:
            stored = json.loads(data["Value"].decode("utf-8"))
            return int(stored.get("index", 0))
        except (json.JSONDecodeError, ValueError):
            logger.warning("Consul key %s has unreadable value, resetting to 0", key)
            return 0
    return 0


def save_index(c_consul: consul.Consul, key: str, index: int) -> None:
    payload = json.dumps({"index": index, "updated": datetime.now(UTC).isoformat()})
    c_consul.kv.put(key, payload)


def _nomad_headers() -> dict[str, str]:
    token = os.getenv("NOMAD_TOKEN", "")
    return {"X-Nomad-Token": token} if token else {}


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


def collect_window(index: int, window_seconds: int) -> tuple[list[dict[str, Any]], int]:
    """Stream allocation events from index until the silence window expires."""
    nomad_addr = os.getenv("NOMAD_ADDR", "http://127.0.0.1:4646")
    url = f"{nomad_addr}/v1/event/stream"
    params = {"topic": "Allocation", "index": str(index)}

    collected: list[dict[str, Any]] = []
    new_index = index

    try:
        with requests.get(
            url,
            params=params,
            headers=_nomad_headers(),
            stream=True,
            timeout=(CONNECT_TIMEOUT, window_seconds),
        ) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if line:
                    batch = json.loads(line)
                    if batch.get("Events"):
                        collected.extend(batch["Events"])
                    if batch.get("Index"):
                        new_index = int(batch["Index"])
    except requests.exceptions.RequestException as e:
        # ReadTimeout may be wrapped as ConnectionError in some urllib3 versions
        is_timeout = (
            isinstance(e, requests.exceptions.Timeout) or "timed out" in str(e).lower()
        )
        if not is_timeout:
            raise
        # window expired normally - return what we have

    return collected, new_index


def extract_task_events(
    stream_events: list[dict[str, Any]],
    node_name_list: list[str],
    job_id_list: list[str],
    event_types_list: list[str],
    event_message_filters: list[str],
) -> list[dict[str, Any]]:
    """Extract the latest task event per task from stream allocation events."""
    task_events = []
    for stream_event in stream_events:
        alloc = stream_event.get("Payload", {}).get("Allocation", {})
        if not alloc:
            continue
        if node_name_list and alloc.get("NodeName") not in node_name_list:
            continue
        if job_id_list and alloc.get("JobID") not in job_id_list:
            continue
        for task_name, state in (alloc.get("TaskStates") or {}).items():
            events = state.get("Events") or []
            if not events:
                continue
            latest = events[-1]  # only the most recent transition per task
            if event_types_list and latest["Type"] not in event_types_list:
                continue
            if (
                event_message_filters
                and latest.get("Message") not in event_message_filters
            ):
                continue
            task_events.append(
                {
                    "AllocationID": alloc["ID"],
                    "NodeName": alloc.get("NodeName", ""),
                    "JobID": alloc.get("JobID", ""),
                    "JobType": alloc.get("JobType", ""),
                    "TaskGroup": alloc.get("TaskGroup", ""),
                    "TaskName": task_name,
                    "Time": datetime.fromtimestamp(
                        float(latest["Time"]) / 1000000000, tz=UTC
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "EventType": latest["Type"],
                    "EventMessage": latest.get("Message", ""),
                    "EventDisplayMessage": latest.get("DisplayMessage", ""),
                    "EventDetails": latest.get("Details", {}),
                }
            )
    return task_events


def group_events_by_job(
    events: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        groups.setdefault(event["JobID"], []).append(event)
    return groups


def format_job_events_to_slack(job_id: str, events: list[dict[str, Any]]) -> str:
    has_oom = any(e["EventDetails"].get("oom_killed") == "true" for e in events)
    color = COLOR_OOM if has_oom else os.getenv("EVENTS_COLOR", COLOR_DEFAULT)
    n = len(events)
    header = f"{'💀 ' if has_oom else ''}{job_id} — {n} event{'s' if n != 1 else ''}"

    event_lines = []
    for e in events:
        oom_flag = " 💀" if e["EventDetails"].get("oom_killed") == "true" else ""
        line = f"• *{e['TaskName']}* — {e['EventType']}{oom_flag}"
        if e["EventDisplayMessage"]:
            line += f"\n  _{e['EventDisplayMessage']}_"
        event_lines.append(line)

    nodes = sorted({e["NodeName"] for e in events})
    earliest_time = min(e["Time"] for e in events)

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
            "elements": [
                {"type": "mrkdwn", "text": f"*Nodes:* {', '.join(nodes)}"},
                {"type": "mrkdwn", "text": f"*Time:* {earliest_time}"},
            ],
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
    window_seconds = int(os.getenv("WINDOW_SECONDS", "10"))

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

    index = 0
    if use_consul == "true" and my_consul:
        try:
            index = get_stored_index(my_consul, consul_key)
            logger.info("Resuming from Consul index %s", index)
        except consul.base.ConsulException as e:
            logger.exception("Failed to read index from Consul: %s", e)
            raise RuntimeError from None

    if index == 0:
        try:
            index = get_current_nomad_index()
            logger.info("No stored index — starting from current Nomad index %s", index)
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Could not fetch current Nomad index, starting from 0: %s", e
            )

    logger.info("ENV is ok. Starting event stream loop (window=%ss).", window_seconds)

    while True:
        try:
            stream_events, new_index = collect_window(index, window_seconds)
        except requests.exceptions.ConnectionError as e:
            logger.error("Stream connection failed: %s", e)
            time.sleep(10)
            continue
        except requests.exceptions.HTTPError as e:
            logger.error("Stream HTTP error: %s", e)
            time.sleep(10)
            continue

        if stream_events:
            task_events = extract_task_events(
                stream_events, node_names, job_ids, event_types, event_message_filters
            )
            job_groups = group_events_by_job(task_events)

            logger.info(
                "Window: %s stream events → %s task events across %s job(s)",
                len(stream_events),
                len(task_events),
                len(job_groups),
            )

            for job_id, job_events in job_groups.items():
                try:
                    post_message_to_slack(
                        slack_web_hook_url,
                        format_job_events_to_slack(job_id, job_events),
                    )
                except (ConnectionError, OSError) as e:
                    logger.exception(
                        "Failed to post Slack message for %s: %s", job_id, e
                    )
                    raise RuntimeError from None

        if new_index > index:
            index = new_index
            if use_consul == "true" and my_consul:
                try:
                    save_index(my_consul, consul_key, index)
                except consul.base.ConsulException as e:
                    logger.exception("Failed to save index to Consul: %s", e)


if __name__ == "__main__":
    main()
