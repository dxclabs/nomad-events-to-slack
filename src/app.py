import base64
import http.client
import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any

import consul
import nomad

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

COLOR_OOM = "#d72b3f"
COLOR_DEFAULT = "#36a64f"


def clear_input_list(in_list: list[str]) -> None:
    while "" in in_list:
        in_list.remove("")
    while "None" in in_list:
        in_list.remove("None")


def consul_put(c_consul: consul.Consul, key: str, value: list[dict[str, Any]]) -> bool:
    logger.debug("Put key: %s and value: %s to consul", key, value)
    return c_consul.kv.put(
        key, base64.urlsafe_b64encode(json.dumps(value).encode("utf-8"))
    )


def consul_get(c_consul: consul.Consul, key: str) -> list[dict[str, Any]]:
    index, data = c_consul.kv.get(key)
    logger.debug("Get index: %s and data %s from consul", index, data)
    if data and data["Value"]:
        logger.debug("Get key: %s and value: %s from consul", key, data["Value"])
        return json.loads(base64.urlsafe_b64decode(data["Value"]).decode("utf-8"))
    c_consul.kv.put(key, [])
    return []


def get_alloc_events(
    c_nomad: nomad.Nomad,
    sent_events_list: list[dict[str, Any]],
    node_name_list: list[str],
    job_id_list: list[str],
    event_types_list: list[str],
    event_message_filters: list[str],
) -> list[dict[str, Any]]:
    alloc_events = []
    allocations = c_nomad.allocations.get_allocations()
    logger.info("Get %s allocations", len(allocations))
    for allocation in allocations:
        logger.debug("Raw allocation: %s", allocation)
        if (
            (
                allocation["NodeName"] in node_name_list
                and allocation["JobID"] in job_id_list
            )
            or (allocation["NodeName"] in node_name_list and len(job_id_list) == 0)
            or (len(node_name_list) == 0 and allocation["JobID"] in job_id_list)
            or (len(node_name_list) == 0 and len(job_id_list) == 0)
        ):
            for task, state in allocation["TaskStates"].items():
                for event in state["Events"]:
                    if (
                        (
                            event["Type"] in event_types_list
                            and event["Message"] in event_message_filters
                        )
                        or (
                            event["Type"] in event_types_list
                            and len(event_message_filters) == 0
                        )
                        or (
                            len(event_types_list)
                            and event["Message"] in event_message_filters
                        )
                        or (
                            len(event_types_list) == 0
                            and len(event_message_filters) == 0
                        )
                    ):
                        alloc_event = {
                            "AllocationID": allocation["ID"],
                            "NodeName": allocation["NodeName"],
                            "JobID": allocation["JobID"],
                            "JobType": allocation["JobType"],
                            "TaskGroup": allocation["TaskGroup"],
                            "TaskName": task,
                            "Time": datetime.fromtimestamp(
                                float(event["Time"]) / 1000000000, tz=UTC
                            ).strftime("%Y-%m-%d %H:%M:%S"),
                            "EventType": event["Type"],
                            "EventMessage": event["Message"],
                            "EventDisplayMessage": event["DisplayMessage"],
                            "EventDetails": event["Details"],
                        }
                        logger.debug("Filtered alloc event: ", alloc_event)
                        alloc_events.append(alloc_event)
    current_alloc_events = [evt for evt in alloc_events if evt not in sent_events_list]
    logger.debug("Current alloc_events: ", current_alloc_events)
    return current_alloc_events


def format_event_to_slack_message(event: dict[str, Any]) -> str:
    is_oom = event["EventDetails"].get("oom_killed") == "true"
    color = COLOR_OOM if is_oom else os.getenv("EVENTS_COLOR", COLOR_DEFAULT)
    header = (
        f"{'💀 OOM Kill' if is_oom else event['EventType']}: "
        f"{event['TaskName']} ({event['JobID']})"
    )

    details_text = (
        " | ".join(f"{k}: {v}" for k, v in event["EventDetails"].items())
        if event["EventDetails"]
        else "_none_"
    )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header, "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Event Type*\n{event['EventType']}"},
                {
                    "type": "mrkdwn",
                    "text": f"*Message*\n{event['EventDisplayMessage'] or '_none_'}",
                },
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Job*\n{event['JobID']}"},
                {"type": "mrkdwn", "text": f"*Node*\n{event['NodeName']}"},
                {"type": "mrkdwn", "text": f"*Task Group*\n{event['TaskGroup']}"},
                {"type": "mrkdwn", "text": f"*Job Type*\n{event['JobType']}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*Details:* {details_text}"},
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*Time:* {event['Time']}  |  "
                        f"*Allocation:* {event['AllocationID']}"
                    ),
                },
            ],
        },
    ]

    return json.dumps({"text": header, "attachments": [{"color": color, "blocks": blocks}]})


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


def main() -> None:
    sent_events = []
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
    if slack_web_hook_url == "":
        logger.error(
            "ENV SLACK_WEB_HOOK_URL not set. Set it to non-empty string and try again"
        )
        raise RuntimeError from None
    clear_input_list(node_names)
    clear_input_list(job_ids)
    clear_input_list(event_types)
    clear_input_list(event_message_filters)
    my_nomad = nomad.Nomad()
    my_consul = consul.Consul()
    logger.info("ENV is ok. Start Loop.")
    while True:
        if use_consul == "true":
            try:
                sent_events = consul_get(my_consul, consul_key)
            except consul.base.ACLPermissionDenied as e:
                logger.exception("Consul permission denied Exception: %s", e)
                raise RuntimeError from None
            except consul.base.Timeout as e:
                logger.exception("Consul timeout Exception: %s", e)
                raise RuntimeError from None
            except consul.base.ConsulException as e:
                logger.exception("Consul general Exception: %s", e)
                raise RuntimeError from None
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.exception("Json Exception: %s", e)
                raise RuntimeError from None
            except Exception as e:
                logger.exception("General Exception: %s", e)
                raise RuntimeError from None

        try:
            events = get_alloc_events(
                my_nomad,
                sent_events,
                node_names,
                job_ids,
                event_types,
                event_message_filters,
            )
        except Exception as e:
            logger.exception("Can't get info from Nomad. Exception: %s", e)
            raise SystemExit from None

        logger.info("Get %s new events", len(events))
        for event in events:
            logger.debug("Event to Send: %s", event)
            try:
                slack_result = post_message_to_slack(
                    slack_web_hook_url, format_event_to_slack_message(event)
                )
            except Exception as e:
                logger.exception("Can't send message to Slack. Exception: %s", e)
                raise RuntimeError from None

            if slack_result:
                sent_events.append(event)
                if use_consul == "true":
                    try:
                        consul_put(my_consul, consul_key, sent_events)
                    except Exception as e:
                        logger.exception("Can't put value to Consul. Exception: %s", e)
                        raise RuntimeError from None

        if len(sent_events):
            logger.info("Sent %s events. Wait 30 sec and check again", len(sent_events))

        time.sleep(30)


if __name__ == "__main__":
    main()
