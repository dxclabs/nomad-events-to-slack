import json

import pytest

from app import (
    clear_input_list,
    extract_task_events,
    format_job_events_to_slack,
    group_events_by_job,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def stream_event_started():
    return {
        "Topic": "Allocation",
        "Type": "AllocationUpdated",
        "Index": 1234,
        "Payload": {
            "Allocation": {
                "ID": "4f5c9fe9-7087-e213-6830-fc8e924db354",
                "JobID": "my-job",
                "JobType": "service",
                "NodeName": "node1",
                "TaskGroup": "my-group",
                "TaskStates": {
                    "my-task": {
                        "Events": [
                            {
                                "Type": "Received",
                                "Time": 1704067200000000000,
                                "Message": "",
                                "DisplayMessage": "Task received",
                                "Details": {},
                            },
                            {
                                "Type": "Started",
                                "Time": 1704067215000000000,
                                "Message": "",
                                "DisplayMessage": "Task started successfully",
                                "Details": {},
                            },
                        ],
                    },
                },
            },
        },
    }


@pytest.fixture()
def stream_event_oom():
    return {
        "Topic": "Allocation",
        "Type": "AllocationUpdated",
        "Index": 1235,
        "Payload": {
            "Allocation": {
                "ID": "aaaaaaaa-0000-0000-0000-000000000000",
                "JobID": "my-job",
                "JobType": "service",
                "NodeName": "node1",
                "TaskGroup": "my-group",
                "TaskStates": {
                    "my-task": {
                        "Events": [
                            {
                                "Type": "Terminated",
                                "Time": 1704067300000000000,
                                "Message": "OOM Killed",
                                "DisplayMessage": "Task killed: OOM",
                                "Details": {"exit_code": "137", "oom_killed": "true"},
                            },
                        ],
                    },
                },
            },
        },
    }


@pytest.fixture()
def task_event_started():
    return {
        "AllocationID": "4f5c9fe9-7087-e213-6830-fc8e924db354",
        "NodeName": "node1",
        "JobID": "my-job",
        "JobType": "service",
        "TaskGroup": "my-group",
        "TaskName": "my-task",
        "Time": "2024-01-01 12:00:15",
        "EventType": "Started",
        "EventMessage": "",
        "EventDisplayMessage": "Task started successfully",
        "EventDetails": {},
    }


@pytest.fixture()
def task_event_oom():
    return {
        "AllocationID": "aaaaaaaa-0000-0000-0000-000000000000",
        "NodeName": "node1",
        "JobID": "my-job",
        "JobType": "service",
        "TaskGroup": "my-group",
        "TaskName": "my-task",
        "Time": "2024-01-01 12:01:40",
        "EventType": "Terminated",
        "EventMessage": "OOM Killed",
        "EventDisplayMessage": "Task killed: OOM",
        "EventDetails": {"exit_code": "137", "oom_killed": "true"},
    }


# ---------------------------------------------------------------------------
# TestClearInputList
# ---------------------------------------------------------------------------


class TestClearInputList:
    def test_removes_empty_strings(self):
        lst = ["a", "", "b", ""]
        clear_input_list(lst)
        assert lst == ["a", "b"]

    def test_removes_none_strings(self):
        lst = ["a", "None", "b"]
        clear_input_list(lst)
        assert lst == ["a", "b"]

    def test_removes_mixed(self):
        lst = ["", "None", "a", "", "None"]
        clear_input_list(lst)
        assert lst == ["a"]

    def test_empty_list(self):
        lst = []
        clear_input_list(lst)
        assert lst == []

    def test_no_removals_needed(self):
        lst = ["a", "b", "c"]
        clear_input_list(lst)
        assert lst == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# TestExtractTaskEvents
# ---------------------------------------------------------------------------


class TestExtractTaskEvents:
    def test_extracts_latest_event_only(self, stream_event_started):
        events = extract_task_events([stream_event_started], [], [], [], [])
        assert len(events) == 1
        assert events[0]["EventType"] == "Started"  # latest, not Received

    def test_fields_mapped_correctly(self, stream_event_started):
        e = extract_task_events([stream_event_started], [], [], [], [])[0]
        assert e["JobID"] == "my-job"
        assert e["NodeName"] == "node1"
        assert e["TaskName"] == "my-task"
        assert e["TaskGroup"] == "my-group"
        assert e["AllocationID"] == "4f5c9fe9-7087-e213-6830-fc8e924db354"

    def test_filters_by_node_name(self, stream_event_started):
        assert (
            extract_task_events([stream_event_started], ["other-node"], [], [], [])
            == []
        )

    def test_node_name_match_passes(self, stream_event_started):
        assert (
            len(extract_task_events([stream_event_started], ["node1"], [], [], [])) == 1
        )

    def test_filters_by_job_id(self, stream_event_started):
        assert (
            extract_task_events([stream_event_started], [], ["other-job"], [], []) == []
        )

    def test_job_id_match_passes(self, stream_event_started):
        assert (
            len(extract_task_events([stream_event_started], [], ["my-job"], [], []))
            == 1
        )

    def test_filters_by_event_type(self, stream_event_started):
        assert (
            extract_task_events([stream_event_started], [], [], ["Terminated"], [])
            == []
        )

    def test_skips_missing_payload(self):
        assert extract_task_events([{"Topic": "Allocation"}], [], [], [], []) == []

    def test_skips_empty_task_states(self):
        event = {
            "Payload": {
                "Allocation": {
                    "ID": "x",
                    "JobID": "j",
                    "JobType": "s",
                    "NodeName": "n",
                    "TaskGroup": "g",
                    "TaskStates": {},
                }
            }
        }
        assert extract_task_events([event], [], [], [], []) == []


# ---------------------------------------------------------------------------
# TestGroupEventsByJob
# ---------------------------------------------------------------------------


class TestGroupEventsByJob:
    def test_groups_by_job_id(self, task_event_started, task_event_oom):
        other = {**task_event_started, "JobID": "other-job"}
        groups = group_events_by_job([task_event_started, task_event_oom, other])
        assert set(groups.keys()) == {"my-job", "other-job"}
        assert len(groups["my-job"]) == 2
        assert len(groups["other-job"]) == 1

    def test_empty_events(self):
        assert group_events_by_job([]) == {}

    def test_single_job(self, task_event_started):
        groups = group_events_by_job([task_event_started])
        assert list(groups.keys()) == ["my-job"]


# ---------------------------------------------------------------------------
# TestFormatJobEventsToSlack
# ---------------------------------------------------------------------------


class TestFormatJobEventsToSlack:
    def _parsed(self, job_id, events):
        return json.loads(format_job_events_to_slack(job_id, events))

    def _attachment(self, job_id, events):
        return self._parsed(job_id, events)["attachments"][0]

    def _blocks(self, job_id, events):
        return self._attachment(job_id, events)["blocks"]

    def test_returns_valid_json(self, task_event_started):
        result = self._parsed("my-job", [task_event_started])
        assert "attachments" in result
        assert "text" in result

    def test_notification_text_matches_header(self, task_event_started):
        result = self._parsed("my-job", [task_event_started])
        assert (
            result["text"]
            == self._blocks("my-job", [task_event_started])[0]["text"]["text"]
        )

    def test_header_contains_job_id_and_count(self, task_event_started):
        header = self._blocks("my-job", [task_event_started])[0]["text"]["text"]
        assert "my-job" in header
        assert "1 event" in header

    def test_plural_events(self, task_event_started, task_event_oom):
        header = self._blocks("my-job", [task_event_started, task_event_oom])[0][
            "text"
        ]["text"]
        assert "2 events" in header

    def test_oom_color(self, task_event_oom):
        assert self._attachment("my-job", [task_event_oom])["color"] == "#d72b3f"

    def test_oom_header_prefix(self, task_event_oom):
        header = self._blocks("my-job", [task_event_oom])[0]["text"]["text"]
        assert "💀" in header

    def test_normal_event_default_color(self, task_event_started, monkeypatch):
        monkeypatch.delenv("EVENTS_COLOR", raising=False)
        assert self._attachment("my-job", [task_event_started])["color"] == "#36a64f"

    def test_custom_color(self, task_event_started, monkeypatch):
        monkeypatch.setenv("EVENTS_COLOR", "#ff9500")
        assert self._attachment("my-job", [task_event_started])["color"] == "#ff9500"

    def test_event_body_contains_task_and_type(self, task_event_started):
        section = self._blocks("my-job", [task_event_started])[1]["text"]["text"]
        assert "my-task" in section
        assert "Started" in section

    def test_context_contains_node_and_time(self, task_event_started):
        context = self._blocks("my-job", [task_event_started])[-1]
        text = " ".join(e["text"] for e in context["elements"])
        assert "node1" in text
        assert "2024-01-01" in text

    def test_mixed_batch_oom_wins_color(self, task_event_started, task_event_oom):
        assert (
            self._attachment("my-job", [task_event_started, task_event_oom])["color"]
            == "#d72b3f"
        )
