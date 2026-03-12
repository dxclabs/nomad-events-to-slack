import json
import os

import pytest

from app import clear_input_list, format_event_to_slack_message


@pytest.fixture()
def sample_event():
    return {
        "AllocationID": "4f5c9fe9-7087-e213-6830-fc8e924db354",
        "NodeName": "node1",
        "JobID": "my-job",
        "JobType": "service",
        "TaskGroup": "my-group",
        "TaskName": "my-task",
        "Time": "2024-01-01 12:00:00",
        "EventType": "Terminated",
        "EventMessage": "OOM Killed",
        "EventDisplayMessage": "Task was OOM killed",
        "EventDetails": {"exit_code": "137", "oom_killed": "true"},
    }


@pytest.fixture()
def normal_event():
    return {
        "AllocationID": "1a2b3c4d-0000-0000-0000-000000000000",
        "NodeName": "node2",
        "JobID": "my-job",
        "JobType": "service",
        "TaskGroup": "my-group",
        "TaskName": "my-task",
        "Time": "2024-01-01 13:00:00",
        "EventType": "Started",
        "EventMessage": "",
        "EventDisplayMessage": "Task started",
        "EventDetails": {},
    }


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


class TestFormatEventToSlackMessage:
    def _blocks(self, event):
        result = json.loads(format_event_to_slack_message(event))
        return result["attachments"][0]["blocks"]

    def _attachment(self, event):
        return json.loads(format_event_to_slack_message(event))["attachments"][0]

    def test_returns_valid_json(self, sample_event):
        result = json.loads(format_event_to_slack_message(sample_event))
        assert "attachments" in result

    def test_attachment_has_color_and_blocks(self, sample_event):
        attachment = self._attachment(sample_event)
        assert "color" in attachment
        assert "blocks" in attachment

    def test_oom_kill_uses_red_color(self, sample_event):
        assert self._attachment(sample_event)["color"] == "#d72b3f"

    def test_oom_kill_header(self, sample_event):
        header = self._blocks(sample_event)[0]
        assert header["type"] == "header"
        assert "💀 OOM Kill" in header["text"]["text"]
        assert "my-task" in header["text"]["text"]
        assert "my-job" in header["text"]["text"]

    def test_normal_event_header(self, normal_event):
        header = self._blocks(normal_event)[0]
        assert "Started" in header["text"]["text"]
        assert "💀" not in header["text"]["text"]

    def test_default_color_for_normal_event(self, normal_event, monkeypatch):
        monkeypatch.delenv("EVENTS_COLOR", raising=False)
        assert self._attachment(normal_event)["color"] == "#36a64f"

    def test_custom_color_for_normal_event(self, normal_event, monkeypatch):
        monkeypatch.setenv("EVENTS_COLOR", "#ff0000")
        assert self._attachment(normal_event)["color"] == "#ff0000"

    def test_event_type_and_message_in_blocks(self, sample_event):
        fields_block = self._blocks(sample_event)[1]
        text = " ".join(f["text"] for f in fields_block["fields"])
        assert "Terminated" in text
        assert "Task was OOM killed" in text

    def test_job_and_node_in_blocks(self, sample_event):
        fields_block = self._blocks(sample_event)[2]
        text = " ".join(f["text"] for f in fields_block["fields"])
        assert "my-job" in text
        assert "node1" in text
        assert "my-group" in text
        assert "service" in text

    def test_context_has_time_and_allocation(self, sample_event):
        context = self._blocks(sample_event)[3]
        text = " ".join(e["text"] for e in context["elements"])
        assert "4f5c9fe9" in text
        assert "2024-01-01 12:00:00" in text

    def test_event_details_in_context(self, sample_event):
        context = self._blocks(sample_event)[3]
        details_text = context["elements"][0]["text"]
        assert "exit_code" in details_text
        assert "137" in details_text

    def test_empty_details_shows_none(self, normal_event):
        context = self._blocks(normal_event)[3]
        assert "_none_" in context["elements"][0]["text"]
