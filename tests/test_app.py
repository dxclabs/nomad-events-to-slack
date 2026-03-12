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
    def test_returns_valid_json(self, sample_event):
        result = format_event_to_slack_message(sample_event)
        parsed = json.loads(result)
        assert "attachments" in parsed

    def test_attachment_has_required_fields(self, sample_event):
        result = json.loads(format_event_to_slack_message(sample_event))
        attachment = result["attachments"][0]
        assert "color" in attachment
        assert "footer" in attachment
        assert "fields" in attachment

    def test_footer_contains_allocation_id(self, sample_event):
        result = json.loads(format_event_to_slack_message(sample_event))
        footer = result["attachments"][0]["footer"]
        assert sample_event["AllocationID"] in footer
        assert sample_event["Time"] in footer

    def test_fields_contain_task_and_job_info(self, sample_event):
        result = json.loads(format_event_to_slack_message(sample_event))
        fields = {f["title"]: f["value"] for f in result["attachments"][0]["fields"]}
        assert sample_event["TaskName"] in fields["New Event"]
        assert sample_event["NodeName"] in fields["Job Info"]
        assert sample_event["JobID"] in fields["Job Info"]

    def test_event_details_included(self, sample_event):
        result = json.loads(format_event_to_slack_message(sample_event))
        fields = {f["title"]: f["value"] for f in result["attachments"][0]["fields"]}
        assert "exit_code" in fields["Event Details"]
        assert "137" in fields["Event Details"]

    def test_default_color(self, sample_event):
        os.environ.pop("EVENTS_COLOR", None)
        result = json.loads(format_event_to_slack_message(sample_event))
        assert result["attachments"][0]["color"] == "#36a64f"

    def test_custom_color(self, sample_event, monkeypatch):
        monkeypatch.setenv("EVENTS_COLOR", "#ff0000")
        result = json.loads(format_event_to_slack_message(sample_event))
        assert result["attachments"][0]["color"] == "#ff0000"
