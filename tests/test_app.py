import json
from datetime import UTC, datetime

import pytest

from app import (
    _get_job_type,
    _get_termination_rule,
    _is_abnormal,
    _update_meta,
    clear_input_list,
    format_job_events_to_slack,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_meta(**overrides: int) -> dict:
    base = {
        "oom_kill_count": 0,
        "restart_count": 0,
        "download_attempts": 0,
        "health_check_failures": 0,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def task_event_started():
    return {
        "AllocationID": "4f5c9fe9-7087-e213-6830-fc8e924db354",
        "NodeName": "node1",
        "JobID": "my-job",
        "JobType": "service",
        "TaskGroup": "my-group",
        "TaskName": "my-task",
        "Time": datetime(2024, 1, 1, 12, 0, 15, tzinfo=UTC),
        "EventType": "Started",
        "EventMessage": "",
        "EventDisplayMessage": "Task started successfully",
        "EventDetails": {},
        "DeploymentHealthy": True,
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
        "Time": datetime(2024, 1, 1, 12, 1, 40, tzinfo=UTC),
        "EventType": "Terminated",
        "EventMessage": "OOM Killed",
        "EventDisplayMessage": "Task killed: OOM",
        "EventDetails": {"exit_code": "137", "oom_killed": "true"},
        "DeploymentHealthy": None,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(
    *,
    job_type: str = "batch",
    last_state: str = "Terminated",
    deployment_healthy: bool | None = None,
    oom_kill_count: int = 0,
    events: list | None = None,
) -> dict:
    """Minimal job record for testing terminal logic helpers."""
    return {
        "job_type": job_type,
        "job_id": "my-batch-job",
        "alloc_name": "my-batch-job.run[0]",
        "last_state": last_state,
        "deployment_healthy": deployment_healthy,
        "events": events or [],
        "meta": _default_meta(oom_kill_count=oom_kill_count),
    }


def _terminated_event(exit_code: str = "0") -> dict:
    return {
        "EventType": "Terminated",
        "EventDetails": {"exit_code": exit_code},
    }


# ---------------------------------------------------------------------------
# TestGetJobType
# ---------------------------------------------------------------------------


class TestGetJobType:
    def test_periodic_job_returns_batch(self):
        assert _get_job_type("my-cron/periodic-1773827100") == "batch"

    def test_periodic_job_with_nested_path(self):
        assert _get_job_type("esg-netsuite-cron/periodic-1773751021") == "batch"

    def test_service_job_returns_empty(self):
        assert _get_job_type("my-web-service") == ""

    def test_job_with_periodic_in_name_but_no_slash(self):
        # "periodic-" must be preceded by "/" to match
        assert _get_job_type("my-periodic-job") == ""

    def test_empty_job_id_returns_empty(self):
        assert _get_job_type("") == ""


# ---------------------------------------------------------------------------
# TestIsAbnormal
# ---------------------------------------------------------------------------


class TestIsAbnormal:
    def test_clean_batch_termination_is_not_abnormal(self):
        job = _make_job(events=[_terminated_event("0")])
        assert _is_abnormal(job) is False

    def test_nonzero_exit_code_is_abnormal(self):
        job = _make_job(events=[_terminated_event("1")])
        assert _is_abnormal(job) is True

    def test_exit_137_is_abnormal(self):
        job = _make_job(events=[_terminated_event("137")])
        assert _is_abnormal(job) is True

    def test_oom_kill_is_abnormal(self):
        job = _make_job(oom_kill_count=1, events=[_terminated_event("0")])
        assert _is_abnormal(job) is True

    def test_deployment_unhealthy_is_abnormal(self):
        job = _make_job(deployment_healthy=False)
        assert _is_abnormal(job) is True

    def test_deployment_healthy_true_is_not_abnormal(self):
        job = _make_job(
            job_type="service",
            last_state="Started",
            deployment_healthy=True,
            events=[_terminated_event("0")],
        )
        assert _is_abnormal(job) is False

    def test_killed_state_is_abnormal(self):
        job = _make_job(last_state="Killed")
        assert _is_abnormal(job) is True

    def test_not_restarting_state_is_abnormal(self):
        job = _make_job(last_state="Not Restarting")
        assert _is_abnormal(job) is True

    def test_started_state_with_clean_events_is_not_abnormal(self):
        job = _make_job(
            job_type="service", last_state="Started", deployment_healthy=True
        )
        assert _is_abnormal(job) is False

    def test_non_terminated_event_types_ignored_for_exit_code(self):
        # A "Killing" event with no exit_code should not trigger abnormal
        job = _make_job(
            last_state="Killing",
            events=[{"EventType": "Killing", "EventDetails": {}}],
        )
        assert _is_abnormal(job) is False


# ---------------------------------------------------------------------------
# TestGetTerminationRule
# ---------------------------------------------------------------------------


class TestGetTerminationRule:
    def test_service_default_reports_always(self):
        rule = _get_termination_rule("service", "any-job")
        assert rule["report"] == "always"
        assert rule["partial"] == "always"

    def test_system_default_reports_always(self):
        rule = _get_termination_rule("system", "any-job")
        assert rule["report"] == "always"

    def test_batch_default_reports_abnormal(self):
        rule = _get_termination_rule("batch", "any-job")
        assert rule["report"] == "abnormal"
        assert rule["partial"] == "always"

    def test_sysbatch_default_reports_abnormal(self):
        rule = _get_termination_rule("sysbatch", "any-job")
        assert rule["report"] == "abnormal"

    def test_unknown_job_type_falls_back_to_default_rule(self):
        # Empty string or unrecognised type should not suppress
        rule = _get_termination_rule("", "any-job")
        assert rule["report"] == "always"

    def test_unknown_job_type_none_falls_back(self):
        rule = _get_termination_rule("widget", "any-job")
        assert rule["report"] == "always"


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
# TestUpdateMeta
# ---------------------------------------------------------------------------


class TestUpdateMeta:
    def test_oom_increments_oom_count(self):
        meta = _default_meta()
        _update_meta(meta, "Terminated", {"oom_killed": "true"})
        assert meta["oom_kill_count"] == 1

    def test_terminated_without_oom_does_not_increment(self):
        meta = _default_meta()
        _update_meta(meta, "Terminated", {"exit_code": "1"})
        assert meta["oom_kill_count"] == 0

    def test_restarting_increments_restart_count(self):
        meta = _default_meta()
        _update_meta(meta, "Restarting", {})
        assert meta["restart_count"] == 1

    def test_downloading_artifacts_increments_download_attempts(self):
        meta = _default_meta()
        _update_meta(meta, "Downloading Artifacts", {})
        assert meta["download_attempts"] == 1

    def test_untracked_event_type_changes_nothing(self):
        meta = _default_meta()
        _update_meta(meta, "Started", {})
        assert meta == _default_meta()

    def test_multiple_updates_accumulate(self):
        meta = _default_meta()
        _update_meta(meta, "Restarting", {})
        _update_meta(meta, "Restarting", {})
        _update_meta(meta, "Terminated", {"oom_killed": "true"})
        assert meta["restart_count"] == 2
        assert meta["oom_kill_count"] == 1


# ---------------------------------------------------------------------------
# TestFormatJobEventsToSlack
# ---------------------------------------------------------------------------


class TestFormatJobEventsToSlack:
    def _parsed(self, job_id, events, meta=None, **kwargs):
        m = meta or _default_meta()
        return json.loads(format_job_events_to_slack(job_id, events, m, **kwargs))

    def _attachment(self, job_id, events, meta=None, **kwargs):
        return self._parsed(job_id, events, meta, **kwargs)["attachments"][0]

    def _blocks(self, job_id, events, meta=None, **kwargs):
        return self._attachment(job_id, events, meta, **kwargs)["blocks"]

    def test_returns_valid_json(self, task_event_started):
        result = self._parsed("my-job", [task_event_started])
        assert "attachments" in result
        assert "text" in result

    def test_notification_text_matches_header(self, task_event_started):
        result = self._parsed("my-job", [task_event_started])
        header_text = self._blocks("my-job", [task_event_started])[0]["text"]["text"]
        assert result["text"] == header_text

    def test_header_contains_job_id_and_count(self, task_event_started):
        header = self._blocks("my-job", [task_event_started])[0]["text"]["text"]
        assert "my-job" in header
        assert "1 event" in header

    def test_plural_events(self, task_event_started, task_event_oom):
        events = [task_event_started, task_event_oom]
        header = self._blocks("my-job", events)[0]["text"]["text"]
        assert "2 events" in header

    def test_oom_color_from_meta(self, task_event_oom):
        meta = _default_meta(oom_kill_count=1)
        assert self._attachment("my-job", [task_event_oom], meta)["color"] == "#d72b3f"

    def test_oom_header_prefix_from_meta(self, task_event_oom):
        meta = _default_meta(oom_kill_count=1)
        header = self._blocks("my-job", [task_event_oom], meta)[0]["text"]["text"]
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

    def test_oom_wins_color_in_mixed_batch(self, task_event_started, task_event_oom):
        meta = _default_meta(oom_kill_count=1)
        events = [task_event_started, task_event_oom]
        assert self._attachment("my-job", events, meta)["color"] == "#d72b3f"

    def test_partial_flag_adds_hourglass_prefix(self, task_event_started):
        header = self._blocks("my-job", [task_event_started])[0]["text"]["text"]
        assert "⏳" not in header
        partial_header = self._blocks("my-job", [task_event_started], partial=True)[0][
            "text"
        ]["text"]
        assert "⏳" in partial_header

    def test_meta_counts_appear_in_context(self, task_event_started):
        meta = _default_meta(restart_count=3, download_attempts=2)
        context = self._blocks("my-job", [task_event_started], meta)[-1]
        text = " ".join(e["text"] for e in context["elements"])
        assert "restarts: 3" in text
        assert "dl attempts: 2" in text

    def test_zero_meta_counts_not_shown(self, task_event_started):
        context = self._blocks("my-job", [task_event_started])[-1]
        text = " ".join(e["text"] for e in context["elements"])
        assert "restarts" not in text
        assert "OOM kills" not in text
