"""Tests for the shared TSFM runtime as an explicitly manageable resource.

The runtime (venv) is installed implicitly by any model download, and can also be
installed or destroyed on its own. Both entry points are covered here because they
must agree on the resource's name -- one resource, one name:

    foundation_model._RUNTIME_RESOURCE_NAME == resource_manifest.SHARED_RUNTIME_NAME
"""

import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from taosanalytics.tsfmservice import foundation_model
from taosanalytics.tsfmservice.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
    init_dao,
)

RUNTIME_INFO = {
    "kind": "runtime",
    "name": "tsfm_runtime",
    "version": "1.0.0",
    "file": "tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz",
    "size": 578639183,
}


@pytest.fixture(autouse=True)
def reset_models():
    """Run every test against a private database seeded with one model row."""
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db_path = temp_db.name
    temp_db.close()

    dao = init_dao(temp_db_path)
    dao.create_model(FoundationModel(
        name="moirai",
        model_id="Salesforce/moirai-moe-1.0-R-small",
        package="tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip",
        type="forecast",
        description="Salesforce Moirai MoE 1.0 R-Small forecasting model",
        size_gb=1.8,
        status="ready",
        downloaded=False,
        running=False,
    ))

    yield dao

    Path(temp_db_path).unlink(missing_ok=True)


def _request(payload=None):
    request = Mock()
    request.remote_addr = "127.0.0.1"
    request.args = {}
    request.get_json.return_value = payload or {}
    return request


def _task_state(status, done=0, total=100, kind="runtime", name="tsfm_runtime"):
    """One resource task file as written by taosanodectl."""
    return {
        "kind": kind,
        "name": name,
        "status": status,
        "bytes_done": done,
        "bytes_total": total,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _sse_events(dao, seen_states, task_states):
    """Drive the SSE emitter once and return the decoded payloads."""
    with patch.object(
        foundation_model, "_read_resource_task_states", return_value=task_states
    ):
        events = list(
            foundation_model._emit_operation_status_updates(
                dao, dao.list_async_operations(active_only=True), seen_states
            )
        )
    return [json.loads(event[len("data: "):]) for event in events if event]


class TestDownloadRuntime:
    """Test download_foundation_runtime."""

    def test_queues_download_under_the_shared_name(self, reset_models):
        """The op row and the taosanodectl target use the same name."""
        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, RUNTIME_INFO, None),
        ) as validate, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            status, code = foundation_model.download_foundation_runtime(_request())

        assert code == 202
        assert status["status"] == "queued"
        assert status["model_name"] == "tsfm_runtime"
        assert status["package"] == RUNTIME_INFO["file"]
        assert status["task_id"].startswith("download-tsfm_runtime-")

        # Repository lookup asks for the runtime kind under the same name.
        assert validate.call_args.args[0] == "tsfm_runtime"
        assert validate.call_args.kwargs["kind"] == "runtime"

        # The worker is driven with the same name: that is what `resource apply`
        # matches, and what the task file records as `name`.
        assert trigger.call_args.args[0] == status["task_id"]
        assert trigger.call_args.args[1] == "tsfm_runtime"

        operations = reset_models.list_async_operations(model_name="tsfm_runtime")
        assert len(operations) == 1
        assert operations[0].task_id == status["task_id"]
        assert operations[0].operation == "download"
        assert operations[0].status == "queued"

    def test_rejects_runtime_missing_from_repository(self, reset_models):
        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(False, None, "Runtime 'tsfm_runtime' not found in repository"),
        ), patch.object(foundation_model, "_trigger_model_download_background") as trigger:
            status, code = foundation_model.download_foundation_runtime(_request())

        assert code == 404
        assert status["status"] == "error"
        assert "not found" in status["message"]
        assert not trigger.called
        assert reset_models.list_async_operations(model_name="tsfm_runtime") == []

    def test_reuses_active_runtime_download(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-active",
            operation="download",
            model_name="tsfm_runtime",
            status="running",
        ))

        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, RUNTIME_INFO, None),
        ), patch.object(foundation_model, "_trigger_model_download_background") as trigger:
            status, code = foundation_model.download_foundation_runtime(_request())

        assert code == 200
        assert status["reused"] is True
        assert status["task_id"] == "download-tsfm_runtime-active"
        assert not trigger.called
        assert len(reset_models.list_async_operations(model_name="tsfm_runtime")) == 1

    def test_cleans_up_finished_runtime_operations(self, reset_models):
        for state in ("interrupted", "failed", "completed"):
            reset_models.create_async_operation(AsyncOperation(
                task_id=f"download-tsfm_runtime-{state}",
                operation="download",
                model_name="tsfm_runtime",
                status=state,
            ))

        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, RUNTIME_INFO, None),
        ), patch.object(foundation_model, "_trigger_model_download_background"):
            status, code = foundation_model.download_foundation_runtime(_request())

        assert code == 202
        operations = reset_models.list_async_operations(model_name="tsfm_runtime")
        assert [op.task_id for op in operations] == [status["task_id"]]

    def test_forwards_release_url_and_replace(self, reset_models):
        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, RUNTIME_INFO, None),
        ) as validate, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            foundation_model.download_foundation_runtime(
                _request({
                    "release": "2.0.0",
                    "resource_base_url": "http://mirror.local",
                    "replace": True,
                })
            )

        assert validate.call_args.args[1] == "2.0.0"
        assert validate.call_args.args[2] == "http://mirror.local"
        assert trigger.call_args.args[2:] == ("2.0.0", "http://mirror.local", True)


class TestRuntimeRoutingFromModelEndpoints:
    """The model endpoints must recognise model_name="tsfm_runtime"."""

    def test_download_routes_runtime(self):
        with patch.object(
            foundation_model,
            "download_foundation_runtime",
            return_value=({"status": "queued"}, 202),
        ) as runtime_download:
            response, code = foundation_model.download_foundation_model(
                _request({"model_name": "tsfm_runtime"})
            )

        assert code == 202
        assert runtime_download.called

    def test_download_keeps_model_flow_for_model_names(self, reset_models):
        """A real model name must still go through the model path."""
        with patch.object(foundation_model, "download_foundation_runtime") as runtime_download, \
                patch.object(
                    foundation_model,
                    "_validate_model_in_repository",
                    return_value=(False, None, "Model 'moirai' not found in repository"),
                ):
            response, code = foundation_model.download_foundation_model(
                _request({"model_name": "moirai"})
            )

        assert code == 404
        assert not runtime_download.called
        assert reset_models.get_model("runtime") is None

    def test_download_and_start_skips_worker_start_for_runtime(self):
        with patch.object(
            foundation_model,
            "download_foundation_runtime",
            return_value=({"status": "queued", "task_id": "download-runtime-1"}, 202),
        ), patch.object(foundation_model, "start_foundation_model") as start:
            response, code = foundation_model.download_and_start_foundation_model(
                _request({"model_name": "tsfm_runtime"})
            )

        assert code == 202
        assert not start.called
        assert "no model worker to start" in response["message"]


class TestModelDownloadRequiresRuntime:
    """A model download needs a ready runtime -- or one that is being downloaded."""

    def test_rejects_when_runtime_is_unusable(self, reset_models):
        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, {"name": "moirai"}, None),
        ), patch.object(
            foundation_model,
            "_runtime_download_blocker",
            return_value="runtime import is incomplete",
        ), patch.object(foundation_model, "_trigger_model_download_background") as trigger:
            status, code = foundation_model.download_foundation_model(
                _request({"model_name": "moirai"})
            )

        assert code == 400
        assert "not ready" in status["message"]
        assert "runtime import is incomplete" in status["message"]
        assert not trigger.called
        assert reset_models.list_async_operations(model_name="moirai") == []

    def test_allowed_while_a_runtime_download_is_queued(self, reset_models):
        """The venv is still missing, but a runtime download is in flight."""
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-queued",
            operation="download",
            model_name="tsfm_runtime",
            status="queued",
        ))

        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, {"name": "moirai"}, None),
        ), patch.object(foundation_model, "_runtime_download_blocker") as blocker, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            status, code = foundation_model.download_foundation_model(
                _request({"model_name": "moirai"})
            )

        assert code == 202
        assert status["status"] == "queued"
        assert trigger.called
        # The in-flight download short-circuits the probe entirely.
        assert not blocker.called

    def test_allowed_while_a_runtime_download_is_running(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-running",
            operation="download",
            model_name="tsfm_runtime",
            status="running",
        ))

        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, {"name": "moirai"}, None),
        ), patch.object(foundation_model, "_runtime_download_blocker") as blocker, patch.object(
            foundation_model, "_trigger_model_download_start_background"
        ) as trigger:
            status, code = foundation_model.download_and_start_foundation_model(
                _request({"model_name": "moirai"})
            )

        assert code == 202
        assert trigger.called
        assert not blocker.called

    def test_rejected_once_the_runtime_download_is_no_longer_active(self, reset_models):
        """A failed runtime download does not keep the door open."""
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-failed",
            operation="download",
            model_name="tsfm_runtime",
            status="failed",
        ))

        with patch.object(
            foundation_model,
            "_validate_model_in_repository",
            return_value=(True, {"name": "moirai"}, None),
        ), patch.object(
            foundation_model,
            "_runtime_download_blocker",
            return_value="runtime import is incomplete",
        ), patch.object(foundation_model, "_trigger_model_download_background") as trigger:
            status, code = foundation_model.download_foundation_model(
                _request({"model_name": "moirai"})
            )

        assert code == 400
        assert not trigger.called


class TestRuntimeDownloadStream:
    """A runtime download streams as its own phases, never as a generic row."""

    def test_phase_events_replace_the_generic_row(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-1",
            operation="download",
            model_name="tsfm_runtime",
            status="running",
        ))

        events = _sse_events(reset_models, {}, [_task_state("downloading", 10, 100)])

        assert [event["operation"] for event in events] == ["runtime_download"]
        assert events[0]["model_name"] == "tsfm_runtime"
        assert events[0]["status"] == "running"
        assert events[0]["progress"] == 10

    def test_queued_task_is_reported_under_the_download_phase(self, reset_models):
        """No phase yet: the task still has to be visible, under its own name."""
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-2",
            operation="download",
            model_name="tsfm_runtime",
            status="queued",
        ))

        events = _sse_events(reset_models, {}, [])

        assert [event["operation"] for event in events] == ["runtime_download"]
        assert events[0]["status"] == "queued"

    def test_failure_without_a_task_file_still_reaches_the_client(self, reset_models):
        """A child that dies before writing a task file must not fail silently."""
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-3",
            operation="download",
            model_name="tsfm_runtime",
            status="queued",
        ))
        seen_states = {}

        assert [e["operation"] for e in _sse_events(reset_models, seen_states, [])] == [
            "runtime_download"
        ]

        reset_models.update_async_operation(
            "download-tsfm_runtime-3",
            status="failed",
            error="resource not found in release manifest: tsfm_runtime",
        )

        events = _sse_events(reset_models, seen_states, [])

        assert [event["operation"] for event in events] == ["runtime_download"]
        assert events[0]["status"] == "failed"

    def test_model_download_also_uses_its_phase_name(self, reset_models):
        """No generic `download` row for models either: the phase carries it."""
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-moirai-1",
            operation="download",
            model_name="moirai",
            status="queued",
        ))

        events = _sse_events(reset_models, {}, [])

        assert [event["operation"] for event in events] == ["model_download"]
        assert events[0]["model_name"] == "moirai"
        assert events[0]["status"] == "queued"

    def test_model_failure_without_a_task_file_still_reaches_the_client(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-moirai-2",
            operation="download",
            model_name="moirai",
            status="queued",
        ))
        seen_states = {}

        assert [e["operation"] for e in _sse_events(reset_models, seen_states, [])] == [
            "model_download"
        ]

        reset_models.update_async_operation(
            "download-moirai-2",
            status="failed",
            error="Shared TSFM runtime is not ready: runtime import is incomplete",
        )

        events = _sse_events(reset_models, seen_states, [])

        assert [event["operation"] for event in events] == ["model_download"]
        assert events[0]["status"] == "failed"
        assert "not ready" in events[0]["error"]


class TestConcurrentRuntimeAndModelStreams:
    """Two downloads running side by side must not cross-report progress."""

    def test_model_operation_ignores_a_concurrent_runtime_task_file(self, reset_models):
        """Regression: a runtime download running next to a model download used
        to be reported as that model operation's runtime phase."""
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tdtsfm-ffc0901f",
            operation="download",
            model_name="tdtsfm",
            status="running",
        ))

        events = _sse_events(
            reset_models,
            {},
            [
                _task_state("downloading", 17, 100),
                _task_state("downloading", 5, 100, kind="model", name="tdtsfm"),
            ],
        )

        # Only the model's own phase: no generic `download` row, and nothing from
        # the runtime download that happens to be running beside it.
        assert [event["operation"] for event in events] == ["model_download"]
        assert {event["model_name"] for event in events} == {"tdtsfm"}
        assert all("runtime" not in event["operation"] for event in events)

    def test_runtime_operation_ignores_a_concurrent_model_task_file(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-ffc0901f",
            operation="download",
            model_name="tsfm_runtime",
            status="running",
        ))

        events = _sse_events(
            reset_models,
            {},
            [
                _task_state("downloading", 17, 100),
                _task_state("downloading", 5, 100, kind="model", name="tdtsfm"),
            ],
        )

        assert [event["operation"] for event in events] == ["runtime_download"]
        assert events[0]["model_name"] == "tsfm_runtime"
        assert events[0]["progress"] == 17


class TestModelStartPhase:
    """The auto-start step of a download_start request is reported, not silent.

    Regression: the stream ended at `model_import` 100 and then said nothing --
    neither while the worker was starting nor when it came up -- because the start
    step had no phase of its own. It streams as `start`, the operation name an
    explicit start uses, so a client renders both with one code path.
    """

    @staticmethod
    def _download_start(dao, status="running"):
        dao.create_async_operation(AsyncOperation(
            task_id="download-start-moment-aec54246",
            operation="download_start",
            model_name="moment",
            status=status,
        ))
        return "download-start-moment-aec54246"

    @staticmethod
    def _installed_state():
        return _task_state(
            "completed", 421135114, 421135114, kind="model", name="moment"
        )

    def test_the_start_step_is_reported_once_the_install_finished(self, reset_models):
        self._download_start(reset_models)

        events = _sse_events(reset_models, {}, [self._installed_state()])

        assert [event["operation"] for event in events] == [
            "model_download",
            "model_import",
            "start",
        ]
        assert events[-1]["status"] == "running"
        assert events[-1]["model_name"] == "moment"

    def test_the_poll_reports_the_start_step_too(self, reset_models):
        task_id = self._download_start(reset_models)
        op = reset_models.get_async_operation(task_id)

        with patch.object(
            foundation_model,
            "_read_resource_task_states",
            return_value=[self._installed_state()],
        ):
            body = foundation_model._build_operation_response(op)

        assert body["phase"] == "start"
        assert body["progress"] == 0

    def test_a_plain_download_has_no_start_step(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-moment-1",
            operation="download",
            model_name="moment",
            status="running",
        ))

        events = _sse_events(reset_models, {}, [self._installed_state()])

        assert [event["operation"] for event in events] == [
            "model_download",
            "model_import",
        ]

    def test_the_start_step_waits_for_the_install_to_finish(self, reset_models):
        self._download_start(reset_models)

        events = _sse_events(
            reset_models,
            {},
            [_task_state("importing", 5, 10, kind="model", name="moment")],
        )

        assert [event["operation"] for event in events] == [
            "model_download",
            "model_import",
        ]

    def test_a_failed_start_carries_the_operations_own_reason(self, reset_models):
        task_id = self._download_start(reset_models)
        seen_states = {}

        # The client watched the install finish and the start begin.
        _sse_events(reset_models, seen_states, [self._installed_state()])
        reset_models.update_async_operation(
            task_id, status="failed", error="Model start failed: port 6066 never opened"
        )

        events = _sse_events(reset_models, seen_states, [self._installed_state()])

        assert [event["operation"] for event in events] == ["start"]
        assert events[0]["status"] == "failed"
        assert "never opened" in events[0]["error"]

    def test_the_completion_closes_the_start_step_with_one_event(self, reset_models):
        task_id = self._download_start(reset_models)
        seen_states = {}

        _sse_events(reset_models, seen_states, [self._installed_state()])
        reset_models.update_async_operation(task_id, status="completed")

        events = _sse_events(reset_models, seen_states, [self._installed_state()])

        assert [event["operation"] for event in events] == ["start"]
        assert events[0]["status"] == "completed"
        assert events[0]["progress"] == 100

    def test_a_cancelled_row_closes_the_start_step_silently(self, reset_models):
        """The row is gone: there is no outcome left to report."""
        task_id = self._download_start(reset_models)
        seen_states = {(task_id, "start"): ("running", 0)}
        reset_models.delete_async_operation(task_id)

        events = _sse_events(reset_models, seen_states, [self._installed_state()])

        assert [event["operation"] for event in events] == []
        assert seen_states == {}

    def test_a_client_that_subscribes_late_still_gets_the_outcome(self, reset_models):
        """A reconnected stream has no phase history: the outcome is replayed."""
        task_id = self._download_start(reset_models)
        reset_models.update_async_operation(
            task_id, status="completed", completed_at=time.time()
        )

        events = _sse_events(reset_models, {}, [self._installed_state()])

        assert [event["operation"] for event in events] == [
            "model_download",
            "model_import",
            "start",
        ]
        assert events[-1]["status"] == "completed"
        assert events[-1]["progress"] == 100

    def test_no_duplicate_events_after_completion(self, reset_models):
        """Once every phase reported its terminal state, subsequent polls emit nothing."""
        task_id = self._download_start(reset_models)
        seen_states = {}

        # Poll 1: install finished, start is running.
        _sse_events(reset_models, seen_states, [self._installed_state()])
        # Poll 2: operation completed — start emits its terminal event.
        reset_models.update_async_operation(
            task_id, status="completed", completed_at=time.time()
        )
        events = _sse_events(reset_models, seen_states, [self._installed_state()])
        assert any(
            e["operation"] == "start" and e["status"] == "completed" for e in events
        )

        # Polls 3-5: nothing should be emitted — the replay window is still
        # open, but seen_states guards against re-emission.
        for _ in range(3):
            events = _sse_events(reset_models, seen_states, [self._installed_state()])
            assert events == [], f"unexpected duplicate events: {events}"

    def test_an_operation_that_finished_long_ago_is_not_replayed(self, reset_models):
        """The window keeps a reconnect from becoming a history dump."""
        task_id = self._download_start(reset_models)
        reset_models.update_async_operation(
            task_id, status="completed", completed_at=time.time() - 3600
        )

        assert _sse_events(reset_models, {}, [self._installed_state()]) == []


class TestDestroyRuntime:
    """Test delete_foundation_runtime (the runtime counterpart)."""

    def test_blocked_while_a_model_worker_runs(self, reset_models):
        with patch.object(
            foundation_model, "_live_worker_names", return_value=["moirai"]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_runtime(_request())

        assert code == 400
        assert "moirai" in status["message"]
        assert not trigger.called

    def test_not_blocked_by_an_interrupted_download_row(self, reset_models):
        """An interrupted row is history, not a reason to refuse.

        `active_only` also returns interrupted rows and nothing ever reclaims
        them, so one restart while a download was in flight used to make
        DELETE /runtime/delete answer 400 forever -- and further restarts did not
        help. Only liveness and an unfinished start may refuse.
        """
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-moirai-dead",
            operation="download",
            model_name="moirai",
            status="interrupted",
        ))

        with patch.object(
            foundation_model, "_live_worker_names", return_value=[]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_runtime(_request())

        assert code == 202
        assert trigger.called

    def test_blocked_while_a_model_worker_starts(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="start-moirai-1",
            operation="start",
            model_name="moirai",
            status="running",
        ))

        with patch.object(foundation_model, "_live_worker_names", return_value=[]):
            status, code = foundation_model.delete_foundation_runtime(_request())

        assert code == 400
        assert "starting" in status["message"]

    def test_queued_when_idle_under_the_shared_name(self, reset_models):
        """Deletion is driven by the same name the download uses."""
        with patch.object(
            foundation_model, "_live_worker_names", return_value=[]
        ), patch.object(
            foundation_model, "_trigger_model_delete_background"
        ) as trigger:
            status, code = foundation_model.delete_foundation_runtime(_request())

        assert code == 202
        assert status["model_name"] == "tsfm_runtime"
        assert trigger.call_args.args[1] == "tsfm_runtime"


class TestRuntimeStateIsolation:
    """The runtime has no models-table row, so it must not write one."""

    def test_completed_runtime_download_leaves_models_table_alone(self, reset_models):
        reset_models.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-done",
            operation="download",
            model_name="tsfm_runtime",
            status="running",
        ))

        foundation_model._complete_async_operation(
            "download-tsfm_runtime-done", "tsfm_runtime", "download", "completed"
        )

        assert reset_models.get_async_operation("download-tsfm_runtime-done").status == "completed"
        assert reset_models.get_model("tsfm_runtime") is None
        assert reset_models.get_model("moirai").downloaded is False
