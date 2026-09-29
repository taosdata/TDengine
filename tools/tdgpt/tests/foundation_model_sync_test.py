# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Tests for restart recovery and for the delete/download serialization.

Three groups, one per mechanism:

1. The startup policy: what happens to every ``(status, operation)`` pair a
   service restart leaves behind (``fail_stale_running_operations``).
2. Re-arming the deletes that policy asked to resume, exactly once per process.
3. The delete that found the resource busy: it is deferred and retried instead
   of failing, and a permanent refusal still fails with the CLI's own reason.

Plus the reconciliation of the model ``running`` flags: a restart does not stop
model workers, so the cached flags are rebuilt from the pid files at startup
(``reconcile_model_running_state``).

And the outcome every worker ending shares: a return, an abort and a failed
completion all end in one written status (``_handle_operation_result``), because a
row nobody closes stays ``running`` until the next restart reconciles it.
"""

import tempfile
import time
from concurrent.futures import Future
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from taosanalytics.tsfm import executor, foundation_model, response_event
from taosanalytics.tsfm.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
    init_dao,
)


@pytest.fixture(autouse=True)
def dao():
    """Run every test against a private database seeded with one model row."""
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db_path = temp_db.name
    temp_db.close()

    handle = init_dao(temp_db_path)
    handle.create_model(FoundationModel(
        name="moirai",
        model_id="Salesforce/moirai-moe-1.0-R-small",
        package="tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip",
        type="forecast",
        description="Salesforce Moirai MoE 1.0 R-Small forecasting model",
        size_gb=1.8,
        status="ready",
        downloaded=True,
        running=False,
        model_state="idle",
    ))

    # The resume trigger is a process-wide flag; every test starts fresh.
    foundation_model._PENDING_DELETES_RESUMED = False

    yield handle

    Path(temp_db_path).unlink(missing_ok=True)


def _create(dao, task_id, operation, status, model_name="moirai", data=None):
    dao.create_async_operation(AsyncOperation(
        task_id=task_id,
        operation=operation,
        model_name=model_name,
        status=status,
        data=data,
    ))


class TestRunningStateReconciliation:
    """The cached `running` flag is rebuilt from the pid files, never assumed."""

    def test_live_worker_marks_the_model_running(self, dao):
        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=["moirai"]):
            corrected = foundation_model.reconcile_model_running_state()

        assert corrected == 1
        assert dao.get_model("moirai").running is True

    def test_dead_worker_clears_a_stale_flag(self, dao):
        """The crash case: the column says running, no worker exists."""
        dao.update_model("moirai", running=True)

        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=[]):
            corrected = foundation_model.reconcile_model_running_state()

        assert corrected == 1
        assert dao.get_model("moirai").running is False

    def test_unknown_liveness_changes_nothing(self, dao):
        """An unreadable liveness is not evidence that the model is stopped."""
        dao.update_model("moirai", running=True)

        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=None):
            corrected = foundation_model.reconcile_model_running_state()

        assert corrected == 0
        assert dao.get_model("moirai").running is True

    def test_a_worker_without_a_row_is_ignored(self, dao):
        """A live worker the catalog does not know leaves every flag alone."""
        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=["chronos"]):
            corrected = foundation_model.reconcile_model_running_state()

        assert corrected == 0
        assert dao.get_model("moirai").running is False

    def test_startup_runs_the_reconciliation(self, dao):
        """Wired into the startup that owns it -- this is the whole fix."""
        foundation_model._MODELS_INITIALIZED = False
        try:
            with patch.object(
                foundation_model, "reconcile_model_running_state"
            ) as reconcile:
                foundation_model.init_models_db()
        finally:
            foundation_model._MODELS_INITIALIZED = False

        reconcile.assert_called_once_with()

    def test_starting_state_reverts_to_idle_on_restart(self, dao):
        """A model stuck in 'starting' with no live worker reverts to idle."""
        dao.update_model("moirai", model_state="starting")

        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=[]):
            foundation_model.reconcile_model_running_state()

        assert dao.get_model("moirai").model_state == "idle"

    def test_stopping_with_dead_worker_reverts_to_idle(self, dao):
        """A model stuck in 'stopping' whose worker is gone is treated as idle."""
        dao.update_model("moirai", running=True, model_state="stopping")

        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=[]):
            foundation_model.reconcile_model_running_state()

        assert dao.get_model("moirai").model_state == "idle"

    def test_stopping_with_live_worker_reverts_to_started(self, dao):
        """A model stuck in 'stopping' whose worker is still alive reverts to started."""
        dao.update_model("moirai", running=True, model_state="stopping")

        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=["moirai"]):
            foundation_model.reconcile_model_running_state()

        assert dao.get_model("moirai").model_state == "started"

    def test_deleting_state_is_not_touched_by_reconciliation(self, dao):
        """A model in 'deleting' is left alone; the re-queue mechanism handles it."""
        dao.update_model("moirai", model_state="deleting")

        with patch("taosanalytics.tsfm.foundation_util.live_worker_names", return_value=[]):
            foundation_model.reconcile_model_running_state()

        assert dao.get_model("moirai").model_state == "deleting"


class TestStartupPolicy:
    """`queued` is dropped, downloads are reported stopped, deletes resume."""

    def test_queued_is_dropped_for_every_operation(self, dao):
        """Nothing that was only waiting can start again -- the pool is gone."""
        for index, operation in enumerate(
            ("download", "download_start", "start", "stop", "delete")
        ):
            _create(dao, f"{operation}-{index}", operation, "queued")

        dao.fail_stale_running_operations()

        assert dao.list_async_operations(active_only=True) == []

    def test_running_download_is_reported_stopped(self, dao):
        """Kept, and told why: the archive may already be on disk."""
        _create(dao, "download-moirai-1", "download", "running")

        dao.fail_stale_running_operations()

        op = dao.get_async_operation("download-moirai-1")
        assert op.status == "interrupted"
        assert "re-issue the download" in op.error

    def test_running_download_start_is_reported_stopped(self, dao):
        _create(dao, "download-start-moirai-1", "download_start", "running")

        dao.fail_stale_running_operations()

        assert dao.get_async_operation("download-start-moirai-1").status == "interrupted"

    def test_running_delete_is_queued_again_for_resume(self, dao):
        """A delete is an explicit request and is idempotent, so it is carried out."""
        _create(dao, "delete-moirai-1", "delete", "running")

        dao.fail_stale_running_operations()

        op = dao.get_async_operation("delete-moirai-1")
        assert op.status == "queued"
        assert op.data["restart_requeue"] is True
        assert op.progress == 0
        assert op.started_at is None
        assert op.error is None
        # The task_id is preserved so the client's polling continues.
        assert op.task_id == "delete-moirai-1"

    def test_running_start_and_stop_are_dropped(self, dao):
        """There is nothing to resume: a live worker is found through its pid."""
        _create(dao, "start-moirai-1", "start", "running")
        _create(dao, "stop-moirai-1", "stop", "running")

        dao.fail_stale_running_operations()

        assert dao.get_async_operation("start-moirai-1") is None
        assert dao.get_async_operation("stop-moirai-1") is None


class TestResumeInterruptedDeletes:
    """The resume runs once per process and exactly one worker claims a task."""

    def test_claim_is_atomic_so_only_one_worker_runs_it(self, dao):
        _create(dao, "delete-moirai-2", "delete", "running")
        dao.fail_stale_running_operations()

        pending = dao.list_requeued_deletes()
        assert [op.task_id for op in pending] == ["delete-moirai-2"]

        assert dao.claim_requeued_delete(pending[0]) is True
        # A second worker holds the same snapshot: it must lose the claim.
        assert dao.claim_requeued_delete(pending[0]) is False
        assert dao.list_requeued_deletes() == []

    def test_resume_submits_each_task_once_and_only_once(self, dao):
        _create(dao, "delete-moirai-3", "delete", "running")
        dao.fail_stale_running_operations()

        with patch.object(foundation_model, "submit_service_task") as submit:
            foundation_model.ensure_pending_deletes_resumed()
            foundation_model.ensure_pending_deletes_resumed()

        assert submit.call_count == 1
        assert submit.call_args.kwargs["task_id"] == "delete-moirai-3"
        assert submit.call_args.kwargs["model_name"] == "moirai"

    def test_resume_ignores_operations_that_are_not_requeued(self, dao):
        """A plain queued delete is not a resume candidate."""
        _create(dao, "delete-moirai-4", "delete", "queued")

        with patch.object(foundation_model, "submit_service_task") as submit:
            foundation_model.ensure_pending_deletes_resumed()

        assert not submit.called

    def test_resubmit_drops_a_task_that_was_cancelled_meanwhile(self, dao):
        _create(dao, "delete-moirai-5", "delete", "failed")

        with patch.object(foundation_model, "submit_service_task") as submit:
            foundation_model._resubmit_service_task(
                foundation_model._run_model_delete_task,
                ("delete-moirai-5", "moirai"),
                "delete-moirai-5",
                "moirai",
                "delete",
            )

        assert not submit.called

    def test_claim_does_not_depend_on_how_the_payload_was_serialized(self, dao):
        """The CAS token must not be a re-serialization of the `data` column.

        A delete that was deferred before the restart carries its keys in
        insertion order (attempt, wait_since, waiting_for, retry_after), and the
        requeue writer appends its flag to that order -- while the claim used to
        compare the column against `json.dumps(..., sort_keys=True)`. The two
        strings can only agree by luck, so the task was listed as resumable and
        then never claimed: it stayed `queued` forever and the delete the client
        asked for never ran.
        """
        _create(dao, "delete-moirai-6", "delete", "running", data={
            "attempt": 1,
            "wait_since": time.time(),
            "waiting_for": "download-moirai-inflight",
            "retry_after": 5,
        })
        dao.fail_stale_running_operations()

        pending = dao.list_requeued_deletes()
        assert [op.task_id for op in pending] == ["delete-moirai-6"]

        assert dao.claim_requeued_delete(pending[0]) is True
        # A second worker holds the same snapshot: it must lose the claim.
        assert dao.claim_requeued_delete(pending[0]) is False

        op = dao.get_async_operation("delete-moirai-6")
        assert "restart_requeue" not in op.data
        assert op.data["attempt"] == 0

    def test_resume_runs_a_delete_that_was_deferred_before_the_restart(self, dao):
        """The whole point of the re-queue: the delete is carried out.

        Regression: the claim could not match (see the test above), so the
        resume path found the task, failed to claim it and dropped it -- silently,
        because the claim returning False is indistinguishable from another
        worker winning the race.
        """
        _create(dao, "delete-moirai-7", "delete", "running", data={
            "attempt": 2,
            "wait_since": time.time(),
            "retry_after": 10,
        })
        dao.fail_stale_running_operations()

        with patch.object(foundation_model, "submit_service_task") as submit:
            foundation_model.ensure_pending_deletes_resumed()

        assert submit.call_count == 1
        assert submit.call_args.kwargs["task_id"] == "delete-moirai-7"


class TestDeleteDeferral:
    """A busy resource defers the delete; a permanent refusal fails it."""

    @staticmethod
    def _run(task_id="delete-moirai-9", returncode=75, stderr=""):
        with patch(
            "taosanalytics.tsfm.foundation_util.get_taosanodectl_cmd", return_value=["taosanodectl"]
        ), patch(
            "taosanalytics.tsfm.foundation_util.resolve_resource_settings", return_value=("http://x", 30)
        ), patch(
            "taosanalytics.tsfm.foundation_model.subprocess.run"
        ) as run:
            run.return_value = Mock(returncode=returncode, stdout="", stderr=stderr)
            return foundation_model._run_model_delete_task(task_id, "moirai")

    def test_transient_refusal_defers_instead_of_failing(self, dao):
        _create(dao, "delete-moirai-9", "delete", "queued")

        result = self._run(stderr="another TDgpt resource operation is running")

        assert result["status"] == "deferred"
        assert result["delay"] == foundation_model._SHARED_RETRY_CONFIG["base_seconds"]
        op = dao.get_async_operation("delete-moirai-9")
        # Released back to the queue: the worker is not parked on the wait.
        assert op.status == "queued"
        assert op.progress == 0
        assert op.data["attempt"] == 1
        assert "another TDgpt" in op.data["waiting_for"]

    def test_backoff_grows_with_each_attempt(self, dao):
        _create(dao, "delete-moirai-9", "delete", "queued")

        delays = []
        for attempt in range(3):
            dao.update_async_operation(
                "delete-moirai-9",
                status="queued",
                data={"attempt": attempt, "wait_since": time.time()},
            )
            delays.append(self._run()["delay"])

        assert delays == [5, 10, 20]
        assert delays[-1] <= foundation_model._SHARED_RETRY_CONFIG["max_delay_seconds"]

    def test_gives_up_once_the_wait_budget_is_spent(self, dao):
        _create(dao, "delete-moirai-9", "delete", "queued", data={
            "attempt": 4,
            "wait_since": time.time() - foundation_model._SHARED_RETRY_CONFIG["max_wait_seconds"] - 1,
        })

        result = self._run(stderr="another TDgpt resource operation is running")

        assert result["status"] == "failed"
        assert "Timed out" in result["message"]

    def test_permanent_refusal_fails_with_the_cli_reason(self, dao):
        _create(dao, "delete-moirai-9", "delete", "queued")

        result = self._run(
            returncode=foundation_model._CLI_EXIT_CONFLICT,
            stderr="cannot delete running model worker: moirai",
        )

        assert result["status"] == "failed"
        assert "cannot delete running model worker" in result["message"]

    def test_deferred_result_is_rearmed_and_not_completed(self, dao):
        """`_on_done` must not write a terminal status for a deferred task."""
        _create(dao, "delete-moirai-10", "delete", "queued")
        future = Mock()
        future.result.return_value = {"status": "deferred", "delay": 5, "waiting_for": "x"}

        with patch.object(foundation_model, "_schedule_service_retry") as retry, patch.object(
            foundation_model, "_complete_async_operation"
        ) as complete, patch.object(executor, "get_service_executor") as pool:
            pool.return_value.submit.return_value = future
            foundation_model.submit_service_task(
                foundation_model._run_model_delete_task,
                ("delete-moirai-10", "moirai"),
                "delete-moirai-10",
                "moirai",
                "delete",
                complete_async_operation_fn=foundation_model._complete_async_operation,
                retry_scheduler_fn=foundation_model._schedule_service_retry,
            )
            future.add_done_callback.call_args.args[0](future)

        assert retry.called
        assert not complete.called


class TestAtomicDownloadDedup:
    """Two processes cannot both queue a download of the same resource."""

    def test_second_claim_reuses_the_first_task(self, dao):
        first = AsyncOperation(
            task_id="download-moirai-a",
            operation="download",
            model_name="moirai",
            status="queued",
        )
        second = AsyncOperation(
            task_id="download-moirai-b",
            operation="download",
            model_name="moirai",
            status="queued",
        )

        assert foundation_model._claim_download_operation(dao, first, "moirai") is None

        response = foundation_model._claim_download_operation(dao, second, "moirai")

        assert response is not None
        body, code = response
        assert code == 200
        assert body["reused"] is True
        assert body["task_id"] == "download-moirai-a"
        assert dao.get_async_operation("download-moirai-b") is None

    def test_a_finished_download_does_not_block_the_next_one(self, dao):
        _create(dao, "download-moirai-old", "download", "completed", data={})
        fresh = AsyncOperation(
            task_id="download-moirai-new",
            operation="download",
            model_name="moirai",
            status="queued",
        )

        assert foundation_model._claim_download_operation(dao, fresh, "moirai") is None
        assert dao.get_async_operation("download-moirai-new") is not None

    def test_the_runtime_is_deduplicated_under_its_own_name(self, dao):
        runtime = foundation_model.RUNTIME_RESOURCE_NAME
        first = AsyncOperation(
            task_id=f"download-{runtime}-a",
            operation="download",
            model_name=runtime,
            status="running",
        )
        second = AsyncOperation(
            task_id=f"download-{runtime}-b",
            operation="download",
            model_name=runtime,
            status="queued",
        )

        assert foundation_model._claim_download_operation(dao, first, runtime) is None
        body, _ = foundation_model._claim_download_operation(dao, second, runtime)

        assert body["task_id"] == f"download-{runtime}-a"


class TestPhaseWatermark:
    """`started_at` must not move: it is what matches a phase to its task file.

    The reported bug, in SSE terms: a download_start op streamed
    ``model_download`` 99% running, ``model_download`` 100% completed,
    ``model_import`` 0% running -- and then ``model_download`` 0% running again,
    followed by a second ``model_download`` 100% completed.

    Phase 2 of that op (the start) runs on the same row as phase 1 and used to
    reset ``started_at`` to now, which pushed the watermark
    ``_find_resource_state`` compares against past the task file the download
    phase had just written. With no phase left to report, the stream fell back to
    its stand-in row.
    """

    def test_marking_running_keeps_the_first_start_time(self, dao):
        _create(dao, "download-start-moment-1", "download_start", "queued")

        assert dao.mark_operation_running("download-start-moment-1") is True
        first = dao.get_async_operation("download-start-moment-1")
        assert first.status == "running"
        assert first.started_at is not None

        time.sleep(0.01)
        dao.mark_operation_running("download-start-moment-1")  # phase 2 (start)

        assert (
            dao.get_async_operation("download-start-moment-1").started_at
            == first.started_at
        )

    def test_the_download_phase_stays_visible_when_the_start_phase_begins(self, dao):
        _create(dao, "download-start-moment-1", "download_start", "queued")
        dao.mark_operation_running("download-start-moment-1")  # phase 1 (download)

        first = dao.get_async_operation("download-start-moment-1")
        # What `resource apply` leaves behind once the archive is on disk.
        state = {
            "kind": "model",
            "name": "moment",
            "status": "completed",
            "updated_at": datetime.fromtimestamp(
                first.started_at + 0.005, tz=timezone.utc
            ).isoformat(),
        }

        time.sleep(0.02)
        dao.mark_operation_running("download-start-moment-1")  # phase 2 (start)

        op = dao.get_async_operation("download-start-moment-1")
        assert response_event._find_resource_state(op, [state], "model", "moment") is state

    def test_an_unknown_task_is_reported(self, dao):
        assert dao.mark_operation_running("no-such-task") is False

    def test_a_different_model_is_not_blocked(self, dao):
        _create(dao, "download-moirai-inflight", "download", "queued")
        other = AsyncOperation(
            task_id="download-chronos-1",
            operation="download",
            model_name="chronos",
            status="queued",
        )

        assert foundation_model._claim_download_operation(dao, other, "chronos") is None
        assert dao.get_async_operation("download-chronos-1") is not None


class TestWorkerOutcomeRecording:
    """Every ending of a worker writes one outcome -- even an unusable one.

    A callback that raises instead of writing leaves the row `running`: it stays in
    the active-only queries (SSE keeps seeing it, and a `running` start row refuses
    shared runtime deletion) until the next restart reconciles it. So a non-dict
    result, an abort and a failed completion all have to end in a write. The one
    ending that is never written is a deferred result with a handler -- that one
    re-arms instead.
    """

    @staticmethod
    def _drive(result=None, exception=None, deferred_fn=None, complete=None):
        future = Future()
        if exception is not None:
            future.set_exception(exception)
        else:
            future.set_result(result)
        if complete is None:
            complete = Mock()

        executor._handle_operation_result(
            future, "start-moirai-1", "moirai", "start", complete, deferred_fn
        )
        return complete

    def test_a_result_dict_is_recorded(self):
        complete = self._drive(result={"status": "completed", "message": None})

        complete.assert_called_once_with(
            "start-moirai-1", "moirai", "start", "completed", None
        )

    def test_none_is_recorded_as_a_failure(self):
        complete = self._drive(result=None)

        complete.assert_called_once_with(
            "start-moirai-1", "moirai", "start", "failed", "Worker returned no result"
        )

    def test_a_non_dict_result_is_recorded_as_a_failure(self):
        complete = self._drive(result="oops")

        complete.assert_called_once_with(
            "start-moirai-1", "moirai", "start", "failed",
            "Worker returned str, not a result dict",
        )

    def test_an_abort_is_recorded_as_a_failure(self):
        class Abort(BaseException):
            """Not an Exception: `except Exception` misses it."""

        complete = self._drive(exception=Abort())

        complete.assert_called_once_with(
            "start-moirai-1", "moirai", "start", "failed", "Worker aborted: Abort"
        )

    def test_a_deferred_result_without_a_handler_is_not_recorded_as_deferred(self):
        complete = self._drive(result={"status": "deferred", "delay": 5})

        status, error = complete.call_args.args[3], complete.call_args.args[4]
        assert status == "failed"
        assert "cannot be retried" in error

    def test_a_deferred_result_with_a_handler_is_re_armed_not_recorded(self):
        rearm = Mock()

        complete = self._drive(
            result={"status": "deferred", "delay": 5}, deferred_fn=rearm
        )

        assert not complete.called
        rearm.assert_called_once_with("deferred", {"status": "deferred", "delay": 5})

    def test_a_completion_that_cannot_be_written_does_not_escape(self):
        complete = Mock(side_effect=RuntimeError("database is gone"))

        self._drive(result={"status": "completed"}, complete=complete)  # must not raise

        complete.assert_called_once()

    def test_an_abort_inside_the_completion_is_not_swallowed(self):
        """The guard is for a write that failed, not for a real abort."""
        complete = Mock(side_effect=KeyboardInterrupt())

        with pytest.raises(KeyboardInterrupt):
            self._drive(result={"status": "completed"}, complete=complete)
