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

import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from taosanalytics.tsfm import executor, foundation_model, foundation_util, response_event
from taosanalytics.tsfm.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
    get_dao,
    init_dao,
)


@pytest.fixture(autouse=True)
def reset_foundation_models():
    """Initialize a temporary test database before each test."""
    # Create temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db_path = temp_db.name
    temp_db.close()

    # Initialize DAO with test database
    dao = init_dao(temp_db_path)

    # Seed with initial test model
    test_model = FoundationModel(
        name="moirai",
        model_id="Salesforce/moirai-moe-1.0-R-small",
        package="tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip",
        type="forecast",
        description="Salesforce Moirai MoE 1.0 R-Small forecasting model",
        size_gb=1.8,
        status="ready",
        downloaded=False,
        running=False,
    )
    dao.create_model(test_model)

    yield dao

    # Cleanup temporary database
    Path(temp_db_path).unlink(missing_ok=True)


class TestListAllTsfm:
    """Test list_all_tsfm endpoint."""

    def test_list_all_tsfm_success(self):
        """Test successful model listing from repository."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {"release": "1.0.0"}

        mock_response = {
            "resources": [
                {
                    "kind": "model",
                    "name": "moirai",
                    "model_id": "Salesforce/moirai-moe-1.0-R-small",
                    "size": 2147483648,
                    "note": "Salesforce Moirai model",
                    "state": "installed",
                }
            ]
        }

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 200
            assert status["status"] == "success"
            assert len(status["data"]["resources"]) == 1

    def test_list_all_tsfm_timeout(self):
        """Test timeout handling."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("cmd", 30)

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 504
            assert status["status"] == "error"

    def test_list_all_tsfm_invalid_json(self):
        """Test invalid JSON response handling."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="invalid json")

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 500
            assert status["status"] == "error"
            assert "parse" in status["message"].lower()

    def test_list_all_tsfm_merges_service_status_and_deployed_at(self):
        """Each model carries this host's worker state and deploy time."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [
                {
                    "kind": "model",
                    "name": "moirai",
                    "model_id": "Salesforce/moirai-moe-1.0-R-small",
                    "size": 2147483648,
                    "state": "installed",
                }
            ]
        }
        local_response = {
            "runtime": {"runtime_id": "tsfm"},
            "models": [
                {
                    "name": "moirai",
                    "install_status": "installed",
                    "service_status": "running",
                    "deployed_at": "2026-01-02T03:04:05+00:00",
                }
            ],
        }

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout=json.dumps(check_response)),
                Mock(returncode=0, stdout=json.dumps(local_response)),
            ]

            status, code = foundation_model.list_all_tsfm(mock_request)

        assert code == 200
        entry = status["data"]["resources"][0]
        assert entry["service_status"] == "running"
        assert entry["deployed_at"] == "2026-01-02T03:04:05+00:00"
        # The local view is read through the CLI, not from the resource layout.
        assert mock_run.call_args_list[1].args[0][-4:] == [
            "resource",
            "list",
            "--format",
            "json",
        ]

    def test_list_all_tsfm_leaves_an_unknown_model_without_local_state(self):
        """A model only the repository knows acquires no local facts."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [
                {"kind": "model", "name": "chronos", "state": "missing", "size": 1},
                {"kind": "model", "name": "moirai", "state": "installed", "size": 1},
            ]
        }
        local_response = {
            "runtime": None,
            "models": [{"name": "moirai", "service_status": "stopped"}],
        }

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout=json.dumps(check_response)),
                Mock(returncode=0, stdout=json.dumps(local_response)),
            ]

            status, code = foundation_model.list_all_tsfm(mock_request)

        assert code == 200
        resources = {entry["name"]: entry for entry in status["data"]["resources"]}
        assert "service_status" not in resources["chronos"]
        assert "deployed_at" not in resources["chronos"]
        assert resources["moirai"]["service_status"] == "stopped"
        # The local entry had no deployed_at key, so the field stays absent
        # rather than being invented.
        assert "deployed_at" not in resources["moirai"]

    def test_list_all_tsfm_survives_a_broken_local_view(self):
        """A local view that cannot be read degrades the listing, not fails it."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [{"kind": "model", "name": "moirai", "state": "installed", "size": 1}]
        }

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout=json.dumps(check_response)),
                Mock(returncode=1, stdout="", stderr="resource list is unavailable"),
            ]

            status, code = foundation_model.list_all_tsfm(mock_request)

        assert code == 200
        assert "service_status" not in status["data"]["resources"][0]


class TestLocalResourceStateMerge:
    """Test the local-state merge behind the foundation model listing."""

    def test_merge_never_reports_service_state_for_the_runtime(self):
        """The shared runtime has no worker, so it has no service state."""
        resources = [
            {"kind": "runtime", "name": "tsfm_runtime", "state": "installed"},
            {"kind": "model", "name": "moirai", "state": "installed"},
        ]
        local = {
            "tsfm_runtime": {
                "service_status": "stopped",
                "deployed_at": "2026-01-01T00:00:00+00:00",
            },
            "moirai": {
                "service_status": "running",
                "deployed_at": "2026-01-02T00:00:00+00:00",
            },
        }

        with patch.object(foundation_model, "_local_model_state", return_value=local):
            foundation_model._merge_local_resource_state(resources, 30)

        assert "service_status" not in resources[0]
        assert "deployed_at" not in resources[0]
        assert resources[1]["service_status"] == "running"
        assert resources[1]["deployed_at"] == "2026-01-02T00:00:00+00:00"

    def test_merge_keeps_the_repository_state_field(self):
        """Merging adds local facts; it never rewrites the release's own state."""
        resources = [{"kind": "model", "name": "moirai", "state": "outdated"}]
        local = {"moirai": {"service_status": "running", "install_status": "installed"}}

        with patch.object(foundation_model, "_local_model_state", return_value=local):
            foundation_model._merge_local_resource_state(resources, 30)

        assert resources[0]["state"] == "outdated"
        assert "install_status" not in resources[0]
        assert resources[0]["service_status"] == "running"


class TestGetModelInfo:
    """Test get_model_info endpoint."""

    def test_get_model_info_success(self, reset_foundation_models):
        """Test successful model info retrieval."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        mock_response = {
            "models": [
                {
                    "name": "moirai",
                    "model_id": "Salesforce/moirai-moe-1.0-R-small",
                    "service_status": "stopped",
                    "install_status": "installed",
                }
            ]
        }

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            status, code = foundation_model.get_model_info(mock_request, "moirai")

            assert code == 200
            assert status["status"] == "success"
            assert status["data"]["name"] == "moirai"
            assert reset_foundation_models.get_model("moirai").downloaded is True
            command = mock_run.call_args.args[0]
            assert "--release" not in command
            assert "--resource-base-url" not in command

    def test_get_model_info_not_found(self, reset_foundation_models):
        """Test model not found."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        mock_response = {"models": []}

        reset_foundation_models.update_model("moirai", downloaded=True)

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            status, code = foundation_model.get_model_info(mock_request, "moirai")

            assert code == 404
            assert status["status"] == "error"
            assert reset_foundation_models.get_model("moirai").downloaded is False


class TestDownloadFoundationModel:
    """Test download_foundation_model endpoint."""

    def test_download_missing_model_name(self):
        """Test download request without model_name."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {}

        status, code = foundation_model.download_foundation_model(mock_request)

        assert code == 400
        assert status["status"] == "error"
        assert "model_name" in status["message"]

    def test_download_invalid_model(self):
        """Test download request for non-existent model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "nonexistent"}

        with patch(
            "taosanalytics.tsfm.foundation_model._extract_model_status_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (False, None, "Model not found")

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 404
            assert status["status"] == "error"

    def test_download_already_downloaded(self):
        """Test download request for already downloaded model without replace."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai", "replace": False}

        # Mock validation to succeed and set model as already downloaded
        dao = get_dao()
        dao.update_model("moirai", downloaded=True)
        model_info = {"name": "moirai", "model_id": "test"}

        with patch(
            "taosanalytics.tsfm.foundation_model._extract_model_status_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (True, model_info, None)

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 400
            assert "already downloaded" in status["message"]

    def test_download_running_model_with_replace(self):
        """Test replace request for running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai", "replace": True}

        # Mock validation to succeed and set model as running
        dao = get_dao()
        dao.update_model("moirai", running=True)
        model_info = {"name": "moirai", "model_id": "test"}

        with patch(
            "taosanalytics.tsfm.foundation_model._extract_model_status_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (True, model_info, None)

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 400
            assert "running" in status["message"].lower()

    def test_download_success(self, reset_foundation_models):
        """Test successful download initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        with patch(
            "taosanalytics.tsfm.foundation_model._extract_model_status_in_repository"
        ) as mock_validate, patch(
            "taosanalytics.tsfm.foundation_model._trigger_model_download_background"
        ), patch(
            "taosanalytics.tsfm.foundation_model._runtime_model_download_blocker",
            return_value=None,
        ):
            dao = reset_foundation_models
            for operation in ("download", "download_start"):
                for operation_status in ("interrupted", "failed", "completed"):
                    dao.create_async_operation(AsyncOperation(
                        task_id=f"{operation}-moirai-{operation_status}",
                        operation=operation,
                        model_name="moirai",
                        status=operation_status,
                    ))

            mock_validate.return_value = (True, {"name": "moirai"}, None)

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status
            assert "moirai" in status["task_id"]
            operations = dao.list_async_operations(model_name="moirai")
            assert len(operations) == 1
            assert operations[0].task_id == status["task_id"]


class TestCleanupDownloadOperations:
    """A terminal download row must not outlive the model's next request.

    Both variants share one per-model namespace: get_latest_operation reads the
    newest row whatever its name, so an `interrupted` or `failed` row left by
    `download_start` keeps reporting a state the resource is no longer in -- and
    `cleanup_old_operations` never sweeps `interrupted`, so this purge is the
    only thing that removes it.
    """

    def test_terminal_rows_of_both_variants_are_deleted(self, reset_foundation_models):
        dao = reset_foundation_models
        for operation in ("download", "download_start"):
            for operation_status in ("interrupted", "failed", "completed"):
                dao.create_async_operation(AsyncOperation(
                    task_id=f"{operation}-moirai-{operation_status}",
                    operation=operation,
                    model_name="moirai",
                    status=operation_status,
                ))

        assert dao.cleanup_download_operations("moirai") == 6
        assert dao.list_async_operations(model_name="moirai") == []

    def test_in_flight_rows_survive(self, reset_foundation_models):
        """A queued or running download is work still owed to the client."""
        dao = reset_foundation_models
        for operation_status in ("queued", "running"):
            dao.create_async_operation(AsyncOperation(
                task_id=f"download-moirai-{operation_status}",
                operation="download",
                model_name="moirai",
                status=operation_status,
            ))

        assert dao.cleanup_download_operations("moirai") == 0
        assert len(dao.list_async_operations(model_name="moirai")) == 2

    def test_other_operations_are_left_alone(self, reset_foundation_models):
        """The other names belong to the startup policy, not to this purge."""
        dao = reset_foundation_models
        for operation in ("start", "stop", "delete"):
            dao.create_async_operation(AsyncOperation(
                task_id=f"{operation}-moirai-1",
                operation=operation,
                model_name="moirai",
                status="completed",
            ))

        assert dao.cleanup_download_operations("moirai") == 0
        assert len(dao.list_async_operations(model_name="moirai")) == 3


class TestUpdateColumnAllowList:
    """Both UPDATE statements name columns from a constant, never from the caller.

    The identifiers are interpolated while the values are bound parameters, so the
    allow-list is not only a filter on the request -- it is the sole source of
    column names. These tests pin that, plus the two semantics the rewrite had to
    preserve: `update_model` skips a None, `update_async_operation` writes NULL.
    """

    def test_an_unknown_column_is_ignored(self, reset_foundation_models):
        dao = reset_foundation_models

        assert dao.update_model("moirai", description="kept") is True
        assert dao.update_model("moirai", not_a_column="x") is False
        assert dao.get_model("moirai").description == "kept"

    def test_a_value_is_bound_and_never_parsed_as_sql(self, reset_foundation_models):
        dao = reset_foundation_models
        payload = "'); DROP TABLE models; --"

        assert dao.update_model("moirai", description=payload) is True
        assert dao.get_model("moirai").description == payload
        # The table is still there: the payload travelled as a bound parameter.
        assert dao.list_models() != []

    def test_an_unknown_operation_column_is_ignored(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="start-moirai-1",
            operation="start",
            model_name="moirai",
            status="queued",
        ))

        assert dao.update_async_operation("start-moirai-1", not_a_column="x") is False
        assert dao.update_async_operation("start-moirai-1", progress=7) is True
        assert dao.get_async_operation("start-moirai-1").progress == 7

    def test_none_is_skipped_for_a_model(self, reset_foundation_models):
        dao = reset_foundation_models
        before = dao.get_model("moirai").description

        assert dao.update_model("moirai", description=None) is False
        assert dao.get_model("moirai").description == before

    def test_none_clears_an_operation_field(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="start-moirai-2",
            operation="start",
            model_name="moirai",
            status="failed",
            error="boom",
            error_code="E1",
        ))

        assert dao.update_async_operation("start-moirai-2", error=None, error_code=None) is True

        op = dao.get_async_operation("start-moirai-2")
        assert op.error is None
        assert op.error_code is None


class TestStartFoundationModel:
    """Test start_foundation_model endpoint."""

    def test_start_missing_model_name(self):
        """Test start request without model_name."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {}

        status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 400
        assert status["status"] == "error"
        assert "model_name" in status["message"]

    def test_start_non_downloaded_model(self):
        """Test start request for non-downloaded model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Ensure model is not downloaded
        dao = get_dao()
        dao.update_model("moirai", downloaded=False, running=False)

        status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 400
        assert "not downloaded" in status["message"].lower()

    def test_start_already_running_model(self):
        """Test start request for already running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded and running
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True)

        status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 400
        assert "already running" in status["message"].lower()

    def test_start_success(self):
        """Test successful model start initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded but not running (state = idle)
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False, model_state="idle")

        with patch(
            "taosanalytics.tsfm.foundation_model._trigger_model_start_background"
        ):
            status, code = foundation_model.start_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status


class TestDownloadAndStartFoundationModel:
    """Test download_and_start_foundation_model endpoint."""

    def test_missing_model_name(self):
        """Test download-and-start request without model_name."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {}

        status, code = foundation_model.download_and_start_foundation_model(mock_request)

        assert code == 400
        assert status["status"] == "error"
        assert "model_name" in status["message"]

    def test_invalid_model(self):
        """Test download-and-start request for non-existent model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "nonexistent"}

        with patch(
            "taosanalytics.tsfm.foundation_model._extract_model_status_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (False, None, "Model not found")

            status, code = foundation_model.download_and_start_foundation_model(mock_request)

            assert code == 404
            assert status["status"] == "error"

    def test_already_running_model(self):
        """Test download-and-start request for already running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True)

        status, code = foundation_model.download_and_start_foundation_model(mock_request)

        assert code == 400
        assert "already running" in status["message"].lower()

    def test_already_downloaded_starts_without_download(self):
        """Test already-downloaded model triggers start only (download skipped)."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False, model_state="idle")

        with patch(
            "taosanalytics.tsfm.foundation_model._trigger_model_start_background"
        ):
            status, code = foundation_model.download_and_start_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "download skipped" in status["message"].lower()
            assert status["task_id"].startswith("start-")
            # Only a start operation is created (no download_start operation).
            operations = dao.list_async_operations(model_name="moirai")
            assert len(operations) == 1
            assert operations[0].operation == "start"

    def test_download_and_start_success(self, reset_foundation_models):
        """Test successful combined download-and-start initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        with patch(
            "taosanalytics.tsfm.foundation_model._extract_model_status_in_repository"
        ) as mock_validate, patch(
            "taosanalytics.tsfm.foundation_model._trigger_model_download_start_background"
        ), patch(
            "taosanalytics.tsfm.foundation_model._runtime_model_download_blocker",
            return_value=None,
        ):
            dao = reset_foundation_models
            mock_validate.return_value = (True, {"name": "moirai"}, None)

            status, code = foundation_model.download_and_start_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert status["auto_start"] is True
            assert "task_id" in status
            assert "download-start" in status["task_id"]
            operations = dao.list_async_operations(model_name="moirai")
            assert len(operations) == 1
            assert operations[0].operation == "download_start"
            assert operations[0].task_id == status["task_id"]


class TestDeferredDownloadStart:
    """A start the lifecycle lock refuses is deferred, not recorded as the end.

    Regression: the auto-start phase was handed to the service executor with a
    bare submit() + future.result(), so its deferral reached neither the retry
    scheduler nor a deferred-aware callback. The deferred result was then written
    by submit_download_task's completion callback as if it were the operation's
    outcome, which put `deferred` in the table -- a value that is not a TaskStatus,
    that no active-only query excludes and no cleanup sweep removes -- while the
    model stayed downloaded and never started.
    """

    def test_the_start_phase_is_submitted_with_the_retry_wiring(self, reset_foundation_models):
        with patch.object(
            foundation_model, "_run_download_task", return_value={"status": "completed"}
        ), patch.object(foundation_model, "submit_service_task") as submit:
            submit.return_value.result.return_value = {
                "status": "deferred",
                "delay": 5,
                "waiting_for": "download-chronos-1",
            }

            result = foundation_model._run_download_and_start_task(
                "download-start-moirai-1", "moirai", "1.0.0", "http://x", False
            )

        assert result["status"] == "async_delegated"
        assert submit.call_args.kwargs["operation"] == "download_start"
        assert (
            submit.call_args.kwargs["retry_scheduler_fn"]
            is foundation_model._schedule_service_retry
        )
        assert (
            submit.call_args.kwargs["complete_async_operation_fn"]
            is foundation_model._complete_async_operation
        )

    def test_a_successful_start_is_still_the_outcome(self, reset_foundation_models):
        with patch.object(
            foundation_model, "_run_download_task", return_value={"status": "completed"}
        ), patch.object(foundation_model, "submit_service_task") as submit:
            submit.return_value.result.return_value = {"status": "completed", "message": None}

            result = foundation_model._run_download_and_start_task(
                "download-start-moirai-1", "moirai", "1.0.0", "http://x", False
            )

        assert result["status"] == "async_delegated"
        assert "message" in result

    def test_a_deferred_start_leaves_the_row_queued(self, reset_foundation_models):
        """Nothing writes an outcome: the row has to stay claimable by the retry."""
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-start-moirai-1",
            operation="download_start",
            model_name="moirai",
            status="queued",
        ))

        with patch.object(executor, "get_download_executor") as pool, patch.object(
            foundation_model, "_run_download_and_start_task"
        ):
            future = pool.return_value.submit.return_value
            future.result.return_value = {
                "status": "deferred",
                "delay": 5,
                "waiting_for": "download-chronos-1",
            }

            foundation_model._trigger_model_download_start_background(
                "download-start-moirai-1", "moirai", "1.0.0", "http://x", False
            )
            future.add_done_callback.call_args.args[0](future)

        op = dao.get_async_operation("download-start-moirai-1")
        assert op.status == "queued"
        assert op.completed_at is None


class TestDeferralReasonContract:
    """The deferral path carries text: only _cli_reason touches the child's streams.

    The helper used to be handed a hand-built object carrying bytes ``stderr``
    and a ``returncode`` -- an implicit contract with a fake subprocess result,
    which held only because _cli_reason happened to accept bytes. Decoding at the
    boundary keeps bytes out of the JSON ``data`` column by construction.
    """

    def test_a_bytes_stream_is_decoded(self):
        assert foundation_model._cli_reason(b"boom\n") == "boom"

    def test_stdout_is_the_fallback_when_stderr_is_silent(self):
        assert foundation_model._cli_reason(b"", "held by a download\n") == "held by a download"

    def test_the_reason_reaches_the_row_as_text(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="delete-moirai-1",
            operation="delete",
            model_name="moirai",
            status="running",
        ))

        result = foundation_model._defer_operation(
            dao, "delete-moirai-1", "moirai", "delete", {}, "held by a download"
        )

        assert result["status"] == "deferred"
        op = dao.get_async_operation("delete-moirai-1")
        # `data` is a JSON column: a reason that was not text would not survive it.
        assert op.status == "queued"
        assert op.data["waiting_for"] == "held by a download"

    def test_a_refused_start_defers_with_the_childs_own_reason(self, reset_foundation_models):
        """What a Popen child writes as bytes arrives in the row as text."""
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="start-moirai-1",
            operation="start",
            model_name="moirai",
            status="queued",
        ))
        proc = Mock(pid=4321, returncode=foundation_model._CLI_EXIT_TEMPFAIL)
        proc.communicate.return_value = (b"", b"another TDgpt resource operation is running\n")

        with patch.object(
            foundation_model.subprocess, "Popen", return_value=proc
        ), patch.object(
            foundation_util, "get_taosanodectl_cmd", return_value=["taosanodectl"]
        ):
            result = foundation_model._run_model_start_task("start-moirai-1", "moirai")

        assert result["status"] == "deferred"
        op = dao.get_async_operation("start-moirai-1")
        assert op.status == "queued"
        assert op.data["waiting_for"] == "another TDgpt resource operation is running"


class TestRunDownloadTaskCancellation:
    """Test _run_download_task abort handling while waiting for the download lock."""

    @staticmethod
    def _make_queued_op(dao, task_id="download-moirai-wait1"):
        dao.create_async_operation(AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            data={"release": "1.0.0", "resource_base_url": "", "replace": False},
        ))
        return task_id

    def test_aborts_wait_when_op_deleted(self, reset_foundation_models):
        """Waiting for the lock aborts if the op row is deleted while waiting."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)

        def _peer_holds_the_lock_and_op_is_cancelled(*args, **kwargs):
            """Lose the lock race, and the op is removed while we wait."""
            dao.delete_async_operation(task_id)
            return None

        with patch(
            "taosanalytics.tsfm.foundation_model._try_acquire_download_lock",
            side_effect=_peer_holds_the_lock_and_op_is_cancelled,
        ) as mock_lock, patch(
            "taosanalytics.tsfm.foundation_model.time.sleep"
        ) as mock_sleep:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
        mock_lock.assert_called_once()
        # One wait, then the next validity check sees the deleted row and stops.
        mock_sleep.assert_called_once()

    def test_aborts_wait_when_op_no_longer_queued(self, reset_foundation_models):
        """Waiting for the lock aborts if the op is no longer 'queued' (cancelled)."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        # Simulate a cancellation that flips the status away from queued.
        dao.update_async_operation(
            task_id, status=foundation_model.TaskStatus.INTERRUPTED.value
        )

        with patch(
            "taosanalytics.tsfm.foundation_model._try_acquire_download_lock",
            return_value=None,
        ) as mock_lock:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
        # A task that is already cancelled never even contends for the lock.
        mock_lock.assert_not_called()

    def test_aborts_after_lock_when_op_invalid(self, reset_foundation_models):
        """After winning the lock, a stale/cancelled op is not executed."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)

        def _op_cancelled_while_acquiring_the_lock(*args, **kwargs):
            """The op disappears in the window between check and lock."""
            dao.delete_async_operation(task_id)
            return 1234

        with patch(
            "taosanalytics.tsfm.foundation_model._try_acquire_download_lock",
            side_effect=_op_cancelled_while_acquiring_the_lock,
        ), patch(
            "taosanalytics.tsfm.foundation_model._release_download_lock"
        ) as mock_release, patch(
            "taosanalytics.tsfm.foundation_util.get_taosanodectl_cmd"
        ) as mock_cmd:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
        # The lock must be handed to the next waiter, not held by a dead task.
        mock_release.assert_called_once_with(1234)
        mock_cmd.assert_not_called()


class TestStopFoundationModel:
    """Test stop_foundation_model endpoint."""

    def test_stop_missing_model_name(self):
        """Test stop request without model_name."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {}

        status, code = foundation_model.stop_foundation_model(mock_request)

        assert code == 400
        assert status["status"] == "error"
        assert "model_name" in status["message"]

    def test_stop_non_running_model(self):
        """Test stop request for non-running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Ensure model is not running
        dao = get_dao()
        dao.update_model("moirai", running=False)

        status, code = foundation_model.stop_foundation_model(mock_request)

        assert code == 400
        assert "not running" in status["message"].lower()

    def test_stop_success(self):
        """Test successful model stop initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as running
        dao = get_dao()
        dao.update_model("moirai", running=True, model_state="started")

        with patch(
            "taosanalytics.tsfm.foundation_model._trigger_model_stop_background"
        ):
            status, code = foundation_model.stop_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status

            # Clean up
            dao.update_model("moirai", running=False, model_state="idle")


class TestDeleteFoundationModel:
    """Test delete_foundation_model endpoint."""

    def test_delete_missing_model_name(self):
        """Test delete request without model_name."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {}

        status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 400
        assert status["status"] == "error"
        assert "model_name" in status["message"]

    def test_delete_running_model(self):
        """A live worker -- pid liveness -- refuses the delete."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        with patch.object(
            foundation_util, "live_worker_names", return_value=["moirai"]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 400
        assert "running" in status["message"].lower()
        assert not trigger.called

    def test_delete_not_refused_by_a_stale_running_flag(self):
        """`models.running` is a cache and must not lock deletion forever.

        The flag is only written when this API completes a start, so a worker
        that crashed -- or one started from the CLI -- leaves it wrong. Liveness
        is what decides now; the resource-delete command checks the same pids
        again before removing anything.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", running=True)

        with patch.object(
            foundation_util, "live_worker_names", return_value=[]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 202
        assert trigger.called

        dao.update_model("moirai", running=False)

    def test_delete_non_downloaded_model(self):
        """Test delete request for non-downloaded model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as not downloaded (state = missing)
        dao = get_dao()
        dao.update_model("moirai", downloaded=False, running=False, model_state="missing")

        with patch.object(
            foundation_model, "_trigger_model_delete_background"
        ), patch.object(foundation_util, "live_worker_names", return_value=[]):
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 202
        assert status["status"] == "queued"

    def test_delete_success(self):
        """Test successful model delete initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded but not running (state = idle)
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False, model_state="idle")

        with patch(
            "taosanalytics.tsfm.foundation_model._trigger_model_delete_background"
        ), patch.object(foundation_util, "live_worker_names", return_value=[]):
            status, code = foundation_model.delete_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status


class TestGetOperationStatus:
    """Test get_operation_status endpoint."""

    def test_status_task_not_found(self):
        """Test status request for non-existent task."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        status, code = response_event.get_operation_status(mock_request, "nonexistent-task")

        assert code == 404
        assert status["status"] == "error"
        assert "not found" in status["message"].lower()

    def test_status_pending_operation(self):
        """Test status for pending operation returns current state without mock advancement."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        # Create a pending operation
        from taosanalytics.tsfm.foundation_models_db import AsyncOperation, get_dao
        dao = get_dao()
        task_id = "download-test-model-abc123"
        operation = AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name="test-model",
            progress=0,
            status="queued",
            created_at=0,
            data={
                "steps": ["Step 1", "Step 2", "Step 3"],
                "current_step": 0
            }
        )
        dao.create_async_operation(operation)

        status, code = response_event.get_operation_status(mock_request, task_id)

        assert code == 200
        assert status["status"] == "success"
        assert status["data"]["status"] == "queued"
        # Progress stays at 0 until the pool callback fires — no mock advancement
        assert status["data"]["progress"] == 0

        # Clean up
        dao.delete_async_operation(task_id)

    def test_status_completed_operation(self):
        """Test status for completed operation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        # Create a completed operation
        from taosanalytics.tsfm.foundation_models_db import AsyncOperation, get_dao
        dao = get_dao()
        task_id = "start-test-model-def456"
        operation = AsyncOperation(
            task_id=task_id,
            operation="start",
            model_name="test-model",
            progress=100,
            status="completed",
        )
        dao.create_async_operation(operation)

        status, code = response_event.get_operation_status(mock_request, task_id)

        assert code == 200
        assert status["status"] == "success"
        assert status["data"]["status"] == "completed"
        assert status["data"]["progress"] == 100

        # Clean up
        dao.delete_async_operation(task_id)

    def test_status_failed_operation(self):
        """Test status for failed operation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        # Create a failed operation
        from taosanalytics.tsfm.foundation_models_db import AsyncOperation, get_dao
        dao = get_dao()
        task_id = "delete-test-model-ghi789"
        operation = AsyncOperation(
            task_id=task_id,
            operation="delete",
            model_name="test-model",
            progress=50,
            status="failed",
            error="Disk space insufficient",
        )
        dao.create_async_operation(operation)

        status, code = response_event.get_operation_status(mock_request, task_id)

        assert code == 200
        assert status["status"] == "success"
        assert status["data"]["status"] == "failed"
        assert "error" in status["data"]
        assert "Disk space" in status["data"]["error"]

        # Clean up
        dao.delete_async_operation(task_id)


class TestValidateModelInRepository:
    """Test _extract_model_status_in_repository helper."""

    def test_validate_model_exists(self):
        """Test model validation when model exists."""
        mock_response = {
            "resources": [
                {
                    "kind": "model",
                    "name": "moirai",
                    "model_id": "Salesforce/moirai-moe-1.0-R-small",
                }
            ]
        }

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            is_valid, model_info, error = foundation_model._extract_model_status_in_repository(
                "moirai", "1.0.0", "https://example.com"
            )

            assert is_valid
            assert model_info is not None
            assert error is None

    def test_validate_model_not_exists(self):
        """Test model validation when model doesn't exist."""
        mock_response = {"resources": []}

        with patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            is_valid, model_info, error = foundation_model._extract_model_status_in_repository(
                "nonexistent", "1.0.0", "https://example.com"
            )

            assert not is_valid
            assert model_info is None
            assert "not found" in error.lower()

    def test_validate_taosanodectl_not_found(self):
        """Test validation when taosanodectl is missing.

        Both lookups are pinned on purpose: this resolution prefers a
        ``taosanodectl`` found on PATH, so on a host that has one installed the
        test would otherwise run the real CLI instead of exercising the
        "not found anywhere" branch it is about.
        """
        with patch("pathlib.Path.exists", return_value=False), patch(
            "shutil.which", return_value=None
        ), patch("taosanalytics.tsfm.foundation_model.subprocess.run") as mock_run:
            is_valid, model_info, error = foundation_model._extract_model_status_in_repository(
                "moirai", "1.0.0", "https://example.com"
            )

            assert not is_valid
            assert model_info is None
            assert "not found" in error.lower()
            mock_run.assert_not_called()
