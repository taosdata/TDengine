import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from taosanalytics.tsfmservice import foundation_model
from taosanalytics.tsfmservice.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
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

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
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

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("cmd", 30)

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 504
            assert status["status"] == "error"

    def test_list_all_tsfm_invalid_json(self):
        """Test invalid JSON response handling."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="invalid json")

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 500
            assert status["status"] == "error"
            assert "parse" in status["error"].lower()


class TestGetModelInfo:
    """Test get_model_info endpoint."""

    def test_get_model_info_success(self):
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

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            status, code = foundation_model.get_model_info(mock_request, "moirai")

            assert code == 200
            assert status["status"] == "success"
            assert status["data"]["name"] == "moirai"

    def test_get_model_info_not_found(self):
        """Test model not found."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        mock_response = {"models": []}

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            status, code = foundation_model.get_model_info(mock_request, "nonexistent")

            assert code == 404
            assert status["status"] == "error"


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
        assert "model_name" in status["error"]

    def test_download_invalid_model(self):
        """Test download request for non-existent model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "nonexistent"}

        with patch(
            "taosanalytics.tsfmservice.foundation_model._validate_model_in_repository"
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
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=True)
        model_info = {"name": "moirai", "model_id": "test"}

        with patch(
            "taosanalytics.tsfmservice.foundation_model._validate_model_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (True, model_info, None)

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 400
            assert "already downloaded" in status["error"]

    def test_download_running_model_with_replace(self):
        """Test replace request for running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai", "replace": True}

        # Mock validation to succeed and set model as running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", running=True)
        model_info = {"name": "moirai", "model_id": "test"}

        with patch(
            "taosanalytics.tsfmservice.foundation_model._validate_model_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (True, model_info, None)

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 400
            assert "running" in status["error"].lower()

    def test_download_success(self, reset_foundation_models):
        """Test successful download initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        with patch(
            "taosanalytics.tsfmservice.foundation_model._validate_model_in_repository"
        ) as mock_validate, patch(
            "taosanalytics.tsfmservice.foundation_model._trigger_model_download_background"
        ):
            dao = reset_foundation_models
            for operation_status in ("interrupted", "failed", "completed"):
                dao.create_async_operation(AsyncOperation(
                    task_id=f"download-moirai-{operation_status}",
                    operation="download",
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
        assert "model_name" in status["error"]

    def test_start_non_downloaded_model(self):
        """Test start request for non-downloaded model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Ensure model is not downloaded
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=False, running=False)

        status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 400
        assert "not downloaded" in status["error"].lower()

    def test_start_already_running_model(self):
        """Test start request for already running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded and running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True)

        status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 400
        assert "already running" in status["error"].lower()

    def test_start_success(self):
        """Test successful model start initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded but not running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False)

        with patch(
            "taosanalytics.tsfmservice.foundation_model._trigger_model_start_background"
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
        assert "model_name" in status["error"]

    def test_invalid_model(self):
        """Test download-and-start request for non-existent model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "nonexistent"}

        with patch(
            "taosanalytics.tsfmservice.foundation_model._validate_model_in_repository"
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

        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True)

        status, code = foundation_model.download_and_start_foundation_model(mock_request)

        assert code == 400
        assert "already running" in status["error"].lower()

    def test_already_downloaded_starts_without_download(self):
        """Test already-downloaded model triggers start only (download skipped)."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False)

        with patch(
            "taosanalytics.tsfmservice.foundation_model._trigger_model_start_background"
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
            "taosanalytics.tsfmservice.foundation_model._validate_model_in_repository"
        ) as mock_validate, patch(
            "taosanalytics.tsfmservice.foundation_model._trigger_model_download_start_background"
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
        # Simulate the task being cancelled/removed while it waits for the lock.
        dao.delete_async_operation(task_id)

        with patch(
            "taosanalytics.tsfmservice.foundation_model._try_acquire_download_lock",
            return_value=None,
        ) as mock_lock:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
        mock_lock.assert_called_once()

    def test_aborts_wait_when_op_no_longer_queued(self, reset_foundation_models):
        """Waiting for the lock aborts if the op is no longer 'queued' (cancelled)."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        # Simulate a cancellation that flips the status away from queued.
        dao.update_async_operation(
            task_id, status=foundation_model.TaskStatus.INTERRUPTED.value
        )

        with patch(
            "taosanalytics.tsfmservice.foundation_model._try_acquire_download_lock",
            return_value=None,
        ):
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value

    def test_aborts_after_lock_when_op_invalid(self, reset_foundation_models):
        """After winning the lock, a stale/cancelled op is not executed."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        dao.delete_async_operation(task_id)

        with patch(
            "taosanalytics.tsfmservice.foundation_model._try_acquire_download_lock",
            return_value=1234,
        ), patch(
            "taosanalytics.tsfmservice.foundation_model._release_download_lock"
        ) as mock_release, patch(
            "taosanalytics.tsfmservice.foundation_model._get_taosanodectl_cmd"
        ) as mock_cmd:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
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
        assert "model_name" in status["error"]

    def test_stop_non_running_model(self):
        """Test stop request for non-running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Ensure model is not running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", running=False)

        status, code = foundation_model.stop_foundation_model(mock_request)

        assert code == 400
        assert "not running" in status["error"].lower()

    def test_stop_success(self):
        """Test successful model stop initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", running=True)

        with patch(
            "taosanalytics.tsfmservice.foundation_model._trigger_model_stop_background"
        ):
            status, code = foundation_model.stop_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status

            # Clean up
            dao.update_model("moirai", running=False)


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
        assert "model_name" in status["error"]

    def test_delete_running_model(self):
        """Test delete request for running model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", running=True)

        status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 400
        assert "running" in status["error"].lower()

        # Clean up
        dao.update_model("moirai", running=False)

    def test_delete_non_downloaded_model(self):
        """Test delete request for non-downloaded model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as not downloaded
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=False, running=False)

        status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 400
        assert "not downloaded" in status["error"].lower()

    def test_delete_success(self):
        """Test successful model delete initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded but not running
        from taosanalytics.tsfmservice.foundation_models_db import get_dao
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False)

        with patch(
            "taosanalytics.tsfmservice.foundation_model._trigger_model_delete_background"
        ):
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

        status, code = foundation_model.get_operation_status(mock_request, "nonexistent-task")

        assert code == 404
        assert status["status"] == "error"
        assert "not found" in status["error"].lower()

    def test_status_pending_operation(self):
        """Test status for pending operation returns current state without mock advancement."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        # Create a pending operation
        from taosanalytics.tsfmservice.foundation_models_db import AsyncOperation, get_dao
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

        status, code = foundation_model.get_operation_status(mock_request, task_id)

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
        from taosanalytics.tsfmservice.foundation_models_db import AsyncOperation, get_dao
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

        status, code = foundation_model.get_operation_status(mock_request, task_id)

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
        from taosanalytics.tsfmservice.foundation_models_db import AsyncOperation, get_dao
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

        status, code = foundation_model.get_operation_status(mock_request, task_id)

        assert code == 200
        assert status["status"] == "success"
        assert status["data"]["status"] == "failed"
        assert "error" in status["data"]
        assert "Disk space" in status["data"]["error"]

        # Clean up
        dao.delete_async_operation(task_id)


class TestValidateModelInRepository:
    """Test _validate_model_in_repository helper."""

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

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            is_valid, model_info, error = foundation_model._validate_model_in_repository(
                "moirai", "1.0.0", "https://example.com"
            )

            assert is_valid
            assert model_info is not None
            assert error is None

    def test_validate_model_not_exists(self):
        """Test model validation when model doesn't exist."""
        mock_response = {"resources": []}

        with patch("taosanalytics.tsfmservice.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            is_valid, model_info, error = foundation_model._validate_model_in_repository(
                "nonexistent", "1.0.0", "https://example.com"
            )

            assert not is_valid
            assert model_info is None
            assert "not found" in error.lower()

    def test_validate_taosanodectl_not_found(self):
        """Test validation when taosanodectl is missing."""
        with patch("pathlib.Path.exists", return_value=False):
            is_valid, model_info, error = foundation_model._validate_model_in_repository(
                "moirai", "1.0.0", "https://example.com"
            )

            assert not is_valid
            assert model_info is None
            assert "not found" in error.lower()
