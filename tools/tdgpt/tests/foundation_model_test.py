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
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lib.tsfm.mgmt import executor, foundation_model, foundation_util, response_event
from lib.tsfm.mgmt.foundation_models_db import (
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


@pytest.fixture(autouse=True)
def no_live_workers():
    """Every control-plane request asks the pid files which models are running.

    Answered here so the suite never depends on the workers of the machine it
    runs on. Tests that need a live worker patch ``live_worker_names``
    themselves, which overrides this.
    """
    with patch.object(foundation_util, "live_worker_names", return_value=[]):
        yield


@pytest.fixture(autouse=True)
def no_installed_inventory():
    """The installed-resource inventory is probed through the CLI as well.

    ``None`` means "unknown", which leaves ``downloaded`` as the database has it
    -- the behaviour every existing test was written against. The tests that check
    the inventory-driven repair return a list instead.
    """
    with patch.object(foundation_util, "installed_resource_names", return_value=None):
        yield


@pytest.fixture(autouse=True)
def no_cli_inflight_apply():
    """The resource task directory belongs to the machine, not to the test.

    ``_cli_inflight_downloads`` reads it to learn what the CLI is applying, so a
    task file left by a real download on this host would otherwise decide the
    runtime state these tests assert on.
    """
    with patch.object(foundation_model, "_cli_inflight_downloads", return_value=[]):
        yield


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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(mock_response))

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 200
            assert status["status"] == "success"
            assert len(status["data"]["resources"]) == 1

    @pytest.mark.parametrize("release", [None, "1.0.0"])
    def test_resource_check_default_and_explicit_release(self, release):
        mock_request = Mock(remote_addr="127.0.0.1")
        mock_request.args = {} if release is None else {"release": release}
        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as run:
            run.return_value = Mock(returncode=0, stdout=json.dumps({"resources": []}))
            _, code = foundation_model.list_all_tsfm(mock_request)
        assert code == 200
        command = next(call.args[0] for call in run.call_args_list if "--release" in call.args[0])
        assert command[command.index("--release") + 1] == (release or "1.0.1")

    def test_list_all_tsfm_timeout(self):
        """Test timeout handling."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("cmd", 30)

            status, code = foundation_model.list_all_tsfm(mock_request)

            assert code == 504
            assert status["status"] == "error"

    def test_list_all_tsfm_invalid_json(self):
        """Test invalid JSON response handling."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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

    def test_list_all_tsfm_reports_idle_for_an_installed_runtime(self):
        """The shared runtime has no row in the models table.

        ``model_state`` came from that row, so it answered "missing" for a
        runtime that is installed. It is now derived from the resource itself: an
        installed runtime is present and idle, while ``state`` keeps the CLI's
        own word for it.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [
                {"kind": "runtime", "name": "tsfm_runtime", "state": "installed", "size": 1},
                {"kind": "model", "name": "moirai", "state": "installed", "size": 1},
            ]
        }
        local_response = {"runtime": {"runtime_id": "tsfm"}, "models": []}

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout=json.dumps(check_response)),
                Mock(returncode=0, stdout=json.dumps(local_response)),
            ]

            status, code = foundation_model.list_all_tsfm(mock_request)

        assert code == 200
        resources = {entry["name"]: entry for entry in status["data"]["resources"]}
        assert resources["tsfm_runtime"]["model_state"] == "idle"
        assert resources["tsfm_runtime"]["state"] == "installed"
        # A model keeps its own value, from its row.
        assert resources["moirai"]["model_state"] == "missing"

    def test_list_all_tsfm_reports_missing_for_an_absent_runtime(self):
        """An uninstalled runtime is missing, like a model that was never downloaded."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [
                {"kind": "runtime", "name": "tsfm_runtime", "state": "missing", "size": 1},
            ]
        }
        local_response = {"runtime": None, "models": []}

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout=json.dumps(check_response)),
                Mock(returncode=0, stdout=json.dumps(local_response)),
            ]

            status, code = foundation_model.list_all_tsfm(mock_request)

        assert code == 200
        assert status["data"]["resources"][0]["model_state"] == "missing"

    def test_list_all_tsfm_follows_an_in_flight_runtime_operation(self):
        """A runtime download owns the state while it runs, as it does for a model."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [
                {"kind": "runtime", "name": "tsfm_runtime", "state": "missing", "size": 1},
            ]
        }
        local_response = {"runtime": None, "models": []}

        get_dao().create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-1",
            operation="download",
            model_name="tsfm_runtime",
            status="queued",
        ))

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout=json.dumps(check_response)),
                Mock(returncode=0, stdout=json.dumps(local_response)),
            ]

            status, code = foundation_model.list_all_tsfm(mock_request)

        assert code == 200
        assert status["data"]["resources"][0]["model_state"] == "downloading"

    def test_list_all_tsfm_survives_a_broken_local_view(self):
        """A local view that cannot be read degrades the listing, not fails it."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.args = {}

        check_response = {
            "resources": [{"kind": "model", "name": "moirai", "state": "installed", "size": 1}]
        }

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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


class TestRuntimeModelState:
    """The runtime's model_state, derived from its own state and operation row."""

    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            # Settled, present: nothing is running, and the entry is actionable.
            ("installed", "idle"),
            # `outdated` is a directory that is not the current release, so
            # nothing here can be acted on: it is `missing`, which is what lets a
            # client offer the install.
            ("outdated", "missing"),
            ("missing", "missing"),
            # The CLI's task-file phases without a live task file behind them.
            # `resource check` copies the phase into `state` without checking its
            # timestamp, so a killed apply leaves one behind for good; the
            # caller's snapshot is the age-checked signal (see
            # test_a_stale_phase_is_not_reported_as_downloading).
            ("downloading", "idle"),
            ("verifying", "idle"),
            ("importing", "idle"),
            # A failed apply leaves nothing usable.
            ("failed", "missing"),
            # Not a value this version knows: read as present, so a client that
            # acts on the entry is not left with an empty action set.
            ("upgrading", "idle"),
            ("launching", "idle"),
            ("starting", "idle"),
            ("stopping", "idle"),
            ("delete_pending", "idle"),
            ("Delete Pending", "idle"),
        ],
    )
    def test_state_maps_to_a_model_state(self, state, expected):
        assert foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": state}, None, set()
        ) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (" Installed ", "installed"),
            ("OUTDATED", "outdated"),
            (" Delete-Pending ", "delete_pending"),
            ("Delete Pending", "delete_pending"),
            (" VERIFYING ", "verifying"),
            (None, ""),
        ],
    )
    def test_a_resource_state_is_normalized_before_it_is_read(self, value, expected):
        """Spacing, case and punctuation are the manifest's, not this module's.

        Normalization is asserted here, on the helper, because most spellings now
        map onto the same `model_state`: only a value that changes the answer
        (see the next test) would show it through the mapping.
        """
        assert foundation_model._normalize_resource_state(value) == expected

    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            # Spelling does not change what the mapping answers.
            (" Installed ", "idle"),
            ("OUTDATED", "missing"),
            (" VERIFYING ", "idle"),
            (" Failed ", "missing"),
        ],
    )
    def test_a_differently_spelled_state_maps_the_same_way(self, state, expected):
        assert foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": state}, None, set()
        ) == expected

    def test_an_in_flight_operation_wins_over_the_entry_state(self):
        """A replace download runs while the files are still installed."""
        op = Mock(operation="download", status="queued")

        assert foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": "installed"}, op, set()
        ) == "downloading"

    @pytest.mark.parametrize(
        ("operation", "status", "expected"),
        [
            # The runtime has exactly two operations: it is downloaded and
            # deleted, never started or stopped. Both in-flight sets are what
            # makes the entry un-actionable, so each one is pinned here.
            ("download", "queued", "downloading"),
            ("download", "running", "downloading"),
            ("download", "deferred", "downloading"),
            ("delete", "queued", "deleting"),
            ("delete", "running", "deleting"),
            ("delete", "deferred", "deleting"),
            # `_RUNTIME_OPERATIONS` is a whitelist, not "any in-flight row": a
            # worker lifecycle belongs to a model, and the runtime has no worker.
            # Reading one as the runtime's own would hand back a state that
            # permits no action and that nothing could move the entry out of.
            ("start", "running", "idle"),
            ("stop", "queued", "idle"),
        ],
    )
    def test_an_in_flight_runtime_operation_owns_the_state(
        self, operation, status, expected
    ):
        op = Mock(operation=operation, status=status)

        assert foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": "installed"}, op, set()
        ) == expected

    def test_a_finished_operation_does_not_override_the_entry_state(self):
        """History is not evidence: a completed delete leaves `state` to answer."""
        op = Mock(operation="delete", status="completed")

        assert foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": "installed"}, op, set()
        ) == "idle"

    def test_a_cli_apply_is_reported_as_downloading(self):
        """An apply the CLI started has no operation row; the caller's snapshot says so.

        `resource check` only reports a task-file phase for the digest it is
        checking, so an apply for another release leaves `state` settled and the
        snapshot of the task files is the only witness.
        """
        result = foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": "installed"},
            None,
            {"tsfm_runtime"},
        )

        assert result == "downloading"

    def test_a_live_task_file_keeps_its_phase_reporting_as_downloading(self):
        """A phase is believed only while the snapshot still lists the resource.

        The same phase that is ignored as residue below is the correct answer
        while the task file is fresh -- otherwise the fix would stop reporting
        applies the API did not start.
        """
        for state in ("downloading", "verifying", "importing"):
            assert foundation_model._runtime_model_state(
                {"kind": "runtime", "name": "tsfm_runtime", "state": state},
                None,
                {"tsfm_runtime"},
            ) == "downloading"

    def test_a_stale_phase_is_not_reported_as_downloading(self):
        """A killed CLI apply must not pin the runtime at `downloading`.

        `check_release` copies the task file's phase into `state` without looking
        at `updated_at`, so an apply killed mid-import leaves `importing` there
        for good. `downloading` permits no action, so the entry would stay greyed
        out until someone re-applied or deleted it from the CLI; the snapshot
        already drops stale files, and that answer has to win.
        """
        with patch.object(foundation_model.AppLogger, "info") as info:
            result = foundation_model._runtime_model_state(
                {"kind": "runtime", "name": "tsfm_runtime", "state": "importing"},
                None,
                set(),  # the task file is stale, so the scan dropped it
            )

        assert result == "idle"
        assert info.called

    def test_another_resources_cli_apply_does_not_change_the_runtime(self):
        """Task files are matched by name: a model's apply is not the runtime's."""
        result = foundation_model._runtime_model_state(
            {"kind": "runtime", "name": "tsfm_runtime", "state": "installed"},
            None,
            {"moirai"},
        )

        assert result == "idle"

    def test_an_unrecognized_state_is_logged_and_read_as_installed(self):
        """An unknown resource state is reported as present, and logged.

        `model_state` is the closed set a client keys its allowed actions on, so
        an unfamiliar value must not reach it: the entry would come back with no
        action at all. `state` keeps the value the CLI published.
        """
        with patch.object(foundation_model.AppLogger, "debug") as debug:
            result = foundation_model._runtime_model_state(
                {"kind": "runtime", "name": "tsfm_runtime", "state": "brand-new"},
                None,
                set(),
            )

        assert result == "idle"
        assert debug.called


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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository"
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
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository"
        ) as mock_validate:
            mock_validate.return_value = (True, model_info, None)

            status, code = foundation_model.download_foundation_model(mock_request)

            assert code == 400
            assert "already downloaded" in status["message"]

    def test_a_repeated_request_rearms_the_task_it_reuses(self, reset_foundation_models):
        """The caller asking again is what re-arms a queued download's retry.

        Dedupe answers "already in progress" for a queued download whose timer was
        lost, so without this the task would only move if the client later polled
        /status -- which a client reading the SSE stream never does. The resume is
        aimed at the reused task, not at whichever download is due first.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}
        active_op = Mock(task_id="download-moirai-lost", status="queued")

        with patch(
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository",
            return_value=(True, {"name": "moirai"}, None),
        ), patch.object(
            foundation_util, "get_active_download_op", return_value=active_op
        ), patch(
            "lib.tsfm.mgmt.foundation_model.resume_due_downloads"
        ) as resume, patch(
            "lib.tsfm.mgmt.foundation_model._reuse_download_response",
            return_value=({"status": "success", "reused": True}, 200),
        ):
            status, code = foundation_model.download_foundation_model(mock_request)

        assert code == 200
        assert status["reused"] is True
        resume.assert_called_once_with(task_id="download-moirai-lost")

    def test_a_failed_resume_still_answers_with_the_reused_task(self):
        """Recovery is best-effort: the caller still learns which task it got.

        The resume is a repair, not part of the dedupe answer: a database error
        there must not turn "already in progress" into an error the client would
        retry with a second request.
        """
        active_op = Mock(task_id="download-moirai-lost", status="queued")

        with patch(
            "lib.tsfm.mgmt.foundation_model.resume_due_downloads",
            side_effect=RuntimeError("database is locked"),
        ), patch(
            "lib.tsfm.mgmt.foundation_model._reuse_download_response",
            return_value=({"status": "success", "reused": True}, 200),
        ):
            status, code = foundation_model._reuse_resumed_download(active_op, "moirai")

        assert code == 200
        assert status["reused"] is True

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
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository"
        ) as mock_validate, patch.object(
            foundation_util, "live_worker_names", return_value=["moirai"]
        ):
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
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository"
        ) as mock_validate, patch(
            "lib.tsfm.mgmt.foundation_model._trigger_model_download_background"
        ), patch(
            "lib.tsfm.mgmt.foundation_model._runtime_model_download_blocker",
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
        """A live worker -- pid liveness, not the cached flag -- refuses the start."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as downloaded and running
        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True)

        with patch.object(foundation_util, "live_worker_names", return_value=["moirai"]):
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
            "lib.tsfm.mgmt.foundation_model._trigger_model_start_background"
        ):
            status, code = foundation_model.start_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status

    def test_start_refused_when_the_files_are_gone(self):
        """The inventory is the authority on `downloaded`, not the cached flag.

        A directory removed from the CLI leaves the row claiming an idle, installed
        model. The request reconciles against the inventory first, so it is refused
        as "not downloaded" instead of spawning a worker that cannot start.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False, model_state="idle")

        with patch.object(
            foundation_util, "installed_resource_names", return_value=[]
        ), patch(
            "lib.tsfm.mgmt.foundation_model._trigger_model_start_background"
        ) as trigger:
            status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 400
        assert "not downloaded" in status["message"].lower()
        assert not trigger.called

        model = dao.get_model("moirai")
        assert model.model_state == "missing"
        assert model.downloaded is False

    def test_start_recovers_a_model_whose_worker_died(self):
        """The same crash that blocks delete also blocked restart: both are fixed."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True, model_state="started")

        with patch.object(
            foundation_util, "live_worker_names", return_value=[]
        ), patch(
            "lib.tsfm.mgmt.foundation_model._trigger_model_start_background"
        ) as trigger:
            status, code = foundation_model.start_foundation_model(mock_request)

        assert code == 202
        assert trigger.called
        assert dao.get_model("moirai").model_state == "starting"


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
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository"
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
        dao.update_model("moirai", downloaded=True, running=True, model_state="started")

        with patch.object(foundation_util, "live_worker_names", return_value=["moirai"]):
            status, code = foundation_model.download_and_start_foundation_model(mock_request)

        assert code == 400
        assert "started" in status["message"].lower()

    def test_already_downloaded_starts_without_download(self):
        """Test already-downloaded model triggers start only (download skipped)."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=False, model_state="idle")

        with patch(
            "lib.tsfm.mgmt.foundation_model._trigger_model_start_background"
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

    def test_a_repeated_request_rearms_the_task_it_reuses(self, reset_foundation_models):
        """The download-and-start dedupe re-arms the task it hands back too.

        It returns before `_claim_download_operation`, so the resume in that path
        never runs for it; without this the caller would be answered "already in
        progress" about a queued task whose timer was lost, and only a later
        /status poll would move it -- which an SSE client never makes.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}
        active_op = Mock(task_id="download-start-moirai-lost", status="queued")

        with patch(
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository",
            return_value=(True, {"name": "moirai"}, None),
        ), patch.object(
            foundation_util, "get_active_download_op", return_value=active_op
        ), patch(
            "lib.tsfm.mgmt.foundation_model.resume_due_downloads"
        ) as resume, patch(
            "lib.tsfm.mgmt.foundation_model._reuse_download_response",
            return_value=({"status": "success", "reused": True}, 200),
        ):
            status, code = foundation_model.download_and_start_foundation_model(mock_request)

        assert code == 200
        assert status["reused"] is True
        resume.assert_called_once_with(task_id="download-start-moirai-lost")

    def test_download_and_start_success(self, reset_foundation_models):
        """Test successful combined download-and-start initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        with patch(
            "lib.tsfm.mgmt.foundation_model._extract_model_status_in_repository"
        ) as mock_validate, patch(
            "lib.tsfm.mgmt.foundation_model._trigger_model_download_start_background"
        ), patch(
            "lib.tsfm.mgmt.foundation_model._runtime_model_download_blocker",
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
    """Test _run_download_task's pre-spawn check and its CLI-lock deferral."""

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

    def test_a_cancelled_task_never_spawns_the_cli(self, reset_foundation_models):
        """A task removed while it sat in the local queue must not apply."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        dao.delete_async_operation(task_id)

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.Popen") as popen:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
        popen.assert_not_called()

    def test_an_already_cancelled_task_never_spawns_the_cli(self, reset_foundation_models):
        """A row flipped away from queued is not an apply either."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        dao.update_async_operation(
            task_id, status=foundation_model.TaskStatus.INTERRUPTED.value
        )

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.Popen") as popen:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == foundation_model.TaskStatus.INTERRUPTED.value
        popen.assert_not_called()

    def test_an_unreadable_row_defers_instead_of_cancelling(self, reset_foundation_models):
        """A failed read is not a cancellation: the row is still there.

        `get_async_operation` answers None for a database error as well as for a
        missing row, and this check used to read that as "cancelled" -- finishing
        a download that is merely waiting as `interrupted`, with its terminal
        status written over it. The read is strict now, and the task goes back to
        the retry scheduler with its parameters intact.
        """
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)

        with patch.object(
            dao,
            "require_async_operation",
            side_effect=RuntimeError("database is locked"),
        ), patch("lib.tsfm.mgmt.foundation_model.subprocess.Popen") as popen:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == "deferred"
        assert "database is locked" in result["waiting_for"]
        popen.assert_not_called()
        op = dao.get_async_operation(task_id)
        assert op.status == foundation_model.TaskStatus.QUEUED.value
        assert op.retry_after is not None
        # The parameters a recovery runs with survive: the unreadable state means
        # its retry metadata could not be merged into them, so `data` is untouched.
        assert op.data["release"] == "1.0.0"
        assert op.data["resource_base_url"] == ""

    def test_a_refused_apply_defers_with_the_childs_reason(self, reset_foundation_models):
        """Exit 75 means another resource operation holds the CLI lifecycle lock."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        proc = Mock(returncode=foundation_model._CLI_EXIT_TEMPFAIL)
        proc.communicate.return_value = (
            b"",
            b"another TDgpt resource operation is running\n",
        )

        with patch.object(
            foundation_model.subprocess, "Popen", return_value=proc
        ), patch.object(
            foundation_model, "_runtime_model_download_blocker", return_value=None
        ), patch.object(
            foundation_util, "get_taosanodectl_cmd", return_value=["taosanodectl"]
        ):
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == "deferred"
        assert result["phase"] == "download"
        assert result["waiting_for"] == "another TDgpt resource operation is running"
        op = dao.get_async_operation(task_id)
        assert op.status == foundation_model.TaskStatus.QUEUED.value
        assert op.data["waiting_for"] == "another TDgpt resource operation is running"

    def test_a_permanent_refusal_is_reported_as_failed(self, reset_foundation_models):
        """Every other exit code is the child's verdict, not a retry."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        proc = Mock(returncode=foundation_model._CLI_EXIT_CONFLICT)
        proc.communicate.return_value = (b"", b"model is running\n")

        with patch.object(
            foundation_model.subprocess, "Popen", return_value=proc
        ), patch.object(
            foundation_model, "_runtime_model_download_blocker", return_value=None
        ), patch.object(
            foundation_util, "get_taosanodectl_cmd", return_value=["taosanodectl"]
        ):
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == "failed"
        assert "model is running" in result["message"]

    def test_a_model_download_defers_while_the_runtime_is_installing(self, reset_foundation_models):
        """A runtime still on its way is a wait, not a permanent failure.

        The request path accepts a model download while a runtime download is in
        flight (see _reject_download_without_runtime); without the download flock
        this check is what makes the worker wait for that runtime instead of
        failing the model download for a venv that is seconds from existing.
        """
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        dao.create_async_operation(AsyncOperation(
            task_id="download-tsfm_runtime-1",
            operation="download",
            model_name="tsfm_runtime",
            status=foundation_model.TaskStatus.RUNNING.value,
        ))

        with patch.object(
            foundation_model,
            "_runtime_model_download_blocker",
            return_value="runtime import is incomplete",
        ) as blocker, patch("lib.tsfm.mgmt.foundation_model.subprocess.Popen") as popen:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == "deferred"
        assert "still being installed" in result["waiting_for"]
        popen.assert_not_called()
        # An installation is running: the second probe is never reached.
        assert blocker.call_count == 1
        # The row stays queued for the retry scheduler to pick up.
        assert dao.get_async_operation(task_id).status == foundation_model.TaskStatus.QUEUED.value

    def test_a_model_download_fails_when_no_runtime_is_coming(self, reset_foundation_models):
        """A runtime nobody is installing is a failure, with the probe's reason."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)

        with patch.object(
            foundation_model,
            "_runtime_model_download_blocker",
            return_value="runtime is not installed",
        ) as blocker, patch("lib.tsfm.mgmt.foundation_model.subprocess.Popen") as popen:
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == "failed"
        assert "runtime is not installed" in result["message"]
        popen.assert_not_called()
        # Two probes agree before the download is refused for good.
        assert blocker.call_count == 2

    def test_a_runtime_finishing_between_the_checks_does_not_fail_the_download(
        self, reset_foundation_models
    ):
        """The probe and the in-flight lookup observe different moments.

        A runtime install can complete in between: the stale probe still names a
        blocker, while no installation is active any more. Failing on that
        combination refused a download for a runtime that is ready, so the
        terminal decision is taken by a second probe.
        """
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)
        proc = Mock(returncode=0)
        proc.communicate.return_value = (b"", b"")

        with patch.object(
            foundation_model,
            "_runtime_model_download_blocker",
            side_effect=["runtime import is incomplete", None],
        ) as blocker, patch(
            "lib.tsfm.mgmt.foundation_model.subprocess.Popen", return_value=proc
        ) as popen, patch.object(
            foundation_util, "get_taosanodectl_cmd", return_value=["taosanodectl"]
        ):
            result = foundation_model._run_download_task(
                task_id, "moirai", "1.0.0", "https://example.com", False
            )

        assert result["status"] == "completed"
        assert blocker.call_count == 2
        popen.assert_called_once()

    def test_the_deferral_backoff_grows_within_its_cap(self, reset_foundation_models):
        """Each re-armed attempt waits longer, up to the configured ceiling."""
        dao = reset_foundation_models
        task_id = self._make_queued_op(dao)

        delays = [
            foundation_model._defer_download(dao, task_id, "moirai")["delay"]
            for _ in range(6)
        ]

        assert delays[0] == foundation_model._DOWNLOAD_RETRY_CONFIG["base_seconds"]
        assert delays[1] == 2 * foundation_model._DOWNLOAD_RETRY_CONFIG["base_seconds"]
        assert delays[-1] == foundation_model._DOWNLOAD_RETRY_CONFIG["max_delay_seconds"]


class TestDeferredDownload:
    """Test the deferral that replaces blocking on the CLI lifecycle lock."""

    def test_a_queued_download_is_resubmitted(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-rearm",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
        ))
        deferred_fn = Mock()

        with patch("lib.tsfm.mgmt.foundation_model.submit_download_task") as submit:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("download-moirai-rearm", "moirai", "1.0.0", "", False),
                "download-moirai-rearm",
                "moirai",
                "download",
                deferred_fn,
            )

        submit.assert_called_once()
        assert submit.call_args.kwargs["deferred_fn"] is deferred_fn
        # The claim clears the due time: a second submitter must find nothing.
        assert dao.get_async_operation("download-moirai-rearm").retry_after is None

    def test_a_cancelled_download_is_not_resubmitted(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-rearm",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
        ))
        dao.update_async_operation(
            "download-moirai-rearm", status=foundation_model.TaskStatus.INTERRUPTED.value
        )

        with patch("lib.tsfm.mgmt.foundation_model.submit_download_task") as submit:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("download-moirai-rearm", "moirai", "1.0.0", "", False),
                "download-moirai-rearm",
                "moirai",
                "download",
                Mock(),
            )

        submit.assert_not_called()

    def test_the_handler_rearms_only_the_download_phase(self):
        """A start phase deferred on the service executor owns its own retry."""
        other = Mock()
        handler = foundation_model._download_deferred_handler(
            foundation_model._run_download_task,
            ("t", "moirai", "1.0.0", "", False),
            "t",
            "moirai",
            "download",
            on_other_deferred=other,
        )

        with patch("lib.tsfm.mgmt.foundation_model._schedule_download_retry") as schedule:
            handler("deferred", {"status": "deferred", "delay": 5, "phase": "download"})
            handler("deferred", {"status": "deferred", "delay": 5})

        schedule.assert_called_once()
        other.assert_called_once()

    def test_a_due_download_is_claimed_only_once(self, reset_foundation_models):
        """A timer and a status poll can both see the task; only one submits it."""
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-claim",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
        ))
        args = ("download-moirai-claim", "moirai", "1.0.0", "", False)

        with patch("lib.tsfm.mgmt.foundation_model.submit_download_task") as submit:
            for _ in range(2):
                foundation_model._resubmit_download_task(
                    foundation_model._run_download_task,
                    args,
                    "download-moirai-claim",
                    "moirai",
                    "download",
                    Mock(),
                )

        submit.assert_called_once()

    def test_an_early_timer_is_rearmed_instead_of_dropping_the_task(
        self, reset_foundation_models
    ):
        """Whole-second rounding can wake the timer before the due time."""
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-early",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() + 30,
        ))

        with patch(
            "lib.tsfm.mgmt.foundation_model.submit_download_task"
        ) as submit, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_download_retry"
        ) as schedule:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("download-moirai-early", "moirai", "1.0.0", "", False),
                "download-moirai-early",
                "moirai",
                "download",
                Mock(),
            )

        submit.assert_not_called()
        schedule.assert_called_once()
        assert schedule.call_args.args[5]["delay"] >= 1

    def test_a_read_failure_rearms_instead_of_dropping_the_retry(self):
        """A transient database error is not evidence that the task is gone."""
        fake_dao = Mock()
        fake_dao.claim_due_download.return_value = None
        fake_dao.require_async_operation.side_effect = RuntimeError("database is locked")

        with patch.object(
            foundation_model, "get_dao", return_value=fake_dao
        ), patch(
            "lib.tsfm.mgmt.foundation_model.submit_download_task"
        ) as submit, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_download_retry"
        ) as schedule:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("t", "moirai", "1.0.0", "", False),
                "t",
                "moirai",
                "download",
                Mock(),
            )

        submit.assert_not_called()
        schedule.assert_called_once()
        assert schedule.call_args.args[5]["delay"] == foundation_model._DOWNLOAD_RETRY_CONFIG[
            "max_delay_seconds"
        ]

    def test_a_failed_timer_submission_restores_the_due_time(self, reset_foundation_models):
        """The claim is spent when submit raises: without the due time it is lost.

        The next timer would find a queued row with no due time and assume
        somebody else submitted it, and status-poll recovery would skip the row
        too -- a permanently queued download.
        """
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-submitfail",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
        ))

        with patch(
            "lib.tsfm.mgmt.foundation_model.submit_download_task",
            side_effect=RuntimeError("executor shut down"),
        ), patch(
            "lib.tsfm.mgmt.foundation_model._schedule_download_retry"
        ) as schedule:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("download-moirai-submitfail", "moirai", "1.0.0", "", False),
                "download-moirai-submitfail",
                "moirai",
                "download",
                Mock(),
            )

        op = dao.get_async_operation("download-moirai-submitfail")
        assert op.retry_after is not None
        # The restored due time is the recovery path -- and a timer is armed on
        # top, because a client watching the SSE stream never polls /status.
        assert schedule.call_count == 1
        assert schedule.call_args.args[5]["delay"] == foundation_model._DOWNLOAD_RETRY_CONFIG[
            "base_seconds"
        ]
        # The row is claimable again once that due time arrives.
        assert dao.claim_due_download(now=op.retry_after) is not None

    def test_a_failed_release_write_keeps_retrying_until_the_task_is_submitted(
        self, reset_foundation_models
    ):
        """A database error while restoring the due time must not strand the task.

        The retry claims the task, the submission fails, and the write that hands
        the due time back fails too. The row is then queued with a null due time:
        the ordinary claim timer reads that as "already submitted" and a status
        poll skips it. The failed write itself has to be retried until it lands.
        """
        dao = reset_foundation_models
        task_id = "download-moirai-releasefail"
        dao.create_async_operation(AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
            data={"release": "1.0.0", "resource_base_url": "", "replace": False},
        ))

        real_update = dao.update_async_operation
        attempts = []

        def flaky_update(target, **kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1:
                return False  # the release write fails
            return real_update(target, **kwargs)

        reason = RuntimeError("executor shut down")
        with patch.object(
            dao, "update_async_operation", side_effect=flaky_update
        ), patch(
            "lib.tsfm.mgmt.foundation_model.submit_download_task", side_effect=reason
        ), patch(
            "lib.tsfm.mgmt.foundation_model._schedule_download_retry"
        ) as schedule, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule_release, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                (task_id, "moirai", "1.0.0", "", False),
                task_id,
                "moirai",
                "download",
                Mock(),
            )

            # The ordinary claim timer is not armed: it would find the null due
            # time and leave the task queued.
            schedule.assert_not_called()
            schedule_release.assert_called_once()
            assert dao.get_async_operation(task_id).retry_after is None

            # Database recovers: the retried release write lands and the task
            # goes back through the claim path, which submits it.
            foundation_model._retry_claim_release(task_id, reason, 1)

        trigger.assert_called_once()
        assert trigger.call_args.args[0] == task_id
        assert dao.get_async_operation(task_id).retry_after is None

    def test_a_release_write_that_keeps_failing_rearms_itself(
        self, reset_foundation_models
    ):
        """The ladder escalates instead of waiting for a request that may never come."""
        dao = reset_foundation_models

        with patch.object(
            dao, "update_async_operation", return_value=False
        ), patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule:
            foundation_model._retry_claim_release(
                "download-moirai-releasefail", RuntimeError("database is locked"), 1
            )

        schedule.assert_called_once()
        # Attempt 2 is next: the delay keeps growing while the write fails.
        assert schedule.call_args.args[2] == 2

    def test_the_release_ladder_ends_once_the_due_time_is_restored(
        self, reset_foundation_models
    ):
        """A restored task belongs to whoever claims it next, not to this ladder.

        Another caller can claim the restored row and leave it ``queued`` while
        its submission waits in that caller's executor. Repeating the release
        from here would make that task due a second time, and a duplicate that
        finds the operation running reports ``interrupted`` over the real
        outcome.
        """
        dao = reset_foundation_models
        task_id = "download-moirai-claimed-elsewhere"
        dao.create_async_operation(AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            data={"release": "1.0.0", "resource_base_url": "", "replace": False},
        ))

        def claimed_by_another_caller(task_id=None):
            # The other caller's claim clears the due time, and its submission is
            # still waiting in an executor queue -- so the row reads ``queued``.
            dao.update_async_operation(task_id, retry_after=None)
            return None

        with patch.object(
            dao, "claim_due_download", side_effect=claimed_by_another_caller
        ), patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule_release, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_retry"
        ) as schedule_claim, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            foundation_model._retry_claim_release(task_id, RuntimeError("x"), 1)

            # The release landed, so it is not retried; responsibility moves to
            # the ordinary claim path instead.
            schedule_release.assert_not_called()
            schedule_claim.assert_called_once()
            trigger.assert_not_called()
            # The due time the other caller cleared is not written back.
            assert dao.get_async_operation(task_id).retry_after is None

            # That claim timer reads the null due time as ownership and stops.
            foundation_model._resume_restored_download(task_id, 1)

        trigger.assert_not_called()

    def test_a_claim_that_failed_after_the_release_gets_its_own_retry(
        self, reset_foundation_models
    ):
        """A database error in the claim is not another caller's ownership.

        The release write landed, so the release ladder is done -- but nothing
        was submitted, and a client watching the SSE stream never polls. Without
        a claim timer of its own the download would sit queued for good.
        """
        dao = reset_foundation_models
        task_id = "download-moirai-restored-unclaimed"
        dao.create_async_operation(AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            data={"release": "1.0.0", "resource_base_url": "", "replace": False},
        ))

        real_claim = dao.claim_due_download
        failed = []

        def flaky_claim(now=None, task_id=None):
            if not failed:
                failed.append(task_id)
                raise RuntimeError("database is locked")
            return real_claim(now=now, task_id=task_id)

        with patch.object(
            dao, "claim_due_download", side_effect=flaky_claim
        ), patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule_release, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_retry"
        ) as schedule_claim, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            foundation_model._retry_claim_release(task_id, RuntimeError("x"), 1)

            schedule_release.assert_not_called()
            schedule_claim.assert_called_once()
            # The restored due time is left for the claim, not written again.
            assert dao.get_async_operation(task_id).retry_after is not None

            # The database recovers: the claim timer submits the task.
            foundation_model._resume_restored_download(task_id, 1)

        trigger.assert_called_once()
        assert trigger.call_args.args[0] == task_id
        assert dao.get_async_operation(task_id).retry_after is None

    def test_a_submission_that_failed_again_gets_its_own_claim_retry(
        self, reset_foundation_models
    ):
        """The sweep restored the due time itself, so only the claim is retried."""
        dao = reset_foundation_models
        task_id = "download-moirai-resubmit-failed"
        dao.create_async_operation(AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            data={"release": "1.0.0", "resource_base_url": "", "replace": False},
        ))

        with patch.object(
            foundation_model,
            "_trigger_model_download_background",
            side_effect=RuntimeError("executor is gone"),
        ) as trigger, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule_release, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_retry"
        ) as schedule_claim:
            foundation_model._retry_claim_release(task_id, RuntimeError("x"), 1)

            schedule_release.assert_not_called()
            schedule_claim.assert_called_once()
            assert dao.get_async_operation(task_id).retry_after is not None

            # The sweep handed the due time back with the base backoff, so the
            # claim timer only submits once that time has arrived.
            dao.update_async_operation(task_id, retry_after=time.time() - 1)
            trigger.side_effect = None
            foundation_model._resume_restored_download(task_id, 1)

        assert trigger.call_count == 2
        assert dao.get_async_operation(task_id).retry_after is None

    def test_the_release_ladder_stops_when_the_task_is_gone(
        self, reset_foundation_models
    ):
        """A task that no longer exists must not keep a release timer alive."""
        with patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_retry"
        ) as schedule_claim, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            foundation_model._retry_claim_release("download-gone", RuntimeError("x"), 3)

            # The claim path is what discovers it is gone; the release is never
            # written again.
            schedule.assert_not_called()
            schedule_claim.assert_called_once()

            foundation_model._resume_restored_download("download-gone", 1)

        trigger.assert_not_called()
        schedule_claim.assert_called_once()

    def test_a_claim_error_rearms_instead_of_waiting_for_a_request(
        self, reset_foundation_models
    ):
        """claim_due_download raises now, so a database blip is not "nothing due"."""
        fake_dao = Mock()
        fake_dao.claim_due_download.side_effect = RuntimeError("database is locked")

        with patch.object(
            foundation_model, "get_dao", return_value=fake_dao
        ), patch(
            "lib.tsfm.mgmt.foundation_model.submit_download_task"
        ) as submit, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_download_retry"
        ) as schedule:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("t", "moirai", "1.0.0", "", False),
                "t",
                "moirai",
                "download",
                Mock(),
            )

        submit.assert_not_called()
        schedule.assert_called_once()
        assert schedule.call_args.args[5]["delay"] == foundation_model._DOWNLOAD_RETRY_CONFIG[
            "max_delay_seconds"
        ]

    def test_a_due_but_unclaimed_row_is_rearmed(self, reset_foundation_models):
        """"Already scheduled" is only true when the due time was cleared."""
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-unclaimed",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
        ))

        with patch.object(
            dao, "claim_due_download", return_value=None
        ), patch(
            "lib.tsfm.mgmt.foundation_model.submit_download_task"
        ) as submit, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_download_retry"
        ) as schedule:
            foundation_model._resubmit_download_task(
                foundation_model._run_download_task,
                ("download-moirai-unclaimed", "moirai", "1.0.0", "", False),
                "download-moirai-unclaimed",
                "moirai",
                "download",
                Mock(),
            )

        submit.assert_not_called()
        schedule.assert_called_once()
        assert schedule.call_args.args[5]["delay"] == foundation_model._DOWNLOAD_RETRY_CONFIG[
            "base_seconds"
        ]

    def test_download_start_passes_a_deferred_download_through(self):
        """A lost race is not a failed download phase."""
        with patch.object(
            foundation_model,
            "_run_download_task",
            return_value={"status": "deferred", "delay": 5, "phase": "download"},
        ), patch.object(
            foundation_model.foundation_util, "update_model_state"
        ) as update, patch(
            "lib.tsfm.mgmt.foundation_model.submit_service_task"
        ) as submit:
            result = foundation_model._run_download_and_start_task(
                "download-start-moirai-1", "moirai", "1.0.0", "", False
            )

        assert result["status"] == "deferred"
        # The model is not marked downloaded, and the start phase is not submitted.
        update.assert_not_called()
        submit.assert_not_called()


class TestDueDownloadRecovery:
    """The due time on the row recovers a download whose timer was lost."""

    @staticmethod
    def _make_due_op(dao, task_id="download-moirai-lost", model_name="moirai"):
        dao.create_async_operation(AsyncOperation(
            task_id=task_id,
            operation="download",
            model_name=model_name,
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
            data={"release": "1.0.0", "resource_base_url": "https://x", "replace": False},
        ))
        return task_id

    def test_a_due_download_nobody_is_running_is_submitted(self, reset_foundation_models):
        """No timer is involved: this is the request-driven path."""
        dao = reset_foundation_models
        task_id = self._make_due_op(dao)

        with patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            assert foundation_model.resume_due_downloads() == 1

        trigger.assert_called_once()
        assert trigger.call_args.args[0] == task_id
        # The claim cleared the due time, so a second sweep finds nothing.
        assert dao.get_async_operation(task_id).retry_after is None

        with patch.object(foundation_model, "_trigger_model_download_background") as again:
            assert foundation_model.resume_due_downloads() == 0
            again.assert_not_called()

    def test_a_download_that_is_not_due_is_left_alone(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-later",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() + 60,
        ))

        with patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            assert foundation_model.resume_due_downloads() == 0
            trigger.assert_not_called()

    def test_a_failed_submission_hands_the_due_time_back(self, reset_foundation_models):
        """A claim that could not be submitted must not swallow the retry."""
        dao = reset_foundation_models
        task_id = self._make_due_op(dao)

        with patch.object(
            foundation_model,
            "_trigger_model_download_background",
            side_effect=RuntimeError("executor is gone"),
        ):
            assert foundation_model.resume_due_downloads() == 0

        assert dao.get_async_operation(task_id).retry_after is not None

    def test_a_failed_release_here_is_retried_until_the_task_is_submitted(
        self, reset_foundation_models
    ):
        """The request path holds the same duty as the timer: retry the write.

        A submission that raises spends the claim, so the due time has to be put
        back. When that write fails too, the row is queued with a null due time --
        invisible to this sweep and to every timer -- so the *write* is what has
        to be retried, not the claim that keeps finding nothing due.
        """
        dao = reset_foundation_models
        task_id = self._make_due_op(dao)

        real_update = dao.update_async_operation
        failed = []

        def flaky_update(target, **kwargs):
            if "retry_after" in kwargs and not failed:
                failed.append(target)
                return False
            return real_update(target, **kwargs)

        with patch.object(
            dao, "update_async_operation", side_effect=flaky_update
        ), patch.object(
            foundation_model,
            "_trigger_model_download_background",
            side_effect=RuntimeError("executor is gone"),
        ) as trigger, patch(
            "lib.tsfm.mgmt.foundation_model._schedule_claim_release_retry"
        ) as schedule_release:
            assert foundation_model.resume_due_downloads() == 0

            # The claim is spent and the due time was not restored: only the
            # release write itself can make the task findable again.
            schedule_release.assert_called_once()
            assert dao.get_async_operation(task_id).retry_after is None

            # The database recovers: the ladder restores the due time and hands
            # the task back to the claim path, which submits it.
            trigger.side_effect = None
            foundation_model._retry_claim_release(
                task_id, RuntimeError("executor is gone"), 1
            )

        assert trigger.call_count == 2
        assert trigger.call_args.args[0] == task_id
        assert dao.get_async_operation(task_id).retry_after is None

    def test_a_claim_hands_back_a_row_with_no_due_time(
        self, reset_foundation_models
    ):
        """The returned row must agree with the write the claim performed.

        The row is selected before the UPDATE clears ``retry_after``, so the
        object would otherwise still show the due time that was just spent --
        and a caller reading it as "not yet claimed" would wait for a claim it
        already holds.
        """
        dao = reset_foundation_models
        task_id = self._make_due_op(dao)

        claimed = dao.claim_due_download(task_id=task_id)

        assert claimed is not None
        assert claimed.task_id == task_id
        assert claimed.retry_after is None
        assert dao.require_async_operation(task_id).retry_after is None

    def test_a_claim_error_does_not_break_the_request_driven_recovery(
        self, reset_foundation_models
    ):
        """A failed claim is logged and retried by the next check, not raised."""
        dao = reset_foundation_models
        with patch.object(
            dao,
            "claim_due_download",
            side_effect=RuntimeError("database is locked"),
        ):
            assert foundation_model.resume_due_downloads() == 0

    def test_a_download_start_is_resumed_with_its_own_worker(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-start-moirai-lost",
            operation="download_start",
            model_name="moirai",
            status=foundation_model.TaskStatus.QUEUED.value,
            retry_after=time.time() - 1,
            data={"release": "1.0.0", "resource_base_url": "", "replace": False},
        ))

        with patch.object(
            foundation_model, "_trigger_model_download_start_background"
        ) as trigger, patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as plain:
            assert foundation_model.resume_due_downloads() == 1

        trigger.assert_called_once()
        plain.assert_not_called()


class TestDeferPersistsItsDueTime:
    """_defer_download must leave a row the retry can be recovered from."""

    def test_the_due_time_is_written_with_the_retry_state(self, reset_foundation_models):
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-due",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.RUNNING.value,
        ))

        before = time.time()
        result = foundation_model._defer_download(dao, "download-moirai-due", "moirai")

        op = dao.get_async_operation("download-moirai-due")
        assert result["status"] == "deferred"
        assert op.status == foundation_model.TaskStatus.QUEUED.value
        assert op.retry_after is not None
        assert op.retry_after >= before + result["delay"] - 1
        assert op.data["attempt"] == 1

    def test_an_unrecordable_retry_is_reported_as_failed(self, reset_foundation_models):
        """A deferred result whose row never went back to queued helps nobody."""
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-nodue",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.RUNNING.value,
        ))

        with patch.object(dao, "update_async_operation", return_value=False) as update:
            result = foundation_model._defer_download(
                dao, "download-moirai-nodue", "moirai"
            )

        assert result["status"] == foundation_model.TaskStatus.FAILED.value
        assert "retry" in result["message"].lower()
        assert update.call_count == foundation_model._STATE_UPDATE_ATTEMPTS

    def test_a_failed_state_read_keeps_the_download_parameters(self, reset_foundation_models):
        """The payload drives the recovered run, so a read failure must not erase it.

        Reading through the error-swallowing getter would answer {} and the write
        would then replace release/resource_base_url/replace with retry metadata:
        a status poll claiming the retry would silently run with the defaults.
        """
        dao = reset_foundation_models
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-params",
            operation="download",
            model_name="moirai",
            status=foundation_model.TaskStatus.RUNNING.value,
            data={
                "release": "2.0.0",
                "resource_base_url": "https://repo.example/rel",
                "replace": True,
            },
        ))

        with patch.object(
            dao,
            "require_async_operation",
            side_effect=RuntimeError("database is locked"),
        ):
            result = foundation_model._defer_download(
                dao, "download-moirai-params", "moirai"
            )

        assert result["status"] == "deferred"
        op = dao.get_async_operation("download-moirai-params")
        assert op.status == foundation_model.TaskStatus.QUEUED.value
        assert op.retry_after is not None
        # The write omitted `data`, so the parameters a recovery runs with survive.
        assert op.data["release"] == "2.0.0"
        assert op.data["resource_base_url"] == "https://repo.example/rel"
        assert op.data["replace"] is True

        # Request-driven recovery submits it with those parameters, not defaults.
        dao.update_async_operation(
            "download-moirai-params", retry_after=time.time() - 1
        )
        with patch.object(
            foundation_model, "_trigger_model_download_background"
        ) as trigger:
            assert foundation_model.resume_due_downloads() == 1

        trigger.assert_called_once()
        assert trigger.call_args.args[1] == "moirai"
        assert trigger.call_args.args[2] == "2.0.0"
        assert trigger.call_args.args[3] == "https://repo.example/rel"
        assert trigger.call_args.args[4] is True


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

    def test_stop_refused_while_its_download_is_in_flight(self):
        """A model being downloaded has no worker to stop, and the state machine
        has no `downloading -> stopping` edge: name the download rather than
        answering the misleading "not running"."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", model_state="downloading")
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-abc",
            operation="download",
            model_name="moirai",
            status="running",
        ))

        with patch.object(foundation_model, "_trigger_model_stop_background") as trigger:
            status, code = foundation_model.stop_foundation_model(mock_request)

        assert code == 400
        assert "download" in status["message"].lower()
        assert "download-moirai-abc" in status["message"]
        assert not trigger.called

    def test_stop_success(self):
        """Test successful model stop initiation."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        # Set model as running
        dao = get_dao()
        dao.update_model("moirai", running=True, model_state="started")

        with patch(
            "lib.tsfm.mgmt.foundation_model._trigger_model_stop_background"
        ), patch.object(foundation_util, "live_worker_names", return_value=["moirai"]):
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

    def test_delete_refused_while_its_download_is_in_flight(self):
        """The delete runs `resource delete`, which takes the same lifecycle lock
        as the download, and the state machine has no `downloading -> deleting`
        edge. The request is refused now instead of being queued to wait."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", model_state="downloading")
        dao.create_async_operation(AsyncOperation(
            task_id="download-moirai-abc",
            operation="download",
            model_name="moirai",
            status="running",
        ))

        with patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 400
        assert "download" in status["message"].lower()
        assert not trigger.called

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
            "lib.tsfm.mgmt.foundation_model._trigger_model_delete_background"
        ), patch.object(foundation_util, "live_worker_names", return_value=[]):
            status, code = foundation_model.delete_foundation_model(mock_request)

            assert code == 202
            assert status["status"] == "queued"
            assert "task_id" in status


    def test_delete_recovers_a_model_whose_worker_died(self):
        """A crashed worker leaves `started`, a state only `stopping` may leave.

        The pid files say the worker is gone, so the delete must not be refused
        by a lifecycle that ended without being recorded -- and the caller gets a
        task, not a 500.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True, model_state="started")

        with patch.object(
            foundation_util, "live_worker_names", return_value=[]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 202
        assert status["status"] == "queued"
        assert trigger.called
        assert dao.get_model("moirai").model_state == "deleting"

    def test_delete_is_not_queued_when_its_transition_is_refused(self):
        """A refused transition must not leave a queued delete nobody runs.

        `starting` belongs to a start operation in flight: deleting is not allowed
        from it, and the answer is a client error with no row created.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, model_state="starting")
        dao.create_async_operation(AsyncOperation(
            task_id="start-moirai-1",
            operation="start",
            model_name="moirai",
            status="running",
        ))

        with patch.object(
            foundation_util, "live_worker_names", return_value=[]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 400
        assert "starting" in status["message"]
        assert not trigger.called
        assert [op.operation for op in dao.list_async_operations(model_name="moirai")] == [
            "start"
        ]
        assert dao.get_model("moirai").model_state == "starting"

    def test_delete_heals_a_start_that_died_without_an_outcome(self):
        """`starting` with no operation and no worker is crash residue.

        Nothing owns that state, so it is corrected from the pid files before the
        request is judged -- which is what lets a wedged model be cleaned up
        without editing the database.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, model_state="starting")

        with patch.object(
            foundation_util, "live_worker_names", return_value=[]
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 202
        assert trigger.called
        assert dao.get_model("moirai").model_state == "deleting"

    def test_refused_when_liveness_cannot_be_probed(self):
        """An unknown liveness is refused instead of guessed.

        Neither "running" nor "stopped" can be established, so any decision would
        be a coin flip: it could spawn a second worker or delete a live one's
        files.
        """
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, model_state="idle")

        with patch.object(
            foundation_util, "live_worker_names", return_value=None
        ), patch.object(foundation_model, "_trigger_model_delete_background") as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 503
        assert "taosanodectl" in status["message"]
        assert not trigger.called
        assert dao.get_model("moirai").model_state == "idle"

    def test_refused_when_the_reconciled_state_cannot_be_written(self):
        """A repair that cannot be recorded leaves the state untrusted."""
        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.get_json.return_value = {"model_name": "moirai"}

        dao = get_dao()
        dao.update_model("moirai", downloaded=True, running=True, model_state="started")

        with patch.object(
            foundation_util, "live_worker_names", return_value=[]
        ), patch.object(
            dao, "update_model_if_state_unchanged", return_value=False
        ), patch.object(
            foundation_model, "_trigger_model_delete_background"
        ) as trigger:
            status, code = foundation_model.delete_foundation_model(mock_request)

        assert code == 503
        assert not trigger.called


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
        from lib.tsfm.mgmt.foundation_models_db import AsyncOperation, get_dao
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
        from lib.tsfm.mgmt.foundation_models_db import AsyncOperation, get_dao
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
        from lib.tsfm.mgmt.foundation_models_db import AsyncOperation, get_dao
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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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

        with patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
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
        ), patch("lib.tsfm.mgmt.foundation_model.subprocess.run") as mock_run:
            is_valid, model_info, error = foundation_model._extract_model_status_in_repository(
                "moirai", "1.0.0", "https://example.com"
            )

            assert not is_valid
            assert model_info is None
            assert "not found" in error.lower()
            mock_run.assert_not_called()
