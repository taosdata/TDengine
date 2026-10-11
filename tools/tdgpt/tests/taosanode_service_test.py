# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Tests for TaosanodeService Windows startup behavior."""

import errno
from unittest.mock import MagicMock, patch

import taosanode_service as service_module


class TestTaosanodeService:
    """Targeted tests for startup diagnostics and basic lifecycle helpers."""

    def test_service_init(self, taosanode_service, mock_config):
        assert taosanode_service.config == mock_config
        assert taosanode_service.process_mgr is not None
        assert taosanode_service.logger is not None

    def test_status(self, taosanode_service):
        with patch.object(
            taosanode_service.process_mgr, "is_running", return_value=True
        ), patch.object(taosanode_service.process_mgr, "read_pid", return_value=1234):
            status = taosanode_service.status()

        assert status["service"] == "taosanode"
        assert status["running"] is True
        assert status["pid"] == 1234
        assert status["bind"] == "0.0.0.0:6035"

    def test_stop_not_running(self, taosanode_service):
        with patch.object(
            taosanode_service.process_mgr, "is_running", return_value=False
        ), patch.object(taosanode_service.process_mgr, "remove_pid") as remove_pid:
            assert taosanode_service.stop() is True
        remove_pid.assert_called_once_with("taosanode")

    def test_stop_success(self, taosanode_service):
        with patch.object(
            taosanode_service.process_mgr, "is_running", side_effect=[True, False]
        ), patch.object(
            taosanode_service.process_mgr, "read_pid", return_value=1234
        ), patch.object(
            taosanode_service.process_mgr, "kill_process"
        ) as kill_process, patch.object(
            taosanode_service.process_mgr, "remove_pid"
        ) as remove_pid:
            assert taosanode_service.stop() is True
        kill_process.assert_called_once_with(1234, force=False)
        remove_pid.assert_called_once_with("taosanode")

    def test_default_preflight_mode_is_off(self, taosanode_service, monkeypatch):
        monkeypatch.delenv("TAOSANODE_PREFLIGHT_MODE", raising=False)
        monkeypatch.delenv("TAOSANODE_FULL_PREFLIGHT", raising=False)
        monkeypatch.delenv("TAOSANODE_LIGHT_PREFLIGHT", raising=False)

        assert taosanode_service._get_requested_preflight_mode() == "off"

    def test_light_preflight_env_warns_and_maps_to_off(
        self, taosanode_service, monkeypatch
    ):
        monkeypatch.setenv("TAOSANODE_PREFLIGHT_MODE", "light")
        monkeypatch.delenv("TAOSANODE_FULL_PREFLIGHT", raising=False)
        monkeypatch.delenv("TAOSANODE_LIGHT_PREFLIGHT", raising=False)
        with patch.object(taosanode_service.logger, "warning") as warning:
            assert taosanode_service._get_requested_preflight_mode() == "off"
        warning.assert_called_once()

    def test_import_failure_triggers_full_diagnostics(self, taosanode_service):
        with patch.object(
            taosanode_service, "_collect_startup_failure_diagnostics"
        ) as collect:
            failure_kind = taosanode_service._handle_startup_failure(
                "python.exe",
                {},
                taosanode_service.config.install_dir,
                phase="import",
                reason="foreground import/startup failure",
                exc=ImportError("No module named waitress"),
            )

        assert failure_kind == "import_native"
        collect.assert_called_once()

    def test_bind_failure_does_not_trigger_full_diagnostics(self, taosanode_service):
        with patch.object(
            taosanode_service, "_collect_startup_failure_diagnostics"
        ) as collect:
            failure_kind = taosanode_service._handle_startup_failure(
                "python.exe",
                {},
                taosanode_service.config.install_dir,
                phase="serve",
                reason="foreground serve failure",
                exc=OSError(errno.EADDRINUSE, "Address already in use"),
            )

        assert failure_kind == "bind"
        collect.assert_not_called()

    def test_background_unknown_failure_triggers_full_diagnostics(
        self, taosanode_service
    ):
        with patch.object(
            taosanode_service, "_collect_startup_failure_diagnostics"
        ) as collect:
            failure_kind = taosanode_service._handle_startup_failure(
                "python.exe",
                {},
                taosanode_service.config.install_dir,
                phase="background",
                reason="background child exited early with code 3221225477",
            )

        assert failure_kind == "unknown"
        collect.assert_called_once()

    def test_access_violation_log_is_classified_as_native_failure(
        self, taosanode_service
    ):
        with patch.object(
            taosanode_service, "_collect_startup_failure_diagnostics"
        ) as collect:
            failure_kind = taosanode_service._handle_startup_failure(
                "python.exe",
                {},
                taosanode_service.config.install_dir,
                phase="background",
                reason="background child exited early",
                detail_text="Windows fatal exception: access violation\nexit code 0xc0000005",
            )

        assert failure_kind == "import_native"
        collect.assert_called_once()

    def test_startup_diagnostic_cooldown_suppresses_duplicate_runs(
        self, taosanode_service, monkeypatch
    ):
        monkeypatch.setenv("TAOSANODE_STARTUP_DIAGNOSTIC_COOLDOWN", "300")
        with patch.object(
            taosanode_service, "_collect_preflight_diagnostics"
        ) as collect:
            taosanode_service._collect_startup_failure_diagnostics(
                "python.exe",
                {},
                taosanode_service.config.install_dir,
                reason="first import failure",
            )
            taosanode_service._collect_startup_failure_diagnostics(
                "python.exe",
                {},
                taosanode_service.config.install_dir,
                reason="second import failure",
            )

        assert collect.call_count == 1

    def test_wait_ready_tolerates_windows_service_pending_state(
        self, taosanode_service
    ):
        response = MagicMock()
        response.status = 200
        response.read.return_value = b"ready"
        response_ctx = MagicMock()
        response_ctx.__enter__.return_value = response
        response_ctx.__exit__.return_value = False

        with patch.object(
            service_module.urllib.request,
            "urlopen",
            side_effect=[
                RuntimeError("not ready"),
                RuntimeError("still starting"),
                response_ctx,
            ],
        ), patch.object(
            taosanode_service.process_mgr, "is_running", return_value=False
        ), patch.object(
            taosanode_service,
            "_query_windows_service_state",
            side_effect=["START_PENDING", "RUNNING"],
        ), patch.object(
            service_module.time, "sleep", return_value=None
        ):
            assert taosanode_service.wait_ready(timeout=2, interval=0.01) is True


class TestResourceVersionDisplay:
    def test_list_displays_model_and_resource_versions(self, capsys):
        models = [
            {"name": "moirai", "model_version": "moirai-moe-1.0-r-small", "version": "1.0.0"},
            {"name": "timesfm", "model_version": "timesfm-2.5-200m-pytorch", "version": "1.0.0"},
            {"name": "tdtsfm", "model_version": "tdtsfm-v1.0", "version": "1.0.0"},
        ]
        service_module.print_resource_table({"models": models})
        lines = capsys.readouterr().out.splitlines()
        headers = lines[0].split("  ")
        assert "MODEL_VERSION" in headers
        assert "RESOURCE_VERSION" in headers
        assert "VERSION" not in headers
        for item, line in zip(models, lines[2:]):
            row = dict(zip(headers, line.split("  ")))
            assert row["NAME"] == item["name"]
            assert row["MODEL_VERSION"] == item["model_version"]
            assert row["RESOURCE_VERSION"] == "1.0.0"

    def test_check_displays_release_model_version_and_runtime_placeholder(self, capsys):
        service_module.print_resource_table({
            "release": "1.0.0",
            "resources": [
                {"kind": "model", "name": "moirai", "state": "missing",
                 "model_version": "moirai-moe-1.0-r-small", "version": "1.0.0"},
                {"kind": "runtime", "name": "tsfm_runtime", "state": "installed",
                 "model_version": None, "version": "1.0.0"},
            ],
        }, check=True)
        lines = capsys.readouterr().out.splitlines()
        assert lines[0] == "Release: 1.0.0"
        headers = lines[1].split("  ")
        model, runtime = [dict(zip(headers, line.split("  "))) for line in lines[3:]]
        assert model["MODEL_VERSION"] == "moirai-moe-1.0-r-small"
        assert model["RESOURCE_VERSION"] == "1.0.0"
        assert runtime["MODEL_VERSION"] == "-"
        assert runtime["RESOURCE_VERSION"] == "1.0.0"

    def test_missing_model_version_is_unknown(self, capsys):
        for value in ({}, {"model_version": None}, {"model_version": ""}):
            item = {"name": "legacy", "version": "1.0.0", **value}
            for check in (False, True):
                service_module.print_resource_table({"resources" if check else "models": [item]}, check=check)
                lines = capsys.readouterr().out.splitlines()
                headers = lines[1 if check else 0].split("  ")
                row = dict(zip(headers, lines[-1].split("  ")))
                assert row["MODEL_VERSION"] == "unknown"
                assert row["RESOURCE_VERSION"] == "1.0.0"
