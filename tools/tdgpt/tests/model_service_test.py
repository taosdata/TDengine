# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""
Tests for ModelService class
"""

import os
from types import SimpleNamespace

import pytest
from taosanode_service import EXIT_CONFLICT, execute_command


def _complete_model(mock_config, model_name="tdtsfm"):
    model_dir = os.path.join(mock_config.tsfm_model_dir, model_name)
    os.makedirs(model_dir, exist_ok=True)
    marker = "taos.pth" if model_name == "tdtsfm" else "model.safetensors"
    open(os.path.join(model_dir, marker), "wb").write(b"weights")
    if model_name != "tdtsfm":
        open(os.path.join(model_dir, "config.json"), "w", encoding="utf-8").write("{}")


class TestModelService:
    """Test cases for ModelService"""

    def test_service_init(self, model_service, mock_config):
        """Test ModelService initialization"""
        assert model_service.config == mock_config
        assert model_service.process_mgr is not None
        assert model_service.logger is not None

    def test_start_unknown_model(self, model_service, mocker):
        """Test starting unknown model"""
        result = model_service.start("unknown_model")
        assert result is False

    def test_start_model_already_running(self, model_service, mocker):
        """Test starting model when it's already running"""
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=True)

        result = model_service.start("tdtsfm")
        assert result is True

    def test_start_required_model_missing(self, model_service, mocker):
        """Test starting required model when directory is missing"""
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch("os.path.exists", return_value=False)

        result = model_service.start("tdtsfm")
        assert result is True

    def test_start_optional_model_missing(self, model_service, mocker):
        """Test starting optional model when directory is missing"""
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)

        # Mock os.path.exists to return False for model_dir but True for others
        def exists_side_effect(path):
            if "model" in path and "chronos" in path:
                return False
            return True

        mocker.patch("os.path.exists", side_effect=exists_side_effect)

        result = model_service.start("chronos")
        assert result is True  # Should skip optional model

    def test_legacy_model_directory_is_not_started(self, model_service, mock_config, mocker):
        os.makedirs(os.path.join(mock_config.model_dir, "tdtsfm"), exist_ok=True)
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        assert model_service.start("tdtsfm") is True
        assert model_service._model_directory_exists("tdtsfm") is False

    def test_model_with_files_but_missing_runtime_is_not_started(
        self, model_service, mock_config, mocker
    ):
        _complete_model(mock_config)
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(model_service, "_get_dynamic_tsfm_runtime", return_value=None)
        assert model_service.start("tdtsfm") is False

    def test_start_model_success(self, model_service, mock_config, mocker):
        """Test successful model start"""
        _complete_model(mock_config)
        mocker.patch.object(model_service, "_get_dynamic_tsfm_runtime", return_value=({}, "/usr/bin/python3"))
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(
            model_service.process_mgr,
            "_get_python_exe",
            return_value="/usr/bin/python3",
        )
        mocker.patch.object(
            model_service.process_mgr, "wait_for_service", return_value=True
        )
        wait_for_tcp_port = mocker.patch.object(
            model_service.process_mgr, "wait_for_tcp_port", return_value=True
        )
        mocker.patch.object(model_service.process_mgr, "read_pid", return_value=5678)
        mocker.patch.object(model_service.process_mgr, "write_pid")
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.Popen")
        mocker.patch("builtins.open", create=True)

        result = model_service.start("tdtsfm")
        assert result is True
        wait_for_tcp_port.assert_called_once_with(
            "model-tdtsfm", "127.0.0.1", 6061, timeout=300
        )

    def test_start_model_timeout_override(self, model_service, mock_config, mocker):
        _complete_model(mock_config)
        mocker.patch.object(model_service, "_get_dynamic_tsfm_runtime", return_value=({}, "/usr/bin/python3"))
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(
            model_service.process_mgr,
            "_get_python_exe",
            return_value="/usr/bin/python3",
        )
        wait_for_tcp_port = mocker.patch.object(
            model_service.process_mgr, "wait_for_tcp_port", return_value=True
        )
        mocker.patch.object(model_service.process_mgr, "read_pid", return_value=5678)
        mocker.patch.object(model_service.process_mgr, "write_pid")
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.Popen")
        mocker.patch("builtins.open", create=True)

        assert model_service.start("tdtsfm", timeout=300) is True
        wait_for_tcp_port.assert_called_once_with(
            "model-tdtsfm", "127.0.0.1", 6061, timeout=300
        )

    def test_start_model_timeout_reports_diagnostics(self, model_service, mock_config, mocker):
        _complete_model(mock_config)
        mocker.patch.object(model_service, "_get_dynamic_tsfm_runtime", return_value=({}, "/usr/bin/python3"))
        mocker.patch.object(
            model_service.process_mgr, "is_running", side_effect=[False, True]
        )
        mocker.patch.object(
            model_service.process_mgr,
            "_get_python_exe",
            return_value="/usr/bin/python3",
        )
        mocker.patch.object(
            model_service.process_mgr, "wait_for_tcp_port", return_value=False
        )
        mocker.patch.object(model_service.process_mgr, "write_pid")
        mocker.patch.object(model_service, "stop", return_value=True)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.Popen")
        mocker.patch("builtins.open", create=True)
        log_error = mocker.patch.object(model_service.logger, "error")

        assert model_service.start("tdtsfm") is False
        messages = [str(call.args[0]) for call in log_error.call_args_list]
        assert any("Model startup timed out after 300 seconds" in message for message in messages)
        assert any("model_tdtsfm.log" in message for message in messages)

    def test_dynamic_tdtsfm_worker_gets_dynamic_model_root(
        self, model_service, mock_config, mocker
    ):
        model_dir = os.path.join(mock_config.tsfm_model_dir, "tdtsfm")
        os.makedirs(model_dir, exist_ok=True)
        service_dir = os.path.join(
            mock_config.install_dir, "lib", "tsfm", "service"
        )
        os.makedirs(service_dir, exist_ok=True)
        open(os.path.join(service_dir, "tdtsfm_server.py"), "a", encoding="utf-8").close()
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(model_service.process_mgr, "wait_for_service", return_value=True)
        mocker.patch.object(model_service.process_mgr, "wait_for_tcp_port", return_value=True)
        mocker.patch.object(model_service.process_mgr, "read_pid", return_value=5678)
        mocker.patch.object(model_service.process_mgr, "write_pid")
        mocker.patch.object(
            model_service,
            "_resolve_dynamic_model",
            return_value=(model_dir, "/shared/venv/bin/python3"),
        )
        popen = mocker.patch("subprocess.Popen")

        assert model_service.start("tdtsfm") is True
        assert popen.call_args.kwargs["env"]["TDGPT_MODEL_DIR"] == mock_config.tsfm_model_dir

    def test_stop_unknown_model(self, model_service):
        """Test stopping unknown model"""
        result = model_service.stop("unknown_model")
        assert result is False

    def test_stop_model_not_running(self, model_service, mocker):
        """Test stopping model when it's not running"""
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(model_service.process_mgr, "remove_pid")

        result = model_service.stop("tdtsfm")
        assert result is True

    def test_stop_model_success(self, model_service, mocker):
        """Test successful model stop"""
        mocker.patch.object(
            model_service.process_mgr,
            "is_running",
            side_effect=[True, False],  # Running initially, then stopped
        )
        mocker.patch.object(model_service.process_mgr, "read_pid", return_value=5678)
        mocker.patch.object(model_service.process_mgr, "kill_process")
        mocker.patch.object(model_service.process_mgr, "remove_pid")

        result = model_service.stop("tdtsfm")
        assert result is True

    def test_start_all_models(self, model_service, mocker):
        """Test starting all models"""
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(
            model_service.process_mgr,
            "_get_python_exe",
            return_value="/usr/bin/python3",
        )
        mocker.patch.object(
            model_service.process_mgr, "wait_for_service", return_value=True
        )
        mocker.patch.object(
            model_service.process_mgr, "wait_for_tcp_port", return_value=True
        )
        mocker.patch.object(model_service.process_mgr, "read_pid", return_value=5678)
        mocker.patch.object(model_service.process_mgr, "write_pid")
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.Popen")
        mocker.patch("builtins.open", create=True)

        result = model_service.start("all")
        assert result is True

    def test_stop_all_models(self, model_service, mocker):
        """Test stopping all models"""
        mocker.patch.object(model_service.process_mgr, "is_running", return_value=False)
        mocker.patch.object(model_service.process_mgr, "remove_pid")

        result = model_service.stop("all")
        assert result is True

    def test_status(self, model_service, mocker):
        """Test getting model status"""
        mocker.patch.object(
            model_service.process_mgr,
            "is_running",
            side_effect=[
                True,
                False,
                True,
                False,
                False,
                False,
            ],  # Status for each model
        )
        mocker.patch.object(
            model_service.process_mgr,
            "read_pid",
            side_effect=[1111, None, 2222, None, None, None],
        )

        status = model_service.status()
        assert len(status) == 6  # 6 models
        assert status[0]["model"] == "tdtsfm"
        assert status[0]["running"] is True
        assert status[0]["pid"] == 1111
        assert status[0]["port"] == 6061
        assert status[1]["port"] == 6062
        assert status[2]["port"] == 6064
        assert status[3]["port"] == 6063
        assert status[4]["port"] == 6065
        assert status[5]["port"] == 6066

    def test_delete_runtime_alias_rejects_running_model(
        self, mock_config, mocker
    ):
        models = mocker.patch("taosanode_service.ModelService").return_value
        models.status.return_value = [
            {"model": "moment", "running": True},
        ]
        manager = mocker.patch(
            "taosanode_service.DynamicResourceManager"
        ).return_value

        with pytest.raises(SystemExit) as exc:
            execute_command(
                SimpleNamespace(command="resource-delete", target="runtime"),
                mock_config,
            )

        assert exc.value.code == EXIT_CONFLICT
        manager.delete_resource.assert_not_called()

    def test_get_model_venv(self, model_service, mock_config):
        """Test getting model virtual environment path"""
        venv = model_service._get_model_venv()
        assert venv == mock_config.venv_dir

        venv = model_service._get_model_venv()
        assert venv == mock_config.venv_dir

        venv = model_service._get_model_venv()
        assert venv == mock_config.venv_dir

        venv = model_service._get_model_venv()
        assert venv == mock_config.venv_dir

        # All supported model servers use the unified venv.
        venv = model_service._get_model_venv()
        assert venv == mock_config.venv_dir

    def test_dynamic_tsfm_model_uses_shared_runtime(
        self, model_service, mock_config, mocker
    ):
        model_dir = os.path.join(mock_config.tsfm_model_dir, "moirai")
        os.makedirs(model_dir, exist_ok=True)
        runtime_manifest = {"runtime_id": "tsfm-runtime-1.0.0"}
        mocker.patch.object(
            model_service,
            "_get_dynamic_tsfm_runtime",
            return_value=(runtime_manifest, "/shared/venv/bin/python3"),
        )
        validate = mocker.patch(
            "taosanode_service.validate_tsfm_model", return_value={}
        )

        resolved = model_service._resolve_dynamic_model("moirai")

        assert resolved == (model_dir, "/shared/venv/bin/python3")
        validate.assert_called_once()

    def test_dynamic_tsfm_model_skips_when_runtime_is_missing(
        self, model_service, mock_config, mocker
    ):
        model_dir = os.path.join(mock_config.tsfm_model_dir, "chronos")
        os.makedirs(model_dir, exist_ok=True)
        mocker.patch.object(
            model_service, "_get_dynamic_tsfm_runtime", return_value=None
        )

        assert model_service._resolve_dynamic_model("chronos") is False

    def test_dynamic_model_directory_is_available(self, model_service, mock_config):
        model_dir = os.path.join(mock_config.tsfm_model_dir, "moment")
        os.makedirs(model_dir, exist_ok=True)
        open(os.path.join(model_dir, "model.safetensors"), "wb").write(b"weights")
        open(os.path.join(model_dir, "config.json"), "w", encoding="utf-8").write("{}")

        assert model_service._model_directory_exists("moment") is True

    def test_build_model_args(self, model_service, mock_config):
        """tdtsfm must be told where its weights are; the others also get name/port."""
        dynamic_dir = os.path.join(mock_config.tsfm_model_dir, "tdtsfm")
        os.makedirs(dynamic_dir, exist_ok=True)
        expected = [
            "--model-folder",
            dynamic_dir,
            "--port",
            str(mock_config.models["tdtsfm"]["port"]),
        ]

        # Without a caller-supplied directory the dynamic layout is what the
        # installer creates, so that is what must be passed on.
        assert model_service._build_model_args("tdtsfm") == expected
        # A directory the caller already resolved wins over the default.
        assert model_service._build_model_args("tdtsfm", dynamic_dir) == expected

        args = model_service._build_model_args("timemoe")
        assert "--model-folder" in args
        assert "--model-name" in args
        assert "--port" in args

    @pytest.mark.parametrize("model_name", ["moirai", "chronos", "timesfm", "moment"])
    def test_build_model_args_contract_for_optional_models(
        self, model_service, mock_config, model_name
    ):
        """Validate full startup arg contract for optional forecast model servers."""
        model_cfg = mock_config.models[model_name]
        expected = [
            "--model-folder",
            os.path.join(mock_config.model_dir, model_name),
            "--model-name",
            model_cfg["default_model"],
            "--port",
            str(model_cfg["port"]),
        ]

        assert model_service._build_model_args(model_name) == expected
