"""
Tests for ModelService class
"""
import pytest
import os
from unittest.mock import Mock, patch, MagicMock


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
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=True
        )

        result = model_service.start("tdtsfm")
        assert result is True

    def test_start_required_model_missing(self, model_service, mocker):
        """Test starting required model when directory is missing"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch('os.path.exists', return_value=False)

        result = model_service.start("tdtsfm")
        assert result is True

    def test_start_optional_model_missing(self, model_service, mocker):
        """Test starting optional model when directory is missing"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=False
        )

        # Mock os.path.exists to return False for model_dir but True for others
        def exists_side_effect(path):
            if "model" in path and "chronos" in path:
                return False
            return True

        mocker.patch('os.path.exists', side_effect=exists_side_effect)

        result = model_service.start("chronos")
        assert result is True  # Should skip optional model

    def test_start_model_success(self, model_service, mocker):
        """Test successful model start"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            model_service.process_mgr,
            '_get_python_exe',
            return_value='/usr/bin/python3'
        )
        mocker.patch.object(
            model_service.process_mgr,
            'wait_for_service',
            return_value=True
        )
        mocker.patch.object(
            model_service.process_mgr,
            'read_pid',
            return_value=5678
        )
        mocker.patch.object(
            model_service.process_mgr,
            'write_pid'
        )
        mocker.patch('os.path.exists', return_value=True)
        mocker.patch('subprocess.Popen')
        mocker.patch('builtins.open', create=True)

        result = model_service.start("tdtsfm")
        assert result is True

    def test_stop_unknown_model(self, model_service):
        """Test stopping unknown model"""
        result = model_service.stop("unknown_model")
        assert result is False

    def test_stop_model_not_running(self, model_service, mocker):
        """Test stopping model when it's not running"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            model_service.process_mgr,
            'remove_pid'
        )

        result = model_service.stop("tdtsfm")
        assert result is True

    def test_stop_model_success(self, model_service, mocker):
        """Test successful model stop"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            side_effect=[True, False]  # Running initially, then stopped
        )
        mocker.patch.object(
            model_service.process_mgr,
            'read_pid',
            return_value=5678
        )
        mocker.patch.object(
            model_service.process_mgr,
            'kill_process'
        )
        mocker.patch.object(
            model_service.process_mgr,
            'remove_pid'
        )

        result = model_service.stop("tdtsfm")
        assert result is True

    def test_start_all_models(self, model_service, mocker):
        """Test starting all models"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            model_service.process_mgr,
            '_get_python_exe',
            return_value='/usr/bin/python3'
        )
        mocker.patch.object(
            model_service.process_mgr,
            'wait_for_service',
            return_value=True
        )
        mocker.patch.object(
            model_service.process_mgr,
            'read_pid',
            return_value=5678
        )
        mocker.patch.object(
            model_service.process_mgr,
            'write_pid'
        )
        mocker.patch('os.path.exists', return_value=True)
        mocker.patch('subprocess.Popen')
        mocker.patch('builtins.open', create=True)

        result = model_service.start("all")
        assert result is True

    def test_stop_all_models(self, model_service, mocker):
        """Test stopping all models"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            model_service.process_mgr,
            'remove_pid'
        )

        result = model_service.stop("all")
        assert result is True

    def test_status(self, model_service, mocker):
        """Test getting model status"""
        mocker.patch.object(
            model_service.process_mgr,
            'is_running',
            side_effect=[True, False, True, False, False, False]  # Status for each model
        )
        mocker.patch.object(
            model_service.process_mgr,
            'read_pid',
            side_effect=[1111, None, 2222, None, None, None]
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

    def test_get_model_venv(self, model_service, mock_config):
        """Test getting model virtual environment path"""
        venv = model_service._get_model_venv("timesfm")
        assert venv == mock_config.timesfm_venv

        venv = model_service._get_model_venv("moirai")
        assert venv == mock_config.moirai_venv

        venv = model_service._get_model_venv("chronos")
        assert venv == mock_config.chronos_venv

        venv = model_service._get_model_venv("moment")
        assert venv == mock_config.moment_venv

        # Default venv for unknown models
        venv = model_service._get_model_venv("tdtsfm")
        assert venv == mock_config.venv_dir

    def test_build_model_args(self, model_service):
        """Test building model arguments"""
        args = model_service._build_model_args("tdtsfm")
        assert args == []

        args = model_service._build_model_args("timemoe")
        assert "--model-folder" in args
        assert "--model-name" in args
        assert "--port" in args

    @pytest.mark.parametrize("model_name", ["moirai", "chronos", "timesfm", "moment"])
    def test_build_model_args_contract_for_optional_models(self, model_service, mock_config, model_name):
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

