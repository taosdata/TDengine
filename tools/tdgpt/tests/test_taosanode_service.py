"""
Tests for TaosanodeService class
"""
import pytest
import os
from unittest.mock import Mock, patch, MagicMock


class TestTaosanodeService:
    """Test cases for TaosanodeService"""

    def test_service_init(self, taosanode_service, mock_config):
        """Test TaosanodeService initialization"""
        assert taosanode_service.config == mock_config
        assert taosanode_service.process_mgr is not None
        assert taosanode_service.logger is not None

    def test_start_already_running(self, taosanode_service, mocker):
        """Test starting service when it's already running"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            return_value=True
        )

        result = taosanode_service.start()
        assert result is True

    def test_start_success(self, taosanode_service, mocker):
        """Test successful service start"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            '_get_python_exe',
            return_value='/usr/bin/python3'
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'wait_for_service',
            return_value=True
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'read_pid',
            return_value=1234
        )
        mocker.patch('subprocess.Popen')

        result = taosanode_service.start()
        assert result is True

    def test_start_timeout(self, taosanode_service, mocker):
        """Test service start timeout"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            '_get_python_exe',
            return_value='/usr/bin/python3'
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'wait_for_service',
            return_value=False
        )
        mocker.patch('subprocess.Popen')

        result = taosanode_service.start()
        assert result is False

    def test_stop_not_running(self, taosanode_service, mocker):
        """Test stopping service when it's not running"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'remove_pid'
        )

        result = taosanode_service.stop()
        assert result is True

    def test_stop_success(self, taosanode_service, mocker):
        """Test successful service stop"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            side_effect=[True, False]  # Running initially, then stopped
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'read_pid',
            return_value=1234
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'kill_process'
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'remove_pid'
        )

        result = taosanode_service.stop()
        assert result is True

    def test_status(self, taosanode_service, mocker):
        """Test getting service status"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            return_value=True
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            'read_pid',
            return_value=1234
        )

        status = taosanode_service.status()
        assert status["service"] == "taosanode"
        assert status["running"] is True
        assert status["pid"] == 1234
        assert status["bind"] == "0.0.0.0:6035"

    def test_start_with_exception(self, taosanode_service, mocker):
        """Test service start with exception"""
        mocker.patch.object(
            taosanode_service.process_mgr,
            'is_running',
            return_value=False
        )
        mocker.patch.object(
            taosanode_service.process_mgr,
            '_get_python_exe',
            side_effect=FileNotFoundError("Python not found")
        )

        result = taosanode_service.start()
        assert result is False
