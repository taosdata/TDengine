"""
Tests for ProcessManager class
"""
import pytest
import os
import platform
from unittest.mock import Mock, patch, MagicMock

IS_WINDOWS = platform.system().lower() == "windows"


class TestProcessManager:
    """Test cases for ProcessManager"""

    def test_read_pid_file_exists(self, process_manager, temp_dir):
        """Test reading PID from file when it exists"""
        pid_file = os.path.join(temp_dir, "test.pid")
        with open(pid_file, 'w') as f:
            f.write("1234")

        # Mock the _get_pid_file method to return our test file
        with patch.object(process_manager, '_get_pid_file', return_value=pid_file):
            pid = process_manager.read_pid("test")
            assert pid == 1234

    def test_read_pid_file_not_exists(self, process_manager):
        """Test reading PID when file doesn't exist"""
        pid = process_manager.read_pid("nonexistent")
        assert pid is None

    def test_read_pid_invalid_content(self, process_manager, temp_dir):
        """Test reading PID with invalid content"""
        pid_file = os.path.join(temp_dir, "invalid.pid")
        with open(pid_file, 'w') as f:
            f.write("not_a_number")

        with patch.object(process_manager, '_get_pid_file', return_value=pid_file):
            pid = process_manager.read_pid("invalid")
            assert pid is None

    def test_write_pid(self, process_manager, temp_dir):
        """Test writing PID to file"""
        pid_file = os.path.join(temp_dir, "write_test.pid")

        with patch.object(process_manager, '_get_pid_file', return_value=pid_file):
            process_manager.write_pid(5678, "write_test")

            assert os.path.exists(pid_file)
            with open(pid_file, 'r') as f:
                content = f.read()
            assert content == "5678"

    def test_remove_pid(self, process_manager, temp_dir):
        """Test removing PID file"""
        pid_file = os.path.join(temp_dir, "remove_test.pid")
        with open(pid_file, 'w') as f:
            f.write("9999")

        with patch.object(process_manager, '_get_pid_file', return_value=pid_file):
            assert os.path.exists(pid_file)
            process_manager.remove_pid("remove_test")
            assert not os.path.exists(pid_file)

    @pytest.mark.skipif(IS_WINDOWS, reason="Unix-only test")
    def test_is_running_unix_process_exists(self, process_manager):
        """Test checking if process is running on Unix (process exists)"""
        # Use current process PID which should exist
        current_pid = os.getpid()

        with patch.object(process_manager, 'read_pid', return_value=current_pid):
            result = process_manager.is_running("test")
            assert result is True

    @pytest.mark.skipif(IS_WINDOWS, reason="Unix-only test")
    def test_is_running_unix_process_not_exists(self, process_manager):
        """Test checking if process is running on Unix (process doesn't exist)"""
        # Use a very high PID that shouldn't exist
        fake_pid = 999999

        with patch.object(process_manager, 'read_pid', return_value=fake_pid):
            result = process_manager.is_running("test")
            assert result is False

    @pytest.mark.skipif(not IS_WINDOWS, reason="Windows-only test")
    def test_is_running_windows_process_exists(self, process_manager, mocker):
        """Test checking if process is running on Windows (process exists)"""
        mock_result = Mock()
        mock_result.stdout = "python.exe                    1234 Console                 1"
        mocker.patch('subprocess.run', return_value=mock_result)

        with patch.object(process_manager, 'read_pid', return_value=1234):
            result = process_manager.is_running("test")
            assert result is True

    @pytest.mark.skipif(not IS_WINDOWS, reason="Windows-only test")
    def test_is_running_windows_process_not_exists(self, process_manager, mocker):
        """Test checking if process is running on Windows (process doesn't exist)"""
        mock_result = Mock()
        mock_result.stdout = ""
        mocker.patch('subprocess.run', return_value=mock_result)

        with patch.object(process_manager, 'read_pid', return_value=99999):
            result = process_manager.is_running("test")
            assert result is False

    def test_wait_for_service_success(self, process_manager, mocker):
        """Test waiting for service to start - success case"""
        mocker.patch.object(process_manager, 'is_running', return_value=True)

        result = process_manager.wait_for_service("test", timeout=5)
        assert result is True

    def test_wait_for_service_timeout(self, process_manager, mocker):
        """Test waiting for service to start - timeout case"""
        mocker.patch.object(process_manager, 'is_running', return_value=False)

        result = process_manager.wait_for_service("test", timeout=1)
        assert result is False

    def test_wait_for_service_delayed_start(self, process_manager, mocker):
        """Test waiting for service that starts after a delay"""
        # First call returns False, subsequent calls return True
        mocker.patch.object(
            process_manager,
            'is_running',
            side_effect=[False, False, True]
        )

        result = process_manager.wait_for_service("test", timeout=5)
        assert result is True

    @pytest.mark.skipif(not IS_WINDOWS, reason="Windows-only test")
    def test_kill_process_windows_graceful(self, process_manager, mocker):
        """Test graceful process termination on Windows"""
        mock_run = mocker.patch('subprocess.run')

        process_manager.kill_process(1234, force=False)

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "taskkill" in args
        assert "/PID" in args
        assert "1234" in args
        assert "/F" not in args  # No force flag

    @pytest.mark.skipif(not IS_WINDOWS, reason="Windows-only test")
    def test_kill_process_windows_force(self, process_manager, mocker):
        """Test forceful process termination on Windows"""
        mock_run = mocker.patch('subprocess.run')

        process_manager.kill_process(1234, force=True)

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "taskkill" in args
        assert "/F" in args  # Force flag present

    @pytest.mark.skipif(IS_WINDOWS, reason="Unix-only test")
    def test_kill_process_unix_graceful(self, process_manager, mocker):
        """Test graceful process termination on Unix"""
        import signal
        mock_kill = mocker.patch('os.kill')

        process_manager.kill_process(1234, force=False)

        mock_kill.assert_called_once_with(1234, signal.SIGTERM)

    @pytest.mark.skipif(IS_WINDOWS, reason="Unix-only test")
    def test_kill_process_unix_force(self, process_manager, mocker):
        """Test forceful process termination on Unix"""
        import signal
        mock_kill = mocker.patch('os.kill')

        process_manager.kill_process(1234, force=True)

        mock_kill.assert_called_once_with(1234, signal.SIGKILL)
