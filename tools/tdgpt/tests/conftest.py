"""
Pytest configuration and shared fixtures for TDGPT tests
"""
import pytest
import tempfile
import shutil
import os
import sys
from unittest import mock

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'script'))

from taosanode_service import Config, ProcessManager, TaosanodeService, ModelService


class _SimpleMocker:
    """Minimal pytest-mock compatible helper for this test suite."""

    Mock = mock.Mock
    MagicMock = mock.MagicMock
    call = mock.call
    ANY = mock.ANY

    class _PatchProxy:
        def __init__(self, owner):
            self._owner = owner

        def __call__(self, target, *args, **kwargs):
            return self._owner._start(mock.patch(target, *args, **kwargs))

        def object(self, target, attribute, *args, **kwargs):
            return self._owner._start(mock.patch.object(target, attribute, *args, **kwargs))

    def __init__(self):
        self._patchers = []
        self.patch = self._PatchProxy(self)

    def _start(self, patcher):
        started = patcher.start()
        self._patchers.append(patcher)
        return started

    def stopall(self):
        while self._patchers:
            self._patchers.pop().stop()


@pytest.fixture
def mocker():
    helper = _SimpleMocker()
    try:
        yield helper
    finally:
        helper.stopall()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def mock_config(temp_dir):
    """Create a mock Config instance with temporary directories"""
    config = Config()
    config.install_dir = temp_dir
    config.log_dir = os.path.join(temp_dir, "log")
    config.data_dir = os.path.join(temp_dir, "data")
    config.cfg_dir = os.path.join(temp_dir, "cfg")
    config.pid_file = os.path.join(temp_dir, "taosanode.pid")
    config.app_log = os.path.join(config.log_dir, "taosanode.app.log")
    config.model_dir = os.path.join(config.data_dir, "model")
    config.venv_dir = os.path.join(temp_dir, "venvs", "venv")
    config.timesfm_venv = os.path.join(temp_dir, "venvs", "timesfm_venv")
    config.moirai_venv = os.path.join(temp_dir, "venvs", "moirai_venv")
    config.chronos_venv = os.path.join(temp_dir, "venvs", "chronos_venv")
    config.moment_venv = os.path.join(temp_dir, "venvs", "momentfm_venv")
    config.bind = "0.0.0.0:6035"
    config.workers = 2

    # Create necessary directories
    os.makedirs(config.log_dir, exist_ok=True)
    os.makedirs(config.data_dir, exist_ok=True)
    os.makedirs(config.cfg_dir, exist_ok=True)
    os.makedirs(config.model_dir, exist_ok=True)
    os.makedirs(os.path.dirname(config.venv_dir), exist_ok=True)

    return config


@pytest.fixture
def process_manager(mock_config):
    """Create a ProcessManager instance with mock config"""
    return ProcessManager(mock_config)


@pytest.fixture
def taosanode_service(mock_config, process_manager):
    """Create a TaosanodeService instance"""
    return TaosanodeService(mock_config, process_manager)


@pytest.fixture
def model_service(mock_config, process_manager):
    """Create a ModelService instance"""
    return ModelService(mock_config, process_manager)
