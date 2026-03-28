"""
Tests for Config class
"""
import pytest
import os
import tempfile
import shutil


class TestConfig:
    """Test cases for Config class"""

    def test_config_default_values(self, mock_config):
        """Test that Config has default values"""
        assert mock_config.bind == "0.0.0.0:6035"
        assert mock_config.workers == 2
        assert mock_config.models is not None
        assert len(mock_config.models) > 0

    def test_config_required_models(self, mock_config):
        """Test that required models are marked correctly"""
        assert mock_config.models["tdtsfm"]["required"] is True
        assert mock_config.models["timemoe"]["required"] is True

    def test_config_optional_models(self, mock_config):
        """Test that optional models are marked correctly"""
        assert mock_config.models["chronos"]["required"] is False
        assert mock_config.models["timesfm"]["required"] is False
        assert mock_config.models["moirai"]["required"] is False
        assert mock_config.models["moment"]["required"] is False

    def test_config_model_scripts(self, mock_config):
        """Test that model scripts are configured"""
        assert mock_config.models["tdtsfm"]["script"] == "tdtsfm-server.py"
        assert mock_config.models["timemoe"]["script"] == "timemoe-server.py"
        assert mock_config.models["chronos"]["script"] == "chronos-server.py"

    def test_config_model_ports(self, mock_config):
        """Test that model ports are configured"""
        assert mock_config.models["tdtsfm"]["port"] == 6061
        assert mock_config.models["timemoe"]["port"] == 6062

    def test_config_paths_created(self, mock_config):
        """Test that config creates necessary paths"""
        assert os.path.exists(mock_config.log_dir)
        assert os.path.exists(mock_config.data_dir)
        assert os.path.exists(mock_config.cfg_dir)
        assert os.path.exists(mock_config.model_dir)

    def test_config_with_custom_path(self, temp_dir):
        """Test Config with custom configuration file"""
        from taosanode_service import Config

        # Create a custom config file
        config_file = os.path.join(temp_dir, "custom.config.py")
        with open(config_file, 'w') as f:
            f.write("""
bind = '127.0.0.1:8080'
workers = 4
models = {
    "tdtsfm": {"script": "tdtsfm-server.py", "port": 6061, "required": True},
    "timemoe": {"script": "timemoe-server.py", "port": 6062, "required": True},
}
""")

        config = Config(config_file)
        assert config.bind == "127.0.0.1:8080"
        assert config.workers == 4

    def test_config_fallback_on_missing_file(self, temp_dir):
        """Test Config fallback when config file doesn't exist"""
        from taosanode_service import Config

        config = Config(os.path.join(temp_dir, "nonexistent.py"))
        # Should use defaults
        assert config.bind == "0.0.0.0:6035"
        assert config.workers == 2

    def test_config_model_default_models(self, mock_config):
        """Test that default models are loaded"""
        default_models = mock_config._get_default_models()
        assert "tdtsfm" in default_models
        assert "timemoe" in default_models
        assert "chronos" in default_models
        assert "timesfm" in default_models
        assert "moirai" in default_models
        assert "moment" in default_models

    def test_config_paths_are_strings(self, mock_config):
        """Test that all config paths are strings"""
        assert isinstance(mock_config.install_dir, str)
        assert isinstance(mock_config.log_dir, str)
        assert isinstance(mock_config.data_dir, str)
        assert isinstance(mock_config.cfg_dir, str)
        assert isinstance(mock_config.pid_file, str)
        assert isinstance(mock_config.app_log, str)
        assert isinstance(mock_config.model_dir, str)
        assert isinstance(mock_config.venv_dir, str)
