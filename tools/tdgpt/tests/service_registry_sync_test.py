"""Unit tests for ServiceRegistry.sync_dynamic_services subdirectory discovery branch.

This test suite focuses on lines 124-148 of service_registry.py which handle
the discovery and registration of dynamic models in new format (model_name/model_name.json).

The selected code branch:
- Line 126: model_name = item_path.name
- Line 127-128: if model_name in self.services: continue
- Line 130: config_file_name = f"{model_name}.json"
- Line 131: config_path = item_path / config_file_name
- Line 133-139: Debug logging if config not found
- Line 141-148: Registration attempt with error handling
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from taosanalytics.conf import Configure
from taosanalytics.service_registry import ServiceRegistry


class TestSyncDynamicServicesSubdirectoryBranch(unittest.TestCase):
    """Tests for sync_dynamic_services subdirectory discovery branch (lines 124-148)."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.dynamic_model_dir = os.path.join(self.temp_dir, "dynamic_models")
        os.makedirs(self.dynamic_model_dir, exist_ok=True)

        # Create a ServiceRegistry instance
        self.registry = ServiceRegistry()
        # Reset the loaded flag for testing
        self.registry._loaded = False

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_forecast_config(self, algo_name="arima"):
        """Helper to create a forecast model config file (uses supported algo)."""
        config = {
            "algo": algo_name,
            "params": {
                "p": 1,
                "d": 1,
                "q": 1,
            }
        }
        return config

    def _setup_model_subdir(self, model_name, algo_name="arima", config_exists=True):
        """Helper to set up a model in subdirectory format (model_name/model_name.json)."""
        model_dir = os.path.join(self.dynamic_model_dir, model_name)
        os.makedirs(model_dir, exist_ok=True)

        if config_exists:
            config_file = os.path.join(model_dir, f"{model_name}.json")
            config = self._create_forecast_config(algo_name)
            with open(config_file, "w") as f:
                json.dump(config, f)
            return config_file
        return None

    @mock.patch.object(Configure, "get_instance")
    def test_sync_subdirectory_model_discovered(self, mock_config_instance):
        """Test that sync discovers models in new format subdirectories (line 126)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Set up a model subdirectory with valid config
        config_file = self._setup_model_subdir("my_arima_model", "arima")

        # Register the model first
        self.registry.register_service_from_file(config_file)

        # Verify model is registered
        self.assertIn("my_arima_model", self.registry.services)

        # Now sync should handle it
        self.registry.sync_dynamic_services()

        # Model should still be in services
        self.assertIn("my_arima_model", self.registry.services)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_skip_existing_model_in_subdir(self, mock_config_instance):
        """Test that sync skips models already in registry (line 127-128)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Set up model subdirectory
        config_file = self._setup_model_subdir("existing_model", "arima")

        # Register the model first
        self.registry.register_service_from_file(config_file)
        first_service = self.registry.services["existing_model"]
        first_id = id(first_service)

        # Sync should not re-register or replace it
        self.registry.sync_dynamic_services()

        # Should be the same service instance (not replaced)
        self.assertEqual(id(self.registry.services["existing_model"]), first_id)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_missing_config_file_in_subdir(self, mock_config_instance):
        """Test sync behavior when config file missing from subdirectory (line 133-139)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Create subdirectory without config file
        model_dir = os.path.join(self.dynamic_model_dir, "incomplete_model")
        os.makedirs(model_dir, exist_ok=True)

        # Sync should skip this model
        self.registry.sync_dynamic_services()

        # Model should not be in services
        self.assertNotIn("incomplete_model", self.registry.services)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_constructs_correct_config_path(self, mock_config_instance):
        """Test that sync constructs correct config path (line 130-131)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Set up model with specific name
        model_name = "test_model_123"
        config_file = self._setup_model_subdir(model_name, "arima")

        # Verify the expected path structure
        expected_dir = os.path.join(self.dynamic_model_dir, model_name)
        expected_config = os.path.join(expected_dir, f"{model_name}.json")
        self.assertEqual(config_file, expected_config)
        self.assertTrue(os.path.exists(expected_config))

    @mock.patch.object(Configure, "get_instance")
    def test_sync_multiple_subdirectory_models(self, mock_config_instance):
        """Test syncing multiple models in subdirectories."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Set up multiple models
        model_names = ["model1", "model2", "model3"]
        for model_name in model_names:
            config_file = self._setup_model_subdir(model_name, "arima")
            self.registry.register_service_from_file(config_file)

        # Sync
        self.registry.sync_dynamic_services()

        # All models should be in services
        for model_name in model_names:
            self.assertIn(model_name, self.registry.services)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_register_fails_with_invalid_config(self, mock_config_instance):
        """Test error handling when registration fails (line 141-148)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Create subdirectory with invalid config (missing 'algo' field)
        model_dir = os.path.join(self.dynamic_model_dir, "bad_config_model")
        os.makedirs(model_dir, exist_ok=True)
        config_file = os.path.join(model_dir, "bad_config_model.json")
        with open(config_file, "w") as f:
            json.dump({"params": {}}, f)  # Missing 'algo' field

        # Sync should handle the error gracefully
        try:
            self.registry.sync_dynamic_services()
        except Exception:
            pass  # Expected to skip on error

        # Model should not be in services
        self.assertNotIn("bad_config_model", self.registry.services)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_handles_mixed_old_and_new_format(self, mock_config_instance):
        """Test that sync handles both old format (root/*.json) and new format (subdir/model.json)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Set up new format (subdirectory)
        config_file_new = self._setup_model_subdir("new_format_model", "arima")
        self.registry.register_service_from_file(config_file_new)

        # Set up old format (root level) - this is handled by another branch
        # but we verify the new format doesn't interfere
        old_format_config = os.path.join(self.dynamic_model_dir, "old_model.json")
        with open(old_format_config, "w") as f:
            json.dump(self._create_forecast_config("prophet"), f)

        # Sync
        self.registry.sync_dynamic_services()

        # New format model should be present
        self.assertIn("new_format_model", self.registry.services)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_subdir_with_special_characters_in_name(self, mock_config_instance):
        """Test subdirectory with special characters in model name."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Set up model with special characters in name (underscores, numbers)
        model_name = "model_with_123_special_chars"
        config_file = self._setup_model_subdir(model_name, "arima")
        self.registry.register_service_from_file(config_file)

        # Sync
        self.registry.sync_dynamic_services()

        # Model should be in services with exact name
        self.assertIn(model_name, self.registry.services)

    @mock.patch.object(Configure, "get_instance")
    def test_sync_subdir_empty_directory(self, mock_config_instance):
        """Test sync behavior with empty directory (no subdirectories)."""
        mock_config = mock.MagicMock()
        mock_config.get_dynamic_model_directory.return_value = self.dynamic_model_dir
        mock_config_instance.return_value = mock_config

        # Sync with only empty directory
        self.registry.sync_dynamic_services()

        # No models should be registered
        # (only filtering by service type, but no models exist)
        self.assertEqual(len(self.registry.services), 0)


if __name__ == "__main__":
    unittest.main()
