# encoding:utf-8
"""Tests for DeepAR dynamic deployment."""

import os
import sys
import tempfile
import unittest
from unittest import mock

import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from taosanalytics.algo.dynamic.forecaster import DeepARModelForecaster
from taosanalytics.algo.dynamic.model_loader import ModelLoader


class TestModelLoaderPyTorch(unittest.TestCase):
    """Test PyTorch model loading utility."""

    def test_load_pt_model_missing_file(self):
        """Test that missing .pth file is handled gracefully."""
        state_dict, pipeline_state = ModelLoader.load_pt_model("/nonexistent/model.pth")
        self.assertIsNone(state_dict)
        self.assertIsNone(pipeline_state)

    @mock.patch("torch.load")
    def test_load_pt_model_direct_state_dict(self, mock_torch_load):
        """Test loading a direct state_dict format."""
        mock_state_dict = {
            "layer1.weight": mock.MagicMock(),
            "layer1.bias": mock.MagicMock(),
        }
        mock_torch_load.return_value = mock_state_dict

        state_dict, pipeline_state = ModelLoader.load_pt_model("/fake/model.pth")

        self.assertEqual(state_dict, mock_state_dict)
        self.assertEqual(pipeline_state, {})
        mock_torch_load.assert_called_once_with("/fake/model.pth", map_location="cpu", weights_only=True)

    @mock.patch("torch.load")
    def test_load_pt_model_wrapped_state_dict(self, mock_torch_load):
        """Test loading a wrapped state_dict (dict with 'model_state_dict' key)."""
        mock_wrapped = {
            "model_state_dict": {
                "layer1.weight": mock.MagicMock(),
                "layer1.bias": mock.MagicMock(),
            }
        }
        mock_torch_load.return_value = mock_wrapped

        state_dict, pipeline_state = ModelLoader.load_pt_model("/fake/model.pth")

        self.assertIn("layer1.weight", state_dict)
        self.assertEqual(pipeline_state, {})

    @mock.patch("torch.load")
    def test_load_pt_model_torch_error(self, mock_torch_load):
        """Test handling of torch.load errors."""
        mock_torch_load.side_effect = RuntimeError("Corrupted checkpoint")

        state_dict, pipeline_state = ModelLoader.load_pt_model("/fake/model.pth")

        self.assertIsNone(state_dict)
        self.assertIsNone(pipeline_state)


class TestDeepARModelForecasterConfig(unittest.TestCase):
    """Test DeepAR config loading and validation."""

    def setUp(self):
        """Create a temporary directory for test configs."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_deepar_config_loading(self):
        """Test that a valid DeepAR config is loaded correctly."""
        import json

        config = {
            "algo": "deepar",
            "model_path": "/path/to/model.pth",
            "config_path": "/path/to/config.json",
            "freq": "D",
            "prediction_length": 12,
        }

        config_file = os.path.join(self.temp_dir, "deepar.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        # Create sample input data
        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=100, freq="D"),
            "y": range(100),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        model_info = forecaster._load_config()

        self.assertEqual(model_info["algo"], "deepar")
        self.assertEqual(model_info["model_path"], "/path/to/model.pth")
        self.assertEqual(model_info["prediction_length"], 12)

    def test_deepar_algo_validation(self):
        """Test that non-DeepAR configs are rejected."""
        import json

        config = {
            "algo": "arima",
            "best_params": {"p": 1, "d": 0, "q": 1},
        }

        config_file = os.path.join(self.temp_dir, "wrong_algo.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=100, freq="D"),
            "y": range(100),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        forecaster.model_info = forecaster._load_config()

        # Should detect wrong algorithm
        self.assertFalse(forecaster._is_expected_algo())

    def test_deepar_missing_config_file(self):
        """Test handling of missing config file."""
        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=100, freq="D"),
            "y": range(100),
        })

        forecaster = DeepARModelForecaster("/nonexistent/config.json", df, horizon=10)
        model_info = forecaster._load_config()

        self.assertIsNone(model_info)

    def test_deepar_required_columns(self):
        """Test that required columns (ts, y) are validated."""
        import json

        config = {
            "algo": "deepar",
            "model_path": "/path/to/model.pth",
            "config_path": "/path/to/config.json",
        }

        config_file = os.path.join(self.temp_dir, "deepar.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        # DataFrame with missing 'y' column
        df_bad = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=100, freq="D"),
            "x": range(100),
        })

        forecaster = DeepARModelForecaster(config_file, df_bad, horizon=10)
        self.assertFalse(forecaster._has_required_columns())

    def test_deepar_get_param(self):
        """Test that parameters are correctly extracted from config."""
        import json

        config = {
            "algo": "deepar",
            "model_path": "/path/to/model.pth",
            "freq": "H",
            "prediction_length": 24,
            "best_params": {"context_length": 168},
        }

        config_file = os.path.join(self.temp_dir, "deepar.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=100, freq="H"),
            "y": range(100),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        forecaster.model_info = forecaster._load_config()
        params = forecaster.get_param()

        self.assertEqual(params["freq"], "H")
        self.assertEqual(params["prediction_length"], 24)


class TestDeepARServiceRegistry(unittest.TestCase):
    """Test DeepAR registration in service registry."""

    def test_deepar_in_forecast_models(self):
        """Test that deepar is registered as a forecast model."""
        from taosanalytics.service_registry import ServiceRegistry

        self.assertIn("deepar", ServiceRegistry._forecast_models)

    def test_deepar_not_in_anomaly_models(self):
        """Test that deepar is not misclassified as anomaly model."""
        from taosanalytics.service_registry import ServiceRegistry

        self.assertNotIn("deepar", ServiceRegistry._anomaly_models)

    def test_deepar_not_in_regression_models(self):
        """Test that deepar is not misclassified as regression model."""
        from taosanalytics.service_registry import ServiceRegistry

        self.assertNotIn("deepar", ServiceRegistry._regression_models)


class TestDeepARPytorchPathResolution(unittest.TestCase):
    """Test DeepAR pytorch/ subdirectory path resolution."""

    def setUp(self):
        """Create a temporary directory structure for test configs."""
        self.temp_dir = tempfile.mkdtemp()
        self.pytorch_dir = os.path.join(self.temp_dir, "pytorch")
        os.makedirs(self.pytorch_dir, exist_ok=True)

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_pytorch_subdir_path_weights_exists(self):
        """Test resolving weights.pt from pytorch/ subdirectory."""
        import json

        # Create config file
        config = {
            "algo": "deepar",
            "freq": "D",
            "prediction_length": 12,
        }
        config_file = os.path.join(self.temp_dir, "deepar_config.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        # Create weights.pt file in pytorch/ subdirectory
        weights_file = os.path.join(self.pytorch_dir, "weights.pt")
        with open(weights_file, "w") as f:
            f.write("dummy weights")

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=50, freq="D"),
            "y": range(50),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        resolved_path = forecaster._get_pytorch_subdir_path("weights.pt")

        self.assertIsNotNone(resolved_path)
        self.assertEqual(resolved_path, weights_file)
        self.assertTrue(os.path.exists(resolved_path))

    def test_get_pytorch_subdir_path_config_exists(self):
        """Test resolving config.json from pytorch/ subdirectory."""
        import json

        # Create config file
        config = {
            "algo": "deepar",
            "freq": "D",
            "prediction_length": 12,
        }
        config_file = os.path.join(self.temp_dir, "deepar_config.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        # Create config.json file in pytorch/ subdirectory
        pytorch_config_file = os.path.join(self.pytorch_dir, "config.json")
        with open(pytorch_config_file, "w") as f:
            json.dump({"context_length": 24, "num_layers": 2}, f)

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=50, freq="D"),
            "y": range(50),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        resolved_path = forecaster._get_pytorch_subdir_path("config.json")

        self.assertIsNotNone(resolved_path)
        self.assertEqual(resolved_path, pytorch_config_file)

    def test_get_pytorch_subdir_path_not_exists(self):
        """Test that None is returned when file doesn't exist in pytorch/ subdirectory."""
        import json

        config = {
            "algo": "deepar",
            "freq": "D",
            "prediction_length": 12,
        }
        config_file = os.path.join(self.temp_dir, "deepar_config.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=50, freq="D"),
            "y": range(50),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        # Try to resolve a file that doesn't exist
        resolved_path = forecaster._get_pytorch_subdir_path("nonexistent.pt")

        self.assertIsNone(resolved_path)

    def test_build_model_with_auto_resolved_paths(self):
        """Test that _build_model uses auto-resolved pytorch/ paths when explicit paths not set."""
        import json

        config = {
            "algo": "deepar",
            "freq": "D",
            "prediction_length": 12,
            "best_params": {"num_layers": 1, "hidden_size": 256},
        }
        config_file = os.path.join(self.temp_dir, "deepar_config.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        # Create pytorch subdirectory with dummy files
        pytorch_config_file = os.path.join(self.pytorch_dir, "config.json")
        with open(pytorch_config_file, "w") as f:
            json.dump({"context_length": 24}, f)

        weights_file = os.path.join(self.pytorch_dir, "weights.pt")
        with open(weights_file, "w") as f:
            f.write("dummy weights")

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=50, freq="D"),
            "y": range(50),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        forecaster.model_info = forecaster._load_config()

        # Verify that config doesn't have explicit paths
        self.assertIsNone(forecaster.model_info.get("model_path"))
        self.assertIsNone(forecaster.model_info.get("config_path"))

        # Verify that _get_pytorch_subdir_path resolves paths correctly
        resolved_weights = forecaster._get_pytorch_subdir_path("weights.pt")
        resolved_config = forecaster._get_pytorch_subdir_path("config.json")

        self.assertEqual(resolved_weights, weights_file)
        self.assertEqual(resolved_config, pytorch_config_file)

    def test_build_model_explicit_paths_take_priority(self):
        """Test that explicit model_path/config_path in config take priority over pytorch/ subdirectory."""
        import json

        explicit_weights = "/explicit/path/to/weights.pt"
        explicit_config = "/explicit/path/to/config.json"

        config = {
            "algo": "deepar",
            "model_path": explicit_weights,
            "config_path": explicit_config,
            "freq": "D",
            "prediction_length": 12,
        }
        config_file = os.path.join(self.temp_dir, "deepar_config.json")
        with open(config_file, "w") as f:
            json.dump(config, f)

        # Create dummy files in pytorch/ subdirectory (should not be used)
        weights_file = os.path.join(self.pytorch_dir, "weights.pt")
        with open(weights_file, "w") as f:
            f.write("dummy weights")

        pytorch_config_file = os.path.join(self.pytorch_dir, "config.json")
        with open(pytorch_config_file, "w") as f:
            json.dump({"context_length": 24}, f)

        df = pd.DataFrame({
            "ts": pd.date_range("2023-01-01", periods=50, freq="D"),
            "y": range(50),
        })

        forecaster = DeepARModelForecaster(config_file, df, horizon=10)
        forecaster.model_info = forecaster._load_config()

        # Verify that explicit paths are used from config
        self.assertEqual(forecaster.model_info.get("model_path"), explicit_weights)
        self.assertEqual(forecaster.model_info.get("config_path"), explicit_config)


if __name__ == "__main__":
    unittest.main()
