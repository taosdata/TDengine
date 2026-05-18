# encoding:utf-8
# pylint: disable=c0103
"""unit test module"""
import os.path
import unittest
import sys
import tempfile
import types
from unittest import mock

import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from taosanalytics.algo.imputation import check_freq_param
from taosanalytics.service_registry import loader, ServiceRegistry
from taosanalytics.util import convert_results_to_windows, is_white_noise, parse_options, is_stationary, \
    parse_time_delta_string, validate_pay_load


class UtilTest(unittest.TestCase):
    """utility test cases"""

    def test_generate_anomaly_window(self):
        # Test case 1: Normal input
        wins, mask = convert_results_to_windows([1, -1, -2, 1, 1, 1, -1, -1, -1, 1, 1, -1],
                                                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 1)
        print(f"The result window is:{wins}")

        # Assert the number of windows
        self.assertEqual(len(wins), 4)

        # Assert the first window
        self.assertListEqual(wins[0], [2, 2])
        self.assertListEqual(wins[1], [3, 3])

        self.assertListEqual(wins[2], [7, 9])

        # Assert the second window
        self.assertListEqual(wins[3], [12, 12])

        self.assertEqual(mask[0], -1)
        self.assertEqual(mask[1], -2)
        self.assertEqual(mask[2], -1)
        self.assertEqual(mask[3], -1)

        # Test case 2: Anomaly input list is empty
        wins = convert_results_to_windows([], [1, 2], 1)
        self.assertListEqual(wins, [])

        # Test case 3: Anomaly input list is None
        wins = convert_results_to_windows([], None, 1)
        self.assertListEqual(wins, [])

        # Test case 4: Timestamp list is None
        wins = convert_results_to_windows(None, [], 1)
        self.assertListEqual(wins, [])

    @pytest.mark.skip(reason="validate_input_data helper is not implemented in taosanalytics.util")
    def test_validate_input_data(self):
        """placeholder for removed legacy helper."""

    def test_validate_pay_load(self):
        valid_payload = {
            "data": [
                list(range(10)),
                list(range(10, 20)),
            ],
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "DOUBLE", 8],
            ],
        }
        self.assertIsNone(validate_pay_load(valid_payload))

        with self.assertRaisesRegex(ValueError, '"data" does not exist in request json'):
            validate_pay_load({})

        with self.assertRaisesRegex(ValueError, "schema is missing"):
            validate_pay_load({"data": [list(range(10)), list(range(10, 20))]})

        with self.assertRaisesRegex(ValueError, "only one column provided"):
            validate_pay_load({"data": [list(range(10))], "schema": [["val", "DOUBLE", 8]]})

    @pytest.mark.skip(reason="validate_forecast_input_data helper is not implemented in taosanalytics.util")
    def test_validate_forecast_input_data(self):
        """placeholder for removed legacy helper."""

    def test_convert_results_to_windows(self):
        wins, mask = convert_results_to_windows([1, -1, -1, 1], [10, 20, 30, 40], 1)
        self.assertListEqual(wins, [[20, 30]])
        self.assertListEqual(mask, [-1])

        more_wins, more_mask = convert_results_to_windows([1, 1, -1, 1], [10, 20, 30, 40], 1)
        self.assertListEqual(more_wins, [[30, 30]])
        self.assertListEqual(more_mask, [-1])

        empty_windows = convert_results_to_windows([1, -1], [1000], 1)
        self.assertIsInstance(empty_windows, list)
        self.assertListEqual(empty_windows, [])

    def test_is_white_noise(self):
        """
        Test the is_white_noise function.
        This function tests the functionality of the is_white_noise function by providing a list and asserting the expected result.
        """
        list1 = []
        wn = is_white_noise(list1)
        self.assertFalse(wn)

        list2 = [247511, 257094, 257608, 243091, 253939, 259045, 248344, 235077, 269781, 257511, 258071, 253365, 258183,
                250891, 250763, 252676, 253324, 247570, 254403, 237292, 247909, 251868, 243086, 250216, 242900, 255638,
                244888, 272288, 252368, 254691, 252974, 243096, 247038, 255276, 251619, 236311, 247814, 250090, 239415,
                266783, 251648, 244245, 253508, 250260, 242150, 230585, 261644, 250960, 250574, 242501, 240237, 236069,
                250297, 245787, 239381, 253123, 246583, 240956, 237913, 249129, 252029, 254002, 244694, 248745, 245447,
                255747, 245754, 260273, 253340, 253769, 246203, 251977, 245523, 249441, 247925, 248722, 242326, 255040,
                247812, 256229, 258871, 260190, 252385, 232068, 272231, 248222, 248073, 250324, 260827, 239761, 255077,
                245773, 240380, 252500, 239677, 250281, 258338, 242776, 248348, 256002, 249827, 250280, 244887, 253200,
                250143, 252502, 251982, 256365, 258569, 250180, 257315, 254351, 238344, 247509, 245239, 243630, 249638,
                245019, 264868, 245770, 242752, 252651, 270625, 243761, 247255, 250909, 247590, 258596, 265892, 264066,
                243132, 254879, 258478, 246465, 271865, 257378, 247627, 252983, 248719, 256654, 242170, 265693, 242795,
                243425]

        for _ in range(10):
            wn = is_white_noise(list2)
            self.assertTrue(wn)


    def test_is_stationary(self):
        """test whether data is stationary or not"""
        st = is_stationary([1, 2, 3, 4, 5, 7, 5, 1, 54, 3, 6, 87, 45, 14, 24])
        self.assertEqual(st, False)

    def test_parse_options(self):
        """test case for parse key/value string into k/v pair"""
        option_str = "algo=ksigma,k=2,invalid_option=invalid_str"
        opt = parse_options(option_str)

        self.assertEqual(len(opt), 3)
        self.assertDictEqual(opt, {'algo': 'ksigma', 'k': '2', 'invalid_option': 'invalid_str'})

    def test_get_data_index(self):
        """  test the get the data index method"""
        schema = [
            ["val", "INT", 4],
            ["ts", "TIMESTAMP", 8]
        ]
        for index, val in enumerate(schema):
            if val[0] == "val":
                return index

    @pytest.mark.skip
    def test_download_tsfmmodel(self):
        from huggingface_hub import snapshot_download
        from tqdm import tqdm

        os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

        model_list = ['Salesforce/moirai-1.0-R-small']
        for item in tqdm(model_list):
            snapshot_download(
                repo_id=item,
                local_dir="/var/lib/taos/taosanode/model/moirai",  # storage directory
                local_dir_use_symlinks=False,   # disable the link
                resume_download=True,
                endpoint='https://hf-mirror.com'
            )
        
        print("download moirai-moe-1.0-small success")

    def test_parse_freq(self):
        val, unit = parse_time_delta_string('12s')
        self.assertEqual(val, 12)
        self.assertEqual(unit, 's')

        val, unit = parse_time_delta_string('m')
        self.assertEqual(val, 1)
        self.assertEqual(unit, 'm')

    def test_list_delta(self):
        with self.assertRaises(ValueError):
            check_freq_param([100, 200, 300, 400, 500, 600], '1s', 'ms')

        with self.assertRaises(ValueError):
            check_freq_param([123, 456, 789], '1m', 'ms')

        check_freq_param([100, 200, 300, 400, 500, 600], '20s', 's')
        check_freq_param([20, 30, 40, 50, 60, 90], '10s', 's')
        check_freq_param([1, 2, 3, 4, 5, 6],'10s', 'm')
        check_freq_param([123, 419, 533, 918], '20ms', 'ms')


class ServiceTest(unittest.TestCase):
    def setUp(self):
        """ load all service before start unit test """
        loader.register_all_services()

    def test_get_all_algos(self):
        service_list = loader.get_service_list()
        self.assertEqual(len(service_list["details"]), 4)

        version = sys.version_info

        for item in service_list["details"]:
            if item["type"] == "anomaly-detection":
                builtins = [i for i in item["algo"] if i.get('builtins') == True]
                if (version.major, version.minor) == (3, 12):
                    self.assertEqual(len(builtins), 4)
                else:
                    self.assertEqual(len(builtins), 5)

            elif item["type"] == "forecast":
                builtins = [i for i in item["algo"] if i.get('builtins') == True]
                self.assertEqual(len(builtins), 8)

            elif item["type"] == 'correlation':
                self.assertEqual(len(item['algo']), 2)
            else:
                self.assertEqual(len(item["algo"]), 1)

    def test_dynamic_load_service(self):
        """ test dynamic load service by name """
        import os

        config_path = os.path.join(tempfile.gettempdir(), "arima_model_config.json0")
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 1,
            "d": 0,
            "q": 1
          },
          "freq": "MS"
        }
        """
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            with self.assertRaises(ValueError):
                loader.register_service_from_file(config_path)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_dynamic_load_service_success(self):
        """ test dynamic load service with valid config file """
        import os

        config_path = os.path.join(tempfile.gettempdir(), "arima_model_config.json")
        service_name = "arima_model_config"
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 2,
            "d": 1,
            "q": 1
          },
          "freq": "MS"
        }
        """
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            if service_name in loader.services:
                del loader.services[service_name]

            loader.register_service_from_file(config_path)
            service = loader.get_service(service_name)
            self.assertIsNotNone(service)
            self.assertEqual(service.name, service_name)
        finally:
            if service_name in loader.services:
                del loader.services[service_name]
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_get_service_list_syncs_dynamic_models_from_directory(self):
        """list API should load dynamic models that exist in the shared directory"""
        from taosanalytics.conf import Configure

        service_name = "sync_dynamic_service"
        config_path = os.path.join(tempfile.mkdtemp(), service_name + ".json")
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 1,
            "d": 1,
            "q": 1
          },
          "freq": "MS"
        }
        """

        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            loader.services.pop(service_name, None)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=os.path.dirname(config_path)):
                service_list = loader.get_service_list()

            forecast_item = next(item for item in service_list["details"] if item["type"] == "forecast")
            self.assertTrue(any(item["name"] == service_name for item in forecast_item["algo"]))
            self.assertIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            temp_dir = os.path.dirname(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_get_service_list_removes_deleted_dynamic_models(self):
        """list API should drop dynamic models whose config file has been removed"""
        from taosanalytics.conf import Configure

        service_name = "sync_deleted_dynamic_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 1,
            "d": 1,
            "q": 0
          },
          "freq": "MS"
        }
        """

        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            loader.services.pop(service_name, None)
            loader.register_service_from_file(config_path)
            os.remove(config_path)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service_list = loader.get_service_list()

            forecast_item = next(item for item in service_list["details"] if item["type"] == "forecast")
            self.assertFalse(any(item["name"] == service_name for item in forecast_item["algo"]))
            self.assertNotIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_get_service_syncs_dynamic_model_from_directory(self):
        """get_service should load a dynamic model that exists in shared storage"""
        from taosanalytics.conf import Configure

        service_name = "sync_dynamic_lookup_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 2,
            "d": 1,
            "q": 0
          },
          "freq": "MS"
        }
        """

        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            loader.services.pop(service_name, None)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service = loader.get_service(service_name)

            self.assertIsNotNone(service)
            self.assertEqual(service.name, service_name)
            self.assertIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_get_service_removes_deleted_dynamic_model(self):
        """get_service should return None after the dynamic model file is deleted"""
        from taosanalytics.conf import Configure

        service_name = "sync_deleted_lookup_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 1,
            "d": 1,
            "q": 1
          },
          "freq": "MS"
        }
        """

        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            loader.services.pop(service_name, None)
            loader.register_service_from_file(config_path)
            os.remove(config_path)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service = loader.get_service(service_name)

            self.assertIsNone(service)
            self.assertNotIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_dynamic_load_service_missing_algo(self):
        """dynamic register should fail when 'algo' field is missing"""
        import os

        config_path = os.path.join(tempfile.gettempdir(), "arima_model_missing_algo.json")
        conf_file_content = """
        {
          "best_params": {
            "p": 1,
            "d": 0,
            "q": 1
          },
          "freq": "MS"
        }
        """
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            with self.assertRaises(ValueError):
                loader.register_service_from_file(config_path)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_dynamic_load_service_unsupported_algo(self):
        """dynamic register should fail for unsupported algorithm names"""
        import os

        config_path = os.path.join(tempfile.gettempdir(), "arima_model_bad_algo.json")
        conf_file_content = """
        {
          "algo": "lstm",
          "best_params": {
            "p": 1
          },
          "freq": "MS"
        }
        """
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            with self.assertRaises(ValueError):
                loader.register_service_from_file(config_path)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_dynamic_load_service_invalid_json(self):
        """dynamic register should fail when config file content is invalid"""
        import os

        config_path = os.path.join(tempfile.gettempdir(), "arima_model_invalid_json.json")
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write('{"algo": "arima"')

        try:
            with self.assertRaises(ValueError):
                loader.register_service_from_file(config_path)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_dynamic_load_service_duplicate_name(self):
        """dynamic register should fail when model name already exists"""
        import os

        config_path = os.path.join(tempfile.gettempdir(), "arima_model_duplicate.json")
        service_name = "arima_model_duplicate"
        conf_file_content = """
        {
          "algo": "arima",
          "best_params": {
            "p": 1,
            "d": 0,
            "q": 1
          },
          "freq": "MS"
        }
        """
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        try:
            if service_name in loader.services:
                del loader.services[service_name]

            loader.register_service_from_file(config_path)
            with self.assertRaises(RuntimeError):
                loader.register_service_from_file(config_path)
        finally:
            if service_name in loader.services:
                del loader.services[service_name]
            if os.path.exists(config_path):
                os.remove(config_path)

    def _register_dynamic_service_for_algo(self, algo_name):
        import os

        config_path = os.path.join(tempfile.gettempdir(), f"{algo_name}_model_config.json")
        service_name = f"{algo_name}_model_config"
        conf_file_content = f"""
        {{
          "algo": "{algo_name}",
          "best_params": {{
            "p": 1,
            "d": 0,
            "q": 1
          }},
          "freq": "MS"
        }}
        """

        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(conf_file_content)

        if service_name in loader.services:
            del loader.services[service_name]

        loader.register_service_from_file(config_path)
        return service_name, config_path

    def test_dynamic_prophet_service_is_supported(self):
        """prophet dynamic service should register and execute with forecast outputs."""
        import os
        import json
        import pandas as pd
        from taosanalytics.handlers.dynamic_forecast import DynamicForecastService

        service_name = None
        config_path = None
        try:
            config_path = os.path.join(tempfile.gettempdir(), "prophet_model_config.json")
            service_name = "prophet_model_config"
            conf_file_content = json.dumps({
                "algo": "prophet",
                "best_params": {
                    "changepoint_prior_scale": 0.05,
                    "seasonality_mode": "additive"
                },
                "freq": "D"
            })
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(conf_file_content)

            if service_name in loader.services:
                del loader.services[service_name]

            loader.register_service_from_file(config_path)
            service = loader.get_service(service_name)
            self.assertIsNotNone(service)
            self.assertIsInstance(service, DynamicForecastService)
            self.assertEqual(service.algo.lower(), "prophet")

            service.set_input_list(
                [10.0, 11.0, 12.0, 13.0, 14.0],
                [
                    1704067200000,
                    1704153600000,
                    1704240000000,
                    1704326400000,
                    1704412800000
                ],
            )
            service.set_params({
                "rows": 2,
                "start_ts": 1704499200000,
                "time_step": 86400000,
                "tz": "UTC",
            })

            forecast_df = pd.DataFrame({
                "ds": pd.date_range("2024-01-01", periods=7, freq="D"),
                "yhat": [1, 2, 3, 4, 5, 6, 7],
                "yhat_lower": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
                "yhat_upper": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
            })

            with mock.patch("taosanalytics.handlers.dynamic_forecast.ProphetModelForecaster") as mocked_forecaster:
                mocked_forecaster.return_value.forecast.return_value = forecast_df
                mocked_forecaster.return_value.get_param.return_value = {
                    "changepoint_prior_scale": 0.05,
                    "seasonality_mode": "additive",
                    "freq": "D",
                }

                result = service.execute()

                _, forecaster_df, rows = mocked_forecaster.call_args.args[:3]
                self.assertEqual(rows, 2)
                self.assertTrue(pd.api.types.is_datetime64_any_dtype(forecaster_df["ts"]))
                self.assertFalse(isinstance(forecaster_df["ts"].dtype, pd.DatetimeTZDtype))
                self.assertEqual(result["res"][0], [1704499200000, 1704585600000])
                self.assertEqual(result["res"][1], [6, 7])
                self.assertEqual(result["res"][2], [5.5, 6.5])
                self.assertEqual(result["res"][3], [6.5, 7.5])
        finally:
            if service_name and service_name in loader.services:
                del loader.services[service_name]
            if config_path and os.path.exists(config_path):
                os.remove(config_path)

    def test_dynamic_register_holtwinters_not_supported(self):
        """holtwinters is not accepted as a dynamic model algorithm; registration must raise ValueError."""
        import os

        config_path = None
        try:
            config_path = os.path.join(tempfile.gettempdir(), "holtwinters_model_config.json")
            conf_file_content = """
            {
              "algo": "holtwinters",
              "best_params": {
                "p": 1,
                "d": 0,
                "q": 1
              },
              "freq": "MS"
            }
            """
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(conf_file_content)

            with self.assertRaises(ValueError):
                loader.register_service_from_file(config_path)
        finally:
            if config_path and os.path.exists(config_path):
                os.remove(config_path)

    def test_dynamic_execute_theta_not_implemented(self):
        import os

        service_name = None
        config_path = None
        try:
            service_name, config_path = self._register_dynamic_service_for_algo("theta")
            service = loader.get_service(service_name)
            self.assertIsNotNone(service)
            with self.assertRaisesRegex(NotImplementedError, "Theta model is not implemented yet"):
                service.execute()
        finally:
            if service_name and service_name in loader.services:
                del loader.services[service_name]
            if config_path and os.path.exists(config_path):
                os.remove(config_path)

    def test_register_services_in_dir_ignores_non_python_suffix(self):
        registry = ServiceRegistry()

        fake_module = types.ModuleType("taosanalytics.algo.fc.valid")
        with mock.patch("os.path.exists", return_value=True), \
                mock.patch("os.path.isdir", return_value=False), \
                mock.patch("os.listdir", return_value=["fake.npy", "valid.py"]), \
                mock.patch("importlib.import_module", return_value=fake_module) as import_mod:
            registry._register_services_in_dir("/tmp", "taosanalytics.algo.fc.", "algo/fc/", True)

        import_mod.assert_called_once_with("taosanalytics.algo.fc.valid")

    def test_register_services_in_dir_skip_imported_underscored_class(self):
        registry = ServiceRegistry()
        module_name = "taosanalytics.algo.fc.mockmod"
        fake_module = types.ModuleType(module_name)

        imported_class = type("_ImportedForecastService", (), {"name": "_imported_forecast", "__module__": "other.module"})
        setattr(fake_module, "_ImportedForecastService", imported_class)

        with mock.patch("os.path.exists", return_value=True), \
                mock.patch("os.path.isdir", return_value=False), \
                mock.patch("os.listdir", return_value=["mockmod.py"]), \
                mock.patch("importlib.import_module", return_value=fake_module):
            registry._register_services_in_dir("/tmp", "taosanalytics.algo.fc.", "algo/fc/", True)

        self.assertNotIn("_imported_forecast", registry.services)

    # ------------------------------------------------------------------
    # DynamicAnomalyService (iforest) tests
    # ------------------------------------------------------------------

    def _iforest_config_content(self):
        """Return a minimal valid iforest config as a JSON string."""
        import json
        return json.dumps({
            "algo": "iforest",
            "best_params": {
                "n_estimators": 10,
                "contamination": 0.05,
                "window_size": 5,
                "stride": 1
            }
        })

    def test_dynamic_load_iforest_service_success(self):
        """Registering a valid iforest config must create a DynamicAnomalyService."""
        import os
        from taosanalytics.handlers.dynamic_anomaly import DynamicAnomalyService

        config_path = os.path.join(tempfile.gettempdir(), "iforest_model_config.json")
        service_name = "iforest_model_config"
        try:
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(self._iforest_config_content())

            loader.services.pop(service_name, None)
            loader.register_service_from_file(config_path)

            service = loader.get_service(service_name)
            self.assertIsNotNone(service)
            self.assertIsInstance(service, DynamicAnomalyService)
            self.assertEqual(service.algo.lower(), "iforest")
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_get_service_list_syncs_iforest_from_directory(self):
        """list API should load a dynamic iforest model that exists in the shared directory."""
        import os
        from taosanalytics.conf import Configure

        service_name = "sync_iforest_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        try:
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(self._iforest_config_content())

            loader.services.pop(service_name, None)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service_list = loader.get_service_list()

            anomaly_item = next(item for item in service_list["details"] if item["type"] == "anomaly-detection")
            self.assertTrue(any(item["name"] == service_name for item in anomaly_item["algo"]))
            self.assertIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_get_service_list_removes_deleted_iforest_model(self):
        """list API should drop a dynamic iforest model whose config file has been removed."""
        import os
        from taosanalytics.conf import Configure

        service_name = "sync_deleted_iforest_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        try:
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(self._iforest_config_content())

            loader.services.pop(service_name, None)
            loader.register_service_from_file(config_path)
            os.remove(config_path)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service_list = loader.get_service_list()

            anomaly_item = next(item for item in service_list["details"] if item["type"] == "anomaly-detection")
            self.assertFalse(any(item["name"] == service_name for item in anomaly_item["algo"]))
            self.assertNotIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_get_service_syncs_iforest_from_directory(self):
        """get_service should load a dynamic iforest model that exists in shared storage."""
        import os
        from taosanalytics.conf import Configure

        service_name = "sync_iforest_lookup_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        try:
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(self._iforest_config_content())

            loader.services.pop(service_name, None)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service = loader.get_service(service_name)

            self.assertIsNotNone(service)
            self.assertEqual(service.name, service_name)
            self.assertIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_get_service_removes_deleted_iforest_model(self):
        """get_service should return None after a dynamic iforest model's config is deleted."""
        import os
        from taosanalytics.conf import Configure

        service_name = "sync_deleted_iforest_lookup_service"
        temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(temp_dir, service_name + ".json")
        try:
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(self._iforest_config_content())

            loader.services.pop(service_name, None)
            loader.register_service_from_file(config_path)
            os.remove(config_path)

            conf = Configure.get_instance()
            with mock.patch.object(conf, 'get_dynamic_model_directory', return_value=temp_dir):
                service = loader.get_service(service_name)

            self.assertIsNone(service)
            self.assertNotIn(service_name, loader.services)
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)

    def test_dynamic_iforest_execute_returns_one_code_per_point(self):
        """DynamicAnomalyService.execute() dispatches to IsolationForestModelDetector.detect()."""
        import os
        from taosanalytics.handlers.dynamic_anomaly import DynamicAnomalyService
        from taosanalytics.algo.tool.detector import IsolationForestModelDetector

        n_points = 20
        input_data = list(range(n_points))
        expected_codes = [1] * n_points

        config_path = os.path.join(tempfile.gettempdir(), "iforest_exec_test.json")
        service_name = "iforest_exec_test"
        try:
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(self._iforest_config_content())

            loader.services.pop(service_name, None)
            loader.register_service_from_file(config_path)
            service = loader.get_service(service_name)
            self.assertIsInstance(service, DynamicAnomalyService)

            service.set_input_list(input_data, list(range(n_points)))

            with mock.patch.object(IsolationForestModelDetector, "detect",
                                   return_value=expected_codes) as mocked_detect:
                result = service.execute()

            self.assertEqual(len(result), n_points)
            self.assertEqual(result, expected_codes)
            mocked_detect.assert_called_once()
        finally:
            loader.services.pop(service_name, None)
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_iforest_detector_feature_matrix_and_result_size(self):
        """IsolationForestModelDetector.detect() exercises feature-matrix construction,
        sklearn parameter filtering, and per-point result-size validation end-to-end."""
        import json
        import os
        from taosanalytics.algo.tool.detector import IsolationForestModelDetector

        # Build a 30-point series: 28 normal values, 2 obvious spikes.
        n_points = 30
        normal = [float(i % 3) for i in range(n_points)]
        normal[14] = 1000.0
        normal[15] = 1000.0

        config_path = os.path.join(tempfile.gettempdir(), "iforest_detector_real_test.json")
        try:
            config = {
                "algo": "iforest",
                "best_params": {
                    "n_estimators": 10,
                    "contamination": 0.1,
                    "window_size": 5,
                    "stride": 1,
                    "random_state": 42,
                    "feature_fns": ["mean", "std"]
                }
            }
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(config))

            detector = IsolationForestModelDetector(
                path=config_path,
                input_list=normal,
                ts_list=list(range(n_points)),
                valid_code=1,
            )
            result = detector.detect()

            # Result must have exactly one code per input point.
            self.assertEqual(len(result), n_points)
            # All codes must be either valid_code (1) or anomaly (-1).
            valid_values = {1, -1}
            self.assertTrue(all(c in valid_values for c in result),
                            f"unexpected code values: {set(result) - valid_values}")
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_iforest_detector_result_size_with_stride_gt_1(self):
        """IsolationForestModelDetector returns one code per point even when stride > 1."""
        import json
        import os
        from taosanalytics.algo.tool.detector import IsolationForestModelDetector

        n_points = 20
        input_data = [float(i) for i in range(n_points)]

        config_path = os.path.join(tempfile.gettempdir(), "iforest_stride_test.json")
        try:
            config = {
                "algo": "iforest",
                "best_params": {
                    "n_estimators": 10,
                    "contamination": 0.05,
                    "window_size": 4,
                    "stride": 3,
                    "random_state": 0
                }
            }
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(config))

            detector = IsolationForestModelDetector(
                path=config_path,
                input_list=input_data,
                valid_code=1,
            )
            result = detector.detect()

            self.assertEqual(len(result), n_points)
            self.assertTrue(all(c in {1, -1} for c in result))
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

    def test_iforest_detector_validates_invalid_window_params(self):
        """IsolationForestModelDetector raises ValueError for non-positive window_size or stride."""
        import json
        import os
        from taosanalytics.algo.tool.detector import IsolationForestModelDetector

        for bad_params in [
            {"window_size": 0, "stride": 1},
            {"window_size": 5, "stride": 0},
            {"window_size": -1, "stride": 1},
        ]:
            config_path = os.path.join(tempfile.gettempdir(), "iforest_invalid_params.json")
            try:
                config = {"algo": "iforest", "best_params": dict(bad_params, n_estimators=10, contamination=0.1)}
                with open(config_path, "w", encoding="utf-8") as handle:
                    handle.write(json.dumps(config))

                detector = IsolationForestModelDetector(
                    path=config_path,
                    input_list=list(range(20)),
                    valid_code=1,
                )
                with self.assertRaises(ValueError):
                    detector.detect()
            finally:
                if os.path.exists(config_path):
                    os.remove(config_path)


if __name__ == '__main__':
    unittest.main()
