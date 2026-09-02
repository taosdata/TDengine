# encoding:utf-8
# pylint: disable=c0103
"""forecast unit test cases"""

import math
import os.path
import sys
import unittest

import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from taosanalytics.algo.forecast import draw_forecast_results
from taosanalytics.log import setup_log_info
from taosanalytics.service_registry import loader


class ForecastTest(unittest.TestCase):
    """forecast unit test cases"""

    airline_passengers = [
        112,
        118,
        132,
        129,
        121,
        135,
        148,
        148,
        136,
        119,
        104,
        118,
        115,
        126,
        141,
        135,
        125,
        149,
        170,
        170,
        158,
        133,
        114,
        140,
        145,
        150,
        178,
        163,
        172,
        178,
        199,
        199,
        184,
        162,
        146,
        166,
        171,
        180,
        193,
        181,
        183,
        218,
        230,
        242,
        209,
        191,
        172,
        194,
        196,
        196,
        236,
        235,
        229,
        243,
        264,
        272,
        237,
        211,
        180,
        201,
        204,
        188,
        235,
        227,
        234,
        264,
        302,
        293,
        259,
        229,
        203,
        229,
        242,
        233,
        267,
        269,
        270,
        315,
        364,
        347,
        312,
        274,
        237,
        278,
        284,
        277,
        317,
        313,
        318,
        374,
        413,
        405,
        355,
        306,
        271,
        306,
        315,
        301,
        356,
        348,
        355,
        422,
        465,
        467,
        404,
        347,
        305,
        336,
        340,
        318,
        362,
        348,
        363,
        435,
        491,
        505,
        404,
        359,
        310,
        337,
        360,
        342,
        406,
        396,
        420,
        472,
        548,
        559,
        463,
        407,
        362,
        405,
        417,
        391,
        419,
        461,
        472,
        535,
        622,
        606,
        508,
        461,
        390,
        432,
    ]

    @classmethod
    def setUpClass(cls):
        """set up the environment for unit test"""
        setup_log_info("unit_test.log")
        loader.register_all_services()

    def get_input_list(self):
        """Load deterministic forecast data without external network access."""
        ts_list = pd.date_range(
            start="1949-01-01",
            periods=len(self.airline_passengers),
            freq="MS",
        ).tolist()
        dst_list = [int(item.timestamp()) for item in ts_list]

        return self.airline_passengers, dst_list

    def assert_forecast_result(self, result, rows, return_conf=True):
        """check the common forecast result format"""
        expected_columns = 4 if return_conf else 2
        self.assertEqual(len(result["res"]), expected_columns)
        self.assertEqual(len(result["res"][0]), rows)
        self.assertEqual(len(result["res"][1]), rows)
        self.assertTrue(math.isfinite(result["mse"]))
        self.assertGreaterEqual(result["mse"], 0)

        if return_conf:
            self.assertEqual(len(result["res"][2]), rows)
            self.assertEqual(len(result["res"][3]), rows)
            for forecast, lower, upper in zip(
                result["res"][1], result["res"][2], result["res"][3]
            ):
                self.assertTrue(math.isfinite(forecast))
                self.assertTrue(math.isfinite(lower))
                self.assertTrue(math.isfinite(upper))
                self.assertLessEqual(lower, forecast)
                self.assertLessEqual(forecast, upper)

    def test_holt_winters_forecast(self):
        """test holt winters forecast with invalid and then valid parameters"""
        s = loader.get_service("holtwinters")
        data, ts = self.get_input_list()

        s.set_input_list(data, ts)
        self.assertRaises(ValueError, s.execute)

        s.set_params({"rows": 10, "start_ts": 171000000, "time_step": 86400 * 30})

        r = s.execute()
        draw_forecast_results(data, len(r["res"]) > 2, s.conf, r["res"], "holtwinters")

    def test_holt_winters_forecast_2(self):
        """test holt winters with valid parameters"""
        s = loader.get_service("holtwinters")
        data, ts = self.get_input_list()

        s.set_input_list(data, ts)
        s.set_params(
            {
                "rows": 10,
                "trend": "mul",
                "seasonal": "mul",
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "period": 12,
            }
        )

        r = s.execute()
        draw_forecast_results(data, len(r["res"]) > 2, s.conf, r["res"], "holtwinters")

    def test_holt_winter_invalid_params(self):
        """parameters validation check"""
        s = loader.get_service("holtwinters")

        self.assertRaises(ValueError, s.set_params, {"trend": "mul"})

        self.assertRaises(ValueError, s.set_params, {"trend": "mul"})

        self.assertRaises(ValueError, s.set_params, {"trend": "mul", "rows": 10})

        self.assertRaises(ValueError, s.set_params, {"trend": "multi"})

        self.assertRaises(ValueError, s.set_params, {"seasonal": "additive"})

        self.assertRaises(
            ValueError,
            s.set_params,
            {
                "rows": 10,
                "trend": "multi",
                "seasonal": "addi",
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "period": 12,
            },
        )

        self.assertRaises(
            ValueError,
            s.set_params,
            {
                "rows": 10,
                "trend": "mul",
                "seasonal": "add",
                "time_step": 86400 * 30,
                "period": 12,
            },
        )

        s.set_params({"rows": 10, "start_ts": 171000000, "time_step": 86400 * 30})

        self.assertRaises(
            ValueError,
            s.set_params,
            {"rows": "abc", "start_ts": 171000000, "time_step": 86400 * 30},
        )

        self.assertRaises(
            ValueError, s.set_params, {"rows": 10, "start_ts": "aaa", "time_step": "30"}
        )

        self.assertRaises(
            ValueError,
            s.set_params,
            {"rows": 10, "start_ts": 171000000, "time_step": 0},
        )

    def test_ces_forecast(self):
        """CES algorithm check"""
        s = loader.get_service("ces")
        data, ts = self.get_input_list()

        self.assertIsNotNone(s)
        s.set_input_list(data, ts)
        self.assertRaises(ValueError, s.execute)

        s.set_params(
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "period": 12,
                "model": "Z",
            }
        )
        r = s.execute()

        self.assert_forecast_result(r, 10)
        draw_forecast_results(data, len(r["res"]) > 2, s.conf, r["res"], "ces")

        s = loader.get_service("ces")
        s.set_input_list(data, ts)
        s.set_params(
            {
                "rows": 3,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "return_conf": 0,
            }
        )
        r = s.execute()
        self.assert_forecast_result(r, 3, False)

    def test_ces_invalid_params(self):
        """CES parameters validation check"""
        s = loader.get_service("ces")
        self.assertIsNotNone(s)

        self.assertRaises(
            ValueError,
            s.set_params,
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "model": "invalid",
            },
        )

    def test_ces_rejects_non_finite_input(self):
        """CES input data validation check"""
        data, ts = self.get_input_list()
        for non_finite in (math.nan, math.inf, -math.inf):
            with self.subTest(value=non_finite):
                invalid_data = list(data)
                invalid_data[5] = non_finite
                s = loader.get_service("ces")
                s.set_input_list(invalid_data, ts)
                s.set_params(
                    {
                        "rows": 10,
                        "start_ts": 171000000,
                        "time_step": 86400 * 30,
                        "period": 12,
                    }
                )

                self.assertRaisesRegex(ValueError, "NaN or infinite", s.execute)

    def test_theta_forecast(self):
        """Theta algorithm check"""
        s = loader.get_service("theta")
        data, ts = self.get_input_list()

        self.assertIsNotNone(s)
        s.set_input_list(data, ts)
        s.set_params(
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "period": 12,
                "decomposition_type": "multiplicative",
            }
        )
        r = s.execute()

        self.assert_forecast_result(r, 10)
        draw_forecast_results(data, len(r["res"]) > 2, s.conf, r["res"], "theta")

    def test_theta_invalid_params(self):
        """Theta parameters validation check"""
        s = loader.get_service("theta")
        self.assertIsNotNone(s)

        self.assertRaises(
            ValueError,
            s.set_params,
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "decomposition_type": "invalid",
            },
        )

    def test_theta_rejects_non_finite_input(self):
        """Theta input data validation check"""
        data, ts = self.get_input_list()
        for non_finite in (math.nan, math.inf, -math.inf):
            with self.subTest(value=non_finite):
                invalid_data = list(data)
                invalid_data[5] = non_finite
                s = loader.get_service("theta")
                s.set_input_list(invalid_data, ts)
                s.set_params(
                    {
                        "rows": 10,
                        "start_ts": 171000000,
                        "time_step": 86400 * 30,
                        "period": 12,
                    }
                )

                self.assertRaisesRegex(ValueError, "NaN or infinite", s.execute)

    def test_theta_multiplicative_requires_positive_input(self):
        """Theta multiplicative decomposition requires positive input"""
        data, ts = self.get_input_list()
        for non_positive in (0, -1):
            with self.subTest(value=non_positive):
                invalid_data = list(data)
                invalid_data[5] = non_positive
                s = loader.get_service("theta")
                s.set_input_list(invalid_data, ts)
                s.set_params(
                    {
                        "rows": 10,
                        "start_ts": 171000000,
                        "time_step": 86400 * 30,
                        "period": 12,
                        "decomposition_type": "multiplicative",
                    }
                )

                self.assertRaisesRegex(ValueError, "strictly positive", s.execute)

    def test_ets_forecast(self):
        """ETS algorithm check"""
        s = loader.get_service("ets")
        data, ts = self.get_input_list()

        self.assertIsNotNone(s)
        s.set_input_list(data, ts)
        s.set_params(
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "period": 12,
                "model": "ZZZ",
                "damped": 1,
            }
        )
        r = s.execute()

        self.assert_forecast_result(r, 10)
        draw_forecast_results(data, len(r["res"]) > 2, s.conf, r["res"], "ets")

    def test_ets_invalid_params(self):
        """ETS parameters validation check"""
        s = loader.get_service("ets")
        self.assertIsNotNone(s)

        params = {"rows": 10, "start_ts": 171000000, "time_step": 86400 * 30}
        for model in ("AA", "NAA", "AXA", "AAX", "AAAA"):
            with self.subTest(model=model):
                self.assertRaises(ValueError, s.set_params, {**params, "model": model})

        for damped in (2, -1, "yes", ""):
            with self.subTest(damped=damped):
                self.assertRaises(
                    ValueError, s.set_params, {**params, "damped": damped}
                )

    def test_ets_rejects_non_finite_input(self):
        """ETS input data validation check"""
        data, ts = self.get_input_list()
        for non_finite in (math.nan, math.inf, -math.inf):
            with self.subTest(value=non_finite):
                invalid_data = list(data)
                invalid_data[5] = non_finite
                s = loader.get_service("ets")
                s.set_input_list(invalid_data, ts)
                s.set_params(
                    {
                        "rows": 10,
                        "start_ts": 171000000,
                        "time_step": 86400 * 30,
                        "period": 12,
                    }
                )

                self.assertRaisesRegex(ValueError, "NaN or infinite", s.execute)

    def test_ets_multiplicative_requires_positive_input(self):
        """ETS multiplicative components require positive input"""
        data, ts = self.get_input_list()
        for non_positive in (0, -1):
            with self.subTest(value=non_positive):
                invalid_data = list(data)
                invalid_data[5] = non_positive
                s = loader.get_service("ets")
                s.set_input_list(invalid_data, ts)
                s.set_params(
                    {
                        "rows": 10,
                        "start_ts": 171000000,
                        "time_step": 86400 * 30,
                        "period": 12,
                        "model": "MNN",
                    }
                )

                self.assertRaisesRegex(ValueError, "strictly positive", s.execute)

    def test_new_forecasters_require_two_complete_periods(self):
        """CES, Theta, and ETS require two complete periods when period is set"""
        data, ts = self.get_input_list()
        params = {
            "rows": 2,
            "start_ts": 171000000,
            "time_step": 86400 * 30,
            "period": 12,
        }

        for algo in ("ces", "theta", "ets"):
            with self.subTest(algo=algo):
                s = loader.get_service(algo)
                s.set_input_list(data[:23], ts[:23])
                s.set_params(params)
                self.assertRaisesRegex(
                    ValueError, "less than the required periods", s.execute
                )

    def test_new_forecasters_reject_empty_and_single_value_input(self):
        """CES, Theta, and ETS reject input that cannot be fitted"""
        params = {
            "rows": 2,
            "start_ts": 171000000,
            "time_step": 86400 * 30,
        }

        for algo in ("ces", "theta", "ets"):
            for data in ([], [10]):
                with self.subTest(algo=algo, input_size=len(data)):
                    s = loader.get_service(algo)
                    s.set_input_list(data, list(range(len(data))))
                    s.set_params(params)
                    self.assertRaisesRegex(
                        ValueError, "less than the required periods", s.execute
                    )

    def test_new_forecasters_constant_series(self):
        """Constant input produces finite forecasts and a near-zero fitted MSE"""
        data = [10.0] * 36
        ts = list(range(len(data)))
        params = {
            "rows": 3,
            "start_ts": 171000000,
            "time_step": 86400 * 30,
            "period": 12,
        }

        for algo in ("ces", "theta", "ets"):
            with self.subTest(algo=algo):
                s = loader.get_service(algo)
                s.set_input_list(data, ts)
                s.set_params(params)
                result = s.execute()

                self.assert_forecast_result(result, 3)
                self.assertAlmostEqual(result["mse"], 0.0, places=10)
                for forecast in result["res"][1]:
                    self.assertAlmostEqual(forecast, 10.0, places=10)

    def test_new_forecasters_invalid_common_params(self):
        """CES, Theta, and ETS enforce all common forecast parameter constraints"""
        valid = {
            "rows": 2,
            "start_ts": 171000000,
            "time_step": 86400 * 30,
        }
        invalid_params = (
            {},
            {"rows": 2, "start_ts": 171000000},
            {**valid, "rows": 0},
            {**valid, "rows": "invalid"},
            {**valid, "start_ts": "invalid"},
            {**valid, "time_step": 0},
            {**valid, "time_step": "invalid"},
            {**valid, "period": -1},
            {**valid, "period": "invalid"},
            {**valid, "conf": -0.1},
            {**valid, "conf": 1.0},
            {**valid, "conf": "invalid"},
        )

        for algo in ("ces", "theta", "ets"):
            for params in invalid_params:
                with self.subTest(algo=algo, params=params):
                    s = loader.get_service(algo)
                    self.assertRaises(ValueError, s.set_params, params)

    def test_arima(self):
        """arima algorithm check"""
        s = loader.get_service("arima")
        data, ts = self.get_input_list()

        s.set_input_list(data, ts)
        self.assertRaises(ValueError, s.execute)

        s.set_params(
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "period": 12,
                "start_p": 0,
                "max_p": 10,
                "start_q": 0,
                "max_q": 10,
            }
        )
        r = s.execute()

        rows = len(r["res"][0])
        draw_forecast_results(data, len(r["res"]) > 1, s.conf, r["res"], "arima")

    def test_gpt_fc(self):
        """for local test only, disabled it in github action"""
        data, ts = self.get_input_list()
        pass

        # s = loader.get_service("td_gpt_fc")
        # s.set_input_list(data, ts)
        #
        # s.set_params({"host":'192.168.2.90:5000/ds_predict', 'rows': 10, 'start_ts': 171000000, 'time_step': 86400*30})
        # r = s.execute()
        #
        # rows = len(r["res"][0])
        # draw_forecast_results(data, False, r["res"], rows, "gpt")

    def test_prophet_forecast(self):
        """prophet algorithm check"""
        s = loader.get_service("prophet")
        data, ts = self.get_input_list()

        s.set_input_list(data, ts)
        self.assertRaises(ValueError, s.execute)

        s.set_params(
            {
                "rows": 10,
                "start_ts": 171000000,
                "time_step": 86400 * 30,
                "seasonality_mode": "additive",
                "changepoint_prior_scale": 0.05,
            }
        )
        r = s.execute()

        rows = len(r["res"][0])
        draw_forecast_results(data, len(r["res"]) > 1, s.conf, r["res"], "prophet")


if __name__ == "__main__":
    unittest.main()
