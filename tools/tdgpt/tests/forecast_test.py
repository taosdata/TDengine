# encoding:utf-8
# pylint: disable=c0103
"""forecast unit test cases"""

import unittest, os.path, sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from taosanalytics.algo.forecast import draw_forecast_results
from taosanalytics.service_registry import loader
from taosanalytics.log import setup_log_info

class ForecastTest(unittest.TestCase):
    """forecast unit test cases"""
    airline_passengers = [
        112, 118, 132, 129, 121, 135, 148, 148, 136, 119, 104, 118,
        115, 126, 141, 135, 125, 149, 170, 170, 158, 133, 114, 140,
        145, 150, 178, 163, 172, 178, 199, 199, 184, 162, 146, 166,
        171, 180, 193, 181, 183, 218, 230, 242, 209, 191, 172, 194,
        196, 196, 236, 235, 229, 243, 264, 272, 237, 211, 180, 201,
        204, 188, 235, 227, 234, 264, 302, 293, 259, 229, 203, 229,
        242, 233, 267, 269, 270, 315, 364, 347, 312, 274, 237, 278,
        284, 277, 317, 313, 318, 374, 413, 405, 355, 306, 271, 306,
        315, 301, 356, 348, 355, 422, 465, 467, 404, 347, 305, 336,
        340, 318, 362, 348, 363, 435, 491, 505, 404, 359, 310, 337,
        360, 342, 406, 396, 420, 472, 548, 559, 463, 407, 362, 405,
        417, 391, 419, 461, 472, 535, 622, 606, 508, 461, 390, 432,
    ]

    @classmethod
    def setUpClass(cls):
        """ set up the environment for unit test """
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


    def test_holt_winters_forecast(self):
        """ test holt winters forecast with invalid and then valid parameters"""
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
                "rows": 10, "trend": 'mul', "seasonal": 'mul', "start_ts": 171000000,
                "time_step": 86400 * 30, "period": 12
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

        self.assertRaises(ValueError, s.set_params, {
            "rows": 10, "trend": 'multi', "seasonal": 'addi', "start_ts": 171000000,
            "time_step": 86400 * 30, "period": 12}
                          )

        self.assertRaises(ValueError, s.set_params,
                          {"rows": 10, "trend": 'mul', "seasonal": 'add', "time_step": 86400 * 30, "period": 12}
                          )

        s.set_params({"rows": 10, "start_ts": 171000000, "time_step": 86400 * 30})

        self.assertRaises(ValueError, s.set_params, {"rows": 'abc', "start_ts": 171000000, "time_step": 86400 * 30})

        self.assertRaises(ValueError, s.set_params, {"rows": 10, "start_ts": "aaa", "time_step": "30"})

        self.assertRaises(ValueError, s.set_params, {"rows": 10, "start_ts": 171000000, "time_step": 0})

    def test_arima(self):
        """arima algorithm check"""
        s = loader.get_service("arima")
        data, ts = self.get_input_list()

        s.set_input_list(data, ts)
        self.assertRaises(ValueError, s.execute)

        s.set_params(
            {"rows": 10, "start_ts": 171000000, "time_step": 86400 * 30, "period": 12,
             "start_p": 0, "max_p": 10, "start_q": 0, "max_q": 10}
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

       
        s.set_params({
        "rows": 10,
        "start_ts": 171000000,
        "time_step": 86400 * 30,
        "seasonality_mode": "additive",
        "changepoint_prior_scale": 0.05,
        })
        r = s.execute()

        rows = len(r["res"][0])
        draw_forecast_results(data, len(r["res"]) > 1, s.conf, r["res"], "prophet")


if __name__ == '__main__':
    unittest.main()
