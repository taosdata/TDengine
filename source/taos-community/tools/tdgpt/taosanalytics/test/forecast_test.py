# encoding:utf-8
# pylint: disable=c0103
"""forecast unit test cases"""

import unittest, os.path, sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.algo.forecast import draw_fc_results
from taosanalytics.conf import setup_log_info, app_logger
from taosanalytics.servicemgmt import loader


class ForecastTest(unittest.TestCase):
    """forecast unit test cases"""

    @classmethod
    def setUpClass(cls):
        """ set up the environment for unit test """
        setup_log_info("unit_test.log")
        loader.load_all_service()

    def get_input_list(self):
        """ load data from csv """
        url = ('https://raw.githubusercontent.com/jbrownlee/Datasets/refs/heads/master/'
               'airline-passengers.csv')
        data = pd.read_csv(url, index_col='Month', parse_dates=True)
        
        ts_list = data[['Passengers']].index.tolist()
        dst_list = [int(item.timestamp()) for item in ts_list]

        return data['Passengers'].values.tolist(), dst_list


    def test_holt_winters_forecast(self):
        """ test holt winters forecast with invalid and then valid parameters"""
        s = loader.get_service("holtwinters")
        data, ts = self.get_input_list()

        s.set_input_list(data, ts)
        self.assertRaises(ValueError, s.execute)

        s.set_params({"rows": 10, "start_ts": 171000000, "time_step": 86400 * 30})

        r = s.execute()
        draw_fc_results(data, len(r["res"]) > 2, s.conf, r["res"], "holtwinters")

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
        draw_fc_results(data, len(r["res"]) > 2, s.conf, r["res"], "holtwinters")

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
        draw_fc_results(data, len(r["res"]) > 1, s.conf, r["res"], "arima")


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
        # draw_fc_results(data, False, r["res"], rows, "gpt")

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
        draw_fc_results(data, len(r["res"]) > 1, s.conf, r["res"], "prophet")


if __name__ == '__main__':
    unittest.main()
