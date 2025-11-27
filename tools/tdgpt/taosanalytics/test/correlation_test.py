# encoding:utf-8
# pylint: disable=c0103
"""anomaly detection unit test"""
import math
import unittest, sys, os.path

import numpy as np
from matplotlib import pyplot as plt

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.conf import setup_log_info, app_logger
from taosanalytics.servicemgmt import loader

def draw_lags_result(lags, ccf_vals, name):
    plt.figure(figsize=(10, 5))

    plt.stem(lags, ccf_vals)
    plt.axhline(0, color='black', linewidth=1)
    plt.axvline(0, color='gray', linestyle='--')
    plt.title('Cross-Correlation Function (CCF)')
    plt.xlabel('Lag (k)')
    plt.ylabel('Correlation')
    plt.grid(True)

    plt.savefig(name)


class CorrelationTest(unittest.TestCase):
    """ anomaly detection unit test class"""
    input1 = [1, 1.1, 1.0, 1.2, 1.1]
    input2 = [1, 1.5, 1.3, 1.8, 1.6]

    @classmethod
    def setUpClass(cls):
        """ set up environment for unit test, set the log file path """
        setup_log_info("unit_test.log")
        loader.load_all_service()

    def test_dtw(self):
        """ dtw unit case"""

        s = loader.get_service("dtw")
        s.set_input_list(CorrelationTest.input1, None)
        s.set_second_input_data(CorrelationTest.input2)

        val, path = s.execute()

        self.assertTrue(math.isclose(val, 1.6))
        self.assertEqual(len(path), 7)

    def test_tlcc(self):
        np.random.seed(42)
        n = 100

        # x_t:
        x = np.sin(np.linspace(0, 10, n)) + np.random.normal(0, 0.2, n)

        # y_t: delay 3 steps
        y = np.roll(x, 3) + np.random.normal(0, 0.2, n)

        # draw the result png
        # plt.plot(x)
        # plt.plot(y)
        # plt.savefig('data.png')
        plt.clf()

        # === 2. calculate the CCF ===
        s = loader.get_service("tlcc")
        s.set_input_list(x, None)
        s.set_second_input_data(y)
        s.set_params({"lag_start":-20, "lag_end":20})

        lags, ccf_vals = s.execute()

        # draw the results
        self.assertEqual(np.argmax(ccf_vals), 23)
        # draw_lags_result(lags, ccf_vals, 'res.png')
