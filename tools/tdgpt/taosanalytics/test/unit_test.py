# encoding:utf-8
# pylint: disable=c0103
"""unit test module"""
import os.path
import unittest
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.servicemgmt import loader
from taosanalytics.util import convert_results_to_windows, is_white_noise, parse_options, is_stationary


class UtilTest(unittest.TestCase):
    """utility test cases"""

    def test_generate_anomaly_window(self):
        # Test case 1: Normal input
        wins = convert_results_to_windows([1, 1, 1, 1, 1, 1, -1, -1, -1, 1, 1, -1],
                                          [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 1)
        print(f"The result window is:{wins}")

        # Assert the number of windows
        self.assertEqual(len(wins), 2)

        # Assert the first window
        self.assertListEqual(wins[0], [7, 9])

        # Assert the second window
        self.assertListEqual(wins[1], [12, 12])

        # Test case 2: Anomaly input list is empty
        wins = convert_results_to_windows([], [1, 2], 1)
        self.assertListEqual(wins, [])

        # Test case 3: Anomaly input list is None
        wins = convert_results_to_windows([], None, 1)
        self.assertListEqual(wins, [])

        # Test case 4: Timestamp list is None
        wins = convert_results_to_windows(None, [], 1)
        self.assertListEqual(wins, [])

    def test_validate_input_data(self):
        pass

    def test_validate_pay_load(self):
        pass

    def test_validate_forecast_input_data(self):
        pass

    def test_convert_results_to_windows(self):
        pass

    def test_is_white_noise(self):
        """
        Test the is_white_noise function.
        This function tests the functionality of the is_white_noise function by providing a list and asserting the expected result.
        """
        list1 = []
        wn = is_white_noise(list1)
        self.assertFalse(wn)

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


class ServiceTest(unittest.TestCase):
    def setUp(self):
        """ load all service before start unit test """
        loader.load_all_service()

    def test_get_all_algos(self):
        service_list = loader.get_service_list()
        self.assertEqual(len(service_list["details"]), 2)

        for item in service_list["details"]:
            if item["type"] == "anomaly-detection":
                self.assertEqual(len(item["algo"]), 6)
            else:
                self.assertEqual(len(item["algo"]), 7)


if __name__ == '__main__':
    unittest.main()
