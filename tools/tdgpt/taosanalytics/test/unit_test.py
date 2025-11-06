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

    def test_download_tsfmmodel(self):
        # from huggingface_hub import snapshot_download
        # from tqdm import tqdm

        # export HF_ENDPOINT=https://hf-mirror.com
        # model_list = ['Salesforce/moirai-1.0-R-small']
        # for item in tqdm(model_list):
        #     snapshot_download(
        #         repo_id=item,
        #         local_dir="/var/lib/taos/taosanode/model/moirai",  # storage directory
        #         local_dir_use_symlinks=False,   # disable the link
        #         resume_download=True,
        #         endpoint='https://hf-mirror.com'
        #     )
        #
        print("download moirai-moe-1.0-small success")

class ServiceTest(unittest.TestCase):
    def setUp(self):
        """ load all service before start unit test """
        loader.load_all_service()

    def test_get_all_algos(self):
        service_list = loader.get_service_list()
        self.assertEqual(len(service_list["details"]), 4)

        for item in service_list["details"]:
            if item["type"] == "anomaly-detection":
                self.assertEqual(len(item["algo"]), 6)
            elif item["type"] == "forecast":
                self.assertEqual(len(item["algo"]), 8)
            elif item["type"] == 'correlation':
                self.assertEqual(len(item['algo']), 2)
            else:
                self.assertEqual(len(item["algo"]), 1)

if __name__ == '__main__':
    unittest.main()
