# encoding:utf-8
# pylint: disable=c0103
"""unit test module"""
import os.path
import unittest
import sys


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.algo.imputation import check_freq_param

from taosanalytics.servicemgmt import loader
from taosanalytics.algo.tool.profile_match import do_profile_match_impl
from taosanalytics.util import convert_results_to_windows, is_white_noise, parse_options, is_stationary, \
    parse_time_delta_string


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
        loader.load_all_service()

    def test_get_all_algos(self):
        service_list = loader.get_service_list()
        self.assertEqual(len(service_list["details"]), 4)

        version = sys.version_info

        for item in service_list["details"]:
            if item["type"] == "anomaly-detection":
                if (version.major, version.minor) == (3, 12):
                    self.assertEqual(len(item["algo"]), 5)
                else:
                    self.assertEqual(len(item["algo"]), 6)
            elif item["type"] == "forecast":
                self.assertEqual(len(item["algo"]), 8)
            elif item["type"] == 'correlation':
                self.assertEqual(len(item['algo']), 2)
            else:
                self.assertEqual(len(item["algo"]), 1)


class ProfileMatchImplTest(unittest.TestCase):
    """unit tests for do_profile_match_impl"""

    def test_dtw_with_profile_list_and_top_n(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 2,
                    "min_window": 5,
                    "max_window": 5
                }
            },
            "result": {
                "num": 2
            },
            "source_data": [1, 2, 3, 4, 5],
            "target_data": {
                "ts": [[1, 5], [2, 6], [3, 7]],
                "data": [
                    [1, 2, 3, 4, 5],
                    [2, 3, 4, 5, 6],
                    [5, 4, 3, 2, 1]
                ]
            }
        }

        result = do_profile_match_impl(req_json)

        self.assertEqual(result["metric_type"], "dtw_distance")
        self.assertEqual(result["rows"], 2)
        self.assertLessEqual(result["matches"][0]["criteria"], result["matches"][1]["criteria"])
        self.assertAlmostEqual(result["matches"][0]["criteria"], 0.0)
        self.assertEqual(result["matches"][0]["ts_window"], [1, 5])

    def test_dtw_series_sliding_window_with_threshold(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 1,
                    "min_window": 3,
                    "max_window": 3
                }
            },
            "result": {
                "threshold": 0.0
            },
            "source_data": [2, 3, 4],
            "target_data": {
                "ts": [1, 2, 3, 4, 5, 6, 7],
                "data": [1, 2, 3, 4, 5, 6, 7]
            }
        }

        result = do_profile_match_impl(req_json)

        self.assertEqual(result["metric_type"], "dtw_distance")
        self.assertEqual(result["rows"], 1)
        self.assertAlmostEqual(result["matches"][0]["criteria"], 0.0)
        self.assertEqual(result["matches"][0]["ts_window"], [2, 4])
        self.assertEqual(result["matches"][0]["num"], 3)

    def test_cosine_with_threshold(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "cosine",
                "params": {}
            },
            "result": {
                "threshold": 0.9
            },
            "source_data": [1, 0, -1],
            "target_data": {
                "ts": [[10, 12], [20, 22], [30, 32]],
                "data": [
                    [2, 0, -2],
                    [-1, 0, 1],
                    [1, 1, 1]
                ]
            }
        }

        result = do_profile_match_impl(req_json)

        self.assertEqual(result["metric_type"], "cosine_similarity")
        self.assertEqual(result["rows"], 1)
        self.assertAlmostEqual(result["matches"][0]["criteria"], 1.0)
        self.assertEqual(result["matches"][0]["ts_window"], [10, 12])

    def test_cosine_rejects_min_window_or_max_window(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "cosine",
                "params": {
                    "min_window": 3
                }
            },
            "result": {
                "num": 1
            },
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [[1, 3]],
                "data": [[1, 2, 3]]
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn("can only be set for dtw", str(ctx.exception))

    def test_different_min_max_window_for_dtw(self):
        req_json = {
            "normalization": "z-score",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 1,
                    "min_window": 2,
                    "max_window": 4
                }
            },
            "result": {
                "num": 10
            },
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000],
                "data": 
                    [1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 3, 4, 5, 6, 7]
            }
        }

        result = do_profile_match_impl(req_json)
        self.assertEqual(result["metric_type"], "dtw_distance")
        self.assertEqual(result["rows"], 10)
        self.assertLessEqual(result["matches"][0]["criteria"], result["matches"][1]["criteria"])
        self.assertLessEqual(result["matches"][1]["criteria"], result["matches"][2]["criteria"])
        self.assertEqual(result["matches"][0]["num"], 3)
        self.assertEqual(result["matches"][1]["num"], 3)
        self.assertEqual(result["matches"][2]["num"], 3)
        self.assertEqual(result["matches"][0]["ts_window"], [1000, 3000])
        self.assertEqual(result["matches"][1]["ts_window"], [2000, 4000])
        self.assertEqual(result["matches"][2]["ts_window"], [3000, 5000])

    def test_num_and_threshold_conflict(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 1
                }
            },
            "result": {
                "num": 1,
                "threshold": 1.0
            },
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [[1, 3]],
                "data": [[1, 2, 3]]
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn('cannot be set at the same time', str(ctx.exception))

    def test_cosine_requires_equal_length_profiles(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "cosine",
            },
            "result": {
                "num": 2,
            },

            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [[1000, 4000], [11000, 14000], [21000, 23000]],
                "data": [[1, 2, 3, 4], [9, 10, 11, 12], [11, 12, 13]]
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn('for cosine similarity, source_data and each candidate profile must have the same length', str(ctx.exception))

    def test_invalid_ts_format(self):
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 1
                }
            },
            "result": {
                "num": 2,
            },

            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [1000, 4000, 11000, 14000, 21000, 23000],
                "data": [[1, 2, 3, 4, 5, 6]]
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn('when "target_data.data" is a list of profiles, ' \
        'each corresponding item in "target_data.ts" must be a [start_ts, end_ts] pair', 
                    str(ctx.exception))

    def test_threshold_result_is_hard_capped_to_500(self):
        profile_count = 700
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "cosine",
                "params": {}
            },
            "result": {
                "threshold": -1.0
            },
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [[i, i + 2] for i in range(profile_count)],
                "data": [[1, 2, 3] for _ in range(profile_count)]
            }
        }

        result = do_profile_match_impl(req_json)

        self.assertEqual(result["metric_type"], "cosine_similarity")
        self.assertEqual(result["rows"], 500)
        self.assertEqual(len(result["matches"]), 500)

    def test_invalid_threshold_value(self):
        invalid_thresholds = [
            "invalid",
            "",
            "1,2",
            [],
            {},
            -11,
        ]

        base_req_json = {
            "normalization": "none",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 1
                }
            },
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [[1, 3]],
                "data": [[1, 2, 3]]
            }
        }

        for val in invalid_thresholds:
            req_json = dict(base_req_json)
            req_json["result"] = {"threshold": val}

            with self.subTest(threshold=val):
                with self.assertRaises((ValueError, TypeError)):
                    do_profile_match_impl(req_json)

    def test_source_data_too_large(self):
        """source_data exceeding MAX_SOURCE_LEN should raise ValueError"""
        req_json = {
            "normalization": "none",
            "algo": {"type": "dtw", "params": {"radius": 1}},
            "result": {"num": 1},
            "source_data": list(range(10001)),
            "target_data": {
                "ts": [[1, 3]],
                "data": [[1, 2, 3]]
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn("exceeds maximum allowed", str(ctx.exception))

    def test_target_too_many_profiles(self):
        """target_data.data with more profiles than MAX_PROFILES should raise ValueError"""
        profile_count = 10001
        req_json = {
            "normalization": "none",
            "algo": {"type": "cosine", "params": {}},
            "result": {"num": 1},
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": [[i, i + 2] for i in range(profile_count)],
                "data": [[1, 2, 3] for _ in range(profile_count)]
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn("too many profiles", str(ctx.exception))

    def test_sliding_window_too_many_candidates(self):
        """sliding window generating more candidates than MAX_WINDOW_CANDIDATES should raise ValueError"""
        req_json = {
            "normalization": "none",
            "algo": {
                "type": "dtw",
                "params": {
                    "radius": 1,
                    "min_window": 1,
                    "max_window": 2000
                }
            },
            "result": {"num": 1},
            "source_data": [1, 2, 3],
            "target_data": {
                "ts": list(range(10000)),
                "data": list(range(10000))
            }
        }

        with self.assertRaises(ValueError) as ctx:
            do_profile_match_impl(req_json)

        self.assertIn("exceeds the maximum", str(ctx.exception))

if __name__ == '__main__':
    unittest.main()
