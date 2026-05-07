# encoding:utf-8
# pylint: disable=c0103
"""anomaly detection unit test"""
import unittest, sys, os.path
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.algo.anomaly import draw_ad_results
from taosanalytics.conf import setup_log_info, app_logger
from taosanalytics.servicemgmt import loader


class AnomalyDetectionTest(unittest.TestCase):
    """ anomaly detection unit test class"""
    input_list = [5, 14, 15, 15, 14, 19, 17, 16, 20, 22, 8, 21, 28, 11, 9, 29, 40]
    large_list = [
        13, 14, 8, 10, 16, 26, 32, 27, 18, 32, 36, 24,
        22, 23, 22, 18, 25, 21, 21, 14, 8, 11, 14, 23,
        18, 17, 19, 20, 22, 19, 13, 26, 13, 14, 22, 24,
        21, 22, 26, 21, 23, 24, 27, 41, 31, 27, 35, 26,
        28, 36, 39, 21, 17, 22, 17, 19, 15, 34, 10, 15,
        22, 18, 15, 20, 15, 22, 19, 16, 30, 27, 29, 23,
        20, 16, 21, 21, 25, 16, 18, 15, 18, 14, 10, 15,
        8, 15, 6, 11, 8, 7, 13, 10, 23, 16, 15, 25,
        22, 20, 16
    ]

    @classmethod
    def setUpClass(cls):
        """ set up environment for unit test, set the log file path """
        setup_log_info("unit_test.log")
        loader.load_all_service()

    def test_ksigma(self):
        """
        Test the ksigma algorithm for anomaly detection. This test case verifies the
        functionality of the ksigma algorithm by setting up the input data,
        executing the algorithm, and asserting the expected results.
        """
        s = loader.get_service("ksigma")
        self.assertTrue(s is not None, "failed to get the ksigma service")

        s.set_input_list(AnomalyDetectionTest.input_list, None)
        s.set_params({"k": 2})

        r = s.execute()
        draw_ad_results(AnomalyDetectionTest.input_list, r, "ksigma", s.valid_code)

        self.assertEqual(r[-1], -1)
        self.assertEqual(len(r), len(AnomalyDetectionTest.input_list))

    def test_iqr(self):
        """
        Test the IQR(Interquartile Range) algorithm for anomaly detection. This test case verifies the functionality
        of the IQR algorithm by setting up the input data, executing the algorithm, and asserting the expected results.
        """

        s = loader.get_service("iqr")
        s.set_input_list(AnomalyDetectionTest.input_list, None)

        try:
            s.set_params({"k": 2})
        except ValueError as e:
            self.assertEqual(1, 0, e)

        r = s.execute()
        draw_ad_results(AnomalyDetectionTest.input_list, r, "iqr", s.valid_code)

        self.assertEqual(r[-1], -1)
        self.assertEqual(len(r), len(AnomalyDetectionTest.input_list))

    def test_grubbs(self):
        """
        Test the Grubbs algorithm for anomaly detection.

        This test case verifies the functionality of the Grubbs algorithm by setting up the input data,
        executing the algorithm, and asserting the expected results.
        """

        s = loader.get_service("grubbs")
        s.set_input_list(AnomalyDetectionTest.input_list, None)
        s.set_params({"alpha": 0.95})

        r = s.execute()
        draw_ad_results(AnomalyDetectionTest.input_list, r, "grubbs", s.valid_code)

        self.assertEqual(r[-1], -1)
        self.assertEqual(len(r), len(AnomalyDetectionTest.input_list))

    def test_shesd(self):
        """
        Test the SHESD (Seasonal Hybrid ESD) algorithm for anomaly detection.

        This test case verifies the functionality of the SHESD algorithm by setting up the input data,
        executing the algorithm, and asserting the expected results.
        """

        s = loader.get_service("shesd")

        ver = sys.version_info

        # Python3.12 not loaded shesd service
        if (ver.major, ver.minor) == (3, 12):
            self.assertTrue(s is None)
        else:
            s.set_params({"period": 3})
            s.set_input_list(AnomalyDetectionTest.input_list, None)

            r = s.execute()
            draw_ad_results(AnomalyDetectionTest.input_list, r, "shesd", s.valid_code)

            self.assertEqual(r[-1], -1)

    def test_lof(self):
        """
        Test the LOF (Local Outlier Factor) algorithm for anomaly detection.

        This test case verifies the functionality of the LOF algorithm by setting up the input data,
        executing the algorithm, and asserting the expected results.
        """
        s = loader.get_service("lof")
        s.set_params({"period": 3})
        s.set_input_list(AnomalyDetectionTest.input_list, None)

        r = s.execute()
        draw_ad_results(AnomalyDetectionTest.input_list, r, "lof", s.valid_code)

        self.assertEqual(r[-1], -1)
        self.assertEqual(r[-2], -1)

    def test_lof_multivariate(self):
        """Test LOF with multi-variate input data (2 features)."""
        s = loader.get_service("lof")

        feature_1 = [1.0, 1.1, 0.9, 1.2, 1.0, 1.1, 0.95, 1.05, 1.0, 30.0]
        feature_2 = [2.0, 2.1, 1.9, 2.0, 2.2, 2.1, 1.95, 2.05, 2.0, 45.0]
        input_features = [feature_1, feature_2]

        s.set_input_list(input_features, None)
        s.set_params({"neighbors": 5, "algorithm": "auto"})

        r = s.execute()

        self.assertEqual(len(r), len(feature_1))
        self.assertTrue(all(x in (-1, 1) for x in r))
        self.assertEqual(r[-1], -1)
        self.assertIn(1, r[:-1])
        self.assertIn(-1, r)

    def test_lof_multivariate_three_features(self):
        """Test LOF with multi-variate input data (3 features)."""
        s = loader.get_service("lof")

        feature_1 = [10.0, 10.2, 9.9, 10.1, 10.0, 9.8, 10.1, 10.0, 10.2, 40.0]
        feature_2 = [20.0, 20.1, 19.8, 20.0, 20.2, 19.9, 20.0, 20.1, 19.95, 55.0]
        feature_3 = [30.0, 30.1, 29.9, 30.0, 30.2, 29.8, 30.0, 30.1, 30.0, 70.0]

        s.set_input_list([feature_1, feature_2, feature_3], None)
        s.set_params({"neighbors": 5, "algorithm": "auto"})

        r = s.execute()

        self.assertEqual(len(r), len(feature_1))
        self.assertTrue(all(x in (-1, 1) for x in r))
        self.assertEqual(r[-1], -1)

    def test_lof_multivariate_invalid_input_length(self):
        """Test multi-variate anomaly input rejects non-equal feature lengths."""
        s = loader.get_service("lof")

        with self.assertRaises(ValueError):
            s.set_input_list([
                [1.0, 2.0, 3.0, 4.0],
                [1.0, 2.0, 3.0]
            ], None)

    def test_multithread_safe(self):
        """ Test the multithread safe function"""
        s1 = loader.get_service("ksigma")
        s2 = loader.get_service("ksigma")

        s1.set_params({"k": 1})
        self.assertNotEqual(s1.k_val, s2.k_val)

    def __load_remote_data_for_ad(self):
        """load the remote data for anomaly detection"""

        url = ("https://raw.githubusercontent.com/numenta/NAB/master/data/artificialWithAnomaly/"
               "art_daily_jumpsup.csv")

        remote_data = pd.read_csv(url, parse_dates=True, index_col="timestamp")
        k = remote_data.values.ravel().tolist()
        return k

    def test_autoencoder_ad(self):
        """for local test only, disabled it in github action"""
        pass 
        # data = self.__load_remote_data_for_ad()
        #
        # s = loader.get_service("sample_ad_model")
        # s.set_input_list(data)
        #
        # try:
        #     s.set_params({"model": "sample-ad-autoencoder"})
        # except ValueError as e:
        #     app_logger.log_inst.error(f"failed to set the param for auto_encoder algorithm, reason:{e}")
        #     return
        #
        # r = s.execute()
        #
        # num_of_error = -(sum(filter(lambda x: x == -1, r)))
        # draw_ad_results(data, r, "autoencoder")
        #
        # self.assertEqual(num_of_error, 109)

    def test_get_all_services(self):
        """Test get all services"""
        loader.get_anomaly_detection_algo_list()


if __name__ == '__main__':
    unittest.main()
