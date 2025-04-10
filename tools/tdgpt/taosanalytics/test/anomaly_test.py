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

    def test_multithread_safe(self):
        """ Test the multithread safe function"""
        s1 = loader.get_service("shesd")
        s2 = loader.get_service("shesd")

        s1.set_params({"period": 3})
        self.assertNotEqual(s1.period, s2.period)

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
