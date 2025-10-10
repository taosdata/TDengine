# encoding:utf-8
# pylint: disable=c0103
"""anomaly detection unit test"""
import unittest, sys, os.path

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.conf import setup_log_info, app_logger
from taosanalytics.servicemgmt import loader


def get_ccf(list_1: list, list_2: list, lags_start, lags_end):
    ccf_vals = []
    if len(list_1) != len(list_2) or len(list_1) == 0 or len(list_2) == 0:
        return ccf_vals

    n = len(list_1)
    for lag in np.arange(lags_start, lags_end):
        x1 = list_1[max(0, -lag):n - lag if lag < 0 else n - lag]
        y1 = list_2[max(0, lag):n if lag > 0 else n + lag]

        ccf_vals.append(np.corrcoef(x1, y1)[0, 1])

    return ccf_vals

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

        print(val)
        print(path)

        # self.assertEqual(len(r), len(AnomalyDetectionTest.input_list))
    def test_tlcc(self):
        from statsmodels.tsa.stattools import ccf
        import numpy as np

        # 生成示例数据（y 是 x 的滞后版本）
        x = np.random.randn(100)
        y = np.roll(x, 3) #+ 0.1 * np.random.randn(100)  # 滞后3个单位

        # 计算TLCC（滞后范围：-10 到 10）
        max_lag = 10
        tlcc_values = ccf(x, y, adjusted=True, nlags=10)#[:max_lag * 2 + 1]
        lags = np.arange(-max_lag, max_lag)

        tlcc_values_1 = ccf(x, y, adjusted=True)
        res = []
        for i in range(10):
            res.append(np.corrcoef(x, np.roll(x, i))[0, 1])


        ccf_vals = get_ccf(x, y, -10, 10)

        plt.figure(figsize=(10, 5))
        plt.stem(lags, ccf_vals)
        plt.axhline(0, color='black', linewidth=1)
        plt.axvline(0, color='gray', linestyle='--')
        plt.title('Cross-Correlation Function (CCF)')
        plt.xlabel('Lag (k)')
        plt.ylabel('Correlation')
        plt.grid(True)
        plt.savefig('aaa11.png')

        shifted_x = x.shift(1)
        window = pd.concat([shifted_x, y], axis=1).dropna()
        corr_x = np.corrcoef(window.iloc[:, 0], window.iloc[:, 1])[0, 1]

        print(tlcc_values)
        print(lags)

    def test_1(self):
        # === 1. 构造两个时间序列 ===
        np.random.seed(42)
        n = 100

        # x_t: 随机波动的基础序列
        x = np.sin(np.linspace(0, 10, n)) + np.random.normal(0, 0.2, n)

        # y_t: 延迟版本的 x_t，加上一点噪声
        # 这里我们人为设定 y 落后 x 3 期
        y = np.roll(x, 3) + np.random.normal(0, 0.2, n)

        plt.plot(x)
        plt.plot(y)
        plt.savefig('data.png')
        plt.clf()

        # y[:3] = np.nan  # 由于前3个是移位产生的空值

        # === 2. 计算 CCF ===
        # statsmodels.ccf 默认计算的是正方向的滞后关系（即 x 领先 y）
        lags = np.arange(-20, 21)  # 自定义滞后范围
        ccf_vals = []
        for lag in lags:
            x1 = x[max(0, -lag):n - lag if lag < 0 else n - lag]
            y1 = y[max(0, lag):n if lag > 0 else n + lag]
            ccf_vals.append(np.corrcoef(x1, y1)[0, 1])


        # === 3. 绘图 ===
        plt.figure(figsize=(10, 5))
        plt.stem(lags, ccf_vals)
        plt.axhline(0, color='black', linewidth=1)
        plt.axvline(0, color='gray', linestyle='--')
        plt.title('Cross-Correlation Function (CCF)')
        plt.xlabel('Lag (k)')
        plt.ylabel('Correlation')
        plt.grid(True)
        plt.savefig('aaa.png')
        # plt.show()
