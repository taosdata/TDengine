import os
import numpy as np
import pytest
from pathlib import Path
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.algo.tool.batch import do_batch_process, get_default_config

############################################
# 边界测试用例
############################################

class TestBoundaryConditions:
    """边界条件测试"""

    def test_empty_input_data(self):
        """test case:空输入数据"""
        time = np.array([])
        values = np.array([])
        windows = [(0, 10)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 空数据应该返回空结果
        assert len(batches) == 0
        assert len(center) == 0
        assert len(lower) == 0
        assert len(upper) == 0

    def test_empty_windows(self):
        """test case:空时间窗口列表"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = []
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0
        assert len(center) == 0

    def test_single_data_point(self):
        """test case:单个数据点"""
        time = np.array([1.0])
        values = np.array([5.0])
        windows = [(0, 2)]
        config = get_default_config()

        # 单个点应该被过滤掉（< 10 个点的阈值）
        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0

    def test_exactly_threshold_points(self):
        """test case:刚好达到阈值的数据点（10个）"""
        time = np.linspace(0, 10, 10)
        values = np.random.randn(10)
        windows = [(0, 10)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 10个点应该可以处理
        assert len(batches) == 1

    def test_below_threshold_points(self):
        """test case:少于阈值的数据点（9个）"""
        time = np.linspace(0, 10, 9)
        values = np.random.randn(9)
        windows = [(0, 10)]
        config = get_default_config()

        # 9个点应该被过滤掉
        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0

    def test_window_with_no_data(self):
        """test case:时间窗口内没有数据"""
        time = np.linspace(0, 10, 100)
        values = np.sin(time)
        windows = [(20, 30)]  # 超出数据范围
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0

    def test_overlapping_windows(self):
        """test case:重叠的时间窗口"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (0, 50),
            (25, 75),  # 与第一个窗口重叠
            (50, 100)
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 应该有3个批次
        assert len(batches) == 3

    def test_adjacent_windows(self):
        """test case:相邻的时间窗口"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (0, 33),
            (33, 66),
            (66, 100)
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 3

    def test_single_window(self):
        """test case:单个时间窗口"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 1
        assert len(center) == config["normalize"]["target_len"]

    def test_very_small_window(self):
        """test case:非常小的时间窗口"""
        time = np.linspace(0, 100, 10000)
        values = np.sin(time / 10)
        windows = [(50, 50.1)]  # 很小的窗口
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 可能有足够的点，也可能不够
        # 这取决于采样率

    def test_window_at_data_boundary(self):
        """test case:时间窗口在数据边界"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (0, 10),  # 开始边界
            (90, 100)  # 结束边界
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 2

    def test_partial_window_overlap_with_data(self):
        """test case:部分窗口与数据重叠"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (-10, 10),  # 部分在范围外
            (90, 110)  # 部分在范围外
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 应该处理重叠部分的数据
        assert len(batches) <= 2


############################################
# 异常测试用例
############################################

class TestExceptionHandling:
    """异常处理测试"""

    def test_mismatched_time_values_length(self):
        """test case:时间和数值数组长度不匹配"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)[:500]  # 长度不匹配
        windows = [(0, 100)]
        config = get_default_config()

        # 应该抛出异常或处理不匹配
        with pytest.raises((ValueError, IndexError)):
            do_batch_process(time, values, windows, config)

    def test_nan_in_values(self):
        """test case:数值中包含NaN"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        values[100:200] = np.nan  # 插入NaN
        windows = [(0, 100)]
        config = get_default_config()

        # NaN可能导致问题，应该处理或抛出异常
        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
            # 如果没有抛出异常，检查结果是否包含NaN
            if len(batches) > 0:
                assert not np.any(np.isnan(batches[0]))
        except (ValueError, RuntimeError):
            pass  # 抛出异常也是可接受的

    def test_inf_in_values(self):
        """test case:数值中包含无穷大"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        values[100] = np.inf
        values[200] = -np.inf
        windows = [(0, 100)]
        config = get_default_config()

        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
            if len(batches) > 0:
                assert not np.any(np.isinf(batches[0]))
        except (ValueError, RuntimeError):
            pass

    def test_non_monotonic_time(self):
        """test case:非单调递增的时间序列"""
        time = np.array([0, 1, 2, 1.5, 3, 4])  # 不是单调的
        values = np.array([1, 2, 3, 4, 5, 6])
        windows = [(0, 5)]
        config = get_default_config()

        # 可能导致插值问题
        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
        except (ValueError, RuntimeError):
            pass

    def test_invalid_window_format(self):
        """test case:无效的时间窗口格式"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (10,),  # 缺少结束时间
            (20, 30, 40)  # 多余的参数
        ]
        config = get_default_config()

        with pytest.raises((ValueError, IndexError, TypeError)):
            do_batch_process(time, values, windows, config)

    def test_reversed_window(self):
        """test case:反向的时间窗口（end < start）"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(50, 20)]  # 结束时间 < 开始时间
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 应该没有数据或正确处理
        assert len(batches) == 0

    def test_missing_config_fields(self):
        """test case:配置缺少必要字段"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = {}  # 空配置

        with pytest.raises((KeyError, ValueError)):
            do_batch_process(time, values, windows, config)

    def test_invalid_hampel_window_size(self):
        """test case:无效的Hampel窗口大小"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        # test invalid window size
        config["hampel"]["window_size"] = -5

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_invalid_savgol_parameters(self):
        """test case:无效的Savitzky-Golay滤波器参数"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        # polyorder >= window_length (invalid)
        config["savgol"]["window"] = 5
        config["savgol"]["polyorder"] = 5

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_even_savgol_window(self):
        """test case: Savitzky-Golay window size is even"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        # Savgol window must be odd
        config["savgol"]["window"] = 10

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_zero_target_length(self):
        """test case:归一化目标长度为0"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        config["normalize"]["target_len"] = 0

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_negative_target_length(self):
        """test case:归一化目标长度为负数"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        config["normalize"]["target_len"] = -100

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)


############################################
# 特殊场景测试
############################################

class TestSpecialScenarios:
    """特殊场景测试"""

    def test_all_windows_filtered_out(self):
        """test case:所有窗口都被过滤掉"""
        time = np.linspace(0, 100, 100)
        values = np.random.randn(100)

        # 创建很多小窗口，每个都少于10个点
        windows = [(i, i + 0.5) for i in range(0, 100, 10)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0
        assert len(center) == 0

    def test_extreme_outliers(self):
        """test case:极端异常值"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        values[500] = 1e10  # 极端异常值
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # Hampel滤波器应该处理异常值
        assert len(batches) > 0
        assert np.max(batches[0]) < 1e5  # 异常值应该被处理

    def test_constant_values(self):
        """test case:常数值"""
        time = np.linspace(0, 100, 1000)
        values = np.ones(1000) * 5.0  # 所有值相同
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0
        # 常数应该被保持
        assert np.allclose(batches[0], 5.0, atol=0.1)

    def test_high_frequency_noise(self):
        """test case:高频噪声"""
        time = np.linspace(0, 100, 10000)
        values = np.sin(time / 10) + np.sin(time * 50) * 0.5  # 高频噪声
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0
        # Savgol滤波器应该平滑高频噪声

    def test_step_function(self):
        """test case:阶跃函数"""
        time = np.linspace(0, 100, 1000)
        values = np.ones(1000)
        values[500:] = 10.0  # 阶跃
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0

    def test_multiple_batches_different_characteristics(self):
        """test case:多个特征不同的批次"""
        time = np.linspace(0, 300, 3000)
        values = np.concatenate([
            np.sin(time[:1000] / 10),  # 正弦波
            np.random.randn(1000) * 2,  # 随机噪声
            np.linspace(0, 10, 1000)  # 线性趋势
        ])
        windows = [(0, 100), (100, 200), (200, 300)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 3
        # Golden batch应该能处理不同特征的批次

    def test_very_large_dataset(self):
        """test case:非常大的数据集"""
        time = np.linspace(0, 1000, 100000)
        values = np.sin(time / 100)
        windows = [(0, 1000)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0

    def test_cubic_interpolation_with_few_points(self):
        """test case:数据点较少时的三次插值"""
        time = np.linspace(0, 100, 15)  # 只有15个点
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        config["normalize"]["method"] = "cubic"

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 少于4个点时应该降级到线性插值
        assert len(batches) > 0


    def test_zero_derivative_rate_limit(self):
        """test case: th derivative rate limitation is 0"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()
        config["derivative"]["max_rate"] = 0

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # all data points are filtered out
        assert len(batches) == 0 or len(batches[0]) < 10


############################################
# 数据质量测试
############################################

class TestDataQuality:
    """data quality test"""

    def test_sparse_data(self):
        """test case: sparse data"""
        time = np.array([0, 10, 20, 50, 80, 100])
        values = np.array([1, 2, 3, 4, 5, 6])
        windows = [(0, 100)]
        config = get_default_config()

        # 可能因为点太少被过滤
        center, lower, upper, batches = do_batch_process(time, values, windows, config)

    def test_irregular_sampling(self):
        """test case:不规则采样"""
        time = np.sort(np.random.uniform(0, 100, 1000))
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0

    def test_duplicate_timestamps(self):
        """test case:重复的时间戳"""
        time = np.array([0, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        values = np.random.randn(12)
        windows = [(0, 10)]
        config = get_default_config()

        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
        except (ValueError, RuntimeError):
            pass  # 插值可能失败

    def test_missing_data_segments(self):
        """test case:缺失数据段"""
        time1 = np.linspace(0, 30, 300)
        time2 = np.linspace(70, 100, 300)
        time = np.concatenate([time1, time2])
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0


############################################
# 配置参数测试
############################################

class TestConfigurationParameters:
    """配置参数测试"""

    def test_different_golden_methods(self):
        """test case:不同的Golden Batch方法"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 50), (50, 100)]

        # 测试 mean_std 方法
        config1 = get_default_config()
        config1["golden"]["method"] = "mean_std"
        config1["golden"]["n_std"] = 2

        center1, lower1, upper1, batches1 = do_batch_process(time, values, windows, config1)

        # 测试 median_percentile 方法
        config2 = get_default_config()
        config2["golden"]["method"] = "median_percentile"
        config2["golden"]["lower_percentile"] = 10
        config2["golden"]["upper_percentile"] = 90

        center2, lower2, upper2, batches2 = do_batch_process(time, values, windows, config2)

        # 两种方法都应该产生结果
        assert len(batches1) == 2
        assert len(batches2) == 2

    def test_extreme_percentiles(self):
        """test case:极端的百分位数"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 50), (50, 100)]
        config = get_default_config()

        config["golden"]["method"] = "median_percentile"
        config["golden"]["lower_percentile"] = 0
        config["golden"]["upper_percentile"] = 100

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 2

    def test_plot_disabled(self):
        """test case:禁用绘图"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()
        config["plot"] = False

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0
