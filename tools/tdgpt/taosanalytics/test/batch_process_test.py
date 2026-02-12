import os
import numpy as np
import pytest
from pathlib import Path
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from taosanalytics.algo.tool.batch import do_batch_process, get_default_config

############################################
# boundary test cases
############################################

class TestBoundaryConditions:
    """Tests for boundary conditions"""

    def test_empty_input_data(self):
        """test case: empty input"""
        time = np.array([])
        values = np.array([])
        windows = [(0, 10)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0
        assert len(center) == 0
        assert len(lower) == 0
        assert len(upper) == 0

    def test_empty_windows(self):
        """test case: empty time window list"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = []
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0
        assert len(center) == 0

    def test_single_data_point(self):
        """test case: single data point"""
        time = np.array([1.0])
        values = np.array([5.0])
        windows = [(0, 2)]
        config = get_default_config()

        # less than threshold (< 10)
        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0

    def test_exactly_threshold_points(self):
        """test case: exactly 10 data points"""
        time = np.linspace(0, 10, 10)
        values = np.random.randn(10)
        windows = [(0, 10)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # 10 points should be processed
        assert len(batches) == 1

    def test_below_threshold_points(self):
        """test case: below threshold (9 points)"""
        time = np.linspace(0, 10, 9)
        values = np.random.randn(9)
        windows = [(0, 10)]
        config = get_default_config()

        # 9 points should be filtered out
        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0

    def test_window_with_no_data(self):
        """test case: no data in time window"""
        time = np.linspace(0, 10, 100)
        values = np.sin(time)
        windows = [(20, 30)]  # out of range
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0

    def test_overlapping_windows(self):
        """test case: overlapping time windows"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (0, 50),
            (25, 75),  # overlap with first window
            (50, 100)
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 3

    def test_adjacent_windows(self):
        """test case: adjacent time windows"""
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
        """test case: single time window"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 1
        assert len(center) == config["normalize"]["target_len"]

    def test_very_small_window(self):
        """test case: very small time window"""
        time = np.linspace(0, 100, 10000)
        values = np.sin(time / 10)
        windows = [(50, 50.1)]  # very small window
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)


    def test_window_at_data_boundary(self):
        """test case: time window at data boundary"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (0, 10),  # start boundary
            (90, 100)  # end boundary
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 2

    def test_partial_window_overlap_with_data(self):
        """test case: partial window overlap with data"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (-10, 10),  # partially out of range
            (90, 110)  # partially out of range
        ]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # should process overlapping data
        assert len(batches) <= 2


############################################
# exceptional cases
############################################

class TestExceptionHandling:
    """exceptional cases handling"""

    def test_mismatched_time_values_length(self):
        """test case: time and values array length mismatch"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)[:500]  # length mismatch
        windows = [(0, 100)]
        config = get_default_config()

        # should raise exception or handle mismatch
        with pytest.raises((ValueError, IndexError)):
            do_batch_process(time, values, windows, config)

    def test_nan_in_values(self):
        """test case: NaN values in data"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        values[100:200] = np.nan  # insert NaN
        windows = [(0, 100)]
        config = get_default_config()

        # NaN may cause issues, should handle or raise exception
        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
            # if no exception raised, check results for NaN
            if len(batches) > 0:
                assert not np.any(np.isnan(batches[0]))
        except (ValueError, RuntimeError):
            pass  # raising exception is also acceptable

    def test_inf_in_values(self):
        """test case: infinity values in data"""
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
        """test case: non-monotonic time sequence"""
        time = np.array([0, 1, 2, 1.5, 3, 4])  # not monotonic
        values = np.array([1, 2, 3, 4, 5, 6])
        windows = [(0, 5)]
        config = get_default_config()

        # may cause interpolation issues
        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
        except (ValueError, RuntimeError):
            pass

    def test_invalid_window_format(self):
        """test case: invalid time window format"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [
            (10,),  # missing end time
            (20, 30, 40)  # extra parameter
        ]
        config = get_default_config()

        with pytest.raises((ValueError, IndexError, TypeError)):
            do_batch_process(time, values, windows, config)

    def test_reversed_window(self):
        """test case: reversed time window (end < start)"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(50, 20)]  # end time < start time
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # should have no data or handle correctly
        assert len(batches) == 0

    def test_missing_config_fields(self):
        """test case: missing required config fields"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = {}  # empty config

        with pytest.raises((KeyError, ValueError)):
            do_batch_process(time, values, windows, config)

    def test_invalid_hampel_window_size(self):
        """test case: invalid Hampel window size"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        # test negative window size
        config["hampel"]["window_size"] = -5

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_invalid_savgol_parameters(self):
        """test case: invalid Savitzky-Golay filter parameters"""
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
        """test case: even Savitzky-Golay window size"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        # Savgol window must be odd
        config["savgol"]["window"] = 10

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_zero_target_length(self):
        """test case: normalization target length is zero"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        config["normalize"]["target_len"] = 0

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)

    def test_negative_target_length(self):
        """test case: negative normalization target length"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        config["normalize"]["target_len"] = -100

        with pytest.raises((ValueError, RuntimeError)):
            do_batch_process(time, values, windows, config)


############################################
# Special cases
############################################

class TestSpecialScenarios:
    """Special scenarios"""

    def test_all_windows_filtered_out(self):
        """test case: all windows filtered out"""
        time = np.linspace(0, 100, 100)
        values = np.random.randn(100)

        # create many small windows, each with fewer than 10 points
        windows = [(i, i + 0.5) for i in range(0, 100, 10)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 0
        assert len(center) == 0

    def test_extreme_outliers(self):
        """test case: extreme outlier values"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        values[500] = 1e10  # extreme outlier
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # Hampel filter should handle outliers
        assert len(batches) > 0
        assert np.max(batches[0]) < 1e5  # outlier should be handled

    def test_constant_values(self):
        """test case: constant values"""
        time = np.linspace(0, 100, 1000)
        values = np.ones(1000) * 5.0  # all values the same
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0
        # constant should be preserved
        assert np.allclose(batches[0], 5.0, atol=0.1)

    def test_high_frequency_noise(self):
        """test case: high frequency noise"""
        time = np.linspace(0, 100, 10000)
        values = np.sin(time / 10) + np.sin(time * 50) * 0.5  # high frequency noise
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0
        # Savgol filter should smooth high frequency noise

    def test_step_function(self):
        """test case: step function"""
        time = np.linspace(0, 100, 1000)
        values = np.ones(1000)
        values[500:] = 10.0  # step change
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0

    def test_multiple_batches_different_characteristics(self):
        """test case: multiple batches with different characteristics"""
        time = np.linspace(0, 300, 3000)
        values = np.concatenate([
            np.sin(time[:1000] / 10),  # sine wave
            np.random.randn(1000) * 2,  # random noise
            np.linspace(0, 10, 1000)  # linear trend
        ])
        windows = [(0, 100), (100, 200), (200, 300)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) == 3
        # Golden batch should handle batches with different characteristics

    def test_very_large_dataset(self):
        """test case: very large dataset"""
        time = np.linspace(0, 1000, 100000)
        values = np.sin(time / 100)
        windows = [(0, 1000)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0

    def test_cubic_interpolation_with_few_points(self):
        """test case: cubic interpolation with few data points"""
        time = np.linspace(0, 100, 15)  # only 15 points
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        config["normalize"]["method"] = "cubic"

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # should fallback to linear interpolation when fewer than 4 points
        assert len(batches) > 0


    def test_zero_derivative_rate_limit(self):
        """test case: derivative rate limitation is zero"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()
        config["derivative"]["max_rate"] = 0

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        # all data points should be filtered out
        assert len(batches) == 0 or len(batches[0]) < 10


############################################
# Data quality tests
############################################

class TestDataQuality:
    """Data quality tests"""

    def test_sparse_data(self):
        """test case: sparse data"""
        time = np.array([0, 10, 20, 50, 80, 100])
        values = np.array([1, 2, 3, 4, 5, 6])
        windows = [(0, 100)]
        config = get_default_config()

        # may be filtered due to insufficient points
        center, lower, upper, batches = do_batch_process(time, values, windows, config)

    def test_irregular_sampling(self):
        """test case: irregular sampling"""
        time = np.sort(np.random.uniform(0, 100, 1000))
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0

    def test_duplicate_timestamps(self):
        """test case: duplicate timestamps"""
        time = np.array([0, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        values = np.random.randn(12)
        windows = [(0, 10)]
        config = get_default_config()

        try:
            center, lower, upper, batches = do_batch_process(time, values, windows, config)
        except (ValueError, RuntimeError):
            pass  # interpolation may fail

    def test_missing_data_segments(self):
        """test case: missing data segments"""
        time1 = np.linspace(0, 30, 300)
        time2 = np.linspace(70, 100, 300)
        time = np.concatenate([time1, time2])
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0


############################################
# Configuration parameter tests
############################################

class TestConfigurationParameters:
    """Configuration parameter tests"""

    def test_different_golden_methods(self):
        """test case: different Golden Batch methods"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 50), (50, 100)]

        # test mean_std method
        config1 = get_default_config()
        config1["golden"]["method"] = "mean_std"
        config1["golden"]["n_std"] = 2

        center1, lower1, upper1, batches1 = do_batch_process(time, values, windows, config1)

        # test median_percentile method
        config2 = get_default_config()
        config2["golden"]["method"] = "median_percentile"
        config2["golden"]["lower_percentile"] = 10
        config2["golden"]["upper_percentile"] = 90

        center2, lower2, upper2, batches2 = do_batch_process(time, values, windows, config2)

        # both methods should produce results
        assert len(batches1) == 2
        assert len(batches2) == 2

    def test_extreme_percentiles(self):
        """test case: extreme percentiles"""
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
        """test case: plot disabled"""
        time = np.linspace(0, 100, 1000)
        values = np.sin(time / 10)
        windows = [(0, 100)]
        config = get_default_config()
        config["plot"] = False

        center, lower, upper, batches = do_batch_process(time, values, windows, config)

        assert len(batches) > 0