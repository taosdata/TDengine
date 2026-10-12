# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from fun_ts_window_case_helper import FunTsWindowCaseHelper


class TestFunTsWithWindow(FunTsWindowCaseHelper):
    def test_fun_ts_csum_with_window(self):
        """
        Verify csum in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        SQL-window partition/order/frame behavior, plus null, numeric-type,
        and interval-specific cases.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        - 2026-09-29 Codex added CSUM SQL-window regression coverage.
        """
        self.run_csum_with_window_case()

    def test_fun_ts_diff_with_window(self):
        """
        Verify diff in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        numeric-type coverage, and partition-by behavior.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_diff_with_window_case()

    def test_fun_ts_derivative_with_window(self):
        """
        Verify derivative in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        partition-by behavior, and unsupported-type rejection.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_derivative_with_window_case()

    def test_fun_ts_interp_with_window(self):
        """
        Verify interp rejection in ordinary window queries.

        Cover ordinary window forms and partition/fill combinations that should
        remain unsupported for interp.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_interp_with_window_case()

    def test_fun_ts_irate_with_window(self):
        """
        Verify irate in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        partition-by behavior, and unsupported-type rejection.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_irate_with_window_case()

    def test_fun_ts_mavg_with_window(self):
        """
        Verify mavg in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        partition-by behavior, and different moving-window sizes.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_mavg_with_window_case()

    def test_fun_ts_sample_with_window(self):
        """
        Verify sample in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        partition-by behavior, fill rejection, and duplicate-timestamp cases.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_sample_with_window_case()

    def test_fun_ts_statecount_with_window(self):
        """
        Verify statecount in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        partition-by behavior, and operator coverage.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_statecount_with_window_case()

    def test_fun_ts_stateduration_with_window(self):
        """
        Verify stateduration in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics,
        partition-by behavior, and operator coverage.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_stateduration_with_window_case()

    def test_fun_ts_twa_with_window(self):
        """
        Verify twa in ordinary window queries.

        Cover interval/session/state_window/event_window/count_window semantics
        and partition-by behavior.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_twa_with_window_case()

    def test_fun_ts_mix_with_window(self):
        """
        Verify mixed time-series functions in ordinary window queries.

        Cover cross-block golden comparison, compatible same-statement mixes,
        outer-query consumption, and incompatible-function rejection.

        Since: 3.0.0.0

        Catalog: Functions/TimeSeries

        Labels: common,ci

        Jira: None

        History:
        - 2026-04-10 Joey Sima migrated ordinary-window coverage into 04-Timeseries.
        """
        self.run_mix_with_window_case()
