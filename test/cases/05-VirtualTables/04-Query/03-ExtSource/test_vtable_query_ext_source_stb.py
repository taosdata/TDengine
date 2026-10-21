# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
import os
import sys

import pytest

from new_test_framework.utils import tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import VtableQueryUtil


pytestmark = pytest.mark.timeout(3600)

_MULTI_RESULT_TAG_QUERIES = {
    "test_vstable_select_test_projection",
    "test_vstable_select_test_projection_filter",
    "test_vstable_select_test_projection_timerange_filter",
    "test_vstable_select_test_interval",
    "test_vstable_select_test_session",
    "test_vstable_select_test_event",
    "test_vstable_select_test_count",
    "test_vstable_select_test_state_mode_0",
    "test_vstable_select_test_state_mode_2",
}


class _ExtSourceStbQueryMixin:
    def run_normal_query(self, test_case):
        tdLog.info(f"test case : {test_case}.")
        if test_case in _MULTI_RESULT_TAG_QUERIES:
            tdSql.execute('alter local "multiResultFunctionStarReturnTags" "1";')
        case_dir = os.path.dirname(os.path.dirname(__file__))
        sql_file = os.path.join(case_dir, "in", f"{test_case}.in")
        ans_file = os.path.join(case_dir, "ans", f"{test_case}.ans")
        VtableQueryUtil.compare_ext_source_testcase_result(
            sql_file, ans_file, test_case, float_tolerance=1e-1
        )


class _ExtSourceStbBasicQueryBase(_ExtSourceStbQueryMixin):
    @classmethod
    def setup_class(cls):
        cls._prepare_vtables()

    @classmethod
    def _prepare_vtables(cls):
        raise NotImplementedError

    def test_projection(self):
        """Query ext-source virtual stable projection.

        Query ext-source virtual stable projection.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_projection")

    def test_projection_filter(self):
        """Query ext-source virtual stable projection filter.

        Query ext-source virtual stable projection filter.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_projection_filter")

    def test_projection_timerange_filter(self):
        """Query ext-source virtual stable projection timerange filter.

        Query ext-source virtual stable projection timerange filter.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")

    def test_function_cols(self):
        """Query ext-source virtual stable column functions.

        Query ext-source virtual stable column functions.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_function_cols")

    def test_function_tags(self):
        """Query ext-source virtual stable tag functions.

        Query ext-source virtual stable tag functions.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_function_tags")

    def test_partition(self):
        """Query ext-source virtual stable partition.

        Query ext-source virtual stable partition.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_partition")

    def test_group(self):
        """Query ext-source virtual stable group.

        Query ext-source virtual stable group.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_group")

    def test_orderby(self):
        """Query ext-source virtual stable order by.

        Query ext-source virtual stable order by.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_orderby")

    def test_interval(self):
        """Query ext-source virtual stable interval.

        Query ext-source virtual stable interval.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_interval")

    def test_session(self):
        """Query ext-source virtual stable session.

        Query ext-source virtual stable session.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_session")

    def test_event(self):
        """Query ext-source virtual stable event.

        Query ext-source virtual stable event.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_event")

    def test_count(self):
        """Query ext-source virtual stable count.

        Query ext-source virtual stable count.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_count")

    def test_state_mode_0(self):
        """Query ext-source virtual stable state mode 0.

        Query ext-source virtual stable state mode 0.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_state_mode_0")

    def test_state_mode_2(self):
        """Query ext-source virtual stable state mode 2.

        Query ext-source virtual stable state mode 2.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_state_mode_2")


class _ExtSourceStbAggQueryBase(_ExtSourceStbQueryMixin):
    @classmethod
    def setup_class(cls):
        cls._prepare_vtables()

    @classmethod
    def _prepare_vtables(cls):
        raise NotImplementedError

    def test_agg(self):
        """Query ext-source virtual stable aggregate.

        Query ext-source virtual stable aggregate.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_agg")

    def test_agg_group(self):
        """Query ext-source virtual stable aggregate group by.

        Query ext-source virtual stable aggregate group by.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_agg_group_by")

    def test_agg_tag_filter(self):
        """Query ext-source virtual stable aggregate tag filter.

        Query ext-source virtual stable aggregate tag filter.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_agg_tag_cond")

    def test_agg_time_filter(self):
        """Query ext-source virtual stable aggregate time filter.

        Query ext-source virtual stable aggregate time filter.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_agg_time_cond")

    def test_agg_partition_expr(self):
        """Query ext-source virtual stable aggregate partition expression.

        Query ext-source virtual stable aggregate partition expression.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vstable_select_test_agg_partition_expr")


class TestVTableQueryExtSourceInfluxStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("influx")


class TestVTableQueryExtSourceMysqlStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("mysql")


class TestVTableQueryExtSourcePgStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("postgres")


# SQL Server is a step-1 (zero pushdown) source: predicates and aggregations
# stay local and the remote SQL is a plain base scan, so the shared golden
# files apply unchanged. Its external timestamps are microsecond-precision.
class TestVTableQueryExtSourceSqlserverStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("sqlserver")


class TestVTableQueryExtSourceInfluxMixedStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("influx")


class TestVTableQueryExtSourceMysqlMixedStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("mysql")


class TestVTableQueryExtSourcePgMixedStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("postgres")


class TestVTableQueryExtSourceSqlserverMixedStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("sqlserver")


class TestVTableQueryExtSourceAllStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_all_ext_source_vtables()

    def test_projection_timerange_filter(self):
        """Projection with a timestamp-range filter over the all-sources table.

        Runs the shared projection/range case where every column is backed by a
        different source (the multi-source cycle includes SQL Server), so the query
        exercises cross-source column binding end to end.

        Since: v3.4.4.0

        """
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")


class TestVTableQueryExtSourceInfluxStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("influx", mode=2)


class TestVTableQueryExtSourceMysqlStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("mysql", mode=2)


class TestVTableQueryExtSourcePgStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("postgres", mode=2)


class TestVTableQueryExtSourceSqlserverStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("sqlserver", mode=2)


class TestVTableQueryExtSourceInfluxMixedStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("influx", mode=2)


class TestVTableQueryExtSourceMysqlMixedStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("mysql", mode=2)


class TestVTableQueryExtSourcePgMixedStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("postgres", mode=2)


class TestVTableQueryExtSourceSqlserverMixedStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("sqlserver", mode=2)


class TestVTableQueryExtSourceAllStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_all_ext_source_vtables(mode=2)

    def test_projection_timerange_filter(self):
        """Projection with a timestamp-range filter over the all-sources table.

        Runs the shared projection/range case where every column is backed by a
        different source (the multi-source cycle includes SQL Server), so the query
        exercises cross-source column binding end to end.

        Since: v3.4.4.0

        """
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")
