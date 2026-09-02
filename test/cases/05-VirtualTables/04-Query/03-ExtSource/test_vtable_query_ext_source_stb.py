###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import os
import sys

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql

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
        tdCom.compare_testcase_result(
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
        """Query ext-source virtual stable projection."""
        self.run_normal_query("test_vstable_select_test_projection")

    def test_projection_filter(self):
        """Query ext-source virtual stable projection filter."""
        self.run_normal_query("test_vstable_select_test_projection_filter")

    def test_projection_timerange_filter(self):
        """Query ext-source virtual stable projection timerange filter."""
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")

    def test_function_cols(self):
        """Query ext-source virtual stable column functions."""
        self.run_normal_query("test_vstable_select_test_function_cols")

    def test_function_tags(self):
        """Query ext-source virtual stable tag functions."""
        self.run_normal_query("test_vstable_select_test_function_tags")

    def test_partition(self):
        """Query ext-source virtual stable partition."""
        self.run_normal_query("test_vstable_select_test_partition")

    def test_group(self):
        """Query ext-source virtual stable group."""
        self.run_normal_query("test_vstable_select_test_group")

    def test_orderby(self):
        """Query ext-source virtual stable order by."""
        self.run_normal_query("test_vstable_select_test_orderby")

    def test_interval(self):
        """Query ext-source virtual stable interval."""
        self.run_normal_query("test_vstable_select_test_interval")

    def test_session(self):
        """Query ext-source virtual stable session."""
        self.run_normal_query("test_vstable_select_test_session")

    def test_event(self):
        """Query ext-source virtual stable event."""
        self.run_normal_query("test_vstable_select_test_event")

    def test_count(self):
        """Query ext-source virtual stable count."""
        self.run_normal_query("test_vstable_select_test_count")

    def test_state_mode_0(self):
        """Query ext-source virtual stable state mode 0."""
        self.run_normal_query("test_vstable_select_test_state_mode_0")

    def test_state_mode_2(self):
        """Query ext-source virtual stable state mode 2."""
        self.run_normal_query("test_vstable_select_test_state_mode_2")


class _ExtSourceStbAggQueryBase(_ExtSourceStbQueryMixin):
    @classmethod
    def setup_class(cls):
        cls._prepare_vtables()

    @classmethod
    def _prepare_vtables(cls):
        raise NotImplementedError

    def test_agg(self):
        """Query ext-source virtual stable aggregate."""
        self.run_normal_query("test_vstable_select_test_agg")

    def test_agg_group(self):
        """Query ext-source virtual stable aggregate group by."""
        self.run_normal_query("test_vstable_select_test_agg_group_by")

    def test_agg_tag_filter(self):
        """Query ext-source virtual stable aggregate tag filter."""
        self.run_normal_query("test_vstable_select_test_agg_tag_cond")

    def test_agg_time_filter(self):
        """Query ext-source virtual stable aggregate time filter."""
        self.run_normal_query("test_vstable_select_test_agg_time_cond")

    def test_agg_partition_expr(self):
        """Query ext-source virtual stable aggregate partition expression."""
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


class TestVTableQueryExtSourceAllStbBasic(_ExtSourceStbBasicQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_all_ext_source_vtables()


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


class TestVTableQueryExtSourceAllStbAgg(_ExtSourceStbAggQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_all_ext_source_vtables(mode=2)
