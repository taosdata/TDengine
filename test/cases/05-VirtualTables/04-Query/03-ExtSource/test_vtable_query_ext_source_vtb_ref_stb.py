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


class _ExtSourceVtbRefStbQueryBase:
    @classmethod
    def setup_class(cls):
        cls._prepare_vtables()

    @classmethod
    def _prepare_vtables(cls):
        raise NotImplementedError

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

    def test_projection(self):
        """Query ext-source virtual stable projection through virtual references."""
        self.run_normal_query("test_vstable_select_test_projection")

    def test_projection_filter(self):
        """Query ext-source virtual stable projection filter through virtual references."""
        self.run_normal_query("test_vstable_select_test_projection_filter")

    def test_projection_timerange_filter(self):
        """Query ext-source virtual stable projection timerange filter through virtual references."""
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")

    def test_function_cols(self):
        """Query ext-source virtual stable column functions through virtual references."""
        self.run_normal_query("test_vstable_select_test_function_cols")

    def test_function_tags(self):
        """Query ext-source virtual stable tag functions through virtual references."""
        self.run_normal_query("test_vstable_select_test_function_tags")

    def test_partition(self):
        """Query ext-source virtual stable partition through virtual references."""
        self.run_normal_query("test_vstable_select_test_partition")

    def test_group(self):
        """Query ext-source virtual stable group through virtual references."""
        self.run_normal_query("test_vstable_select_test_group")

    def test_orderby(self):
        """Query ext-source virtual stable order by through virtual references."""
        self.run_normal_query("test_vstable_select_test_orderby")

    def test_interval(self):
        """Query ext-source virtual stable interval through virtual references."""
        self.run_normal_query("test_vstable_select_test_interval")

    def test_session(self):
        """Query ext-source virtual stable session through virtual references."""
        self.run_normal_query("test_vstable_select_test_session")

    def test_event(self):
        """Query ext-source virtual stable event through virtual references."""
        self.run_normal_query("test_vstable_select_test_event")

    def test_count(self):
        """Query ext-source virtual stable count through virtual references."""
        self.run_normal_query("test_vstable_select_test_count")

    def test_state_mode_0(self):
        """Query ext-source virtual stable state mode 0 through virtual references."""
        self.run_normal_query("test_vstable_select_test_state_mode_0")

    def test_state_mode_2(self):
        """Query ext-source virtual stable state mode 2 through virtual references."""
        self.run_normal_query("test_vstable_select_test_state_mode_2")


class TestVTableQueryExtSourceInfluxVtbRefStb(_ExtSourceVtbRefStbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_virtual_ref_vtables("influx")


class TestVTableQueryExtSourceMysqlVtbRefStb(_ExtSourceVtbRefStbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_virtual_ref_vtables("mysql")


class TestVTableQueryExtSourcePgVtbRefStb(_ExtSourceVtbRefStbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_virtual_ref_vtables("postgres")
