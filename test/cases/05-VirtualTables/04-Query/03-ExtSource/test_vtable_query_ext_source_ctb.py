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

from new_test_framework.utils import tdCom, tdLog

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import VtableQueryUtil


pytestmark = pytest.mark.timeout(3600)


class _ExtSourceCtbQueryBase:
    @classmethod
    def setup_class(cls):
        cls._prepare_vtables()

    @classmethod
    def _prepare_vtables(cls):
        raise NotImplementedError

    def run_normal_query(self, test_case):
        tdLog.info(f"test case : {test_case}.")
        case_dir = os.path.dirname(os.path.dirname(__file__))
        sql_file = os.path.join(case_dir, "in", f"{test_case}.in")
        ans_file = os.path.join(case_dir, "ans", f"{test_case}.ans")
        tdCom.compare_testcase_result(
            sql_file, ans_file, test_case, float_tolerance=1e-1
        )

    def test_projection(self):
        """Query ext-source virtual child table projection."""
        self.run_normal_query("test_vctable_select_test_projection")

    def test_projection_filter(self):
        """Query ext-source virtual child table projection filter."""
        self.run_normal_query("test_vctable_select_test_projection_filter")

    def test_projection_timerange_filter(self):
        """Query ext-source virtual child table projection timerange filter."""
        self.run_normal_query("test_vctable_select_test_projection_timerange_filter")

    def test_function(self):
        """Query ext-source virtual child table function."""
        self.run_normal_query("test_vctable_select_test_function")

    def test_interval(self):
        """Query ext-source virtual child table interval."""
        self.run_normal_query("test_vctable_select_test_interval")

    def test_session(self):
        """Query ext-source virtual child table session."""
        self.run_normal_query("test_vctable_select_test_session")

    def test_event(self):
        """Query ext-source virtual child table event."""
        self.run_normal_query("test_vctable_select_test_event")

    def test_count(self):
        """Query ext-source virtual child table count."""
        self.run_normal_query("test_vctable_select_test_count")

    def test_state_mode_0(self):
        """Query ext-source virtual child table state mode 0."""
        self.run_normal_query("test_vctable_select_test_state_mode_0")

    def test_state_mode_1(self):
        """Query ext-source virtual child table state mode 1."""
        self.run_normal_query("test_vctable_select_test_state_mode_1")

    def test_state_mode_2(self):
        """Query ext-source virtual child table state mode 2."""
        self.run_normal_query("test_vctable_select_test_state_mode_2")

    def test_partition(self):
        """Query ext-source virtual child table partition."""
        self.run_normal_query("test_vctable_select_test_partition")

    def test_group(self):
        """Query ext-source virtual child table group."""
        self.run_normal_query("test_vctable_select_test_group")

    def test_orderby(self):
        """Query ext-source virtual child table order by."""
        self.run_normal_query("test_vctable_select_test_orderby")


class TestVTableQueryExtSourceInfluxCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("influx")


class TestVTableQueryExtSourceMysqlCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("mysql")


class TestVTableQueryExtSourcePgCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("postgres")


class TestVTableQueryExtSourceInfluxMixedCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("influx")


class TestVTableQueryExtSourceMysqlMixedCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("mysql")


class TestVTableQueryExtSourcePgMixedCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("postgres")


class TestVTableQueryExtSourceAllCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_all_ext_source_vtables()
