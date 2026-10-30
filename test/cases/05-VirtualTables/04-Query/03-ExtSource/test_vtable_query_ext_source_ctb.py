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

from new_test_framework.utils import tdLog

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import VtableQueryUtil


pytestmark = pytest.mark.timeout(3600)


# Oracle coverage map (Oracle TS 5.7.2): TestVTableQueryExtSourceOracleCtb
# and ...OracleMixedCtb below cover FQOR-VTBL-012 (zero-pushdown virtual
# table query) on the child-table form.
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
        VtableQueryUtil.compare_ext_source_testcase_result(
            sql_file, ans_file, test_case, float_tolerance=1e-1,
            bool_text_columns=getattr(self, "NORMALIZE_BOOL_TEXT", False),
            relative_tolerance=getattr(self, "RELATIVE_TOLERANCE", 0.0)
        )

    def test_projection(self):
        """Query ext-source virtual child table projection.

        Query ext-source virtual child table projection.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_projection")

    def test_projection_filter(self):
        """Query ext-source virtual child table projection filter.

        Query ext-source virtual child table projection filter.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_projection_filter")

    def test_projection_timerange_filter(self):
        """Query ext-source virtual child table projection timerange filter.

        Query ext-source virtual child table projection timerange filter.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_projection_timerange_filter")

    def test_function(self):
        """Query ext-source virtual child table function.

        Query ext-source virtual child table function.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_function")

    def test_interval(self):
        """Query ext-source virtual child table interval.

        Query ext-source virtual child table interval.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_interval")

    def test_session(self):
        """Query ext-source virtual child table session.

        Query ext-source virtual child table session.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_session")

    def test_event(self):
        """Query ext-source virtual child table event.

        Query ext-source virtual child table event.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_event")

    def test_count(self):
        """Query ext-source virtual child table count.

        Query ext-source virtual child table count.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_count")

    def test_state_mode_0(self):
        """Query ext-source virtual child table state mode 0.

        Query ext-source virtual child table state mode 0.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_state_mode_0")

    def test_state_mode_1(self):
        """Query ext-source virtual child table state mode 1.

        Query ext-source virtual child table state mode 1.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_state_mode_1")

    def test_state_mode_2(self):
        """Query ext-source virtual child table state mode 2.

        Query ext-source virtual child table state mode 2.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_state_mode_2")

    def test_partition(self):
        """Query ext-source virtual child table partition.

        Query ext-source virtual child table partition.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_partition")

    def test_group(self):
        """Query ext-source virtual child table group.

        Query ext-source virtual child table group.

        Since: v3.4.3.0

        """
        self.run_normal_query("test_vctable_select_test_group")

    def test_orderby(self):
        """Query ext-source virtual child table order by.

        Query ext-source virtual child table order by.

        Since: v3.4.3.0

        """
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


# SQL Server is a step-1 (zero pushdown) source: predicates and aggregations
# stay local and the remote SQL is a plain base scan, so the shared golden
# files apply unchanged. Its external timestamps are microsecond-precision.
class TestVTableQueryExtSourceSqlserverCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("sqlserver")


# Oracle is also a step-1 (zero pushdown) source: predicates and aggregations
# stay local and the remote SQL is a plain base scan, so the shared golden
# files apply unchanged. Its external timestamps are millisecond-precision
# (TIMESTAMP(3)), matching the shared millisecond baselines.
class TestVTableQueryExtSourceOracleCtb(_ExtSourceCtbQueryBase):
    # Oracle has no SQL-level BOOLEAN: bool_col is NUMBER(1,0) -> TINYINT,
    # so its 1/0 values are compared against the goldens' true/false.
    NORMALIZE_BOOL_TEXT = True
    # The goldens print numbers at DOUBLE precision; DECIMAL(19,0) columns
    # (Oracle NUMBER(19,0)) print every digit, so a 19-digit value can differ
    # from the golden by its 15-significant-digit rounding (relative ~1e-15).
    RELATIVE_TOLERANCE = 1e-13

    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ext_source_vtables("oracle")


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


class TestVTableQueryExtSourceSqlserverMixedCtb(_ExtSourceCtbQueryBase):
    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("sqlserver")


class TestVTableQueryExtSourceOracleMixedCtb(_ExtSourceCtbQueryBase):
    # Oracle has no SQL-level BOOLEAN: bool_col is NUMBER(1,0) -> TINYINT,
    # so its 1/0 values are compared against the goldens' true/false.
    NORMALIZE_BOOL_TEXT = True
    # The goldens print numbers at DOUBLE precision; DECIMAL(19,0) columns
    # (Oracle NUMBER(19,0)) print every digit, so a 19-digit value can differ
    # from the golden by its 15-significant-digit rounding (relative ~1e-15).
    RELATIVE_TOLERANCE = 1e-13

    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_mixed_source_vtables("oracle")


class TestVTableQueryExtSourceAllCtb(_ExtSourceCtbQueryBase):
    # Oracle has no SQL-level BOOLEAN: bool_col is NUMBER(1,0) -> TINYINT,
    # so its 1/0 values are compared against the goldens' true/false.
    NORMALIZE_BOOL_TEXT = True
    # The goldens print numbers at DOUBLE precision; DECIMAL(19,0) columns
    # (Oracle NUMBER(19,0)) print every digit, so a 19-digit value can differ
    # from the golden by its 15-significant-digit rounding (relative ~1e-15).
    RELATIVE_TOLERANCE = 1e-13

    @classmethod
    def _prepare_vtables(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_all_ext_source_vtables()

    def test_projection_timerange_filter(self):
        """Projection with a timestamp-range filter over the all-sources table.

        Runs the shared projection/range case where every column is backed by a
        different source (the multi-source cycle includes SQL Server and Oracle),
        so the query exercises cross-source column binding end to end.

        Since: v3.4.4.0

        """
        self.run_normal_query("test_vctable_select_test_projection_timerange_filter")
