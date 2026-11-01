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
    def run_normal_query(self, test_case, ans_case=None):
        """Run one shared .in case and compare against its committed baseline.

        ans_case lets a setup variant compare against its own baseline when the
        variant legitimately changes the rendered output (mode-2 child tables
        carry different tag values and widened column types).
        """
        tdLog.info(f"test case : {test_case}.")
        if test_case in _MULTI_RESULT_TAG_QUERIES:
            tdSql.execute('alter local "multiResultFunctionStarReturnTags" "1";')
        case_dir = os.path.dirname(os.path.dirname(__file__))
        sql_file = os.path.join(case_dir, "in", f"{test_case}.in")
        ans_file = os.path.join(case_dir, "ans", f"{ans_case or test_case}.ans")
        VtableQueryUtil.compare_ext_source_testcase_result(
            sql_file, ans_file, test_case, float_tolerance=1e-1,
            bool_text_columns=getattr(self, "NORMALIZE_BOOL_TEXT", False),
            relative_tolerance=getattr(self, "RELATIVE_TOLERANCE", 0.0)
        )


# Oracle coverage map (Oracle TS 5.7.2): TestVTableQueryExtSourceOracleStb*
# classes below cover FQOR-VTBL-012 (zero-pushdown virtual super-table
# query) on both the basic and the aggregation shapes.
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


# Oracle is also a step-1 (zero pushdown) source: predicates and aggregations
# stay local and the remote SQL is a plain base scan, so the shared golden
# files apply unchanged. Its external timestamps are millisecond-precision
# (TIMESTAMP(3)), matching the shared millisecond baselines.
class TestVTableQueryExtSourceOracleStbBasic(_ExtSourceStbBasicQueryBase):
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


class TestVTableQueryExtSourceOracleMixedStbBasic(_ExtSourceStbBasicQueryBase):
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


class TestVTableQueryExtSourceAllStbBasic(_ExtSourceStbBasicQueryBase):
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


_VTABLE_SELECT_DB = "test_vtable_select"
_VTABLE_SELECT_TB = "vtb_virtual_stb"


class _OracleDecimalAggQuery:
    """Aggregate coverage for Oracle-backed virtual tables.

    TDengine's stddev()/variance()/spread()/apercentile() reject DECIMAL
    operands, and Oracle NUMBER(19,0) maps to DECIMAL(19,0) (DS 5.3.3), so the
    shared aggregate golden cannot be produced over bigint_col.  Instead of
    skipping the case, the aggregate shapes the engine supports are executed
    and the DECIMAL operand rule is asserted on the column itself: rejected
    when the describing type is DECIMAL, computed when the mixed layout maps
    it to an integer type.
    """

    _DECIMAL_AGG_ERRNO = 0x80002802  # TSDB_CODE_FUNC_FUNTION_PARA_TYPE

    @staticmethod
    def _oracle_agg_table():
        return f"{_VTABLE_SELECT_DB}.{_VTABLE_SELECT_TB}"

    def _check_agg_rows(self, sql):
        tdSql.query(sql)
        assert tdSql.queryRows > 0, f"aggregate returned no rows: {sql}"

    def _decimal_column(self):
        tdSql.query(f"describe {self._oracle_agg_table()}")
        for row in tdSql.queryResult:
            if str(row[0]).lower() == "bigint_col":
                return str(row[1]).upper()
        raise AssertionError("bigint_col is missing from the virtual stable")

    def _check_decimal_operand_rule(self, sql):
        column_type = self._decimal_column()
        if "DECIMAL" in column_type:
            tdSql.error(sql, expectedErrno=self._DECIMAL_AGG_ERRNO)
        else:
            tdSql.query(sql)
            assert tdSql.queryRows > 0, (
                f"aggregate over non-DECIMAL bigint_col ({column_type}) "
                f"returned no rows: {sql}")

    def test_agg(self):
        """Oracle aggregates: supported functions run, DECIMAL operands protected

        Runs the aggregate functions TDengine accepts over every non-DECIMAL
        column of the Oracle-backed table, then asserts the DECIMAL operand
        rule on bigint_col itself.

        Since: v3.4.4.0

        """
        table = self._oracle_agg_table()
        self._check_agg_rows(
            "select count(*), avg(u_int_col), sum(u_bigint_col), "
            f"min(int_col), max(smallint_col) from {table}")
        self._check_decimal_operand_rule(f"select stddev(bigint_col) from {table}")

    def test_agg_group(self):
        """Oracle aggregates: GROUP BY works, DECIMAL operands protected

        Groups the aggregate by an integer column and repeats the DECIMAL
        operand assertion on the grouped form.

        Since: v3.4.4.0

        """
        table = self._oracle_agg_table()
        self._check_agg_rows(
            f"select u_tinyint_col, count(*), avg(u_int_col) from {table} "
            "group by u_tinyint_col order by u_tinyint_col")
        self._check_decimal_operand_rule(
            f"select variance(bigint_col) from {table} group by u_tinyint_col")

    def test_agg_time_filter(self):
        """Oracle aggregates: time filter works, DECIMAL operands protected

        Restricts the aggregate to a timestamp range wide enough to include
        every row and repeats the DECIMAL operand assertion on the filtered
        form.

        Since: v3.4.4.0

        """
        table = self._oracle_agg_table()
        self._check_agg_rows(
            "select count(*), avg(u_int_col), sum(u_bigint_col) "
            f"from {table} where ts >= '2020-01-01 00:00:00' "
            "and ts < '2030-01-01 00:00:00'")
        self._check_decimal_operand_rule(
            f"select spread(bigint_col) from {table} "
            "where ts >= '2020-01-01 00:00:00'")

    def test_agg_partition_expr(self):
        """Oracle aggregates: PARTITION BY works, DECIMAL operands protected

        Runs the aggregate partitioned by table name and repeats the DECIMAL
        operand assertion on the partitioned form.

        Since: v3.4.4.0

        """
        table = self._oracle_agg_table()
        self._check_agg_rows(
            f"select count(*), avg(u_int_col) from {table} partition by tbname")
        self._check_decimal_operand_rule(
            f"select apercentile(bigint_col, 50) from {table} partition by tbname")


class TestVTableQueryExtSourceOracleStbAgg(_OracleDecimalAggQuery,
                                           _ExtSourceStbAggQueryBase):
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
        vtb_util.prepare_ext_source_vtables("oracle", mode=2)


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


class TestVTableQueryExtSourceOracleMixedStbAgg(_OracleDecimalAggQuery,
                                                  _ExtSourceStbAggQueryBase):
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
        vtb_util.prepare_mixed_source_vtables("oracle", mode=2)


class TestVTableQueryExtSourceAllStbAgg(_ExtSourceStbAggQueryBase):
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
        vtb_util.prepare_all_ext_source_vtables(mode=2)

