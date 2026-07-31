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

from new_test_framework.utils import tdCom, tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import VtableQueryUtil


class _NormalQueryMixin:
    def run_normal_query(self, test_case):
        tdLog.info(f"test case : {test_case}.")
        case_dir = os.path.dirname(os.path.dirname(__file__))
        sql_file = os.path.join(case_dir, "in", f"{test_case}.in")
        ans_file = os.path.join(case_dir, "ans", f"{test_case}.ans")
        tdCom.compare_testcase_result(sql_file, ans_file, test_case)


RAW_DB = "test_vtable_cross_db_vstb_ref_vstb_raw"


MID_DB = "test_vtable_cross_db_vstb_ref_vstb_mid"


TOP_DB = "test_vtable_cross_db_vstb_ref_vstb_top"


class _RowsMixin:
    @staticmethod
    def _fetch_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    def _assert_rows(self, sql, expected):
        assert self._fetch_rows(sql) == expected

    def _assert_same_result(self, stable_sql, child_sql):
        stable_rows = self._fetch_rows(stable_sql)
        child_rows = self._fetch_rows(child_sql)
        assert stable_rows == child_rows


class TestVTableQueryCrossDBStbBasic(_NormalQueryMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables()

    def test_projection(self):
        """Query cross db virtual stable projection."""
        self.run_normal_query("test_vstable_select_test_projection")

    def test_projection_filter(self):
        """Query cross db virtual stable projection filter."""
        self.run_normal_query("test_vstable_select_test_projection_filter")

    def test_projection_timerange_filter(self):
        """Query cross db virtual stable projection timerange filter."""
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")

    def test_function_cols(self):
        """Query cross db virtual stable column functions."""
        self.run_normal_query("test_vstable_select_test_function_cols")

    def test_function_tags(self):
        """Query cross db virtual stable tag functions."""
        self.run_normal_query("test_vstable_select_test_function_tags")

    def test_partition(self):
        """Query cross db virtual stable partition."""
        self.run_normal_query("test_vstable_select_test_partition")

    def test_group(self):
        """Query cross db virtual stable group."""
        self.run_normal_query("test_vstable_select_test_group")

    def test_orderby(self):
        """Query cross db virtual stable order by."""
        self.run_normal_query("test_vstable_select_test_orderby")

    def test_interval(self):
        """Query cross db virtual stable interval."""
        self.run_normal_query("test_vstable_select_test_interval")

    def test_session(self):
        """Query cross db virtual stable session."""
        self.run_normal_query("test_vstable_select_test_session")

    def test_event(self):
        """Query cross db virtual stable event."""
        self.run_normal_query("test_vstable_select_test_event")

    def test_count(self):
        """Query cross db virtual stable count."""
        self.run_normal_query("test_vstable_select_test_count")

    def test_state_mode_0(self):
        """Query cross db virtual stable state mode 0."""
        self.run_normal_query("test_vstable_select_test_state_mode_0")

    def test_state_mode_1(self):
        """Query cross db virtual stable state mode 1."""
        self.run_normal_query("test_vstable_select_test_state_mode_1")

    def test_state_mode_2(self):
        """Query cross db virtual stable state mode 2."""
        self.run_normal_query("test_vstable_select_test_state_mode_2")


class TestVTableQueryCrossDBStbMode2(_NormalQueryMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(mode=2)

    def test_agg(self):
        """Query cross db virtual stable aggregate."""
        self.run_normal_query("test_vstable_select_test_agg")

    def test_agg_group(self):
        """Query cross db virtual stable aggregate group by."""
        self.run_normal_query("test_vstable_select_test_agg_group_by")

    def test_agg_partition_expr(self):
        """Query cross db virtual stable aggregate partition expression."""
        self.run_normal_query("test_vstable_select_test_agg_partition_expr")

    def test_agg_tag_filter(self):
        """Query cross db virtual stable aggregate tag filter."""
        self.run_normal_query("test_vstable_select_test_agg_tag_cond")

    def test_agg_time_filter(self):
        """Query cross db virtual stable aggregate time filter."""
        self.run_normal_query("test_vstable_select_test_agg_time_cond")


class TestVTableQueryCrossDBStbSma(_NormalQueryMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(mode=2, sma=True)

    def test_agg_sma(self):
        """Query cross db virtual stable aggregate sma."""
        self.run_normal_query("test_vstable_select_test_agg_sma")


class TestVTableQueryCrossDBStbRef(_NormalQueryMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(ref_mode="virtual_ref")

    def test_projection_vtb_ref(self):
        """Query cross db virtual stable projection through virtual reference."""
        self.run_normal_query("test_vstable_select_test_projection")

    def test_projection_filter_vtb_ref(self):
        """Query cross db virtual stable projection filter through virtual reference."""
        self.run_normal_query("test_vstable_select_test_projection_filter")

    def test_projection_timerange_filter_vtb_ref(self):
        """Query cross db virtual stable projection timerange filter through virtual reference."""
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")

    def test_function_cols_vtb_ref(self):
        """Query cross db virtual stable column functions through virtual reference."""
        self.run_normal_query("test_vstable_select_test_function_cols")

    def test_function_tags_vtb_ref(self):
        """Query cross db virtual stable tag functions through virtual reference."""
        self.run_normal_query("test_vstable_select_test_function_tags")

    def test_partition_vtb_ref(self):
        """Query cross db virtual stable partition through virtual reference."""
        self.run_normal_query("test_vstable_select_test_partition")

    def test_group_vtb_ref(self):
        """Query cross db virtual stable group through virtual reference."""
        self.run_normal_query("test_vstable_select_test_group")

    def test_orderby_vtb_ref(self):
        """Query cross db virtual stable order by through virtual reference."""
        self.run_normal_query("test_vstable_select_test_orderby")

    def test_interval_vtb_ref(self):
        """Query cross db virtual stable interval through virtual reference."""
        self.run_normal_query("test_vstable_select_test_interval")

    def test_session_vtb_ref(self):
        """Query cross db virtual stable session through virtual reference."""
        self.run_normal_query("test_vstable_select_test_session")

    def test_event_vtb_ref(self):
        """Query cross db virtual stable event through virtual reference."""
        self.run_normal_query("test_vstable_select_test_event")

    def test_count_vtb_ref(self):
        """Query cross db virtual stable count through virtual reference."""
        self.run_normal_query("test_vstable_select_test_count")

    def test_state_mode_0_vtb_ref(self):
        """Query cross db virtual stable state mode 0 through virtual reference."""
        self.run_normal_query("test_vstable_select_test_state_mode_0")

    def test_state_mode_1_vtb_ref(self):
        """Query cross db virtual stable state mode 1 through virtual reference."""
        self.run_normal_query("test_vstable_select_test_state_mode_1")

    def test_state_mode_2_vtb_ref(self):
        """Query cross db virtual stable state mode 2 through virtual reference."""
        self.run_normal_query("test_vstable_select_test_state_mode_2")


class TestVTableQueryCrossDBStbMode2Ref(_NormalQueryMixin, _RowsMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }
    ROOT_DB = "test_vtable_select"

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(mode=2, ref_mode="virtual_ref")

    def test_agg_vtb_ref(self):
        """Query cross db virtual stable aggregate through virtual reference."""
        self.run_normal_query("test_vstable_select_test_agg")

    def test_agg_group_vtb_ref(self):
        """Query cross db virtual stable aggregate group by through virtual reference."""
        self.run_normal_query("test_vstable_select_test_agg_group_by")

    def test_agg_partition_expr_vtb_ref(self):
        """Query cross db virtual stable aggregate partition expression through virtual reference."""
        self.run_normal_query("test_vstable_select_test_agg_partition_expr")

    def test_agg_tag_filter_vtb_ref(self):
        """Query cross db virtual stable aggregate tag filter through virtual reference."""
        self.run_normal_query("test_vstable_select_test_agg_tag_cond")

    def test_agg_time_filter_vtb_ref(self):
        """Query cross db virtual stable aggregate time filter through virtual reference."""
        self.run_normal_query("test_vstable_select_test_agg_time_cond")

    def test_tag_filtered_aggregate_matches_full_child(self):
        """Tag filtered aggregate matches full child.

        Verify tag filtered aggregate matches full child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        stable_sql = (
            f"select count(*), sum(u_tinyint_col), min(int_col), max(float_col) "
            f"from {self.ROOT_DB}.vtb_virtual_stb "
            f"where nchar_32_tag = 'full' and binary_32_tag = 'child0';"
        )
        child_sql = (
            f"select count(*), sum(u_tinyint_col), min(int_col), max(float_col) "
            f"from {self.ROOT_DB}.vtb_virtual_ctb_full_0;"
        )
        self._assert_same_result(stable_sql, child_sql)

    def test_tag_filtered_null_aggregate_matches_half_child(self):
        """Tag filtered null aggregate matches half child.

        Verify tag filtered null aggregate matches half child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        stable_sql = (
            f"select count(*), count(u_bigint_col), count(tinyint_col), "
            f"count(nchar_32_col), sum(int_col) "
            f"from {self.ROOT_DB}.vtb_virtual_stb "
            f"where nchar_32_tag = 'half' and binary_32_tag = 'child0';"
        )
        child_sql = (
            f"select count(*), count(u_bigint_col), count(tinyint_col), "
            f"count(nchar_32_col), sum(int_col) "
            f"from {self.ROOT_DB}.vtb_virtual_ctb_half_full_0;"
        )
        self._assert_same_result(stable_sql, child_sql)

    def test_tag_filtered_projection_matches_child(self):
        """Tag filtered projection matches child.

        Verify tag filtered projection matches child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        stable_sql = (
            f"select int_tag, nchar_32_tag, binary_32_tag, u_tinyint_col, int_col "
            f"from {self.ROOT_DB}.vtb_virtual_stb "
            f"where nchar_32_tag = 'full' and binary_32_tag = 'child1' "
            f"and u_tinyint_col is not null order by ts limit 5;"
        )
        child_sql = (
            f"select int_tag, nchar_32_tag, binary_32_tag, u_tinyint_col, int_col "
            f"from {self.ROOT_DB}.vtb_virtual_ctb_full_1 "
            f"where u_tinyint_col is not null order by ts limit 5;"
        )
        self._assert_same_result(stable_sql, child_sql)

    def test_tag_filtered_function_matches_child(self):
        """Tag filtered function matches child.

        Verify tag filtered function matches child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        stable_sql = (
            f"select round(stddev(u_tinyint_col)), round(avg(float_col), 3), "
            f"count(*) "
            f"from {self.ROOT_DB}.vtb_virtual_stb "
            f"where nchar_32_tag = 'full' and binary_32_tag = 'child2';"
        )
        child_sql = (
            f"select round(stddev(u_tinyint_col)), round(avg(float_col), 3), "
            f"count(*) "
            f"from {self.ROOT_DB}.vtb_virtual_ctb_full_2;"
        )
        self._assert_same_result(stable_sql, child_sql)


class TestVTableQueryCrossDBStbSmaRef(_NormalQueryMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
        "minReservedMemorySize": "1024",
    }

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(mode=2, sma=True, ref_mode="virtual_ref")

    def test_agg_sma_vtb_ref(self):
        """Query cross db virtual stable aggregate sma through virtual reference."""
        self.run_normal_query("test_vstable_select_test_agg_sma")


class TestVTableQueryCrossDBVStbRefVStb(_RowsMixin):
    updatecfgDict = {
        "supportVnodes": "1000",
    }

    @staticmethod
    def _create_mid_child(table_name, source_table, source_name):
        tdSql.execute(
            f"create vtable {table_name} ("
            f"mid_v1 from {RAW_DB}.{source_table}.raw_v1, "
            f"mid_v2 from {RAW_DB}.{source_table}.raw_v2, "
            f"mid_flag from {RAW_DB}.{source_table}.raw_flag"
            f") using mid_vstb tags ("
            f"{RAW_DB}.{source_table}.plant, "
            f"{RAW_DB}.{source_table}.line_id, "
            f"{RAW_DB}.{source_table}.quality, "
            f"'{source_name}');"
        )

    @staticmethod
    def _create_top_child(table_name, mid_table, plant, line_id, src_name, ref_quality, chain_label):
        tdSql.execute(
            f"create vtable {table_name} ("
            f"top_v1 from {MID_DB}.{mid_table}.mid_v1, "
            f"top_v2 from {MID_DB}.{mid_table}.mid_v2, "
            f"top_flag from {MID_DB}.{mid_table}.mid_flag"
            f") using top_vstb tags ("
            f"'{plant}', "
            f"{line_id}, "
            f"'{src_name}', "
            f"'{ref_quality}', "
            f"'{chain_label}');"
        )

    def setup_class(cls):
        tdLog.info("prepare cross-db vstb ref vstb query env.")

        tdSql.execute(f"drop database if exists {TOP_DB};")
        tdSql.execute(f"drop database if exists {MID_DB};")
        tdSql.execute(f"drop database if exists {RAW_DB};")
        tdSql.execute(f"create database {RAW_DB} vgroups 2;")
        tdSql.execute(f"create database {MID_DB} vgroups 2;")
        tdSql.execute(f"create database {TOP_DB} vgroups 2;")

        tdSql.execute(f"use {RAW_DB};")
        tdSql.execute(
            "create stable src_stb(ts timestamp, raw_v1 int, raw_v2 int, raw_flag bool) "
            "tags (plant nchar(16), line_id int, quality nchar(16));"
        )
        tdSql.execute("create table src_west_gold using src_stb tags ('west', 11, 'gold');")
        tdSql.execute("create table src_west_silver using src_stb tags ('west', 12, 'silver');")
        tdSql.execute("create table src_east_gold using src_stb tags ('east', 21, 'gold');")
        tdSql.execute("insert into src_west_gold values "
                      "(1702000000000, 12, 120, true) "
                      "(1702000001000, 18, 180, false) "
                      "(1702000002000, 24, 240, true);")
        tdSql.execute("insert into src_west_silver values "
                      "(1702000000000, 14, 140, true) "
                      "(1702000001000, 28, 280, true);")
        tdSql.execute("insert into src_east_gold values "
                      "(1702000000000, 35, 350, true) "
                      "(1702000001000, 45, 450, false);")

        tdSql.execute(f"use {MID_DB};")
        tdSql.execute(
            "create stable mid_vstb(ts timestamp, mid_v1 int, mid_v2 int, mid_flag bool) "
            "tags (plant nchar(16), line_id int, quality nchar(16), src_name nchar(32)) virtual 1;"
        )
        cls._create_mid_child("mid_west_gold", "src_west_gold", "src_west_gold")
        cls._create_mid_child("mid_west_silver", "src_west_silver", "src_west_silver")
        cls._create_mid_child("mid_east_gold", "src_east_gold", "src_east_gold")

        tdSql.execute(f"use {TOP_DB};")
        tdSql.execute(
            "create stable top_vstb(ts timestamp, top_v1 int, top_v2 int, top_flag bool) "
            "tags (plant nchar(16), line_id int, src_name nchar(32), ref_quality nchar(16), chain_label nchar(16)) virtual 1;"
        )
        cls._create_top_child("top_west_gold", "mid_west_gold", "west", 11, "src_west_gold", "gold", "cross_db")
        cls._create_top_child("top_west_silver", "mid_west_silver", "west", 12, "src_west_silver", "silver", "cross_db")
        cls._create_top_child("top_east_gold", "mid_east_gold", "east", 21, "src_east_gold", "gold", "cross_db")

    def test_child_projection_from_cross_db_virtual_stable_reference(self):
        """Child projection from cross db virtual stable reference.

        Verify child projection from cross db virtual stable reference.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"use {TOP_DB};")
        expected = [("west", "src_west_gold", 18, 180), ("west", "src_west_gold", 24, 240)]
        self._assert_rows(
            "select plant, src_name, top_v1, top_v2 from top_west_gold where top_v2 >= 180 order by ts;",
            expected,
        )

    def test_child_aggregate_on_second_layer_virtual_stable(self):
        """Child aggregate on second layer virtual stable.

        Verify child aggregate on second layer virtual stable.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"use {TOP_DB};")
        self._assert_rows(
            "select count(*), sum(top_v1), min(top_v2), max(top_v2) from top_west_gold where plant = 'west' and ref_quality = 'gold';",
            [(3, 54, 120, 240)],
        )

    def test_child_tag_predicate_with_data_filter(self):
        """Child tag predicate with data filter.

        Verify child tag predicate with data filter.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"use {TOP_DB};")
        expected = [("west", "src_west_silver", "silver", 14), ("west", "src_west_silver", "silver", 28)]
        self._assert_rows(
            "select plant, src_name, ref_quality, top_v1 from top_west_silver where chain_label = 'cross_db' and top_flag = true order by ts;",
            expected,
        )

    def test_child_combined_tag_and_value_filter(self):
        """Child combined tag and value filter.

        Verify child combined tag and value filter.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"use {TOP_DB};")
        expected = [("west", "src_west_gold", "gold", 24, 240)]
        self._assert_rows(
            "select plant, src_name, ref_quality, top_v1, top_v2 from top_west_gold where ref_quality = 'gold' and top_v2 >= 200 order by ts;",
            expected,
        )
