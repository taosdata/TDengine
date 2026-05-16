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
from new_test_framework.utils import tdLog, tdSql


DB_NAME = "test_vtable_same_db_vstb_ref_vstb"


class TestVTableQuerySameDBVStbRefVStb:

    @staticmethod
    def _fetch_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    def _assert_rows(self, sql, expected):
        assert self._fetch_rows(sql) == expected

    @staticmethod
    def _create_mid_child(table_name, source_table, source_name):
        tdSql.execute(
            f"create vtable {table_name} ("
            f"mid_v1 from {source_table}.raw_v1, "
            f"mid_v2 from {source_table}.raw_v2, "
            f"mid_flag from {source_table}.raw_flag"
            f") using mid_vstb tags ("
            f"{source_table}.plant, "
            f"{source_table}.line_id, "
            f"{source_table}.quality, "
            f"'{source_name}');"
        )

    @staticmethod
    def _create_top_child(table_name, mid_table, plant, line_id, src_name, ref_quality, chain_label):
        tdSql.execute(
            f"create vtable {table_name} ("
            f"top_v1 from {mid_table}.mid_v1, "
            f"top_v2 from {mid_table}.mid_v2, "
            f"top_flag from {mid_table}.mid_flag"
            f") using top_vstb tags ("
            f"'{plant}', "
            f"{line_id}, "
            f"'{src_name}', "
            f"'{ref_quality}', "
            f"'{chain_label}');"
        )

    def setup_class(cls):
        tdLog.info("prepare same-db vstb ref vstb query env.")

        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.execute(
            "create stable src_stb(ts timestamp, raw_v1 int, raw_v2 int, raw_flag bool) "
            "tags (plant nchar(16), line_id int, quality nchar(16));"
        )
        tdSql.execute("create table src_north_gold using src_stb tags ('north', 1, 'gold');")
        tdSql.execute("create table src_north_silver using src_stb tags ('north', 2, 'silver');")
        tdSql.execute("create table src_south_gold using src_stb tags ('south', 3, 'gold');")

        tdSql.execute("insert into src_north_gold values "
                      "(1701000000000, 10, 100, true) "
                      "(1701000001000, 20, 200, true) "
                      "(1701000002000, 30, 300, false);")
        tdSql.execute("insert into src_north_silver values "
                      "(1701000000000, 15, 150, true) "
                      "(1701000001000, 25, 250, false);")
        tdSql.execute("insert into src_south_gold values "
                      "(1701000000000, 40, 400, true) "
                      "(1701000001000, 50, 500, true);")

        tdSql.execute(
            "create stable mid_vstb(ts timestamp, mid_v1 int, mid_v2 int, mid_flag bool) "
            "tags (plant nchar(16), line_id int, quality nchar(16), src_name nchar(32)) virtual 1;"
        )
        cls._create_mid_child("mid_north_gold", "src_north_gold", "src_north_gold")
        cls._create_mid_child("mid_north_silver", "src_north_silver", "src_north_silver")
        cls._create_mid_child("mid_south_gold", "src_south_gold", "src_south_gold")

        tdSql.execute(
            "create stable top_vstb(ts timestamp, top_v1 int, top_v2 int, top_flag bool) "
            "tags (plant nchar(16), line_id int, src_name nchar(32), ref_quality nchar(16), chain_label nchar(16)) virtual 1;"
        )
        cls._create_top_child("top_north_gold", "mid_north_gold", "north", 1, "src_north_gold", "gold", "same_db")
        cls._create_top_child("top_north_silver", "mid_north_silver", "north", 2, "src_north_silver", "silver", "same_db")
        cls._create_top_child("top_south_gold", "mid_south_gold", "south", 3, "src_south_gold", "gold", "same_db")

    def test_child_projection_from_virtual_stable_reference(self):
        """Child projection from virtual stable reference.

        Verify child projection from virtual stable reference.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"use {DB_NAME};")

        expected = [
            ("north", "src_north_gold", 20, 200),
            ("north", "src_north_gold", 30, 300),
        ]
        self._assert_rows(
            "select plant, src_name, top_v1, top_v2 "
            "from top_north_gold where top_v1 >= 20 order by ts;",
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
        tdSql.execute(f"use {DB_NAME};")

        self._assert_rows(
            "select count(*), sum(top_v1), min(top_v2), max(top_v2) "
            "from top_north_gold where plant = 'north' and ref_quality = 'gold';",
            [(3, 60, 100, 300)],
        )

    def test_child_tag_predicate_with_data_filter(self):
        """Child tag predicate with data filter.

        Verify child tag predicate with data filter.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"use {DB_NAME};")

        expected = [
            ("south", "src_south_gold", "gold", 40),
            ("south", "src_south_gold", "gold", 50),
        ]
        self._assert_rows(
            "select plant, src_name, ref_quality, top_v1 "
            "from top_south_gold where chain_label = 'same_db' and top_flag = true "
            "order by ts;",
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
        tdSql.execute(f"use {DB_NAME};")

        expected = [("north", "src_north_silver", "silver", 25, 250)]
        self._assert_rows(
            "select plant, src_name, ref_quality, top_v1, top_v2 "
            "from top_north_silver where ref_quality = 'silver' and top_v2 >= 200 "
            "order by ts;",
            expected,
        )
