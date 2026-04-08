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


RAW_DB = "test_vtable_cross_db_vstb_ref_vstb_raw"
MID_DB = "test_vtable_cross_db_vstb_ref_vstb_mid"
TOP_DB = "test_vtable_cross_db_vstb_ref_vstb_top"


class TestVTableQueryCrossDBVStbRefVStb:
    updatecfgDict = {
        "supportVnodes": "1000",
    }

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
        tdSql.execute(f"use {TOP_DB};")

        expected = [
            ("west", "src_west_gold", 18, 180),
            ("west", "src_west_gold", 24, 240),
        ]
        self._assert_rows(
            "select plant, src_name, top_v1, top_v2 "
            "from top_west_gold where top_v2 >= 180 order by ts;",
            expected,
        )

    def test_child_aggregate_on_second_layer_virtual_stable(self):
        tdSql.execute(f"use {TOP_DB};")

        self._assert_rows(
            "select count(*), sum(top_v1), min(top_v2), max(top_v2) "
            "from top_west_gold where plant = 'west' and ref_quality = 'gold';",
            [(3, 54, 120, 240)],
        )

    def test_child_tag_predicate_with_data_filter(self):
        tdSql.execute(f"use {TOP_DB};")

        expected = [
            ("west", "src_west_silver", "silver", 14),
            ("west", "src_west_silver", "silver", 28),
        ]
        self._assert_rows(
            "select plant, src_name, ref_quality, top_v1 "
            "from top_west_silver where chain_label = 'cross_db' and top_flag = true "
            "order by ts;",
            expected,
        )

    def test_child_combined_tag_and_value_filter(self):
        tdSql.execute(f"use {TOP_DB};")

        expected = [("west", "src_west_gold", "gold", 24, 240)]
        self._assert_rows(
            "select plant, src_name, ref_quality, top_v1, top_v2 "
            "from top_west_gold where ref_quality = 'gold' and top_v2 >= 200 "
            "order by ts;",
            expected,
        )
