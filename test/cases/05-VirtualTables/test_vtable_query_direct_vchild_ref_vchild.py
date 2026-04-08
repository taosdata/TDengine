# ###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
# ###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql


DB_NAME = "test_vtable_direct_vchild_ref_vchild"
MAX_VTABLE_REF_DEPTH = 32
MAX_DEPTH_CHAIN_LEN = MAX_VTABLE_REF_DEPTH + 1


class TestVTableQueryDirectVChildRefVChild:

    @staticmethod
    def _create_vchild_chain(prefix, chain_len, source_table, tag_base, region):
        prev_table = source_table
        prev_col_v1 = "v1"
        prev_col_v2 = "v2"
        leaf_table = None

        for idx in range(1, chain_len + 1):
            leaf_table = f"{prefix}_{idx:02d}"
            tdSql.execute(
                f"create vtable {leaf_table} ("
                f"ref_v1 from {prev_table}.{prev_col_v1}, "
                f"ref_v2 from {prev_table}.{prev_col_v2}"
                f") using vstb tags ({tag_base + idx - 1}, '{region}');"
            )
            prev_table = leaf_table
            prev_col_v1 = "ref_v1"
            prev_col_v2 = "ref_v2"

        return leaf_table

    def setup_class(cls):
        tdLog.info("prepare direct virtual-child reference query env.")

        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.execute("create stable src_stb(ts timestamp, v1 int, v2 int) tags (gid int, region nchar(16));")
        tdSql.execute("create table src_ctb using src_stb tags (1, 'alpha');")
        tdSql.execute("create table src_ctb_beta using src_stb tags (2, 'beta');")

        tdSql.execute("insert into src_ctb values "
                      "(1700000000000, 10, 100) "
                      "(1700000001000, 20, 200) "
                      "(1700000002000, 30, 300);")
        tdSql.execute("insert into src_ctb_beta values "
                      "(1700000000000, 15, 150) "
                      "(1700000001000, 25, 250) "
                      "(1700000002000, 35, 350);")

        tdSql.execute("create stable vstb(ts timestamp, ref_v1 int, ref_v2 int) "
                      "tags (gid int, region nchar(16)) virtual 1;")

        cls.simple_chain_leaf = cls._create_vchild_chain("vctb_simple", 2, "src_ctb", 11, "simple")
        cls.tagged_chain_leaf = cls._create_vchild_chain("vctb_tagged", 2, "src_ctb_beta", 21, "beta")
        cls.max_depth_leaf = cls._create_vchild_chain("vctb_depth", MAX_DEPTH_CHAIN_LEN, "src_ctb", 100, "depth")

    def test_query_direct_virtual_child_reference(self):
        """Query: direct virtual child reference another virtual child

        1. query leaf virtual child with projection/order
        2. query leaf virtual child with filter/agg

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual

        Jira: None

        History:
            - 2026-4-7 Copilot created

        """
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select * from (select * from {self.simple_chain_leaf}) order by ts;")
        tdSql.checkRows(3)
        expected = [(10, 100), (20, 200), (30, 300)]
        for row_idx, (v1, v2) in enumerate(expected):
            tdSql.checkData(row_idx, 1, v1)
            tdSql.checkData(row_idx, 2, v2)

        tdSql.query(
            f"select count(*), sum(ref_v1), max(ref_v2) "
            f"from (select * from {self.simple_chain_leaf}) where ref_v1 >= 20;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 50)
        tdSql.checkData(0, 2, 300)

    def test_query_direct_virtual_child_reference_max_depth(self):
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select * from (select * from {self.max_depth_leaf}) order by ts;")
        tdSql.checkRows(3)
        expected = [(10, 100), (20, 200), (30, 300)]
        for row_idx, (v1, v2) in enumerate(expected):
            tdSql.checkData(row_idx, 1, v1)
            tdSql.checkData(row_idx, 2, v2)

        tdSql.query(
            f"select count(*), sum(ref_v1), max(ref_v2) "
            f"from (select * from {self.max_depth_leaf}) where ref_v1 >= 20;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 50)
        tdSql.checkData(0, 2, 300)

    def test_query_direct_virtual_child_tag_condition(self):
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(
            f"select gid, region, ref_v1, ref_v2 "
            f"from {self.tagged_chain_leaf} "
            f"where gid = 22 and region = 'beta' and ref_v1 >= 25 "
            f"order by ts;"
        )
        tdSql.checkRows(2)
        expected = [(22, "beta", 25, 250), (22, "beta", 35, 350)]
        for row_idx, (gid, region, v1, v2) in enumerate(expected):
            tdSql.checkData(row_idx, 0, gid)
            tdSql.checkData(row_idx, 1, region)
            tdSql.checkData(row_idx, 2, v1)
            tdSql.checkData(row_idx, 3, v2)

    def test_query_direct_virtual_child_stb_tag_aggregate(self):
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(
            "select count(*), sum(ref_v1), max(ref_v2) "
            "from vstb where gid = 22 and region = 'beta';"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 75)
        tdSql.checkData(0, 2, 350)

    def test_query_direct_virtual_child_combined_tag_and_data_condition(self):
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(
            f"select gid, region, ref_v1, ref_v2 "
            f"from {self.tagged_chain_leaf} "
            f"where gid = 22 and region = 'beta' and ref_v2 >= 250 "
            f"order by ts;"
        )
        tdSql.checkRows(2)
        expected = [(22, "beta", 25, 250), (22, "beta", 35, 350)]
        for row_idx, (gid, region, v1, v2) in enumerate(expected):
            tdSql.checkData(row_idx, 0, gid)
            tdSql.checkData(row_idx, 1, region)
            tdSql.checkData(row_idx, 2, v1)
            tdSql.checkData(row_idx, 3, v2)

    def test_query_direct_virtual_child_stb_combined_filter_aggregate(self):
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(
            "select count(*), sum(ref_v1), min(ref_v2), max(ref_v2) "
            "from vstb where gid = 22 and region = 'beta' and ref_v1 >= 25;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 60)
        tdSql.checkData(0, 2, 250)
        tdSql.checkData(0, 3, 350)
