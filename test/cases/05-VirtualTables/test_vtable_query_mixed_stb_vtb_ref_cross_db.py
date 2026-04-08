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
from new_test_framework.utils import tdSql
from vtable_util import VtableQueryUtil


class _BaseMixedVtbRef:
    ROOT_DB = "test_vtable_select"

    @staticmethod
    def _fetch_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    def _assert_same_result(self, stable_sql, child_sql):
        stable_rows = self._fetch_rows(stable_sql)
        child_rows = self._fetch_rows(child_sql)
        assert stable_rows == child_rows

    def test_tag_filtered_aggregate_matches_full_child(self):
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


class TestVTableQueryCrossDBStbMixedVtbRef(_BaseMixedVtbRef):
    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_cross_db_vtables(mode=2, ref_mode="virtual_ref")
