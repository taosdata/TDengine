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
"""SELECT TAGS coverage for virtual tables that use tag references."""

from new_test_framework.utils import tdLog, tdSql

DB = "td_tagref_select_tags"

SRC = {
    "s_bj": {"city": "beijing", "code": 100, "data": [1, 2, 3]},
    "s_sh": {"city": "shanghai", "code": 200, "data": [10, 11]},
    "s_sz": {"city": "shenzhen", "code": 300, "data": [20]},
}

VTABLES = [
    ("v0", "s_bj", 1, "s_bj", "s_bj"),
    ("v1", "s_sh", 1, "s_sh", "s_sh"),
    ("v2", "s_sz", 2, "s_bj", "s_sh"),
]


class TestVtableTagRefSelectTags:

    @staticmethod
    def _check_values(sql, expected, desc):
        tdSql.query(sql)
        actual = sorted(
            tuple(str(tdSql.getData(i, j)) for j in range(len(expected[0])))
            for i in range(tdSql.queryRows)
        )
        exp = sorted(tuple(str(v) for v in row) for row in expected)
        assert actual == exp, f"{desc}: expected {exp}, got {actual}"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")

        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
            "TAGS (city NCHAR(20), code INT)"
        )
        for table, info in SRC.items():
            tdSql.execute(
                f"CREATE TABLE {table} USING src_stb "
                f"TAGS ('{info['city']}', {info['code']})"
            )
            for offset, value in enumerate(info["data"]):
                tdSql.execute(
                    f"INSERT INTO {table} VALUES ({1700000000000 + offset * 1000}, {value})"
                )

        tdSql.execute(
            "CREATE STABLE vstb (ts TIMESTAMP, val INT) "
            "TAGS (local_group INT, ref_city NCHAR(20), ref_code INT) VIRTUAL 1"
        )
        for vt, data_src, local_group, city_src, code_src in VTABLES:
            tdSql.execute(
                f"CREATE VTABLE {vt} (val FROM {data_src}.val) USING vstb TAGS ("
                f"{local_group}, "
                f"ref_city FROM {city_src}.city, "
                f"ref_code FROM {code_src}.code)"
            )

    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")

    def test_select_tags_ref_tag(self):
        """SELECT TAGS returns row-wise tag values for referenced tags.

        Verify that the system correctly handles the case: select tags returns row-wise tag values for referenced tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        expected = [
            (SRC[city_src]["city"],)
            for _, data_src, _, city_src, _ in VTABLES
            for _ in SRC[data_src]["data"]
        ]
        self._check_values(
            "SELECT TAGS ref_city FROM vstb",
            expected,
            "SELECT TAGS ref_city",
        )

    def test_select_tags_tbname_multi_tags(self):
        """SELECT TAGS supports TBNAME plus local and referenced tags.

        Verify that the system correctly handles the case: select tags supports tbname plus local and referenced tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        expected = [
            (vt, local_group, SRC[city_src]["city"], SRC[code_src]["code"])
            for vt, data_src, local_group, city_src, code_src in VTABLES
            for _ in SRC[data_src]["data"]
        ]
        self._check_values(
            "SELECT TAGS TBNAME, local_group, ref_city, ref_code FROM vstb",
            expected,
            "SELECT TAGS TBNAME, local_group, ref_city, ref_code",
        )

    def test_select_tags_where_on_ref_tag(self):
        """SELECT TAGS supports WHERE conditions on referenced tags.

        Verify that the system correctly handles the case: select tags supports where conditions on referenced tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        expected = [
            (vt, SRC[city_src]["city"])
            for vt, data_src, _, city_src, _ in VTABLES
            if SRC[city_src]["city"] == "beijing"
            for _ in SRC[data_src]["data"]
        ]
        self._check_values(
            "SELECT TAGS TBNAME, ref_city FROM vstb WHERE ref_city = 'beijing'",
            expected,
            "SELECT TAGS with ref tag filter",
        )

    def test_select_tags_where_on_local_and_ref_tags(self):
        """SELECT TAGS supports combined WHERE conditions across local and ref tags.

        Verify that the system correctly handles the case: select tags supports combined where conditions across local and ref tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        expected = [
            (vt, local_group, SRC[code_src]["code"])
            for vt, data_src, local_group, _, code_src in VTABLES
            if local_group == 1 and SRC[code_src]["code"] >= 200
            for _ in SRC[data_src]["data"]
        ]
        self._check_values(
            "SELECT TAGS TBNAME, local_group, ref_code "
            "FROM vstb WHERE local_group = 1 AND ref_code >= 200",
            expected,
            "SELECT TAGS with local+ref filter",
        )

    def test_select_tags_accepts_data_column_projection(self):
        """SELECT TAGS can project data columns together with tag-ref columns.

        Verify that the system correctly handles the case: select tags can project data columns together with tag-ref columns.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        expected = [
            (vt, value, SRC[city_src]["city"])
            for vt, data_src, _, city_src, _ in VTABLES
            for value in SRC[data_src]["data"]
        ]
        self._check_values(
            "SELECT TAGS TBNAME, val, ref_city FROM vstb",
            expected,
            "SELECT TAGS with data projection",
        )

    def test_select_tags_accepts_data_predicate(self):
        """SELECT TAGS can filter on data columns as well as tags.

        Verify that the system correctly handles the case: select tags can filter on data columns as well as tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        expected = [
            (vt, value, SRC[city_src]["city"])
            for vt, data_src, _, city_src, _ in VTABLES
            for value in SRC[data_src]["data"]
            if value > 1
        ]
        self._check_values(
            "SELECT TAGS TBNAME, val, ref_city FROM vstb WHERE val > 1",
            expected,
            "SELECT TAGS with data predicate",
        )
