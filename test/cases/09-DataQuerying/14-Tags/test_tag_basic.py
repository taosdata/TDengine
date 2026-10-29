# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestTagBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_constant_tag_aggregate(self):
        """Single-table constant tag aggregation

        1. Filtered SUM/AVG/COUNT across tables and vgroups, including NULL tags
        2. Memory and file blocks, with ordinary data-column aggregation
        3. DISTINCT, tag filters, mixed-table windows and ordered subquery fallback
        4. Integer partial sums wider than block statistics
        5. Reuse constant tag blocks for empty-table count placeholders

        Catalog:
            - Query:Tags

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-13 Codex Add constant tag aggregate regression
            - 2026-10-22 Jing Sima Cover empty groups after constant tag blocks
        """
        self.do_constant_tag_aggregate()
        self.do_constant_tag_empty_groups()

    def do_constant_tag_empty_groups(self):
        db = "constant_tag_empty_groups"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 1 keep 5000")
            tdSql.execute(f"use {db}")
            tdSql.execute("create stable st(ts timestamp, v int) "
                          "tags(t_float_null float, iv bigint, label varchar(8))")
            tables = ("empty_group_a", "empty_group_b", "empty_group_c")
            values = "(1451606400000,1)(1451606400001,2)(1451606400002,3)"
            for table in tables:
                tdSql.execute(f"create table {table} using st tags(NULL,7,'x')")
                tdSql.execute(f"insert into {table} values {values}")

            for on_disk in (False, True):
                for empty_table in tables:
                    # Rotate the empty group so a populated group precedes it
                    # regardless of the executor's table/group traversal order.
                    tdSql.execute(f"delete from {empty_table}")
                    if on_disk:
                        tdSql.execute(f"flush database {db}")
                    for grouping in ("group by", "partition by"):
                        tdSql.query("select tbname,count(t_float_null) from st "
                                    f"{grouping} tbname order by tbname")
                        tdSql.checkRows(len(tables))
                        for row, table in enumerate(tables):
                            tdSql.checkData(row, 0, table)
                            tdSql.checkData(row, 1, 0)

                        tdSql.query("select tbname,count(t_float_null),count(iv),"
                                    "count(label),count(v),count(*) from st "
                                    f"{grouping} tbname order by tbname")
                        tdSql.checkRows(len(tables))
                        for row, table in enumerate(tables):
                            count = 0 if table == empty_table else 3
                            expected = (table, 0, count, count, count, count)
                            for col, value in enumerate(expected):
                                tdSql.checkData(row, col, value)
                    tdSql.execute(f"insert into {empty_table} values {values}")
            print("constant tag empty groups ......................... [ passed ]")
        finally:
            tdSql.execute(f"drop database if exists {db}")

    def do_constant_tag_aggregate(self):
        db = "constant_tag_aggregate"
        tdSql.execute(f"drop database if exists {db}")
        try:
            tdSql.execute(f"create database {db} vgroups 2 keep 5000")
            tdSql.execute(f"use {db}")
            tdSql.execute("create stable st(ts timestamp, velocity double, v double) "
                          "tags(fleet varchar(16), f double, iv bigint, label varchar(16))")
            tables = [
                ("a", "F", "8", "'A'", [(0, "2", "11"), (10, "0", "12"),
                                            (20, "3", "NULL"), (30, "NULL", "14")]),
                ("b", "F", "10", "'B'", [(1, "2", "21"), (11, "3", "22"), (21, "4", "23")]),
                ("c", "F", "NULL", "NULL", [(2, "2", "31"), (12, "0", "32")]),
                ("d", "G", "20", "'D'", [(3, "2", "41"), (13, "3", "42")]),
                ("null_tbl", "H", "NULL", "NULL", [(4, "2", "51")]),
            ]
            for table, fleet, tag, label, rows in tables:
                tdSql.execute(f"create table {table} using st tags('{fleet}', {tag}, {tag}, {label})")
                for offset, velocity, value in rows:
                    tdSql.execute(f"insert into {table} values({1451606400000 + offset}, {velocity}, {value})")

            expected = [
                ("F", 46, 9.2, 5, 46, 9.2, 5, 5, 108, 21.6, 5, 6),
                ("G", 40, 20, 2, 40, 20, 2, 2, 83, 41.5, 2, 2),
                ("H", None, None, 0, None, None, 0, 0, 51, 51, 1, 1),
            ]
            direct = ("select fleet,sum(f),avg(f),count(f),sum(iv),avg(iv),count(iv),count(label),"
                      "sum(v),avg(v),count(v),count(*) from st where velocity>1 group by fleet order by fleet")
            dense = ("select fleet,sum(f+0),avg(f+0),count(f+0),sum(iv+0),avg(iv+0),count(iv+0),count(label),"
                     "sum(v),avg(v),count(v),count(*) from st where velocity>1 group by fleet order by fleet")
            for on_disk in (False, True):
                if on_disk:
                    tdSql.execute(f"flush database {db}")
                for sql in (direct, dense):
                    tdSql.query(sql)
                    tdSql.checkRows(len(expected))
                    for row, values in enumerate(expected):
                        for col, value in enumerate(values):
                            tdSql.checkData(row, col, value)

                # Also cover scans that can supply block statistics on disk.
                tdSql.query("select fleet,sum(f),avg(f),count(f),sum(iv),avg(iv),count(iv) "
                            "from st group by fleet order by fleet")
                for row, values in enumerate([("F", 62, 62 / 7, 7, 62, 62 / 7, 7),
                                              ("G", 40, 20, 2, 40, 20, 2),
                                              ("H", None, None, 0, None, None, 0)]):
                    for col, value in enumerate(values):
                        tdSql.checkData(row, col, value)

                tdSql.query("select sum(distinct f),avg(distinct f),count(distinct f) from st where velocity>1")
                tdSql.checkData(0, 0, 38)
                tdSql.checkData(0, 1, 38 / 3)
                tdSql.checkData(0, 2, 3)
                tdSql.query("select fleet,sum(f),avg(f),count(f) from st "
                            "where velocity>1 and f>9 group by fleet order by fleet")
                tdSql.checkRows(2)
                for row, values in enumerate([("F", 30, 10, 3), ("G", 40, 20, 2)]):
                    for col, value in enumerate(values):
                        tdSql.checkData(row, col, value)

                tdSql.query("select sum(f),avg(f),count(f) from "
                            "(select ts,f from st where velocity>1 order by ts limit 4)")
                tdSql.checkData(0, 0, 38)
                tdSql.checkData(0, 1, 38 / 3)
                tdSql.checkData(0, 2, 3)
                tdSql.query("select fleet,sum(f),avg(f),count(f) from st where velocity>1 "
                            "partition by fleet interval(1d) order by fleet")
                tdSql.checkRows(3)
                for row, values in enumerate([("F", 46, 9.2, 5), ("G", 40, 20, 2), ("H", None, None, 0)]):
                    for col, value in enumerate(values):
                        tdSql.checkData(row, col, value)

            # Keep empty-table count placeholders on the original path.
            tdSql.execute("create table empty_tbl using st tags('G',30,30,'E')")
            tdSql.query("select count(f),count(iv),count(label),count(*) from empty_tbl where velocity>1")
            tdSql.checkRows(1)
            for col in range(4):
                tdSql.checkData(0, col, 0)
            tdSql.execute("create stable wide(ts timestamp, velocity int) tags(t bigint)")
            tdSql.execute("create table wide_a using wide tags(9223372036854775807)")
            tdSql.execute("insert into wide_a values(1451606400000,2)(1451606400001,2)(1451606400002,2)")
            for expression in ("t", "t+0"):
                tdSql.query(f"select avg({expression}),count({expression}) from wide where velocity>1")
                tdSql.checkData(0, 0, float(9223372036854775807))
                tdSql.checkData(0, 1, 3)
            print("constant tag aggregate ......................... [ passed ]")
        finally:
            tdSql.execute(f"drop database if exists {db}")

    def test_tag_basic(self):
        """Tag query

        1. Projection queries combining data columns and the tbname pseudocolumn
        2. Projection queries with the DISTINCT keyword
        3. Queries combining tags and selection functions like LAST、FITST、MAX、MIN
        4. Grouping, sorting, and filtering by tags

        Catalog:
            - Query:Tags

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/select_distinct_tag.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/select_with_tags.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/tag_scan.sim

        """

        self.DistinctTag()
        tdStream.dropAllStreamsAndDbs()
        self.SelectTags()
        tdStream.dropAllStreamsAndDbs()
        self.TagScan()
        tdStream.dropAllStreamsAndDbs()
        
    def DistinctTag(self):   
        dbPrefix = "sav_db"
        tbPrefix = "sav_tb"
        stbPrefix = "sav_stb"
        tbNum = 20
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} , 0 )")
            tdSql.execute(f"insert into {tb} (ts, c1) values (now, 1);")
            i = i + 1

        tdLog.info(f"====== table created")

        #### select distinct tag
        tdSql.query(f"select distinct t1 from {stb}")
        tdSql.checkRows(tbNum)

        #### select distinct tag
        tdSql.query(f"select distinct t2 from {stb}")
        tdSql.checkRows(1)

        #### unsupport sql
        tdSql.error(f"select distinct t1, t2 from &stb")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def SelectTags(self):
        dbPrefix = "select_tags_db"
        tbPrefix = "select_tags_tb"
        mtPrefix = "select_tags_mt"

        tbNum = 16
        rowNum = 800
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== select_with_tags.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12), t3 int)"
        )

        i = 0
        j = 1

        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc" + str(i) + "'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} , 123 )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100
                c1 = c + i

                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c1} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            j = j + 10000
            tstart = 100000 + j

        # ======================= only check first table tag, TD-4827
        tdSql.query(f"select count(*) from {mt} where t1 in (0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, rowNum)

        secTag = "'abc0'"
        tdSql.query(f"select count(*) from {mt} where t2 ={secTag}  and t1 in (0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, rowNum)

        # ================================
        tdSql.query(f"select ts from select_tags_mt0")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(12800)

        tdSql.query(f"select first(ts), tbname, t1, t2 from select_tags_mt0;", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(0, 1, "select_tags_tb0")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "abc0")

        tdSql.query(f"select last(ts), tbname, t1, t2 from select_tags_mt0;", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:04:10.800")
        tdSql.checkData(0, 1, "select_tags_tb15")
        tdSql.checkData(0, 2, 15)
        tdSql.checkData(0, 3, "abc15")

        tdSql.query(f"select min(c1), tbname, t1, t2 from select_tags_mt0;", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, "select_tags_tb0")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "abc0")

        tdSql.query(f"select max(c1), tbname, t1, t2 from select_tags_mt0;", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 114)
        tdSql.checkData(0, 1, "select_tags_tb15")
        tdSql.checkData(0, 2, 15)
        tdSql.checkData(0, 3, "abc15")

        tdSql.query(f"select top(c6, 3) from select_tags_mt0 interval(10a)", show=True)
        tdSql.query(
            f"select top(c3,10) from select_tags_mt0 partition by tbname,t1,t2 interval(10a)"
        )
        tdSql.query(
            f"select top(c6, 3) from select_tags_mt0 partition by tbname interval(10a)", show=True
        )

        tdSql.query(f"select top(c6, 10) from select_tags_mt0 interval(10a);", show=True)
        tdSql.checkRows(12800)

        tdSql.query(
            f"select ts, top(c1, 80), tbname, t1, t2 from select_tags_mt0 order by ts;", show=True
        )
        tdSql.checkRows(80)
        tdSql.checkData(0, 0, "1970-01-01 08:03:40.100")
        tdSql.checkData(1, 0, "1970-01-01 08:03:40.200")
        tdSql.checkData(0, 1, 111)
        tdSql.checkData(0, 2, "select_tags_tb12")
        tdSql.checkData(0, 3, 12)
        tdSql.checkData(0, 4, "abc12")

        tdSql.query(
            f"select ts, top(c1, 80), tbname, t1, t2 from select_tags_mt0 order by ts;", show=True
        )
        tdSql.checkRows(80)
        tdSql.checkData(0, 0, "1970-01-01 08:03:40.100")
        tdSql.checkData(1, 0, "1970-01-01 08:03:40.200")
        tdSql.checkData(0, 1, 111)
        tdSql.checkData(0, 2, "select_tags_tb12")
        tdSql.checkData(0, 3, 12)
        tdSql.checkData(0, 4, "abc12")

        tdSql.query(
            f"select ts, bottom(c1, 72), tbname, t1, t2 from select_tags_mt0 order by ts;", show=True
        )
        tdSql.checkRows(72)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.001")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdSql.query(f"select last_row(c1, c2), tbname, t1, t2 from select_tags_mt0;", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 114)

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 99.00000)
        tdSql.checkData(0, 2, "select_tags_tb15")
        tdSql.checkData(0, 3, 15)

        tdLog.info(f"====== selectivity+tags+group by tags=======================")
        tdSql.query(
            f"select first(c1), tbname, t1, t2, tbname from select_tags_mt0 group by tbname order by t1;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, "select_tags_tb0")
        tdSql.checkData(1, 1, "select_tags_tb1")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "abc0")
        tdSql.checkData(0, 4, "select_tags_tb0")

        tdSql.query(
            f"select last_row(ts,c1), tbname, t1, t2, tbname from select_tags_mt0 group by tbname order by t1;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.799")
        tdSql.checkData(1, 0, "1970-01-01 08:01:50.800")

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 99)
        tdSql.checkData(1, 1, 100)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdLog.info(f'"really this line"')
        tdSql.query(
            f"select distinct tbname,t1,t2 from select_tags_mt0 order by tbname;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "select_tags_tb0")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(1, 0, "select_tags_tb1")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "abc1")

        tdSql.query(f"select tbname,ts from select_tags_mt0 order by ts;")
        tdSql.checkRows(12800)

        tdLog.info(f"{tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkData(0, 0, "select_tags_tb0")
        tdSql.checkData(1, 0, "select_tags_tb0")
        tdSql.checkData(0, 1, "1970-01-01 08:01:40.000")
        tdSql.checkData(1, 1, "1970-01-01 08:01:40.001")

        tdSql.query(
            f"select ts, top(c1, 100), tbname, t1, t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by tbname order by ts;"
        )
        tdSql.checkRows(200)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.087")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.088")
        tdSql.checkData(2, 0, "1970-01-01 08:01:40.089")
        tdSql.checkData(9, 0, "1970-01-01 08:01:40.096")
        tdSql.checkData(0, 1, 87)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdSql.query(
            f"select ts, top(c1, 100), tbname, t1, t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') partition by tbname order by ts;"
        )
        tdSql.checkRows(200)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.087")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.088")
        tdSql.checkData(2, 0, "1970-01-01 08:01:40.089")
        tdSql.checkData(9, 0, "1970-01-01 08:01:40.096")
        tdSql.checkData(0, 1, 87)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdSql.query(
            f"select ts, top(c1, 2), t2, tbname, t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by tbname,t2 order by ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.099")
        tdSql.checkData(0, 1, 99)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, "select_tags_tb0")
        tdSql.checkData(0, 4, "abc0")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.199")
        tdSql.checkData(1, 1, 99)
        tdSql.checkData(1, 2, "abc0")
        tdSql.checkData(1, 3, "select_tags_tb0")
        tdSql.checkData(1, 4, "abc0")
        tdSql.checkData(2, 0, "1970-01-01 08:01:50.100")
        tdSql.checkData(2, 1, 100)
        tdSql.checkData(2, 2, "abc1")
        tdSql.checkData(2, 3, "select_tags_tb1")
        tdSql.checkData(2, 4, "abc1")
        tdSql.checkData(3, 0, "1970-01-01 08:01:50.200")
        tdSql.checkData(3, 1, 100)
        tdSql.checkData(3, 2, "abc1")
        tdSql.checkData(3, 3, "select_tags_tb1")
        tdSql.checkData(3, 4, "abc1")

        # slimit /limit
        tdSql.query(
            f"select ts, top(c1, 2), t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by tbname,t2 limit 2 offset 1;"
        )
        tdSql.checkRows(2)

        tdLog.info(
            f"======= selectivity + tags + group by + tags + filter ==========================="
        )
        tdSql.query(
            f"select first(c1), t1, tbname from select_tags_mt0 where c1<=2 group by tbname order by t1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "select_tags_tb1")
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, "select_tags_tb2")

        tdSql.query(
            f"select _wstart, first(c1), tbname from select_tags_mt0 where c1<=2 interval(1s);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(1, 0, "1970-01-01 08:01:50.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "select_tags_tb1")
        tdSql.checkData(2, 0, "1970-01-01 08:02:00.000")
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, "select_tags_tb2")

        tdSql.query(f"select first(ts),ts from select_tags_tb0 where c1<3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tdSql.getData(0, 1))

        tdSql.query(f"select last(ts),ts from select_tags_tb0 where c1<3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tdSql.getData(0, 1))

        tdSql.query(f"select first(ts), ts  from select_tags_tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "1970-01-01 08:01:50.001")

        tdLog.info(
            f"======= selectivity + tags + group by + tags + filter + interval ================"
        )
        tdSql.query(
            f"select _wstart,first(c1), t2, t1, tbname, tbname from select_tags_mt0 where c1<=2 partition by tbname interval(1d) order by t1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "1970-01-01 00:00:00.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "select_tags_tb0")
        tdSql.checkData(0, 5, "select_tags_tb0")
        tdSql.checkData(1, 5, "select_tags_tb1")
        tdSql.checkData(2, 5, "select_tags_tb2")

        tdSql.query(
            f"select ts, top(c1, 5), t2, tbname from select_tags_mt0 where c1<=2 partition by tbname interval(1d) order by ts, t2;"
        )
        tdSql.checkRows(15)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.002")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, "select_tags_tb0")
        tdSql.checkData(9, 0, "1970-01-01 08:01:50.402")
        tdSql.checkData(9, 1, 2)
        tdSql.checkData(9, 2, "abc1")
        tdSql.checkData(9, 3, "select_tags_tb1")

        # if data
        tdSql.query(
            f"select ts, top(c1, 50), t2, t1, tbname, tbname from select_tags_mt0 where c1<=2  partition by tbname interval(1d) order by ts, t2;"
        )
        tdSql.checkRows(48)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "select_tags_tb0")
        tdSql.checkData(0, 5, "select_tags_tb0")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.001")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "abc0")
        tdSql.checkData(1, 3, 0)
        tdSql.checkData(1, 4, "select_tags_tb0")
        tdSql.checkData(1, 5, "select_tags_tb0")
        tdSql.checkData(9, 0, "1970-01-01 08:01:40.300")
        tdSql.checkData(9, 1, 0)
        tdSql.checkData(9, 2, "abc0")
        tdSql.checkData(9, 3, 0)
        tdSql.checkData(9, 4, "select_tags_tb0")
        tdSql.checkData(9, 5, "select_tags_tb0")

        tdSql.query(f"select last(ts),TBNAME from select_tags_mt0 interval(1y)")
        tdSql.checkRows(1)

        tdLog.info(
            f"TODO ======= selectivity + tags+ group by + tags + filter + interval + join==========="
        )

        tdLog.info(
            f"==========================mix tag columns and group by columns======================"
        )
        tdSql.query(
            f"select ts, top(c1, 100), tbname, t3 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by t3 order by ts, tbname;"
        )
        tdSql.checkRows(100)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.094")
        tdSql.checkData(0, 1, 94)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 123)
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.095")
        tdSql.checkData(1, 1, 95)
        tdSql.checkData(1, 2, "select_tags_tb0")
        tdSql.checkData(1, 3, 123)

        tdLog.info(f"======error sql=============================================")
        tdSql.error(f"select first(*), tbname from select_tags_mt0;")
        tdSql.error(f"select first(ts), first(c1),tbname from select_tags_mt0;")
        tdSql.error(f"select first(ts), last(ts), tbname from select_tags_mt0;")
        tdSql.error(
            f"select last_row(*), first(ts), tbname, t1, t2 from select_tags_mt0;"
        )
        tdSql.error(f"select tbname, last_row(*), t1, first(ts) from select_tags_mt0;")
        tdSql.error(f"select count(*), tbname from select_tags_mt0;")
        tdSql.error(f"select sum(c2), tbname from select_tags_mt0;")
        tdSql.error(f"select avg(c3), tbname from select_tags_mt0;")
        tdSql.error(f"select percentile(c3, 50), tbname from select_tags_mt0;")
        tdSql.error(f"select apercentile(c4, 50), tbname from select_tags_mt0;")
        tdSql.error(f"select spread(c2), tbname, t1 from select_tags_mt0;")
        tdSql.error(f"select stddev(c2), tbname from select_tags_mt0;")
        tdSql.error(f"select twa(c2), tbname from select_tags_mt0;")
        tdSql.error(f"select interp(c2), tbname from select_tags_mt0 where ts=100001;")

        tdSql.query(f"select count(tbname) from select_tags_mt0 interval(1d);")
        tdSql.query(f"select count(tbname) from select_tags_mt0 group by t1;")
        tdSql.query(f"select count(tbname),SUM(T1) from select_tags_mt0 interval(1d);")
        tdSql.error(
            f"select first(c1), count(*), t2, t1, tbname from select_tags_mt0 where c1<=2 interval(1d) group by tbname;"
        )
        tdSql.query(f"select ts from select_tags_mt0 interval(1y);")
        tdSql.error(f"select count(*), tbname from select_tags_mt0 interval(1y);")
        tdSql.query(f"select tbname, t1 from select_tags_mt0 interval(1y);")

        # ===error sql + group by ===============================================
        # valid sql: select first(c1), last(c2), tbname from select_tags_mt0 group by tbname;
        # valid sql: select first(c1), last(c2), count(*), tbname from select_tags_mt0 group by tbname;
        # valid sql: select first(c1), last(c2), count(*) from select_tags_mt0 group by tbname, t1;
        # valid sql: select first(c1), tbname, t1 from select_tags_mt0 group by t2;

        tdLog.info(f"==================================>TD-4231")
        tdSql.query(f"select t1,tbname from select_tags_mt0 where c1<0")
        tdSql.query(
            f"select t1,tbname from select_tags_mt0 where c1<0 and tbname in ('select_tags_tb12')"
        )
        tdSql.query(
            f"select tbname from select_tags_mt0 where tbname in ('select_tags_tb12');"
        )

        tdSql.query(f"select first(ts), tbname from select_tags_mt0 group by tbname;")
        tdSql.query(
            f"select count(c1) from select_tags_mt0 where c1=99 group by tbname;"
        )
        tdSql.query(f"select count(*),tbname from select_tags_mt0 group by tbname")

        tdLog.info(f"==================================> tag supported in group")
        tdSql.query(f"select t1,t2,tbname from select_tags_mt0 group by tbname;")
        tdSql.query(
            f"select first(c1), last(c2), t1 from select_tags_mt0 group by tbname;"
        )
        tdSql.query(
            f"select first(c1), last(c2), tbname, t2 from select_tags_mt0 group by tbname;"
        )
        tdSql.query(
            f"select first(c1), count(*), t2, t1, tbname from select_tags_mt0 group by tbname;"
        )
        
        tdSql.query(f"select t1, t2 from select_tags_tb1 group by t1, t2;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "abc1")
        
        tdSql.query(f"select tbname from select_tags_tb1 group by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "select_tags_tb1")
        
        tdSql.query(f"select t1, t2 from select_tags_tb1 partition by t1, t2;")
        tdSql.checkRows(800)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "abc1")
        
        tdSql.query(f"select tbname from select_tags_tb1 partition by tbname;")
        tdSql.checkRows(800)
        tdSql.checkData(0, 0, "select_tags_tb1")      
    
        tdSql.query(f"select tbname, t2 from select_tags_tb1 group by t1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "select_tags_tb1")
        tdSql.checkData(0, 1, "abc1")

    def TagScan(self):
        tdSql.connect("root")
        tdSql.execute(f"drop database if exists test")
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table st(ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values(now, 2);")
        tdSql.execute(f"insert into ct3 using st tags(3) values(now, 3);")
        tdSql.execute(f"insert into ct4 using st tags(4) values(now, 4);")

        tdSql.execute(f"create table st2(ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct21 using st2 tags(1) values(now, 1);")
        tdSql.execute(f"insert into ct22 using st2 tags(2) values(now, 2);")
        tdSql.execute(f"insert into ct23 using st2 tags(3) values(now, 3);")
        tdSql.execute(f"insert into ct24 using st2 tags(4) values(now, 4);")

        tdSql.query(f"select tbname, 1 from st group by tbname order by tbname;")
        tdLog.info(
            f"{tdSql.getRows()})  {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ct1")
        tdSql.checkData(1, 0, "ct2")

        tdSql.query(f"select tbname, 1 from st group by tbname slimit 0, 1;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, 1 from st group by tbname slimit 2, 2;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkRows(2)

        tdSql.query(
            f"select tbname, 1 from st group by tbname order by tbname slimit 0, 1;"
        )
        tdLog.info(
            f"{tdSql.getRows()})  {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdSql.execute(
            f"create table stt1(ts timestamp, f int) tags (t int, b varchar(10));"
        )
        tdSql.execute(f"insert into ctt11 using stt1 tags(1, '1aa') values(now, 1);")
        tdSql.execute(f"insert into ctt12 using stt1 tags(2, '1bb') values(now, 2);")
        tdSql.execute(f"insert into ctt13 using stt1 tags(3, '1cc') values(now, 3);")
        tdSql.execute(f"insert into ctt14 using stt1 tags(4, '1dd') values(now, 4);")
        tdSql.execute(f"insert into ctt14 values(now, 5);")

        tdSql.execute(
            f"create table stt2(ts timestamp, f int) tags (t int, b varchar(10));"
        )
        tdSql.execute(f"insert into ctt21 using stt2 tags(1, '2aa') values(now, 1);")
        tdSql.execute(f"insert into ctt22 using stt2 tags(2, '2bb') values(now, 2);")
        tdSql.execute(f"insert into ctt23 using stt2 tags(3, '2cc') values(now, 3);")
        tdSql.execute(f"insert into ctt24 using stt2 tags(4, '2dd') values(now, 4);")

        tdSql.query(f"select tags t, b from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(3,0)} {tdSql.getData(3,1)}"
        )
        tdSql.checkRows(4)
        tdSql.checkData(3, 1, "1dd")

        tdSql.query(f"select tags t, b from stt2 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(3,0)} {tdSql.getData(3,1)}"
        )
        tdSql.checkRows(4)
        tdSql.checkData(3, 1, "2dd")

        tdSql.query(f"select tags t,b,f from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(5)
        tdSql.checkData(4, 2, 5)

        tdSql.query(f"select tags tbname,t,b from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(3, 0, "ctt14")
        tdSql.checkData(3, 2, "1dd")

        tdSql.query(f"select tags t,b from stt1 where t=1")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "1aa")

        tdSql.query(f"select tags t,b from stt1 where tbname='ctt11'")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "1aa")

        tdSql.query(f"select tags t,b from ctt11")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "1aa")

        tdSql.execute(f"create table stb34 (ts timestamp, f int) tags(t int);")
        tdSql.execute(
            f"insert into ctb34 using stb34 tags(1) values(now, 1)(now+1s, 2);"
        )
        tdSql.query(f"select 1 from (select tags t from stb34 order by t)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from (select tags t from stb34)")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select 1 from (select tags ts from stb34)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select count(*) from (select tags ts from stb34)")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select tags 2 from stb34")
        tdSql.checkRows(1)
