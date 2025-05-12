from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSelectAcrossVnodes:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_across_vnodes(self):
        """select across vnodes

        1.

        Catalog:
            - Query:Others

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/parser/select_across_vnodes.sim

        """

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
        tdSql.execute(f"create database {db} vgroups 10")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'涛思" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1
        tdLog.info(f"====== tables created")

        ##### select * from stb
        tdSql.query(f"select * from {stb}")
        tdSql.checkRows(totalNum)

        ##### select * from $stb with limit
        tdSql.query(f"select * from {stb} limit 1")
        tdSql.checkRows(1)

        limit = int(rowNum / 2)
        tdSql.query(f"select * from {stb} limit {limit}")
        tdSql.checkRows(limit)

        tdSql.query(f"select last(*) from {stb} where t1 >= 0 group by t1 limit 5")
        tdSql.checkRows(tbNum)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
