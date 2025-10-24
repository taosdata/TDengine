from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDistinct:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_distinct(self):
        """Distinct

        1. Using on data columns and tag columns
        2. Using on super tables

        Catalog:
            - Query:Distinct

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/distinct.sim

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
        tdSql.execute(f"create database {db} keep 36500d ")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags(t1 int, t2 int)")

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} , 0 )")
            i = i + 1
            tdSql.execute(f"insert into {tb} values('2015-08-18 00:00:00', 1);")
            tdSql.execute(f"insert into {tb} values('2015-08-18 00:06:00', 2);")
            tdSql.execute(f"insert into {tb} values('2015-08-18 00:12:00', 3);")
            tdSql.execute(f"insert into {tb} values('2015-08-18 00:18:00', 4);")
            tdSql.execute(f"insert into {tb} values('2015-08-18 00:24:00', 5);")
            tdSql.execute(f"insert into {tb} values('2015-08-18 00:30:00', 6);")
        i = 0
        tb = tbPrefix + str(i)

        tdLog.info(f"====== table created")

        #### select distinct tag
        tdSql.query(f"select distinct t1 from {stb}")
        tdSql.checkRows(tbNum)

        #### select distinct tag
        tdSql.query(f"select distinct t2 from {stb}")
        tdSql.checkRows(1)
        # return -1

        #### select multi normal column
        tdSql.query(f"select distinct ts, c1  from {stb}")
        tdSql.checkRows(6)

        #### select multi column
        tdSql.query(f"select distinct ts from {stb}")
        tdSql.checkRows(6)

        ### select multi normal column
        ### select distinct multi column on sub table

        tdSql.query(f"select distinct ts, c1 from {tb}")
        tdSql.checkRows(6)

        ### select distinct
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
