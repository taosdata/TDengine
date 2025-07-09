from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertDrop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_drop(self):
        """insert sub table (drop)

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion:Delete

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/insert_drop.sim

        """

        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== insert_drop.sim")
        i = 0
        db = "iddb"
        stb = "stb"

        tdSql.prepare(dbname=db)
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags(t1 int)")

        i = 0
        ts = ts0
        while i < 10:
            tb = "tb" + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"====== tables created")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")
        tdSql.execute(f"drop table tb5")
        i = 0
        while i < 4:
            tb = "tb" + str(i)
            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table tb5 using {stb} tags(5)")
        tdSql.query(f"select * from tb5")
        tdLog.info(f"{tdSql.getRows()}) should be 0")
        tdSql.checkRows(0)
