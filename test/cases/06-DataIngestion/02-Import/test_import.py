from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestImport:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_import(self):
        """import data

        1.

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/import.sim

        """

        dbPrefix = "impt_db"
        tbPrefix = "tb"
        stbPrefix = "stb"
        tbNum = 10
        rowNum = 10
        totalNum = tbNum * rowNum
        tdLog.info(f"========== import.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 timestamp)")
        tdSql.execute(f"insert into tb values ('2019-05-05 11:30:00.000', 1, now)")
        tdSql.execute(f"insert into tb values ('2019-05-05 12:00:00.000', 1, now)")
        tdSql.execute(f"import into tb values ('2019-05-05 11:00:00.000', -1, now)")
        tdSql.execute(f"import into tb values ('2019-05-05 11:59:00.000', -1, now)")
        tdSql.execute(f"import into tb values ('2019-05-04 08:00:00.000', -1, now)")
        tdSql.execute(f"import into tb values ('2019-05-04 07:59:00.000', -1, now)")

        tdSql.query(f"select * from tb")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2019-05-04 07:59:00")
        tdSql.checkData(1, 0, "2019-05-04 08:00:00")
        tdSql.checkData(2, 0, "2019-05-05 11:00:00")
        tdSql.checkData(3, 0, "2019-05-05 11:30:00")
        tdSql.checkData(4, 0, "2019-05-05 11:59:00")
        tdSql.checkData(5, 0, "2019-05-05 12:00:00")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.connect("root")

        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from tb")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2019-05-04 07:59:00")
        tdSql.checkData(1, 0, "2019-05-04 08:00:00")
        tdSql.checkData(2, 0, "2019-05-05 11:00:00")
        tdSql.checkData(3, 0, "2019-05-05 11:30:00")
        tdSql.checkData(4, 0, "2019-05-05 11:59:00")
        tdSql.checkData(5, 0, "2019-05-05 12:00:00")
