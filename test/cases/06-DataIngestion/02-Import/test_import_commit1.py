from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestImportCommit1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_import_commit1(self):
        """import data commit1

        1. 

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/import_commit1.sim

        """

        dbPrefix = "ic_db"
        tbPrefix = "ic_tb"
        stbPrefix = "ic_stb"
        tbNum = 1
        # $rowNum = 166
        rowNum = 1361
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== limit.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")

        i = 0
        ts = ts0
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, c1 int)")
        x = 0
        while x < rowNum:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into {tb} values ( {ts} , {x} )")
            x = x + 1

        tdLog.info(f"====== tables created")

        ts = ts0 + delta
        ts = ts + 1
        tdSql.execute(f"import into {tb} values ( {ts} , -1)")
        tdSql.query(f"select count(*) from {tb}")
        res = rowNum + 1
        tdSql.checkData(0, 0, res)
