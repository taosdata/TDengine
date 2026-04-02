from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFuncGconcat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_gconcat(self):
        """Agg-basic: group_concat

        Test the GROUP_CONCAT function

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-18 Stephen Jin

        """

        self.smoking()
        tdStream.dropAllStreamsAndDbs()

    def smoking(self):
        db = "testdb"
        tb = "testtb"
        rowNum = 10

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, tbcol varchar(10))")
        tdSql.execute(f"CREATE STABLE s_ptr_data (ts TIMESTAMP, test_value DOUBLE, test_flag INT, hi_limit DOUBLE, lo_limit DOUBLE) TAGS (factory VARCHAR(32), fileid VARCHAR(64), testnum INT, testtxt VARCHAR(256), headnum INT, sitenum INT, productid VARCHAR(64), lotid VARCHAR(64), sublotid VARCHAR(64), jobname VARCHAR(128), flowid VARCHAR(32), datatype VARCHAR(10), nodename VARCHAR(32))")

        x = 0
        while x < rowNum:
            ms = 1601481600000 + x * 60000
            xfield = str(x)
            tdSql.execute(f"insert into {tb} values ({ms} , {xfield})")

            x = x + 1

        tdSql.query(f"select group_concat(tbcol, '?') from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '0?1?2?3?4?5?6?7?8?9')

        tdSql.query(f"SELECT _wstart as ws, AVG(test_value) as avg_test_value, GROUP_CONCAT(CAST(test_value AS VARCHAR) , ',') FROM s_ptr_data PARTITION BY lotId, subLotId, headNum, siteNum, jobName, testTxt, testNum count_window(100) ;")
        tdSql.checkRows(0)
