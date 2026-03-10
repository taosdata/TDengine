from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import os


class TestInsertBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_basic(self):
        """insert use ns precision

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/basic.sim

        """

        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table tb(ts timestamp,c1 double);")

        csvPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv")
        tdLog.debug(f"csvPath: {csvPath}")

        tdSql.error(f"insert into tb file '{csvPath}/1.csv';", expectErrInfo="syntax err", fullMatched=False)
        tdSql.error(f"insert into tb file '{csvPath}/2.csv';", expectErrInfo="syntax err", fullMatched=False)
        tdSql.execute(f"insert into tb file '{csvPath}/3.csv';")
        tdSql.query("select * from tb;")
        tdSql.checkRows(10)