import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDeleteReuse2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_delete_reuse2(self):
        """db reuse 2

        1. -

        Catalog:
            - Database:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/delete_reuse2.sim

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database d1 replica 1")
        tdSql.execute(f"create database d2 replica 1")
        tdSql.execute(f"create database d3 replica 1")
        tdSql.execute(f"create database d4 replica 1")

        tdSql.execute(f"create table d1.t1 (ts timestamp, i int)")
        tdSql.execute(f"create table d2.t2 (ts timestamp, i int)")
        tdSql.execute(f"create table d3.t3 (ts timestamp, i int)")
        tdSql.execute(f"create table d4.t4 (ts timestamp, i int)")

        tdSql.execute(f"insert into d2.t2 values(now, 1)")
        tdSql.execute(f"insert into d1.t1 values(now, 1)")
        tdSql.execute(f"insert into d3.t3 values(now, 1)")
        tdSql.execute(f"insert into d4.t4 values(now, 1)")

        tdSql.query(f"select * from d1.t1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d2.t2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d3.t3")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d4.t4")
        tdSql.checkRows(1)

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop database d1")
        tdSql.error(f"insert into d1.t1 values(now, 2)")

        tdLog.info(f"========= step3")
        tdSql.execute(f"create database db1 replica 1")
        tdSql.execute(f"reset query cache")

        tdSql.execute(f"create table db1.tb1 (ts timestamp, i int)")
        tdSql.execute(f"insert into db1.tb1 values(now, 2)")
        tdSql.query(f"select * from db1.tb1")
        tdSql.checkRows(1)

        tdLog.info(f"========= step4")
        x = 1
        while x < 20:
            db = "db" + str(x)
            tb = "tb" + str(x)
            tdSql.execute(f"use {db}")
            tdSql.execute(f"drop database {db}")

            tdSql.error(f"insert into {tb} values(now, -1)")

            x = x + 1
            db = "db" + str(x)
            tb = "tb" + str(x)

            tdSql.execute(f"reset query cache")

            tdSql.execute(f"create database {db} replica 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(f"create table {tb} (ts timestamp, i int)")
            tdSql.execute(f"insert into {tb} values(now, {x} )")
            tdSql.query(f"select * from {tb}")
            tdSql.checkRows(1)

            tdLog.info(f"===> loop times: {x}")
