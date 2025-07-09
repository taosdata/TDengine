import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDbFlush:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_db_flush(self):
        """flush database

        1. -

        Catalog:
            - Database:Flush

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/ts-6314.sim

        """

        dbPrefix = "test"
        tbPrefix = "test"
        tbNum = 1
        rowNum = 9000

        totalNum = tbNum * rowNum

        ts0 = 1685959190000
        delta = 1
        tdLog.info(f"========== ts-6314")

        tdSql.execute(f"drop database if exists test")
        tdSql.execute(f"create database test vgroups 1 stt_trigger 1")

        tdLog.info(f"====== create tables")
        tdSql.execute(f"use test")
        tdSql.execute(
            f"create table t1 (ts timestamp, v1 varchar(20) primary key, v2 int)"
        )

        tdLog.info(f"============ insert 8192 rows")

        ts = ts0
        val = 0

        tdLog.info(f"============ {val}")

        x = 0

        while x < 8191:
            xs = x * delta
            ts = ts0 + xs
            tdSql.execute(f"insert into t1 values ( {ts} , 'abcdefghijklmn' , {val} )")
            x = x + 1
            val = val + 1

        tdLog.info(f"====== 8192 rows inserted ,  flush database")
        tdSql.execute(f"flush database test")

        tdLog.info(f"====== insert remain 809 rows")
        x = x - 1

        while x < 9000:
            xs = x * delta
            ts = ts0 + xs

            tdSql.execute(f"insert into t1 values ( {ts} , 'abcdefghijklmn' , {val} )")
            x = x + 1
            val = val + 1

        tdSql.execute(f"flush database test")

        tdLog.info(
            f"================== flush database, total insert 9001 rows, with one duplicate ts, start query"
        )
        tdSql.query(f"select * from test.t1")

        tdSql.checkRows(9000)
