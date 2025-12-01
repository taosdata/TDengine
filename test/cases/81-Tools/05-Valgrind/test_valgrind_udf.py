import os
import platform
import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindUdf:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_udf(self):
        """valgrind udf

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkUdf.sim

        """

        tdLog.info(f"======== step1 udf")
        os.system("cases/50-Others/01-Valgrind/sh/copy_udf.sh")

        tdSql.execute(f"create database udf vgroups 3;")
        tdSql.execute(f"use udf;")
        tdSql.query(f"select * from information_schema.ins_databases;")

        tdSql.execute(f"create table t (ts timestamp, f int);")
        tdSql.execute(f"insert into t values(now, 1)(now+1s, 2);")

        tdSql.execute(f"create function udf1 as '/tmp/udf/libudf1.so' outputtype int;")
        tdSql.execute(
            f"create aggregate function udf2 as '/tmp/udf/libudf2.so' outputtype double bufSize 8;"
        )

        tdSql.query(f"show functions;")
        tdSql.checkRows(2)

        tdSql.query(f"select udf1(f) from t;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select udf2(f) from t;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.236067977)

        tdSql.execute(f"create table t2 (ts timestamp, f1 int, f2 int);")
        tdSql.execute(f"insert into t2 values(now, 0, 0)(now+1s, 1, 1);")
        tdSql.query(f"select udf1(f1, f2) from t2;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select udf2(f1, f2) from t2;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.execute(f"insert into t2 values(now+2s, 1, null)(now+3s, null, 2);")
        tdSql.query(f"select udf1(f1, f2) from t2;")
        tdLog.info(
            f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)} , {tdSql.getData(2,0)} , {tdSql.getData(3,0)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.query(f"select udf2(f1, f2) from t2;")
        tdLog.info(f"{tdSql.getRows()}), {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.645751311)

        tdSql.execute(f"insert into t2 values(now+4s, 4, 8)(now+5s, 5, 9);")
        tdSql.query(f"select udf2(f1-f2), udf2(f1+f2) from t2;")
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 5.656854249)

        tdSql.checkData(0, 1, 18.547236991)

        tdSql.query(f"select udf2(udf1(f2-f1)), udf2(udf1(f2/f1)) from t2;")
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.000000000)

        tdSql.checkData(0, 1, 1.732050808)

        tdSql.query(
            f"select udf2(f2) from udf.t2 group by 1-udf1(f1) order by 1-udf1(f1)"
        )
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)}")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 2.000000000)

        tdSql.checkData(1, 0, 12.083045974)

        tdSql.execute(f"drop function udf1;")
        tdSql.query(f"show functions;")
        tdSql.checkRows(1)

        tdSql.checkData("udf2")

        tdSql.execute(f"drop function udf2;")
        tdSql.query(f"show functions;")
        tdSql.checkRows(0)
