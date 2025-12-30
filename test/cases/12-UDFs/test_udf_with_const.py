import os
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUdfPy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_udf_py(self):
        """Udf C for const

        1. Create database and normal table for udf test
        2. Create function gpd with C code that has const parameter
        3. Insert data into normal table
        4. Query function gpd with const parameter from normal table

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/query/udf_with_const.sim

        """

        tdLog.info(f"======== step1 udf")
        os.system("cases/12-UDFs/sh/compile_udf.sh")

        tdSql.execute(f"create database udf vgroups 3;")
        tdSql.execute(f"use udf;")

        tdSql.execute(f"create table t (ts timestamp, f int);")
        tdSql.execute(
            f"insert into t values(now, 1)(now+1s, 2)(now+2s,3)(now+3s,4)(now+4s,5)(now+5s,6)(now+6s,7);"
        )

        tdSql.execute(f"create function gpd as '/tmp/udf/libgpd.so' outputtype int;")

        tdSql.query(f"show functions;")
        tdSql.checkRows(1)

        tdSql.query(f"select gpd(ts, tbname, 'detail') from t;")
        tdSql.checkRows(7)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.execute(f"drop function gpd;")
