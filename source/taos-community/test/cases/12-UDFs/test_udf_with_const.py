import os
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUdfPy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _find_lib(proj_path, name):
        is_win = platform.system().lower() == 'windows'
        filename = f"{name}.dll" if is_win else f"lib{name}.so"
        for root, _dirs, files in os.walk(proj_path):
            if filename in files:
                full = os.path.join(root, filename)
                if "build" in full:
                    return full
        return ""

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

        # Find pre-built gpd library from CMake build tree
        selfPath = os.path.dirname(os.path.realpath(__file__))
        projPath = selfPath
        while projPath and projPath != os.path.dirname(projPath):
            if os.path.isdir(os.path.join(projPath, "debug")):
                break
            projPath = os.path.dirname(projPath)

        gpd_path = self._find_lib(projPath, "gpd")
        if not gpd_path:
            raise RuntimeError(
                f"gpd library not found under {projPath}. "
                "Build target 'gpd' first (cmake --build . --target gpd)."
            )
        tdLog.info(f"gpd lib path: {gpd_path}")

        tdSql.execute(f"drop function if exists gpd;")
        tdSql.execute(f"drop database if exists udf;")
        tdSql.execute(f"create database udf vgroups 3;")
        tdSql.execute(f"use udf;")

        tdSql.execute(f"create table t1 (ts timestamp, f int);")
        tdSql.execute(f"insert into t1 values(now, 1)(now+1s, 2);")

        tdSql.execute(f"create function gpd as '{gpd_path}' outputtype int;")

        # gpd takes (ts, tbname, dbname) — test with const string parameters
        tdSql.query(f"select gpd(ts, 't1', 'udf') from t1;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)

        tdSql.execute(f"drop function gpd;")
        tdSql.execute(f"drop database udf;")
