import os
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUdfC:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _find_lib(proj_path, name):
        """Find a UDF library under proj_path build tree."""
        is_win = platform.system().lower() == 'windows'
        filename = f"{name}.dll" if is_win else f"lib{name}.so"
        for root, dirs, files in os.walk(proj_path):
            if filename in files:
                full = os.path.join(root, filename)
                if "build" in full:
                    return full
        return ""

    def test_udf_c(self):
        """Udf for C language

        1. Compile UDF C code
        2. Create scalar UDF function bit_and
        3. Create aggregate UDF function l2norm
        4. Test scalar UDF function bit_and

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/query/udf.sim

        """

        tdLog.info(f"======== step1 udf")

        # Locate pre-built UDF libraries from the CMake build tree
        selfPath = os.path.dirname(os.path.realpath(__file__))
        # Walk upward to find project root (contains 'debug/' build dir)
        projPath = selfPath
        while projPath and projPath != os.path.dirname(projPath):
            if os.path.isdir(os.path.join(projPath, "debug")):
                break
            projPath = os.path.dirname(projPath)

        bitand_path = self._find_lib(projPath, "bitand")
        l2norm_path = self._find_lib(projPath, "l2norm")
        if not bitand_path or not l2norm_path:
            raise RuntimeError(
                f"UDF libraries not found under {projPath}. "
                "Build targets 'bitand' and 'l2norm' first (cmake --build . --target bitand l2norm)."
            )
        tdLog.info(f"bitand lib: {bitand_path}")
        tdLog.info(f"l2norm lib: {l2norm_path}")

        # Drop ALL leftover functions from any previous test runs
        functions = tdSql.getResult("show functions")
        if functions:
            for func in functions:
                tdSql.execute(f"drop function if exists {func[0]}")

        tdSql.execute(f"drop database if exists udf;")
        tdSql.execute(f"create database udf vgroups 3;")
        tdSql.execute(f"use udf;")
        tdSql.query(f"select * from information_schema.ins_databases;")

        tdSql.execute(f"create table t (ts timestamp, f int);")
        tdSql.execute(f"insert into t values(now, 1)(now+1s, 2);")

        if platform.system().lower() == "windows":
            tdSql.execute(
                f"create function bit_and as '{bitand_path}' outputtype int;"
            )
            tdSql.execute(
                f"create aggregate function l2norm as '{l2norm_path}' outputtype double bufSize 8;"
            )
        else:
            tdSql.execute(
                f"create function bit_and as '{bitand_path}' outputtype int;"
            )
            tdSql.execute(
                f"create aggregate function l2norm as '{l2norm_path}' outputtype double bufSize 8;"
            )

        tdSql.error(
            f"create function bit_and as '/tmp/udf/libbitand.so' oputtype json;"
        )

        tdSql.query(f"show functions;")
        tdSql.checkRows(2)

        tdSql.query(f"select bit_and(f, f) from t;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 2)

        tdSql.query(f"select l2norm(f) from t;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.236067977)

        tdSql.execute(f"create table t2 (ts timestamp, f1 int, f2 int);")
        tdSql.execute(f"insert into t2 values(now, 0, 0)(now+1s, 1, 1);")
        tdSql.query(f"select bit_and(f1, f2) from t2;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select l2norm(f1, f2) from t2;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.execute(f"insert into t2 values(now+2s, 1, null)(now+3s, null, 2);")
        tdSql.query(f"select bit_and(f1, f2) from t2;")
        tdLog.info(
            f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)} , {tdSql.getData(2,0)} , {tdSql.getData(3,0)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.query(f"select l2norm(f1, f2) from t2;")
        tdLog.info(f"{tdSql.getRows()}), {tdSql.getData(0,0)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.645751311)

        tdSql.execute(f"insert into t2 values(now+4s, 4, 8)(now+5s, 5, 9);")
        tdSql.query(f"select l2norm(f1-f2), l2norm(f1+f2) from t2;")
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 5.656854249)

        tdSql.checkData(0, 1, 18.547236991)

        tdSql.query(f"select l2norm(bit_and(f2, f1)), l2norm(bit_and(f1, f2)) from t2;")
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 1.414213562)

        tdSql.query(
            f"select l2norm(f2) from udf.t2 group by 1-bit_and(f1, f2) order by 1-bit_and(f1,f2);"
        )
        tdLog.info(
            f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)} , {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 2.000000000)

        tdSql.checkData(1, 0, 9.055385138)

        tdSql.checkData(2, 0, 8.000000000)




# sql drop function bit_and;
# sql show functions;
# if $rows != 1 then
#  return -1
# endi
# if $tdSql.getData(0,0) != @l2norm@ then
#  return -1
#  endi
# sql drop function l2norm;
# sql show functions;
# if $rows != 0 then
#  return -1
# endi

# system sh/exec.sh -n dnode1 -s stop -x SIGINT
