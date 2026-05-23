import os
import platform
import shutil
import tempfile
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUdf:

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

    @staticmethod
    def _get_proj_path():
        p = os.path.dirname(os.path.realpath(__file__))
        while p and p != os.path.dirname(p):
            if os.path.isdir(os.path.join(p, "debug")):
                return p
            p = os.path.dirname(p)
        return p

    def test_udf(self):
        """Udf python sim case

        1. Create database and normal table for udf test
        2. Create scalar UDF function bit_and with python file
        3. Create aggregate UDF function l2norm with python file
        4. Insert data into normal table
        5. Query scalar UDF function bit_and from normal table
        6. Query aggregate UDF function l2norm from normal table
        7. Test UDF with null values
        8. Test UDF with multiple columns
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/query/udfpy.sim

        """

        # system sh/cfg.sh -n dnode1 -c udf -v 1

        tdLog.info(f"======== step1 udf")

        # Locate pre-built C UDF DLLs/SOs from CMake build tree
        projPath = self._get_proj_path()
        bitand_path = self._find_lib(projPath, "bitand")
        l2norm_path = self._find_lib(projPath, "l2norm")
        if not bitand_path or not l2norm_path:
            raise RuntimeError(
                f"UDF libraries not found under {projPath}. "
                "Build targets 'bitand' and 'l2norm' first."
            )

        # Copy Python UDF scripts to a temp directory
        is_win = platform.system().lower() == 'windows'
        pyudf_dir = os.path.join(tempfile.gettempdir(), "pyudf")
        os.makedirs(pyudf_dir, exist_ok=True)
        # Python UDF source files are in docs/examples/udf/
        udf_examples = os.path.join(projPath, "source", "taos-community", "docs", "examples", "udf")
        for pyfile in ("pybitand.py", "pyl2norm.py", "pycumsum.py"):
            src = os.path.join(udf_examples, pyfile)
            dst = os.path.join(pyudf_dir, pyfile)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
            else:
                tdLog.info(f"Warning: {src} not found, skipping")
        tdLog.info(f"Python UDF dir: {pyudf_dir}")

        pybitand_path = os.path.join(pyudf_dir, "pybitand.py")
        pyl2norm_path = os.path.join(pyudf_dir, "pyl2norm.py")

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

        tdSql.execute(
            f"create function bit_and as '{bitand_path}' outputtype int;"
        )
        tdSql.execute(
            f"create aggregate function l2norm as '{l2norm_path}' outputtype double bufSize 8;"
        )

        tdSql.execute(
            f"create function pybitand as '{pybitand_path}' outputtype int language 'python';"
        )
        tdSql.execute(
            f"create aggregate function pyl2norm as '{pyl2norm_path}' outputtype double bufSize 128 language 'python';"
        )

        tdSql.query(f"show functions;")
        tdSql.checkRows(4)

        tdSql.query(
            f"select func_language, name from information_schema.ins_functions order by name"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "C")

        tdSql.checkData(1, 0, "C")

        tdSql.checkData(2, 0, "Python")

        tdSql.checkData(3, 0, "Python")

        tdSql.query(f"select bit_and(f, f) from t;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 2)

        tdSql.query(f"select pybitand(f, f) from t;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 2)

        tdSql.query(f"select l2norm(f) from t;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 2.236067977)

        tdSql.query(f"select pyl2norm(f) from t;")
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

        tdSql.query(f"select pybitand(f1, f2) from t2;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select pyl2norm(f1, f2) from t2;")
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

        tdSql.query(f"select pybitand(f1, f2) from t2;")
        tdLog.info(
            f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)} , {tdSql.getData(2,0)} , {tdSql.getData(3,0)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.query(f"select pyl2norm(f1, f2) from t2;")
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

        tdSql.query(f"select pyl2norm(f1-f2), pyl2norm(f1+f2) from t2;")
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 5.656854249)

        tdSql.checkData(0, 1, 18.547236991)

        tdSql.query(
            f"select pyl2norm(pybitand(f2, f1)), pyl2norm(pybitand(f1, f2)) from t2;"
        )
        tdLog.info(f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1.414213562)

        tdSql.checkData(0, 1, 1.414213562)

        tdSql.query(
            f"select pyl2norm(f2) from udf.t2 group by 1-pybitand(f1, f2) order by 1-pybitand(f1,f2);"
        )
        tdLog.info(
            f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)} , {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 2.000000000)

        tdSql.checkData(1, 0, 9.055385138)

        tdSql.checkData(2, 0, 8.000000000)

        # sql create aggregate function pycumsum as '/tmp/pyudf/pycumsum.py' outputtype double bufSize 128 language 'python';
        # sql select pycumsum(f2) from udf.t2
        # print ======= pycumsum
        # print $rows $tdSql.getData(0,0)
        # if $rows != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != 20.000000000 then
        #  return -1
        # endi
        # sql drop function pycumsum

        tdSql.execute(
            f"create or replace function bit_and as '{bitand_path}' outputtype int"
        )
        tdSql.query(
            f"select func_version from information_schema.ins_functions where name='bit_and'"
        )
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select bit_and(f1, f2) from t2;")
        tdLog.info(
            f"{tdSql.getRows()}) , {tdSql.getData(0,0)} , {tdSql.getData(1,0)} , {tdSql.getData(2,0)} , {tdSql.getData(3,0)} , {tdSql.getData(4,0)} , {tdSql.getData(5,0)}"
        )
        tdSql.checkRows(6)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, None)

        tdSql.checkData(3, 0, None)

        tdSql.checkData(4, 0, 0)

        tdSql.checkData(5, 0, 1)


# sql drop function bit_and;
# sql show functions;
# if $rows != 1 then
#  return -1
# endi
# if $tdSql.getData(0,0,l2norm@ then
#  return -1
#  endi
# sql drop function l2norm;
# sql show functions;
# if $rows != 0 then
#  return -1
# endi

# system sh/exec.sh -n dnode1 -s stop -x SIGINT
