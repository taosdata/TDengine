import os
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUdf:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

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
        os.system("cases/12-UDFs/sh/compile_udf.sh")
        os.system("cases/12-UDFs/sh/prepare_pyudf.sh")
        os.system("mkdir -p /tmp/pyudf")
        os.system("cp cases/12-UDFs/sh/pybitand.py /tmp/pyudf/")
        os.system("cp cases/12-UDFs/sh/pyl2norm.py /tmp/pyudf/")
        os.system("cp cases/12-UDFs/sh/pycumsum.py /tmp/pyudf/")
        os.system("ls /tmp/pyudf")

        tdSql.execute(f"create database udf vgroups 3;")
        tdSql.execute(f"use udf;")
        tdSql.query(f"select * from information_schema.ins_databases;")

        tdSql.execute(f"create table t (ts timestamp, f int);")
        tdSql.execute(f"insert into t values(now, 1)(now+1s, 2);")

        tdSql.execute(
            f"create function bit_and as '/tmp/udf/libbitand.so' outputtype int;"
        )
        tdSql.execute(
            f"create aggregate function l2norm as '/tmp/udf/libl2norm.so' outputtype double bufSize 8;"
        )

        tdSql.execute(
            f"create function pybitand as '/tmp/pyudf/pybitand.py' outputtype int language 'python';"
        )
        tdSql.execute(
            f"create aggregate function pyl2norm as '/tmp/pyudf/pyl2norm.py' outputtype double bufSize 128 language 'python';"
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
            f"create or replace function bit_and as '/tmp/udf/libbitand.so' outputtype int"
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
