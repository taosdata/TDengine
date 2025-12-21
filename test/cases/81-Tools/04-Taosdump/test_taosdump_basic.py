###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool
import inspect
import glob
import random
import os
import subprocess
import string


class TestTaosdumpBasic:

    #
    # ------------------- test_taosdump_test_basic.py ----------------
    #
    def do_taosdump_test_basic(self):
        self.tmpdir = "./taosdumptest/tmpdir_basic"        
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "create table db.nt1 (ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s -D db -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop database db")
        #        sys.exit(1)

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "db":
                found = True
                break

        assert found == True

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(3)

        print("do test basic ......................... [passed]")

    #
    # ------------------- test_taosdump_test.py ----------------
    #
    def checkCommunity(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if "community" in selfPath:
            return False
        else:
            return True

    def do_taosdump_test(self):
        self.ts = 1538548685000
        self.numberOfTables = 10000
        self.numberOfRecords = 100

        if not os.path.exists("./taosdumptest/tmp1"):
            os.makedirs("./taosdumptest/tmp1")
        else:
            os.system("rm -rf ./taosdumptest/tmp1")
            os.makedirs("./taosdumptest/tmp1")

        if not os.path.exists("./taosdumptest/tmp2"):
            os.makedirs("./taosdumptest/tmp2")
        else:
            os.system("rm -rf ./taosdumptest/tmp2")
            os.makedirs("./taosdumptest/tmp2")

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db duration 11 keep 3649")
        tdSql.execute("create database db1 duration 12 keep 3640")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))"
        )
        tdSql.execute("create table t1 using st tags(1, 'beijing')")
        sql = "insert into t1 values"
        currts = self.ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)
        tdSql.execute("create table t2 using st tags(2, 'shanghai')")
        sql = "insert into t2 values"
        currts = self.ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found: %s" % binPath)

        os.system("%s --password < %s --databases db -o ./taosdumptest/tmp1 -c /etc/taos" % (binPath, os.path.dirname(os.path.abspath(__file__)) + "/pwd.txt"))
        os.system("%s -p < %s --databases db1 -o ./taosdumptest/tmp2" % (binPath, os.path.dirname(os.path.abspath(__file__)) + "/pwd.txt"))

        tdSql.execute("drop database db")
        tdSql.execute("drop database db1")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        os.system("%s -W db=newdb -i ./taosdumptest/tmp1" % binPath)
        os.system("%s -W \"db=newdb|db1=newdb1\" -i ./taosdumptest/tmp2" % binPath)

        tdSql.execute("use newdb")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkRows(4)
        dbresult = tdSql.queryResult
        # 6--duration,7--keep0,keep1,keep

        isCommunity = self.checkCommunity()
        print("iscommunity: %d" % isCommunity)
        for i in range(len(dbresult)):
            if dbresult[i][0] == "newdb":
                print(dbresult[i])
                print(type(dbresult[i][6]))
                print(type(dbresult[i][7]))
                print((dbresult[i][6]))
                assert dbresult[i][6] == "11d"
                assert dbresult[i][7] == "3649d,3649d,3649d"
            if dbresult[i][0] == "newdb1":
                print((dbresult[i][6]))
                assert dbresult[i][6] == "12d"
                print((dbresult[i][7]))
                assert dbresult[i][7] == "3640d,3640d,3640d"

        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(2)
        dbresult = tdSql.queryResult
        print(dbresult)
        for i in range(len(dbresult)):
            assert (dbresult[i][0] == "t1") or (dbresult[i][0] == "t2")

        tdSql.query("select * from t1")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

        tdSql.query("select * from t2")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

        # drop all databases，boundary value testing.
        # length(databasename)<=32;length(tablesname)<=192
        tdSql.execute("drop database newdb")
        tdSql.execute("drop database newdb1")
        os.system("rm -rf ./taosdumptest/tmp1")
        os.system("rm -rf ./taosdumptest/tmp2")
        os.makedirs("./taosdumptest/tmp1")
        tdSql.execute("create database db12312313231231321312312312_323")
        
        tdSql.error(
            "create database db012345678911234567892234567893323456789423456789523456789bcdefe"
        )
        tdSql.execute("use db12312313231231321312312312_323")
        tdSql.execute(
            "create stable st12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))"
        )
        tdSql.error(
            "create stable st_12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))"
        )
        tdSql.execute(
            "create stable st(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))"
        )
        tdSql.error(
            "create stable st1(ts timestamp, c1 int, col2_012345678901234567890123456789012345678901234567890123456789 nchar(10)) tags(t1 int, t2 binary(10))"
        )

        tdSql.execute(
            "select * from db12312313231231321312312312_323.st12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9"
        )
        tdSql.error(
            "create table t0_12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678_9 using st tags(1, 'beijing')"
        )
        tdSql.query("show stables")
        tdSql.checkRows(2)
        os.system(
            "%s --databases db12312313231231321312312312_323 -o ./taosdumptest/tmp1"
            % binPath
        )
        tdSql.execute("drop database db12312313231231321312312312_323")
        os.system("%s -W db12312313231231321312312312_323=db12312313231231321312312312_323abc -i ./taosdumptest/tmp1" % binPath)
        tdSql.execute("use db12312313231231321312312312_323abc")
        tdSql.query("show stables")
        tdSql.checkRows(2)
        tdSql.execute("drop database db12312313231231321312312312_323abc")

        os.system("rm -rf ./taosdumptest/tmp1")
        os.system("rm -rf ./taosdumptest/tmp2")
        os.system("rm -rf ./dump_result.txt")
        os.system("rm -rf ./db.csv")

        print("do test basic1 ......................... [passed]")

    #
    # ------------------- test_taosdump_test2.py ----------------
    #
    def generateString(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        v = ""
        for i in range(length):
            v += random.choice(chars)
        return v

    def do_taosdump_test2(self):
        self.ts = 1601481600000
        self.numberOfTables = 1
        self.numberOfRecords = 150

        if not os.path.exists("./taosdumptest/tmp"):
            os.makedirs("./taosdumptest/tmp")
        else:
            print("directory exists")
            os.system("rm -rf ./taosdumptest/tmp/*")

        tdSql.prepare()

        tdSql.execute(
            "create table st(ts timestamp, c1 timestamp, c2 int, c3 bigint, c4 float, c5 double, c6 binary(8), c7 smallint, c8 tinyint, c9 bool, c10 nchar(8)) tags(t1 int)"
        )
        tdSql.execute("create table t1 using st tags(0)")
        currts = self.ts
        finish = 0
        while finish < self.numberOfRecords:
            sql = "insert into t1 values"
            for i in range(finish, self.numberOfRecords):
                sql += (
                    "(%d, 1019774612, 29931, 1442173978, 165092.468750, 1128.643179, 'MOCq1pTu', 18405, 82, 0, 'g0A6S0Fu')"
                    % (currts + i)
                )
                finish = i + 1
                if (1048576 - len(sql)) < 65519:
                    break
            tdSql.execute(sql)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        os.system("rm ./taosdumptest/tmp/*.sql")
        os.system("rm ./taosdumptest/tmp/*.avro*")
        os.system("rm -rf ./taosdumptest/taosdump.*")
        os.system("%s --databases db -o ./taosdumptest/tmp " % binPath)

        tdSql.execute("drop database db")
        tdSql.query("show databases")
        tdSql.checkRows(2)

        os.system("%s -i ./taosdumptest/tmp " % binPath)

        tdSql.query("show databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "db")

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, self.numberOfRecords)

        # test case for TS-1225
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute(
            "create table stb(ts timestamp, c1 binary(16374), c2 binary(16374), c3 binary(16374)) tags(t1 nchar(256))"
        )
        tdSql.execute(
            "insert into t1 using stb tags('t1') values(now, '%s', '%s', '%s')"
            % (
                self.generateString(16374),
                self.generateString(16374),
                self.generateString(16374),
            )
        )

        os.system("rm ./taosdumptest/tmp/*.sql")
        os.system("rm ./taosdumptest/tmp/*.avro*")
        os.system("rm -rf ./taosdumptest/tmp/taosdump.*")
        os.system("%s -D test -o ./taosdumptest/tmp " % binPath)

        tdSql.execute("drop database test")
        tdSql.query("show databases")
        tdSql.checkRows(3)

        os.system("%s -i ./taosdumptest/tmp " % binPath)

        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")

        tdSql.query("select * from stb")
        tdSql.checkRows(1)
        os.system("rm -rf dump_result.txt")

        print("do basic2 ............................. [passed]")

    #
    # ------------------- test_taosdump_test_loose_mode.py ----------------
    #
    def do_loose_mode(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 INT) tags(n1 INT)")
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(1640000000000, 1)")
        tdSql.execute("create table t2 using st tags(NULL)")
        tdSql.execute("insert into t2 values(1640000000000, NULL)")
        tdSql.execute(
            "create table db.nt1 (ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s -L -D db -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop database db")
        #        sys.exit(1)

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "db":
                found = True
                break

        assert found == True

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(3)

        print("do loose mode ......................... [passed]")

    #
    # ------------------- test_taosdump_test_inspect.py ----------------
    #
    def do_inspect(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s --databases db -o %s -T 1" % (binPath, self.tmpdir))

        #        sys.exit(1)
        avro_files = glob.glob(os.path.join(self.tmpdir, "taosdump.*", "*.avro*"))
        for avro_file in avro_files:
            taosdumpInspectCmd = f"{binPath} -I \"{avro_file}\" -s"
            print(taosdumpInspectCmd)
            os.system(taosdumpInspectCmd)

        avro_files = glob.glob(os.path.join(self.tmpdir, "taosdump.*", "*.avro*"))
        schemaTimes = 0
        for avro_file in avro_files:
            taosdumpInspectCmd = f"{binPath} -I \"{avro_file}\" -s"
            output = subprocess.check_output(taosdumpInspectCmd, shell=True).decode("utf-8")
            schemaTimes += sum(1 for line in output.splitlines() if "Schema:" in line)
        print("schema found times: %d" % int(schemaTimes))

        if int(schemaTimes) != 1:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected schema found times 1, actual %d"
                % (caller.filename, caller.lineno, int(schemaTimes))
            )

        data_avro_files = glob.glob(os.path.join(self.tmpdir, "taosdump*", "data*", "*.avro*"))
        schemaTimes = 0
        for avro_file in data_avro_files:
            taosdumpInspectCmd = f"{binPath} -I \"{avro_file}\" -s"
            output = subprocess.check_output(taosdumpInspectCmd, shell=True).decode("utf-8")
            schemaTimes += sum(1 for line in output.splitlines() if "Schema:" in line)
        print("schema found times: %d" % int(schemaTimes))

        if int(schemaTimes) != 2:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected schema found times 2, actual %d"
                % (caller.filename, caller.lineno, int(schemaTimes))
            )

        avro_files = glob.glob(os.path.join(self.tmpdir, "taosdump.*", "*.avro*"))
        recordsTimes = 0
        for avro_file in avro_files:
            taosdumpInspectCmd = f"{binPath} -I \"{avro_file}\""
            print(taosdumpInspectCmd)
            output = subprocess.check_output(taosdumpInspectCmd, shell=True).decode("utf-8")
            recordsTimes += sum(1 for line in output.splitlines() if "=== Records:" in line)
        print("records found times: %d" % int(recordsTimes))

        if int(recordsTimes) != 1:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected records found times 1, actual %d"
                % (caller.filename, caller.lineno, int(recordsTimes))
            )

        data_avro_files = glob.glob(os.path.join(self.tmpdir, "taosdump*", "data*", "*.avro*"))
        recordsTimes = 0
        for avro_file in data_avro_files:
            taosdumpInspectCmd = f"{binPath} -I \"{avro_file}\""
            output = subprocess.check_output(taosdumpInspectCmd, shell=True).decode("utf-8")
            recordsTimes += sum(1 for line in output.splitlines() if "=== Records:" in line)
        print("records found times: %d" % int(recordsTimes))

        if int(recordsTimes) != 2:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected records found times 2, actual %d"
                % (caller.filename, caller.lineno, int(recordsTimes))
            )

        print("do test inspect ....................... [passed]")

    #
    # ------------------- test_taosdump_db_ntb.py ----------------
    #
    def do_taosdump_db_ntb(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "create table db.nt1 (ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t1 -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop database db")
        #        sys.exit(1)

        os.system("%s -i %s -T 1 -W db=newdb" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "newdb":
                found = True
                break

        assert found == True

        tdSql.execute("use newdb")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(1)

        print("do normal table dump .................. [passed]")        

    #
    # ------------------- test_taosdump_db_stb.py ----------------
    #
    def do_taosdump_db_stb(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "create table db.nt1 (ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db st -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop database db")
        #        sys.exit(1)

        os.system("%s -i %s -T 1 -W db=newdb" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "newdb":
                found = True
                break

        assert found == True

        tdSql.execute("use newdb")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(2)

        print("do stable dump ........................ [passed]")

    #
    # ------------------- test_taosdump_escaped_db.py ----------------
    #
    def do_taosdump_escaped_db(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database `Db`")

        tdSql.execute("use `Db`")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT) tags(n1 INT)"
        )
        tdSql.execute(
            "create table t1 using st tags(1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1)"
        )
        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        print("%s Db st -e -o %s -T 1" % (binPath, self.tmpdir))
        os.system("%s Db st -e -o %s -T 1" % (binPath, self.tmpdir))
        # sys.exit(1)

        tdSql.execute("drop database `Db`")
        #        sys.exit(1)

        os.system("%s -e -i %s -T 1 -W Db=NewDb" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "NewDb":
                found = True
                break

        assert found == True

        tdSql.execute("use `NewDb`")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("select count(*) from `NewDb`.st")
        tdSql.checkData(0, 0, 1)

        print("do escape option ...................... [passed]")

    #
    # ------------------- test_taosdump_in_diff_type.py ----------------
    #
    def do_taosdump_in_diff_type(self):
        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table tb(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "insert into tb values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        #        sys.exit(1)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop table tb")
        tdSql.execute(
            "create table tb(ts timestamp, c1 FLOAT, c2 DOUBLE, c3 BOOL, c4 BINARY(10), c5 NCHAR(10), c6 INT, c7 BOOL, c8 BINARY(10), c9 BOOL, c10 FLOAT, c11 DOUBLE, c12 BOOL, c13 INT, c14 BIGINT)"
        )
        #        sys.exit(1)

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "db":
                found = True
                break

        assert found == True

        tdSql.execute("use db")

        tdSql.query("SELECT * from tb")
        for i in range(1, len(tdSql.queryResult[0])):
            tdSql.checkData(0, i, None)

        print("do diff data type ..................... [passed]")

    #
    # ------------------- test_taosdump_many_cols.py ----------------
    #
    def do_taosdump_many_cols(self):
        tdSql.prepare()
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        stb_sql = "create stable stb(ts timestamp"

        # "show create table" only return 64k sql, this over 64k sql need wait engine group fixed
        #colCnt = 4095 - 128  # todo
        maxCol = 300
        for index in range(maxCol):
            stb_sql += ", col%d INT" % (index + 1)
        stb_sql += ") tags(tag0 INT"
        for index in range(127):
            stb_sql += ", tag%d INT" % (index + 1)
        stb_sql += ")"

        tdSql.execute(stb_sql)
        #        sys.exit(1)

        tb_sql = "create table tb using stb tags(0"
        for index in range(127):
            tb_sql += ",%d" % (index + 1)
        tb_sql += ")"

        tdSql.execute(tb_sql)

        #        sys.exit(1)

        for record in range(100):
            ins_sql = "insert into tb values(%d" % (1640000000000 + record)
            for index in range(maxCol):
                ins_sql += ",%d" % index
            ins_sql += ")"
            tdSql.execute(ins_sql)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop database db")
        #        sys.exit(1)

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.queryResult

        found = False
        for i in range(len(dbresult)):
            print("Found db: %s" % dbresult[i][0])
            if dbresult[i][0] == "db":
                found = True
                break

        assert found == True

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")

        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "tb")

        tdSql.query("select count(*) from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        print("do many cols .......................... [passed]")



    #
    # ------------------- main ----------------
    #
    
        print("do many cols .......................... [passed]")

    #
    # ------------------- main ----------------
    #
    
        print("do many cols .......................... [passed]")


    #
    # ------------------- main ----------------
    #
    def test_taosdump_basic(self):
        """taosdump basic

        1. Create database and tables with various data types
        2. Insert data with special values
        3. Use taosdump to export the database
        4. Drop the original database
        5. Use taosdump to import the database back with different options
        6. Verify the imported database, tables, and data
        7. Boundary value testing for database and table names
        8. Check large data insertion and export/import
        9. Check taosdump with different configurations
        10. Check taosdump export/import with chinese string value
        11. Inspect avro files generated with -I argument
        12. Dump/restore a specific normal table in a database
        13. Dump/restore some super/child tables in a database
        14. Dump/restore database with escaped argument -e
        15. Dump/restore into different data types
        16. Dump/restore table with many columns(MAX 300 columns)
        
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_basic.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test2.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_loose_mode.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_test_inspect.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_db_ntb.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_db_stb.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_escaped_db.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_in_diff_type.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_many_cols.py
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/

        """
        self.do_taosdump_test_basic()
        self.do_taosdump_test()
        self.do_taosdump_test2()
        self.do_loose_mode()
        self.do_inspect()
        self.do_taosdump_db_ntb()
        self.do_taosdump_db_stb()
        self.do_taosdump_escaped_db()
        self.do_taosdump_in_diff_type()
        self.do_taosdump_many_cols()