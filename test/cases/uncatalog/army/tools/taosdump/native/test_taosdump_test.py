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
import os

class TestTaosdumpTest:
    def checkCommunity(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if "community" in selfPath:
            return False
        else:
            return True


    def test_taosdump_test(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:
            - xxx
            - xxx
        """
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
        os.system("rm -rf ./taosdumptest/tmp1")
        os.system("rm -rf ./taosdumptest/tmp2")
        os.system("rm -rf ./dump_result.txt")
        os.system("rm -rf ./db.csv")

        tdLog.success("%s successfully executed" % __file__)


