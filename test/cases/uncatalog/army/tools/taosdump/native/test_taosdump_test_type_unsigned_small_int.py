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

class TestTaosdumpTestTypeUnsignedSmallInt:
    def caseDescription(self):
        """
        case1<sdsang>: [TD-12526] taosdump supports unsigned small int
        """




    def test_taosdump_test_type_unsigned_small_int(self):
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
            - xxx
        """
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 SMALLINT UNSIGNED) tags(usntag SMALLINT UNSIGNED)"
        )
        tdSql.execute("create table t1 using st tags(0)")
        tdSql.execute("insert into t1 values(1640000000000, 0)")
        tdSql.execute("create table t2 using st tags(65534)")
        tdSql.execute("insert into t2 values(1640000000000, 65534)")
        tdSql.execute("create table t3 using st tags(NULL)")
        tdSql.execute("insert into t3 values(1640000000000, NULL)")

        #        sys.exit(1)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found: %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s --databases db -o %s -T 1 -g" % (binPath, self.tmpdir))

        #        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%s -i %s -T 1 -g" % (binPath, self.tmpdir))

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

        tdSql.query("select * from st where usntag = 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 0)

        tdSql.query("select * from st where usntag = 65534")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, 65534)
        tdSql.checkData(0, 2, 65534)

        tdSql.query("select * from st where usntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.success("%s successfully executed" % __file__)


