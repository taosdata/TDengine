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

class TestTaosdumpTestTypeBool:
    def caseDescription(self):
        """
        case1<sdsang>: [TD-12526] taosdump supports bool
        """

    def test_taosdump_test_type_bool(self):
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
        tdSql.execute("create database db keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute("create table db.st(ts timestamp, c1 BOOL) tags(btag BOOL)")
        tdSql.execute("create table db.t1 using  db.st tags(true)")
        tdSql.execute("insert into db.t1 values(1640000000000, true)")
        tdSql.execute("create table db.t2 using  db.st tags(false)")
        tdSql.execute("insert into db.t2 values(1640000000000, false)")
        tdSql.execute("create table db.t3 using  db.st tags(NULL)")
        tdSql.execute("insert into db.t3 values(1640000000000, NULL)")

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

        os.system("%s -R -D db -o %s" % (binPath, self.tmpdir))

        #        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%s -R -i %s" % (binPath, self.tmpdir))

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
        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(3)
        dbresult = tdSql.queryResult
        print(dbresult)
        for i in range(len(dbresult)):
            assert (
                (dbresult[i][0] == "t1")
                or (dbresult[i][0] == "t2")
                or (dbresult[i][0] == "t3")
            )

        tdSql.query("select btag from db.st")
        tdSql.checkRows(3)
        dbresult = tdSql.queryResult
        print(dbresult)

        tdSql.query("select * from  db.st where btag = true")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "True")
        tdSql.checkData(0, 2, "True")

        tdSql.query("select * from  db.st where btag = false")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "False")
        tdSql.checkData(0, 2, "False")

        tdSql.query("select * from  db.st where btag is null")
        dbresult = tdSql.queryResult
        print(dbresult)
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.success("%s successfully executed" % __file__)


