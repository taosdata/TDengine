###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestTaosdumpTestTypeJson:
    def caseDescription(self):
        """
        case1<sdsang>: [TD-12362] taosdump supports JSON
        """

    def test_taosdump_test_type_json(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:            - xxx
            - xxx
        """
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute("create table db.st(ts timestamp, c1 int) tags(jtag JSON)")
        tdSql.execute('create table db.t1 using db.st tags(\'{"location": "beijing"}\')')
        tdSql.execute("insert into db.t1 values(1500000000000, 1)")

        tdSql.execute("create table db.t2 using db.st tags(NULL)")
        tdSql.execute("insert into db.t2 values(1500000000000, NULL)")

        tdSql.execute("create table db.t3 using db.st tags('')")
        tdSql.execute("insert into db.t3 values(1500000000000, 0)")

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

        os.system("%s -R -D db -o %s" % (binPath, self.tmpdir))

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

        tdSql.query("select jtag->'location' from db.st")
        tdSql.checkRows(3)

        dbresult = tdSql.queryResult
        print(dbresult)
        found = False
        for i in range(len(dbresult)):
            if dbresult[i][0] == '"beijing"':
                found = True
                break

        assert found == True

        tdSql.query("select * from db.st where jtag contains 'location'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        val = '{"location":"beijing"}'
        tdSql.checkData(0, 2, val)

        tdSql.query("select jtag from db.st")
        tdSql.checkRows(3)

        dbresult = tdSql.queryResult
        print(dbresult)
        found = False
        for i in range(len(dbresult)):
            if dbresult[i][0] == val:
                found = True
                break

        assert found == True

        tdLog.success("%s successfully executed" % __file__)


