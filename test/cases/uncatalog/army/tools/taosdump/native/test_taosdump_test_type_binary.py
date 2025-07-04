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

class TestTaosdumpTestTypeBinary:
    def caseDescription(self):
        """
        case1<sdsang>: [TD-12526] taosdump supports binary
        """




    def test_taosdump_test_type_binary(self):
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
            "create table st(ts timestamp, c1 BINARY(5), c2 BINARY(5)) tags(btag BINARY(5))"
        )
        tdSql.execute("create table t1 using st tags('test')")
        tdSql.execute("insert into t1 values(1640000000000, '01234', '56789')")
        tdSql.execute("insert into t1 values(1640000000001, 'abcd', 'efgh')")
        tdSql.execute("create table t2 using st tags(NULL)")
        tdSql.execute("insert into t2 values(1640000000000, NULL, NULL)")

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

        os.system("%s --databases db -o %s" % (binPath, self.tmpdir))

        #        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%s -i %s" % (binPath, self.tmpdir))

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
        tdSql.checkRows(2)
        dbresult = tdSql.queryResult
        print(dbresult)
        for i in range(len(dbresult)):
            assert dbresult[i][0] in ("t1", "t2")

        tdSql.query("select distinct(btag) from st where tbname = 't1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "test")

        tdSql.query("select distinct(btag) from st where tbname = 't2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select * from st where btag = 'test'")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "01234")
        tdSql.checkData(0, 2, "56789")
        tdSql.checkData(1, 1, "abcd")
        tdSql.checkData(1, 2, "efgh")

        tdSql.query("select * from st where btag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.success("%s successfully executed" % __file__)


