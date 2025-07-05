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
import math

class TestTaosdumpTestTypeFloat:
    def caseDescription(self):
        """
        case1<sdsang>: [TD-12526] taosdump supports float
        """

    def test_taosdump_test_type_float(self):
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
        tdSql.execute("create table db.st(ts timestamp, c1 FLOAT) tags(ftag FLOAT)")
        tdSql.execute("create table db.t1 using db.st tags(1.0)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1.0)")

        tdSql.execute("create table db.t2 using db.st tags(3.40E+38)")
        tdSql.execute("insert into db.t2 values(1640000000000, 3.40E+38)")

        tdSql.execute("create table db.t3 using db.st tags(-3.40E+38)")
        tdSql.execute("insert into db.t3 values(1640000000000, -3.40E+38)")

        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

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

        os.system("%s -R -D db -o %s -T 1" % (binPath, self.tmpdir))

        #        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%s -R -i %s -T 1" % (binPath, self.tmpdir))

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
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where ftag = 1.0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), 1.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 1.0))
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), 1.0):
            tdLog.exit("data is different")

        tdSql.query("select * from db.st where ftag > 3.399999E38 and ftag < 3.4000001E38")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), 3.4e38, rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 3.4e38)
            )
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), 3.4e38, rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), 3.4e38)
            )
            tdLog.exit("data is different")

        tdSql.query("select * from db.st where ftag < -3.399999E38 and ftag > -3.4000001E38")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), (-3.4e38), rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), -3.4e38)
            )
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), (-3.4e38), rel_tol=1e-07, abs_tol=0.0):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), -3.4e38)
            )
            tdLog.exit("data is different")

        tdSql.query("select * from db.st where ftag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        tdLog.success("%s successfully executed" % __file__)


