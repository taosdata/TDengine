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

import os
import math
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        case1<sdsang>: [TD-12526] taosdump supports double
        """




    def run(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 DOUBLE) tags(dbtag DOUBLE)")
        tdSql.execute("create table t1 using st tags(1.0)")
        tdSql.execute("insert into t1 values(1640000000000, 1.0)")

        tdSql.execute("create table t2 using st tags(1.7E308)")
        tdSql.execute("insert into t2 values(1640000000000, 1.7E308)")

        tdSql.execute("create table t3 using st tags(-1.7E308)")
        tdSql.execute("insert into t3 values(1640000000000, -1.7E308)")

        tdSql.execute("create table t4 using st tags(NULL)")
        tdSql.execute("insert into t4 values(1640000000000, NULL)")

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
        tdSql.execute("drop database db")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("show databases")
        dbresult = tdSql.res

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
        tdSql.checkRows(4)

        tdSql.query("select * from st where dbtag = 1.0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), 1.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 1.0))
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), 1.0):
            tdLog.debug("getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), 1.0))
            tdLog.exit("data is different")

        tdSql.query("select * from st where dbtag = 1.7E308")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), 1.7e308):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), 1.7e308)
            )
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), 1.7e308):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), 1.7e308)
            )
            tdLog.exit("data is different")

        tdSql.query("select * from st where dbtag = -1.7E308")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        if not math.isclose(tdSql.getData(0, 1), -1.7e308):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 1), -1.7e308)
            )
            tdLog.exit("data is different")
        if not math.isclose(tdSql.getData(0, 2), -1.7e308):
            tdLog.debug(
                "getData(0, 1): %f, to compare %f" % (tdSql.getData(0, 2), -1.7e308)
            )
            tdLog.exit("data is different")

        tdSql.query("select * from st where dbtag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
