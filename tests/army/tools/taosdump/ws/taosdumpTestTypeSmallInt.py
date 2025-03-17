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

import os
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
        case1<sdsang>: [TD-12526] taosdump supports small int
        """

    def run(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        tdSql.execute("create table db.st(ts timestamp, c1 SMALLINT) tags(sntag SMALLINT)")
        tdSql.execute("create table db.t1 using db.st tags(1)")
        tdSql.execute("insert into db.t1 values(1640000000000, 1)")

        tdSql.execute("create table db.t2 using db.st tags(32767)")
        tdSql.execute("insert into db.t2 values(1640000000000, 32767)")

        tdSql.execute("create table db.t3 using db.st tags(-32767)")
        tdSql.execute("insert into db.t3 values(1640000000000, -32767)")

        tdSql.execute("create table db.t4 using db.st tags(NULL)")
        tdSql.execute("insert into db.t4 values(1640000000000, NULL)")

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

        os.system("%s -R -D db -o %s -T 1" % (binPath, self.tmpdir))

        #        sys.exit(1)
        tdSql.execute("drop database db")

        os.system("%s -R -i %s -T 1" % (binPath, self.tmpdir))

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
        tdSql.query("show db.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show db.tables")
        tdSql.checkRows(4)

        tdSql.query("select * from db.st where sntag = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from db.st where sntag = 32767")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, 32767)
        tdSql.checkData(0, 2, 32767)

        tdSql.query("select * from db.st where sntag = -32767")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, -32767)
        tdSql.checkData(0, 2, -32767)

        tdSql.query("select * from db.st where sntag is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1640000000000)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
