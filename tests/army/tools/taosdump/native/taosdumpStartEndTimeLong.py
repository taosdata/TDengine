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
        case1<sdsang>: [TS-2769] taosdump start-time end-time test
        """




    def run(self):
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
            "create table t1 (ts timestamp, n int)"
        )
        tdSql.execute(
            "create table t2 (ts timestamp, n int)"
        )
        tdSql.execute(
            "create table t3 (ts timestamp, n int)"
        )
        tdSql.execute(
            "insert into t1 values('2023-02-28 12:00:00.997',11)('2023-02-28 12:00:01.997',12)('2023-02-28 12:00:02.997',15)('2023-02-28 12:00:03.997',16)('2023-02-28 12:00:04.997',17)"
        )
        tdSql.execute(
            "insert into t2 values('2023-02-28 12:00:00.997',11)('2023-02-28 12:00:01.997',12)('2023-02-28 12:00:02.997',15)('2023-02-28 12:00:03.997',16)('2023-02-28 12:00:04.997',17)"
        )
        tdSql.execute(
            "insert into t3 values('2023-02-28 12:00:00.997',11)('2023-02-28 12:00:01.997',12)('2023-02-28 12:00:02.997',15)('2023-02-28 12:00:03.997',16)('2023-02-28 12:00:04.997',17)"
        )

        #        sys.exit(1)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t1 -o %s -T 1 --start-time=2023-02-28T12:00:01.997+0800 --end-time=2023-02-28T12:00:03.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t1")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 3)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t2 -o %s -T 1 --start-time=2023-02-28T12:00:01.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t2")
        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 4)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s db t3 -o %s -T 1 --end-time=2023-02-28T12:00:03.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t3")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 4)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
