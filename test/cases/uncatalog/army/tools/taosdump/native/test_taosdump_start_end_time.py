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

class TestTaosdumpStartEndTime:
    def caseDescription(self):
        """
        case1<sdsang>: [TS-2769] taosdump start-time end-time test
        """




    def test_taosdump_start_end_time(self):
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

        os.system("%s db t1 -o %s -T 1 -S 2023-02-28T12:00:01.997+0800 -E 2023-02-28T12:00:03.997+0800 " % (binPath, self.tmpdir))

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

        os.system("%s db t2 -o %s -T 1 -S 2023-02-28T12:00:01.997+0800 " % (binPath, self.tmpdir))

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

        os.system("%s db t3 -o %s -T 1 -E 2023-02-28T12:00:03.997+0800 " % (binPath, self.tmpdir))

        tdSql.execute("drop table t3")

        os.system("%s -i %s -T 1" % (binPath, self.tmpdir))

        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 4)

        tdLog.success("%s successfully executed" % __file__)


