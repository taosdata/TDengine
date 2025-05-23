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
        case1<sdsang>: [TS-1762] taosdump with many columns
        """




    def run(self):
        tdSql.prepare()

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db  keep 3649 ")

        tdSql.execute("use db")
        stb_sql = "create stable stb(ts timestamp"

        # "show create table" only return 64k sql, this over 64k sql need wait engine group fixed
        #colCnt = 4095 - 128  # todo
        maxCol = 300
        for index in range(maxCol):
            stb_sql += ", col%d INT" % (index + 1)
        stb_sql += ") tags(tag0 INT"
        for index in range(127):
            stb_sql += ", tag%d INT" % (index + 1)
        stb_sql += ")"

        tdSql.execute(stb_sql)
        #        sys.exit(1)

        tb_sql = "create table tb using stb tags(0"
        for index in range(127):
            tb_sql += ",%d" % (index + 1)
        tb_sql += ")"

        tdSql.execute(tb_sql)

        #        sys.exit(1)

        for record in range(100):
            ins_sql = "insert into tb values(%d" % (1640000000000 + record)
            for index in range(maxCol):
                ins_sql += ",%d" % index
            ins_sql += ")"
            tdSql.execute(ins_sql)

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

        os.system("%s db -o %s -T 1" % (binPath, self.tmpdir))

        tdSql.execute("drop database db")
        #        sys.exit(1)

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
        tdSql.checkData(0, 0, "stb")

        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "tb")

        tdSql.query("select count(*) from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
