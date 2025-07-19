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

class TestTaosdumpManyCols:
    def caseDescription(self):
        """
        case1<sdsang>: [TS-1762] taosdump with many columns
        """




    def test_taosdump_many_cols(self):
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
        """
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
        tdSql.checkData(0, 0, "stb")

        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "tb")

        tdSql.query("select count(*) from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        tdLog.success("%s successfully executed" % __file__)


