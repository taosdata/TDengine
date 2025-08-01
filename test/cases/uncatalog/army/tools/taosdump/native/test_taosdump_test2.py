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
import string
import random

class TestTaosdumpTest2:
    def generateString(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        v = ""
        for i in range(length):
            v += random.choice(chars)
        return v

    def test_taosdump_test2(self):
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
        self.ts = 1601481600000
        self.numberOfTables = 1
        self.numberOfRecords = 150

        if not os.path.exists("./taosdumptest/tmp"):
            os.makedirs("./taosdumptest/tmp")
        else:
            print("directory exists")
            os.system("rm -rf ./taosdumptest/tmp/*")

        tdSql.prepare()

        tdSql.execute(
            "create table st(ts timestamp, c1 timestamp, c2 int, c3 bigint, c4 float, c5 double, c6 binary(8), c7 smallint, c8 tinyint, c9 bool, c10 nchar(8)) tags(t1 int)"
        )
        tdSql.execute("create table t1 using st tags(0)")
        currts = self.ts
        finish = 0
        while finish < self.numberOfRecords:
            sql = "insert into t1 values"
            for i in range(finish, self.numberOfRecords):
                sql += (
                    "(%d, 1019774612, 29931, 1442173978, 165092.468750, 1128.643179, 'MOCq1pTu', 18405, 82, 0, 'g0A6S0Fu')"
                    % (currts + i)
                )
                finish = i + 1
                if (1048576 - len(sql)) < 65519:
                    break
            tdSql.execute(sql)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        os.system("rm ./taosdumptest/tmp/*.sql")
        os.system("rm ./taosdumptest/tmp/*.avro*")
        os.system("rm -rf ./taosdumptest/taosdump.*")
        os.system("%s --databases db -o ./taosdumptest/tmp " % binPath)

        tdSql.execute("drop database db")
        tdSql.query("show databases")
        tdSql.checkRows(2)

        os.system("%s -i ./taosdumptest/tmp " % binPath)

        tdSql.query("show databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "db")

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, self.numberOfRecords)

        # test case for TS-1225
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute(
            "create table stb(ts timestamp, c1 binary(16374), c2 binary(16374), c3 binary(16374)) tags(t1 nchar(256))"
        )
        tdSql.execute(
            "insert into t1 using stb tags('t1') values(now, '%s', '%s', '%s')"
            % (
                self.generateString(16374),
                self.generateString(16374),
                self.generateString(16374),
            )
        )

        os.system("rm ./taosdumptest/tmp/*.sql")
        os.system("rm ./taosdumptest/tmp/*.avro*")
        os.system("rm -rf ./taosdumptest/tmp/taosdump.*")
        os.system("%s -D test -o ./taosdumptest/tmp " % binPath)

        tdSql.execute("drop database test")
        tdSql.query("show databases")
        tdSql.checkRows(3)

        os.system("%s -i ./taosdumptest/tmp " % binPath)

        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")

        tdSql.query("select * from stb")
        tdSql.checkRows(1)
        os.system("rm -rf dump_result.txt")

        tdLog.success("%s successfully executed" % __file__)


