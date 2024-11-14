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

from frame import etool
from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *

class TDTestCase(TBase):
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def insert_data(self):
        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute("create database test keep 36500")
        tdSql.execute("use test")
        tdSql.execute(
            f'''create table if not exists test.td32727
                (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10), c9 tinyint unsigned, c10 smallint unsigned, c11 int unsigned, c12 bigint unsigned)
                '''
        )

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"insert into test.td32727 values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar', 5, 5, 5, 5)")
        tdSql.execute(f"insert into test.td32727 values ('2020-02-01 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar', 10, 10, 10, 10)")
        tdSql.execute(f"insert into test.td32727 values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar', 15, 15, 15, 15)")


    def test_normal_query_new(self, testCase):
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_interp(self):
        self.test_normal_query_new("interp")

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.insert_data()

        # math function
        self.test_interp()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
