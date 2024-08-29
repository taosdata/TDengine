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

import sys
import time
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    updatecfgDict = {
        "keepColumnName"   : "1",
        "ttlChangeOnWrite" : "1",
        "querySmaOptimize" : "1",
        "slowLogScope"     : "none",
        "queryBufferSize"  : 10240
    }


    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        datafile = etool.curFile(__file__, "data/d1001.data")

        tdSql.execute("create database ts_4893;")
        tdSql.execute(f"use ts_4893;")
        tdSql.execute("select database();")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT, "
                      "`id` INT, `name` VARCHAR(64), `nch1` NCHAR(50), `nch2` NCHAR(50), `var1` VARCHAR(50), "
                      "`var2` VARCHAR(50)) TAGS (`groupid` TINYINT, `location` VARCHAR(16));")
        tdSql.execute("CREATE table d0 using meters tags(1, 'beijing')")
        tdSql.execute("insert into d0 file '%s'" % datafile)

    def test_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test normal query.")
        sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")
        with open(sqlFile, 'r') as sql_file:
            sql_statement = ''
            tdSql.csvLine = 0
            for line in sql_file:
                if not line.strip() or line.strip().startswith('--'):
                    continue

                sql_statement += line.strip()

                if sql_statement.endswith(';'):
                    # 去掉末尾的分号
                    sql_statement = sql_statement.rstrip(';')
                    tdSql.checkDataCsvByLine(sql_statement, ansFile)
                    # 清空 sql_statement 以便处理下一条语句
                    sql_statement = ''

    def test_pi(self):
        self.test_normal_query("pi")

    def test_round(self):
        self.test_normal_query("round")
    def test_exp(self):
        self.test_normal_query("exp")

    def test_trunc(self):
        self.test_normal_query("trunc")
    def test_ln(self):
        self.test_normal_query("ln")
    def test_mod(self):
        self.test_normal_query("mod")
    def test_sign(self):
        self.test_normal_query("sign")
    def test_degrees(self):
        self.test_normal_query("degrees")
    def test_radians(self):
        self.test_normal_query("radians")
    def test_char_length(self):
        self.test_normal_query("char_length")
    def test_char(self):
        self.test_normal_query("char")
    def test_ascii(self):
        self.test_normal_query("ascii")
    def test_position(self):
        self.test_normal_query("position")
    def test_replace(self):
        self.test_normal_query("replace")
    def test_repeat(self):
        self.test_normal_query("repeat")
    def test_substr(self):
        self.test_normal_query("substr")
    def test_substr_idx(self):
        self.test_normal_query("substr_idx")
    def test_trim(self):
        self.test_normal_query("trim")
    def test_timediff(self):
        self.test_normal_query("timediff")
    def test_week(self):
        self.test_normal_query("week")
    def test_weekday(self):
        self.test_normal_query("weekday")
    def test_weekofyear(self):
        self.test_normal_query("weekofyear")
    def test_dayofweek(self):
        self.test_normal_query("dayofweek")
    def test_stddev(self):
        self.test_normal_query("stddev")
    def test_varpop(self):
        self.test_normal_query("varpop")
    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # math function
        self.test_pi()
        self.test_round()
        self.test_exp()
        self.test_trunc()
        self.test_ln()
        self.test_mod()
        self.test_sign()
        self.test_degrees()
        self.test_radians()

        # char function
        self.test_char_length()
        self.test_char()
        self.test_ascii()
        self.test_position()
        self.test_replace()
        self.test_repeat()
        self.test_substr()
        self.test_substr_idx()
        self.test_trim()

        # time function
        self.test_timediff()
        self.test_week()
        self.test_weekday()
        self.test_weekofyear()
        self.test_dayofweek()

        # agg function
        self.test_stddev()
        self.test_varpop()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
