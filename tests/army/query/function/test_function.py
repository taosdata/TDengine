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
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
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
        err_file_path = etool.curFile(__file__, f"in/{testCase}.err")
        if not os.path.isfile(err_file_path):
            return None
        with open(err_file_path, 'r') as err_file:
            err_statement = ''
            for line in err_file:
                if not line.strip() or line.strip().startswith('--'):
                    continue

                err_statement += line.strip()

                if err_statement.endswith(';'):
                    tdSql.error(err_statement)
                    err_statement = ''

    def test_pi(self):
        self.test_normal_query("pi")

    def test_round(self):
        self.test_normal_query("round")

        tdSql.query("select round(10, null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select round(null, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_exp(self):
        self.test_normal_query("exp")

        tdSql.query("select exp(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_trunc(self):
        self.test_normal_query("trunc")

        tdSql.query("select truncate(99.99, null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select truncate(null, 3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_ln(self):
        self.test_normal_query("ln")

        tdSql.query("select ln(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_mod(self):
        self.test_normal_query("mod")

        tdSql.query("select mod(null, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select mod(10, null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select mod(10, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_sign(self):
        self.test_normal_query("sign")

        tdSql.query("select sign(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_degrees(self):
        self.test_normal_query("degrees")

        tdSql.query("select degrees(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_radians(self):
        self.test_normal_query("radians")

        tdSql.query("select radians(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_char_length(self):
        self.test_normal_query("char_length")

        tdSql.query("select char_length(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_char(self):
        self.test_normal_query("char")

        tdSql.query("select char(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.query("select char('ustc');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, chr(0))

        result1 = [[chr(0)], [chr(1)], [chr(2)], [chr(3)], [chr(4)], [chr(5)], [chr(6)], [chr(7)], [chr(8)], [chr(9)]]
        tdSql.checkDataMem("select char(id) from ts_4893.d0 limit 10;", result1)
        tdSql.checkDataMem("select char(id) from ts_4893.meters limit 10;", result1)

        result2 = [[chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)]]
        tdSql.checkDataMem("select char(nch1) from ts_4893.d0 limit 10;", result2)
        tdSql.checkDataMem("select char(nch1) from ts_4893.meters limit 10;", result2)

        tdSql.checkDataMem("select char(var1) from ts_4893.d0 limit 10;", result2)
        tdSql.checkDataMem("select char(var1) from ts_4893.meters limit 10;", result2)

    def test_ascii(self):
        self.test_normal_query("ascii")

        tdSql.query("select ascii(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_position(self):
        self.test_normal_query("position")

        tdSql.query("select position('t' in null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select position(null in 'taos');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_replace(self):
        self.test_normal_query("replace")

        tdSql.query("select replace(null, 'aa', 'ee');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select replace('aabbccdd', null, 'ee');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_repeat(self):
        self.test_normal_query("repeat")

        tdSql.query("select repeat('taos', null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select repeat(null, 3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select repeat('taos', 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

    def test_substr(self):
        self.test_normal_query("substr")

        tdSql.query("select substring('tdengine', null, 3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select substring(null, 1, 3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select substring('tdengine', 1, null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select substring('tdengine', 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.query("select substring('tdengine', 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.query("select substring('tdengine', 1, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.query("select substring('tdengine', 1, -1);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

    def test_substr_idx(self):
        self.test_normal_query("substr_idx")

        tdSql.query("select substring_index(null, '.', 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select substring_index('www.taosdata.com', null, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select substring_index('www.taosdata.com', '.', null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select substring_index('www.taosdata.com', '.', 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

    def test_trim(self):
        self.test_normal_query("trim")

        tdSql.query("select trim(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_timediff(self):
        self.test_normal_query("timediff")

        tdSql.query("select timediff(null, '2022-01-01 08:00:01',1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('2022-01-01 08:00:00', null,1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('2022/01/31', '2022/01/01',1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('20220131', '20220101',1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('22/01/31', '22/01/01',1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('01/31/22', '01/01/22',1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('31-JAN-22', '01-JAN-22',1s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('22/01/31', '22/01/01');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select timediff('www', 'ttt');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_week(self):
        self.test_normal_query("week")

        tdSql.query("select week(null, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('abc');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('1721020591', 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('1721020666229', 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('01/01/2020', 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('20200101', 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('20/01/01', 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('11/01/31', 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select week('01-JAN-20', 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_weekday(self):
        self.test_normal_query("weekday")

        tdSql.query("select weekday(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('1721020591');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('1721020666229');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('abc');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('01/01/2020');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('20200101');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('20/01/01');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('11/01/32');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekday('01-JAN-20');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_weekofyear(self):
        self.test_normal_query("weekofyear")

        tdSql.query("select weekofyear(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('1721020591');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('1721020666229');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('abc');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('01/01/2020');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('20200101');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('20/01/01');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('11/01/31');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select weekofyear('01-JAN-20');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_dayofweek(self):
        self.test_normal_query("dayofweek")

        tdSql.query("select dayofweek(null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('1721020591');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('1721020666229');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('abc');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('01/01/2020');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('20200101');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('20/01/01');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('11/01/31');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select dayofweek('01-JAN-20');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_stddev(self):
        self.test_normal_query("stddev")

        tdSql.query("select stddev_pop(null) from ts_4893.d0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select stddev_pop(null) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_varpop(self):
        self.test_normal_query("varpop")

        tdSql.query("select var_pop(null) from ts_4893.d0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select var_pop(null) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
    def test_error(self):
        tdSql.error(
            "select * from (select to_iso8601(ts, timezone()), timezone() from meters order by ts desc) limit 1000;",
            expectErrInfo="Not supported timzone format")  # TS-5340

    def test_rand(self):
        self.test_normal_query("rand")

        tdSql.query("select rand();")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        self.check_result_in_range(0, 0)

        tdSql.query("select rand(null);")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        self.check_result_in_range(0, 0)

        tdSql.query("select rand() from (select 1) t limit 1;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        self.check_result_in_range(0, 0)

        tdSql.query("select rand(id) from ts_4893.d0 limit 100;")
        tdSql.checkRows(100)
        tdSql.checkCols(1)
        for i in range(len(tdSql.res)):
            self.check_result_in_range(i, 0)

        tdSql.query("select rand(id) from ts_4893.meters limit 100;")
        tdSql.checkRows(100)
        tdSql.checkCols(1)
        for i in range(len(tdSql.res)):
            self.check_result_in_range(i, 0)

        tdSql.query("select rand(123), rand(123);")
        tdSql.checkRows(1)
        tdSql.checkCols(2)
        if tdSql.res[0][0] != tdSql.res[0][1]:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, tdSql.sql, tdSql.res[0][0], tdSql.res[0][1])
            tdLog.exit("%s(%d) failed: sql:%s data1:%s ne data2:%s" % args)

    def check_result_in_range(self, row, col):
        res = tdSql.res[row][col]
        if res < 0 or res >= 1:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, tdSql.sql, row, col, res)
            tdLog.exit("%s(%d) failed: sql:%s row:%s col:%s data:%s lt 0 or ge 1" % args)

    def test_max(self):
        self.test_normal_query("max")

        tdSql.query("select max(null) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "None")

        tdSql.query("select max(id) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, 9999)

        tdSql.query("select max(name) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "x")

        tdSql.query("select max(current) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, 11.9989996)

        tdSql.query("select max(nch1) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "一二三四五六七八九十")

        tdSql.query("select max(var1) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "一二三四五六七八九十")

    def test_min(self):
        self.test_normal_query("min")

        tdSql.query("select min(null) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "None")

        tdSql.query("select min(id) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query("select min(name) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "haha")

        tdSql.query("select min(current) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, 8.0000000)

        tdSql.query("select min(nch1) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "abc一二三abc一二三abc")

        tdSql.query("select min(var1) from ts_4893.meters;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "abc一二三abc一二三abc")

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
        self.test_rand()

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

        # select function
        self.test_max()
        self.test_min()

        # error function
        self.test_error()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
