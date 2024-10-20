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
    updatecfg_dict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def insert_data(self):
        tdLog.info(f"insert data.")
        datafile = etool.curFile(__file__, "data/d1001.data")

        tdSql.execute("create database ts_4893;")
        tdSql.execute("use ts_4893;")
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
                    sql_statement = sql_statement.rstrip(';')
                    tdSql.checkDataCsvByLine(sql_statement, ansFile)
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

    def test_normal_query_new(self, testCase):
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}_1.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_pi(self):
        self.test_normal_query_new("pi")

    def test_round(self):
        self.test_normal_query_new("round")

        tdSql.error("select round(name, 2) from ts_4893.meters limit 1;")

    def test_exp(self):
        self.test_normal_query_new("exp")

    def test_truncate(self):
        self.test_normal_query_new("trunc")

        tdSql.error("select truncate(0.999);")
        tdSql.error("select truncate(-1.999);")
        tdSql.error("select truncate(null);")
        tdSql.error("select truncate(name, 1) from ts_4893.meters limit 1;")

    def test_ln(self):
        self.test_normal_query_new("ln")

        tdSql.error("select ln(name) from ts_4893.meters limit 1;")

    def test_mod(self):
        self.test_normal_query_new("mod")

        tdSql.error("select mod(name, 2) from ts_4893.meters limit 1;")

    def test_sign(self):
        self.test_normal_query_new("sign")

        tdSql.error("select sign('');")
        tdSql.error("select sign('abc');")
        tdSql.error("select sign('123');")
        tdSql.error("select sign('-456');")

    def test_degrees(self):
        self.test_normal_query_new("degrees")

        tdSql.error("select degrees('');")
        tdSql.error("select degrees('abc');")
        tdSql.error("select degrees('1.57');")

    def test_radians(self):
        self.test_normal_query_new("radians")

        tdSql.error("select radians('');")
        tdSql.error("select radians('abc');")
        tdSql.error("select radians('45');")

    def test_char_length(self):
        self.test_normal_query_new("char_length")

        tdSql.error("select char_length(12345);")
        tdSql.error("select char_length(true);")
        tdSql.error("select char_length(repeat('a', 1000000));")
        tdSql.error("select char_length(id) from ts_4893.meters;")

    def test_char(self):
        self.test_normal_query_new("char")

        res = [[chr(0)], [chr(1)], [chr(2)], [chr(3)], [chr(4)], [chr(5)], [chr(6)], [chr(7)], [chr(8)], [chr(9)]]
        tdSql.checkDataMem("select char(id) from ts_4893.d0 limit 10;", res)
        tdSql.checkDataMem("select char(id) from ts_4893.meters limit 10;", res)

        res = [[chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)]]
        tdSql.checkDataMem("select char(nch1) from ts_4893.d0 limit 10;", res)
        tdSql.checkDataMem("select char(nch1) from ts_4893.meters limit 10;", res)
        tdSql.checkDataMem("select char(var1) from ts_4893.d0 limit 10;", res)
        tdSql.checkDataMem("select char(var1) from ts_4893.meters limit 10;", res)

    def test_ascii(self):
        self.test_normal_query_new("ascii")

        tdSql.error("select ascii(123);")

    def test_position(self):
        self.test_normal_query_new("position")

    def test_replace(self):
        self.test_normal_query_new("replace")

    def test_repeat(self):
        self.test_normal_query_new("repeat")

    def test_substr(self):
        self.test_normal_query_new("substr")

    def test_substr_idx(self):
        self.test_normal_query_new("substr_idx")

    def test_trim(self):
        self.test_normal_query_new("trim")

    def test_timediff(self):
        self.test_normal_query_new("timediff")

        tdSql.error("select timediff(min(ts), '2023-01-01 00:00:00') from ts_4893.meters limit 1;")
        tdSql.error("select timediff(max(ts), '2023-12-31 23:59:59') from ts_4893.meters limit 1;")
        tdSql.error("select (select timediff(ts, (select max(ts) from ts_4893.meters)) from ts_4893.meters where id = m.id) from ts_4893.meters m;")

    def test_week(self):
        self.test_normal_query_new("week")

    def test_weekday(self):
        self.test_normal_query_new("weekday")

        tdSql.error("select weekday(hello) from ts_4893.meters limit 1;")

    def test_weekofyear(self):
        self.test_normal_query_new("weekofyear")

    def test_dayofweek(self):
        self.test_normal_query_new("dayofweek")

    def test_stddev_pop(self):
        self.test_normal_query_new("stddev")

        tdSql.error("select stddev_pop(var1) from ts_4893.meters;")
        tdSql.error("select stddev_pop(current) from empty_ts_4893.meters;")
        tdSql.error("select stddev_pop(name) from ts_4893.meters;")
        tdSql.error("select stddev_pop(nonexistent_column) from ts_4893.meters;")

    def test_varpop(self):
        self.test_normal_query_new("varpop")

        tdSql.error("select var_pop(var1) from ts_4893.meters;")
        tdSql.error("select var_pop(current) from empty_ts_4893.meters;")
        tdSql.error("select var_pop(name) from ts_4893.meters;")
        tdSql.error("select var_pop(nonexistent_column) from ts_4893.meters;")

    def test_rand(self):
        self.test_normal_query_new("rand")

        tdSql.query("select rand();")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select rand(null);")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select rand() where rand() >= 0;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select rand() where rand() < 1;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select rand() where rand() >= 0 and rand() < 1;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select rand() from (select 1) t limit 1;")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select round(rand(), 3)")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select pow(rand(), 2)")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.query("select rand(12345), rand(12345);")
        tdSql.checkRows(1)
        tdSql.checkCols(2)
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        if res0 != res1:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, 1, self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)

        tdSql.error("select rand(3.14);")
        tdSql.error("select rand(-3.14);")
        tdSql.error("select rand('');")
        tdSql.error("select rand('hello');")

    def check_rand_data_range(self, data, row):
        if data < 0 or data >= 1:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, row+1, self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)

    def test_max(self):
        self.test_normal_query_new("max")

        tdSql.error("select max(nonexistent_column) from ts_4893.meters;")

    def test_min(self):
        self.test_normal_query_new("min")

        tdSql.error("select min(nonexistent_column) from ts_4893.meters;")

    def test_error(self):
        tdSql.error("select * from (select to_iso8601(ts, timezone()), timezone() from ts_4893.meters \
            order by ts desc) limit 1000;", expectErrInfo="Not supported timzone format") # TS-5340

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.insert_data()

        # math function
        self.test_pi()
        self.test_round()
        self.test_exp()
        self.test_truncate()
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
        self.test_stddev_pop()
        self.test_varpop()

        # select function
        self.test_max()
        self.test_min()

        # error function
        self.test_error()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
