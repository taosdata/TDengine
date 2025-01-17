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
        tdLog.info(f"insert data.")
        datafile1 = etool.curFile(__file__, "data/d1001.data")
        datafile2 = etool.curFile(__file__, "data/d1002.data")
        datafile3 = etool.curFile(__file__, "data/d1003.data")
        datafile4 = etool.curFile(__file__, "data/d1004.data")

        tdSql.execute("create database ts_5452;")
        tdSql.execute("use ts_5452;")
        tdSql.execute("select database();")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT, "
            "`id` INT, `name` VARCHAR(64), `nch1` NCHAR(50), `nch2` NCHAR(50), `var1` VARCHAR(50), "
            "`var2` VARCHAR(50)) TAGS (`groupid` TINYINT, `location` VARCHAR(16));")
        tdSql.execute("CREATE table d1 using meters tags(1, 'beijing')")
        tdSql.execute("insert into d1 file '%s'" % datafile1)
        tdSql.execute("CREATE table d2 using meters tags(2, 'shanghai')")
        tdSql.execute("insert into d2 file '%s'" % datafile2)
        tdSql.execute("CREATE table d3 using meters tags(3, 'tianjin')")
        tdSql.execute("insert into d3 file '%s'" % datafile3)
        tdSql.execute("CREATE table d4 using meters tags(4, 'chongqing')")
        tdSql.execute("insert into d4 file '%s'" % datafile4)

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
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_syntax(self):
        self.test_normal_query_new("pi")

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

    def test_weekday(self):
        self.test_normal_query_new("weekday")

        tdSql.error("select weekday(hello) from ts_4893.meters limit 1;")

    def test_rand(self):
        self.test_normal_query_new("rand")

        tdSql.query("select rand();")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        res = tdSql.getData(0, 0)
        self.check_rand_data_range(res, 0)

        tdSql.error("select rand(3.14);")
        tdSql.error("select rand(-3.14);")
        tdSql.error("select rand('');")
        tdSql.error("select rand('hello');")

    def check_rand_data_range(self, data, row):
        if data < 0 or data >= 1:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, row+1, self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)

    def test_error(self):
        tdSql.error("select * from (select to_iso8601(ts, timezone()), timezone() from ts_4893.meters \
            order by ts desc) limit 1000;", expectErrInfo="Invalid parameter data type : to_iso8601") # TS-5340

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.insert_data()

        # syntax test
        self.test_syntax()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
