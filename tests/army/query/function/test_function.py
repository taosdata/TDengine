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

from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *

from frame import etool
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
        tdLog.info(f"test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}_1.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_pi(self):
        self.test_normal_query_new('pi')

        tdSql.query('select pi();')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 3.1415927

        tdSql.query('select pi() / 2;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.5707963

        tdSql.query('select pi() * 0.5;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.5707963

        tdSql.query('select round(pi(), 6);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 3.1415930

        tdSql.query('select pi() * 0;')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select pi() * voltage from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 694.2919764

        tdSql.query('select 2 * pi() * phase from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 3.1975006

        tdSql.query('select current, sqrt(current / pi()) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert round(res0, 7) == 10.6499996
        assert round(res1, 7) == 1.8411953

        tdSql.query('select id, case when voltage > 100 then pi() else pi() / 2 end from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 0
        assert round(res1, 7) == 3.1415927

        tdSql.query('select pi() + null;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select pi() * name from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select pi() * -1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == -3.1415927

        tdSql.query('select voltage / pi() from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 70.3464848

        tdSql.query('select pi() / 0;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select round(pi() * phase, 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1.6

        tdSql.query('select abs(pi() * phase) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.5987503

        tdSql.query('select sqrt(pi() * voltage) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 26.3494208

        tdSql.query('select log(pi() * voltage) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 6.5428926

    def test_round(self):
        self.test_normal_query('round')

        tdSql.query('select round(10, null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select round(null, 2);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select round(100);')
        res = tdSql.getData(0, 0)
        assert res == 100

        tdSql.query('select round(123.456, 0);')
        res = tdSql.getData(0, 0)
        assert res == 123

        tdSql.query('select round(-123.456, 2);')
        res = tdSql.getData(0, 0)
        assert res == -123.46

        tdSql.query('select round(12345.6789, -2);')
        res = tdSql.getData(0, 0)
        assert res == 12300

        tdSql.query('select round(0.00123, -2);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select round(voltage, 0) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 221

        tdSql.query('select round(current, 1) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 10.6999998

        tdSql.query('select round(phase, 3) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 3) == 0.509

        tdSql.query('select round(voltage, -1) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 220

        tdSql.query('select round(current * voltage, 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 2353.65

        tdSql.query('select round(123.456, -5);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select round(-1234.5678, 2);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == -1234.5700000

        tdSql.query('select round(123.456, null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select round(abs(voltage), 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 221

        tdSql.query('select round(pi() * phase, 3) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1.599

        tdSql.query('select round(sqrt(voltage), 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 14.8700000

        tdSql.query('select round(log(current), 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 2.37

        tdSql.error('select round(name, 2) from ts_4893.meters limit 1;')

    def test_exp(self):
        self.test_normal_query('exp')

        tdSql.query('select exp(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select exp(0);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select exp(1);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.7182818

        tdSql.query('select exp(-1);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.3678794

        tdSql.query('select exp(100);')
        res = tdSql.getData(0, 0)
        assert res == 2.6881171418161356e+43

        tdSql.query('select exp(0.0001);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.0001000

        tdSql.query('select exp(voltage) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 9.529727902367202e+95

        tdSql.query('select exp(current) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 42192.5784536

        tdSql.query('select exp(phase) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.6634571

        tdSql.query('select exp(voltage + current) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 4.0208379216243076e+100

        tdSql.query('select exp(2);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 7.3890561

        tdSql.query('select exp(-1000);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select exp(100000);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select exp(-9999999999);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select round(exp(voltage), 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 9.529727902367202e+95

        tdSql.query('select exp(abs(current)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 42192.5784536

        tdSql.query('select exp(log(voltage)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 221.0000000

        tdSql.query('select exp(pi());')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 23.1406926

    def test_truncate(self):
        self.test_normal_query('trunc')

        tdSql.query('select truncate(99.99, null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select truncate(null, 3);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select truncate(1.0001, 3);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select truncate(100.9876, 2);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 100.98

        tdSql.query('select truncate(2.71828, 4);')
        res = tdSql.getData(0, 0)
        assert res == 2.7182

        tdSql.query('select truncate(voltage, 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 221

        tdSql.query('select truncate(current, 1) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 1) == 10.6

        tdSql.query('select truncate(phase, 3) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 3) == 0.508

        tdSql.query('select truncate(voltage + current, 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 231.6400000

        tdSql.query('select truncate(3.14159, 2);')
        res = tdSql.getData(0, 0)
        assert res == 3.14

        tdSql.query('select truncate(-5.678, 2);')
        res = tdSql.getData(0, 0)
        assert res == -5.67

        tdSql.query('select truncate(99999999999999.9999, 2);')
        res = tdSql.getData(0, 0)
        assert res == 1.000000000000000e+14

        tdSql.query('select truncate(voltage, -1) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 220

        tdSql.query('select round(truncate(voltage, 1), 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 221

        tdSql.query('select truncate(abs(current), 1) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 10.600000381469727

        tdSql.query('select truncate(exp(phase), 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1.66

        tdSql.query('select truncate(log(current), 1) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 2.3

        tdSql.error('select truncate(0.999);')
        tdSql.error('select truncate(-1.999);')
        tdSql.error('select truncate(null);')
        tdSql.error('select truncate(name, 1) from ts_4893.meters limit 1;')

    def test_ln(self):
        self.test_normal_query('ln')

        tdSql.query('select ln(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select ln(1);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select ln(exp(1));')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select ln(0.1);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == -2.3025851

        tdSql.query('select ln(2.718);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.9998963

        tdSql.query('select ln(10);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.3025851

        tdSql.query('select ln(voltage) from ts_4893.meters where voltage > 0 limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 5.3981627

        tdSql.query('select ln(current) from ts_4893.meters where current > 0 limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.3655599

        tdSql.query('select ln(phase) from ts_4893.meters where phase > 0 limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == -0.6755076

        tdSql.query('select ln(20);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.9957323

        tdSql.query('select ln(100);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 4.6051702

        tdSql.query('select ln(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select ln(-5);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select ln(0);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select ln(99999999999999);')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 32.2361913

        tdSql.query('select ln(exp(voltage)) from ts_4893.meters where voltage > 0 limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 221

        tdSql.query('select ln(abs(current)) from ts_4893.meters where current != 0 limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.3655599

        tdSql.query('select ln(sqrt(phase)) from ts_4893.meters where phase >= 0 limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == -0.3377538

        tdSql.query('select ln(log(current)) from ts_4893.meters where current > 1 limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.8610147

        tdSql.error('select ln(name) from ts_4893.meters limit 1;')

    def test_mod(self):
        self.test_normal_query('mod')

        tdSql.query('select mod(null, 2);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select mod(10, null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select mod(10, 0);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select mod(1, 1);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select mod(0, 1);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select mod(5, 2);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select mod(-5, 3);')
        res = tdSql.getData(0, 0)
        assert res == -2

        tdSql.query('select mod(5, -3);')
        res = tdSql.getData(0, 0)
        assert res == 2

        tdSql.query('select mod(voltage, 2) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select mod(current, 10) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.6499996

        tdSql.query('select mod(phase, 4) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.5088980

        tdSql.query('select mod(10, 3);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select mod(15, 4);')
        res = tdSql.getData(0, 0)
        assert res == 3

        tdSql.query('select mod(5, 0);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select mod(10, -3);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select mod(-10, 0);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select mod(abs(voltage), 3) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 2

        tdSql.query('select mod(phase, sqrt(16)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.5088980

        tdSql.query('select mod(round(voltage), 5) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select mod(current, log(100)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.4396592

        tdSql.error('select mod(name, 2) from ts_4893.meters limit 1;')

    def test_sign(self):
        self.test_normal_query('sign')

        tdSql.query('select sign(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select sign(0);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select sign(1);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(-1);')
        res = tdSql.getData(0, 0)
        assert res == -1

        tdSql.query('select sign(0.1);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(-0.1);')
        res = tdSql.getData(0, 0)
        assert res == -1

        tdSql.query('select sign(current) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(voltage) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(phase) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(25);')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(-10);')
        res = tdSql.getData(0, 0)
        assert res == -1

        tdSql.query('select sign(abs(voltage)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(round(current)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(sqrt(voltage)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select sign(log(current + 1)) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.error("select sign('');")
        tdSql.error("select sign('abc');")
        tdSql.error("select sign('123');")
        tdSql.error("select sign('-456');")

    def test_degrees(self):
        self.test_normal_query('degrees')

        tdSql.query("select degrees(null);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select degrees(0);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select degrees(pi()/2);')
        res = tdSql.getData(0, 0)
        assert res == 90

        tdSql.query('select degrees(pi());')
        res = tdSql.getData(0, 0)
        assert res == 180

        tdSql.query('select degrees(-pi()/2);')
        res = tdSql.getData(0, 0)
        assert res == -90

        tdSql.query('select degrees(2*pi());')
        res = tdSql.getData(0, 0)
        assert res == 360

        tdSql.query('select degrees(current) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 610.20

        tdSql.query('select degrees(voltage) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 12662.37

        tdSql.query('select degrees(1);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 57.30

        tdSql.query('select degrees(phase) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 29.16

        tdSql.query('select degrees(3.14);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 179.91

        tdSql.query('select degrees(-5);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == -286.48

        tdSql.query('select degrees(1000000);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 57295779.51

        tdSql.query('select degrees(sin(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 48.21

        tdSql.query('select degrees(cos(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 30.96

        tdSql.query('select degrees(tan(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 89.23

        tdSql.query('select degrees(radians(90));')
        res = tdSql.getData(0, 0)
        assert res == 90

        tdSql.query('select degrees(atan(1));')
        res = tdSql.getData(0, 0)
        assert res == 45

        tdSql.error("select degrees('');")
        tdSql.error("select degrees('abc');")
        tdSql.error("select degrees('1.57');")

    def test_radians(self):
        self.test_normal_query('radians')

        tdSql.query('select radians(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select radians(0);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select radians(90);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 1.57

        tdSql.query('select radians(180);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 3.14

        tdSql.query('select radians(-90);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == -1.57

        tdSql.query('select radians(360);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 6.28

        tdSql.query('select radians(current) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.19

        tdSql.query('select radians(voltage) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 3.86

        tdSql.query('select radians(phase) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.01

        tdSql.query('select radians(45);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.79

        tdSql.query('select radians(180);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 3.14

        tdSql.query('select radians(-45);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == -0.79

        tdSql.query('select radians(1000000);')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 17453.29

        tdSql.query('select radians(sin(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.01

        tdSql.query('select radians(cos(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.01

        tdSql.query('select radians(tan(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.03

        tdSql.query('select radians(degrees(90));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 90.00

        tdSql.query('select radians(atan(1));')
        res = tdSql.getData(0, 0)
        assert round(res, 2) == 0.01

        tdSql.error("select radians('');")
        tdSql.error("select radians('abc');")
        tdSql.error("select radians('45');")

    def test_char_length(self):
        self.test_normal_query('char_length')

        tdSql.query('select char_length(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select min(char_length(name)) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select max(char_length(name)) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 4

        tdSql.query("select char_length('');")
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select char_length('あいうえお');")
        res = tdSql.getData(0, 0)
        assert res == 5

        tdSql.query('select name, char_length(name) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 4

        tdSql.query('select nch1, char_length(nch1) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 5

        tdSql.query('select groupid, max(char_length(name)) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 4

        tdSql.query('select location, avg(char_length(name)) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert res1 == 3.2446

        tdSql.query('select upper(name), char_length(upper(name)) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'LILI'
        assert res1 == 4

        tdSql.query("select concat(name, ' - ', location), char_length(concat(name, ' - ', location)) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili - beijing'
        assert res1 == 14

        tdSql.query('select substring(name, 1, 5), char_length(substring(name, 1, 5)) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 4

        tdSql.query('select trim(name), char_length(trim(name)) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 4

        tdSql.error('select char_length(12345);')
        tdSql.error('select char_length(true);')
        tdSql.error("select char_length(repeat('a', 1000000));")
        tdSql.error('select char_length(id) from ts_4893.meters;')

    def test_char(self):
        self.test_normal_query('char')

        tdSql.query('select char(null);')
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select char('ustc');")
        res = tdSql.getData(0, 0)
        assert res == chr(0)

        res = [[chr(0)], [chr(1)], [chr(2)], [chr(3)], [chr(4)], [chr(5)], [chr(6)], [chr(7)], [chr(8)], [chr(9)]]
        tdSql.checkDataMem("select char(id) from ts_4893.meters limit 10;", res)

        res = [[chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)]]
        tdSql.checkDataMem("select char(nch1) from ts_4893.meters limit 10;", res)
        tdSql.checkDataMem("select char(var1) from ts_4893.meters limit 10;", res)

        tdSql.query('select char(65.99);')
        res = tdSql.getData(0, 0)
        assert res == 'A'

        tdSql.query('select char(65, 66, 67);')
        res = tdSql.getData(0, 0)
        assert res == 'ABC'

        tdSql.query('select char(72, 101, 108, 108, 111);')
        res = tdSql.getData(0, 0)
        assert res == 'Hello'

    def test_ascii(self):
        self.test_normal_query('ascii')

        tdSql.query('select ascii(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        # tdSql.query("select ascii('');")
        # res = tdSql.getData(0, 0)
        # assert res == 175

        tdSql.query("select ascii('0');")
        res = tdSql.getData(0, 0)
        assert res == 48

        tdSql.query("select ascii('~');")
        res = tdSql.getData(0, 0)
        assert res == 126

        tdSql.query("select ascii('Hello');")
        res = tdSql.getData(0, 0)
        assert res == 72

        tdSql.query('select name, ascii(name) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 108

        tdSql.query('select nch1, ascii(nch1) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 110

        tdSql.query('select var1, ascii(var1) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 110

        tdSql.query("select ascii('123abc');")
        res = tdSql.getData(0, 0)
        assert res == 49

        tdSql.query("select ascii('中');")
        res = tdSql.getData(0, 0)
        assert res == 228

        tdSql.query("select ascii('é');")
        res = tdSql.getData(0, 0)
        assert res == 195

        tdSql.query("select ascii(' ');")
        res = tdSql.getData(0, 0)
        assert res == 32

        tdSql.query("select ascii('!@#')")
        res = tdSql.getData(0, 0)
        assert res == 33

        tdSql.query("select ascii(concat('A', 'B'));")
        res = tdSql.getData(0, 0)
        assert res == 65

        tdSql.query('select name, ascii(substring(name, 1, 1)) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 108

        tdSql.query('select ascii(char(65));')
        res = tdSql.getData(0, 0)
        assert res == 65

        tdSql.query("select ascii(upper('b'));")
        res = tdSql.getData(0, 0)
        assert res == 66

        tdSql.query("select ascii(trim(' A '));")
        res = tdSql.getData(0, 0)
        assert res == 65

        tdSql.error('select ascii(123);')

    def test_position(self):
        self.test_normal_query('position')

        tdSql.query("select position('t' in null);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select position(null in 'taos');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select position('A' in 'A');")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query("select position('A' in '');")
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select position('' in 'A');")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query("select position('A' in null);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select position('Z' in 'ABC');")
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select position('l' in 'Hello');")
        res = tdSql.getData(0, 0)
        assert res == 3

        tdSql.query("select name, position('e' in name) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 0

        tdSql.query("select nch1, position('n' in nch1) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 1

        tdSql.query("select var1, position('1' in var1) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 0

        tdSql.query("select position('s' in 'meters');")
        res = tdSql.getData(0, 0)
        assert res == 6

        tdSql.query("select position('中' in '中国');")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query("select position('e' in 'é');")
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select position('W' in 'Hello World');")
        res = tdSql.getData(0, 0)
        assert res == 7

        tdSql.query("select position('@' in '!@#');")
        res = tdSql.getData(0, 0)
        assert res == 2

        tdSql.query("select position('6' in '12345');")
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select position('B' in concat('A', 'B'));")
        res = tdSql.getData(0, 0)
        assert res == 2

        tdSql.query("select name, position('a' in substring(name, 2, 5)) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 0

        tdSql.query("select position('A' in upper('abc'));")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query("select position('A' in trim(' A '));")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query("select position('x' in replace('Hello', 'l', 'x'));")
        res = tdSql.getData(0, 0)
        assert res == 3

    def test_replace(self):
        self.test_normal_query('replace')

        tdSql.query("select replace(null, 'aa', 'ee');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select replace('aabbccdd', null, 'ee');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select replace('A', 'A', '');")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select replace('', '', 'B');")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select replace('', 'A', 'B');")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select replace(null, 'A', 'B');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select replace('Hello', 'Z', 'X');")
        res = tdSql.getData(0, 0)
        assert res == 'Hello'

        tdSql.query("select replace('Hello World', 'World', 'MySQL');")
        res = tdSql.getData(0, 0)
        assert res == 'Hello MySQL'

        tdSql.query("select name, replace(name, 'a', 'o') from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lili'

        tdSql.query("select nch1, replace(nch1, 'n', 'm') from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'movel'

        tdSql.query("select var1, replace(var1, '1', 'one') from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novel'

        tdSql.query("select replace('12345', '5', 'five');")
        res = tdSql.getData(0, 0)
        assert res == '1234five'

        tdSql.query("select replace('中国', '中', '国');")
        res = tdSql.getData(0, 0)
        assert res == '国国'

        tdSql.query("select replace('é', 'e', 'a');")
        res = tdSql.getData(0, 0)
        assert res == 'é'

        tdSql.query("select replace('!@#', '@', '#');")
        res = tdSql.getData(0, 0)
        assert res == '!##'

        tdSql.query("select replace('Hello World', ' ', '_');")
        res = tdSql.getData(0, 0)
        assert res == 'Hello_World'

        tdSql.query("select replace('123456', '7', 'eight');")
        res = tdSql.getData(0, 0)
        assert res == '123456'

        tdSql.query("select replace(concat('A', 'B', 'C'), 'B', 'Z');")
        res = tdSql.getData(0, 0)
        assert res == 'AZC'

        tdSql.query("select name, replace(substring(name, 1, 5), 'e', 'o') from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lili'

        tdSql.query("select replace(upper('abc'), 'A', 'X');")
        res = tdSql.getData(0, 0)
        assert res == 'XBC'

        tdSql.query("select replace(trim('  Hello  '), 'l', 'L');")
        res = tdSql.getData(0, 0)
        assert res == 'HeLLo'

        tdSql.query("select replace(lower('HELLO'), 'h', 'H');")
        res = tdSql.getData(0, 0)
        assert res == 'Hello'

    def test_repeat(self):
        self.test_normal_query('repeat')

        tdSql.query("select repeat('taos', null);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select repeat(null, 3);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select repeat('taos', 0);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select repeat('A', 0);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select repeat('', 5);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select repeat('ABC', 1);")
        res = tdSql.getData(0, 0)
        assert res == 'ABC'

        tdSql.query('select repeat(null, 3);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select repeat('A', 10);")
        res = tdSql.getData(0, 0)
        assert res == 'AAAAAAAAAA'

        tdSql.query("select repeat('Hello', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'HelloHello'

        tdSql.query("select name, repeat(name, 3) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lililililili'

        tdSql.query("select nch1, repeat(nch1, 4) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novelnovelnovelnovel'

        tdSql.query("select repeat('123', 5);")
        res = tdSql.getData(0, 0)
        assert res == '123123123123123'

        tdSql.query("select var1, repeat(var1, 2) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novelnovel'

        tdSql.query("select repeat('!@#', 3);")
        res = tdSql.getData(0, 0)
        assert res == '!@#!@#!@#'

        tdSql.query("select repeat('你好', 2);")
        res = tdSql.getData(0, 0)
        assert res == '你好你好'

        tdSql.query("select repeat('12345', 3);")
        res = tdSql.getData(0, 0)
        assert res == '123451234512345'

        tdSql.query("select repeat('HelloWorld', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'HelloWorldHelloWorld'

        tdSql.query("select repeat('A B', 5);")
        res = tdSql.getData(0, 0)
        assert res == 'A BA BA BA BA B'

        tdSql.query("select repeat(concat('A', 'B', 'C'), 3);")
        res = tdSql.getData(0, 0)
        assert res == 'ABCABCABC'

        tdSql.query("select name, repeat(substring(name, 1, 5), 2) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lililili'

        tdSql.query("select repeat(upper('abc'), 4);")
        res = tdSql.getData(0, 0)
        assert res == 'ABCABCABCABC'

        tdSql.query("select repeat(trim('  Hello  '), 3);")
        res = tdSql.getData(0, 0)
        assert res == 'HelloHelloHello'

        tdSql.query("select repeat('abc', length('abc'));")
        res = tdSql.getData(0, 0)
        assert res == 'abcabcabc'

    def test_substr(self):
        self.test_normal_query('substr')

        tdSql.query("select substring('tdengine', null, 3);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring(null, 1, 3);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring('tdengine', 1, null);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring('tdengine', 0);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substring('tdengine', 10);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substring('tdengine', 1, 0);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substring('tdengine', 1, -1);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substr('Hello', 1, 3);")
        res = tdSql.getData(0, 0)
        assert res == 'Hel'

        tdSql.query("select substr('', 1, 5);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substr('ABCDE', 0, 3);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substr('ABCDEFG', -3, 2);")
        res = tdSql.getData(0, 0)
        assert res == 'EF'

        tdSql.query('select substr(null, 1, 3);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substr('HelloWorld', 2, 5);")
        res = tdSql.getData(0, 0)
        assert res == 'elloW'

        tdSql.query('select name, substr(name, 1, 3) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lil'

        tdSql.query('select nch1, substr(nch1, 2, 4) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'ovel'

        tdSql.query("select substr('1234567890', -5, 5);")
        res = tdSql.getData(0, 0)
        assert res == '67890'

        tdSql.query('select var1, substr(var1, 1, 6) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novel'

        tdSql.query("select substr('!@#$%^&*()', 2, 4);")
        res = tdSql.getData(0, 0)
        assert res == '@#$%'

        tdSql.query("select substr('你好世界', 3, 2);")
        res = tdSql.getData(0, 0)
        assert res == '世界'

        tdSql.query("select substr('ABCDEFG', 10, 5);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substr('ABCDEFG', -1, 3);")
        res = tdSql.getData(0, 0)
        assert res == 'G'

        tdSql.query("select substr('1234567890', -15, 5);")
        res = tdSql.getData(0, 0)
        assert res == '12345'

        tdSql.query("select substr(concat('Hello', 'World'), 1, 5);")
        res = tdSql.getData(0, 0)
        assert res == 'Hello'

        tdSql.query("select substr('HelloWorld', 1, length('Hello'));")
        res = tdSql.getData(0, 0)
        assert res == 'Hello'

        tdSql.query("select substr(upper('helloworld'), 2, 4);")
        res = tdSql.getData(0, 0)
        assert res == 'ELLO'

        tdSql.query("select substr(trim('  HelloWorld  '), 1, 5);")
        res = tdSql.getData(0, 0)
        assert res == 'Hello'

    def test_substr_idx(self):
        self.test_normal_query('substr_idx')

        tdSql.query("select substring_index(null, '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring_index('www.taosdata.com', null, 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring_index('www.taosdata.com', '.', null);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring_index('www.taosdata.com', '.', 0);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substring_index('a.b.c', '.', 1);")
        res = tdSql.getData(0, 0)
        assert res == 'a'

        tdSql.query("select substring_index('a.b.c', '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'a.b'

        tdSql.query("select substring_index('a.b.c', '.', -1);")
        res = tdSql.getData(0, 0)
        assert res == 'c'

        tdSql.query("select substring_index('', '.', 1);")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select substring_index(NULL, '.', 1);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select substring_index('apple.orange.banana', '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange'

        tdSql.query("select name, substring_index(name, ' ', 1) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lili'

        tdSql.query("select var1, substring_index(var1, '-', -1) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novel'

        tdSql.query("select substring_index('192.168.1.1', '.', 3);")
        res = tdSql.getData(0, 0)
        assert res == '192.168.1'

        tdSql.query("select nch1, substring_index(nch1, ',', 3) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novel'

        tdSql.query("select substring_index('abc@xyz.com', '.', 5);")
        res = tdSql.getData(0, 0)
        assert res == 'abc@xyz.com'

        tdSql.query("select substring_index('123456789', '.', 1);")
        res = tdSql.getData(0, 0)
        assert res == '123456789'

        tdSql.query("select substring_index('abcdef', ' ', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'abcdef'

        tdSql.query("select substring_index('ABCDEFG', '-', -1);")
        res = tdSql.getData(0, 0)
        assert res == 'ABCDEFG'

        tdSql.query("select substring_index('apple', '.', -3);")
        res = tdSql.getData(0, 0)
        assert res == 'apple'

        tdSql.query("select substring_index(concat('apple', '.', 'orange', '.', 'banana'), '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange'

        tdSql.query("select substring_index('apple.orange.banana', '.', length('apple'));")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange.banana'

        tdSql.query("select substring_index(upper('apple.orange.banana'), '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'APPLE.ORANGE'

        tdSql.query("select substring_index(trim('  apple.orange.banana  '), '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange'

        tdSql.query("select substring_index(concat('apple', '.', 'orange', '.', 'banana'), '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange'

        tdSql.query("select substring_index('apple.orange.banana', '.', length('apple'));")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange.banana'

        tdSql.query("select substring_index(upper('apple.orange.banana'), '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'APPLE.ORANGE'

        tdSql.query("select substring_index(trim('  apple.orange.banana  '), '.', 2);")
        res = tdSql.getData(0, 0)
        assert res == 'apple.orange'

    def test_trim(self):
        self.test_normal_query('trim')

        tdSql.query('select trim(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select trim('  hello  ');")
        res = tdSql.getData(0, 0)
        assert res == 'hello'

        tdSql.query("select trim(leading ' ' from '   hello');")
        res = tdSql.getData(0, 0)
        assert res == 'hello'

        tdSql.query("select trim(trailing ' ' from 'hello   ');")
        res = tdSql.getData(0, 0)
        assert res == 'hello'

        tdSql.query("select trim('0' from '000123000');")
        res = tdSql.getData(0, 0)
        assert res == '123'

        tdSql.query("select trim('');")
        res = tdSql.getData(0, 0)
        assert res == ''

        tdSql.query("select name, trim(name) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'lili'
        assert res1 == 'lili'

        tdSql.query("select var1, trim(trailing '!' from var1) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novel'

        tdSql.query("select nch1, trim(leading '-' from nch1) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'novel'
        assert res1 == 'novel'

        tdSql.query("select trim('   apple banana   ');")
        res = tdSql.getData(0, 0)
        assert res == 'apple banana'

        tdSql.query("select var2, trim('*' from var2) from ts_4893.meters limit 1;")
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'e'
        assert res1 == 'e'

        tdSql.query("select trim('x' from 'hello');")
        res = tdSql.getData(0, 0)
        assert res == 'hello'

        tdSql.query("select trim('longer' from 'short');")
        res = tdSql.getData(0, 0)
        assert res == 'short'

        tdSql.query("select trim('hello');")
        res = tdSql.getData(0, 0)
        assert res == 'hello'

        tdSql.query("select trim('   12345   ');")
        res = tdSql.getData(0, 0)
        assert res == '12345'

        tdSql.query("select trim(concat('   hello', '   world   '));")
        res = tdSql.getData(0, 0)
        assert res == 'hello   world'

        tdSql.query("select trim(upper('  hello world  '));")
        res = tdSql.getData(0, 0)
        assert res == 'HELLO WORLD'

        tdSql.query("select trim(substring('   hello world   ', 4));")
        res = tdSql.getData(0, 0)
        assert res == 'hello world'

        tdSql.query("select trim(replace('   hello world   ', ' ', '-'));")
        res = tdSql.getData(0, 0)
        assert res == '---hello-world---'

    def test_timediff(self):
        self.test_normal_query('timediff')

        tdSql.query("select timediff(null, '2022-01-01 08:00:01', 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('2022-01-01 08:00:00', null, 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('2022/01/31', '2022/01/01', 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('20220131', '20220101', 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('22/01/31', '22/01/01', 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('01/31/22', '01/01/22', 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('31-JAN-22', '01-JAN-22', 1s);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('22/01/31', '22/01/01');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('www', 'ttt');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select timediff(ts, ts) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select timediff(ts, ts - 1d) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 86400000

        tdSql.query("select timediff(ts, '00:00:00') from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select timediff(ts, null) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('25:61:61', ts) from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select timediff('invalid_format', ts) from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select timediff(name, ts) from ts_4893.meters limit 2;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(1, 0)
        assert res0 == None
        assert res1 == None

        tdSql.query("select timediff('string_value', 'another_string') from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.error("select timediff(min(ts), '2023-01-01 00:00:00') from ts_4893.meters limit 1;")
        tdSql.error("select timediff(max(ts), '2023-12-31 23:59:59') from ts_4893.meters limit 1;")
        tdSql.error('select (select timediff(ts, (select max(ts) from ts_4893.meters)) from ts_4893.meters where id = m.id) from ts_4893.meters m;')

    def test_week(self):
        self.test_normal_query('week')

        tdSql.query('select week(null, 0);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('abc');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('1721020591', 0);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('1721020666229', 0);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('01/01/2020', 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('20200101', 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('20/01/01', 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('11/01/31', 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('01-JAN-20', 2);")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('2024-02-29 00:00:00');")
        res = tdSql.getData(0, 0)
        assert res == 8

        tdSql.query("select week('2023-09-25');")
        res = tdSql.getData(0, 0)
        assert res == 39

        tdSql.query("select week('2023-09-24');")
        res = tdSql.getData(0, 0)
        assert res == 39

        tdSql.query('select week(ts) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 28

        tdSql.query('select id, week(ts) from ts_4893.meters where id = 1 limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 28

        tdSql.query('select week(name) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select week('9999-12-31');")
        res = tdSql.getData(0, 0)
        assert res == 52

        tdSql.query('select week(ts), dayofweek(ts) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 28
        assert res1 == 6

        tdSql.query("select week(timediff(ts, '2024-10-10 09:36:50.172')) from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == 39

        tdSql.query('select groupid, sum(week(ts)) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 2669490

    def test_weekday(self):
        self.test_normal_query('weekday')

        tdSql.query('select weekday(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('1721020591');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('1721020666229');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('abc');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('01/01/2020');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('20200101');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('20/01/01');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('11/01/32');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('01-JAN-20');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('2024-02-29');")
        res = tdSql.getData(0, 0)
        assert res == 3

        tdSql.query("select weekday('2023-09-24');")
        res = tdSql.getData(0, 0)
        assert res == 6

        tdSql.query("select weekday('2023-09-25');")
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select id, weekday(ts) from ts_4893.meters where id = 1 limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 4

        tdSql.query('select weekday(name) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekday('9999-12-31')")
        res = tdSql.getData(0, 0)
        assert res == 4

        tdSql.query('select weekday(ts), dayofweek(ts) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 4
        assert res1 == 6

        tdSql.query("select weekday(timediff(ts, '2024-10-10 09:36:50.172')) from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == 4

        tdSql.query('select groupid, sum(weekday(ts)) from ts_4893.meters group by groupid;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select weekday(ts) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 4

        tdSql.error('select weekday(hello) from ts_4893.meters limit 1;')

    def test_weekofyear(self):
        self.test_normal_query('weekofyear')

        tdSql.query('select weekofyear(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('1721020591');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('1721020666229');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('abc');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('01/01/2020');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('20200101');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('20/01/01');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('11/01/31');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('01-JAN-20');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('2024-02-29');")
        res = tdSql.getData(0, 0)
        assert res == 9

        tdSql.query("select weekofyear('2024-01-01');")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query("select weekofyear('2024-12-31');")
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select weekofyear(ts) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 28

        tdSql.query('select id, weekofyear(ts) from ts_4893.meters where id = 1 limit 1;')
        res0 = tdSql.getData(0, 0)
        res2 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res2 == 28

        tdSql.query('select weekofyear(name) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select weekofyear('9999-12-31');")
        res = tdSql.getData(0, 0)
        assert res == 52

        tdSql.query('select weekofyear(ts), dayofweek(ts) from ts_4893.meters limit 1;')
        res0 = tdSql.getData(0, 0)
        res2 = tdSql.getData(0, 1)
        assert res0 == 28
        assert res2 == 6

        tdSql.query("select weekofyear(timediff(ts, '2024-10-10 09:36:50.172')) from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == 40

        tdSql.query('select groupid, sum(weekofyear(ts)) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res2 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res2 == 2720120

    def test_dayofweek(self):
        self.test_normal_query('dayofweek')

        tdSql.query('select dayofweek(null);')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('1721020591');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('1721020666229');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('abc');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('01/01/2020');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('20200101');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('20/01/01');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('11/01/31');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('01-JAN-20');")
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('2024-02-29');")
        res = tdSql.getData(0, 0)
        assert res == 5

        tdSql.query("select dayofweek('2024-01-01');")
        res = tdSql.getData(0, 0)
        assert res == 2

        tdSql.query("select dayofweek('2024-12-31');")
        res = tdSql.getData(0, 0)
        assert res == 3

        tdSql.query('select dayofweek(ts) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 6

        tdSql.query('select id, dayofweek(ts) from ts_4893.meters where id = 1 limit 1;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select dayofweek(name) from ts_4893.meters limit 1;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query("select dayofweek('9999-12-31');")
        res = tdSql.getData(0, 0)
        assert res == 6

        tdSql.query("select dayofweek(timediff(ts, '2024-10-10 09:36:50.172')) from ts_4893.meters limit 1;")
        res = tdSql.getData(0, 0)
        assert res == 6

        tdSql.query('select groupid, sum(dayofweek(ts)) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 400012

    def test_stddev_pop(self):
        self.test_normal_query('stddev')

        tdSql.query('select stddev_pop(null) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select stddev_pop(current) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.1543397

        tdSql.query('select stddev_pop(voltage) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.8764597

        tdSql.query("select stddev_pop(phase) from ts_4893.meters where ts between '2023-01-01 00:00:00' and '2023-12-31 23:59:59';")
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.2880754

        tdSql.query('select stddev_pop(id) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2886.7513315

        tdSql.query('select groupid, stddev_pop(voltage) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert round(res1, 7) == 2.8764597

        tdSql.query('select location, stddev_pop(current) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert round(res1, 7) == 1.1543397

        tdSql.query('select stddev_pop(phase) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.2885955

        tdSql.query('select location, stddev_pop(voltage) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert round(res1, 7) == 2.8764597

        tdSql.query('select stddev_pop(voltage) from ts_4893.meters where voltage is not null;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.8764597

        tdSql.query('select stddev_pop(voltage) from ts_4893.meters where voltage is not null;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.8764597

        tdSql.query('select round(stddev_pop(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 1.15

        tdSql.query('select pow(stddev_pop(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.3325001

        tdSql.query('select log(stddev_pop(voltage) + 1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.3549223

        tdSql.query('select stddev_pop(total_voltage) from (select sum(voltage) as total_voltage from ts_4893.meters group by location);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.error('select stddev_pop(var1) from ts_4893.meters;')
        tdSql.error('select stddev_pop(current) from empty_ts_4893.meters;')
        tdSql.error('select stddev_pop(name) from ts_4893.meters;')
        tdSql.error('select stddev_pop(nonexistent_column) from ts_4893.meters;')

    def test_var_pop(self):
        self.test_normal_query('varpop')

        tdSql.query('select var_pop(null) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select var_pop(current) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.3325001

        tdSql.query('select var_pop(voltage) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 8.2740204

        tdSql.query("select var_pop(phase) from ts_4893.meters where ts between '2023-01-01 00:00:00' and '2023-12-31 23:59:59';")
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.0829874

        tdSql.query('select var_pop(id) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 8333333.25

        tdSql.query('select groupid, var_pop(voltage) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert round(res1, 7) == 8.2740204

        tdSql.query('select location, var_pop(current) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert round(res1, 7) == 1.3325001

        tdSql.query('select var_pop(phase) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.0832873

        tdSql.query('select location, var_pop(voltage) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert round(res1, 7) == 8.2740204

        tdSql.query('select var_pop(voltage) from ts_4893.meters where voltage is not null;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 8.2740204

        tdSql.query('select round(var_pop(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 1.33

        tdSql.query('select pow(var_pop(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 1.7755564

        tdSql.query('select log(var_pop(voltage) + 1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 2.2272170

        tdSql.query('select var_pop(total_voltage) from (select sum(voltage) as total_voltage from ts_4893.meters group by location);')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.error('select var_pop(var1) from ts_4893.meters;')
        tdSql.error('select var_pop(current) from empty_ts_4893.meters;')
        tdSql.error('select var_pop(name) from ts_4893.meters;')
        tdSql.error('select var_pop(nonexistent_column) from ts_4893.meters;')

    def test_rand(self):
        self.test_normal_query('rand')

        tdSql.query('select rand();')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(null);')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(0);')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(1);')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(-1);')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(12345678901234567890);')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(-12345678901234567890);')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(12345), rand(12345);')
        res0, res1 = tdSql.getData(0, 0), tdSql.getData(0, 1)
        assert res0 == res1

        tdSql.query('select rand() where rand() >= 0;')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand() where rand() < 1;')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand() where rand() >= 0 and rand() < 1;')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(9999999999) where rand(9999999999) >= 0 and rand(9999999999) < 1;')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand() from (select 1) t limit 1;')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select round(rand(), 3)')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select pow(rand(), 2)')
        res = tdSql.getData(0, 0)
        assert res >= 0
        assert res < 1

        tdSql.query('select rand(id) from ts_4893.meters limit 100;')
        for i in range(len(tdSql.res)):
            res = tdSql.getData(i, 0)
            assert res >= 0
            assert res < 1

        tdSql.error('select rand(3.14);')
        tdSql.error('select rand(-3.14);')
        tdSql.error("select rand('');")
        tdSql.error("select rand('hello');")

    def test_max(self):
        self.test_normal_query('max')

        tdSql.query('select max(null) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select max(name) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 'x'

        tdSql.query('select max(current) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 11.9989996

        tdSql.query('select max(voltage) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 224

        tdSql.query('select max(nch1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == '一二三四五六七八九十'

        tdSql.query('select max(var1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == '一二三四五六七八九十'

        tdSql.query('select max(id) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 9999

        tdSql.query('select max(id) from ts_4893.meters where id > 0;')
        res = tdSql.getData(0, 0)
        assert res == 9999

        tdSql.query('select max(id) from ts_4893.meters where id <= 0;')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select max(phase) from ts_4893.meters where ts between '2023-01-01 00:00:00' and '2023-12-31 23:59:59';")
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.9999660

        tdSql.query('select groupid, max(voltage) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 224

        tdSql.query('select location, max(current) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert round(res1, 7) == 11.9989996

        tdSql.query('select location, max(id) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert res1 == 9999

        tdSql.query('select max(voltage) from ts_4893.meters where voltage is not null;')
        res = tdSql.getData(0, 0)
        assert res == 224

        tdSql.query('select round(max(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 12

        tdSql.query('select pow(max(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 143.9759913

        tdSql.query('select log(max(voltage) + 1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 5.4161004

        tdSql.query('select max(total_voltage) from (select sum(voltage) as total_voltage from ts_4893.meters group by location);')
        res = tdSql.getData(0, 0)
        assert res == 21948660

        tdSql.error('select max(nonexistent_column) from ts_4893.meters;')

    def test_min(self):
        self.test_normal_query('min')

        tdSql.query('select min(null) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == None

        tdSql.query('select min(name) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 'haha'

        tdSql.query('select min(current) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 8

        tdSql.query('select min(voltage) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 215

        tdSql.query('select min(nch1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 'abc一二三abc一二三abc'

        tdSql.query('select min(var1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 'abc一二三abc一二三abc'

        tdSql.query('select min(id) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query('select min(id) from ts_4893.meters where id > 0;')
        res = tdSql.getData(0, 0)
        assert res == 1

        tdSql.query('select min(id) from ts_4893.meters where id <= 0;')
        res = tdSql.getData(0, 0)
        assert res == 0

        tdSql.query("select min(phase) from ts_4893.meters where ts between '2023-01-01 00:00:00' and '2023-12-31 23:59:59';")
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 0.0001700

        tdSql.query('select groupid, min(voltage) from ts_4893.meters group by groupid;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 1
        assert res1 == 215

        tdSql.query('select location, min(current) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert res1 == 8

        tdSql.query('select location, min(id) from ts_4893.meters group by location;')
        res0 = tdSql.getData(0, 0)
        res1 = tdSql.getData(0, 1)
        assert res0 == 'beijing'
        assert res1 == 0

        tdSql.query('select min(voltage) from ts_4893.meters where voltage is not null;')
        res = tdSql.getData(0, 0)
        assert res == 215

        tdSql.query('select round(min(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 8.0000000e+00

        tdSql.query('select pow(min(current), 2) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert res == 64

        tdSql.query('select log(min(voltage) + 1) from ts_4893.meters;')
        res = tdSql.getData(0, 0)
        assert round(res, 7) == 5.3752784

        tdSql.query('select min(total_voltage) from (select sum(voltage) as total_voltage from ts_4893.meters group by location);')
        res = tdSql.getData(0, 0)
        assert res == 21948660

        tdSql.error('select min(nonexistent_column) from ts_4893.meters;')

    def test_error(self):
        tdSql.error('select * from (select to_iso8601(ts, timezone()), timezone() from ts_4893.meters \
            order by ts desc) limit 1000;', expectErrInfo="Not supported timzone format") # TS-5340

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
        self.test_var_pop()

        # select function
        self.test_max()
        self.test_min()

        # error function
        self.test_error()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
