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
        datafile = etool.curFile(__file__, "data/d1001.data")

        tdSql.execute("create database ts_4893;")
        tdSql.execute("use ts_4893;")
        tdSql.execute("select database();")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT, "
            "`id` INT, `name` VARCHAR(64), `nch1` NCHAR(50), `nch2` NCHAR(50), `var1` VARCHAR(50), "
            "`var2` VARCHAR(50)) TAGS (`groupid` TINYINT, `location` VARCHAR(16));")
        tdSql.execute("CREATE table d0 using meters tags(1, 'beijing')")
        tdSql.execute("insert into d0 file '%s'" % datafile)
        tdSql.execute("CREATE TABLE `n1` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, co NCHAR(10))")
        tdSql.execute("insert into n1 values(now, 1, null, '23')")
        tdSql.execute("insert into n1 values(now+1a, null, 3, '23')")
        tdSql.execute("insert into n1 values(now+2a, 5, 3, '23')")

    def test_normal_query_new(self, testCase):
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.csv")

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
        
    def test_base64(self):
        self.test_normal_query_new("base64")
    
    def test_crc32(self):
        self.test_normal_query_new("crc32")

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

    def test_st_x(self):
        self.test_normal_query_new("st_x")

        tdSql.error("select st_x('');")
        tdSql.error("select st_x(FALSE);")
        tdSql.error("select st_x(123);")
        tdSql.error("select st_x(123.45);")
        tdSql.error("select st_x('strings are not allowed');")
        tdSql.error("select st_x(ST_GeomFromText('LINESTRING (30 10, 10 30, 40 40)'));")
        tdSql.error("select st_x(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")
        tdSql.error("select st_x(ST_GeomFromText('POINT(45 900)'), 'strings are not allowed');")
        tdSql.error("select st_x(ST_GeomFromText('POINT(45 900)'), TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")
        tdSql.error("select st_x(ST_GeomFromText('POINT(45 900)'), 100000000000000000000000000000000000000003445);")

    def test_st_y(self):
        self.test_normal_query_new("st_y")

        tdSql.error("select st_y('');")
        tdSql.error("select st_y(FALSE);")
        tdSql.error("select st_y(123);")
        tdSql.error("select st_y(123.45);")
        tdSql.error("select st_y('strings are not allowed');")
        tdSql.error("select st_y(ST_GeomFromText('LINESTRING (30 10, 10 30, 40 40)'));")
        tdSql.error("select st_y(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")
        tdSql.error("select st_y(ST_GeomFromText('POINT(45 900)'), 'strings are not allowed');")
        tdSql.error("select st_y(ST_GeomFromText('POINT(45 900)'), TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")
        tdSql.error("select st_y(ST_GeomFromText('POINT(45 900)'), 100000000000000000000000000000000000000003445);")
        
    def test_st_numpoints(self):
        self.test_normal_query_new("st_numpoints")

        tdSql.error("select st_numpoints('');")
        tdSql.error("select st_numpoints(FALSE);")
        tdSql.error("select st_numpoints(123);")
        tdSql.error("select st_numpoints(123.45);")
        tdSql.error("select st_numpoints('strings are not allowed');")
        tdSql.error("select st_numpoints(ST_GeomFromText('POINT(45 900)'));")
        tdSql.error("select st_numpoints(ST_GeomFromText('LINESTRING (45 900)'));")

    def test_st_numinteriorrings(self): 
        self.test_normal_query_new("st_numinteriorrings")

        tdSql.error("select st_numinteriorrings('');")
        tdSql.error("select st_numinteriorrings(FALSE);")
        tdSql.error("select st_numinteriorrings(123);")
        tdSql.error("select st_numinteriorrings(123.45);")
        tdSql.error("select st_numinteriorrings('strings are not allowed');")
        tdSql.error("select st_numinteriorrings(ST_GeomFromText('POINT(45 900)'));")
        tdSql.error("select st_numinteriorrings(ST_GeomFromText('LINESTRING (30 10, 10 30, 40 40)'));")
        tdSql.error("select st_numinteriorrings(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")

    def test_st_numgeometries(self): 
        self.test_normal_query_new("st_numgeometries")

        tdSql.error("select st_numgeometries('');")
        tdSql.error("select st_numgeometries(FALSE);")
        tdSql.error("select st_numgeometries(123);")
        tdSql.error("select st_numgeometries(123.45);")
        tdSql.error("select st_numgeometries('strings are not allowed');")
        tdSql.error("select st_numgeometries(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")

    def test_st_issimple(self): 
        self.test_normal_query_new("st_issimple")

        tdSql.error("select st_issimple('');")
        tdSql.error("select st_issimple(FALSE);")
        tdSql.error("select st_issimple(123);")
        tdSql.error("select st_issimple(123.45);")
        tdSql.error("select st_issimple('strings are not allowed');")
        tdSql.error("select st_issimple(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")

    def test_st_isempty(self): 
        self.test_normal_query_new("st_isempty")

        tdSql.error("select st_isempty('');")
        tdSql.error("select st_isempty(FALSE);")
        tdSql.error("select st_isempty(123);")
        tdSql.error("select st_isempty(123.45);")
        tdSql.error("select st_isempty('strings are not allowed');")
        tdSql.error("select st_isempty(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")

    def test_st_dimension(self): 
        self.test_normal_query_new("st_dimension")

        tdSql.error("select st_dimension('');")
        tdSql.error("select st_dimension(FALSE);")
        tdSql.error("select st_dimension(123);")
        tdSql.error("select st_dimension(123.45);")
        tdSql.error("select st_dimension('strings are not allowed');")
        tdSql.error("select st_dimension(TO_TIMESTAMP('2000-01-01 00:00:00+00', 'yyyy-mm-dd hh24:mi:ss'));")

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

    def test_sum(self):
        self.test_normal_query_new("sum")

    def test_statecount(self):
        self.test_normal_query_new("statecount")

    def test_avg(self):
        self.test_normal_query_new("avg")

    def test_leastsquares(self):
        self.test_normal_query_new("leastsquares")

    def test_error(self):
        tdSql.error("select * from (select to_iso8601(ts, timezone()), timezone() from ts_4893.meters \
            order by ts desc) limit 1000;", expectErrInfo="Invalid parameter data type : to_iso8601") # TS-5340
        tdSql.error("select * from ts_4893.meters where ts between(timetruncate(now, 1h) - 10y) and timetruncate(now(), 10y) partition by voltage;",
                    expectErrInfo="Invalid timzone format : timetruncate") #

    def test_greatest(self):
        self.test_normal_query_new("greatest")
        
        tdSql.execute("alter local 'compareAsStrInGreatest' '1';")
        
        tdSql.query("select GREATEST(NULL, NULL, NULL, NULL);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select GREATEST(1, NULL, NULL, NULL);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select GREATEST(id, NULL, 1) from ts_4893.meters order by ts limit 10;")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, None)

        tdSql.query("select GREATEST(cast(100 as tinyint), cast(101 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.101")

        tdSql.query("select GREATEST(cast(101 as tinyint), cast(100 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.101")

        tdSql.query("select GREATEST(cast(1000 as smallint), cast(1001 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:01.001")

        tdSql.query("select GREATEST(cast(1001 as smallint), cast(1000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:01.001")

        tdSql.query("select GREATEST(cast(1000000 as int), cast(1000001 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:16:40.001")

        tdSql.query("select GREATEST(cast(1000001 as int), cast(1000000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:16:40.001")

        tdSql.query("select GREATEST(cast(1000000000 as bigint), cast(1000000001 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-12 21:46:40.001")

        tdSql.query("select GREATEST(cast(1000000001 as bigint), cast(1000000000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-12 21:46:40.001")

        tdSql.query("select GREATEST(cast(1725506504000 as timestamp), cast(1725506510000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-09-05 11:21:50")

        tdSql.query("select GREATEST(cast(1725506510000 as timestamp), cast(1725506504000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-09-05 11:21:50")

        tdSql.query("select GREATEST(cast(100 as tinyint), cast(101 as varchar(20)), cast(102 as float));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "102.000000")

        tdSql.query("select GREATEST(cast(100 as varchar(20)), cast(101 as tinyint), cast(102 as float));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "102.000000")
        
        tdSql.query("select GREATEST(now, 1);")
        tdSql.query("select GREATEST(now, 1.0);")
        tdSql.query("select GREATEST(now, '1');")
        
        tdSql.error("select GREATEST(1)")
        tdSql.error("select GREATEST(cast('a' as varbinary), cast('b' as varbinary), 'c', 'd');")
        tdSql.error("select GREATEST(6, cast('f' as varbinary), cast('b' as varbinary), 'c', 'd');")       

    def test_least(self):
        self.test_normal_query_new("least")

        tdSql.execute("alter local 'compareAsStrInGreatest' '1';")
        
        tdSql.query("select LEAST(NULL, NULL, NULL, NULL);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select LEAST(1, NULL, NULL, NULL);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select LEAST(id, NULL, 1) from ts_4893.meters order by ts limit 10;")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, None)

        tdSql.query("select LEAST(cast(100 as tinyint), cast(101 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.100")

        tdSql.query("select LEAST(cast(101 as tinyint), cast(100 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.100")

        tdSql.query("select LEAST(cast(1000 as smallint), cast(1001 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:01.000")

        tdSql.query("select LEAST(cast(1001 as smallint), cast(1000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:01.000")

        tdSql.query("select LEAST(cast(1000000 as int), cast(1000001 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:16:40.000")

        tdSql.query("select LEAST(cast(1000001 as int), cast(1000000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:16:40.000")

        tdSql.query("select LEAST(cast(1000000000 as bigint), cast(1000000001 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-12 21:46:40.000")

        tdSql.query("select LEAST(cast(1000000001 as bigint), cast(1000000000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-12 21:46:40.000")

        tdSql.query("select LEAST(cast(1725506504000 as timestamp), cast(1725506510000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-09-05 11:21:44")

        tdSql.query("select LEAST(cast(1725506510000 as timestamp), cast(1725506504000 as timestamp));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2024-09-05 11:21:44")

        tdSql.query("select LEAST(cast(100 as tinyint), cast(101 as varchar(20)), cast(102 as float));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "100")

        tdSql.query("select LEAST(cast(100 as varchar(20)), cast(101 as tinyint), cast(102 as float));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "100")

        tdSql.query("select LEAST(cast(100 as float), cast(101 as tinyint), cast(102 as varchar(20)));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "100.000000")

        tdSql.query("select LEAST(cast(100 as float), cast(101 as varchar(20)), cast(102 as tinyint));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "100.000000")
        
        tdSql.query("select LEAST(now, 1);")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "1970-01-01 08:00:00.001")
        
        tdSql.query("select LEAST(now, 1.0);")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, 1)
        
        tdSql.query("select LEAST(now, '1');")
        tdSql.checkRows(1)
        tdSql.checkCols(1)
        tdSql.checkData(0, 0, "1")

        tdSql.error("select LEAST(cast('a' as varbinary), cast('b' as varbinary), 'c', 'd');")
        tdSql.error("select LEAST(cast('f' as varbinary), cast('b' as varbinary), 'c', 'd');")

    def test_greatest_large_table(self):
        tdLog.info("test greatest large table.")
           
        ts = 1741341251000
        create_table_sql = "CREATE TABLE `large_table` (`ts` TIMESTAMP"
        for i in range(1, 1001):
            if i % 5 == 1:
                create_table_sql += f", `col{i}` INT"
            elif i % 5 == 2:
                create_table_sql += f", `col{i}` FLOAT"
            elif i % 5 == 3:
                create_table_sql += f", `col{i}` DOUBLE"
            elif i % 5 == 4:
                create_table_sql += f", `col{i}` VARCHAR(64)"
            else:
                create_table_sql += f", `col{i}` NCHAR(50)"
        create_table_sql += ");"
        tdSql.execute(create_table_sql)
 
        for j in range(1000):
            insert_sql = f"INSERT INTO `large_table` VALUES ({ts +j}"
            for i in range(1, 1001):
                if i % 5 == 1:
                    insert_sql += f", {j + i}"
                elif i % 5 == 2:
                    insert_sql += f", {j + i}.1"
                elif i % 5 == 3:
                    insert_sql += f", {j + i}.2"
                elif i % 5 == 4:
                    insert_sql += f", '{j + i}'"
                else:
                    insert_sql += f", '{j + i}'"
            insert_sql += ");"
            tdSql.execute(insert_sql)

        greatest_query = "SELECT GREATEST("
        for i in range(1, 1001):
            greatest_query += f"`col{i}`"
            if i < 1000:
                greatest_query += ", "
        greatest_query += ") FROM `large_table` LIMIT 1;"
        tdLog.info(f"greatest_query: {greatest_query}")
        tdSql.execute(greatest_query)
        
        greatest_query = "SELECT "
        for i in range(1, 1001):
            greatest_query += f"`col{i}` > `col5`"
            if i < 1000:
                greatest_query += ", "
        greatest_query += " FROM `large_table` LIMIT 1;"
        tdLog.info(f"greatest_query: {greatest_query}")
        tdSql.execute(greatest_query)

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
        self.test_greatest()
        self.test_least()
        self.test_greatest_large_table()
        
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
        self.test_base64()
        self.test_crc32()

        # time function
        self.test_timediff()
        self.test_week()
        self.test_weekday()
        self.test_weekofyear()
        self.test_dayofweek()

        # agg function
        self.test_stddev_pop()
        self.test_varpop()
        self.test_avg()
        self.test_sum()
        self.test_leastsquares()
        self.test_statecount()

        # select function
        self.test_max()
        self.test_min()

        # geometry function
        self.test_st_x()
        self.test_st_y()
        self.test_st_numpoints()
        self.test_st_numinteriorrings()
        self.test_st_numgeometries()
        self.test_st_issimple()
        self.test_st_isempty()
        self.test_st_dimension()

        # error function
        self.test_error()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
