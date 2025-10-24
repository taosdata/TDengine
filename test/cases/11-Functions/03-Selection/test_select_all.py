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
from new_test_framework.utils import tdLog, tdSql, etool, inspect, tdCom
import os

class TestSelectFunction:
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def setup_class(cls):
        tdLog.info(f"insert data.")
        datafile = etool.getFilePath(os.path.dirname(__file__), "resource", "data", "d1001.data")

        tdSql.execute("create database ts_4893;")
        tdSql.execute("use ts_4893;")
        tdSql.execute("select database();")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT, "
            "`id` INT, `name` VARCHAR(64), `nch1` NCHAR(50), `nch2` NCHAR(50), `var1` VARCHAR(50), "
            "`var2` VARCHAR(50)) TAGS (`groupid` TINYINT, `location` VARCHAR(16));")
        tdSql.execute("CREATE table d0 using meters tags(1, 'beijing')")
        tdSql.execute('insert into d0 file "%s"' % datafile)
        tdSql.execute("CREATE TABLE `n1` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, co NCHAR(10))")
        tdSql.execute("insert into n1 values(now, 1, null, '23')")
        tdSql.execute("insert into n1 values(now+1a, null, 3, '23')")
        tdSql.execute("insert into n1 values(now+2a, 5, 3, '23')")

    def run_normal_query_new(self, testCase):
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "resource", "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "resource", "ans", f"{testCase}.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def run_pi(self):
        self.run_normal_query_new("pi")

    def run_round(self):
        self.run_normal_query_new("round")
        tdSql.error("select round(name, 2) from ts_4893.meters limit 1;")

    def run_exp(self):
        self.run_normal_query_new("exp")

    def run_truncate(self):
        self.run_normal_query_new("trunc")
        tdSql.error("select truncate(0.999);")
        tdSql.error("select truncate(-1.999);")
        tdSql.error("select truncate(null);")
        tdSql.error("select truncate(name, 1) from ts_4893.meters limit 1;")

    def run_ln(self):
        self.run_normal_query_new("ln")
        tdSql.error("select ln(name) from ts_4893.meters limit 1;")

    def run_mod(self):
        self.run_normal_query_new("mod")
        tdSql.error("select mod(name, 2) from ts_4893.meters limit 1;")

    def run_sign(self):
        self.run_normal_query_new("sign")
        tdSql.error("select sign('');")
        tdSql.error("select sign('abc');")
        tdSql.error("select sign('123');")
        tdSql.error("select sign('-456');")

    def run_degrees(self):
        self.run_normal_query_new("degrees")
        tdSql.error("select degrees('');")
        tdSql.error("select degrees('abc');")
        tdSql.error("select degrees('1.57');")

    def run_radians(self):
        self.run_normal_query_new("radians")
        tdSql.error("select radians('');")
        tdSql.error("select radians('abc');")
        tdSql.error("select radians('45');")

    def run_char_length(self):
        self.run_normal_query_new("char_length")
        tdSql.error("select char_length(12345);")
        tdSql.error("select char_length(true);")
        tdSql.error("select char_length(repeat('a', 1000000));")
        tdSql.error("select char_length(id) from ts_4893.meters;")

    def run_char(self):
        self.run_normal_query_new("char")
        res = [[chr(0)], [chr(1)], [chr(2)], [chr(3)], [chr(4)], [chr(5)], [chr(6)], [chr(7)], [chr(8)], [chr(9)]]
        tdSql.checkDataMem("select char(id) from ts_4893.d0 limit 10;", res)
        tdSql.checkDataMem("select char(id) from ts_4893.meters limit 10;", res)
        res = [[chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)], [chr(0)]]
        tdSql.checkDataMem("select char(nch1) from ts_4893.d0 limit 10;", res)
        tdSql.checkDataMem("select char(nch1) from ts_4893.meters limit 10;", res)
        tdSql.checkDataMem("select char(var1) from ts_4893.d0 limit 10;", res)
        tdSql.checkDataMem("select char(var1) from ts_4893.meters limit 10;", res)

    def run_ascii(self):
        self.run_normal_query_new("ascii")
        tdSql.error("select ascii(123);")

    def run_position(self):
        self.run_normal_query_new("position")

    def run_replace(self):
        self.run_normal_query_new("replace")

    def run_repeat(self):
        self.run_normal_query_new("repeat")

    def run_substr(self):
        self.run_normal_query_new("substr")

    def run_substr_idx(self):
        self.run_normal_query_new("substr_idx")

    def run_trim(self):
        self.run_normal_query_new("trim")

    def run_timediff(self):
        self.run_normal_query_new("timediff")
        tdSql.error("select timediff(min(ts), '2023-01-01 00:00:00') from ts_4893.meters limit 1;")
        tdSql.error("select timediff(max(ts), '2023-12-31 23:59:59') from ts_4893.meters limit 1;")
        tdSql.error("select (select timediff(ts, (select max(ts) from ts_4893.meters)) from ts_4893.meters where id = m.id) from ts_4893.meters m;")

    def run_week(self):
        self.run_normal_query_new("week")

    def run_weekday(self):
        self.run_normal_query_new("weekday")
        tdSql.error("select weekday(hello) from ts_4893.meters limit 1;")

    def run_weekofyear(self):
        self.run_normal_query_new("weekofyear")

    def run_dayofweek(self):
        self.run_normal_query_new("dayofweek")

    def run_stddev_pop(self):
        self.run_normal_query_new("stddev")
        tdSql.error("select stddev_pop(var1) from ts_4893.meters;")
        tdSql.error("select stddev_pop(current) from empty_ts_4893.meters;")
        tdSql.error("select stddev_pop(name) from ts_4893.meters;")
        tdSql.error("select stddev_pop(nonexistent_column) from ts_4893.meters;")

    def run_varpop(self):
        self.run_normal_query_new("varpop")
        tdSql.error("select var_pop(var1) from ts_4893.meters;")
        tdSql.error("select var_pop(current) from empty_ts_4893.meters;")
        tdSql.error("select var_pop(name) from ts_4893.meters;")
        tdSql.error("select var_pop(nonexistent_column) from ts_4893.meters;")

    def run_rand(self):
        self.run_normal_query_new("rand")
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

    def run_max(self):
        self.run_normal_query_new("max")
        tdSql.error("select max(nonexistent_column) from ts_4893.meters;")

    def run_min(self):
        self.run_normal_query_new("min")
        tdSql.error("select min(nonexistent_column) from ts_4893.meters;")

    def run_sum(self):
        self.run_normal_query_new("sum")

    def run_statecount(self):
        self.run_normal_query_new("statecount")

    def run_avg(self):
        self.run_normal_query_new("avg")

    def run_leastsquares(self):
        self.run_normal_query_new("leastsquares")

    def run_error(self):
        tdSql.error("select * from (select to_iso8601(ts, timezone()), timezone() from ts_4893.meters \
            order by ts desc) limit 1000;", expectErrInfo="Invalid parameter data type : to_iso8601") # TS-5340
        tdSql.error("select * from ts_4893.meters where ts between(timetruncate(now, 1h) - 10y) and timetruncate(now(), 10y) partition by voltage;",
                    expectErrInfo="Invalid timzone format : timetruncate") #

    def run_greatest(self):
        self.run_normal_query_new("greatest")
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

    def run_least(self):
        self.run_normal_query_new("least")
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

    def run_greatest_large_table(self):
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

    def test_select_function(self):
        """Select: all

        test select function max, min

        Catalog:
            - Function:Selection

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2024-9-28 qevolg Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        self.run_max()
        self.run_min()
        self.run_error()



