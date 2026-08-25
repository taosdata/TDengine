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
        "slowLogScope": "none"
    }

    def setup_class(cls):
        tdLog.info(f"insert data.")
        datafile = etool.getFilePath(os.path.dirname(__file__), "resource", "data", "d1001.data")

        tdSql.execute("create database if not exists ts_4893;")
        tdSql.execute("use ts_4893;")
        tdSql.execute("select database();")
        tdSql.execute("CREATE STABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT, "
            "`id` INT, `name` VARCHAR(64), `nch1` NCHAR(50), `nch2` NCHAR(50), `var1` VARCHAR(50), "
            "`var2` VARCHAR(50)) TAGS (`groupid` TINYINT, `location` VARCHAR(16));")
        tdSql.execute("CREATE table IF NOT EXISTS d0 using meters tags(1, 'beijing')")
        tdSql.execute('insert into d0 file "%s"' % datafile)
        tdSql.execute("CREATE TABLE IF NOT EXISTS `n1` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, co NCHAR(10))")
        tdSql.execute("insert into n1 values(now, 1, null, '23')")
        tdSql.execute("insert into n1 values(now+1a, null, 3, '23')")
        tdSql.execute("insert into n1 values(now+2a, 5, 3, '23')")

        # Small controlled table for function-coexistence tests (5 rows, deterministic data)
        tdSql.execute("CREATE TABLE IF NOT EXISTS fc_d (ts TIMESTAMP, voltage INT, current FLOAT, id INT)")
        tdSql.execute(
            "INSERT INTO fc_d VALUES"
            "('2024-01-01 00:00:00', 220, 10.0, 0)"
            "('2024-01-01 00:00:01', 215,  8.0, 1)"
            "('2024-01-01 00:00:02', 225, 12.0, 2)"
            "('2024-01-01 00:00:03', 210,  9.0, 3)"
            "('2024-01-01 00:00:04', 230, 11.0, 4)"
        )

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
            order by ts desc) limit 1000;",
            # the message quotes the rejected timezone, which varies per host
            expectErrInfo="Invalid timezone", fullMatched=False) # TS-5340
        tdSql.error("select * from ts_4893.meters where ts between(timetruncate(now, 1h) - 10y) and timetruncate(now(), 10y) partition by voltage;",
                    expectErrInfo="Invalid time unit : timetruncate") #

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

        Labels: common,ci,integration,functional
        History:
            - 2024-9-28 qevolg Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        self.run_max()
        self.run_min()
        self.run_error()

    # -----------------------------------------------------------------------
    # Function coexistence rule tests
    # Covers FS "函数分类与使用规则" v1.0 (TSDB-v3.4.2)
    # -----------------------------------------------------------------------

    def run_func_coexist(self):
        # --- Result-file comparison: Rule 1 constant-expression examples ---
        self.run_normal_query_new("func_coexist")

        # --- Rule 1: single-row selection + aggregate on table data (always OK) ---
        tdSql.query("SELECT max(voltage), sum(id) FROM ts_4893.fc_d")
        tdSql.checkRows(1)
        tdSql.query("SELECT min(current), count(*) FROM ts_4893.fc_d")
        tdSql.checkRows(1)
        tdSql.query("SELECT first(voltage), count(*) FROM ts_4893.fc_d")
        tdSql.checkRows(1)
        tdSql.query("SELECT last(current), max(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(1)

        # --- Rule 1: multi-row selection alone (always OK) ---
        tdSql.query("SELECT top(voltage, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)
        tdSql.query("SELECT bottom(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)
        tdSql.query("SELECT tail(voltage, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)

        # --- Rule 2: multi-row selection + aggregate — always illegal ---
        tdSql.error("SELECT sum(current), top(voltage, 5) FROM ts_4893.fc_d")
        tdSql.error("SELECT top(voltage, 5), sum(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT bottom(voltage, 3), avg(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT sample(voltage, 3), count(*) FROM ts_4893.fc_d")

        # --- Rule 6: selection-agg (1 row) + set function (N/N-1 rows) → rows not equal → error ---
        tdSql.error("SELECT first(voltage), diff(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT max(voltage), csum(current) FROM ts_4893.fc_d")

        # --- Rule 3: multi-row selection + set — always illegal ---
        tdSql.error("SELECT top(voltage, 3), diff(current) FROM ts_4893.fc_d")

        # --- Rule 3: set functions with mismatched row counts — illegal ---
        tdSql.error("SELECT diff(voltage), csum(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT unique(voltage), csum(current) FROM ts_4893.fc_d")

        # --- Selection row-count mismatch — illegal ---
        tdSql.error("SELECT top(voltage, 3), bottom(current, 5) FROM ts_4893.fc_d")

    def test_func_coexist(self):
        """Function coexistence: valid combinations and known rule violations.

        Tests that valid function combinations produce correct results and
        known invalid combinations are rejected. Covers the 4-category
        function classification (Scalar/Aggregate/Set/Selection) coexistence
        rules from FS 函数分类与使用规则 v1.0.

        Catalog:
            - Function:Selection

        Since: v3.4.2.0

        Labels: common,ci

        Jira: TD-XXXXX

        """
        self.run_func_coexist()

    def test_func_coexist_after_fix(self):
        """Function coexistence: behavior expected after 差异 I/II/III/IV fixes.

        Tests document the target behavior after code fixes for discrepancies
        差异 I/II/III/IV recorded in FS appendix A (函数分类与使用规则 v1.0).
        Valid combos use result-file comparison; error cases use tdSql.error().

        Stable K-row / N-row / N-1 row combos are verified via:
          func_coexist_fix.in  — top+bottom(3), top+top(5), tail+tail(3), histogram pairs,
                                  diff pairs, derivative pairs, N-row pipeline pairs
          func_coexist_sets2.in — bottom+bottom, tail+top, top+tail, tail+bottom, bottom+tail,
                                   histogram+bottom, triple K-row (top+bottom+tail), triple diff,
                                   lead+stateduration, csum+statecount, fill_forward+statecount,
                                   csum+csum, mavg+mavg K=2/K=3

        Catalog:
            - Function:Selection

        Since: v3.4.2.0

        Labels: common,ci

        Jira: TD-XXXXX

        """
        # --- Stable combos: result-file comparison ---
        self.run_normal_query_new("func_coexist_fix")
        self.run_normal_query_new("func_coexist_sets2")

        # --- Non-deterministic (sample output is random): checkRows only ---
        tdSql.query("SELECT top(voltage, 3), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)
        tdSql.query("SELECT bottom(voltage, 3), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)
        tdSql.query("SELECT sample(voltage, 3), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)
        tdSql.query("SELECT tail(voltage, 3), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)

        # --- 差异 I error: K mismatch ---
        tdSql.error("SELECT top(voltage, 5), bottom(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT top(voltage, 5), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.error(
            "SELECT histogram(voltage,'user_input','[0,100,200,300]',0),"
            " histogram(current,'user_input','[0,10,20]',0) FROM ts_4893.fc_d"
        )
        tdSql.error(
            "SELECT histogram(voltage,'user_input','[0,100,200,300]',0),"
            " top(current, 5) FROM ts_4893.fc_d"
        )
        tdSql.error("SELECT tail(voltage, 5), tail(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT tail(voltage, 5), top(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT tail(voltage, 3), bottom(current, 5) FROM ts_4893.fc_d")
        tdSql.error("SELECT sample(voltage, 5), sample(current, 3) FROM ts_4893.fc_d")

        # --- 差异 II error: MAVG returns N-K+1 rows (not N), cannot coexist with N-row funcs ---
        tdSql.error("SELECT lag(voltage, 1, 0), mavg(current, 2) FROM ts_4893.fc_d")
        tdSql.error("SELECT mavg(voltage, 3), csum(current) FROM ts_4893.fc_d")
        tdSql.error(
            "SELECT statecount(voltage, 'GE', 220), mavg(current, 2) FROM ts_4893.fc_d"
        )

        # --- 差异 IV error: MAVG with different K or non-MAVG multi-row funcs ---
        tdSql.error("SELECT mavg(voltage, 2), mavg(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT mavg(voltage, 2), diff(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT mavg(voltage, 2), sum(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT mavg(voltage, 2), top(current, 3) FROM ts_4893.fc_d")

        # --- 差异 III error: N-1 row ≠ N row ---
        tdSql.error("SELECT diff(voltage), csum(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT diff(voltage), lead(current, 1, 0) FROM ts_4893.fc_d")
        # diff+diff with any option combo is OK: in PROCESS_BY_ROW combined execution,
        # ignoreNull/ignoreNeg options output NULL (not skip) when another function runs
        # simultaneously, so row count is always N-1.
        tdSql.query("SELECT diff(voltage, 2), diff(current) FROM ts_4893.fc_d")
        # derivative(ignoreNeg=1) + derivative(ignoreNeg=0): same PROCESS_BY_ROW semantics
        tdSql.query(
            "SELECT derivative(voltage, 1s, 1), derivative(current, 1s, 0)"
            " FROM ts_4893.fc_d"
        )
        # K-row and N-1 row functions cannot coexist (row count mismatch)
        tdSql.error("SELECT top(voltage, 3), bottom(current, 3), diff(id) FROM ts_4893.fc_d")

    def check_pipeline_column_anchor(self, sql, expected_rows, first_anchor, last_anchor):
        tdSql.query(sql)
        tdSql.checkRows(expected_rows)
        tdSql.checkData(0, 0, first_anchor)
        tdSql.checkData(expected_rows - 1, 0, last_anchor)

    def do_pipeline_same_type_column_anchor(self):
        cases = [
            (
                "SELECT id, lag(voltage, 1), lag(current, 1) FROM ts_4893.fc_d",
                5,
                0,
                4,
            ),
            (
                "SELECT id, lead(voltage, 1), lead(current, 1) FROM ts_4893.fc_d",
                5,
                0,
                4,
            ),
            (
                "SELECT id, fill_forward(voltage), fill_forward(current) FROM ts_4893.fc_d",
                5,
                0,
                4,
            ),
            (
                "SELECT id, csum(voltage), csum(current) FROM ts_4893.fc_d",
                5,
                0,
                4,
            ),
            (
                "SELECT id, statecount(voltage, 'GE', 220),"
                " statecount(current, 'GE', 10) FROM ts_4893.fc_d",
                5,
                0,
                4,
            ),
            (
                "SELECT id, stateduration(voltage, 'GE', 220, 1s),"
                " stateduration(current, 'GE', 10, 1s) FROM ts_4893.fc_d",
                5,
                0,
                4,
            ),
            (
                "SELECT id, diff(voltage), diff(current) FROM ts_4893.fc_d",
                4,
                1,
                4,
            ),
            (
                "SELECT id, derivative(voltage, 1s, 0),"
                " derivative(current, 1s, 0) FROM ts_4893.fc_d",
                4,
                1,
                4,
            ),
            (
                "SELECT id, mavg(voltage, 2), mavg(current, 2) FROM ts_4893.fc_d",
                4,
                1,
                4,
            ),
        ]

        for sql, expected_rows, first_anchor, last_anchor in cases:
            self.check_pipeline_column_anchor(sql, expected_rows, first_anchor, last_anchor)

        print("pipeline same-type column anchors .......... [ passed ]")

    def do_pipeline_cross_type_column_anchor(self):
        self.check_pipeline_column_anchor(
            "SELECT id, lag(voltage, 1), lead(current, 1),"
            " fill_forward(id), csum(voltage),"
            " statecount(current, 'GE', 10),"
            " stateduration(current, 'GE', 10, 1s) FROM ts_4893.fc_d",
            5,
            0,
            4,
        )
        self.check_pipeline_column_anchor(
            "SELECT id, diff(voltage), derivative(current, 1s, 0) FROM ts_4893.fc_d",
            4,
            1,
            4,
        )
        self.check_pipeline_column_anchor(
            "SELECT id, mavg(voltage, 2), mavg(current, 2),"
            " mavg(id, 2) FROM ts_4893.fc_d",
            4,
            1,
            4,
        )

        tdSql.query(
            "SELECT ts, lag(voltage, 1), lead(current, 1),"
            " fill_forward(id), csum(voltage),"
            " statecount(current, 'GE', 10),"
            " stateduration(current, 'GE', 10, 1s) FROM ts_4893.fc_d"
        )
        tdSql.checkRows(5)
        tdSql.query(
            "SELECT ts, diff(voltage), derivative(current, 1s, 0) FROM ts_4893.fc_d"
        )
        tdSql.checkRows(4)
        tdSql.query(
            "SELECT ts, mavg(voltage, 2), mavg(current, 2),"
            " mavg(id, 2) FROM ts_4893.fc_d"
        )
        tdSql.checkRows(4)

        print("pipeline cross-type column anchors ......... [ passed ]")

    def do_pipeline_column_anchor_rejections(self):
        tdSql.error(
            "SELECT ts, mavg(voltage, 2), mavg(current, 3) FROM ts_4893.fc_d"
        )
        tdSql.error("SELECT ts, mavg(voltage, 2), csum(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT ts, diff(voltage), csum(current) FROM ts_4893.fc_d")
        tdSql.error(
            "SELECT ts, statecount(voltage, 'GE', 220),"
            " mavg(current, 2) FROM ts_4893.fc_d"
        )

        print("pipeline column-anchor rejections .......... [ passed ]")

    def test_pipeline_function_column_coexistence(self):
        """Pipeline functions coexist with ordinary-column row anchors.

        1. Cover two same-type functions for every row-transform pipeline function
        2. Cover compatible N-row, N-1-row, and same-K N-K+1 cross-function groups
        3. Verify both timestamp and ordinary-column anchors preserve row alignment
        4. Keep rejecting combinations whose output row counts differ

        Catalog:
            - Function:Selection

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-06 Codex Added pipeline-function column-anchor coverage

        """
        self.do_pipeline_same_type_column_anchor()
        self.do_pipeline_cross_type_column_anchor()
        self.do_pipeline_column_anchor_rejections()

    def test_func_coexist_rules(self):
        """Function coexistence: rule 4/5, uniqueness constraint, DIFF ignore param.

        Covers supplementary scenarios from TS §4.5: scalar+aggregate (rule 4),
        scalar+non-pipeline set (rule 5), uniqueness constraint for subset-selecting
        pipeline functions, DIFF ignore parameter effects on row count, and additional
        error regression cases (selectivity agg + K-row set).

        Stable valid combos are verified via func_coexist_rules.in result-file comparison:
          abs+max, abs+min, abs+first, abs+last (rule 4 valid variants),
          voltage+1 + lag + csum (N-row uniqueness),
          diff(v,0) / diff(v,1) (DIFF ignore param N-1 rows),
          abs+top(3), abs+bottom(3), abs+top(5), abs+1+tail(2) (rule 5/scalar+K-row).

        Catalog:
            - Function:Selection

        Since: v3.4.2.0

        Labels: common,ci

        Jira: TD-XXXXX

        """
        # --- Stable valid combos: result-file comparison ---
        self.run_normal_query_new("func_coexist_rules")

        # --- T-R4-E: rule 4 error (non-selectivity aggregate, no row anchor) ---
        tdSql.error("SELECT abs(voltage), sum(voltage) FROM ts_4893.fc_d")
        tdSql.error("SELECT abs(voltage), count(*) FROM ts_4893.fc_d")
        tdSql.error("SELECT abs(voltage), max(voltage), sum(current) FROM ts_4893.fc_d")

        # --- T-R5-E1: rule 5 error (scalar + non-pipeline set function) ---
        tdSql.error(
            "SELECT abs(voltage), histogram(voltage,'user_input','[0,100,200,300]',0)"
            " FROM ts_4893.fc_d"
        )

        # --- Uniqueness constraint errors (2 subset-selecting + scalar) ---
        tdSql.error("SELECT abs(voltage), max(voltage), min(current) FROM ts_4893.fc_d")
        tdSql.error("SELECT abs(voltage), top(voltage, 5), top(current, 5) FROM ts_4893.fc_d")
        tdSql.error("SELECT abs(voltage), top(voltage, 3), max(current) FROM ts_4893.fc_d")

        # --- T-DI-E1: DIFF ignore_option=2 + other N-row set function (not DIFF) → error ---
        tdSql.error("SELECT diff(voltage, 2), csum(current) FROM ts_4893.fc_d")

        # --- T-REG-7: regression (N-1 row ≠ K row) ---
        tdSql.error("SELECT diff(voltage), top(current, 3) FROM ts_4893.fc_d")
        # diff(2)+diff(0): OK in combined PROCESS_BY_ROW execution (outputs NULL, not skip)

        # --- T-REG-9/T-REG-10: selectivity agg (1 row) cannot coexist with K-row set func ---
        # MAX/MIN/FIRST/LAST are 1-row aggregates; mixing with TOP/BOTTOM/TAIL (K rows) → error
        tdSql.error("SELECT max(voltage), top(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT first(voltage), tail(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT min(voltage), bottom(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT last(voltage), top(current, 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT count(voltage), csum(current) FROM ts_4893.fc_d")

        # --- Nested function legality ---
        tdSql.error("SELECT top(sum(voltage), 3) FROM ts_4893.fc_d")
        tdSql.error("SELECT sum(top(voltage, 3)) FROM ts_4893.fc_d")

        # --- Non-deterministic (sample output is random): checkRows only ---
        tdSql.query("SELECT sample(voltage, 3), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)
        tdSql.query("SELECT tail(voltage, 3), sample(current, 3) FROM ts_4893.fc_d")
        tdSql.checkRows(3)

    def run_sys_meta_func(self):
        self.run_normal_query_new("sys_meta_func")

    def test_sys_meta_func(self):
        """System/metadata functions: standalone use, coexistence with AGG/SET, and nesting.

        Verifies that system and metadata functions (CLIENT_VERSION, SERVER_VERSION,
        CURRENT_USER, DATABASE, SERVER_STATUS, SLEEP) behave as scalar functions and
        can coexist with aggregate and set functions in the same SELECT.

        Stable-result queries (DATABASE, SERVER_STATUS, SLEEP) are verified via
        sys_meta_func.in result-file comparison.  Version and user functions whose
        output is environment-specific are verified with row-count / no-error checks.

        Catalog:
            - Function:Selection

        Since: v3.4.2.0

        Labels: common,ci

        Jira: TD-XXXXX

        """
        # --- T-SYS-1: stable-value functions, result-file comparison ---
        # Covers: database() standalone, server_status() standalone,
        #   database()/server_status() + avg (AGG coexistence),
        #   database() + diff (SET coexistence),
        #   length(database()) scalar nesting,
        #   avg/diff of length(database()) (AGG/SET wrapping scalar-nested system func),
        #   sleep(0) + avg, sleep(0) + diff.
        self.run_sys_meta_func()

        # --- T-SYS-2: standalone use (no table) ---
        tdSql.query("SELECT client_version()")
        tdSql.checkRows(1)
        tdSql.query("SELECT server_version()")
        tdSql.checkRows(1)
        tdSql.query("SELECT current_user()")
        tdSql.checkRows(1)
        tdSql.query("SELECT server_status()")
        tdSql.checkRows(1)

        # --- T-SYS-3: version/user functions coexist with AGG (checkRows only) ---
        tdSql.query("SELECT client_version(), avg(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(1)
        tdSql.query("SELECT server_version(), avg(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(1)
        tdSql.query("SELECT current_user(), avg(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(1)

        # --- T-SYS-4: version/user functions coexist with SET (checkRows only) ---
        tdSql.query("SELECT client_version(), diff(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(4)
        tdSql.query("SELECT server_version(), diff(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(4)
        tdSql.query("SELECT current_user(), diff(voltage) FROM ts_4893.fc_d")
        tdSql.checkRows(4)

        # --- T-SYS-5: scalar nesting (system func inside scalar func) ---
        tdSql.query("SELECT length(client_version()) FROM ts_4893.fc_d LIMIT 1")
        tdSql.checkRows(1)
        tdSql.query("SELECT length(server_version()) FROM ts_4893.fc_d LIMIT 1")
        tdSql.checkRows(1)
        tdSql.query("SELECT length(current_user()) FROM ts_4893.fc_d LIMIT 1")
        tdSql.checkRows(1)

        # --- T-SYS-6: system func nested in AGG ---
        tdSql.query("SELECT avg(length(client_version())) FROM ts_4893.fc_d")
        tdSql.checkRows(1)
        tdSql.query("SELECT count(current_user()) FROM ts_4893.fc_d")
        tdSql.checkRows(1)

        # --- T-SYS-7: system func nested in SET ---
        tdSql.query("SELECT diff(length(client_version())) FROM ts_4893.fc_d")
        tdSql.checkRows(4)
        tdSql.query("SELECT csum(length(server_version())) FROM ts_4893.fc_d")
        tdSql.checkRows(5)

        # --- T-SYS-8: SLEEP cannot be used as a direct numeric aggregate argument ---
        tdSql.error("SELECT avg(sleep(0)) FROM ts_4893.fc_d")
        tdSql.error("SELECT sample(voltage, 5), sample(current, 3) FROM ts_4893.fc_d")
