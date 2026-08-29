import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster,tdCom
from random import randint
import os
import subprocess
import json
import random
import time
import datetime

class Test_Last:
    caseName = "test_last"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test"
    stbname= "meters"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_last(self):
        """Agg-basic: last

        Test the LAST function

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-18 Stephen Jin

        """

        self.prepareHistoryData()
        self.insertNowData()
        self.checkResult()

    def prepareHistoryData(self):
        cmd = f"taosBenchmark -t 100 -n 10000 -y"
        ret = os.system(cmd)
        if ret != 0:
            raise Exception("taosBenchmark run failed")
        time.sleep(5)
        tdLog.info(f"Prepare history data:taosBenchmark -t 100 -n 10000 -y")

    def insertNowData(self):
        tdSql.execute(f"use {self.dbname}")

        tdSql.execute(f"insert into {self.dbname}.d1 values (1759194759000, 1.1, 1.1, 245)")
        tdSql.execute(f"insert into {self.dbname}.d15 values (1759194759001, 1.1, 1.1, 245)")

    def checkResult(self):
        tdSql.query(f"select last(ts) from {self.dbname}.{self.stbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1759194759001)

        tdSql.query(f"select last(ts), first(ts) from {self.dbname}.{self.stbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1759194759001)

        tdSql.query(f"select first(ts), last(ts) from {self.dbname}.{self.stbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1759194759001)

    def test_last_with_all_null_column_in_newest_block(self):
        """Agg: LAST with an all-NULL column

        1. Verify LAST retains the latest non-NULL value from an older block.
        2. Verify an all-NULL LAST input remains NULL with tag grouping.
        3. Verify the same result with a value filter that preloads block SMA.

        Catalog:
            - Function:Aggregate

        Since: v3.4.1.13

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-19 wpan Add all-NULL LAST regression coverage

        """
        dbname = "test_last_sma_null"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} duration 1 keep 3650 cachemodel 'none'")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create table stb (ts timestamp, c_later int, c_all_null int, c_value int) tags (grp int)")
        tdSql.execute("create table ct0 using stb tags (1)")

        tdSql.execute("insert into ct0 values (1735689600000, 7, null, 1)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.execute("insert into ct0 values (1735862400000, null, null, 2) (1735862400001, null, null, 3)")
        tdSql.execute(f"flush database {dbname}")

        tdSql.query(
            "select grp, last(ts), last(c_later), last(c_all_null), last(c_value) "
            "from stb where ts >= 1735689600000 and ts < 1735862400002 group by grp"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1735862400001)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, 3)

        tdSql.query(
            "select grp, last(ts), last(c_later), last(c_all_null), last(c_value) "
            "from stb where ts >= 1735689600000 and ts < 1735862400002 and c_value > 0 group by grp"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1735862400001)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, 3)

    def test_interval_max_and_last_with_historical_added_null_column(self):
        """Agg: interval MAX and LAST with an added all-NULL column

        1. Verify interval MAX reads correct values and NULLs for historical blocks after a column is added.
        2. Verify a block written after the ALTER retains its non-NULL added-column values.
        3. Verify LAST and mixed MAX/MIN aggregation on existing columns keep their result semantics.

        Catalog:
            - Function:Aggregate

        Since: v3.4.1.13

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-20 wpan Add interval MAX block SMA regression coverage

        """
        dbname = "test_interval_max_sma_null"
        start_ts = 1735689600000
        values = tuple((idx * 17) % 113 for idx in range(600))
        added_values = tuple(1000 + idx for idx in range(200))
        added_column_values = tuple(2000 + idx for idx in range(200))

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} minrows 200 maxrows 200 duration 1 keep 3650 cachemodel 'none'")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create table stb (ts timestamp, c_value int) tags (grp int)")
        tdSql.execute("create table ct0 using stb tags (1)")

        rows = " ".join(f"({start_ts + idx * 1000}, {value})" for idx, value in enumerate(values))
        tdSql.execute(f"insert into ct0 values {rows}")
        tdSql.execute(f"flush database {dbname}")
        tdSql.execute("alter table stb add column c_all_null double")

        added_rows = " ".join(
            f"({start_ts + (600 + idx) * 1000}, {value}, {added_column_values[idx]})"
            for idx, value in enumerate(added_values)
        )
        tdSql.execute(f"insert into ct0 values {added_rows}")
        tdSql.execute(f"flush database {dbname}")

        tdSql.query("select last(c_value), last(c_all_null) from stb partition by grp")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, added_values[-1])
        tdSql.checkData(0, 1, added_column_values[-1])

        tdSql.query("select max(c_value), max(c_all_null) from stb partition by grp interval(200s)")
        tdSql.checkRows(4)
        for row in range(4):
            if row < 3:
                tdSql.checkData(row, 0, max(values[row * 200 : (row + 1) * 200]))
                tdSql.checkData(row, 1, None)
            else:
                tdSql.checkData(row, 0, max(added_values))
                tdSql.checkData(row, 1, max(added_column_values))

        # Each 200-row block crosses 90-second interval boundaries. The values
        # must come from data blocks, without a redundant SMA dynamic-prune read.
        all_values = values + added_values
        tdSql.query("select max(c_value), max(c_all_null) from stb partition by grp interval(90s)")
        tdSql.checkRows(9)
        for row in range(9):
            start = row * 90
            end = min(start + 90, len(all_values))
            tdSql.checkData(row, 0, max(all_values[start:end]))

            if end <= 600:
                tdSql.checkData(row, 1, None)
            else:
                added_start = max(start, 600) - 600
                added_end = end - 600
                tdSql.checkData(row, 1, max(added_column_values[added_start:added_end]))

        tdSql.query(
            "explain analyze verbose true select max(c_value), max(c_all_null) "
            "from stb partition by grp interval(90s)"
        )
        plan = "".join(
            str(tdSql.getData(row, col)).lower()
            for row in range(tdSql.queryRows)
            for col in range(tdSql.queryCols)
        )
        assert "sma_load_blocks=0" in plan, (
            "interval-crossing MAX blocks must not load SMA for dynamic pruning"
        )

        tdSql.query("select max(c_value), min(c_value) from stb partition by grp interval(200s)")
        tdSql.checkRows(4)
        for row in range(4):
            window_values = values[row * 200 : (row + 1) * 200] if row < 3 else added_values
            tdSql.checkData(row, 0, max(window_values))
            tdSql.checkData(row, 1, min(window_values))

        tdSql.query("select last(c_value), last(c_all_null) from stb partition by grp")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, added_values[-1])
        tdSql.checkData(0, 1, added_column_values[-1])

    def test_last_tag(self):
        """Agg: last/last_row with tag

        description: verify the behavior of selecting last/last_row with tag column outside.
                    For example: select last(ts), tag1, tag2 from stable group by tbname.
                    In this case, we should read cache data to get the tag column value.

        Since: ver-3.4.0.0

        Labels: last/last_row,tag,integration,functional

        Jira: TS-6146

        Catalog:
            - xxx:xxx

        History:
            - Tony Zhang, 2025/10/10, Created

        """
        tdSql.execute("create database test_last_tag cachemodel 'both' keep 3650;")
        tdSql.execute("use test_last_tag;")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags (tag1 int, tag2 float)")

        tdSql.execute("create table tb1 using stb tags (1, 1.1);")
        tdSql.execute("create table tb2 using stb tags (2, 2.2);")

        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:00', 0);")
        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:02', 2);")
        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:04', 4);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:01', 1);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:03', 3);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:05', null);")

        tdCom.compare_testcase_result(
            "cases/11-Functions/resource/in/last_tag.in",
            "cases/11-Functions/resource/ans/last_tag.csv",
            "test_last_tag")

    def test_last_pk(self):
        """Agg-basic: last with pk

        Test the LAST function with composite key outside.
        For example: select last(ts), pk from stb group by tbname.

        Catalog:
            - Function:Aggregate

        Since: v3.4.0.0

        Labels: composite key,last/last_row,integration,functional

        Jira: TD-38004

        History:
            - Tony zhang, 2025/10/10, created

        """
        tdSql.execute("create database if not exists test_last_pk cachemodel 'both' keep 3650")
        tdSql.execute("use test_last_pk")
        tdSql.execute("create table stb (ts timestamp,a int COMPOSITE key,b int,c int) tags(ta int,tb int,tc int)")
        tdSql.execute("create table aaat1 using stb tags(1,1,1)")
        tdSql.execute("create table bbbt2 using stb tags(2,2,2)")
        tdSql.execute("insert into aaat1 values('2024-06-05 11:00:00',1,2,3)")
        tdSql.execute("insert into aaat1 values('2024-06-05 12:00:00',2,2,3)")
        tdSql.execute("insert into bbbt2 values('2024-06-05 13:00:00',3,2,3)")
        tdSql.execute("insert into bbbt2 values('2024-06-05 14:00:00',4,2,3)")

        tdCom.compare_testcase_result(
            "cases/11-Functions/resource/in/last_pk.in",
            "cases/11-Functions/resource/ans/last_pk.csv",
            "test_last_pk"
        )
