# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import taos
import sys
import time
import socket
import os
import threading
from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql
import random
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from random import randint


class TestDatabasePrecision:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")

    hostname = socket.gethostname()


    #
    # ------------------- test_TS_3311.py ----------------
    #
    def create_tables(self):
        tdSql.execute("create database if not exists dbus precision 'us'")
        tdSql.execute("create database if not exists dbns precision 'ns'")

        tdSql.execute("use dbus")

        tdSql.execute(f"CREATE STABLE `stb_us` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_us` USING `stb_us` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_us` USING `stb_us` (`t1`) TAGS (2)")

        tdSql.execute("use dbns")

        tdSql.execute(f"CREATE STABLE `stb_ns` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_ns` USING `stb_ns` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_ns` USING `stb_ns` (`t1`) TAGS (2)")


    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"INSERT INTO `dbus`.`ctb1_us` VALUES ('2023-07-01 00:00:00.000', 10.30000, 100)")
        tdSql.execute(f"INSERT INTO `dbus`.`ctb2_us` VALUES ('2023-08-01 00:00:00.000', 20.30000, 200)")

        tdSql.execute(f"INSERT INTO `dbns`.`ctb1_ns` VALUES ('2023-07-01 00:00:00.000', 10.30000, 100)")
        tdSql.execute(f"INSERT INTO `dbns`.`ctb2_ns` VALUES ('2023-08-01 00:00:00.000', 20.30000, 200)")

        tdLog.debug("insert data ............ [OK]")


    def do_ts_3311(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3311")

        # test ns
        tdSql.query(f"select _wstart, _wend, count(*) from `dbns`.`stb_ns` interval(1n)")
        tdSql.checkRows(2)

        tdSql.checkData(0,  0, '2023-07-01 00:00:00.000000000')
        tdSql.checkData(1,  0, '2023-08-01 00:00:00.000000000')

        tdSql.checkData(0,  1, '2023-08-01 00:00:00.000000000')
        tdSql.checkData(1,  1, '2023-09-01 00:00:00.000000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbns`.`stb_ns` interval(12n)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbns`.`stb_ns` interval(1y)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000000')


        ## test us
        tdSql.query(f"select _wstart, _wend, count(*) from `dbus`.`stb_us` interval(1n)")
        tdSql.checkRows(2)

        tdSql.checkData(0,  0, '2023-07-01 00:00:00.000000')
        tdSql.checkData(1,  0, '2023-08-01 00:00:00.000000')

        tdSql.checkData(0,  1, '2023-08-01 00:00:00.000000')
        tdSql.checkData(1,  1, '2023-09-01 00:00:00.000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbus`.`stb_us` interval(12n)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000')

        tdSql.query(f"select _wstart, _wend, count(*) from `dbus`.`stb_us` interval(1y)")
        tdSql.checkRows(1)

        tdSql.checkData(0,  0, '2023-01-01 00:00:00.000000')
        tdSql.checkData(0,  1, '2024-01-01 00:00:00.000000')

        # Cleanup from original stop method
        print("do TS-3311 ............................ [passed]")


    #
    # ------------------- main ----------------
    #
    def test_db_precision_bugs(self):
        """Precision bugs

        1. Verify bug TS-3311 (timestamp precision cause wrong window function result)
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_TS_3311.py
            - 2026-11-20 Alex Merged into 06-Precision/test_db_precision.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        self.do_ts_3311()


    #
    # --------------- test precisionNS ----------------
    #

    def check_window_ns(self):
        dbPrefix = "m_di_db_ns"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        ntPrefix = "m_di_nt"
        tbNum = 2
        rowNum = 200
        futureTs = 300000000000

        tdLog.info(f"=============== step1: create database and tables and insert data")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        nt = ntPrefix + str(i)

        tdSql.execute(f"create database {db} precision 'ns'")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < rowNum:
                cc = futureTs + x * 100 + 43
                ns = str(cc) + "b"
                tdSql.execute(f"insert into {tb} values (now + {ns} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.execute(f"create table {nt} (ts timestamp, tbcol int)")
        x = 0
        while x < rowNum:
            cc = futureTs + x * 100 + 43
            ns = str(cc) + "b"
            tdSql.execute(f"insert into {nt} values (now + {ns} , {x} )")
            x = x + 1

        tdLog.info(f"=============== step2: select count(*) from tables")
        i = 0
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(*) from {tb}")
        tdSql.checkData(0, 0, rowNum)

        i = 0
        mt = mtPrefix + str(i)
        tdSql.query(f"select count(*) from {mt}")

        mtRowNum = tbNum * rowNum
        tdSql.checkData(0, 0, mtRowNum)

        i = 0
        nt = ntPrefix + str(i)

        tdSql.query(f"select count(*) from {nt}")

        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3: check nano second timestamp")
        i = 0
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"insert into {tb} values (now-43b , {x} )")
        tdSql.query(f"select count(*) from {tb} where ts<now")
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"=============== step4: check interval/sliding nano second")
        i = 0
        mt = mtPrefix + str(i)
        tdSql.error(f"select count(*) from {mt} interval(1000b) sliding(100b)")
        tdSql.error(f"select count(*) from {mt} interval(10000000b) sliding(99999b)")

        tdSql.query(
            f"select count(*) from {mt} interval(100000000b) sliding(100000000b)"
        )

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)



    #
    # --------------- test precisionNS ----------------
    #

    # get col value and total max min ...
    def getColsValue_ns(self, i, j):
        # c1 value
        if random.randint(1, 10) == 5:
            c1 = None
        else:
            c1 = 1

        # c2 value
        if j % 3200 == 0:
            c2 = 8764231
        elif random.randint(1, 10) == 5:
            c2 = None
        else:
            c2 = random.randint(-87654297, 98765321)    


        value = f"({self.ts}, "

        # c1
        if c1 is None:
            value += "null,"
        else:
            self.c1Cnt += 1
            value += f"{c1},"
        # c2
        if c2 is None:
            value += "null,"
        else:
            value += f"{c2},"
            # total count
            self.c2Cnt += 1
            # max
            if self.c2Max is None:
                self.c2Max = c2
            else:
                if c2 > self.c2Max:
                    self.c2Max = c2
            # min
            if self.c2Min is None:
                self.c2Min = c2
            else:
                if c2 < self.c2Min:
                    self.c2Min = c2
            # sum
            if self.c2Sum is None:
                self.c2Sum = c2
            else:
                self.c2Sum += c2

        # c3 same with ts
        value += f"{self.ts})"
        
        # move next
        self.ts += 1

        return value


    # insert data
    def insertData_ns(self):
        tdLog.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValue_ns(i, j)
                else:
                    values += "," + self.getColsValue_ns(i, j)

                # batch insert    
                if j % self.batchSize == 0  and values != "":
                    sql = pre_insert + values
                    tdSql.execute(sql)
                    values = ""
            # append last
            if values != "":
                sql = pre_insert + values
                tdSql.execute(sql)
                values = ""

        sql = "flush database db;"
        tdLog.info(sql)
        tdSql.execute(sql)
        # insert finished
        tdLog.info(f"insert data successfully.\n"
        f"                            inserted child table = {self.childCnt}\n"
        f"                            inserted child rows  = {self.childRow}\n"
        f"                            total inserted rows  = {self.childCnt*self.childRow}\n")
        return



    # prepareEnv_ns
    def prepareEnv_ns(self):
        # init                
        self.ts = 1680000000000*1000*1000
        self.childCnt = 5
        self.childRow = 10000
        self.batchSize = 5000
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # create database  db
        sql = f"create database db vgroups 2 precision 'ns' "
        tdLog.info(sql)
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
            tdSql.execute(sql)

        # create stream
        #newstm if platform.system().lower() != 'windows':
        #newstm     sql = "create stream ma into sta as select count(ts) from st interval(100b)"
        #newstm     tdLog.info(sql)
        #newstm     tdSql.execute(sql)

        # insert data
        self.insertData_ns()


    # check data correct
    def checkExpect_ns(self, sql, expectVal):
        tdSql.query(sql)
        rowCnt = tdSql.getRows()
        for i in range(rowCnt):
            val = tdSql.getData(i,0)
            if val != expectVal:
                tdLog.exit(f"Not expect . query={val} expect={expectVal} i={i} sql={sql}")
                return False

        tdLog.info(f"check expect ok. sql={sql} expect ={expectVal} rowCnt={rowCnt}")
        return True

    

        

    # check time macro
    def checkTimeMacro_ns(self):
        # 2 week
        val = 2
        nsval = -val*7*24*60*60*1000*1000*1000
        expectVal = self.childCnt * self.childRow
        sql = f"select count(ts) from st where timediff(ts - {val}w, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)

        # 20 day
        val = 20
        nsval = -val*24*60*60*1000*1000*1000
        uint = "d"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)

        # 30 hour
        val = 30
        nsval = -val*60*60*1000*1000*1000
        uint = "h"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)

        # 90 minutes
        val = 90
        nsval = -val*60*1000*1000*1000
        uint = "m"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)
        # 2s
        val = 2
        nsval = -val*1000*1000*1000
        uint = "s"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)
        # 20a
        val = 5
        nsval = -val*1000*1000
        uint = "a"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)
        # 300u
        val = 300
        nsval = -val*1000
        uint = "u"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect_ns(sql, expectVal)
        # 8b
        val = 8
        sql = f"select timediff(ts1, ts - {val}b) from st "
        self.checkExpect_ns(sql, val)

        # timetruncate check
        tdSql.execute("set first_day_of_week 4")
        sql = '''select ts,timetruncate(ts,1u),
                          timetruncate(ts,1b),
                          timetruncate(ts,1m),
                          timetruncate(ts,1h),
                          timetruncate(ts,1w)
               from t0 order by ts desc limit 1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1, "2023-03-28 18:40:00.000009000")
        tdSql.checkData(0,2, "2023-03-28 18:40:00.000009999")
        tdSql.checkData(0,3, "2023-03-28 18:40:00.000000000")
        tdSql.checkData(0,4, "2023-03-28 18:00:00.000000000")
        tdSql.checkData(0,5, "2023-03-23 00:00:00.000000000")

        # timediff
        sql = '''select ts,timediff(ts,ts+1b,1b),
                          timediff(ts,ts+1u,1u),
                          timediff(ts,ts+1a,1a),
                          timediff(ts,ts+1s,1s),
                          timediff(ts,ts+1m,1m),
                          timediff(ts,ts+1h,1h),
                          timediff(ts,ts+1d,1d),
                          timediff(ts,ts+1w,1w)
               from t0 order by ts desc limit 1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1, -1)
        tdSql.checkData(0,2, -1)
        tdSql.checkData(0,3, -1)
        tdSql.checkData(0,4, -1)
        tdSql.checkData(0,5, -1)
        tdSql.checkData(0,6, -1)
        tdSql.checkData(0,7, -1)
        tdSql.checkData(0,8, -1)



    # where
    def checkWhere_ns(self):
        cnt = 300
        start = self.ts - cnt
        sql = f"select count(ts) from st where ts >= {start} and ts <= {self.ts}"
        self.checkExpect_ns(sql, cnt)

        for i in range(50):
            cnt =  random.randint(1,40000)
            base = 2000
            start = self.ts - cnt - base
            end   = self.ts - base 
            sql = f"select count(ts) from st where ts >= {start} and ts < {end}"
            self.checkExpect_ns(sql, cnt)


    # stream
    def checkStream_ns(self):
        allRows = self.childCnt * self.childRow
        # ensure write data is expected
        sql = "select count(*) from (select diff(ts) as a from (select ts from st order by ts asc)) where a=1;"
        self.checkExpect_ns(sql, allRows - 1)

        # stream count is ok
        sql =f"select count(*) from sta"
        cnt = int(allRows / 100) - 1 # last window is not close, so need reduce one
        self.checkExpect_ns(sql, cnt)

        # check fields
        sql =f"select count(*) from sta where `count(ts)` != 100"
        self.checkExpect_ns(sql, 0)

        # check timestamp
        sql =f"select count(*) from (select diff(`_wstart`) from sta)"
        self.checkExpect_ns(sql, cnt - 1)
        sql =f"select count(*) from (select diff(`_wstart`) as a from sta) where a != 100"
        self.checkExpect_ns(sql, 0)


    # run
    def check_precisionNS(self):

        # prepare env
        self.prepareEnv_ns()

        # time macro like 1w 1d 1h 1m 1s 1a 1u 1b 
        self.checkTimeMacro_ns()

        # check where
        self.checkWhere_ns()



    #
    # main
    #
    def test_database_precision_ns(self):
        """Precision: ns

        1. Create a nanosecond-precision database.
        2. Insert data using numeric timestamps.
        3. Verify the row count.
        4. Insert data using now().
        5. Filter data by timestamp.
        6. Validate INTERVAL … SLIDING queries.
        7. Validate time macro functions.
        8. Validate where clause with timestamp comparisons.

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/precision_ns.sim
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_precisionNS.py
            - 2026-11-20 Alex Merged into 06-Precision/test_db_precision.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        self._setup_ns()

        # case1
        self.check_window_ns()

        # case2
        self.check_precisionNS()



    #
    # --------------- case1 ----------------
    #

    def check_database_precision_us(self):
        """Precision: ms and us

        1. Millisecond-precision test
        2. Create a millisecond-precision database
        3. Insert both valid and invalid timestamps
        4. Query the data
        5. Microsecond-precision test
        6. Create a microsecond-precision database
        7. Repeat the same insert and query steps as above

        Catalog:
            - Database:Precision

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/date.sim
            - 2026-11-20 Alex Merged into 06-Precision/test_db_precision.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        i = 0
        dbPrefix = "lm_da_db"
        tbPrefix = "lm_da_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1 ms db")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")
        tdSql.execute(f"insert into {tb} values ('2017-01-01 08:00:00.001', 1)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2017-01-01 08:00:00.001")

        tdLog.info(f"=============== step2 ms db")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46cd .429', 2)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3 ms db")
        tdSql.error(f"insert into {tb} values ('1970-01-01 08:00:00.000', 3)")
        tdSql.error(f"insert into {tb} values ('1970-01-01 08:00:00.000', 3)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step4 ms db")
        tdSql.execute(f"insert into {tb} values(now, 4);")
        tdSql.execute(f"insert into {tb} values(now+1a, 5);")
        tdSql.execute(f"insert into {tb} values(now+1s, 6);")
        tdSql.execute(f"insert into {tb} values(now+1m, 7);")
        tdSql.execute(f"insert into {tb} values(now+1h, 8);")
        tdSql.execute(f"insert into {tb} values(now+1d, 9);")
        tdSql.execute(f"insert into {tb} values(now+3w, 10);")
        tdSql.execute(f"insert into {tb} values(31556995200000, 11);")
        tdSql.execute(f"insert into {tb} values('2970-01-01 00:00:00.000', 12);")

        tdSql.error(f"insert into {tb} values(now+1n, 20);")
        tdSql.error(f"insert into {tb} values(now+1y, 21);")
        tdSql.error(f"insert into {tb} values(31556995200001, 22);")
        tdSql.error(f"insert into {tb} values('2970-01-02 00:00:00.000', 23);")
        tdSql.error(f"insert into {tb} values(9223372036854775807, 24);")
        tdSql.error(f"insert into {tb} values(9223372036854775808, 25);")
        tdSql.error(f"insert into {tb} values(92233720368547758088, 26);")

        tdLog.info(f"=============== step5 ms db")
        tdSql.error(f"insert into {tb} values ('9999-12-31 213:59:59.999', 27)")
        tdSql.query(f"select ts from {tb}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step6 ms db")
        tdSql.error(f"insert into {tb} values ('9999-12-99 23:59:59.999', 28)")

        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(10)

        tdLog.info(f"=============== step7 ms db")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {tb} (ts timestamp, ts2 timestamp)")

        tdLog.info(f"=============== step8 ms db")
        tdSql.execute(f"insert into {tb} values (now, now)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step20 us db")
        tdSql.execute(f"create database {db} precision 'us' keep 365000d;")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")
        tdSql.execute(f"insert into {tb} values ('2017-01-01 08:00:00.001', 1)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2017-01-01 08:00:00.001")

        tdLog.info(f"=============== step21 us db")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46cd .429', 2)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step22 us db")
        tdSql.error(f"insert into {tb} values ('970-01-01 08:00:00.000', 3)")
        tdSql.error(f"insert into {tb} values ('970-01-01 08:00:00.000', 3)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step23 us db")
        tdSql.execute(f"insert into {tb} values(now, 4);")
        tdSql.execute(f"insert into {tb} values(now+1a, 5);")
        tdSql.execute(f"insert into {tb} values(now+1s, 6);")
        tdSql.execute(f"insert into {tb} values(now+1m, 7);")
        tdSql.execute(f"insert into {tb} values(now+1h, 8);")
        tdSql.execute(f"insert into {tb} values(now+1d, 9);")
        tdSql.execute(f"insert into {tb} values(now+3w, 10);")
        tdSql.execute(f"insert into {tb} values(31556995200000000, 11);")
        tdSql.execute(f"insert into {tb} values('2970-01-01 00:00:00.000000', 12);")

        tdSql.error(f"insert into {tb} values(now+1n, 20);")
        tdSql.error(f"insert into {tb} values(now+1y, 21);")
        tdSql.error(f"insert into {tb} values(31556995200000001, 22);")
        tdSql.error(f"insert into {tb} values('2970-01-02 00:00:00.000000', 23);")
        tdSql.error(f"insert into {tb} values(9223372036854775807, 24);")
        tdSql.error(f"insert into {tb} values(9223372036854775808, 25);")
        tdSql.error(f"insert into {tb} values(92233720368547758088, 26);")
        tdSql.error(f"insert into {tb} values ('9999-12-31 213:59:59.999', 27)")

        tdLog.info(f"=============== step24 us db")
        tdSql.query(f"select ts from {tb}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(10)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step30 ns db")
        tdSql.execute(f"create database {db} precision 'ns' keep 36500d;")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")
        tdSql.execute(f"insert into {tb} values ('2017-01-01 08:00:00.001', 1)")
        tdSql.query(f"select to_char(ts, 'yyyy-mm-dd hh24:mi:ss.ms') from {tb}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2017-01-01 08:00:00.001")

        tdLog.info(f"=============== step31 ns db")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.error(f"insert into {tb} values ('2017-08-28 00:23:46cd .429', 2)")
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step32 ns db")
        # sql_error insert into $tb values ('970-01-01 08:00:00.000000000', 3)
        # sql_error insert into $tb values ('970-01-01 08:00:00.000000000', 3)
        tdSql.query(f"select ts from {tb}")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step33 ns db")
        tdSql.execute(f"insert into {tb} values(now, 4);")
        tdSql.execute(f"insert into {tb} values(now+1a, 5);")
        tdSql.execute(f"insert into {tb} values(now+1s, 6);")
        tdSql.execute(f"insert into {tb} values(now+1m, 7);")
        tdSql.execute(f"insert into {tb} values(now+1h, 8);")
        tdSql.execute(f"insert into {tb} values(now+1d, 9);")
        tdSql.execute(f"insert into {tb} values(now+3w, 10);")
        tdSql.execute(f"insert into {tb} values(9214646400000000000, 11);")
        tdSql.execute(f"insert into {tb} values('2262-01-01 00:00:00.000000000', 12);")

        tdSql.error(f"insert into {tb} values(now+1n, 20);")
        tdSql.error(f"insert into {tb} values(now+1y, 21);")
        tdSql.error(f"insert into {tb} values(9214646400000000001, 22);")
        tdSql.error(f"insert into {tb} values('2262-01-02 00:00:00.000000000', 23);")
        tdSql.error(f"insert into {tb} values(9223372036854775807, 24);")
        tdSql.error(f"insert into {tb} values(9223372036854775808, 25);")
        tdSql.error(f"insert into {tb} values(92233720368547758088, 26);")
        tdSql.error(f"insert into {tb} values ('9999-12-31 213:59:59.999', 27)")

        tdLog.info(f"=============== step34 ns db")
        tdSql.query(f"select ts from {tb}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(10)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)


    #
    # --------------- case2 ----------------
    #

    # get col value and total max min ...
    def getColsValue_us(self, i, j):
        # c1 value
        if random.randint(1, 10) == 5:
            c1 = None
        else:
            c1 = 1

        # c2 value
        if j % 3200 == 0:
            c2 = 8764231
        elif random.randint(1, 10) == 5:
            c2 = None
        else:
            c2 = random.randint(-87654297, 98765321)    


        value = f"({self.ts}, "

        # c1
        if c1 is None:
            value += "null,"
        else:
            self.c1Cnt += 1
            value += f"{c1},"
        # c2
        if c2 is None:
            value += "null,"
        else:
            value += f"{c2},"
            # total count
            self.c2Cnt += 1
            # max
            if self.c2Max is None:
                self.c2Max = c2
            else:
                if c2 > self.c2Max:
                    self.c2Max = c2
            # min
            if self.c2Min is None:
                self.c2Min = c2
            else:
                if c2 < self.c2Min:
                    self.c2Min = c2
            # sum
            if self.c2Sum is None:
                self.c2Sum = c2
            else:
                self.c2Sum += c2

        # c3 same with ts
        value += f"{self.ts})"
        
        # move next
        self.ts += 1

        return value


    # insert data
    def insertData_us(self):
        tdLog.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValue_us(i, j)
                else:
                    values += "," + self.getColsValue_us(i, j)

                # batch insert    
                if j % self.batchSize == 0  and values != "":
                    sql = pre_insert + values
                    tdSql.execute(sql)
                    values = ""
            # append last
            if values != "":
                sql = pre_insert + values
                tdSql.execute(sql)
                values = ""

        sql = "flush database db;"
        tdLog.info(sql)
        tdSql.execute(sql)
        # insert finished
        tdLog.info(f"insert data successfully.\n"
        f"                            inserted child table = {self.childCnt}\n"
        f"                            inserted child rows  = {self.childRow}\n"
        f"                            total inserted rows  = {self.childCnt*self.childRow}\n")
        return



    # prepareEnv_us
    def prepareEnv_us(self):
        # init                
        self.ts = 1680000000000*1000
        self.childCnt = 5
        self.childRow = 10000
        self.batchSize = 5000
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # create database  db
        sql = f"create database db vgroups 2 precision 'us' "
        tdLog.info(sql)
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
            tdSql.execute(sql)

        # create stream
        #newstm if platform.system().lower() != 'windows':
        #newstm     sql = "create stream ma into sta as select count(ts) from st interval(100u)"
        #newstm     tdLog.info(sql)
        #newstm     tdSql.execute(sql)

        # insert data
        self.insertData_us()


    # check data correct
    def checkExpect_us(self, sql, expectVal):
        tdSql.query(sql)
        rowCnt = tdSql.getRows()
        for i in range(rowCnt):
            val = tdSql.getData(i,0)
            if val != expectVal:
                tdLog.exit(f"Not expect . query={val} expect={expectVal} i={i} sql={sql}")
                return False

        tdLog.info(f"check expect ok. sql={sql} expect ={expectVal} rowCnt={rowCnt}")
        return True



    # check time macro
    def checkTimeMacro_us(self):
        # 2 week
        val = 2
        usval = -val*7*24*60*60*1000*1000
        expectVal = self.childCnt * self.childRow
        sql = f"select count(ts) from st where timediff(ts - {val}w, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)

        # 20 day
        val = 20
        usval = -val*24*60*60*1000*1000
        uint = "d"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)

        # 30 hour
        val = 30
        usval = -val*60*60*1000*1000
        uint = "h"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)

        # 90 minutes
        val = 90
        usval = -val*60*1000*1000
        uint = "m"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)
        # 2s
        val = 2
        usval = -val*1000*1000
        uint = "s"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)
        # 20a
        val = 20
        usval = -val*1000
        uint = "a"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)
        # 300u
        val = 300
        usval = -val*1
        uint = "u"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect_us(sql, expectVal)

        # timetruncate check
        tdSql.execute("set first_day_of_week 4")
        sql = '''select ts,timetruncate(ts,1a),
                          timetruncate(ts,1s),
                          timetruncate(ts,1m),
                          timetruncate(ts,1h),
                          timetruncate(ts,1w)
                from t0 order by ts desc limit 1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1, "2023-03-28 18:40:00.009000")
        tdSql.checkData(0,2, "2023-03-28 18:40:00.000000")
        tdSql.checkData(0,3, "2023-03-28 18:40:00.000000")
        tdSql.checkData(0,4, "2023-03-28 18:00:00.000000")
        tdSql.checkData(0,5, "2023-03-23 00:00:00.000000")

        #tdSql.init(conn.cursor(), logSql), True)

    # where
    def checkWhere_us(self):
        cnt = 300
        start = self.ts - cnt
        sql = f"select count(ts) from st where ts >= {start} and ts <= {self.ts}"
        self.checkExpect_us(sql, cnt)

        for i in range(50):
            cnt =  random.randint(1,40000)
            base = 2000
            start = self.ts - cnt - base
            end   = self.ts - base 
            sql = f"select count(ts) from st where ts >= {start} and ts < {end}"
            self.checkExpect_us(sql, cnt)


    # stream
    def checkStream_us(self):
        allRows = self.childCnt * self.childRow
        # ensure write data is expected
        sql = "select count(*) from (select diff(ts) as a from (select ts from st order by ts asc)) where a=1;"
        self.checkExpect_us(sql, allRows - 1)

        # stream count is ok
        sql =f"select count(*) from sta"
        cnt = int(allRows / 100) - 1 # last window is not close, so need reduce one
        self.checkExpect_us(sql, cnt)

        # check fields
        sql =f"select count(*) from sta where `count(ts)` != 100"
        self.checkExpect_us(sql, 0)

        # check timestamp
        sql =f"select count(*) from (select diff(`_wstart`) from sta)"
        self.checkExpect_us(sql, cnt - 1)
        sql =f"select count(*) from (select diff(`_wstart`) as a from sta) where a != 100"
        self.checkExpect_us(sql, 0)


    # run
    def check_precisionUS(self):
        # prepare env
        self.prepareEnv_us()

        # time macro like 1w 1d 1h 1m 1s 1a 1u
        self.checkTimeMacro_us()

        # check where
        self.checkWhere_us()



    #
    # main 
    #
    def test_database_precision_us(self):
        """Precision: ms and us

        1. Millisecond-precision test
        2. Create a millisecond-precision database
        3. Insert both valid and invalid timestamps
        4. Query the data
        5. Microsecond-precision test
        6. Create a microsecond-precision database
        7. Repeat the same insert and query steps as above
        8. Validate time macro functions.
        9. Validate where clause with timestamp comparisons.

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_precisionUS.py
            - 2026-11-20 Alex Merged into 06-Precision/test_db_precision.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_us()

        # case1
        self.check_database_precision_us()
        # case2
        self.check_precisionUS()

    def _setup_ns(self):
        seed = time.time() % 10000 
        random.seed(seed)        
        tdLog.debug(f"start to execute {__file__}")

    def _setup_us(self):
        seed = time.time() % 10000
        random.seed(seed)
        tdLog.debug(f"start to excute {__file__}")
