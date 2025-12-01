from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import random
from random import randint
import os
import time
import platform

class TestDatabasePrecisionUs:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")


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

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/date.sim

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
    def getColsValue(self, i, j):
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
    def insertData(self):
        tdLog.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValue(i, j)
                else:
                    values += "," + self.getColsValue(i, j)

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


    # prepareEnv
    def prepareEnv(self):
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
        self.insertData()

    # check data correct
    def checkExpect(self, sql, expectVal):
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
    def checkTimeMacro(self):
        # 2 week
        val = 2
        usval = -val*7*24*60*60*1000*1000
        expectVal = self.childCnt * self.childRow
        sql = f"select count(ts) from st where timediff(ts - {val}w, ts1) = {usval} "
        self.checkExpect(sql, expectVal)

        # 20 day
        val = 20
        usval = -val*24*60*60*1000*1000
        uint = "d"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect(sql, expectVal)

        # 30 hour
        val = 30
        usval = -val*60*60*1000*1000
        uint = "h"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect(sql, expectVal)

        # 90 minutes
        val = 90
        usval = -val*60*1000*1000
        uint = "m"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect(sql, expectVal)
        # 2s
        val = 2
        usval = -val*1000*1000
        uint = "s"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect(sql, expectVal)
        # 20a
        val = 20
        usval = -val*1000
        uint = "a"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect(sql, expectVal)
        # 300u
        val = 300
        usval = -val*1
        uint = "u"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {usval} "
        self.checkExpect(sql, expectVal)

        # timetruncate check
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

    # init
    def setup_class(cls):
        seed = time.time() % 10000
        random.seed(seed)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql), True)

    # where
    def checkWhere(self):
        cnt = 300
        start = self.ts - cnt
        sql = f"select count(ts) from st where ts >= {start} and ts <= {self.ts}"
        self.checkExpect(sql, cnt)

        for i in range(50):
            cnt =  random.randint(1,40000)
            base = 2000
            start = self.ts - cnt - base
            end   = self.ts - base 
            sql = f"select count(ts) from st where ts >= {start} and ts < {end}"
            self.checkExpect(sql, cnt)

    # stream
    def checkStream(self):
        allRows = self.childCnt * self.childRow
        # ensure write data is expected
        sql = "select count(*) from (select diff(ts) as a from (select ts from st order by ts asc)) where a=1;"
        self.checkExpect(sql, allRows - 1)

        # stream count is ok
        sql =f"select count(*) from sta"
        cnt = int(allRows / 100) - 1 # last window is not close, so need reduce one
        self.checkExpect(sql, cnt)

        # check fields
        sql =f"select count(*) from sta where `count(ts)` != 100"
        self.checkExpect(sql, 0)

        # check timestamp
        sql =f"select count(*) from (select diff(`_wstart`) from sta)"
        self.checkExpect(sql, cnt - 1)
        sql =f"select count(*) from (select diff(`_wstart`) as a from sta) where a != 100"
        self.checkExpect(sql, 0)

    # run
    def check_precisionUS(self):
        # prepare env
        self.prepareEnv()

        # time macro like 1w 1d 1h 1m 1s 1a 1u
        self.checkTimeMacro()

        # check where
        self.checkWhere()


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

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_precisionUS.py
        """

        # case1
        self.check_database_precision_us()
        # case2
        self.check_precisionUS()