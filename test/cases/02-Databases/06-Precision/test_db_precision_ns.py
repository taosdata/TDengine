import random
import time
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabasePrecisionNs:

    def setup_class(cls):
        seed = time.time() % 10000 
        random.seed(seed)        
        tdLog.debug(f"start to execute {__file__}")

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
        nsval = -val*7*24*60*60*1000*1000*1000
        expectVal = self.childCnt * self.childRow
        sql = f"select count(ts) from st where timediff(ts - {val}w, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)

        # 20 day
        val = 20
        nsval = -val*24*60*60*1000*1000*1000
        uint = "d"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)

        # 30 hour
        val = 30
        nsval = -val*60*60*1000*1000*1000
        uint = "h"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)

        # 90 minutes
        val = 90
        nsval = -val*60*1000*1000*1000
        uint = "m"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 2s
        val = 2
        nsval = -val*1000*1000*1000
        uint = "s"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 20a
        val = 5
        nsval = -val*1000*1000
        uint = "a"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 300u
        val = 300
        nsval = -val*1000
        uint = "u"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 8b
        val = 8
        sql = f"select timediff(ts1, ts - {val}b) from st "
        self.checkExpect(sql, val)

        # timetruncate check
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
    def check_precisionNS(self):

        # prepare env
        self.prepareEnv()

        # time macro like 1w 1d 1h 1m 1s 1a 1u 1b 
        self.checkTimeMacro()

        # check where
        self.checkWhere()


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

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/precision_ns.sim
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_precisionNS.py

        """

        # case1
        self.check_window_ns()

        # case2
        self.check_precisionNS()


