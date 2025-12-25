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

import time
from new_test_framework.utils import tdLog, tdSql
import random
class TestSmabasic:

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
        self.ts += 1

        # c1
        if c1 is None:
            value += "null,"
        else:
            self.c1Cnt += 1
            value += f"{c1},"
        # c2
        if c2 is None:
            value += "null)"
        else:
            value += f"{c2})"
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
        self.ts = 1600000000000
        self.childCnt = 5
        self.childRow = 1000000
        self.batchSize = 5000
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # change log level to info level, 
        # because debug level will affect performance
        sql = "alter all dnodes 'debugFlag' '131'"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create database  db
        sql = f"create database db vgroups 5 replica 3 stt_trigger 1"
        tdLog.info(sql)
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 int, c2 bigint) tags(area int)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
            tdSql.execute(sql)

        # insert data
        self.insertData()

    # query sql value
    def queryValue(self, sql):
        tdSql.query(sql)
        return tdSql.getData(0, 0)
    
    # sum
    def checkCorrentSum(self):
        # query count
        sql = "select sum(c1) from st"
        val = self.queryValue(sql)
        # c1Sum is equal c1Cnt
        if  val != self.c1Cnt:
            tdLog.exit(f"Sum Not Expect. expect={self.c1Cnt} query={val} sql:{sql}")
            return 
        
        # not
        sql1 = "select sum(c1) from st where c2 = 8764231"
        val1 = self.queryValue(sql1)
        sql2 = "select sum(c1) from st where c2 != 8764231"
        val2 = self.queryValue(sql2)
        sql3 = "select sum(c1) from st where c2 is null"
        val3 = self.queryValue(sql3)
        if  val != val1 + val2 + val3:
            tdLog.exit(f"Sum Not Equal. val != val1 + val2 + val3. val={val} val1={val1} val2={val2} val2={val3} sql1={sql1} sql2={sql2} sql2={sql3}")
            return 
        
        # over than
        sql1 = "select sum(c1) from st where c2 > 8000"
        val1 = self.queryValue(sql1)
        sql2 = "select sum(c1) from st where c2 <= 8000"
        val2 = self.queryValue(sql2)
        sql3 = "select sum(c1) from st where c2 is null"
        val3 = self.queryValue(sql3)
        if  val != val1 + val2 + val3:
            tdLog.exit(f"Sum Not Equal. val != val1 + val2 + val3. val={val} val1={val1} val2={val2} val2={val3} sql1={sql1} sql2={sql2} sql2={sql3}")
            return 
        
        tdLog.info(f"check correct sum on c1 successfully.")

    # check result
    def checkResult(self, fun, val, val1, val2, sql1, sql2):
        if fun == "count":
            if  val != val1 + val2:
                tdLog.exit(f"{fun} NOT SAME. val != val1 + val2. val={val} val1={val1} val2={val2} sql1={sql1} sql2={sql2}")
                return
        elif fun == "max":
            if val != max([val1, val2]):
                tdLog.exit(f"{fun} NOT SAME . val != max(val1 ,val2) val={val} val1={val1} val2={val2} sql1={sql1} sql2={sql2}")
                return
        elif fun == "min":
            if val != min([val1, val2]):
                tdLog.exit(f"{fun} NOT SAME . val != min(val1 ,val2) val={val} val1={val1} val2={val2} sql1={sql1} sql2={sql2}")
                return

    # sum
    def checkCorrentFun(self, fun, expectVal):
        # query
        sql = f"select {fun}(c2) from st"
        val = self.queryValue(sql)
        if  val != expectVal:
            tdLog.exit(f"{fun} Not Expect. expect={expectVal} query={val} sql:{sql}")
            return 
        
        # not
        sql1 = f"select {fun}(c2) from st where c2 = 8764231"
        val1 = self.queryValue(sql1)
        sql2 = f"select {fun}(c2) from st where c2 != 8764231"
        val2 = self.queryValue(sql2)
        self.checkResult(fun, val, val1, val2, sql1, sql2)
        
        # over than
        sql1 = f"select {fun}(c2) from st where c2 > 8000"
        val1 = self.queryValue(sql1)
        sql2 = f"select {fun}(c2) from st where c2 <= 8000"
        val2 = self.queryValue(sql2)
        self.checkResult(fun, val, val1, val2, sql1, sql2)

        # successful
        tdLog.info(f"check correct {fun} on c2 successfully.")

    # check query corrent
    def checkCorrect(self):
        # count
        self.checkCorrentFun("count", self.c2Cnt)
        # max
        self.checkCorrentFun("max", self.c2Max)
        # min
        self.checkCorrentFun("min", self.c2Min)
        # sum
        self.checkCorrentSum()

        # c2 sum
        sql = "select sum(c2) from st"
        val = self.queryValue(sql)
        # c1Sum is equal c1Cnt
        if  val != self.c2Sum:
            tdLog.exit(f"c2 Sum Not Expect. expect={self.c2Sum} query={val} sql:{sql}")
            return

    def checkPerformance(self):
        # have sma caculate
        sql1 = "select count(*) from st"
        stime = time.time()
        tdSql.execute(sql1, 1)
        spend1 = time.time() - stime

        # no sma caculate
        sql2 = "select count(*) from st where c2 != 8764231 or c2 is null"
        stime = time.time()
        tdSql.execute(sql2, 1)
        spend2 = time.time() - stime

        time1 = "%.2f"%(spend1*1000)
        time2 = "%.2f"%(spend2*1000)
        if spend2 < spend1 * 8:
            tdLog.exit(f"performance not passed! sma spend1={time1}ms no sma spend2= {time2}ms sql1={sql1} sql2= {sql2}")
            return 
        tdLog.info(f"performance passed! sma spend1={time1}ms no sma spend2= {time2}ms sql1={sql1} sql2= {sql2}")

    # init
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    #
    # ------------------- test_blockSMA.py ----------------
    #
    def do_blockSMA(self):
        dbname = "db"
        self.rowNum = 10000
        self.ts = 1537146000000
        tdSql.prepare(dbname=dbname, drop=True, stt_trigger=1)

        tdSql.execute(f'''create table {dbname}.ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        sql = f"insert into {dbname}.ntb values"
        for i in range(self.rowNum):
            sql += f"(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"\
                        % (self.ts + i, i % 127 + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i % 255 + 1, i + 1, i + 1, i + 1)
            if i % 2100 == 0 and i != 0:
                tdSql.execute(sql)
                sql = f"insert into {dbname}.ntb values"
        # insert last            
        tdSql.execute(sql)


        tdSql.execute('flush database db')

        # test functions using sma result
        tdSql.query(f"select count(col1),min(col1),max(col1),avg(col1),sum(col1),spread(col1),percentile(col1, 0),first(col1),last(col1) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 127)
        tdSql.checkData(0, 3, 63.8449)
        tdSql.checkData(0, 4, 638449)
        tdSql.checkData(0, 5, 126.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 94)

        tdSql.query(f"select count(col2),min(col2),max(col2),avg(col2),sum(col2),spread(col2),percentile(col2, 0),first(col2),last(col2) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col3),min(col3),max(col3),avg(col3),sum(col3),spread(col3),percentile(col3, 0),first(col3),last(col3) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col4),min(col4),max(col4),avg(col4),sum(col4),spread(col4),percentile(col4, 0),first(col4),last(col4) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col5),min(col5),max(col5),avg(col5),sum(col5),spread(col5),percentile(col5, 0),first(col5),last(col5) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 0.1)
        tdSql.checkData(0, 2, 9999.09961)
        tdSql.checkData(0, 3, 4999.599985846)
        tdSql.checkData(0, 4, 49995999.858455874)
        tdSql.checkData(0, 5, 9998.999609374)
        tdSql.checkData(0, 6, 0.100000001)
        tdSql.checkData(0, 7, 0.1)
        tdSql.checkData(0, 8, 9999.09961)

        tdSql.query(f"select count(col6),min(col6),max(col6),avg(col6),sum(col6),spread(col6),percentile(col6, 0),first(col6),last(col6) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 0.1)
        tdSql.checkData(0, 2, 9999.100000000)
        tdSql.checkData(0, 3, 4999.600000001)
        tdSql.checkData(0, 4, 49996000.000005305)
        tdSql.checkData(0, 5, 9999.000000000)
        tdSql.checkData(0, 6, 0.1)
        tdSql.checkData(0, 7, 0.1)
        tdSql.checkData(0, 8, 9999.1)

        tdSql.query(f"select count(col11),min(col11),max(col11),avg(col11),sum(col11),spread(col11),percentile(col11, 0),first(col11),last(col11) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 255)
        tdSql.checkData(0, 3, 127.45)
        tdSql.checkData(0, 4, 1274500)
        tdSql.checkData(0, 5, 254.000000000)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 55)

        tdSql.query(f"select count(col12),min(col12),max(col12),avg(col12),sum(col12),spread(col12),percentile(col12, 0),first(col12),last(col12) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col13),min(col13),max(col13),avg(col13),sum(col13),spread(col13),percentile(col13, 0),first(col13),last(col13) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

        tdSql.query(f"select count(col14),min(col14),max(col14),avg(col14),sum(col14),spread(col14),percentile(col14, 0),first(col14),last(col14) from {dbname}.ntb")
        tdSql.checkData(0, 0, 10000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 10000)
        tdSql.checkData(0, 3, 5000.5)
        tdSql.checkData(0, 4, 50005000)
        tdSql.checkData(0, 5, 9999.0)
        tdSql.checkData(0, 6, 1.0)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 10000)

    #
    # ------------------- main ----------------
    #
    def test_sma_basic(self):
        """Sma basic
        
        1. Create 1 super table and 5 child tables
        2. Insert 1 million rows data into each child table
        3. Put special number value (8764231) on c2 column interval 3200 rows
        4. Query and check the result of count/max/min/sum on c1/c2 column with different where condition
        5. Query count(*) where c2 != specail number(8764231) as no using sma
        6. Query count(*) no where as using sma
        7. Expect the performance step5 < step6 * 8
        8. Query each function(count/max/min/avg/sum/spread/percentile/first/last) on all columns of the table ntb using sma


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-20 Alex Duan Migrated from uncatalog/system-test/2-query/test_smaBasic.py
            - 2025-12-21 Alex Duan Migrated from uncatalog/system-test/2-query/test_blockSMA.py

        """

        # prepare env
        self.prepareEnv()

        # query 
        self.checkCorrect()

        # performance
        self.checkPerformance()
        
        self.do_blockSMA()
