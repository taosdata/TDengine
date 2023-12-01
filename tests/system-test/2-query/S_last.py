import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
import random
import operator

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.tb_nums = 100
        self.row_nums = 10
        self.ts = 1434938400000
        self.ts_update = 1700038400000
        self.time_step = 1000

    def create_db(self, cache_value, dbname="test"):
        tdSql.execute(f"drop database if exists {dbname} ")
        tdLog.info(f"prepare database for cachemode:{cache_value} ")

        tdSql.execute(f"create database {dbname} cachemodel {cache_value} ")
        tdSql.execute(f"use {dbname} ")
        tdSql.execute(f"create stable {dbname}.stb (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint,\
             c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)")

    def insert_data(self, tbnums, rownums, time_step, dbname="test"):
        tdSql.execute(f"use {dbname} ")
        for tbnum in range(tbnums):
            tbname = f"{dbname}.t{tbnum}"
            tdSql.execute(f"create table  {tbname} using {dbname}.stb tags({tbnum}) ")

            ts = self.ts
            for row in range(rownums):
                ts = self.ts + time_step*row
                c1 = random.randint(0,10000)
                c2 = random.randint(0,100000)
                c3 = random.randint(0,125)
                c4 = random.randint(0,125)
                c5 = random.random()/1.0
                c6 = random.random()/1.0
                c7 = "'true'"
                c8 = "'binary_val'"
                c9 = "'nchar_val'"
                c10 = ts
                tdSql.execute(f" insert into   {tbname} values ({ts},{c1},{c2},{c3},{c4},{c5},{c6},{c7},{c8},{c9},{c10})")

    def update_data(self, tbnums, rownums, time_step, dbname="test"):
        tdSql.execute(f"use {dbname} ")
        for tbnum in range(tbnums):
            tbname = f"{dbname}.t{tbnum}"
            ts = self.ts
            for row in range(rownums):
                ts = self.ts + time_step*row
                c1 = random.randint(0,10000)
                c2 = random.randint(0,100000)
                c3 = random.randint(0,125)
                c4 = random.randint(0,125)
                c5 = random.random()/1.0
                c6 = random.random()/1.0
                c7 = "'true'"
                c8 = "'binary_val'"
                c9 = "'nchar_val'"
                c10 = ts
                tdSql.execute(f" insert into  {tbname} values ({ts},{c1},{c2},{c3},{c4},{c5},{c6},{c7},{c8},{c9},{c10})")


    def insert_disorder_data(self, tbnums, rownums, time_step, dbname="test"):
        tdSql.execute(f"use {dbname} ")
        for tbnum in range(tbnums):
            tbname = f"{dbname}.t{tbnum}"
            ts = self.ts
            for row in range(rownums):
                if row%2 == 0:
                    ts = self.ts + time_step*row
                    c1 = random.randint(0,10000)
                    c2 = random.randint(0,100000)
                    c3 = random.randint(0,125)
                    c4 = random.randint(0,125)
                    c5 = random.random()/1.0
                    c6 = random.random()/1.0
                    c7 = "'true'"
                    c8 = "'binary_val'"
                    c9 = "'nchar_val'"
                    c10 = ts
                    tdSql.execute(f" insert into   {tbname} values ({ts},{c1},{c2},{c3},{c4},{c5},{c6},{c7},{c8},{c9},{c10})")
            for row in range(rownums):
                if row%2 == 1:
                    ts = self.ts + time_step*row
                    c1 = random.randint(0,10000)
                    c2 = random.randint(0,100000)
                    c3 = random.randint(0,125)
                    c4 = random.randint(0,125)
                    c5 = random.random()/1.0
                    c6 = random.random()/1.0
                    c7 = "'true'"
                    c8 = "'binary_val'"
                    c9 = "'nchar_val'"
                    c10 = ts
                    tdSql.execute(f" insert into  {tbname} values ({ts},{c1},{c2},{c3},{c4},{c5},{c6},{c7},{c8},{c9},{c10})")

    def delete_data(self, rownums, time_step, origin_dbname="db_none",test_dbname="db_both"):
        ts = self.ts
        for row in range(rownums-1):
            ts = self.ts + time_step*(rownums - row -1 )
            tdSql.execute(f"use {origin_dbname} ")
            tdSql.execute(f" delete from   {origin_dbname}.t1 where ts={ts}")
            tdSql.execute(f"use {test_dbname} ")
            tdSql.execute(f" delete from  {test_dbname}.t1 where ts={ts}")
            tdLog.printNoPrefix(f"==========step4.{row}:test delete ts data ==============")
            if row % 4 == 0:
                tdSql.execute(f"flush database {origin_dbname} ")
                tdSql.execute(f"flush database {test_dbname} ")
            self.check_query()


    def check_result_ts(self, origin_query, test_query):
        origin_result = tdSql.getResult(origin_query)
        test_result = tdSql.getResult(test_query)
        if origin_result[0][0]  != None and test_result[0][0] != None:
            origin_ts=origin_result[0][0]
            test_ts=test_result[0][0]
            if origin_ts != test_ts:
                tdLog.notice(f"origin_ts is {origin_ts}, test_ts is {test_ts}")
                tdLog.exit(f"test_query value has not as expected , origin sql is {origin_query}, test_query is {test_query}")
            elif origin_ts == test_ts:
                tdLog.info(f"test_query value check pass , it work as expected ,origin sql is {origin_query}, test_query is {test_query}"  )
        else:
            tdLog.notice("query is None")
            
    def check_result_all(self, origin_query, test_query):
        origin_result = tdSql.getResult(origin_query)
        test_result = tdSql.getResult(test_query)
        # print(origin_result,test_result)
        if operator.eq(origin_result,test_result) == False:
            tdLog.notice(f"origin_ts is {origin_result}, test_ts is {test_result}")
            tdLog.exit(f"test_query value has not as expected , origin sql is {origin_query}, test_query is {test_query}")
        elif operator.eq(origin_result,test_result) == True:
            tdLog.info(f"test_query value check pass , it work as expected ,origin sql is {origin_query}, test_query is {test_query}"  )
            
    def check_query(self):
        #check last/last_row in none and both cache;
        self.check_result_ts( f"select last(*) from {self.dbname_none}.t1" , f"select last(*) from  {self.dbname_both}.t1" )
        self.check_result_ts( f"select last_row(*) from {self.dbname_none}.t1" , f"select last_row(*) from  {self.dbname_both}.t1" )

        #check last(ts) and last(*) in  'both' cache;
        self.check_result_ts( f"select last(ts) from {self.dbname_both}.t1" , f"select last(*) from  {self.dbname_both}.t1" )
        #check last_row(ts) and last_row(*) in  'both' cache;
        self.check_result_ts( f"select last_row(ts) from {self.dbname_both}.t1" , f"select last_row(*) from  {self.dbname_both}.t1" )

        #check last and last_row in  'both' cache;
        self.check_result_all( f"select last(*) from {self.dbname_both}.t1" , f"select last_row(*) from  {self.dbname_both}.t1" )

        #check last and order by ts limit 1 in  'both' cache;
        self.check_result_all( f"select last(*) from {self.dbname_both}.t1" , f"select * from  {self.dbname_both}.t1 order by ts desc limit 1 " )


    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        # tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create db and insert data ==============")
        self.dbname_none="db_none"
        self.dbname_both="db_both"
        # cache_last:none
        self,   self.create_db("'none'",self.dbname_none)
        self.insert_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_none)
        # cache_last:both
        self.create_db("'both'",self.dbname_both)
        self.insert_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_both)
        tdLog.printNoPrefix("==========step2:check init last/last_row result==============")
        self.check_query()
        # update data

        tdLog.printNoPrefix("==========step3:check init last/last_row result when update data==============")
        self.check_query()
        self.update_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_both)
        self.update_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_none)
        self.check_query()

        # disorder data
        tdLog.printNoPrefix("==========step4:check  last/last_row result when disorder data==============")
        self.insert_disorder_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_none)
        self.insert_disorder_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_both)
        self.check_query()


        # delete data
        tdLog.printNoPrefix("==========step5:check init last/last_row result when delete data==============")
        self.delete_data(self.row_nums,self.time_step,self.dbname_none,self.dbname_both)


        # insert data twice
        tdLog.printNoPrefix("==========step6:check init last/last_row result when update data twice==============")
        self.update_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_both)
        self.update_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_none)
        tdSql.execute(f"alter database {self.dbname_none} cachemodel 'both' ")
        tdSql.execute(f"alter database {self.dbname_both} cachemodel 'none' ")
        time.sleep(4)
        self.check_query()

        # disorder data
        tdLog.printNoPrefix("==========step7:check  last/last_row result when disorder data==============")
        self.insert_disorder_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_none)
        self.insert_disorder_data(self.tb_nums,self.row_nums,self.time_step,self.dbname_both)
        time.sleep(4)
        self.check_query()


        # delete data
        tdLog.printNoPrefix("==========step8:check init last/last_row result when delete data==============")
        self.delete_data(self.row_nums,self.time_step,self.dbname_none,self.dbname_both)



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
