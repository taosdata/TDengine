import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
from util.common import tdCom

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        
    def initDB(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        
    def stopTest(self):
        tdSql.execute("drop database if exists db")

    def like_wildcard_test(self):
        tdSql.execute("create table db.t1x (ts timestamp, c1 varchar(100))")
        tdSql.execute("create table db.t_1x (ts timestamp, c1 varchar(100))")

        tdSql.query("select * from information_schema.ins_columns where table_name like '%1x'")
        tdSql.checkRows(4)
        
        tdSql.query("select * from information_schema.ins_columns where table_name like '%\_1x'")
        tdSql.checkRows(2)


        tdSql.query("insert into db.t1x values(now, 'abc'), (now+1s, 'a%c'),(now+2s, 'a_c'),(now+3s, '_c'),(now+4s, '%c')")
        
        tdSql.query("select * from db.t1x")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '%_c'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '%__c'")
        tdSql.checkRows(3)
        
        tdSql.query("select * from db.t1x where c1 like '%\_c'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t1x where c1 like '%\%c'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t1x where c1 like '_\%c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a%c")
        
        tdSql.query("select * from db.t1x where c1 like '_\_c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a_c")
        
        tdSql.query("select * from db.t1x where c1 like '%%_c'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '%_%c'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '__%c'")
        tdSql.checkRows(3)
        
        tdSql.query("select * from db.t1x where c1 not like '__%c'")
        tdSql.checkRows(2)
       
    def like_cnc_wildcard_test(self): 
        tdSql.execute("create table db.t3x (ts timestamp, c1 varchar(100))")
        
        tdSql.execute("insert into db.t3x values(now, '我是中文'), (now+1s, '我是_中文'), (now+2s, '我是%中文'), (now+3s, '%中文'),(now+4s, '_中文')")
        tdSql.query("select * from db.t3x")
        tdSql.checkRows(5)
          
        tdSql.query("select * from db.t3x where c1 like '%中文'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t3x where c1 like '%中_文'")
        tdSql.checkRows(0)
        
        tdSql.query("select * from db.t3x where c1 like '%\%中文'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t3x where c1 like '%\_中文'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t3x where c1 like '_中文'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t3x where c1 like '\_中文'")
        tdSql.checkRows(1)
        
    def like_multi_wildcard_test(self): 
        tdSql.execute("create table db.t4x (ts timestamp, c1 varchar(100))")

        # 插入测试数据
        tdSql.execute("insert into db.t4x values(now, 'abc'), (now+1s, 'a%c'),(now+2s, 'a_c'),(now+3s, '_c'),(now+4s, '%c')")
        tdSql.execute("insert into db.t4x values(now+5s, '%%%c'),(now+6s, '___c'),(now+7s, '%_%c'),(now+8s, '%\\c')")

        tdSql.query("select * from db.t4x where c1 like '%%%_'")
        tdSql.checkRows(9)

        tdSql.query("select * from db.t4x where c1 like '\%\%\%_'")
        tdSql.checkRows(1)
        
        tdSql.query("select * from db.t4x where c1 like '%\_%%'")
        tdSql.checkRows(4)

        tdSql.query("select * from db.t4x where c1 like '_\%\%'")
        tdSql.checkRows(0)
        
        tdSql.query("select * from db.t4x where c1 like '%abc%'")
        tdSql.checkRows(1)
        
        tdSql.query("select * from db.t4x where c1 like '_%abc%'")
        tdSql.checkRows(0)
        
        tdSql.query("select * from db.t4x where c1 like '\%%\%%'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t4x where c1 like '\%\_%\%%'")
        tdSql.checkRows(1)
        
    def like_wildcard_test2(self):
        tdSql.execute("create table db.t5x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t5x values(now(), 'a\%c')")
        tdSql.execute("insert into db.t5x values(now+1s, 'a\%bbbc')")
        tdSql.execute("insert into db.t5x values(now()+2s, 'a%c')")

        tdSql.query("select * from db.t5x where c1 like 'a\%c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a%c")

        tdSql.execute("create table db.t6x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t6x values(now(), '\%c')")
        tdSql.execute("insert into db.t6x values(now+1s, '\%bbbc')")
        tdSql.execute("insert into db.t6x values(now()+2s, '%c')")

        tdSql.query("select * from db.t6x where c1 like '\%c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "%c")

        tdSql.execute("create table db.t7x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t7x values(now(), 'a\_c')")
        tdSql.execute("insert into db.t7x values(now+1s, 'a\_bbbc')")
        tdSql.execute("insert into db.t7x values(now()+2s, 'a_c')")

        tdSql.query("select * from db.t7x where c1 like 'a\_c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a_c")

        tdSql.execute("create table db.t8x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t8x values(now(), '\_c')")
        tdSql.execute("insert into db.t8x values(now+1s, '\_bbbc')")
        tdSql.execute("insert into db.t8x values(now()+2s, '_c')")

        tdSql.query("select * from db.t8x where c1 like '\_c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "_c")

    def run(self):
        tdLog.printNoPrefix("==========start like_wildcard_test run ...............")
        tdSql.prepare(replica = self.replicaVar)


        self.initDB()
        self.like_wildcard_test()
        self.like_cnc_wildcard_test()
        self.like_multi_wildcard_test()
        self.like_wildcard_test2()
        tdLog.printNoPrefix("==========end like_wildcard_test run ...............")
        
        self.stopTest()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
