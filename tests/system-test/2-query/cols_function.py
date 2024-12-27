import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.dbname = 'test'

    def create_test_data(self):
        tdLog.info("create test data")
        tdLog.info("taosBenchmark -y -t 10 -n 100  -b INT,FLOAT,NCHAR,BOOL")
        os.system("taosBenchmark -y -t 10 -n 100  -b INT,FLOAT,NCHAR,BOOL")
        
        tdSql.execute('use test')        
        tdSql.execute(f'Create table  {self.dbname}.normal_table (ts timestamp, c0 int, c1 float, c2 nchar(30), c3 bool)')
        tdSql.execute(f'insert into {self.dbname}.normal_table (select * from {self.dbname}.d0)')
        
    def one_cols_1output_test(self):
        tdLog.info("one_cols_1output_test")
        tdSql.query(f'select cols(last(ts), ts) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), ts) as t1 from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), ts as t1) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), c0) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), c1) from {self.dbname}.meters group by tbname')
        

        tdSql.query(f'select cols(last(ts+1), ts) as t1 from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts+1), ts+2 as t1) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts+1), c0+10) from {self.dbname}.meters')


    def one_cols_multi_output_test(self):
        tdLog.info("one_cols_1output_test")
        tdSql.query(f'select cols(last(ts), ts, c0) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), ts, c0) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), ts as time, c0 cc) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), c0, c1, c2, c3) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(c1), ts) from {self.dbname}.meters group by tbname')
        

        tdSql.query(f'select cols(max(c0), ts) from {self.dbname}.meters')
        tdSql.query(f'select cols(min(c1), ts, c0) from {self.dbname}.meters')
        
        tdSql.query(f'select cols(last(ts), ts, c0), count(1) from {self.dbname}.meters')
        tdSql.query(f'select count(1), cols(last(ts), ts, c0), min(c0) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), ts, c0), count(1) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), ts as time, c0 cc), count(1) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(ts), c0, c1, c2, c3), count(1) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(c1), ts), count(1) from {self.dbname}.meters group by tbname')
        

        tdSql.query(f'select cols(max(c0), ts), count(1) from {self.dbname}.meters')
        tdSql.query(f'select cols(min(c1), ts, c0), count(1) from {self.dbname}.meters')
        tdSql.query(f'select count(1), cols(max(c0), ts) from {self.dbname}.meters')
        tdSql.query(f'select max(c0), cols(max(c0), ts) from {self.dbname}.meters')
        tdSql.query(f'select max(c1), cols(max(c0), ts) from {self.dbname}.meters')



    def multi_cols_output_test(self):
        tdLog.info("multi_cols_output_test")
        tdSql.query(f'select cols(last(c0), ts, c1), cols(first(c0), ts, c1), count(1) from {self.dbname}.meters')
        tdSql.query(f'select cols(last(c0), ts as t1, c1 as c11), cols(first(c0), ts as c2, c1 c21), count(1) from {self.dbname}.meters')

    def funcSupperTableTest(self):
        tdSql.execute('create database if not exists db;')
        tdSql.execute('use db')
        tdSql.execute(f'drop table if exists db.st')
        
        tdSql.execute('create table db.st (ts timestamp, c0 int, c1 float, c2 nchar(30), c3 bool) tags (t1 nchar(30))')
        tdSql.execute('create table db.st_1 using db.st tags("st1")')
        tdSql.execute('create table db.st_2 using db.st tags("st1")')
        tdSql.execute('insert into db.st_1 values(1734574929000, 1, 1, "a1", true)')
        tdSql.execute('insert into db.st_1 values(1734574929001, 2, 2, "bbbbbbbbb1", false)')
        tdSql.execute('insert into db.st_1 values(1734574929002, 3, 3, "a2", true)')
        tdSql.execute('insert into db.st_1 values(1734574929003, 4, 4, "bbbbbbbbb2", false)')
        
        tdSql.query(f'select cols(last(c0), ts, c1, c2, c3), cols(first(c0), ts, c1, c2, c3) from db.st')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574929003)
        tdSql.checkData(0, 1, '4.0')
        tdSql.checkData(0, 2, 'bbbbbbbbb2')
        tdSql.checkData(0, 3, False)
        tdSql.checkData(0, 4, 1734574929000)
        tdSql.checkData(0, 5, '1.0')
        tdSql.checkData(0, 6, 'a1')
        tdSql.checkData(0, 7, True)
        
        #tdSql.execute(f'drop table if exists db.st')

    
    def funcNestTest(self):
        tdSql.execute('create database db;')
        tdSql.execute('use db')
        tdSql.execute(f'drop table if exists db.d1')
        
        tdSql.execute('create table db.d1 (ts timestamp, c0 int, c1 float, c2 nchar(30), c3 bool)')
        tdSql.execute('insert into db.d1 values(1734574929000, 1, 1.1, "a", true)')
        tdSql.execute('insert into db.d1 values(1734574930000, 2, 2.2, "bbbbbbbbb", false)')
        
        tdSql.query(f'select cols(last(c0), ts, c2), cols(first(c0), ts, c2) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(4)
        tdSql.checkData(0, 0, 1734574930000)
        tdSql.checkData(0, 1, 'bbbbbbbbb')
        tdSql.checkData(0, 2, 1734574929000)
        tdSql.checkData(0, 3, 'a')
        tdSql.query(f'select cols(last(c0), ts, c1, c2, c3), cols(first(c0), ts, c1, c2, c3) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(6)
        tdSql.checkData(0, 0, 1734574930000)
        tdSql.checkData(0, 1, 2.2)
        tdSql.checkData(0, 2, 'bbbbbbbbb')
        tdSql.checkData(0, 3, False)
        tdSql.checkData(0, 4, 1734574929000)
        tdSql.checkData(0, 5, 1.1)
        tdSql.checkData(0, 6, 'a')
        tdSql.checkData(0, 7, True)
        
        tdSql.query(f'select cols(last(ts), c1), cols(first(ts), c1) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(6)
        tdSql.checkData(0, 0, 2.2)
        tdSql.checkData(0, 1, 1.1)
        
        tdSql.query(f'select cols(first(ts), c0, c1), cols(first(ts), c0, c1) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1.1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.1)
        
        tdSql.query(f'select cols(first(ts), c0, c1), cols(first(ts+1), c0, c1) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1.1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.1)
        
        tdSql.query(f'select cols(first(ts), c0, c1), cols(first(ts), c0+1, c1+2) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1.1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 3.1)
        
        tdSql.query(f'select cols(first(c0), ts, length(c2)), cols(last(c0), ts, length(c2)) from db.d1')
        tdSql.checkRows(1)
        #tdSql.checkCols(6)
        tdSql.checkData(0, 0, 1734574929000)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 1734574930000)
        tdSql.checkData(0, 3, 36)
        tdSql.query(f'select cols(first(c0), ts, length(c2)), cols(last(c0), ts, length(c2) + 2) from db.d1')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574929000)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 1734574930000)
        tdSql.checkData(0, 3, 38)
        
        tdSql.query(f'select cols(first(c0), ts, c2), cols(last(c0), ts, length(c2) + 2) from db.d1')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574929000)
        tdSql.checkData(0, 1, 'a')
        tdSql.checkData(0, 2, 1734574930000)
        tdSql.checkData(0, 3, 38)
        
        tdSql.query(f'select cols(min(c0), ts, c2), cols(last(c0), ts, length(c2) + 2) from db.d1')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574929000)
        tdSql.checkData(0, 1, 'a')
        tdSql.checkData(0, 2, 1734574930000)
        tdSql.checkData(0, 3, 38)

        tdSql.query(f'select cols(min(c0), ts, c2), cols(first(c0), ts, length(c2) + 2) from db.d1')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1734574929000)
        tdSql.checkData(0, 1, 'a')
        tdSql.checkData(0, 2, 1734574929000)
        tdSql.checkData(0, 3, 6)
    
    
    def parse_test(self):
        tdLog.info("parse test")
        
        #** error sql  **#
        tdSql.error(f'select cols(ts) from {self.dbname}.meters group by tbname')
        tdSql.error(f'select cols(ts) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(ts)) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(ts, ts)) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(ts, ts), ts) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(last(ts), ts), ts) from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), ts as t1) as t1 from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), ts, c0) t1 from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), ts t1) tt from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), c0 cc0, c1 cc1) cc from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), c0 as cc0) as cc from {self.dbname}.meters')
        tdSql.error(f'select cols(ts) + 1 from {self.dbname}.meters group by tbname')
        tdSql.error(f'select last(cols(ts)+1) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(ts+1, ts)) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(ts, ts), ts+1) from {self.dbname}.meters')
        tdSql.error(f'select last(cols(last(ts+1), ts+1), ts) from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), ts+1 as t1) as t1 from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts+1), ts, c0) t1 from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), ts t1) tt from {self.dbname}.meters')
        tdSql.error(f'select cols(first(ts+1), c0+2 cc0, c1 cc1) cc from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts)+1, c0+2 as cc0) as cc from {self.dbname}.meters')
        
        tdSql.error(f'select cols(last(ts)+1, ts) from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts)+10, c1+10) from {self.dbname}.meters group by tbname')
        
        tdSql.error(f'select cols(cols(last(ts), c0), c0) as cc from {self.dbname}.meters')
        tdSql.error(f'select cols(last(ts), cols(last(ts), c0), c0) as cc from {self.dbname}.meters')
        

    def run(self):
        self.funcNestTest()
        self.funcSupperTableTest()
        return
        self.create_test_data()
        self.parse_test()
        self.one_cols_1output_test()
        self.one_cols_multi_output_test()
        self.multi_cols_output_test()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
