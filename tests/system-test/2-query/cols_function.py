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
        

    def run(self):
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
