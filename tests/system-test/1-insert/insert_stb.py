import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf
import taos

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TS-3932] insert into stb 
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn
        
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use insert_stb")


    def run_normal(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists insert_stb")
        tdSql.execute("create database if not exists insert_stb")
        tdSql.execute('use insert_stb')
        tdSql.execute('create database d1')

        tdSql.execute('create database d2')

        tdSql.execute('use d1;')

        tdSql.execute('create table st(ts timestamp, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")

        tdSql.execute("insert into ct1 values('2021-04-19 00:00:02', 2);")

        tdSql.execute('use d2;')

        tdSql.execute('create table st(ts timestamp, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")

        tdSql.execute('create database db1 vgroups 1;')

        tdSql.execute('create table db1.stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int);')

        tdSql.execute('use d1;')

        tdSql.execute("insert into st (tbname, ts, f, t) values('ct3', '2021-04-19 08:00:03', 3, 3);")

        tdSql.execute("insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:04', 6);")

        tdSql.execute("insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:05', 7)('ct8', '2021-04-19 08:00:06', 8);")

        tdSql.execute("insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:07', 9, 9)('ct8', '2021-04-19 08:00:08', 10, 10);")

        tdSql.execute("insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:09', 9, 9)('ct8', '2021-04-19 08:00:10', 10, 10) d2.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:11', 9, 9)('ct8', '2021-04-19 08:00:12', 10, 10);")

        tdSql.query('select * from d1.st order by ts;')
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 8, 0, 3))
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 8, 0, 4))
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 8, 0, 5))
        tdSql.checkData(5, 1, 7)
        tdSql.checkData(5, 2, None)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 8, 0, 6))
        tdSql.checkData(6, 1, 8)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 8, 0, 7))
        tdSql.checkData(7, 1, 9)
        tdSql.checkData(7, 2, None)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 8, 0, 8))
        tdSql.checkData(8, 1, 10)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 8, 0, 9))
        tdSql.checkData(9, 1, 9)
        tdSql.checkData(9, 2, None)
        tdSql.checkData(10, 0, datetime.datetime(2021, 4, 19, 8, 0, 10))
        tdSql.checkData(10, 1, 10)
        tdSql.checkData(10, 2, None)

        tdSql.query('select * from d2.st order by ts;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 8, 0, 11))
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(2, 2, 9)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 8, 0, 12))
        tdSql.checkData(3, 1, 10)
        tdSql.checkData(3, 2, 10)

        tdSql.execute("insert into d2.st(ts, f, tbname) values('2021-04-19 08:00:13', 1, 'ct1') d1.ct1 values('2021-04-19 08:00:14', 1);")

        tdSql.query('select * from d1.st order by ts;')
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 8, 0, 3))
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 8, 0, 4))
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 8, 0, 5))
        tdSql.checkData(5, 1, 7)
        tdSql.checkData(5, 2, None)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 8, 0, 6))
        tdSql.checkData(6, 1, 8)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 8, 0, 7))
        tdSql.checkData(7, 1, 9)
        tdSql.checkData(7, 2, None)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 8, 0, 8))
        tdSql.checkData(8, 1, 10)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 8, 0, 9))
        tdSql.checkData(9, 1, 9)
        tdSql.checkData(9, 2, None)
        tdSql.checkData(10, 0, datetime.datetime(2021, 4, 19, 8, 0, 10))
        tdSql.checkData(10, 1, 10)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 0, datetime.datetime(2021, 4, 19, 8, 0, 14))
        tdSql.checkData(11, 1, 1)
        tdSql.checkData(11, 2, 1)

        tdSql.query('select * from d2.st order by ts;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 8, 0, 11))
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(2, 2, 9)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 8, 0, 12))
        tdSql.checkData(3, 1, 10)
        tdSql.checkData(3, 2, 10)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 8, 0, 13))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, None)
    
    def run_stmt_error(self):
        conn = self.conn
        conn.select_db('insert_stb')
        conn.execute('create table stb9(ts timestamp, f int) tags (t int)')
        try:
            stmt = conn.statement("insert into stb9(tbname, f, t) values('ctb91', 1, ?)")
            params = taos.new_bind_params(1)
            params[0].int(1)
            stmt.bind_param(params)
            stmt.execute()
            result = stmt.use_result()
        except Exception as err:
            print(str(err))    
        
        
    def run(self):
        self.run_normal()
        self.run_stmt_error()
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
