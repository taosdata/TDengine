import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-] 
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use pk_func_group")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists pk_func_group")
        tdSql.execute("create database if not exists pk_func_group")
        tdSql.execute('use pk_func_group')
        tdSql.execute('drop database IF EXISTS d1;')

        tdSql.execute('drop database IF EXISTS d2;')

        tdSql.execute('create database d1 vgroups 1')

        tdSql.execute('use d1;')

        tdSql.execute('create table st(ts timestamp, pk int primary key, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1, 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 2, 2);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', 3, 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', 4, 4);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 1, 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 4, 4);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 3, 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2, 2);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', 6, 6);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', 5, 5);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 8, 8);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 7, 7);")

        tdSql.query('select first(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d1.st partition by tbname order by tbname,ts,pk;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(5, 1, 3)
        tdSql.checkData(5, 2, 3)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(6, 1, 4)
        tdSql.checkData(6, 2, 4)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, 2)
        tdSql.checkData(7, 2, 2)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, 7)
        tdSql.checkData(8, 2, 7)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, 8)
        tdSql.checkData(9, 2, 8)

        tdSql.execute('create database d2 vgroups 2')

        tdSql.execute('use d2;')

        tdSql.execute('create table st(ts timestamp, pk int primary key, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1, 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 2, 2);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', 3, 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', 4, 4);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 1, 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 4, 4);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 3, 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2, 2);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', 6, 6);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', 5, 5);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 8, 8);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 7, 7);")

        tdSql.query('select first(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d1.st partition by tbname order by tbname,ts,pk;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(5, 1, 3)
        tdSql.checkData(5, 2, 3)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(6, 1, 4)
        tdSql.checkData(6, 2, 4)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, 2)
        tdSql.checkData(7, 2, 2)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, 7)
        tdSql.checkData(8, 2, 7)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, 8)
        tdSql.checkData(9, 2, 8)

        tdSql.execute('drop database pk_func_group')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
