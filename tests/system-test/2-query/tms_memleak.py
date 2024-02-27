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
        tdSql.execute(f"use tms_memleak")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tms_memleak")
        tdSql.execute("create database if not exists tms_memleak")
        tdSql.execute('use tms_memleak')

        tdSql.execute('create table st(ts timestamp, f int) tags (t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 1)('2021-04-19 00:00:02', 2)('2021-04-19 00:00:03', 3)('2021-04-19 00:00:04', 4)")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-20 00:00:01', 5)('2021-04-20 00:00:02', 6)('2021-04-20 00:00:03', 7)('2021-04-20 00:00:04', 8)")

        tdSql.execute("insert into ct3 using st tags(3) values('2021-04-21 00:00:01', 5)('2021-04-21 00:00:02', 6)('2021-04-21 00:00:03', 7)('2021-04-21 00:00:04', 8)")

        tdSql.execute("insert into ct4 using st tags(4) values('2021-04-22 00:00:01', 5)('2021-04-22 00:00:02', 6)('2021-04-22 00:00:03', 7)('2021-04-22 00:00:04', 8)")

        tdSql.query("select * from st order by ts  limit 1 ");
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1);

        tdSql.execute('drop database tms_memleak')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
