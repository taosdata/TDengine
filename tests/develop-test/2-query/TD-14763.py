import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-14763]csum can not be used on state window
        '''
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn 

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists td14763")
        tdSql.execute("create database td14763")
        tdSql.execute("use td14763")

        tdSql.execute("create table st ( ts timestamp, i int, b bool) tags (t1 int)")
        tdSql.execute("create table ct1 using st tags(1)")
        tdSql.execute("create table ct2 using st tags(2)")

        for i in range(0, 4000, 2):
            b = True if i%3 == 0 else False
            tdSql.execute("insert into ct1 values(now + {}a, {}, {})".format(i, i, b))
            tdSql.execute("insert into ct2 values(now + {}a, {}, {})".format(i+1, i+1, b))

        tdSql.error("select csum(i) from  ct1 state_window(b)")
        tdSql.error("select csum(i) from (select * from st) state_window(b)")
        tdSql.query("select csum(i) from ct1");

        tdSql.execute('drop database td14763')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

