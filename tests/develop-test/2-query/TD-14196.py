import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-13946]core dump of sampling binary column so that when result from  vnode exceeds INT16_MAX bytes
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists td14196")
        tdSql.execute("create database td14196")
        tdSql.execute("use td14196")

        tdSql.execute("create table st ( ts timestamp, dint int) tags (t1 int)")
        tdSql.execute("create table ct1 using st tags(1)")

        tdSql.execute("insert into ct1 values(now, 100)")

        tdSql.query("select last(dint)/100 from ct1")
        tdSql.checkData(0, 0, 1)
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

