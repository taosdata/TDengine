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
        tdSql.execute(f"use tbname_vgroup")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tbname_vgroup")
        tdSql.execute("create database if not exists tbname_vgroup")
        tdSql.execute('use tbname_vgroup')
        tdSql.execute('drop database if exists dbvg')
        tdSql.execute('create database dbvg vgroups 8;')

        tdSql.execute('use dbvg;')

        tdSql.execute('create table st(ts timestamp, f int) tags (t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 1)")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 2)")

        tdSql.execute("insert into ct3 using st tags(3) values('2021-04-19 00:00:03', 3)")

        tdSql.execute("insert into ct4 using st tags(4) values('2021-04-19 00:00:04', 4)")


        tdSql.execute('create table st2(ts timestamp, f int) tags (t int);')

        tdSql.execute("insert into ct21 using st2 tags(1) values('2021-04-19 00:00:01', 1)")

        tdSql.execute("insert into ct22 using st2 tags(2) values('2021-04-19 00:00:02', 2)")

        tdSql.execute("insert into ct23 using st2 tags(3) values('2021-04-19 00:00:03', 3)")

        tdSql.execute("insert into ct24 using st2 tags(4) values('2021-04-19 00:00:04', 4)")
        
        
        col_names = tdSql.getColNameList("compact database tbname_vgroup")
        if col_names[0] != "result":
           raise Exception("first column name of compact result shall be result")

        col_names = tdSql.getColNameList("show variables")
        if col_names[0] != "name":
           raise Exception("first column name of compact result shall be name")

        tdSql.execute('drop database tbname_vgroup')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
