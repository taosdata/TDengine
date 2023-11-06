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
        tdSql.execute('create database dbvg vgroups 8;')

        tdSql.execute('use dbvg;')

        tdSql.execute('create table st(ts timestamp, f int) tags (t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 1)")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 2)")

        tdSql.execute("insert into ct3 using st tags(3) values('2021-04-19 00:00:03', 3)")

        tdSql.execute("insert into ct4 using st tags(4) values('2021-04-19 00:00:04', 4)")

        tdSql.query("select * from st where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from st where tbname='ct3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)

        tdSql.query("select * from st where tbname='ct3' and f=2")
        tdSql.checkRows(0)

        tdSql.query("select * from st where tbname='ct1' and tbname='ct4'")
        tdSql.checkRows(0)

        tdSql.query("select * from st where tbname='ct1' or tbname='ct4' order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)

        tdSql.query("select * from st where tbname='ct2' or tbname='ct3' order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query("select * from st where tbname='ct1' or tbname='ct4' or tbname='ct3' or tbname='ct2' order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)

        tdSql.query("select * from st where tbname='ct4' or 1=1 order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)

        tdSql.execute('drop database dbvg;')

        tdSql.execute('drop database tbname_vgroup')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
