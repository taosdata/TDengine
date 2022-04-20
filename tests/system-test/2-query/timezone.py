from tarfile import SYMTYPE
from time import sleep
from tkinter import E
import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):  # sourcery skip: extract-duplicate-method
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create normal table==========")
        tdSql.execute(
            '''create table if not exists normaltb
            (ts timestamp, c1 int, c2 float,c3 double)
            '''
        )
        tdLog.printNoPrefix("==========step2:insert data==========")
        tdSql.execute("insert into normaltb values(now,1,1.55,100.555555)")
        tdSql.query("select timezone() from normaltb")
        tdSql.checkRows(1)

        tdSql.execute("insert into normaltb values(now(),2,2.222,222.222222)")
        tdSql.query("select timezone() from normaltb")
        tdSql.checkRows(2)

        # tdSql.checkData(0,0,"Asia/Shanghai (CST, +0800)")
        # tdSql.checkData(1,0,"Asia/Shanghai (CST, +0800)")

        tdSql.execute("insert into normaltb values(today(),3,3.333,333.333333)")
        tdSql.query("select * from normaltb where ts=today()")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,3)
        # for i in range(0,50):
        #     tdSql.query("select timezone() from db.normaltb")
        #     tdSql.checkData(0,0,"Asia/Shanghai (CST, +0800)")
        #     i+=1
        #     sleep(0.5)
        
        tdSql.query("select today() from normaltb")
        tdSql.checkRows(3)
        tdSql.query("select today() from db.normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() +1w from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1w from normaltb")
        tdSql.checkRows(3) 

        tdSql.query("select now() +1d from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1d from normaltb")
        tdSql.checkRows(3) 

        tdSql.query("select now() +1h from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1h from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() +1m from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1m from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() +1s from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1s from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() +1a from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1a from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() +1u from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1u from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() +1b from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() +1b from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1w from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1w from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1d from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1d from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1h from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1h from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1m from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1m from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1s from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1s from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1a from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1a from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1u from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1u from normaltb")
        tdSql.checkRows(3)

        tdSql.query("select now() -1b from db.normaltb")
        tdSql.checkRows(3)
        tdSql.query("select now() -1b from normaltb")
        tdSql.checkRows(3)



        # tdLog.printNoPrefix("==========step3:create super table==========")
        # tdSql.execute("create table stb (ts timestamp,c0 int) tags(t0 int)")
        # tdSql.execute("create table stb_1 using stb tags(1)")
        # tdSql.execute("create table stb_2 using stb tags(2)")

        # tdLog.printNoPrefix("==========step4:insert data==========")
        # tdSql.execute("insert into stb_1 values(now(),1000)")
        # tdSql.query("select timezone() from stb")
        # tdSql.checkRows(1)
        # tdSql.checkData(0,0,"Asia/Shanghai (CST, +0800)")

        # tdSql.execute("insert into stb_1 values(now(),1000)")
        # tdSql.query("select timezone() from stb")
        # tdSql.checkRows(1)
        # tdSql.checkData(0,0,"Asia/Shanghai (CST, +0800)")

        # tdSql.execute("insert into stb_2 values(now,1000)")
        # tdSql.query("select timezone() from stb")
        # tdSql.checkRows(2)

        # tdSql.query("select now() from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() +1w from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1w from stb")
        # tdSql.checkRows(3) 

        # tdSql.query("select now() +1d from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1d from stb")
        # tdSql.checkRows(3) 

        # tdSql.query("select now() +1h from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1h from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() +1m from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1m from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() +1s from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1s from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() +1a from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1a from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() +1u from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1u from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() +1b from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() +1b from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1w from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1w from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1d from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1d from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1h from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1h from sltb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1m from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1m from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1s from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1s from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1a from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1a from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1u from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1u from stb")
        # tdSql.checkRows(3)

        # tdSql.query("select now() -1b from db.stb")
        # tdSql.checkRows(3)
        # tdSql.query("select now() -1b from stb")
        # tdSql.checkRows(3)

        # tdSql.execute("insert into stb_1 values(today(),100)")
        # tdSql.query("select * from stb_1 where ts=today()")
        # tdSql.checkRows(1)
        # tdSql.checkData(0,0,100)


        


        
        
        # tdSql.execute()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
