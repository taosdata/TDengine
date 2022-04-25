from time import sleep
import taos
from util.log import *
from util.sql import *
from util.cases import *

import os


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):  # sourcery skip: extract-duplicate-method
        tdSql.prepare()
        # get system timezone
        today_date = datetime.datetime.strptime(
            datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
        
        tdLog.printNoPrefix("==========step1:create tables==========")
        tdSql.execute(
            '''create table if not exists ntb
            (ts timestamp, c1 int, c2 float,c3 double,c4 timestamp)
            '''
        )
        tdSql.execute(
            '''create table if not exists stb
            (ts timestamp, c1 int, c2 float,c3 double,c4 timestamp) tags(t0 int)
            '''
        )
        tdSql.execute(
            '''create table if not exists stb_1 using stb tags(100)
            '''
        )

        tdLog.printNoPrefix("==========step2:insert data==========")
        tdSql.execute('insert into ntb values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.execute('insert into stb_1 values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')

        tdSql.query("select to_iso8601(ts) from ntb")
        tdSql.checkRows(3)
        tdSql.query("select c1 from ntb where ts = to_iso8601(1577808000000)")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,10)
        tdSql.query("select * from ntb where ts = to_iso8601(1577808000000)")
        tdSql.checkRows(1)
        tdSql.query("select to_iso8601(ts) from ntb where ts=today()")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,10)
        for i in range(1,10):
            tdSql.query("select to_iso8601(1) from ntb")
            tdSql.checkData(0,0,"1970-01-01T08:00:01+0800")
            i+=1
            sleep(0.2)
            tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from db.ntb")
        
        tdSql.query("select to_iso8601(today()) from ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from ntb")
        tdSql.checkRows(3)
        
        tdSql.error("select to_iso8601(timezone()) from ntb")
        tdSql.error("select to_iso8601('abc') from ntb")




    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
