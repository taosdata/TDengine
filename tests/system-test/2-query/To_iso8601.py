import time
from time import sleep

from util.log import *
from util.sql import *
from util.cases import *
import os



class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.rowNum = 10
        self.ts = 1640966400000  # 2022-1-1 00:00:00.000
    def check_customize_param_ms(self):

        time_zone = time.strftime('%z')
        tdSql.execute('create database db1 precision "ms"')
        tdSql.execute('use db1')
        tdSql.execute('create table if not exists ntb(ts timestamp, c1 int, c2 timestamp)')
        for i in range(self.rowNum):
            tdSql.execute("insert into ntb values(%d, %d, %d)"
                        % (self.ts + i, i + 1, self.ts + i))
        tdSql.query('select to_iso8601(ts) from ntb')
        for i in range(self.rowNum):
            tdSql.checkEqual(tdSql.queryResult[i][0],f'2022-01-01T00:00:00.00{i}{time_zone}')

        timezone_list = ['+0000','+0100','+0200','+0300','+0330','+0400','+0500','+0530','+0600','+0700','+0800','+0900','+1000','+1100','+1200',\
                        '+00','+01','+02','+03','+04','+05','+06','+07','+08','+09','+10','+11','+12',\
                            '+00:00','+01:00','+02:00','+03:00','+03:30','+04:00','+05:00','+05:30','+06:00','+07:00','+08:00','+09:00','+10:00','+11:00','+12:00',\
                            '-0000','-0100','-0200','-0300','-0400','-0500','-0600','-0700','-0800','-0900','-1000','-1100','-1200',\
                        '-00','-01','-02','-03','-04','-05','-06','-07','-08','-09','-10','-11','-12',\
                            '-00:00','-01:00','-02:00','-03:00','-04:00','-05:00','-06:00','-07:00','-08:00','-09:00','-10:00','-11:00','-12:00',\
                                'z','Z']
        for j in timezone_list:
            tdSql.query(f'select to_iso8601(ts,"{j}") from ntb')
            for i in range(self.rowNum):
                tdSql.checkEqual(tdSql.queryResult[i][0],f'2022-01-01T00:00:00.00{i}{j}')

        error_param_list = [0,100.5,'a','!']
        for i in error_param_list:
            tdSql.error(f'select to_iso8601(ts,"{i}") from ntb')
        #! bug TD-16372:对于错误的时区，缺少校验
        error_timezone_param = ['+13','-13','+1300','-1300','+0001','-0001','-0330','-0530']
        for i in error_timezone_param:
            tdSql.error(f'select to_iso8601(ts,"{i}") from ntb')

    def check_base_function(self):
        tdSql.prepare()
        tdLog.printNoPrefix("==========step1:create tables==========")
        tdSql.execute('create table if not exists ntb(ts timestamp, c1 int, c2 float,c3 double,c4 timestamp)')
        tdSql.execute('create table if not exists stb(ts timestamp, c1 int, c2 float,c3 double,c4 timestamp) tags(t0 int)')
        tdSql.execute('create table if not exists stb_1 using stb tags(100)')

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
        for i in range(0,3):
            tdSql.query("select to_iso8601(1) from ntb")
            tdSql.checkData(i,0,"1970-01-01T08:00:01+0800")
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

        for i in ['+','-','*','/']:
            tdSql.query(f"select to_iso8601(today()) {i}null from ntb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)
            tdSql.query(f"select to_iso8601(today()) {i}null from db.ntb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)
        tdSql.query("select to_iso8601(9223372036854775807) from ntb")
        tdSql.checkRows(3)
        # bug TD-15207
        # tdSql.query("select to_iso8601(10000000000) from ntb")
        # tdSql.checkData(0,0,None)
        # tdSql.query("select to_iso8601(-1) from ntb")
        # tdSql.checkRows(3)
        # tdSql.query("select to_iso8601(-10000000000) from ntb")
        # tdSql.checkData(0,0,None)
        err_param = [1.5,'a','c2']
        for i in err_param:
            tdSql.error(f"select to_iso8601({i}) from ntb")
            tdSql.error(f"select to_iso8601({i}) from db.ntb")

        tdSql.query("select to_iso8601(now) from stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(1) from stb")
        for i in range(0,3):
            tdSql.checkData(i,0,"1970-01-01T08:00:01+0800")
            tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts)+1 from stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts)+'a' from stb ")
        tdSql.checkRows(3)
        for i in ['+','-','*','/']:
            tdSql.query(f"select to_iso8601(today()) {i}null from stb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)
            tdSql.query(f"select to_iso8601(today()) {i}null from db.stb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)

    def run(self):  # sourcery skip: extract-duplicate-method
        self.check_base_function()
        self.check_customize_param_ms()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
