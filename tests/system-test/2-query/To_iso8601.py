import time
from time import sleep

from util.log import *
from util.sql import *
from util.cases import *
import os



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.rowNum = 10
        self.ts = 1640966400000  # 2022-1-1 00:00:00.000
        self.dbname = 'db'
        self.stbname = f'{self.dbname}.stb'
        self.ntbname = f'{self.dbname}.ntb'
    def check_customize_param_ms(self):
        time_zone = time.strftime('%z')
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "ms"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.ntbname}(ts timestamp, c1 int, c2 timestamp)')
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {self.ntbname} values({self.ts + i}, {i + 1}, {self.ts + i})")
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        for i in range(self.rowNum):
            tdSql.checkEqual(tdSql.queryResult[i][0],f'2022-01-01T00:00:00.00{i}{time_zone}')

        tz_list = ['+0000','+0530', '+00', '+06', '+00:00', '+12:00', '-0000', '-0900', '-00', '-05', '-00:00', '-03:00','z', 'Z']
        res_list = ['2021-12-31T16:00:00.000+0000', '2021-12-31T21:30:00.000+0530', '2021-12-31T16:00:00.000+00', '2021-12-31T22:00:00.000+06',\
                '2021-12-31T16:00:00.000+00:00', '2022-01-01T04:00:00.000+12:00','2021-12-31T16:00:00.000-0000','2021-12-31T07:00:00.000-0900',\
                '2021-12-31T16:00:00.000-00', '2021-12-31T11:00:00.000-05','2021-12-31T16:00:00.000-00:00','2021-12-31T13:00:00.000-03:00',\
                '2021-12-31T16:00:00.000z', '2021-12-31T16:00:00.000Z']
        for i in range(len(tz_list)):
            tdSql.query(f'select to_iso8601(ts,"{tz_list[i]}") from {self.ntbname} where c1==1')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list[i])

        error_param_list = [0,100.5,'a','!']
        for i in error_param_list:
            tdSql.error(f'select to_iso8601(ts,"{i}") from {self.ntbname}')
        error_timezone_param = ['+13','-13','+1300','-1300','+0001','-0001','-0330','-0530']
        for i in error_timezone_param:
            tdSql.error(f'select to_iso8601(ts,"{i}") from {self.ntbname}')

    def check_base_function(self):
        tdSql.prepare()
        tdSql.execute('create table if not exists db.ntb(ts timestamp, c1 int, c2 float,c3 double,c4 timestamp)')
        tdSql.execute('create table if not exists db.stb(ts timestamp, c1 int, c2 float,c3 double,c4 timestamp) tags(t0 int)')
        tdSql.execute('create table if not exists db.stb_1 using db.stb tags(100)')
        tdSql.execute('insert into db.ntb values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.execute('insert into db.stb_1 values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.query("select to_iso8601(ts) from db.ntb")
        tdSql.checkRows(3)
        tdSql.query("select c1 from db.ntb where ts = to_iso8601(1577808000000)")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,10)
        tdSql.query("select * from db.ntb where ts = to_iso8601(1577808000000)")
        tdSql.checkRows(1)
        tdSql.query("select to_iso8601(ts) from db.ntb where ts=today()")
        tdSql.checkRows(1)
        for i in range(0,3):
            tdSql.query("select to_iso8601(1) from db.ntb")
            tdSql.checkData(i,0,"1970-01-01T08:00:01+0800")
            tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from db.ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(today()) from db.ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from db.ntb")
        tdSql.checkRows(3)
        tdSql.error("select to_iso8601(timezone()) from db.ntb")
        tdSql.error("select to_iso8601('abc') from db.ntb")
        for i in ['+','-','*','/']:
            tdSql.query(f"select to_iso8601(today()) {i}null from db.ntb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)
        tdSql.query("select to_iso8601(9223372036854775807) from db.ntb")
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
            tdSql.error(f"select to_iso8601({i}) from db.ntb")
        tdSql.query("select to_iso8601(now) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(1) from db.stb")
        for i in range(0,3):
            tdSql.checkData(i,0,"1970-01-01T08:00:01+0800")
            tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts)+1 from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts)+'a' from db.stb ")
        tdSql.checkRows(3)
        for i in ['+','-','*','/']:
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
