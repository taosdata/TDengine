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
    def check_timestamp_precision(self):
        time_zone = time.strftime('%z')
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "us"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.ntbname}(ts timestamp, c1 int, c2 timestamp)')
        tdSql.execute(f'insert into {self.ntbname} values(now,1,today())')
        ts_list = ['1', '11', '111', '1111', '11111', '111111', '1111111', '11111111', '111111111', '1111111111',
                   '11111111111','111111111111','1111111111111','11111111111111','111111111111111','1111111111111111',
                   '11111111111111111','111111111111111111','1111111111111111111']
        res_list_ms = ['1970-01-01T08:00:00.001+0800', '1970-01-01T08:00:00.011+0800', '1970-01-01T08:00:00.111+0800',
                       '1970-01-01T08:00:01.111+0800', '1970-01-01T08:00:11.111+0800', '1970-01-01T08:01:51.111+0800',
                       '1970-01-01T08:18:31.111+0800', '1970-01-01T11:05:11.111+0800', '1970-01-02T14:51:51.111+0800',
                       '1970-01-14T04:38:31.111+0800', '1970-05-09T22:25:11.111+0800', '1973-07-10T08:11:51.111+0800',
                       '2005-03-18T09:58:31.111+0800', '2322-02-06T03:45:11.111+0800', '5490-12-21T13:31:51.111+0800',
                       '37179-09-17T15:18:31.111+0800', '354067-02-04T09:05:11.111+0800',
                       '3522940-12-11T18:51:51.111+0800', '35211679-06-14T20:38:31.111+0800']
        res_list_us = ['1970-01-01T08:00:00.000001+0800', '1970-01-01T08:00:00.000011+0800',
                       '1970-01-01T08:00:00.000111+0800', '1970-01-01T08:00:00.001111+0800',
                       '1970-01-01T08:00:00.011111+0800', '1970-01-01T08:00:00.111111+0800',
                       '1970-01-01T08:00:01.111111+0800', '1970-01-01T08:00:11.111111+0800',
                       '1970-01-01T08:01:51.111111+0800', '1970-01-01T08:18:31.111111+0800',
                       '1970-01-01T11:05:11.111111+0800', '1970-01-02T14:51:51.111111+0800',
                       '1970-01-14T04:38:31.111111+0800', '1970-05-09T22:25:11.111111+0800',
                       '1973-07-10T08:11:51.111111+0800', '2005-03-18T09:58:31.111111+0800',
                       '2322-02-06T03:45:11.111111+0800', '5490-12-21T13:31:51.111111+0800',
                       '37179-09-17T15:18:31.111111+0800']
        res_list_ns = ['1970-01-01T08:00:00.000000001+0800', '1970-01-01T08:00:00.000000011+0800',
                       '1970-01-01T08:00:00.000000111+0800', '1970-01-01T08:00:00.000001111+0800',
                       '1970-01-01T08:00:00.000011111+0800', '1970-01-01T08:00:00.000111111+0800',
                       '1970-01-01T08:00:00.001111111+0800', '1970-01-01T08:00:00.011111111+0800',
                       '1970-01-01T08:00:00.111111111+0800', '1970-01-01T08:00:01.111111111+0800',
                       '1970-01-01T08:00:11.111111111+0800', '1970-01-01T08:01:51.111111111+0800',
                       '1970-01-01T08:18:31.111111111+0800', '1970-01-01T11:05:11.111111111+0800',
                       '1970-01-02T14:51:51.111111111+0800', '1970-01-14T04:38:31.111111111+0800',
                       '1970-05-09T22:25:11.111111111+0800', '1973-07-10T08:11:51.111111111+0800',
                       '2005-03-18T09:58:31.111111111+0800']
        # test to_iso8601's precision with default precision 'ms'
        for i in range(len(ts_list)):
            tdSql.query(f'select to_iso8601({ts_list[i]})')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list_ms[i])
        # test to_iso8601's precision with table's precision 'us'
        for i in range(len(ts_list)):
            tdSql.query(f'select to_iso8601({ts_list[i]}) from {self.ntbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list_us[i])

        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "ns"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.ntbname}(ts timestamp, c1 int, c2 timestamp)')
        tdSql.execute(f'insert into {self.ntbname} values(now,1,today())')
        # test to_iso8601's precision with table's precision 'ns'
        for i in range(len(ts_list)):
            tdSql.query(f'select to_iso8601({ts_list[i]}) from {self.ntbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list_ns[i])
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
            tdSql.checkData(i,0,"1970-01-01T08:00:00.001+0800")
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
        tdSql.error(f"select to_iso8601(ts, timezone()) from db.stb")
        tdSql.query("select to_iso8601(now) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(1) from db.stb")
        for i in range(0,3):
            tdSql.checkData(i,0,"1970-01-01T08:00:00.001+0800")
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
        self.check_timestamp_precision()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
