from time import sleep

from util.log import *
from util.sql import *
from util.cases import *




class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        # name of normal table
        self.ntbname = 'ntb'
        # name of stable
        self.stbname = 'stb'
        # structure of column
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'binary(20)',
            'c4':'nchar(20)'
        }
        # structure of tag
        self.tag_dict = {
            't0':'int'
        }
        # number of child tables
        self.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        self.tag_values = [
            f'10',
            f'100'
        ]
        # values of rows, structure should be same as column
        self.values_list = [
            f'now,10,99.99,"2020-1-1 00:00:00"',
            f'today(),100,11.111,22.222222'

        ]
        self.error_param = [1,'now()']

    def run(self):  # sourcery skip: extract-duplicate-method
        tdSql.prepare()
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
        tdLog.printNoPrefix("==========step2:insert data into ntb==========")

        # RFC3339:2020-01-01T00:00:00+8:00
        # ISO8601:2020-01-01T00:00:00.000+0800
        tdSql.execute(
            'insert into ntb values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.execute(
            'insert into stb_1 values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.query("select to_unixtimestamp('1970-01-01T08:00:00+0800') from ntb")
        tdSql.checkData(0,0,0)
        tdSql.checkData(1,0,0)
        tdSql.checkData(2,0,0)
        tdSql.checkRows(3)
        tdSql.query("select to_unixtimestamp('1970-01-01T08:00:00+08:00') from ntb")
        tdSql.checkData(0,0,0)
        tdSql.checkRows(3)
        tdSql.query("select to_unixtimestamp('1900-01-01T08:00:00+08:00') from ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_unixtimestamp('2020-01-32T08:00:00') from ntb")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,None)
        tdSql.query("select to_unixtimestamp('2020-13-32T08:00:00') from ntb")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,None)
        tdSql.query("select to_unixtimestamp('acd') from ntb")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,None)
        tdSql.error("select to_unixtimestamp(1) from ntb")
        tdSql.error("select to_unixtimestamp(1.5) from ntb")
        tdSql.error("select to_unixtimestamp(ts) from ntb")

        tdSql.query("select ts from ntb where to_unixtimestamp('1970-01-01T08:00:00+08:00')=0")
        tdSql.checkRows(3)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
