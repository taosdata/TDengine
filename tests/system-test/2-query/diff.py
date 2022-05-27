from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        self.perfix = 'dev'
        self.tables = 10


    def run(self):
        tdSql.prepare()
        tdSql.execute(
            "create table ntb(ts timestamp,c1 int,c2 double,c3 float)")
        tdSql.execute(
            "insert into ntb values(now,1,1.0,10.5)(now+1s,10,-100.0,5.1)(now+10s,-1,15.1,5.0)")

        tdSql.query("select diff(c1,0) from ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, -11)
        tdSql.query("select diff(c1,1) from ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, None)

        tdSql.query("select diff(c2,0) from ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -101)
        tdSql.checkData(1, 0, 115.1)
        tdSql.query("select diff(c2,1) from ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 115.1)

        tdSql.query("select diff(c3,0) from ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -5.4)
        tdSql.checkData(1, 0, -0.1)
        tdSql.query("select diff(c3,1) from ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.execute('''create table stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table stb_1 using stb tags('beijing')")
        tdSql.execute(
            "insert into stb_1 values(%d, 0, 0, 0, 0, 0.0, 0.0, False, ' ', ' ', 0, 0, 0, 0)" % (self.ts - 1))

        # diff verifacation
        tdSql.query("select diff(col1) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col2) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col3) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col4) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col5) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col6) from stb_1")
        tdSql.checkRows(0)

        tdSql.query("select diff(col7) from stb_1")
        tdSql.checkRows(0)

        for i in range(self.rowNum):
            tdSql.execute("insert into stb_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))

        tdSql.error("select diff(ts) from stb")
        tdSql.error("select diff(ts) from stb_1")

        # tdSql.error("select diff(col7) from stb")

        tdSql.error("select diff(col8) from stb")
        tdSql.error("select diff(col8) from stb_1")
        tdSql.error("select diff(col9) from stb")
        tdSql.error("select diff(col9) from stb_1")
        tdSql.error("select diff(col11) from stb_1")
        tdSql.error("select diff(col12) from stb_1")
        tdSql.error("select diff(col13) from stb_1")
        tdSql.error("select diff(col14) from stb_1")
        tdSql.error("select ts,diff(col1),ts from stb_1")

        tdSql.query("select diff(col1) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col2) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col3) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col4) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col5) from stb_1")
        tdSql.checkRows(10)

        tdSql.query("select diff(col6) from stb_1")
        tdSql.checkRows(10)

        tdSql.execute('''create table stb1(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table stb1_1 using stb tags('shanghai')")

        for i in range(self.rowNum):
            tdSql.execute("insert into stb1_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        for i in range(self.rowNum):
            tdSql.execute("insert into stb1_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                          % (self.ts - i-1, i-1, i-1, i-1, i-1, -i - 0.1, -i - 0.1, -i % 2, i - 1, i - 1, i + 1, i + 1, i + 1, i + 1))
        tdSql.query("select diff(col1,0) from stb1_1")
        tdSql.checkRows(19)
        tdSql.query("select diff(col1,1) from stb1_1")
        tdSql.checkRows(19)
        tdSql.checkData(0,0,None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
