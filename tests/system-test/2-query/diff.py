from wsgiref.headers import tspecials
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        self.perfix = 'dev'
        self.tables = 10


    def run(self):
        tdSql.prepare()
        dbname = "db"
        tdSql.execute(
            f"create table {dbname}.ntb(ts timestamp,c1 int,c2 double,c3 float)")
        tdSql.execute(
            f"insert into {dbname}.ntb values(now,1,1.0,10.5)(now+1s,10,-100.0,5.1)(now+10s,-1,15.1,5.0)")

        tdSql.query(f"select diff(c1,0) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, -11)
        tdSql.query(f"select diff(c1,1) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, None)

        tdSql.query(f"select diff(c2,0) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -101)
        tdSql.checkData(1, 0, 115.1)
        tdSql.query(f"select diff(c2,1) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 115.1)

        tdSql.query(f"select diff(c3,0) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -5.4)
        tdSql.checkData(1, 0, -0.1)
        tdSql.query(f"select diff(c3,1) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(
            f"insert into {dbname}.stb_1 values(%d, 0, 0, 0, 0, 0.0, 0.0, False, ' ', ' ', 0, 0, 0, 0)" % (self.ts - 1))

        # diff verifacation
        tdSql.query(f"select diff(col1) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col2) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col3) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col4) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col5) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col6) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col7) from {dbname}.stb_1")
        tdSql.checkRows(0)

        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))

        # tdSql.error(f"select diff(col7) from  {dbname}.stb")

        tdSql.error(f"select diff(col8) from {dbname}.stb")
        tdSql.error(f"select diff(col8) from {dbname}.stb_1")
        tdSql.error(f"select diff(col9) from {dbname}.stb")
        tdSql.error(f"select diff(col9) from {dbname}.stb_1")
        tdSql.error(f"select diff(col11) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col12) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col13) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col14) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col14) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1,col) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,'123') from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,1.23) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,-1) from  {dbname}.stb_1")
        tdSql.query(f"select ts,diff(col1),ts from  {dbname}.stb_1")

        tdSql.query(f"select diff(ts) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col1) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col2) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col3) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col4) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col5) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col6) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.execute(f'''create table  {dbname}.stb1(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table  {dbname}.stb1_1 using  {dbname}.stb tags('shanghai')")

        for i in range(self.rowNum):
            tdSql.execute(f"insert into  {dbname}.stb1_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        for i in range(self.rowNum):
            tdSql.execute(f"insert into  {dbname}.stb1_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                          % (self.ts - i-1, i-1, i-1, i-1, i-1, -i - 0.1, -i - 0.1, -i % 2, i - 1, i - 1, i + 1, i + 1, i + 1, i + 1))
        tdSql.query(f"select diff(col1,0) from  {dbname}.stb1_1")
        tdSql.checkRows(19)
        tdSql.query(f"select diff(col1,1) from  {dbname}.stb1_1")
        tdSql.checkRows(19)
        tdSql.checkData(0,0,None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
