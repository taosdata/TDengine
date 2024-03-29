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

    def check_result(self):
        for i in range(self.rowNum):
            tdSql.checkData(i, 0, 1);
    
    def full_datatype_test(self):
        tdSql.execute("use db;")
        sql = "create table db.st(ts timestamp, c1 bool, c2 float, c3 double,c4 tinyint, c5 smallint, c6 int, c7 bigint, c8 tinyint unsigned, c9 smallint unsigned, c10 int unsigned, c11 bigint unsigned) tags( area int);"
        tdSql.execute(sql)

        sql = "create table db.t1 using db.st tags(1);"
        tdSql.execute(sql)

        ts = 1694000000000
        rows = 126
        for i in range(rows):
            ts += 1
            sql = f"insert into db.t1 values({ts},true,{i},{i},{i%127},{i%32767},{i},{i},{i%127},{i%32767},{i},{i});"
            tdSql.execute(sql)

        sql = "select diff(ts),diff(c1),diff(c3),diff(c4),diff(c5),diff(c6),diff(c7),diff(c8),diff(c9),diff(c10),diff(c11) from db.t1"
        tdSql.query(sql)
        tdSql.checkRows(rows - 1)
        for i in range(rows - 1):
            for j in range(10):
               if j == 1: # bool
                 tdSql.checkData(i, j, 0)
               else:
                 tdSql.checkData(i, j, 1)

    def run(self):
        tdSql.prepare()
        dbname = "db"

        # full type test
        self.full_datatype_test()

        tdSql.execute(
            f"create table {dbname}.ntb(ts timestamp,c1 int,c2 double,c3 float)")
        tdSql.execute(
                f"insert into {dbname}.ntb values('2023-01-01 00:00:01',1,1.0,10.5)('2023-01-01 00:00:02',10,-100.0,5.1)('2023-01-01 00:00:03',-1,15.1,5.0)")

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

        # handle null values
        tdSql.execute(
            f"create table {dbname}.ntb_null(ts timestamp,c1 int,c2 double,c3 float,c4 bool)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now, 1,    1.0,  NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+1s, NULL, 2.0,  2.0,  NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+2s, 2,    NULL, NULL, false)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+3s, NULL, 1.0,  1.0,  NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+4s, NULL, 3.0,  NULL, true)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+5s, 3,    NULL, 3.0,  NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+6s, 1,    NULL, NULL, true)")

        tdSql.query(f"select diff(c1) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, -2)

        tdSql.query(f"select diff(c2) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, -1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select diff(c3) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, -1)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select diff(c4) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, 0)

        tdSql.query(f"select diff(c1),diff(c2),diff(c3),diff(c4) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, -2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, -1)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(5, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(4, 3, None)
        tdSql.checkData(5, 3, 0)

        tdSql.query(f"select diff(c1),diff(c2),diff(c3),diff(c4) from {dbname}.ntb_null where c1 is not null")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, -2)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 3, 1)

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
        tdSql.error(f"select diff(col1,col1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1,col) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,'123') from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,1.23) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,-1) from  {dbname}.stb_1")
        tdSql.query(f"select ts,diff(col1),ts from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1, 1),diff(col2) from {dbname}.stb_1")
        tdSql.error(f"select diff(col1, 1),diff(col2, 0) from {dbname}.stb_1")
        tdSql.error(f"select diff(col1, 1),diff(col2, 1) from {dbname}.stb_1")

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

        tdSql.query(f"select diff(col11) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.query(f"select diff(col12) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.query(f"select diff(col13) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.query(f"select diff(col14) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

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

        # TD-25098

        tdSql.query(f"select ts, diff(c1) from  {dbname}.ntb order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:02.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:03.000')

        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, -11)

        tdSql.query(f"select ts, diff(c1) from  {dbname}.ntb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:03.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:02.000')

        tdSql.checkData(0, 1, -11)
        tdSql.checkData(1, 1, 9)

        tdSql.query(f"select ts, diff(c1) from (select * from {dbname}.ntb order by ts)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:02.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:03.000')

        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, -11)

        tdSql.query(f"select ts, diff(c1) from (select * from {dbname}.ntb order by ts desc)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:02.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:01.000')

        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, -9)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
