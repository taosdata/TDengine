import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        dbname = "db"
        tbname = "tb"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname}
            (ts timestamp, c0 int, c1 bool, c2 varchar(100), c3 nchar(100), c4 varbinary(100))
            '''
        )

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"use db")

        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, true, 'varchar', 'nchar', 'varbinary')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:10', 10, true, 'varchar', 'nchar', 'varbinary')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:15', 15, NULL, NULL, NULL, NULL)")

        tdLog.printNoPrefix("==========step3:fill data")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, 'xx');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 9)
        tdSql.checkData(0, 1, False)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(4, 1, False)

        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, True);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(4, 1, True)

        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, False);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, False)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(4, 1, False)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, 'abc');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, "abc")
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, "abc")
        tdSql.checkData(4, 1, "abc")

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, '我是#$^中文');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, '我是#$^中文')
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, "我是#$^中文")
        tdSql.checkData(4, 1, "我是#$^中文")

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, '我是#$^中文');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, '我是#$^中文')
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, "我是#$^中文")
        tdSql.checkData(4, 1, "我是#$^中文")

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, '我是中文');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, b'\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe6\x96\x87')
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, b'\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe6\x96\x87')
        tdSql.checkData(4, 1, b'\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe6\x96\x87')
        
        tdLog.printNoPrefix("==========step4:fill null")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, NULL, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdLog.printNoPrefix("==========step5:fill prev")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(4, 1, True)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, 'varchar')
        tdSql.checkData(4, 1, 'varchar')

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, 'nchar')
        tdSql.checkData(4, 1, 'nchar')

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, b'varbinary')
        tdSql.checkData(4, 1, b'varbinary')

        tdLog.printNoPrefix("==========step6:fill next")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'varchar')
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'nchar')
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, b'varbinary')
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:20', 15, False, '中文', '中文', '中文');")
        tdLog.printNoPrefix("==========step6:fill next")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(4, 1, False)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'varchar')
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, '中文')
        tdSql.checkData(4, 1, '中文')

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'nchar')
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, '中文')
        tdSql.checkData(4, 1, '中文')

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, b'varbinary')
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, b'\xe4\xb8\xad\xe6\x96\x87')
        tdSql.checkData(4, 1, b'\xe4\xb8\xad\xe6\x96\x87')
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
