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
        tbname1 = "tb1"
        stbname = "stb"
        ctbname1 = "ctb1"
        ctbname2 = "ctb2"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname}
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10))
            '''
        )

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdLog.printNoPrefix("==========step3:fill null")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, 15)
        tdSql.checkData(12, 0, None)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(null)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(null)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(null)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdLog.printNoPrefix("==========step4:fill value")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 1)
        tdSql.checkData(9, 0, 1)
        tdSql.checkData(10, 0, 1)
        tdSql.checkData(11, 0, 15)
        tdSql.checkData(12, 0, 1)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(value, 1)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(value, 1)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdLog.printNoPrefix("==========step5:fill prev")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(prev)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, 10)
        tdSql.checkData(8, 0, 10)
        tdSql.checkData(9, 0, 10)
        tdSql.checkData(10, 0, 15)
        tdSql.checkData(11, 0, 15)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(0)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(prev)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 10)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(prev)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)

        tdLog.printNoPrefix("==========step6:fill next")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(next)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 10)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, 15)
        tdSql.checkData(8, 0, 15)
        tdSql.checkData(9, 0, 15)
        tdSql.checkData(10, 0, 15)
        tdSql.checkData(11, 0, 15)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(next)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 10)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(next)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 10)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(next)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(next)")
        tdSql.checkRows(0)


        tdLog.printNoPrefix("==========step7:fill linear")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 9)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 11)
        tdSql.checkData(7, 0, 12)
        tdSql.checkData(8, 0, 13)
        tdSql.checkData(9, 0, 14)
        tdSql.checkData(10, 0, 15)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(0)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(linear)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(2, 0, 8)
        tdSql.checkData(3, 0, 9)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(linear)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 9)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 11)
        tdSql.checkData(4, 0, 12)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 13)
        tdSql.checkData(1, 0, 14)
        tdSql.checkData(2, 0, 15)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(linear)")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("==========step8:test _irowts with interp")

        # fill null
        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(null)")
        tdSql.checkRows(9)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        tdSql.checkRows(13)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:15.000')
        tdSql.checkData(12, 0, '2020-02-01 00:00:16.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        # fill value
        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(value, 1)")
        tdSql.checkRows(9)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:15.000')
        tdSql.checkData(12, 0, '2020-02-01 00:00:16.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        # fill prev
        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(prev)")
        tdSql.checkRows(9)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(prev)")
        tdSql.checkRows(12)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:15.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:16.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(prev)")
        tdSql.checkRows(6)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        # fill next
        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(next)")
        tdSql.checkRows(9)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(next)")
        tdSql.checkRows(12)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:15.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(next)")
        tdSql.checkRows(6)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        # fill linear
        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:15.000')

        tdSql.query(f"select _irowts,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkCols(2)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        # multiple _irowts
        tdSql.query(f"select interp(c0),_irowts from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(2)

        tdSql.checkData(0, 1, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 1, '2020-02-01 00:00:06.000')
        tdSql.checkData(2, 1, '2020-02-01 00:00:07.000')
        tdSql.checkData(3, 1, '2020-02-01 00:00:08.000')
        tdSql.checkData(4, 1, '2020-02-01 00:00:09.000')
        tdSql.checkData(5, 1, '2020-02-01 00:00:10.000')
        tdSql.checkData(6, 1, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 1, '2020-02-01 00:00:12.000')
        tdSql.checkData(8, 1, '2020-02-01 00:00:13.000')
        tdSql.checkData(9, 1, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        tdSql.query(f"select _irowts, interp(c0), interp(c0), _irowts from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(4)

        cols = (0, 3)
        for i in cols:
          tdSql.checkData(0, i, '2020-02-01 00:00:05.000')
          tdSql.checkData(1, i, '2020-02-01 00:00:06.000')
          tdSql.checkData(2, i, '2020-02-01 00:00:07.000')
          tdSql.checkData(3, i, '2020-02-01 00:00:08.000')
          tdSql.checkData(4, i, '2020-02-01 00:00:09.000')
          tdSql.checkData(5, i, '2020-02-01 00:00:10.000')
          tdSql.checkData(6, i, '2020-02-01 00:00:11.000')
          tdSql.checkData(7, i, '2020-02-01 00:00:12.000')
          tdSql.checkData(8, i, '2020-02-01 00:00:13.000')
          tdSql.checkData(9, i, '2020-02-01 00:00:14.000')
          tdSql.checkData(10, i, '2020-02-01 00:00:15.000')


        tdLog.printNoPrefix("==========step9:test intra block interpolation")
        tdSql.execute(f"drop database {dbname}");

        tdSql.prepare()

        tdSql.execute(f"create table if not exists {dbname}.{tbname} (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10))")

        # set two data point has 10 days interval will be stored in different datablocks
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-11 00:00:05', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdSql.execute(
            f'''create stable if not exists {dbname}.{stbname}
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10)) tags(t1 int)
            '''
        )


        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname1} using {dbname}.{stbname} tags(1)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname2} using {dbname}.{stbname} tags(1)
            '''
        )

        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdSql.execute(f"insert into {dbname}.{ctbname2} values ('2020-02-02 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname2} values ('2020-02-02 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname2} values ('2020-02-02 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")


        tdSql.execute(f"flush database {dbname}");

        # test fill null

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, None)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)


        # test fill value

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 1)
        tdSql.checkData(9, 0, 1)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)


        # test fill prev

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(6, 0, 5)
        tdSql.checkData(7, 0, 5)
        tdSql.checkData(8, 0, 5)
        tdSql.checkData(9, 0, 5)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(5, 0, 15)

        # test fill next

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(5, 0, 15)
        tdSql.checkData(6, 0, 15)
        tdSql.checkData(7, 0, 15)
        tdSql.checkData(8, 0, 15)
        tdSql.checkData(9, 0, 15)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(5, 0, 15)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)

        # test fill linear

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 9)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 11)
        tdSql.checkData(7, 0, 12)
        tdSql.checkData(8, 0, 13)
        tdSql.checkData(9, 0, 14)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 7)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(2, 0, 9)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 11)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 9)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 14)
        tdSql.checkData(1, 0, 15)


        tdLog.printNoPrefix("==========step10:test interp with null data")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname1}
            (ts timestamp, c0 int, c1 int)
            '''
        )


        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:00', 0,    NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:05', NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:10', 10,   10)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:15', NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:20', 20,   NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:25', NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:30', 30,   30)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:35', 35,   NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:40', 40,   40)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:45', NULL, 45)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:50', 50,   NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:00:55', NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname1} values ('2020-02-02 00:01:00', 55,   60)")

        # test fill linear

        # check c0
        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:00') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:08') every(1s) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, None)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)


        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:01', '2020-02-02 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:03', '2020-02-02 00:00:08') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:10') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, 10)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, 10)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(14)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, 10)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, None)
        tdSql.checkData(12, 0, None)
        tdSql.checkData(13, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:20') every(1s) fill(linear)")
        tdSql.checkRows(16)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, 10)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, None)
        tdSql.checkData(12, 0, None)
        tdSql.checkData(13, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, 20)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:09', '2020-02-02 00:00:11') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:10', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:12', '2020-02-02 00:00:13') every(1s) fill(linear)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:12', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:12', '2020-02-02 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(7)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:30', '2020-02-02 00:00:40') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0,  0, 30)
        tdSql.checkData(1,  0, 31)
        tdSql.checkData(2,  0, 32)
        tdSql.checkData(3,  0, 33)
        tdSql.checkData(4,  0, 34)
        tdSql.checkData(5,  0, 35)
        tdSql.checkData(6,  0, 36)
        tdSql.checkData(7,  0, 37)
        tdSql.checkData(8,  0, 38)
        tdSql.checkData(9,  0, 39)
        tdSql.checkData(10, 0, 40)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:25', '2020-02-02 00:00:45') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(5,  0, 30)
        tdSql.checkData(6,  0, 31)
        tdSql.checkData(7,  0, 32)
        tdSql.checkData(8,  0, 33)
        tdSql.checkData(9,  0, 34)
        tdSql.checkData(10, 0, 35)
        tdSql.checkData(11, 0, 36)
        tdSql.checkData(12, 0, 37)
        tdSql.checkData(13, 0, 38)
        tdSql.checkData(14, 0, 39)
        tdSql.checkData(15, 0, 40)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:20', '2020-02-02 00:00:40') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(0,  0, 20)
        tdSql.checkData(10, 0, 30)
        tdSql.checkData(11, 0, 31)
        tdSql.checkData(12, 0, 32)
        tdSql.checkData(13, 0, 33)
        tdSql.checkData(14, 0, 34)
        tdSql.checkData(15, 0, 35)
        tdSql.checkData(16, 0, 36)
        tdSql.checkData(17, 0, 37)
        tdSql.checkData(18, 0, 38)
        tdSql.checkData(19, 0, 39)
        tdSql.checkData(20, 0, 40)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:30', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(0,  0, 30)
        tdSql.checkData(1,  0, 31)
        tdSql.checkData(2,  0, 32)
        tdSql.checkData(3,  0, 33)
        tdSql.checkData(4,  0, 34)
        tdSql.checkData(5,  0, 35)
        tdSql.checkData(6,  0, 36)
        tdSql.checkData(7,  0, 37)
        tdSql.checkData(8,  0, 38)
        tdSql.checkData(9,  0, 39)
        tdSql.checkData(10, 0, 40)
        tdSql.checkData(20, 0, 50)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:20', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(31)
        tdSql.checkData(0,  0, 20)
        tdSql.checkData(10, 0, 30)
        tdSql.checkData(11, 0, 31)
        tdSql.checkData(12, 0, 32)
        tdSql.checkData(13, 0, 33)
        tdSql.checkData(14, 0, 34)
        tdSql.checkData(15, 0, 35)
        tdSql.checkData(16, 0, 36)
        tdSql.checkData(17, 0, 37)
        tdSql.checkData(18, 0, 38)
        tdSql.checkData(19, 0, 39)
        tdSql.checkData(20, 0, 40)
        tdSql.checkData(30, 0, 50)

        # check c1
        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:08') every(1s) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:10') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(16)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:20') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)
        tdSql.checkData(16,  0, None)
        tdSql.checkData(17,  0, None)
        tdSql.checkData(18,  0, None)
        tdSql.checkData(19,  0, None)
        tdSql.checkData(20,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:25') every(1s) fill(linear)")
        tdSql.checkRows(26)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)
        tdSql.checkData(16,  0, None)
        tdSql.checkData(17,  0, None)
        tdSql.checkData(18,  0, None)
        tdSql.checkData(19,  0, None)
        tdSql.checkData(20,  0, None)
        tdSql.checkData(21,  0, None)
        tdSql.checkData(22,  0, None)
        tdSql.checkData(23,  0, None)
        tdSql.checkData(24,  0, None)
        tdSql.checkData(25,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:30') every(1s) fill(linear)")
        tdSql.checkRows(31)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)
        tdSql.checkData(16,  0, None)
        tdSql.checkData(17,  0, None)
        tdSql.checkData(18,  0, None)
        tdSql.checkData(19,  0, None)
        tdSql.checkData(20,  0, None)
        tdSql.checkData(21,  0, None)
        tdSql.checkData(22,  0, None)
        tdSql.checkData(23,  0, None)
        tdSql.checkData(24,  0, None)
        tdSql.checkData(25,  0, None)
        tdSql.checkData(26,  0, None)
        tdSql.checkData(27,  0, None)
        tdSql.checkData(28,  0, None)
        tdSql.checkData(29,  0, None)
        tdSql.checkData(30,  0, 30)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:35') every(1s) fill(linear)")
        tdSql.checkRows(36)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:40') every(1s) fill(linear)")
        tdSql.checkRows(41)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:45') every(1s) fill(linear)")
        tdSql.checkRows(46)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(51)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)


        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:55') every(1s) fill(linear)")
        tdSql.checkRows(56)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)


        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(linear)")
        tdSql.checkRows(61)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)
        tdSql.checkData(60, 0, 60)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:40', '2020-02-02 00:00:45') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 40)
        tdSql.checkData(1, 0, 41)
        tdSql.checkData(2, 0, 42)
        tdSql.checkData(3, 0, 43)
        tdSql.checkData(4, 0, 44)
        tdSql.checkData(5, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:35', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(16)
        tdSql.checkData(5,  0, 40)
        tdSql.checkData(6,  0, 41)
        tdSql.checkData(7,  0, 42)
        tdSql.checkData(8,  0, 43)
        tdSql.checkData(9,  0, 44)
        tdSql.checkData(10, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:35', '2020-02-02 00:00:55') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(5,  0, 40)
        tdSql.checkData(6,  0, 41)
        tdSql.checkData(7,  0, 42)
        tdSql.checkData(8,  0, 43)
        tdSql.checkData(9,  0, 44)
        tdSql.checkData(10, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:30', '2020-02-02 00:01:00') every(1s) fill(linear)")
        tdSql.checkRows(31)
        tdSql.checkData(0,  0, 30)
        tdSql.checkData(10, 0, 40)
        tdSql.checkData(11, 0, 41)
        tdSql.checkData(12, 0, 42)
        tdSql.checkData(13, 0, 43)
        tdSql.checkData(14, 0, 44)
        tdSql.checkData(15, 0, 45)
        tdSql.checkData(30, 0, 60)

        # two interps
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(linear)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, None)
        tdSql.checkData(19, 0, None)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, None)
        tdSql.checkData(24, 0, None)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, None)
        tdSql.checkData(29, 0, None)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 31)
        tdSql.checkData(32, 0, 32)
        tdSql.checkData(33, 0, 33)
        tdSql.checkData(34, 0, 34)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 36)
        tdSql.checkData(37, 0, 37)
        tdSql.checkData(38, 0, 38)
        tdSql.checkData(39, 0, 39)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, None)
        tdSql.checkData(44, 0, None)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, None)
        tdSql.checkData(49, 0, None)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, None)
        tdSql.checkData(54, 0, None)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, None)
        tdSql.checkData(59, 0, None)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, None)
        tdSql.checkData(9,  1, None)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, None)
        tdSql.checkData(14, 1, None)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, None)
        tdSql.checkData(29, 1, None)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, None)
        tdSql.checkData(34, 1, None)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, None)
        tdSql.checkData(39, 1, None)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 41)
        tdSql.checkData(42, 1, 42)
        tdSql.checkData(43, 1, 43)
        tdSql.checkData(44, 1, 44)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, None)
        tdSql.checkData(49, 1, None)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, None)
        tdSql.checkData(59, 1, None)
        tdSql.checkData(60, 1, 60)   #

        # test fill null
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(null)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, None)
        tdSql.checkData(19, 0, None)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, None)
        tdSql.checkData(24, 0, None)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, None)
        tdSql.checkData(29, 0, None)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, None)
        tdSql.checkData(34, 0, None)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, None)
        tdSql.checkData(39, 0, None)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, None)
        tdSql.checkData(44, 0, None)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, None)
        tdSql.checkData(49, 0, None)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, None)
        tdSql.checkData(54, 0, None)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, None)
        tdSql.checkData(59, 0, None)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, None)
        tdSql.checkData(9,  1, None)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, None)
        tdSql.checkData(14, 1, None)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, None)
        tdSql.checkData(29, 1, None)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, None)
        tdSql.checkData(34, 1, None)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, None)
        tdSql.checkData(39, 1, None)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, None)
        tdSql.checkData(44, 1, None)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, None)
        tdSql.checkData(49, 1, None)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, None)
        tdSql.checkData(59, 1, None)
        tdSql.checkData(60, 1, 60)   #

        # test fill value
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(value, 123)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, 123)
        tdSql.checkData(4,  0, 123)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, 123)
        tdSql.checkData(9,  0, 123)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, 123)
        tdSql.checkData(14, 0, 123)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, 123)
        tdSql.checkData(19, 0, 123)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, 123)
        tdSql.checkData(24, 0, 123)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, 123)
        tdSql.checkData(29, 0, 123)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 123)
        tdSql.checkData(34, 0, 123)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 123)
        tdSql.checkData(39, 0, 123)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, 123)
        tdSql.checkData(44, 0, 123)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, 123)
        tdSql.checkData(49, 0, 123)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, 123)
        tdSql.checkData(54, 0, 123)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(59, 0, 123)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, 123)
        tdSql.checkData(4,  1, 123)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, 123)
        tdSql.checkData(9,  1, 123)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, 123)
        tdSql.checkData(14, 1, 123)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, 123)
        tdSql.checkData(19, 1, 123)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, 123)
        tdSql.checkData(24, 1, 123)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 123)
        tdSql.checkData(29, 1, 123)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 123)
        tdSql.checkData(34, 1, 123)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, 123)
        tdSql.checkData(39, 1, 123)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 123)
        tdSql.checkData(44, 1, 123)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, 123)
        tdSql.checkData(49, 1, 123)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, 123)
        tdSql.checkData(54, 1, 123)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, 123)
        tdSql.checkData(59, 1, 123)
        tdSql.checkData(60, 1, 60)   #

        # test fill prev
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(prev)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, 0)
        tdSql.checkData(4,  0, 0)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, 10)
        tdSql.checkData(14, 0, 10)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, None)
        tdSql.checkData(19, 0, None)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, 20)
        tdSql.checkData(24, 0, 20)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, None)
        tdSql.checkData(29, 0, None)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 30)
        tdSql.checkData(34, 0, 30)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 35)
        tdSql.checkData(39, 0, 35)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, 40)
        tdSql.checkData(44, 0, 40)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, None)
        tdSql.checkData(49, 0, None)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, 50)
        tdSql.checkData(54, 0, 50)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, None)
        tdSql.checkData(59, 0, None)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, None)
        tdSql.checkData(9,  1, None)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, 10)
        tdSql.checkData(14, 1, 10)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, None)
        tdSql.checkData(29, 1, None)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 30)
        tdSql.checkData(34, 1, 30)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, None)
        tdSql.checkData(39, 1, None)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 40)
        tdSql.checkData(44, 1, 40)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, 45)
        tdSql.checkData(49, 1, 45)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, None)
        tdSql.checkData(59, 1, None)
        tdSql.checkData(60, 1, 60)   #

        # test fill next
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(next)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, 10)
        tdSql.checkData(9,  0, 10)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, 20)
        tdSql.checkData(19, 0, 20)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, None)
        tdSql.checkData(24, 0, None)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, 30)
        tdSql.checkData(29, 0, 30)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 35)
        tdSql.checkData(34, 0, 35)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 40)
        tdSql.checkData(39, 0, 40)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, None)
        tdSql.checkData(44, 0, None)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, 50)
        tdSql.checkData(49, 0, 50)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, None)
        tdSql.checkData(54, 0, None)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, 55)
        tdSql.checkData(59, 0, 55)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, 10)
        tdSql.checkData(9,  1, 10)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, None)
        tdSql.checkData(14, 1, None)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 30)
        tdSql.checkData(29, 1, 30)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, None)
        tdSql.checkData(34, 1, None)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, 40)
        tdSql.checkData(39, 1, 40)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 45)
        tdSql.checkData(44, 1, 45)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, None)
        tdSql.checkData(49, 1, None)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, 60)
        tdSql.checkData(59, 1, 60)
        tdSql.checkData(60, 1, 60)   #

        tdLog.printNoPrefix("==========step11:test multi-interp cases")
        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, None)
          tdSql.checkData(1, i, None)
          tdSql.checkData(2, i, 15)
          tdSql.checkData(3, i, None)
          tdSql.checkData(4, i, None)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, 1)
          tdSql.checkData(1, i, 1)
          tdSql.checkData(2, i, 15)
          tdSql.checkData(3, i, 1)
          tdSql.checkData(4, i, 1)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, 5)
          tdSql.checkData(1, i, 5)
          tdSql.checkData(2, i, 15)
          tdSql.checkData(3, i, 15)
          tdSql.checkData(4, i, 15)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, 15)
          tdSql.checkData(1, i, 15)
          tdSql.checkData(2, i, 15)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkCols(4)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3),interp(c4),interp(c5) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkCols(6)

        for i in range (tdSql.queryCols):
            tdSql.checkData(0, i, 13)

        tdLog.printNoPrefix("==========step12:test error cases")

        tdSql.error(f"select interp(c0) from {dbname}.{tbname}")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05')")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} every(1s) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where ts >= '2020-02-10 00:00:05' and ts <= '2020-02-15 00:00:05' every(1s) fill(null)")

        # input can only be numerical types
        tdSql.error(f"select interp(ts) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c6) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c7) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c8) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # input can only be columns
        tdSql.error(f"select interp(1) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(1.5) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(true) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(false) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp('abcd') from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp('中文字符') from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        tdLog.printNoPrefix("==========step13:stable cases")

        #tdSql.query(f"select interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        #tdSql.checkRows(13)

        #tdSql.query(f"select interp(c0) from {dbname}.{ctbname1} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        #tdSql.checkRows(13)

        #tdSql.query(f"select interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:04', '2020-02-02 00:00:16') every(1s) fill(null)")
        #tdSql.checkRows(13)

        #tdSql.query(f"select _irowts,interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:04', '2020-02-02 00:00:16') every(1h) fill(prev)")
        #tdSql.query(f"select tbname,_irowts,interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:04', '2020-02-02 00:00:16') every(1h) fill(prev)")


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
