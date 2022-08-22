import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):
        dbname = "db"
        tbname = "tb"

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


        tdLog.printNoPrefix("==========step8:test intra block interpolation")
        tdSql.execute(f"drop database {dbname}");

        tdSql.prepare()

        tdSql.execute(f"create table if not exists {dbname}.{tbname} (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10))")

        # set two data point has 10 days interval will be stored in different datablocks
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-11 00:00:05', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

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

        tdLog.printNoPrefix("==========step9:test error cases")

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

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
