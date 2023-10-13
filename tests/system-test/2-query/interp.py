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
        tbname2 = "tb2"
        tbname3 = "tb3"
        stbname = "stb"
        ctbname1 = "ctb1"
        ctbname2 = "ctb2"
        ctbname3 = "ctb3"
        num_of_ctables = 3

        tbname_null = "tb_null"
        ctbname1_null = "ctb1_null"
        ctbname2_null = "ctb2_null"
        ctbname3_null = "ctb3_null"
        stbname_null = "stb_null"

        tbname_single = "tb_single"
        ctbname1_single = "ctb1_single"
        ctbname2_single = "ctb2_single"
        ctbname3_single = "ctb3_single"
        stbname_single = "stb_single"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname}
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10), c9 tinyint unsigned, c10 smallint unsigned, c11 int unsigned, c12 bigint unsigned)
            '''
        )

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"use db")

        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar', 5, 5, 5, 5)")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar', 10, 10, 10, 10)")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar', 15, 15, 15, 15)")

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
        col_list = {'c0', 'c1', 'c2', 'c3', 'c9', 'c10', 'c11', 'c12'}
        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
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

        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
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

        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
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

        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
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

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, None)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, None)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)
        tdSql.checkData(4, 0, True)
        tdSql.checkData(5, 0, True)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, True)
        tdSql.checkData(8, 0, True)
        tdSql.checkData(9, 0, True)
        tdSql.checkData(10, 0, True)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, True)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)
        tdSql.checkData(4, 0, True)
        tdSql.checkData(5, 0, True)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, True)
        tdSql.checkData(8, 0, True)
        tdSql.checkData(9, 0, True)
        tdSql.checkData(10, 0, True)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, True)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)
        tdSql.checkData(4, 0, True)
        tdSql.checkData(5, 0, True)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, True)
        tdSql.checkData(8, 0, True)
        tdSql.checkData(9, 0, True)
        tdSql.checkData(10, 0, True)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, True)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, false)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, False)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, False)
        tdSql.checkData(3, 0, False)
        tdSql.checkData(4, 0, False)
        tdSql.checkData(5, 0, False)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, False)
        tdSql.checkData(8, 0, False)
        tdSql.checkData(9, 0, False)
        tdSql.checkData(10, 0, False)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, False)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, None)

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

        ## test fill value with string
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 'abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '123')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 123)
        tdSql.checkData(1, 0, 123)
        tdSql.checkData(2, 0, 123)
        tdSql.checkData(3, 0, 123)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '123.123')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 123)
        tdSql.checkData(1, 0, 123)
        tdSql.checkData(2, 0, 123)
        tdSql.checkData(3, 0, 123)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '12abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 12)
        tdSql.checkData(1, 0, 12)
        tdSql.checkData(2, 0, 12)
        tdSql.checkData(3, 0, 12)

        ## test fill value with scalar expression
        # data types
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c2) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c3) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3.0)
        tdSql.checkData(1, 0, 3.0)
        tdSql.checkData(2, 0, 3.0)
        tdSql.checkData(3, 0, 3.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3.0)
        tdSql.checkData(1, 0, 3.0)
        tdSql.checkData(2, 0, 3.0)
        tdSql.checkData(3, 0, 3.0)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)

        # expr types
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1.0 + 2.0)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2.5)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + '2')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + '2.0')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '3' + 'abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '2' + '1abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)


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

        tdLog.printNoPrefix("==========step8:test _irowts,_isfilled with interp")

        # fill null
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(null)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(4, 1, False)
        tdSql.checkData(5, 1, True)
        tdSql.checkData(6, 1, True)
        tdSql.checkData(7, 1, True)
        tdSql.checkData(8, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        tdSql.checkRows(13)
        tdSql.checkCols(3)

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

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  False)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  False)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, True)
        tdSql.checkData(11, 1, False)
        tdSql.checkData(12, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        # fill value
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(value, 1)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkCols(3)

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

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  False)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  False)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, True)
        tdSql.checkData(11, 1, False)
        tdSql.checkData(12, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)

        # fill prev
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(prev)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(prev)")
        tdSql.checkRows(12)
        tdSql.checkCols(3)

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

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, False)
        tdSql.checkData(11, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(prev)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        # fill next
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(next)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(next)")
        tdSql.checkRows(12)
        tdSql.checkCols(3)

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

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  False)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  False)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, True)
        tdSql.checkData(11, 1, False)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(next)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)

        # fill linear
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(3)

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

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, False)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)

        # multiple _irowts,_isfilled
        tdSql.query(f"select interp(c0),_irowts,_isfilled from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(3)

        tdSql.checkData(0,  1, '2020-02-01 00:00:05.000')
        tdSql.checkData(1,  1, '2020-02-01 00:00:06.000')
        tdSql.checkData(2,  1, '2020-02-01 00:00:07.000')
        tdSql.checkData(3,  1, '2020-02-01 00:00:08.000')
        tdSql.checkData(4,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(5,  1, '2020-02-01 00:00:10.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:11.000')
        tdSql.checkData(7,  1, '2020-02-01 00:00:12.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:13.000')
        tdSql.checkData(9,  1, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        tdSql.checkData(0,  2,  False)
        tdSql.checkData(1,  2,  True)
        tdSql.checkData(2,  2,  True)
        tdSql.checkData(3,  2,  True)
        tdSql.checkData(4,  2,  True)
        tdSql.checkData(5,  2,  False)
        tdSql.checkData(6,  2,  True)
        tdSql.checkData(7,  2,  True)
        tdSql.checkData(8,  2,  True)
        tdSql.checkData(9,  2,  True)
        tdSql.checkData(10, 2,  False)

        tdSql.query(f"select _irowts, _isfilled, interp(c0), interp(c0), _isfilled, _irowts from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(6)

        cols = (0, 5)
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

        cols = (1, 4)
        for i in cols:
          tdSql.checkData(0,  i,  False)
          tdSql.checkData(1,  i,  True)
          tdSql.checkData(2,  i,  True)
          tdSql.checkData(3,  i,  True)
          tdSql.checkData(4,  i,  True)
          tdSql.checkData(5,  i,  False)
          tdSql.checkData(6,  i,  True)
          tdSql.checkData(7,  i,  True)
          tdSql.checkData(8,  i,  True)
          tdSql.checkData(9,  i,  True)
          tdSql.checkData(10, i,  False)


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
            f'''create table if not exists {dbname}.{ctbname2} using {dbname}.{stbname} tags(2)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname3} using {dbname}.{stbname} tags(3)
            '''
        )

        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:01', 1, 1, 1, 1, 1.0, 1.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:07', 7, 7, 7, 7, 7.0, 7.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:13', 13, 13, 13, 13, 13.0, 13.0, true, 'varchar', 'nchar')")

        tdSql.execute(f"insert into {dbname}.{ctbname2} values ('2020-02-01 00:00:03', 3, 3, 3, 3, 3.0, 3.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname2} values ('2020-02-01 00:00:09', 9, 9, 9, 9, 9.0, 9.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname2} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdSql.execute(f"insert into {dbname}.{ctbname3} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname3} values ('2020-02-01 00:00:11', 11, 11, 11, 11, 11.0, 11.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{ctbname3} values ('2020-02-01 00:00:17', 17, 17, 17, 17, 17.0, 17.0, true, 'varchar', 'nchar')")


        tdSql.execute(f"flush database {dbname}");

        # test fill null

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(null)")
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
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(value, 1)")
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

        # | . | {} | . |
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
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(prev)")
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
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(next)")
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
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(linear)")
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
        tdSql.query(f"select _irowts, interp(c0), _irowts, interp(c1), _irowts from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(value, 123, 456)")
        tdSql.checkRows(61)
        tdSql.checkCols(5)
        tdSql.checkData(0,  1, 0)    #
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
        tdSql.checkData(20, 1, 20)   #
        tdSql.checkData(21, 1, 123)
        tdSql.checkData(24, 1, 123)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 123)
        tdSql.checkData(29, 1, 123)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 123)
        tdSql.checkData(34, 1, 123)
        tdSql.checkData(35, 1, 35)   #
        tdSql.checkData(36, 1, 123)
        tdSql.checkData(39, 1, 123)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 123)
        tdSql.checkData(44, 1, 123)
        tdSql.checkData(45, 1, None) #
        tdSql.checkData(46, 1, 123)
        tdSql.checkData(49, 1, 123)
        tdSql.checkData(50, 1, 50)   #
        tdSql.checkData(51, 1, 123)
        tdSql.checkData(54, 1, 123)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(59, 1, 123)
        tdSql.checkData(60, 1, 55)   #

        tdSql.checkData(0,  3, None) #
        tdSql.checkData(1,  3, 456)
        tdSql.checkData(4,  3, 456)
        tdSql.checkData(5,  3, None) #
        tdSql.checkData(6,  3, 456)
        tdSql.checkData(9,  3, 456)
        tdSql.checkData(10, 3, 10)   #
        tdSql.checkData(11, 3, 456)
        tdSql.checkData(14, 3, 456)
        tdSql.checkData(15, 3, None) #
        tdSql.checkData(16, 3, 456)
        tdSql.checkData(19, 3, 456)
        tdSql.checkData(20, 3, None) #
        tdSql.checkData(21, 3, 456)
        tdSql.checkData(24, 3, 456)
        tdSql.checkData(25, 3, None) #
        tdSql.checkData(26, 3, 456)
        tdSql.checkData(29, 3, 456)
        tdSql.checkData(30, 3, 30)   #
        tdSql.checkData(31, 3, 456)
        tdSql.checkData(34, 3, 456)
        tdSql.checkData(35, 3, None) #
        tdSql.checkData(36, 3, 456)
        tdSql.checkData(39, 3, 456)
        tdSql.checkData(40, 3, 40)   #
        tdSql.checkData(41, 3, 456)
        tdSql.checkData(44, 3, 456)
        tdSql.checkData(45, 3, 45)   #
        tdSql.checkData(46, 3, 456)
        tdSql.checkData(49, 3, 456)
        tdSql.checkData(50, 3, None) #
        tdSql.checkData(51, 3, 456)
        tdSql.checkData(54, 3, 456)
        tdSql.checkData(55, 3, None) #
        tdSql.checkData(56, 3, 456)
        tdSql.checkData(59, 3, 456)
        tdSql.checkData(60, 3, 60)   #

        tdSql.query(f"select _isfilled, interp(c0), _isfilled, interp(c1), _isfilled from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(value, 123 + 123, 234 + 234)")
        tdSql.checkRows(61)
        tdSql.checkCols(5)
        tdSql.checkData(0,  1, 0)    #
        tdSql.checkData(1,  1, 246)
        tdSql.checkData(4,  1, 246)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, 246)
        tdSql.checkData(9,  1, 246)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, 246)
        tdSql.checkData(14, 1, 246)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, 246)
        tdSql.checkData(19, 1, 246)
        tdSql.checkData(20, 1, 20)   #
        tdSql.checkData(21, 1, 246)
        tdSql.checkData(24, 1, 246)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 246)
        tdSql.checkData(29, 1, 246)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 246)
        tdSql.checkData(34, 1, 246)
        tdSql.checkData(35, 1, 35)   #
        tdSql.checkData(36, 1, 246)
        tdSql.checkData(39, 1, 246)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 246)
        tdSql.checkData(44, 1, 246)
        tdSql.checkData(45, 1, None) #
        tdSql.checkData(46, 1, 246)
        tdSql.checkData(49, 1, 246)
        tdSql.checkData(50, 1, 50)   #
        tdSql.checkData(51, 1, 246)
        tdSql.checkData(54, 1, 246)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(59, 1, 246)
        tdSql.checkData(60, 1, 55)   #

        tdSql.checkData(0,  3, None) #
        tdSql.checkData(1,  3, 468)
        tdSql.checkData(4,  3, 468)
        tdSql.checkData(5,  3, None) #
        tdSql.checkData(6,  3, 468)
        tdSql.checkData(9,  3, 468)
        tdSql.checkData(10, 3, 10)   #
        tdSql.checkData(11, 3, 468)
        tdSql.checkData(14, 3, 468)
        tdSql.checkData(15, 3, None) #
        tdSql.checkData(16, 3, 468)
        tdSql.checkData(19, 3, 468)
        tdSql.checkData(20, 3, None) #
        tdSql.checkData(21, 3, 468)
        tdSql.checkData(24, 3, 468)
        tdSql.checkData(25, 3, None) #
        tdSql.checkData(26, 3, 468)
        tdSql.checkData(29, 3, 468)
        tdSql.checkData(30, 3, 30)   #
        tdSql.checkData(31, 3, 468)
        tdSql.checkData(34, 3, 468)
        tdSql.checkData(35, 3, None) #
        tdSql.checkData(36, 3, 468)
        tdSql.checkData(39, 3, 468)
        tdSql.checkData(40, 3, 40)   #
        tdSql.checkData(41, 3, 468)
        tdSql.checkData(44, 3, 468)
        tdSql.checkData(45, 3, 45)   #
        tdSql.checkData(46, 3, 468)
        tdSql.checkData(49, 3, 468)
        tdSql.checkData(50, 3, None) #
        tdSql.checkData(51, 3, 468)
        tdSql.checkData(54, 3, 468)
        tdSql.checkData(55, 3, None) #
        tdSql.checkData(56, 3, 468)
        tdSql.checkData(59, 3, 468)
        tdSql.checkData(60, 3, 60)   #

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

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(value, 1, 1, 1, 1)")
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

        tdLog.printNoPrefix("==========step12:test interp with boolean type")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname2}
            (ts timestamp, c0 bool)
            '''
        )


        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:01', false)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:03', true)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:05', false)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:07', true)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:09', true)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:11', false)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:13', false)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:15', NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname2} values ('2020-02-02 00:00:17', NULL)")

        # test fill null
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(NULL)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, None)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, None)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        # test fill prev
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(18)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:01.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)

        tdSql.checkData(17, 0, '2020-02-02 00:00:18.000')

        # test fill next
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(18)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)

        tdSql.checkData(17, 0, '2020-02-02 00:00:17.000')

        # test fill value
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, 1234)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, True)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, True)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, True)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, True)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, True)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, True)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, false)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, true)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, True)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, True)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, True)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, True)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, True)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, True)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, '0')")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, '123')")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, True)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, True)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, True)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, True)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, True)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, True)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, 'abc')")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, NULL)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, None)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, None)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        # test fill linear
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:01.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, False)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, None)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)

        tdSql.checkData(16, 0, '2020-02-02 00:00:17.000')

        tdLog.printNoPrefix("==========step13:test error cases")

        tdSql.error(f"select interp(c0) from {dbname}.{tbname}")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05')")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} every(1s) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where ts >= '2020-02-10 00:00:05' and ts <= '2020-02-15 00:00:05' every(1s) fill(null)")

        # input can only be numerical types
        tdSql.error(f"select interp(ts) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        #tdSql.error(f"select interp(c6) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c7) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c8) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # input can only be columns
        tdSql.error(f"select interp(1) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(1.5) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(true) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(false) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp('abcd') from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp('中文字符') from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # invalid pseudo column usage
        tdSql.error(f"select interp(_irowts) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(_isfilled) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where _isfilled = true range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where _irowts > 0 range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # fill value number mismatch
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(value, 1, 2)")
        tdSql.error(f"select interp(c0), interp(c1) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(value, 1)")




        tdLog.printNoPrefix("==========step13:test stable cases")

        # select interp from supertable
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.checkRows(19)

        tdSql.checkData(0,  2, None)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, None)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, None)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, 17)
        tdSql.checkData(18, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(19)

        tdSql.checkData(0,  2, 0)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 0)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 0)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, 0)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, 0)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 0)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, 0)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, 0)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, 0)
        tdSql.checkData(17, 2, 17)
        tdSql.checkData(18, 2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(18)

        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 11)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, 13)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, 15)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, 17)
        tdSql.checkData(17, 2, 17)

        tdSql.checkData(17, 0, '2020-02-01 00:00:18.000')
        tdSql.checkData(17, 1, True)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(18)

        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 11)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, 13)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, 15)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, 17)
        tdSql.checkData(17, 2, 17)

        tdSql.checkData(17, 0, '2020-02-01 00:00:17.000')
        tdSql.checkData(17, 1, False)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 2)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 4)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 10)
        tdSql.checkData(10, 2, 11)
        tdSql.checkData(11, 2, 12)
        tdSql.checkData(12, 2, 13)
        tdSql.checkData(13, 2, 14)
        tdSql.checkData(14, 2, 15)
        tdSql.checkData(15, 2, 16)
        tdSql.checkData(16, 2, 17)

        # select interp from supertable partition by tbname

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")

        point_idx = {1, 7, 13, 22, 28, 34, 43, 49, 55}
        point_dict = {1:1, 7:7, 13:13, 22:3, 28:9, 34:15, 43:5, 49:11, 55:17}
        rows_per_partition = 19
        tdSql.checkRows(rows_per_partition * num_of_ctables)
        for i in range(num_of_ctables):
          for j in range(rows_per_partition):
            row = j + i * rows_per_partition
            tdSql.checkData(row,  0, f'ctb{i + 1}')
            tdSql.checkData(j,  1, f'2020-02-01 00:00:{j}.000')
            if row in point_idx:
                tdSql.checkData(row, 2, False)
            else:
                tdSql.checkData(row, 2, True)

            if row in point_idx:
                tdSql.checkData(row, 3, point_dict[row])
            else:
                tdSql.checkData(row, 3, None)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")

        point_idx = {1, 7, 13, 22, 28, 34, 43, 49, 55}
        point_dict = {1:1, 7:7, 13:13, 22:3, 28:9, 34:15, 43:5, 49:11, 55:17}
        rows_per_partition = 19
        tdSql.checkRows(rows_per_partition * num_of_ctables)
        for i in range(num_of_ctables):
          for j in range(rows_per_partition):
            row = j + i * rows_per_partition
            tdSql.checkData(row,  0, f'ctb{i + 1}')
            tdSql.checkData(j,  1, f'2020-02-01 00:00:{j}.000')
            if row in point_idx:
                tdSql.checkData(row, 2, False)
            else:
                tdSql.checkData(row, 2, True)

            if row in point_idx:
                tdSql.checkData(row, 3, point_dict[row])
            else:
                tdSql.checkData(row, 3, 0)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")

        tdSql.checkRows(48)
        for i in range(0, 18):
            tdSql.checkData(i, 0, 'ctb1')

        for i in range(18, 34):
            tdSql.checkData(i, 0, 'ctb2')

        for i in range(34, 48):
            tdSql.checkData(i, 0, 'ctb3')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:18.000')

        tdSql.checkData(18, 1, '2020-02-01 00:00:03.000')
        tdSql.checkData(33, 1, '2020-02-01 00:00:18.000')

        tdSql.checkData(34, 1, '2020-02-01 00:00:05.000')
        tdSql.checkData(47, 1, '2020-02-01 00:00:18.000')

        for i in range(0, 6):
            tdSql.checkData(i, 3, 1)

        for i in range(6, 12):
            tdSql.checkData(i, 3, 7)

        for i in range(12, 18):
            tdSql.checkData(i, 3, 13)

        for i in range(18, 24):
            tdSql.checkData(i, 3, 3)

        for i in range(24, 30):
            tdSql.checkData(i, 3, 9)

        for i in range(30, 34):
            tdSql.checkData(i, 3, 15)

        for i in range(34, 40):
            tdSql.checkData(i, 3, 5)

        for i in range(40, 46):
            tdSql.checkData(i, 3, 11)

        for i in range(46, 48):
            tdSql.checkData(i, 3, 17)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")

        tdSql.checkRows(48)
        for i in range(0, 14):
            tdSql.checkData(i, 0, 'ctb1')

        for i in range(14, 30):
            tdSql.checkData(i, 0, 'ctb2')

        for i in range(30, 48):
            tdSql.checkData(i, 0, 'ctb3')

        tdSql.checkData(0,  1, '2020-02-01 00:00:00.000')
        tdSql.checkData(13, 1, '2020-02-01 00:00:13.000')

        tdSql.checkData(14, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(29, 1, '2020-02-01 00:00:15.000')

        tdSql.checkData(30, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(47, 1, '2020-02-01 00:00:17.000')

        for i in range(0, 2):
            tdSql.checkData(i, 3, 1)

        for i in range(2, 8):
            tdSql.checkData(i, 3, 7)

        for i in range(8, 14):
            tdSql.checkData(i, 3, 13)

        for i in range(14, 18):
            tdSql.checkData(i, 3, 3)

        for i in range(18, 24):
            tdSql.checkData(i, 3, 9)

        for i in range(24, 30):
            tdSql.checkData(i, 3, 15)

        for i in range(30, 36):
            tdSql.checkData(i, 3, 5)

        for i in range(36, 42):
            tdSql.checkData(i, 3, 11)

        for i in range(42, 48):
            tdSql.checkData(i, 3, 17)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")

        tdSql.checkRows(39)
        for i in range(0, 13):
            tdSql.checkData(i, 0, 'ctb1')

        for i in range(13, 26):
            tdSql.checkData(i, 0, 'ctb2')

        for i in range(26, 39):
            tdSql.checkData(i, 0, 'ctb3')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(12, 1, '2020-02-01 00:00:13.000')

        tdSql.checkData(13, 1, '2020-02-01 00:00:03.000')
        tdSql.checkData(25, 1, '2020-02-01 00:00:15.000')

        tdSql.checkData(26, 1, '2020-02-01 00:00:05.000')
        tdSql.checkData(38, 1, '2020-02-01 00:00:17.000')

        for i in range(0, 13):
            tdSql.checkData(i, 3, i + 1)

        for i in range(13, 26):
            tdSql.checkData(i, 3, i - 10)

        for i in range(26, 39):
            tdSql.checkData(i, 3, i - 21)

        # select interp from supertable partition by column

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.checkRows(171)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(171)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(90)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(90)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(9)

        # select interp from supertable partition by tag

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.checkRows(57)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(57)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(48)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(48)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(39)

        # select interp from supertable filter

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(27)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(27)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        # select interp from supertable filter limit

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 13")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 20")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 13")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 40")
        tdSql.checkRows(39)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        # select interp from supertable with scalar expression

        tdSql.query(f"select _irowts, _isfilled, interp(1 + 1) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, 2.0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0 + 1) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0 * 2) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, (i + 1) * 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0 + c1) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, (i + 1) * 2)

        # check duplicate timestamp

        # add duplicate timestamp for different child tables
        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:15') every(1s) fill(null)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")

        tdLog.printNoPrefix("======step 14: test interp ignore null values")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname_null}
            (ts timestamp, c0 int, c1 float, c2 bool)
            '''
        )

        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:01', 1,    1.0,  true)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:02', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:03', 3,    3.0,  false)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:04', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:05', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:06', 6,    6.0,  true)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:07', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:08', 8,    8.0,  false)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:09', 9,    9.0,  true)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:10', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{tbname_null} values ('2020-02-02 00:00:11', NULL, NULL, NULL)")

        tdSql.execute(
            f'''create table if not exists {dbname}.{stbname_null}
            (ts timestamp, c0 int, c1 float, c2 bool) tags (t0 int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname1_null} using {dbname}.{stbname_null} tags(1)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname2_null} using {dbname}.{stbname_null} tags(2)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname3_null} using {dbname}.{stbname_null} tags(3)
            '''
        )

        tdSql.execute(f"insert into {dbname}.{ctbname1_null} values ('2020-02-01 00:00:01', 1, 1.0, true)")
        tdSql.execute(f"insert into {dbname}.{ctbname1_null} values ('2020-02-01 00:00:07', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{ctbname1_null} values ('2020-02-01 00:00:13', 13, 13.0, false)")

        tdSql.execute(f"insert into {dbname}.{ctbname2_null} values ('2020-02-01 00:00:03', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{ctbname2_null} values ('2020-02-01 00:00:09', 9, 9.0, true)")
        tdSql.execute(f"insert into {dbname}.{ctbname2_null} values ('2020-02-01 00:00:15', 15, 15.0, false)")

        tdSql.execute(f"insert into {dbname}.{ctbname3_null} values ('2020-02-01 00:00:05', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{ctbname3_null} values ('2020-02-01 00:00:11', NULL, NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.{ctbname3_null} values ('2020-02-01 00:00:17', NULL, NULL, NULL)")

        # fill null
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(27)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 18):
            tdSql.checkData(i, 0, 'ctb2_null')

        for i in range(18, 27):
            tdSql.checkData(i, 0, 'ctb3_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        tdSql.checkData(18, 1, '2020-02-01 00:00:01.000')
        tdSql.checkData(26, 1, '2020-02-01 00:00:17.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(18)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 17):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        # fill value
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 0)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 0)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 0)
        tdSql.checkData(10, 2, 0)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 0)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 0)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 0)
        tdSql.checkData(10, 2, 0)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 0)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 0)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 0)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 0)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 0)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 0)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(27)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 18):
            tdSql.checkData(i, 0, 'ctb2_null')

        for i in range(18, 27):
            tdSql.checkData(i, 0, 'ctb3_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        tdSql.checkData(18, 1, '2020-02-01 00:00:01.000')
        tdSql.checkData(26, 1, '2020-02-01 00:00:17.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(18)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 18):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        # fill prev
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 3)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 6)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 9)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 3)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 6)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 9)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 1)
        tdSql.checkData(3,  2, 1)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 9)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 15)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 1)
        tdSql.checkData(3,  2, 1)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 9)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 15)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(14)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 13):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(13, 1, '2020-02-01 00:00:17.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(14)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 13):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(13, 1, '2020-02-01 00:00:17.000')

        # fill next
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 6)
        tdSql.checkData(4,  2, 6)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 8)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 6)
        tdSql.checkData(4,  2, 6)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 8)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 9)
        tdSql.checkData(2,  2, 9)
        tdSql.checkData(3,  2, 9)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 13)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 9)
        tdSql.checkData(2,  2, 9)
        tdSql.checkData(3,  2, 9)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 13)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(15)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 15):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(14, 1, '2020-02-01 00:00:15.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(15)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 15):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(14, 1, '2020-02-01 00:00:15.000')

        # fill linear
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 2)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 4)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 2)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 4)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 5)
        tdSql.checkData(3,  2, 7)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 11)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)


        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 5)
        tdSql.checkData(3,  2, 7)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 11)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(11)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 11):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(11)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 11):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        # multiple column with ignoring null value is not allowed

        tdSql.query(f"select _irowts, _isfilled, interp(c0), interp(c1), interp(c2) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0), interp(c1, 0), interp(c2, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0), interp(c1, 0), interp(c2) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0), interp(c1, 1), interp(c2) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0, 1), interp(c1, 0), interp(c2, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0), interp(c1, 0), interp(c2, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0, 1), interp(c1, 1), interp(c2, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")



        tdLog.printNoPrefix("======step 15: test interp pseudo columns")
        tdSql.error(f"select _irowts, c6 from {dbname}.{tbname}")

        tdLog.printNoPrefix("======step 16: test interp in nested query")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{stbname}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{stbname}) partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1}) partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union select * from {dbname}.{ctbname2}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union select * from {dbname}.{ctbname2} order by ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2} order by ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2} order by ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select {ctbname1}.ts,{ctbname1}.c0 from {dbname}.{ctbname1}, {dbname}.{ctbname2} where {ctbname1}.ts = {ctbname2}.ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdLog.printNoPrefix("======step 17: test interp single point")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname_single}
            (ts timestamp, c0 int)
            '''
        )

        tdSql.execute(f"insert into {dbname}.{tbname_single} values ('2020-02-01 00:00:01', 1)")
        tdSql.execute(f"insert into {dbname}.{tbname_single} values ('2020-02-01 00:00:03', 3)")
        tdSql.execute(f"insert into {dbname}.{tbname_single} values ('2020-02-01 00:00:05', 5)")

        tdSql.execute(
            f'''create table if not exists {dbname}.{stbname_single}
            (ts timestamp, c0 int, c1 float, c2 bool) tags (t0 int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname1_single} using {dbname}.{stbname_single} tags(1)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname2_single} using {dbname}.{stbname_single} tags(2)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname3_single} using {dbname}.{stbname_single} tags(3)
            '''
        )

        tdSql.execute(f"insert into {dbname}.{ctbname1_single} values ('2020-02-01 00:00:01', 1, 1.0, true)")

        tdSql.execute(f"insert into {dbname}.{ctbname2_single} values ('2020-02-01 00:00:03', 3, 3.0, false)")

        tdSql.execute(f"insert into {dbname}.{ctbname3_single} values ('2020-02-01 00:00:05', 5, 5.0, true)")

        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(prev)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(prev)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(next)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(next)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(linear)")
        tdSql.checkRows(0)

        #super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(prev)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(prev)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(next)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(next)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(linear)")
        tdSql.checkRows(0)

        # partition by tbname
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(null)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(null)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(value,0)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(prev)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(prev)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(prev)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, False)
        tdSql.checkData(1,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03') fill(prev)")
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, False)
        tdSql.checkData(1,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04') fill(prev)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2,  1, False)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05') fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2,  1, False)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(next)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(next)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03') fill(next)")
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03') fill(linear)")
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(linear)")
        tdSql.checkRows(0)

        #### TS-3799 ####

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname3} (ts timestamp, c0 double)'''
        )

        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:51.000000000', 4.233947800000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:52.000000000', 3.606781000000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:52.500000000', 3.162353500000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:53.000000000', 3.162292500000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:53.500000000', 4.998230000000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:54.400000000', 8.800414999999999)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:54.900000000', 8.853271500000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:55.900000000', 7.507751500000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:56.400000000', 7.510681000000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:56.900000000', 7.841614000000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:57.900000000', 8.153809000000001)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:58.500000000', 6.866455000000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-06 23:59:59.000000000', 6.869140600000000)")
        tdSql.execute(f"insert into {dbname}.{tbname3} values ('2023-08-07 00:00:00.000000000', 0.261475000000001)")

        tdSql.query(f"select _irowts, interp(c0) from {dbname}.{tbname3} range('2023-08-06 23:59:00','2023-08-06 23:59:59') every(1m) fill(next)")
        tdSql.checkRows(1);
        tdSql.checkData(0,  0, '2023-08-06 23:59:00')
        tdSql.checkData(0,  1, 4.233947800000000)

        tdSql.query(f"select _irowts, interp(c0) from {dbname}.{tbname3} range('2023-08-06 23:59:00','2023-08-06 23:59:59') every(1m) fill(value, 1)")
        tdSql.checkRows(1);
        tdSql.checkData(0,  0, '2023-08-06 23:59:00')
        tdSql.checkData(0,  1, 1)

        tdSql.query(f"select _irowts, interp(c0) from {dbname}.{tbname3} range('2023-08-06 23:59:00','2023-08-06 23:59:59') every(1m) fill(null)")
        tdSql.checkRows(1);
        tdSql.checkData(0,  0, '2023-08-06 23:59:00')
        tdSql.checkData(0,  1, None)



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
