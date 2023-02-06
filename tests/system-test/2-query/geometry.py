import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def prepare_data(self, dbname = "db"):
        tdSql.execute(
            f'''create table {dbname}.t1
            (ts timestamp, c1 int, c2 float, c3 varchar(256), c4 geometry(512))
            '''
        )
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 float, c3 varchar(256), c4 geometry(512)) tags (t1 int)
            '''
        )
        for i in range(2):
            tdSql.execute(
                f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )'
            )

        # WKT strings
        point = "'POINT (1.000000 1.500000)'"
        lineString = "'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)'"
        polygon = "'POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))'"
        values = f'''
            (now()-1s, 1, 1.1, {point}, {point})
            (now(), 2, 2.2, {lineString}, {lineString})
            (now()+1s, 3, 3.3, {polygon}, {polygon})
            (now()+2s, 4, 4.4, NULL, NULL)
            '''
        tdSql.execute(f"insert into {dbname}.t1 values{values}")
        tdSql.execute(f"insert into {dbname}.ct1 values{values}")
        tdSql.execute(f"insert into {dbname}.ct2 values{values}")

        # lack of the last letter of 'POINT'
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'POIN(1.0 1.5)')")
        # redundant comma at the end
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'LINESTRING(1.0 1.0, 2.0 2.0, 5.0 5.0,)')")
        #  the first point and last one are not same
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0))')")
        # wrong WTK at all
        tdSql.error(f"insert into {dbname}.ct2 values (now(), 1, 1.1, NULL, 'XXX')")

    def basic_test(self, dbname = "db"):
        tdSql.query(f"select c3, ST_AsText(c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        tdSql.query(f"select c3, ST_AsText(c4) from {dbname}.ct1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1: create tables and insert data")
        self.prepare_data()

        tdLog.printNoPrefix("==========step2: basic test on geometry")
        self.basic_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
