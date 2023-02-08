import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

        # WKT strings to be input as GEOMETRY type
        self.point = "POINT (3.000000 3.000000)"
        self.lineString = "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)"
        self.polygon = "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))"

        # expected errno
        self.errno_TSC_SQL_SYNTAX_ERROR = -2147483114;
        self.errno_PAR_SYNTAX_ERROR = -2147473920

        self.errno_FUNTION_PARA_NUM = -2147473407;
        self.errno_FUNTION_PARA_TYPE = -2147473406;
        self.errno_FUNTION_PARA_VALUE = -2147473405;

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
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

        values = f'''
            (now()-1s, 1, 1.1, '{self.point}', '{self.point}')
            (now(), 2, 2.2, '{self.lineString}', '{self.lineString}')
            (now()+1s, 3, 3.3, '{self.polygon}', '{self.polygon}')
            (now()+2s, 4, 4.4, NULL, NULL)
            '''
        tdSql.execute(f"insert into {dbname}.t1 values{values}")
        tdSql.execute(f"insert into {dbname}.ct1 values{values}")
        tdSql.execute(f"insert into {dbname}.ct2 values{values}")

        # the following errors would happen when casting a string to GEOMETRY by ST_GeomFromText(), but raise an error as syntax error
        # wrong WTK
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'POIN(1.0 1.5)')", self.errno_TSC_SQL_SYNTAX_ERROR)
        # wrong WTK at all
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'XXX')", self.errno_TSC_SQL_SYNTAX_ERROR)
        # empty WTK
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, '')", self.errno_TSC_SQL_SYNTAX_ERROR)
        # wrong type
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 2)", self.errno_TSC_SQL_SYNTAX_ERROR)

    def geomFromText_test(self, dbname = "db"):
        # [ToDo] remove ST_AsText() calling in geomFromText_test once GEOMETRY type is supported  in taos-connector-python

        # column input, including NULL value
        tdSql.query(f"select ST_AsText(ST_GeomFromText(c3)), ST_AsText(c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        # constant input
        tdSql.query(f"select ST_AsText(ST_GeomFromText('{self.point}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], self.point)

        # empty input
        tdSql.query(f"select ST_AsText(ST_GeomFromText(''))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # NULL input
        tdSql.query(f"select ST_AsText(ST_GeomFromText(NULL))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # wrong type input
        tdSql.error(f"select ST_GeomFromText(2)", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select ST_GeomFromText(c1) from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)

        # wrong number of params input
        tdSql.error(f"select ST_GeomFromText()", self.errno_PAR_SYNTAX_ERROR)
        tdSql.error(f"select ST_GeomFromText(c3, c3) from {dbname}.t1", self.errno_FUNTION_PARA_NUM)

        # wrong param content input
        # lack of the last letter of 'POINT'
        tdSql.error(f"select ST_GeomFromText('POIN(1.0 1.5)')", self.errno_FUNTION_PARA_VALUE)
        # redundant comma at the end
        tdSql.error(f"select ST_GeomFromText('LINESTRING(1.0 1.0, 2.0 2.0, 5.0 5.0,)')", self.errno_FUNTION_PARA_VALUE)
        # the first point and last one are not same
        tdSql.error(f"select ST_GeomFromText('POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0))')", self.errno_FUNTION_PARA_VALUE)
        # wrong WTK at all
        tdSql.error(f"select ST_GeomFromText('XXX')", self.errno_FUNTION_PARA_VALUE)

    def asText_test(self, dbname = "db"):
        # column input, including NULL value
        tdSql.query(f"select c3, ST_AsText(c4) from {dbname}.ct1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        # constant input
        tdSql.query(f"select ST_AsText(c4) from {dbname}.ct1 where c1 = 1")
        tdSql.checkEqual(tdSql.queryResult[0][0], self.point)

        # empty input should NOT happen for GEOMETRY type

        # NULL input
        tdSql.query(f"select ST_AsText(NULL)")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # wrong type input
        tdSql.error(f"select ST_AsText('XXX')", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select ST_AsText(c2) from {dbname}.ct1", self.errno_FUNTION_PARA_TYPE)

        # wrong number of params input
        tdSql.error(f"select ST_AsText() from {dbname}.ct1", self.errno_PAR_SYNTAX_ERROR)
        tdSql.error(f"select ST_AsText(c4, c4) from {dbname}.ct1", self.errno_FUNTION_PARA_NUM)

        # wrong param content input should NOT happen for GEOMETRY type

    def intersects_test(self, dbname = "db"):
        # two columns input, including NULL value
        repectedResult = [True, True, True, None]
        tdSql.query(f"select ST_Intersects(ST_GeomFromText(c3), c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, repectedResult[i])

        # constant and column input
        repectedResult = [True, True, False, None]
        tdSql.query(f"select ST_Intersects(ST_GeomFromText('{self.point}'), c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, repectedResult[i])

        # column and constant input
        tdSql.query(f"select ST_Intersects(c4, ST_GeomFromText('{self.point}')) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, repectedResult[i])

        # two constants input
        tdSql.query(f"select ST_Intersects(ST_GeomFromText('{self.point}'), ST_GeomFromText('{self.lineString}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], True)

        tdSql.query(f"select ST_Intersects(ST_GeomFromText('{self.point}'), ST_GeomFromText('{self.polygon}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], False)

        # NULL type input
        tdSql.query(f"select ST_Intersects(NULL, ST_GeomFromText('{self.point}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        tdSql.query(f"select ST_Intersects(ST_GeomFromText('{self.lineString}'), NULL)")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        tdSql.query(f"select ST_Intersects(NULL, NULL)")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # wrong type input
        tdSql.error(f"select ST_Intersects(c1, c4) from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select ST_Intersects(c4, c2) from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select ST_Intersects(c4, 'XXX') from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)

        # wrong number of params input
        tdSql.error(f"select ST_Intersects(c4) from {dbname}.t1", self.errno_FUNTION_PARA_NUM)
        tdSql.error(f"select ST_Intersects(ST_GeomFromText(c3), c4, c4) from {dbname}.t1", self.errno_FUNTION_PARA_NUM)

        # used in where clause
        repectedResult = [self.point, self.lineString]
        tdSql.query(f"select c3 from {dbname}.t1 where ST_Intersects(ST_GeomFromText('{self.point}'), c4)=true")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, repectedResult[i])

    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1: create tables and insert data")
        self.prepare_data()

        tdLog.printNoPrefix("==========step2: ST_GeomFromText function test")
        self.geomFromText_test()

        tdLog.printNoPrefix("==========step3: ST_AsText function test")
        self.asText_test()

        tdLog.printNoPrefix("==========step3: ST_Intersects function test")
        self.intersects_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
