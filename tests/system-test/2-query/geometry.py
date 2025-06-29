import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

        # WKT strings
        self.point_wtk = "POINT (3.000000 6.000000)"
        self.lineString_wtk = "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)"
        self.polygon_wtk = "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))"
        
        # GeoJSON strings
        self.point_geojson = '{"type":"Point","coordinates":[3.0,6.0]}'
        self.lineString_geojson = '{"type":"LineString","coordinates":[[1.0,1.0],[2.0,2.0],[5.0,5.0]]}'
        self.polygon_geojson = '{"type":"Polygon","coordinates":[[[3.0,6.0],[5.0,6.0],[5.0,8.0],[3.0,8.0],[3.0,6.0]]]}'

        # expected errno
        self.errno_TSC_SQL_SYNTAX_ERROR = -2147483114;
        self.errno_PAR_SYNTAX_ERROR = -2147473920

        self.errno_FUNTION_PARA_NUM = -2147473407;
        self.errno_FUNTION_PARA_TYPE = -2147473406;
        self.errno_FUNTION_PARA_VALUE = -2147473405;

    def prepare_data(self, dbname = "db"):
        tdSql.execute(
            f'''create table {dbname}.t1
            (ts timestamp, c1 int, c2 float, c3 varchar(256), c4 geometry(512), c5 varchar(512))
            '''
        )
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 float, c3 varchar(256), c4 geometry(512), c5 varchar(512)) tags (t1 int)
            '''
        )
        for i in range(2):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

        values = f'''
            (now()-1s, 1, 1.1, '{self.point_wtk}', '{self.point_wtk}', '{self.point_geojson}')
            (now(), 2, 2.2, '{self.lineString_wtk}', '{self.lineString_wtk}', '{self.lineString_geojson}')
            (now()+1s, 3, 3.3, '{self.polygon_wtk}', '{self.polygon_wtk}', '{self.polygon_geojson}')
            (now()+2s, 4, 4.4, NULL, NULL, NULL)
            '''
        tdSql.execute(f"insert into {dbname}.t1 values{values}")
        tdSql.execute(f"insert into {dbname}.ct1 values{values}")
        tdSql.execute(f"insert into {dbname}.ct2 values{values}")

        # the following errors would happen when casting a string to GEOMETRY by ST_GeomFromText(), but raise an error as syntax error
        # wrong WTK
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'POIN(1.0 1.5)', NULL)", self.errno_TSC_SQL_SYNTAX_ERROR)
        # wrong WTK at all
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 'XXX', NULL)", self.errno_TSC_SQL_SYNTAX_ERROR)
        # empty WTK
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, '', NULL)", self.errno_TSC_SQL_SYNTAX_ERROR)
        # wrong type
        tdSql.error(f"insert into {dbname}.ct2 values(now(), 1, 1.1, NULL, 2, NULL)", self.errno_TSC_SQL_SYNTAX_ERROR)

    def geomFromText_test(self, dbname = "db"):
        # [ToDo] remove ST_AsText() calling in geomFromText_test once GEOMETRY type is supported in taos-connector-python

        # column input, including NULL value
        tdSql.query(f"select ST_AsText(ST_GeomFromText(c3)), ST_AsText(c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        # constant input
        tdSql.query(f"select ST_AsText(ST_GeomFromText('{self.point_wtk}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], self.point_wtk)

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
        tdSql.checkEqual(tdSql.queryResult[0][0], self.point_wtk)

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

    def geomFromGeoJSON_test(self, dbname = "db"):
        # column input, including NULL value
        tdSql.query(f"select ST_AsGeoJSON(ST_GeomFromGeoJSON(c5)), ST_AsGeoJSON(c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        # constant input
        tdSql.query(f"select ST_AsGeoJSON(ST_GeomFromGeoJSON('{self.point_geojson}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], self.point_geojson)

        # empty input
        tdSql.query(f"select ST_AsGeoJSON(ST_GeomFromGeoJSON(''))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # NULL input
        tdSql.query(f"select ST_AsGeoJSON(ST_GeomFromGeoJSON(NULL))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # wrong type input
        tdSql.error(f"select ST_GeomFromGeoJSON(2)", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select ST_GeomFromGeoJSON(c1) from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)

        # wrong number of params input
        tdSql.error(f"select ST_GeomFromGeoJSON()", self.errno_PAR_SYNTAX_ERROR)
        tdSql.error(f"select ST_GeomFromGeoJSON(c5, c5) from {dbname}.t1", self.errno_FUNTION_PARA_NUM)

        # wrong param content input
        # invalid GeoJSON
        tdSql.error("select ST_GeomFromGeoJSON('{\"type\":\"Point\"}')", self.errno_FUNTION_PARA_VALUE)
        # invalid coordinates
        tdSql.error("select ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":\"abc\"}')", self.errno_FUNTION_PARA_VALUE)
        # invalid GeoJSON at all
        tdSql.error(f"select ST_GeomFromGeoJSON('XXX')", self.errno_FUNTION_PARA_VALUE)

    def asGeoJSON_test(self, dbname = "db"):
        # column input, including NULL value
        tdSql.query(f"select ST_AsGeoJSON(c4), c5 from {dbname}.ct1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        # constant input
        tdSql.query(f"select ST_AsGeoJSON(c4) from {dbname}.ct1 where c1 = 1")
        tdSql.checkEqual(tdSql.queryResult[0][0], self.point_geojson)

        # NULL input
        tdSql.query(f"select ST_AsGeoJSON(NULL)")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # wrong type input
        tdSql.error(f"select ST_AsGeoJSON('XXX')", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select ST_AsGeoJSON(c2) from {dbname}.ct1", self.errno_FUNTION_PARA_TYPE)

        # wrong number of params input
        tdSql.error(f"select ST_AsGeoJSON() from {dbname}.ct1", self.errno_PAR_SYNTAX_ERROR)
        tdSql.error(f"select ST_AsGeoJSON(c4, c4) from {dbname}.ct1", self.errno_FUNTION_PARA_NUM)

    def geomRelationFunc_test(self, geomRelationFuncName, expectedResults, dbname = "db"):
        # two columns input, including NULL value
        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText(c3), c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[0][i])

        # constant and column input
        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.point_wtk}'), c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[1][i])

        # column and constant input
        tdSql.query(f"select {geomRelationFuncName}(c4, ST_GeomFromText('{self.point_wtk}')) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[2][i])

        # two constants input
        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.point_wtk}'), ST_GeomFromText('{self.lineString_wtk}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], expectedResults[3])

        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.polygon_wtk}'), ST_GeomFromText('{self.point_wtk}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], expectedResults[4])

        # NULL type input
        tdSql.query(f"select {geomRelationFuncName}(NULL, ST_GeomFromText('{self.point_wtk}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.lineString_wtk}'), NULL)")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        tdSql.query(f"select {geomRelationFuncName}(NULL, NULL)")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        # wrong type input
        tdSql.error(f"select {geomRelationFuncName}(c1, c4) from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select {geomRelationFuncName}(c4, c2) from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)
        tdSql.error(f"select {geomRelationFuncName}(c4, 'XXX') from {dbname}.t1", self.errno_FUNTION_PARA_TYPE)

        # wrong number of params input
        tdSql.error(f"select {geomRelationFuncName}(c4) from {dbname}.t1", self.errno_FUNTION_PARA_NUM)
        tdSql.error(f"select {geomRelationFuncName}(ST_GeomFromText(c3), c4, c4) from {dbname}.t1", self.errno_FUNTION_PARA_NUM)

        # used in where clause
        tdSql.query(f"select c3 from {dbname}.t1 where {geomRelationFuncName}(ST_GeomFromText('{self.point_wtk}'), c4)=true")
        tdSql.checkEqual(tdSql.queryRows, expectedResults[5][0])
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[5][i+1])

    def test_td28365(self):
        # verify TD-28365
        tdSql.execute("create database db2;")
        tdSql.execute("use db2;")
        tdSql.execute("create table st (ts timestamp, c1 int) tags(id int, location geometry(512));")
        tdSql.execute("create table ct1 using st tags(1, 'POINT (3.000000 6.000000)')")
        tdSql.execute("insert into ct1 values(now, 1)")
        tdSql.execute("create table ct2 using st tags(2, 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)')")
        tdSql.execute("insert into ct2 values(now, 2)")
        tdSql.execute("create table ct3 using st tags(3, 'POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))')")
        tdSql.execute("insert into ct3 values(now, 3)")
        tdSql.query("select ST_AsText(location) from st order by location;")
        tdSql.checkEqual(tdSql.queryRows, 3)
        tdLog.debug(tdSql.queryResult)
        # check geometry data
        tdSql.checkEqual(tdSql.queryResult[0][0], "POINT (3.000000 6.000000)")
        tdSql.checkEqual(tdSql.queryResult[1][0], "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)")
        tdSql.checkEqual(tdSql.queryResult[2][0], "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))")

    def run(self):
        tdSql.prepare()

        step_num = 1
        tdLog.printNoPrefix(f"==========step{step_num}: create tables and insert data")
        self.prepare_data()

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_GeomFromText function test")
        self.geomFromText_test()

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_AsText function test")
        self.asText_test()

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_GeomFromGeoJSON function test")
        self.geomFromGeoJSON_test()

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_AsGeoJSON function test")
        self.asGeoJSON_test()

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_Intersects function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, True, None],    # constant and column
            [True, False, True, None],    # column and constant
            False,                        # two constants 1
            True,                         # two constants 2
            [2, self.point_wtk, self.polygon_wtk] # in where clause
        ]
        self.geomRelationFunc_test('ST_Intersects', expectedResults)

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_Equals function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, False, None],   # constant and column
            [True, False, False, None],   # column and constant
            False,                        # two constants 1
            False,                        # two constants 2
            [1, self.point_wtk]               # in where clause
        ]
        self.geomRelationFunc_test('ST_Equals', expectedResults)

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_Touches function test")
        expectedResults = [
            [False, False, False, None],  # two columns
            [False, False, True, None],   # constant and column
            [False, False, True, None],   # column and constant
            False,                        # two constants 1
            True,                         # two constants 2
            [1, self.polygon_wtk]             # in where clause
        ]
        self.geomRelationFunc_test('ST_Touches', expectedResults)

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_Covers function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, False, None],   # constant and column
            [True, False, True, None],    # column and constant
            False,                        # two constants 1
            True,                         # two constants 2
            [1, self.point_wtk]               # in where clause
        ]
        self.geomRelationFunc_test('ST_Covers', expectedResults)

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_Contains function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, False, None],   # constant and column
            [True, False, False, None],   # column and constant
            False,                        # two constants 1
            False,                        # two constants 2
            [1, self.point_wtk]               # in where clause
        ]
        self.geomRelationFunc_test('ST_Contains', expectedResults)

        step_num += 1
        tdLog.printNoPrefix(f"==========step{step_num}: ST_ContainsProperly function test")
        expectedResults = [
            [True, False, False, None],   # two columns
            [True, False, False, None],   # constant and column
            [True, False, False, None],   # column and constant
            False,                        # two constants 1
            False,                        # two constants 2
            [1, self.point_wtk]               # in where clause
        ]
        self.geomRelationFunc_test('ST_ContainsProperly', expectedResults)
        self.test_td28365()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
