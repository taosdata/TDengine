from new_test_framework.utils import tdLog, tdSql

class TestGeometry:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        # tdSql.init(conn.cursor())

        # WKT strings to be input as GEOMETRY type
        cls.point = "POINT (3.000000 6.000000)"
        cls.lineString = "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)"
        cls.polygon = "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))"
        cls.polygon_irregular = "POLYGON ((3 006, 5.00 6, 05.0 8, 3.000 8.00, 3.0 6))" # actually same with self.polygon

        # expected errno
        cls.errno_TSC_SQL_SYNTAX_ERROR = -2147483114
        cls.errno_PAR_SYNTAX_ERROR = -2147473920

        cls.errno_FUNTION_PARA_NUM = -2147473407
        cls.errno_FUNTION_PARA_TYPE = -2147473406
        cls.errno_FUNTION_PARA_VALUE = -2147473405

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
        # [TODO] remove ST_AsText() calling in geomFromText_test once GEOMETRY type is supported in taos-connector-python

        # column input, including NULL value
        tdSql.query(f"select ST_AsText(ST_GeomFromText(c3)), ST_AsText(c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkEqual(tdSql.queryResult[i][0], tdSql.queryResult[i][1])

        # handle irregular WKT
        tdSql.query(f"select * from t1 where c4 = ST_GeomFromText('{self.polygon_irregular}')")
        tdSql.checkRows(1)

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

    def geomRelationFunc_test(self, geomRelationFuncName, expectedResults, dbname = "db"):
        # two columns input, including NULL value
        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText(c3), c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[0][i])

        # constant and column input
        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.point}'), c4) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[1][i])

        # column and constant input
        tdSql.query(f"select {geomRelationFuncName}(c4, ST_GeomFromText('{self.point}')) from {dbname}.t1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[2][i])

        # two constants input
        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.point}'), ST_GeomFromText('{self.lineString}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], expectedResults[3])

        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.polygon}'), ST_GeomFromText('{self.point}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], expectedResults[4])

        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.polygon_irregular}'), ST_GeomFromText('{self.point}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], expectedResults[4])

        # NULL type input
        tdSql.query(f"select {geomRelationFuncName}(NULL, ST_GeomFromText('{self.point}'))")
        tdSql.checkEqual(tdSql.queryResult[0][0], None)

        tdSql.query(f"select {geomRelationFuncName}(ST_GeomFromText('{self.lineString}'), NULL)")
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
        tdSql.query(f"select c3 from {dbname}.t1 where {geomRelationFuncName}(ST_GeomFromText('{self.point}'), c4)=true")
        tdSql.checkEqual(tdSql.queryRows, expectedResults[5][0])
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, expectedResults[5][i+1])

    def check_td28365(self):
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

    def test_fun_geo_basic(self):
        """Geometry basic

        1. Create super/child/normal tables and insert geometry data
        2. Query with ST_GeomFromText() and check results
        3. Query with ST_AsText() and check results
        4. Verify TD-28365 Bug

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan create doc

        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1: create tables and insert data")
        self.prepare_data()

        tdLog.printNoPrefix("==========step2: ST_GeomFromText function test")
        self.geomFromText_test()

        tdLog.printNoPrefix("==========step3: ST_AsText function test")
        self.asText_test()

        tdLog.printNoPrefix("==========step4: ST_Intersects function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, True, None],    # constant and column
            [True, False, True, None],    # column and constant
            False,                        # two constants 1
            True,                         # two constants 2
            [2, self.point, self.polygon] # in where clause
        ]
        self.geomRelationFunc_test('ST_Intersects', expectedResults)

        tdLog.printNoPrefix("==========step5: ST_Equals function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, False, None],   # constant and column
            [True, False, False, None],   # column and constant
            False,                        # two constants 1
            False,                        # two constants 2
            [1, self.point]               # in where clause
        ]
        self.geomRelationFunc_test('ST_Equals', expectedResults)

        tdLog.printNoPrefix("==========step6: ST_Touches function test")
        expectedResults = [
            [False, False, False, None],  # two columns
            [False, False, True, None],   # constant and column
            [False, False, True, None],   # column and constant
            False,                        # two constants 1
            True,                         # two constants 2
            [1, self.polygon]             # in where clause
        ]
        self.geomRelationFunc_test('ST_Touches', expectedResults)

        tdLog.printNoPrefix("==========step7: ST_Covers function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, False, None],   # constant and column
            [True, False, True, None],    # column and constant
            False,                        # two constants 1
            True,                         # two constants 2
            [1, self.point]               # in where clause
        ]
        self.geomRelationFunc_test('ST_Covers', expectedResults)

        tdLog.printNoPrefix("==========step8: ST_Contains function test")
        expectedResults = [
            [True, True, True, None],     # two columns
            [True, False, False, None],   # constant and column
            [True, False, False, None],   # column and constant
            False,                        # two constants 1
            False,                        # two constants 2
            [1, self.point]               # in where clause
        ]
        self.geomRelationFunc_test('ST_Contains', expectedResults)

        tdLog.printNoPrefix("==========step9: ST_ContainsProperly function test")
        expectedResults = [
            [True, False, False, None],   # two columns
            [True, False, False, None],   # constant and column
            [True, False, False, None],   # column and constant
            False,                        # two constants 1
            False,                        # two constants 2
            [1, self.point]               # in where clause
        ]
        self.geomRelationFunc_test('ST_ContainsProperly', expectedResults)
        self.check_td28365()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

    def test_fun_geo_st_geomfromtext(self):
        """ Fun: st_geomfromtext()

        1. Create 1 super table 2 child tables and 1 normal table
        2. Insert geometry data and null into above tables 
        3. Insert invalid geometry data into child/normal table and expect error
        4. Use st_geomfromtext() to query geometry data from above tables and check the results
        5. Query with invalid parameter "POIN(1.0 1.5)"
        6. Query with invalid parameter "LINESTRING(1.0 1.0, 2.0 2.0, 5.0 5.0,)"
        7. Query with invalid parameter "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0))"
        8. Query with invalid parameter "XXX"

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan add doc

        """
        pass

    def test_fun_geo_st_astext(self):
        """ Fun: st_astext()

        1. Create 1 super table 2 child tables and 1 normal table
        2. Insert geometry data and null into above tables 
        3. Insert invalid geometry data into child/normal table and expect error
        4. Query with st_astext() on super/child/normal tables and check the results
        5. Query with parameter "NULL"
        6. Query with invalid parameter "XXX"
        7. Query with no parameter 
        8. Query on not geometry datatype column

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan add doc

        """
        pass