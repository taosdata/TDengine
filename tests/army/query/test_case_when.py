from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *
from datetime import datetime, timedelta


class TDTestCase(TBase):
    """Verify the case...when... expression in the query statement
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.stable_schema = {
            "columns": {
                "ts": "timestamp",
                "c_null": "int",
                "c_bool": "bool",
                "c_tinyint": "tinyint",
                "c_smallint": "smallint",
                "c_int": "int",
                "c_bigint": "bigint",
                "c_float": "float",
                "c_double": "double",
                "c_varchar": "varchar(16)",
                "c_timestamp": "timestamp",
                "c_nchar": "nchar(16)",
                "c_utinyint": "tinyint unsigned",
                "c_usmallint": "smallint unsigned",
                "c_uint": "int unsigned",
                "c_ubigint": "bigint unsigned",
                "c_varbinary": "varbinary(16)",
                "c_geometry": "geometry(32)"
            },
            "tags": {
                "t_null": "int",
                "t_bool": "bool",
                "t_tinyint": "tinyint",
                "t_smallint": "smallint",
                "t_int": "int",
                "t_bigint": "bigint",
                "t_float": "float",
                "t_double": "double",
                "t_varchar": "varchar(16)",
                "t_timestamp": "timestamp",
                "t_nchar": "nchar(16)",
                "t_utinyint": "tinyint unsigned",
                "t_usmallint": "smallint unsigned",
                "t_uint": "int unsigned",
                "t_ubigint": "bigint unsigned",
                "t_varbinary": "varbinary(16)",
                "t_geometry": "geometry(32)"
            }
        }

    def prepare_data(self):
        # create database
        tdSql.execute("create database test_case_when;")
        tdSql.execute("use test_case_when;")
        # create stable
        columns = ",".join([f"{k} {v}" for k, v in self.stable_schema["columns"].items()])
        tags = ",".join([f"{k} {v}" for k, v in self.stable_schema["tags"].items()])
        st_sql = f"create stable st1 ({columns}) tags ({tags});"
        tdSql.execute(st_sql)
        st_sql_json_tag = f"create stable st2 ({columns}) tags (t json);"
        tdSql.execute(st_sql_json_tag)
        # create child table
        tdSql.execute("create table ct1 using st1 tags(NULL, True, 1, 1, 1, 1, 1.1, 1.11, 'aaaaaaaa', '2021-09-01 00:00:00.000', 'aaaaaaaa', 1, 1, 1, 1, \"0x06\",'POINT(1 1)');")
        tdSql.execute("""create table ct2 using st2 tags('{"name": "test", "location": "beijing"}');""")
        # insert data
        ct1_data = [
            """'2024-10-01 00:00:00.000', NULL, True, 2, 2, 2, 2, 2.2, 2.22, 'bbbbbbbb', '2021-09-01 00:00:00.000', 'bbbbbbbb', 2, 2, 2, 2, "0x07",'POINT(2 2)'""",
            """'2024-10-01 00:00:01.000', NULL, False, 3, 3, 3, 3, 3.3, 3.33, 'cccccccc', '2021-09-01 00:00:00.000', 'cccccccc', 3, 3, 3, 3, "0x08",'POINT(3 3)'""",
            """'2024-10-01 00:00:02.000', NULL, True, 4, 4, 4, 4, 4.4, 4.44, 'dddddddd', '2021-09-01 00:00:00.000', 'dddddddd', 4, 4, 4, 4, "0x09",'POINT(4 4)'""",
            """'2024-10-01 00:00:03.000', NULL, False, 5, 5, 5, 5, 5.5, 5.55, 'eeeeeeee', '2021-09-01 00:00:00.000', 'eeeeeeee', 5, 5, 5, 5, "0x0A",'POINT(5 5)'""",
            """'2024-10-01 00:00:04.000', NULL, True, 6, 6, 6, 6, 6.6, 6.66, 'ffffffff', '2021-09-01 00:00:00.000', 'ffffffff', 6, 6, 6, 6, "0x0B",'POINT(6 6)'""",
            """'2024-10-01 00:00:05.000', NULL, False, 7, 7, 7, 7, 7.7, 7.77, 'gggggggg', '2021-09-01 00:00:00.000', 'gggggggg', 7, 7, 7, 7, "0x0C",'POINT(7 7)'""",
            """'2024-10-01 00:00:06.000', NULL, True, 8, 8, 8, 8, 8.8, 8.88, 'hhhhhhhh', '2021-09-01 00:00:00.000', 'hhhhhhhh', 8, 8, 8, 8, "0x0D",'POINT(8 8)'""",
            """'2024-10-01 00:00:07.000', NULL, False, 9, 9, 9, 9, 9.9, 9.99, 'iiiiiiii', '2021-09-01 00:00:00.000', 'iiiiiiii', 9, 9, 9, 9, "0x0E",'POINT(9 9)'""",
            """'2024-10-01 00:00:08.000', NULL, True, 10, 10, 10, 10, 10.10, 10.1010, 'jjjjjjjj', '2021-09-01 00:00:00.000', 'jjjjjjjj', 10, 10, 10, 10, "0x0F",'POINT(10 10)'""",
            """'2024-10-01 00:00:09.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL"""
        ]
        ct1_insert_sql = "insert into ct1 values(%s);" % "),(".join(ct1_data)
        tdSql.execute(ct1_insert_sql)
        ct2_data = [
            """'2024-10-01 00:00:00.000', NULL, True, 2, 2, 2, 2, 2.2, 2.22, 'bbbbbbbb', '2021-09-01 00:00:00.000', 'bbbbbbbb', 2, 2, 2, 2, "0x07",'POINT(2 2)'""",
            """'2024-10-01 00:00:01.000', NULL, False, 3, 3, 3, 3, 3.3, 3.33, 'cccccccc', '2021-09-01 00:00:00.000', 'cccccccc', 3, 3, 3, 3, "0x08",'POINT(3 3)'""",
            """'2024-10-01 00:00:02.000', NULL, True, 4, 4, 4, 4, 4.4, 4.44, 'dddddddd', '2021-09-01 00:00:00.000', 'dddddddd', 4, 4, 4, 4, "0x09",'POINT(4 4)'""",
            """'2024-10-01 00:00:03.000', NULL, False, 5, 5, 5, 5, 5.5, 5.55, 'eeeeeeee', '2021-09-01 00:00:00.000', 'eeeeeeee', 5, 5, 5, 5, "0x0A",'POINT(5 5)'""",
            """'2024-10-01 00:00:04.000', NULL, True, 6, 6, 6, 6, 6.6, 6.66, 'ffffffff', '2021-09-01 00:00:00.000', 'ffffffff', 6, 6, 6, 6, "0x0B",'POINT(6 6)'""",
            """'2024-10-01 00:00:05.000', NULL, False, 7, 7, 7, 7, 7.7, 7.77, 'gggggggg', '2021-09-01 00:00:00.000', 'gggggggg', 7, 7, 7, 7, "0x0C",'POINT(7 7)'""",
            """'2024-10-01 00:00:06.000', NULL, True, 8, 8, 8, 8, 8.8, 8.88, 'hhhhhhhh', '2021-09-01 00:00:00.000', 'hhhhhhhh', 8, 8, 8, 8, "0x0D",'POINT(8 8)'""",
            """'2024-10-01 00:00:07.000', NULL, False, 9, 9, 9, 9, 9.9, 9.99, 'iiiiiiii', '2021-09-01 00:00:00.000', 'iiiiiiii', 9, 9, 9, 9, "0x0E",'POINT(9 9)'""",
            """'2024-10-01 00:00:08.000', NULL, True, 10, 10, 10, 10, 10.10, 10.1010, 'jjjjjjjj', '2021-09-01 00:00:00.000', 'jjjjjjjj', 10, 10, 10, 10, "0x0F",'POINT(10 10)'""",
            """'2024-10-01 00:00:09.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL"""
        ]
        ct2_insert_sql = "insert into ct2 values(%s);" % "),(".join(ct2_data)
        tdSql.execute(ct2_insert_sql)

    def test_case_when_statements(self):
        tdSql.execute("use test_case_when;")
        tdSql.query("select case when c_null is null then c_null else t_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_null is not null then c_null else t_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_bool is null then c_bool else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_bool is not null then c_bool else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_tinyint is null then c_tinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))

        tdSql.query("select case when c_tinyint is not null then c_tinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_smallint is null then c_smallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_smallint is not null then c_smallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])

        tdSql.query("select case when c_int is null then c_int else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))

        tdSql.query("select case when c_int is not null then c_int else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_bigint is null then c_bigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_bigint is not null then c_bigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_float is null then c_float else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_float is not null then c_float else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('2.200000',), ('3.300000',), ('4.400000',), ('5.500000',), ('6.600000',), ('7.700000',), ('8.800000',), ('9.900000',), ('10.100000',), (None,)])

        tdSql.query("select case when c_double is null then c_double else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_double is not null then c_double else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('2.220000',), ('3.330000',), ('4.440000',), ('5.550000',), ('6.660000',), ('7.770000',), ('8.880000',), ('9.990000',), ('10.101000',), (None,)])
        
        tdSql.query("select case when c_varchar is null then c_varchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_varchar is not null then c_varchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('bbbbbbbb',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])

        tdSql.query("select case when c_nchar is null then c_nchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_nchar is not null then c_nchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('bbbbbbbb',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])
        
        tdSql.query("select case when c_utinyint is null then c_utinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_utinyint is not null then c_utinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_usmallint is null then c_usmallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_usmallint is not null then c_usmallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_uint is null then c_uint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_uint is not null then c_uint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_ubigint is null then c_ubigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_ubigint is not null then c_ubigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('2',), ('3',), ('4',), ('5',), ('6',), ('7',), ('8',), ('9',), ('10',), (None,)])
        
        tdSql.error("select case when c_varbinary is null then c_varbinary else c_null end from st1;")
        tdSql.error("select case when c_varbinary is not null then c_varbinary else c_null end from st1;")
        
        tdSql.query("select case when c_null is null then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_null is not null then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(True,), (False,), (True,), (False,), (True,), (False,), (True,), (False,), (True,), (None,)])
        
        tdSql.query("select case when c_bool=true then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(None,), (False,), (None,), (False,), (None,), (False,), (None,), (False,), (None,), (None,)])
        
        tdSql.query("select case when c_bool!=true then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(True,), (None,), (True,), (None,), (True,), (None,), (True,), (None,), (True,), (None,)])
        
        tdSql.query("select case when c_tinyint=2 then c_tinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_tinyint!=2 then c_tinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])

        tdSql.query("select case when c_smallint=2 then c_smallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_smallint!=2 then c_smallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_int=2 then c_int else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_int!=2 then c_int else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_bigint=2 then c_bigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_bigint!=2 then c_bigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_float=2.2 then c_float else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res[1:] == [(0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (None,)])
        
        tdSql.query("select case when c_float!=2.2 then c_float else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res[0] == (1.0,))
        
        tdSql.query("select case when c_double=2.22 then c_double else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2.22,), (0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (None,)])
        
        tdSql.query("select case when c_double!=2.2 then c_double else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2.22,), (3.33,), (4.44,), (5.55,), (6.66,), (7.77,), (8.88,), (9.99,), (10.101,), (None,)])
        
        tdSql.query("select case when c_varchar='bbbbbbbb' then c_varchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('bbbbbbbb',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), (None,)])
        
        tdSql.query("select case when c_varchar!='bbbbbbbb' then c_varchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('true',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])
        
        tdSql.query("select case when c_timestamp='2021-09-01 00:00:00.000' then c_timestamp else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (None,)])
        
        tdSql.query("select case when c_timestamp!='2021-09-01 00:00:00.000' then c_timestamp else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_nchar='bbbbbbbb' then c_nchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('bbbbbbbb',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), (None,)])

        tdSql.query("select case when c_nchar!='bbbbbbbb' then c_nchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('true',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])
        
        tdSql.query("select case when c_utinyint=2 then c_utinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_utinyint!=2 then c_utinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_usmallint=2 then c_usmallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_usmallint!=2 then c_usmallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_uint=2 then c_uint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_uint!=2 then c_uint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_ubigint=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_ubigint!=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_ubigint=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_ubigint!=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])

        tdSql.error("select case when c_varbinary='\x30783037' then c_varbinary else c_bool end from st1;")
        tdSql.error("select case when c_varbinary!='\x30783037' then c_varbinary else c_bool end from st1;")
        
        tdSql.query("select case when c_null is null then NULL else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.res]))
        
        tdSql.query("select case when c_null is not null then NULL else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_bool=true then false else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(0,), (3,), (0,), (5,), (0,), (7,), (0,), (9,), (0,), (None,)])
        
        tdSql.query("select case when c_bool!=true then false else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (0,), (4,), (0,), (6,), (0,), (8,), (0,), (10,), (None,)])
        
        tdSql.query("select case when c_smallint=2 then c_smallint else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_smallint!=2  then c_smallint else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_int=2 then c_smallint else c_int end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_int!=2  then c_smallint else c_int end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_float=2.2 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('387897',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',)])
        
        tdSql.query("select case when c_double=2.22 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('387897',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',)])

        tdSql.query("select case when c_varchar='cccccccc' then 'test' when c_varchar='bbbbbbbb' then 'bbbb' else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('bbbb',), ('test',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',)])
        
        tdSql.query("select case when ts='2024-10-01 00:00:04.000' then 456646546 when ts>'2024-10-01 00:00:04.000' then 'after today' else 'before today or unknow date' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('before today or unknow date',), ('before today or unknow date',), ('before today or unknow date',), ('before today or unknow date',), ('456646546',), ('after today',), ('after today',), ('after today',), ('after today',), ('after today',)])

        tdSql.error("select case when c_geometry is null then c_geometry else c_null end from st1;")
        tdSql.error("select case when c_geometry is not null then c_geometry else c_null end from st1;")
        tdSql.error("select case when c_geometry='POINT(2 2)' then c_geometry else c_bool end from st1;")
        tdSql.error("select case when c_geometry!='POINT(2 2)' then c_geometry else c_bool end from st1;")

        tdSql.error("select case when t is null then t else c_null end from st2;")
        tdSql.error("select case when t is not null then t else c_null end from st2;")
        tdSql.error("select case when t->'location'='beijing' then t->'location' else c_bool end from st2;")
        tdSql.error("select case when t->'location'!='beijing' then t->'location' else c_bool end from st1;")

        tdSql.query("select case when c_float!=2.2 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('test message',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('test message',)])

        tdSql.query("select case when c_double!=2.22 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('test message',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('test message',)])

        tdSql.query("select case c_tinyint when 2 then -2147483648 when 3 then 'three' else '4294967295' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('-2147483648',), ('three',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',)])

        tdSql.query("select case c_float when 2.2 then 9.2233720e+18 when 3.3 then -9.2233720e+18 else 'aa' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.res == [('9223372000000000000.000000',), ('-9223372000000000000.000000',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',)])

        tdSql.query("select case t1.c_int when 2 then 'run' when t1.c_int is null then 'other' else t2.c_varchar end from st1 t1, st2 t2 where t1.ts=t2.ts;")
        print(tdSql.res)
        assert(tdSql.checkRows(10) and tdSql.res == [('run',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])

        tdSql.query("select avg(case when c_tinyint>=2 then c_tinyint else c_null end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.res == [(6.0,)])
        
        tdSql.query("select sum(case when c_tinyint>=2 then c_tinyint else c_null end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.res == [(54,)])
        
        tdSql.query("select first(case when c_int >=2 then 'abc' else 0 end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.res == [('abc',)])
        
        tdSql.query("select last(case when c_int >=2 then c_int else 0 end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.res == [(0,)])

    def run(self):
        self.prepare_data()
        self.test_case_when_statements()

    def stop(self):
        tdSql.execute("drop database test_case_when;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
