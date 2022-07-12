import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
import random


class TDTestCase:
    updatecfgDict = {'debugFlag': 143, "cDebugFlag": 143, "uDebugFlag": 143, "rpcDebugFlag": 143, "tmrDebugFlag": 143,
                     "jniDebugFlag": 143, "simDebugFlag": 143, "dDebugFlag": 143, "dDebugFlag": 143, "vDebugFlag": 143, "mDebugFlag": 143, "qDebugFlag": 143,
                     "wDebugFlag": 143, "sDebugFlag": 143, "tsdbDebugFlag": 143, "tqDebugFlag": 143, "fsDebugFlag": 143, "udfDebugFlag": 143}

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.tb_nums = 10
        self.row_nums = 20
        self.ts = 1434938400000
        self.time_step = 1000

    def insert_datas_and_check_abs(self ,tbnums , rownums , time_step ):
        tdLog.info(" prepare datas for auto check abs function ")

        tdSql.execute(" create database test ")
        tdSql.execute(" use test ")
        tdSql.execute(" create stable stb (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint,\
             c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)")
        for tbnum in range(tbnums):
            tbname = "sub_tb_%d"%tbnum
            tdSql.execute(" create table %s using stb tags(%d) "%(tbname , tbnum))

            ts = self.ts
            for row in range(rownums):
                ts = self.ts + time_step*row
                c1 = random.randint(0,10000)
                c2 = random.randint(0,100000)
                c3 = random.randint(0,125)
                c4 = random.randint(0,125)
                c5 = random.random()/1.0
                c6 = random.random()/1.0
                c7 = "'true'"
                c8 = "'binary_val'"
                c9 = "'nchar_val'"
                c10 = ts
                tdSql.execute(f" insert into  {tbname} values ({ts},{c1},{c2},{c3},{c4},{c5},{c6},{c7},{c8},{c9},{c10})")
        
        tdSql.execute("use test")
        tbnames = ["stb", "sub_tb_1"]
        support_types = ["BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "INT"]
        for tbname in tbnames:
            tdSql.query("desc {}".format(tbname))
            coltypes = tdSql.queryResult
            for coltype in coltypes:
                colname = coltype[0]
                abs_sql = "select abs({}) from {} order by tbname ".format(colname, tbname)
                origin_sql = "select {} from {} order by tbname".format(colname, tbname)
                if coltype[1] in support_types:
                    self.check_result_auto(origin_sql , abs_sql)
                    

    def prepare_datas(self):
        tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        tdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')

        for i in range(9):
            tdSql.execute(
                f"insert into ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            "insert into ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(
            "insert into ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(
            "insert into ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

    def prepare_tag_datas(self):
        # prepare datas
        tdSql.execute(
            "create database if not exists testdb keep 3650 duration 1000")
        tdSql.execute(" use testdb ")
        tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        tdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(
                f'create table ct{i+1} using stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            "insert into ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(
            "insert into ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(
            "insert into ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

    def check_result_auto(self, origin_query, abs_query):
        abs_result = tdSql.getResult(abs_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result = []

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                elif elem >= 0:
                    elem = elem
                else:
                    elem = -elem
                row_check.append(elem)
            auto_result.append(row_check)

        check_status = True
        for row_index, row in enumerate(abs_result):
            for col_index, elem in enumerate(row):
                if auto_result[row_index][col_index] != elem:
                    check_status = False
        if not check_status:
            tdLog.notice(
                "abs function value has not as expected , sql is \"%s\" " % abs_query)
            sys.exit(1)
        else:
            tdLog.info(
                "abs value check pass , it work as expected ,sql is \"%s\"   " % abs_query)

    def test_errors(self):
        tdSql.execute("use testdb")
        error_sql_lists = [
            "select abs from t1",
            "select abs(-+--+c1) from t1",
            # "select +-abs(c1) from t1",
            # "select ++-abs(c1) from t1",
            # "select ++--abs(c1) from t1",
            # "select - -abs(c1)*0 from t1",
            # "select abs(tbname+1) from t1 ",
            "select abs(123--123)==1 from t1",
            "select abs(c1) as 'd1' from t1",
            "select abs(c1 ,c2 ) from t1",
            "select abs(c1 ,NULL) from t1",
            "select abs(,) from t1;",
            "select abs(abs(c1) ab from t1)",
            "select abs(c1) as int from t1",
            "select abs from stb1",
            # "select abs(-+--+c1) from stb1",
            # "select +-abs(c1) from stb1",
            # "select ++-abs(c1) from stb1",
            # "select ++--abs(c1) from stb1",
            # "select - -abs(c1)*0 from stb1",
            # "select abs(tbname+1) from stb1 ",
            "select abs(123--123)==1 from stb1",
            "select abs(c1) as 'd1' from stb1",
            "select abs(c1 ,c2 ) from stb1",
            "select abs(c1 ,NULL) from stb1",
            "select abs(,) from stb1;",
            "select abs(abs(c1) ab from stb1)",
            "select abs(c1) as int from stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self):
        tdSql.execute("use testdb")
        tbnames = ["stb1", "t1", "ct1", "ct2"]
        support_types = ["BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "INT"]
        for tbname in tbnames:
            tdSql.query("desc {}".format(tbname))
            coltypes = tdSql.queryResult
            for coltype in coltypes:
                colname = coltype[0]
                abs_sql = "select abs({}) from {}".format(colname, tbname)
                if coltype[1] in support_types:
                    tdSql.query(abs_sql)
                else:
                    tdSql.error(abs_sql)

    def basic_abs_function(self):

        # basic query
        tdSql.query("select c1 from ct3")
        tdSql.checkRows(0)
        tdSql.query("select c1 from t1")
        tdSql.checkRows(12)
        tdSql.query("select c1 from stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query("select abs(c1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select abs(c2) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select abs(c3) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select abs(c4) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select abs(c5) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select abs(c6) from ct3")

        # used for regular table
        tdSql.query("select abs(c1) from t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(3, 0, 3)
        tdSql.checkData(5, 0, None)

        tdSql.query("select c1, c2, c3 , c4, c5 from t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)
        tdSql.query("select ts,c1, c2, c3 , c4, c5 from t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto("select c1, c2, c3 , c4, c5 from t1",
                               "select (c1), abs(c2) ,abs(c3), abs(c4), abs(c5) from t1")

        # used for sub table
        tdSql.query("select abs(c1) from ct1")
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(5, 0, 4)

        tdSql.query("select abs(c1) from ct1")
        self.check_result_auto("select c1, c2, c3 , c4, c5 from ct1",
                               "select (c1), abs(c2) ,abs(c3), abs(c4), abs(c5) from ct1")
        self.check_result_auto(
            "select abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))) nest_col_func from ct1;", "select c1 from ct1")

        # used for stable table

        tdSql.query("select abs(c1) from stb1")
        tdSql.checkRows(25)
        self.check_result_auto("select c1, c2, c3 , c4, c5 from ct4 ",
                               "select (c1), abs(c2) ,abs(c3), abs(c4), abs(c5) from ct4")
        self.check_result_auto(
            "select abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))) nest_col_func from ct4;", "select c1 from ct4")

        # used for not exists table
        tdSql.error("select abs(c1) from stbbb1")
        tdSql.error("select abs(c1) from tbname")
        tdSql.error("select abs(c1) from ct5")

        # mix with common col
        tdSql.query("select c1, abs(c1) from ct1")
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(4, 0, 0)
        tdSql.checkData(4, 1, 0)
        tdSql.query("select c1, abs(c1) from ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(5, 1, None)
        tdSql.query("select c1, abs(c1) from ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, 5)

        # mix with common functions
        tdSql.query("select c1, abs(c1),c5, floor(c5) from ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)

        tdSql.checkData(3, 0, 6)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(3, 2, 6.66000)
        tdSql.checkData(3, 3, 6.00000)

        tdSql.query("select c1, abs(c1),c5, floor(c5) from stb1 ")

        # mix with agg functions , not support
        tdSql.error("select c1, abs(c1),c5, count(c5) from stb1 ")
        tdSql.error("select c1, abs(c1),c5, count(c5) from ct1 ")
        tdSql.error("select abs(c1), count(c5) from stb1 ")
        tdSql.error("select abs(c1), count(c5) from ct1 ")
        tdSql.error("select c1, count(c5) from ct1 ")
        tdSql.error("select c1, count(c5) from stb1 ")

        # agg functions mix with agg functions

        tdSql.query("select max(c5), count(c5) from stb1")
        tdSql.query("select max(c5), count(c5) from ct1")

        # bug fix for count
        tdSql.query("select count(c1) from ct4 ")
        tdSql.checkData(0, 0, 9)
        tdSql.query("select count(*) from ct4 ")
        tdSql.checkData(0, 0, 12)
        tdSql.query("select count(c1) from stb1 ")
        tdSql.checkData(0, 0, 22)
        tdSql.query("select count(*) from stb1 ")
        tdSql.checkData(0, 0, 25)

        # bug fix for compute
        tdSql.query("select c1, abs(c1) -0 ,ceil(c1)-0 from ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 8.000000000)
        tdSql.checkData(1, 2, 8.000000000)

        tdSql.query(" select c1, abs(c1) -0 ,ceil(c1-0.1)-0.1 from ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 8.000000000)
        tdSql.checkData(1, 2, 7.900000000)

    def abs_func_filter(self):
        tdSql.execute("use db")
        tdSql.query(
            "select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(0, 1, 8.000000000)
        tdSql.checkData(0, 2, 8.000000000)
        tdSql.checkData(0, 3, 7.900000000)
        tdSql.checkData(0, 4, 3.000000000)

        tdSql.query(
            "select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 5.000000000)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 4.900000000)
        tdSql.checkData(0, 4, 2.000000000)

        tdSql.query(
            "select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 5.000000000)
        tdSql.checkData(0, 2, 5.000000000)
        tdSql.checkData(0, 3, 4.900000000)
        tdSql.checkData(0, 4, 2.000000000)

        tdSql.query(
            "select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from ct4 where c1>log(c1,2) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(0, 1, 88888)
        tdSql.checkData(0, 2, 8.000000000)
        tdSql.checkData(0, 3, 8.000000000)
        tdSql.checkData(0, 4, 7.900000000)
        tdSql.checkData(0, 5, 3.000000000)

    def abs_Arithmetic(self):
        pass

    def check_boundary_values(self):

        tdSql.execute("drop database if exists bound_test")
        tdSql.execute("create database if not exists bound_test")
        time.sleep(3)
        tdSql.execute("use bound_test")
        tdSql.execute(
            "create table stb_bound (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp) tags (t1 int);"
        )
        tdSql.execute(f'create table sub1_bound using stb_bound tags ( 1 )')
        tdSql.execute(
            f"insert into sub1_bound values ( now()-1s, 2147483647, 9223372036854775807, 32767, 127, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into sub1_bound values ( now()-1s, -2147483647, -9223372036854775807, -32767, -127, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into sub1_bound values ( now(), 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into sub1_bound values ( now(), -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.error(
            f"insert into sub1_bound values ( now()+1s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        self.check_result_auto("select c1, c2, c3 , c4, c5 ,c6 from sub1_bound ",
                               "select abs(c1), abs(c2) ,abs(c3), abs(c4), abs(c5) ,abs(c6) from sub1_bound")
        self.check_result_auto("select c1, c2, c3 , c3, c2 ,c1 from sub1_bound ",
                               "select abs(c1), abs(c2) ,abs(c3), abs(c3), abs(c2) ,abs(c1) from sub1_bound")
        self.check_result_auto(
            "select abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))) nest_col_func from sub1_bound;", "select abs(c1) from sub1_bound")

        # check basic elem for table per row
        tdSql.query(
            "select abs(c1) ,abs(c2) , abs(c3) , abs(c4), abs(c5), abs(c6) from sub1_bound ")
        tdSql.checkData(0, 0, 2147483647)
        tdSql.checkData(0, 1, 9223372036854775807)
        tdSql.checkData(0, 2, 32767)
        tdSql.checkData(0, 3, 127)
        tdSql.checkData(0, 4, 339999995214436424907732413799364296704.00000)
        tdSql.checkData(0, 5, 169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000)
        tdSql.checkData(1, 0, 2147483647)
        tdSql.checkData(1, 1, 9223372036854775807)
        tdSql.checkData(1, 2, 32767)
        tdSql.checkData(1, 3, 127)
        tdSql.checkData(1, 4, 339999995214436424907732413799364296704.00000)
        tdSql.checkData(1, 5, 169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000)
        tdSql.checkData(3, 0, 2147483646)
        tdSql.checkData(3, 1, 9223372036854775806)
        tdSql.checkData(3, 2, 32766)
        tdSql.checkData(3, 3, 126)
        tdSql.checkData(3, 4, 339999995214436424907732413799364296704.00000)
        tdSql.checkData(3, 5, 169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000)

        # check  + - * / in functions
        tdSql.query(
            "select abs(c1+1) ,abs(c2) , abs(c3*1) , abs(c4/2), abs(c5)/2, abs(c6) from sub1_bound ")
        tdSql.checkData(0, 0, 2147483648.000000000)
        tdSql.checkData(0, 1, 9223372036854775807)
        tdSql.checkData(0, 2, 32767.000000000)
        tdSql.checkData(0, 3, 63.500000000)
        tdSql.checkData(
            0, 4, 169999997607218212453866206899682148352.000000000)
        tdSql.checkData(0, 5, 169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000)

        tdSql.checkData(1, 0, 2147483646.000000000)
        tdSql.checkData(1, 1, 9223372036854775808.000000000)
        tdSql.checkData(1, 2, 32767.000000000)
        tdSql.checkData(1, 3, 63.500000000)
        tdSql.checkData(
            1, 4, 169999997607218212453866206899682148352.000000000)

        self.check_result_auto("select c1+1 ,c2 , c3*1 , c4/2, c5/2, c6 from sub1_bound",
                               "select abs(c1+1) ,abs(c2) , abs(c3*1) , abs(c4/2), abs(c5)/2, abs(c6) from sub1_bound ")

    def test_tag_compute_for_scalar_function(self):

        tdSql.execute("use testdb")

        self.check_result_auto("select c1, t2, t3 , t4, t5 from ct4 ",
                               "select (c1), abs(t2) ,abs(t3), abs(t4), abs(t5) from ct4")
        self.check_result_auto("select c1+2, t2+2, t3 , t4, t5 from ct4 ",
                               "select (c1)+2, abs(t2)+2 ,abs(t3), abs(t4), abs(t5) from ct4")
        self.check_result_auto("select c1+2, t2+2, t3 , t4, t5 from stb1 order by t1 ",
                               "select (c1)+2, abs(t2)+2 ,abs(t3), abs(t4), abs(t5) from stb1 order by t1")

        # bug need fix

        # tdSql.query(" select sum(c1) from stb1 where t1+10 >1; ")  #　taosd crash
        tdSql.query("select c1 ,t1 from stb1 where t1 =0 ")
        tdSql.checkRows(13)
        tdSql.query("select t1 from stb1 where t1 >0 ")
        tdSql.checkRows(12)
        tdSql.query("select t1 from stb1 where t1 =3 ")
        tdSql.checkRows(12)
        # tdSql.query("select sum(t1) from (select c1 ,t1 from stb1)")
        # tdSql.checkData(0,0,61)
        # tdSql.query("select distinct(c1) ,t1 from stb1")
        # tdSql.checkRows(20)
        tdSql.query("select max(t2) , t1 ,c1, t2 from stb1")
        tdSql.checkData(0,3,33333)

        # tag filter with abs function
        tdSql.query("select t1 from stb1 where abs(t1)=1")
        tdSql.checkRows(0)
        tdSql.query("select t1 from stb1 where abs(c1+t1)=1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)

        tdSql.query(
            "select abs(c1+t1)*t1 from stb1 where abs(c1)/floor(abs(ceil(t1))) ==1")

    def support_super_table_test(self):
        tdSql.execute(" use testdb ")
        self.check_result_auto( " select c1 from stb1 order by ts " , "select abs(c1) from stb1 order by ts" )
        self.check_result_auto( " select c1 from stb1 order by tbname " , "select abs(c1) from stb1 order by tbname" )
        self.check_result_auto( " select c1 from stb1 where c1 > 0 order by tbname  " , "select abs(c1) from stb1 where c1 > 0 order by tbname" )
        self.check_result_auto( " select c1 from stb1 where c1 > 0 order by tbname  " , "select abs(c1) from stb1 where c1 > 0 order by tbname" )

        self.check_result_auto( " select t1,c1 from stb1 order by ts " , "select t1, abs(c1) from stb1 order by ts" )
        self.check_result_auto( " select t2,c1 from stb1 order by tbname " , "select t2 ,abs(c1) from stb1 order by tbname" )
        self.check_result_auto( " select t3,c1 from stb1 where c1 > 0 order by tbname  " , "select t3 ,abs(c1) from stb1 where c1 > 0 order by tbname" )
        self.check_result_auto( " select t4,c1 from stb1 where c1 > 0 order by tbname  " , "select t4 , abs(c1) from stb1 where c1 > 0 order by tbname" )
        pass


    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()
        self.prepare_tag_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.test_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: abs basic query ============")

        self.basic_abs_function()

        tdLog.printNoPrefix("==========step5: abs boundary query ============")

        self.check_boundary_values()

        tdLog.printNoPrefix("==========step6: abs filter query ============")

        self.abs_func_filter()

        tdLog.printNoPrefix("==========step6: tag coumpute query ============")

        self.test_tag_compute_for_scalar_function()

        tdLog.printNoPrefix("==========step7: check result of query ============")

        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step)

        tdLog.printNoPrefix("==========step8: check abs result of  stable query ============")

        self.support_super_table_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
