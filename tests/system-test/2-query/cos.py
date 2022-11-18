import taos
import sys
import datetime
import inspect
import math
from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:
    # updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    # "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    # "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143}
    def init(self, conn,  logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def prepare_datas(self, dbname="db"):
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
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

    def check_result_auto_cos(self ,origin_query , pow_query):

        pow_result = tdSql.getResult(pow_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                else:
                    elem = math.cos(elem)
                row_check.append(elem)
            auto_result.append(row_check)

        tdSql.query(pow_query)
        for row_index , row in enumerate(pow_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index,col_index,auto_result[row_index][col_index])

    def test_errors(self, dbname="db"):
        error_sql_lists = [
            f"select cos from {dbname}.t1",
            # f"select cos(-+--+c1 ) from {dbname}.t1",
            # f"select +-cos(c1) from {dbname}.t1",
            # f"select ++-cos(c1) from {dbname}.t1",
            # f"select ++--cos(c1) from {dbname}.t1",
            # f"select - -cos(c1)*0 from {dbname}.t1",
            # f"select cos(tbname+1) from {dbname}.t1 ",
            f"select cos(123--123)==1 from {dbname}.t1",
            f"select cos(c1) as 'd1' from {dbname}.t1",
            f"select cos(c1 ,c2) from {dbname}.t1",
            f"select cos(c1 ,NULL ) from {dbname}.t1",
            f"select cos(,) from {dbname}.t1;",
            f"select cos(cos(c1) ab from {dbname}.t1)",
            f"select cos(c1 ) as int from {dbname}.t1",
            f"select cos from {dbname}.stb1",
            # f"select cos(-+--+c1) from {dbname}.stb1",
            # f"select +-cos(c1) from {dbname}.stb1",
            # f"select ++-cos(c1) from {dbname}.stb1",
            # f"select ++--cos(c1) from {dbname}.stb1",
            # f"select - -cos(c1)*0 from {dbname}.stb1",
            # f"select cos(tbname+1) from {dbname}.stb1 ",
            f"select cos(123--123)==1 from {dbname}.stb1",
            f"select cos(c1) as 'd1' from {dbname}.stb1",
            f"select cos(c1 ,c2 ) from {dbname}.stb1",
            f"select cos(c1 ,NULL) from {dbname}.stb1",
            f"select cos(,) from {dbname}.stb1;",
            f"select cos(cos(c1) ab from {dbname}.stb1)",
            f"select cos(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self, dbname="db"):
        type_error_sql_lists = [
            f"select cos(ts) from {dbname}.t1" ,
            f"select cos(c7) from {dbname}.t1",
            f"select cos(c8) from {dbname}.t1",
            f"select cos(c9) from {dbname}.t1",
            f"select cos(ts) from {dbname}.ct1" ,
            f"select cos(c7) from {dbname}.ct1",
            f"select cos(c8) from {dbname}.ct1",
            f"select cos(c9) from {dbname}.ct1",
            f"select cos(ts) from {dbname}.ct3" ,
            f"select cos(c7) from {dbname}.ct3",
            f"select cos(c8) from {dbname}.ct3",
            f"select cos(c9) from {dbname}.ct3",
            f"select cos(ts) from {dbname}.ct4" ,
            f"select cos(c7) from {dbname}.ct4",
            f"select cos(c8) from {dbname}.ct4",
            f"select cos(c9) from {dbname}.ct4",
            f"select cos(ts) from {dbname}.stb1" ,
            f"select cos(c7) from {dbname}.stb1",
            f"select cos(c8) from {dbname}.stb1",
            f"select cos(c9) from {dbname}.stb1" ,

            f"select cos(ts) from {dbname}.stbbb1" ,
            f"select cos(c7) from {dbname}.stbbb1",

            f"select cos(ts) from {dbname}.tbname",
            f"select cos(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)


        type_sql_lists = [
            f"select cos(c1) from {dbname}.t1",
            f"select cos(c2) from {dbname}.t1",
            f"select cos(c3) from {dbname}.t1",
            f"select cos(c4) from {dbname}.t1",
            f"select cos(c5) from {dbname}.t1",
            f"select cos(c6) from {dbname}.t1",

            f"select cos(c1) from {dbname}.ct1",
            f"select cos(c2) from {dbname}.ct1",
            f"select cos(c3) from {dbname}.ct1",
            f"select cos(c4) from {dbname}.ct1",
            f"select cos(c5) from {dbname}.ct1",
            f"select cos(c6) from {dbname}.ct1",

            f"select cos(c1) from {dbname}.ct3",
            f"select cos(c2) from {dbname}.ct3",
            f"select cos(c3) from {dbname}.ct3",
            f"select cos(c4) from {dbname}.ct3",
            f"select cos(c5) from {dbname}.ct3",
            f"select cos(c6) from {dbname}.ct3",

            f"select cos(c1) from {dbname}.stb1",
            f"select cos(c2) from {dbname}.stb1",
            f"select cos(c3) from {dbname}.stb1",
            f"select cos(c4) from {dbname}.stb1",
            f"select cos(c5) from {dbname}.stb1",
            f"select cos(c6) from {dbname}.stb1",

            f"select cos(c6) as alisb from {dbname}.stb1",
            f"select cos(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)

    def basic_cos_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select cos(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select cos(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select cos(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select cos(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select cos(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select cos(c6) from {dbname}.ct3")
        tdSql.checkRows(0)


        # # used for regular table
        tdSql.query(f"select cos(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 0.540302306)
        tdSql.checkData(3 , 0, -0.989992497)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)

        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto_cos( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.t1", f"select cos(abs(c1)), cos(abs(c2)) ,cos(abs(c3)), cos(abs(c4)), cos(abs(c5)) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c2 ,cos(c2) from {dbname}.ct1")
        tdSql.checkData(0, 1, 0.975339851)
        tdSql.checkData(1 , 1, -0.830564903)
        tdSql.checkData(3 , 1, 0.602244939)
        tdSql.checkData(4 , 1, 1.000000000)

        tdSql.query(f"select c1, c5 ,cos(c5) from {dbname}.ct4")
        tdSql.checkData(0 , 2, None)
        tdSql.checkData(1 , 2, -0.855242438)
        tdSql.checkData(2 , 2, 0.083882969)
        tdSql.checkData(3 , 2, 0.929841474)
        tdSql.checkData(5 , 2, None)

        self.check_result_auto_cos( f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select cos(c1), cos(c2) ,cos(c3), cos(c4), cos(c5) from {dbname}.ct1")

        # nest query for cos functions
        tdSql.query(f"select c4  , cos(c4) ,cos(cos(c4)) , cos(cos(cos(c4))) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 88)
        tdSql.checkData(0 , 1 , 0.999373284)
        tdSql.checkData(0 , 2 , 0.540829563)
        tdSql.checkData(0 , 3 , 0.857281878)

        tdSql.checkData(1 , 0 , 77)
        tdSql.checkData(1 , 1 , -0.030975032)
        tdSql.checkData(1 , 2 , 0.999520312)
        tdSql.checkData(1 , 3 , 0.540705887)

        tdSql.checkData(11 , 0 , -99)
        tdSql.checkData(11 , 1 , 0.039820880)
        tdSql.checkData(11 , 2 , 0.999207254)
        tdSql.checkData(11 , 3 , 0.540969209)

        # used for stable table

        tdSql.query(f"select cos(c1) from {dbname}.stb1")
        tdSql.checkRows(25)


        # used for not exists table
        tdSql.error(f"select cos(c1) from {dbname}.stbbb1")
        tdSql.error(f"select cos(c1) from {dbname}.tbname")
        tdSql.error(f"select cos(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, cos(c1) from {dbname}.ct1")
        tdSql.query(f"select c2, cos(c2) from {dbname}.ct4")


        # mix with common functions
        tdSql.query(f"select c1, cos(c1),cos(c1), cos(cos(c1)) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 ,0.960170287)
        tdSql.checkData(3 , 2 ,0.960170287)
        tdSql.checkData(3 , 3 ,0.573380480)

        tdSql.query(f"select c1, cos(c1),c5, floor(c5) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, cos(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, cos(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select cos(c1), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select cos(c1), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")


        # # bug fix for compute
        tdSql.query(f"select c1, cos(c1) -0 ,cos(c1-4)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, -0.145500034)
        tdSql.checkData(1, 2, -0.653643621)

        tdSql.query(f" select c1, cos(c1) -0 ,cos(c1-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, -0.145500034)
        tdSql.checkData(1, 2, -0.146002126)
        tdSql.query(f"select c1, cos(c1), c2, cos(c2), c3, cos(c3) from {dbname}.ct1")


    def test_big_number(self, dbname="db"):

        tdSql.query(f"select c1, cos(100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.cos(100000000))

        tdSql.query(f"select c1, cos(10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.cos(10000000000000))

        tdSql.query(f"select c1, cos(10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, cos(10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(1, 1, math.cos(10000000000000000000000000.0))

        tdSql.query(f"select c1, cos(10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, cos(10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(4, 1, math.cos(10000000000000000000000000000000000.0))

        tdSql.query(f"select c1, cos(10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, cos(10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value

        tdSql.checkData(4, 1, math.cos(10000000000000000000000000000000000000000.0))

        tdSql.query(f"select c1, cos(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow

    def abs_func_filter(self, dbname="db"):
        tdSql.execute(f"use {dbname}")
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(cos(c1)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,0.000000000)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(cos(c1)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,0.000000000)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(cos(c1)-0.5) from {dbname}.ct4 where c1>cos(c1) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,88888)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,8.000000000)
        tdSql.checkData(0,4,7.900000000)
        tdSql.checkData(0,5,0.000000000)

    def check_boundary_values(self, dbname="bound_test"):

        PI=3.1415926

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database if not exists {dbname}")
        time.sleep(3)
        tdSql.execute(f"use {dbname}")
        tdSql.execute(
            f"create table {dbname}.stb_bound (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp) tags (t1 int);"
        )
        tdSql.execute(f'create table {dbname}.sub1_bound using {dbname}.stb_bound tags ( 1 )')
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()-10s, 2147483647, 9223372036854775807, 32767, 127, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()-5s, -2147483647, -9223372036854775807, -32767, -127, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now(), 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+5s, -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        # self.check_result_auto_cos( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.sub1_bound ", f"select cos(abs(c1)), cos(abs(c2)) ,cos(abs(c3)), cos(abs(c4)), cos(abs(c5)) from {dbname}.sub1_bound")

        self.check_result_auto_cos( f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select cos(c1), cos(c2) ,cos(c3), cos(c3), cos(c2) ,cos(c1) from {dbname}.sub1_bound")

        self.check_result_auto_cos(f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select cos(abs(c1)) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select cos(abs(c1)) ,cos(abs(c2)) , cos(abs(c3)) , cos(abs(c4)), cos(abs(c5)), cos(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.cos(2147483647))
        tdSql.checkData(0,1,math.cos(9223372036854775807))
        tdSql.checkData(0,2,math.cos(32767))
        tdSql.checkData(0,3,math.cos(127))
        tdSql.checkData(0,4,math.cos(339999995214436424907732413799364296704.00000))
        tdSql.checkData(1,0,math.cos(2147483647))
        tdSql.checkData(1,1,math.cos(9223372036854775807))
        tdSql.checkData(1,2,math.cos(32767))
        tdSql.checkData(1,3,math.cos(127))
        tdSql.checkData(1,4,math.cos(339999995214436424907732413799364296704.00000))
        tdSql.checkData(3,0,math.cos(2147483646))
        tdSql.checkData(3,1,math.cos(9223372036854775806))
        tdSql.checkData(3,2,math.cos(32766))
        tdSql.checkData(3,3,math.cos(126))
        tdSql.checkData(3,4,math.cos(339999995214436424907732413799364296704.00000))

        # check  + - * / in functions
        tdSql.query(f"select cos(abs(c1+1)) ,cos(abs(c2)) , cos(abs(c3*1)) , cos(abs(c4/2)), cos(abs(c5))/2, cos(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.cos(2147483648.000000000))
        tdSql.checkData(0,1,math.cos(9223372036854775807))
        tdSql.checkData(0,2,math.cos(32767.000000000))
        tdSql.checkData(0,3,math.cos(63.500000000))

        tdSql.execute(f"create stable {dbname}.st (ts timestamp,  num1 float, num2 double) tags (t1 int);")
        tdSql.execute(f'create table {dbname}.tb1 using {dbname}.st tags (1)')
        tdSql.execute(f'create table {dbname}.tb2 using {dbname}.st tags (2)')
        tdSql.execute(f'create table {dbname}.tb3 using {dbname}.st tags (3)')
        tdSql.execute(f'insert into {dbname}.tb1 values (now()-40s, {PI/2}, {PI/2})')
        tdSql.execute(f'insert into {dbname}.tb1 values (now()-30s, {PI}, {PI})')
        tdSql.execute(f'insert into {dbname}.tb1 values (now()-20s, {PI*1.5}, {PI*1.5})')
        tdSql.execute(f'insert into {dbname}.tb1 values (now()-10s, {PI*2}, {PI*2})')
        tdSql.execute(f'insert into {dbname}.tb1 values (now(), {PI*2.5}, {PI*2.5})')

        tdSql.execute(f'insert into {dbname}.tb2 values (now()-40s, {PI/2}, {PI/2})')
        tdSql.execute(f'insert into {dbname}.tb2 values (now()-30s, {PI}, {PI})')
        tdSql.execute(f'insert into {dbname}.tb2 values (now()-20s, {PI*1.5}, {PI*1.5})')
        tdSql.execute(f'insert into {dbname}.tb2 values (now()-10s, {PI*2}, {PI*2})')
        tdSql.execute(f'insert into {dbname}.tb2 values (now(), {PI*2.5}, {PI*2.5})')

        for i in range(100):
            tdSql.execute(f'insert into {dbname}.tb3 values (now()+{i}s, {PI*(5+i)/2}, {PI*(5+i)/2})')

        # self.check_result_auto_cos(f"select num1,num2 from {dbname}.tb3;" , f"select cos(num1),cos(num2) from {dbname}.tb3")

    def support_super_table_test(self, dbname="db"):
        tdSql.execute(f" use {dbname} ")
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 order by ts " , f"select cos(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 order by tbname " , f"select cos(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 order by ts " , f"select cos(t1), cos(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 order by tbname " , f"select cos(t1) ,cos(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(t1) ,cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(t1) , cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.test_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: cos basic query ============")

        self.basic_cos_function()

        tdLog.printNoPrefix("==========step5: big number cos query ============")

        self.test_big_number()


        tdLog.printNoPrefix("==========step6: cos boundary query ============")

        self.check_boundary_values()

        tdLog.printNoPrefix("==========step7: cos filter query ============")

        self.abs_func_filter()

        tdLog.printNoPrefix("==========step8: check cos result of  stable query ============")

        self.support_super_table_test()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
