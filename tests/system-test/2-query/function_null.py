import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
import random


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.tb_nums = 10
        self.row_nums = 20
        self.ts = 1434938400000
        self.time_step = 1000

    def prepare_tag_datas(self, dbname="testdb"):
        # prepare datas
        tdSql.execute(
            f"create database if not exists {dbname} keep 3650 duration 1000")
        tdSql.execute(f"use {dbname} ")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, tag1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(
                f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

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

    def function_for_null_data(self, dbname="testdb"):

        function_names = ["abs" , "floor" , "ceil" , "round"]

        for function_name in function_names:

            scalar_sql_1 = f"select {function_name}(c1)/0 from {dbname}.t1 group by c1 order by c1"
            scalar_sql_2 = f"select {function_name}(c1/0) from {dbname}.t1 group by c1 order by c1"
            scalar_sql_3 = f"select {function_name}(NULL) from {dbname}.t1 group by c1 order by c1"
            tdSql.query(scalar_sql_1)
            tdSql.checkRows(10)
            tdSql.checkData(0,0,None)
            tdSql.checkData(9,0,None)
            tdSql.query(scalar_sql_2)
            tdSql.checkRows(10)
            tdSql.checkData(0,0,None)
            tdSql.checkData(9,0,None)
            tdSql.query(scalar_sql_3)
            tdSql.checkRows(10)
            tdSql.checkData(0,0,None)
            tdSql.checkData(9,0,None)

        function_names = ["sin" ,"cos" ,"tan" ,"asin" ,"acos" ,"atan"]

        PI = 3.141592654

        # sin
        tdSql.query(f"select sin(c1/0) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select sin(NULL) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select sin(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,0.000000000)

        tdSql.query(f"select sin({PI/2}) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,1.0)

        tdSql.query(f"select sin(1000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,0.826879541)

        # cos
        tdSql.query(f"select cos(c1/0) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select cos(NULL) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select cos(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,1.000000000)

        tdSql.query(f"select cos({PI}/2) from {dbname}.t1 group by c1 order by c1")

        tdSql.query(f"select cos(1000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,0.562379076)


        # tan
        tdSql.query(f"select tan(c1/0) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select tan(NULL) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select tan(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,0.000000000)

        tdSql.query(f"select tan({PI}/2) from {dbname}.t1 group by c1 order by c1")

        tdSql.query(f"select tan(1000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,1.470324156)

        # atan
        tdSql.query(f"select atan(c1/0) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select atan(NULL) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select atan(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,0.000000000)

        tdSql.query(f"select atan({PI}/2) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,1.003884822)

        tdSql.query(f"select atan(1000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,1.569796327)

        # asin
        tdSql.query(f"select asin(c1/0) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select asin(NULL) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select asin(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,0.000000000)

        tdSql.query(f"select asin({PI}/2) from {dbname}.t1 group by c1 order by c1")

        tdSql.query(f"select asin(1000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        # acos

        tdSql.query(f"select acos(c1/0) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select acos(NULL) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select acos(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,1.570796327)

        tdSql.query(f"select acos({PI}/2) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select acos(1000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        function_names = ["log" ,"pow"]

        # log
        tdSql.query(f"select log(-10) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select log(NULL ,2) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select log(c1)/0 from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select log(0.00) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        # pow
        tdSql.query(f"select pow(c1,10000) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select pow(c1,2)/0 from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select pow(NULL,2) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

        tdSql.query(f"select pow(c1/0 ,1 ) from {dbname}.t1 group by c1 order by c1")
        tdSql.checkData(9,0,None)

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_tag_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.function_for_null_data()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())