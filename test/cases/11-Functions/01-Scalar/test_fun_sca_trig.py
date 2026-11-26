# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import math
from new_test_framework.utils import tdLog, tdSql
import time
import time, math


class TestFunScaTrig:

    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to excute {__file__}")

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")

        # tdSql.init(conn.cursor())

    def prepare_datas_sin(self, dbname="db"):
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


    def check_result_auto_sin(self ,origin_query , pow_query):

        pow_result = tdSql.getResult(pow_query)

        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                else:
                    elem = math.sin(elem)
                row_check.append(elem)
            auto_result.append(row_check)
        tdSql.query(pow_query)
        for row_index , row in enumerate(pow_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index ,col_index ,auto_result[row_index][col_index])


    def check_errors_sin(self, dbname="db"):
        error_sql_lists = [
            f"select sin from {dbname}.t1",
            # f"select sin(-+--+c1 ) from {dbname}.t1",
            # f"select +-sin(c1) from {dbname}.t1",
            # f"select ++-sin(c1) from {dbname}.t1",
            # f"select ++--sin(c1) from {dbname}.t1",
            # f"select - -sin(c1)*0 from {dbname}.t1",
            # f"select sin(tbname+1) from {dbname}.t1 ",
            f"select sin(123--123)==1 from {dbname}.t1",
            f"select sin(c1) as 'd1' from {dbname}.t1",
            f"select sin(c1 ,c2) from {dbname}.t1",
            f"select sin(c1 ,NULL ) from {dbname}.t1",
            f"select sin(,) from {dbname}.t1;",
            f"select sin(sin(c1) ab from {dbname}.t1)",
            f"select sin(c1 ) as int from {dbname}.t1",
            f"select sin from {dbname}.stb1",
            # f"select sin(-+--+c1) from {dbname}.stb1",
            # f"select +-sin(c1) from {dbname}.stb1",
            # f"select ++-sin(c1) from {dbname}.stb1",
            # f"select ++--sin(c1) from {dbname}.stb1",
            # f"select - -sin(c1)*0 from {dbname}.stb1",
            # f"select sin(tbname+1) from {dbname}.stb1 ",
            f"select sin(123--123)==1 from {dbname}.stb1",
            f"select sin(c1) as 'd1' from {dbname}.stb1",
            f"select sin(c1 ,c2 ) from {dbname}.stb1",
            f"select sin(c1 ,NULL) from {dbname}.stb1",
            f"select sin(,) from {dbname}.stb1;",
            f"select sin(sin(c1) ab from {dbname}.stb1)",
            f"select sin(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)


    def support_types_sin(self, dbname="db"):
        type_error_sql_lists = [
            f"select sin(ts) from {dbname}.t1" ,
            f"select sin(c7) from {dbname}.t1",
            f"select sin(c8) from {dbname}.t1",
            f"select sin(c9) from {dbname}.t1",
            f"select sin(ts) from {dbname}.ct1" ,
            f"select sin(c7) from {dbname}.ct1",
            f"select sin(c8) from {dbname}.ct1",
            f"select sin(c9) from {dbname}.ct1",
            f"select sin(ts) from {dbname}.ct3" ,
            f"select sin(c7) from {dbname}.ct3",
            f"select sin(c8) from {dbname}.ct3",
            f"select sin(c9) from {dbname}.ct3",
            f"select sin(ts) from {dbname}.ct4" ,
            f"select sin(c7) from {dbname}.ct4",
            f"select sin(c8) from {dbname}.ct4",
            f"select sin(c9) from {dbname}.ct4",
            f"select sin(ts) from {dbname}.stb1" ,
            f"select sin(c7) from {dbname}.stb1",
            f"select sin(c8) from {dbname}.stb1",
            f"select sin(c9) from {dbname}.stb1" ,

            f"select sin(ts) from {dbname}.stbbb1" ,
            f"select sin(c7) from {dbname}.stbbb1",

            f"select sin(ts) from {dbname}.tbname",
            f"select sin(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)

        type_sql_lists = [
            f"select sin(c1) from {dbname}.t1",
            f"select sin(c2) from {dbname}.t1",
            f"select sin(c3) from {dbname}.t1",
            f"select sin(c4) from {dbname}.t1",
            f"select sin(c5) from {dbname}.t1",
            f"select sin(c6) from {dbname}.t1",

            f"select sin(c1) from {dbname}.ct1",
            f"select sin(c2) from {dbname}.ct1",
            f"select sin(c3) from {dbname}.ct1",
            f"select sin(c4) from {dbname}.ct1",
            f"select sin(c5) from {dbname}.ct1",
            f"select sin(c6) from {dbname}.ct1",

            f"select sin(c1) from {dbname}.ct3",
            f"select sin(c2) from {dbname}.ct3",
            f"select sin(c3) from {dbname}.ct3",
            f"select sin(c4) from {dbname}.ct3",
            f"select sin(c5) from {dbname}.ct3",
            f"select sin(c6) from {dbname}.ct3",

            f"select sin(c1) from {dbname}.stb1",
            f"select sin(c2) from {dbname}.stb1",
            f"select sin(c3) from {dbname}.stb1",
            f"select sin(c4) from {dbname}.stb1",
            f"select sin(c5) from {dbname}.stb1",
            f"select sin(c6) from {dbname}.stb1",

            f"select sin(c6) as alisb from {dbname}.stb1",
            f"select sin(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)


    def basic_sin_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select sin(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select sin(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select sin(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select sin(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select sin(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select sin(c6) from {dbname}.ct3")
        tdSql.checkRows(0)

        # # used for regular table
        tdSql.query(f"select sin(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 0.841470985)
        tdSql.checkData(3 , 0, 0.141120008)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)

        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto_sin( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.t1", f"select sin(abs(c1)), sin(abs(c2)) ,sin(abs(c3)), sin(abs(c4)), sin(abs(c5)) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c2 ,sin(c2) from {dbname}.ct1")
        tdSql.checkData(0, 1, -0.220708349)
        tdSql.checkData(1 , 1, -0.556921845)
        tdSql.checkData(3 , 1, -0.798311364)
        tdSql.checkData(4 , 1, 0.000000000)

        tdSql.query(f"select c1, c5 ,sin(c5) from {dbname}.ct4")
        tdSql.checkData(0 , 2, None)
        tdSql.checkData(1 , 2, 0.518228108)
        tdSql.checkData(2 , 2, 0.996475613)
        tdSql.checkData(3 , 2, 0.367960369)
        tdSql.checkData(5 , 2, None)

        self.check_result_auto_sin( f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select sin(c1), sin(c2) ,sin(c3), sin(c4), sin(c5) from {dbname}.ct1")

        # nest query for sin functions
        tdSql.query(f"select c4  , sin(c4) ,sin(sin(c4)) , sin(sin(sin(c4))) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 88)
        tdSql.checkData(0 , 1 , 0.035398303)
        tdSql.checkData(0 , 2 , 0.035390911)
        tdSql.checkData(0 , 3 , 0.035383523)

        tdSql.checkData(1 , 0 , 77)
        tdSql.checkData(1 , 1 , 0.999520159)
        tdSql.checkData(1 , 2 , 0.841211629)
        tdSql.checkData(1 , 3 , 0.745451290)

        tdSql.checkData(11 , 0 , -99)
        tdSql.checkData(11 , 1 , 0.999206834)
        tdSql.checkData(11 , 2 , 0.841042171)
        tdSql.checkData(11 , 3 , 0.745338326)

        # used for stable table

        tdSql.query(f"select sin(c1) from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for not exists table
        tdSql.error(f"select sin(c1) from {dbname}.stbbb1")
        tdSql.error(f"select sin(c1) from {dbname}.tbname")
        tdSql.error(f"select sin(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, sin(c1) from {dbname}.ct1")
        tdSql.query(f"select c2, sin(c2) from {dbname}.ct4")

        # mix with common functions
        tdSql.query(f"select c1, sin(c1),sin(c1), sin(sin(c1)) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 ,-0.279415498)
        tdSql.checkData(3 , 2 ,-0.279415498)
        tdSql.checkData(3 , 3 ,-0.275793863)

        tdSql.query(f"select c1, sin(c1),c5, floor(c5) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, sin(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, sin(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select sin(c1), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select sin(c1), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")

        # # bug fix for compute
        tdSql.query(f"select c1, sin(c1) -0 ,sin(c1-4)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 0.989358247)
        tdSql.checkData(1, 2, -0.756802495)

        tdSql.query(f"select c1, sin(c1) -0 ,sin(c1-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 0.989358247)
        tdSql.checkData(1, 2, 0.898941342)

        tdSql.query(f"select c1, sin(c1), c2, sin(c2), c3, sin(c3) from {dbname}.ct1")


    def check_big_number_sin(self, dbname="db"):

        tdSql.query(f"select c1, sin(100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.sin(100000000))

        tdSql.query(f"select c1, sin(10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.sin(10000000000000))

        tdSql.query(f"select c1, sin(10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, sin(10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(1, 1, math.sin(10000000000000000000000000.0))

        tdSql.query(f"select c1, sin(10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, sin(10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(4, 1, math.sin(10000000000000000000000000000000000.0))

        tdSql.query(f"select c1, sin(10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, sin(10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value

        tdSql.checkData(4, 1, math.sin(10000000000000000000000000000000000000000.0))

        tdSql.query(f"select c1, sin(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow


    def abs_func_filter_sin(self, dbname="db"):
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(sin(c1)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,1.000000000)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(sin(c1)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,-1.000000000)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(sin(c1)-0.5) from {dbname}.ct4 where c1=sin(c1) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0.000000000)
        tdSql.checkData(0,3,0.000000000)
        tdSql.checkData(0,4,-0.100000000)
        tdSql.checkData(0,5,0.000000000)


    def check_boundary_values_sin(self, dbname="testdb"):

        PI=3.1415926

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database if not exists {dbname}")
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
        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        self.check_result_auto_sin( f"select abs(c1), abs(c2), abs(c3) , abs(c4) from {dbname}.sub1_bound ", f"select sin(abs(c1)), sin(abs(c2)) ,sin(abs(c3)), sin(abs(c4)) from {dbname}.sub1_bound")

        self.check_result_auto_sin( f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select sin(c1), sin(c2) ,sin(c3), sin(c3), sin(c2) ,sin(c1) from {dbname}.sub1_bound")

        self.check_result_auto_sin(f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select sin(abs(c1)) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select sin(abs(c1)) ,sin(abs(c2)) , sin(abs(c3)) , sin(abs(c4)), sin(abs(c5)), sin(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.sin(2147483647))
        tdSql.checkData(0,1,math.sin(9223372036854775807))
        tdSql.checkData(0,2,math.sin(32767))
        tdSql.checkData(0,3,math.sin(127))
        tdSql.checkData(0,4,math.sin(339999995214436424907732413799364296704.00000))
        tdSql.checkData(1,0,math.sin(2147483647))
        tdSql.checkData(1,1,math.sin(9223372036854775807))
        tdSql.checkData(1,2,math.sin(32767))
        tdSql.checkData(1,3,math.sin(127))
        tdSql.checkData(1,4,math.sin(339999995214436424907732413799364296704.00000))
        tdSql.checkData(3,0,math.sin(2147483646))
        tdSql.checkData(3,1,math.sin(9223372036854775806))
        tdSql.checkData(3,2,math.sin(32766))
        tdSql.checkData(3,3,math.sin(126))
        tdSql.checkData(3,4,math.sin(339999995214436424907732413799364296704.00000))

        # check  + - * / in functions
        tdSql.query(f"select sin(abs(c1+1)) ,sin(abs(c2)) , sin(abs(c3*1)) , sin(abs(c4/2)), sin(abs(c5))/2, sin(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.sin(2147483648.000000000))
        tdSql.checkData(0,1,math.sin(9223372036854775807))
        tdSql.checkData(0,2,math.sin(32767.000000000))
        tdSql.checkData(0,3,math.sin(63.500000000))

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

        self.check_result_auto_sin(f"select num1,num2 from {dbname}.tb3;" , f"select sin(num1),sin(num2) from {dbname}.tb3")


    def support_super_table_test_sin(self, dbname="db"):
        self.check_result_auto_sin( f"select c5 from {dbname}.stb1 order by ts " , f"select sin(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_sin( f"select c5 from {dbname}.stb1 order by tbname " , f"select sin(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_sin( f"select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select sin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_sin( f"select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select sin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_sin( f"select t1,c5 from {dbname}.stb1 order by ts " , f"select sin(t1), sin(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_sin( f"select t1,c5 from {dbname}.stb1 order by tbname " , f"select sin(t1) ,sin(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_sin( f"select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select sin(t1) ,sin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_sin( f"select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select sin(t1) , sin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )


    def test_fun_sca_sin(self):
        """ Fun: sin()

        1. Support types
        2. Basic query
        3. Big number query
        4. Boundary query
        5. Filter query
        6. Stable table query
        7. Error query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_sin.py
            - 2026-11-20 Alex Merged into test_fun_sca_trig.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas_sin()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors_sin()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types_sin()

        tdLog.printNoPrefix("==========step4: sin basic query ============")

        self.basic_sin_function()

        tdLog.printNoPrefix("==========step5: sin filter query ============")

        self.abs_func_filter_sin()

        tdLog.printNoPrefix("==========step6: big number sin query ============")

        self.check_big_number_sin()

        tdLog.printNoPrefix("==========step7: sin boundary query ============")

        self.check_boundary_values_sin()

        tdLog.printNoPrefix("==========step8: check sin result of  stable query ============")

        self.support_super_table_test_sin()

        # tdSql.init(conn.cursor())

    def prepare_datas_cos(self, dbname="db"):
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


    def check_errors_cos(self, dbname="db"):
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


    def support_types_cos(self, dbname="db"):
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



    def check_big_number_cos(self, dbname="db"):

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


    def abs_func_filter_cos(self, dbname="db"):
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


    def check_boundary_values_cos(self, dbname="bound_test"):

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

    def support_super_table_test_cos(self, dbname="db"):
        tdSql.execute(f" use {dbname} ")
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 order by ts " , f"select cos(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 order by tbname " , f"select cos(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_cos( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 order by ts " , f"select cos(t1), cos(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 order by tbname " , f"select cos(t1) ,cos(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(t1) ,cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_cos( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select cos(t1) , cos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )


    def test_fun_sca_cos(self):
        """ Fun: cos()

        1. Support types
        2. Basic query
        3. Big number query
        4. Boundary query
        5. Filter query
        6. Stable table query
        7. Error query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_cos.py
            - 2026-11-20 Alex Merged into test_fun_sca_trig.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas_cos()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors_cos()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types_cos()

        tdLog.printNoPrefix("==========step4: cos basic query ============")

        self.basic_cos_function()

        tdLog.printNoPrefix("==========step5: big number cos query ============")

        self.check_big_number_cos()

        tdLog.printNoPrefix("==========step6: cos boundary query ============")

        self.check_boundary_values_cos()

        tdLog.printNoPrefix("==========step7: cos filter query ============")

        self.abs_func_filter_cos()

        tdLog.printNoPrefix("==========step8: check cos result of  stable query ============")

        self.support_super_table_test_cos()

        # tdSql.init(conn.cursor())

    def prepare_datas_tan(self, dbname="db"):
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
            ( '2022-12-31 01:01:36.000', 9, -99999, -999, -99, -9.99, -99999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )


    def check_result_auto_tan(self ,origin_query , pow_query):

        pow_result = tdSql.getResult(pow_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                else:
                    elem = math.tan(elem)
                row_check.append(elem)
            auto_result.append(row_check)
        tdSql.query(pow_query)
        for row_index , row in enumerate(pow_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index , col_index ,auto_result[row_index][col_index] )


    def check_errors_tan(self, dbname="db"):
        error_sql_lists = [
            f"select tan from {dbname}.t1",
            # f"select tan(-+--+c1 ) from {dbname}.t1",
            # f"select +-tan(c1) from {dbname}.t1",
            # f"select ++-tan(c1) from {dbname}.t1",
            # f"select ++--tan(c1) from {dbname}.t1",
            # f"select - -tan(c1)*0 from {dbname}.t1",
            # f"select tan(tbname+1) from {dbname}.t1 ",
            f"select tan(123--123)==1 from {dbname}.t1",
            f"select tan(c1) as 'd1' from {dbname}.t1",
            f"select tan(c1 ,c2) from {dbname}.t1",
            f"select tan(c1 ,NULL ) from {dbname}.t1",
            f"select tan(,) from {dbname}.t1;",
            f"select tan(tan(c1) ab from {dbname}.t1)",
            f"select tan(c1 ) as int from {dbname}.t1",
            f"select tan from {dbname}.stb1",
            # f"select tan(-+--+c1) from {dbname}.stb1",
            # f"select +-tan(c1) from {dbname}.stb1",
            # f"select ++-tan(c1) from {dbname}.stb1",
            # f"select ++--tan(c1) from {dbname}.stb1",
            # f"select - -tan(c1)*0 from {dbname}.stb1",
            # f"select tan(tbname+1) from {dbname}.stb1 ",
            f"select tan(123--123)==1 from {dbname}.stb1",
            f"select tan(c1) as 'd1' from {dbname}.stb1",
            f"select tan(c1 ,c2 ) from {dbname}.stb1",
            f"select tan(c1 ,NULL) from {dbname}.stb1",
            f"select tan(,) from {dbname}.stb1;",
            f"select tan(tan(c1) ab from {dbname}.stb1)",
            f"select tan(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)


    def support_types_tan(self, dbname="db"):
        type_error_sql_lists = [
            f"select tan(ts) from {dbname}.t1" ,
            f"select tan(c7) from {dbname}.t1",
            f"select tan(c8) from {dbname}.t1",
            f"select tan(c9) from {dbname}.t1",
            f"select tan(ts) from {dbname}.ct1" ,
            f"select tan(c7) from {dbname}.ct1",
            f"select tan(c8) from {dbname}.ct1",
            f"select tan(c9) from {dbname}.ct1",
            f"select tan(ts) from {dbname}.ct3" ,
            f"select tan(c7) from {dbname}.ct3",
            f"select tan(c8) from {dbname}.ct3",
            f"select tan(c9) from {dbname}.ct3",
            f"select tan(ts) from {dbname}.ct4" ,
            f"select tan(c7) from {dbname}.ct4",
            f"select tan(c8) from {dbname}.ct4",
            f"select tan(c9) from {dbname}.ct4",
            f"select tan(ts) from {dbname}.stb1" ,
            f"select tan(c7) from {dbname}.stb1",
            f"select tan(c8) from {dbname}.stb1",
            f"select tan(c9) from {dbname}.stb1" ,

            f"select tan(ts) from {dbname}.stbbb1" ,
            f"select tan(c7) from {dbname}.stbbb1",

            f"select tan(ts) from {dbname}.tbname",
            f"select tan(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)

        type_sql_lists = [
            f"select tan(c1) from {dbname}.t1",
            f"select tan(c2) from {dbname}.t1",
            f"select tan(c3) from {dbname}.t1",
            f"select tan(c4) from {dbname}.t1",
            f"select tan(c5) from {dbname}.t1",
            f"select tan(c6) from {dbname}.t1",

            f"select tan(c1) from {dbname}.ct1",
            f"select tan(c2) from {dbname}.ct1",
            f"select tan(c3) from {dbname}.ct1",
            f"select tan(c4) from {dbname}.ct1",
            f"select tan(c5) from {dbname}.ct1",
            f"select tan(c6) from {dbname}.ct1",

            f"select tan(c1) from {dbname}.ct3",
            f"select tan(c2) from {dbname}.ct3",
            f"select tan(c3) from {dbname}.ct3",
            f"select tan(c4) from {dbname}.ct3",
            f"select tan(c5) from {dbname}.ct3",
            f"select tan(c6) from {dbname}.ct3",

            f"select tan(c1) from {dbname}.stb1",
            f"select tan(c2) from {dbname}.stb1",
            f"select tan(c3) from {dbname}.stb1",
            f"select tan(c4) from {dbname}.stb1",
            f"select tan(c5) from {dbname}.stb1",
            f"select tan(c6) from {dbname}.stb1",

            f"select tan(c6) as alisb from {dbname}.stb1",
            f"select tan(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)


    def basic_tan_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select tan(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select tan(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select tan(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select tan(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select tan(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select tan(c6) from {dbname}.ct3")
        tdSql.checkRows(0)

        # # used for regular table
        tdSql.query(f"select tan(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 1.557407725)
        tdSql.checkData(3 , 0, -0.142546543)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)

        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto_tan( f"select abs(c1), abs(c2), abs(c3) , abs(c4) from {dbname}.t1", f"select tan(abs(c1)), tan(abs(c2)) ,tan(abs(c3)), tan(abs(c4)) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c2 ,tan(c2) from {dbname}.ct1")
        tdSql.checkData(0, 1, -0.226288661)
        tdSql.checkData(1 , 1, 0.670533806)
        tdSql.checkData(3 , 1, -1.325559275)
        tdSql.checkData(4 , 1, 0.000000000)

        tdSql.query(f"select c1, c5 ,tan(c5) from {dbname}.ct4")
        tdSql.checkData(0 , 2, None)
        tdSql.checkData(1 , 2, -0.605942929)
        tdSql.checkData(2 , 2, 11.879355609)
        tdSql.checkData(3 , 2, 0.395723765)
        tdSql.checkData(5 , 2, None)

        self.check_result_auto_tan( f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select tan(c1), tan(c2) ,tan(c3), tan(c4), tan(c5) from {dbname}.ct1")

        # nest query for tan functions
        tdSql.query(f"select c4  , tan(c4) ,tan(tan(c4)) , tan(tan(tan(c4))) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 88)
        tdSql.checkData(0 , 1 , 0.035420501)
        tdSql.checkData(0 , 2 , 0.035435322)
        tdSql.checkData(0 , 3 , 0.035450161)

        tdSql.checkData(1 , 0 , 77)
        tdSql.checkData(1 , 1 , -32.268575776)
        tdSql.checkData(1 , 2 , -1.144433254)
        tdSql.checkData(1 , 3 , -2.201545113)

        tdSql.checkData(11 , 0 , -99)
        tdSql.checkData(11 , 1 , 25.092534980)
        tdSql.checkData(11 , 2 , -0.040227928)
        tdSql.checkData(11 , 3 , -0.040249642)

        # used for stable table

        tdSql.query(f"select tan(c1) from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for not exists table
        tdSql.error(f"select tan(c1) from {dbname}.stbbb1")
        tdSql.error(f"select tan(c1) from {dbname}.tbname")
        tdSql.error(f"select tan(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, tan(c1) from {dbname}.ct1")
        tdSql.query(f"select c2, tan(c2) from {dbname}.ct4")

        # mix with common functions
        tdSql.query(f"select c1, tan(c1),tan(c1), tan(tan(c1)) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 ,-0.291006191)
        tdSql.checkData(3 , 2 ,-0.291006191)
        tdSql.checkData(3 , 3 ,-0.299508909)

        tdSql.query(f"select c1, tan(c1),c5, floor(c5) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, tan(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, tan(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select tan(c1), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select tan(c1), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")

        # # bug fix for compute
        tdSql.query(f"select c1, tan(c1) -0 ,tan(c1-4)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, -6.799711455)
        tdSql.checkData(1, 2, 1.157821282)

        tdSql.query(f"select c1, tan(c1) -0 ,tan(c1-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, -6.799711455)
        tdSql.checkData(1, 2, -21.815112681)

        tdSql.query(f"select c1, tan(c1), c2, tan(c2), c3, tan(c3) from {dbname}.ct1")


    def check_big_number_tan(self, dbname="db"):

        tdSql.query(f"select c1, tan(100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.tan(100000000))

        tdSql.query(f"select c1, tan(10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.tan(10000000000000))

        tdSql.query(f"select c1, tan(10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, tan(10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(1, 1, math.tan(10000000000000000000000000.0))

        tdSql.query(f"select c1, tan(10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, tan(10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(4, 1, math.tan(10000000000000000000000000000000000.0))

        tdSql.query(f"select c1, tan(10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, tan(10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value

        tdSql.checkData(4, 1, math.tan(10000000000000000000000000000000000000000.0))

        tdSql.query(f"select c1, tan(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow


    def abs_func_filter_tan(self, dbname="db"):
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(tan(c1)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,-7.000000000)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(tan(c1)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,-3.000000000)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(tan(c1)-0.5) from {dbname}.ct4 where c1>tan(c1) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,88888)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,8.000000000)
        tdSql.checkData(0,4,7.900000000)
        tdSql.checkData(0,5,-7.000000000)


    def check_boundary_values_tan(self, dbname="bound_test"):

        PI=3.1415926

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database if not exists {dbname}")
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
        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        self.check_result_auto_tan( f"select abs(c1), abs(c2), abs(c3) , abs(c4) from {dbname}.sub1_bound ", f"select tan(abs(c1)), tan(abs(c2)) ,tan(abs(c3)), tan(abs(c4)) from {dbname}.sub1_bound")

        self.check_result_auto_tan( f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select tan(c1), tan(c2) ,tan(c3), tan(c3), tan(c2) ,tan(c1) from {dbname}.sub1_bound")

        self.check_result_auto_tan(f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select tan(abs(c1)) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select tan(abs(c1)) ,tan(abs(c2)) , tan(abs(c3)) , tan(abs(c4)), tan(abs(c5)), tan(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.tan(2147483647))
        tdSql.checkData(0,1,math.tan(9223372036854775807))
        tdSql.checkData(0,2,math.tan(32767))
        tdSql.checkData(0,3,math.tan(127))
        tdSql.checkData(0,4,math.tan(339999995214436424907732413799364296704.00000))
        tdSql.checkData(1,0,math.tan(2147483647))
        tdSql.checkData(1,1,math.tan(9223372036854775807))
        tdSql.checkData(1,2,math.tan(32767))
        tdSql.checkData(1,3,math.tan(127))
        tdSql.checkData(1,4,math.tan(339999995214436424907732413799364296704.00000))
        tdSql.checkData(3,0,math.tan(2147483646))
        tdSql.checkData(3,1,math.tan(9223372036854775806))
        tdSql.checkData(3,2,math.tan(32766))
        tdSql.checkData(3,3,math.tan(126))
        tdSql.checkData(3,4,math.tan(339999995214436424907732413799364296704.00000))

        # check  + - * / in functions
        tdSql.query(f"select tan(abs(c1+1)) ,tan(abs(c2)) , tan(abs(c3*1)) , tan(abs(c4/2)), tan(abs(c5))/2, tan(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.tan(2147483648.000000000))
        tdSql.checkData(0,1,math.tan(9223372036854775807))
        tdSql.checkData(0,2,math.tan(32767.000000000))
        tdSql.checkData(0,3,math.tan(63.500000000))

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

        self.check_result_auto_tan(f"select num1,num2 from {dbname}.tb3;" , f"select tan(num1),tan(num2) from {dbname}.tb3")


    def support_super_table_test_tan(self, dbname="db"):
        self.check_result_auto_tan( f"select c5 from {dbname}.stb1 order by ts " , f"select tan(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_tan( f"select c5 from {dbname}.stb1 order by tbname " , f"select tan(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_tan( f"select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select tan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_tan( f"select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select tan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_tan( f"select t1,c5 from {dbname}.stb1 order by ts " , f"select tan(t1), tan(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_tan( f"select t1,c5 from {dbname}.stb1 order by tbname " , f"select tan(t1) ,tan(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_tan( f"select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select tan(t1) ,tan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_tan( f"select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select tan(t1) , tan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        pass


    def test_fun_sca_tan(self):
        """ Fun: tan()

        1. Support types
        2. Basic query
        3. Big number query
        4. Boundary query
        5. Filter query
        6. Stable table query
        7. Error query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_tan.py
            - 2026-11-20 Alex Merged into test_fun_sca_trig.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas_tan()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors_tan()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types_tan()

        tdLog.printNoPrefix("==========step4: tan basic query ============")

        self.basic_tan_function()

        tdLog.printNoPrefix("==========step5: big number tan query ============")

        self.check_big_number_tan()

        tdLog.printNoPrefix("==========step6: tan boundary query ============")

        self.check_boundary_values_tan()

        tdLog.printNoPrefix("==========step7: tan filter query ============")

        self.abs_func_filter_tan()

        tdLog.printNoPrefix("==========step8: check tan result of  stable query ============")

        self.support_super_table_test_tan()

        # tdSql.init(conn.cursor())

    def prepare_datas_asin(self, dbname="db"):
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


    def check_result_auto_asin(self ,origin_query , pow_query):

        pow_result = tdSql.getResult(pow_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                elif elem >=-1 and  elem <=1:
                    elem = math.asin(elem)
                else:
                    elem = None
                row_check.append(elem)
            auto_result.append(row_check)

      
        tdSql.query(pow_query)
        for row_index , row in enumerate(pow_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index,col_index,auto_result[row_index][col_index])

              
      

    def check_errors_asin(self, dbname="db"):
        error_sql_lists = [
            f"select asin from {dbname}.t1",
            # f"select asin(-+--+c1 ) from {dbname}.t1",
            # f"select +-asin(c1) from {dbname}.t1",
            # f"select ++-asin(c1) from {dbname}.t1",
            # f"select ++--asin(c1) from {dbname}.t1",
            # f"select - -asin(c1)*0 from {dbname}.t1",
            # f"select asin(tbname+1) from {dbname}.t1 ",
            f"select asin(123--123)==1 from {dbname}.t1",
            f"select asin(c1) as 'd1' from {dbname}.t1",
            f"select asin(c1 ,c2) from {dbname}.t1",
            f"select asin(c1 ,NULL ) from {dbname}.t1",
            f"select asin(,) from {dbname}.t1;",
            f"select asin(asin(c1) ab from {dbname}.t1)",
            f"select asin(c1 ) as int from {dbname}.t1",
            f"select asin from {dbname}.stb1",
            # f"select asin(-+--+c1) from {dbname}.stb1",
            # f"select +-asin(c1) from {dbname}.stb1",
            # f"select ++-asin(c1) from {dbname}.stb1",
            # f"select ++--asin(c1) from {dbname}.stb1",
            # f"select - -asin(c1)*0 from {dbname}.stb1",
            # f"select asin(tbname+1) from {dbname}.stb1 ",
            f"select asin(123--123)==1 from {dbname}.stb1",
            f"select asin(c1) as 'd1' from {dbname}.stb1",
            f"select asin(c1 ,c2 ) from {dbname}.stb1",
            f"select asin(c1 ,NULL) from {dbname}.stb1",
            f"select asin(,) from {dbname}.stb1;",
            f"select asin(asin(c1) ab from {dbname}.stb1)",
            f"select asin(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)


    def support_types_asin(self, dbname="db"):
        type_error_sql_lists = [
            f"select asin(ts) from {dbname}.t1" ,
            f"select asin(c7) from {dbname}.t1",
            f"select asin(c8) from {dbname}.t1",
            f"select asin(c9) from {dbname}.t1",
            f"select asin(ts) from {dbname}.ct1" ,
            f"select asin(c7) from {dbname}.ct1",
            f"select asin(c8) from {dbname}.ct1",
            f"select asin(c9) from {dbname}.ct1",
            f"select asin(ts) from {dbname}.ct3" ,
            f"select asin(c7) from {dbname}.ct3",
            f"select asin(c8) from {dbname}.ct3",
            f"select asin(c9) from {dbname}.ct3",
            f"select asin(ts) from {dbname}.ct4" ,
            f"select asin(c7) from {dbname}.ct4",
            f"select asin(c8) from {dbname}.ct4",
            f"select asin(c9) from {dbname}.ct4",
            f"select asin(ts) from {dbname}.stb1" ,
            f"select asin(c7) from {dbname}.stb1",
            f"select asin(c8) from {dbname}.stb1",
            f"select asin(c9) from {dbname}.stb1" ,

            f"select asin(ts) from {dbname}.stbbb1" ,
            f"select asin(c7) from {dbname}.stbbb1",

            f"select asin(ts) from {dbname}.tbname",
            f"select asin(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)


        type_sql_lists = [
            f"select asin(c1) from {dbname}.t1",
            f"select asin(c2) from {dbname}.t1",
            f"select asin(c3) from {dbname}.t1",
            f"select asin(c4) from {dbname}.t1",
            f"select asin(c5) from {dbname}.t1",
            f"select asin(c6) from {dbname}.t1",

            f"select asin(c1) from {dbname}.ct1",
            f"select asin(c2) from {dbname}.ct1",
            f"select asin(c3) from {dbname}.ct1",
            f"select asin(c4) from {dbname}.ct1",
            f"select asin(c5) from {dbname}.ct1",
            f"select asin(c6) from {dbname}.ct1",

            f"select asin(c1) from {dbname}.ct3",
            f"select asin(c2) from {dbname}.ct3",
            f"select asin(c3) from {dbname}.ct3",
            f"select asin(c4) from {dbname}.ct3",
            f"select asin(c5) from {dbname}.ct3",
            f"select asin(c6) from {dbname}.ct3",

            f"select asin(c1) from {dbname}.stb1",
            f"select asin(c2) from {dbname}.stb1",
            f"select asin(c3) from {dbname}.stb1",
            f"select asin(c4) from {dbname}.stb1",
            f"select asin(c5) from {dbname}.stb1",
            f"select asin(c6) from {dbname}.stb1",

            f"select asin(c6) as alisb from {dbname}.stb1",
            f"select asin(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)


    def basic_asin_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select asin(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select asin(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select asin(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select asin(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select asin(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select asin(c6) from {dbname}.ct3")
        tdSql.checkRows(0)


        # # used for regular table
        tdSql.query(f"select asin(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 1.570796327)
        tdSql.checkData(3 , 0, None)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)

        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto_asin( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.t1", f"select asin(abs(c1)), asin(abs(c2)) ,asin(abs(c3)), asin(abs(c4)), asin(abs(c5)) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c2 ,asin(c2) from {dbname}.ct1")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1 , 1, None)
        tdSql.checkData(3 , 1, None)
        tdSql.checkData(4 , 1, 0)

        tdSql.query(f"select c1, c5 ,asin(c5) from {dbname}.ct4")
        tdSql.checkData(0 , 2, None)
        tdSql.checkData(1 , 2, None)
        tdSql.checkData(2 , 2, None)
        tdSql.checkData(3 , 2, None)
        tdSql.checkData(5 , 2, None)

        self.check_result_auto_asin( f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select asin(c1), asin(c2) ,asin(c3), asin(c4), asin(c5) from {dbname}.ct1")

        # nest query for asin functions
        tdSql.query(f"select c4  , asin(c4) ,asin(asin(c4)) , asin(asin(asin(c4))) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 88)
        tdSql.checkData(0 , 1 , None)
        tdSql.checkData(0 , 2 , None)
        tdSql.checkData(0 , 3 , None)

        tdSql.checkData(1 , 0 , 77)
        tdSql.checkData(1 , 1 , None)
        tdSql.checkData(1 , 2 , None)
        tdSql.checkData(1 , 3 , None)

        tdSql.checkData(11 , 0 , -99)
        tdSql.checkData(11 , 1 , None)
        tdSql.checkData(11 , 2 , None)
        tdSql.checkData(11 , 3 , None)

        # used for stable table

        tdSql.query(f"select asin(c1) from {dbname}.stb1")
        tdSql.checkRows(25)


        # used for not exists table
        tdSql.error(f"select asin(c1) from {dbname}.stbbb1")
        tdSql.error(f"select asin(c1) from {dbname}.tbname")
        tdSql.error(f"select asin(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, asin(c1) from {dbname}.ct1")
        tdSql.query(f"select c2, asin(c2) from {dbname}.ct4")


        # mix with common functions
        tdSql.query(f"select c1, asin(c1),asin(c1), asin(asin(c1)) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 ,None)
        tdSql.checkData(3 , 2 ,None)
        tdSql.checkData(3 , 3 ,None)

        tdSql.query(f"select c1, asin(c1),c5, floor(c5) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, asin(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, asin(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select asin(c1), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select asin(c1), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")


        # # bug fix for compute
        tdSql.query(f"select c1, asin(c1) -0 ,asin(c1-4)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)

        tdSql.query(f" select c1, asin(c1) -0 ,asin(c1-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)

        tdSql.query(f"select c1, asin(c1), c2, asin(c2), c3, asin(c3) from {dbname}.ct1")


    def check_big_number_asin(self, dbname="db"):

        tdSql.query(f"select c1, asin(100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, None)


        tdSql.query(f"select c1, asin(10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, asin(10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, asin(10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(1, 1, None)

        tdSql.query(f"select c1, asin(10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, asin(10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, asin(10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, asin(10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value

        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, asin(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow


    def abs_func_filter_asin(self, dbname="db"):
        tdSql.execute(f"use {dbname}")
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(asin(c1)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,None)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(asin(c1)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,None)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(asin(c1)-0.5) from {dbname}.ct4 where c1<asin(c1) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,1)
        tdSql.checkData(0,1,11111)
        tdSql.checkData(0,2,1.000000000)
        tdSql.checkData(0,3,1.000000000)
        tdSql.checkData(0,4,0.900000000)
        tdSql.checkData(0,5,2)


    def pow_Arithmetic_asin(self):
        pass


    def check_boundary_values_asin(self, dbname="bound_test"):

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
        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        self.check_result_auto_asin( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.sub1_bound ", f"select asin(abs(c1)), asin(abs(c2)) ,asin(abs(c3)), asin(abs(c4)), asin(abs(c5)) from {dbname}.sub1_bound")

        self.check_result_auto_asin( f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select asin(c1), asin(c2) ,asin(c3), asin(c3), asin(c2) ,asin(c1) from {dbname}.sub1_bound")

        self.check_result_auto_asin(f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select asin(abs(c1)) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select asin(abs(c1)) ,asin(abs(c2)) , asin(abs(c3)) , asin(abs(c4)), asin(abs(c5)), asin(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,None)
        tdSql.checkData(0,4,None)
        tdSql.checkData(1,0,None)
        tdSql.checkData(1,1,None)
        tdSql.checkData(1,2,None)
        tdSql.checkData(1,3,None)
        tdSql.checkData(1,4,None)
        tdSql.checkData(3,0,None)
        tdSql.checkData(3,1,None)
        tdSql.checkData(3,2,None)
        tdSql.checkData(3,3,None)
        tdSql.checkData(3,4,None)

        # check  + - * / in functions
        tdSql.query(f"select asin(abs(c1+1)) ,asin(abs(c2)) , asin(abs(c3*1)) , asin(abs(c4/2)), asin(abs(c5))/2, asin(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,None)

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

        self.check_result_auto_asin(f"select num1,num2 from {dbname}.tb3;" , f"select asin(num1),asin(num2) from {dbname}.tb3")


    def support_super_table_test_asin(self, dbname="db"):
        tdSql.execute(f" use {dbname} ")
        self.check_result_auto_asin( f" select c5 from {dbname}.stb1 order by ts " , f"select asin(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_asin( f" select c5 from {dbname}.stb1 order by tbname " , f"select asin(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_asin( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select asin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_asin( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select asin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_asin( f" select t1,c5 from {dbname}.stb1 order by ts " , f"select asin(t1), asin(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_asin( f" select t1,c5 from {dbname}.stb1 order by tbname " , f"select asin(t1) ,asin(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_asin( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select asin(t1) ,asin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_asin( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select asin(t1) , asin(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        pass


    def test_fun_sca_asin(self):
        """ Fun: asin()

        1. Support types
        2. Basic query
        3. Big number query
        4. Boundary query
        5. Filter query
        6. Stable table query
        7. Error query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_arcsin.py
            - 2026-11-20 Alex Merged into test_fun_sca_trig.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas_asin()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors_asin()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types_asin()

        tdLog.printNoPrefix("==========step4: asin basic query ============")

        self.basic_asin_function()

        tdLog.printNoPrefix("==========step5: big number asin query ============")

        self.check_big_number_asin()

        tdLog.printNoPrefix("==========step6: asin boundary query ============")

        self.check_boundary_values_asin()

        tdLog.printNoPrefix("==========step7: asin filter query ============")

        self.abs_func_filter_asin()

        tdLog.printNoPrefix("==========step8: check asin result of  stable query ============")

        self.support_super_table_test_asin()


    def prepare_datas_acos(self, dbname="db"):
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


    def check_result_auto_acos(self ,origin_query , pow_query):

        pow_result = tdSql.getResult(pow_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                elif elem >=-1 and  elem <=1:
                    elem = math.acos(elem)
                else:
                    elem = None
                row_check.append(elem)
            auto_result.append(row_check)

        tdSql.query(pow_query)
        for row_index , row in enumerate(pow_result):
            for col_index , elem in enumerate(row):
                
                tdSql.checkData(row_index,col_index,auto_result[row_index][col_index])


    def check_errors_acos(self, dbname="db"):
        error_sql_lists = [
            f"select acos from {dbname}.t1",
            # f"select acos(-+--+c1 ) from {dbname}.t1",
            # f"select +-acos(c1) from {dbname}.t1",
            # f"select ++-acos(c1) from {dbname}.t1",
            # f"select ++--acos(c1) from {dbname}.t1",
            # f"select - -acos(c1)*0 from {dbname}.t1",
            # f"select acos(tbname+1) from {dbname}.t1 ",
            f"select acos(123--123)==1 from {dbname}.t1",
            f"select acos(c1) as 'd1' from {dbname}.t1",
            f"select acos(c1 ,c2) from {dbname}.t1",
            f"select acos(c1 ,NULL ) from {dbname}.t1",
            f"select acos(,) from {dbname}.t1;",
            f"select acos(acos(c1) ab from {dbname}.t1)",
            f"select acos(c1 ) as int from {dbname}.t1",
            f"select acos from {dbname}.stb1",
            # f"select acos(-+--+c1) from {dbname}.stb1",
            # f"select +-acos(c1) from {dbname}.stb1",
            # f"select ++-acos(c1) from {dbname}.stb1",
            # f"select ++--acos(c1) from {dbname}.stb1",
            # f"select - -acos(c1)*0 from {dbname}.stb1",
            # f"select acos(tbname+1) from {dbname}.stb1 ",
            f"select acos(123--123)==1 from {dbname}.stb1",
            f"select acos(c1) as 'd1' from {dbname}.stb1",
            f"select acos(c1 ,c2 ) from {dbname}.stb1",
            f"select acos(c1 ,NULL) from {dbname}.stb1",
            f"select acos(,) from {dbname}.stb1;",
            f"select acos(acos(c1) ab from {dbname}.stb1)",
            f"select acos(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)


    def support_types_acos(self, dbname="db"):
        type_error_sql_lists = [
            f"select acos(ts) from {dbname}.t1" ,
            f"select acos(c7) from {dbname}.t1",
            f"select acos(c8) from {dbname}.t1",
            f"select acos(c9) from {dbname}.t1",
            f"select acos(ts) from {dbname}.ct1" ,
            f"select acos(c7) from {dbname}.ct1",
            f"select acos(c8) from {dbname}.ct1",
            f"select acos(c9) from {dbname}.ct1",
            f"select acos(ts) from {dbname}.ct3" ,
            f"select acos(c7) from {dbname}.ct3",
            f"select acos(c8) from {dbname}.ct3",
            f"select acos(c9) from {dbname}.ct3",
            f"select acos(ts) from {dbname}.ct4" ,
            f"select acos(c7) from {dbname}.ct4",
            f"select acos(c8) from {dbname}.ct4",
            f"select acos(c9) from {dbname}.ct4",
            f"select acos(ts) from {dbname}.stb1" ,
            f"select acos(c7) from {dbname}.stb1",
            f"select acos(c8) from {dbname}.stb1",
            f"select acos(c9) from {dbname}.stb1" ,

            f"select acos(ts) from {dbname}.stbbb1" ,
            f"select acos(c7) from {dbname}.stbbb1",

            f"select acos(ts) from {dbname}.tbname",
            f"select acos(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)

        type_sql_lists = [
            f"select acos(c1) from {dbname}.t1",
            f"select acos(c2) from {dbname}.t1",
            f"select acos(c3) from {dbname}.t1",
            f"select acos(c4) from {dbname}.t1",
            f"select acos(c5) from {dbname}.t1",
            f"select acos(c6) from {dbname}.t1",

            f"select acos(c1) from {dbname}.ct1",
            f"select acos(c2) from {dbname}.ct1",
            f"select acos(c3) from {dbname}.ct1",
            f"select acos(c4) from {dbname}.ct1",
            f"select acos(c5) from {dbname}.ct1",
            f"select acos(c6) from {dbname}.ct1",

            f"select acos(c1) from {dbname}.ct3",
            f"select acos(c2) from {dbname}.ct3",
            f"select acos(c3) from {dbname}.ct3",
            f"select acos(c4) from {dbname}.ct3",
            f"select acos(c5) from {dbname}.ct3",
            f"select acos(c6) from {dbname}.ct3",

            f"select acos(c1) from {dbname}.stb1",
            f"select acos(c2) from {dbname}.stb1",
            f"select acos(c3) from {dbname}.stb1",
            f"select acos(c4) from {dbname}.stb1",
            f"select acos(c5) from {dbname}.stb1",
            f"select acos(c6) from {dbname}.stb1",

            f"select acos(c6) as alisb from {dbname}.stb1",
            f"select acos(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)


    def basic_acos_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select acos(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select acos(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select acos(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select acos(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select acos(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select acos(c6) from {dbname}.ct3")
        tdSql.checkRows(0)

        # # used for regular table
        tdSql.query(f"select acos(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 0.000000000)
        tdSql.checkData(3 , 0, None)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)

        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto_acos( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.t1", f"select acos(abs(c1)), acos(abs(c2)) ,acos(abs(c3)), acos(abs(c4)), acos(abs(c5)) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c2 ,acos(c2) from {dbname}.ct1")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1 , 1, None)
        tdSql.checkData(3 , 1, None)
        tdSql.checkData(4 , 1, 1.570796327)

        tdSql.query(f"select c1, c5 ,acos(c5) from {dbname}.ct4")
        tdSql.checkData(0 , 2, None)
        tdSql.checkData(1 , 2, None)
        tdSql.checkData(2 , 2, None)
        tdSql.checkData(3 , 2, None)
        tdSql.checkData(5 , 2, None)

        self.check_result_auto_acos( f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select acos(c1), acos(c2) ,acos(c3), acos(c4), acos(c5) from {dbname}.ct1")

        # nest query for acos functions
        tdSql.query(f"select c4  , acos(c4) ,acos(acos(c4)) , acos(acos(acos(c4))) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 88)
        tdSql.checkData(0 , 1 , None)
        tdSql.checkData(0 , 2 , None)
        tdSql.checkData(0 , 3 , None)

        tdSql.checkData(1 , 0 , 77)
        tdSql.checkData(1 , 1 , None)
        tdSql.checkData(1 , 2 , None)
        tdSql.checkData(1 , 3 , None)

        tdSql.checkData(11 , 0 , -99)
        tdSql.checkData(11 , 1 , None)
        tdSql.checkData(11 , 2 , None)
        tdSql.checkData(11 , 3 , None)

        # used for stable table

        tdSql.query(f"select acos(c1) from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for not exists table
        tdSql.error(f"select acos(c1) from {dbname}.stbbb1")
        tdSql.error(f"select acos(c1) from {dbname}.tbname")
        tdSql.error(f"select acos(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, acos(c1) from {dbname}.ct1")
        tdSql.query(f"select c2, acos(c2) from {dbname}.ct4")

        # mix with common functions
        tdSql.query(f"select c1, acos(c1),acos(c1), acos(acos(c1)) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 ,None)
        tdSql.checkData(3 , 2 ,None)
        tdSql.checkData(3 , 3 ,None)

        tdSql.query(f"select c1, acos(c1),c5, floor(c5) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, acos(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, acos(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select acos(c1), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select acos(c1), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")

        # # bug fix for compute
        tdSql.query(f"select c1, acos(c1) -0 ,acos(c1-4)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)

        tdSql.query(f" select c1, acos(c1) -0 ,acos(c1-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)

        tdSql.query(f"select c1, acos(c1), c2, acos(c2), c3, acos(c3) from {dbname}.ct1")


    def check_big_number_acos(self, dbname="db"):

        tdSql.query(f"select c1, acos(100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, acos(10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, acos(10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, acos(10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(1, 1, None)

        tdSql.query(f"select c1, acos(10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, acos(10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, acos(10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, acos(10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value

        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, acos(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow


    def abs_func_filter_acos(self, dbname="db"):
        tdSql.execute(f"use {dbname}")
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(acos(c1)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,None)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(acos(c1)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,None)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(acos(c1)-0.5) from {dbname}.ct4 where c1<acos(c1) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0.000000000)
        tdSql.checkData(0,3,0.000000000)
        tdSql.checkData(0,4,-0.100000000)
        tdSql.checkData(0,5,2)


    def pow_Arithmetic_acos(self):
        pass


    def check_boundary_values_acos(self, dbname="bound_test"):

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
        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        self.check_result_auto_acos( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.sub1_bound ", f"select acos(abs(c1)), acos(abs(c2)) ,acos(abs(c3)), acos(abs(c4)), acos(abs(c5)) from {dbname}.sub1_bound")

        self.check_result_auto_acos( f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select acos(c1), acos(c2) ,acos(c3), acos(c3), acos(c2) ,acos(c1) from {dbname}.sub1_bound")

        self.check_result_auto_acos(f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select acos(abs(c1)) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select acos(abs(c1)) ,acos(abs(c2)) , acos(abs(c3)) , acos(abs(c4)), acos(abs(c5)), acos(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,None)
        tdSql.checkData(0,4,None)
        tdSql.checkData(1,0,None)
        tdSql.checkData(1,1,None)
        tdSql.checkData(1,2,None)
        tdSql.checkData(1,3,None)
        tdSql.checkData(1,4,None)
        tdSql.checkData(3,0,None)
        tdSql.checkData(3,1,None)
        tdSql.checkData(3,2,None)
        tdSql.checkData(3,3,None)
        tdSql.checkData(3,4,None)

        # check  + - * / in functions
        tdSql.query(f"select acos(abs(c1+1)) ,acos(abs(c2)) , acos(abs(c3*1)) , acos(abs(c4/2)), acos(abs(c5))/2, acos(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,None)

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

        self.check_result_auto_acos(f"select num1,num2 from {dbname}.tb3;" , f"select acos(num1),acos(num2) from {dbname}.tb3")


    def support_super_table_test_acos(self, dbname="db"):
        tdSql.execute(f" use {dbname} ")
        self.check_result_auto_acos( f" select c5 from {dbname}.stb1 order by ts " , f"select acos(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_acos( f" select c5 from {dbname}.stb1 order by tbname " , f"select acos(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_acos( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select acos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_acos( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select acos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_acos( f" select t1,c5 from {dbname}.stb1 order by ts " , f"select acos(t1), acos(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_acos( f" select t1,c5 from {dbname}.stb1 order by tbname " , f"select acos(t1) ,acos(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_acos( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select acos(t1) ,acos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_acos( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select acos(t1) , acos(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        pass


    def test_fun_sca_acos(self):
        """ Fun: acos()

        1. Support types
        2. Basic query
        3. Big number query
        4. Boundary query
        5. Filter query
        6. Stable table query
        7. Error query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_arccos.py
            - 2026-11-20 Alex Merged into test_fun_sca_trig.py so the methods share one taosd deployment (CI deploys once per test class)
        """
  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas_acos()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors_acos()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types_acos()

        tdLog.printNoPrefix("==========step4: acos basic query ============")

        self.basic_acos_function()

        tdLog.printNoPrefix("==========step5: big number acos query ============")

        self.check_big_number_acos()

        tdLog.printNoPrefix("==========step6: acos boundary query ============")

        self.check_boundary_values_acos()

        tdLog.printNoPrefix("==========step7: acos filter query ============")

        self.abs_func_filter_acos()

        # tdLog.printNoPrefix("==========step7: acos filter query ============")

        # self.abs_func_filter_acos()

        tdLog.printNoPrefix("==========step8: check acos result of  stable query ============")

        self.support_super_table_test_acos()

        # tdSql.init(conn.cursor())

    def prepare_datas_atan(self, dbname="db"):
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


    def check_result_auto_atan(self ,origin_query , pow_query):

        pow_result = tdSql.getResult(pow_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if elem == None:
                    elem = None
                else:
                    elem = math.atan(elem)
                row_check.append(elem)
            auto_result.append(row_check)

        tdSql.query(pow_query)
        for row_index , row in enumerate(pow_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index,col_index,auto_result[row_index][col_index])


               
    def check_errors_atan(self, dbname="db"):
        error_sql_lists = [
            f"select atan from {dbname}.t1",
            # f"select atan(-+--+c1 ) from {dbname}.t1",
            # f"select +-atan(c1) from {dbname}.t1",
            # f"select ++-atan(c1) from {dbname}.t1",
            # f"select ++--atan(c1) from {dbname}.t1",
            # f"select - -atan(c1)*0 from {dbname}.t1",
            # f"select atan(tbname+1) from {dbname}.t1 ",
            f"select atan(123--123)==1 from {dbname}.t1",
            f"select atan(c1) as 'd1' from {dbname}.t1",
            f"select atan(c1 ,c2) from {dbname}.t1",
            f"select atan(c1 ,NULL ) from {dbname}.t1",
            f"select atan(,) from {dbname}.t1;",
            f"select atan(atan(c1) ab from {dbname}.t1)",
            f"select atan(c1 ) as int from {dbname}.t1",
            f"select atan from {dbname}.stb1",
            # f"select atan(-+--+c1) from {dbname}.stb1",
            # f"select +-atan(c1) from {dbname}.stb1",
            # f"select ++-atan(c1) from {dbname}.stb1",
            # f"select ++--atan(c1) from {dbname}.stb1",
            # f"select - -atan(c1)*0 from {dbname}.stb1",
            # f"select atan(tbname+1) from {dbname}.stb1 ",
            f"select atan(123--123)==1 from {dbname}.stb1",
            f"select atan(c1) as 'd1' from {dbname}.stb1",
            f"select atan(c1 ,c2 ) from {dbname}.stb1",
            f"select atan(c1 ,NULL) from {dbname}.stb1",
            f"select atan(,) from {dbname}.stb1;",
            f"select atan(atan(c1) ab from {dbname}.stb1)",
            f"select atan(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)


    def support_types_atan(self, dbname="db"):
        type_error_sql_lists = [
            f"select atan(ts) from {dbname}.t1" ,
            f"select atan(c7) from {dbname}.t1",
            f"select atan(c8) from {dbname}.t1",
            f"select atan(c9) from {dbname}.t1",
            f"select atan(ts) from {dbname}.ct1" ,
            f"select atan(c7) from {dbname}.ct1",
            f"select atan(c8) from {dbname}.ct1",
            f"select atan(c9) from {dbname}.ct1",
            f"select atan(ts) from {dbname}.ct3" ,
            f"select atan(c7) from {dbname}.ct3",
            f"select atan(c8) from {dbname}.ct3",
            f"select atan(c9) from {dbname}.ct3",
            f"select atan(ts) from {dbname}.ct4" ,
            f"select atan(c7) from {dbname}.ct4",
            f"select atan(c8) from {dbname}.ct4",
            f"select atan(c9) from {dbname}.ct4",
            f"select atan(ts) from {dbname}.stb1" ,
            f"select atan(c7) from {dbname}.stb1",
            f"select atan(c8) from {dbname}.stb1",
            f"select atan(c9) from {dbname}.stb1" ,

            f"select atan(ts) from {dbname}.stbbb1" ,
            f"select atan(c7) from {dbname}.stbbb1",

            f"select atan(ts) from {dbname}.tbname",
            f"select atan(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)

        type_sql_lists = [
            f"select atan(c1) from {dbname}.t1",
            f"select atan(c2) from {dbname}.t1",
            f"select atan(c3) from {dbname}.t1",
            f"select atan(c4) from {dbname}.t1",
            f"select atan(c5) from {dbname}.t1",
            f"select atan(c6) from {dbname}.t1",

            f"select atan(c1) from {dbname}.ct1",
            f"select atan(c2) from {dbname}.ct1",
            f"select atan(c3) from {dbname}.ct1",
            f"select atan(c4) from {dbname}.ct1",
            f"select atan(c5) from {dbname}.ct1",
            f"select atan(c6) from {dbname}.ct1",

            f"select atan(c1) from {dbname}.ct3",
            f"select atan(c2) from {dbname}.ct3",
            f"select atan(c3) from {dbname}.ct3",
            f"select atan(c4) from {dbname}.ct3",
            f"select atan(c5) from {dbname}.ct3",
            f"select atan(c6) from {dbname}.ct3",

            f"select atan(c1) from {dbname}.stb1",
            f"select atan(c2) from {dbname}.stb1",
            f"select atan(c3) from {dbname}.stb1",
            f"select atan(c4) from {dbname}.stb1",
            f"select atan(c5) from {dbname}.stb1",
            f"select atan(c6) from {dbname}.stb1",

            f"select atan(c6) as alisb from {dbname}.stb1",
            f"select atan(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)


    def basic_atan_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select atan(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select atan(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select atan(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select atan(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select atan(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select atan(c6) from {dbname}.ct3")
        tdSql.checkRows(0)

        # # used for regular table
        tdSql.query(f"select atan(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 0.785398163)
        tdSql.checkData(3 , 0, 1.249045772)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 4, 1.11000)
        tdSql.checkData(3, 3, 33)
        tdSql.checkData(5, 4, None)

        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)

        self.check_result_auto_atan( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.t1", f"select atan(abs(c1)), atan(abs(c2)) ,atan(abs(c3)), atan(abs(c4)), atan(abs(c5)) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c2 ,atan(c2) from {dbname}.ct1")
        tdSql.checkData(0, 1, 1.570785077)
        tdSql.checkData(1 , 1, 1.570783470)
        tdSql.checkData(3 , 1, 1.570778327)
        tdSql.checkData(4 , 1, 0)

        tdSql.query(f"select c1, c5 ,atan(c5) from {dbname}.ct4")
        tdSql.checkData(0 , 2, None)
        tdSql.checkData(1 , 2, 1.458656162)
        tdSql.checkData(2 , 2, 1.442799803)
        tdSql.checkData(3 , 2, 1.421759533)
        tdSql.checkData(5 , 2, None)

        self.check_result_auto_atan( f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select atan(c1), atan(c2) ,atan(c3), atan(c4), atan(c5) from {dbname}.ct1")

        # nest query for atan functions
        tdSql.query(f"select c4  , atan(c4) ,atan(atan(c4)) , atan(atan(atan(c4))) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 88)
        tdSql.checkData(0 , 1 , 1.559433180)
        tdSql.checkData(0 , 2 , 1.000590740)
        tdSql.checkData(0 , 3 , 0.785693446)

        tdSql.checkData(1 , 0 , 77)
        tdSql.checkData(1 , 1 , 1.557810044)
        tdSql.checkData(1 , 2 , 1.000117426)
        tdSql.checkData(1 , 3 , 0.785456873)

        tdSql.checkData(11 , 0 , -99)
        tdSql.checkData(11 , 1 , -1.560695660)
        tdSql.checkData(11 , 2 , -1.000958403)
        tdSql.checkData(11 , 3 , -0.785877135)

        # used for stable table

        tdSql.query(f"select atan(c1) from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for not exists table
        tdSql.error(f"select atan(c1) from {dbname}.stbbb1")
        tdSql.error(f"select atan(c1) from {dbname}.tbname")
        tdSql.error(f"select atan(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, atan(c1) from {dbname}.ct1")
        tdSql.query(f"select c2, atan(c2) from {dbname}.ct4")

        # mix with common functions
        tdSql.query(f"select c1, atan(c1),atan(c1), atan(atan(c1)) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 ,1.405647649)
        tdSql.checkData(3 , 2 ,1.405647649)
        tdSql.checkData(3 , 3 ,0.952449745)

        tdSql.query(f"select c1, atan(c1),c5, floor(c5) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, atan(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, atan(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select atan(c1), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select atan(c1), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")

        # # bug fix for compute
        tdSql.query(f"select c1, atan(c1) -0 ,atan(c1-4)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 1.446441332)
        tdSql.checkData(1, 2, 1.325817664)

        tdSql.query(f" select c1, atan(c1) -0 ,atan(c1-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 1.446441332)
        tdSql.checkData(1, 2, 1.344883701)

        tdSql.query(f"select c1, atan(c1), c2, atan(c2), c3, atan(c3) from {dbname}.ct1")


    def check_big_number_atan(self, dbname="db"):

        tdSql.query(f"select c1, atan(100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.atan(100000000))

        tdSql.query(f"select c1, atan(10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(4, 1, math.atan(10000000000000))

        tdSql.query(f"select c1, atan(10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, atan(10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(1, 1, math.atan(10000000000000000000000000.0))

        tdSql.query(f"select c1, atan(10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, atan(10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(4, 1, math.atan(10000000000000000000000000000000000.0))

        tdSql.query(f"select c1, atan(10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, atan(10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value

        tdSql.checkData(4, 1, math.atan(10000000000000000000000000000000000000000.0))

        tdSql.query(f"select c1, atan(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow


    def abs_func_filter_atan(self, dbname="db"):
        tdSql.execute(f"use {dbname}")
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(atan(c1)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,1.000000000)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(atan(c1)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,1.000000000)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(atan(c1)-0.5) from {dbname}.ct4 where c1=atan(c1) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0.000000000)
        tdSql.checkData(0,3,0.000000000)
        tdSql.checkData(0,4,-0.100000000)
        tdSql.checkData(0,5,0.000000000)


    def pow_Arithmetic_atan(self):
        pass


    def check_boundary_values_atan(self, dbname="bound_test"):

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
        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        self.check_result_auto_atan( f"select abs(c1), abs(c2), abs(c3) , abs(c4), abs(c5) from {dbname}.sub1_bound ", f"select atan(abs(c1)), atan(abs(c2)) ,atan(abs(c3)), atan(abs(c4)), atan(abs(c5)) from {dbname}.sub1_bound")

        self.check_result_auto_atan( f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select atan(c1), atan(c2) ,atan(c3), atan(c3), atan(c2) ,atan(c1) from {dbname}.sub1_bound")

        self.check_result_auto_atan(f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select atan(abs(c1)) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select atan(abs(c1)) ,atan(abs(c2)) , atan(abs(c3)) , atan(abs(c4)), atan(abs(c5)), atan(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.atan(2147483647))
        tdSql.checkData(0,1,math.atan(9223372036854775807))
        tdSql.checkData(0,2,math.atan(32767))
        tdSql.checkData(0,3,math.atan(127))
        tdSql.checkData(0,4,math.atan(339999995214436424907732413799364296704.00000))
        tdSql.checkData(1,0,math.atan(2147483647))
        tdSql.checkData(1,1,math.atan(9223372036854775807))
        tdSql.checkData(1,2,math.atan(32767))
        tdSql.checkData(1,3,math.atan(127))
        tdSql.checkData(1,4,math.atan(339999995214436424907732413799364296704.00000))
        tdSql.checkData(3,0,math.atan(2147483646))
        tdSql.checkData(3,1,math.atan(9223372036854775806))
        tdSql.checkData(3,2,math.atan(32766))
        tdSql.checkData(3,3,math.atan(126))
        tdSql.checkData(3,4,math.atan(339999995214436424907732413799364296704.00000))

        # check  + - * / in functions
        tdSql.query(f"select atan(abs(c1+1)) ,atan(abs(c2)) , atan(abs(c3*1)) , atan(abs(c4/2)), atan(abs(c5))/2, atan(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.atan(2147483648.000000000))
        tdSql.checkData(0,1,math.atan(9223372036854775807))
        tdSql.checkData(0,2,math.atan(32767.000000000))
        tdSql.checkData(0,3,math.atan(63.500000000))

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

        self.check_result_auto_atan(f"select num1,num2 from {dbname}.tb3;" , f"select atan(num1),atan(num2) from {dbname}.tb3")


    def support_super_table_test_atan(self, dbname="db"):
        tdSql.execute(f" use {dbname} ")
        self.check_result_auto_atan( f" select c5 from {dbname}.stb1 order by ts " , f"select atan(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_atan( f" select c5 from {dbname}.stb1 order by tbname " , f"select atan(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_atan( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select atan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_atan( f" select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select atan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_atan( f" select t1,c5 from {dbname}.stb1 order by ts " , f"select atan(t1), atan(c5) from {dbname}.stb1 order by ts" )
        self.check_result_auto_atan( f" select t1,c5 from {dbname}.stb1 order by tbname " , f"select atan(t1) ,atan(c5) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_atan( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select atan(t1) ,atan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_atan( f" select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select atan(t1) , atan(c5) from {dbname}.stb1 where c1 > 0 order by tbname" )
        pass


    def test_fun_sca_atan(self):
        """ Fun: atan()

        1. Support types
        2. Basic query
        3. Big number query
        4. Boundary query
        5. Filter query
        6. Stable table query
        7. Error query

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_arctan.py
            - 2026-11-20 Alex Merged into test_fun_sca_trig.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas_atan()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors_atan()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types_atan()

        tdLog.printNoPrefix("==========step4: atan basic query ============")

        self.basic_atan_function()

        tdLog.printNoPrefix("==========step5: big number atan query ============")

        self.check_big_number_atan()


        tdLog.printNoPrefix("==========step6: atan boundary query ============")

        self.check_boundary_values_atan()

        tdLog.printNoPrefix("==========step7: atan filter query ============")

        self.abs_func_filter_atan()

        tdLog.printNoPrefix("==========step8: check atan result of  stable query ============")

        self.support_super_table_test_atan()
