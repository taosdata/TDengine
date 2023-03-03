import taos
import sys

import math
import numpy as np
from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import TDSetSql
from util.common import *
class TDTestCase:
    # updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    # "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    # "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.setsql = TDSetSql()
        self.column_dict = {
            'ts':'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
    
        }
        self.dbname = tdCom.getLongName(3,"letters")
        self.row_num = 10
        self.ts = 1537146000000
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)

    def avg_check_unsigned(self):
        stbname = f'{self.dbname}.{tdCom.getLongName(5,"letters")}'
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname}")
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        tdSql.execute(f"create table {stbname}_1 using {stbname} tags({tag_values[0]})")
        self.insert_data(self.column_dict,f'{stbname}_1',self.row_num)
        for col in self.column_dict.keys():
            col_val_list = []
            if col.lower() != 'ts':
                tdSql.query(f'select {col} from {stbname}_1')
                sum_val = 0
                for col_val in tdSql.queryResult:
                    col_val_list.append(col_val[0])
                col_avg = np.mean(col_val_list)
                tdSql.query(f'select avg({col}) from {stbname}_1')
                tdSql.checkEqual(col_avg,tdSql.queryResult[0][0])
        tdSql.execute(f'drop database {self.dbname}')

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

    def check_avg(self ,origin_query , check_query):
        avg_result = tdSql.getResult(origin_query)
        origin_result = tdSql.getResult(check_query)

        tdSql.query(origin_query)
        for row_index , row in enumerate(avg_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index,col_index,origin_result[row_index][col_index])

    def test_errors(self, dbname="db"):
        error_sql_lists = [
            f"select avg from {dbname}.t1",
            # f"select avg(-+--+c1) from {dbname}.t1",
            # f"select +-avg(c1) from {dbname}.t1",
            # f"select ++-avg(c1) from {dbname}.t1",
            # f"select ++--avg(c1) from {dbname}.t1",
            # f"select - -avg(c1)*0 from {dbname}.t1",
            # f"select avg(tbname+1) from {dbname}.t1 ",
            f"select avg(123--123)==1 from {dbname}.t1",
            f"select avg(c1) as 'd1' from {dbname}.t1",
            f"select avg(c1 ,c2 ) from {dbname}.t1",
            f"select avg(c1 ,NULL) from {dbname}.t1",
            f"select avg(,) from {dbname}.t1;",
            f"select avg(avg(c1) ab from {dbname}.t1)",
            f"select avg(c1) as int from {dbname}.t1",
            f"select avg from {dbname}.stb1",
            # f"select avg(-+--+c1) from {dbname}.stb1",
            # f"select +-avg(c1) from {dbname}.stb1",
            # f"select ++-avg(c1) from {dbname}.stb1",
            # f"select ++--avg(c1) from {dbname}.stb1",
            # f"select - -avg(c1)*0 from {dbname}.stb1",
            # f"select avg(tbname+1) from {dbname}.stb1 ",
            f"select avg(123--123)==1 from {dbname}.stb1",
            f"select avg(c1) as 'd1' from {dbname}.stb1",
            f"select avg(c1 ,c2 ) from {dbname}.stb1",
            f"select avg(c1 ,NULL) from {dbname}.stb1",
            f"select avg(,) from {dbname}.stb1;",
            f"select avg(avg(c1) ab from {dbname}.stb1)",
            f"select avg(c1) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self, dbname="db"):
        type_error_sql_lists = [
            f"select avg(ts) from {dbname}.t1" ,
            f"select avg(c7) from {dbname}.t1",
            f"select avg(c8) from {dbname}.t1",
            f"select avg(c9) from {dbname}.t1",
            f"select avg(ts) from {dbname}.ct1" ,
            f"select avg(c7) from {dbname}.ct1",
            f"select avg(c8) from {dbname}.ct1",
            f"select avg(c9) from {dbname}.ct1",
            f"select avg(ts) from {dbname}.ct3" ,
            f"select avg(c7) from {dbname}.ct3",
            f"select avg(c8) from {dbname}.ct3",
            f"select avg(c9) from {dbname}.ct3",
            f"select avg(ts) from {dbname}.ct4" ,
            f"select avg(c7) from {dbname}.ct4",
            f"select avg(c8) from {dbname}.ct4",
            f"select avg(c9) from {dbname}.ct4",
            f"select avg(ts) from {dbname}.stb1" ,
            f"select avg(c7) from {dbname}.stb1",
            f"select avg(c8) from {dbname}.stb1",
            f"select avg(c9) from {dbname}.stb1" ,

            f"select avg(ts) from {dbname}.stbbb1" ,
            f"select avg(c7) from {dbname}.stbbb1",

            f"select avg(ts) from {dbname}.tbname",
            f"select avg(c9) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)


        type_sql_lists = [
            f"select avg(c1) from {dbname}.t1",
            f"select avg(c2) from {dbname}.t1",
            f"select avg(c3) from {dbname}.t1",
            f"select avg(c4) from {dbname}.t1",
            f"select avg(c5) from {dbname}.t1",
            f"select avg(c6) from {dbname}.t1",

            f"select avg(c1) from {dbname}.ct1",
            f"select avg(c2) from {dbname}.ct1",
            f"select avg(c3) from {dbname}.ct1",
            f"select avg(c4) from {dbname}.ct1",
            f"select avg(c5) from {dbname}.ct1",
            f"select avg(c6) from {dbname}.ct1",

            f"select avg(c1) from {dbname}.ct3",
            f"select avg(c2) from {dbname}.ct3",
            f"select avg(c3) from {dbname}.ct3",
            f"select avg(c4) from {dbname}.ct3",
            f"select avg(c5) from {dbname}.ct3",
            f"select avg(c6) from {dbname}.ct3",

            f"select avg(c1) from {dbname}.stb1",
            f"select avg(c2) from {dbname}.stb1",
            f"select avg(c3) from {dbname}.stb1",
            f"select avg(c4) from {dbname}.stb1",
            f"select avg(c5) from {dbname}.stb1",
            f"select avg(c6) from {dbname}.stb1",

            f"select avg(c6) as alisb from {dbname}.stb1",
            f"select avg(c6) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)

    def basic_avg_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select avg(c1) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select avg(c2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select avg(c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select avg(c4) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select avg(c5) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select avg(c6) from {dbname}.ct3")

        # used for regular table
        tdSql.query(f"select avg(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, 5.000000000)


        tdSql.query(f"select ts,c1, c2, c3 , c4, c5 from {dbname}.t1")
        tdSql.checkData(1, 5, 1.11000)
        tdSql.checkData(3, 4, 33)
        tdSql.checkData(5, 5, None)
        self.check_avg(f" select avg(c1) , avg(c2) , avg(c3) from {dbname}.t1 " , f" select sum(c1)/count(c1) , sum(c2)/count(c2) , sum(c3)/count(c3) from {dbname}.t1 ")

        # used for sub table
        tdSql.query(f"select avg(c1) from {dbname}.ct1")
        tdSql.checkData(0, 0, 4.846153846)

        tdSql.query(f"select avg(c1) from {dbname}.ct3")
        tdSql.checkRows(0)

        self.check_avg(f" select avg(abs(c1)) , avg(abs(c2)) , avg(abs(c3)) from {dbname}.t1 " , f" select sum(abs(c1))/count(c1) , sum(abs(c2))/count(c2) , sum(abs(c3))/count(c3) from {dbname}.t1 ")
        self.check_avg(f" select avg(abs(c1)) , avg(abs(c2)) , avg(abs(c3)) from {dbname}.stb1 " , f" select sum(abs(c1))/count(c1) , sum(abs(c2))/count(c2) , sum(abs(c3))/count(c3) from {dbname}.stb1 ")

        # used for stable table

        tdSql.query(f"select avg(c1) from {dbname}.stb1")
        tdSql.checkRows(1)

        self.check_avg(f" select avg(abs(ceil(c1))) , avg(abs(ceil(c2))) , avg(abs(ceil(c3))) from {dbname}.stb1 " , f" select sum(abs(ceil(c1)))/count(c1) , sum(abs(ceil(c2)))/count(c2) , sum(abs(ceil(c3)))/count(c3) from {dbname}.stb1 ")

        # used for not exists table
        tdSql.error(f"select avg(c1) from {dbname}.stbbb1")
        tdSql.error(f"select avg(c1) from {dbname}.tbname")
        tdSql.error(f"select avg(c1) from {dbname}.ct5")

        # mix with common col
        tdSql.error(f"select c1, avg(c1) from {dbname}.ct1")
        tdSql.error(f"select c1, avg(c1) from {dbname}.ct4")


        # mix with common functions
        tdSql.error(f"select c1, avg(c1),c5, floor(c5) from {dbname}.ct4 ")
        tdSql.error(f"select c1, avg(c1),c5, floor(c5) from {dbname}.stb1 ")

        # mix with agg functions , not support
        tdSql.error(f"select c1, avg(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, avg(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f" select max(c5), count(c5) , avg(c5) from {dbname}.stb1 ")
        tdSql.checkData(0, 0, 8.88000 )
        tdSql.checkData(0, 1, 22 )
        tdSql.checkData(0, 2, 2.270454591 )

        tdSql.query(f" select max(c5), count(c5) , avg(c5) ,elapsed(ts) , spread(c1)  from {dbname}.ct1; ")
        tdSql.checkData(0, 0, 8.88000 )
        tdSql.checkData(0, 1, 13 )
        tdSql.checkData(0, 2, 0.768461603 )

        # bug fix for count
        tdSql.query(f"select count(c1) from {dbname}.ct4 ")
        tdSql.checkData(0,0,9)
        tdSql.query(f"select count(*) from {dbname}.ct4 ")
        tdSql.checkData(0,0,12)
        tdSql.query(f"select count(c1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,22)
        tdSql.query(f"select count(*) from {dbname}.stb1 ")
        tdSql.checkData(0,0,25)

        # bug fix for compute
        tdSql.error(f"select c1, avg(c1) -0 ,ceil(c1)-0 from {dbname}.ct4 ")
        tdSql.error(f" select c1, avg(c1) -0 ,avg(ceil(c1-0.1))-0.1 from {dbname}.ct4")

        # mix with nest query
        self.check_avg(f"select avg(col) from (select abs(c1) col from {dbname}.stb1)" , f"select avg(abs(c1)) from {dbname}.stb1")
        self.check_avg(f"select avg(col) from (select ceil(abs(c1)) col from {dbname}.stb1)" , f"select avg(abs(c1)) from {dbname}.stb1")

        tdSql.query(f" select abs(avg(abs(abs(c1)))) from {dbname}.stb1 ")
        tdSql.checkData(0, 0, 4.500000000)
        tdSql.query(f" select abs(avg(abs(abs(c1)))) from {dbname}.t1 ")
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(f" select abs(avg(abs(abs(c1)))) from {dbname}.stb1 ")
        tdSql.checkData(0, 0, 4.500000000)

        tdSql.query(f" select avg(c1) from {dbname}.stb1 where c1 is null ")
        tdSql.checkRows(1)


    def avg_func_filter(self, dbname="db"):
        tdSql.execute(f"use {dbname}")
        tdSql.query(f" select avg(c1), avg(c1) -0 ,avg(ceil(c1-0.1))-0 ,avg(floor(c1+0.1))-0.1 ,avg(ceil(log(c1,2)-0.5)) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,7.000000000)
        tdSql.checkData(0,1,7.000000000)
        tdSql.checkData(0,2,7.000000000)
        tdSql.checkData(0,3,6.900000000)
        tdSql.checkData(0,4,3.000000000)

        tdSql.query(f"select avg(c1), avg(c1) -0 ,avg(ceil(c1-0.1))-0 ,avg(floor(c1+0.1))-0.1 ,avg(ceil(log(c1,2)-0.5)) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5.000000000)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,2.000000000)

        tdSql.query(f"select avg(c1) ,avg(c2) , avg(c1) -0 , avg(ceil(c1-0.1))-0 ,avg(floor(c1+0.1))-0.1 ,avg(ceil(log(c1,2))-0.5) from {dbname}.ct4 where c1>log(c1,2) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.500000000)
        tdSql.checkData(0, 1, 49999.500000000)
        tdSql.checkData(0, 5, 1.625000000)

    def check_boundary_values(self, dbname="bound_test"):

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
                f"insert into {dbname}.sub1_bound values ( now()+5s, 2147483645, 9223372036854775805, 32765, 125, 3.40E+37, 1.7e+307, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483644, 9223372036854775804, 32764, 124, 3.40E+37, 1.7e+307, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+15s, -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+20s, 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )


        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+5s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        #self.check_avg(f"select avg(c1), avg(c2), avg(c3) , avg(c4), avg(c5) ,avg(c6) from {dbname}.sub1_bound " , f" select sum(c1)/count(c1), sum(c2)/count(c2) ,sum(c3)/count(c3), sum(c4)/count(c4), sum(c5)/count(c5) ,sum(c6)/count(c6) from {dbname}.sub1_bound ")


        # check basic elem for table per row
        tdSql.query(f"select avg(c1) ,avg(c2) , avg(c3) , avg(c4), avg(c5), avg(c6) from {dbname}.sub1_bound ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,920350133.571428537)
        tdSql.checkData(0,1,3.952873730080618e+18)
        tdSql.checkData(0,2,14042.142857143)
        tdSql.checkData(0,3,53.571428571)
        tdSql.checkData(0,4,5.828571332045761e+37)
        tdSql.checkData(0,5,None)


        # check  + - * / in functions
        tdSql.query(f" select avg(c1+1) ,avg(c2) , avg(c3*1) , avg(c4/2), avg(c5)/2, avg(c6) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,920350134.5714285)
        tdSql.checkData(0,1,3.952873730080618e+18)
        tdSql.checkData(0,2,14042.142857143)
        tdSql.checkData(0,3,26.785714286)
        tdSql.checkData(0,4,2.9142856660228804e+37)
        tdSql.checkData(0,5,None)

    #
    # test bigint to check overflow
    #
    def avg_check_overflow(self):
        # create db
        tdSql.execute(f"drop database if exists db")
        tdSql.execute(f"create database if not exists db")
        time.sleep(3)
        tdSql.execute(f"use db")
        tdSql.execute(f"create table db.st(ts timestamp, ibv bigint, ubv bigint unsigned) tags(area int)")
        # insert t1 data
        tdSql.execute(f"insert into db.t1 using db.st tags(1) values(now,9223372036854775801,18446744073709551611)")
        tdSql.execute(f"insert into db.t1 using db.st tags(1) values(now,8223372036854775801,17446744073709551611)")
        tdSql.execute(f"insert into db.t1 using db.st tags(1) values(now,7223372036854775801,16446744073709551611)")
        # insert t2 data
        tdSql.execute(f"insert into db.t2 using db.st tags(2) values(now,9223372036854775801,18446744073709551611)")
        tdSql.execute(f"insert into db.t2 using db.st tags(2) values(now,8223372036854775801,17446744073709551611)")
        tdSql.execute(f"insert into db.t2 using db.st tags(2) values(now,7223372036854775801,16446744073709551611)")
        
        # check single table answer
        tdSql.query(f"select avg(ibv), avg(ubv) from db.t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0,8.223372036854776e+18)
        tdSql.checkData(0, 1,1.744674407370955e+19)

        # check super table
        tdSql.query(f"select avg(ibv), avg(ubv) from db.st")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0,8.223372036854776e+18)
        tdSql.checkData(0, 1,1.744674407370955e+19)

        # check child query
        tdSql.query(f"select avg(ibv), avg(ubv) from (select * from db.st)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0,8.223372036854776e+18)
        tdSql.checkData(0, 1,1.744674407370955e+19)

        # check group by
        tdSql.query(f"select avg(ibv), avg(ubv) from db.st group by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0,8.223372036854776e+18)
        tdSql.checkData(0, 1,1.744674407370955e+19)
        tdSql.checkData(1, 0,8.223372036854776e+18)
        tdSql.checkData(1, 1,1.744674407370955e+19)

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.test_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: avg basic query ============")

        self.basic_avg_function()

        tdLog.printNoPrefix("==========step5: avg boundary query ============")

        self.check_boundary_values()

        tdLog.printNoPrefix("==========step6: avg filter query ============")

        self.avg_func_filter()
        self.avg_check_unsigned()

        # check avg overflow
        self.avg_check_overflow()
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
