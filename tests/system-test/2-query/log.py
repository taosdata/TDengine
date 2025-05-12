import taos
import sys
import datetime
import inspect
import math
from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
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

    def check_result_auto_log(self ,base , origin_query , log_query):

        log_result = tdSql.getResult(log_query)
        origin_result = tdSql.getResult(origin_query)

        auto_result =[]

        for row in origin_result:
            row_check = []
            for elem in row:
                if base ==1:
                    elem = None
                else:
                    if elem == None:
                        elem = None
                    elif elem ==1:
                        elem = 0.0
                    elif elem >0 and elem !=1 :
                        if  base==None :
                            elem = math.log(elem )
                        else:
                            print(base , elem)
                            elem = math.log(elem , base)
                    elif elem <=0:
                        elem = None
                row_check.append(elem)
            auto_result.append(row_check)

        tdSql.query(log_query)
        for row_index , row in enumerate(log_result):
            for col_index , elem in enumerate(row):
                tdSql.checkData(row_index , col_index ,auto_result[row_index][col_index])
               
    def test_errors(self, dbname="db"):
        error_sql_lists = [
            f"select log from {dbname}.t1",
            # f"select log(-+--+c1 ,2) from {dbname}.t1",
            # f"select +-log(c1,2) from {dbname}.t1",
            # f"select ++-log(c1,2) from {dbname}.t1",
            # f"select ++--log(c1,2) from {dbname}.t1",
            # f"select - -log(c1,2)*0 from {dbname}.t1",
            # f"select log(tbname+1,2) from {dbname}.t1 ",
            f"select log(123--123,2)==1 from {dbname}.t1",
            f"select log(c1,2) as 'd1' from {dbname}.t1",
            f"select log(c1 ,c2 ,2) from {dbname}.t1",
            f"select log(c1 ,NULL ,2) from {dbname}.t1",
            f"select log(, 2) from {dbname}.t1;",
            f"select log(log(c1, 2) ab from {dbname}.t1)",
            f"select log(c1 ,2 ) as int from {dbname}.t1",
            f"select log from {dbname}.stb1",
            # f"select log(-+--+c1) from {dbname}.stb1",
            # f"select +-log(c1) from {dbname}.stb1",
            # f"select ++-log(c1) from {dbname}.stb1",
            # f"select ++--log(c1) from {dbname}.stb1",
            # f"select - -log(c1)*0 from {dbname}.stb1",
            # f"select log(tbname+1) from {dbname}.stb1 ",
            f"select log(123--123 ,2)==1 from {dbname}.stb1",
            f"select log(c1 ,2) as 'd1' from {dbname}.stb1",
            f"select log(c1 ,c2 ,2 ) from {dbname}.stb1",
            f"select log(c1 ,NULL,2) from {dbname}.stb1",
            f"select log(,) from {dbname}.stb1;",
            f"select log(log(c1 , 2) ab from {dbname}.stb1)",
            f"select log(c1 , 2) as int from {dbname}.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self, dbname="db"):
        type_error_sql_lists = [
            f"select log(ts ,2 ) from {dbname}.t1" ,
            f"select log(c7,c2 ) from {dbname}.t1",
            f"select log(c8,c1 ) from {dbname}.t1",
            f"select log(c9,c2 ) from {dbname}.t1",
            f"select log(ts,c7 ) from {dbname}.ct1" ,
            f"select log(c7,c9 ) from {dbname}.ct1",
            f"select log(c8,c2 ) from {dbname}.ct1",
            f"select log(c9,c1 ) from {dbname}.ct1",
            f"select log(ts,2 ) from {dbname}.ct3" ,
            f"select log(c7,2 ) from {dbname}.ct3",
            f"select log(c8,2 ) from {dbname}.ct3",
            f"select log(c9,2 ) from {dbname}.ct3",
            f"select log(ts,2 ) from {dbname}.ct4" ,
            f"select log(c7,2 ) from {dbname}.ct4",
            f"select log(c8,2 ) from {dbname}.ct4",
            f"select log(c9,2 ) from {dbname}.ct4",
            f"select log(ts,2 ) from {dbname}.stb1" ,
            f"select log(c7,2 ) from {dbname}.stb1",
            f"select log(c8,2 ) from {dbname}.stb1",
            f"select log(c9,2 ) from {dbname}.stb1" ,

            f"select log(ts,2 ) from {dbname}.stbbb1" ,
            f"select log(c7,2 ) from {dbname}.stbbb1",

            f"select log(ts,2 ) from {dbname}.tbname",
            f"select log(c9,2 ) from {dbname}.tbname"

        ]

        for type_sql in type_error_sql_lists:
            tdSql.error(type_sql)


        type_sql_lists = [
            f"select log(c1,2 ) from {dbname}.t1",
            f"select log(c2,2 ) from {dbname}.t1",
            f"select log(c3,2 ) from {dbname}.t1",
            f"select log(c4,2 ) from {dbname}.t1",
            f"select log(c5,2 ) from {dbname}.t1",
            f"select log(c6,2 ) from {dbname}.t1",

            f"select log(c1,2 ) from {dbname}.ct1",
            f"select log(c2,2 ) from {dbname}.ct1",
            f"select log(c3,2 ) from {dbname}.ct1",
            f"select log(c4,2 ) from {dbname}.ct1",
            f"select log(c5,2 ) from {dbname}.ct1",
            f"select log(c6,2 ) from {dbname}.ct1",

            f"select log(c1,2 ) from {dbname}.ct3",
            f"select log(c2,2 ) from {dbname}.ct3",
            f"select log(c3,2 ) from {dbname}.ct3",
            f"select log(c4,2 ) from {dbname}.ct3",
            f"select log(c5,2 ) from {dbname}.ct3",
            f"select log(c6,2 ) from {dbname}.ct3",

            f"select log(c1,2 ) from {dbname}.stb1",
            f"select log(c2,2 ) from {dbname}.stb1",
            f"select log(c3,2 ) from {dbname}.stb1",
            f"select log(c4,2 ) from {dbname}.stb1",
            f"select log(c5,2 ) from {dbname}.stb1",
            f"select log(c6,2 ) from {dbname}.stb1",

            f"select log(c6,2) as alisb from {dbname}.stb1",
            f"select log(c6,2) alisb from {dbname}.stb1",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)

    def basic_log_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty

        tdSql.query(f"select log(c1 ,2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select log(c2 ,2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select log(c3 ,2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select log(c4 ,2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select log(c5 ,2) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select log(c6 ,2) from {dbname}.ct3")
        tdSql.checkRows(0)

        # log used for different param types

        tdSql.query(f"select log(c1,c2) from {dbname}.ct1;")
        tdSql.query(f"select log(c1,c2) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(c1,2) from {dbname}.ct1;")
        tdSql.query(f"select log(c1,2) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(2,c2) from {dbname}.ct1;")
        tdSql.query(f"select log(2,c2) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(2,1) from {dbname}.ct1;")
        tdSql.query(f"select log(2,2) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(2,floor(1)) from {dbname}.ct1;")
        tdSql.query(f"select log(2,floor(2)) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(abs(2),floor(1)) from {dbname}.ct1;")
        tdSql.query(f"select log(abs(2),floor(2)) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(abs(c2),c1) from {dbname}.ct1;")
        tdSql.query(f"select log(abs(c2),c1) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(c2,abs(c1)) from {dbname}.ct1;")
        tdSql.query(f"select log(c2,abs(c1)) from {dbname}.stb1 partition by tbname order by tbname;")

        tdSql.query(f"select log(abs(c2),2) from {dbname}.ct1;")
        tdSql.query(f"select log(abs(c2),2) from {dbname}.stb1 partition by tbname order by tbname;")



        # # used for regular table
        tdSql.query(f"select log(c1 ,2) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 0.000000000)
        tdSql.checkData(3 , 0, 1.584962501)
        tdSql.checkData(5 , 0, None)

        tdSql.query(f"select log(c1) from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1 , 0, 0.000000000)
        tdSql.checkData(2 , 0, 0.693147181)
        tdSql.checkData(3 , 0, 1.098612289)
        tdSql.checkData(4 , 0, 1.386294361)

        self.check_result_auto_log( None , f"select c1, c2, c3 , c4, c5 from {dbname}.t1", f"select log(c1), log(c2) ,log(c3), log(c4), log(c5) from {dbname}.t1")
        self.check_result_auto_log( 2 ,  f"select c1, c2, c3 , c4, c5 from {dbname}.t1", f"select log(c1 ,2), log(c2 ,2) ,log(c3, 2), log(c4 ,2), log(c5 ,2) from {dbname}.t1")
        self.check_result_auto_log( 1, f"select c1, c2, c3 , c4, c5 from {dbname}.t1", f"select log(c1 ,1), log(c2 ,1) ,log(c3, 1), log(c4 ,1), log(c5 ,1) from {dbname}.t1")
        self.check_result_auto_log( 10 ,f"select c1, c2, c3 , c4, c5 from {dbname}.t1", f"select log(c1 ,10), log(c2 ,10) ,log(c3, 10), log(c4 ,10), log(c5 ,10) from {dbname}.t1")

        # used for sub table
        tdSql.query(f"select c1 ,log(c1 ,3) from {dbname}.ct1")
        tdSql.checkData(0, 1, 1.892789261)
        tdSql.checkData(1 , 1, 1.771243749)
        tdSql.checkData(3 , 1, 1.464973521)
        tdSql.checkData(4 , 1, None)

        # test bug fix for log(c1,c2)

        tdSql.query(f"select c1, c2 ,log(c1,c2) from {dbname}.ct1")
        tdSql.checkData(0 , 2, 0.182485070)
        tdSql.checkData(1 , 2, 0.172791608)
        tdSql.checkData(2 , 2, 0.161311499)
        tdSql.checkData(3 , 2, 0.147315235)
        tdSql.checkData(4 , 2, None)

        self.check_result_auto_log( None ,f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select log(c1), log(c2) ,log(c3), log(c4), log(c5) from {dbname}.ct1")
        self.check_result_auto_log( 2, f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select log(c1,2), log(c2,2) ,log(c3,2), log(c4,2), log(c5,2) from {dbname}.ct1")
        self.check_result_auto_log( 10 , f"select c1, c2, c3 , c4, c5 from {dbname}.ct1", f"select log(c1,10), log(c2,10) ,log(c3,10), log(c4,10), log(c5,10) from {dbname}.ct1")

        # nest query for log functions
        tdSql.query(f"select c1  , log(c1,3) ,log(log(c1,3),3) , log(log(log(c1,3),3),3) from {dbname}.ct1;")
        tdSql.checkData(0 , 0 , 8)
        tdSql.checkData(0 , 1 , 1.892789261)
        tdSql.checkData(0 , 2 , 0.580779541)
        tdSql.checkData(0 , 3 , -0.494609470)

        tdSql.checkData(1 , 0 , 7)
        tdSql.checkData(1 , 1 , 1.771243749)
        tdSql.checkData(1 , 2 , 0.520367366)
        tdSql.checkData(1 , 3 , -0.594586689)

        tdSql.checkData(4 , 0 , 0)
        tdSql.checkData(4 , 1 , None)
        tdSql.checkData(4 , 2 , None)
        tdSql.checkData(4 , 3 , None)

        # # used for stable table

        tdSql.query(f"select log(c1, 2) from {dbname}.stb1")
        tdSql.checkRows(25)

        

        # used for not exists table
        tdSql.error(f"select log(c1, 2) from {dbname}.stbbb1")
        tdSql.error(f"select log(c1, 2) from {dbname}tbname")
        tdSql.error(f"select log(c1, 2) from {dbname}.ct5")

        # mix with common col
        tdSql.query(f"select c1, log(c1 ,2) from {dbname}.ct1")
        tdSql.checkData(0 , 0 ,8)
        tdSql.checkData(0 , 1 ,3.000000000)
        tdSql.checkData(4 , 0 ,0)
        tdSql.checkData(4 , 1 ,None)
        tdSql.query(f"select c1, log(c1,2) from {dbname}.ct4")
        tdSql.checkData(0 , 0 , None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(4 , 0 ,5)
        tdSql.checkData(4 , 1 ,2.321928095)
        tdSql.checkData(5 , 0 ,None)
        tdSql.checkData(5 , 1 ,None)
        tdSql.query(f"select c1, log(c1 ,2 ) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(4 , 0 ,5)
        tdSql.checkData(4 , 1 ,2.321928095)

        # mix with common functions
        tdSql.query(f"select c1, log(c1 ,2),c5, log(c5 ,2) from {dbname}.ct4 ")
        tdSql.checkData(0 , 0 ,None)
        tdSql.checkData(0 , 1 ,None)
        tdSql.checkData(0 , 2 ,None)
        tdSql.checkData(0 , 3 ,None)

        tdSql.checkData(3 , 0 , 6)
        tdSql.checkData(3 , 1 , 2.584962501)
        tdSql.checkData(3 , 2 ,6.66000)
        tdSql.checkData(3 , 3 ,2.735522144)

        tdSql.query(f"select c1, log(c1,1),c5, floor(c5 ) from {dbname}.stb1 ")

        # # mix with agg functions , not support
        tdSql.error(f"select c1, log(c1 ,2),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1, log(c1 ,2),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select log(c1 ,2), count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select log(c1 ,2), count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select max(c5), count(c5) from {dbname}.stb1")
        tdSql.query(f"select max(c5), count(c5) from {dbname}.ct1")


        # bug fix for count
        tdSql.query(f"select count(c1) from {dbname}.ct4 ")
        tdSql.checkData(0,0,9)
        tdSql.query(f"select count(*) from {dbname}.ct4 ")
        tdSql.checkData(0,0,12)
        tdSql.query(f"select count(c1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,22)
        tdSql.query(f"select count(*) from {dbname}.stb1 ")
        tdSql.checkData(0,0,25)

        # # bug fix for compute
        tdSql.query(f"select c1, log(c1 ,2) -0 ,log(c1-4 ,2)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 3.000000000)
        tdSql.checkData(1, 2, 2.000000000)

        tdSql.query(f"select c1, log(c1 ,2) -0 ,log(c1-0.1 ,2)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 3.000000000)
        tdSql.checkData(1, 2, 2.881852653)

        tdSql.query(f"select c1, log(c1, -10), c2, log(c2, -10), c3, log(c3, -10) from {dbname}.ct1")

    def test_big_number(self, dbname="db"):

        tdSql.query(f"select c1, log(c1, 100000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(0, 1, 0.112886248)
        tdSql.checkData(1, 1, 0.105637255)
        tdSql.checkData(4, 1, None)


        tdSql.query(f"select c1, log(c1, 10000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.checkData(0, 1, 0.069468461)
        tdSql.checkData(1, 1, 0.065007542)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, log(c1, 10000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, log(c1, 10000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(0, 1, 0.036123599)
        tdSql.checkData(1, 1, 0.033803922)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, log(c1, 10000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, log(c1, 10000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(0, 1, 0.026561470)
        tdSql.checkData(1, 1, 0.024855825)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, log(c1, 10000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow
        tdSql.query(f"select c1, log(c1, 10000000000000000000000000000000000000000.0) from {dbname}.ct1") # 10000000000000000000000000.0 is a double value
        tdSql.checkData(0, 1, 0.022577250)
        tdSql.checkData(1, 1, 0.021127451)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, log(c1, 10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000) from {dbname}.ct1")  # bigint to double data overflow

    def log_base_test(self, dbname="db"):

        # base is an regular number ,int or double
        tdSql.query(f"select c1, log(c1, 2) from {dbname}.ct1")
        tdSql.checkData(0, 1,3.000000000)
        tdSql.query(f"select c1, log(c1, 2.0) from {dbname}.ct1")
        tdSql.checkData(0, 1, 3.000000000)

        tdSql.query(f"select c1, log(1, 2.0) from {dbname}.ct1")
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkRows(13)


        # # bug for compute in functions
        # tdSql.query(f"select c1, abs(1/0) from {dbname}.ct1")
        # tdSql.checkData(0, 0, 8)
        # tdSql.checkData(0, 1, 1)

        tdSql.query(f"select c1, log(1, 2.0) from {dbname}.ct1")
        tdSql.checkData(0, 1, 0.000000000)
        tdSql.checkRows(13)

        # two cols start log(x,y)
        tdSql.query(f"select c1,c2, log(c1,c2) from {dbname}.ct1")
        tdSql.checkData(0, 2, 0.182485070)
        tdSql.checkData(1, 2, 0.172791608)
        tdSql.checkData(4, 2, None)

        tdSql.query(f"select c1,c2, log(c2,c1) from {dbname}.ct1")
        tdSql.checkData(0, 2, 5.479900349)
        tdSql.checkData(1, 2, 5.787318105)
        tdSql.checkData(4, 2, None)

        tdSql.query(f"select c1, log(2.0 , c1) from {dbname}.ct1")
        tdSql.checkData(0, 1, 0.333333333)
        tdSql.checkData(1, 1, 0.356207187)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select c1, log(2.0 , ceil(abs(c1))) from {dbname}.ct1")
        tdSql.checkData(0, 1, 0.333333333)
        tdSql.checkData(1, 1, 0.356207187)
        tdSql.checkData(4, 1, None)


    def abs_func_filter(self, dbname="db"):
        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from {dbname}.ct4 where c1>5 ")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,8.000000000)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,7.900000000)
        tdSql.checkData(0,4,3.000000000)

        tdSql.query(f"select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from {dbname}.ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5)
        tdSql.checkData(0,1,5.000000000)
        tdSql.checkData(0,2,5.000000000)
        tdSql.checkData(0,3,4.900000000)
        tdSql.checkData(0,4,2.000000000)

        tdSql.query(f"select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from {dbname}.ct4 where c1>log(c1,2) limit 1 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,8)
        tdSql.checkData(0,1,88888)
        tdSql.checkData(0,2,8.000000000)
        tdSql.checkData(0,3,8.000000000)
        tdSql.checkData(0,4,7.900000000)
        tdSql.checkData(0,5,3.000000000)


    def check_boundary_values(self, dbname="bound_test"):

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database if not exists {dbname}")
        time.sleep(3)
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
        self.check_result_auto_log(None ,  f"select c1, c2, c3 , c4, c5 ,c6 from {dbname}.sub1_bound ", f"select log(c1), log(c2) ,log(c3), log(c4), log(c5) ,log(c6) from {dbname}.sub1_bound")
        self.check_result_auto_log( 2 ,  f"select c1, c2, c3 , c4, c5 ,c6 from {dbname}.sub1_bound ", f"select log(c1,2), log(c2,2) ,log(c3,2), log(c4,2), log(c5,2) ,log(c6,2) from {dbname}.sub1_bound")
        self.check_result_auto_log( 10 , f"select c1, c2, c3 , c4, c5 ,c6 from {dbname}.sub1_bound ", f"select log(c1,10), log(c2,10) ,log(c3,10), log(c4,10), log(c5,10) ,log(c6,10) from {dbname}.sub1_bound")

        self.check_result_auto_log( 2 , f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select log(c1,2), log(c2,2) ,log(c3,2), log(c3,2), log(c2,2) ,log(c1,2) from {dbname}.sub1_bound")
        self.check_result_auto_log( None ,  f"select c1, c2, c3 , c3, c2 ,c1 from {dbname}.sub1_bound ", f"select log(c1), log(c2) ,log(c3), log(c3), log(c2) ,log(c1) from {dbname}.sub1_bound")


        self.check_result_auto_log(2 , f"select abs(abs(abs(abs(abs(abs(abs(abs(abs(c1)))))))))  nest_col_func from {dbname}.sub1_bound" , f"select log(abs(c1) ,2) from {dbname}.sub1_bound" )

        # check basic elem for table per row
        tdSql.query(f"select log(abs(c1),2) ,log(abs(c2),2) , log(abs(c3),2) , log(abs(c4),2), log(abs(c5),2), log(abs(c6),2) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.log(2147483647,2))
        tdSql.checkData(0,1,math.log(9223372036854775807 ,2))
        tdSql.checkData(0,2,math.log(32767,2))
        tdSql.checkData(0,3,math.log(127 ,2))
        tdSql.checkData(0,4,math.log(339999995214436424907732413799364296704.00000,2))
        tdSql.checkData(0,5,math.log(169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000 ,2))
        tdSql.checkData(1,0,math.log(2147483647 ,2))
        tdSql.checkData(1,1,math.log(9223372036854775807 ,2))
        tdSql.checkData(1,2,math.log(32767 ,2))
        tdSql.checkData(1,3,math.log(127,2))
        tdSql.checkData(1,4,math.log(339999995214436424907732413799364296704.00000 ,2))
        tdSql.checkData(1,5,math.log(169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000 ,2))
        tdSql.checkData(3,0,math.log(2147483646,2))
        tdSql.checkData(3,1,math.log(9223372036854775806,2))
        tdSql.checkData(3,2,math.log(32766,2))
        tdSql.checkData(3,3,math.log(126 ,2))
        tdSql.checkData(3,4,math.log(339999995214436424907732413799364296704.00000,2))
        tdSql.checkData(3,5,math.log(169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000,2))

        # check basic elem for table per row
        tdSql.query(f"select log(abs(c1)) ,log(abs(c2)) , log(abs(c3)) , log(abs(c4)), log(abs(c5)), log(abs(c6)) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.log(2147483647))
        tdSql.checkData(0,1,math.log(9223372036854775807))
        tdSql.checkData(0,2,math.log(32767))
        tdSql.checkData(0,3,math.log(127))
        tdSql.checkData(0,4,math.log(339999995214436424907732413799364296704.00000))
        tdSql.checkData(0,5,math.log(169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000 ))
        tdSql.checkData(1,0,math.log(2147483647))
        tdSql.checkData(1,1,math.log(9223372036854775807))
        tdSql.checkData(1,2,math.log(32767))
        tdSql.checkData(1,3,math.log(127))
        tdSql.checkData(1,4,math.log(339999995214436424907732413799364296704.00000 ))
        tdSql.checkData(1,5,math.log(169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000))
        tdSql.checkData(3,0,math.log(2147483646))
        tdSql.checkData(3,1,math.log(9223372036854775806))
        tdSql.checkData(3,2,math.log(32766))
        tdSql.checkData(3,3,math.log(126))
        tdSql.checkData(3,4,math.log(339999995214436424907732413799364296704.00000))
        tdSql.checkData(3,5,math.log(169999999999999993883079578865998174333346074304075874502773119193537729178160565864330091787584707988572262467983188919169916105593357174268369962062473635296474636515660464935663040684957844303524367815028553272712298986386310828644513212353921123253311675499856875650512437415429217994623324794855339589632.000000000))

        # check  + - * / in functions
        tdSql.query(f"select log(abs(c1+1) ,2) ,log(abs(c2),2) , log(abs(c3*1),2) , log(abs(c4/2),2), log(abs(c5) ,2)/2, log(abs(c6) ,2) from {dbname}.sub1_bound ")
        tdSql.checkData(0,0,math.log(2147483648.000000000,2))
        tdSql.checkData(0,1,math.log(9223372036854775807,2))
        tdSql.checkData(0,2,math.log(32767.000000000,2))
        tdSql.checkData(0,3,math.log(63.500000000,2))
        tdSql.checkData(0,4,63.999401166)

    def support_super_table_test(self, dbname="db"):

        self.check_result_auto_log( 2 ,  f"select c5 from {dbname}.stb1 order by ts " , f"select log(c5,2) from {dbname}.stb1 order by ts" )
        self.check_result_auto_log( 2 ,f"select c5 from {dbname}.stb1 order by tbname " , f"select log(c5,2) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_log( 2 ,f"select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select log(c5,2) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_log( 2 , f"select c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select log(c5,2) from {dbname}.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto_log( 2 , f"select t1,c5 from {dbname}.stb1 order by ts " , f"select log(t1,2), log(c5,2) from {dbname}.stb1 order by ts" )
        self.check_result_auto_log( 2 , f"select t1,c5 from {dbname}.stb1 order by tbname " , f"select log(t1,2) ,log(c5,2) from {dbname}.stb1 order by tbname" )
        self.check_result_auto_log( 2 , f"select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select log(t1,2) ,log(c5,2) from {dbname}.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto_log( 2 ,f"select t1,c5 from {dbname}.stb1 where c1 > 0 order by tbname  " , f"select log(t1,2) , log(c5,2) from {dbname}.stb1 where c1 > 0 order by tbname" )

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.test_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: log basic query ============")

        self.basic_log_function()

        tdLog.printNoPrefix("==========step5: big number log query ============")

        self.test_big_number()

        tdLog.printNoPrefix("==========step6: base  number for log query ============")

        self.log_base_test()

        tdLog.printNoPrefix("==========step7: log boundary query ============")

        self.check_boundary_values()

        tdLog.printNoPrefix("==========step8: log filter query ============")

        self.abs_func_filter()

        tdLog.printNoPrefix("==========step9: check log result of  stable query ============")

        self.support_super_table_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
