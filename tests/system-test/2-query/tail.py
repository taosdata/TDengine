from random import randint, random
from numpy import equal
import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 , 
    "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"fnDebugFlag":143}

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
    
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
        tdSql.execute("insert into ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute("insert into ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute("insert into ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute("insert into ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute("insert into ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute("insert into ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute("insert into ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

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
    
    def test_errors(self):
        error_sql_lists = [
            "select tail from t1",
            "select tail(123--123)==1 from t1",
            "select tail(123,123) from t1",
            "select tail(c1,ts) from t1",
            "select tail(c1,c1,ts) from t1",
            "select tail(c1) as 'd1' from t1",
            "select tail(c1 ,c2 ) from t1",
            "select tail(c1 ,NULL) from t1",
            "select tail(,) from t1;",
            "select tail(tail(c1) ab from t1)",
            "select tail(c1) as int from t1",
            "select tail('c1') from t1",
            "select tail(NULL) from t1",
            "select tail('') from t1",
            "select tail(c%) from t1",
            "select tail(t1) from t1",
            "select tail(True) from t1",
            "select tail(c1,1) , count(c1) from t1",
            "select tail(c1,1) , avg(c1) from t1",
            "select tail(c1,1) , min(c1) from t1",
            "select tail(c1,1) , spread(c1) from t1",
            "select tail(c1,1) , diff(c1) from t1",
            "select tail from stb1 partition by tbname",
            "select tail(123--123)==1 from stb1 partition by tbname",
            "select tail(123,123) from stb1 partition by tbname",
            "select tail(c1,ts) from stb1 partition by tbname",
            "select tail(c1,c1,ts) from stb1 partition by tbname",
            "select tail(c1) as 'd1' from stb1 partition by tbname",
            "select tail(c1 ,c2 ) from stb1 partition by tbname",
            "select tail(c1 ,NULL) from stb1 partition by tbname",
            "select tail(,) from stb1 partition by tbname;",
            "select tail(tail(c1) ab from stb1 partition by tbname)",
            "select tail(c1) as int from stb1 partition by tbname",
            "select tail('c1') from stb1 partition by tbname",
            "select tail(NULL) from stb1 partition by tbname",
            "select tail('') from stb1 partition by tbname",
            "select tail(c%) from stb1 partition by tbname",
            "select tail(t1) from stb1 partition by tbname",
            "select tail(True) from stb1 partition by tbname",
            "select tail(c1,1) , count(c1) from stb1 partition by tbname",
            "select tail(c1,1) , avg(c1) from stb1 partition by tbname",
            "select tail(c1,1) , min(c1) from stb1 partition by tbname",
            "select tail(c1,1) , spread(c1) from stb1 partition by tbname",
            "select tail(c1,1) , diff(c1) from stb1 partition by tbname",          
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)
    
    def support_types(self):
        other_no_value_types = [
            "select tail(ts,1) from t1" , 
            "select tail(c7,1) from t1",
            "select tail(c8,1) from t1",
            "select tail(c9,1) from t1",
            "select tail(ts,1) from ct1" , 
            "select tail(c7,1) from ct1",
            "select tail(c8,1) from ct1",
            "select tail(c9,1) from ct1",
            "select tail(ts,1) from ct3" , 
            "select tail(c7,1) from ct3",
            "select tail(c8,1) from ct3",
            "select tail(c9,1) from ct3",
            "select tail(ts,1) from ct4" , 
            "select tail(c7,1) from ct4",
            "select tail(c8,1) from ct4",
            "select tail(c9,1) from ct4",
            "select tail(ts,1) from stb1 partition by tbname" , 
            "select tail(c7,1) from stb1 partition by tbname",
            "select tail(c8,1) from stb1 partition by tbname",
            "select tail(c9,1) from stb1 partition by tbname" 
        ]
        
        for type_sql in other_no_value_types:
            tdSql.query(type_sql)
        
        type_sql_lists = [
            "select tail(c1,1) from t1",
            "select tail(c2,1) from t1",
            "select tail(c3,1) from t1",
            "select tail(c4,1) from t1",
            "select tail(c5,1) from t1",
            "select tail(c6,1) from t1",

            "select tail(c1,1) from ct1",
            "select tail(c2,1) from ct1",
            "select tail(c3,1) from ct1",
            "select tail(c4,1) from ct1",
            "select tail(c5,1) from ct1",
            "select tail(c6,1) from ct1",

            "select tail(c1,1) from ct3",
            "select tail(c2,1) from ct3",
            "select tail(c3,1) from ct3",
            "select tail(c4,1) from ct3",
            "select tail(c5,1) from ct3",
            "select tail(c6,1) from ct3",

            "select tail(c1,1) from stb1 partition by tbname",
            "select tail(c2,1) from stb1 partition by tbname",
            "select tail(c3,1) from stb1 partition by tbname",
            "select tail(c4,1) from stb1 partition by tbname",
            "select tail(c5,1) from stb1 partition by tbname",
            "select tail(c6,1) from stb1 partition by tbname",

            "select tail(c6,1) as alisb from stb1 partition by tbname", 
            "select tail(c6,1) alisb from stb1 partition by tbname", 
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)

    def check_tail_table(self , tbname , col_name , tail_rows , offset):
        tail_sql = f"select tail({col_name} , {tail_rows} , {offset}) from {tbname}"
        #equal_sql = f"select {col_name} from (select ts , {col_name} from {tbname} order by ts desc limit {tail_rows} offset {offset}) order by ts"
        equal_sql = f"select {col_name} from {tbname} order by ts desc limit {tail_rows} offset {offset}"
        tdSql.query(tail_sql)
        tail_result = tdSql.queryResult

        tdSql.query(equal_sql)
        print(equal_sql)

        equal_result = tdSql.queryResult

        if tail_result == equal_result:
            tdLog.info(" tail query check pass , tail sql is: %s" %tail_sql)
        else:
            tdLog.exit(" tail query check fail , tail sql is: %s " %tail_sql)

    def basic_tail_function(self):

        # basic query 
        tdSql.query("select c1 from ct3")
        tdSql.checkRows(0)
        tdSql.query("select c1 from t1")
        tdSql.checkRows(12)
        tdSql.query("select c1 from stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query("select tail(c1,1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select tail(c2,1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select tail(c3,1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select tail(c4,1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select tail(c5,1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select tail(c6,1) from ct3")
        
        # auto check for t1 table
        # used for regular table
        tdSql.query("select tail(c1,1) from t1")
        
        tdSql.query("desc t1")
        col_lists_rows = tdSql.queryResult
        col_lists = []
        for col_name in col_lists_rows:
            if col_name[0] =="ts":
                continue
            
            col_lists.append(col_name[0])
 
        for col in col_lists:
            for loop in range(100):       
                limit = randint(1,100)
                offset = randint(0,100)
                self.check_tail_table("t1" , col , limit , offset)

        # tail for invalid params
        
        tdSql.error("select tail(c1,-10,10) from ct1")
        tdSql.error("select tail(c1,10,10000) from ct1")
        tdSql.error("select tail(c1,10,-100) from ct1")
        tdSql.error("select tail(c1,100/2,10) from ct1")
        tdSql.error("select tail(c1,5,10*2) from ct1")
        tdSql.query("select tail(c1,100,100) from ct1")
        tdSql.checkRows(0)
        tdSql.query("select tail(c1,10,100) from ct1")
        tdSql.checkRows(0)
        tdSql.error("select tail(c1,10,101) from ct1")
        tdSql.query("select tail(c1,10,0) from ct1")
        tdSql.query("select tail(c1,100,10) from ct1")
        tdSql.checkRows(3)
        
        # tail with super tags

        tdSql.query("select tail(c1,10,10) from ct1")
        tdSql.checkRows(3)

        tdSql.query("select tail(c1,10,10),tbname from ct1")
        tdSql.query("select tail(c1,10,10),t1 from ct1")

        # tail with common col 
        tdSql.query("select tail(c1,10,10) ,ts  from ct1")
        tdSql.query("select tail(c1,10,10) ,c1  from ct1")

        # tail with scalar function 
        tdSql.query("select tail(c1,10,10) ,abs(c1)  from ct1")
        tdSql.error("select tail(c1,10,10) , tail(c2,10,10) from ct1")
        tdSql.query("select tail(c1,10,10) , abs(c2)+2 from ct1")
  
        # bug need fix for scalar value or compute again
        # tdSql.error(" select tail(c1,10,10) , 123 from ct1")
        # tdSql.error(" select abs(tail(c1,10,10)) from ct1")
        # tdSql.error(" select abs(tail(c1,10,10)) + 2 from ct1")

        # tail with aggregate function 
        tdSql.error("select tail(c1,10,10) ,sum(c1)  from ct1")
        tdSql.error("select tail(c1,10,10) ,max(c1)  from ct1")
        tdSql.error("select tail(c1,10,10) ,csum(c1)  from ct1")
        tdSql.error("select tail(c1,10,10) ,count(c1)  from ct1")

        # tail with filter where
        tdSql.query("select tail(c1,3,1) from ct4 where c1 is null")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.query("select tail(c1,3,2) from ct4 where c1 >2 order by 1")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)

        tdSql.query("select tail(c1,2,1) from ct4  where c2 between 0  and   99999 order by 1")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)

        # tail with union all 
        tdSql.query("select tail(c1,2,1) from ct4 union all select c1 from ct1")
        tdSql.checkRows(15)
        tdSql.query("select tail(c1,2,1) from ct4 union all select c1 from ct2 order by 1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.query("select tail(c2,2,1) from ct4 union all select abs(c2)/2 from ct4")
        tdSql.checkRows(14)

        # tail with join 
        # prepare join datas with same ts 

        tdSql.execute(" use db ")
        tdSql.execute(" create stable st1 (ts timestamp , num int) tags(ind int)")
        tdSql.execute(" create table tb1 using st1 tags(1)")
        tdSql.execute(" create table tb2 using st1 tags(2)")

        tdSql.execute(" create stable st2 (ts timestamp , num int) tags(ind int)")
        tdSql.execute(" create table ttb1 using st2 tags(1)")
        tdSql.execute(" create table ttb2 using st2 tags(2)")

        start_ts = 1622369635000 # 2021-05-30 18:13:55

        for i in range(10):
            ts_value = start_ts+i*1000
            tdSql.execute(f" insert into tb1 values({ts_value} , {i})")
            tdSql.execute(f" insert into tb2 values({ts_value} , {i})")

            tdSql.execute(f" insert into ttb1 values({ts_value} , {i})")
            tdSql.execute(f" insert into ttb2 values({ts_value} , {i})")

        tdSql.query("select tail(tb2.num,3,2)  from tb1, tb2 where tb1.ts=tb2.ts order by 1 desc")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,7)
        tdSql.checkData(1,0,6)
        tdSql.checkData(2,0,5)

        # nest query
        # tdSql.query("select tail(c1,2) from (select _rowts , c1 from ct1)")
        tdSql.query("select c1 from (select tail(c1,2) c1 from ct4) order by 1 nulls first")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0)

        tdSql.query("select sum(c1) from (select tail(c1,2) c1 from ct1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 18)

        tdSql.query("select abs(c1) from (select tail(c1,2) c1 from ct1)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        
        #partition by tbname
        tdSql.query(" select tail(c1,5) from stb1 partition by tbname ")
        tdSql.checkRows(10)

        tdSql.query(" select tail(c1,3) from stb1 partition by tbname ")
        tdSql.checkRows(6)
        
        # group by 
        tdSql.error("select tail(c1,2) from ct1 group by c1")
        tdSql.error("select tail(c1,2) from ct1 group by tbname")

        # super table
        tdSql.error("select tbname , tail(c1,2) from stb1 group by tbname")
        tdSql.query("select tail(c1,2) from stb1 partition by tbname")
        tdSql.checkRows(4)


        # bug need fix 
        # tdSql.query("select tbname , tail(c1,2) from stb1 partition by tbname")
        # tdSql.checkRows(4)

        # tdSql.query("select tbname , tail(c1,2) from stb1 partition by tbname order by tbname")
        # tdSql.checkRows(4)

        # tdSql.query(" select tbname , count(c1) from stb1 partition by tbname order by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(" select tbname , max(c1) ,c1 from stb1 partition by tbname order by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(" select tbname ,first(c1) from stb1 partition by tbname order by tbname ")
        # tdSql.checkRows(2)

        tdSql.query("select tail(c1,2) from stb1 partition by tbname")
        tdSql.checkRows(4)


        # # bug need fix 
        # tdSql.query(" select tbname , tail(c1,2) from stb1  where t1 = 0 partition by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(" select tbname , tail(c1,2) from stb1  where t1 = 0 partition by tbname order by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(" select tbname , tail(c1,2) from stb1  where c1 = 0 partition by tbname order by tbname ")
        # tdSql.checkRows(3)
        # tdSql.query(" select tbname , tail(c1,2) from stb1  where c1 = 0 partition by tbname ")
        # tdSql.checkRows(3)
        # tdSql.query(" select tbname , tail(c1,2) from stb1  where c1 = 0 partition by tbname ")
        # tdSql.checkRows(3)
        tdSql.query(" select tail(t1,2) from stb1  ")
        tdSql.checkRows(2)
        tdSql.query(" select tail(t1+c1,2) from stb1 ")
        tdSql.checkRows(2)
        tdSql.query(" select tail(t1+c1,2) from stb1 partition by tbname ")
        tdSql.checkRows(4)
        tdSql.query(" select tail(t1,2) from stb1 partition by tbname ")
        tdSql.checkRows(4)

        # nest query 
        tdSql.query(" select  tail(c1,2) from (select _rowts , t1 ,c1 , tbname from stb1 ) ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,None)
        tdSql.checkData(1,0,9)
        tdSql.query("select  tail(t1,2) from (select _rowts , t1 , tbname from stb1 )")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,4)
        tdSql.checkData(1,0,1)

    def check_boundary_values(self):

        tdSql.execute("drop database if exists bound_test")
        tdSql.execute("create database if not exists bound_test")
        tdSql.execute("use bound_test")
        tdSql.execute(
            "create table stb_bound (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp) tags (t1 int);"
        )
        tdSql.execute(f'create table sub1_bound using stb_bound tags ( 1 )')
        tdSql.execute(
                f"insert into sub1_bound values ( now()-1s, 2147483647, 9223372036854775807, 32767, 127, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        tdSql.execute(
                f"insert into sub1_bound values ( now(), 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into sub1_bound values ( now(), -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into sub1_bound values ( now(), 2147483643, 9223372036854775803, 32763, 123, 3.39E+38, 1.69e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into sub1_bound values ( now(), -2147483643, -9223372036854775803, -32763, -123, -3.39E+38, -1.69e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        
        tdSql.error(
                f"insert into sub1_bound values ( now()+1s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        
        tdSql.query("select tail(c2,2) from sub1_bound order by 1 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,9223372036854775803)

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")
        
        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")    

        self.test_errors()
        
        tdLog.printNoPrefix("==========step3:support types ============") 

        self.support_types()

        tdLog.printNoPrefix("==========step4: tail basic query ============") 

        self.basic_tail_function()

        tdLog.printNoPrefix("==========step5: tail boundary query ============") 

        self.check_boundary_values()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
