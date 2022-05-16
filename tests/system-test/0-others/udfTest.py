import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
import subprocess

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath
    
    def prepare_udf_so(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]
        print(projPath)

        libudf1 = subprocess.Popen('find %s -name "libudf1.so"|grep lib|head -n1'%projPath , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        libudf2 = subprocess.Popen('find %s -name "libudf2.so"|grep lib|head -n1'%projPath , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        os.system("mkdir /tmp/udf/")
        os.system("sudo cp %s /tmp/udf/ "%libudf1.replace("\n" ,""))
        os.system("sudo cp  %s /tmp/udf/ "%libudf2.replace("\n" ,""))


    def prepare_data(self):
        
        tdSql.execute("use db")
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

        tdSql.execute("create table tb (ts timestamp , num1 int , num2 int, num3 double , num4 binary(30))")
        tdSql.execute(
            f'''insert into tb values
            ( '2020-04-21 01:01:01.000',   NULL,    1,       1,     "binary1" )
            ( '2020-10-21 01:01:01.000',   1,       1,      1.11,   "binary1" )
            ( '2020-12-31 01:01:01.000',   2,    22222,      22,    "binary1" )
            ( '2021-01-01 01:01:06.000',   3,    33333,      33,    "binary1" )
            ( '2021-05-07 01:01:10.000',   4,    44444,      44,    "binary1" )
            ( '2021-07-21 01:01:01.000',   NULL,   NULL,     NULL,  "binary1" )
            ( '2021-09-30 01:01:16.000',   5,    55555,      55,    "binary1" )
            ( '2022-02-01 01:01:20.000',   6,    66666,      66,    "binary1" )
            ( '2022-10-28 01:01:26.000',   0,    00000,      00,    "binary1" )
            ( '2022-12-01 01:01:30.000',   8,   -88888,     -88,    "binary1" )
            ( '2022-12-31 01:01:36.000',   9, -9999999,     -99,    "binary1" )
            ( '2023-02-21 01:01:01.000',  NULL,    NULL,    NULL,   "binary1" )
            '''
        )


    def create_udf_function(self):

        for i in range(10):
            # create  scalar functions
            tdSql.execute("create function udf1 as '/tmp/udf/libudf1.so' outputtype int bufSize 8;")

            # create aggregate functions

            tdSql.execute("create aggregate function udf2 as '/tmp/udf/libudf2.so' outputtype double bufSize 8;")
            
            functions = tdSql.getResult("show functions")
            function_nums = len(functions)
            if function_nums == 2:
                tdLog.info("create two udf functions success ")

            # drop functions

            tdSql.execute("drop function udf1")
            tdSql.execute("drop function udf2")

            functions = tdSql.getResult("show functions")
            for function in functions:
                if "udf1" in function[0] or  "udf2" in function[0]:
                    tdLog.info("drop udf functions failed ")
                    tdLog.exit("drop udf functions failed")

                tdLog.info("drop two udf functions success ")

        # create  scalar functions
        tdSql.execute("create function udf1 as '/tmp/udf/libudf1.so' outputtype int bufSize 8;")

        # create aggregate functions

        tdSql.execute("create aggregate function udf2 as '/tmp/udf/libudf2.so' outputtype double bufSize 8;")
        
        functions = tdSql.getResult("show functions")
        function_nums = len(functions)
        if function_nums == 2:
            tdLog.info("create two udf functions success ")
       
    def basic_udf_query(self):
        
        # scalar functions

        tdSql.execute("use db ")
        tdSql.query("select num1 , udf1(num1) ,num2 ,udf1(num2),num3 ,udf1(num3),num4 ,udf1(num4) from tb")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,1)
        tdSql.checkData(0,3,88)
        tdSql.checkData(0,4,1.000000000)
        tdSql.checkData(0,5,88)
        tdSql.checkData(0,6,"binary1")
        tdSql.checkData(0,7,88)

        tdSql.checkData(3,0,3)
        tdSql.checkData(3,1,88)
        tdSql.checkData(3,2,33333)
        tdSql.checkData(3,3,88)
        tdSql.checkData(3,4,33.000000000)
        tdSql.checkData(3,5,88)
        tdSql.checkData(3,6,"binary1")
        tdSql.checkData(3,7,88)

        tdSql.checkData(11,0,None)
        tdSql.checkData(11,1,None)
        tdSql.checkData(11,2,None)
        tdSql.checkData(11,3,None)
        tdSql.checkData(11,4,None)
        tdSql.checkData(11,5,None)
        tdSql.checkData(11,6,"binary1")
        tdSql.checkData(11,7,88)

        tdSql.query("select c1 , udf1(c1) ,c2 ,udf1(c2), c3 ,udf1(c3), c4 ,udf1(c4) from stb1 order by c1")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,None)
        tdSql.checkData(0,4,None)
        tdSql.checkData(0,5,None)
        tdSql.checkData(0,6,None)
        tdSql.checkData(0,7,None)

        tdSql.checkData(20,0,8)
        tdSql.checkData(20,1,88)
        tdSql.checkData(20,2,88888)
        tdSql.checkData(20,3,88)
        tdSql.checkData(20,4,888)
        tdSql.checkData(20,5,88)
        tdSql.checkData(20,6,88)
        tdSql.checkData(20,7,88)


        # aggregate functions
        tdSql.query("select udf2(num1) ,udf2(num2), udf2(num3) from tb")
        tdSql.checkData(0,0,15.362291496)
        tdSql.checkData(0,1,10000949.553189287)
        tdSql.checkData(0,2,168.633425216)

        # Arithmetic compute
        tdSql.query("select udf2(num1)+100 ,udf2(num2)-100, udf2(num3)*100 ,udf2(num3)/100 from tb")
        tdSql.checkData(0,0,115.362291496)
        tdSql.checkData(0,1,10000849.553189287)
        tdSql.checkData(0,2,16863.342521576)
        tdSql.checkData(0,3,1.686334252)

        tdSql.query("select udf2(c1) ,udf2(c6) from stb1 ")
        tdSql.checkData(0,0,25.514701644)
        tdSql.checkData(0,1,265.247614504)

        tdSql.query("select udf2(c1)+100 ,udf2(c6)-100 ,udf2(c1)*100 ,udf2(c6)/100 from stb1 ")
        tdSql.checkData(0,0,125.514701644)
        tdSql.checkData(0,1,165.247614504)
        tdSql.checkData(0,2,2551.470164435)
        tdSql.checkData(0,3,2.652476145)
        
        # # bug for crash when query sub table
        tdSql.query("select udf2(c1+100) ,udf2(c6-100) ,udf2(c1*100) ,udf2(c6/100) from ct1")
        tdSql.checkData(0,0,378.215547010)
        tdSql.checkData(0,1,353.808067460)
        tdSql.checkData(0,2,2114.237451187)
        tdSql.checkData(0,3,2.125468151)

        tdSql.query("select udf2(c1+100) ,udf2(c6-100) ,udf2(c1*100) ,udf2(c6/100) from stb1 ")
        tdSql.checkData(0,0,490.358032462)
        tdSql.checkData(0,1,400.460106627)
        tdSql.checkData(0,2,2551.470164435)
        tdSql.checkData(0,3,2.652476145)


        # regular table with aggregate functions

        tdSql.error("select udf1(num1) , count(num1) from tb;")
        tdSql.error("select udf1(num1) , avg(num1) from tb;")
        tdSql.error("select udf1(num1) , twa(num1) from tb;")
        tdSql.error("select udf1(num1) , irate(num1) from tb;")
        tdSql.error("select udf1(num1) , sum(num1) from tb;")
        tdSql.error("select udf1(num1) , stddev(num1) from tb;")
        tdSql.error("select udf1(num1) , mode(num1) from tb;")
        tdSql.error("select udf1(num1) , HYPERLOGLOG(num1) from tb;")
        # stable 
        tdSql.error("select udf1(c1) , count(c1) from stb1;")
        tdSql.error("select udf1(c1) , avg(c1) from stb1;")
        tdSql.error("select udf1(c1) , twa(c1) from stb1;")
        tdSql.error("select udf1(c1) , irate(c1) from stb1;")
        tdSql.error("select udf1(c1) , sum(c1) from stb1;")
        tdSql.error("select udf1(c1) , stddev(c1) from stb1;")
        tdSql.error("select udf1(c1) , mode(c1) from stb1;")
        tdSql.error("select udf1(c1) , HYPERLOGLOG(c1) from stb1;")

        # regular table with select functions

        tdSql.query("select udf1(num1) , max(num1) from tb;")
        tdSql.checkRows(1)
        tdSql.query("select floor(num1) , max(num1) from tb;")
        tdSql.checkRows(1)
        tdSql.query("select udf1(num1) , min(num1) from tb;")
        tdSql.checkRows(1)
        tdSql.query("select ceil(num1) , min(num1) from tb;")
        tdSql.checkRows(1)
        tdSql.error("select udf1(num1) , first(num1) from tb;")
        
        tdSql.error("select abs(num1) , first(num1) from tb;")
        
        tdSql.error("select udf1(num1) , last(num1) from tb;")
        
        tdSql.error("select round(num1) , last(num1) from tb;")
        
        tdSql.query("select udf1(num1) , top(num1,1) from tb;")
        tdSql.checkRows(1)
        tdSql.query("select udf1(num1) , bottom(num1,1) from tb;")
        tdSql.checkRows(1)
        tdSql.error("select udf1(num1) , last_row(num1) from tb;")
       
        tdSql.error("select round(num1) , last_row(num1) from tb;")
   
        
        # stable 
        tdSql.query("select udf1(c1) , max(c1) from stb1;")
        tdSql.checkRows(1)
        tdSql.query("select abs(c1) , max(c1) from stb1;")
        tdSql.checkRows(1)
        tdSql.query("select udf1(c1) , min(c1) from stb1;")
        tdSql.checkRows(1)
        tdSql.query("select floor(c1) , min(c1) from stb1;")
        tdSql.checkRows(1)
        tdSql.error("select udf1(c1) , first(c1) from stb1;")
        
        tdSql.error("select udf1(c1) , last(c1) from stb1;")
        
        tdSql.query("select udf1(c1) , top(c1 ,1) from stb1;")
        tdSql.checkRows(1)
        tdSql.query("select abs(c1) , top(c1 ,1) from stb1;")
        tdSql.checkRows(1)
        tdSql.query("select udf1(c1) , bottom(c1,1) from stb1;")
        tdSql.checkRows(1)
        tdSql.query("select ceil(c1) , bottom(c1,1) from stb1;")
        tdSql.checkRows(1)

        tdSql.error("select udf1(c1) , last_row(c1) from stb1;")
        tdSql.error("select ceil(c1) , last_row(c1) from stb1;")
        
        # regular table with compute functions

        tdSql.query("select udf1(num1) , abs(num1) from tb;")
        tdSql.checkRows(12)
        tdSql.query("select floor(num1) , abs(num1) from tb;")
        tdSql.checkRows(12)

        # # bug need fix 

        tdSql.query("select udf1(num1) , csum(num1) from tb;")
        tdSql.checkRows(9)
        tdSql.query("select ceil(num1) , csum(num1) from tb;")
        tdSql.checkRows(9)
        tdSql.query("select udf1(c1) , csum(c1) from stb1;")
        tdSql.checkRows(22)
        tdSql.query("select floor(c1) , csum(c1) from stb1;")
        tdSql.checkRows(22)

        # stable  with compute functions
        tdSql.query("select udf1(c1) , abs(c1) from stb1;")
        tdSql.checkRows(25)
        tdSql.query("select abs(c1) , ceil(c1) from stb1;")
        tdSql.checkRows(25)

        # nest query
        tdSql.query("select abs(udf1(c1)) , abs(ceil(c1)) from stb1 order by ts;")
        tdSql.checkRows(25)
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(1,0,88)
        tdSql.checkData(1,1,8)

        tdSql.query("select abs(udf1(c1)) , abs(ceil(c1)) from ct1 order by ts;")
        tdSql.checkRows(13)
        tdSql.checkData(0,0,88)
        tdSql.checkData(0,1,8)
        tdSql.checkData(1,0,88)
        tdSql.checkData(1,1,7)

        # bug fix for crash 
        # order by udf function result 
        for _ in range(50):
            tdSql.query("select udf2(c1) from stb1 group by 1-udf1(c1)")
            print(tdSql.queryResult)

        # udf functions with filter

        tdSql.query("select abs(udf1(c1)) , abs(ceil(c1)) from stb1 where c1 is null  order by ts;")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)

        tdSql.query("select c1 ,udf1(c1) , c6 ,udf1(c6) from stb1 where c1 > 8  order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,88)
        tdSql.checkData(0,2,-99.990000000)
        tdSql.checkData(0,3,88)

        # udf functions with join
        ts_start = 1652517451000
        tdSql.execute("create stable st (ts timestamp , c1 int , c2 int ,c3 double ,c4 double ) tags(ind int)")
        tdSql.execute("create table sub1 using st tags(1)")
        tdSql.execute("create table sub2 using st tags(2)")

        for i in range(10):
            ts = ts_start + i *1000
            tdSql.execute(" insert into sub1 values({} , {},{},{},{})".format(ts,i ,i*10,i*100.0,i*1000.0))
            tdSql.execute(" insert into sub2 values({} , {},{},{},{})".format(ts,i ,i*10,i*100.0,i*1000.0))
        
        tdSql.query("select sub1.c1, sub2.c2 from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,1,0)
        tdSql.checkData(1,0,1)
        tdSql.checkData(1,1,10)

        tdSql.query("select udf1(sub1.c1), udf1(sub2.c2) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
        tdSql.checkData(0,0,88)
        tdSql.checkData(0,1,88)
        tdSql.checkData(1,0,88)
        tdSql.checkData(1,1,88)

        tdSql.query("select sub1.c1 , udf1(sub1.c1), sub2.c2 ,udf1(sub2.c2) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,1,88)
        tdSql.checkData(0,2,0)
        tdSql.checkData(0,3,88)
        tdSql.checkData(1,0,1)
        tdSql.checkData(1,1,88)
        tdSql.checkData(1,2,10)
        tdSql.checkData(1,3,88)

        tdSql.query("select udf2(sub1.c1), udf2(sub2.c2) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
        tdSql.checkData(0,0,16.881943016)
        tdSql.checkData(0,1,168.819430161)
        tdSql.error("select sub1.c1 , udf2(sub1.c1), sub2.c2 ,udf2(sub2.c2) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")

        # udf functions with  group by 
        tdSql.query("select udf1(c1) from ct1 group by c1")
        tdSql.checkRows(10)
        tdSql.query("select udf1(c1) from stb1 group by c1")
        tdSql.checkRows(11)
        tdSql.query("select c1,c2, udf1(c1,c2) from ct1 group by c1,c2")
        tdSql.checkRows(10)
        tdSql.query("select c1,c2, udf1(c1,c2) from stb1 group by c1,c2")
        tdSql.checkRows(11)

        tdSql.query("select udf2(c1) from ct1 group by c1")
        tdSql.checkRows(10)
        tdSql.query("select udf2(c1) from stb1 group by c1")
        tdSql.checkRows(11)
        tdSql.query("select c1,c2, udf2(c1,c6) from ct1 group by c1,c2")
        tdSql.checkRows(10)
        tdSql.query("select c1,c2, udf2(c1,c6) from stb1 group by c1,c2")
        tdSql.checkRows(11)
        tdSql.query("select udf2(c1) from stb1 group by udf1(c1)")
        tdSql.checkRows(2)
        tdSql.query("select udf2(c1) from stb1 group by floor(c1)")
        tdSql.checkRows(11)

        # udf mix with order by  
        tdSql.query("select udf2(c1) from stb1 group by floor(c1) order by udf2(c1)")
        tdSql.checkRows(11)


    def multi_cols_udf(self):
        tdSql.query("select num1,num2,num3,udf1(num1,num2,num3) from tb")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,1)
        tdSql.checkData(0,2,1.000000000)
        tdSql.checkData(0,3,None)
        tdSql.checkData(1,0,1)
        tdSql.checkData(1,1,1)
        tdSql.checkData(1,2,1.110000000)
        tdSql.checkData(1,3,88)

        tdSql.query("select c1,c6,udf1(c1,c6) from stb1 order by ts")
        tdSql.checkData(1,0,8)
        tdSql.checkData(1,1,88.880000000)
        tdSql.checkData(1,2,88)

        tdSql.query("select abs(udf1(c1,c6,c1,c6)) , abs(ceil(c1)) from stb1 where c1 is not null  order by ts;")
        tdSql.checkRows(22)

        tdSql.query("select udf2(sub1.c1 ,sub1.c2), udf2(sub2.c2 ,sub2.c1) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
        tdSql.checkData(0,0,169.661427555)
        tdSql.checkData(0,1,169.661427555)


    def unexpected_create(self):
        
        tdSql.query("select udf2(sub1.c1 ,sub1.c2), udf2(sub2.c2 ,sub2.c1) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")

    def loop_kill_udfd(self):

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        
        cfgPath = buildPath + "/../sim/dnode1/cfg"
        udfdPath = buildPath +'/build/bin/udfd'

        for i in range(5):

            tdLog.info(" loop restart udfd  %d_th" % i)

            tdSql.query("select udf2(sub1.c1 ,sub1.c2), udf2(sub2.c2 ,sub2.c1) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
            tdSql.checkData(0,0,169.661427555)
            tdSql.checkData(0,1,169.661427555)
            # stop udfd cmds 
            get_processID = "ps -ef | grep -w udfd | grep 'root' | grep -v grep| grep -v defunct | awk '{print $2}'"
            processID = subprocess.check_output(get_processID, shell=True).decode("utf-8")
            stop_udfd = " kill -9 %s" % processID
            os.system(stop_udfd)

            time.sleep(2)
            
            tdSql.query("select udf2(sub1.c1 ,sub1.c2), udf2(sub2.c2 ,sub2.c1) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
            tdSql.checkData(0,0,169.661427555)
            tdSql.checkData(0,1,169.661427555)

            # # start udfd  cmds 
            # start_udfd = "nohup " + udfdPath +'-c' +cfgPath +" > /dev/null 2>&1 &"
            # tdLog.info("start udfd : %s " % start_udfd)

    
    def restart_taosd_query_udf(self):

        for i in range(5):
            time.sleep(5)
            tdLog.info("  this is %d_th restart taosd " %i)
            tdSql.execute("use db ")
            tdSql.query("select count(*) from stb1")
            tdSql.checkRows(1)
            tdSql.query("select udf2(sub1.c1 ,sub1.c2), udf2(sub2.c2 ,sub2.c1) from sub1, sub2 where sub1.ts=sub2.ts and sub1.c1 is not null")
            tdSql.checkData(0,0,169.661427555)
            tdSql.checkData(0,1,169.661427555)
            tdDnodes.stop(1)
            time.sleep(2)
            tdDnodes.start(1)
            time.sleep(5)
            
            
            
    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        
        self.prepare_udf_so()
        self.prepare_data()
        self.create_udf_function()
        self.basic_udf_query()
        self.loop_kill_udfd()
        # self.restart_taosd_query_udf()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
