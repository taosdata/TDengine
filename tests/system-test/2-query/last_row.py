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
                     "wDebugFlag": 143, "sDebugFlag": 143, "tsdbDebugFlag": 143, "tqDebugFlag": 143, "fsDebugFlag": 143, "fnDebugFlag": 143 ,"udf":0}

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.tb_nums = 10
        self.row_nums = 20
        self.ts = 1434938400000
        self.time_step = 1000

    def insert_datas_and_check_abs(self ,tbnums , rownums , time_step ,cache_value ):
        tdSql.execute("drop database if exists test ")
        tdLog.info(" prepare datas for auto check abs function ")

        tdSql.execute(f" create database test cachemodel {cache_value} ")
        tdSql.execute(" use test ")
        tdSql.execute(" create stable test.stb (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint,\
             c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)")
        for tbnum in range(tbnums):
            tbname = "test.sub_tb_%d"%tbnum
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
                abs_sql = "select abs({}) from {} order by tbname ".format(colname, 'test.'+tbname)
                origin_sql = "select {} from {} order by tbname".format(colname, 'test.'+tbname)
                if coltype[1] in support_types:
                    self.check_result_auto(origin_sql , abs_sql)
                    
    def prepare_datas(self ,cache_value):
        tdSql.execute("drop database if exists db ")
        create_db_sql = f"create database if not exists db keep 3650 duration 1000 cachemodel {cache_value}"
        tdSql.execute(create_db_sql)

        tdSql.execute("use db")
        tdSql.execute(
            '''create table db.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        tdSql.execute(
            '''
            create table db.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table db.ct{i+1} using stb1 tags ( {i+1} )')

        for i in range(9):
            tdSql.execute(
                f"insert into db.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into db.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            "insert into db.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(
            "insert into db.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into db.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into db.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(
            "insert into db.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into db.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into db.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into db.t1 values
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

    def prepare_tag_datas(self,cache_value):

        tdSql.execute("drop database if exists testdb ")
        # prepare datas
        tdSql.execute(f"create database if not exists testdb keep 3650 duration 1000 cachemodel {cache_value}")

        tdSql.execute(" use testdb ")

        tdSql.execute(f" create stable testdb.stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp , uc1 int unsigned,\
             uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags( t1 int , t2 bigint , t3 smallint , t4 tinyint , t5 float , t6 double , t7 bool , t8 binary(36)\
                 , t9 nchar(36) , t10 int unsigned , t11 bigint unsigned ,t12 smallint unsigned , t13 tinyint unsigned ,t14 timestamp  ) ")

        tdSql.execute(
            '''
            create table testdb.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(
                f'create table testdb.ct{i+1} using stb1 tags ( {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" ,{111*i}, {1*i},{1*i},{1*i},now())')

        for i in range(9):
            tdSql.execute(
                f"insert into testdb.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a ,{111*i},{1111*i},{i},{i} )"
            )
            tdSql.execute(
                f"insert into testdb.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a ,{111*i},{1111*i},{i},{i})"
            )
        tdSql.execute(
            "insert into testdb.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a ,0,0,0,0)")
        tdSql.execute(
            "insert into testdb.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a , 999 , 9999 , 9 , 9)")
        tdSql.execute(
            "insert into testdb.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a , 999 , 99999 , 9 , 9)")
        tdSql.execute(
            "insert into testdb.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a ,999 , 99999 , 9 , 9)")

        tdSql.execute(
            "insert into testdb.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL) ")
        tdSql.execute(
            "insert into testdb.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL) ")
        tdSql.execute(
            "insert into testdb.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL ) ")

        tdSql.execute(
            f'''insert into testdb.t1 values
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

        # bug need fix 
        tdSql.error("select last_row(c1 ,NULL) from testdb.t1")

        error_sql_lists = [
            "select last_row from testdb.t1",
            "select last_row(-+--+c1) from testdb.t1",
            "select last_row(123--123)==1 from testdb.t1",
            "select last_row(c1) as 'd1' from testdb.t1",
            "select last_row(c1 ,NULL) from testdb.t1",
            "select last_row(,) from testdb.t1;",
            "select last_row(abs(c1) ab from testdb.t1)",
            "select last_row(c1) as int from testdb.t1",
            "select last_row from testdb.stb1",
            "select last_row(123--123)==1 from testdb.stb1",
            "select last_row(c1) as 'd1' from testdb.stb1",
            "select last_row(c1 ,NULL) from testdb.stb1",
            "select last_row(,) from testdb.stb1;",
            "select last_row(abs(c1) ab from testdb.stb1)",
            "select last_row(c1) as int from testdb.stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self):
        tdSql.execute("use testdb")
        tbnames = ["stb1", "t1", "ct1", "ct2"]
        
        for tbname in tbnames:
            tdSql.query("desc {}".format(tbname))
            coltypes = tdSql.queryResult
            for coltype in coltypes:
                colname = coltype[0]
                col_note = coltype[-1]
                if col_note != "TAG":
                    abs_sql = "select last_row({}) from {}".format(colname, "testdb."+tbname)
                    tdSql.query(abs_sql)
                

    def basic_abs_function(self):

        # basic query
        tdSql.query("select c1 from testdb.ct3")
        tdSql.checkRows(0)
        tdSql.query("select c1 from testdb.t1")
        tdSql.checkRows(12)
        tdSql.query("select c1 from testdb.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query("select last_row(c1) from testdb.ct3")
        tdSql.checkRows(0)
        tdSql.query("select last_row(c2) from testdb.ct3")
        tdSql.checkRows(0)
        tdSql.query("select last_row(c3) from testdb.ct3")
        tdSql.checkRows(0)
        tdSql.query("select last_row(c4) from testdb.ct3")
        tdSql.checkRows(0)
        tdSql.query("select last_row(c5) from testdb.ct3")
        tdSql.checkRows(0)
        tdSql.query("select last_row(c6) from testdb.ct3")

        # used for regular table

        # bug need fix 
        tdSql.query("select last_row(c1) from testdb.t1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select last_row(c1) from testdb.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.query("select last_row(c1) from testdb.stb1")
        tdSql.checkData(0, 0, None)

        # # bug need fix 
        tdSql.query("select last_row(c1), c2, c3 , c4, c5 from testdb.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

        # # bug need fix 
        tdSql.query("select last_row(c1), c2, c3 , c4, c5 from testdb.ct1")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, -99999)
        tdSql.checkData(0, 2, -999)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4,-9.99000)
        
        # bug need fix 
        tdSql.query("select last_row(c1), c2, c3 , c4, c5 from testdb.stb1 where tbname='ct1'")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, -99999)
        tdSql.checkData(0, 2, -999)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4,-9.99000)

        # bug fix 
        tdSql.query("select last_row(abs(c1)) from testdb.ct1")
        tdSql.checkData(0,0,9)

        # # bug fix 
        tdSql.query("select last_row(c1+1) from testdb.ct1")
        tdSql.query("select last_row(c1+1) from testdb.stb1") 
        tdSql.query("select last_row(c1+1) from testdb.t1")      

        # used for stable table
        tdSql.query("select last_row(c1 ,c2 ,c3) ,last_row(c4) from testdb.ct1")
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,-99999)
        tdSql.checkData(0,2,-999)
        tdSql.checkData(0,3,None)

        # bug need fix 
        tdSql.query("select last_row(c1 ,c2 ,c3) from testdb.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)


        tdSql.query('select last_row(c1) from testdb.t1 where ts <"2022-12-31 01:01:36.000"')
        tdSql.checkData(0,0,8)
        # bug need fix 
        tdSql.query("select abs(last_row(c1)-2)+max(c1),ceil(last_row(c4)-2) from testdb.stb1 where c4 is not null")
        tdSql.checkData(0,0,16.000000000)
        tdSql.checkData(0,1,-101.000000000)

        tdSql.query("select abs(last_row(c1)-2)+max(c1),ceil(last_row(c4)-2) from testdb.ct1 where c4<0")
        tdSql.checkData(0,0,16.000000000)
        tdSql.checkData(0,1,-101.000000000)

        tdSql.query("select last_row(ceil(c1+2)+floor(c1)-10) from testdb.stb1")
        tdSql.checkData(0,0,None)

        tdSql.query("select last_row(ceil(c1+2)+floor(c1)-10) from testdb.ct1")
        tdSql.checkData(0,0,10.000000000)

        # filter for last_row

        # bug need fix for all function

        tdSql.query("select last_row(ts ,c1 ) from testdb.ct4 where t1 = 1 ")
        tdSql.checkRows(0)

        tdSql.query("select count(c1) from testdb.ct4 where t1 = 1 ")
        tdSql.checkRows(0)

        tdSql.query("select last_row(c1) ,last(c1)  from testdb.stb1 where  c1 is null")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,None)

        tdSql.query("select last_row(c1) ,count(*)  from testdb.stb1 where  c1 is null")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        
        tdSql.query("select last_row(c1) ,count(c1)  from testdb.stb1 where  c1 is null")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,0)

        # bug need fix
        tdSql.query("select tbname ,last_row(c1) from testdb.stb1")
        tdSql.checkData(0,0,'ct4')
        tdSql.checkData(0,1,None)
        
        tdSql.query(" select tbname ,last_row(c1) from testdb.stb1 partition by tbname order by tbname ")
        tdSql.checkData(0,0,'ct1')
        tdSql.checkData(0,1,9)
        tdSql.checkData(1,0,'ct4')
        tdSql.checkData(1,1,None)

        tdSql.query(" select tbname ,last_row(c1) from testdb.stb1 group by tbname order by tbname ")
        tdSql.checkData(0,0,'ct1')
        tdSql.checkData(0,1,9)
        tdSql.checkData(1,0,'ct4')
        tdSql.checkData(1,1,None)

        tdSql.query(" select t1 ,count(c1) from testdb.stb1 partition by t1 ")
        tdSql.checkRows(2)

        # filter by tbname 
        tdSql.query("select last_row(c1) from testdb.stb1 where tbname = 'ct1' ")
        tdSql.checkData(0,0,9)

        # bug need fix 
        tdSql.query("select tbname ,last_row(c1) from testdb.stb1 where tbname = 'ct1' ")
        tdSql.checkData(0,1,9)
        tdSql.query("select tbname ,last_row(c1) from testdb.stb1 partition by tbname order by tbname")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, 'ct4')
        tdSql.checkData(1, 1, None)

        tdSql.query("select tbname ,last_row(c1) from testdb.stb1 group by tbname order by tbname")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, 'ct4')
        tdSql.checkData(1, 1, None)

        # last_row for only tag
        tdSql.query("select last_row(t1 ,t2 ,t3 , t4 ) from testdb.stb1")
        tdSql.checkData(0,0,3)
        tdSql.checkData(0,1,33333)
        tdSql.checkData(0,2,333)
        tdSql.checkData(0,3,3)

        tdSql.query("select last_row(abs(floor(t1)) ,t2 ,ceil(abs(t3)) , abs(ceil(t4)) ) from testdb.stb1")
        tdSql.checkData(0,0,3)
        tdSql.checkData(0,1,33333)
        tdSql.checkData(0,2,333)
        tdSql.checkData(0,3,3)

        # filter by tag 
        tdSql.query("select tbname ,last_row(c1) from testdb.stb1 where t1 =0 ")
        tdSql.checkData(0,1,9)
        tdSql.query("select tbname ,last_row(c1) ,t1 from testdb.stb1 partition by t1 order by t1")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 0, 'ct4')
        tdSql.checkData(1, 1, None)

        # filter by col 

        tdSql.query("select tbname ,last_row(c1),abs(c1)from testdb.stb1 where c1 =1;")
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.query("select last_row(c1) from testdb.stb1 where abs(ceil(c1))*c1==1")
        tdSql.checkData(0,0,1)
    
        # mix with common functions
        tdSql.query("select last_row(*) ,last(*) from testdb.stb1  ")
        tdSql.checkRows(1)

        tdSql.query("select last_row(*) ,last(*) from testdb.stb1  ")
        tdSql.checkRows(1)


        tdSql.query("select last_row(c1+abs(c1)) from testdb.stb1 partition by tbname order by tbname")
        tdSql.query("select last(c1), max(c1+abs(c1)),last_row(c1+abs(c1)) from testdb.stb1 partition by tbname order by tbname")

        # # bug need fix ,taosd crash 
        tdSql.error("select last_row(*) ,last(*) from testdb.stb1 partition by tbname order by last(*)")
        tdSql.error("select last_row(*) ,last(*) from testdb.stb1 partition by tbname order by last_row(*)")

        # mix with agg functions 
        tdSql.query("select last(*), last_row(*),last(c1), last_row(c1) from testdb.stb1 ")
        tdSql.query("select last(*), last_row(*),last(c1), last_row(c1) from testdb.ct1 ")
        tdSql.query("select last(*), last_row(*),last(c1+1)*max(c1), last_row(c1+2)/2 from testdb.t1 ")
        tdSql.query("select last_row(*) ,abs(c1/2)+100 from testdb.stb1 where tbname =\"ct1\" ")
        tdSql.query("select c1, last_row(c5) from testdb.ct1 ")
        tdSql.error("select c1, last_row(c5) ,last(c1) from testdb.stb1 ")

        # agg functions mix with agg functions

        tdSql.query("select last(c1) , max(c5), count(c5) from testdb.stb1")
        tdSql.query("select last_row(c1) , max(c5), count(c5) from testdb.ct1")

        # bug fix for compute
        tdSql.query("select  last_row(c1) -0 ,last(c1)-0 ,last(c1)+last_row(c1) from testdb.ct4 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,0.000000000)
        tdSql.checkData(0,2,None)

        tdSql.query(" select c1, abs(c1) -0 ,last_row(c1-0.1)-0.1 from testdb.ct1")
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,1,9.000000000)
        tdSql.checkData(0,2,8.800000000)

    def abs_func_filter(self):
        tdSql.execute("use db")
        tdSql.query(
            "select c1, abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,last_row(log(c1,2)-0.5) from db.ct4 where c1>5 ")
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 6.000000000)
        tdSql.checkData(0, 2, 6.000000000)
        tdSql.checkData(0, 3, 5.900000000)
        tdSql.checkData(0, 4, 2.084962501)

        tdSql.query(
            "select last_row(c1,c2,c1+5) from db.ct4 where c1=5 ")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 55555)
        tdSql.checkData(0, 2, 10.000000000)

        tdSql.query(
            "select last(c1,c2,c1+5) from db.ct4 where c1=5 ")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 55555)
        tdSql.checkData(0, 2, 10.000000000)

        tdSql.query(
            "select c1,c2 , abs(c1) -0 ,ceil(c1-0.1)-0 ,floor(c1+0.1)-0.1 ,ceil(log(c1,2)-0.5) from db.ct4 where c1>log(c1,2) limit 1 ")
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
        tdSql.execute("create database if not exists bound_test cachemodel 'LAST_ROW' ")

        time.sleep(3)
        tdSql.execute("use bound_test")
        tdSql.execute(
            "create table bound_test.stb_bound (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp) tags (t1 int);"
        )
        tdSql.execute(f'create table bound_test.sub1_bound using stb_bound tags ( 1 )')
        tdSql.execute(
            f"insert into bound_test.sub1_bound values ( now()-1s, 2147483647, 9223372036854775807, 32767, 127, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into bound_test.sub1_bound values ( now()-1s, -2147483647, -9223372036854775807, -32767, -127, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into bound_test.sub1_bound values ( now(), 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.execute(
            f"insert into bound_test.sub1_bound values ( now(), -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.error(
            f"insert into bound_test.sub1_bound values ( now()+1s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )

        # check basic elem for table per row
        tdSql.query(
            "select last(c1) ,last_row(c2), last_row(c3)+1 , last(c4)+1  from bound_test.sub1_bound ")
        tdSql.checkData(0, 0, -2147483646)
        tdSql.checkData(0, 1, -9223372036854775806)
        tdSql.checkData(0, 2, -32765.000000000)
        tdSql.checkData(0, 3, -125.000000000)
        # check  + - * / in functions
        tdSql.query(
            "select last_row(c1+1) ,last_row(c2) , last(c3*1) , last(c4/2)  from bound_test.sub1_bound ")

    def test_tag_compute_for_scalar_function(self):

        tdSql.execute("use testdb")

        # bug need fix

        tdSql.query(" select sum(c1) from testdb.stb1 where t1+10 >1; ")  
        tdSql.query("select c1 ,t1 from testdb.stb1 where t1 =0 ")
        tdSql.checkRows(13)
        tdSql.query("select last_row(c1,t1) from testdb.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        tdSql.query("select last_row(c1),t1 from testdb.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        tdSql.query("select last_row(c1,t1),last(t1) from testdb.stb1 ")
        tdSql.checkData(0,0,None)
        tdSql.checkData(0,1,3)
        tdSql.checkData(0,2,3)

        tdSql.query("select last_row(t1) from testdb.stb1 where t1 >0 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,3)
        tdSql.query("select last_row(t1) from testdb.stb1 where t1 =3 ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,3)

        tdSql.query("select last_row(t1) from testdb.stb1 where t1 =2")
        tdSql.checkRows(0)

        # nest query for last_row
        tdSql.query("select last_row(t1) from (select ts , c1 ,t1 from testdb.stb1)")
        tdSql.checkData(0,0,3)
        tdSql.query("select distinct(c1) ,t1 from testdb.stb1")
        tdSql.checkRows(20)
        tdSql.query("select last_row(c1) from (select _rowts , c1 ,t1 from testdb.stb1)")
        tdSql.checkData(0,0,None)

        tdSql.query("select last_row(c1) from (select ts , c1 ,t1 from testdb.stb1)")
        tdSql.checkData(0,0,None)

        tdSql.query("select ts , last_row(c1) ,c1  from (select ts , c1 ,t1 from testdb.stb1)")
        tdSql.checkData(0,1,None)

        tdSql.query("select ts , last_row(c1) ,c1  from (select ts , max(c1) c1  ,t1 from testdb.stb1 where ts >now -1h and ts <now interval(10s) fill(value ,10 ))")
        tdSql.checkData(0,1,10)
        tdSql.checkData(0,1,10)

        tdSql.error("select ts , last_row(c1) ,c1  from (select count(c1) c1 from testdb.stb1 where ts >now -1h and ts <now interval(10s) fill(value ,10 ))")

        tdSql.error("select  last_row(c1) ,c1  from (select  count(c1) c1 from testdb.stb1 where ts >now -1h and ts <now interval(10s) fill(value ,10 ))")

        # tag filter with last_row function
        tdSql.query("select last_row(t1) from testdb.stb1 where abs(t1)=1")
        tdSql.checkRows(0)
        tdSql.query("select last_row(t1) from testdb.stb1 where abs(t1)=0")
        tdSql.checkRows(1)
        tdSql.query(" select last_row(t1),last_row(c1) from db.ct1 where abs(c1+t1)=1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,1)
        tdSql.checkData(0,1,0)

        tdSql.query(
            "select last_row(c1+t1)*t1 from testdb.stb1 where abs(c1)/floor(abs(ceil(t1))) ==1")

    def group_test(self):
        tdSql.execute(" use testdb ")
        tdSql.query(" select last_row(c1) from testdb.stb1 group by t1 order by t1 ")
        tdSql.checkRows(2)
        
        # bug need fix 
        tdSql.query("select last_row(c1) from testdb.stb1 group by c1 order by c1,t1 ")
        tdSql.checkRows(10)
        tdSql.checkData(9,0,8)
        tdSql.query("select last_row(c1) from db.stb1 group by c1 order by t1 ")
        tdSql.checkRows(10)
        tdSql.checkData(0,0,4)

        tdSql.query("select last_row(c1) from testdb.stb1 group by c1 order by t1")
        tdSql.checkRows(11)

        tdSql.query("select last_row(c1) from testdb.stb1 group by c1 order by c1,t1;")
        tdSql.checkRows(11)
        tdSql.checkData(10,0,9)

        # bug need fix , result is error 
        tdSql.query("select last_row(c1) from testdb.ct4 group by c1 order by t1 ")
        tdSql.query("select last_row(t1) from testdb.ct4 group by c1 order by t1 ")

        tdSql.query("select last_row(t1) from testdb.stb1 group by t1 order by t1 ")
        tdSql.checkRows(2)
        tdSql.query("select last_row(c1) from testdb.stb1 group by c1 order by c1 ")
        tdSql.checkRows(11)
        tdSql.checkData(0,0,None)
        tdSql.checkData(10,0,9)

        tdSql.query("select ceil(abs(last_row(abs(c1)))) from testdb.stb1 group by abs(c1) order by abs(c1);")
        tdSql.checkRows(11)
        tdSql.checkData(0,0,None)
        tdSql.checkData(10,0,9)
        tdSql.query("select last_row(c1+c3) from testdb.stb1 group by abs(c1+c3) order by abs(c1+c3)")
        tdSql.checkRows(11)

        # bug need fix , taosd crash 
        tdSql.query("select last_row(c1+c3)+c2 from testdb.stb1 group by abs(c1+c3)+c2 order by abs(c1+c3)+c2")
        tdSql.checkRows(11)
        tdSql.query("select last_row(c1+c3)+last_row(c2) from testdb.stb1 group by abs(c1+c3)+abs(c2) order by abs(c1+c3)+abs(c2)")
        tdSql.checkRows(11)
        tdSql.checkData(0,0,None)
        tdSql.checkData(2,0,11223.000000000)
        
        tdSql.query("select last_row(t1) from testdb.stb1 where abs(c1+t1)=1 partition by tbname")
        tdSql.checkData(0,0,1)
        
        tdSql.query("select tbname , last_row(c1) from testdb.stb1 partition by tbname order by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1,  9)
        tdSql.checkData(0, 2, 'ct4')
        tdSql.checkData(0, 3, None)
        
        tdSql.query("select tbname , last_row(c1) from testdb.stb1 partition by t1 order by t1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'ct1')
        tdSql.checkData(0, 1,  9)
        tdSql.checkData(0, 2, 'ct4')
        tdSql.checkData(0, 3, None)

        # bug need fix 
        tdSql.query("select tbname , last_row(c1) from testdb.stb1 partition by c2 order by c1")
        tdSql.checkRows(11)
        tdSql.checkData(10,1,9)

        tdSql.query("select tbname , last_row(c1) from testdb.stb1 partition by c2 order by c2")
        tdSql.checkRows(11)
        tdSql.checkData(10,1,88888)

        tdSql.query("select tbname , last_row(t1) from testdb.stb1 partition by c2 order by t1")
        tdSql.checkRows(11)

        tdSql.query("select abs(c1) ,c2 ,t1, last_row(t1) from testdb.stb1 partition by c2 order by t1")
        tdSql.checkRows(11)
   
        tdSql.query("select t1 ,last_row(t1) ,c2 from testdb.stb1 partition by c2 order by t1")
        tdSql.checkRows(11)

        tdSql.query("select last_row(t1) ,last_row(t1) ,last_row(c2) from testdb.stb1 partition by c2 order by c2")
        tdSql.checkRows(11)

        tdSql.query("select abs(c1) , last_row(t1) ,c2 from testdb.stb1 partition by tbname order by tbname")
        tdSql.checkRows(2)

        tdSql.query("select last_row(c1) , ceil(t1) ,c2 from testdb.stb1 partition by t1 order by t1")
        tdSql.checkRows(2)

        tdSql.query("select last_row(c1) , abs(t1) ,c2 from testdb.stb1 partition by abs(c1) order by abs(c1)")
        tdSql.checkRows(11)

        tdSql.query("select abs(last_row(c1)) , abs(floor(t1)) ,floor(c2) from testdb.stb1 partition by abs(floor(c1)) order by abs(c1)")
        tdSql.checkRows(11)

        tdSql.query("select last_row(ceil(c1-2)) , abs(floor(t1+1)) ,floor(c2-c1) from testdb.stb1 partition by abs(floor(c1)) order by abs(c1)")
        tdSql.checkRows(11)

        # interval 
        tdSql.query("select last_row(c1) from testdb.stb1 interval(50s) sliding(30s)")
        tdSql.checkRows(27)

        tdSql.query("select last_row(c1) from testdb.ct1 interval(50s) sliding(30s)")
        tdSql.checkRows(5)
        last_row_result = tdSql.queryResult
        tdSql.query("select last(c1) from testdb.ct1 interval(50s) sliding(30s)")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        # bug need fix
        tdSql.query('select max(c1) from testdb.t1 where ts>="2021-01-01 01:01:06.000" and ts < "2021-07-21 01:01:01.000" interval(50d) sliding(30d) fill(NULL)')
        tdSql.checkRows(8)
        tdSql.checkData(7,0,None)

        tdSql.query('select last_row(c1) from testdb.t1 where ts>="2021-01-01 01:01:06.000" and ts < "2021-07-21 01:01:01.000" interval(50d) sliding(30d) fill(value ,2 )')
        tdSql.checkRows(8)
        tdSql.checkData(7,0,2)

        tdSql.query('select last_row(c1) from testdb.stb1 where ts>="2022-07-06 16:00:00.000 " and ts < "2022-07-06 17:00:00.000 " interval(50s) sliding(30s)')
        tdSql.query('select last_row(c1) from (select ts ,  c1  from testdb.t1 where ts>="2021-01-01 01:01:06.000" and ts < "2021-07-21 01:01:01.000" ) interval(10s) sliding(5s)')

        # join 
        tdSql.query("use test")
        tdSql.query("select last(sub_tb_1.c1), last(sub_tb_2.c2) from sub_tb_1, sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        tdSql.checkCols(2)
        last_row_result = tdSql.queryResult
        tdSql.query("select last_row(sub_tb_1.c1), last_row(sub_tb_2.c2) from sub_tb_1, sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        tdSql.query("select last(*), last(*) from sub_tb_1, sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")

        last_row_result = tdSql.queryResult
        tdSql.query("select last_row(*), last_row(*) from sub_tb_1, sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        tdSql.query("select last(*), last_row(*) from sub_tb_1, sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])

        tdSql.query("select last_row(*), last(*) from sub_tb_1, sub_tb_2 where sub_tb_1.ts=sub_tb_2.ts")
        for ind , row in enumerate(last_row_result):
            tdSql.checkData(ind , 0 , row[0])
        

    def support_super_table_test(self):
        tdSql.execute(" use testdb ")
        self.check_result_auto( " select c1 from testdb.stb1 order by ts " , "select abs(c1) from testdb.stb1 order by ts" )
        self.check_result_auto( " select c1 from testdb.stb1 order by tbname " , "select abs(c1) from testdb.stb1 order by tbname" )
        self.check_result_auto( " select c1 from testdb.stb1 where c1 > 0 order by tbname  " , "select abs(c1) from testdb.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto( " select c1 from testdb.stb1 where c1 > 0 order by tbname  " , "select abs(c1) from testdb.stb1 where c1 > 0 order by tbname" )

        self.check_result_auto( " select t1,c1 from testdb.stb1 order by ts " , "select t1, abs(c1) from testdb.stb1 order by ts" )
        self.check_result_auto( " select t2,c1 from testdb.stb1 order by tbname " , "select t2 ,abs(c1) from testdb.stb1 order by tbname" )
        self.check_result_auto( " select t3,c1 from testdb.stb1 where c1 > 0 order by tbname  " , "select t3 ,abs(c1) from testdb.stb1 where c1 > 0 order by tbname" )
        self.check_result_auto( " select t4,c1 from testdb.stb1 where c1 > 0 order by tbname  " , "select t4 , abs(c1) from testdb.stb1 where c1 > 0 order by tbname" )
        pass
    
    def basic_query(self):
        
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


        tdLog.printNoPrefix("==========step8: check abs result of  stable query ============")

        self.support_super_table_test()

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        # tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        # cache_last 0
        self.prepare_datas("'NONE' ")
        self.prepare_tag_datas("'NONE'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'NONE'")
        self.basic_query()

        # cache_last 1 
        self.prepare_datas("'LAST_ROW'")
        self.prepare_tag_datas("'LAST_ROW'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'LAST_ROW'")
        self.basic_query()

        # cache_last 2 
        self.prepare_datas("'LAST_VALUE'")
        self.prepare_tag_datas("'LAST_VALUE'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'LAST_VALUE'")
        self.basic_query()

        # cache_last 3 
        self.prepare_datas("'BOTH'")
        self.prepare_tag_datas("'BOTH'")
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step,"'BOTH'")
        self.basic_query()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
