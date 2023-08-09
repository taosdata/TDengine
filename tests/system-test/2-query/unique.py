from math import floor
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

    def test_errors(self, dbname="db"):
        error_sql_lists = [
            f"select unique from  {dbname}.t1",
            f"select unique(123--123)==1 from  {dbname}.t1",
            f"select unique(123,123) from  {dbname}.t1",
            f"select unique(c1,ts) from  {dbname}.t1",
            f"select unique(c1,c1,ts) from  {dbname}.t1",
            f"select unique(c1) as 'd1' from  {dbname}.t1",
            f"select unique(c1 ,c2 ) from  {dbname}.t1",
            f"select unique(c1 ,NULL) from  {dbname}.t1",
            f"select unique(,) from  {dbname}.t1;",
            f"select unique(floor(c1) ab from  {dbname}.t1)",
            f"select unique(c1) as int from  {dbname}.t1",
            f"select unique('c1') from  {dbname}.t1",
            f"select unique(NULL) from  {dbname}.t1",
            f"select unique('') from  {dbname}.t1",
            f"select unique(c%) from  {dbname}.t1",
            f"select unique(t1) from  {dbname}.t1",
            f"select unique(True) from  {dbname}.t1",
            f"select unique(c1) , count(c1) from  {dbname}.t1",
            f"select unique(c1) , avg(c1) from  {dbname}.t1",
            f"select unique(c1) , min(c1) from  {dbname}.t1",
            f"select unique(c1) , spread(c1) from  {dbname}.t1",
            f"select unique(c1) , diff(c1) from  {dbname}.t1",
            #f"select unique(c1) , abs(c1) from  {dbname}.t1",  # support
            #f"select unique(c1) , c1 from  {dbname}.t1",
            f"select unique from  {dbname}.stb1 partition by tbname",
            f"select unique(123--123)==1 from  {dbname}.stb1 partition by tbname",
            f"select unique(123) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1,ts) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1,c1,ts) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1) as 'd1' from  {dbname}.stb1 partition by tbname",
            f"select unique(c1 ,c2 ) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1 ,NULL) from  {dbname}.stb1 partition by tbname",
            f"select unique(,) from  {dbname}.stb1 partition by tbname;",
            #f"select unique(floor(c1) ab from  {dbname}.stb1 partition by tbname)",  # support
            #f"select unique(c1) as int from  {dbname}.stb1 partition by tbname",
            f"select unique('c1') from  {dbname}.stb1 partition by tbname",
            f"select unique(NULL) from  {dbname}.stb1 partition by tbname",
            f"select unique('') from  {dbname}.stb1 partition by tbname",
            f"select unique(c%) from  {dbname}.stb1 partition by tbname",
            #f"select unique(t1) from  {dbname}.stb1 partition by tbname",  # support
            f"select unique(True) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1) , count(c1) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1) , avg(c1) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1) , min(c1) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1) , spread(c1) from  {dbname}.stb1 partition by tbname",
            f"select unique(c1) , diff(c1) from  {dbname}.stb1 partition by tbname",
            #f"select unique(c1) , abs(c1) from  {dbname}.stb1 partition by tbname", # support
            #f"select unique(c1) , c1 from  {dbname}.stb1 partition by tbname" # support

        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)
            pass

    def support_types(self, dbname="db"):
        other_no_value_types = [
            f"select unique(ts) from  {dbname}.t1" ,
            f"select unique(c7) from  {dbname}.t1",
            f"select unique(c8) from  {dbname}.t1",
            f"select unique(c9) from  {dbname}.t1",
            f"select unique(ts) from  {dbname}.ct1" ,
            f"select unique(c7) from  {dbname}.ct1",
            f"select unique(c8) from  {dbname}.ct1",
            f"select unique(c9) from  {dbname}.ct1",
            f"select unique(ts) from  {dbname}.ct3" ,
            f"select unique(c7) from  {dbname}.ct3",
            f"select unique(c8) from  {dbname}.ct3",
            f"select unique(c9) from  {dbname}.ct3",
            f"select unique(ts) from  {dbname}.ct4" ,
            f"select unique(c7) from  {dbname}.ct4",
            f"select unique(c8) from  {dbname}.ct4",
            f"select unique(c9) from  {dbname}.ct4",
            f"select unique(ts) from  {dbname}.stb1 partition by tbname" ,
            f"select unique(c7) from  {dbname}.stb1 partition by tbname",
            f"select unique(c8) from  {dbname}.stb1 partition by tbname",
            f"select unique(c9) from  {dbname}.stb1 partition by tbname"
        ]

        for type_sql in other_no_value_types:
            tdSql.query(type_sql)
            tdLog.info("support type ok ,  sql is : %s"%type_sql)

        type_sql_lists = [
            f"select unique(c1) from  {dbname}.t1",
            f"select unique(c2) from  {dbname}.t1",
            f"select unique(c3) from  {dbname}.t1",
            f"select unique(c4) from  {dbname}.t1",
            f"select unique(c5) from  {dbname}.t1",
            f"select unique(c6) from  {dbname}.t1",

            f"select unique(c1) from  {dbname}.ct1",
            f"select unique(c2) from  {dbname}.ct1",
            f"select unique(c3) from  {dbname}.ct1",
            f"select unique(c4) from  {dbname}.ct1",
            f"select unique(c5) from  {dbname}.ct1",
            f"select unique(c6) from  {dbname}.ct1",

            f"select unique(c1) from  {dbname}.ct3",
            f"select unique(c2) from  {dbname}.ct3",
            f"select unique(c3) from  {dbname}.ct3",
            f"select unique(c4) from  {dbname}.ct3",
            f"select unique(c5) from  {dbname}.ct3",
            f"select unique(c6) from  {dbname}.ct3",

            f"select unique(c1) from  {dbname}.stb1 partition by tbname",
            f"select unique(c2) from  {dbname}.stb1 partition by tbname",
            f"select unique(c3) from  {dbname}.stb1 partition by tbname",
            f"select unique(c4) from  {dbname}.stb1 partition by tbname",
            f"select unique(c5) from  {dbname}.stb1 partition by tbname",
            f"select unique(c6) from  {dbname}.stb1 partition by tbname",

            f"select unique(c6) as alisb from  {dbname}.stb1 partition by tbname",
            f"select unique(c6) alisb from  {dbname}.stb1 partition by tbname",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)

    def check_unique_table(self , unique_sql):
        # unique_sql =  f"select unique(c1) from  {dbname}.ct1"
        origin_sql = unique_sql.replace("unique(","").replace(")","")
        tdSql.query(unique_sql)
        unique_result = tdSql.queryResult

        unique_datas = []
        for elem in unique_result:
            unique_datas.append(elem[0])
        unique_datas.sort(key=lambda x: (x is None, x))

        tdSql.query(origin_sql)
        origin_result = tdSql.queryResult
        origin_datas = []
        for elem in origin_result:
            origin_datas.append(elem[0])

        pre_unique = []
        for elem in origin_datas:
            if elem in pre_unique:
                continue
            else:
                pre_unique.append(elem)
        pre_unique.sort(key=lambda x: (x is None, x))

        if pre_unique == unique_datas:
            tdLog.info(" unique query check pass , unique sql is: %s" %unique_sql)
        else:
            tdLog.exit(" unique query check fail , unique sql is: %s " %unique_sql)

    def basic_unique_function(self, dbname="db"):

        # basic query
        tdSql.query(f"select c1 from  {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from  {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from  {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select unique(c1) from  {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select unique(c2) from  {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select unique(c3) from  {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select unique(c4) from  {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select unique(c5) from  {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select unique(c6) from  {dbname}.ct3")

        # will support _rowts mix with
        # tdSql.query(f"select unique(c6),_rowts from  {dbname}.ct3")

        # auto check for t1 table
        # used for regular table
        tdSql.query(f"select unique(c1) from  {dbname}.t1")

        tdSql.query(f"desc {dbname}.t1")
        col_lists_rows = tdSql.queryResult
        col_lists = []
        for col_name in col_lists_rows:
            col_lists.append(col_name[0])

        for col in col_lists:
            self.check_unique_table(f"select unique({col}) from  {dbname}.t1")

        # unique with super tags

        tdSql.query(f"select unique(c1) from  {dbname}.ct1")
        tdSql.checkRows(10)

        tdSql.query(f"select unique(c1) from  {dbname}.ct4")
        tdSql.checkRows(10)

        #tdSql.error(f"select unique(c1),tbname from  {dbname}.ct1") #support
        #tdSql.error(f"select unique(c1),t1 from  {dbname}.ct1")    #support

        # unique with common col
        #tdSql.error(f"select unique(c1) ,ts  from  {dbname}.ct1")
        #tdSql.error(f"select unique(c1) ,c1  from  {dbname}.ct1")

        # unique with scalar function
        #tdSql.error(f"select unique(c1) ,abs(c1)  from  {dbname}.ct1")
        tdSql.error(f"select unique(c1) , unique(c2) from  {dbname}.ct1")
        #tdSql.error(f"select unique(c1) , abs(c2)+2 from  {dbname}.ct1")


        # unique with aggregate function
        tdSql.error(f"select unique(c1) ,sum(c1)  from  {dbname}.ct1")
        tdSql.error(f"select unique(c1) ,max(c1)  from  {dbname}.ct1")
        tdSql.error(f"select unique(c1) ,csum(c1)  from  {dbname}.ct1")
        tdSql.error(f"select unique(c1) ,count(c1)  from  {dbname}.ct1")

        # unique with filter where
        tdSql.query(f"select unique(c1) from  {dbname}.ct4 where c1 is null")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select unique(c1) from  {dbname}.ct4 where c1 >2 order by 1")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(5, 0, 8)

        tdSql.query(f"select unique(c1) from  {dbname}.ct4  where c2 between 0  and   99999 order by 1 desc")
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(2, 0, 6)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 4)
        tdSql.checkData(5, 0, 3)
        tdSql.checkData(6, 0, 2)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 0)

        # unique with union all
        tdSql.query(f"select unique(c1) from  {dbname}.ct4 union all select c1 from  {dbname}.ct1")
        tdSql.checkRows(23)
        tdSql.query(f"select unique(c1) from  {dbname}.ct4 union all select distinct(c1) from  {dbname}.ct4")
        tdSql.checkRows(20)
        tdSql.query(f"select unique(c2) from  {dbname}.ct4 union all select abs(c2)/2 from  {dbname}.ct4")
        tdSql.checkRows(22)

        # unique with join
        # prepare join datas with same ts

        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st1 (ts timestamp , num int) tags(ind int)")
        tdSql.execute(" create table db.tb1 using db.st1 tags(1)")
        tdSql.execute(" create table db.tb2 using db.st1 tags(2)")

        tdSql.execute(" create stable db.st2 (ts timestamp , num int) tags(ind int)")
        tdSql.execute(" create table db.ttb1 using db.st2 tags(1)")
        tdSql.execute(" create table db.ttb2 using db.st2 tags(2)")

        start_ts = 1622369635000 # 2021-05-30 18:13:55

        for i in range(10):
            ts_value = start_ts+i*1000
            tdSql.execute(f" insert into {dbname}.tb1 values({ts_value} , {i})")
            tdSql.execute(f" insert into {dbname}.tb2 values({ts_value} , {i})")

            tdSql.execute(f" insert into {dbname}.ttb1 values({ts_value} , {i})")
            tdSql.execute(f" insert into {dbname}.ttb2 values({ts_value} , {i})")

        tdSql.query(f"select unique(tb2.num)  from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts order by 1")
        tdSql.checkRows(10)
        tdSql.checkData(0,0,0)
        tdSql.checkData(1,0,1)
        tdSql.checkData(2,0,2)
        tdSql.checkData(9,0,9)

        tdSql.query(f"select unique(tb2.num)  from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts union all select unique(tb1.num)  from  db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts order by 1")
        tdSql.checkRows(20)
        tdSql.checkData(0,0,0)
        tdSql.checkData(2,0,1)
        tdSql.checkData(4,0,2)
        tdSql.checkData(18,0,9)

        # nest query
        # tdSql.query(f"select unique(c1) from (select c1 from  {dbname}.ct1)")
        tdSql.query(f"select c1 from (select unique(c1) c1 from  {dbname}.ct4) order by 1 desc nulls first")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(9, 0, 0)

        tdSql.query(f"select sum(c1) from (select unique(c1) c1 from  {dbname}.ct1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 45)

        tdSql.query(f"select sum(c1) from (select distinct(c1) c1 from  {dbname}.ct1) union all select sum(c1) from (select unique(c1) c1 from  {dbname}.ct1)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 45)
        tdSql.checkData(1, 0, 45)

        tdSql.query(f"select 1-abs(c1) from (select unique(c1) c1 from  {dbname}.ct4) order by 1 nulls first")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, -7.000000000)


        # bug for stable
        #partition by tbname
        # tdSql.query(f"select unique(c1) from  {dbname}.stb1 partition by tbname ")
        # tdSql.checkRows(21)

        # tdSql.query(f"select unique(c1) from  {dbname}.stb1 partition by tbname ")
        # tdSql.checkRows(21)

        # group by
        tdSql.error(f"select unique(c1) from  {dbname}.ct1 group by c1")
        tdSql.error(f"select unique(c1) from  {dbname}.ct1 group by tbname")

        # super table

        # super table
        tdSql.error(f"select tbname , tail(c1,2) from  {dbname}.stb1 group by tbname")
        tdSql.query(f"select tail(c1,2) from  {dbname}.stb1 partition by tbname")
        tdSql.checkRows(4)


        # bug need fix
        # tdSql.query(f"select tbname , tail(c1,2) from  {dbname}.stb1 partition by tbname")
        # tdSql.checkRows(4)

        # tdSql.query(f"select tbname , tail(c1,2) from  {dbname}.stb1 partition by tbname order by tbname")
        # tdSql.checkRows(4)

        # tdSql.query(f"select tbname , count(c1) from  {dbname}.stb1 partition by tbname order by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(f"select tbname , max(c1) ,c1 from  {dbname}.stb1 partition by tbname order by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(f"select tbname ,first(c1) from  {dbname}.stb1 partition by tbname order by tbname ")
        # tdSql.checkRows(2)

        tdSql.query(f"select tail(c1,2) from  {dbname}.stb1 partition by tbname")
        tdSql.checkRows(4)


        # # bug need fix
        # tdSql.query(f"select tbname , unique(c1) from  {dbname}.stb1  where t1 = 0 partition by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(f"select tbname , unique(c1) from  {dbname}.stb1  where t1 = 0 partition by tbname order by tbname ")
        # tdSql.checkRows(2)
        # tdSql.query(f"select tbname , unique(c1) from  {dbname}.stb1  where c1 = 0 partition by tbname order by tbname ")
        # tdSql.checkRows(3)
        # tdSql.query(f"select tbname , unique(c1) from  {dbname}.stb1  where c1 = 0 partition by tbname ")
        # tdSql.checkRows(3)

        tdSql.query(f"select unique(t1) from  {dbname}.stb1  ")
        tdSql.checkRows(2)
        tdSql.query(f"select unique(t1+c1) from  {dbname}.stb1 ")
        tdSql.checkRows(13)
        tdSql.query(f"select unique(t1+c1) from  {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(20)
        tdSql.query(f"select unique(t1) from  {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(2)

        # nest query
        tdSql.query(f"select  unique(c1) v from (select _rowts , t1 ,c1 , tbname from  {dbname}.stb1 ) order by v")
        tdSql.checkRows(11)
        tdSql.checkData(1,0,0)
        tdSql.checkData(10,0,9)
        tdSql.query(f"select  unique(t1) v from (select _rowts , t1 , tbname from  {dbname}.stb1 ) order by v desc")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,4)
        tdSql.checkData(1,0,1)

        tdSql.query(f"select  unique(c1) v from (select _rowts, c1 from  {dbname}.ct1 partition by c2)")
        tdSql.checkRows(10)

        tdSql.query(f"select  unique(c1) v from (select _rowts, c1 from  {dbname}.ct1 order by c2)")
        tdSql.checkRows(10)

        tdSql.query(f"select  unique(c1) v from (select _rowts, c1 from  {dbname}.ct1 union all select _rowts, c1 from {dbname}.ct1)")
        tdSql.checkRows(10)

        # TD-19911
        tdSql.error("select  unique(mode(12)) from (select _rowts , t1 , tbname from  db.stb1 );")
        tdSql.error("select  unique(mode(t1,1)) from (select _rowts , t1 , tbname from  db.stb1 );")

    def check_boundary_values(self, dbname="bound_test"):

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
                f"insert into {dbname}.sub1_bound values ( now()-5s, 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+5s, -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483643, 9223372036854775803, 32763, 123, 3.39E+38, 1.69e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+20s, -2147483643, -9223372036854775803, -32763, -123, -3.39E+38, -1.69e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+30s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.query(f"select unique(c2) from  {dbname}.sub1_bound order by 1 desc")
        tdSql.checkRows(5)
        tdSql.checkData(0,0,9223372036854775807)

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.test_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: floor basic query ============")

        self.basic_unique_function()

        tdLog.printNoPrefix("==========step5: floor boundary query ============")

        self.check_boundary_values()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
