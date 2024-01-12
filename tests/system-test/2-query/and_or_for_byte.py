import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
import random


class TDTestCase:
    # updatecfgDict = {'debugFlag': 143, "cDebugFlag": 143, "uDebugFlag": 143, "rpcDebugFlag": 143, "tmrDebugFlag": 143,
    #                  "jniDebugFlag": 143, "simDebugFlag": 143, "dDebugFlag": 143, "dDebugFlag": 143, "vDebugFlag": 143, "mDebugFlag": 143, "qDebugFlag": 143,
    #                  "wDebugFlag": 143, "sDebugFlag": 143, "tsdbDebugFlag": 143, "tqDebugFlag": 143, "fsDebugFlag": 143, "udfDebugFlag": 143}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.tb_nums = 10
        self.row_nums = 20
        self.ts = 1434938400000
        self.time_step = 1000

    def insert_datas_and_check_abs(self ,tbnums , rownums , time_step ):
        dbname = "test"
        stb = f"{dbname}.stb"
        ctb_pre = f"{dbname}.sub_tb_"
        tdLog.info(" prepare datas for auto check abs function ")

        tdSql.execute(f" create database {dbname} ")
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(f" create stable {stb} (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint,\
             c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)")
        for tbnum in range(tbnums):
            tbname = f"{ctb_pre}{tbnum}"
            tdSql.execute(f" create table {tbname} using {stb} tags({tbnum}) ")

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
        tbnames = [stb, f"{ctb_pre}1"]
        support_types = ["BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "INT"]
        for tbname in tbnames:
            tdSql.query("desc {}".format(tbname))
            coltypes = tdSql.queryResult
            colnames = []
            for coltype in coltypes:
                colname = coltype[0]
                if coltype[1] in support_types:
                    colnames.append(colname)
            cols = random.sample(colnames,3)
            self.check_function("&",False,tbname,cols[0],cols[1],cols[2])
            self.check_function("|",False,tbname,cols[0],cols[1],cols[2])


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

    def prepare_tag_datas(self, dbname="testdb"):
        # prepare datas
        tdSql.execute(
            f"create database if not exists {dbname} keep 3650 duration 1000")
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
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

    def check_function(self, opera ,agg, tbname ,  *args):

        if opera =="&":
            pass
        elif opera =="|":
            pass
        else:
            pass
        work_sql = " select "
        for ind , arg in enumerate(args):
            if ind ==len(args)-1:
                work_sql += f"cast({arg} as bigint) "
            else:
                work_sql += f"cast({arg} as bigint){opera}"

        if not agg:
            work_sql+= f" from {tbname} order by tbname ,ts"
        else:
            work_sql+= f" from {tbname} "
        tdSql.query(work_sql)
        work_result = tdSql.queryResult

        origin_sql = " select "
        for ind , arg in enumerate(args):
            if ind ==len(args)-1:
                origin_sql += f"cast({arg} as bigint) "
            else:
                origin_sql += f"cast({arg} as bigint),"
        if not agg:
            origin_sql+= f" from {tbname} order by tbname ,ts"
        else:
            origin_sql+= f" from {tbname} "
        tdSql.query(origin_sql)
        origin_result = tdSql.queryResult

        # compute and or with byte in binary data
        compute_result = []

        for row in origin_result:
            if None in row:
                compute_result.append(None)
            else:
                if opera == "&":
                    result = row[0]
                    for elem in row:
                        result = result&elem
                elif opera == "|":
                    result = row[0]
                    for elem in row:
                        result = result|elem
                compute_result.append(result)

        tdSql.query(work_sql)
        for ind , result in enumerate(compute_result):
            tdSql.checkData(ind,0,result)

    def test_errors(self, dbname="testdb"):
        tdSql.execute(f"use {dbname}")
        error_sql_lists = [
            f"select c1&&c2 from {dbname}.t1",
            f"select c1&|c2 from {dbname}.t1",
            f"select c1&(c1=c2) from {dbname}.t1",
            f"select c1&* from {dbname}.t1",
            f"select 123&, from {dbname}.t1",
            f"select 123&\" from {dbname}.t1",
            f"select c1&- from {dbname}.t1;",
            f"select c1&&= from {dbname}.t1)",
            f"select c1&! from {dbname}.t1",
            f"select c1&@ from {dbname}.stb1",
            f"select c1&# from {dbname}.stb1",
            f"select c1&$ from {dbname}.stb1",
            f"select c1&% from {dbname}.stb1",
            f"select c1&() from {dbname}.stb1",
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def basic_query(self, dbname="testdb"):
        # basic query
        tdSql.query(f"select c1&c2|c3 from {dbname}.ct1")
        tdSql.checkRows(13)
        tdSql.query(f"select c1 ,c2&c3, c1&c2&c3 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 ,c1&c1&c1|c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select abs(c1)&c2&c3 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select abs(c2&c1&c3) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select abs(c3)+c1&c3+c2 from {dbname}.ct3")
        tdSql.checkRows(0)

        tdSql.query(f"select abs(c1)&c2&c3 from {dbname}.ct4")
        tdSql.checkRows(12)
        tdSql.checkData(0,0,None)
        tdSql.checkData(1,0,8)
        tdSql.checkData(10,0,0)
        tdSql.query(f"select abs(c2&c1&c3) from {dbname}.ct4")
        tdSql.checkRows(12)
        tdSql.checkData(0,0,None)
        tdSql.checkData(1,0,8)
        tdSql.checkData(10,0,0)
        tdSql.query(f"select (abs(c3)+c1)&(c3+c2) from {dbname}.ct4")
        tdSql.checkRows(12)
        tdSql.checkData(0,0,None)
        tdSql.checkData(1,0,640)
        tdSql.checkData(10,0,0)

        # used for regular table
        tdSql.query(f"select abs(c1)&c3&c3 from {dbname}.t1")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select abs(c1)&c2|ceil(c3)&c4|floor(c5) from {dbname}.t1")
        tdSql.checkData(1, 0, 11)
        tdSql.checkData(3, 0, 3)
        tdSql.checkData(5, 0, None)
        tdSql.query(f"select ts,c1, c2, c3&c4|c5 from {dbname}.t1")
        tdSql.checkData(1, 3, 11)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(5, 3, None)

        self.check_function("&",False,f"{dbname}.stb1","c1","ceil(c2)","abs(c3)","c4+1")
        self.check_function("|",False,f"{dbname}.stb1","c1","ceil(c2)","abs(c3)","c4+1")
        self.check_function("&",False,f"{dbname}.stb1","c1+c2","ceil(c2)","abs(c3+c2)","c4+1")
        self.check_function("&",False,f"{dbname}.ct4","123","ceil(c2)","abs(c3+c2)","c4+1")
        self.check_function("&",False,f"{dbname}.ct4","123","ceil(t1)","abs(c3+c2)","c4+1")
        self.check_function("&",False,f"{dbname}.ct4","t1+c1","-ceil(t1)","abs(c3+c2)","c4+1")
        self.check_function("&",False,f"{dbname}.stb1","c1","floor(t1)","abs(c1+c2)","t1+1")
        self.check_function("&",True,f"{dbname}.stb1","max(c1)","min(floor(t1))","sum(abs(c1+c2))","last(t1)+1")
        self.check_function("&",False,f"{dbname}.stb1","abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(c1))))))))))","floor(t1)","abs(c1+c2)","t1+1")

        # mix with common col
        tdSql.query(f"select c1&abs(c1)&c2&c3 ,c1,c2, t1 from {dbname}.ct1")
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(4, 0, 0)
        tdSql.checkData(4, 3, 0)
        tdSql.checkData(3, 2, 55555)


        # mix with common functions
        tdSql.query(f" select c1&abs(c1)&c2&c3, abs(c1), c5, floor(c5) from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)

        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(3, 2, 6.66000)
        tdSql.checkData(3, 3, 6.00000)

        tdSql.query(f"select c1&abs(c1)&c2&c3, abs(c1),c5, floor(c5) from {dbname}.stb1 order by ts ")
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 6)
        tdSql.checkData(3, 2, 6.66000)
        tdSql.checkData(3, 3, 6.00000)

        # mix with agg functions , not support
        tdSql.error(f"select c1&abs(c1)&c2&c3, abs(c1),c5, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1&abs(c1)&c2&c3, abs(c1),c5, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1&abs(c1)&c2&c3, count(c5) from {dbname}.stb1 ")
        tdSql.error(f"select c1&abs(c1)&c2&c3, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1&abs(c1)&c2&c3, count(c5) from {dbname}.ct1 ")
        tdSql.error(f"select c1&abs(c1)&c2&c3, count(c5) from {dbname}.stb1 ")

        # agg functions mix with agg functions

        tdSql.query(f"select sum(c1&abs(c1)&c2&c3) ,max(c5), count(c5) from {dbname}.stb1")

        tdSql.query(f"select max(c1)&max(c2)|first(ts), count(c5) from {dbname}.ct1")

        # bug fix for compute
        tdSql.query(f"select c1&abs(c1)&c2&c3, abs(c1&abs(c1)&c2&c3) -0 ,ceil(c1&abs(c1)&c2&c3)-0 from {dbname}.ct4 ")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(1, 1, 8.000000000)
        tdSql.checkData(1, 2, 8.000000000)

        tdSql.query(f" select c1&c2|c3, abs(c1&c2|c3) -0 ,ceil(c1&c2|c3-0.1)-0.1 from {dbname}.ct4")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 888)
        tdSql.checkData(1, 1, 888.000000000)
        tdSql.checkData(1, 2, 894.900000000)




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
            f"insert into {dbname}.sub1_bound values ( now()+5s, -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        tdSql.error(
            f"insert into {dbname}.sub1_bound values ( now()+10s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
        )
        self.check_function("&", False , f"{dbname}.sub1_bound" ,"c1","c2","c3","c4","c5","c6" )
        self.check_function("&", False , f"{dbname}.sub1_bound","abs(c1)","abs(c2)","abs(c3)","abs(c4)","abs(c5)","abs(c6)" )
        self.check_function("&", False , f"{dbname}.stb_bound","123","abs(c2)","t1","abs(c4)","abs(c5)","abs(c6)" )

        # check basic elem for table per row
        tdSql.query(
            f"select abs(c1) ,abs(c2) , abs(c3) , abs(c4), abs(c5), abs(c6) from {dbname}.sub1_bound ")
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
        self.check_function("&", False , f"{dbname}.stb_bound","abs(c1+1)","abs(c2)","t1","abs(c3*1)","abs(c5)/2","abs(c6)" )

        tdSql.query(
            f"select abs(c1+1) ,abs(c2) , abs(c3*1) , abs(c4/2), abs(c5)/2, abs(c6) from {dbname}.sub1_bound ")
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


    def test_tag_compute_for_scalar_function(self, dbname="testdb"):

        tdSql.execute(f"use {dbname}")

        self.check_function("&", False , f"{dbname}.ct4","123","abs(c1)","t1","abs(t2)","abs(t3)","abs(t4)","t5")
        self.check_function("&", False , f"{dbname}.ct4","c1+2","abs(t2+2)","t3","abs(t4)","abs(t5)","abs(c1)","t5")

        tdSql.query(f" select sum(c1) from {dbname}.stb1 where t1+10 >1; ")
        tdSql.query(f"select c1 ,t1 from {dbname}.stb1 where t1 =0 ")
        tdSql.checkRows(13)
        self.check_function("&", False , f"{dbname}.t1","c1+2","abs(c2)")
        tdSql.query(f"select t1 from {dbname}.stb1 where t1 >0 ")
        tdSql.checkRows(12)
        tdSql.query(f"select t1 from {dbname}.stb1 where t1 =3 ")
        tdSql.checkRows(12)
        # tdSql.query("select sum(t1) from (select c1 ,t1 from stb1)")
        # tdSql.checkData(0,0,61)
        # tdSql.query("select distinct(c1) ,t1 from stb1")
        # tdSql.checkRows(20)
        tdSql.query(f"select max(c1) , t1&c2&t2 from {dbname}.stb1;")
        tdSql.checkData(0,1,0)

        # tag filter with abs function
        tdSql.query(f"select t1 from {dbname}.stb1 where abs(t1)=1")
        tdSql.checkRows(0)
        tdSql.query(f"select t1 from {dbname}.stb1 where abs(c1+t1)=1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)

        tdSql.query(
            f"select abs(c1+t1)*t1 from {dbname}.stb1 where abs(c1)/floor(abs(ceil(t1))) ==1")

    def support_super_table_test(self, dbname="testdb"):
        tdSql.execute(f" use {dbname} ")
        self.check_function("|", False , f"{dbname}.stb1" , "c1","c2","c3","c4" )
        self.check_function("|", False , f"{dbname}.stb1" , "c1","c2","abs(c3)","c4","ceil(t1)" )
        self.check_function("&", False , f"{dbname}.stb1" , "c1","c2","abs(c3)","floor(c4)","ceil(t1)" )
        self.check_function("&", True , f"{dbname}.stb1" , "max(c1)","max(c2)","sum(abs(c3))","max(floor(c4))","min(ceil(t1))" )


    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        self.prepare_datas()
        self.prepare_tag_datas()
        self.test_errors()
        self.basic_query()
        self.check_boundary_values()
        self.test_tag_compute_for_scalar_function()
        self.support_super_table_test()
        self.insert_datas_and_check_abs(self.tb_nums,self.row_nums,self.time_step)



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
