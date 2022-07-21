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
                     "wDebugFlag": 143, "sDebugFlag": 143, "tsdbDebugFlag": 143, "tqDebugFlag": 143, "fsDebugFlag": 143, "udfDebugFlag": 143}

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.tb_nums = 10
        self.row_nums = 20
        self.ts = 1434938400000
        self.time_step = 1000

    def insert_datas_and_check_irate(self ,tbnums , rownums , time_step ):

        tdLog.info(" prepare datas for auto check irate function ")

        tdSql.execute(" create database test ")
        tdSql.execute(" use test ")
        tdSql.execute(" create stable stb (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint,\
             c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)")
        for tbnum in range(tbnums):
            tbname = "sub_tb_%d"%tbnum
            tdSql.execute(" create table %s using stb tags(%d) "%(tbname , tbnum))

            ts = self.ts
            for row in range(rownums):
                ts = self.ts + time_step*row
                c1 = random.randint(0,1000)
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
                if coltype[1] in support_types and coltype[-1] != "TAG" :
                    irate_sql = "select irate({}) from {}".format(colname, tbname)
                    origin_sql = "select tail({}, 2), cast(ts as bigint) from {} order by ts".format(colname, tbname)

                    tdSql.query(irate_sql)
                    irate_result = tdSql.queryResult
                    tdSql.query(origin_sql)
                    origin_result = tdSql.queryResult
                    irate_value = irate_result[0][0]
                    if origin_result[1][-1] - origin_result[0][-1] == 0:
                        comput_irate_value = 0
                    elif (origin_result[1][0] - origin_result[0][0])<0:
                        comput_irate_value = origin_result[1][0]*1000/( origin_result[1][-1] - origin_result[0][-1])
                    else:
                        comput_irate_value = (origin_result[1][0] - origin_result[0][0])*1000/( origin_result[1][-1] - origin_result[0][-1])
                    if comput_irate_value ==irate_value:
                        tdLog.info(" irate work as expected , sql is %s "% irate_sql)
                    else:
                        tdLog.exit(" irate work not as expected , sql is %s "% irate_sql)

    def prepare_tag_datas(self):
        # prepare datas
        tdSql.execute(
            "create database if not exists testdb keep 3650 duration 1000")
        tdSql.execute(" use testdb ")
        tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        tdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(
                f'create table ct{i+1} using stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            "insert into ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(
            "insert into ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(
            "insert into ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(
            "insert into ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(
            "insert into ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

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
        tdSql.execute("use testdb")
        error_sql_lists = [
            "select irate from t1",
            "select irate(-+--+c1) from t1",
            # "select +-irate(c1) from t1",
            # "select ++-irate(c1) from t1",
            # "select ++--irate(c1) from t1",
            # "select - -irate(c1)*0 from t1",
            # "select irate(tbname+1) from t1 ",
            "select irate(123--123)==1 from t1",
            "select irate(c1) as 'd1' from t1",
            "select irate(c1 ,c2 ) from t1",
            "select irate(c1 ,NULL) from t1",
            "select irate(,) from t1;",
            "select irate(irate(c1) ab from t1)",
            "select irate(c1) as int from t1",
            "select irate from stb1",
            # "select irate(-+--+c1) from stb1",
            # "select +-irate(c1) from stb1",
            # "select ++-irate(c1) from stb1",
            # "select ++--irate(c1) from stb1",
            # "select - -irate(c1)*0 from stb1",
            # "select irate(tbname+1) from stb1 ",
            "select irate(123--123)==1 from stb1",
            "select irate(c1) as 'd1' from stb1",
            "select irate(c1 ,c2 ) from stb1",
            "select irate(c1 ,NULL) from stb1",
            "select irate(,) from stb1;",
            "select irate(abs(c1) ab from stb1)",
            "select irate(c1) as int from stb1"
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)

    def support_types(self):
        tdSql.execute("use testdb")
        tbnames = ["stb1", "t1", "ct1", "ct2"]
        support_types = ["BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "INT"]
        for tbname in tbnames:
            tdSql.query("desc {}".format(tbname))
            coltypes = tdSql.queryResult
            for coltype in coltypes:
                colname = coltype[0]
                irate_sql = "select irate({}) from {}".format(colname, tbname)
                if coltype[1] in support_types:
                    tdSql.query(irate_sql)
                else:
                    tdSql.error(irate_sql)

    def basic_irate_function(self):

        # used for empty table  , ct3 is empty
        tdSql.query("select irate(c1) from ct3")
        tdSql.checkRows(0)
        tdSql.query("select irate(c2) from ct3")
        tdSql.checkRows(0)

        # used for regular table
        tdSql.query("select irate(c1) from t1")
        tdSql.checkData(0, 0, 0.000000386)

        # used for sub table
        tdSql.query("select irate(abs(c1+c2)) from ct1")
        tdSql.checkData(0, 0, 0.000000000)


        # mix with common col
        tdSql.error("select c1, irate(c1) from ct1")

        # mix with common functions
        tdSql.error("select irate(c1), abs(c1) from ct4 ")

        # agg functions mix with agg functions
        tdSql.query("select irate(c1), count(c5) from stb1 partition by tbname ")
        tdSql.checkData(0, 0, 0.000000000)
        tdSql.checkData(1, 0, 0.000000000)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(1, 1, 9)


    def irate_func_filter(self):
        tdSql.execute("use testdb")
        tdSql.query(
            "select irate(c1+2)/2 from ct4 where c1>5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0.000000514)

        tdSql.query(
            "select irate(c1+c2)/10 from ct4 where c1=5 ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0.000000000)

        tdSql.query(
            "select irate(c1+c2)/10 from stb1 where c1 = 5 partition by tbname ")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0.000000000)


    def irate_Arithmetic(self):
        pass


    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_tag_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.test_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4: irate basic query ============")

        self.basic_irate_function()

        tdLog.printNoPrefix("==========step5: irate filter query ============")

        self.irate_func_filter()


        tdLog.printNoPrefix("==========step6: check result of query ============")

        self.insert_datas_and_check_irate(self.tb_nums,self.row_nums,self.time_step)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
