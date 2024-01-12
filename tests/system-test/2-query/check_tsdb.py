import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:
    # updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    # "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    # "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def prepare_datas(self, dbname="db"):
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        # tdSql.execute(
        #     f'''
        #     create table t1
        #     (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
        #     '''
        # )
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

    def restart_taosd_query_sum(self, dbname="db"):

        for i in range(5):
            tdLog.notice("  this is %d_th restart taosd " %i)
            # os.system(f"taos -s ' use db ;select c6 from {dbname}.stb1 ; '")
            tdSql.execute(f"use {dbname} ")
            tdSql.query(f"select count(*) from {dbname}.stb1")
            tdSql.checkRows(1)
            tdSql.query(f"select sum(c1),sum(c2),sum(c3),sum(c4),sum(c5),sum(c6) from {dbname}.stb1;")
            tdSql.checkData(0,0,99)
            tdSql.checkData(0,1,499995)
            tdSql.checkData(0,2,4995)
            tdSql.checkData(0,3,594)
            tdSql.checkData(0,4,49.950001001)
            tdSql.checkData(0,5,599.940000000)
            tdDnodes.stop(1)
            tdDnodes.start(1)
            time.sleep(2)
            tdSql.query("select * from information_schema.ins_databases")

            status = False
            while status==False:
                tdSql.query("select * from information_schema.ins_databases")
                for db_info in tdSql.queryResult:
                    if db_info[0]==dbname :
                        if db_info[15]=="ready":
                            status = True
                            tdLog.notice(" ==== database {} status is ready  ==== ".format(dbname))
                            break
                        else:
                            status = False
                    else:
                        continue







    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        dbname = "db"

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        # os.system(f"taos -s ' select c6 from {dbname}.stb1 ; '")
        self.restart_taosd_query_sum()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
