from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
UN_NUM_COL = [BOOL_COL, BINARY_COL, NCHAR_COL, ]
TS_TYPE_COL = [TS_COL]

DBNAME = "db"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database if not exists db vgroups 1')

    def __create_tb(self, dbname="db"):
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, f1 int
            ) tags (tag1 binary(16300))
            '''
        tdSql.execute(create_stb_sql)

        tag_value = 'a'
        for i in range(1200):
            tag_value = tag_value + 'a'

        for i in range(8000):
            tdSql.execute(f"create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( '{tag_value}' )")

    def __query_data(self, rows, dbname="db"):
        tdSql.execute(
            f'''select count(*) from {dbname}.stb1 where tag1 like '%a'
            '''
        )
        tdSql.checkRows(0)

    def run(self):
        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:query data")
        self.__query_data(10)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
