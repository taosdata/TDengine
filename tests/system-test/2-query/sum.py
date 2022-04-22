import taos
import sys
import datetime
import inspect

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

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BOOL_COL]
UN_NUM_COL = [BINARY_COL, NCHAR_COL, TS_COL]

class TDTestCase:



    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def __sum_current_check(self, row, col,data):
        pass

    def __sum_err_check(self,tbanme):
        sqls = []

        for un_num_col in UN_NUM_COL:
            sqls.append( f"select sum( {un_num_col} ) from {tbanme} " )
            sqls.extend( f"select sum( {un_num_col} + {num_col} ) from {tbanme} " for num_col in NUM_COL )
            sqls.extend( f"select sum( {un_num_col} + {un_num_col_2} ) from {tbanme} " for un_num_col_2 in UN_NUM_COL )

        return sqls

    def __test_error(self):
        tbname = ["ct1", "ct2", "ct4", "t1"]

        for tb in tbname:
            for errsql in self.__sum_err_check(tb):
                tdSql.error(sql=errsql)


    def all_test(self):

        pass

    def __create_tb(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')

    def __insert_data(self, rows):
        for i in range(9):
            tdSql.execute(
                f"insert into ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct2 values ( now()-{i*90}d, {-1*i}, {-11111*i}, {-111*i}, {-11*i}, {-1.11*i}, {-11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            '''insert into ct1 values
            ( now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )
            ( now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )
            '''
        )

        tdSql.execute(
            f'''insert into ct4 values
            ( now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()+{rows * 9}d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                now()+{rows * 9-10}d, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nachar_limit-1", now()-1d
                )
            (
                now()+{rows * 9-20}d, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nachar_limit-2", now()-2d
                )
            '''
        )

        tdSql.execute(
            f'''insert into ct2 values
            ( now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()+{rows * 9}d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                now()+{rows * 9-10}d, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nachar_limit-1", now()-1d
                )
            (
                now()+{rows * 9-20}d, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nachar_limit-2", now()-2d
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into t1 values
                ( now()-{i}h, {i}, {i}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_{i}", now()-{i}s )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into t1 values
            ( now() + 3h, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-{ ( rows // 2 ) * 60 + 30 }m, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-{rows}h, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now() + 2h, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nachar_limit-1", now()-1d
                )
            (
                now() + 1h , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nachar_limit-2", now()-2d
                )
            '''
        )


    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data(1000)


        self.all_test()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.execute("use db")

        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
