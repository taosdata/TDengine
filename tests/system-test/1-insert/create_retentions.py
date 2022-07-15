import datetime

from dataclasses import dataclass, field
from typing import List
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

PRIMARY_COL = "ts"

INT_COL     = "c_int"
BINT_COL    = "c_bint"
SINT_COL    = "c_sint"
TINT_COL    = "c_tint"
FLOAT_COL   = "c_float"
DOUBLE_COL  = "c_double"
BOOL_COL    = "c_bool"
TINT_UN_COL = "c_tint_un"
SINT_UN_COL = "c_sint_un"
BINT_UN_COL = "c_bint_un"
INT_UN_COL  = "c_int_un"

BINARY_COL  = "c_binary"
NCHAR_COL   = "c_nchar"
TS_COL      = "c_ts"

NUM_COL     = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [ BOOL_COL, ]
TS_TYPE_COL = [ TS_COL, ]

## insert data args：
TIME_STEP = 10000
NOW = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

@dataclass
class DataSet:
    ts_data     : List[int]     = field(default_factory=list)
    int_data    : List[int]     = field(default_factory=list)
    bint_data   : List[int]     = field(default_factory=list)
    sint_data   : List[int]     = field(default_factory=list)
    tint_data   : List[int]     = field(default_factory=list)
    int_un_data : List[int]     = field(default_factory=list)
    bint_un_data: List[int]     = field(default_factory=list)
    sint_un_data: List[int]     = field(default_factory=list)
    tint_un_data: List[int]     = field(default_factory=list)
    float_data  : List[float]   = field(default_factory=list)
    double_data : List[float]   = field(default_factory=list)
    bool_data   : List[int]     = field(default_factory=list)
    binary_data : List[str]     = field(default_factory=list)
    nchar_data  : List[str]     = field(default_factory=list)


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    @property
    def create_databases_sql_err(self):
        return [
            "create database db1 retentions 0s:1d",
            "create database db3 retentions 1s:0d",
            "create database db1 retentions 1s:1y",
            "create database db1 retentions 1s:1n",
            "create database db2 retentions 1w:1d ;",
            "create database db5 retentions 1s:1d,3s:3d,2s:2d",
            "create database db1 retentions 1s:1n,2s:2d,3s:3d,4s:4d",
        ]

    @property
    def create_databases_sql_current(self):
        return [
            "create database db1 retentions 1s:1d",
            "create database db2 retentions 1s:1d,2m:2d,3h:3d",
        ]

    @property
    def alter_database_sql(self):
        return [
            "alter database db1 retentions 99h:99d",
            "alter database db2 retentions 97h:97d,98h:98d,99h:99d,",
        ]

    @property
    def create_stable_sql_err(self):
        return [
            f"create stable stb11 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(ceil) watermark 1s max_delay 1m",
            f"create stable stb12 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(count) watermark  1min",
            f"create stable stb13 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay -1s",
            f"create stable stb14 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark -1m",
            f"create stable stb15 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) watermark 1m ",
            f"create stable stb16 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) max_delay 1m ",
            f"create stable stb21 ({PRIMARY_COL} timestamp, {INT_COL} int, {BINARY_COL} binary(16)) tags (tag1 int) rollup(avg) watermark 1s",
            f"create stable stb22 ({PRIMARY_COL} timestamp, {INT_COL} int, {NCHAR_COL} nchar(16)) tags (tag1 int) rollup(avg) max_delay 1m",
            f"create table ntb_1 ({PRIMARY_COL} timestamp, {INT_COL} int, {NCHAR_COL} nchar(16)) rollup(avg) watermark 1s max_delay 1s",
            f"create stable stb23 ({PRIMARY_COL} timestamp, {INT_COL} int, {NCHAR_COL} nchar(16)) tags (tag1 int) " ,
            f"create stable stb24 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) " ,
            f"create stable stb25 ({PRIMARY_COL} timestamp, {INT_COL} int) " ,
            f"create stable stb26 ({PRIMARY_COL} timestamp, {INT_COL} int, {BINARY_COL} nchar(16)) " ,

            # watermark, max_delay: [0, 900000], [ms, s, m, ?]
            f"create stable stb17 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 1u",
            f"create stable stb18 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 1b",
            f"create stable stb19 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 900001ms",
            f"create stable stb20 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 16m",
            f"create stable stb27 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 901s",
            f"create stable stb28 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 1h",
            f"create stable stb29 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 0.2h",
            f"create stable stb30 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 0.002d",

        ]

    @property
    def create_stable_sql_current(self):
        return [
            f"create stable stb1 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(avg)",
            f"create stable stb2 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 5s max_delay 1m",
            f"create stable stb3 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(max) watermark 5s max_delay 1m",
            f"create stable stb4 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(sum) watermark 5s max_delay 1m",
            f"create stable stb5 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(last) watermark 5s max_delay 1m",
            f"create stable stb6 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(first) watermark 5s max_delay 1m",
            f"create stable stb7 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(first) watermark 5s max_delay 1m sma({INT_COL})",
        ]

    def test_create_stb(self, db="db2"):
        tdSql.execute(f"use {db}")
        for err_sql in self.create_stable_sql_err:
            tdSql.error(err_sql)
        for cur_sql in self.create_stable_sql_current:
            tdSql.execute(cur_sql)
        tdSql.query("show stables")
        # assert "rollup" in tdSql.description
        tdSql.checkRows(len(self.create_stable_sql_current))

        tdSql.execute("use db")  # because db is a noraml database, not a rollup database, should not be able to create a rollup stable
        tdSql.error(f"create stable nor_db_rollup_stb ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) watermark 5s max_delay 1m")


    def test_create_databases(self):
        for err_sql in self.create_databases_sql_err:
            tdSql.error(err_sql)
        for cur_sql in self.create_databases_sql_current:
            tdSql.execute(cur_sql)
            # tdSql.query("show databases")
        for alter_sql in self.alter_database_sql:
            tdSql.error(alter_sql)

    def all_test(self):
        self.test_create_databases()
        self.test_create_stb()

    def __create_tb(self):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql  =  f'''create table stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')

    def __data_set(self, rows):
        data_set = DataSet()

        for i in range(rows):
            data_set.ts_data.append(NOW + 1 * (rows - i))
            data_set.int_data.append(rows - i)
            data_set.bint_data.append(11111 * (rows - i))
            data_set.sint_data.append(111 * (rows - i) % 32767)
            data_set.tint_data.append(11 * (rows - i) % 127)
            data_set.int_un_data.append(rows - i)
            data_set.bint_un_data.append(11111 * (rows - i))
            data_set.sint_un_data.append(111 * (rows - i) % 32767)
            data_set.tint_un_data.append(11 * (rows - i) % 127)
            data_set.float_data.append(1.11 * (rows - i))
            data_set.double_data.append(1100.0011 * (rows - i))
            data_set.bool_data.append((rows - i) % 2)
            data_set.binary_data.append(f'binary{(rows - i)}')
            data_set.nchar_data.append(f'nchar_测试_{(rows - i)}')

        return data_set

    def __insert_data(self):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        data = self.__data_set(rows=self.rows)

        # now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        null_data = '''null, null, null, null, null, null, null, null, null, null, null, null, null, null'''
        zero_data = "0, 0, 0, 0, 0, 0, 0, 'binary_0', 'nchar_0', 0, 0, 0, 0, 0"

        for i in range(self.rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.binary_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.tint_un_data[i]},
                {data.sint_un_data[i]}, {data.int_un_data[i]}, {data.bint_un_data[i]}
            '''
            neg_row_data = f'''
                {-1 * data.int_data[i]}, {-1 * data.bint_data[i]}, {-1 * data.sint_data[i]}, {-1 * data.tint_data[i]}, {-1 * data.float_data[i]}, {-1 * data.double_data[i]},
                {data.bool_data[i]}, '{data.binary_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {1 * data.tint_un_data[i]},
                {1 * data.sint_un_data[i]}, {1 * data.int_un_data[i]}, {1 * data.bint_un_data[i]}
            '''

            tdSql.execute( f"insert into ct1 values ( {NOW - i * TIME_STEP}, {row_data} )" )
            tdSql.execute( f"insert into ct2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )" )
            tdSql.execute( f"insert into ct4 values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )" )
            tdSql.execute( f"insert into t1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )" )

        tdSql.execute( f"insert into ct2 values ( {NOW + int(TIME_STEP * 0.6)}, {null_data} )" )
        tdSql.execute( f"insert into ct2 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.6)}, {null_data} )" )
        tdSql.execute( f"insert into ct2 values ( {NOW - self.rows * int(TIME_STEP * 0.29) }, {null_data} )" )

        tdSql.execute( f"insert into ct4 values ( {NOW + int(TIME_STEP * 0.8)}, {null_data} )" )
        tdSql.execute( f"insert into ct4 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.8)}, {null_data} )" )
        tdSql.execute( f"insert into ct4 values ( {NOW - self.rows * int(TIME_STEP * 0.39)}, {null_data} )" )

        tdSql.execute( f"insert into t1 values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )" )
        tdSql.execute( f"insert into t1 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )" )
        tdSql.execute( f"insert into t1 values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )" )


    def run(self):
        self.rows = 10
        tdSql.prepare()

        tdLog.printNoPrefix("==========step0:all check")
        self.all_test()

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        # return

        tdLog.printNoPrefix("==========step2:create table in rollup database")
        tdSql.execute("create database db3 retentions 1s:4m,2s:8m,3s:12m")

        tdSql.execute("drop database if exists db1 ")
        tdSql.execute("drop database if exists db2 ")

        tdSql.execute("use db3")
        self.test_create_stb(db="db3")
        # self.__create_tb()
        # self.__insert_data()
        self.all_test()

        tdSql.execute("drop database if exists db1 ")
        tdSql.execute("drop database if exists db2 ")

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
