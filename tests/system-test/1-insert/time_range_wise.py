import datetime

from dataclasses import dataclass
from typing import List, Any, Tuple
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.constant import *

PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_tint_un"
SINT_UN_COL = "c_sint_un"
BINT_UN_COL = "c_bint_un"
INT_UN_COL = "c_int_un"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"



NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL = [BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [BOOL_COL, ]
TS_TYPE_COL = [TS_COL, ]

# insert data args：
TIME_STEP = 10000
NOW = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)


@dataclass
class DataSet:
    ts_data     : List[int]     = None
    int_data    : List[int]     = None
    bint_data   : List[int]     = None
    sint_data   : List[int]     = None
    tint_data   : List[int]     = None
    int_un_data : List[int]     = None
    bint_un_data: List[int]     = None
    sint_un_data: List[int]     = None
    tint_un_data: List[int]     = None
    float_data  : List[float]   = None
    double_data : List[float]   = None
    bool_data   : List[int]     = None
    binary_data : List[str]     = None
    nchar_data  : List[str]     = None

    def __post_init__(self):
        self.ts_data        = []
        self.int_data       = []
        self.bint_data      = []
        self.sint_data      = []
        self.tint_data      = []
        self.int_un_data    = []
        self.bint_un_data   = []
        self.sint_un_data   = []
        self.tint_un_data   = []
        self.float_data     = []
        self.double_data    = []
        self.bool_data      = []
        self.binary_data    = []
        self.nchar_data     = []


@dataclass
class SMAschema:
    creation    : str           = "CREATE"
    index_name  : str           = "sma_index_1"
    index_flag  : str           = "SMA INDEX"
    operator    : str           = "ON"
    tbname      : str           = None
    watermark   : str           = None
    maxdelay    : str           = None
    func        : Tuple[str]    = None
    interval    : Tuple[str]    = None
    sliding     : str           = None
    other       : Any           = None
    drop        : str           = "DROP"
    drop_flag   : str           = "INDEX"

    def __post_init__(self):
        if isinstance(self.other, dict):
            for k,v in self.other.items():

                if k.lower() == "index_name" and isinstance(v, str) and not self.index_name:
                    self.index_name = v
                    del self.other[k]

                if k.lower() == "index_flag" and isinstance(v, str) and not self.index_flag:
                    self.index_flag = v
                    del self.other[k]

                if k.lower() == "operator" and isinstance(v, str) and not self.operator:
                    self.operator = v
                    del self.other[k]

                if k.lower() == "tbname" and isinstance(v, str) and not self.tbname:
                    self.tbname = v
                    del self.other[k]

                if k.lower() == "watermark" and isinstance(v, str) and not self.watermark:
                    self.watermark = v
                    del self.other[k]

                if k.lower() == "maxdelay" and isinstance(v, str) and not self.maxdelay:
                    self.maxdelay = v
                    del self.other[k]

                if k.lower() == "functions" and isinstance(v, tuple) and not self.func:
                    self.func = v
                    del self.other[k]

                if k.lower() == "interval" and isinstance(v, tuple) and not self.interval:
                    self.interval = v
                    del self.other[k]

                if k.lower() == "sliding" and isinstance(v, str) and not self.sliding:
                    self.sliding = v
                    del self.other[k]

                if k.lower() == "drop_flag" and isinstance(v, str) and not self.drop_flag:
                    self.drop_flag = v
                    del self.other[k]


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    """
        create sma index :
            1. only create on stable, err_type: [child-table, normal-table]
            2. one taosd, one sma index , err_type: [
                one stb --> multi sma index,
                multi stb in one db--> multi sma index,
                multi stb in multi db --> multi sma index
            ]
            3. arg of (interval/sliding) in query sql is equal to this arg in sma index
            4. client timezone is equal to timezone of sma index
            5. does not take effect unless querySmaOptimize flag is turned on,
    """
    def __create_sma_index(self, sma:SMAschema):
        sql = f"{sma.creation} {sma.index_flag} {sma.index_name} {sma.operator} {sma.tbname}"
        if sma.func:
            sql += f" function({', '.join(sma.func)})"
        if sma.interval:
            sql += f" interval({', '.join(sma.interval)})"
        if sma.sliding:
            sql += f" sliding({sma.sliding})"
        if sma.watermark:
            sql += f" watermark {sma.watermark}"
        if sma.maxdelay:
            sql += f" maxdelay {sma.maxdelay}"
        if isinstance(sma.other, dict):
            for k,v in sma.other.items():
                if isinstance(v,tuple) or isinstance(v, list):
                    sql += f" {k} ({' '.join(v)})"
                else:
                    sql += f" {k} {v}"
        if isinstance(sma.other, tuple) or isinstance(sma.other, list):
            sql += " ".join(sma.other)
        if isinstance(sma.other, int) or isinstance(sma.other, float) or isinstance(sma.other, str):
            sql += sma.other

        return sql

    def sma_create_check(self, sma:SMAschema):
        tdSql.query("show stables")
        stb_in_list = False
        for row in tdSql.queryResult:
            if sma.tbname == row[0]:
                stb_in_list = True
                break
        if not stb_in_list:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.creation:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.index_flag:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.index_name:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.operator:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.tbname:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.func:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.interval:
            tdSql.error(self.__create_sma_index(sma))
        if not sma.sliding:
            tdSql.error(self.__create_sma_index(sma))
        if sma.other:
            tdSql.error(self.__create_sma_index(sma))


    def all_test(self):
        pass

    def __create_tb(self):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags (tag1 int)
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

            tdSql.execute(
                f"insert into ct1 values ( {NOW - i * TIME_STEP}, {row_data} )")
            tdSql.execute(
                f"insert into ct2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )")
            tdSql.execute(
                f"insert into ct4 values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )")
            tdSql.execute(
                f"insert into t1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )")

        tdSql.execute(
            f"insert into ct2 values ( {NOW + int(TIME_STEP * 0.6)}, {null_data} )")
        tdSql.execute(
            f"insert into ct2 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.6)}, {null_data} )")
        tdSql.execute(
            f"insert into ct2 values ( {NOW - self.rows * int(TIME_STEP * 0.29) }, {null_data} )")

        tdSql.execute(
            f"insert into ct4 values ( {NOW + int(TIME_STEP * 0.8)}, {null_data} )")
        tdSql.execute(
            f"insert into ct4 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.8)}, {null_data} )")
        tdSql.execute(
            f"insert into ct4 values ( {NOW - self.rows * int(TIME_STEP * 0.39)}, {null_data} )")

        tdSql.execute(
            f"insert into t1 values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )")
        tdSql.execute(
            f"insert into t1 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )")
        tdSql.execute(
            f"insert into t1 values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )")

    def run(self):
        sma1 = SMAschema(func=("min(c1)","max(c2)"))
        sql1 = self.__create_sma_index(sma1)
        print("================")
        print(sql1)
        # a = DataSet()
        # return
        self.rows = 10

        tdLog.printNoPrefix("==========step0:all check")
        # self.all_test()

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        return

        tdLog.printNoPrefix("==========step2:create table in rollup database")
        tdSql.execute("create database db3 retentions 1s:4m,2s:8m,3s:12m")
        tdSql.execute("use db3")
        self.__create_tb()
        self.__insert_data()

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
