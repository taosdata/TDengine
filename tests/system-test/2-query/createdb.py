import datetime

from dataclasses import dataclass
from typing import List
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

COL_SCHEMA = "INT"
PRIMARY_COL = "tS"
STBNAME = "stb1"
NTBNAME = "ntb1"

TAG_SCHEMA = "INT"
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


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def __create_tb(self):
        tdLog.printNoPrefix("==========step1:create table")

        cols = f"{PRIMARY_COL} timestamp "
        tags = f"tag_0 {TAG_SCHEMA}"

        for i in range(4095):
            cols += f", col_{i} {COL_SCHEMA}"

        for i in range(127):
            tags += f", tag_{i+1} {TAG_SCHEMA}"

        create_stb_sql = f"create table {STBNAME}({cols}) tags ({tags})"
        create_ntb_sql = f"create table {NTBNAME}({cols})"

        # tdSql.execute(create_stb_sql)

        # tag_row = ""
        # for i in range(128):
        #     if tag_row:
        #         tag_row += f", {i}"

        # for i in range(4):
        #     tag_row = f"{i+1}"
        #     for j in range(127):
        #         tag_row += f", {i+1}"
        #     tdSql.execute(f'create table ct{i+1} using stb1 tags ( {tag_row} )')

        # return
        tdSql.execute(create_ntb_sql)


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

    def __insert_data(self, rows):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        data = self.__data_set(rows=self.rows)

        for i in range(rows):
            rows_data = f"{NOW - i * 1000}"
            for j in range(4095):
                rows_data += f", {data.int_data[i]} "
            tdSql.execute(
                f"insert into ct1 values ({rows_data})")


    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        # self.__insert_data(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        # self.all_test()

        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        # tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        # self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
