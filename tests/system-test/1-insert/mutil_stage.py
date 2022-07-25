import datetime

from dataclasses import dataclass, field
from typing import List, Any, Tuple
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_utint"
SINT_UN_COL = "c_usint"
BINT_UN_COL = "c_ubint"
INT_UN_COL = "c_uint"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL = [BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [BOOL_COL, ]
TS_TYPE_COL = [TS_COL, ]

INT_TAG = "t_int"

ALL_COL = [PRIMARY_COL, INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BINARY_COL, NCHAR_COL, BOOL_COL, TS_COL]
TAG_COL = [INT_TAG]
# insert data args：
TIME_STEP = 10000
NOW = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = f"{DBNAME}.stb1"
CTBNAME = f"{DBNAME}.ct1"
NTBNAME = f"{DBNAME}.nt1"

L0 = 0
L1 = 1
L2 = 2

PRIMARY_DIR = 1
NON_PRIMARY_DIR = 0

DATA_PRE0 = f"data0"
DATA_PRE1 = f"data1"
DATA_PRE2 = f"data2"

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
        self.taos_cfg_path = tdDnodes.dnodes[0].cfgPath
        self.taos_data_dir = tdDnodes.dnodes[0].dataDir


    def cfg(self, filename, **update_dict):
        self.del_old_datadir(filename)
        cmd = "echo "
        for k, v in update_dict.items():
            cmd += f"{k} {v}\n"

        cmd += f" >> {filename}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def cfg_str(self, filename, update_str):
        self.del_old_datadir(filename)
        cmd = f'echo "{update_str}" >> {filename}'
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def cfg_str_list(self, filename, update_list):
        for update_str in update_list:
            self.cfg_str(filename, update_str)

    def del_old_datadir(self, filename):
        cmd = f"sed -i '/^dataDir/d' {filename}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    @property
    def __err_cfg(self):
        cfg_list = []
        err_case1 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE1}1 {L1} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE2}2 {L2} {NON_PRIMARY_DIR}"
        ]
        err_case2 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE1}1 {L1} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE2}2 {L2} {PRIMARY_DIR}"
        ]
        err_case3 = [
            f"dataDir {self.taos_data_dir}/data33 3 {NON_PRIMARY_DIR}"
        ]
        err_case4 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE1}1 {L1} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE2}2 {L2} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE2}2 {L1} {NON_PRIMARY_DIR}"
        ]
        err_case5 = []
        for i in range(17):
            err_case5.append(f"dataDir {self.taos_data_dir}/{DATA_PRE0}{i} {L0} {NON_PRIMARY_DIR}")

        err_case6 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 {L0} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}1 {L0} {PRIMARY_DIR}",
        ]
        err_case7 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 {L0} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE2}2 {L2} {PRIMARY_DIR}",
        ]
        err_case8 = [
            f"dataDir {self.taos_data_dir}/data33 3 {PRIMARY_DIR}"
        ]
        err_case9 = [
            f"dataDir {self.taos_data_dir}/data33 -1 {NON_PRIMARY_DIR}"
        ]

        cfg_list.append(err_case1)
        cfg_list.append(err_case2)
        cfg_list.append(err_case3)
        cfg_list.append(err_case4)
        cfg_list.append(err_case5)
        cfg_list.append(err_case6)
        cfg_list.append(err_case7)
        cfg_list.append(err_case8)
        cfg_list.append(err_case9)

        return cfg_list

    @property
    def __current_cfg(self):
        cfg_list = []
        current_case1 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 {L0} {PRIMARY_COL}",
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}1 {L0} 0",
            f"dataDir {self.taos_data_dir}/{DATA_PRE1}1 {L1} 0",
            f"dataDir {self.taos_data_dir}/{DATA_PRE2}2 {L2} 0"
        ]

        current_case2 = [
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 0 1",
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}1 0 1",
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 0 1",
            f"dataDir {self.taos_data_dir}/{DATA_PRE0}0 0 1",

        ]

        cfg_list.append(current_case1)

        return cfg_list

    def cfg_check(self):
        for cfg_case in self.__err_cfg:
            tdDnodes.stop(1)
            self.cfg_str_list(filename=self.taos_cfg_path, update_list=cfg_case)
            tdDnodes.start(1)

            tdSql.error(f"show databases")


    def run(self):
        self.cfg_check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
