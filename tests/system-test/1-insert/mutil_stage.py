from datetime import datetime
from platform import platform
import time

from typing import List, Any, Tuple
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

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

TAG_COL = [INT_TAG]
# insert data args：
TIME_STEP = 10000
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = "stb1"
CTB_PRE = "ct"
NTB_PRE = "nt"

L0 = 0
L1 = 1
L2 = 2

PRIMARY_DIR = 1
NON_PRIMARY_DIR = 0

DATA_PRE0 = f"data0"
DATA_PRE1 = f"data1"
DATA_PRE2 = f"data2"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.taos_cfg_path = tdDnodes.dnodes[0].cfgPath
        self.taos_data_dir = tdDnodes.dnodes[0].dataDir


    def cfg(self, filename, **update_dict):
        cmd = "echo "
        for k, v in update_dict.items():
            cmd += f"{k} {v}\n"

        cmd += f" >> {filename}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def cfg_str(self, filename, update_str):
        cmd = f'echo {update_str} >> {filename}'
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def cfg_str_list(self, filename, update_list):
        for update_str in update_list:
            self.cfg_str(filename, update_str)

    def del_old_datadir(self, filename):
        cmd = f"sed -i '/^dataDir/d' {filename}"
        if platform.system().lower() == 'darwin':
            cmd = f"sed -i '' '/^dataDir/d' {filename}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    @property
    def __err_cfg(self):
        cfg_list = []
        err_case1 = [
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE1}1 {L1} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}2 {L2} {NON_PRIMARY_DIR}"
        ]
        err_case2 = [
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE1}1 {L1} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}2 {L2} {PRIMARY_DIR}"
        ]
        err_case3 = [
            f"dataDir {self.taos_data_dir}{os.sep}data33 3 {NON_PRIMARY_DIR}"
        ]
        err_case4 = [
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE1}1 {L1} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}2 {L2} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}2 {L1} {NON_PRIMARY_DIR}"
        ]
        err_case5 = [f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {PRIMARY_DIR}"]
        for i in range(16):
            err_case5.append(f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}{i+1} {L0} {NON_PRIMARY_DIR}")

        err_case6 = [
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}1 {L0} {PRIMARY_DIR}",
        ]
        err_case7 = [
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}2 {L2} {PRIMARY_DIR}",
        ]
        err_case8 = [
            f"dataDir {self.taos_data_dir}{os.sep}data33 3 {PRIMARY_DIR}"
        ]
        err_case9 = [
            f"dataDir {self.taos_data_dir}{os.sep}data33 -1 {NON_PRIMARY_DIR}"
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
            #f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}1 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE1}1 {L1} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}2 {L2} {NON_PRIMARY_DIR}"
        ]

        #current_case2 = [f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 {L0} {PRIMARY_DIR}"]
        current_case2 = []
        for i in range(9):
            current_case2.append(f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}{i+1} {L0} {NON_PRIMARY_DIR}")

        # TD-17773bug
        current_case3 = [
            #f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}0 ",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE0}1 {L0} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE1}0 {L1} {NON_PRIMARY_DIR}",
            f"dataDir {self.taos_data_dir}{os.sep}{DATA_PRE2}0 {L2} {NON_PRIMARY_DIR}",
        ]
        cfg_list.append(current_case1)
        cfg_list.append(current_case3)

        # case2 must in last of least, because use this cfg as data uniformity test
        cfg_list.append(current_case2)

        return cfg_list

    def cfg_check(self):
        for cfg_case in self.__err_cfg:
            self.del_old_datadir(filename=self.taos_cfg_path)
            tdDnodes.stop(1)
            tdDnodes.deploy(1)
            self.cfg_str_list(filename=self.taos_cfg_path, update_list=cfg_case)
            tdDnodes.starttaosd(1)
            time.sleep(2)
            tdSql.error(f"select * from information_schema.ins_databases")

        for cfg_case in self.__current_cfg:
            self.del_old_datadir(filename=self.taos_cfg_path)
            tdDnodes.stop(1)
            tdDnodes.deploy(1)
            self.cfg_str_list(filename=self.taos_cfg_path, update_list=cfg_case)
            tdDnodes.start(1)
            tdSql.query(f"select * from information_schema.ins_databases")

    def __create_tb(self, stb=STBNAME, ctb_pre = CTB_PRE, ctb_num=20, ntb_pre=NTB_PRE, ntbnum=1, dbname=DBNAME):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table {dbname}.{stb}(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        tdSql.execute(create_stb_sql)

        for i in range(ntbnum):
            create_ntb_sql = f'''create table {dbname}.{ntb_pre}{i+1}(
                    ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
            tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.{ctb_pre}{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __insert_data(self, rows, dbname=DBNAME, ctb_num=20):
        data = DataSet()
        data.get_order_set(rows)

        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        for i in range(self.rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.utint_data[i]},
                {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
            '''
            neg_row_data = f'''
                {-1 * data.int_data[i]}, {-1 * data.bint_data[i]}, {-1 * data.sint_data[i]}, {-1 * data.tint_data[i]}, {-1 * data.float_data[i]}, {-1 * data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {1 * data.utint_data[i]},
                {1 * data.usint_data[i]}, {1 * data.uint_data[i]}, {1 * data.ubint_data[i]}
            '''

            for j in range(ctb_num):
                tdSql.execute(
                    f"insert into {dbname}.{CTB_PRE}{j + 1} values ( {NOW - i * TIME_STEP}, {row_data} )")

            # tdSql.execute(
            #     f"insert into {dbname}.{CTB_PRE}2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )")
            # tdSql.execute(
            #     f"insert into {dbname}.{CTB_PRE}4 values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )")
            tdSql.execute(
                f"insert into {dbname}.{NTB_PRE}1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )")

    def run(self):
        self.rows = 10
        self.cfg_check()
        tdSql.prepare(dbname=DBNAME, **{"keep": "1d, 1500m, 26h", "duration":"1h", "vgroups": 10})
        self.__create_tb(dbname=DBNAME)
        self.__insert_data(rows=self.rows, dbname=DBNAME)
        tdSql.query(f"select count(*) from {DBNAME}.{NTB_PRE}1")
        tdSql.execute(f"flush database {DBNAME}")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
