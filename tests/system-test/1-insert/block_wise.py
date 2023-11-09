import datetime
import re

from dataclasses import dataclass, field
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
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"


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


@dataclass
class BSMAschema:
    creation            : str           = "CREATE"
    tb_type             : str           = "stable"
    tbname              : str           = STBNAME
    cols                : Tuple[str]    = None
    tags                : Tuple[str]    = None
    sma_flag            : str           = "SMA"
    sma_cols            : Tuple[str]    = None
    create_tabel_sql    : str           = None
    other               : Any           = None

    drop                : str           = "DROP"
    drop_flag           : str           = "INDEX"
    querySmaOptimize    : int           = 1
    show                : str           = "SHOW"
    show_msg            : str           = "INDEXES"
    show_oper           : str           = "FROM"
    dbname              : str           = None
    rollup_db           : bool          = False

    def __post_init__(self):
        if isinstance(self.other, dict):
            for k,v in self.other.items():

                if k.lower() == "tbname" and isinstance(v, str) and not self.tbname:
                    self.tbname = v
                    del self.other[k]

                if k.lower() == "cols" and (isinstance(v, tuple) or isinstance(v, list)) and not self.cols:
                    self.cols = v
                    del self.other[k]

                if k.lower() == "tags" and (isinstance(v, tuple) or isinstance(v, list)) and not self.tags:
                    self.tags = v
                    del self.other[k]

                if k.lower() == "sma_flag" and isinstance(v, str) and not self.sma_flag:
                    self.sma_flag = v
                    del self.other[k]

                if k.lower() == "sma_cols" and (isinstance(v, tuple) or isinstance(v, list)) and not self.sma_cols:
                    self.sma_cols = v
                    del self.other[k]

                if k.lower() == "create_tabel_sql" and isinstance(v, str) and not self.create_tabel_sql:
                    self.create_tabel_sql = v
                    del self.other[k]

                # bSma show and drop operator is  not completed
                if k.lower() == "drop_flag" and isinstance(v, str) and not self.drop_flag:
                    self.drop_flag = v
                    del self.other[k]

                if k.lower() == "show_msg" and isinstance(v, str) and not self.show_msg:
                    self.show_msg = v
                    del self.other[k]

                if k.lower() == "dbname" and isinstance(v, str) and not self.dbname:
                    self.dbname = v
                    del self.other[k]

                if k.lower() == "show_oper" and isinstance(v, str) and not self.show_oper:
                    self.show_oper = v
                    del self.other[k]

                if k.lower() == "rollup_db" and isinstance(v, bool) and not self.rollup_db:
                    self.rollup_db = v
                    del self.other[k]



# from ...pytest.util.sql import *
# from ...pytest.util.constant import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.precision = "ms"
        self.sma_count = 0
        self.sma_created_index = []

    def __create_sma_index(self, sma:BSMAschema):
        if sma.create_tabel_sql:
            sql = sma.create_tabel_sql
        else:
            sql = f"{sma.creation} {sma.tb_type} {sma.tbname} ({', '.join(sma.cols)}) "

            if sma.tb_type == "stable" or (sma.tb_type=="table" and sma.tags):
                sql = f"{sma.creation} {sma.tb_type} {sma.tbname} ({', '.join(sma.cols)}) tags ({', '.join(sma.tags)}) "


        if sma.sma_flag:
            sql += sma.sma_flag
        if sma.sma_cols:
            sql += f"({', '.join(sma.sma_cols)})"

        if isinstance(sma.other, dict):
            for k,v in sma.other.items():
                if isinstance(v,tuple) or isinstance(v, list):
                    sql += f" {k} ({' '.join(v)})"
                else:
                    sql += f" {k} {v}"
        if isinstance(sma.other, tuple) or isinstance(sma.other, list):
            sql += " ".join(sma.other)
        if isinstance(sma.other, int) or isinstance(sma.other, float) or isinstance(sma.other, str):
            sql += f" {sma.other}"

        return sql

    def __get_bsma_table_col_tag_str(self, sql:str):
        p = re.compile(r"[(](.*)[)]", re.S)

        if "tags" in (col_str := sql):
            col_str = re.findall(p, sql.split("tags")[0])[0].split(",")
            if (tag_str := re.findall(p, sql.split("tags")[1])[0].split(",") ):
                col_str.extend(tag_str)

        return col_str

    def __get_bsma_col_tag_names(self, col_tags:list):
        return [ col_tag.strip().split(" ")[0] for col_tag in col_tags ]

    @property
    def __get_db_tbname(self):
        tb_list = []
        tdSql.query("show tables")
        for row in tdSql.queryResult:
            tb_list.append(row[0])
        tdSql.query("show tables")
        for row in tdSql.queryResult:
            tb_list.append(row[0])

        return tb_list

    def __bsma_create_check(self, sma:BSMAschema):
        if not sma.creation:
            return False
        if not sma.create_tabel_sql and (not sma.tbname or not sma.tb_type or not sma.cols):
            return False
        if not sma.create_tabel_sql and (sma.tb_type == "stable" and not sma.tags):
            return  False
        if not sma.sma_flag or not isinstance(sma.sma_flag, str) or sma.sma_flag.upper() != "SMA":
            return False
        if sma.tbname in self.__get_db_tbname:
            return False

        if sma.create_tabel_sql:
            col_tag_list = self.__get_bsma_col_tag_names(self.__get_bsma_table_col_tag_str(sma.create_tabel_sql))
        else:
            col_str = list(sma.cols)
            if sma.tags:
                col_str.extend(list(sma.tags))
            col_tag_list = self.__get_bsma_col_tag_names(col_str)
        if not sma.sma_cols:
            return False
        for col in sma.sma_cols:
            if col not in col_tag_list:
                return False

        return True

    def bsma_create_check(self, sma:BSMAschema):
        if self.__bsma_create_check(sma):
            tdSql.query(self.__create_sma_index(sma))
            tdLog.info(f"current sql: {self.__create_sma_index(sma)}")

        else:
            tdSql.error(self.__create_sma_index(sma))


    def __sma_drop_check(self, sma:BSMAschema):
        pass

    def sma_drop_check(self, sma:BSMAschema):
        pass

    def __show_sma_index(self, sma:BSMAschema):
        pass

    def __sma_show_check(self, sma:BSMAschema):
        pass

    def sma_show_check(self, sma:BSMAschema):
        pass

    @property
    def __create_sma_sql(self):
        err_sqls = []
        cur_sqls = []
        # err_set
        ### case 1: required fields check
        err_sqls.append( BSMAschema(creation="", tbname="stb2", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(f"{INT_TAG} int",), sma_cols=(PRIMARY_COL, INT_COL ) ) )
        err_sqls.append( BSMAschema(tbname="", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(f"{INT_TAG} int",), sma_cols=(PRIMARY_COL, INT_COL ) ) )
        err_sqls.append( BSMAschema(tbname="stb2", cols=(), tags=(f"{INT_TAG} int",), sma_cols=(PRIMARY_COL, INT_COL ) ) )
        err_sqls.append( BSMAschema(tbname="stb2", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(), sma_cols=(PRIMARY_COL, INT_COL ) ) )
        err_sqls.append( BSMAschema(tbname="stb2", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(f"{INT_TAG} int",), sma_flag="", sma_cols=(PRIMARY_COL, INT_COL ) ) )
        err_sqls.append( BSMAschema(tbname="stb2", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(f"{INT_TAG} int",), sma_cols=() ) )
        ### case 2:
        err_sqls.append( BSMAschema(tbname="stb2", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(f"{INT_TAG} int",), sma_cols=({BINT_COL}) ) )

        # current_set
        cur_sqls.append( BSMAschema(tbname="stb2", cols=(f"{PRIMARY_COL} timestamp", f"{INT_COL} int"), tags=(f"{INT_TAG} int",), sma_cols=(PRIMARY_COL, INT_COL ) ) )

        return err_sqls, cur_sqls

    def test_create_sma(self):
        err_sqls , cur_sqls = self.__create_sma_sql
        for err_sql in err_sqls:
            self.bsma_create_check(err_sql)
        for cur_sql in cur_sqls:
            self.bsma_create_check(cur_sql)

    @property
    def __drop_sma_sql(self):
        err_sqls = []
        cur_sqls = []
        # err_set
        ## case 1: required fields check
        return err_sqls, cur_sqls

    def test_drop_sma(self):
        err_sqls , cur_sqls = self.__drop_sma_sql
        for err_sql in err_sqls:
            self.sma_drop_check(err_sql)
        for cur_sql in cur_sqls:
            self.sma_drop_check(cur_sql)

    def all_test(self):
        self.test_create_sma()

    def __create_tb(self, rollup=None):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table {STBNAME}(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        create_ntb_sql = f'''create table {NTBNAME}(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            )
            '''
        if rollup is not None:
            create_stb_sql += f" rollup({rollup})"
            tdSql.execute(create_stb_sql)
        else:
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

    def __insert_data(self, rollup=None):
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

            tdSql.execute(
                f"insert into ct1 values ( {NOW - i * TIME_STEP}, {row_data} )")
            tdSql.execute(
                f"insert into ct2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )")
            tdSql.execute(
                f"insert into ct4 values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )")
            if rollup is None:
                tdSql.execute(
                    f"insert into {NTBNAME} values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )")

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
        if rollup is None:
            tdSql.execute(
                f"insert into {NTBNAME} values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )")
            tdSql.execute(
                f"insert into {NTBNAME} values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )")
            tdSql.execute(
                f"insert into {NTBNAME} values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )")

    def run(self):
        self.rows = 10

        tdLog.printNoPrefix("==========step0:all check")

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        self.all_test()

        # drop databases, create same name db、stb and sma index
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        self.all_test()

        tdLog.printNoPrefix("==========step2:create table in rollup database")
        tdSql.execute("create database db3 retentions -:4m,2s:8m,3s:12m")
        tdSql.execute("use db3")
        tdSql.query(f"create stable stb1 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(first) watermark 5s max_delay 1m sma({INT_COL})")

        tdSql.execute("drop database if exists db1 ")
        tdSql.execute("drop database if exists db2 ")

        tdDnodes.stop(1)
        tdDnodes.start(1)


        tdLog.printNoPrefix("==========step3: sleep 20s for catalogUpdateTableIndex")
        tdSql.execute("create database db_s20")
        tdSql.execute("use db_s20")
        tdSql.execute(f"create stable stb1 (ts timestamp, c1 int) tags (t1 int) sma(c1);")
        tdSql.execute("alter stable stb1 add column tinyint_col tinyint")
        time.sleep(20)
        tdSql.query("select count(*) from stb1")
        tdSql.execute("drop database if exists db_s20 ")

        tdLog.printNoPrefix("==========step4:insert and flush in rollup database")
        tdSql.execute("create database db4 retentions -:4m,2s:8m,3s:12m")
        tdSql.execute("use db4")
        self.__create_tb(rollup="first")
        self.__insert_data(rollup="first")
        tdSql.execute(f'drop stable if exists {STBNAME}')
        tdSql.execute(f'flush database db4')


        tdLog.printNoPrefix("==========step5:after wal, all check again ")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        self.all_test()

        # drop databases, create same name db、stb and sma index
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
