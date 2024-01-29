from datetime import datetime
import time

from dataclasses import dataclass
from typing import List, Any, Tuple
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.constant import *
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

ALL_COL = [PRIMARY_COL, INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BINARY_COL, NCHAR_COL, BOOL_COL, TS_COL]
TAG_COL = [INT_TAG]

# insert data args：
TIME_STEP = 10000
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"


@dataclass
class SMAschema:
    creation            : str           = "CREATE"
    index_name          : str           = "sma_index_1"
    index_flag          : str           = "SMA INDEX"
    operator            : str           = "ON"
    tbname              : str           = None
    watermark           : str           = "5s"
    max_delay           : str           = "6m"
    func                : Tuple[str]    = None
    interval            : Tuple[str]    = ("6m", "10s")
    sliding             : str           = "6m"
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

                if k.lower() == "max_delay" and isinstance(v, str) and not self.max_delay:
                    self.max_delay = v
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


class TDTestCase:
    updatecfgDict = {"querySmaOptimize": 1}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.precision = "ms"
        self.sma_count = 0
        self.sma_created_index = []

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
            interval, offset = self.__get_interval_offset(sma.interval)
            if offset:
                sql += f" interval({interval}, {offset})"
            else:
                sql += f" interval({interval})"
        if sma.sliding:
            sql += f" sliding({sma.sliding})"
        if sma.watermark:
            sql += f" watermark {sma.watermark}"
        if sma.max_delay:
            sql += f" max_delay {sma.max_delay}"
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

    def __get_sma_func_col(self, func):
        cols = []
        if isinstance(func, str):
            cols.append( func.split("(")[-1].split(")")[0] )
        elif isinstance(func, tuple) or isinstance(func, list):
            for func_col in func:
                cols.append(func_col.split("(")[-1].split(")")[0])
        else:
            cols = []
        return cols

    def __check_sma_func(self, func:tuple):
        if not isinstance(func, str) and not isinstance(func, tuple) and not isinstance(func, list):
            return False
        if isinstance(func, str) :
            if "(" not in func or ")" not in func:
                return False
            if func.split("(")[0].upper() not in SMA_INDEX_FUNCTIONS:
                return False
            if func.split("(")[1].split(")")[0] not in ALL_COL and func.split("(")[1].split(")")[0] not in TAG_COL :
                return False
        if isinstance(func, tuple) or isinstance(func, list):
            for arg in func:
                if not isinstance(arg, str):
                    return False
                if "(" not in arg or ")" not in arg:
                    return False
                if arg.split("(")[0].upper() not in SMA_INDEX_FUNCTIONS:
                    return False
                if arg.split("(")[1].split(")")[0] not in ALL_COL and arg.split("(")[1].split(")")[0] not in TAG_COL :
                    return False
        return True

    def __check_sma_watermark(self, arg):
        if not arg:
            return False
        if not isinstance(arg, str):
            return False
        if arg[-1] not in SMA_WATMARK_MAXDELAY_INIT:
            return False
        if len(arg) == 1:
            return False
        if not arg[:-1].isdecimal():
            return False
        if tdSql.get_times(arg) > WATERMARK_MAX:
            return False
        if tdSql.get_times(arg) < WATERMARK_MIN:
            return False

        return True

    def __check_sma_max_delay(self, arg):
        if not self.__check_sma_watermark(arg):
            return False
        if tdSql.get_times(arg) < MAX_DELAY_MIN:
            return False

        return True

    def __check_sma_sliding(self, arg):
        if not isinstance(arg, str):
            return False
        if arg[-1] not in TAOS_TIME_INIT:
            return False
        if len(arg) == 1:
            return False
        if not arg[:-1].isdecimal():
            return False

        return True

    def __get_interval_offset(self, args):
        if isinstance(args, str):
            interval, offset = args, None
        elif isinstance(args,tuple) or isinstance(args, list):
            if len(args) == 1:
                interval, offset = args[0], None
            elif len(args) == 2:
                interval, offset = args
            else:
                interval, offset = False, False
        else:
            interval, offset = False, False

        return interval, offset

    def __check_sma_interval(self, args):
        if not isinstance(args, tuple) and not isinstance(args,str):
            return False
        interval, offset =  self.__get_interval_offset(args)
        if not interval:
            return False
        if not self.__check_sma_sliding(interval):
            return False
        if tdSql.get_times(interval) < INTERVAL_MIN:
            return False
        if offset:
            if not self.__check_sma_sliding(offset):
                return False
            if tdSql.get_times(interval) <= tdSql.get_times(offset) :
                return False

        return True

    def __sma_create_check(self, sma:SMAschema):
        if  self.updatecfgDict["querySmaOptimize"] == 0:
            return False
        tdSql.query("select database()")
        dbname =  tdSql.getData(0,0)
        tdSql.query("select * from information_schema.ins_databases")
        for index , value in enumerate(tdSql.cursor.description):
            if value[0] == "retentions":
                r_index = index
                break
        for row in tdSql.queryResult:
            if row[0] == dbname:
                if row[r_index] is None:
                    continue
                if ":" in row[r_index]:
                    sma.rollup_db = True
        if sma.rollup_db :
            return False
        tdSql.query("show stables")
        if not sma.tbname:
            return False
        stb_in_list = False
        for row in tdSql.queryResult:
            if sma.tbname == row[0]:
                stb_in_list = True
        if not stb_in_list:
            return False
        if not sma.creation or not isinstance(sma.creation, str) or sma.creation.upper() != "CREATE":
            return False
        if not sma.index_flag or not isinstance(sma.index_flag, str) or  sma.index_flag.upper() != "SMA INDEX" :
            return False
        if not sma.index_name or not isinstance(sma.index_name, str) or sma.index_name.upper() in TAOS_KEYWORDS:
            return False
        if not sma.operator or not isinstance(sma.operator, str) or sma.operator.upper() != "ON":
            return False

        if not sma.func or not self.__check_sma_func(sma.func):
            return False
        tdSql.query(f"desc {sma.tbname}")
        _col_list = []
        for col_row in  tdSql.queryResult:
            _col_list.append(col_row[0])
        _sma_func_cols = self.__get_sma_func_col(sma.func)
        for  _sma_func_col in _sma_func_cols:
            if _sma_func_col not in _col_list:
                return False

        if sma.sliding and not self.__check_sma_sliding(sma.sliding):
            return False
        interval, _ =  self.__get_interval_offset(sma.interval)
        if not sma.interval or not self.__check_sma_interval(sma.interval) :
            return False
        if sma.sliding and tdSql.get_times(interval) < tdSql.get_times(sma.sliding):
            return False
        if sma.watermark and not self.__check_sma_watermark(sma.watermark):
            return False
        if sma.max_delay and not self.__check_sma_max_delay(sma.max_delay):
            return False
        if sma.other:
            return False

        return True

    def sma_create_check(self, sma:SMAschema):
        if self.__sma_create_check(sma):
            tdSql.query(self.__create_sma_index(sma))
            self.sma_count += 1
            self.sma_created_index.append(sma.index_name)
            tdSql.query(self.__show_sma_index(sma))
            tdSql.checkRows(self.sma_count)
            tdSql.checkData(0, 2, sma.tbname)

        else:
            tdSql.error(self.__create_sma_index(sma))

    def __drop_sma_index(self, sma:SMAschema):
        sql = f"{sma.drop} {sma.drop_flag} {sma.index_name}"
        return sql

    def __sma_drop_check(self, sma:SMAschema):
        if not sma.drop:
            return False
        if not sma.drop_flag:
            return False
        if not sma.index_name:
            return False

        return True

    def sma_drop_check(self, sma:SMAschema):
        if self.__sma_drop_check(sma):
            tdSql.query(self.__drop_sma_index(sma))
            self.sma_count -= 1
            self.sma_created_index = list(filter(lambda x: x != sma.index_name, self.sma_created_index))
            tdSql.query("show streams")
            tdSql.checkRows(self.sma_count)
            time.sleep(1)
        else:
            tdSql.error(self.__drop_sma_index(sma))

    def __show_sma_index(self, sma:SMAschema):
        sql = f"{sma.show} {sma.show_msg} {sma.show_oper} {sma.tbname}"
        return sql

    def __sma_show_check(self, sma:SMAschema):
        if not sma.show:
            return False
        if not sma.show_msg:
            return False
        if not sma.show_oper:
            return False
        if not sma.tbname:
            return False

        return True

    def sma_show_check(self, sma:SMAschema):
        if self.__sma_show_check(sma):
            tdSql.query(self.__show_sma_index(sma))
            tdSql.checkRows(self.sma_count)
        else:
            tdSql.error(self.__show_sma_index(sma))

    @property
    def __create_sma_sql(self):
        err_sqls = []
        cur_sqls = []
        # err_set
        # # case 1: required fields check
        err_sqls.append( SMAschema(creation="", tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(index_name="",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(index_flag="",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(operator="",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(tbname="", func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(func=("",),tbname=STBNAME ) )
        err_sqls.append( SMAschema(interval=(""),tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )

        # # case 2: err fields
        err_sqls.append( SMAschema(creation="show",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(creation="alter",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(creation="select",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )

        err_sqls.append( SMAschema(index_flag="SMA INDEXES", tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(index_flag="SMA INDEX ,", tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        err_sqls.append( SMAschema(index_name="tbname", tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )

        # current_set

        cur_sqls.append( SMAschema(max_delay="",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        cur_sqls.append( SMAschema(watermark="",index_name="sma_index_2",tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )
        cur_sqls.append( SMAschema(sliding="",index_name='sma_index_3',tbname=STBNAME, func=(f"min({INT_COL})",f"max({INT_COL})") ) )

        return err_sqls, cur_sqls

    def test_create_sma(self):
        err_sqls , cur_sqls = self.__create_sma_sql
        for err_sql in err_sqls:
            self.sma_create_check(err_sql)
        for cur_sql in cur_sqls:
            self.sma_create_check(cur_sql)

    @property
    def __drop_sma_sql(self):
        err_sqls = []
        cur_sqls = []
        # err_set
        ## case 1: required fields check
        err_sqls.append( SMAschema(drop="") )
        err_sqls.append( SMAschema(drop_flag="") )
        err_sqls.append( SMAschema(index_name="") )

        for index in self.sma_created_index:
            cur_sqls.append(SMAschema(index_name=index))

        return err_sqls, cur_sqls

    def test_drop_sma(self):
        err_sqls , cur_sqls = self.__drop_sma_sql
        for err_sql in err_sqls:
            self.sma_drop_check(err_sql)
        for cur_sql in cur_sqls:
            self.sma_drop_check(cur_sql)

    def all_test(self):
        self.test_create_sma()
        self.test_drop_sma()

    def __create_tb(self, stb=STBNAME, ctb_num=20, ntbnum=1, dbname=DBNAME):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table {dbname}.{stb}(
                {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        tdSql.execute(create_stb_sql)

        for i in range(ntbnum):
            create_ntb_sql = f'''create table {dbname}.nt{i+1}(
                    {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
            tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __insert_data(self, rows, ctb_num=20, dbname=DBNAME, star_time=NOW):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        # from ...pytest.util.common import DataSet
        data = DataSet()
        data.get_order_set(rows, bint_step=2)

        for i in range(rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.utint_data[i]},
                {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
            '''
            tdSql.execute( f"insert into {dbname}.{NTBNAME} values ( {star_time - i * int(TIME_STEP * 1.2)}, {row_data} )" )

            for j in range(ctb_num):
                tdSql.execute( f"insert into {dbname}.ct{j+1} values ( {star_time - j * i * TIME_STEP}, {row_data} )" )

    def run(self):
        self.rows = 10

        tdLog.printNoPrefix("==========step0:all check")

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb(dbname=DBNAME)
        self.__insert_data(rows=self.rows)
        self.all_test()

        # # from ...pytest.util.sql import *

        # drop databases, create same name db、stb and sma index
        tdSql.prepare()
        self.__create_tb(dbname=DBNAME)
        self.__insert_data(rows=self.rows,star_time=NOW + self.rows * 2 * TIME_STEP)
        tdLog.printNoPrefix("==========step1.1 : create a tsma index and checkdata")
        tdSql.execute(f"create sma index {DBNAME}.sma_index_name1 on {DBNAME}.{STBNAME} function(max({INT_COL}),max({BINT_COL}),min({INT_COL})) interval(6m,10s) sliding(6m)")
        self.__insert_data(rows=self.rows)
        tdSql.query(f"select max({INT_COL}), max({BINT_COL}), min({INT_COL}) from {DBNAME}.{STBNAME} interval(6m,10s) sliding(6m)")
        tdSql.checkData(0, 0, self.rows - 1)
        tdSql.checkData(0, 1, (self.rows - 1) * 2 )
        tdSql.checkData(tdSql.queryRows - 1, 2, 0)
        # tdSql.checkData(0, 2, 0)

        tdLog.printNoPrefix("==========step1.2 : alter table schema, drop col without index")
        tdSql.execute(f"alter stable {DBNAME}.{STBNAME} drop column {BINARY_COL}")
        tdSql.query(f"select max({INT_COL}), max({BINT_COL}), min({INT_COL}) from {DBNAME}.{STBNAME} interval(6m,10s) sliding(6m)")
        tdSql.checkData(0, 0, self.rows - 1)
        tdSql.checkData(0, 1, (self.rows - 1) * 2 )
        tdSql.checkData(tdSql.queryRows - 1, 2, 0)

        tdLog.printNoPrefix("==========step1.3 : alter table schema, drop col with index")
        # TODO: TD-18047, can not drop col,  when col in tsma-index and tsma-index is not dropped.
        tdSql.error(f"alter stable {DBNAME}.stb1 drop column {BINT_COL}")

        tdLog.printNoPrefix("==========step1.4 : alter table schema, add col")
        tdSql.execute(f"alter stable {DBNAME}.{STBNAME} add column {BINT_COL}_1 bigint")
        tdSql.execute(f"insert into {DBNAME}.{CTBNAME} ({PRIMARY_COL}, {BINT_COL}_1) values(now(), 111)")
        tdSql.query(f"select max({INT_COL}), max({BINT_COL}), min({INT_COL}) from {DBNAME}.{STBNAME} interval(6m,10s) sliding(6m)")
        tdSql.checkData(0, 0, self.rows - 1)
        tdSql.checkData(0, 1, (self.rows - 1) * 2 )
        tdSql.checkData(tdSql.queryRows - 1, 2, 0)
        # tdSql.checkData(0, 2, 0)
        tdSql.query(f"select max({BINT_COL}_1) from {DBNAME}.{STBNAME} ")
        tdSql.checkData(0, 0 , 111)

        tdSql.execute(f"flush database {DBNAME}")
        
        tdLog.printNoPrefix("==========step1.5 : drop index")
        tdSql.execute(f"drop index {DBNAME}.sma_index_name1")

        tdLog.printNoPrefix("==========step1.6 : drop child table")
        tdSql.execute(f"drop table {CTBNAME}")
        tdSql.query(f"select max({INT_COL}), max({BINT_COL}), min({INT_COL}) from {DBNAME}.{STBNAME} interval(6m,10s) sliding(6m)")
        tdSql.checkData(0, 0, self.rows - 1)
        tdSql.checkData(0, 1, (self.rows - 1) * 2 )
        tdSql.checkData(tdSql.queryRows - 1, 2, 0)

        tdLog.printNoPrefix("==========step1.7 : drop stable")
        tdSql.execute(f"drop table {STBNAME}")
        tdSql.error(f"select * from {DBNAME}.{STBNAME}")

        self.all_test()

        tdLog.printNoPrefix("==========step2:create table in rollup database")
        tdSql.execute("create database db3 retentions -:4m,2s:8m,3s:12m")
        tdSql.execute("use db3")
        tdSql.execute(f"create stable stb1 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(first) watermark 5s max_delay 1m sma({INT_COL}) ")
        self.all_test()

        tdSql.execute("drop database if exists db1 ")
        tdSql.execute("drop database if exists db2 ")

        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        tdSql.execute("flush database db ")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

        # add for TS-2440
        for i in range(self.rows):
            tdSql.execute("drop database if exists db3 ")
            tdSql.execute("create database db3 retentions -:4m,2s:8m,3s:12m")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
