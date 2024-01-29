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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def __query_condition(self,tbname):
        query_condition = []
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"{tbname}.{char_col}",
                    # f"upper( {tbname}.{char_col} )",
                )
            )
            query_condition.extend( f"cast( {tbname}.{un_char_col} as binary(16) ) " for un_char_col in NUM_COL)
        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"sin( {tbname}.{num_col} )",
                )
            )
            query_condition.extend( f"{tbname}.{num_col} + {tbname}.{num_col_1} " for num_col_1 in NUM_COL )

        query_condition.append(''' "test1234!@#$%^&*():'><?/.,][}{" ''')

        return query_condition

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False, alias_tb1="tb1", alias_tb2="tb2"):
        table_reference = tb_list[0]
        join_condition = table_reference
        join = "inner join" if INNER else "join"
        for i in range(len(tb_list[1:])):
            join_condition += f" as {alias_tb1} {join} {tb_list[i+1]} as {alias_tb2} on {alias_tb1}.{filter}={alias_tb2}.{filter}"

        return join_condition

    def __where_condition(self, col=None, tbname=None, query_conditon=None):
        if query_conditon and isinstance(query_conditon, str):
            if query_conditon.startswith("count"):
                query_conditon = query_conditon[6:-1]
            elif query_conditon.startswith("max"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("sum"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("min"):
                query_conditon = query_conditon[4:-1]

        if query_conditon:
            return f" where {query_conditon} is not null"
        if col in NUM_COL:
            return f" where abs( {tbname}.{col} ) >= 0"
        if col in CHAR_COL:
            return f" where lower( {tbname}.{col} ) like 'bina%' or lower( {tbname}.{col} ) like '_cha%' "
        if col in BOOLEAN_COL:
            return f" where {tbname}.{col} in (false, true)  "
        if col in TS_TYPE_COL or col in PRIMARY_COL:
            return f" where cast( {tbname}.{col} as binary(16) ) is not null "

        return ""

    def __group_condition(self, col, having = None):
        if isinstance(col, str):
            if col.startswith("count"):
                col = col[6:-1]
            elif col.startswith("max"):
                col = col[4:-1]
            elif col.startswith("sum"):
                col = col[4:-1]
            elif col.startswith("min"):
                col = col[4:-1]
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __gen_sql(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0] != from_clause.split(".")[0]:
            return
        return f"select {select_clause} from {from_clause} {where_condition} {group_condition}"

    @property
    def __join_tblist(self, dbname=DBNAME):
        return [
            # ["ct1", "ct2"],
            [f"{dbname}.ct1", f"{dbname}.ct4"],
            [f"{dbname}.ct1", f"{dbname}.nt1"],
            # ["ct2", "ct4"],
            # ["ct2", "nt1"],
            # ["ct4", "nt1"],
            # ["ct1", "ct2", "ct4"],
            # ["ct1", "ct2", "nt1"],
            # ["ct1", "ct4", "nt1"],
            # ["ct2", "ct4", "nt1"],
            # ["ct1", "ct2", "ct4", "nt1"],
        ]

    @property
    def __sqls_list(self):
        sqls = []
        __join_tblist = self.__join_tblist
        for join_tblist in __join_tblist:
            alias_tb = "tb1"
            # for join_tb in join_tblist:
            select_claus_list = self.__query_condition(alias_tb)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition( col=select_claus)
                where_claus = self.__where_condition( query_conditon=select_claus )
                having_claus = self.__group_condition( col=select_claus, having=f"{select_claus} is not null" )
                sqls.extend(
                    (
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus, group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus, having_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), group_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), having_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb)),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), where_claus, group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), where_claus, having_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), where_claus, ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), having_claus ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), group_claus ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb) ),
                    )
                )
        return list(filter(None, sqls))

    def __join_check(self,):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        for i in range(len(self.__sqls_list)):
            tdSql.query(self.__sqls_list[i])
            # if i % 10 == 0 :
            #     tdLog.success(f"{i} sql is already executed success !")

    def __join_check_old(self, tblist, checkrows, join_flag=True):
        query_conditions = self.__query_condition(tblist[0])
        join_condition = self.__join_condition(tb_list=tblist) if join_flag else " "
        for condition in query_conditions:
            where_condition =  self.__where_condition(col=condition, tbname=tblist[0])
            group_having = self.__group_condition(col=condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition(col=condition )
            groups = ["", group_having, group_no_having]
            for group_condition in groups:
                if where_condition:
                    sql = f" select {condition} from {tblist[0]},{tblist[1]} where {join_condition} and {where_condition} {group_condition} "
                else:
                    sql = f" select {condition} from {tblist[0]},{tblist[1]} where {join_condition}  {group_condition} "

                if not join_flag :
                    tdSql.error(sql=sql)
                    break
                if len(tblist) == 2:
                    if "ct1" in tblist or "nt1" in tblist:
                        self.__join_current(sql, checkrows)
                    elif where_condition or "not null" in group_condition:
                        self.__join_current(sql, checkrows + 2 )
                    elif group_condition:
                        self.__join_current(sql, checkrows + 3 )
                    else:
                        self.__join_current(sql, checkrows + 5 )
                if len(tblist) > 2 or len(tblist) < 1:
                    tdSql.error(sql=sql)

    def __join_current(self, sql, checkrows):
        tdSql.query(sql=sql)
        # tdSql.checkRows(checkrows)

    def __test_error(self, dbname=DBNAME):
        # sourcery skip: extract-duplicate-method, move-assign-in-block
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        err_list_1 = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4"]
        err_list_2 = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.nt1"]
        err_list_3 = [f"{dbname}.ct1", f"{dbname}.ct4", f"{dbname}.nt1"]
        err_list_4 = [f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1"]
        err_list_5 = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1"]
        self.__join_check_old(err_list_1, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_1} over==========")
        self.__join_check_old(err_list_2, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_2} over==========")
        self.__join_check_old(err_list_3, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_3} over==========")
        self.__join_check_old(err_list_4, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_4} over==========")
        self.__join_check_old(err_list_5, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_5} over==========")
        self.__join_check_old(["ct2", "ct4"], -1, join_flag=False)
        tdLog.printNoPrefix("==========err sql condition check in has no join condition over==========")

        tdSql.error( f"select c1, c2 from {dbname}.ct2, {dbname}.ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{INT_COL}=ct4.{INT_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{TS_COL}=ct4.{TS_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{TS_COL}" )
        tdSql.error( f"select ct2.c1, ct1.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL}" )
        tdSql.error( f"select ct2.c1, ct4.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL} and c1 is not null " )
        tdSql.error( f"select ct2.c1, ct4.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL} and ct1.c1 is not null " )


        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1"]

        # for tb in tbname:
        #     for errsql in self.__join_err_check(tb):
        #         tdSql.error(sql=errsql)
        #     tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test(self):
        self.__join_check()
        self.__test_error()


    def __create_tb(self, stb="stb1", ctb_num=20, ntbnum=1, dbname=DBNAME):
        create_stb_sql = f'''create table {dbname}.{stb}(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        for i in range(ntbnum):

            create_ntb_sql = f'''create table {dbname}.nt{i+1}(
                    ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.{stb} tags ( {i+1} )')

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

    def __insert_data(self, dbname=DBNAME):
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

            tdSql.execute( f"insert into {dbname}.ct1 values ( {NOW - i * TIME_STEP}, {row_data} )" )
            tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )" )
            tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )" )
            tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )" )

        tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW + int(TIME_STEP * 0.6)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.6)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW - self.rows * int(TIME_STEP * 0.29) }, {null_data} )" )

        tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW + int(TIME_STEP * 0.8)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.8)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW - self.rows * int(TIME_STEP * 0.39)}, {null_data} )" )

        tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )" )


    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb(dbname=DBNAME)

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(dbname=DBNAME)

        tdLog.printNoPrefix("==========step3:all check")
        tdSql.query(f"select count(*) from {DBNAME}.ct1")
        tdSql.checkData(0, 0, self.rows)
        self.all_test()

        tdLog.printNoPrefix("==========step4:cross db check")
        dbname1 = "db1"
        tdSql.execute(f"create database {dbname1} duration 432000m")
        tdSql.execute(f"use {dbname1}")
        self.__create_tb(dbname=dbname1)
        self.__insert_data(dbname=dbname1)

        tdSql.query("select ct1.c_int from db.ct1 as ct1 join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.stb1 as ct1 join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows + int(self.rows * 0.6 //3)+ int(self.rows * 0.8 // 4))
        tdSql.query("select ct1.c_int from db.nt1 as ct1 join db1.nt1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows + 3)
        tdSql.query("select ct1.c_int from db.stb1 as ct1 join db1.stb1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(50)

        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        tdSql.query("select count(*) from db1.ct1")
        tdSql.checkData(0, 0, self.rows)

        self.all_test()
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        tdSql.query("select count(*) from db1.ct1")
        tdSql.checkData(0, 0, self.rows)

        tdSql.execute(f"flush database {DBNAME}")
        tdSql.execute(f"flush database {dbname1}")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        tdSql.execute("use db")
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        tdSql.query("select count(*) from db1.ct1")
        tdSql.checkData(0, 0, self.rows)

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
