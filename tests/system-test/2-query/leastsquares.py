import datetime

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

PRIMARY_COL = "ts"

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

UINT_COL     = "c11"
UBINT_COL    = "c12"
USINT_COL    = "c13"
UTINT_COL    = "c14"

NUM_COL     = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, UINT_COL, UBINT_COL, USINT_COL, UTINT_COL]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [ BOOL_COL, ]
TS_TYPE_COL = [ TS_COL, ]

ALL_COL = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BOOL_COL, BINARY_COL, NCHAR_COL, TS_COL, UINT_COL, UBINT_COL, USINT_COL, UTINT_COL ]

DBNAME = "db"
class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def __query_condition(self,tbname):
        query_condition = [f"{tbname}.{col}" for col in ALL_COL]
        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"abs( {tbname}.{num_col} )",
                    f"acos( {tbname}.{num_col} )",
                    f"asin( {tbname}.{num_col} )",
                    f"atan( {tbname}.{num_col} )",
                    f"avg( {tbname}.{num_col} )",
                    f"ceil( {tbname}.{num_col} )",
                    f"cos( {tbname}.{num_col} )",
                    f"count( {tbname}.{num_col} )",
                    f"floor( {tbname}.{num_col} )",
                    f"log( {tbname}.{num_col},  {tbname}.{num_col})",
                    f"max( {tbname}.{num_col} )",
                    f"min( {tbname}.{num_col} )",
                    f"pow( {tbname}.{num_col}, 2)",
                    f"round( {tbname}.{num_col} )",
                    f"sum( {tbname}.{num_col} )",
                    f"sin( {tbname}.{num_col} )",
                    f"sqrt( {tbname}.{num_col} )",
                    f"tan( {tbname}.{num_col} )",
                    f"cast( {tbname}.{num_col} as timestamp)",
                )
            )
            query_condition.extend((f"{num_col} + {any_col}" for any_col in ALL_COL))
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"sum(cast({tbname}.{char_col} as bigint ))",
                    f"max(cast({tbname}.{char_col} as bigint ))",
                    f"min(cast({tbname}.{char_col} as bigint ))",
                    f"avg(cast({tbname}.{char_col} as bigint ))",
                )
            )
        query_condition.extend(
            (
                1010,
                ''' "test1234!@#$%^&*():'><?/.,][}{" ''',
                "null"
            )
        )

        return query_condition

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False):
        table_reference = tb_list[0]
        join_condition = table_reference
        join = "inner join" if INNER else "join"
        for i in range(len(tb_list[1:])):
            join_condition += f" {join} {tb_list[i+1]} on {table_reference}.{filter}={tb_list[i+1]}.{filter}"

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
            elif query_conditon.startswith("avg"):
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
            elif col.startswith("avg"):
                col = col[4:-1]
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __single_sql(self, select_clause, from_clause, start_val=None, step_val=None, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"select leastsquares({select_clause}, {start_val}, {step_val}) from {from_clause} {where_condition} {group_condition}"

    @property
    def __tb_list(self, dbname=DBNAME):
        return [
            f"{dbname}.ct1",
            f"{dbname}.ct4",
            f"{dbname}.nt1",
            f"{dbname}.ct2",
            f"{dbname}.stb1",
        ]

    @property
    def start_step_val(self):
        return [
            1,
            0,
            1.25,
            -2.5,
            True,
            False,
            None,
            "",
            "str",
        ]

    def sql_list(self):
        current_sqls = []
        err_sqls = []
        __no_join_tblist = self.__tb_list
        for tb in __no_join_tblist:
            tbname = tb.split(".")[-1]
            select_claus_list = self.__query_condition(tbname)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition(col=select_claus)
                where_claus = self.__where_condition(query_conditon=select_claus)
                having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                for arg in self.start_step_val:
                    if not  isinstance(arg,int) or isinstance(arg, bool) :
                        err_sqls.extend(
                            (
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=arg),
                                self.__single_sql(select_clause=select_claus, from_clause=tb, step_val=arg, group_condition=group_claus),
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=arg, where_condition=where_claus, group_condition=having_claus),
                            )
                        )
                    elif isinstance(select_claus, str) and any([BOOL_COL in select_claus, BINARY_COL in select_claus, NCHAR_COL in select_claus, TS_COL in select_claus]):
                        err_sqls.extend(
                            (
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=arg),
                                self.__single_sql(select_clause=select_claus, from_clause=tb, step_val=arg, group_condition=group_claus),
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=arg, where_condition=where_claus, group_condition=having_claus),
                            )
                        )
                    else:
                        current_sqls.extend(
                            (
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=arg, step_val=0),
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=0, step_val=arg, group_condition=group_claus),
                                self.__single_sql(select_clause=select_claus, from_clause=tb, start_val=arg, step_val=arg, where_condition=where_claus, group_condition=having_claus),
                            )
                        )

        # return filter(None, sqls)
        return list(filter(None, current_sqls)), list(filter(None, err_sqls))

    def leastsquares_check(self):
        current_sqls, err_sqls = self.sql_list()
        for i in range(len(err_sqls)):
            tdSql.error(err_sqls[i])

        tdLog.printNoPrefix("===step 1: curent case, must return query OK")
        for i in range(len(current_sqls)):
            tdLog.info(f"sql: {current_sqls[i]}")
            tdSql.query(current_sqls[i])

    def check_result(self):
        for col in NUM_COL:
            tdSql.query("select leastsquares(%s, 1, 9) from %s.stb1" % (col, DBNAME))
            tdSql.checkRows(1)
            res = tdSql.getData(0, 0)
            if res is None:
                tdLog.exit("result is not correct")        

    def __test_current(self):
        # tdSql.query("explain select c1 from {dbname}.ct1")
        # tdSql.query("explain select 1 from {dbname}.ct2")
        # tdSql.query("explain select cast(ceil(c6) as bigint) from {dbname}.ct4 group by c6")
        # tdSql.query("explain select count(c3) from {dbname}.ct4 group by c7 having count(c3) > 0")
        # tdSql.query("explain select ct2.c3 from {dbname}.ct4 join ct2 on ct4.ts=ct2.ts")
        # tdSql.query("explain select c1 from stb1 where c1 is not null and c1 in (0, 1, 2) or c1 between 2 and 100 ")

        self.leastsquares_check()

    def __test_error(self, dbname=DBNAME):

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"select leastsquares(c1) from {dbname}.ct8" )
        tdSql.error( f"select leastsquares(c1, 1) from {dbname}.ct1 " )
        tdSql.error( f"select leastsquares(c1, null, 1) from {dbname}.ct1 " )
        tdSql.error( f"select leastsquares(c1, 1, null) from {dbname}.ct1 " )
        tdSql.error( f"select leastsquares(null, 1, 1) from {dbname}.ct1 " )
        tdSql.error( f'''select leastsquares(['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10'])
                    from {dbname}.ct1
                    where ['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10'] is not null
                    group by ['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10']
                    having ['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10'] is not null ''' )

    def all_test(self):
        self.__test_error()
        self.__test_current()
        self.check_result()

    def __create_tb(self, dbname=DBNAME):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp, {UINT_COL} int unsigned,
                 {UBINT_COL} bigint unsigned, {USINT_COL} smallint unsigned, {UTINT_COL} tinyint unsigned
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.nt1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp, {UINT_COL} int unsigned,
                 {UBINT_COL} bigint unsigned, {USINT_COL} smallint unsigned, {UTINT_COL}  tinyint unsigned
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

    def __insert_data(self, rows, dbname=DBNAME):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127} )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127} )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127} )"
            )
        tdSql.execute(
            f'''insert into {dbname}.ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 }, 0, 0, 0, 0)
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 }, 0, 0, 0, NULL )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000}, NULL, NULL, NULL, NULL
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000}, NULL, NULL, NULL, NULL
                )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }, NULL, NULL, NULL, NULL
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }, NULL, NULL, NULL, NULL
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into {dbname}.nt1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i }, NULL, NULL, NULL, NULL )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into {dbname}.nt1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }, NULL, NULL, NULL, NULL
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }, NULL, NULL, NULL, NULL
                )
            '''
        )


    def run(self):
        tdSql.prepare(DBNAME)

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdSql.execute(f"flush database {DBNAME}")

        tdSql.execute(f"use {DBNAME}")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
