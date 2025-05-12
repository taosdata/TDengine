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

NUM_COL     = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [ BOOL_COL, ]
TS_TYPE_COL = [ TS_COL, ]

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
    def __join_tblist(self):
        return [
            # ["ct1", "ct2"],
            # ["ct1", "ct4"],
            # ["ct1", "t1"],
            ["ct2", "ct4"],
            # ["ct2", "t1"],
            ["ct4", "t1"],
            # ["ct1", "ct2", "ct4"],
            # ["ct1", "ct2", "t1"],
            # ["ct1", "ct4", "t1"],
            # ["ct2", "ct4", "t1"],
            # ["ct1", "ct2", "ct4", "t1"],
        ]

    @property
    def __sqls_list(self):
        sqls = []
        __join_tblist = self.__join_tblist
        for join_tblist in __join_tblist:
            alias_tb = "tb1"
            select_claus_list = self.__query_condition(alias_tb)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition( col=select_claus)
                where_claus = self.__where_condition( query_conditon=select_claus )
                having_claus = self.__group_condition( col=select_claus, having=f"{select_claus} is not null" )
                sqls.extend(
                    (
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist), where_claus, group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus, having_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist), group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), having_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb)),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True), where_claus, group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb, INNER=True), where_claus, having_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb, INNER=True), where_claus, ),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb, INNER=True), having_claus ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True), group_claus ),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb, INNER=True) ),
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
                    if "ct1" in tblist or "t1" in tblist:
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

    def __test_error(self):
        # sourcery skip: extract-duplicate-method, move-assign-in-block
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        err_list_1 = ["ct1","ct2", "ct4"]
        err_list_2 = ["ct1","ct2", "t1"]
        err_list_3 = ["ct1","ct4", "t1"]
        err_list_4 = ["ct2","ct4", "t1"]
        err_list_5 = ["ct1", "ct2","ct4", "t1"]
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

        tdSql.error( f"select c1, c2 from ct2, ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from ct2, ct4 where ct2.{INT_COL}=ct4.{INT_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from ct2, ct4 where ct2.{TS_COL}=ct4.{TS_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from ct2, ct4 where ct2.{PRIMARY_COL}=ct4.{TS_COL}" )
        tdSql.error( f"select ct2.c1, ct1.c2 from ct2, ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL}" )
        tdSql.error( f"select ct2.c1, ct4.c2 from ct2, ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL} and c1 is not null " )
        tdSql.error( f"select ct2.c1, ct4.c2 from ct2, ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL} and ct1.c1 is not null " )


        tbname = ["ct1", "ct2", "ct4", "t1"]

        # for tb in tbname:
        #     for errsql in self.__join_err_check(tb):
        #         tdSql.error(sql=errsql)
        #     tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test(self):
        self.__join_check()
        self.__test_error()


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
            { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}

    def __insert_data(self, rows):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000}
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000}
                )
            '''
        )

        tdSql.execute(
            f'''insert into ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into t1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )


    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdSql.execute(f"flush database db")


        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
