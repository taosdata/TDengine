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
        tdSql.init(conn.cursor())

    def __query_condition(self,tbname):
        query_condition = []
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"{tbname}.{char_col}",
                    f"upper( {tbname}.{char_col} )",
                    f"char_length( {tbname}.{char_col} )",
                    f"concat( {tbname}.{char_col}, {tbname}.{char_col} )",
                )
            )

        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"{tbname}.{num_col}",
                    f"ceil( {tbname}.{num_col} )",
                    f"abs( {tbname}.{num_col} )",
                    f"acos( {tbname}.{num_col} )",
                    f"max( {tbname}.{num_col} )",
                )
            )

        query_condition.extend(
            (
                # 1010,
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

    def __single_sql(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0] != from_clause.split(".")[0]:
            return
        return f"select {select_clause} from {from_clause} {where_condition} {group_condition}"


    @property
    def __join_tblist(self):
        return [
            ["ct1", "ct2"],
            ["ct1", "ct4"],
            ["ct1", "t1"],
            ["ct2", "ct4"],
            ["ct2", "t1"],
            ["ct4", "t1"],
            # ["ct1", "ct2", "ct4"],
            # ["ct1", "ct2", "t1"],
            # ["ct1", "ct4", "t1"],
            # ["ct2", "ct4", "t1"],
            # ["ct1", "ct2", "ct4", "t1"],
        ]

    @property
    def __tb_liast(self):
        return [
            "ct1",
            "ct2",
            "ct4",
            "t1",
        ]

    def sql_list(self):
        sqls = []
        __join_tblist = self.__join_tblist
        for join_tblist in __join_tblist:
            for join_tb in join_tblist:
                select_claus_list = self.__query_condition(join_tb)
                for select_claus in select_claus_list:
                    group_claus = self.__group_condition( col=select_claus)
                    where_claus = self.__where_condition(query_conditon=select_claus)
                    having_claus = self.__group_condition( col=select_claus, having=f"{select_claus} is not null")
                    sqls.extend(
                        (
                            self.__single_sql(select_claus, join_tb, where_claus, group_claus),
                            self.__single_sql(select_claus, join_tb, where_claus, having_claus),
                            self.__single_sql(select_claus, self.__join_condition(join_tblist), where_claus, having_claus),
                            self.__single_sql(select_claus, self.__join_condition(join_tblist, INNER=True), where_claus, having_claus),
                            self.__single_sql(select_claus, join_tb, where_claus),
                            self.__single_sql(select_claus, join_tb, having_claus),
                            self.__single_sql(select_claus, join_tb, group_claus),
                            self.__single_sql(select_claus, join_tb),

                        )
                    )
        __no_join_tblist = self.__tb_liast
        for tb in __no_join_tblist:
                select_claus_list = self.__query_condition(tb)
                for select_claus in select_claus_list:
                    group_claus = self.__group_condition(col=select_claus)
                    where_claus = self.__where_condition(query_conditon=select_claus)
                    having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                    sqls.extend(
                        (
                            self.__single_sql(select_claus, join_tb, where_claus, group_claus),
                            self.__single_sql(select_claus, join_tb, where_claus, having_claus),
                            self.__single_sql(select_claus, join_tb, where_claus),
                            self.__single_sql(select_claus, join_tb, group_claus),
                            self.__single_sql(select_claus, join_tb, having_claus),
                            self.__single_sql(select_claus, join_tb),
                        )
                    )

        # return filter(None, sqls)
        return list(filter(None, sqls))

    def __get_type(self, col):
        if tdSql.cursor.istype(col, "BOOL"):
            return "BOOL"
        if tdSql.cursor.istype(col, "INT"):
            return "INT"
        if tdSql.cursor.istype(col, "BIGINT"):
            return "BIGINT"
        if tdSql.cursor.istype(col, "TINYINT"):
            return "TINYINT"
        if tdSql.cursor.istype(col, "SMALLINT"):
            return "SMALLINT"
        if tdSql.cursor.istype(col, "FLOAT"):
            return "FLOAT"
        if tdSql.cursor.istype(col, "DOUBLE"):
            return "DOUBLE"
        if tdSql.cursor.istype(col, "BINARY"):
            return "BINARY"
        if tdSql.cursor.istype(col, "NCHAR"):
            return "NCHAR"
        if tdSql.cursor.istype(col, "TIMESTAMP"):
            return "TIMESTAMP"
        if tdSql.cursor.istype(col, "JSON"):
            return "JSON"
        if tdSql.cursor.istype(col, "TINYINT UNSIGNED"):
            return "TINYINT UNSIGNED"
        if tdSql.cursor.istype(col, "SMALLINT UNSIGNED"):
            return "SMALLINT UNSIGNED"
        if tdSql.cursor.istype(col, "INT UNSIGNED"):
            return "INT UNSIGNED"
        if tdSql.cursor.istype(col, "BIGINT UNSIGNED"):
            return "BIGINT UNSIGNED"

    def union_check(self):
        sqls = self.sql_list()
        for i in range(len(sqls)):
            tdSql.query(sqls[i])
            res1_type = self.__get_type(0)
            for j in range(len(sqls[i:])):
                tdSql.query(sqls[j+i])
                order_union_type = False
                rev_order_type = False
                all_union_type = False
                res2_type =  self.__get_type(0)

                if res2_type == res1_type:
                    all_union_type = True
                elif res1_type in ( "BIGINT" , "NCHAR" ) and res2_type in ("BIGINT" , "NCHAR"):
                    all_union_type = True
                elif res1_type in ("BIGINT", "NCHAR"):
                    order_union_type = True
                elif res2_type in ("BIGINT", "NCHAR"):
                    rev_order_type = True
                elif res1_type == "TIMESAMP" and res2_type not in ("BINARY", "NCHAR"):
                    order_union_type = True
                elif res2_type == "TIMESAMP" and res1_type not in ("BINARY", "NCHAR"):
                    rev_order_type = True
                elif res1_type == "BINARY" and res2_type != "NCHAR":
                    order_union_type = True
                elif res2_type == "BINARY" and res1_type != "NCHAR":
                    rev_order_type = True

                if all_union_type:
                    tdSql.query(f"{sqls[i]} union {sqls[j+i]}")
                    tdSql.query(f"{sqls[j+i]} union {sqls[i]}")
                    tdSql.checkCols(1)
                    tdSql.query(f"{sqls[i]} union all {sqls[j+i]}")
                    tdSql.query(f"{sqls[j+i]} union all {sqls[i]}")
                    tdSql.checkCols(1)
                elif order_union_type:
                    tdSql.query(f"{sqls[i]} union {sqls[j+i]}")
                    tdSql.checkCols(1)
                    tdSql.query(f"{sqls[i]} union all {sqls[j+i]}")
                    tdSql.checkCols(1)
                elif rev_order_type:
                    tdSql.query(f"{sqls[j+i]} union {sqls[i]}")
                    tdSql.checkCols(1)
                    tdSql.query(f"{sqls[j+i]} union all {sqls[i]}")
                    tdSql.checkCols(1)
                else:
                    tdSql.error(f"{sqls[i]} union {sqls[j+i]}")

    def __test_error(self):

        tdSql.error( "show tables union show tables" )
        tdSql.error( "create table errtb1 union all create table errtb2" )
        tdSql.error( "drop table ct1 union all drop table ct3" )
        tdSql.error( "select c1 from ct1 union all drop table ct3" )
        tdSql.error( "select c1 from ct1 union all '' " )
        tdSql.error( " '' union all select c1 from ct1 " )
        tdSql.error( "select c1 from ct1 union select c1 from ct2 union select c1 from ct4 ")

    def all_test(self):
        self.__test_error()
        self.union_check()


    def __create_tb(self):

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

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
