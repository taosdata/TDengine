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
                    f"count( {tbname}.{char_col} )",
                    f"cast( {tbname}.{char_col} as nchar(3) )",
                )
            )

        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"log( {tbname}.{num_col},  {tbname}.{num_col})",
                )
            )

        query_condition.extend(
            (
                ''' "test12" ''',
                # 1010,
            )
        )

        return query_condition

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False):
        table_reference = tb_list[0]
        join_condition = f'{table_reference} {table_reference.split(".")[-1]}'
        join = "inner join" if INNER else "join"
        for i in range(len(tb_list[1:])):
            join_condition += f" {join} {tb_list[i+1]} {tb_list[i+1].split('.')[-1]} on {table_reference.split('.')[-1]}.{filter}={tb_list[i+1].split('.')[-1]}.{filter}"

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
    def __join_tblist(self, dbname="db"):
        return [
            [f"{dbname}.ct1", f"{dbname}.t1"],
            [f"{dbname}.ct4", f"{dbname}.t1"],
            # ["ct1", "ct2", "ct4"],
            # ["ct1", "ct2", "t1"],
            # ["ct1", "ct4", "t1"],
            # ["ct2", "ct4", "t1"],
            # ["ct1", "ct2", "ct4", "t1"],
        ]

    @property
    def __tb_list(self, dbname="db"):
        return [
            f"{dbname}.ct1",
            f"{dbname}.ct4",
        ]

    def sql_list(self):
        sqls = []
        __join_tblist = self.__join_tblist
        for join_tblist in __join_tblist:
            for join_tb in join_tblist:
                join_tb_name = join_tb.split(".")[-1]
                select_claus_list = self.__query_condition(join_tb_name)
                for select_claus in select_claus_list:
                    group_claus = self.__group_condition( col=select_claus)
                    where_claus = self.__where_condition(query_conditon=select_claus)
                    having_claus = self.__group_condition( col=select_claus, having=f"{select_claus} is not null")
                    sqls.extend(
                        (
                            self.__single_sql(select_claus, self.__join_condition(join_tblist, INNER=True), where_claus, having_claus),
                        )
                    )
        __no_join_tblist = self.__tb_list
        for tb in __no_join_tblist:
                tb_name = join_tb.split(".")[-1]
                select_claus_list = self.__query_condition(tb_name)
                for select_claus in select_claus_list:
                    group_claus = self.__group_condition(col=select_claus)
                    where_claus = self.__where_condition(query_conditon=select_claus)
                    having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                    sqls.extend(
                        (
                            self.__single_sql(select_claus, tb, where_claus, having_claus),
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

    def union_check(self, dbname = "db"):
        sqls = self.sql_list()
        for i in range(len(sqls)):
            tdSql.query(sqls[i])
            res1_type = self.__get_type(0)
            # if i % 5 == 0:
            #         tdLog.success(f"{i} : sql is already executing!")
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
                    tdSql.execute(f"{sqls[i]} union {sqls[j+i]}")
                    tdSql.execute(f"{sqls[j+i]} union all {sqls[i]}")
                elif order_union_type:
                    tdSql.execute(f"{sqls[i]} union all {sqls[j+i]}")
                elif rev_order_type:
                    tdSql.execute(f"{sqls[j+i]} union {sqls[i]}")
                else:
                    tdSql.error(f"{sqls[i]} union {sqls[j+i]}")

        # check union with timeline function
        tdSql.query(f"select first(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1 order by ts)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)
        tdSql.query(f"select last(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1 order by ts desc)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2147450880)
        tdSql.query(f"select irate(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1 order by ts)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9.102222222222222)
        tdSql.query(f"select elapsed(ts) from (select * from {dbname}.t1 union select * from {dbname}.t1 order by ts)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 46800000.000000000000000)
        tdSql.query(f"select diff(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1 order by ts)")
        tdSql.checkRows(14)
        tdSql.query(f"select derivative(c1, 1s, 0) from (select * from {dbname}.t1 union select * from {dbname}.t1 order by ts)")
        tdSql.checkRows(11)
        tdSql.query(f"select count(*) from {dbname}.t1 as a join {dbname}.t1 as b on a.ts = b.ts and a.ts is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select first(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1)")
        tdSql.query(f"select last(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1)")
        tdSql.error(f"select irate(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1)")
        tdSql.error(f"select elapsed(ts) from (select * from {dbname}.t1 union select * from {dbname}.t1)")
        tdSql.error(f"select diff(c1) from (select * from {dbname}.t1 union select * from {dbname}.t1)")
        tdSql.error(f"select derivative(c1, 1s, 0) from (select * from {dbname}.t1 union select * from {dbname}.t1)")


    def __test_error(self, dbname="db"):

        tdSql.error( f"show {dbname}.tables union show {dbname}.tables" )
        tdSql.error( f"create table {dbname}.errtb1 union all create table {dbname}.errtb2" )
        tdSql.error( f"drop table {dbname}.ct1 union all drop table {dbname}.ct3" )
        tdSql.error( f"select c1 from {dbname}.ct1 union all drop table {dbname}.ct3" )
        tdSql.error( f"select c1 from {dbname}.ct1 union all '' " )
        tdSql.error( f" '' union all select c1 from{dbname}. ct1 " )

    def all_test(self):
        self.__test_error()
        self.union_check()

    def __create_tb(self, dbname="db"):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (tag1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

    def __insert_data(self, rows, dbname="db"):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into {dbname}.ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct4 values
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
            f'''insert into {dbname}.ct2 values
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
            insert_data = f'''insert into {dbname}.t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into {dbname}.t1 values
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

    def test_TS_5630(self):
        sql = "CREATE DATABASE `ep_iot` BUFFER 256 CACHESIZE 20 CACHEMODEL 'both' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 3 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0"
        tdSql.execute(sql, queryTimes=1)
        tdLog.info("database ep_iot created")
        sql = "CREATE STABLE `ep_iot`.`sldc_dp` (`ts` TIMESTAMP, `data_write_time` TIMESTAMP, `jz1fdgl` DOUBLE, `jz1ssfdfh` DOUBLE, `jz1fdmh` DOUBLE, `jz1gdmh` DOUBLE, `jz1qjrhl` DOUBLE, `jz1zhcydl` DOUBLE, `jz1zkby` DOUBLE, `jz1zzqyl` DOUBLE, `jz1zzqwda` DOUBLE, `jz1zzqwdb` DOUBLE, `jz1zzqll` DOUBLE, `jz1gswd` DOUBLE, `jz1gsll` DOUBLE, `jz1glxl` DOUBLE, `jz1qjrh` DOUBLE, `jz1zhrxl` DOUBLE, `jz1gmjassllfk` DOUBLE, `jz1gmjasslllj` DOUBLE, `jz1gmjbssllfk` DOUBLE, `jz1gmjbsslllj` DOUBLE, `jz1gmjcssllfk` DOUBLE, `jz1gmjcsslllj` DOUBLE, `jz1gmjdssllfk` DOUBLE, `jz1gmjdsslllj` DOUBLE, `jz1gmjessllfk` DOUBLE, `jz1gmjesslllj` DOUBLE, `jz1gmjfssllfk` DOUBLE, `jz1gmjfsslllj` DOUBLE, `jz1zrqwda` DOUBLE, `jz1zrqwdb` DOUBLE, `jz1zrzqyl` DOUBLE, `jz1mmjadl` DOUBLE, `jz1mmjbdl` DOUBLE, `jz1mmjcdl` DOUBLE, `jz1mmjddl` DOUBLE, `jz1mmjedl` DOUBLE, `jz1mmjfdl` DOUBLE, `jz1cyqckwda` DOUBLE, `jz1cyqckwdb` DOUBLE, `jz1njswd` DOUBLE, `jz1nqqxhsckawd` DOUBLE, `jz1nqqxhsckbwd` DOUBLE, `jz1nqqxhsrkawd` DOUBLE, `jz1nqqxhsrkbwd` DOUBLE, `jz1kyqackyqwdsel` DOUBLE, `jz1kyqbckyqwdsel` DOUBLE, `jz1yfjackyqwd` DOUBLE, `jz1yfjbckyqwd` DOUBLE, `jz1trkyqwd` DOUBLE, `jz1trkyqwd1` DOUBLE, `jz1trkyqwd2` DOUBLE, `jz1trkyqwd3` DOUBLE, `jz1tckjyqwd1` DOUBLE, `jz1tckjyqwd2` DOUBLE, `jz1tckyqwd1` DOUBLE, `jz1bya` DOUBLE, `jz1byb` DOUBLE, `jz1pqwda` DOUBLE, `jz1pqwdb` DOUBLE, `jz1gmjadl` DOUBLE, `jz1gmjbdl` DOUBLE, `jz1gmjcdl` DOUBLE, `jz1gmjddl` DOUBLE, `jz1gmjedl` DOUBLE, `jz1gmjfdl` DOUBLE, `jz1yfjadl` DOUBLE, `jz1yfjbdl` DOUBLE, `jz1ycfjadl` DOUBLE, `jz1ycfjbdl` DOUBLE, `jz1sfjadl` DOUBLE, `jz1sfjbdl` DOUBLE, `jz1fdjyggl` DOUBLE, `jz1fdjwggl` DOUBLE, `jz1sjzs` DOUBLE, `jz1zfl` DOUBLE, `jz1ltyl` DOUBLE, `jz1smb` DOUBLE, `jz1rll` DOUBLE, `jz1grd` DOUBLE, `jz1zjwd` DOUBLE, `jz1yl` DOUBLE, `jz1kyqckwd` DOUBLE, `jz1abmfsybrkcy` DOUBLE, `jz1bbmfsybrkcy` DOUBLE, `jz1abjcsdmfytwdzdz` DOUBLE, `jz1bbjcsdmfytwdzdz` DOUBLE, `jz2fdgl` DOUBLE, `jz2ssfdfh` DOUBLE, `jz2fdmh` DOUBLE, `jz2gdmh` DOUBLE, `jz2qjrhl` DOUBLE, `jz2zhcydl` DOUBLE, `jz2zkby` DOUBLE, `jz2zzqyl` DOUBLE, `jz2zzqwda` DOUBLE, `jz2zzqwdb` DOUBLE, `jz2zzqll` DOUBLE, `jz2gswd` DOUBLE, `jz2gsll` DOUBLE, `jz2glxl` DOUBLE, `jz2qjrh` DOUBLE, `jz2zhrxl` DOUBLE, `jz2gmjassllfk` DOUBLE, `jz2gmjasslllj` DOUBLE, `jz2gmjbssllfk` DOUBLE, `jz2gmjbsslllj` DOUBLE, `jz2gmjcssllfk` DOUBLE, `jz2gmjcsslllj` DOUBLE, `jz2gmjdssllfk` DOUBLE, `jz2gmjdsslllj` DOUBLE, `jz2gmjessllfk` DOUBLE, `jz2gmjesslllj` DOUBLE, `jz2gmjfssllfk` DOUBLE, `jz2gmjfsslllj` DOUBLE, `jz2zrqwda` DOUBLE, `jz2zrqwdb` DOUBLE, `jz2zrzqyl` DOUBLE, `jz2mmjadl` DOUBLE, `jz2mmjbdl` DOUBLE, `jz2mmjcdl` DOUBLE, `jz2mmjddl` DOUBLE, `jz2mmjedl` DOUBLE, `jz2mmjfdl` DOUBLE, `jz2cyqckwda` DOUBLE, `jz2cyqckwdb` DOUBLE, `jz2njswd` DOUBLE, `jz2nqqxhsckawd` DOUBLE, `jz2nqqxhsckbwd` DOUBLE, `jz2nqqxhsrkawd` DOUBLE, `jz2nqqxhsrkbwd` DOUBLE, `jz2kyqackyqwdsel` DOUBLE, `jz2kyqbckyqwdsel` DOUBLE, `jz2yfjackyqwd` DOUBLE, `jz2yfjbckyqwd` DOUBLE, `jz2trkyqwd` DOUBLE, `jz2trkyqwd1` DOUBLE, `jz2trkyqwd2` DOUBLE, `jz2trkyqwd3` DOUBLE, `jz2tckjyqwd1` DOUBLE, `jz2tckjyqwd2` DOUBLE, `jz2tckyqwd1` DOUBLE, `jz2bya` DOUBLE, `jz2byb` DOUBLE, `jz2pqwda` DOUBLE, `jz2pqwdb` DOUBLE, `jz2gmjadl` DOUBLE, `jz2gmjbdl` DOUBLE, `jz2gmjcdl` DOUBLE, `jz2gmjddl` DOUBLE, `jz2gmjedl` DOUBLE, `jz2gmjfdl` DOUBLE, `jz2yfjadl` DOUBLE, `jz2yfjbdl` DOUBLE, `jz2ycfjadl` DOUBLE, `jz2ycfjbdl` DOUBLE, `jz2sfjadl` DOUBLE, `jz2sfjbdl` DOUBLE, `jz2fdjyggl` DOUBLE, `jz2fdjwggl` DOUBLE, `jz2sjzs` DOUBLE, `jz2zfl` DOUBLE, `jz2ltyl` DOUBLE, `jz2smb` DOUBLE, `jz2rll` DOUBLE, `jz2grd` DOUBLE, `jz2zjwd` DOUBLE, `jz2yl` DOUBLE, `jz2kyqckwd` DOUBLE, `jz2abmfsybrkcy` DOUBLE, `jz2bbmfsybrkcy` DOUBLE, `jz2abjcsdmfytwdzdz` DOUBLE, `jz2bbjcsdmfytwdzdz` DOUBLE) TAGS (`iot_hub_id` VARCHAR(100), `device_group_code` VARCHAR(100), `device_code` VARCHAR(100))"
        tdLog.info("stable ep_iot.sldc_dp created")
        tdSql.execute(sql, queryTimes=1)
        sql = "insert into ep_iot.sldc_dp_t1 using ep_iot.sldc_dp tags('a','a','a') values(now, now, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9,0,1);"
        tdSql.execute(sql, queryTimes=1)
        sql = "insert into ep_iot.sldc_dp_t1 using ep_iot.sldc_dp tags('b','b','b') values(now, now, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9,0,1);"
        tdSql.execute(sql, queryTimes=1)
        sql = "insert into ep_iot.sldc_dp_t1 using ep_iot.sldc_dp tags('c','c','c') values(now, now, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9,0,1);"
        tdSql.execute(sql, queryTimes=1)
        sql = "insert into ep_iot.sldc_dp_t1 using ep_iot.sldc_dp tags('d','d','d') values(now, now, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9,0,1);"
        tdSql.execute(sql, queryTimes=1)
        sql = "insert into ep_iot.sldc_dp_t1 using ep_iot.sldc_dp tags('e','e','e') values(now, now, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9, 0,1,2,3,4,5,6,7,8,9,0,1);"
        tdSql.execute(sql, queryTimes=1)
        sql = "select scdw_code, scdw_name, jzmc, fdgl, jzzt from ((select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组1' as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组2' as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp)) where scdw_code like '%%';"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkCols(5)
        tdSql.checkRows(6)

        sql = "select scdw_name, scdw_code, jzmc, fdgl, jzzt from ((select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组1' as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组2' as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp)) where scdw_code like '%%';"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkCols(5)
        tdSql.checkRows(6)
        sql = "select scdw_name, scdw_code, jzzt from ((select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组1' as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组2' as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp)) where scdw_code like '%%';"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        sql = "select scdw_code, scdw_name, jzmc, fdgl, jzzt,ts from ((select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组1' as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01072016' as scdw_code, '盛鲁电厂' as scdw_name, '机组2' as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '00103673' as scdw_code, '鲁西电厂' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt, last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组1'as jzmc, last(jz1fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp) union all ( select '01061584' as scdw_code, '富源热电' as scdw_name, '机组2'as jzmc, last(jz2fdjyggl) as fdgl, '填报' as jzzt ,last(ts) as ts from ep_iot.sldc_dp)) where scdw_code like '%%';"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkCols(6)
        tdSql.checkRows(6)
        ##tdSql.execute("drop database ep_iot")

    def test_case_for_nodes_match_node(self):
        sql = "create table db.nt (ts timestamp, c1 int primary key, c2 int)"
        tdSql.execute(sql, queryTimes=1)
        sql = 'select diff (ts) from (select * from db.tt union select * from db.tt order by c1, case when ts < now - 1h then ts + 1h else ts end) partition by c1, case when ts < now - 1h then ts + 1h else ts end'
        tdSql.error(sql, -2147473917)

    def run(self):
        tdSql.prepare()
        self.test_TS_5630()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()
        self.test_TD_33137()
        self.test_case_for_nodes_match_node()
    
    def test_TD_33137(self):
        sql = "select 'asd' union all select 'asdasd'"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(2)
        sql = "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, stable_name `TABLE_NAME`, 'TABLE' `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_stables union all select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, table_name `TABLE_NAME`,  case when `type`='SYSTEM_TABLE' then 'TABLE'       when `type`='NORMAL_TABLE' then 'TABLE'       when `type`='CHILD_TABLE' then 'TABLE'       else 'UNKNOWN'  end `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_tables union all select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, view_name `TABLE_NAME`, 'VIEW' `TABLE_TYPE`, NULL `REMARKS` from information_schema.ins_views"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(54)

        sql = "select null union select null"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        sql = "select null union all select null"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        sql = "select null union select 1"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)

        sql = "select null union select 'asd'"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 'asd')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
