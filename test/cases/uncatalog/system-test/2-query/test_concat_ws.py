from new_test_framework.utils import tdLog, tdSql
import datetime
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

class TestConcatWs:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def __concat_ws_condition(self):  # sourcery skip: extract-method
        concat_ws_condition = []
        for char_col in CHAR_COL:
            concat_ws_condition.extend(
                (
                    char_col,
                    # f"upper( {char_col} )",
                )
            )
            concat_ws_condition.extend( f"cast( {num_col} as binary(16) ) " for num_col in NUM_COL)
            concat_ws_condition.extend( f"cast( {char_col} + {num_col} as binary(16) ) " for num_col in NUM_COL )
            # concat_ws_condition.extend( f"cast( {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL )
            # concat_ws_condition.extend( f"cast( {char_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL )
            concat_ws_condition.extend( f"cast( {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL )
            # concat_ws_condition.extend( f"cast( {char_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL )
            concat_ws_condition.extend( f"cast( {char_col} + {char_col_2} as binary(16) ) " for char_col_2 in CHAR_COL )

        for num_col in NUM_COL:
            # concat_ws_condition.extend( f"cast( {num_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL )
            concat_ws_condition.extend( f"cast( {num_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL if num_col is not FLOAT_COL and num_col is not DOUBLE_COL)

        # concat_ws_condition.extend( f"cast( {bool_col} + {ts_col} as binary(16) )" for bool_col in BOOLEAN_COL for ts_col in TS_TYPE_COL )

        concat_ws_condition.append('''"test1234!@#$%^&*():'><?/.,][}{"''')

        return concat_ws_condition

    def __where_condition(self, col):
        # return f" where count({col}) > 0 "
        return ""

    def __concat_ws_num(self, concat_ws_lists, num):
        return [ concat_ws_lists[i] for i in range(num) ]


    def __group_condition(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __concat_ws_check(self, tbname, num):
        concat_ws_condition = self.__concat_ws_condition()
        for i in range(len(concat_ws_condition) - num + 1 ):
            condition = self.__concat_ws_num(concat_ws_condition[i:], num)
            concat_ws_filter = f"concat_ws('_',  {','.join( condition ) }) "
            where_condition = self.__where_condition(condition[0])
            # group_having = self.__group_condition(condition[0], having=f"{condition[0]} is not null " )
            concat_ws_group_having = self.__group_condition(concat_ws_filter, having=f"{concat_ws_filter} is not null " )
            # group_no_having= self.__group_condition(condition[0] )
            concat_ws_group_no_having= self.__group_condition(concat_ws_filter)
            groups = ["", concat_ws_group_having, concat_ws_group_no_having]

            if num > 8 or num < 2 :
                [tdSql.error(f"select concat_ws('_',  {','.join( condition ) })  from {tbname} {where_condition}  {group} ") for group in groups ]
                break

            tdSql.query(f"select  {','.join(condition)}  from {tbname}  ")
            rows = tdSql.queryRows
            concat_ws_data = []
            for m in range(rows):
                concat_ws_data.append("_".join(tdSql.queryResult[m])) if tdSql.getData(m, 0) else concat_ws_data.append(None)
            tdSql.query(f"select concat_ws('_',  {','.join( condition ) })  from {tbname} ")
            tdSql.checkRows(rows)
            for j in range(tdSql.queryRows):
                assert tdSql.getData(j, 0) in concat_ws_data

            [ tdSql.query(f"select concat_ws('_',  {','.join( condition ) })  from {tbname} {where_condition}  {group} ") for group in groups ]

    def __concat_ws_err_check(self,tbname):
        sqls = []

        for char_col in CHAR_COL:
            sqls.extend(
                (
                    f"select concat_ws('_', {char_col} ) from {tbname} ",
                    f"select concat_ws('_', ceil( {char_col} )) from {tbname} ",
                    f"select {char_col} from {tbname} group by concat_ws('_',  {char_col} ) ",
                )
            )

            sqls.extend( f"select concat_ws('_',  {char_col} , {num_col} ) from {tbname} " for num_col in NUM_COL )
            sqls.extend( f"select concat_ws('_',  {char_col} , {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL )
            sqls.extend( f"select concat_ws('_',  {char_col} , {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL )

        sqls.extend( f"select concat_ws('_',  {ts_col}, {bool_col} ) from {tbname} " for ts_col in TS_TYPE_COL for bool_col in BOOLEAN_COL )
        sqls.extend( f"select concat_ws('_',  {num_col} , {ts_col} ) from {tbname} " for num_col in NUM_COL for ts_col in TS_TYPE_COL)
        sqls.extend( f"select concat_ws('_',  {num_col} , {bool_col} ) from {tbname} " for num_col in NUM_COL for bool_col in BOOLEAN_COL)
        sqls.extend( f"select concat_ws('_',  {num_col} , {num_col} ) from {tbname} " for num_col in NUM_COL for num_col in NUM_COL)
        sqls.extend( f"select concat_ws('_',  {ts_col}, {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL for ts_col in TS_TYPE_COL )
        sqls.extend( f"select concat_ws('_',  {bool_col}, {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL for bool_col in BOOLEAN_COL )

        sqls.extend( f"select concat_ws('_',  {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL for char_col_2 in CHAR_COL )
        sqls.extend( f"select concat_ws('_', {char_col}, 11) from {tbname} " for char_col in CHAR_COL )
        sqls.extend( f"select concat_ws('_', {num_col}, '1') from {tbname} " for num_col in NUM_COL )
        sqls.extend( f"select concat_ws('_', {ts_col}, '1') from {tbname} " for ts_col in TS_TYPE_COL )
        sqls.extend( f"select concat_ws('_', {bool_col}, '1') from {tbname} " for bool_col in BOOLEAN_COL )
        sqls.extend( f"select concat_ws('_', {char_col},'1') from {tbname} interval(2d) sliding(1d)" for char_col in CHAR_COL )
        sqls.extend(
            (
                f"select concat_ws('_', ) from {tbname} ",
                f"select concat_ws('_', *) from {tbname} ",
                f"select concat_ws('_', ccccccc) from {tbname} ",
                f"select concat_ws('_', 111) from {tbname} ",
            )
        )

        return sqls

    def __test_current(self,dbname="db"):  # sourcery skip: use-itertools-product
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [
            f"{dbname}.t1",
            f"{dbname}.stb1"
        ]
        for tb in tbname:
            for i in range(2,8):
                self.__concat_ws_check(tb,i)
                tdLog.printNoPrefix(f"==========current sql condition check in {tb}, col num: {i} over==========")

    def __test_error(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [
            f"{dbname}.ct1",
            f"{dbname}.ct2",
            f"{dbname}.ct4",
        ]
        tdSql.query("select concat_ws(null,null,null);")  # TD-31572
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        for tb in tbname:
            for errsql in self.__concat_ws_err_check(tb):
                tdSql.error(sql=errsql)
            self.__concat_ws_check(tb,1)
            self.__concat_ws_check(tb,9)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")

    def all_test(self,dbname="db"):
        self.__test_current(dbname)
        self.__test_error(dbname)

    def __create_tb(self, dbname="db"):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (t1 int)
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

    def test_concat_ws(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb(dbname="db")

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows, dbname="db")

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test(dbname="db")

        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test(dbname="db")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
