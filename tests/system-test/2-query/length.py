from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


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

UN_CHAR_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BOOL_COL, ]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
TS_TYPE_COL = [TS_COL]

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def __length_condition(self):
        length_condition = []
        for char_col in CHAR_COL:
            length_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            length_condition.extend( f"cast( {un_char_col} as binary(16) ) " for un_char_col in UN_CHAR_COL)
            length_condition.extend( f"cast( {char_col} + {char_col_2} as binary(32) ) " for char_col_2 in CHAR_COL )
            length_condition.extend( f"cast( {char_col} + {un_char_col} as binary(32) ) " for un_char_col in UN_CHAR_COL )

        length_condition.append('''"test1234!@#$%^&*():'><?/.,][}{"''')

        return length_condition

    def __where_condition(self, col):
        # return f" where count({col}) > 0 "
        return ""

    def __group_condition(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __length_current_check(self, tbname):
        length_condition = self.__length_condition()
        for condition in length_condition:
            where_condition = self.__where_condition(condition)
            group_having = self.__group_condition(condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition(condition )
            groups = ["", group_having, group_no_having]

            for group_condition in groups:
                tdSql.query(f"select {condition} from {tbname} {where_condition}  {group_condition} ")
                datas = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
                length_data = [ len(str(data))  if data else None for data in datas ]
                tdSql.query(f"select length( {condition} ) from {tbname} {where_condition}  {group_condition}")
                for i in range(len(length_data)):
                    tdSql.checkData(i, 0, length_data[i] ) if length_data[i] else tdSql.checkData(i, 0, None)

    def __length_err_check(self,tbname):
        sqls = []

        for un_char_col in UN_CHAR_COL:
            sqls.extend(
                (
                    f"select length( {un_char_col} ) from {tbname} ",
                    f"select length(ceil( {un_char_col} )) from {tbname} ",
                    f"select {un_char_col} from {tbname} group by length( {un_char_col} ) ",
                )
            )

            sqls.extend( f"select length( {un_char_col} + {un_char_col_2} ) from {tbname} " for un_char_col_2 in UN_CHAR_COL )
            sqls.extend( f"select length( {un_char_col} + {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL )

        sqls.extend( f"select {char_col} from {tbname} group by length( {char_col} ) " for char_col in CHAR_COL)
        sqls.extend( f"select length( {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL )
        sqls.extend( f"select length( {char_col} + {ts_col} ) from {tbname} " for char_col in UN_CHAR_COL for ts_col in TS_TYPE_COL)
        sqls.extend( f"select length( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL for char_col_2 in CHAR_COL )
        sqls.extend( f"select upper({char_col}, 11) from {tbname} " for char_col in CHAR_COL )
        sqls.extend( f"select upper({char_col}) from {tbname} interval(2d) sliding(1d)" for char_col in CHAR_COL )
        sqls.extend(
            (
                f"select length() from {tbname} ",
                f"select length(*) from {tbname} ",
                f"select length(ccccccc) from {tbname} ",
                f"select length(111) from {tbname} ",
                f"select length(c8, 11) from {tbname} ",
            )
        )

        return sqls

    def __test_current(self):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = ["ct1", "ct2", "ct4", "t1"]
        for tb in tbname:
            self.__length_current_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")

    def __test_error(self):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = ["ct1", "ct2", "ct4", "t1"]

        for tb in tbname:
            for errsql in self.__length_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test(self):
        self.__test_current()
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

    def __insert_data(self, rows):
        for i in range(9):
            tdSql.execute(
                f"insert into ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct2 values ( now()-{i*90}d, {-1*i}, {-11111*i}, {-111*i}, {-11*i}, {-1.11*i}, {-11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute(
            '''insert into ct1 values
            ( now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )
            ( now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )
            '''
        )

        tdSql.execute(
            f'''insert into ct4 values
            ( now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()+{rows * 9}d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                now()+{rows * 9-10}d, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nachar_limit-1", now()-1d
                )
            (
                now()+{rows * 9-20}d, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nachar_limit-2", now()-2d
                )
            '''
        )

        tdSql.execute(
            f'''insert into ct2 values
            ( now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()+{rows * 9}d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                now()+{rows * 9-10}d, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nachar_limit-1", now()-1d
                )
            (
                now()+{rows * 9-20}d, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nachar_limit-2", now()-2d
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into t1 values
                ( now()-{i}h, {i}, {i}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_{i}", now()-{i}s )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into t1 values
            ( now() + 3h, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-{ ( rows // 2 ) * 60 + 30 }m, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now()-{rows}h, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( now() + 2h, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nachar_limit-1", now()-1d
                )
            (
                now() + 1h , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nachar_limit-2", now()-2d
                )
            '''
        )


    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data(100)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        # tdSql.execute("use db")

        # tdLog.printNoPrefix("==========step4:after wal, all check again ")
        # self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
