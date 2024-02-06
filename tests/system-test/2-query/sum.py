from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.autogen import *


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

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
UN_NUM_COL = [BOOL_COL, BINARY_COL, NCHAR_COL, ]
TS_TYPE_COL = [TS_COL]

DBNAME = "db"

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.autoGen = AutoGen(True)

    def __sum_condition(self):
        sum_condition = []
        for num_col in NUM_COL:
            sum_condition.extend(
                (
                    num_col,
                    f"ceil( {num_col} )",
                )
            )
            sum_condition.extend( f"{num_col} + {num_col_2}" for num_col_2 in NUM_COL )
            sum_condition.extend( f"{num_col} + {un_num_col} " for un_num_col in UN_NUM_COL )

        sum_condition.append(1)

        return sum_condition

    def __where_condition(self, col):
        return f" where abs( {col} ) < 1000000 "

    def __group_condition(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __sum_current_check(self, tbname):
        sum_condition = self.__sum_condition()
        for condition in sum_condition:
            where_condition = self.__where_condition(condition)
            group_condition = self.__group_condition(condition, having=f"{condition} is not null " )

            tdSql.query(f"select {condition} from {DBNAME}.{tbname} {where_condition} ")
            datas = [tdSql.getData(i,0) for i in range(tdSql.queryRows)]
            sum_data = sum(filter(None, datas))
            tdSql.query(f"select sum( {condition} ) from {DBNAME}.{tbname} {where_condition} ")
            tdSql.checkData(0, 0, sum_data)

            tdSql.query(f"select {condition} from {DBNAME}.{tbname} {where_condition} {group_condition} ")
            tdSql.query(f"select sum( {condition} ) from {DBNAME}.{tbname} {where_condition} {group_condition} ")

    def __sum_err_check(self,tbanme):
        sqls = []

        for un_num_col in UN_NUM_COL:
            sqls.extend(
                (
                    f"select sum( {un_num_col} ) from {DBNAME}.{tbanme} ",
                    f"select sum(ceil( {un_num_col} )) {DBNAME}.from {tbanme} ",
                )
            )
            # sqls.extend( f"select sum( {un_num_col} + {un_num_col_2} ) from {tbanme} " for un_num_col_2 in UN_NUM_COL )

        sqls.extend( f"select sum( {num_col} + {ts_col} ) from {DBNAME}.{tbanme} " for num_col in NUM_COL for ts_col in TS_TYPE_COL)
        sqls.extend(
            (
                f"select sum() from {DBNAME}.{tbanme} ",
                f"select sum(*) from {DBNAME}.{tbanme} ",
                f"select sum(ccccccc) {DBNAME}.from {tbanme} ",
                f"select sum('test') from {DBNAME}.{tbanme} ",
            )
        )

        return sqls

    def __test_current(self, dbname="db"):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = ["ct1", "ct2", "ct4", "t1"]
        for tb in tbname:
            self.__sum_current_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")

    def __test_error(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = ["ct1", "ct2", "ct4", "t1"]

        for tb in tbname:
            for errsql in self.__sum_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test(self, dbname="db"):
        self.__test_current(dbname)
        self.__test_error(dbname)


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

    def testAllTypes(self):
        # create stable and insert
        tdLog.info("test all types")
        dbname  = "sumdb"
        stbname = "stb"
        colnum = 16
        self.autoGen.set_batch_size(1000)
        self.autoGen.create_db(dbname)
        self.autoGen.create_stable(stbname, 16, colnum, 8, 16)
        self.autoGen.create_child(stbname, "d", 4)
        self.autoGen.insert_data(10000)

        # check correct
        i = 0
        for c in self.autoGen.mcols:

            if c in [0, 11, 12, 13]:
                i += 1
                continue

            # query
            col = f"c{i}"
            sql = f"select count({col}), sum({col}), avg({col}), max({col}), min({col}), stddev({col}), leastsquares({col},1,9) from {dbname}.{stbname}"
            tdSql.query(sql)
            # sum
            tdSql.checkData(0, 0, 4*10000, True)
            # sum
            tdSql.checkData(0, 1, 4*10000, True)
            # avg
            tdSql.checkData(0, 2, 1, True)
            # max
            tdSql.checkData(0, 3, 1, True)
            # min
            tdSql.checkData(0, 4, 1, True)
            # stddev
            tdSql.checkData(0, 5, 0, True)

            sql = f"select twa({col}) from {dbname}.d0"
            tdSql.query(sql)
            tdSql.checkData(0, 0, 1, True)

            i += 1


    def run(self):
        tdSql.prepare()

        self.testAllTypes()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data(10)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
