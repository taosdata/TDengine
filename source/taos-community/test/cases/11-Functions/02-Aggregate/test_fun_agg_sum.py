import datetime
import numpy as np
import platform
import random ,os ,sys

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, AutoGen 


# ------- test_sum.py global -----------
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

# ------- other global -----------


class TestFunSum:
    # distribute case
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------ sim case ------------------
    #
    def do_sim_sum(self):
        dbPrefix = "m_su_db"
        tbPrefix = "m_su_tb"
        mtPrefix = "m_su_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select sum(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 190)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select sum(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select sum(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 190)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select sum(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select sum(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 190)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select sum(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select sum(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1900)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select sum(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdSql.query(f"select sum(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 950)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select sum(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select sum(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkAssert(tdSql.getData(1, 0) >= 5)

        tdSql.query(f"select sum(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1900)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select sum(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 190)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(
            f"select sum(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1d)"
        )
        tdLog.info(
            f"select sum(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1d)"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkRows(10)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        print("\n")
        print("sum sim case .......................... [passed]")


    #
    # ------------------ test_sum.py ------------------
    #
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

        sum_condition.append(1.1)
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

        #sqls.extend( f"select sum( {num_col} + {ts_col} ) from {DBNAME}.{tbanme} " for num_col in NUM_COL for ts_col in TS_TYPE_COL)
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
        autoGen = AutoGen(step=1, batch=100,genDataMode="fillone")
        autoGen.set_batch_size(1000)
        autoGen.create_db(dbname)
        autoGen.create_stable(stbname, 16, colnum, 8, 16, type_set='varchar_preferred')
        autoGen.create_child(stbname, "d", 4)
        autoGen.insert_data(10000)

        # check correct
        i = 0
        for c in autoGen.mcols:

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

    def do_sum(self):
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

        print("do sum case ........................... [passed]")

    #
    # ------------------ test_distribute_agg_sum.py ------------------
    #

    def check_sum_functions(self, tbname , col_name):

        sum_sql = f"select sum({col_name}) from {tbname};"

        same_sql = f"select {col_name} from {tbname} where {col_name} is not null "

        tdSql.query(same_sql)
        pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
        if (platform.system().lower() == 'windows' and pre_data.dtype == 'int32'):
            pre_data = np.array(pre_data, dtype = 'int64')
        pre_sum = np.sum(pre_data)

        tdSql.query(sum_sql)
        tdSql.checkData(0,0,pre_sum)

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 vgroups 5")
        tdSql.execute(f" use {dbname}")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(20):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )

        for i in range(1,21):
            if i ==1 or i == 4:
                continue
            else:
                tbname = f"ct{i}"
                for j in range(9):
                    tdSql.execute(
                f"insert into {dbname}.{tbname} values ( now()-{(i+j)*10}s, {1*(j+i)}, {11111*(j+i)}, {111*(j+i)}, {11*(j)}, {1.11*(j+i)}, {11.11*(j+i)}, {(j+i)%2}, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
            )

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        tdLog.info(f" prepare data for distributed_aggregate done! ")

    def check_distribute_datas(self, dbname="testdb"):
        # get vgroup_ids of all
        tdSql.query(f"show {dbname}.vgroups ")
        vgroups = tdSql.queryResult

        vnode_tables={}

        for vgroup_id in vgroups:
            vnode_tables[vgroup_id[0]]=[]

        # check sub_table of per vnode ,make sure sub_table has been distributed
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}' and table_name like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            vnode_tables[table_name[6]].append(table_name[0])
        self.vnode_disbutes = vnode_tables

        count = 0
        for k ,v in vnode_tables.items():
            if len(v)>=2:
                count+=1
        if count < 2:
            tdLog.exit(f" the datas of all not satisfy sub_table has been distributed ")

    def check_sum_distribute_diff_vnode(self,col_name, dbname="testdb"):

        vgroup_ids = []
        for k ,v in self.vnode_disbutes.items():
            if len(v)>=2:
                vgroup_ids.append(k)

        distribute_tbnames = []

        for vgroup_id in vgroup_ids:
            vnode_tables = self.vnode_disbutes[vgroup_id]
            distribute_tbnames.append(random.sample(vnode_tables,1)[0])
        tbname_ins = ""
        for tbname in distribute_tbnames:
            tbname_ins += "'%s' ,"%tbname

        tbname_filters = tbname_ins[:-1]

        sum_sql = f"select sum({col_name}) from {dbname}.stb1 where tbname in ({tbname_filters});"

        same_sql = f"select {col_name}  from {dbname}.stb1 where tbname in ({tbname_filters}) and {col_name} is not null "

        tdSql.query(same_sql)
        pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
        if (platform.system().lower() == 'windows' and pre_data.dtype == 'int32'):
            pre_data = np.array(pre_data, dtype = 'int64')
        pre_sum = np.sum(pre_data)

        tdSql.query(sum_sql)
        tdSql.checkData(0,0,pre_sum)

    def check_sum_status(self, dbname="testdb"):
        # check max function work status

        tdSql.query(f"show {dbname}.tables like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            tablenames.append(f"{dbname}.{table_name[0]}")

        tdSql.query(f"desc {dbname}.stb1")
        col_names = tdSql.queryResult

        colnames = []
        for col_name in col_names:
            if col_name[1] in ["INT" ,"BIGINT" ,"SMALLINT" ,"TINYINT" , "FLOAT" ,"DOUBLE"]:
                colnames.append(col_name[0])

        for tablename in tablenames:
            for colname in colnames:
                self.check_sum_functions(tablename,colname)

        # check max function for different vnode

        for colname in colnames:
            if colname.startswith("c"):
                self.check_sum_distribute_diff_vnode(colname)

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select sum(c1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,2592)

        tdSql.query(f"select sum(a) from (select sum(c1) a  from {dbname}.stb1 partition by tbname) ")
        tdSql.checkData(0,0,2592)

        tdSql.query(f"select sum(c1) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,54)

        tdSql.query(f"select sum(c1+c2) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,22224.000000000)

        tdSql.query(f"select sum(c1) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0,0,54)

        tdSql.query(f"select sum(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select sum(c1) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select sum(c1) from {dbname}.stb1 union all select sum(c1) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,2592)

        tdSql.query(f"select sum(a) from (select sum(c1) a from {dbname}.stb1 union all select sum(c1) a  from {dbname}.stb1)")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,5184)

        # join
        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table db.tb1 using db.st tags(1) ")
        tdSql.execute(" create table db.tb2 using db.st tags(2) ")


        for i in range(10):
            ts = i*10 + self.ts
            tdSql.execute(f" insert into db.tb1 values({ts},{i},{i}.0)")
            tdSql.execute(f" insert into db.tb2 values({ts},{i},{i}.0)")


        tdSql.query("select sum(tb1.c1), sum(tb2.c2) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,45)
        tdSql.checkData(0,1,45.000000000)

        # group by
        tdSql.execute(f"use {dbname} ")

        # partition by tbname or partition by tag
        tdSql.query(f"select sum(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        # nest query for support max
        tdSql.query(f"select abs(c2+2)+1 from (select sum(c1) c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,2595.000000000)
        tdSql.query(f"select sum(c1+2)  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,2960.000000000)
        tdSql.query(f"select sum(a+2)  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,2960.000000000)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3),sum(c1+c2) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)
        tdSql.checkData(0,4,28202310.000000000)

    def do_distribute_sum(self):
        #init
        self.vnode_disbutes = None
        self.ts = 1537146000000

        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.check_sum_status()
        self.distribute_agg_query()
        print("do sum distribute ..................... [passed]")

    #
    # ------------------ main ------------------
    #
    def test_func_agg_sum(self):
        """ Fun: sum()

        1. Sim case including time windows, filtering on ordinary data columns, filtering on tag columns, GROUP BY, and PARTITION BY.
        2. Query on super/child/normal table
        3. Support types
        4. Error cases
        5. Query with filter conditions
        6. Query with group by
        7. Query with distribute aggregate


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/sum.sim
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_sum.py
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_distribute_agg_sum.py

        """

        self.do_sim_sum()
        self.do_sum()
        self.do_distribute_sum()