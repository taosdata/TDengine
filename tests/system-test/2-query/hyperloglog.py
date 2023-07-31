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

ALL_COL = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BOOL_COL, BINARY_COL, NCHAR_COL, TS_COL ]
DBNAME = "db"

class TDTestCase:

    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def __query_condition(self,tbname):
        return [ f"{any_col}" for any_col in ALL_COL ]

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False):
        table_reference = tb_list[0]
        join_condition = zwtable_reference
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
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"select hyperloglog({select_clause}) from {from_clause} {where_condition} {group_condition}"

    @property
    def __tb_list(self, dbname=DBNAME):
        return [
            f"{dbname}.ct1",
            f"{dbname}.ct4",
            f"{dbname}.t1",
            f"{dbname}.ct2",
            f"{dbname}.stb1",
        ]

    def sql_list(self):
        sqls = []
        __no_join_tblist = self.__tb_list
        for tb in __no_join_tblist:
            tbname = tb.split(".")[-1]
            select_claus_list = self.__query_condition(tbname)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition(col=select_claus)
                where_claus = self.__where_condition(query_conditon=select_claus)
                having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                sqls.extend(
                    (
                        self.__single_sql(select_claus, tb, where_claus, having_claus),
                        self.__single_sql(select_claus, tb,),
                        self.__single_sql(select_claus, tb, where_condition=where_claus),
                        self.__single_sql(select_claus, tb, group_condition=group_claus),
                    )
                )

        # return filter(None, sqls)
        return list(filter(None, sqls))


    def hyperloglog_check(self):
        sqls = self.sql_list()
        tdLog.printNoPrefix("===step 1: curent case, must return query OK")
        for i in range(len(sqls)):
            tdLog.info(f"sql: {sqls[i]}")
            tdSql.query(sqls[i])

    def __test_current(self, dbname=DBNAME):
        tdSql.query(f"select hyperloglog(ts) from {dbname}.ct1")
        tdSql.checkRows(1)
        tdSql.query(f"select hyperloglog(c1) from {dbname}.ct2")
        tdSql.checkRows(1)
        tdSql.query(f"select hyperloglog(c1) from {dbname}.ct4 group by c1")
        tdSql.checkRows(self.rows + 3)
        tdSql.query(f"select hyperloglog(c1) from {dbname}.ct4 group by c7")
        tdSql.checkRows(3)
        tdSql.query(f"select hyperloglog(ct2.c1) from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.rows + 2)
        tdSql.query(f"select hyperloglog(c1), c1 from {dbname}.stb1 group by c1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i, 0, 1) if  tdSql.queryResult[i][1] is not None else tdSql.checkData(i, 0, 0)

        self.hyperloglog_check()

    def __test_error(self, dbname=DBNAME):

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"select hyperloglog() from {dbname}.ct1" )
        tdSql.error( f"select hyperloglog(c1, c2) from {dbname}.ct2" )
        # tdSql.error( f"select hyperloglog(1) from {dbname}.stb1" )
        # tdSql.error( f"select hyperloglog(abs(c1)) from {dbname}.ct4" )
        tdSql.error( f"select hyperloglog(count(c1)) from {dbname}.t1" )
        # tdSql.error( f"select hyperloglog(1) from {dbname}.ct2" )
        tdSql.error( f"select hyperloglog({NUM_COL[0]}, {NUM_COL[1]}) from {dbname}.ct4" )
        tdSql.error( f'''select hyperloglog(['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10'])
                    from {dbname}.ct1
                    where ['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10'] is not null
                    group by ['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10']
                    having ['c1 + c1', 'c1 + c2', 'c1 + c3', 'c1 + c4', 'c1 + c5', 'c1 + c6', 'c1 + c7', 'c1 + c8', 'c1 + c9', 'c1 + c10'] is not null''' )

    def all_test(self):
        self.__test_error()
        self.__test_current()

    def __create_tb(self, dbname=DBNAME):

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
            { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}
    def __create_stable(self, dbname=DBNAME, stbname='stb',column_dict={'ts':'timestamp','col1': 'tinyint','col2': 'smallint','col3': 'int',
                                                        'col4': 'bigint','col5': 'tinyint unsigned','col6': 'smallint unsigned','col7': 'int unsigned',
                                                        'col8': 'bigint unsigned','col9': 'float','col10': 'double','col11': 'bool','col12': 'binary(20)','col13': 'nchar(20)'},
                                            tag_dict={'ts_tag':'timestamp','t1': 'tinyint','t2': 'smallint','t3': 'int',
                                                        't4': 'bigint','t5': 'tinyint unsigned','t6': 'smallint unsigned','t7': 'int unsigned',
                                                        't8': 'bigint unsigned','t9': 'float','t10': 'double','t11': 'bool','t12': 'binary(20)','t13': 'nchar(20)'}):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v},"
        tdSql.execute(f'create table if not exists {dbname}.{stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]})')

    def __hyperloglog_check_distribute(self):
        dbname = "dbtest"
        stbname = "stb"
        childtable_num = 20
        vgroups_num = 4
        row_num = 10
        ts = 1537146000000
        binary_str = 'taosdata'
        nchar_str = '涛思数据'
        column_dict = {
            'ts':'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': 'binary(20)',
            'col13': 'nchar(20)'
        }
        tag_dict = {
            'loc':'nchar(20)'
        }
        tdSql.execute(f"create database if not exists {dbname} vgroups {vgroups_num}")
        tdSql.execute(f'use {dbname}')
        self.__create_stable(dbname, stbname,column_dict,tag_dict)
        for i in range(childtable_num):
            tdSql.execute(f"create table {dbname}.{stbname}_{i} using {dbname}.{stbname} tags('beijing')")
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}'")
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)
        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >=2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
                continue
            else:
                tdLog.exit(f'This scene does not meet the requirements with {vgroups_num} vgroup!\n')
        for i in range(row_num):
            tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{binary_str}%d', '{nchar_str}%d')"
                          % (ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        for k in column_dict.keys():
            tdSql.query(f"select hyperloglog({k}) from {dbname}.{stbname}")
            tdSql.checkRows(1)
            tdSql.query(f"select hyperloglog({k}) from {dbname}.{stbname} group by {k}")

        tdSql.execute(f'drop database {dbname}')


    def __insert_data(self, rows, dbname=DBNAME):
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


    def run(self):
        tdSql.prepare(dbname=DBNAME)

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb(dbname=DBNAME)

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows,dbname=DBNAME)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdSql.execute("flush database db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

        tdLog.printNoPrefix("==========step5: distribute scene check")
        self.__hyperloglog_check_distribute()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
