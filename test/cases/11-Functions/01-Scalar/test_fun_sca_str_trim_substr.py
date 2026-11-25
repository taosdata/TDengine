# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql
import datetime
import platform


PRIMARY_COL_ltrim = "ts"


INT_COL_ltrim     = "c1"


BINT_COL_ltrim    = "c2"


SINT_COL_ltrim    = "c3"


TINT_COL_ltrim    = "c4"


FLOAT_COL_ltrim   = "c5"


DOUBLE_COL_ltrim  = "c6"


BOOL_COL_ltrim    = "c7"


BINARY_COL_ltrim  = "c8"


NCHAR_COL_ltrim   = "c9"


TS_COL_ltrim      = "c10"


NUM_COL_ltrim     = [ INT_COL_ltrim, BINT_COL_ltrim, SINT_COL_ltrim, TINT_COL_ltrim, FLOAT_COL_ltrim, DOUBLE_COL_ltrim, ]


CHAR_COL_ltrim    = [ BINARY_COL_ltrim, NCHAR_COL_ltrim, ]


BOOLEAN_COL_ltrim = [ BOOL_COL_ltrim, ]


TS_TYPE_COL_ltrim = [ TS_COL_ltrim, ]


DBNAME_ltrim = "db"


PRIMARY_COL_rtrim = "ts"


INT_COL_rtrim     = "c1"


BINT_COL_rtrim    = "c2"


SINT_COL_rtrim    = "c3"


TINT_COL_rtrim    = "c4"


FLOAT_COL_rtrim   = "c5"


DOUBLE_COL_rtrim  = "c6"


BOOL_COL_rtrim    = "c7"


BINARY_COL_rtrim  = "c8"


NCHAR_COL_rtrim   = "c9"


TS_COL_rtrim      = "c10"


NUM_COL_rtrim     = [ INT_COL_rtrim, BINT_COL_rtrim, SINT_COL_rtrim, TINT_COL_rtrim, FLOAT_COL_rtrim, DOUBLE_COL_rtrim, ]


CHAR_COL_rtrim    = [ BINARY_COL_rtrim, NCHAR_COL_rtrim, ]


BOOLEAN_COL_rtrim = [ BOOL_COL_rtrim, ]


TS_TYPE_COL_rtrim = [ TS_COL_rtrim, ]


PRIMARY_COL_substr = "ts"


INT_COL_substr     = "c1"


BINT_COL_substr    = "c2"


SINT_COL_substr    = "c3"


TINT_COL_substr    = "c4"


FLOAT_COL_substr   = "c5"


DOUBLE_COL_substr  = "c6"


BOOL_COL_substr    = "c7"


BINARY_COL_substr  = "c8"


NCHAR_COL_substr   = "c9"


TS_COL_substr      = "c10"


NUM_COL_substr     = [ INT_COL_substr, BINT_COL_substr, SINT_COL_substr, TINT_COL_substr, FLOAT_COL_substr, DOUBLE_COL_substr, ]


CHAR_COL_substr    = [ BINARY_COL_substr, NCHAR_COL_substr, ]


BOOLEAN_COL_substr = [ BOOL_COL_substr, ]


TS_TYPE_COL_substr = [ TS_COL_substr, ]


ERR_POS_substr     = 0


CURRENT_POS_substr = 1


LENS_substr        = 6


IS_WINDOWS_regexp = platform.system().lower() == 'windows'


class TestFunScaStrTrimSubstr:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def setup_method(self):
        # These methods used to run in their own taosd process, so every one of
        # them started from an empty server. They now share a single deployment,
        # therefore drop every user database first to keep that isolation
        # (several cases assert on information_schema.ins_databases).
        tdSql.query(
            "select name from information_schema.ins_databases "
            "where name not in ('information_schema', 'performance_schema')"
        )
        for i in range(tdSql.queryRows):
            tdSql.execute(f"drop database if exists `{tdSql.getData(i, 0)}`")


    def __ltrim_condition(self):  # sourcery skip: extract-method
        ltrim_condition = []
        for char_col in CHAR_COL_ltrim:
            ltrim_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            ltrim_condition.extend( f"cast( {num_col} as binary(16) ) " for num_col in NUM_COL_ltrim)
            ltrim_condition.extend( f"cast( {char_col} + {num_col} as binary(16) ) " for num_col in NUM_COL_ltrim )
            ltrim_condition.extend( f"concat( cast( {char_col} + {num_col} as binary(16) ), {char_col}) " for num_col in NUM_COL_ltrim )
            ltrim_condition.extend( f"cast( {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_ltrim )
            ltrim_condition.extend( f"cast( {char_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_ltrim )
            ltrim_condition.extend( f"cast( {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_ltrim )
            # ltrim_condition.extend( f"cast( {char_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_ltrim )
            ltrim_condition.extend( f"cast( {char_col} + {char_col_2} as binary(16) ) " for char_col_2 in CHAR_COL_ltrim )
            ltrim_condition.extend( f"concat( {char_col}, {char_col_2} ) " for char_col_2 in CHAR_COL_ltrim )

        for num_col in NUM_COL_ltrim:
            ltrim_condition.extend( f"cast( {num_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_ltrim )
            ltrim_condition.extend( f"cast( {num_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_ltrim if num_col is not FLOAT_COL_ltrim and num_col is not DOUBLE_COL_ltrim)

        ltrim_condition.extend( f"cast( {bool_col} + {ts_col} as binary(16) )" for bool_col in BOOLEAN_COL_ltrim for ts_col in TS_TYPE_COL_ltrim )

        ltrim_condition.append(''' "   test1234!@#$%^&*()  :'><?/.,][}{   " ''')

        return ltrim_condition


    def __where_condition_ltrim(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_ltrim(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __ltrim_check(self, tbname):
        ltrim_condition = self.__ltrim_condition()
        for condition in ltrim_condition:
            where_condition = self.__where_condition_ltrim(condition)
            ltrim_group_having = self.__group_condition_ltrim(condition, having=f"{condition} is not null " )
            ltrim_group_no_having= self.__group_condition_ltrim(condition)
            groups = ["", ltrim_group_having, ltrim_group_no_having]

            tdSql.query(f"select ltrim( {condition}) , {condition} from {tbname} ")
            for j in range(tdSql.queryRows):
                tdSql.checkData(j,0, tdSql.getData(j,1).lstrip()) if tdSql.getData(j,1) else tdSql.checkData(j, 0, None)

            [ tdSql.query(f"select ltrim({condition})  from {tbname} {where_condition}  {group} ") for group in groups ]


    def __ltrim_err_check(self,tbname):
        sqls = []

        for num_col in NUM_COL_ltrim:
            sqls.extend(
                (
                    f"select ltrim( {num_col} ) from {tbname} ",
                    f"select ltrim(ceil( {num_col} )) from {tbname} ",
                    f"select {num_col} from {tbname} group by ltrim( {num_col} ) ",
                )
            )

            sqls.extend( f"select ltrim( {char_col} , {num_col} ) from {tbname} " for char_col in CHAR_COL_ltrim )
            sqls.extend( f"select ltrim( {num_col} , {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_ltrim )
            sqls.extend( f"select ltrim( {num_col} , {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL_ltrim )

        sqls.extend( f"select ltrim( {ts_col}+{bool_col} ) from {tbname} " for ts_col in TS_TYPE_COL_ltrim for bool_col in BOOLEAN_COL_ltrim )
        sqls.extend( f"select ltrim( {num_col}+{ts_col} ) from {tbname} " for num_col in NUM_COL_ltrim for ts_col in TS_TYPE_COL_ltrim)
        sqls.extend( f"select ltrim( {num_col}+ {bool_col} ) from {tbname} " for num_col in NUM_COL_ltrim for bool_col in BOOLEAN_COL_ltrim)
        sqls.extend( f"select ltrim( {num_col}+ {num_col} ) from {tbname} " for num_col in NUM_COL_ltrim for num_col in NUM_COL_ltrim)
        sqls.extend( f"select ltrim( {ts_col}+{ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_ltrim for ts_col in TS_TYPE_COL_ltrim )
        sqls.extend( f"select ltrim( {bool_col}+ {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL_ltrim for bool_col in BOOLEAN_COL_ltrim )

        sqls.extend( f"select ltrim( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_ltrim for char_col_2 in CHAR_COL_ltrim )
        sqls.extend( f"select ltrim({num_col}, '1') from {tbname} " for num_col in NUM_COL_ltrim )
        sqls.extend( f"select ltrim({ts_col}, '1') from {tbname} " for ts_col in TS_TYPE_COL_ltrim )
        sqls.extend( f"select ltrim({bool_col}, '1') from {tbname} " for bool_col in BOOLEAN_COL_ltrim )
        sqls.extend(
            (
                f"select ltrim() from {tbname} ",
                f"select ltrim(*) from {tbname} ",
                f"select ltrim(ccccccc) from {tbname} ",
                f"select ltrim(111) from {tbname} ",
            )
        )

        return sqls


    def __test_current_ltrim(self, dbname=DBNAME_ltrim):  # sourcery skip: use-itertools-product
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__ltrim_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_ltrim(self, dbname=DBNAME_ltrim):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__ltrim_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test_ltrim(self):
        self.__test_current_ltrim()
        self.__test_error_ltrim()


    def __create_tb_ltrim(self, dbname=DBNAME_ltrim):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_ltrim} int, {BINT_COL_ltrim} bigint, {SINT_COL_ltrim} smallint, {TINT_COL_ltrim} tinyint,
                 {FLOAT_COL_ltrim} float, {DOUBLE_COL_ltrim} double, {BOOL_COL_ltrim} bool,
                 {BINARY_COL_ltrim} binary(16), {NCHAR_COL_ltrim} nchar(32), {TS_COL_ltrim} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL_ltrim} int, {BINT_COL_ltrim} bigint, {SINT_COL_ltrim} smallint, {TINT_COL_ltrim} tinyint,
                 {FLOAT_COL_ltrim} float, {DOUBLE_COL_ltrim} double, {BOOL_COL_ltrim} bool,
                 {BINARY_COL_ltrim} binary(16), {NCHAR_COL_ltrim} nchar(32), {TS_COL_ltrim} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_ltrim(self, rows, dbname=DBNAME_ltrim):
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


    def test_fun_sca_ltrim(self):
        """ Fun: ltrim()

        1. LTRIM on super/child/normal table
        2. LTRIM between all data types
        3. LTRIM with null values
        4. LTRIM constant operation
        5. LTRIM with invalid parameters
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_ltrim.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_trim_substr.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_ltrim()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data_ltrim(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_ltrim()

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_ltrim()

        #tdSql.init(conn.cursor(), logSql)

    def __rtrim_condition(self):  # sourcery skip: extract-method
        rtrim_condition = []
        for char_col in CHAR_COL_rtrim:
            rtrim_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            rtrim_condition.extend( f"cast( {num_col} as binary(16) ) " for num_col in NUM_COL_rtrim)
            rtrim_condition.extend( f"cast( {char_col} + {num_col} as binary(16) ) " for num_col in NUM_COL_rtrim )
            rtrim_condition.extend( f"concat( cast( {char_col} + {num_col} as binary(16) ), {char_col}) " for num_col in NUM_COL_rtrim )
            rtrim_condition.extend( f"cast( {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_rtrim )
            rtrim_condition.extend( f"cast( {char_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_rtrim )
            rtrim_condition.extend( f"cast( {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_rtrim )
            # rtrim_condition.extend( f"cast( {char_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_rtrim )
            rtrim_condition.extend( f"cast( {char_col} + {char_col_2} as binary(16) ) " for char_col_2 in CHAR_COL_rtrim )
            rtrim_condition.extend( f"concat( {char_col}, {char_col_2} ) " for char_col_2 in CHAR_COL_rtrim )

        for num_col in NUM_COL_rtrim:
            rtrim_condition.extend( f"cast( {num_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_rtrim )
            rtrim_condition.extend( f"cast( {num_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_rtrim if num_col is not FLOAT_COL_rtrim and num_col is not DOUBLE_COL_rtrim )

        rtrim_condition.extend( f"cast( {bool_col} + {ts_col} as binary(16) )" for bool_col in BOOLEAN_COL_rtrim for ts_col in TS_TYPE_COL_rtrim )

        rtrim_condition.append(''' "   test1234!@#$%^&*()  :'><?/.,][}{   " ''')

        return rtrim_condition


    def __where_condition_rtrim(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_rtrim(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __rtrim_check(self, tbname):
        rtrim_condition = self.__rtrim_condition()
        for condition in rtrim_condition:
            where_condition = self.__where_condition_rtrim(condition)
            rtrim_group_having = self.__group_condition_rtrim(condition, having=f"{condition} is not null " )
            rtrim_group_no_having= self.__group_condition_rtrim(condition)
            groups = ["", rtrim_group_having, rtrim_group_no_having]

            tdSql.query(f"select rtrim( {condition}) , {condition} from {tbname} ")
            for j in range(tdSql.queryRows):
                tdSql.checkData(j,0, tdSql.getData(j,1).rstrip()) if tdSql.getData(j,1) else tdSql.checkData(j, 0, None)

            [ tdSql.query(f"select rtrim({condition})  from {tbname} {where_condition}  {group} ") for group in groups ]


    def __rtrim_err_check(self,tbname):
        sqls = []

        for num_col in NUM_COL_rtrim:
            sqls.extend(
                (
                    f"select rtrim( {num_col} ) from {tbname} ",
                    f"select rtrim(ceil( {num_col} )) from {tbname} ",
                    f"select {num_col} from {tbname} group by rtrim( {num_col} ) ",
                )
            )

            sqls.extend( f"select rtrim( {char_col} , {num_col} ) from {tbname} " for char_col in CHAR_COL_rtrim )
            sqls.extend( f"select rtrim( {num_col} , {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_rtrim )
            sqls.extend( f"select rtrim( {num_col} , {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL_rtrim )

        sqls.extend( f"select rtrim( {ts_col}+{bool_col} ) from {tbname} " for ts_col in TS_TYPE_COL_rtrim for bool_col in BOOLEAN_COL_rtrim )
        sqls.extend( f"select rtrim( {num_col}+{ts_col} ) from {tbname} " for num_col in NUM_COL_rtrim for ts_col in TS_TYPE_COL_rtrim)
        sqls.extend( f"select rtrim( {num_col}+ {bool_col} ) from {tbname} " for num_col in NUM_COL_rtrim for bool_col in BOOLEAN_COL_rtrim)
        sqls.extend( f"select rtrim( {num_col}+ {num_col} ) from {tbname} " for num_col in NUM_COL_rtrim for num_col in NUM_COL_rtrim)
        sqls.extend( f"select rtrim( {ts_col}+{ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_rtrim for ts_col in TS_TYPE_COL_rtrim )
        sqls.extend( f"select rtrim( {bool_col}+ {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL_rtrim for bool_col in BOOLEAN_COL_rtrim )

        sqls.extend( f"select rtrim( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_rtrim for char_col_2 in CHAR_COL_rtrim )
        sqls.extend( f"select rtrim({num_col}, '1') from {tbname} " for num_col in NUM_COL_rtrim )
        sqls.extend( f"select rtrim({ts_col}, '1') from {tbname} " for ts_col in TS_TYPE_COL_rtrim )
        sqls.extend( f"select rtrim({bool_col}, '1') from {tbname} " for bool_col in BOOLEAN_COL_rtrim )
        sqls.extend(
            (
                f"select rtrim() from {tbname} ",
                f"select rtrim(*) from {tbname} ",
                f"select rtrim(ccccccc) from {tbname} ",
                f"select rtrim(111) from {tbname} ",
            )
        )

        return sqls


    def __test_current_rtrim(self, dbname="db"):  # sourcery skip: use-itertools-product
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__rtrim_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_rtrim(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__rtrim_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test_rtrim(self):
        self.__test_current_rtrim()
        self.__test_error_rtrim()


    def __create_tb_rtrim(self, dbname="db"):
        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_rtrim} int, {BINT_COL_rtrim} bigint, {SINT_COL_rtrim} smallint, {TINT_COL_rtrim} tinyint,
                 {FLOAT_COL_rtrim} float, {DOUBLE_COL_rtrim} double, {BOOL_COL_rtrim} bool,
                 {BINARY_COL_rtrim} binary(16), {NCHAR_COL_rtrim} nchar(32), {TS_COL_rtrim} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL_rtrim} int, {BINT_COL_rtrim} bigint, {SINT_COL_rtrim} smallint, {TINT_COL_rtrim} tinyint,
                 {FLOAT_COL_rtrim} float, {DOUBLE_COL_rtrim} double, {BOOL_COL_rtrim} bool,
                 {BINARY_COL_rtrim} binary(16), {NCHAR_COL_rtrim} nchar(32), {TS_COL_rtrim} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_rtrim(self, rows, dbname="db"):
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


    def test_fun_sca_rtrim(self):
        """ Fun: rtrim()

        1. RTRIM on super/child/normal table
        2. RTRIM between all data types
        3. RTRIM with null values
        4. RTRIM constant operation
        5. RTRIM with invalid parameters
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_rtrim.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_trim_substr.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_rtrim()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data_rtrim(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_rtrim()

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_rtrim()

        #tdSql.init(conn.cursor(), logSql)

    def __substr_condition(self):  # sourcery skip: extract-method
        substr_condition = []
        for char_col in CHAR_COL_substr:
            substr_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            substr_condition.extend( f"cast( {num_col} as binary(16) ) " for num_col in NUM_COL_substr)
            substr_condition.extend( f"cast( {char_col} + {num_col} as binary(16) ) " for num_col in NUM_COL_substr )
            substr_condition.extend( f"concat( cast( {char_col} + {num_col} as binary(16) ), {char_col}) " for num_col in NUM_COL_substr )
            substr_condition.extend( f"cast( {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_substr )
            substr_condition.extend( f"cast( {char_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_substr )
            substr_condition.extend( f"cast( {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_substr )
            # substr_condition.extend( f"cast( {char_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_substr )
            substr_condition.extend( f"cast( {char_col} + {char_col_2} as binary(16) ) " for char_col_2 in CHAR_COL_substr )
            substr_condition.extend( f"concat( {char_col}, {char_col_2} ) " for char_col_2 in CHAR_COL_substr )

        for num_col in NUM_COL_substr:
            substr_condition.extend( f"cast( {num_col} + {bool_col} as binary(16) )" for bool_col in BOOLEAN_COL_substr )
            substr_condition.extend( f"cast( {num_col} + {ts_col} as binary(16) )" for ts_col in TS_TYPE_COL_substr if num_col is not FLOAT_COL_substr and num_col is not DOUBLE_COL_substr)

        substr_condition.extend( f"cast( {bool_col} + {ts_col} as binary(16) )" for bool_col in BOOLEAN_COL_substr for ts_col in TS_TYPE_COL_substr )

        substr_condition.append(''' "   test1234!@#$%^&*()  :'><?/.,][}{   " ''')

        return substr_condition


    def __where_condition_substr(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_substr(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __substr_check(self, tbname,pos, lens=None):
        substr_condition = self.__substr_condition()
        for condition in substr_condition:
            where_condition = self.__where_condition_substr(condition)
            substr_group_having = self.__group_condition_substr(condition, having=f"{condition} is not null " )
            substr_group_no_having= self.__group_condition_substr(condition)
            groups = ["", substr_group_having, substr_group_no_having]

            if  pos < 1:
                tdSql.query(f"select substr( {condition}, {pos}, {lens}) , {condition} from {tbname} ")
                break

            tdSql.query(f"select substr( {condition}, {pos}, {lens}) , {condition} from {tbname} ")
            for j in range(tdSql.queryRows):
                tdSql.checkData(j,0, tdSql.getData(j,1)[pos-1:lens]) if tdSql.getData(j,1) else tdSql.checkData(j, 0, None)

            [ tdSql.query(f"select substr({condition}, {pos}, {lens})  from {tbname} {where_condition}  {group} ") for group in groups ]


    def __substr_err_check(self,tbname):
        sqls = []

        for num_col in NUM_COL_substr:
            sqls.extend(
                (
                    f"select substr( {num_col} ) from {tbname} ",
                    f"select substr(ceil( {num_col} )) from {tbname} ",
                    f"select {num_col} from {tbname} group by substr( {num_col} ) ",
                )
            )

            sqls.extend( f"select substr( {char_col} + {num_col} ) from {tbname} " for char_col in CHAR_COL_substr )
            sqls.extend( f"select substr( {num_col} , {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_substr )
            sqls.extend( f"select substr( {num_col} , {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL_substr )

        sqls.extend( f"select substr( {ts_col}+{bool_col} ) from {tbname} " for ts_col in TS_TYPE_COL_substr for bool_col in BOOLEAN_COL_substr )
        sqls.extend( f"select substr( {num_col}+{ts_col} ) from {tbname} " for num_col in NUM_COL_substr for ts_col in TS_TYPE_COL_substr)
        sqls.extend( f"select substr( {num_col}+ {bool_col} ) from {tbname} " for num_col in NUM_COL_substr for bool_col in BOOLEAN_COL_substr)
        sqls.extend( f"select substr( {num_col}+ {num_col} ) from {tbname} " for num_col in NUM_COL_substr for num_col in NUM_COL_substr)
        sqls.extend( f"select substr( {ts_col}+{ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_substr for ts_col in TS_TYPE_COL_substr )
        sqls.extend( f"select substr( {bool_col}+ {bool_col} ) from {tbname} " for bool_col in BOOLEAN_COL_substr for bool_col in BOOLEAN_COL_substr )

        sqls.extend( f"select substr( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_substr for char_col_2 in CHAR_COL_substr )
        sqls.extend( f"select substr({num_col}, '1') from {tbname} " for num_col in NUM_COL_substr )
        sqls.extend( f"select substr({ts_col}, '1') from {tbname} " for ts_col in TS_TYPE_COL_substr )
        sqls.extend( f"select substr({bool_col}, '1') from {tbname} " for bool_col in BOOLEAN_COL_substr )
        sqls.extend(
            (
                f"select substr() from {tbname} ",
                f"select substr(*) from {tbname} ",
                f"select substr(ccccccc) from {tbname} ",
                f"select substr(111) from {tbname} ",
            )
        )

        return sqls


    def __test_current_substr(self, dbname="db"):  # sourcery skip: use-itertools-product
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__substr_check(tb, CURRENT_POS_substr, LENS_substr)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_substr(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__substr_err_check(tb):
                tdSql.error(sql=errsql)
            self.__substr_check(tb, ERR_POS_substr, LENS_substr)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test_substr(self, dbname="db"):
        self.__test_current_substr(dbname)
        self.__test_error_substr(dbname)


    def __create_tb_substr(self, dbname="db"):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_substr} int, {BINT_COL_substr} bigint, {SINT_COL_substr} smallint, {TINT_COL_substr} tinyint,
                 {FLOAT_COL_substr} float, {DOUBLE_COL_substr} double, {BOOL_COL_substr} bool,
                 {BINARY_COL_substr} binary(16), {NCHAR_COL_substr} nchar(32), {TS_COL_substr} timestamp
            ) tags (tag1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL_substr} int, {BINT_COL_substr} bigint, {SINT_COL_substr} smallint, {TINT_COL_substr} tinyint,
                 {FLOAT_COL_substr} float, {DOUBLE_COL_substr} double, {BOOL_COL_substr} bool,
                 {BINARY_COL_substr} binary(16), {NCHAR_COL_substr} nchar(32), {TS_COL_substr} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_substr(self, rows, dbname="db"):
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


    def test_fun_sca_substr(self):
        """ Fun: substr()

        1. SUBSTR on super/child/normal table
        2. SUBSTR between all data types
        3. SUBSTR with null values
        4. SUBSTR constant operation
        5. SUBSTR with invalid parameters
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_substr.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_trim_substr.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_substr()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data_substr(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_substr()

        tdSql.execute("flush database db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_substr()


    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_tb_regexp(self, dbname="db"):
        tdSql.execute(f"""CREATE STABLE {dbname}.st (
            ts TIMESTAMP, vc VARCHAR(128), nc NCHAR(64), iv INT
        ) TAGS (t INT)""")
        tdSql.execute(f"CREATE TABLE {dbname}.ct1 USING {dbname}.st TAGS(1)")
        tdSql.execute(f"CREATE TABLE {dbname}.ct2 USING {dbname}.st TAGS(2)")
        tdSql.execute(f"""CREATE TABLE {dbname}.nt (
            ts TIMESTAMP, vc VARCHAR(128), nc NCHAR(64), iv INT
        )""")


    def _insert_data_regexp(self, dbname="db"):
        now = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        # ct1: log-style rows + one NULL row
        ct1_rows = [
            (now - 4000, "'code=42,type=DISK_FULL'", "'code=42,type=DISK_FULL'", 42),
            (now - 3000, "'code=7,type=LOW_MEM'",   "'code=7,type=LOW_MEM'",     7),
            (now - 2000, "'code=0,type=OK'",         "'code=0,type=OK'",          0),
            (now - 1000, "NULL",                     "NULL",                      "NULL"),
        ]
        for ts, vc, nc, iv in ct1_rows:
            tdSql.execute(f"INSERT INTO {dbname}.ct1 VALUES({ts}, {vc}, {nc}, {iv})")
        # ct2: URL-style rows
        ct2_rows = [
            (now - 3000, "'https://example.com'", "'https://example.com'", 1),
            (now - 2000, "'http://api.example.org'", "'http://api.example.org'", 2),
            (now - 1000, "'ftp://files.example.net'", "'ftp://files.example.net'", 3),
        ]
        for ts, vc, nc, iv in ct2_rows:
            tdSql.execute(f"INSERT INTO {dbname}.ct2 VALUES({ts}, {vc}, {nc}, {iv})")
        # nt: same as ct1
        for ts, vc, nc, iv in ct1_rows:
            tdSql.execute(f"INSERT INTO {dbname}.nt VALUES({ts}, {vc}, {nc}, {iv})")


    def _check_basic_regexp(self, dbname="db"):
        # -----------------------------------------------------------------
        # §1  Default group_idx=1 — no-table queries
        # -----------------------------------------------------------------
        # RXE-BASIC-001: single capture group → group 1
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-BASIC-002: multiple capture groups, default → group 1 only
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-BASIC-003: no capture group, default group_idx=1 → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', 'b')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §2  group_idx=0 whole match
        # -----------------------------------------------------------------
        # RXE-GRP0-001: no capture group, group_idx=0 → whole match
        tdSql.query("SELECT REGEXP_EXTRACT('abc', 'b', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-GRP0-002: with capture group, group_idx=0 → whole match ≠ group 1
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)c', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'bc')

        # RXE-GRP0-003: no match, group_idx=0 → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', 'x+', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §3  Multiple capture groups by explicit index
        # -----------------------------------------------------------------
        # RXE-GRP-001: explicit group_idx=1 → group 1
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-GRP-002: explicit group_idx=2 → group 2
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'c')

        # RXE-GRP-003: group_idx out of range → NULL, no error
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)(c)', 3)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §4  NULL and no-match
        # -----------------------------------------------------------------
        # RXE-NULL-001: str=NULL → NULL
        tdSql.query("SELECT REGEXP_EXTRACT(NULL, '(a+)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-002: no match → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(x+)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-003: multiple matches, only first (leftmost) returned
        tdSql.query("SELECT REGEXP_EXTRACT('a1b2', '([0-9])')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1')

        # RXE-NULL-004: str=NULL with group_idx=0 → NULL
        tdSql.query("SELECT REGEXP_EXTRACT(NULL, 'a+', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-005: explicit NULL group_idx → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '(b)', NULL)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-006: non-participating group in alternation → NULL
        # pattern '(a)|(b)' matches 'b' via group 2; group 1 did not participate
        tdSql.query("SELECT REGEXP_EXTRACT('b', '(a)|(b)', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-NULL-007: participating group 2 returns matched content
        tdSql.query("SELECT REGEXP_EXTRACT('b', '(a)|(b)', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'b')

        # RXE-NULL-008: pattern=NULL → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('abc', NULL)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # -----------------------------------------------------------------
        # §5  Empty string scenarios
        # -----------------------------------------------------------------
        # RXE-EMPTY-001: capture group matches empty string → '' (not NULL)
        tdSql.query("SELECT REGEXP_EXTRACT('ac', '(b?)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        # RXE-EMPTY-002: empty input str with zero-length match → ''
        tdSql.query("SELECT REGEXP_EXTRACT('', '(a*)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        # -----------------------------------------------------------------
        # §6  Table queries — per-row scalar behavior
        # -----------------------------------------------------------------
        # RXE-TBL-001: extract numeric code — multiple rows match 'code=([0-9]+)';
        # verify row-by-row extraction for 42, 7, 0, and NULL propagation
        tdSql.query(f"SELECT REGEXP_EXTRACT(vc, 'code=([0-9]+)') FROM {dbname}.ct1 ORDER BY ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '42')
        tdSql.checkData(1, 0, '7')
        tdSql.checkData(2, 0, '0')
        tdSql.checkData(3, 0, None)   # NULL row → NULL

        # RXE-TBL-002: NULL column row → NULL; non-NULL rows → extracted value
        tdSql.query(f"SELECT REGEXP_EXTRACT(vc, 'type=([A-Z_]+)') FROM {dbname}.ct1 ORDER BY ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 'DISK_FULL')
        tdSql.checkData(1, 0, 'LOW_MEM')
        tdSql.checkData(2, 0, 'OK')
        tdSql.checkData(3, 0, None)

        # RXE-TBL-003: empty table → 0 rows, no error
        tdSql.execute(f"CREATE TABLE IF NOT EXISTS {dbname}.empty_t (ts TIMESTAMP, vc VARCHAR(64))")
        tdSql.query(f"SELECT REGEXP_EXTRACT(vc, '([0-9]+)') FROM {dbname}.empty_t")
        tdSql.checkRows(0)

        # -----------------------------------------------------------------
        # §7  WHERE clause
        # -----------------------------------------------------------------
        # RXE-WHERE-001: IS NOT NULL filters to rows with a match
        tdSql.query(f"SELECT vc FROM {dbname}.ct1 "
                    "WHERE REGEXP_EXTRACT(vc, 'code=([4-9][0-9]+)') IS NOT NULL "
                    "ORDER BY ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'code=42,type=DISK_FULL')

        # RXE-WHERE-002: equality on extracted scheme value
        tdSql.query(f"SELECT vc FROM {dbname}.ct2 "
                    "WHERE REGEXP_EXTRACT(vc, '(https?)://') = 'https' "
                    "ORDER BY ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'https://example.com')

        # -----------------------------------------------------------------
        # §8  NCHAR column: extraction result equals VARCHAR equivalent
        # -----------------------------------------------------------------
        # RXE-NCHAR-001: NCHAR input yields same extracted value as VARCHAR
        tdSql.query(f"SELECT REGEXP_EXTRACT(nc, 'code=([0-9]+)') FROM {dbname}.ct1 ORDER BY ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '42')
        tdSql.checkData(1, 0, '7')
        tdSql.checkData(2, 0, '0')
        tdSql.checkData(3, 0, None)

        # -----------------------------------------------------------------
        # §9  Subquery with GROUP BY
        # -----------------------------------------------------------------
        # RXE-SUB-001: group by extracted URL scheme
        tdSql.query(f"""SELECT scheme, COUNT(*) AS cnt
            FROM (SELECT REGEXP_EXTRACT(vc, '(https?)://') AS scheme FROM {dbname}.ct2) t
            WHERE scheme IS NOT NULL
            GROUP BY scheme
            ORDER BY scheme""")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'http')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 'https')
        tdSql.checkData(1, 1, 1)

        # -----------------------------------------------------------------
        # §10  ERE features (character class, anchors, case sensitivity)
        # -----------------------------------------------------------------
        # RXE-RE-001: character class extracts decimal number
        tdSql.query("SELECT REGEXP_EXTRACT('v=3.14', '([0-9]+\\.[0-9]+)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '3.14')

        # RXE-RE-002a: anchor ^ matches at start → 'a'
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '^(a)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        # RXE-RE-002b: anchor ^ requires position 0; 'x' blocks match → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('xabc', '^(a)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-RE-003a: case-sensitive by default → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('ABC', '(abc)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-RE-003b: LOWER() enables case-insensitive extraction → 'abc'
        tdSql.query("SELECT REGEXP_EXTRACT(LOWER('ABC'), '(abc)')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'abc')


    def _check_error_regexp(self, dbname="db"):
        # -----------------------------------------------------------------
        # §11  Error cases
        # -----------------------------------------------------------------
        # RXE-ERR-001: too few arguments (1)
        tdSql.error("SELECT REGEXP_EXTRACT('abc')")

        # RXE-ERR-002: too many arguments (4)
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b)', 1, 0)")

        # RXE-ERR-003: str is non-string type (INT column)
        tdSql.error(f"SELECT REGEXP_EXTRACT(iv, '([0-9]+)') FROM {dbname}.ct1")

        # RXE-ERR-004: pattern is a column reference (not a constant)
        tdSql.error(f"SELECT REGEXP_EXTRACT(vc, vc) FROM {dbname}.ct1")

        # RXE-ERR-005: negative group_idx → translation-phase error
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b)', -1)")

        # RXE-ERR-006: invalid regex (unmatched parenthesis)
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b', 1)")

        # RXE-ERR-006b: non-ASCII patterns defer validation to the server
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '中[', 1)")

        # RXE-ERR-007: group_idx exceeds maximum (512)
        tdSql.error("SELECT REGEXP_EXTRACT('abc', '(b)', 513)")


    def _check_doc_examples_regexp(self):
        # -----------------------------------------------------------------
        # §12  Doc examples — verify the three queries from the user manual
        # -----------------------------------------------------------------
        # RXE-DOC-001: date string, group 1 → year
        tdSql.query("SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2026')

        # RXE-DOC-002: date string, group 0 → whole match
        tdSql.query("SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2026-04-22')

        # RXE-DOC-003: no match → NULL
        tdSql.query("SELECT REGEXP_EXTRACT('no-digits-here', '[0-9]+', 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)


    def _check_engine_consistency_regexp(self):
        """REGEXP engine consistency across platforms.

        The matching backend differs per platform: Linux matches with the POSIX
        ERE engine from the C library, Windows with PCRE2 (the Windows CRT engine
        counts bytes, so '.' could never match a multi byte character).  Pattern
        validation, in contrast, calls regcomp() on both platforms.  The cases
        below pin what each platform does today for the constructs in that
        overlap, so a change to either backend - or to the validation path -
        fails here instead of surfacing as a platform specific bug report.

        Nothing here is meant to make the two platforms agree; the two
        divergent cases document the current difference and are expected to be
        updated if the engines are ever unified.
            - 2026-11-20 Alex Merged into test_fun_sca_str_trim_substr.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        # -----------------------------------------------------------------
        # Cases the two engines already agree on: same expectation everywhere.
        # -----------------------------------------------------------------
        # RXE-ENG-001: two capture groups
        tdSql.query("SELECT REGEXP_EXTRACT('ab', '(a)(b)', 1), REGEXP_EXTRACT('ab', '(a)(b)', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')
        tdSql.checkData(0, 1, 'b')

        # RXE-ENG-002: greedy trailing group under anchors
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '^(a)(.*)$', 1), REGEXP_EXTRACT('abc', '^(a)(.*)$', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')
        tdSql.checkData(0, 1, 'bc')

        # RXE-ENG-003: '.' counts characters and anchors hold
        tdSql.query("SELECT REGEXP_EXTRACT('abc', '^a.c$', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'abc')

        # RXE-ENG-004: alternation whose branches cannot both match
        tdSql.query("SELECT REGEXP_EXTRACT('dog', 'cat|dog', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'dog')

        # RXE-ENG-005: POSIX character class inside a bracket expression
        tdSql.query("SELECT REGEXP_EXTRACT('x123y', '[[:digit:]]+', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '123')

        # RXE-ENG-006: optional quantifier
        tdSql.query("SELECT REGEXP_EXTRACT('color', 'colou?r', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'color')

        # RXE-ENG-007: repetition count that cannot match -> NULL
        tdSql.query("SELECT REGEXP_EXTRACT('1234', '^[0-9]{2,3}$', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        # RXE-ENG-008: which alternative participated in the match
        tdSql.query("SELECT REGEXP_EXTRACT('dog', '(cat)|(dog)', 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'dog')

        # -----------------------------------------------------------------
        # Known divergence: POSIX takes the leftmost-longest match, PCRE2 the
        # leftmost-first one.  'a|ab' against 'ab' therefore yields 'ab' on
        # Linux and 'a' on Windows.
        # -----------------------------------------------------------------
        if IS_WINDOWS_regexp:
            expected_alternation = 'a'
        else:
            expected_alternation = 'ab'
        tdSql.query("SELECT REGEXP_EXTRACT('ab', 'a|ab', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected_alternation)

        # -----------------------------------------------------------------
        # Known divergence: an escape such as '\\d' is a literal 'd' for POSIX
        # ERE (no match against '5') but a digit class for PCRE2.
        # -----------------------------------------------------------------
        if IS_WINDOWS_regexp:
            expected_digit_escape = '5'
        else:
            expected_digit_escape = None
        tdSql.query("SELECT REGEXP_EXTRACT('5', '\\\\d', 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected_digit_escape)


    def all_test_regexp(self, dbname="db"):
        self._check_basic_regexp(dbname)
        self._check_error_regexp(dbname)
        self._check_doc_examples_regexp()
        self._check_engine_consistency_regexp()


    def test_fun_sca_regexp_extract(self):
        """Fun: regexp_extract()

        1. regexp_extract default group_idx=1 returns first capture group
        2. regexp_extract group_idx=0 returns whole match substring
        3. regexp_extract with explicit group index (1, 2, out-of-range)
        4. regexp_extract NULL input (str, pattern, group_idx) and no-match return NULL
        5. regexp_extract capture group matching empty string returns ''
        6. regexp_extract on table columns with per-row scalar semantics
        7. regexp_extract in WHERE clause for row filtering
        8. regexp_extract on NCHAR column (return type NCHAR)
        9. regexp_extract in subquery with GROUP BY
        10. regexp_extract POSIX ERE features: character class, anchors, case sensitivity
        11. regexp_extract invalid parameter error cases (including group_idx > 512)
        12. regexp_extract user-manual doc examples

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-20 Stephen Created
            - 2026-11-20 Alex Merged into test_fun_sca_str_trim_substr.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        dbname = "db"
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self._create_tb_regexp(dbname)

        tdLog.printNoPrefix("==========step2:insert data")
        self._insert_data_regexp(dbname)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_regexp(dbname)

        tdSql.execute(f"flush database {dbname}")

        tdLog.printNoPrefix("==========step4:after wal, all check again")
        self.all_test_regexp(dbname)
