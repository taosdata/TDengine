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

import datetime
from new_test_framework.utils import tdLog, tdSql


INT_COL_length     = "c1"


BINT_COL_length    = "c2"


SINT_COL_length    = "c3"


TINT_COL_length    = "c4"


FLOAT_COL_length   = "c5"


DOUBLE_COL_length  = "c6"


BOOL_COL_length    = "c7"


BINARY_COL_length  = "c8"


NCHAR_COL_length   = "c9"


TS_COL_length      = "c10"


UN_CHAR_COL_length = [INT_COL_length, BINT_COL_length, SINT_COL_length, TINT_COL_length, FLOAT_COL_length, DOUBLE_COL_length, BOOL_COL_length, ]


CHAR_COL_length    = [ BINARY_COL_length, NCHAR_COL_length, ]


TS_TYPE_COL_length = [TS_COL_length]


DBNAME_length = "db"


INT_COL_char_length     = "c1"


BINT_COL_char_length    = "c2"


SINT_COL_char_length    = "c3"


TINT_COL_char_length    = "c4"


FLOAT_COL_char_length   = "c5"


DOUBLE_COL_char_length  = "c6"


BOOL_COL_char_length    = "c7"


BINARY_COL_char_length  = "c8"


NCHAR_COL_char_length   = "c9"


TS_COL_char_length      = "c10"


UN_CHAR_COL_char_length = [INT_COL_char_length, BINT_COL_char_length, SINT_COL_char_length, TINT_COL_char_length, FLOAT_COL_char_length, DOUBLE_COL_char_length, BOOL_COL_char_length, ]


CHAR_COL_char_length    = [ BINARY_COL_char_length, NCHAR_COL_char_length, ]


TS_TYPE_COL_char_length = [TS_COL_char_length]


INT_COL_lower     = "c1"


BINT_COL_lower    = "c2"


SINT_COL_lower    = "c3"


TINT_COL_lower    = "c4"


FLOAT_COL_lower   = "c5"


DOUBLE_COL_lower  = "c6"


BOOL_COL_lower    = "c7"


BINARY_COL_lower  = "c8"


NCHAR_COL_lower   = "c9"


TS_COL_lower      = "c10"


UN_CHAR_COL_lower = [INT_COL_lower, BINT_COL_lower, SINT_COL_lower, TINT_COL_lower, FLOAT_COL_lower, DOUBLE_COL_lower, BOOL_COL_lower, ]


CHAR_COL_lower    = [ BINARY_COL_lower, NCHAR_COL_lower, ]


TS_TYPE_COL_lower = [TS_COL_lower]


INT_COL_upper     = "c1"


BINT_COL_upper    = "c2"


SINT_COL_upper    = "c3"


TINT_COL_upper    = "c4"


FLOAT_COL_upper   = "c5"


DOUBLE_COL_upper  = "c6"


BOOL_COL_upper    = "c7"


BINARY_COL_upper  = "c8"


NCHAR_COL_upper   = "c9"


TS_COL_upper      = "c10"


UN_CHAR_COL_upper = [INT_COL_upper, BINT_COL_upper, SINT_COL_upper, TINT_COL_upper, FLOAT_COL_upper, DOUBLE_COL_upper, BOOL_COL_upper, ]


CHAR_COL_upper    = [ BINARY_COL_upper, NCHAR_COL_upper, ]


TS_TYPE_COL_upper = [TS_COL_upper]


class TestFunScaStrLengthCase:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")

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


    def __length_condition(self):
        length_condition = []
        for char_col in CHAR_COL_length:
            length_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            length_condition.extend( f"cast( {un_char_col} as binary(16) ) " for un_char_col in UN_CHAR_COL_length)
            length_condition.extend( f"cast( {char_col} + {char_col_2} as binary(32) ) " for char_col_2 in CHAR_COL_length )
            length_condition.extend( f"cast( {char_col} + {un_char_col} as binary(32) ) " for un_char_col in UN_CHAR_COL_length )

        length_condition.append('''"test1234!@#$%^&*():'><?/.,][}{"''')

        return length_condition


    def __where_condition_length(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_length(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __length_current_check(self, tbname):
        length_condition = self.__length_condition()
        for condition in length_condition:
            where_condition = self.__where_condition_length(condition)
            group_having = self.__group_condition_length(condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition_length(condition )
            groups = ["", group_having, group_no_having]

            for group_condition in groups:
                tdSql.query(f"select {condition}, length( {condition} ) from {tbname} {where_condition}  {group_condition} ")
                for i in range(tdSql.queryRows):
                    if not tdSql.getData(i,1):
                        tdSql.checkData(i, 1, None)
                    elif "as nchar" in condition or (NCHAR_COL_length in condition and "as binary" not in condition):
                        tdSql.checkData(i, 1, len(str(tdSql.getData(i,0) ) ) * 4 )
                    else:
                        tdSql.checkData(i, 1, len(str(tdSql.getData(i,0) ) ) )


    def __length_err_check(self,tbname):
        sqls = []

        for un_char_col in UN_CHAR_COL_length:
            sqls.extend(
                (
                    f"select length( {un_char_col} ) from {tbname} ",
                    f"select length(ceil( {un_char_col} )) from {tbname} ",
                    f"select {un_char_col} from {tbname} group by length( {un_char_col} ) ",
                )
            )

            sqls.extend( f"select length( {un_char_col} + {un_char_col_2} ) from {tbname} " for un_char_col_2 in UN_CHAR_COL_length )
            sqls.extend( f"select length( {un_char_col} + {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_length )

        sqls.extend( f"select {char_col} from {tbname} group by length( {char_col} ) " for char_col in CHAR_COL_length)
        sqls.extend( f"select length( {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_length )
        sqls.extend( f"select length( {char_col} + {ts_col} ) from {tbname} " for char_col in UN_CHAR_COL_length for ts_col in TS_TYPE_COL_length)
        sqls.extend( f"select length( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_length for char_col_2 in CHAR_COL_length )
        sqls.extend( f"select upper({char_col}, 11) from {tbname} " for char_col in CHAR_COL_length )
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


    def __test_current_length(self, dbname=DBNAME_length):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__length_current_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_length(self, dbname=DBNAME_length):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__length_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test_length(self):
        self.__test_current_length()
        self.__test_error_length()


    def __create_tb_length(self, dbname=DBNAME_length):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_length} int, {BINT_COL_length} bigint, {SINT_COL_length} smallint, {TINT_COL_length} tinyint,
                 {FLOAT_COL_length} float, {DOUBLE_COL_length} double, {BOOL_COL_length} bool,
                 {BINARY_COL_length} binary(16), {NCHAR_COL_length} nchar(32), {TS_COL_length} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.nt1(
                ts timestamp, {INT_COL_length} int, {BINT_COL_length} bigint, {SINT_COL_length} smallint, {TINT_COL_length} tinyint,
                 {FLOAT_COL_length} float, {DOUBLE_COL_length} double, {BOOL_COL_length} bool,
                 {BINARY_COL_length} binary(16), {NCHAR_COL_length} nchar(32), {TS_COL_length} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_length(self, rows, dbname=DBNAME_length):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into {dbname}.ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000+ 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_limit-1", { now_time - 86400000}
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_limit-2", { now_time - 172800000}
                )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000+ 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_limit-2", { now_time - 172800000 }
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into {dbname}.nt1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into {dbname}.nt1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_limit-2", { now_time - 172800000 }
                )
            '''
        )


    def test_fun_sca_length(self):
        """ Fun: length()

        1. LENGTH on super/child/normal table
        2. LENGTH between all data types
        3. LENGTH with null values
        4. LENGTH constant operation
        5. LENGTH with invalid parameters
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_length.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_length_case.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_length()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data_length(10)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_length()

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_length()


    def __char_length_condition(self):
        char_length_condition = []
        for char_col in CHAR_COL_char_length:
            char_length_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            char_length_condition.extend( f"cast( {un_char_col} as binary(16) ) " for un_char_col in UN_CHAR_COL_char_length)
            char_length_condition.extend( f"cast( {char_col} + {char_col_2} as binary(32) ) " for char_col_2 in CHAR_COL_char_length )
            char_length_condition.extend( f"cast( {char_col} + {un_char_col} as binary(32) ) " for un_char_col in UN_CHAR_COL_char_length )

        char_length_condition.append('''"test1234!@#$%^&*():'><?/.,][}{"''')

        return char_length_condition


    def __where_condition_char_length(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_char_length(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __char_length_current_check(self, tbname):
        char_length_condition = self.__char_length_condition()
        for condition in char_length_condition:
            where_condition = self.__where_condition_char_length(condition)
            group_having = self.__group_condition_char_length(condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition_char_length(condition )
            groups = ["", group_having, group_no_having]

            for group_condition in groups:
                tdSql.query(f"select {condition}, char_length( {condition} ) from {tbname} {where_condition}  {group_condition} ")
                for i in range(tdSql.queryRows):
                    if not tdSql.getData(i,1):
                        tdSql.checkData(i, 1, None)
                    # elif "as nchar" in condition or (NCHAR_COL_char_length in condition and "as binary" not in condition):
                    #     tdSql.checkData(i, 1, len(str(tdSql.getData(i,0) ) ) * 4 )
                    else:
                        tdSql.checkData(i, 1, len(str(tdSql.getData(i,0) ) ) )


    def __char_length_err_check(self,tbname):
        sqls = []

        for un_char_col in UN_CHAR_COL_char_length:
            sqls.extend(
                (
                    f"select char_length( {un_char_col} ) from {tbname} ",
                    f"select char_length(ceil( {un_char_col} )) from {tbname} ",
                    f"select {un_char_col} from {tbname} group by char_length( {un_char_col} ) ",
                )
            )

            sqls.extend( f"select char_length( {un_char_col} + {un_char_col_2} ) from {tbname} " for un_char_col_2 in UN_CHAR_COL_char_length )
            sqls.extend( f"select char_length( {un_char_col} + {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_char_length )

        sqls.extend( f"select {char_col} from {tbname} group by char_length( {char_col} ) " for char_col in CHAR_COL_char_length)
        sqls.extend( f"select char_length( {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_char_length )
        sqls.extend( f"select char_length( {char_col} + {ts_col} ) from {tbname} " for char_col in UN_CHAR_COL_char_length for ts_col in TS_TYPE_COL_char_length)
        sqls.extend( f"select char_length( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_char_length for char_col_2 in CHAR_COL_char_length )
        sqls.extend( f"select upper({char_col}, 11) from {tbname} " for char_col in CHAR_COL_char_length )
        sqls.extend(
            (
                f"select char_length() from {tbname} ",
                f"select char_length(*) from {tbname} ",
                f"select char_length(ccccccc) from {tbname} ",
                f"select char_length(111) from {tbname} ",
                f"select char_length(c8, 11) from {tbname} ",
            )
        )

        return sqls


    def __test_current_char_length(self, dbname="db"):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__char_length_current_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_char_length(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__char_length_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")



    def all_test_char_length(self):
        self.__test_current_char_length()
        self.__test_error_char_length()



    def __create_tb_char_length(self, dbname="db"):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_char_length} int, {BINT_COL_char_length} bigint, {SINT_COL_char_length} smallint, {TINT_COL_char_length} tinyint,
                 {FLOAT_COL_char_length} float, {DOUBLE_COL_char_length} double, {BOOL_COL_char_length} bool,
                 {BINARY_COL_char_length} binary(16), {NCHAR_COL_char_length} nchar(32), {TS_COL_char_length} timestamp
            ) tags (t_int int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL_char_length} int, {BINT_COL_char_length} bigint, {SINT_COL_char_length} smallint, {TINT_COL_char_length} tinyint,
                 {FLOAT_COL_char_length} float, {DOUBLE_COL_char_length} double, {BOOL_COL_char_length} bool,
                 {BINARY_COL_char_length} binary(16), {NCHAR_COL_char_length} nchar(32), {TS_COL_char_length} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_char_length(self, rows, dbname="db"):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into {dbname}.ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000+ 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_limit-1", { now_time - 86400000}
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_limit-2", { now_time - 172800000}
                )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000+ 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_limit-2", { now_time - 172800000 }
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into {dbname}.t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_limit-2", { now_time - 172800000 }
                )
            '''
        )


    def test_char_length(self):
        """ Fun: char_length()

        1. char_length with conditions on character columns
        2. char_length with conditions on non-character columns
        3. char_length with group by and having
        4. char_length error conditions
        5. char_length after wal log flush
        6. char_length on stable and normal table
        7. char_length with null values
        8. char_length with upper function
        9. char_length with cast function
        10. char_length with different data types
        11. char_length with boundary values
        12. char_length with multiple table types
        13. char_length with various data distributions
        14. char_length with different timestamp formats
        15. char_length with special characters in strings
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_char_length.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_length_case.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_char_length()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data_char_length(10)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_char_length()

        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_char_length()


    def __lower_condition(self):
        lower_condition = []
        for char_col in CHAR_COL_lower:
            lower_condition.extend(
                (
                    char_col,
                    f"upper( {char_col} )",
                )
            )
            lower_condition.extend(f"cast( {un_char_col} as binary(16) ) " for un_char_col in UN_CHAR_COL_lower)
            lower_condition.extend( f"cast( {char_col} + {char_col_2} as binary(32) ) " for char_col_2 in CHAR_COL_lower )
            lower_condition.extend( f"cast( {char_col} + {un_char_col} as binary(32) ) " for un_char_col in UN_CHAR_COL_lower )

        lower_condition.append('''"test1234!@#$%^&*():'><?/.,][}{"''')

        return lower_condition


    def __where_condition_lower(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_lower(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __lower_current_check(self, tbname):
        lower_condition = self.__lower_condition()
        for condition in lower_condition:
            where_condition = self.__where_condition_lower(condition)
            group_having = self.__group_condition_lower(condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition_lower(condition )
            groups = ["", group_having, group_no_having]

            for group_condition in groups:
                tdSql.query(f"select lower( {condition} ), {condition} from {tbname} {where_condition}  {group_condition}")
                for i in range(tdSql.queryRows):
                    tdSql.checkData(i, 0, str(tdSql.getData(i, 1)).lower() ) if tdSql.getData(i, 1) else tdSql.checkData(i, 0, None)


    def __lower_err_check(self,tbname):
        sqls = []

        for un_char_col in UN_CHAR_COL_lower:
            sqls.extend(
                (
                    f"select lower( {un_char_col} ) from {tbname} ",
                    f"select lower(ceil( {un_char_col} )) from {tbname} ",
                    f"select {un_char_col} from {tbname} group by lower( {un_char_col} ) ",
                )
            )

            sqls.extend( f"select lower( {un_char_col} + {un_char_col_2} ) from {tbname} " for un_char_col_2 in UN_CHAR_COL_lower )
            sqls.extend( f"select lower( {un_char_col} + {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_lower )

        sqls.extend( f"select {char_col} from {tbname} group by lower( {char_col} ) " for char_col in CHAR_COL_lower)
        sqls.extend( f"select lower( {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_lower )
        sqls.extend( f"select lower( {char_col} + {ts_col} ) from {tbname} " for char_col in UN_CHAR_COL_lower for ts_col in TS_TYPE_COL_lower)
        sqls.extend( f"select lower( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_lower for char_col_2 in CHAR_COL_lower )
        sqls.extend( f"select upper({char_col}, 11) from {tbname} " for char_col in CHAR_COL_lower )
        sqls.extend(
            (
                f"select lower() from {tbname} ",
                f"select lower(*) from {tbname} ",
                f"select lower(ccccccc) from {tbname} ",
                f"select lower(111) from {tbname} ",
                f"select lower(c8, 11) from {tbname} ",
            )
        )

        return sqls


    def __test_current_lower(self, dbname="db"):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__lower_current_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_lower(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__lower_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test_lower(self, dbname="db"):
        self.__test_current_lower(dbname)
        self.__test_error_lower(dbname)


    def __create_tb_lower(self, dbname="db"):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_lower} int, {BINT_COL_lower} bigint, {SINT_COL_lower} smallint, {TINT_COL_lower} tinyint,
                 {FLOAT_COL_lower} float, {DOUBLE_COL_lower} double, {BOOL_COL_lower} bool,
                 {BINARY_COL_lower} binary(16), {NCHAR_COL_lower} nchar(32), {TS_COL_lower} timestamp
            ) tags (tag1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL_lower} int, {BINT_COL_lower} bigint, {SINT_COL_lower} smallint, {TINT_COL_lower} tinyint,
                 {FLOAT_COL_lower} float, {DOUBLE_COL_lower} double, {BOOL_COL_lower} bool,
                 {BINARY_COL_lower} binary(16), {NCHAR_COL_lower} nchar(32), {TS_COL_lower} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_lower(self, rows, dbname="db"):
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


    def test_fun_sca_lower(self):
        """ Fun: lower()

        1. LOWER on super/child/normal table
        2. LOWER between all data types
        3. LOWER with null values
        4. LOWER constant operation
        5. LOWER with invalid parameters
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-23 Alex Duan Migrated from uncatalog/system-test/2-query/test_lower.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_length_case.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_lower()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data_lower(10)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_lower()

        tdSql.execute("flush database db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_lower()

        #tdSql.init(conn.cursor(), logSql)

    def __upper_condition(self):
        upper_condition = []
        for char_col in CHAR_COL_upper:
            upper_condition.extend(
                (
                    char_col,
                    f"lower( {char_col} )",
                )
            )
            upper_condition.extend(f"cast( {un_char_col} as binary(16) ) " for un_char_col in UN_CHAR_COL_upper)
            upper_condition.extend( f"cast( {char_col} + {char_col_2} as binary(32) ) " for char_col_2 in CHAR_COL_upper )
            upper_condition.extend( f"cast( {char_col} + {un_char_col} as binary(32) ) " for un_char_col in UN_CHAR_COL_upper )

        upper_condition.append('''"test1234!@#$%^&*():'><?/.,][}{"''')

        return upper_condition


    def __where_condition_upper(self, col):
        # return f" where count({col}) > 0 "
        return ""


    def __group_condition_upper(self, col, having = ""):
        return f" group by {col} having {having}" if having else f" group by {col} "


    def __upper_current_check(self, tbname):
        upper_condition = self.__upper_condition()
        for condition in upper_condition:
            where_condition = self.__where_condition_upper(condition)
            group_having = self.__group_condition_upper(condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition_upper(condition )
            groups = ["", group_having, group_no_having]

            for group_condition in groups:
                tdSql.query(f"select upper( {condition} ), {condition} from {tbname} {where_condition}  {group_condition}")
                for i in range(tdSql.queryRows):
                    tdSql.checkData(i, 0, str(tdSql.getData(i, 1)).upper() ) if tdSql.getData(i, 1) else tdSql.checkData(i, 0, None)


    def __upper_err_check(self,tbname):
        sqls = []

        for un_char_col in UN_CHAR_COL_upper:
            sqls.extend(
                (
                    f"select upper( {un_char_col} ) from {tbname} ",
                    f"select upper(ceil( {un_char_col} )) from {tbname} ",
                    f"select {un_char_col} from {tbname} group by upper( {un_char_col} ) ",
                )
            )

            sqls.extend( f"select upper( {un_char_col} + {un_char_col_2} ) from {tbname} " for un_char_col_2 in UN_CHAR_COL_upper )
            sqls.extend( f"select upper( {un_char_col} + {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_upper )

        sqls.extend( f"select {char_col} from {tbname} group by upper( {char_col} ) " for char_col in CHAR_COL_upper)
        sqls.extend( f"select upper( {ts_col} ) from {tbname} " for ts_col in TS_TYPE_COL_upper )
        sqls.extend( f"select upper( {char_col} + {ts_col} ) from {tbname} " for char_col in UN_CHAR_COL_upper for ts_col in TS_TYPE_COL_upper)
        sqls.extend( f"select upper( {char_col} + {char_col_2} ) from {tbname} " for char_col in CHAR_COL_upper for char_col_2 in CHAR_COL_upper )
        sqls.extend( f"select upper({char_col}, 11) from {tbname} " for char_col in CHAR_COL_upper )
        sqls.extend(
            (
                f"select upper() from {tbname} ",
                f"select upper(*) from {tbname} ",
                f"select upper(ccccccc) from {tbname} ",
                f"select upper(111) from {tbname} ",
            )
        )

        return sqls


    def __test_current_upper(self, dbname="db"):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]
        for tb in tbname:
            self.__upper_current_check(tb)
            tdLog.printNoPrefix(f"==========current sql condition check in {tb} over==========")


    def __test_error_upper(self, dbname="db"):
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.t1", f"{dbname}.stb1"]

        for tb in tbname:
            for errsql in self.__upper_err_check(tb):
                tdSql.error(sql=errsql)
            tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")


    def all_test_upper(self, dbname="db"):
        self.__test_current_upper(dbname)
        self.__test_error_upper(dbname)


    def __create_tb_upper(self, dbname="db"):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL_upper} int, {BINT_COL_upper} bigint, {SINT_COL_upper} smallint, {TINT_COL_upper} tinyint,
                 {FLOAT_COL_upper} float, {DOUBLE_COL_upper} double, {BOOL_COL_upper} bool,
                 {BINARY_COL_upper} binary(16), {NCHAR_COL_upper} nchar(32), {TS_COL_upper} timestamp
            ) tags (tag1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL_upper} int, {BINT_COL_upper} bigint, {SINT_COL_upper} smallint, {TINT_COL_upper} tinyint,
                 {FLOAT_COL_upper} float, {DOUBLE_COL_upper} double, {BOOL_COL_upper} bool,
                 {BINARY_COL_upper} binary(16), {NCHAR_COL_upper} nchar(32), {TS_COL_upper} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')


    def __insert_data_upper(self, rows, dbname="db"):
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


    def test_fun_sca_upper(self):
        """ Fun: upper()

        1. Query from super/child/normal table
        2. Query from cast covert result
        3. Query from group condition
        4. Query Error parameter
        5. Query after flush database
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_upper.py
            - 2026-11-20 Alex Merged into test_fun_sca_str_length_case.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb_upper()

        tdLog.printNoPrefix("==========step2:insert data")
        self.__insert_data_upper(10)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test_upper()

        tdSql.execute("flush database db")

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test_upper()
