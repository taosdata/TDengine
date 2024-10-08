###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import random
import string
import datetime

import taos
from taos import *
from stmt.common import StmtCommon
from taos.constants import FieldType
from ctypes import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase:
    def init(self, conn, log_sql, replica_var=1):
        self.replica_var = int(replica_var)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = "stmt_query_test_cases"
        self.stmt_common = StmtCommon()
        self.connectstmt = None

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    def newcon(self, host, cfg):
        user = "root"
        password = "taosdata"
        port = 6030
        con = taos.connect(host=host, user=user, password=password, config=cfg, port=port)
        tdLog.debug(con)
        return con

    def insert_common_table(self, ctable_name):
        self.connectstmt.execute(f"drop database if exists {self.dbname}")
        self.connectstmt.execute(f"create database if not exists {self.dbname}")
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute(f"create table if not exists {ctable_name} ( \
                                        ts timestamp, \
                                        c1 timestamp, \
                                        c2 int, \
                                        c3 int unsigned, \
                                        c4 bigint, \
                                        c5 bigint unsigned, \
                                        c6 float, \
                                        c7 double, \
                                        c8 binary(20), \
                                        c9 smallint, \
                                        c10 smallint unsigned, \
                                        c11 tinyint, \
                                        c12 tinyint unsigned, \
                                        c13 bool, \
                                        c14 nchar(20), \
                                        c15 varchar(20), \
                                        c16 geometry(100), \
                                        c17 varbinary(20) \
                                    );")

        for _ in range(1000):
            c1 = self.random_timestamp()
            c2 = random.randint(-2147483648, 2147483647)
            c3 = random.randint(0, 4294967295)
            c4 = random.randint(-9223372036854775808, 9223372036854775807)
            c5 = random.randint(0, 18446744073709551615)
            c6 = random.uniform(-1000, 1000)
            c7 = random.uniform(-100000, 100000)
            c8 = '0x' + ''.join(random.choices('0123456789abcdef', k=15))
            c9 = random.randint(-32768, 32767)
            c10 = random.randint(0, 65535)
            c11 = random.randint(-128, 127)
            c12 = random.randint(0, 255)
            c13 = random.choice([True, False])
            c14 = self.random_string(20)
            c15 = self.random_string(20)
            c16 = self.random_point()
            c17 = '0x' + ''.join(random.choices('0123456789abcdef', k=18))

            sql = f"insert into {ctable_name} (ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17) \
                    values (now, '{c1}', {c2}, {c3}, {c4}, {c5}, {c6:.2f}, {c7:.5f}, '{c8}', {c9}, {c10}, {c11}, {c12}, {c13}, '{c14}', '{c15}', '{c16}', '{c17}');"

            self.connectstmt.execute(sql)

        tdLog.debug("Insert common table successfully!")

    def insert_super_table(self, stablename, subtable_name):
        self.connectstmt.execute(f"drop database if exists {self.dbname}")
        self.connectstmt.execute(f"create database if not exists {self.dbname}")
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute(f"create stable if not exists {stablename} ( \
                                        ts timestamp, \
                                        c1 timestamp, \
                                        c2 int, \
                                        c3 int unsigned, \
                                        c4 bigint, \
                                        c5 bigint unsigned, \
                                        c6 float, \
                                        c7 double, \
                                        c8 binary(20), \
                                        c9 smallint, \
                                        c10 smallint unsigned, \
                                        c11 tinyint, \
                                        c12 tinyint unsigned, \
                                        c13 bool, \
                                        c14 nchar(20), \
                                        c15 varchar(20), \
                                        c16 geometry(100), \
                                        c17 varbinary(20) \
                                    ) tags ( \
                                        t1 int \
                                    );")

        self.connectstmt.execute(f"create table {subtable_name} using {stablename} tags(0);")

        for _ in range(1000):
            c1 = self.random_timestamp()
            c2 = random.randint(-2147483648, 2147483647)
            c3 = random.randint(0, 4294967295)
            c4 = random.randint(-9223372036854775808, 9223372036854775807)
            c5 = random.randint(0, 18446744073709551615)
            c6 = random.uniform(-1000, 1000)
            c7 = random.uniform(-100000, 100000)
            c8 = '0x' + ''.join(random.choices('0123456789abcdef', k=15))
            c9 = random.randint(-32768, 32767)
            c10 = random.randint(0, 65535)
            c11 = random.randint(-128, 127)
            c12 = random.randint(0, 255)
            c13 = random.choice([True, False])
            c14 = self.random_string(20)
            c15 = self.random_string(20)
            c16 = self.random_point()
            c17 = '0x' + ''.join(random.choices('0123456789abcdef', k=18))

            sql = f"insert into {subtable_name} (ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17) \
                    values (now, '{c1}', {c2}, {c3}, {c4}, {c5}, {c6:.2f}, {c7:.5f}, '{c8}', {c9}, {c10}, {c11}, {c12}, {c13}, '{c14}', '{c15}', '{c16}', '{c17}');"

            self.connectstmt.execute(sql)

        tdLog.debug("Insert super table successfully!")

    def random_string(self, length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def random_timestamp(self):
        start = datetime.datetime(2000, 1, 1)
        end = datetime.datetime(2025, 12, 31)
        return start + (end - start) * random.random()

    def random_point(self):
        x = random.uniform(-180, 180)
        y = random.uniform(-90, 90)
        return f'POINT({x} {y})'

    def test_query_table_temp(self, sql, types, datas):
        self.connectstmt.select_db(self.dbname)
        stmt2 = self.connectstmt.statement2(sql)
        stmt2.set_columns_type(types)

        for data in datas:
            try:
                stmt2.bind_param(None, None, data)
                stmt2.execute()
            except Exception as err:
                tdLog.exit(f"An error occurred, err: {err}, sql: {sql}")

            for subt_data in data:
                formatted_values = []
                for values in subt_data:
                    if isinstance(values[0], str):
                        formatted_values.append(f"'{values[0]}'")
                    else:
                        formatted_values.append(str(values[0]))

                query = sql
                for value in formatted_values:
                    query = query.replace('?', value, 1)

                self.stmt_common.compare_result(self.connectstmt, query, stmt2.result())

    def test_query_table(self, table_name):
        sql = f"select * from {table_name} where ts < ? and c1 < ?;"
        types = [FieldType.C_TIMESTAMP, FieldType.C_TIMESTAMP]
        datas = [
            [[['2025-11-11 20:10:10'], ['2025-02-20 11:11:11']]],
            [[['2025-01-01 00:00:00'], ['2024-12-31 23:59:59']]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c2 > ? and c3 < ?;"
        types = [FieldType.C_INT, FieldType.C_INT_UNSIGNED]
        datas = [
            [[[100], [200]]],
            [[[300], [400]]],
            [[[500], [600]]],
            [[[700], [800]]],
            [[[900], [1000]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c4 >= ? and c5 <= ?;"
        types = [FieldType.C_BIGINT, FieldType.C_BIGINT_UNSIGNED]
        datas = [
            [[[1000000000], [5000000000]]],
            [[[2000000000], [6000000000]]],
            [[[3000000000], [7000000000]]],
            [[[4000000000], [8000000000]]],
            [[[5000000000], [9000000000]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c6 = ? and c7 = ?;"
        types = [FieldType.C_FLOAT, FieldType.C_DOUBLE]
        datas = [
            [[[1.5], [2.5]]],
            [[[3.5], [4.5]]],
            [[[5.5], [6.5]]],
            [[[7.5], [8.5]]],
            [[[9.5], [10.5]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c14 like ?;"
        types = [FieldType.C_NCHAR]
        datas = [
            [[["abc%"]]],
            [[["def%"]]],
            [[["ghi%"]]],
            [[["jkl%"]]],
            [[["mno%"]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c13 = ?;"
        types = [FieldType.C_BOOL]
        datas = [
            [[[True]]],
            [[[False]]],
            [[[True]]],
            [[[False]]],
            [[[True]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c10 = ? and c12 = ?;"
        types = [FieldType.C_SMALLINT_UNSIGNED, FieldType.C_TINYINT_UNSIGNED]
        datas = [
            [[[10], [20]]],
            [[[30], [40]]],
            [[[50], [60]]],
            [[[70], [80]]],
            [[[90], [100]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c4 < ?;"
        types = [FieldType.C_BIGINT]
        datas = [
            [[[1000000000]]],
            [[[2000000000]]],
            [[[3000000000]]],
            [[[4000000000]]],
            [[[5000000000]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c2 = ? and c5 > ? and c9 < ?;"
        types = [FieldType.C_INT, FieldType.C_BIGINT_UNSIGNED, FieldType.C_SMALLINT]
        datas = [
            [[[1], [100000], [1000]]],
            [[[2], [200000], [2000]]],
            [[[3], [300000], [3000]]],
            [[[4], [400000], [4000]]],
            [[[5], [500000], [5000]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c8 = ?;"
        types = [FieldType.C_BINARY]
        datas = [
            [[["0x1234567890abcdef"]]],
            [[["0xabcdef1234567890"]]],
            [[["0xdeadbeef00112233"]]],
            [[["0x445566778899aabb"]]],
            [[["0xccddeeff00112233"]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c14 = ? and c2 > ?;"
        types = [FieldType.C_NCHAR, FieldType.C_INT]
        datas = [
            [[["string1"], [100]]],
            [[["string2"], [200]]],
            [[["string3"], [300]]],
            [[["string4"], [400]]],
            [[["string5"], [500]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        # sql = f"select * from {table_name} where c13 = false limit 10;"
        # types = [FieldType.C_BOOL]
        # datas = [
        #     [[[False]]],
        #     [[[False]]],
        #     [[[False]]],
        #     [[[False]]],
        #     [[[False]]]
        # ];
        # self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c3 > ? and c5 < ? and c10 = ?;"
        types = [FieldType.C_INT_UNSIGNED, FieldType.C_BIGINT_UNSIGNED, FieldType.C_SMALLINT_UNSIGNED]
        datas = [
            [[[100], [100000000], [1000]]],
            [[[200], [200000000], [2000]]],
            [[[300], [300000000], [3000]]],
            [[[400], [400000000], [4000]]],
            [[[500], [500000000], [5000]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        # sql = f"select * from {table_name} where c6 between ? and ?;"
        # types = [FieldType.C_FLOAT]
        # datas = [
        #     [[[1.5], [2.5]]],
        #     [[[3.5], [4.5]]],
        #     [[[5.5], [6.5]]],
        #     [[[7.5], [8.5]]],
        #     [[[9.5], [10.5]]]
        # ];
        # self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c14 like ? and c15 = ?;"
        types = [FieldType.C_NCHAR, FieldType.C_VARCHAR]
        datas = [
            [[["abc%"], ["string1"]]],
            [[["def%"], ["string2"]]],
            [[["ghi%"], ["string3"]]],
            [[["jkl%"], ["string4"]]],
            [[["mno%"], ["string5"]]]
        ];
        self.test_query_table_temp(sql, types, datas)

        sql = f"select * from {table_name} where c2 = ? and c3 = ? and c10 >= ?;"
        types = [FieldType.C_INT, FieldType.C_INT_UNSIGNED, FieldType.C_SMALLINT_UNSIGNED]
        datas = [
            [[[100], [200], [1000]]],
            [[[300], [400], [2000]]],
            [[[500], [600], [3000]]],
            [[[700], [800], [4000]]],
            [[[900], [1000], [5000]]]
        ];
        self.test_query_table_temp(sql, types, datas)

    def run(self):
        build_path = self.stmt_common.getBuildPath()
        config = build_path + "../sim/dnode1/cfg/"
        host = "localhost"
        self.connectstmt = self.newcon(host, config)

        ctable_name = "common_table"
        self.insert_common_table(ctable_name)
        self.test_query_table(ctable_name)

        stablename = "super_table"
        subtable_name = "d0"
        self.insert_super_table(stablename, subtable_name)
        self.test_query_table(stablename)
        self.test_query_table(subtable_name)

        self.connectstmt.close()
        return


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
