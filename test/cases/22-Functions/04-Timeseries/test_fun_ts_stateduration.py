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
from new_test_framework.utils import tdLog, tdSql

DBNAME = "db"

class TestFunStateduration:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.ts = 1537146000000
        cls.param_list = ['LT', 'lt', 'Lt', 'lT', 'GT', 'gt', 'Gt', 'gT', 'LE', 'le', 'Le',
                           'lE', 'GE', 'ge', 'Ge', 'gE', 'NE', 'ne', 'Ne', 'nE', 'EQ', 'eq', 'Eq', 'eQ']
        cls.row_num = 10
        cls.dbname = 'db'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.stbname = f'{cls.dbname}.stb'

    def do_stateduration(self):
        tdSql.execute(f'create database {self.dbname}')
        # timestamp = 1ms , time_unit = 1s
        tdSql.execute(f'''create table {self.ntbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        sql = f"insert into {self.ntbname} values"
        for i in range(self.row_num):
            sql += f" ({self.ts + i}, {i + 1}, {i + 1}, {i + 1}, {i + 1}, {i + 0.1}, {i + 0.1}, {i % 2}, 'taosdata{i + 1}', '涛思数据{i + 1}', {i + 1}, {i + 1}, {i + 1}, {i + 1})"
        tdSql.execute(sql)
        
        integer_list = [1, 2, 3, 4, 11, 12, 13, 14]
        float_list = [5, 6]

        for i in integer_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5) from {self.ntbname}")
                tdSql.checkRows(10)
                if j in ['LT', 'lt', 'Lt', 'lT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,), (5,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        for i in float_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5) from {self.ntbname}")
                tdSql.checkRows(10)
                if j in ['LT', 'lt', 'Lt', 'lT', 'LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE', 'GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])

        error_column_list = ['ts', 'col7', 'col8', 'col9', 'a', 1]
        for i in error_column_list:
            for j in self.param_list:
                tdSql.error(
                    f"select stateduration({i},{j},5) from {self.ntbname}")

        error_param_list = ['a', 1]
        for i in error_param_list:
            tdSql.error(
                f"select stateduration(col1,{i},5) from {self.ntbname}")
        tdSql.execute(f'drop table {self.ntbname}')
        # timestamp = 1s, time_unit =1s
        tdSql.execute(f'''create table {self.ntbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.row_num):
            tdSql.execute(f"insert into {self.ntbname} values({self.ts + i*1000}, {i + 1}, {i + 1}, {i + 1}, {i + 1}, {i + 0.1}, {i + 0.1}, {i % 2}, 'taosdata{i + 1}', '涛思数据{i + 1}', {i + 1}, {i + 1}, {i + 1}, {i + 1})"
                          )

        for i in integer_list:
            for j in self.param_list:
                tdSql.query(f"select stateduration(col{i},'{j}',5) from {self.ntbname}")
                tdSql.checkRows(10)
                if j in ['LT', 'lt', 'Lt', 'lT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1000,), (2000,), (3000,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1000,), (2000,), (3000,), (4000,)])
                elif j in ['LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1000,), (2000,), (3000,), (4000,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (1000,), (2000,), (3000,), (4000,), (5000,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1000,), (2000,), (3000,), (-1,), (0,), (1000,), (2000,), (3000,), (4000,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        for i in float_list:
            for j in self.param_list:
                tdSql.query(f"select stateduration(col{i},'{j}',5) from {self.ntbname}")
                tdSql.checkRows(10)
                #print(tdSql.queryResult)
                if j in ['LT', 'lt', 'Lt', 'lT', 'LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1000,), (2000,), (3000,), (4000,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE', 'GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1000,), (2000,), (3000,), (4000,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1000,), (2000,), (3000,), (4000,), (5000,), (6000,), (7000,), (8000,), (9000,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        tdSql.execute(f'drop table {self.ntbname}')
        # timestamp = 1m, time_unit =1m
        tdSql.execute(f'''create table {self.ntbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.row_num):
            tdSql.execute(f"insert into {self.ntbname} values({self.ts + i*1000*60}, {i + 1}, {i + 1}, {i + 1}, {i + 1}, {i + 0.1},{i + 0.1}, {i % 2}, 'taosdata{i + 1}', '涛思数据{i + 1}',{i + 1}, {i + 1}, {i + 1}, {i + 1})"
                          )

        for i in integer_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1m) from {self.ntbname}")
                tdSql.checkRows(10)
                if j in ['LT', 'lt', 'Lt', 'lT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,), (5,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        for i in float_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1m) from {self.ntbname}")
                tdSql.checkRows(10)
                #print(tdSql.queryResult)
                if j in ['LT', 'lt', 'Lt', 'lT', 'LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE', 'GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        tdSql.execute(f'drop table {self.ntbname}')
        # timestamp = 1h, time_unit =1h
        tdSql.execute(f'''create table {self.ntbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.row_num):
            tdSql.execute(f"insert into {self.ntbname} values({self.ts + i*1000*60*60}, {i + 1}, {i + 1}, {i + 1}, {i + 1},{i + 0.1}, {i + 0.1}, {i % 2}, 'taosdata{i + 1}', '涛思数据{i + 1}', {i + 1}, {i + 1}, {i + 1}, {i + 1})"
                        )

        for i in integer_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1h) from {self.ntbname}")
                tdSql.checkRows(10)
                if j in ['LT', 'lt', 'Lt', 'lT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,), (5,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        for i in float_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1h) from {self.ntbname}")
                tdSql.checkRows(10)
                #print(tdSql.queryResult)
                if j in ['LT', 'lt', 'Lt', 'lT', 'LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE', 'GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])

        # timestamp = 1h,time_unit =1m
        for i in integer_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1m) from {self.ntbname}")
                tdSql.checkRows(10)
                if j in ['LT', 'lt', 'Lt', 'lT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (60,), (120,), (180,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (60,), (120,), (180,), (240,)])
                elif j in ['LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (60,), (120,), (180,), (240,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (60,), (120,), (180,), (240,), (300,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (60,), (120,), (180,), (-1,), (0,), (60,), (120,), (180,), (240,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        for i in float_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1m) from {self.ntbname}")
                tdSql.checkRows(10)
                #print(tdSql.queryResult)
                if j in ['LT', 'lt', 'Lt', 'lT', 'LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (60,), (120,), (180,), (240,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE', 'GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (60,), (120,), (180,), (240,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (60,), (120,), (180,), (240,), (300,), (360,), (420,), (480,), (540,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])

        # for stb

        tdSql.execute(f'''create table {self.stbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(t0 int)''')
        tdSql.execute(f'create table {self.stbname}_1 using {self.stbname} tags(1)')
        for i in range(self.row_num):
            tdSql.execute(f"insert into {self.stbname}_1 values({self.ts + i*1000*60*60}, {i + 1}, {i + 1},{ i + 1}, {i + 1}, {i + 0.1}, {i + 0.1}, {i % 2},'taosdata{i + 1}', '涛思数据{i + 1}', {i + 1}, {i + 1}, {i + 1}, {i + 1})"
                        )

        for i in integer_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1h) from {self.stbname}")
                tdSql.checkRows(10)
                # print(tdSql.queryResult)
                if j in ['LT', 'lt', 'Lt', 'lT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,), (5,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (0,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        for i in float_list:
            for j in self.param_list:
                tdSql.query(
                    f"select stateduration(col{i},'{j}',5,1h) from {self.stbname}")
                tdSql.checkRows(10)
                #print(tdSql.queryResult)
                if j in ['LT', 'lt', 'Lt', 'lT', 'LE', 'le', 'Le', 'lE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (-1,), (-1,), (-1,), (-1,), (-1,)])
                elif j in ['GE', 'ge', 'Ge', 'gE', 'GT', 'gt', 'Gt', 'gT']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (0,), (1,), (2,), (3,), (4,)])
                elif j in ['NE', 'ne', 'Ne', 'nE']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)])
                elif j in ['EQ', 'eq', 'Eq', 'eQ']:
                    tdSql.checkEqual(tdSql.queryResult, [
                                     (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,), (-1,)])
        
        print("\ndo_stateduration ..................... [passed]")


    #
    # ------------------ test_function_stateduration.py ------------------
    #

    def prepare_datas(self, dbname=DBNAME):
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')

        sql1 = f"insert into {dbname}.ct1 values"
        sql2 = f"insert into {dbname}.ct4 values"
        for i in range(9):
            sql1 += f" ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            sql2 += f" ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
        tdSql.execute(sql1)
        tdSql.execute(sql2)

        sql = "insert into"
        sql += f" {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )"
        sql += f" (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )"
        sql += f" (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )"
        sql += f" (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )"

        sql += f" {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) "
        sql += f" (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) "
        sql += f" (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) "
        tdSql.execute(sql)

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

    def check_errors(self, dbname=DBNAME):
        error_sql_lists = [
            # f"select stateduration(c1,'GT',5,1s) from {dbname}.t1"
            f"select stateduration from {dbname}.t1",
            f"select stateduration(123--123)==1 from {dbname}.t1",
            f"select stateduration(123,123) from {dbname}.t1",
            f"select stateduration(c1,ts) from {dbname}.t1",
            f"select stateduration(c1,c1,ts) from {dbname}.t1",
            f"select stateduration(c1 ,c2 ) from {dbname}.t1",
            f"select stateduration(c1 ,NULL) from {dbname}.t1",
            #f"select stateduration(c1 ,'NULL',1.0,1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT','1',1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT','tbname',1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT','*',1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',ts,1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',max(c1),1s) from {dbname}.t1",
            # f"select stateduration(abs(c1) ,'GT',1,1s) from {dbname}.t1",
            # f"select stateduration(c1+2 ,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,1u) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,now) from {dbname}.t1",
            f"select stateduration(c1 ,'GT','1',1s) from {dbname}.t1",
            f"select stateduration(c1 ,'GT','1',True) from {dbname}.t1",
            f"select stateduration(stateduration(c1) ab from {dbname}.t1)",
            f"select stateduration(c1 ,'GT',1,,)int from {dbname}.t1",
            f"select stateduration('c1','GT',1) from {dbname}.t1",
            f"select stateduration('c1','GT', 1 , NULL) from {dbname}.t1",
            f"select stateduration('c1','GT', 1 , '') from {dbname}.t1",
            f"select stateduration('c1','GT', 1 ,c%) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,t1) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,True) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,1s) , count(c1) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,1s) , avg(c1) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,1s) , min(c1) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,1s) , spread(c1) from {dbname}.t1",
            f"select stateduration(c1 ,'GT',1,1s) , diff(c1) from {dbname}.t1",
        ]
        for error_sql in error_sql_lists:
            tdSql.error(error_sql)
            pass

    def support_types(self, dbname=DBNAME):
        other_no_value_types = [
            f"select stateduration(ts,'GT',1,1s) from {dbname}.t1" ,
            f"select stateduration(c7,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c8,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c9,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(ts,'GT',1,1s) from {dbname}.ct1" ,
            f"select stateduration(c7,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c8,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c9,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(ts,'GT',1,1s) from {dbname}.ct3" ,
            f"select stateduration(c7,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c8,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c9,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(ts,'GT',1,1s) from {dbname}.ct4" ,
            f"select stateduration(c7,'GT',1,1s) from {dbname}.ct4",
            f"select stateduration(c8,'GT',1,1s) from {dbname}.ct4",
            f"select stateduration(c9,'GT',1,1s) from {dbname}.ct4",
            f"select stateduration(ts,'GT',1,1s) from {dbname}.stb1 partition by tbname" ,
            f"select stateduration(c7,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c8,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c9,'GT',1,1s) from {dbname}.stb1 partition by tbname"
        ]

        for type_sql in other_no_value_types:
            tdSql.error(type_sql)
            tdLog.info("support type ok ,  sql is : %s"%type_sql)

        type_sql_lists = [
            f"select stateduration(c1,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c2,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c3,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c4,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c5,'GT',1,1s) from {dbname}.t1",
            f"select stateduration(c6,'GT',1,1s) from {dbname}.t1",

            f"select stateduration(c1,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c2,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c3,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c4,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c5,'GT',1,1s) from {dbname}.ct1",
            f"select stateduration(c6,'GT',1,1s) from {dbname}.ct1",

            f"select stateduration(c1,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c2,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c3,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c4,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c5,'GT',1,1s) from {dbname}.ct3",
            f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3",

            f"select stateduration(c1,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c2,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c3,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c4,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c5,'GT',1,1s) from {dbname}.stb1 partition by tbname",
            f"select stateduration(c6,'GT',1,1s) from {dbname}.stb1 partition by tbname",

            f"select stateduration(c6,'GT',1,1s) as alisb from {dbname}.stb1 partition by tbname",
            f"select stateduration(c6,'GT',1,1s) alisb from {dbname}.stb1 partition by tbname",
        ]

        for type_sql in type_sql_lists:
            tdSql.query(type_sql)

    def support_opers(self, dbname=DBNAME):
        oper_lists =  ['LT','lt','Lt','lT','GT','gt','Gt','gT','LE','le','Le','lE','GE','ge','Ge','gE','NE','ne','Ne','nE','EQ','eq','Eq','eQ']

        oper_errors = [",","*","NULL","tbname","ts","sum","_c0"]

        for oper in oper_lists:
            tdSql.query(f"select stateduration(c1 ,'{oper}',1,1s) as col   from {dbname}.t1")
            tdSql.checkRows(12)

        for oper in oper_errors:
            tdSql.error(f"select stateduration(c1 ,'{oper}',1,1s) as col   from {dbname}.t1")


    def basic_stateduration_function(self, dbname=DBNAME):

        # basic query
        tdSql.query(f"select c1 from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select c1 from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select c1 from {dbname}.stb1")
        tdSql.checkRows(25)

        # used for empty table  , ct3 is empty
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3")
        tdSql.checkRows(0)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct3")

        # will support _rowts mix with
        # tdSql.query(f"select (c6,'GT',1,1s),_rowts from {dbname}.ct3")

        # auto check for t1 table
        # used for regular table
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.t1")

        # unique with super tags

        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct1")
        tdSql.checkRows(13)

        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct4")
        tdSql.checkRows(12)

        tdSql.query(f"select stateduration(c6,'GT',1,1s),tbname from {dbname}.ct1")
        tdSql.checkRows(13)
        tdSql.query(f"select stateduration(c6,'GT',1,1s),t1 from {dbname}.ct1")
        tdSql.checkRows(13)

        # unique with common col
        tdSql.query(f"select stateduration(c6,'GT',1,1s) ,ts  from {dbname}.ct1")
        tdSql.checkRows(13)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) ,c1  from {dbname}.ct1")
        tdSql.checkRows(13)

        # unique with scalar function
        tdSql.query(f"select stateduration(c6,'GT',1,1s) , abs(c1)  from {dbname}.ct1")
        tdSql.checkRows(13)
        tdSql.query(f"select stateduration(c6,'GT',1,1s) , abs(c2)+2 from {dbname}.ct1")
        tdSql.checkRows(13)

        tdSql.error(f"select stateduration(c6,'GT',1,1s) , unique(c2) from {dbname}.ct1")

        # unique with aggregate function
        tdSql.error(f"select stateduration(c6,'GT',1,1s) ,sum(c1)  from {dbname}.ct1")
        tdSql.error(f"select stateduration(c6,'GT',1,1s) ,max(c1)  from {dbname}.ct1")
        tdSql.error(f"select stateduration(c6,'GT',1,1s) ,csum(c1)  from {dbname}.ct1")
        tdSql.error(f"select stateduration(c6,'GT',1,1s) ,count(c1)  from {dbname}.ct1")

        # unique with filter where
        tdSql.query(f"select stateduration(c6,'GT',1,1s) from {dbname}.ct4 where c1 is null")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)

        tdSql.query(f"select stateduration(c1,'GT',1,1s) from {dbname}.t1 where c1 >2 ")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 10886404)
        tdSql.checkData(2, 0, 23500810)
        tdSql.checkData(4, 0, 57456020)
        tdSql.checkData(5, 0, 60393624)

        tdSql.query(f"select stateduration(c2,'GT',1,1s) from {dbname}.t1  where c2 between 0  and   99999")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 6134400)
        tdSql.checkData(6, 0, -1)


        # unique with union all
        tdSql.query(f"select stateduration(c1,'GT',1,1s) from {dbname}.ct4 union all select stateduration(c1,'GT',1,1s) from {dbname}.ct1")
        tdSql.checkRows(25)
        tdSql.query(f"select stateduration(c1,'GT',1,1s) from {dbname}.ct4 union all select distinct(c1) from {dbname}.ct4")
        tdSql.checkRows(22)

        # unique with join
        # prepare join datas with same ts

        tdSql.execute(f"create stable {dbname}.st1 (ts timestamp , num int) tags(ind int)")
        tdSql.execute(f"create table {dbname}.tb1 using {dbname}.st1 tags(1)")
        tdSql.execute(f"create table {dbname}.tb2 using {dbname}.st1 tags(2)")

        tdSql.execute(f"create stable {dbname}.st2 (ts timestamp , num int) tags(ind int)")
        tdSql.execute(f"create table {dbname}.ttb1 using {dbname}.st2 tags(1)")
        tdSql.execute(f"create table {dbname}.ttb2 using {dbname}.st2 tags(2)")

        start_ts = 1622369635000 # 2021-05-30 18:13:55

        sql = "insert into"
        for i in range(10):
            ts_value = start_ts+i*1000
            sql += f" {dbname}.tb1 values({ts_value} , {i})"
            sql += f" {dbname}.tb2 values({ts_value} , {i})"

            sql += f" {dbname}.ttb1 values({ts_value} , {i})"
            sql += f" {dbname}.ttb2 values({ts_value} , {i})"
        tdSql.execute(sql)    

        tdSql.query(f"select stateduration(tb1.num,'GT',1,1s)  from {dbname}.tb1 tb1, {dbname}.tb2 tb2 where tb1.ts=tb2.ts ")
        tdSql.checkRows(10)
        tdSql.checkData(0,0,-1)
        tdSql.checkData(1,0,-1)
        tdSql.checkData(2,0,0)
        tdSql.checkData(9,0,7)

        tdSql.query(f"select stateduration(tb1.num,'GT',1,1s)  from {dbname}.tb1 tb1, {dbname}.tb2 tb2 where tb1.ts=tb2.ts union all select stateduration(tb2.num,'GT',1,1s)  from {dbname}.tb1 tb1, {dbname}.tb2 tb2 where tb1.ts=tb2.ts ")
        tdSql.checkRows(20)

        # nest query
        # tdSql.query(f"select unique(c1) from (select c1 from {dbname}.ct1)")
        tdSql.query(f"select c1 from (select stateduration(c1,'GT',1,1s) c1 from {dbname}.t1)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, -1)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(10, 0, 63072035)

        tdSql.query(f"select sum(c1) from (select stateduration(c1,'GT',1,1d) c1 from {dbname}.t1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2893)

        tdSql.query(f"select sum(c1) from (select distinct(c1) c1 from {dbname}.ct1) union all select sum(c1) from (select stateduration(c1,'GT',1,1s) c1 from {dbname}.ct1)")
        tdSql.checkRows(2)

        tdSql.query(f"select 1-abs(c1) from (select stateduration(c1,'GT',1,1s) c1 from {dbname}.t1)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0.000000000)
        tdSql.checkData(3, 0, -86404.000000000)

        tdSql.query(f"select stateduration(abs(c1) ,'GT',1,1s) from {dbname}.t1")
        tdSql.checkRows(12)
        tdSql.query(f"select stateduration(c1+2 ,'GT',1,1s) from {dbname}.t1")
        tdSql.checkRows(12)


        # bug for stable
        #partition by tbname
        # tdSql.query(f"select unique(c1) from {dbname}.stb1 partition by tbname ")
        # tdSql.checkRows(21)

        # tdSql.query(f"select unique(c1) from {dbname}.stb1 partition by tbname ")
        # tdSql.checkRows(21)

        # group by
        tdSql.error(f"select stateduration(c1,'GT',1,1s) from {dbname}.ct1 group by c1")
        tdSql.error(f"select stateduration(c1,'GT',1,1s) from {dbname}.ct1 group by tbname")

        # super table

    def check_unit_time(self, dbname=DBNAME):
        tdSql.error(f"select stateduration(c1,'GT',1,1b) from {dbname}.ct1")
        tdSql.error(f"select stateduration(c1,'GT',1,1u) from {dbname}.ct1")
        tdSql.error(f"select stateduration(c1,'GT',1,1000s) from {dbname}.t1")
        tdSql.query(f"select stateduration(c1,'GT',1,1s) from {dbname}.t1")
        tdSql.checkData(10,0,63072035)
        tdSql.query(f"select stateduration(c1,'GT',1,1m) from {dbname}.t1")
        tdSql.checkData(10,0,int(63072035/60))
        tdSql.query(f"select stateduration(c1,'GT',1,1h) from {dbname}.t1")
        tdSql.checkData(10,0,int(63072035/60/60))
        tdSql.query(f"select stateduration(c1,'GT',1,1d) from {dbname}.t1")
        tdSql.checkData(10,0,int(63072035/60/24/60))
        tdSql.query(f"select stateduration(c1,'GT',1,1w) from {dbname}.t1")
        tdSql.checkData(10,0,int(63072035/60/7/24/60))


    def check_boundary_values(self, dbname="bound_test"):

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database if not exists {dbname}")
        tdSql.execute(f"use {dbname}")
        tdSql.execute(
            f"create table {dbname}.stb_bound (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp) tags (t1 int);"
        )
        tdSql.execute(f'create table {dbname}.sub1_bound using {dbname}.stb_bound tags ( 1 )')
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()-10s, 2147483647, 9223372036854775807, 32767, 127, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )
        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()-5s, 2147483646, 9223372036854775806, 32766, 126, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now(), -2147483646, -9223372036854775806, -32766, -126, -3.40E+38, -1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+5s, 2147483643, 9223372036854775803, 32763, 123, 3.39E+38, 1.69e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.execute(
                f"insert into {dbname}.sub1_bound values ( now()+10s, -2147483643, -9223372036854775803, -32763, -123, -3.39E+38, -1.69e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.error(
                f"insert into {dbname}.sub1_bound values ( now()+15s, 2147483648, 9223372036854775808, 32768, 128, 3.40E+38, 1.7e+308, True, 'binary_tb1', 'nchar_tb1', now() )"
            )

        tdSql.query(f"select stateduration(c1,'GT',1,1s) from {dbname}.sub1_bound")
        tdSql.checkRows(5)

    def do_function_stateduration(self): 
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("==========step2:test errors ==============")

        self.check_errors()

        tdLog.printNoPrefix("==========step3:support types ============")

        self.support_types()

        tdLog.printNoPrefix("==========step4:support opers ============")
        self.support_opers()

        tdLog.printNoPrefix("==========step5: stateduration basic query ============")

        self.basic_stateduration_function()

        tdLog.printNoPrefix("==========step6: stateduration boundary query ============")

        self.check_boundary_values()

        tdLog.printNoPrefix("==========step6: stateduration unit time test ============")

        self.check_unit_time()

        print("do_function_stateduration ............ [passed]")

    #
    # ------------------ main ------------------
    #
    def test_func_ts_stateduration(self):
        """ Fun: stateduration()

        1. Basic query for different params
        2. Query on super/child/normal table
        3. Support data types
        4. Error cases
        5. Query with where condition
        6. Query with partition/group/order by
        7. Query with sub query
        8. Query with union/join
        9. Query with unit time
        10. Check null value
        11. Boundary values

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:     
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_stateduration.py
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_function_stateduration.py

        """
        self.do_stateduration()
        self.do_function_stateduration()