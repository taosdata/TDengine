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

from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.ts = 1537146000000
        self.param_list = ['LT', 'lt', 'Lt', 'lT', 'GT', 'gt', 'Gt', 'gT', 'LE', 'le', 'Le',
                           'lE', 'GE', 'ge', 'Ge', 'gE', 'NE', 'ne', 'Ne', 'nE', 'EQ', 'eq', 'Eq', 'eQ']
        self.row_num = 10
        self.dbname = 'db'
        self.ntbname = f'{self.dbname}.ntb'
        self.stbname = f'{self.dbname}.stb'

    def duration_check(self):
        tdSql.execute(f'create database {self.dbname}')
        # timestamp = 1ms , time_unit = 1s
        tdSql.execute(f'''create table {self.ntbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.row_num):
            tdSql.execute(f"insert into {self.ntbname} values({self.ts + i}, {i + 1}, {i + 1}, {i + 1}, {i + 1}, {i + 0.1}, {i + 0.1}, {i % 2}, 'taosdata{i + 1}', '涛思数据{i + 1}', {i + 1}, {i + 1}, {i + 1}, {i + 1})"
                          )
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
                print(tdSql.queryResult)
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
                print(tdSql.queryResult)
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
                print(tdSql.queryResult)
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
                print(tdSql.queryResult)
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
                print(tdSql.queryResult)
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

    def run(self):
        self.duration_check()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
