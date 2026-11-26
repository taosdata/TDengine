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
import time
from time import sleep
import platform
import os
from new_test_framework.utils.sqlset import TDSetSql
import datetime
from new_test_framework.utils import tdLog, tdSql, gettime
import pytest


_is_utc8 = (
    datetime.datetime.now(datetime.timezone.utc).astimezone().utcoffset()
    == datetime.timedelta(hours=8)
)


class TestFunScaTimeFormat:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.ts = 1640966400000  
        cls.dbname = 'db'
        cls.stbname = f'{cls.dbname}.stb'
        cls.ntbname = f'{cls.dbname}.ntb'

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


    def check_timestamp_precision(self):
        time_zone = time.strftime('%z')
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "us"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.ntbname}(ts timestamp, c1 int, c2 timestamp)')
        tdSql.execute(f'insert into {self.ntbname} values(now,1,today())')
        ts_list = ['1', '11', '111', '1111', '11111', '111111', '1111111', '11111111', '111111111', '1111111111',
                   '11111111111','111111111111','1111111111111','11111111111111','111111111111111','1111111111111111',
                   '11111111111111111','111111111111111111','1111111111111111111']
        res_list_ms = ['1970-01-01T08:00:00.001+0800', '1970-01-01T08:00:00.011+0800', '1970-01-01T08:00:00.111+0800',
                       '1970-01-01T08:00:01.111+0800', '1970-01-01T08:00:11.111+0800', '1970-01-01T08:01:51.111+0800',
                       '1970-01-01T08:18:31.111+0800', '1970-01-01T11:05:11.111+0800', '1970-01-02T14:51:51.111+0800',
                       '1970-01-14T04:38:31.111+0800', '1970-05-09T22:25:11.111+0800', '1973-07-10T08:11:51.111+0800',
                       '2005-03-18T09:58:31.111+0800', '2322-02-06T03:45:11.111+0800', '5490-12-21T13:31:51.111+0800',
                       '37179-09-17T15:18:31.111+0800', '354067-02-04T09:05:11.111+0800',
                       '3522940-12-11T18:51:51.111+0800', '35211679-06-14T20:38:31.111+0800']
        res_list_us = ['1970-01-01T08:00:00.000001+0800', '1970-01-01T08:00:00.000011+0800',
                       '1970-01-01T08:00:00.000111+0800', '1970-01-01T08:00:00.001111+0800',
                       '1970-01-01T08:00:00.011111+0800', '1970-01-01T08:00:00.111111+0800',
                       '1970-01-01T08:00:01.111111+0800', '1970-01-01T08:00:11.111111+0800',
                       '1970-01-01T08:01:51.111111+0800', '1970-01-01T08:18:31.111111+0800',
                       '1970-01-01T11:05:11.111111+0800', '1970-01-02T14:51:51.111111+0800',
                       '1970-01-14T04:38:31.111111+0800', '1970-05-09T22:25:11.111111+0800',
                       '1973-07-10T08:11:51.111111+0800', '2005-03-18T09:58:31.111111+0800',
                       '2322-02-06T03:45:11.111111+0800', '5490-12-21T13:31:51.111111+0800',
                       '37179-09-17T15:18:31.111111+0800']
        res_list_ns = ['1970-01-01T08:00:00.000000001+0800', '1970-01-01T08:00:00.000000011+0800',
                       '1970-01-01T08:00:00.000000111+0800', '1970-01-01T08:00:00.000001111+0800',
                       '1970-01-01T08:00:00.000011111+0800', '1970-01-01T08:00:00.000111111+0800',
                       '1970-01-01T08:00:00.001111111+0800', '1970-01-01T08:00:00.011111111+0800',
                       '1970-01-01T08:00:00.111111111+0800', '1970-01-01T08:00:01.111111111+0800',
                       '1970-01-01T08:00:11.111111111+0800', '1970-01-01T08:01:51.111111111+0800',
                       '1970-01-01T08:18:31.111111111+0800', '1970-01-01T11:05:11.111111111+0800',
                       '1970-01-02T14:51:51.111111111+0800', '1970-01-14T04:38:31.111111111+0800',
                       '1970-05-09T22:25:11.111111111+0800', '1973-07-10T08:11:51.111111111+0800',
                       '2005-03-18T09:58:31.111111111+0800']
        # test to_iso8601's precision with default precision 'ms'
        for i in range(len(ts_list)):
            if platform.system() == "Windows" and i >= 14:
                continue
            tdSql.query(f'select to_iso8601({ts_list[i]})')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list_ms[i])
        # test to_iso8601's precision with table's precision 'us'
        for i in range(len(ts_list)):
            if platform.system() == "Windows" and i >= 17:
                continue
            tdSql.query(f'select to_iso8601({ts_list[i]}) from {self.ntbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list_us[i])

        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "ns"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.ntbname}(ts timestamp, c1 int, c2 timestamp)')
        tdSql.execute(f'insert into {self.ntbname} values(now,1,today())')
        # test to_iso8601's precision with table's precision 'ns'
        for i in range(len(ts_list)):
            tdSql.query(f'select to_iso8601({ts_list[i]}) from {self.ntbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list_ns[i])

    def check_negative_fractional_timestamp(self):
        # 原始缺陷复现：裸 CAST(-1 AS TIMESTAMP) 使用毫秒精度。
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "ms"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.query('select to_iso8601(cast(-1 as timestamp), "+0000")')
        tdSql.checkData(0, 0, '1969-12-31T23:59:59.999+0000')
        tdSql.execute(f'drop database if exists {self.dbname}')

        # 负小数时间戳：ISO 8601 小数部分须非负，整秒向下取整（-1ms -> -1s + 999ms）。
        # 待测值放入非主键 TIMESTAMP 列 c1，主键 ts 用 now，避免被默认保留期拒绝。
        cases = [
            # (precision, value, expected to_iso8601(value, '+0000'))
            ('ms', -1,           '1969-12-31T23:59:59.999+0000'),
            ('ms', -1000,        '1969-12-31T23:59:59.000+0000'),
            ('ms', -1001,        '1969-12-31T23:59:58.999+0000'),
            ('ms', 0,            '1970-01-01T00:00:00.000+0000'),
            ('ms', 1,            '1970-01-01T00:00:00.001+0000'),
            ('us', -1,           '1969-12-31T23:59:59.999999+0000'),
            ('us', -1000000,     '1969-12-31T23:59:59.000000+0000'),
            ('us', -1000001,     '1969-12-31T23:59:58.999999+0000'),
            ('us', 0,            '1970-01-01T00:00:00.000000+0000'),
            ('us', 1,            '1970-01-01T00:00:00.000001+0000'),
            ('ns', -1,           '1969-12-31T23:59:59.999999999+0000'),
            ('ns', -1000000000,  '1969-12-31T23:59:59.000000000+0000'),
            ('ns', -1000000001,  '1969-12-31T23:59:58.999999999+0000'),
            ('ns', 0,            '1970-01-01T00:00:00.000000000+0000'),
            ('ns', 1,            '1970-01-01T00:00:00.000000001+0000'),
        ]
        for prec, tsval, expected in cases:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{prec}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.ntbname}(ts timestamp, c1 timestamp)')
            tdSql.execute(f'insert into {self.ntbname} values(now, {tsval})')
            # 固定偏移、IANA(UTC) 与字符串往返三条路径应一致。
            tdSql.query(f'select to_iso8601(c1, "+0000"), to_iso8601(c1, "UTC"), '
                        f'to_iso8601(cast(to_iso8601(c1, "+0000") as timestamp), "+0000") from {self.ntbname}')
            tdSql.checkData(0, 0, expected)
            tdSql.checkData(0, 1, expected)
            tdSql.checkData(0, 2, expected)
            tdSql.execute(f'drop database if exists {self.dbname}')

    def check_customize_param_ms(self):
        time_zone = time.strftime('%z')
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname} precision "ms"')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.ntbname}(ts timestamp, c1 int, c2 timestamp)')
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {self.ntbname} values({self.ts + i}, {i + 1}, {self.ts + i})")
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        for i in range(self.rowNum):
            tdSql.checkEqual(tdSql.queryResult[i][0],f'2022-01-01T00:00:00.00{i}{time_zone}')

        tz_list = ['+0000','+0530', '+00', '+06', '+00:00', '+12:00', '-0000', '-0900', '-00', '-05', '-00:00', '-03:00','z', 'Z',
                   '+13', '-13', '+1300', '-1300', '+0001', '-0001', '-0330', '-0530',
                   '+14', '+1400', '+14:00', '+13:00', '-12:00', '+05:30', '+05:45', '+12:45']
        res_list = ['2021-12-31T16:00:00.000+0000', '2021-12-31T21:30:00.000+0530', '2021-12-31T16:00:00.000+00', '2021-12-31T22:00:00.000+06',\
                '2021-12-31T16:00:00.000+00:00', '2022-01-01T04:00:00.000+12:00','2021-12-31T16:00:00.000-0000','2021-12-31T07:00:00.000-0900',\
                '2021-12-31T16:00:00.000-00', '2021-12-31T11:00:00.000-05','2021-12-31T16:00:00.000-00:00','2021-12-31T13:00:00.000-03:00',\
                '2021-12-31T16:00:00.000z', '2021-12-31T16:00:00.000Z',
                '2022-01-01T05:00:00.000+13', '2021-12-31T03:00:00.000-13', '2022-01-01T05:00:00.000+1300', '2021-12-31T03:00:00.000-1300',
                '2021-12-31T16:01:00.000+0001', '2021-12-31T15:59:00.000-0001', '2021-12-31T12:30:00.000-0330', '2021-12-31T10:30:00.000-0530',
                '2022-01-01T06:00:00.000+14', '2022-01-01T06:00:00.000+1400', '2022-01-01T06:00:00.000+14:00', '2022-01-01T05:00:00.000+13:00',
                '2021-12-31T04:00:00.000-12:00', '2021-12-31T21:30:00.000+05:30', '2021-12-31T21:45:00.000+05:45', '2022-01-01T04:45:00.000+12:45']
        for i in range(len(tz_list)):
            tdSql.query(f'select to_iso8601(ts,"{tz_list[i]}") from {self.ntbname} where c1==1')
            tdSql.checkEqual(tdSql.queryResult[0][0],res_list[i])

        error_param_list = [0,100.5,'a','!']
        for i in error_param_list:
            tdSql.error(f'select to_iso8601(ts,"{i}") from {self.ntbname}')
        error_timezone_param = ['+15','-15','+1500','-1500','+1401','-1401','+14:01','-14:01']
        for i in error_timezone_param:
            tdSql.error(f'select to_iso8601(ts,"{i}") from {self.ntbname}')


    def check_base_function(self):
        tdSql.prepare()
        tdSql.execute('create table if not exists db.ntb(ts timestamp, c1 int, c2 float,c3 double,c4 timestamp)')
        tdSql.execute('create table if not exists db.stb(ts timestamp, c1 int, c2 float,c3 double,c4 timestamp) tags(t0 int)')
        tdSql.execute('create table if not exists db.stb_1 using db.stb tags(100)')
        tdSql.execute('insert into db.ntb values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.execute('insert into db.stb_1 values(now,1,1.55,100.555555,today())("2020-1-1 00:00:00",10,11.11,99.999999,now())(today(),3,3.333,333.333333,now())')
        tdSql.query("select to_iso8601(ts) from db.ntb")
        tdSql.checkRows(3)
        tdSql.query("select c1 from db.ntb where ts = to_iso8601(1577808000000)")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,10)
        tdSql.query("select * from db.ntb where ts = to_iso8601(1577808000000)")
        tdSql.checkRows(1)
        tdSql.query("select to_iso8601(ts) from db.ntb where ts=today()")
        tdSql.checkRows(1)
        for i in range(0,3):
            tdSql.query("select to_iso8601(1) from db.ntb")
            tdSql.checkData(i,0,"1970-01-01T08:00:00.001+0800")
            tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from db.ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(today()) from db.ntb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from db.ntb")
        tdSql.checkRows(3)
        tdSql.error("select to_iso8601(timezone()) from db.ntb")
        tdSql.error("select to_iso8601('abc') from db.ntb")
        for i in ['+','-','*','/']:
            tdSql.query(f"select to_iso8601(today()) {i}null from db.ntb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)
        tdSql.query("select to_iso8601(9223372036854775807) from db.ntb")
        tdSql.checkRows(3)
        # bug TD-15207
        # tdSql.query("select to_iso8601(10000000000) from ntb")
        # tdSql.checkData(0,0,None)
        # tdSql.query("select to_iso8601(-1) from ntb")
        # tdSql.checkRows(3)
        # tdSql.query("select to_iso8601(-10000000000) from ntb")
        # tdSql.checkData(0,0,None)
        err_param = [1.5,'a','c2']
        for i in err_param:
            tdSql.error(f"select to_iso8601({i}) from db.ntb")
        tdSql.error(f"select to_iso8601(ts, timezone()) from db.stb")
        tdSql.query("select to_iso8601(now) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(now()) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(1) from db.stb")
        for i in range(0,3):
            tdSql.checkData(i,0,"1970-01-01T08:00:00.001+0800")
            tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts) from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts)+1 from db.stb")
        tdSql.checkRows(3)
        tdSql.query("select to_iso8601(ts)+'a' from db.stb ")
        tdSql.checkRows(3)
        for i in ['+','-','*','/']:
            tdSql.query(f"select to_iso8601(today()) {i}null from db.stb")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,None)


    def test_fun_sca_to_iso8601(self):
        """ Fun: to_iso8601()

        1. Query from child/normal/super/without table
        2. Query from ns/us/ms precision database
        3. Query with different time format string
        4. Query with different timezone string
        5. Query with null value
        6. Query with wrong parameter
        7. Input parameter with now()/today()/null/constant/timestamp field     
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_To_iso8601.py
            - 2026-11-20 Alex Merged into test_fun_sca_time_format.py so the methods share one taosd deployment (CI deploys once per test class)
        """

        self.check_base_function()
        self.check_customize_param_ms()
        self.check_timestamp_precision()
        self.check_negative_fractional_timestamp()



    def test_fun_sca_today(self):
        """ Fun: today()

        1. Insert normal/child table with now and today values
        2. Query with input parameter of to_iso8601 function
        3. Query in select/ where clause

        Since: v3.3.0.0

        Labels: common,ci,integration,functional

        History:
            - 2025-10-16 Alex Duan add doc
            - 2026-11-20 Alex Merged into test_fun_sca_time_format.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        pass


    def data_check_unixts(self,tbname,values_list,tb_type,tb_num=1):
        for time in ['1970-01-01T08:00:00+0800','1970-01-01T08:00:00+08:00']:
            tdSql.query(f"select to_unixtimestamp('{time}') from {tbname}")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
                for i in range(len(values_list)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],0)
            elif tb_type == 'stb':
                tdSql.checkRows(len(self.values_list)*tb_num)
        for time in ['1900-01-01T08:00:00+08:00']:
            tdSql.query(f"select to_unixtimestamp('{time}') from {tbname}")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
            elif tb_type == 'stb':
                tdSql.checkRows(len(self.values_list)*tb_num)
        for time in ['2020-01-32T08:00:00','2020-13-32T08:00:00','acd']:
            tdSql.query(f"select to_unixtimestamp('{time}') from {tbname}")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
                for i in range(len(values_list)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],None)
            elif tb_type == 'stb':
                tdSql.checkRows(len(values_list)*tb_num)
        for i in self.column_dict.keys():
            tdSql.query(f"select {i} from {tbname} where to_unixtimestamp('1970-01-01T08:00:00+08:00')=0")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
            elif tb_type == 'stb':
                tdSql.checkRows(len(values_list)*tb_num)
        # Compare within one statement so NOW cannot change between queries.
        for clock in ('now', 'now()', 'today', 'today()'):
            tdSql.query(
                f"select to_unixtimestamp({clock}), cast({clock} as bigint), "
                f"to_unixtimestamp({clock}, 0), to_unixtimestamp({clock}, 1), "
                f"{clock} from {tbname}"
            )
            tdSql.checkRows(len(values_list) * tb_num)
            for row in tdSql.queryResult:
                tdSql.checkEqual(row[0], row[1])
                tdSql.checkEqual(row[2], row[1])
                tdSql.checkEqual(row[3], row[4])
        for time in self.error_param:
            tdSql.error(f"select to_unixtimestamp({time}) from {tbname}")

 
    def timestamp_change_check_ntb(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in range(len(self.values_list)):
            tdSql.execute(f'insert into {self.ntbname} values({self.values_list[i]})')
        self.data_check_unixts(self.ntbname,self.values_list,'ntb')
        tdSql.execute(f'drop database {self.dbname}')

 
    def timestamp_change_check_stb(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
            for j in range(len(self.values_list)):
                tdSql.execute(f'insert into {self.stbname}_{i} values({self.values_list[j]})')
        for i in range(self.tbnum):
            self.data_check_unixts(f'{self.stbname}_{i}',self.values_list,'ctb')
        self.data_check_unixts(self.stbname,self.values_list,'stb',self.tbnum)
        tdSql.execute(f'drop database {self.dbname}')


    def timestamp_change_return_type(self):
        tdSql.query(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        tdSql.query(f"select to_unixtimestamp('1970-01-01 00:00:00', 1);")
        tdSql.checkData(0, 0, datetime.datetime(1970, 1, 1, 0, 0, 0))
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 2);")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 1.5);")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 'abc');")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', true);")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 1, 3);")


    def timestamp_extended_timezone(self):
        # Extended timezone offsets: +13, +14, fractional-hour offsets
        # 1970-01-01T00:00:00 UTC = epoch 0
        # +13:00 means local is UTC+13, so UTC = local - 13h
        tdSql.query(f"select to_unixtimestamp('1970-01-01T13:00:00+13:00', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        tdSql.query(f"select to_unixtimestamp('1970-01-01T14:00:00+14:00', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        # +05:30 (India)
        tdSql.query(f"select to_unixtimestamp('1970-01-01T05:30:00+05:30', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        # +05:45 (Nepal)
        tdSql.query(f"select to_unixtimestamp('1970-01-01T05:45:00+05:45', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        # +12:45 (Chatham Islands)
        tdSql.query(f"select to_unixtimestamp('1970-01-01T12:45:00+12:45', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        # -09:30
        tdSql.query(f"select to_unixtimestamp('1969-12-31T14:30:00-09:30', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        # +0530 format (without colon)
        tdSql.query(f"select to_unixtimestamp('1970-01-01T05:30:00+0530', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)


    def test_fun_sca_to_unixtimestamp(self):
        """ Fun: to_unixtimestamp()

        1. Query from normal/super/without table
        2. Query from boundary timestamp
        3. Query from different time zone string
        4. Query Error timestamp format string
        5. Verify statement time constants with default and explicit return types
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-11 Accept NOW/TODAY constants after client-side folding
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_To_unixtimestamp.py
            - 2026-11-20 Alex Merged into test_fun_sca_time_format.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_unixts()
        self.timestamp_change_check_ntb()
        self.timestamp_change_check_stb()
        self.timestamp_change_return_type()
        self.timestamp_extended_timezone()


    def check_ms_timestamp(self,unit,date_time, ignore_tz):
        if unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]))
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000)*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/60)*60*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60)*60*60*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60/24)*24*60*60*1000)
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000) % (86400 * 1000)))
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdLog.info(f"queryResult: {tdSql.queryResult[i][0]}, ts_result: {ts_result}, date_time[i]: {date_time[i]}")
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60/24/7)*7*24*60*60*1000)
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000) % (86400 * 7 * 1000)))


    def check_us_timestamp(self,unit,date_time, ignore_tz):
        if unit.lower() == '1u':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]))
        elif unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000)*1000)
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000)*1000*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60)*60*1000*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60)*60*60*1000*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24)*24*60*60*1000*1000 )
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000000) % (86400 * 1000000)))
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24/7)*7*24*60*60*1000*1000)
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000000) % (86400 * 7 * 1000000)))


    def check_ns_timestamp(self,unit,date_time, ignore_tz):
        if unit.lower() == '1b':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]))
        elif unit.lower() == '1u':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000)*1000)
        elif unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000)*1000*1000)
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000)*1000*1000*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60)*60*1000*1000*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60/60)*60*60*1000*1000*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    if ignore_tz == 0:
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60/60/24)*24*60*60*1000*1000*1000 )
                    else:
                        # assuming the client timezone is UTC+0800
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i] - (date_time[i] + 8 * 3600 * 1000000000) % (86400 * 1000000000)))
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    if ignore_tz == 0:
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60/60/24/7)*7*24*60*60*1000*1000*1000)
                    else:
                        # assuming the client timezone is UTC+0800
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i] - (date_time[i] + 8 * 3600  * 1000000000) % (86400 * 7 * 1000000000)))


    def check_tb_type(self,unit,tb_type,ignore_tz):
        if tb_type.lower() == 'ntb':
            tdSql.query(f'select timetruncate(ts,{unit},{ignore_tz}) from {self.ntbname}')
        elif tb_type.lower() == 'ctb':
            tdSql.query(f'select timetruncate(ts,{unit},{ignore_tz}) from {self.ctbname}')
        elif tb_type.lower() == 'stb':
            tdSql.query(f'select timetruncate(ts,{unit},{ignore_tz}) from {self.stbname}')


    def data_check_trunc(self,date_time,precision,tb_type):
        tdLog.info(f"date_time: {date_time}, precision: {precision}, tb_type: {tb_type}")
        tz_options = [0, 1]
        for unit in self.time_unit:
            if (unit.lower() == '1u' and precision.lower() == 'ms') or (unit.lower() == '1b' and precision.lower() == 'us') or (unit.lower() == '1b' and precision.lower() == 'ms'):
                if tb_type.lower() == 'ntb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.stbname}')
            elif precision.lower() == 'ms':
                for ignore_tz in tz_options:
                    self.check_tb_type(unit,tb_type,ignore_tz)
                    tdSql.checkRows(len(self.ts_str))
                    self.check_ms_timestamp(unit,date_time,ignore_tz)
                    for uts in self.unix_ts:
                        ans_time = []
                        if tb_type.lower() == 'ntb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.ntbname}')
                        elif tb_type.lower() == 'ctb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.ctbname}')
                        elif tb_type.lower() == 'stb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.stbname}')
                        for i in range(len(self.ts_str)):
                            ans_time.append(int(uts))
                        tdLog.info(f"ans_time: {ans_time}, uts: {uts}, unit:{unit}, ignore_tz:{ignore_tz}")
                        self.check_ms_timestamp(unit, ans_time, ignore_tz)
            elif precision.lower() == 'us':
                for ignore_tz in tz_options:
                    self.check_tb_type(unit,tb_type,ignore_tz)
                    tdSql.checkRows(len(self.ts_str))
                    self.check_us_timestamp(unit,date_time,ignore_tz)
                    for uts in self.unix_ts:
                        ans_time = []
                        if tb_type.lower() == 'ntb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.ntbname}')
                        elif tb_type.lower() == 'ctb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.ctbname}')
                        elif tb_type.lower() == 'stb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.stbname}')
                        for i in range(len(self.ts_str)):
                            ans_time.append(int(uts))
                        self.check_us_timestamp(unit, ans_time, ignore_tz)
            elif precision.lower() == 'ns':
                for ignore_tz in tz_options:
                    self.check_tb_type(unit,tb_type, ignore_tz)
                    tdSql.checkRows(len(self.ts_str))
                    self.check_ns_timestamp(unit,date_time,ignore_tz)
                    for uts in self.unix_ts:
                        ans_time = []
                        if tb_type.lower() == 'ntb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.ntbname}')
                        elif tb_type.lower() == 'ctb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.ctbname}')
                        elif tb_type.lower() == 'stb':
                            tdSql.query(f'select timetruncate({uts},{unit},{ignore_tz}) from {self.stbname}')
                        for i in range(len(self.ts_str)):
                            ans_time.append(int(uts))
                        self.check_ns_timestamp(unit, ans_time, ignore_tz)
        for unit in self.error_unit:
            if tb_type.lower() == 'ntb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.ntbname}')
            elif tb_type.lower() == 'ctb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.ctbname}')
            elif tb_type.lower() == 'stb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.stbname}')


    def function_check_ntb(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdLog.info(f"=====now is in a {precision} database=====")
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ntbname} values("{ts}",1)')
            date_time = self.get_time.time_transform(self.ts_str,precision)
            self.data_check_trunc(date_time,precision,'ntb')


    def function_check_stb(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags(1)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ctbname} values("{ts}",1)')
            date_time = self.get_time.time_transform(self.ts_str,precision)
            self.data_check_trunc(date_time,precision,'ctb')
            self.data_check_trunc(date_time,precision,'stb')


    def test_fun_sca_timetruncate(self):
        """ Fun: timetruncate()

        1. Query from normal/child/super/without table
        2. Query from ns/us/ms precision database
        3. Query from different unit time character
        4. Consider the influence of timezone
        5. Query from different timestamp format, such as string/unix timestamp
        6. Query Error unit time character
 
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_timetruncate.py
            - 2026-11-20 Alex Merged into test_fun_sca_time_format.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_trunc()

        tdSql.query("select first_day_of_week()")
        previous_first_day_of_week = int(tdSql.queryResult[0][0])
        tdSql.execute("set first_day_of_week 4")
        try:
            self.function_check_ntb()
            self.function_check_stb()
        finally:
            tdSql.execute(f"set first_day_of_week {previous_first_day_of_week}")


        #tdSql.close()

    @pytest.mark.skipif(
        os.name == "nt" or not _is_utc8,
        reason="requires UTC+8 and SET TIMEZONE support",
    )
    def test_fun_sca_timetruncate_dst_fallback(self):
        """summary: TIMETRUNCATE during DST fall-back hour yields stable, non-NULL results.

        description: During the America/New_York 2024 DST fall-back (2024-11-03 02:00 EDT
                     → 01:00 EST), two distinct UTC timestamps both display as 01:30 local
                     time but occupy different UTC epochs (05:30 UTC and 06:30 UTC).
                     TIMETRUNCATE(ts, '1h', 'America/New_York') must:
                       - return a non-NULL result for each
                       - map each to its own unique 1-hour boundary
                     Expected truncations (displayed in UTC):
                       01:30 EDT (05:30 UTC) → 01:00 EDT = 05:00 UTC = "2024-11-03 05:00:00.000"
                       01:30 EST (06:30 UTC) → 01:00 EST = 06:00 UTC = "2024-11-03 06:00:00.000"

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timetruncate

        History:
            - 2026-06-01: Tony Zhang created
            - 2026-11-20 Alex Merged into test_fun_sca_time_format.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_trunc()
        dbname = 'db_tt_dst_fb'
        tdSql.execute(f'drop database if exists {dbname}')
        tdSql.execute(f'create database {dbname} precision "ms"')
        tdSql.execute(f'create table {dbname}.t1 (ts timestamp, v int)')
        # 2024-11-03 05:30:00 UTC = 2024-11-03 01:30:00 EDT (UTC-4, before fall-back)
        tdSql.execute(f'insert into {dbname}.t1 values(1730611800000, 1)')
        # 2024-11-03 06:30:00 UTC = 2024-11-03 01:30:00 EST (UTC-5, after fall-back)
        tdSql.execute(f'insert into {dbname}.t1 values(1730615400000, 2)')
        try:
            tdSql.execute("SET TIMEZONE 'UTC'")
            tdSql.query(
                f"select timetruncate(ts, 1h, 'America/New_York') "
                f"from {dbname}.t1 order by ts"
            )
            tdSql.checkRows(2)
            assert tdSql.queryResult[0][0] is not None, \
                "TIMETRUNCATE should not return NULL for fall-back ts (EDT)"
            assert tdSql.queryResult[1][0] is not None, \
                "TIMETRUNCATE should not return NULL for fall-back ts (EST)"
            # Truncation of 01:30 EDT → 01:00 EDT = 05:00 UTC = 13:00 Asia/Shanghai (python connection defaults to UTC-8)
            tdSql.checkData(0, 0, "2024-11-03 13:00:00.000")
            # Truncation of 01:30 EST → 01:00 EST = 06:00 UTC = 14:00 Asia/Shanghai
            tdSql.checkData(1, 0, "2024-11-03 14:00:00.000")
            assert tdSql.queryResult[0][0] != tdSql.queryResult[1][0], (
                "Two distinct fall-back timestamps must truncate to different epochs"
            )
        finally:
            tdSql.connect()


    @pytest.mark.skipif(
        os.name == "nt" or not _is_utc8,
        reason="requires UTC+8 and SET TIMEZONE support",
    )
    def test_fun_sca_timetruncate_quarter_dst(self):
        """summary: TIMETRUNCATE('1q') correctly computes the quarter boundary under DST.

        description: In a DST-observing timezone (America/New_York), Q1 starts in EST
                     (UTC-5) while Q2 starts in EDT (UTC-4).  TIMETRUNCATE(ts, '1q', tz)
                     must align to the correct local midnight on the first day of the
                     quarter, applying the DST offset that is in effect on that day.

                     Test cases (displayed in America/New_York):
                       2024-05-15 12:00:00 EDT (Q2) → 2024-04-01 00:00:00 EDT (Q2 start)
                       2024-02-10 10:00:00 EST (Q1) → 2024-01-01 00:00:00 EST (Q1 start)

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timetruncate

        History:
            - 2026-06-01: Tony Zhang created
            - 2026-11-20 Alex Merged into test_fun_sca_time_format.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_trunc()
        dbname = 'db_tt_dst_q'
        tdSql.execute(f'drop database if exists {dbname}')
        tdSql.execute(f'create database {dbname} precision "ms"')
        tdSql.execute(f'create table {dbname}.t1 (ts timestamp, v int)')
        # 2024-05-15 16:00:00 UTC = 2024-05-15 12:00:00 EDT (Q2 2024)
        tdSql.execute(f'insert into {dbname}.t1 values(1715788800000, 1)')
        # 2024-02-10 15:00:00 UTC = 2024-02-10 10:00:00 EST (Q1 2024)
        tdSql.execute(f'insert into {dbname}.t1 values(1707577200000, 2)')
        try:
            tdSql.execute("SET TIMEZONE 'America/New_York'")
            tdSql.query(
                f"select timetruncate(ts, 1q, 'America/New_York') "
                f"from {dbname}.t1 order by ts"
            )
            tdSql.checkRows(2)
            # Q1 2024 starts 2024-01-01 00:00:00 EST (UTC-5) → "2024-01-01 00:00:00.000" in NY
            # = "2024-01-01 05:00:00.000" in UTC = "2024-01-01 13:00:00.000" in Asia/Shanghai (python connection defaults to UTC-8)
            tdSql.checkData(0, 0, "2024-01-01 13:00:00.000")
            # Q2 2024 starts 2024-04-01 00:00:00 EDT (UTC-4) → "2024-04-01 00:00:00.000" in NY
            # = "2024-04-01 04:00:00.000" in UTC = "2024-04-01 12:00:00.000" in Asia/Shanghai
            tdSql.checkData(1, 0, "2024-04-01 12:00:00.000")
        finally:
            tdSql.connect()

    def _setup_unixts(self):
        self.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        self.setsql = TDSetSql()
        self.dbname = 'db'
        # name of normal table
        self.ntbname = f'{self.dbname}.ntb'
        # name of stable
        self.stbname = f'{self.dbname}.stb'
        # structure of column
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'binary(20)'

        }
        # structure of tag
        self.tag_dict = {
            't0':'int'
        }
        # number of child tables
        self.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        self.tag_values = [
            '10',
            '100'
        ]
        # values of rows, structure should be same as column
        self.values_list = [
            f'now,10,99.99,"abc"',
            f'today(),100,11.111,"abc"'

        ]
        self.error_param = [1,1.5]

    def _setup_trunc(self):
        self.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        self.rest_tag = str(self.conn).lower().split('.')[0].replace("<taos","")
        self.get_time = gettime.GetTime()
        self.ts_str = [
            '2020-1-1',
            '2020-2-1 00:00:01',
            '2020-3-1 00:00:00.001',
            '2020-4-1 00:00:00.001002',
            '2020-5-1 00:00:00.001002001'
        ]
        self.unix_ts = ['1','1111','1111111','1111111111','1111111111111']
        self.db_param_precision = ['ms','us','ns']
        self.time_unit = ['1w','1d','1h','1m','1s','1a','1u','1b']
        self.error_unit = ['2w','2d','2h','2m','2s','2a','2u','1c','#1']
        self.dbname = 'db'
        self.ntbname = f'{self.dbname}.ntb'
        self.stbname = f'{self.dbname}.stb'
        self.ctbname = f'{self.dbname}.ctb'
