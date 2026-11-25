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
from new_test_framework.utils.sqlset import TDSetSql
import datetime
import pandas as pd
import time
import socket
import os
import threading
from new_test_framework.utils.common import tdCom


class TestFunScaTimeConvert:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.setsql = TDSetSql()
        cls.dbname = 'db'
        # name of normal table
        cls.ntbname = f'{cls.dbname}.ntb'
        # name of stable
        cls.stbname = f'{cls.dbname}.stb'
        # structure of column
        cls.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double'
        }
        # structure of tag
        cls.tag_dict = {
            't0':'int'
        }
        # number of child tables
        cls.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        cls.tag_values = [
            f'10',
            f'100'
        ]
        cls.values_list = [
            f'now,10,99.99,11.111111',
            f'today(),100,11.111,22.222222'
        ]
        cls.time_unit = ['b','u','a','s','m','h','d','w']
        cls.symbol = ['+','-','*','/']
        cls.error_values = ['abc','"abc"','!@','today()']
        cls.db_percision = ['ms','us','ns']
        cls.test_values = [1.5, 10]

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


    def tbtype_check(self,tb_type):
        if tb_type == 'normal table' or tb_type == 'child table':
            tdSql.checkRows(len(self.values_list))
        elif tb_type == 'stable':
            tdSql.checkRows(len(self.values_list) * self.tbnum)

    def data_check_now(self,tbname,tb_type):
        tdSql.query(f'select now() from {tbname}')
        self.tbtype_check(tb_type)
        for unit in self.time_unit:
            for symbol in self.symbol:
                if symbol in ['+','-']:
                    tdSql.query(f'select now() {symbol}1{unit} from {tbname}')
                    self.tbtype_check(tb_type)
        for k,v in self.column_dict.items():
            if v.lower() != 'timestamp':
                continue
            else:
                tdSql.query(f'select * from {tbname} where {k}>=now()')
                tdSql.checkRows(0)
                tdSql.query(f'select * from {tbname} where {k}<now()')
                self.tbtype_check(tb_type)
        for symbol in self.symbol:
            for param in self.error_values:
                tdSql.error(f'select now() {symbol}{param} from {tbname}')
            tdSql.query(f'select now(){symbol}null from {tbname}')
            self.tbtype_check(tb_type)
            for i in range(len(self.values_list)):
                tdSql.checkData(i,0,None)
            for param in self.test_values:
                tdSql.query(f'select now() {symbol}{param} from {tbname}')
                tdSql.query(f'select 1 {symbol}{param} from {tbname}')


    def now_check_ntb(self):
        for time_unit in self.db_percision:
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
            for value in self.values_list:
                tdSql.execute(
                    f'insert into {self.ntbname} values({value})')
            self.data_check_now(self.ntbname,'normal table')
            tdSql.execute(f'drop database {self.dbname}')


    def now_check_stb(self):
        for time_unit in self.db_percision:
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
                for value in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({value})')
            for i in range(self.tbnum):
                self.data_check_now(f'{self.stbname}_{i}','child table')
            self.data_check_now(self.stbname,'stable')
            tdSql.execute(f'drop database {self.dbname}')

    def test_Now(self):
        """ Fun: now()
        
        1. Now with insert clause
        2. Now with select clause
        3. Now with where clause
        4. Now with time arithmetic
        5. Now with different precisions
        6. Now with error values


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-20 Alex Duan Migrated from uncatalog/system-test/2-query/test_Now.py
            - 2026-11-20 Alex Merged into test_fun_sca_time_convert.py so the methods share one taosd deployment (CI deploys once per test class)

        """

        self.now_check_ntb()
        self.now_check_stb()

        ## TD-25540
        tdSql.execute(f'create database db1')
        tdSql.execute(f'use db1')
        tdSql.execute(f"create table db1.tb (ts timestamp, c0 int)")
        tdSql.execute(f'insert into db1.tb values(now + 1h, 1)')

        for func in {"NOW", "NOW()", "TODAY()", "1", "'1970-01-01 00:00:00'"}:
            tdSql.query(f"SELECT _wstart, count(*) FROM (SELECT ts, LAST(c0) FROM db1.tb WHERE ts > {func}) interval(1d);")
            tdSql.checkRows(1)


    def set_create_normaltable_sql(self, ntbname, column_dict):
        column_sql = ''
        for k, v in column_dict.items():
            column_sql += f"{k} {v},"
        create_ntb_sql = f'create table {ntbname} ({column_sql[:-1]})'
        return create_ntb_sql

    def set_create_stable_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v},"
        create_stb_sql = f'create table {stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]})'
        return create_stb_sql


    def data_check_today(self,column_dict={},tbname = '',values_list = [],tb_num = 1,tb = 'tb',precision = 'ms'):
        for k,v in column_dict.items():
            num_up = 0
            num_down = 0
            num_same = 0
            if v.lower() == 'timestamp':
                tdSql.query(f'select {k} from {tbname}')
                
                for i in tdSql.queryResult:
                    if precision == 'ms':
                        self.today_ts_trans = int(self.today_ts)*1000
                        if int(i[0].timestamp())*1000 > int(self.today_ts)*1000:
                            num_up += 1
                        elif int(i[0].timestamp())*1000 == int(self.today_ts)*1000:
                            num_same += 1
                        elif int(i[0].timestamp())*1000 < int(self.today_ts)*1000:
                            num_down += 1
                    elif precision == 'us':
                        self.today_ts_trans = int(self.today_ts)*1000000
                        if int(i[0].timestamp())*1000000 > int(self.today_ts)*1000000:
                            num_up += 1
                        elif int(i[0].timestamp())*1000000 == int(self.today_ts)*1000000:
                            num_same += 1
                        elif int(i[0].timestamp())*1000000 < int(self.today_ts)*1000000:
                            num_down += 1
                    elif precision == 'ns':
                        self.today_ts_trans = int(self.today_ts)*1000000000
                        if i[0] > int(self.today_ts)*1000000000:
                            num_up += 1
                        elif i[0] == int(self.today_ts)*1000000000:
                            num_same += 1
                        elif i[0] < int(self.today_ts)*1000000000:
                            num_down += 1
                tdSql.query(f"select today() from {tbname}")
                tdSql.checkRows(len(values_list)*tb_num)    
                print(self.today_ts_trans,self.today_ts,precision,num_up,num_down,i[0])
                tdSql.checkData(0, 0, self.today_ts_trans)
                tdSql.query(f"select * from {tbname} where {k}=today()")
                if tb == 'tb':
                    tdSql.checkRows(num_same*tb_num)
                elif tb == 'stb':
                    tdSql.checkRows(num_same)
                for i in [f'{tbname}']:
                    for unit in self.time_unit:
                        for symbol in ['+','-']:
                            tdSql.query(f"select today() {symbol}1{unit} from {i}")
                            tdSql.checkRows(len(values_list)*tb_num)
                    for unit in self.error_param:
                        for symbol in self.arithmetic_operators:
                            tdSql.error(f'select today() {symbol}{unit} from {i}')
                    for symbol in self.arithmetic_operators:
                        tdSql.query(f'select now(){symbol}null from {i}')
                        tdSql.checkData(0,0,None)
                    for symbol in self.relational_operator:
                        tdSql.query(f'select * from {i} where {k} {symbol} today()')
                        if symbol == '<' :
                            if tb == 'tb':
                                tdSql.checkRows(num_down*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_down)
                        elif symbol == '<=':
                            if tb == 'tb':
                                tdSql.checkRows((num_same+num_down)*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_same+num_down)
                        elif symbol == '=':
                            if tb == 'tb':
                                tdSql.checkRows(num_same*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_same)
                        elif symbol == '>=':
                            if tb == 'tb':
                                tdSql.checkRows((num_up + num_same)*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_up + num_same)
                        elif symbol == '>':
                            if tb == 'tb':
                                tdSql.checkRows(num_up*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_up)
                tdSql.query(f"select today()/0 from {tbname}")
                tdSql.checkRows(len(values_list)*tb_num)
                tdSql.checkData(0,0,None)
                tdSql.query(f"select {k} from {tbname} where {k}=today()")
                if tb == 'tb':
                    tdSql.checkRows(num_same*tb_num)
                    for i in range(num_same*tb_num):
                        print(self.today_ts_trans,precision,num_up,num_down)
                        tdSql.checkData(i, 0, self.today_ts_trans)
                elif tb == 'stb':
                    tdSql.checkRows(num_same)
                    for i in range(num_same):
                        tdSql.checkData(i, 0, self.today_ts_trans)

    def today_check_ntb(self):
        for time_unit in self.db_percision:
            
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.set_create_normaltable_sql(self.ntbname,self.column_dict))
            for i in self.values_list:
                tdSql.execute(
                    f'insert into {self.ntbname} values({i})')
            self.data_check_today(self.column_dict,self.ntbname,self.values_list,1,'tb',time_unit)
            tdSql.execute(f'drop database {self.dbname}')

    def today_check_stb_tb(self):
        for time_unit in self.db_percision:
            
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table if not exists {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values ({j})')
            # check child table
            for i in range(self.tbnum):
                self.data_check_today(self.column_dict,f'{self.stbname}_{i}',self.values_list,1,'tb',time_unit)
            # check stable
            self.data_check_today(self.column_dict,self.stbname,self.values_list,self.tbnum,'stb',time_unit)
            tdSql.execute(f'drop database {self.dbname}')


    def test_fun_sca_today(self):
        """ Fun: today()

        1. Query from normal/super table
        2. Query from ms/us/ns precision database
        3. Query from where condition
        4. Query Error parameter
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_Today.py
            - 2026-11-20 Alex Merged into test_fun_sca_time_convert.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_today()

        self.today_check_ntb()
        self.today_check_stb_tb()


    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s"%(dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s"%(dbName))
        return


    def create_stable(self,tsql, paraDict):
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s"%(sqlString))
        tsql.execute(sqlString)
        return


    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % \
                    (dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,(i+ctbStartIdx) % 5,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return


    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                if i % 3 == 0:
                    ts_format = 'NULL'
                else:
                    ts_format = "'yyyy-mm-dd hh24:mi:ss'"

                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'2023-11-01 10:10:%d', %s, 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10, j%10, ts_format, j%10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,NULL , %s, 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, ts_format, j%10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return


    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'test',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'varchar', 'len':1024, 'count':2},{'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     100,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"], \
                stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],\
                ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],\
                ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],\
                rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],\
                startTs=paraDict["startTs"],tsStep=paraDict["tsStep"])
        return


    def convert_ts_and_check(self, ts_str: str, ts_format: str, expect_ts_char: str, expect_ts: str):
        tdSql.query("select to_timestamp('%s', '%s')" % (ts_str, ts_format), queryTimes=1)
        tdSql.checkData(0, 0, expect_ts)
        tdSql.query("select to_char(to_timestamp('%s', '%s'), '%s')" % (ts_str, ts_format, ts_format), queryTimes=1)
        tdSql.checkData(0, 0, expect_ts_char)


    def check_to_timestamp(self):
        self.convert_ts_and_check('2023-10-10 12:13:14.123', 'YYYY-MM-DD HH:MI:SS.MS', '2023-10-10 12:13:14.123', '2023-10-10 00:13:14.123000')
        self.convert_ts_and_check('2023-10-10 12:00:00.000AM', 'YYYY-DD-MM HH12:MI:SS.MSPM', '2023-10-10 12:00:00.000AM', '2023-10-10 00:00:00.000000')
        self.convert_ts_and_check('2023-01-01 12:10:10am', 'yyyy-mm-dd HH12:MI:SSAM', '2023-01-01 12:10:10AM', '2023-1-1 00:10:10.000000')
        self.convert_ts_and_check('23-1-01 9:10:10.123p.m.', 'yy-MM-dd HH12:MI:ss.msa.m.', '23-01-01 09:10:10.123p.m.', '2023-1-1 21:10:10.123000')
        self.convert_ts_and_check('23-1-01 9:10:10.123.000456.000000789p.m.', 'yy-MM-dd HH12:MI:ss.ms.us.nsa.m.', '23-01-01 09:10:10.123.123000.123000000p.m.', '2023-1-1 21:10:10.123000')
        self.convert_ts_and_check('  23   -1  - 01   \t 21:10:10 . 12 . \t 00045 . 00000078 \t', 'yy-MM-dd HH24:MI:SS.ms.us.ns', '23-01-01 21:10:10.120.120000.120000000', '2023-1-1 21:10:10.120000')
        self.convert_ts_and_check('  23  年 -1 月 - 01  日 \t 21:10:10 . 12 . \t 00045 . 00000078 \t+08', 'yy\"年\"-MM月-dd日 HH24:MI:SS.ms.us.ns TZH', '23年-01月-01日 21:10:10.120.120000.120000000 +08', '2023-1-1 21:10:10.120000')
        self.convert_ts_and_check('23-1-01 7:10:10.123p.m.6', 'yy-MM-dd HH:MI:ss.msa.m.TZH', '23-01-01 09:10:10.123p.m.+08', '2023-1-1 21:10:10.123000')

        # TZ format: full timezone with hour and minute (+HH:MM, +HHMM, +HH, Z)
        self.convert_ts_and_check('2023-10-10 12:00:00+05:30', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-10 14:30:00+08:00', '2023-10-10 14:30:00.000000')
        self.convert_ts_and_check('2023-10-10 12:00:00+0530', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-10 14:30:00+08:00', '2023-10-10 14:30:00.000000')
        self.convert_ts_and_check('2023-10-10 12:00:00+13:00', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-10 07:00:00+08:00', '2023-10-10 07:00:00.000000')
        self.convert_ts_and_check('2023-10-10 12:00:00Z', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-10 20:00:00+08:00', '2023-10-10 20:00:00.000000')
        self.convert_ts_and_check('2023-10-10 12:00:00+14:00', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-10 06:00:00+08:00', '2023-10-10 06:00:00.000000')
        self.convert_ts_and_check('2023-10-10 12:00:00+05:45', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-10 14:15:00+08:00', '2023-10-10 14:15:00.000000')
        self.convert_ts_and_check('2023-10-10 12:00:00-09:30', 'yyyy-MM-dd HH24:MI:SSTZ', '2023-10-11 05:30:00+08:00', '2023-10-11 05:30:00.000000')

        # TZ format roundtrip: to_char outputs +HH:MM, to_timestamp parses it back
        tdSql.query("select to_timestamp(to_char(ts, 'yyyy-MM-dd HH24:MI:SS.msTZ'), 'yyyy-MM-dd HH24:MI:SS.msTZ') == ts from meters limit 10")
        tdSql.checkRows(10)
        for i in range(10):
            tdSql.checkData(i, 0, 1)

        self.convert_ts_and_check('2023-OCTober-19 10:10:10AM Thu', 'yyyy-month-dd hh24:mi:ssam dy', '2023-october  -19 10:10:10am thu', '2023-10-19 10:10:10')

        tdSql.error("select to_timestamp('210013/2', 'yyyyMM/dd')")
        tdSql.error("select to_timestamp('2100111111111/13/2', 'yyyyMM/dd')")

        tdSql.error("select to_timestamp('210a12/2', 'yyyyMM/dd')")

        tdSql.query("select to_timestamp(to_char(ts, 'yy-mon-dd hh24:mi:ss dy'), 'yy-mon-dd hh24:mi:ss dy') == ts from meters limit 10")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(10)

        tdSql.query("select to_char(ts, 'yy-mon-dd hh24:mi:ss.msa.m.TZH Day') from meters where to_timestamp(to_char(ts, 'yy-mon-dd hh24:mi:ss dy'), 'yy-mon-dd hh24:mi:ss dy') != ts")
        tdSql.checkRows(0)

        tdSql.query("select to_timestamp(c8, 'YYYY-MM-DD hh24:mi:ss') from meters")
        tdSql.query("select to_timestamp(c8, c9) from meters")

        format = "YYYY-MM-DD HH:MI:SS"
        for i in range(500):
            format = format + "1234567890"
        tdSql.query("select to_char(ts, '%s') from meters" % (format), queryTimes=1)
        time_str = '2023-11-11 10:10:10'
        for i in range(500):
            time_str  = time_str + "1234567890"
        tdSql.query("select to_timestamp('%s', '%s')" % (time_str, format))


    def test_func_sca_to_timestamp(self):
        """ Fun: to_timestamp()

        1. Query from child/super/without table
        2. Query from ns/us/ms precision database
        3. Query from different timestamp format
        4. Query Error timestamp format string
        
   
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_func_to_char_timestamp.py
            - 2026-11-20 Alex Merged into test_fun_sca_time_convert.py so the methods share one taosd deployment (CI deploys once per test class)
        """
        self._setup_to_ts()

        self.prepareTestEnv()
        self.check_to_timestamp()
        self.check_ns_to_timestamp()


        #tdSql.close()



    def create_tables(self):
        tdSql.execute("create database if not exists test_us precision 'us'")
        tdSql.execute("create database if not exists test_ns precision 'ns'")
        tdSql.execute("use test_us")
        tdSql.execute(f"CREATE STABLE `meters_us` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT, `ts2` timestamp) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_us` USING `meters_us` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_us` USING `meters_us` (`t1`) TAGS (2)")
        tdSql.execute("use test_ns")
        tdSql.execute(f"CREATE STABLE `meters_ns` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT, `ts2` timestamp) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_ns` USING `meters_ns` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_ns` USING `meters_ns` (`t1`) TAGS (2)")


    def insert_ns_data(self):
        tdLog.debug("start to insert data ............")
        tdSql.execute(f"INSERT INTO `test_us`.`ctb1_us` VALUES ('2023-07-01 00:00:00.123456', 10.30000, 100, '2023-07-01 00:00:00.123456')")
        tdSql.execute(f"INSERT INTO `test_us`.`ctb2_us` VALUES ('2023-08-01 00:00:00.123456', 20.30000, 200, '2023-07-01 00:00:00.123456')")
        tdSql.execute(f"INSERT INTO `test_ns`.`ctb1_ns` VALUES ('2023-07-01 00:00:00.123456789', 10.30000, 100, '2023-07-01 00:00:00.123456000')")
        tdSql.execute(f"INSERT INTO `test_ns`.`ctb2_ns` VALUES ('2023-08-01 00:00:00.123456789', 20.30000, 200, '2023-08-01 00:00:00.123456789')")
        tdLog.debug("insert data ............ [OK]")


    def check_ns_to_timestamp(self):
        self.create_tables()
        self.insert_ns_data()
        tdSql.query("select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')", queryTimes=1)
        tdSql.checkData(0, 0, 1690855810123)
        tdSql.execute('use test_ns', queryTimes=1)
        tdSql.query("select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')", queryTimes=1)
        tdSql.checkData(0, 0, 1690855810123)
        tdSql.query("select to_char(ts2, 'yyyy-mm-dd hh:mi:ss.ns') from meters_ns", queryTimes=1)
        tdSql.checkData(0, 0, '2023-07-01 12:00:00.123456000')
        tdSql.checkData(1, 0, '2023-08-01 12:00:00.123456789')
        tdSql.query("select to_char(ts+1d+1h+1m+1s, 'yyyy-mm-dd') from `test_us`.`ctb1_us`", queryTimes=1)
        tdSql.checkData(0, 0, '2023-07-02')
        tdSql.query("select to_char(ts+1d+1h-1m-1s, 'yyyy-mm-dd') from `test_ns`.`ctb2_ns`", queryTimes=1)
        tdSql.checkData(0, 0, '2023-08-02')

        tdSql.query("select to_timestamp(to_char(ts2, 'yyyy-mm-dd hh:mi:ss.ns'), 'yyyy-mm-dd hh:mi:ss.ns') from meters_ns", queryTimes=1)
        tdSql.checkData(0, 0, 1688140800123456000)
        tdSql.checkData(1, 0, 1690819200123456789)


    def test_fun_sca_to_char(self):
        """ Fun: to_char()

        1. Support data types
        2. Query with 'yyyy-mm-dd hh:mi:ss.ns'
        3. Query with 'yyyy-mm-dd'
        4. Query with 'yyyy-mm-dd hh:mi:ss.ns'
        5. Query with 'yy-mon-dd hh24:mi:ss.msa.m.TZH Day'
        6. Query on super/no table


        Since: v3.3.0.0

        Labels: common,ci,integration,functional

        History:
            - 2025-10-15 Alex Duan add doc
            - 2026-11-20 Alex Merged into test_fun_sca_time_convert.py so the methods share one taosd deployment (CI deploys once per test class)

        """
        self._setup_to_ts()
        pass

    def _setup_today(self):
        self.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        self.today_date = datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
        self.today_ts = datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d").timestamp()
        self.today_ts_ns = 0
        self.time_unit = ['b','u','a','s','m','h','d','w']
        self.error_param = ['abc','!@#','"abc"','today()']
        self.arithmetic_operators = ['+','-','*','/']
        self.relational_operator = ['<','<=','=','>=','>']
        # prepare data
        self.dbname = 'db'
        self.ntbname = f'{self.dbname}.ntb'
        self.stbname = f'{self.dbname}.stb'
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double',
            'c4':'timestamp'
        }
        self.tag_dict = {
            't0':'int'
        }
        self.tbnum = 2
        self.tag_values = [
            f'10',
            f'100'
        ]
        self.values_list = [f'now,1,1.55,100.555555,today()',
                        f'now+1d,10,11.11,99.999999,now()',
                        f'today(),3,3.333,333.333333,now()',
                        f'today()-1d,10,11.11,99.999999,now()',
                        f'today()+1d,1,1.55,100.555555,today()']
        
        self.rest_tag = str(self.conn).lower().split('.')[0].replace("<taos","")
        if self.rest_tag != 'rest':
            self.db_percision = ['ms','us','ns']
        else:
            self.db_percision = ['ms','us']

    def _setup_to_ts(self):
        self.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'
