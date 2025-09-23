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

from new_test_framework.utils import tdLog, tdSql, TDSetSql
import taos
from taos.tmq import *
import inspect

class TestUserPrivilege:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()
        cls.stbname = 'stb'
        cls.user_name = 'test'
        cls.binary_length = 20  # the length of binary for column_dict
        cls.nchar_length = 20  # the length of nchar for column_dict
        cls.dbnames = ['db1', 'db2']
        cls.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
            'col3': 'float',
        }
        cls.tag_dict = {
            't1': 'int',
            't2': f'binary({cls.binary_length})'
        }
        cls.tag_list = [
            f'1, "Beijing"',
            f'2, "Shanghai"',
            f'3, "Guangzhou"',
            f'4, "Shenzhen"'
        ]
        cls.values_list = [
            f'now, 9.1, 200, 0.3'            
        ]
        cls.tbnum = 4
        cls.stbnum_grant = 200

    def create_user(self):
        tdSql.execute(f'create user {self.user_name} pass "test123@#$"')
        tdSql.execute(f'grant read on {self.dbnames[0]}.{self.stbname} with t2 = "Beijing" to {self.user_name}')
        tdSql.execute(f'grant write on {self.dbnames[1]}.{self.stbname} with t1 = 2 to {self.user_name}')
                
    def prepare_data(self):
        for db in self.dbnames:
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
            for i in range(self.stbnum_grant):
                tdSql.execute(f'create table {self.stbname}_grant_{i} (ts timestamp, c0 int) tags(t0 int)')
    
    def user_read_privilege_check(self, dbname):
        testconn = taos.connect(user='test', password='test123@#$')        
        expectErrNotOccured = False
        
        try:
            sql = f"select count(*) from {dbname}.stb where t2 = 'Beijing'"
            res = testconn.query(sql)
            data = res.fetch_all()
            count = data[0][0]            
        except BaseException:
            expectErrNotOccured = True
        
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        elif count != 1:
            tdLog.exit(f"{sql}, expect result doesn't match")
        pass
    
    def user_write_privilege_check(self, dbname):
        testconn = taos.connect(user='test', password='test123@#$')        
        expectErrNotOccured = False
        
        try:            
            sql = f"insert into {dbname}.stb_1 values(now, 1.1, 200, 0.3)"
            testconn.execute(sql)            
        except BaseException:
            expectErrNotOccured = True
        
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        else:
            pass
    
    def user_privilege_error_check(self):
        testconn = taos.connect(user='test', password='test123@#$')        
        expectErrNotOccured = False
        
        sql_list = [f"alter talbe {self.dbnames[0]}.stb_1 set t2 = 'Wuhan'", 
                    f"insert into {self.dbnames[0]}.stb_1 values(now, 1.1, 200, 0.3)", 
                    f"drop table {self.dbnames[0]}.stb_1", 
                    f"select count(*) from {self.dbnames[1]}.stb"]
        
        for sql in sql_list:
            try:
                res = testconn.execute(sql)                        
            except BaseException:
                expectErrNotOccured = True
            
            if expectErrNotOccured:
                pass
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        pass

    def user_privilege_grant_check(self):
        for db in self.dbnames:
            tdSql.execute(f"use {db}")
            for i in range(self.stbnum_grant):
                tdSql.execute(f'grant read on {db}.{self.stbname}_grant_{i} to {self.user_name}')
                tdSql.execute(f'grant write on {db}.{self.stbname}_grant_{i} to {self.user_name}')

    def test_user_privilege(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        self.prepare_data()
        self.create_user()
        self.user_read_privilege_check(self.dbnames[0])
        self.user_write_privilege_check(self.dbnames[1])
        self.user_privilege_error_check()
        self.user_privilege_grant_check()
                
        tdLog.success("%s successfully executed" % __file__)


