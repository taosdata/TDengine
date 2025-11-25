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


import math
from random import randint
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.boundary import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.boundary = DataBoundary()
        self.dbname_length_boundary = self.boundary.DBNAME_MAX_LENGTH
        self.tbname_length_boundary = self.boundary.TBNAME_MAX_LENGTH
        self.stbname_length_boundary = self.boundary.STBNAME_MAX_LENGTH
        self.colname_length_boundary = self.boundary.COL_KEY_MAX_LENGTH
        self.tagname_length_boundary = self.boundary.TAG_KEY_MAX_LENGTH
        self.username_length_boundary = 23
        self.password_length_boundary = 253
    def dbname_length_check(self):
        dbname_length = randint(1,self.dbname_length_boundary-1)
        for dbname in [tdCom.get_long_name(self.dbname_length_boundary),tdCom.get_long_name(dbname_length)]:
            tdSql.execute(f'create database if not exists {dbname}')
            tdSql.query(f'select name from information_schema.ins_databases where name = "{dbname}"')
            tdSql.checkEqual(tdSql.queryResult[0][0],dbname)
            tdSql.execute(f'drop database if exists {dbname}')
        dbname = tdCom.get_long_name(self.dbname_length_boundary+1)
        tdSql.error(f'create database if not exists {dbname}')
        if "Invalid identifier name" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")

    def tbname_length_check(self):
        tdSql.prepare()
        tdSql.execute('use db')
        tbname_length = randint(1,self.tbname_length_boundary-1)
        tdSql.execute(f'create table stb (ts timestamp,c0 int) tags(t0 int)')
        for tbname in [tdCom.get_long_name(self.tbname_length_boundary),tdCom.get_long_name(tbname_length)]:
            tdSql.execute(f'create table {tbname} using stb tags(1)')
            tdSql.query(f'select table_name from information_schema.ins_tables where table_name = "{tbname}"')
            tdSql.checkEqual(tdSql.queryResult[0][0],tbname)
            tdSql.execute(f'drop table {tbname}')
        tbname = tdCom.get_long_name(self.tbname_length_boundary+1)
        tdSql.error(f'create table {tbname} using stb tags(1)')
        if "Invalid identifier name" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
        stbname_length = randint(1,self.stbname_length_boundary-1)
        for stbname in [tdCom.get_long_name(self.stbname_length_boundary),tdCom.get_long_name(stbname_length)]:
            tdSql.execute(f'create table {stbname} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.query(f'select stable_name from information_schema.ins_stables where stable_name = "{stbname}"')
            tdSql.checkEqual(tdSql.queryResult[0][0],stbname)
            tdSql.execute(f'drop table {stbname}')
        stbname = tdCom.get_long_name(self.stbname_length_boundary+1)
        tdSql.error(f'create table {stbname} (ts timestamp,c0 int) tags(t0 int)')
        print(tdSql.error_info)
        if "Invalid identifier name" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
        tdSql.execute('drop database db')

    def colname_length_check(self):
        tdSql.prepare()
        tdSql.execute('use db')
        column_name_length = randint(1,self.colname_length_boundary-1)
        for colname in [tdCom.get_long_name(column_name_length),tdCom.get_long_name(self.colname_length_boundary)]:
            stbname = tdCom.get_long_name(3)
            ntbname = tdCom.get_long_name(4)
            tdSql.execute(f'create table {stbname} (ts timestamp,{colname} int) tags(t0 int)')
            tdSql.query(f'describe {stbname}')
            tdSql.checkEqual(tdSql.queryResult[1][0],colname)
            tdSql.execute(f'create table {ntbname} (ts timestamp,{colname} int)')
            tdSql.query(f'describe {ntbname}')
            tdSql.checkEqual(tdSql.queryResult[1][0],colname)
        colname = tdCom.get_long_name(self.colname_length_boundary+1)
        tdSql.error(f'create table stb (ts timestamp,{colname} int) tags(t0 int)')
        if "Invalid identifier name" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
        tdSql.execute('drop database db')

    def tagname_length_check(self):
        tdSql.prepare()
        tdSql.execute('use db')
        tag_name_length = randint(1,self.tagname_length_boundary-1)
        for tagname in (tdCom.get_long_name(tag_name_length),tdCom.get_long_name(self.tagname_length_boundary)):
            stbname = tdCom.get_long_name(3)
            tdSql.execute(f'create table {stbname} (ts timestamp,c0 int) tags({tagname} int)')
            tdSql.query(f'describe {stbname}')
            tdSql.checkEqual(tdSql.queryResult[-1][0],tagname)
        tagname = tdCom.get_long_name(self.tagname_length_boundary+1)
        tdSql.error(f'create table {stbname} (ts timestamp,c0 int) tags({tagname} int)')
        if "Invalid identifier name" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
        tdSql.execute('drop database db')

    def username_length_check(self):
        username_length = randint(1,self.username_length_boundary-1)
        for username in [tdCom.get_long_name(username_length),tdCom.get_long_name(self.username_length_boundary)]:
            tdSql.execute(f'create user {username} pass "test123@#$"')
            tdSql.query('show users')
            for user in tdSql.queryResult:
                if user[0].lower() != 'root':
                    tdSql.checkEqual(user[0],username)
            tdSql.execute(f'drop user {username}')
        username = tdCom.get_long_name(self.username_length_boundary+1)
        tdSql.error(f'create user {username} pass "test123@#$"')
        if "Name or password too long" in tdSql.error_info:
            tdLog.info("error info is true!")
        elif "Password too short or empty" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
    
    def password_length_check(self):
        password_length = randint(8,self.password_length_boundary-1)
        index = 0
        for password in [tdCom.get_long_name(password_length),tdCom.get_long_name(self.password_length_boundary)]:
            index += 1
            username = tdCom.get_long_name(12) + str(index)
            tdSql.execute(f'create user {username} pass "{password}@1"')
        index += 1
        username = tdCom.get_long_name(12) + str(index)
        password = tdCom.get_long_name(self.password_length_boundary+1)
        tdSql.error(f'create user {username} pass "{password}@1"')
        if "Invalid password format" in tdSql.error_info:
            tdLog.info("error info is true!")
        elif "Name or password too long" in tdSql.error_info:
            tdLog.info("error info is true!")
        elif "Password too short or empty" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
    def sql_length_check(self):
        insert_rows = 1021
        tdSql.prepare()
        tdSql.execute('use db')
        tdSql.execute('create table ntb (ts timestamp,c0 binary(1013))')
        values_sql = ''
        value = tdCom.get_long_name(1013)
        for num in range(insert_rows):
            values_sql += f' (now+{num}s,"{value}")'
        value = tdCom.get_long_name(65)
        values_sql += f"(now-1s,'{value}')"
        tdSql.execute(f'insert into ntb values{values_sql}')
        tdSql.query('select * from ntb')
        tdSql.checkRows(insert_rows+1)
        tdSql.execute('create table ntb1 (ts timestamp,c0 binary(1013))')
        tdSql.error(f'insert into ntb1 values{values_sql};')
        print(tdSql.error_info)
        if "SQL statement too long" in tdSql.error_info:
            tdLog.info("error info is true!")
        else:
            tdLog.exit("error info is not true")
        tdSql.execute('drop database db')

    def row_col_tag_maxlen_check(self):
        tdSql.prepare()
        tdSql.execute('use db')
        tdSql.execute('create table if not exists stb1 (ts timestamp, c1 int,c2 binary(1000)) tags (city binary(16382))')
        tdSql.error('create table if not exists stb1 (ts timestamp, c1 int,c2 binary(1000)) tags (city binary(16383))')
        tdSql.execute('create table if not exists stb2 (ts timestamp, c0 tinyint, c1 int, c2 nchar(16379)) tags (city binary(16382))')
        tdSql.error('create table if not exists stb2 (ts timestamp, c0 smallint, c1 int, c2 nchar(16379)) tags (city binary(16382))')
        tdSql.execute('create table if not exists stb3 (ts timestamp, c1 int, c2 binary(65517)) tags (city binary(16382))')
        tdSql.error('create table if not exists stb3 (ts timestamp, c0 bool, c1 int, c2 binary(65517)) tags (city binary(16382))')
        # prepare the column and tag data
        char100='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN0123456789'
        tag_max_16382=''
        binary_max_65517 = ''
        nchar_max_16379=''
        for num in range(163):
            nchar_max_16379 += char100
            for num in range(4):
                binary_max_65517 += char100
        
        nchar_max_16379 += '0123456789012345678901234567890123456789012345678901234567890123456789012345678'
        tag_max_16382 = nchar_max_16379
        tag_max_16382 += '9ab'

        for num in range(3):
            binary_max_65517 += char100
        binary_max_65517 += '01234567890123456'

        # insert/query and check
        tdSql.execute(f"create table ct1 using stb1 tags('{tag_max_16382}')")
        tdSql.execute(f"create table ct2 using stb2 tags('{tag_max_16382}')")
        tdSql.execute(f"create table ct3 using stb3 tags('{tag_max_16382}')")
        tdSql.execute(f"insert into ct1 values (now,1,'nchar_max_16379')")
        tdSql.execute(f"insert into ct2 values (now,1,1,'{nchar_max_16379}')")
        tdSql.execute(f"insert into ct3 values (now,1,'{binary_max_65517}')")

        tdSql.query("select * from stb1")
        tdSql.checkEqual(tdSql.queryResult[0][3],tag_max_16382)

        tdSql.query("select * from ct2")
        tdSql.checkEqual(tdSql.queryResult[0][3],nchar_max_16379)

        tdSql.query("select * from stb2")
        tdSql.checkEqual(tdSql.queryResult[0][3],nchar_max_16379)
        tdSql.checkEqual(tdSql.queryResult[0][4],tag_max_16382)

        tdSql.query("select * from ct3")
        tdSql.checkEqual(tdSql.queryResult[0][2],binary_max_65517)

        tdSql.query("select * from stb3")
        tdSql.checkEqual(tdSql.queryResult[0][2],binary_max_65517)
        tdSql.checkEqual(tdSql.queryResult[0][3],tag_max_16382)

        tdSql.execute('drop database db')

    def run(self):
        self.dbname_length_check()
        self.tbname_length_check()
        self.colname_length_check()
        self.tagname_length_check()
        self.username_length_check()
        self.password_length_check()
        self.sql_length_check()
        self.row_col_tag_maxlen_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
    
    def test_sql_length_boundary(self):
        """Test with large maxSQLLength (10MB)
        
        Test that very long SQL statements work with large limit.
        """
        tdLog.info("=============== Test with large maxSQLLength scenario")
        tdSql.execute("alter local 'maxSQLLength' '1048576'")
        
        tdSql.execute("drop database if exists db_maxsql_large")
        tdSql.execute("create database db_maxsql_large")
        tdSql.execute("use db_maxsql_large")
        
        cols = []
        for i in range(100):
            cols.append(f"c{i} int")
        create_sql = f"create table t1 (ts timestamp, {', '.join(cols)})"
        tdSql.execute(create_sql)
        
        # test insert sql
        insert_sql = "insert into t1 values "
        values = []
        for i in range(1000):
            val_cols = [f"now+{i}s"] + [str(j) for j in range(100)]
            values.append(f"({', '.join(val_cols)})")
        insert_sql += ", ".join(values)
        
        tdLog.info(f"INSERT SQL length: {len(insert_sql)}")
        tdSql.execute(insert_sql)
        tdSql.query("select * from t1")
        tdSql.checkRows(1000)
        tdSql.execute("alter local 'maxSQLLength' '1024'")
        tdSql.error(insert_sql)

         # test create table sql
        tdSql.execute("alter local 'maxSQLLength' '1048576'")
        cols = ["ts timestamp"]
        for i in range(150):
            cols.append(f"column_{i} int")
        
        create_sql = f"create table t2 ({', '.join(cols)})"
        tdSql.execute(create_sql)
        tdSql.execute("alter local 'maxSQLLength' '1024'")
        tdSql.error(create_sql)


        # test 10MB sql - generate INSERT SQL with exact length 10485760 bytes
        tdSql.execute("create table t3 (ts timestamp, c0 int)")
        
        # Generate INSERT SQL with exact length 10485760 bytes
        target_length = 10485760  # 10MB
        base_sql = "insert into t3 values "
        base_len = len(base_sql)
        num_values = (target_length - base_len) // 16
        values = [f"(now+{i}s,{i})" for i in range(num_values)]
        insert_sql = base_sql + ",".join(values)
        current_len = len(insert_sql)
        if current_len < target_length:
            padding_len = target_length - current_len
            insert_sql += "--" + "x" * (padding_len - 2)
        elif current_len > target_length:
            insert_sql = insert_sql[:target_length]
            last_paren = insert_sql.rfind(")")
            if last_paren > 0:
                insert_sql = insert_sql[:last_paren + 1]
        
        tdLog.info(f"Generated INSERT SQL length: {len(insert_sql)} bytes (target: {target_length})")
        tdSql.error(insert_sql)
        # Test with 1MB limit (should fail)
        tdSql.execute("alter local 'maxSQLLength' '1048576'")
        tdSql.error(insert_sql)
        # Test with 10MB limit (should succeed)
        tdSql.execute("alter local 'maxSQLLength' '10485760'")
        tdSql.execute(insert_sql)
        tdSql.query("select count(*) from t3")
        tdSql.checkRows(1)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())