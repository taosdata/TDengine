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
from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def get_long_name(self, length, mode="mixed"):
        """
        generate long name
        mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            population = string.digits
        elif mode == "letters":
            population = string.ascii_letters.lower()
        elif mode == "letters_mixed":
            population = string.ascii_letters.upper() + string.ascii_letters.lower()
        else:
            population = string.ascii_letters.lower() + string.digits
        return "".join(random.choices(population, k=length))

    def alter_tb_tag_check(self):
        tag_tinyint = random.randint(-127,129)
        tag_int = random.randint(-2147483648,2147483647)
        tag_smallint = random.randint(-32768,32768)
        tag_bigint = random.randint(-2147483648,2147483647)
        tag_untinyint = random.randint(0,256)
        tag_unsmallint = random.randint(0,65536)
        tag_unint = random.randint(0,4294967296)
        tag_unbigint = random.randint(0,2147483647)
        tag_binary = self.get_long_name(length=10, mode="letters")
        tag_nchar = self.get_long_name(length=10, mode="letters")
        dbname = self.get_long_name(length=10, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        stbname = self.get_long_name(length=3, mode="letters")
        tbname = self.get_long_name(length=3, mode="letters")
        tdSql.execute(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 bool,t12 binary(20),t13 nchar(20))')
        tdSql.execute(f'create table if not exists {dbname}.{tbname} using {dbname}.{stbname} tags(now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True,"abc123","涛思数据")')
        tdSql.execute(f'insert into {dbname}.{tbname} values(now, 1)')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag tag_ts = 1640966400000')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag `t1` = 11')
        tdSql.query(f'select * from {dbname}.{stbname}')
        tdSql.checkData(0,3,11)
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t1 = {tag_tinyint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t2 = {tag_smallint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t3 = {tag_int}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t4 = {tag_bigint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t5 = {tag_untinyint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t6 = {tag_unsmallint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t7 = {tag_unint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t8 = {tag_unbigint}')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t11 = false')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t12 = "{tag_binary}"')
        tdSql.execute(f'alter table {dbname}.{tbname} set tag t13 = "{tag_nchar}"')
        tdSql.query(f'select * from {dbname}.{stbname}')
        # bug TD-15899
        tdSql.checkData(0,2,'2022-01-01 00:00:00.000')
        tdSql.checkData(0,3,tag_tinyint)
        tdSql.checkData(0,4,tag_smallint)
        tdSql.checkData(0,5,tag_int)
        tdSql.checkData(0,6,tag_bigint)
        tdSql.checkData(0,7,tag_untinyint)
        tdSql.checkData(0,8,tag_unsmallint)
        tdSql.checkData(0,9,tag_unint)
        tdSql.checkData(0,10,tag_unbigint)
        
        tdSql.checkData(0,13,False)
        tdSql.checkData(0,14,tag_binary)
        tdSql.checkData(0,15,tag_nchar)

        # bug TD-16211 insert length more than setting binary and nchar
        # error_tag_binary = self.get_long_name(length=21, mode="letters")
        # error_tag_nchar = self.get_long_name(length=21, mode="letters")
        # tdSql.error(f'alter table {dbname}.{tbname} set tag t12 = "{error_tag_binary}"')
        # tdSql.error(f'alter table {dbname}.{tbname} set tag t13 = "{error_tag_nchar}"')
        error_tag_binary = self.get_long_name(length=25, mode="letters")
        error_tag_nchar = self.get_long_name(length=25, mode="letters")
        tdSql.error(f'alter table {dbname}.{tbname} set tag t12 = "{error_tag_binary}"')
        tdSql.error(f'alter table {dbname}.{tbname} set tag t13 = "{error_tag_nchar}"')
        # bug TD-16210 modify binary to nchar
        tdSql.error(f'alter table {dbname}.{tbname} modify tag t12 nchar(10)')
        tdSql.execute(f"drop database {dbname}")
    def alter_ntb_column_check(self):
        '''
        alter ntb column check
        '''
        dbname = self.get_long_name(length=10, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tbname = self.get_long_name(length=3, mode="letters")
        tdLog.info('------------------normal table column check---------------------')
        tdLog.info(f'-----------------create normal table {tbname}-------------------')
        tdSql.execute(f'create table if not exists {dbname}.{tbname} (ts timestamp, c1 tinyint, c2 smallint, c3 int, \
                c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 bool,c12 binary(20),c13 nchar(20))')
        tdSql.execute(f'insert into {dbname}.{tbname} values (now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        # bug TD-15757
        tdSql.execute(f'alter table {dbname}.{tbname} add column c14 int')
        tdSql.query(f'select c14 from {dbname}.{tbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'alter table {dbname}.{tbname} add column `c15` int')
        tdSql.query(f'select c15 from {dbname}.{tbname}')
        tdSql.checkRows(1)
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkRows(16)
        tdSql.execute(f'alter table {dbname}.{tbname} drop column c14')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkRows(15)
        tdSql.execute(f'alter table {dbname}.{tbname} drop column `c15`')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkRows(14)
        #! TD-16422
        # tdSql.execute(f'alter table {dbname}.{tbname} add column c16 binary(10)')
        # tdSql.query(f'describe {dbname}.{tbname}')
        # tdSql.checkRows(15)
        # tdSql.checkEqual(tdSql.queryResult[14][2],10)
        # tdSql.execute(f'alter table {dbname}.{tbname} drop column c16')

        # tdSql.execute(f'alter table {dbname}.{tbname} add column c16 nchar(10)')
        # tdSql.query(f'describe {dbname}.{tbname}')
        # tdSql.checkRows(15)
        # tdSql.checkEqual(tdSql.queryResult[14][2],10)
        # tdSql.execute(f'alter table {dbname}.{tbname} drop column c16')


        tdSql.execute(f'alter table {dbname}.{tbname} modify column c12 binary(30)')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkData(12,2,30)
        tdSql.execute(f'alter table {dbname}.{tbname} modify column `c12` binary(35)')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkData(12,2,35)
        tdSql.error(f'alter table {dbname}.{tbname} modify column c12 binary(34)')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c12 nchar(10)')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c12 int')
        tdSql.execute(f'alter table {dbname}.{tbname} modify column c13 nchar(30)')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkData(13,2,30)
        tdSql.execute(f'alter table {dbname}.{tbname} modify column `c13` nchar(35)')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkData(13,2,35)
        tdSql.error(f'alter table {dbname}.{tbname} modify column c13 nchar(34)')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c13 binary(10)')
        tdSql.execute(f'alter table {dbname}.{tbname} rename column c1 c21')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkData(1,0,'c21')
        # !bug TD-16423
        # tdSql.error(f'select c1 from {dbname}.{tbname}')
        # tdSql.query(f'select c21 from {dbname}.{tbname}')
        # tdSql.checkData(0,1,1)
        tdSql.execute(f'alter table {dbname}.{tbname} rename column `c21` c1')
        tdSql.query(f'describe {dbname}.{tbname}')
        tdSql.checkData(1,0,'c1')
        # !bug TD-16423
        # tdSql.error(f'select c1 from {dbname}.{tbname}')
        # tdSql.query(f'select c1 from {dbname}.{tbname}')
        # tdSql.checkData(0,1,1)
        tdSql.error(f'alter table {dbname}.{tbname} modify column c1 bigint')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c1 double')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c4 int')
        tdSql.error(f'alter table {dbname}.{tbname} modify column `c1` double')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c9 double')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c10 float')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c1 bool')
        tdSql.error(f'alter table {dbname}.{tbname} modify column c1 binary(10)')
        tdSql.execute(f'drop database {dbname}')
    def alter_stb_column_check(self):
        dbname = self.get_long_name(length=10, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        stbname = self.get_long_name(length=3, mode="letters")
        tbname = self.get_long_name(length=3, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(
            f'create table {stbname} (ts timestamp, c1 tinyint, c2 smallint, c3 int, \
                c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 bool,c12 binary(20),c13 nchar(20)) tags(t0 int) ')
        tdSql.execute(f'create table {tbname} using {stbname} tags(1)')
        tdSql.execute(f'insert into {tbname} values (now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        tdSql.execute(f'alter table {stbname} add column c14 int')
        tdSql.query(f'select c14 from {stbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'alter table {stbname} add column `c15` int')
        tdSql.query(f'select c15 from {stbname}')
        tdSql.checkRows(1)
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(17)
        tdSql.execute(f'alter table {stbname} drop column c14')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(16)
        tdSql.execute(f'alter table {stbname} drop column `c15`')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(15)
        tdSql.execute(f'alter table {stbname} modify column c12 binary(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(12,2,30)
        tdSql.execute(f'alter table {stbname} modify column `c12` binary(35)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(12,2,35)
        tdSql.error(f'alter table {stbname} modify column `c12` binary(34)')
        tdSql.execute(f'alter table {stbname} modify column c13 nchar(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(13,2,30)
        tdSql.error(f'alter table {stbname} modify column c13 nchar(29)')
        tdSql.error(f'alter table {stbname} rename column c1 c21')
        tdSql.error(f'alter table {stbname} modify column c1 int')
        tdSql.error(f'alter table {stbname} modify column c4 int')
        tdSql.error(f'alter table {stbname} modify column c8 int')
        tdSql.error(f'alter table {stbname} modify column c1 unsigned int')
        tdSql.error(f'alter table {stbname} modify column c9 double')
        tdSql.error(f'alter table {stbname} modify column c10 float')
        tdSql.error(f'alter table {stbname} modify column c11 int')
        tdSql.execute(f'drop database {dbname}')
    def alter_stb_tag_check(self):
        dbname = self.get_long_name(length=10, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        stbname = self.get_long_name(length=3, mode="letters")
        tbname = self.get_long_name(length=3, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(
            f'create table {stbname} (ts timestamp, c1 int) tags(ts_tag timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 bool,t12 binary(20),t13 nchar(20)) ')
        tdSql.execute(f'create table {tbname} using {stbname} tags(now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        tdSql.execute(f'insert into {tbname} values(now,1)')

        tdSql.execute(f'alter table {stbname} add tag t14 int')
        tdSql.query(f'select t14 from {stbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'alter table {stbname} add tag `t15` int')
        tdSql.query(f'select t14 from {stbname}')
        tdSql.checkRows(1)
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(18)
        tdSql.execute(f'alter table {stbname} drop tag t14')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(17)
        tdSql.execute(f'alter table {stbname} drop tag `t15`')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(16)
        tdSql.execute(f'alter table {stbname} modify tag t12 binary(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(14,2,30)
        tdSql.execute(f'alter table {stbname} modify tag `t12` binary(35)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(14,2,35)
        tdSql.error(f'alter table {stbname} modify tag `t12` binary(34)')
        tdSql.execute(f'alter table {stbname} modify tag t13 nchar(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(15,2,30)
        tdSql.error(f'alter table {stbname} modify tag t13 nchar(29)')
        tdSql.execute(f'alter table {stbname} rename tag t1 t21')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(3,0,'t21')
        tdSql.execute(f'alter table {stbname} rename tag `t21` t1')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(3,0,'t1')

        for i in ['bigint','unsigned int','float','double','binary(10)','nchar(10)']:
            for j in [1,2,3]:
                tdSql.error(f'alter table {stbname} modify tag t{j} {i}')
        for i in ['int','unsigned int','float','binary(10)','nchar(10)']:
            tdSql.error(f'alter table {stbname} modify tag t8 {i}')
        tdSql.error(f'alter table {stbname} modify tag t4 int')
        tdSql.execute(f'drop database {dbname}')
    def run(self):
        self.alter_tb_tag_check()
        self.alter_ntb_column_check()
        self.alter_stb_column_check()
        self.alter_stb_tag_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())