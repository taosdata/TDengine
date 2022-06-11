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
    def alter_stable_column_check(self,dbname,stbname,tbname):
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(
            f'create stable {stbname} (ts timestamp, c1 tinyint, c2 smallint, c3 int, \
                c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 bool,c12 binary(20),c13 nchar(20)) tags(t0 int) ')
        tdSql.execute(f'create table {tbname} using {stbname} tags(1)')
        tdSql.execute(f'insert into {tbname} values (now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        tdSql.execute(f'alter stable {stbname} add column c14 int')
        tdSql.query(f'select c14 from {stbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'alter stable {stbname} add column `c15` int')
        tdSql.query(f'select c15 from {stbname}')
        tdSql.checkRows(1)
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(17)
        tdSql.execute(f'alter stable {stbname} drop column c14')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(16)
        tdSql.execute(f'alter stable {stbname} drop column `c15`')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(15)
        tdSql.execute(f'alter stable {stbname} modify column c12 binary(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(12,2,30)
        tdSql.execute(f'alter stable {stbname} modify column `c12` binary(35)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(12,2,35)
        tdSql.error(f'alter stable {stbname} modify column `c12` binary(34)')
        tdSql.execute(f'alter stable {stbname} modify column c13 nchar(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(13,2,30)
        tdSql.error(f'alter stable {stbname} modify column c13 nchar(29)')
        tdSql.error(f'alter stable {stbname} rename column c1 c21')
        tdSql.error(f'alter stable {stbname} modify column c1 int')
        tdSql.error(f'alter stable {stbname} modify column c4 int')
        tdSql.error(f'alter stable {stbname} modify column c8 int')
        tdSql.error(f'alter stable {stbname} modify column c1 unsigned int')
        tdSql.error(f'alter stable {stbname} modify column c9 double')
        tdSql.error(f'alter stable {stbname} modify column c10 float')
        tdSql.error(f'alter stable {stbname} modify column c11 int')
        tdSql.error(f'alter stable {stbname} drop tag t0')
        tdSql.execute(f'drop database {dbname}')

    def alter_stable_tag_check(self,dbname,stbname,tbname):
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(
            f'create stable {stbname} (ts timestamp, c1 int) tags(ts_tag timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 bool,t12 binary(20),t13 nchar(20)) ')
        tdSql.execute(f'create table {tbname} using {stbname} tags(now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        tdSql.execute(f'insert into {tbname} values(now,1)')

        tdSql.execute(f'alter stable {stbname} add tag t14 int')
        tdSql.query(f'select t14 from {stbname}')
        tdSql.checkRows(1)
        tdSql.execute(f'alter stable {stbname} add tag `t15` int')
        tdSql.query(f'select t14 from {stbname}')
        tdSql.checkRows(1)
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(18)
        tdSql.execute(f'alter stable {stbname} drop tag t14')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(17)
        tdSql.execute(f'alter stable {stbname} drop tag `t15`')
        tdSql.query(f'describe {stbname}')
        tdSql.checkRows(16)
        tdSql.execute(f'alter stable {stbname} modify tag t12 binary(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(14,2,30)
        tdSql.execute(f'alter stable {stbname} modify tag `t12` binary(35)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(14,2,35)
        tdSql.error(f'alter stable {stbname} modify tag `t12` binary(34)')
        tdSql.execute(f'alter stable {stbname} modify tag t13 nchar(30)')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(15,2,30)
        tdSql.error(f'alter stable {stbname} modify tag t13 nchar(29)')
        tdSql.execute(f'alter table {stbname} rename tag t1 t21')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(3,0,'t21')
        tdSql.execute(f'alter table {stbname} rename tag `t21` t1')
        tdSql.query(f'describe {stbname}')
        tdSql.checkData(3,0,'t1')

        for i in ['bigint','unsigned int','float','double','binary(10)','nchar(10)']:
            for j in [1,2,3]:
                tdSql.error(f'alter stable {stbname} modify tag t{j} {i}')
        for i in ['int','unsigned int','float','binary(10)','nchar(10)']:
            tdSql.error(f'alter stable {stbname} modify tag t8 {i}')
        tdSql.error(f'alter stable {stbname} modify tag t4 int')
        tdSql.error(f'alter stable {stbname} drop column t0')
        #!bug TD-16410
        # tdSql.error(f'alter stable {tbname} set tag t1=100 ')
        # tdSql.execute(f'create table ntb (ts timestamp,c0 int)')
        tdSql.error(f'alter stable ntb add column c2 ')
        tdSql.execute(f'drop database {dbname}')

    def run(self):

        dbname = self.get_long_name(length=10, mode="letters")
        stbname = self.get_long_name(length=5, mode="letters")
        tbname = self.get_long_name(length=5, mode="letters")
        self.alter_stable_column_check(dbname,stbname,tbname)
        self.alter_stable_tag_check(dbname,stbname,tbname)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())