###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
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
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote


class TestAlterTag(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)

    def alter_tb_tag_check(self):
        '''
        alter tb tag check
        '''
        tag_tinyint = random.randint(-128,127)
        tag_int = random.randint(-2147483648,2147483647)
        tag_smallint = random.randint(-32768,32768)
        tag_bigint = random.randint(-2147483648,2147483647)
        tag_untinyint = random.randint(0,256)
        tag_unsmallint = random.randint(0,65536)
        tag_unint = random.randint(0,4294967296)
        tag_unbigint = random.randint(0,2147483647)
        # tag_float = float('%0.1f'%100.1)
        # tag_double = float('%0.1f'%1000.1)
        tag_binary = self.tdCom.get_long_name()
        tag_nchar = self.tdCom.get_long_name()
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        stbname = self.tdCom.get_long_name()
        tbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 bool,t12 binary(20),t13 nchar(20))')
        self.tdSql.execute(f'create table if not exists {dbname}.{tbname} using {dbname}.{stbname} tags(now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True,"abc123","涛思数据")')
        self.tdSql.execute(f'insert into {dbname}.{tbname} values(now, 1)')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag tag_ts = 1640966400000')
        #bug TD-15798 and TD-15804
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag `t1` = 11')
        self.tdSql.query(f'select * from {dbname}.{stbname}')
        self.tdSql.checkData(0,3,11)
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t1 = {tag_tinyint}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t2 = {tag_smallint}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t3 = {tag_int}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t4 = {tag_bigint}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t5 = {tag_untinyint}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t6 = {tag_unsmallint}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t7 = {tag_unint}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t8 = {tag_unbigint}')
        # self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t9 = {tag_float}')
        # self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t10 = {tag_double}')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t11 = false')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t12 = "{tag_binary}"')
        self.tdSql.execute(f'alter table {dbname}.{tbname} set tag t13 = "{tag_nchar}"')
        self.tdSql.query(f'select * from {dbname}.{stbname}')
        # bug TD-15899
        self.tdSql.checkData(0,2,'2022-01-01 00:00:00.000')
        self.tdSql.checkData(0,3,tag_tinyint)
        self.tdSql.checkData(0,4,tag_smallint)
        self.tdSql.checkData(0,5,tag_int)
        self.tdSql.checkData(0,6,tag_bigint)
        self.tdSql.checkData(0,7,tag_untinyint)
        self.tdSql.checkData(0,8,tag_unsmallint)
        self.tdSql.checkData(0,9,tag_unint)
        self.tdSql.checkData(0,10,tag_unbigint)
        # self.tdSql.checkData(0,11,float('%0.1f'%tag_float))
        # self.tdSql.checkData(0,12,float('%0.1f'%tag_double))
        self.tdSql.checkData(0,13,False)
        self.tdSql.checkData(0,14,tag_binary)
        self.tdSql.checkData(0,15,tag_nchar)

        # bug TD-16211 insert length more than setting binary and nchar
        # tag_binary = self.tdCom.get_long_name(length=21, mode="letters")
        # tag_nchar = self.tdCom.get_long_name(length=21, mode="letters")
        # self.tdSql.error(f'alter table {dbname}.{tbname} set tag t12 = "{tag_binary}"')
        # self.tdSql.error(f'alter table {dbname}.{tbname} set tag t13 = "{tag_nchar}"')

        # bug TD-16210 modify binary to nchar
        # self.tdSql.error(f'alter table {dbname}.{tbname} modify tag t12 nchar(10)')

        self.tdSql.execute(f"drop database {dbname}")
    def alter_ntb_column_check(self):
        '''
        alter ntb column check
        '''
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        tbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'create table if not exists {dbname}.{tbname} (ts timestamp, c1 tinyint, c2 smallint, c3 int, \
                c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 bool,c12 binary(20),c13 nchar(20))')
        self.tdSql.execute(f'insert into {dbname}.{tbname} values (now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        # bug TD-15757
        self.tdSql.execute(f'alter table {dbname}.{tbname} add column c14 int')
        self.tdSql.query(f'select c14 from {dbname}.{tbname}')
        self.tdSql.checkRow(1)
        self.tdSql.execute(f'alter table {dbname}.{tbname} add column `c15` int')
        self.tdSql.query(f'select c15 from {dbname}.{tbname}')
        self.tdSql.checkRow(1)
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkRow(16)
        self.tdSql.execute(f'alter table {dbname}.{tbname} drop column c14')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkRow(15)
        self.tdSql.execute(f'alter table {dbname}.{tbname} drop column `c15`')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkRow(14)
        self.tdSql.execute(f'alter table {dbname}.{tbname} modify column c12 binary(30)')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkData(12,2,30)
        self.tdSql.execute(f'alter table {dbname}.{tbname} modify column `c12` binary(35)')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkData(12,2,35)
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c12 binary(34)')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c12 nchar(10)')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c12 int')
        self.tdSql.execute(f'alter table {dbname}.{tbname} modify column c13 nchar(30)')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkData(13,2,30)
        self.tdSql.execute(f'alter table {dbname}.{tbname} modify column `c13` nchar(35)')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkData(13,2,35)
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c13 nchar(34)')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c13 binary(10)')
        self.tdSql.execute(f'alter table {dbname}.{tbname} rename column c1 c21')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkData(1,0,'c21')
        self.tdSql.execute(f'alter table {dbname}.{tbname} rename column `c21` c1')
        self.tdSql.query(f'describe {dbname}.{tbname}')
        self.tdSql.checkData(1,0,'c1')

        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c1 bigint')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c1 double')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c4 int')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column `c1` double')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c9 double')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c10 float')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c1 bool')
        self.tdSql.error(f'alter table {dbname}.{tbname} modify column c1 binary(10)')
        self.tdSql.execute(f'drop database {dbname}')
        
    def run(self) -> bool:
        self.alter_tb_tag_check()
        self.alter_ntb_column_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            
            """
        return case_description

    def author(self) -> str:
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Table.Alter



