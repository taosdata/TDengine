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

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote

class TestAlterStable(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
    
    def alter_stable_column_check(self,dbname,stbname,tbname):
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create stable {stbname} (ts timestamp, c1 tinyint, c2 smallint, c3 int, \
                c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 bool,c12 binary(20),c13 nchar(20)) tags(t0 int) ')
        self.tdSql.execute(f'create table {tbname} using {stbname} tags(1)')
        self.tdSql.execute(f'insert into {tbname} values (now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')

        self.tdSql.execute(f'alter stable {stbname} add column c14 int')
        self.tdSql.query(f'select c14 from {stbname}')
        self.tdSql.checkRow(1)
        self.tdSql.execute(f'alter stable {stbname} add column `c15` int')
        self.tdSql.query(f'select c15 from {stbname}')
        self.tdSql.checkRow(1)
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkRow(17)
        self.tdSql.execute(f'alter stable {stbname} drop column c14')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkRow(16)
        self.tdSql.execute(f'alter stable {stbname} drop column `c15`')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkRow(15)
        self.tdSql.execute(f'alter stable {stbname} modify column c12 binary(30)')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(12,2,30)
        self.tdSql.execute(f'alter stable {stbname} modify column `c12` binary(35)')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(12,2,35)
        self.tdSql.error(f'alter stable {stbname} modify column `c12` binary(34)')
        self.tdSql.execute(f'alter stable {stbname} modify column c13 nchar(30)')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(13,2,30)
        self.tdSql.error(f'alter stable {stbname} modify column c13 nchar(29)')
        self.tdSql.error(f'alter stable {stbname} rename column c1 c21')
        self.tdSql.execute(f'drop database {dbname}')

    def alter_stable_tag_check(self,dbname,stbname,tbname):
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create stable {stbname} (ts timestamp, c1 int) tags(ts_tag timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 bool,t12 binary(20),t13 nchar(20)) ')
        self.tdSql.execute(f'create table {tbname} using {stbname} tags(now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据")')
        self.tdSql.execute(f'insert into {tbname} values(now,1)')

        self.tdSql.execute(f'alter stable {stbname} add tag t14 int')
        self.tdSql.query(f'select t14 from {stbname}')
        self.tdSql.checkRow(1)
        self.tdSql.execute(f'alter stable {stbname} add tag `t15` int')
        self.tdSql.query(f'select t14 from {stbname}')
        self.tdSql.checkRow(1)
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkRow(18)
        self.tdSql.execute(f'alter stable {stbname} drop tag t14')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkRow(17)
        self.tdSql.execute(f'alter stable {stbname} drop tag `t15`')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkRow(16)
        self.tdSql.execute(f'alter stable {stbname} modify tag t12 binary(30)')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(14,2,30)
        self.tdSql.execute(f'alter stable {stbname} modify tag `t12` binary(35)')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(14,2,35)
        self.tdSql.error(f'alter stable {stbname} modify tag `t12` binary(34)')
        self.tdSql.execute(f'alter stable {stbname} modify tag t13 nchar(30)')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(15,2,30)
        self.tdSql.error(f'alter stable {stbname} modify tag t13 nchar(29)')
        self.tdSql.execute(f'alter table {stbname} rename tag t1 t21')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(3,0,'t21')
        self.tdSql.execute(f'alter table {stbname} rename tag `t21` t1')
        self.tdSql.query(f'describe {stbname}')
        self.tdSql.checkData(3,0,'t1')
        self.tdSql.execute(f'drop database {dbname}')

    def run(self):

        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        stbname = self.tdCom.get_long_name(length=5, mode="letters")
        tbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.alter_stable_column_check(dbname,stbname,tbname)
        self.alter_stable_tag_check(dbname,stbname,tbname)
        

    def cleanup(self):
        pass

    def desc(self) :
        case_description = """
            alter_stable check <jiacy>:  [TD-15384] : alter stable check;
            """
        return case_description

    def author(self) :
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Stable.Alter