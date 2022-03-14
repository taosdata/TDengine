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
from taostest.util.rest import TDRest

class TestAlterInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def insert_after_alter_column(self):
        """
        insert after alter column
        """
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int, c2 int) tags (t1 int, t2 int)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1, 1)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1, 1)')
        # drop column
        self.tdRest.request(f'alter stable {dbname}.stb drop column c2')
        self.tdRest.request(f'insert into {dbname}.tb values (now-1m, 2)')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdRest.error(f'select t1, t2, c1, c2 from {dbname}.tb')
        self.tdRest.request(f'select t1, t2, c1 from {dbname}.tb where c1 = 2')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0], [1, 1, 2])

        # add column
        self.tdRest.request(f'alter stable {dbname}.stb add column c2 int')
        self.tdRest.request(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdRest.request(f'select t1, t2, c1, c2 from {dbname}.tb where c2 = 2')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0], [1, 1, 2, 2])
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.insert_after_alter_column()

    def cleanup(self):
        pass
        
    def desc(self) -> str:
        case_description = '''
            insert_after_alter_column <jayden>: [TD-12748] : insert after alter column;
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Stable.Alter

