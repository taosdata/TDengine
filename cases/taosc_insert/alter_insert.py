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

class TestAlterInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def insert_after_alter_column(self):
        """
        insert after alter column
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int, c2 int) tags (t1 int, t2 int)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1, 1)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, 1, 1)')
        # drop column
        self.tdSql.execute(f'alter stable {dbname}.stb drop column c2')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-1m, 2)')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdSql.error(f'select t1, t2, c1, c2 from {dbname}.tb')
        self.tdSql.query(f'select t1, t2, c1 from {dbname}.tb where c1 = 2')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, 2))

        # add column
        self.tdSql.execute(f'alter stable {dbname}.stb add column c2 int')
        self.tdSql.execute(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdSql.query(f'select t1, t2, c1, c2 from {dbname}.tb where c2 = 2')
        self.tdSql.checkEqual(self.tdSql.query_data[0], (1, 1, 2, 2))
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.insert_after_alter_column()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            insert_after_alter_column <jayden>: [TD-12748] : insert after alter column;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Stable.Alter

