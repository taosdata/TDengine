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

class TestSpecifiedColumnInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def stb_specified_column_insert(self):
        """
        stb_specified_column_insert
        """
        dbname = self.tdCom.get_long_name(5)
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
            (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5)')
        self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5)')
        self.tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5) values (now+1h, 1, 2, 3, 4, 5)')
        self.tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2) values (now+2h, 1, 2)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5, c6) values (now, 1, 2, 3, 4, 5, 6)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c6) values (now, 1, 6)')
        self.tdSql.query(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdSql.query_data[0][0]), 3)
        self.tdSql.execute(f'drop database if exists {dbname}')

    def tb_specified_column_insert(self):
        """
        tb_specified_column_insert
        """
        dbname = self.tdCom.get_long_name(5)
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create table if not exists {dbname}.tb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5)')
        self.tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5) values (now+1h, 1, 2, 3, 4, 5)')
        self.tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2) values (now+2h, 1, 2)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5, c6) values (now, 1, 2, 3, 4, 5, 6)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c6) values (now, 1, 6)')
        self.tdSql.query(f'select count(*) from {dbname}.tb')
        self.tdSql.checkEqual(int(self.tdSql.query_data[0][0]), 3)
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.stb_specified_column_insert()
        self.tb_specified_column_insert()

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            stb_specified_column_insert <jayden>: [TD-13419] : specified column insert;
            tb_specified_column_insert <jayden>: [TD-13419] : specified column insert;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.SpecifiedColumn