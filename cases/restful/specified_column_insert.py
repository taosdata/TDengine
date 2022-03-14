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

class TestSpecifiedColumnInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def specified_column_insert(self):
        '''
            specified_column_insert 
        '''
        dbname = self.tdCom.get_long_name(len=5, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
            (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5)')
        self.tdRest.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5)')
        self.tdRest.request(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5) values (now+1h, 1, 2, 3, 4, 5)')
        self.tdRest.request(f'insert into {dbname}.tb (col_ts, c1, c2) values (now+2h, 1, 2)')
        self.tdRest.error(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5, c6) values (now, 1, 2, 3, 4, 5, 6)')
        self.tdRest.error(f'insert into {dbname}.tb (col_ts, c1, c6) values (now, 1, 6)')
        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 3)
        self.tdRest.request(f'drop database if exists {dbname}')

    def dif_update_specified_column(self):
        '''
            update = 0, 1, 2
        '''
        for update in [0, 1, 2]:
            dbname = self.tdCom.get_long_name(len=5, mode="letters")
            ts = self.tdCom.genTs()[0]
            self.tdRest.request(f'create database if not exists {dbname} update {update}')
            self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
                (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
            self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({ts}, 1, 2, 3, 4, 5)')
            self.tdRest.request(f'insert into {dbname}.tb values ({ts}, 1, 2, 3, 4, 5)')
            self.tdRest.request(f'insert into {dbname}.tb (col_ts, c1, c2) values ({ts}, 1, null)')
            self.tdRest.request(f'select c1, c2, c3, c4, c5 from {dbname}.stb')
            if update == 0:
                self.tdSql.checkEqual(self.tdRest.resp["data"], [[1, 2, 3, 4, 5]])
            if update == 1:
                self.tdSql.checkEqual(self.tdRest.resp["data"], [[1, None, None, None, None]])
            if update == 2:
                self.tdSql.checkEqual(self.tdRest.resp["data"], [[1, 2, 3, 4, 5]])
            self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.specified_column_insert()
        self.dif_update_specified_column()

    def cleanup(self):
        pass

    def desc(self):
        case_description = '''
            specified_column_insert <jayden>: [TD-12748] : specified column insert;\n
            dif_update_specified_column <jayden>: [TD-12748] : different update value of specified column;
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Insert.SpecifiedColumn