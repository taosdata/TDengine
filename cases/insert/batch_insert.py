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

from taostest import TDCase
from taostest.util.common import TDCom

class TestBatchInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def batch_insert(self):
        '''
            batch_insert 
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
            (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5)')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5), (now+1h, 1, 2, 3, 4, 5), (now+2h, 1, 2, 3, 4, 5);')
        self.tdSql.query(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdSql.query_data[0][0]), 3)
        self.tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, 2),(now-2h, 1, 2),(now-3h, 1, 2)')
        self.tdSql.query(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdSql.query_data[0][0]), 6)
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2, c9) values (now-1h, 1, 2, 1), (now-2h, 1, 2, 1), (now-2h, 1, 2, 1)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, "binary"), (now-2h, 1, 2), (now-2h, 1, 2)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, 2)&&(now-2h, 1, 2), (now-2h, 1, 2)')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.batch_insert()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            batch_insert <jayden>: [TD-13419] : batch_insert;
        '''
        return case_description