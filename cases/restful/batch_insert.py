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

class TestBatchInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def batch_insert(self):
        '''
            batch_insert 
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
            (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5), (now+1h, 1, 2, 3, 4, 5), (now+2h, 1, 2, 3, 4, 5);')
        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 3)
        self.tdRest.request(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, 2),(now-2h, 1, 2),(now-3h, 1, 2)')
        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 6)
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2, c9) values (now-1h, 1, 2, 1), (now-2h, 1, 2, 1), (now-2h, 1, 2, 1)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, "binary"), (now-2h, 1, 2), (now-2h, 1, 2)')
        self.tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, 2)&&(now-2h, 1, 2), (now-2h, 1, 2)')
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.batch_insert()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            batch_insert <jayden>: [TD-12748] : batch_insert;
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Insert.BatchInsert