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

class TestNcharBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def nchar_length_check(self):
        '''
            max length: 4093
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        str_4093 = self.tdCom.get_long_name(len=4093, mode="letters")
        str_4094 = self.tdCom.get_long_name(len=4094, mode="letters")
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 nchar(4093)) tags (t1 nchar(4093))')
        self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ("{str_4093}")')
        self.tdSql.execute(f'insert into {dbname}.tb values (now, "{str_4093}")')
        self.tdSql.query(f'select t1, c1 from {dbname}.tb')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), str_4093)
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][1]), str_4093)
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 nchar(4093)) tags (t1 nchar(4094))')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 nchar(4094)) tags (t1 nchar(4093))')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, "{str_4094}")')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1h, "{str_4094}")')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.nchar_length_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            nchar_length_check <jayden>: [TD-13419] : nchar length check (max 4093);
        '''
        return case_description