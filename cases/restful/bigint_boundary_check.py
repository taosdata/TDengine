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

class TestBigintBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def bigint_boundary_check(self):
        '''
            max: +- 9223372036854775807
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 bigint) tags (t1 bigint)')
        self.tdRest.request(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags (9223372036854775807)')
        self.tdRest.request(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags (-9223372036854775807)')
        self.tdRest.request(f'insert into {dbname}.tb1 values (now, -9223372036854775807)')
        self.tdRest.request(f'insert into {dbname}.tb2 values (now, 9223372036854775807)')
        self.tdRest.request(f'select t1, c1 from {dbname}.tb1')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 9223372036854775807)
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][1]), -9223372036854775807)
        self.tdRest.request(f'select t1, c1 from {dbname}.tb2')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), -9223372036854775807)
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][1]), 9223372036854775807)
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 9223372036854775807) tags (t1 9223372036854775808)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 9223372036854775808) tags (t1 9223372036854775807)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error3 (col_ts timestamp, c1 9223372036854775807) tags (t1 -9223372036854775808)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error4 (col_ts timestamp, c1 -9223372036854775808) tags (t1 -9223372036854775807)')
        self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, 9223372036854775808)')
        self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, -9223372036854775808)')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1h, 9223372036854775808)')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1h, -9223372036854775808)')
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.bigint_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            bigint_boundary_check <jayden>: [TD-12748] : bigint boundary check (max 9223372036854775807);
        '''
        return case_description
        
    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Insert.BoundaryTest.Bigint