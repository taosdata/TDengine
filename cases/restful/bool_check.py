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

class TestBool(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def bool_check(self):
        '''
            True: true/TrUe.... != 0
            False: false/FalSe... = 0
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 bool) tags (t1 bool)')
        for true_generator in [self.tdCom.str_trans("true"), (x for x in [2, -2, "true"])]:
            for true_value in true_generator:
                self.tdRest.request(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({true_value})')
                self.tdRest.request(f'insert into {dbname}.tb1 values (now, {true_value})')
                self.tdRest.request(f'select t1, c1 from {dbname}.tb1')
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], True)
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], True)
                self.tdRest.request(f'drop table if exists {dbname}.tb1')
        
        for false_generator in [self.tdCom.str_trans("false"), (x for x in [0])]:
            for false_value in false_generator:
                self.tdRest.request(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({false_value})')
                self.tdRest.request(f'insert into {dbname}.tb1 values (now, {false_value})')
                self.tdRest.request(f'select t1, c1 from {dbname}.tb1')
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], False)
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], False)
                self.tdRest.request(f'drop table if exists {dbname}.tb1')

        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.bool_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            bool_check <jayden>: [TD-12748] : bool check;
        '''
        return case_description
        
    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Insert.BoundaryTest.Bool