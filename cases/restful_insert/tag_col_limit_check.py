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

class TestTagColLimit(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def tag_max_count_check(self):
        """
        max count: 128
        """
        tag_str_exceed = self.tdCom.gen_tag_col_str("tag", "int", 129)
        tag_str = self.tdCom.gen_tag_col_str("tag", "int", 128)
        dbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname} precision "ms"')
        self.tdRest.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags ({tag_str_exceed})')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags ({tag_str})')
        tag_value_str = '1, ' * 127 + '1'
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({tag_value_str})')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1)')
        self.tdRest.request(f'select tag127 from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 1)
        self.tdRest.request(f'drop database if exists {dbname}')

    def col_max_count_check(self):
        """
        max col count: 4096
        """
        col_str_exceed = self.tdCom.gen_tag_col_str("col", "int", 4095)
        col_str = self.tdCom.gen_tag_col_str("col", "int", 4094)
        dbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname} precision "ms"')
        self.tdRest.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, {col_str_exceed}) tags (t1 int)')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, {col_str}) tags (t1 int)')
        col_value_str = '1, ' * 4093 + '1'
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, {col_value_str})')
        self.tdRest.request(f'select col4093 from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 1)
        self.tdRest.request(f'drop database if exists {dbname}')

    def sensitive_check(self):
        """
        tag_key/col_key sensitive
        """
        for test_type in ['binary', 'nchar']:
            dbname = self.tdCom.get_long_name(length=5, mode="letters")
            self.tdRest.request(f'create database if not exists {dbname}')
            self.tdRest.request(f'create stable if not exists {dbname}.stb (Col_ts timestamp, CC1 {test_type}(16), Cc2 {test_type}(16), `3Cc%3` {test_type}(16)) tags (`1Tag_ts^` timestamp, TT1 {test_type}(16), Tt2 {test_type}(16), `3Tt%3` {test_type}(16))')
            self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, "TT1", "Tt2", "3Tt%3")')
            self.tdRest.request(f'insert into {dbname}.tb values (now, "TT1", "Tt2", "3Tt%3")')
            self.tdRest.request(f"describe {dbname}.stb")
            col_key_list = self.tdRest.getColNameList(True)[0]
            self.tdSql.checkEqual(col_key_list, ['col_ts', 'cc1', 'cc2', '3Cc%3', '1Tag_ts^', 'tt1', 'tt2', '3Tt%3'])
            self.tdRest.request(f'select * from {dbname}.stb')
            lres = list(self.tdRest.resp["data"][0])
            lres.pop(0)
            lres.pop(3)
            self.tdSql.checkEqual(lres, ['TT1', 'Tt2', '3Tt%3', 'TT1', 'Tt2', '3Tt%3'])
            self.tdRest.request(f'drop database if exists {dbname}')

    def tag_col_name_length_check(self):
        """
        max tag key length:
        """
        dbname = self.tdCom.get_long_name(length=5, mode="letters")
        tag_key_name = self.tdCom.get_long_name(length=64, mode="letters")
        col_key_name = self.tdCom.get_long_name(length=64, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error (col_ts timestamp, {col_key_name}a int) tags ({tag_key_name} int)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error (col_ts timestamp, {col_key_name} int) tags ({tag_key_name}a int)')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, {col_key_name} int) tags ({tag_key_name} int)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1)')
        self.tdRest.request(f"describe {dbname}.stb")
        col_key_list = self.tdRest.getColNameList(True)[0]
        self.tdSql.checkEqual(col_key_list, ['col_ts', col_key_name, tag_key_name])

    def run(self):
        self.tag_max_count_check()
        self.col_max_count_check()
        self.sensitive_check()
        self.tag_col_name_length_check()

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            tag_max_count_check <jayden>: [TD-12748] : tag_max_count_check;\n
            col_max_count_check <jayden>: [TD-12748] : col_max_count_check;\n
            sensitive_check <jayden>: [TD-12748] : sensitive_check;\n
            tag_col_name_length_check <jayden>: [TD-12748] : tag_col_name_length_check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.RestfulSql.Insert.BoundaryTest.Tag, T.Write.RestfulSql.Insert.BoundaryTest.Column