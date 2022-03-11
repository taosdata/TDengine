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

class TestAutoCreateTable(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def check_tag_value_for_auto_create_table(self):
        """
        check tag value
        """
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdRest.request(f'insert into {dbname}.t1 using {dbname}.stb(t11, t12) tags(11, 12) (ts, c11, c12) values (now, 11, 21)')
        self.tdRest.request(f'insert into {dbname}.t2 using {dbname}.stb(t11) tags(21) (ts, c11, c12) values (now-1m, 12, 22)')
        self.tdRest.request(f'insert into {dbname}.t3 using {dbname}.stb tags(31, 32) (ts, c11, c12) values (now-2m, 13, 23)')
        self.tdRest.request(f'insert into {dbname}.t4 using {dbname}.stb(t11, t12) tags("41", 42) (ts, c11, c12) values (now-3m, 14, 24)')
        # no tags
        self.tdRest.error(f'insert into {dbname}.t5 using {dbname}.stb(t11, t12) (ts, c11, c12) values (now-4m, 11, 21)')
        # blank tags
        self.tdRest.error(f'insert into {dbname}.t6 using {dbname}.stb(t11, t12) tags() (ts, c11, c12) values (now-5m, 11, 21)')
        # count nmatch
        self.tdRest.error(f'insert into {dbname}.t7 using {dbname}.stb(t11, t12) tags(41) (ts, c11, c12) values (now-6m, 11, 21)')
        self.tdRest.error(f'insert into {dbname}.t8 using {dbname}.stb(t11) tags(51, 52) (ts, c11, c12) values (now-7m, 11, 21)')
        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 4)
        self.tdRest.request(f'drop database if exists {dbname}')

    def check_col_value_for_auto_create_table(self):
        """
        check col value
        """
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdRest.request(f'insert into {dbname}.t1 using {dbname}.stb(t11, t12) tags(11, 12) values (now, 11, 21)')
        self.tdRest.request(f'insert into {dbname}.t2 using {dbname}.stb(t11, t12) tags(21, 22) (ts, c11, c12) values (now-1m, Null, 21)')
        self.tdRest.request(f'insert into {dbname}.t3 using {dbname}.stb(t11, t12) tags(31, 32) (ts, c11, c12) values (now-2m, "Null", 31)')
        self.tdRest.error(f'insert into {dbname}.t4 using {dbname}.stb(t11, t12) tags(41, 42) (c11, c12) values (41, 42)')
        # no values
        self.tdRest.error(f'insert into {dbname}.t5 using {dbname}.stb(t11, t12) (ts, c11, c12) tags(51, 52) values ()')
        self.tdRest.error(f'insert into {dbname}.t6 using {dbname}.stb(t11, t12) (ts, c11, c12) tags(61, 62)')
        # count nmatch
        self.tdRest.error(f'insert into {dbname}.t7 using {dbname}.stb(t11, t12) tags(71, 72) values (now-3m, 71)')
        self.tdRest.error(f'insert into {dbname}.t8 using {dbname}.stb(t11, t12) tags(81, 82) (ts, c11) values (now-4m, 81, 82)')
        self.tdRest.error(f'insert into {dbname}.t9 using {dbname}.stb(t11, t12) tags(91, 92) (ts, c11, c12) values (now-5m, 91)')
        # type nmatch
        self.tdRest.request(f'insert into {dbname}.t10 using {dbname}.stb(t11, t12) tags(101, 102) (ts, c11, c12) values (now-6m, Nan, 102)')
        self.tdRest.request(f'insert into {dbname}.t11 using {dbname}.stb(t11, t12) tags(111, 112) (ts, c11, c12) values (now-7m, "Nan", 112)')

        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 3)
        self.tdRest.request(f'drop database if exists {dbname}')

    def check_multi_cols_for_auto_create_table(self):
        """
        check multi cols
        """
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdRest.request(f'insert into {dbname}.t1 using {dbname}.stb(t11, t12) tags(11, 12) (ts, c11, c12) values (now-1m, 11, 21)(now-2m, 11, 21)')
        self.tdRest.request(f'insert into {dbname}.t2 using {dbname}.stb(t11, t12) tags(11, 12) values (now-3m, 11, 21)(now-4m, 11, 21)')
        self.tdRest.request(f'insert into {dbname}.t3 (ts, c11, c12) using {dbname}.stb(t11, t12) tags(11, 12) values (now-5m, 11, 21)(now-6m, 11, 21)')
        self.tdRest.error(f'insert into {dbname}.t4 using {dbname}.stb(t11, t12) tags(11, 12) values (now-7m, 11, 21) (ts, c11, c12) values (now-8m, 11, 21)')
        self.tdRest.error(f'insert into {dbname}.t5 using {dbname}.stb(t11, t12) tags(11, 12) (ts, c11, c12) values (now-9m, 11, 21) (ts, c11, c12) values (now-10m, 11, 21)')
        self.tdRest.error(f'insert into {dbname}.t6 (ts, c11, c12) using {dbname}.stb(t11, t12) values (now-11m, 11, 21) (now-12m, 11, 21) using {dbname}.stb(t11, t12) tags(11, 12')
        self.tdRest.error(f'insert into {dbname}.t7 (ts, c11, c12) using {dbname}.stb(t11, t12) using {dbname}.stb(t11, t12) tags(11, 12')
        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 6)
        self.tdRest.request(f'drop database if exists {dbname}')


    def run(self) -> bool:
        self.check_tag_value_for_auto_create_table()
        self.check_col_value_for_auto_create_table()
        self.check_multi_cols_for_auto_create_table()

    def cleanup(self):
        pass
        
    def desc(self) -> str:
        case_description = '''
            check_tag_value_for_auto_create_table <jayden>: [TD-12748] : check tag value;\n
            check_col_value_for_auto_create_table <jayden>: [TD-12748] : check col value;\n
            check_multi_cols_for_auto_create_table <jayden>: [TD-12748] : check multi cols;
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Table.Create.AutoCreate

