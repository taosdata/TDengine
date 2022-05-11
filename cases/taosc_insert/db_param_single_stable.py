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

class TestSingle_stable(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def single_stable_check(self):
        """
        single_stable check
        """
        test_param = "single_stable"
        # default
        default_value = 0
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        param_value_list = [0, 1]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            if param_value == 0:
                self.tdSql.execute(f'create table {dbname}.stb1 (ts timestamp, c1 int) tags (t1 int);')
                self.tdSql.execute(f'create table {dbname}.stb2 (ts timestamp, c1 int) tags (t1 int);')
                self.tdSql.query(f'show {dbname}.stables')
                self.tdSql.checkEqual(self.tdSql.query_row, 2)
            elif param_value == 1:
                self.tdSql.execute(f'create table {dbname}.stb1 (ts timestamp, c1 int) tags (t1 int);')
                self.tdSql.error(f'create table {dbname}.stb2 (ts timestamp, c1 int) tags (t1 int);')
                self.tdSql.query(f'show {dbname}.stables')
                self.tdSql.checkEqual(self.tdSql.query_row, 1)
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[-1] + 1}')

    def run(self) -> bool:
        self.single_stable_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            single_stable check <jayden>: [TD-14991] : single_stable check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

