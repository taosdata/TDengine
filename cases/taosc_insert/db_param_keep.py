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

class TestKeep(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def keep_check(self):
        """
        keep check
        """
        test_param = "keep"
        # default
        default_value = "5256000,5256000,5256000"
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        param_value_list = [1440, 525600000]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} days 365 {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param_value},{param_value},{param_value}')
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} keep {param_value_list[-1] + 1}')
        # keep2 >= keep1 >= keep0 >= days (default = 14400)
        # keep2 >= keep1 >= keep0 >= days
        self.tdSql.execute(f'create database if not exists {dbname} {test_param} 36500,36501,36502')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "36500,36501,36502")
        self.tdSql.execute(f'drop database {dbname}')
        # keep2(default) > keep1 >= keep0 >= days
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname} {test_param} 36500,36501')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "36500,36501,36501")
        self.tdSql.execute(f'drop database {dbname}')
        # keep2 = keep1 = keep0 = days
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname} days 14400 {test_param} 14400,14400,14400')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "14400,14400,14400")
        self.tdSql.execute(f'drop database {dbname}')
        # error
        # keep2 >= keep1 >= days >= keep0
        # keep2 >= days >= keep1 >= keep0
        # days >= keep2 >= keep1 >= keep0
        # keep2 >= keep0 >= keep1 >= days
        # keep0 >= keep2 >= keep1 >= days
        # keep1 >= keep2 >= keep0 >= days
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        error_value_list = ['10000, 20000, 30000', '10000, 11000, 30000', '10000, 11000, 12000', '20000, 10000, 30000', '30000, 10000, 20000', '10000, 30000, 20000']
        for days_value in ["", 14401]:
            for error_value in error_value_list:
                base_sql = f'create database if not exists {dbname} {test_param} {error_value}'
                if days_value != "":
                    base_sql += f" days {days_value}"



    def run(self) -> bool:
        self.keep_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            keep check <jayden>: [TD-14991] : keep check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

