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

import re
from taostest import TDCase, T
from taostest.util.common import TDCom

class TestDuration(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def duration_check(self):
        """
        duration check
        """
        test_param = "days"
        test_param_bak = "duration"
        # default
        default_value = 14400
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], default_value)
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        param_value_list = [60, 5256000]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], param_value)
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[-1] + 1}')
        param_minute_list =['60m','5256000m']
        for param_value in param_minute_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)))
            self.tdSql.execute(f'drop database {dbname}')
        param_hour_list =['1h','87600h']
        for param_value in param_hour_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)) * 60)
            self.tdSql.execute(f'drop database {dbname}')
        param_day_list =['1d','3650d']
        for param_value in param_day_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)) * 60 *24)
            self.tdSql.execute(f'drop database {dbname}')
    def run(self) -> bool:
        self.duration_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            duration check <jayden>: [TD-14991] : duration check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

