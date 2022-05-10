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

class TestMaxrows(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def days_check(self):
        """
        days check
        """
        test_param = "days"
        # default
        default_value = 14400
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.execute(f'drop database {dbname}')

        # param_list
        
        pass


    def run(self) -> bool:
        self.days_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            comp check <jiacy>: [TD-15381] : days check;
            """
        return case_description

    def author(self) -> str:
        return "jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter