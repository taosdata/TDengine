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

class TestCachelast(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.cfg = self.tdCom.Boundary.DB_PARAM_CACHELAST_CONFIG

    def cachelast_check(self):
        """
        cachelast check
        """
        test_param = self.cfg["create_name"]
        get_param = self.cfg["query_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[get_param], str(self.cfg["default"]).lower())
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: f'"{param_value}"'}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[get_param], str(param_value).lower())
            self.tdSql.execute(f'drop database {dbname}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 0')

    def run(self) -> bool:
        self.cachelast_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            cachelast check <jayden>: [TD-14991] : cachelast check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

