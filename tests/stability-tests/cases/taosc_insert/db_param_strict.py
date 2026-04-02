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
class TestStrict(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.cfg = self.tdCom.Boundary.DB_PARAM_STRICT_CONFIG
    def strict_check(self):
        """
        strict check
        """
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('select * from information_schema.ins_databases')
        #TODO
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        for param, param_value in self.cfg["boundary"].items():
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: f'"{param_value}"'}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdSql.query('select * from information_schema.ins_databases')
            #TODO
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param)
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name()
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 1')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} "a"')

    def run(self) -> bool:
        self.strict_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            strict check <jayden>: [TD-14991] : strict check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

