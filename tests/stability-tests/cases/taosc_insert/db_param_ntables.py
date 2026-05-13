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

class TestNtables(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.cfg = self.tdCom.Boundary.DB_PARAM_NTABLES_CONFIG

    def ntables_check(self):
        """
        ntables check
        """
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        # TODO

    def run(self) -> bool:
        self.ntables_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            ntables check <jayden>: [TD-14991] : ntables check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

