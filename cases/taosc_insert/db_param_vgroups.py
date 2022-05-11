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
from taostest.util.remote import Remote

class TestVgroups(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"

    def get_vnode_count(self):
        return int(self._remote.cmd(self.fqdn, [f'ls {self.vnode_dir} | grep -v vnodes.json | wc -l']))

    def vgroups_check(self):
        """
        vgroups check
        """
        self.tdCom.drop_all_db()
        test_param = "vgroups"
        # default
        default_value = 2
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        # ! 4096 bug TD-15451
        param_value_list = [1, 10]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.execute(f'drop database {dbname}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        # ! bug TD-15096
        # self.tdSql.error(f'create database if not exists {dbname} vgroups {param_value_list[-1] + 1}')
        # check logic
        dbname1 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname1} vgroups 3')
        self.tdSql.query(f'show {dbname1}.vgroups')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.checkEqual(self.get_vnode_count(), 3)
        dbname2 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname2} vgroups 4')
        self.tdSql.query(f'show {dbname2}.vgroups')
        self.tdSql.checkEqual(self.tdSql.query_row, 4)
        self.tdSql.checkEqual(self.get_vnode_count(), 4+3)
        self.tdSql.execute(f'drop database {dbname1}')
        self.tdSql.checkEqual(self.get_vnode_count(), 4)
        self.tdSql.execute(f'drop database {dbname2}')
        self.tdSql.checkEqual(self.get_vnode_count(), 0)

    def run(self) -> bool:
        self.vgroups_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            vgroups check <jayden>: [TD-14991] : vgroups check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

