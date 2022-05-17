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

import json
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote

class TestComp(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
    def get_vnode_json(self,db_vnode_kv_dict):
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"vnode/vnode{db_vnode_kv_dict[0][0]}"
        self._remote.get(self.fqdn,f'{self.vnode_dir}/vnode.json',f'{self.run_log_dir}/vnode.json')
        file = open(f'{self.run_log_dir}/vnode.json')
        return file
    def comp_check(self):
        """
        comp check
        """
        test_param = "comp"
        # default
        default_value = 2
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        # print(db_vnode_kv_dict)
        data = json.load(self.get_vnode_json(db_vnode_kv_dict))
        # print(data)
        self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config']['compression']))
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        # param_list
        param_value_list = [0, 1, 2]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            # print(db_vnode_kv_dict)
            data = json.load(self.get_vnode_json(db_vnode_kv_dict))
            # print(data)
            self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config']['compression']))
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[-1] + 1}')

    def run(self) -> bool:
        self.comp_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            comp check <jayden>: [TD-14991] : comp check;
            comp check <jiacy>:  [TD-15381] : comp check for taosd;
            """
        return case_description

    def author(self) -> str:
        return "Jayden,Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

