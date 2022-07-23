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
class TestPagesize(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.cfg = self.tdCom.Boundary.DB_PARAM_PAGESIZE_CONFIG
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        
    def pagesize_check(self):
        """
        pagesize check
        """
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
        data = json.loads(self.remote.cmd(self.fqdn,f'cat {self.vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
        self.tdSql.checkEqual(db_field_kv_dict[test_param], int(data['config'][self.cfg["vnode_json_key"]])/1024)
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: param_value}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
            data = json.loads(self.remote.cmd(self.fqdn,f'cat {self.vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
            self.tdSql.checkEqual(db_field_kv_dict[test_param], int(data['config'][self.cfg["vnode_json_key"]])/1024)
            self.tdSql.execute(f'drop database {dbname}')

        dbname = self.tdCom.get_long_name()
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][-1] + 1}')

        #! alter database pagesize  bug:TD-16324
        # dbname = self.tdCom.get_long_name()
        # self.tdCom.createDb(dbname)
        # self.cfg["boundary"] = [1, 16384]
        # for param_value in self.cfg["boundary"]:
        #     dbname = self.tdCom.get_long_name()
        #     self.tdSql.execute(f'alter database {dbname} {test_param} {param_value}')
        #     self.tdSql.query('show databases')
        #     db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        #     self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
        #     self.tdSql.query(f'show {dbname}.vgroups')
        #     db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        #     data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
        #     self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config']['szPage'])/1024)
        #! bug TD-16166
        # for i in [self.cfg["boundary"][0]-1,self.cfg["boundary"][-1]+1,100.5,'abc']:
        #     self.tdSql.error(f'alter database  {dbname} {test_param} {i}')
        
        # self.tdSql.execute(f'drop database {dbname}')
    def run(self) -> bool:
        self.pagesize_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            pagesize check <jayden>: [TD-14991] : pagesize check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter