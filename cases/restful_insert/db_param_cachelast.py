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
from taostest.util.rest import TDRest
from taostest.util.remote import Remote
class TestCachelast(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.cfg = self.tdCom.Boundary.DB_PARAM_CACHELAST_CONFIG
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.remote: Remote = Remote(self.logger)
        self.api_type = 'restful'
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
    def cachelast_check(self):
        """
        cachelast check
        """
        test_param = self.cfg["create_name"]
        get_param = self.cfg["query_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdRest.request('show databases')
        #TODO
        db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,get_param,dbname)
        # default
        self.tdSql.checkEqual(db_field, str(list(self.cfg["default"].keys())[0]).lower())
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        for i in self.taosd_setting['spec']['dnodes']:
            fqdn = i['endpoint'].split(':')[0]
            vnode_dir = i['config']['dataDir']+ "/vnode"
            if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                break
            else:
                continue
        self.tdSql.checkEqual(list(self.cfg["default"].values())[0],int(data['config'][self.cfg["vnode_json_key"]]))
        self.tdRest.request(f'drop database {dbname}')
        # param_list
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: f'"{param_value}"'}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdRest.request('show databases')
            db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,get_param,dbname)
            self.tdSql.checkEqual(db_field, str(param_value).lower())
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            for i in self.taosd_setting['spec']['dnodes']:
                fqdn = i['endpoint'].split(':')[0]
                vnode_dir = i['config']['dataDir']+ "/vnode"
                if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                    data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                    break
                else:
                    continue
            self.tdRest.request(f'drop database {dbname}')
        self.tdRest.error(f'create database if not exists {dbname} {test_param} 1')
        self.tdRest.error(f'create database if not exists {dbname} {test_param} "a"')

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

