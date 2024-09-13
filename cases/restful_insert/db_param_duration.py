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
import re
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
from taostest.util.rest import TDRest

class TestDuration(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.cfg = self.tdCom.Boundary.DB_PARAM_DURATION_CONFIG
        self.error_value_list = [-1, 3651, "59m", "5256001m", "0h", "87601h", "0d", "3651d"]
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        self.api_type = 'restful'
    def duration_check(self):
        """
        duration check
        """
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdRest.request('select * from information_schema.ins_databases')
         # TODO
        db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,test_param,dbname)
        # default
        self.tdSql.checkEqual(db_field, self.cfg["default"])
        self.tdRest.request(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdRest.getOneRow(1, dbname)
        for i in self.taosd_setting['spec']['dnodes']:
            fqdn = i['endpoint'].split(':')[0]
            vnode_dir = i['config']['dataDir']+ "/vnode"
            if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                break
            else:
                continue
        self.tdSql.checkEqual(db_field, f'{int(int(data["config"][self.cfg["vnode_json_key"]])/60/24)}d')
        self.tdRest.request(f'drop database {dbname}')
        # without unit
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: param_value}
            int_part = int(''.join(list(filter(str.isdigit, str(param_value).strip()))))
            str_part = str(''.join(list(filter(str.isalpha, str(param_value).strip()))))
            new_keep_value = str(int_part * 3) + str_part
            if int_part < 1440:
                new_keep_value = str(1440) + str_part
            kv_dict["keep"] = new_keep_value
            kv_dict["s3_keeplocal"] = new_keep_value
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdRest.request('select * from information_schema.ins_databases')
            #TODO
            db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,test_param,dbname)
            if param_value == 1: # days
                self.tdSql.checkEqual(db_field, '1d')
            elif param_value == 3650: # days
                self.tdSql.checkEqual(db_field, '3650d')
            elif param_value == '60m': # minutes
                trans_value = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field, '1h')
            elif param_value == '5256000m': # minutes
                trans_value = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field, f'3650d')
            elif param_value == '24h' or param_value =='87600h': # hours
                trans_value = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field, f'{int(trans_value / 24)}d')
            elif param_value == '1d' or param_value == '3650d':
                trans_value = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field, param_value)
            self.tdRest.request(f'show {dbname}.vgroups')
             # TODO
            db_vnode_kv_dict = self.tdRest.getOneRow(1, dbname)
            for i in self.taosd_setting['spec']['dnodes']:
                fqdn = i['endpoint'].split(':')[0]
                vnode_dir = i['config']['dataDir']+ "/vnode"
                if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                    data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                    break
                else:
                    continue
            if param_value == 1 or param_value == 3650 or param_value == "5256000m" or param_value == "1d" or param_value == "3650d": # days
                self.tdSql.checkEqual(db_field, f'{int(int(data["config"][self.cfg["vnode_json_key"]])/60/24)}d')
            elif param_value == "60m": # m
                self.tdSql.checkEqual(db_field, f'{int(int(data["config"][self.cfg["vnode_json_key"]])/60)}h')
            elif param_value == "24h" or param_value == '87600h': # h
                self.tdSql.checkEqual(db_field, f'{int(int(data["config"][self.cfg["vnode_json_key"]])/60/24)}d')
            elif param_value == "1h": # h
                self.tdSql.checkEqual(db_field, f'{int(int(data["config"][self.cfg["vnode_json_key"]])/60)}h')
            else:
                self.tdSql.checkEqual(db_field, f'{data["config"][self.cfg["vnode_json_key"]]}m')
            self.tdRest.request(f'drop database {dbname}')
        for error_value in self.error_value_list:
            self.tdRest.error(f'create database if not exists {dbname} {test_param} {error_value}')

    def run(self) -> bool:
        self.duration_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
                duration check <jiacy> : [TD-15381] : duration check for taosd;
            """
        return case_description

    def author(self) -> str:
        return "Jayden,Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

