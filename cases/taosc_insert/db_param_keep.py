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
class TestKeep(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                

    def get_vnode_json(self,db_vnode_kv_dict):
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"vnode/vnode{db_vnode_kv_dict[0][0]}"
        self._remote.get(self.fqdn,f'{self.vnode_dir}/vnode.json',f'{self.run_log_dir}/vnode.json')
        file = open(f'{self.run_log_dir}/vnode.json')
        return file

    def keep_check(self):
        """
        keep check
        """
        test_param = "keep"
        # default
        default_value = "5256000,5256000,5256000"

        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)

        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.load(self.get_vnode_json(db_vnode_kv_dict))
        self.tdSql.checkEqual(db_field_kv_dict[test_param],f"{data['config']['keep0']},{data['config']['keep1']},{data['config']['keep2']}")
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        param_value_list = [1, 365000,'1440m','525600000m','24h','8760000h','1d','365000d']
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} days 1h {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            if param_value==1 or param_value ==365000:
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param_value*24*60},{param_value*24*60},{param_value*24*60}')
            elif param_value == '1d' or param_value == '365000d':
                param = int(re.sub('\D','',param_value))
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param*24*60},{param*24*60},{param*24*60}')
            elif param_value == '1440m' or param_value == '525600000m':
                param = int(re.sub('\D','',param_value))
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param},{param},{param}')
            elif param_value == '24h' or param_value =='8760000h':
                param = int(re.sub('\D','',param_value))
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param*60},{param*60},{param*60}')
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            data = json.load(self.get_vnode_json(db_vnode_kv_dict))
            self.tdSql.checkEqual(db_field_kv_dict[test_param],f"{data['config']['keep0']},{data['config']['keep1']},{data['config']['keep2']}")
            self.tdSql.execute(f'drop database {dbname}')
        
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} {param_value_list[0]-1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[1] + 1}')
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} 1439m')
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} 525600001m')
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} 23h')
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} 8760001h')
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} 0d')
        self.tdSql.error(f'create database if not exists {dbname} days 1h {test_param} 365001d')
        
        # keep2 >= keep1 >= keep0 >= days (default = 14400)
        # keep2 >= keep1 >= keep0 >= days
        self.tdSql.execute(f'create database if not exists {dbname} {test_param} 36500,36501,36502')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "52560000,52561440,52562880")
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.load(self.get_vnode_json(db_vnode_kv_dict))
        self.tdSql.checkEqual(db_field_kv_dict[test_param],f"{data['config']['keep0']},{data['config']['keep1']},{data['config']['keep2']}")
        self.tdSql.execute(f'drop database {dbname}')
        # keep2(default) > keep1 >= keep0 >= days
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname} {test_param} 36500,36501')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "52560000,52561440,52561440")
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.load(self.get_vnode_json(db_vnode_kv_dict))
        self.tdSql.checkEqual(db_field_kv_dict[test_param],f"{data['config']['keep0']},{data['config']['keep1']},{data['config']['keep2']}")
        self.tdSql.execute(f'drop database {dbname}')
        # keep2 = keep1 = keep0 = days
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname} days 10 {test_param} 10,10,10')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "14400,14400,14400")
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.load(self.get_vnode_json(db_vnode_kv_dict))
        self.tdSql.checkEqual(db_field_kv_dict[test_param],f"{data['config']['keep0']},{data['config']['keep1']},{data['config']['keep2']}")
        self.tdSql.execute(f'drop database {dbname}')
        # error
        # keep2 >= keep1 >= days >= keep0
        # keep2 >= days >= keep1 >= keep0
        # days >= keep2 >= keep1 >= keep0
        # keep2 >= keep0 >= keep1 >= days
        # keep0 >= keep2 >= keep1 >= days
        # keep1 >= keep2 >= keep0 >= days
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        error_value_list = ['3000, 4000, 5000', '3000, 3500, 4000', '3000, 3100, 3200', '20000, 10000, 30000', '30000, 10000, 20000', '10000, 30000, 20000']
        for days_value in ["", 3650]:
            for error_value in error_value_list:
                base_sql = f'create database if not exists {dbname} {test_param} {error_value}'
                if days_value != "":
                    base_sql += f" days {days_value}"
                    self.tdSql.error(base_sql)

    def keep_checkdata(self):
        self.tdSql.execute("drop database if exists db1 ")
        self.tdSql.execute("create database if not exists db1 days 1d keep 10d,15d,20d")
        self.tdSql.execute("use db1")
        self.tdSql.execute("create table ntb (ts timestamp,c0 int)")
        self.tdSql.execute("insert into ntb values(now,1)")
        self.tdSql.query("select * from ntb")
        self.tdSql.checkRow(1)
        self.tdSql.execute("insert into ntb values('2020-1-1 00:00:00',1)")
        self.tdSql.query("select * from ntb")
        self.tdSql.checkRow(1)

        


    def run(self) -> bool:
        self.keep_check()
        self.keep_checkdata()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            keep check <jayden>: [TD-14991] : keep check;
            keep check <jiacy> : [TD-15635] : keep check with taosd
            """
        return case_description

    def author(self) -> str:
        return "Jayden,jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

