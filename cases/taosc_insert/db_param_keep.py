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
import time
class TestKeep(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.logical_error_list = ["1439m", "525600001m", "23h", "8760001h", "0d", "365001d"]
        self.common_days_value = "1h"
        self.error_value_list = ['3000, 4000, 5000', '3000, 3500, 4000', '3000, 3100, 3200', '20000, 10000, 30000', '30000, 10000, 20000', '10000, 30000, 20000']
        self.cfg = self.tdCom.Boundary.DB_PARAM_KEEP_CONFIG
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"

    def keep_check(self):
        """
        keep check
        """
        test_param = self.cfg["create_name"]

        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])

        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
        for i in self.taosd_setting['spec']['dnodes']:
            fqdn = i['endpoint'].split(':')[0]
            vnode_dir = i['config']['dataDir']+ "/vnode"
            if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                break
            else:
                continue
        self.tdSql.checkEqual(db_field_kv_dict[test_param], f"{int(int(data['config']['keep0'])/60/24)}d,{int(int(data['config']['keep1'])/60/24)}d,{int(int(data['config']['keep2'])/60/24)}d")
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {"duration": self.common_days_value, test_param: param_value}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdSql.query('select * from information_schema.ins_databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            if param_value==1 or param_value ==365000:
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param_value}d,{param_value}d,{param_value}d')
            elif param_value == '1d' or param_value == '365000d':
                param = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{param_value},{param_value},{param_value}')
            elif param_value == '1440m' or param_value == '525600000m':
                param = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{int(param/60/24)}d,{int(param/60/24)}d,{int(param/60/24)}d')
            elif param_value == '24h' or param_value =='8760000h':
                param = int(re.sub('\D','', param_value))
                self.tdSql.checkEqual(db_field_kv_dict[test_param], f'{int(param/24)}d,{int(param/24)}d,{int(param/24)}d')
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
            self.tdSql.checkEqual(db_field_kv_dict[test_param], f"{int(int(data['config']['keep0'])/60/24)}d,{int(int(data['config']['keep1'])/60/24)}d,{int(int(data['config']['keep2'])/60/24)}d")
            self.tdSql.execute(f'drop database {dbname}')

        dbname = self.tdCom.get_long_name()

        self.tdSql.error(f'create database if not exists {dbname} duration {self.common_days_value} {test_param} {self.cfg["boundary"][0]-1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][1] + 1}')
        for logical_error_value in self.logical_error_list:
            self.tdSql.error(f'create database if not exists {dbname} duration {self.common_days_value} {test_param} {logical_error_value}')

        # keep2 >= keep1 >= keep0 >= days (default = 14400)
        # keep2 >= keep1 >= keep0 >= days
        kv_dict = {test_param: "36500,36501,36502"}
        self.tdCom.createDb(dbname, **kv_dict)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "36500d,36501d,36502d")
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
        self.tdSql.checkEqual(db_field_kv_dict[test_param], f"{int(int(data['config']['keep0'])/60/24)}d,{int(int(data['config']['keep1'])/60/24)}d,{int(int(data['config']['keep2'])/60/24)}d")
        self.tdSql.execute(f'drop database {dbname}')
        # keep2(default) > keep1 >= keep0 >= days
        dbname = self.tdCom.get_long_name()
        kv_dict = {test_param: "36500,36501"}
        self.tdCom.createDb(dbname, **kv_dict)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "36500d,36501d,36501d")
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
        self.tdSql.checkEqual(db_field_kv_dict[test_param], f"{int(int(data['config']['keep0'])/60/24)}d,{int(int(data['config']['keep1'])/60/24)}d,{int(int(data['config']['keep2'])/60/24)}d")
        self.tdSql.execute(f'drop database {dbname}')
        # keep2 = keep1 = keep0 = days
        dbname = self.tdCom.get_long_name()
        kv_dict = {"duration": 3, test_param: "10,10,10"}
        self.tdCom.createDb(dbname, **kv_dict)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], "10d,10d,10d")
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
        self.tdSql.checkEqual(db_field_kv_dict[test_param], f"{int(int(data['config']['keep0'])/60/24)}d,{int(int(data['config']['keep1'])/60/24)}d,{int(int(data['config']['keep2'])/60/24)}d")
        self.tdSql.execute(f'drop database {dbname}')
        # error
        # keep2 >= keep1 >= days >= keep0
        # keep2 >= days >= keep1 >= keep0
        # days >= keep2 >= keep1 >= keep0
        # keep2 >= keep0 >= keep1 >= days
        # keep0 >= keep2 >= keep1 >= days
        # keep1 >= keep2 >= keep0 >= days
        dbname = self.tdCom.get_long_name()
        for days_value in ["", 3650]:
            for error_value in self.error_value_list:
                base_sql = f'create database if not exists {dbname} {test_param} {error_value}'
                if days_value != "":
                    base_sql += f" days {days_value}"
                    self.tdSql.error(base_sql)

    def keep_checkdata(self):
        self.tdSql.execute("drop database if exists db1 ")
        kv_dict = {"duration": "1d", "keep": "10d,10d,10d"}
        self.tdCom.createDb("db1", **kv_dict)
        self.tdSql.execute("create table ntb (ts timestamp, c0 int)")
        self.tdSql.execute("insert into ntb values(now, 1)")
        self.tdSql.query("select * from ntb")
        self.tdSql.checkRow(1)
        self.tdSql.error("insert into ntb values(now-11d,1)")
        # self.tdSql.error("insert into ntb values(now+2d, 1)")
        self.tdSql.execute("drop database db1")

        # bug TD-15499
        self.tdSql.execute("drop database if exists db1")
        kv_dict = {"keep": "36500d"}
        self.tdCom.createDb("db1", **kv_dict)
        self.tdSql.execute("create table ntb (ts timestamp, c0 int)")
        self.tdSql.execute("insert into ntb values(0, 1)")
        self.tdSql.query("select * from ntb")
        self.tdSql.checkRow(1)
        self.tdSql.execute("insert into ntb values(-1, 1)")
        self.tdSql.checkRow(1)

    def keep_expired_check(self):
        self.tdSql.execute("drop database if exists db1 ")
        kv_dict = {"duration": "1d", "keep": "3d,3d,3d"}
        self.tdCom.createDb("db1", **kv_dict)
        self.tdSql.execute("create table ntb (ts timestamp, c0 int)")
        self.tdSql.execute(f'insert into ntb values(now-{3*86400-10}s, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkRow(1)
        time.sleep(15)
        self.tdSql.query("select * from ntb")
        self.tdSql.checkRow(0)

    def run(self) -> bool:
        self.keep_check()
        self.keep_checkdata()
        self.keep_expired_check()

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

