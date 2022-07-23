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

class TestReplica(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.cfg = self.tdCom.Boundary.DB_PARAM_REPLICA_CONFIG
        self.error_boundary_list = [0, 2, 4]
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
    def replica_check(self):
        """
        replica check
        """
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.loads(self.remote.cmd(self.fqdn,f'cat {self.vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
        self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config'][self.cfg["vnode_json_key"]]))
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            data = json.loads(self.remote.cmd(self.fqdn,f'cat {self.vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
            self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config'][self.cfg["vnode_json_key"]]))
            self.tdSql.execute(f'create table if not exists {dbname}.stb (ts timestamp, c1 int) tags (t1 int)')
            self.tdSql.execute(f'create table if not exists {dbname}.sub_tb using {dbname}.stb tags (1)')
            self.tdSql.execute(f'insert into {dbname}.sub_tb values (now, 1)')
            self.tdSql.execute(f'create table if not exists {dbname}.tb (ts timestamp, c1 int)')
            self.tdSql.execute(f'insert into {dbname}.tb values (now, 1)')
            for sql in [f'select c1 from {dbname}.stb', f'select * from {dbname}.sub_tb', f'select * from {dbname}.tb']:
                self.tdSql.query(sql)
                self.tdSql.checkEqual(self.tdSql.query_row, 1)
            self.tdSql.execute(f'drop table {dbname}.tb')
            self.tdSql.execute(f'drop table {dbname}.sub_tb')
            self.tdSql.execute(f'drop table {dbname}.stb')
            self.tdSql.execute(f'drop database {dbname}')
        for error_param_value in self.error_boundary_list:
            self.tdSql.error(f'create database if not exists {dbname}_error {test_param} {error_param_value}')


    def run(self) -> bool:
        self.replica_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            replica check <jayden>: [TD-14991] : replica check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

