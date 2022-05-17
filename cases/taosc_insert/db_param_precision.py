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

class TestComp(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"

    def precision_check(self):
        """
        precision check
        """
        test_param = "precision"
        # default
        default_value = 'ms'
        precision_data = ''
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)

        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
        file = open(f"{self.vnode_dir}/vnode.json")
        data = json.load(file)

        if int(data['config']['precision']) == 0:
            precision_data = 'ms'
        elif int(data['config']['precision']) == 1:
            precision_data == 'us'
        elif int(data['config']['precision']) == 2:
            precision_data == 'ns'
        self.tdSql.checkEqual(db_field_kv_dict[test_param],precision_data)
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        param_value_list = ['ms','us','ns']
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} "{param_value}"')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
            file = open(f"{self.vnode_dir}/vnode.json")
            data = json.load(file)
            if int(data['config']['precision']) == 0:
                precision_data = 'ms'
            elif int(data['config']['precision']) == 1:
                precision_data = 'us'
            elif int(data['config']['precision']) == 2:
                precision_data = 'ns'
            self.tdSql.checkEqual(db_field_kv_dict[test_param],precision_data)
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} s')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} m')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 1')
    def check_presicion_data(self):
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'drop database if exists {dbname}')
        self.tdSql.execute(f'create database if not exists {dbname} ')
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute('create table ntb (ts timestamp,c0 int)')
        self.tdSql.execute('insert into ntb values(1640966400000,1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0,0,'2022-01-01 00:00:00.000')
        self.tdSql.error('insert into ntb values(1640966400000,1)')
        pass
    def run(self) -> bool:
        self.precision_check()
        self.check_presicion_data()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            precision_check check <jiacy>: [TD-15635] : precision_check;
            
            """
        return case_description

    def author(self) -> str:
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

