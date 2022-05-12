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


class TestDuration(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"

    def duration_check(self):
        """
        duration check
        """
        test_param = "days"
        test_param_bak = "duration"
        # default
        default_value = 14400
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], default_value)
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
        file = open(f"{self.vnode_dir}/vnode.json")
        data = json.load(file)
        # print(data['config']['daysPerFile'])
        self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        # without unit
        param_value_list = [1, 3650]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname}  {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], param_value*60*24)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
            file = open(f"{self.vnode_dir}/vnode.json")
            data = json.load(file)
            print(data['config']['daysPerFile'])
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[-1] + 1}')

        # unit with "m"
        param_minute_list =['60m','5256000m']
        for param_value in param_minute_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname}  {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)))
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
            file = open(f"{self.vnode_dir}/vnode.json")
            data = json.load(file)
            # print(data['config']['daysPerFile'])
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
            self.tdSql.execute(f'drop database {dbname}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 59m')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 5256001m')

        #unit with "h"
        param_hour_list =['1h','87600h']
        for param_value in param_hour_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)) * 60)

            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            # print(db_vnode_kv_dict)
            self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
            file = open(f"{self.vnode_dir}/vnode.json")
            data = json.load(file)
            # print(data['config']['daysPerFile'])
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
            self.tdSql.execute(f'drop database {dbname}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 0h')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 87601h')

        #unit with "d"
        param_day_list =['1d','3650d']
        for param_value in param_day_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)) * 60 *24)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
            file = open(f"{self.vnode_dir}/vnode.json")
            data = json.load(file)
            print(data['config']['daysPerFile'])
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
            self.tdSql.execute(f'drop database {dbname}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} "0d"')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} "3651d"')
    def run(self) -> bool:
        self.duration_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            duration check <jayden>: [TD-14991] : duration check;
            duration check <jiacy> : [TD-15381] : duration check for taosd;
            """
        return case_description

    def author(self) -> str:
        return "Jayden,Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

