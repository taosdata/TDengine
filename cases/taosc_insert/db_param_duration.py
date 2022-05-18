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
from taostest.util.get_json import GetJson

class TestDuration(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)


    def duration_check(self):
        """
        duration check
        """
        test_param = "days"
        test_param_bak = "duration"
        get_data = GetJson(self.logger, self.run_log_dir,self.env_setting)
        # default
        default_value = 14400
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], default_value)
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
        # print(data['config']['daysPerFile'])
        self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        # without unit
        param_value_list = [1, 3650,'60m','5256000m','1h','87600h','1d','3650d']
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname}  {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            if param_value == 1 or param_value == 3650: # days
                self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], param_value*60*24)
            elif param_value == '60m' or param_value == '5256000m': # minutes
                self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)))
            elif param_value == '1h' or param_value =='87600h': # hours
                self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)) * 60)
            elif param_value == '1d' or param_value == '3650d':
                self.tdSql.checkEqual(db_field_kv_dict[test_param_bak], int(re.sub('\D','',param_value)) * 60 *24)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
            # print(data['config']['daysPerFile'])
            self.tdSql.checkEqual(db_field_kv_dict[test_param_bak],int(data['config']['daysPerFile']))
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} -1')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 3651')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 59m')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 5256001m')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 0h')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} 87601h')
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

