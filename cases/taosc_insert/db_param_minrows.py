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
from pkgutil import get_data
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
from taostest.util.get_json import GetJson
class TestMinrows(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
    
    def minrows_check(self):
        """
        minrows check
        """
        test_param = "minrows"
        get_data = GetJson(self.logger, self.run_log_dir,self.env_setting)
        # default
        default_value = 100
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        self.tdSql.checkEqual(db_field_kv_dict[test_param], default_value)
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
        data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
        self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config']['minRows']))
        self.tdSql.execute(f'drop database {dbname}')
        # param_list
        param_value_list = [10, 1000]
        for param_value in param_value_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} {test_param} {param_value}')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1,dbname)
            data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
            self.tdSql.checkEqual(db_field_kv_dict[test_param],int(data['config']['minRows']))
            # start_time = 1641657600000
            # self.tdSql.execute(f'''create table {dbname}.ntb (ts timestamp,c0 binary(1024),c1 binary(1024),c2 binary(1024),c3 binary(1024),c4 binary(1024),
            #                         c5 binary(1024),c6 binary(1024),c7 binary(1024),c8 binary(1024),c9 binary(1024))''')
            # for i in range(99):
            #     insert_time = start_time+i
            #     ran_str = self.generate_random_str(1024)
            #     self.tdSql.execute(f'''insert into {dbname}.ntb values({insert_time},{ran_str},{ran_str},{ran_str},{ran_str},{ran_str},
            #     {ran_str},{ran_str},{ran_str},{ran_str},{ran_str})''')
            # bug TD-15465(cancelled): do not test with unusual exiting
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[0] - 1}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {param_value_list[-1] + 1}')

    def run(self) -> bool:
        self.minrows_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            minrows check <jayden>: [TD-14991] : minrows check;
            minrows check <jiacy> : [TD-15381] : minrows check for taosd;
            """
        return case_description

    def author(self) -> str:
        return "Jayden,Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

