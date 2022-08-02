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

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
class TestSingle_stable(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.cfg = self.tdCom.Boundary.DB_PARAM_SINGLE_STABLE_CONFIG
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.api_type = 'restful'
    def single_stable_check(self):
        """
        single_stable check
        """
        test_param = self.cfg["create_name"]
        get_param = self.cfg["query_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdRest.request('show databases')       
        db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,get_param,dbname)
        # default
        self.tdSql.checkEqual(db_field, self.cfg["default"])
        self.tdRest.request(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: param_value}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdRest.request('show databases')
            #TODO
            db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,get_param,dbname)
            self.tdSql.checkEqual(db_field, param_value)
            if param_value == 0:
                self.tdRest.request(f'create table {dbname}.stb1 (ts timestamp, c1 int) tags (t1 int);')
                self.tdRest.request(f'create table {dbname}.stb2 (ts timestamp, c1 int) tags (t1 int);')
                self.tdRest.request(f'show {dbname}.stables')
                self.tdSql.checkEqual(len(self.tdRest.resp['data']), 2)
            elif param_value == 1:
                self.tdRest.request(f'create table {dbname}.stb1 (ts timestamp, c1 int) tags (t1 int);')
                self.tdRest.error(f'create table {dbname}.stb2 (ts timestamp, c1 int) tags (t1 int);')
                self.tdRest.request(f'show {dbname}.stables')
                self.tdSql.checkEqual(len(self.tdRest.resp['data']), 1)
            self.tdRest.request(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name()
        self.tdRest.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][0] - 1}')
        self.tdRest.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][-1] + 1}')

    def run(self) -> bool:
        self.single_stable_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            single_stable check <jayden>: [TD-14991] : single_stable check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

