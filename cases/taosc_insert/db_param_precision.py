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
from taostest.util.get_json import GetJson
from taostest.util.remote import Remote


class TestComp(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.cfg = self.tdCom.Boundary.DB_PARAM_PRECISION_CONFIG

    def precision_check(self):
        """
        precision check
        """
        test_param = self.cfg["create_name"]
        get_data = GetJson(self.logger, self.run_log_dir, self.env_setting)

        precision_data = ''
        dbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
        data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
        if int(data['config']['precision']) == 0:
            precision_data = 'ms'
        elif int(data['config']['precision']) == 1:
            precision_data == 'us'
        elif int(data['config']['precision']) == 2:
            precision_data == 'ns'
        self.tdSql.checkEqual(db_field_kv_dict[test_param], precision_data)
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            self.tdSql.execute(
                f'create database if not exists {dbname} {test_param} "{param_value}"')
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
            data = json.load(get_data.get_vnode_json(db_vnode_kv_dict))
            if int(data['config']['precision']) == 0:
                precision_data = 'ms'
            elif int(data['config']['precision']) == 1:
                precision_data = 'us'
            elif int(data['config']['precision']) == 2:
                precision_data = 'ns'
            self.tdSql.checkEqual(db_field_kv_dict[test_param], precision_data)
            self.tdSql.execute(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name()
        self.tdSql.error(
            f'create database if not exists {dbname} {test_param} "s"')
        self.tdSql.error(
            f'create database if not exists {dbname} {test_param} "m"')
        self.tdSql.error(
            f'create database if not exists {dbname} {test_param} "1"')

    def check_presicion_data(self):
        dbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'drop database if exists {dbname}')
        self.tdSql.execute(f'create database if not exists {dbname} ')
        self.tdSql.execute(f'use {dbname}')
        ms_ts, ms_dt = self.tdCom.genTs()
        self.tdSql.execute('create table ntb (ts timestamp, c0 int)')
        self.tdSql.execute(f'insert into ntb values({ms_ts}, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0, 0, ms_dt)
        # TD-15674
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("us")[0]}, 1)')
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("ns")[0]}, 1)')

        dbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'drop database if exists {dbname}')
        self.tdSql.execute(
            f'create database if not exists {dbname} precision "us"')
        self.tdSql.execute(f'use {dbname}')
        us_ts, us_dt = self.tdCom.genTs("us")
        self.tdSql.execute('create table ntb (ts timestamp,c0 int)')
        self.tdSql.execute(f'insert into ntb values({us_ts}, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0, 0, us_dt)

        # TD-15674
        self.tdSql.error('insert into ntb values({self.tdCom.genTs("ms")[0]}, 1)')
        self.tdSql.error('insert into ntb values({self.tdCom.genTs("ns")[0]}, 1)')
        dbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'drop database if exists {dbname}')
        self.tdSql.execute(
            f'create database if not exists {dbname} precision "ns"')
        self.tdSql.execute(f'use {dbname}')
        ns_ts, ns_dt = self.tdCom.genTs("ns")
        self.tdSql.execute('create table ntb (ts timestamp, c0 int)')
        self.tdSql.execute(f'insert into ntb values({ns_ts}, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0, 0, ns_dt)
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("ms")[0]}, 1)')
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("us")[0]}, 1)')
        self.tdSql.execute(f'drop database {dbname}')

        # bug TD-15897
        # dbname = self.tdCom.get_long_name()
        # self.tdSql.execute(f'create database if not exists {dbname} precision "us"')
        # stbname = self.tdCom.get_long_name(length=3, mode="letters")
        # tbname = self.tdCom.get_long_name(length=3, mode="letters")
        # self.tdSql.execute(f'use {dbname}')
        # self.tdSql.execute(
        #     f'create table {stbname} (ts timestamp,c0 int) tags(ts_tag timestamp)')
        # self.tdSql.execute(f'create table {tbname} using {stbname} tags(1640966400000000)')
        # self.tdSql.execute(f'insert into {tbname} values(1640966400000000,1)')
        # self.tdSql.query(f'select * from {stbname}')
        # self.tdSql.checkData(0,2,'1970-01-01 08:00:00')
        

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
