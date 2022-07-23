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
        self.remote: Remote = Remote(self.logger)
        self.cfg = self.tdCom.Boundary.DB_PARAM_PRECISION_CONFIG
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
    def precision_check(self):
        """
        precision check
        """
        test_param = self.cfg["create_name"]
        precision_data = ''
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.query(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
        data = json.loads(self.remote.cmd(self.fqdn,f'cat {self.vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
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
            kv_dict = {test_param: param_value}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            self.tdSql.query(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdSql.getOneRow(1, dbname)
            data = json.loads(self.remote.cmd(self.fqdn,f'cat {self.vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
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
        self.tdCom.createDb(dbname)
        ms_ts, ms_dt = self.tdCom.genTs()
        self.tdSql.execute('create table ntb (ts timestamp, c0 int)')
        self.tdSql.execute(f'insert into ntb values({ms_ts}, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0, 0, ms_dt)
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("us")[0]}, 1)')
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("ns")[0]}, 1)')

        dbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'drop database if exists {dbname}')
        kv_dict = {"precision": "us"}
        self.tdCom.createDb(dbname, **kv_dict)
        us_ts, us_dt = self.tdCom.genTs("us")
        self.tdSql.execute('create table ntb (ts timestamp,c0 int)')
        self.tdSql.execute(f'insert into ntb values({us_ts}, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0, 0, us_dt)

        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("ms")[0]}, 1)')
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("ns")[0]}, 1)')
        dbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'drop database if exists {dbname}')
        kv_dict = {"precision": "ns"}
        self.tdCom.createDb(dbname, **kv_dict)
        ns_ts, ns_dt = self.tdCom.genTs("ns")
        self.tdSql.execute('create table ntb (ts timestamp, c0 int)')
        self.tdSql.execute(f'insert into ntb values({ns_ts}, 1)')
        self.tdSql.query("select * from ntb")
        self.tdSql.checkData(0, 0, ns_dt)
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("ms")[0]}, 1)')
        self.tdSql.error(f'insert into ntb values({self.tdCom.genTs("us")[0]}, 1)')
        self.tdSql.execute(f'drop database {dbname}')

        dbname = self.tdCom.get_long_name()
        kv_dict = {"precision": "us"}
        self.tdCom.createDb(dbname, **kv_dict)
        stbname = self.tdCom.get_long_name()
        tbname = self.tdCom.get_long_name()
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create table {stbname} (ts timestamp,c0 int) tags(ts_tag timestamp)')
        self.tdSql.execute(f'create table {tbname} using {stbname} tags(1640966400000000)')
        self.tdSql.execute(f'insert into {tbname} values(1640966400000000,1)')
        self.tdSql.query(f'select * from {stbname}')
        self.tdSql.checkData(0,2,'2022-01-01 00:00:00')
        

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