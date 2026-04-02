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

import datetime
import json
import time
import numpy as np
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
from taostest.util.rest import TDRest

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
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.api_type = 'restful'
        self.tdRest.drop_all_db()
    def precision_check(self):
        """
        precision check
        """
        test_param = self.cfg["create_name"]
        precision_data = ''
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdRest.request('select * from information_schema.ins_databases')
        db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,test_param,dbname)
        # default
        self.tdSql.checkEqual(db_field, self.cfg["default"])
        self.tdRest.request(f'show {dbname}.vgroups')
        db_vnode_kv_dict = self.tdRest.getOneRow(1, dbname)
        for i in self.taosd_setting['spec']['dnodes']:
            fqdn = i['endpoint'].split(':')[0]
            vnode_dir = i['config']['dataDir']+ "/vnode"
            if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                break
            else:
                continue
        if int(data['config']['precision']) == 0:
            precision_data = 'ms'
        elif int(data['config']['precision']) == 1:
            precision_data == 'us'
        elif int(data['config']['precision']) == 2:
            precision_data == 'ns'
        self.tdSql.checkEqual(db_field, precision_data)
        self.tdRest.request(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: param_value}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdRest.request('select * from information_schema.ins_databases')
            db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,test_param,dbname)
            self.tdSql.checkEqual(db_field, param_value)
            self.tdRest.request(f'show {dbname}.vgroups')
            db_vnode_kv_dict = self.tdRest.getOneRow(1, dbname)
            for i in self.taosd_setting['spec']['dnodes']:
                fqdn = i['endpoint'].split(':')[0]
                vnode_dir = i['config']['dataDir']+ "/vnode"
                if self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'):
                    data = json.loads(self.remote.cmd(fqdn,f'cat {vnode_dir}/vnode{db_vnode_kv_dict[0][0]}/vnode.json'))
                    break
                else:
                    continue
            if int(data['config']['precision']) == 0:
                precision_data = 'ms'
            elif int(data['config']['precision']) == 1:
                precision_data = 'us'
            elif int(data['config']['precision']) == 2:
                precision_data = 'ns'
            self.tdSql.checkEqual(db_field, precision_data)
            self.tdRest.request(f'drop database {dbname}')
        dbname = self.tdCom.get_long_name()
        self.tdRest.error(
            f'create database if not exists {dbname} {test_param} "s"')
        self.tdRest.error(
            f'create database if not exists {dbname} {test_param} "m"')
        self.tdRest.error(
            f'create database if not exists {dbname} {test_param} "1"')

    def check_presicion_data(self):
        dbname = self.tdCom.get_long_name()
        self.tdRest.request(f'drop database if exists {dbname}')
        self.tdCom.createDb(dbname)
        ms_ts, ms_dt = self.tdCom.genTs("ms",None,'restful')
        self.tdRest.request(f'create table {dbname}.ntb (ts timestamp, c0 int)')
        self.tdRest.request(f'insert into {dbname}.ntb values({ms_ts}, 1)')
        self.tdRest.request(f"select * from {dbname}.ntb")
        ms_utc = datetime.datetime.utcfromtimestamp(ms_ts/1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        
        ts_ms = self.tdCom.delete_end_zero(ms_utc, "ms").replace(' ','T')+ "Z"
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], ts_ms)
        # TD-15674
        self.tdRest.error(f'insert into {dbname}.ntb values({self.tdCom.genTs("us")[0]}, 1)')
        self.tdRest.error(f'insert into {dbname}.ntb values({self.tdCom.genTs("ns")[0]}, 1)')

        dbname = self.tdCom.get_long_name()
        self.tdRest.request(f'drop database if exists {dbname}')
        kv_dict = {"precision": "us"}
        self.tdCom.createDb(dbname, **kv_dict)
        self.tdRest.request(f'use {dbname}')
        us_ts, us_dt = self.tdCom.genTs("us",None,'restful')
        self.tdRest.request(f'create table {dbname}.ntb (ts timestamp,c0 int)')
        self.tdRest.request(f'insert into {dbname}.ntb values({us_ts}, 1)')
        self.tdRest.request(f"select * from {dbname}.ntb")
        us_utc = datetime.datetime.utcfromtimestamp(us_ts/1000000).strftime("%Y-%m-%d %H:%M:%S.%f")
        ts_us = self.tdCom.delete_end_zero(us_utc, "us").replace(' ','T') + "Z"
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], ts_us)
        # TD-15674
        self.tdRest.error(f'insert into {dbname}.ntb values({self.tdCom.genTs("ms")[0]}, 1)')
        self.tdRest.error(f'insert into {dbname}.ntb values({self.tdCom.genTs("ns")[0]}, 1)')
        dbname = self.tdCom.get_long_name()
        self.tdRest.request(f'drop database if exists {dbname}')
        kv_dict = {"precision": "ns"}
        self.tdCom.createDb(dbname, **kv_dict)
        # ns_ts, ns_dt = self.tdCom.genTs("ns",None,'restful')
        ns_ts = 1665386585499707961
        self.tdRest.request(f'create table {dbname}.ntb (ts timestamp, c0 int)')
        self.tdRest.request(f'insert into {dbname}.ntb values({ns_ts}, 1)')
        self.tdRest.request(f"select * from {dbname}.ntb")
        ns_timestamp = ns_ts % 1000000
        ns_utc = datetime.datetime.utcfromtimestamp(int(ns_ts/1000000)/1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + str(ns_timestamp)
        ts_ns = self.tdCom.delete_end_zero(ns_utc, "ns").replace(' ','T') + "Z"
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], ts_ns)
        self.tdRest.error(f'insert into {dbname}.ntb values({self.tdCom.genTs("ms")[0]}, 1)')
        self.tdRest.error(f'insert into {dbname}.ntb values({self.tdCom.genTs("us")[0]}, 1)')
        self.tdRest.request(f'drop database {dbname}')

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
