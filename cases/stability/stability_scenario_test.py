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
from taostest.util.remote import Remote
from taostest.components.taosd import TaosD
import socket
import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
from taostest.util.file import dict2yaml, read_yaml
import datetime
import re
import sys
import time
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func

class TestStabilityScenario(TDCase):
    def init(self):
        self._fqdn: str = None
        self.taosadapter_ip_port_dict = dict()
        self.agent_settings = None
        self.taosadapter_settings = None
        self.error_msg = None
        self.env_dir = None
        self.tmp_yaml = None
        self.stability_config = read_yaml(os.path.join(os.path.abspath(os.path.dirname(__file__)), "stability_config.yaml"))
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        
        self.case_name = str()
        self.stb_name = str()
        self.ctb_name = str()
        self.tb_name = str()
        self.dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()
        self.dnode_kill_time = 5
        self.default_syncHeartbeatTimeout = 20
        self.dbname = "stability_scenario_test"
        self.range_count = 10
        self.vgroups = 10
        
    def prepare_data(self, range_count=None):
        for endpoint in self.dnodes_out_mnodes[1]:
            host = endpoint.split(":")[0]
            self.tdCom.clean_remote_iptables(self._remote, host)
        self.tdCom.drop_all_db()
        self.case_name = sys._getframe().f_code.co_name
        self.dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "range_count": range_count,
            "start_ts": 1655903478508,
        }
        if range_count is not None:
            self.range_count = range_count

        self.stb_name = self.dataDict["stb_name"]
        self.ctb_name = self.dataDict["ctb_name"]
        self.tb_name = self.dataDict["tb_name"]
        self.date_time = self.tdCom.genTs()[0]
        
        self.tdCom.createDb(dbname=self.dbname, vgroups=self.vgroups)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stb_name)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stb_name, ctbname=self.ctb_name)
        self.tdCom.create_table(dbname=self.dbname, tbname=self.tb_name)
        for i in range(self.range_count):
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{i}s')
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{i}s')
            self.date_time += 1

    def loop_dnode_net_restore(self):
        self.prepare_data()
        for endpoint in self.dnodes_out_mnodes[1]:
            host = endpoint.split(":")[0]
            port = endpoint.split(":")[1]
            self.tdCom.drop_remote_ports(self._remote, host, [port], "OUTPUT", "tcp")
            time.sleep(self.dnode_kill_time)
            self.tdCom.accept_remote_ports(self._remote, host, [port], "OUTPUT", "tcp")
            time.sleep(self.dnode_kill_time)
        for i in range(self.range_count):
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{i}s')
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{i}s')
            self.date_time += 1

        for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            self.tdSql.query(f'select count(*) from {self.dbname}.{tbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.range_count * 2)

    def one_dnode_stop_work(self):
        self.prepare_data()
        for endpoint in self.dnodes_out_mnodes[1]:
            if endpoint != self.dnodes_out_mnodes[1][-1]:
                host = endpoint.split(":")[0]
                port = endpoint.split(":")[1]
                self.tdCom.drop_remote_ports(self._remote, host, [port], "OUTPUT", "tcp")
        import time
        time.sleep(self.default_syncHeartbeatTimeout+1)
        for tbname in [self.ctb_name, self.tb_name]:
            self.tdSql.error(f'insert into {self.dbname}.{tbname} (ts, c1) values (now, 1);')
            # self.tdSql.error(f'select count(*) from {self.dbname}.{tbname};')
        for endpoint in self.dnodes_out_mnodes[1]:
            if endpoint == self.dnodes_out_mnodes[1][1]:
                host = endpoint.split(":")[0]
                port = endpoint.split(":")[1]
                self.tdCom.accept_remote_ports(self._remote, host, [port], "OUTPUT", "tcp")
        for tbname in [self.ctb_name, self.tb_name]:
            self.tdSql.execute(f'insert into {self.dbname}.{tbname} (ts, c1) values (now, 1);')
            self.tdSql.query(f'select count(*) from {self.dbname}.{tbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.range_count + 1)

    def two_dnode_restore_work(self):
        self.prepare_data()
        for tbname in [self.ctb_name, self.tb_name]:
            self.tdSql.query(f'select count(*) from {self.dbname}.{tbname}')
        for endpoint in self.dnodes_out_mnodes[1]:
            host = endpoint.split(":")[0]
            port = endpoint.split(":")[1]
            self.tdCom.drop_remote_ports(self._remote, host, [port], "OUTPUT", "tcp")
        time.sleep(self.dnode_kill_time + 1)
        for tbname in [self.ctb_name, self.tb_name]:
            self.tdSql.error(f'insert into {self.dbname}.{tbname} (ts, c1) values (now, 1);')
            # self.tdSql.error(f'select count(*) from {self.dbname}.{tbname};')
        for endpoint in self.dnodes_out_mnodes[1]:
            if endpoint != self.dnodes_out_mnodes[1][-1]:
                host = endpoint.split(":")[0]
                port = endpoint.split(":")[1]
                self.tdCom.accept_remote_ports(self._remote, host, [port], "OUTPUT", "tcp")
        time.sleep(self.default_syncHeartbeatTimeout)

        for tbname in [self.ctb_name, self.tb_name]:
            self.tdSql.query(f'select count(*) from {self.dbname}.{tbname}')
        for tbname in [self.ctb_name, self.tb_name]:
            self.tdSql.execute(f'insert into {self.dbname}.{tbname} (ts, c1) values (now, 1);')
            self.tdSql.query(f'select count(*) from {self.dbname}.{tbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.range_count + 1)

    def full_vnodes_create_drop(self):
        for endpoint in self.dnodes_out_mnodes[1]:
            host = endpoint.split(":")[0]
            self.tdCom.clean_remote_iptables(self._remote, host)
        last_dnode_id = self.dnodes_out_mnodes[0][-1]
        dnode_list = self.taosd_setting["spec"]["dnodes"]
        end_tag = 0
        for dnode in dnode_list:
            print(dnode)
            if dnode["config"]["supportVnodes"] != 0 and end_tag == 0:
                self.vgroups = dnode["config"]["supportVnodes"]
                end_tag = 1
        self.tdCom.createDb(dbname=self.dbname, vgroups=self.vgroups)
        self.tdSql.error(f'drop dnode {last_dnode_id}')
        self.tdSql.error(f'create database test_erro replica 3 vgroups 1')

    def run(self):
        # self.loop_dnode_net_restore()
        # self.one_dnode_stop_work()
        # self.two_dnode_restore_work()
        self.full_vnodes_create_drop()

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            stability <jayden>: [TD-12533] : stability test;
        """
        return case_description

    def author(self):
        return "Jayden"

    def tags(self):
        return T.Write.Stable

