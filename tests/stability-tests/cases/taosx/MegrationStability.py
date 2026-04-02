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
import os
import threading
import time
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taostest.components import TaosD
from taosx.taosxutil import taosx

class MegrationStability(TDCase):
    def init(self):
        self.tdTaosx = taosx.Runtaosx(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self.remote)
        self.firstEP = []
        self.config_dir = []
        self.source_taosd_list = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
                self.config_dir.append(self.taosd_setting['spec']['dnodes'][0]['config_dir'])
            if env_setting["name"].lower() == 'taosx':
                self.taosx_setting = env_setting
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.target_taosd = self.firstEP[-1].split(':')
        self.target_config_dir = self.config_dir[-1]
        self.test_root = os.environ['TEST_ROOT']
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        self.taosx_fqdn = self.get_fqdn('taosx')
        # param for taosBenchmark with db,stb and ctb check
        self.stbname = [self.tdCom.get_long_name(3),self.tdCom.get_long_name(3)]
        self.tbname_m = [self.tdCom.get_long_name(1),self.tdCom.get_long_name(1)]
        self.tb_num = 10000
        self.row_num = 10000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        self.timeout = '30s'
        self.replica = 3
        self.taosx_num = 5
    def get_lib_path(self):
        self.remote.cmd(self.taosx_fqdn[0],f'rm -rf /tmp/3.0_lib/')
        self.remote.cmd(self.taosx_fqdn[0],f'rm -rf /tmp/2.6_lib/')
        self.remote.cmd(self.taosx_fqdn[0],f'mkdir /tmp/3.0_lib')
        self.remote.cmd(self.taosx_fqdn[0],f'mkdir /tmp/2.6_lib')
        self.lib_26 = []
        for source in range(len(self.source_taosd_list)):
            self.remote.cmd(self.taosx_fqdn[0],f'mkdir /tmp/2.6_lib/{self.source_taosd_list[source][0]}')
            self.remote.cmd(self.source_taosd_list[source][0],f'scp -r /usr/local/taos/driver/ root@{self.taosx_fqdn[0]}:/tmp/2.6_lib/{self.source_taosd_list[source][0]}/')
            self.remote.cmd(self.source_taosd_list[source][0],f'scp -r {self.config_dir[source]}/taos.cfg root@{self.taosx_fqdn[0]}:/tmp/2.6_lib/{self.source_taosd_list[source][0]}/')
            self.lib_26.append(self.remote.cmd(self.taosx_fqdn[0],f'ls -t /tmp/2.6_lib/{self.source_taosd_list[source][0]}/driver/'))
        self.remote.cmd(self.target_taosd[0],f'scp -r /usr/local/taos/driver/ root@{self.taosx_fqdn[0]}:/tmp/3.0_lib/')
        self.remote.cmd(self.target_taosd[0],f'scp -r {self.target_config_dir}/taos.cfg root@{self.taosx_fqdn[0]}:/tmp/3.0_lib/')
        self.lib_30 = self.remote.cmd(self.taosx_fqdn[0],f'ls -t /tmp/3.0_lib/driver/')
    def get_json(self,json_path,host,port,dbname,stbname,tbname_m,tb_num,start_timestamp,row_num,drop_flag,child_table_exist,replica):
        dict = {}
        with open(json_path,'rb') as file:
            params = json.load(file)
            params['host'] = host
            params['port'] = port
            params['databases'][0]['dbinfo']['name'] = dbname
            params['databases'][0]['dbinfo']['drop'] = drop_flag
            params['databases'][0]['dbinfo']['replica'] = replica
            params['databases'][0]['super_tables'][0]['name'] = stbname
            params['databases'][0]['super_tables'][0]['childtable_count'] = tb_num
            params['databases'][0]['super_tables'][0]['child_table_exists'] = child_table_exist
            params['databases'][0]['super_tables'][0]['insert_rows'] = row_num
            params['databases'][0]['super_tables'][0]['childtable_prefix'] = tbname_m
            params['databases'][0]['super_tables'][0]['start_timestamp'] = start_timestamp
            dict = params
        file.close()
        return dict
    def data_megration(self):
        self.get_lib_path()
        taosBenchmark_thread_list = []
        taosx_thread_list = []
        for source in range(len(self.source_taosd_list)):
            host = self.source_taosd_list[source][0]
            port = self.source_taosd_list[source][1]
            self.tdTaosx.write_json(f'{self.test_root}/cases/taosx/basic_megration{source}.json', self.get_json(f'{self.test_root}/cases/taosx/basic_megration.json',
                            host, int(port), self.dbname[source], self.stbname[source], self.tbname_m[source],self.tb_num,self.start_timestamp,self.row_num,self.drop_flag,self.child_table_exist_flag,self.replica))
            self.remote.put(
                self.taosBenchmark_fqdn[0], f'{self.test_root}/cases/taosx/basic_megration{source}.json', f'/tmp/basic_megration{source}')
        for source in range(len(self.source_taosd_list)):   
            taosBenchmark_thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                self.taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/basic_megration{source}/basic_megration{source}.json')))
            taosBenchmark_thread_list[source].start()
        for thread in taosBenchmark_thread_list:
            thread.join()
        for i in range(self.taosx_num):
            for source in range(len(self.source_taosd_list)):
                target_dbname = self.tdCom.get_long_name(10)
                self.remote.cmd(self.target_taosd[0],f'taos -s "create database if not exists {target_dbname} replica 3"')
                taosx_thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                self.taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'taos://{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])}/{self.dbname[source]}?libraryPath=/tmp/2.6_lib/{self.source_taosd_list[source][0]}/driver/{self.lib_26[source]}&configDir=/tmp/2.6_lib/{self.source_taosd_list[source][0]}/taos.cfg'\
                                    -t 'taos://{self.target_taosd[0]}:{int(self.target_taosd[1])}/{target_dbname}'?libraryPath=/tmp/3.0_lib/driver/{self.lib_30}")))
                taosx_thread_list[i].start()
        for thread in taosx_thread_list:
            thread.join()
        
    def run(self):
        self.dbname = [self.tdCom.get_long_name(5)]
        self.data_megration()
    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            test of Megration Stability <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaosSql.Update