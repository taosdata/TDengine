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
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
class StaticSynchronism(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.firstEP = []
        self.source_taosd_list = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(self.taosd_setting['spec']['config']['firstEP'])
            if env_setting["name"].lower() == 'taosx':
                self.taosx_setting = env_setting
            if env_setting["name"].lower() == 'taosAdapter':
                self.taosadapter_setting = env_setting
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        #param for taosBenchmark
        self.dbname = ['db1']
        self.stbname = ['stb1']
        self.tbname_m = ['d']
        self.tb_num = 1000
        self.row_num = 10000
        #param for taosx
        self.timeout = '1s'
        self.target_dbname = 'target'
    
    def get_json(self,json_path,host,port,dbname,stbname,tbname_m):
        dict = {}
        with open(json_path,'rb') as file:
            params = json.load(file)
            params['host'] = host
            params['port'] = port
            params['databases'][0]['dbinfo']['name'] = dbname
            params['databases'][0]['super_tables'][0]['name'] = stbname
            params['databases'][0]['super_tables'][0]['childtable_count'] = self.tb_num
            params['databases'][0]['super_tables'][0]['insert_rows'] = self.row_num
            params['databases'][0]['super_tables'][0]['childtable_prefix'] = tbname_m
            dict = params
        file.close()
        return dict
    def write_json(self,json_path,dict):
        with open(json_path,'w') as r:
            json.dump(dict,r)
        r.close()
    def data_insert(self):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(self.source_taosd_list)):
            host = self.source_taosd_list[source][0]
            port = self.source_taosd_list[source][1]
            self.write_json(f'{self.test_root}/cases/taosx/basic.json',self.get_json(f'{self.test_root}/cases/taosx/basic.json',host,int(port),self.dbname[source],self.stbname[source],self.tbname_m[source]))
            self.remote.put(taosBenchmark_fqdn[0],f'{self.test_root}/cases/taosx/basic.json','/tmp/')
            self.remote.cmd(taosBenchmark_fqdn[0],f'taosBenchmark -f /tmp/basic.json')
    def full_sync_db(self):
        for source_task in ['','+ws']:
            for target_task in ['','+ws']:
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0],port=int(self.source_taosd_list[source][1]))
                    taosd_backup = taos.connect(host=self.target_taosd[0],port=int(self.target_taosd[1]))
                    taosd_master.execute(f'use {self.dbname[source]}')
                    master_count_rows = taosd_master.query(f'select count(*) from {self.stbname[source]}').fetch_all_into_dict()
                    master_sum = taosd_master.query(f'select sum(voltage) from {self.stbname[source]}').fetch_all_into_dict()
                    if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run -f 'tmq{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])+11}/{self.dbname[source]}?group.id={group_id}&timeout={self.timeout}' -t 'taos{target_task}://root:taosdata@{self.target_taosd[0]}:{int(self.target_taosd[1])+11}/{self.target_dbname}'")
                    elif source_task.lower() == '+ws' and target_task.lower() == '':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run -f 'tmq{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])+11}/{self.dbname[source]}?group.id={group_id}&timeout={self.timeout}' -t 'taos{target_task}://root:taosdata@{self.target_taosd[0]}:{int(self.target_taosd[1])}/{self.target_dbname}'")
                    elif source_task.lower() == '' and target_task.lower() == '':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run -f 'tmq{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{self.source_taosd_list[source][1]}/{self.dbname[source]}?group.id={group_id}&timeout={self.timeout}' -t 'taos{target_task}://root:taosdata@{self.target_taosd[0]}:{self.target_taosd[1]}/{self.target_dbname}'")
                    elif source_task.lower() == '' and target_task.lower() == '+ws':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run -f 'tmq{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])}/{self.dbname[source]}?group.id={group_id}&timeout={self.timeout}' -t 'taos{target_task}://root:taosdata@{self.target_taosd[0]}:{int(self.target_taosd[1])+11}/{self.target_dbname}'")
                    taosd_backup.execute(f'use {self.target_dbname}')
                    backup_count_rows = taosd_master.query(f'select count(*) from {self.stbname[source]}').fetch_all_into_dict()
                    backup_sum = taosd_backup.query(f'select sum(voltage) from {self.stbname[source]}').fetch_all_into_dict()
                    self.tdSql.checkEqual(master_count_rows[0]['count(*)'],backup_count_rows[0]['count(*)'])
                    self.tdSql.checkEqual(master_sum[0]['sum(voltage)'],backup_sum[0]['sum(voltage)'])
                    taosd_backup.execute(f'drop database {self.target_dbname}')
    def run(self):
        self.data_insert()
        self.full_sync_db()
        
        pass

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            export test of taosx <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Taosx.Import