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
from taosx.taosxutil import taosx

class StaticFullSync(TDCase):
    def init(self):
        self.tdTaosx = taosx.Runtaosx(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.firstEP = []
        self.source_taosd_list = []
        self.taosadapter_list = []
        self.source_taosadapter_list = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
            if env_setting["name"].lower() == 'taosx':
                self.taosx_setting = env_setting
            if env_setting["name"].lower() == 'taosadapter':
                self.taosdapter_setting = env_setting
                self.taosadapter_list.append(self.taosdapter_setting)
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        for i in range(len(self.taosadapter_list) - 1):
            self.source_taosadapter_list.append(self.taosadapter_list[i])
        self.target_taosadapter = self.taosadapter_list[-1]
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        # param for taosBenchmark with db,stb and ctb check
        self.stbname = [self.tdCom.get_long_name(3)]
        self.tbname_m = [self.tdCom.get_long_name(1)]
        self.tb_num = 1000
        self.row_num = 10000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        # param for taosBenchmark with ntb check
        self.ntb_dbname = [self.tdCom.get_long_name(6)]
        self.ntb_name_m = [self.tdCom.get_long_name(2)]
        self.ntb_num = 1000
        self.ntb_row_num = 10000
        # param for taosx
        self.timeout = '10s'
        self.replica = [1,3]
        # create user param
        self.source_user_name = self.tdCom.get_long_name(5)
        self.source_user_password = self.tdCom.get_long_name(8)
        self.target_user_name = self.tdCom.get_long_name(5)
        self.target_user_password = self.tdCom.get_long_name(8)
    def data_insert_ntb(self,source_taosd_list,dbname,ntbname_m,tb_num,row_num):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        thread_list = []
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -n {row_num} -t {tb_num} -d {dbname[source]} -m {ntbname_m[source]} -N -y')))
            thread_list[source].start()
        for thread in thread_list:
            thread.join()
    
    def sync_db_stb(self,source_type):
        for source_task in ['','']:
            for target_task in ['','']:
                thread_list = []
                master_count_rows = []
                master_sum = []
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    master_count_rows.append(count_rows)
                    sum_rows = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    master_sum.append(sum_rows)
                    if source_type == 'database':
                        self.tdTaosx.run_taosx_db_sync(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.source_taosadapter_list[source]['spec']['adapter_config']['port'],self.target_taosd,self.target_taosadapter['spec']['adapter_config']['port'],self.dbname,self.target_dbname,source,group_id,self.timeout,self.source_user_name,self.source_user_password,self.target_user_name,self.target_user_password)
                    elif source_type == 'stable':
                        self.tdTaosx.run_taosx_stb_sync_with_topic(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.source_taosadapter_list[source]['spec']['adapter_config']['port'],self.target_taosd,self.target_taosadapter['spec']['adapter_config']['port'],self.stbname,self.target_dbname,source,group_id,self.timeout,self.source_user_name,self.source_user_password,self.target_user_name,self.target_user_password)
                    thread_list[source].start()
                for thread in thread_list:
                    thread.join()
                print('taosx task is OK!!!!!')
                taosd_backup = taos.connect(host=self.target_taosd[0], port=int(self.target_taosd[1]))
                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.stbname[source]}').fetch_all_into_dict()
                    backup_count_rows.append(count_rows)
                    sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.stbname[source]}').fetch_all_into_dict()
                    backup_sum.append(sum_rows) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(
                        master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(
                        master_sum[source][0]['sum(voltage)'], backup_sum[source][0]['sum(voltage)'])
                taosd_backup.execute(f'drop database {self.target_dbname}')
    def create_user(self,source_user_name,source_user_password,target_user_name,target_user_password):
        for source in range(len(self.source_taosd_list)):
            taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(
                        self.source_taosd_list[source][1]))
            taosd_master.execute(f'create user {source_user_name} pass "{source_user_password}"')
        taosd_backup = taos.connect(
                    host=self.target_taosd[0], port=int(self.target_taosd[1]))
        taosd_backup.execute(f'create user {target_user_name} pass "{target_user_password}"')            
    def run(self):
        self.create_user(self.source_user_name,self.source_user_password,self.target_user_name,self.target_user_password)
        for replica in self.replica:
            self.dbname = [self.tdCom.get_long_name(5)]
            self.target_dbname = self.tdCom.get_long_name(5)
            self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root,replica=replica)
            for source in range(len(self.source_taosd_list)):
                taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(
                            self.source_taosd_list[source][1]))
                taosd_master.execute(f'create topic {self.dbname[source]} with meta as database {self.dbname[source]}')
                taosd_master.execute(f'grant subscribe on {self.dbname[source]} to {self.source_user_name}')
                taosd_master.execute(f'create topic {self.stbname[source]} with meta as stable {self.dbname[source]}.{self.stbname[source]}')
                taosd_master.execute(f'grant subscribe on {self.stbname[source]} to {self.source_user_name}')
            self.sync_db_stb('database')
            self.sync_db_stb('stable')
            for source in range(len(self.source_taosd_list)):
                taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(
                            self.source_taosd_list[source][1]))
                taosd_master.execute(f'drop topic {self.dbname[source]} ')    
                taosd_master.execute(f'drop topic {self.stbname[source]} ')        
            # self.ntb_dbname = [self.tdCom.get_long_name(5)]
            # self.data_insert_ntb(self.source_taosd_list,self.ntb_dbname,self.ntb_name_m,self.ntb_num,self.ntb_row_num)


    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            full_sync test of taosx <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaosSql.Update