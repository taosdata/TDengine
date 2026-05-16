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
class AddSync(TDCase):
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
                self.firstEP.append(self.taosd_setting['spec']['config']['firstEP'])
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
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        self.target_taosadapter = self.taosadapter_list[-1]
        #param for taosBenchmark
        self.dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
        self.stbname = ['stb1','stb2']
        self.tbname_m = ['d','t']
        self.tb_num = 1000
        self.row_num = 1000
        self.drop_flag = 'yes'
        self.start_timestamp = 1601481600000
        self.child_table_exist_flag = 'no'
        # add_start_timestamp >= start_timestamp + row_num
        self.add_drop_flag = 'no'
        self.add_start_timestamp = 1601481710000
        self.add_row_num = 100
        self.add_child_table_exist_flag = 'yes'
        #param for taosx
        self.timeout = '5s'
        self.target_dbname = 'target'

        # ntb param
        self.ntb_dbname = [self.tdCom.get_long_name(6),self.tdCom.get_long_name(6)]
        self.ntb_tbnum = 1000
        self.ntbname_m = ['nd','nt']
        self.ntb_rownum = 1000
        self.ntb_starttimestamp = 1601481600000
        # add_start_timestamp >= start_timestamp + row_num
        self.ntb_addtimestamp = 1601481620000
        self.ntb_addrownum = 100
        self.replica = [3]
    def data_insert_ntb(self,source_taosd_list,dbname,ntbname_m,tb_num,row_num,create_flag,start_timestamp):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            if create_flag.lower() == 'create':
                self.remote.cmd(
                    taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -n {row_num} -t {tb_num} -d {dbname[source]} -m {ntbname_m[source]} -N -s {start_timestamp} -y')
            elif create_flag.lower() == 'add':
                self.remote.cmd(
                    taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -n {row_num} -t {tb_num} -d {dbname[source]} -m {ntbname_m[source]} -N -y -U -s {start_timestamp}')
    def add_sync_db_stb(self,source_type):
        for source_task in ['','+ws']:
            for target_task in ['','+ws']:
                thread_list = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0],port=int(self.target_taosd[1]))
                taosd_backup.execute(f'drop database if exists {self.target_dbname}')
                taosd_backup.execute(f'create database if not exists {self.target_dbname}')
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    if source_type == 'db':
                        self.tdTaosx.run_taosx_db_sync(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.source_taosadapter_list[source]['spec']['adapter_config']['port'],self.target_taosd,self.target_taosadapter['spec']['adapter_config']['port'],self.dbname,self.target_dbname,source,group_id,self.timeout)
                    elif source_type == 'stable':
                        self.tdTaosx.run_taosx_stb_sync_without_topic(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.source_taosadapter_list[source]['spec']['adapter_config']['port'],self.target_taosd,self.target_taosadapter['spec']['adapter_config']['port'],self.dbname,self.stbname,self.target_dbname,source,group_id,self.timeout)
                    thread_list[source].start()
                self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.add_row_num,self.add_start_timestamp,self.add_drop_flag,self.add_child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root)
                for thread in thread_list:
                    thread.join()
                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0],port=int(self.source_taosd_list[source][1]))
                    master_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    master_sum_each = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    backup_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.stbname[source]}').fetch_all_into_dict()
                    master_count_rows.append(master_rows)
                    master_sum.append(master_sum_each)
                    backup_count_rows.append(backup_rows)
                    backup_sum_each = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.stbname[source]}').fetch_all_into_dict()
                    backup_sum.append(backup_sum_each) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(master_sum[source][0]['sum(voltage)'], backup_sum[source][0]['sum(voltage)'])
                taosd_backup.execute(f'drop database {self.target_dbname}')
    def add_sync_ntb(self):
        for source_task in ['','+ws']:
            for target_task in ['','+ws']:
                thread_list = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0],port=int(self.target_taosd[1]))
                taosd_backup.execute(f'drop database if exists {self.target_dbname}')
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                        self.tdTaosx.run_taosx_tb_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.ntb_dbname,self.ntbname_m,self.target_dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '+ws' and target_task.lower() == '':
                        self.tdTaosx.run_taosx_tb_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.ntb_dbname,self.ntbname_m,self.target_dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '' and target_task.lower() == '':
                        self.tdTaosx.run_taosx_tb_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.ntb_dbname,self.ntbname_m,self.target_dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '' and target_task.lower() == '+ws':
                        self.tdTaosx.run_taosx_tb_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.ntb_dbname,self.ntbname_m,self.target_dbname,source,group_id,self.timeout)
                    thread_list[source].start()
                self.data_insert_ntb(self.source_taosd_list,self.ntb_dbname,self.ntbname_m,self.ntb_tbnum,self.ntb_addrownum,'add',self.ntb_addtimestamp)
                for thread in thread_list:
                    thread.join()
                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0],port=int(self.source_taosd_list[source][1]))
                    master_rows = taosd_master.query(f'select count(*) from {self.ntb_dbname[source]}.{self.ntbname_m[source]}0').fetch_all_into_dict()
                    master_sum_each = taosd_master.query(f'select sum(c1) from {self.ntb_dbname[source]}.{self.ntbname_m[source]}0').fetch_all_into_dict()
                    backup_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.ntbname_m[source]}0').fetch_all_into_dict()
                    master_count_rows.append(master_rows)
                    master_sum.append(master_sum_each)
                    backup_count_rows.append(backup_rows)
                    backup_sum_each = taosd_backup.query(f'select sum(c1) from {self.target_dbname}.{self.ntbname_m[source]}0').fetch_all_into_dict()
                    backup_sum.append(backup_sum_each) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(
                        master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(
                        master_sum[source][0]['sum(c1)'], backup_sum[source][0]['sum(c1)'])
                taosd_backup.execute(f'drop database {self.target_dbname}')

    
    def run(self):
        for replica in self.replica:
            self.dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
            # self.ntb_dbname = [self.tdCom.get_long_name(6),self.tdCom.get_long_name(6)]
            self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root,replica)
            # self.data_insert_ntb(self.source_taosd_list,self.ntb_dbname,self.ntbname_m,self.ntb_tbnum,self.ntb_rownum,'create',self.ntb_starttimestamp)
            self.add_sync_db_stb('db')
            self.add_sync_db_stb('stable')
            # self.add_sync_ntb()
    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            test of taosx <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaosSql.Update