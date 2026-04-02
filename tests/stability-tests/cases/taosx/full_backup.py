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
from random import randint
from socket import timeout
import threading
import time
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taosx.taosxutil import taosx
class FullBackup(TDCase):
    def init(self):
        self.tdTaosx = taosx.Runtaosx(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.firstEP = []
        self.source_taosd_list = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
            if env_setting["name"].lower() == 'taosx':
                self.taosx_setting = env_setting
                
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        # param for taosBenchmark with db,stb and ctb check
        
        self.stbname = ['stb1','stb2']
        self.tbname_m = ['d','t']
        self.tb_num = 1000
        self.row_num = 1000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        # param for taosBenchmark with ntb check
        self.ntb_dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
        self.ntb_name_m = ['nd','nt']
        self.ntb_num = 1000
        self.ntb_row_num = 1000
        # param for taosx
        self.timeout = '5s'
        self.target_dbname = 'target'
        self.replica = [3]
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
    def full_backup_db_stb(self,source_type):
        # for target_task in ['']:
        #     for source_task in ['']:
        for target_task in ['','+ws']:
            for source_task in ['', '+ws']:
                thread_list_source = []
                thread_list_target = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0], port=int(self.target_taosd[1]))
                for source in range(len(self.source_taosd_list)):
                    target_file_dir = f'/home/{self.source_taosd_list[source][0]}_backup_{source}'
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'rm -rf {target_file_dir}')
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'mkdir {target_file_dir}')
                    group_id = self.tdCom.get_long_name(5)
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    
                    taosd_backup.execute(f'drop database if exists {self.dbname[source]}')
                    count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    master_count_rows.append(count_rows)
                    sum_rows = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    master_sum.append(sum_rows)
                    if source_type == 'db':
                        if source_task.lower() == '+ws':
                            self.tdTaosx.run_backup_db_from_ws_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '':
                            self.tdTaosx.run_backup_db_from_native_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,source,group_id,self.timeout)
                        thread_list_source[source].start()
                    elif source_type == 'stb':
                        if source_task.lower() == '+ws':
                            self.tdTaosx.run_backup_stb_from_ws_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,self.stbname,source,group_id,self.timeout)
                        elif source_task.lower() == '':
                            self.tdTaosx.run_backup_stb_from_native_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,self.stbname,source,group_id,self.timeout)
                        thread_list_source[source].start()
                for thread in thread_list_source:
                    thread.join()
                for source in range(len(self.source_taosd_list)):
                    target_file_dir = f'/home/{self.source_taosd_list[source][0]}_backup_{source}'
                    if target_task.lower() == '+ws':
                        self.tdTaosx.run_restore_from_local_to_ws(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.target_taosd,self.dbname,source)
                    elif target_task.lower() == '':
                        self.tdTaosx.run_restore_from_local_to_native(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.target_taosd,self.dbname,source)
                    thread_list_target[source].start()
                for thread in thread_list_target:
                    thread.join()

                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    count_rows = taosd_backup.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    backup_count_rows.append(count_rows)
                    sum_rows = taosd_backup.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    backup_sum.append(sum_rows) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(master_sum[source][0]['sum(voltage)'], backup_sum[source][0]['sum(voltage)'])
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'/home/{self.source_taosd_list[source][0]}_backup_{source}')
                    taosd_backup.execute(f'drop database {self.dbname[source]}')
    def full_backup_ctb(self):
        # for target_task in ['']:
        #     for source_task in ['']:
        for target_task in ['','+ws']:
            for source_task in ['', '+ws']:
                thread_list_source = []
                thread_list_target = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0], port=int(self.target_taosd[1]))
                for source in range(len(self.source_taosd_list)):
                    target_file_dir = f'/home/{self.source_taosd_list[source][0]}_backup_{source}'
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'rm -rf {target_file_dir}')
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'mkdir {target_file_dir}')
                    group_id = self.tdCom.get_long_name(5)
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    master_count_rows.append(count_rows)
                    sum_rows = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    master_sum.append(sum_rows)
                    taosd_backup.execute(f'drop database if exists {self.dbname[source]}')
                    if source_task.lower() == '+ws':
                        self.tdTaosx.run_backup_tb_from_ws_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,self.tbname_m,source,group_id,self.timeout)
                    elif source_task.lower() == '':
                        self.tdTaosx.run_backup_tb_from_native_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,self.tbname_m,source,group_id,self.timeout)
                    thread_list_source[source].start()
                for thread in thread_list_source:
                    thread.join()
                for source in range(len(self.source_taosd_list)):
                    target_file_dir = f'/home/{self.source_taosd_list[source][0]}_backup_{source}'
                    if target_task.lower() == '+ws':
                        self.tdTaosx.run_restore_from_local_to_ws(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.target_taosd,self.dbname,source)
                    elif target_task.lower() == '':
                        self.tdTaosx.run_restore_from_local_to_native(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.target_taosd,self.dbname,source)
                    thread_list_target[source].start()
                for thread in thread_list_target:
                    thread.join()
                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    count_rows = taosd_backup.query(f'select count(*) from {self.dbname[source]}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    backup_count_rows.append(count_rows)
                    sum_rows = taosd_backup.query(f'select sum(voltage) from {self.dbname[source]}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    backup_sum.append(sum_rows) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(master_sum[source][0]['sum(voltage)'], backup_sum[source][0]['sum(voltage)'])
                    taosd_backup.execute(f'drop database {self.dbname[source]}')
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'rm -rf /home/{self.source_taosd_list[source][0]}_backup_{source}')    
    def full_backup_ntb(self):
        # for target_task in ['']:
        #     for source_task in ['']:
        for target_task in ['','+ws']:
            for source_task in ['', '+ws']:
                thread_list_source = []
                thread_list_target = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0], port=int(self.target_taosd[1]))
                for source in range(len(self.source_taosd_list)):
                    target_file_dir = f'/home/{self.source_taosd_list[source][0]}_backup_{source}'
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'rm -rf {target_file_dir}')
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'mkdir {target_file_dir}')
                    group_id = self.tdCom.get_long_name(5)
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    wal_value = randint(1, 1000)
                    taosd_master.execute(f'alter database {self.ntb_dbname[source]} WAL_RETENTION_PERIOD {wal_value}')
                    count_rows = taosd_master.query(f'select count(*) from {self.ntb_dbname[source]}.{self.ntb_name_m[source]}0').fetch_all_into_dict()
                    master_count_rows.append(count_rows)
                    sum_rows = taosd_master.query(f'select sum(c1) from {self.ntb_dbname[source]}.{self.ntb_name_m[source]}0').fetch_all_into_dict()
                    master_sum.append(sum_rows)
                    taosd_backup.execute(f'drop database if exists {self.ntb_name_m[source]}')
                    if source_task.lower() == '+ws':
                            self.tdTaosx.run_backup_tb_from_ws_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.ntb_dbname,self.ntb_name_m,source,group_id,self.timeout)
                    elif source_task.lower() == '':
                        self.tdTaosx.run_backup_tb_from_native_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.ntb_dbname,self.ntb_name_m,source,group_id,self.timeout)
                    thread_list_source[source].start()
                for thread in thread_list_source:
                    thread.join()
                
                for source in range(len(self.source_taosd_list)):
                    target_file_dir = f'/home/{self.source_taosd_list[source][0]}_backup_{source}'
                    if target_task.lower() == '+ws':
                        self.tdTaosx.run_restore_from_local_to_ws(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.target_taosd,self.ntb_name_m,source)
                    elif target_task.lower() == '':
                        self.tdTaosx.run_restore_from_local_to_native(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.target_taosd,self.ntb_name_m,source)
                    thread_list_target[source].start()
                for thread in thread_list_target:
                    thread.join()
                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    count_rows = taosd_backup.query(f'select count(*) from {self.ntb_name_m[source]}.{self.ntb_name_m[source]}0').fetch_all_into_dict()
                    backup_count_rows.append(count_rows)
                    sum_rows = taosd_backup.query(f'select sum(c1) from {self.ntb_name_m[source]}.{self.ntb_name_m[source]}0').fetch_all_into_dict()
                    backup_sum.append(sum_rows) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(master_sum[source][0]['sum(c1)'], backup_sum[source][0]['sum(c1)'])
                    taosd_backup.execute(f'drop database {self.ntb_name_m[source]}')
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'rm -rf /home/{self.source_taosd_list[source][0]}_backup_{source}')
    def run(self):
        for replica in self.replica:
            self.dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
            self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root,replica)
            self.full_backup_db_stb('db')
            self.full_backup_db_stb('stb')
            self.full_backup_ctb()
            self.ntb_dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
            self.data_insert_ntb(self.source_taosd_list,self.ntb_dbname,self.ntb_name_m,self.ntb_num,self.ntb_row_num)
            self.full_backup_ntb()
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
        return T.Write.TaosSql.Update