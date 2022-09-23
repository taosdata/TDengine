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
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(self.taosd_setting['spec']['config']['firstEP'])
            if env_setting["name"].lower() == 'taosx':
                self.taosx_setting = env_setting
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        #param for taosBenchmark
        self.dbname = ['db1','db2']
        self.stbname = ['stb1','stb2']
        self.tbname_m = ['d','t']
        self.tb_num = 100
        self.row_num = 1000
        self.drop_flag = 'yes'
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.child_table_exist_flag = 'no'
        # add_start_timestamp >= start_timestamp + row_num
        self.add_drop_flag = 'no'
        self.add_start_timestamp = "2021-10-01 00:00:10"
        self.add_row_num = 100
        self.add_child_table_exist_flag = 'yes'
        self.add_ctb_start_timestamp = "2022-01-01 00:00:00"
        #param for taosx
        self.timeout = '10s'
        self.target_dbname = 'target'
    
    # def get_json(self,json_path,host,port,dbname,stbname,tbname_m,start_timestamp,row_num,drop_flag,child_table_exist):
    #     dict = {}
    #     with open(json_path,'rb') as file:
    #         params = json.load(file)
    #         params['host'] = host
    #         params['port'] = port
    #         params['databases'][0]['dbinfo']['name'] = dbname
    #         params['databases'][0]['dbinfo']['drop'] = drop_flag
    #         params['databases'][0]['super_tables'][0]['name'] = stbname
    #         params['databases'][0]['super_tables'][0]['childtable_count'] = self.tb_num
    #         params['databases'][0]['super_tables'][0]['child_table_exists'] = child_table_exist
    #         params['databases'][0]['super_tables'][0]['insert_rows'] = row_num
    #         params['databases'][0]['super_tables'][0]['childtable_prefix'] = tbname_m
    #         params['databases'][0]['super_tables'][0]['start_timestamp'] = start_timestamp
    #         dict = params
    #     file.close()
    #     return dict
    # def write_json(self,json_path,dict):
    #     with open(json_path,'w') as r:
    #         json.dump(dict,r)
    #     r.close()
    # def data_insert(self,start_timestamp,row_num,drop_flag,child_table_exist_flag):
    #     taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
    #     thread_list = []
    #     for source in range(len(self.source_taosd_list)):
    #         host = self.source_taosd_list[source][0]
    #         port = self.source_taosd_list[source][1]
    #         self.write_json(f'{self.test_root}/cases/taosx/basic.json',self.get_json(f'{self.test_root}/cases/taosx/basic.json',host,int(port),self.dbname[source],self.stbname[source],self.tbname_m[source],start_timestamp,row_num,drop_flag,child_table_exist_flag))
    #         self.remote.put(taosBenchmark_fqdn[0],f'{self.test_root}/cases/taosx/basic.json','/tmp/basic{source}')
    #     for source in range(len(self.source_taosd_list)):   
    #         thread_list.append(threading.Thread(target=self.remote.cmd,args=(
    #             taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/basic{source}/basic.json')))
    #         thread_list[source].start()   
    #     for thread in thread_list:
    #         thread.join() 
    def data_insert_ntb(self,source_taosd_list,dbname,ntbname_m,tb_num,row_num):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            self.remote.cmd(
                taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -n {row_num} -t {tb_num} -d {dbname[source]} -m {ntbname_m[source]} -N -y')
    def add_sync_db_stb(self,source_type):
        for source_task in ['','+ws']:
            for target_task in ['','+ws']:
                thread_list = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0],port=int(self.target_taosd[1]))
                taosd_backup.execute(f'create database if not exists {self.target_dbname}')
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    if source_type == 'db':
                        if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_db_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '+ws' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_db_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_db_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_db_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout)
                    elif source_type == 'stable':
                        if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_stb_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.stbname,self.target_dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '+ws' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_stb_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.stbname,self.target_dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_stb_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.stbname,self.target_dbname,source,group_id,self.timeout)
                        elif source_task.lower() == '' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_stb_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.stbname,self.target_dbname,source,group_id,self.timeout)
                    thread_list[source].start()
                self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.add_row_num,self.add_start_timestamp,self.add_drop_flag,self.add_child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root)
                # self.tdTaosx.data_insert(self.add_start_timestamp,self.add_row_num,self.add_drop_flag,self.add_child_table_exist_flag)
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
    def add_sync_ctb(self):
        for source_task in ['','+ws']:
            for target_task in ['','+ws']:
                thread_list = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(host=self.target_taosd[0],port=int(self.target_taosd[1]))
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                        self.tdTaosx.run_taosx_tb_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.tbname_m,self.target_dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '+ws' and target_task.lower() == '':
                        self.tdTaosx.run_taosx_tb_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.tbname_m,self.target_dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '' and target_task.lower() == '':
                        self.tdTaosx.run_taosx_tb_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.tbname_m,self.target_dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '' and target_task.lower() == '+ws':
                        self.tdTaosx.run_taosx_tb_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.tbname_m,self.target_dbname,source,group_id,self.timeout)
                    thread_list[source].start()
                self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.add_row_num,self.add_ctb_start_timestamp,self.add_drop_flag,self.add_child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root)
                # self.data_insert(self.add_ctb_start_timestamp,self.add_row_num,self.add_drop_flag,self.add_child_table_exist_flag)
                for thread in thread_list:
                    thread.join()
                backup_count_rows = []
                backup_sum = []
                for source in range(len(self.source_taosd_list)):
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0],port=int(self.source_taosd_list[source][1]))
                    master_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    master_sum_each = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    backup_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    master_count_rows.append(master_rows)
                    master_sum.append(master_sum_each)
                    backup_count_rows.append(backup_rows)
                    backup_sum_each = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.tbname_m[source]}0').fetch_all_into_dict()
                    backup_sum.append(backup_sum_each) 
                for source in range(len(self.source_taosd_list)):
                    self.tdSql.checkEqual(
                        master_count_rows[source][0]['count(*)'], backup_count_rows[source][0]['count(*)'])
                    self.tdSql.checkEqual(
                        master_sum[source][0]['sum(voltage)'], backup_sum[source][0]['sum(voltage)'])
                taosd_backup.execute(f'drop database {self.target_dbname}')
    def add_sync_ntb(self):

        pass
    def run(self):
        self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root)
        self.add_sync_db_stb('db')
        self.add_sync_db_stb('stable')
        self.add_sync_ctb()
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