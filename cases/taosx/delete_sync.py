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

import os
import threading
import time
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taosx.taosxutil import taosx

class DeleteFyns(TDCase):
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
        for i in range(len(self.taosadapter_list) - 1):
            self.source_taosadapter_list.append(self.taosadapter_list[i])
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        self.target_taosadapter = self.taosadapter_list[-1]
        #param for taosBenchmark
        self.dbname = ['db1','db2']
        self.stbname = ['stb1','stb2']
        self.tbname_m = ['d','t']
        self.tb_num = 1000
        self.row_num = 1000
        self.drop_flag = 'yes'
        self.start_timestamp = "2020-10-01 00:00:00.000"
        #start_timestamp <= delete_timestamp <= start_timestamp + row_num
        self.delete_timestamp = "2020-10-01 00:00:00.100"
        self.child_table_exist_flag = 'no'
        self.add_drop_flag = 'no'
        self.add_child_table_exist_flag = 'yes'
        # param for taosx
        self.timeout = '5s'
        self.target_dbname = 'target'
        self.replica = [3]
        self.vgroups = 10
        self.interlace_rows = 0
        self.insert_interval = 0
    def data_insert(self,source_taosd_list,dbname,stbname,tbname_m,tb_num,start_timestamp,row_num,drop_flag,child_table_exist_flag,replica,vgroups,interlace_rows,insert_interval):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        thread_list = []
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            self.tdTaosx.write_json(f'{self.test_root}/cases/taosx/basic.json', self.tdTaosx.get_json(f'{self.test_root}/cases/taosx/basic.json',
                            host, int(port), dbname[source], stbname[source], tbname_m[source],tb_num,start_timestamp,row_num,drop_flag,child_table_exist_flag,replica,vgroups,interlace_rows,insert_interval))
            self.remote.put(
                taosBenchmark_fqdn[0], f'{self.test_root}/cases/taosx/basic.json', f'/tmp/basic{source}')
        for source in range(len(source_taosd_list)):   
            thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/basic{source}/basic.json')))
            thread_list[source].start()   
        for thread in thread_list:
            thread.join()  
    def delete_sync_db_stb(self,source_type):
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
                        self.tdTaosx.run_taosx_db_sync(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.source_taosadapter_list[source]['spec']['adapter_config']['port'],self.target_taosd,self.target_taosadapter['spec']['adapter_config']['port'],self.dbname,self.target_dbname,source,group_id,self.timeout)
                    elif source_type == 'stable':
                        self.tdTaosx.run_taosx_stb_sync_without_topic(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.source_taosadapter_list[source]['spec']['adapter_config']['port'],self.target_taosd,self.target_taosadapter['spec']['adapter_config']['port'],self.dbname,self.stbname,self.target_dbname,source,group_id,self.timeout)
                    thread_list[source].start()
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0],port=int(self.source_taosd_list[source][1]))
                    taosd_master.execute(f'delete from {self.dbname[source]}.{self.stbname[source]} where ts <= "{self.delete_timestamp}" ')
                    time.sleep(0.05)
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
                self.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.start_timestamp,self.row_num,self.add_drop_flag,self.add_child_table_exist_flag,3,self.vgroups,self.interlace_rows,self.insert_interval)

    def run(self):
        for replica in self.replica:
            self.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.start_timestamp,self.row_num,self.drop_flag,self.child_table_exist_flag,replica,self.vgroups,self.interlace_rows,self.insert_interval)
            self.delete_sync_db_stb('db')
            self.delete_sync_db_stb('stable')

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
             test of taosx <jiacy>
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return 