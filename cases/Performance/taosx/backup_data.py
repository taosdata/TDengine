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


from datetime import datetime
import json
import os
from socket import timeout
import time
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taosx.taosxutil import taosx
from taostest.performance.result_reduction import Perf_Base_func
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
        for i in range(self.taosd_num):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.test_root = os.environ['TEST_ROOT']
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        # param for taosBenchmark with db,stb and ctb check
        self.stbname = ['stb1']
        self.tbname_m = ['d']
        self.tb_num = 10000
        self.row_num = 100000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        self.timeout = '5s'
        self.target_dbname = 'target'
        self.replica = 3
        self.Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        
    def full_backup_db_stb(self):
        for source_task in ['']:
            for target_task in ['']:
                target_dbname = [self.tdCom.get_long_name(6)]
                timestamp_start = ''
                timestamp_end = ''
                thread_list_source = []
                for source in range(len(self.source_taosd_list)):
                    timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    target_file_dir = f'{self.run_log_dir}/{self.source_taosd_list[source][0]}_backup_{source}'
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'mkdir {target_file_dir}')
                    group_id = self.tdCom.get_long_name(5)
                    if source_task.lower() == '+ws':
                        self.tdTaosx.run_backup_db_from_ws_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,source,group_id,self.timeout)
                    elif source_task.lower() == '':
                        self.tdTaosx.run_backup_db_from_native_to_local(thread_list_source,self.taosx_setting,source_task,target_file_dir,self.source_taosd_list,self.dbname,source,group_id,self.timeout)
                    thread_list_source[source].start()
                for thread in thread_list_source:
                    thread.join()
                timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                env_setting = self.get_component_by_name("prometheus")
                self.Insert_file.get_process_exporter_info(env_setting, 0.1, timestamp_start, timestamp_end)
                self.Insert_file.get_node_exporter_info(env_setting, 0.1, timestamp_start, timestamp_end)
                time.sleep(5)
                thread_list_target = []
                for source in range(len(self.source_taosd_list)):
                    timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    target_file_dir = f'{self.run_log_dir}/{self.source_taosd_list[source][0]}_backup_{source}'
                    if target_task.lower() == '+ws':
                        self.tdTaosx.run_restore_from_local_to_ws(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.source_taosd_list[0],target_dbname,source)
                    elif target_task.lower() == '':

                        self.tdTaosx.run_restore_from_local_to_native(thread_list_target,self.taosx_setting,target_task,target_file_dir,self.source_taosd_list[0],target_dbname,source)
                    thread_list_target[source].start()
                for thread in thread_list_target:
                    thread.join()
                timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                self.Insert_file.get_process_exporter_info(env_setting, 0.1, timestamp_start, timestamp_end)
                self.Insert_file.get_node_exporter_info(env_setting, 0.1, timestamp_start, timestamp_end)
                time.sleep(5)
                for source in range(len(self.source_taosd_list)):
                    self.remote.cmd(self.taosx_setting['fqdn'][0],f'rm -rf {self.run_log_dir}/{self.source_taosd_list[source][0]}_backup_{source}')

    def run(self):
        self.dbname = [self.tdCom.get_long_name(5)]
        self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root,self.replica)
        self.full_backup_db_stb()
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
