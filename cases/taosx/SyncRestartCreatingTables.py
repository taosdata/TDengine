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
class SyncRestartCreatingTables(TDCase):
    def init(self):
        self.tdTaosx = taosx.Runtaosx(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self.remote)
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
        self.stbname = [self.tdCom.get_long_name(3),self.tdCom.get_long_name(3)]
        self.tbname_m = [self.tdCom.get_long_name(1),self.tdCom.get_long_name(1)]
        self.tb_num = 100000000
        self.row_num = 100
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        self.timeout = '30s'
        self.vgroups = 10
        self.replica = 3
    def sync_restart(self,source_type):
        self.dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
        taosBenchmark_thread_list = []
        # taosx_thread = []
        taosx_thread_list = []
        self.target_dbname = self.tdCom.get_long_name(5)
        for source in range(len(self.source_taosd_list)):
            host = self.source_taosd_list[source][0]
            port = self.source_taosd_list[source][1]
            self.tdTaosx.write_json(f'{self.test_root}/cases/taosx/basic_createtable{source}.json', self.tdTaosx.get_json(f'{self.test_root}/cases/taosx/basic_createtable.json',
                            host, int(port), self.dbname[source], self.stbname[source], self.tbname_m[source],self.tb_num,self.start_timestamp,self.row_num,self.drop_flag,self.child_table_exist_flag,replica=self.replica,vgroups=self.vgroups,interlace_rows=0,insert_interval=0))
            self.remote.put(
                self.taosBenchmark_fqdn[0], f'{self.test_root}/cases/taosx/basic_createtable{source}.json', f'/tmp/basic_createtable{source}')
        for source in range(len(self.source_taosd_list)):   
            taosBenchmark_thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                self.taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/basic_createtable{source}/basic_createtable{source}.json')))
            taosBenchmark_thread_list[source].start()
        time.sleep(5)
        for source in range(len(self.source_taosd_list)):
            target_dbname = self.tdCom.get_long_name(10)
            group_id = self.tdCom.get_long_name(5)
            if source_type.lower() == 'db':
                self.tdTaosx.run_taosx_db_from_native_to_native(taosx_thread_list,self.taosx_setting,'','',self.source_taosd_list,self.target_taosd,self.dbname,target_dbname,source,group_id,self.timeout)
            elif source_type.lower() == 'stable':
                self.tdTaosx.run_taosx_stb_from_native_to_native(taosx_thread_list,self.taosx_setting,'','',self.source_taosd_list,self.target_taosd,self.dbname,self.tbname_m,self.target_dbname,source,group_id,self.timeout)
            taosx_thread_list[source].start()
            print(f'taosx Thread:{source} start!')
        time.sleep(5)
        self.taosd.kill_and_start(self.env_setting['settings'][0],3)
        for thread in taosBenchmark_thread_list:
            thread.join()
        for thread in taosx_thread_list:
            thread.join()
    def run(self):
        
        self.target_dbname = self.tdCom.get_long_name(5)
        self.sync_restart('db')
        self.sync_restart('stable')
        

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            test case  for TD-20172 <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaosSql.Update