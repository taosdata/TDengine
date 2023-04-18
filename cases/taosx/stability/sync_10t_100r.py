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


import datetime
import json
import os
import threading
import time
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taosx.taosxutil import taosx

class Sync_Stability(TDCase):
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
        self.dbname = [self.tdCom.get_long_name(5)]
        self.stbname = [self.tdCom.get_long_name(3)]
        self.tbname_m = [self.tdCom.get_long_name(1)]
        self.tb_num = 100000
        self.row_num = 100000
        self.interlace_rows = 1
        self.insert_interval = 5000
        self.drop_flag = 'yes'
        self.start_timestamp = 1601481600000
        self.child_table_exist_flag = 'no'
        self.vgroups = 10
        self.timeout = '30s'
        self.taosx_num = 1
        self.replica = 1
    def sync_stablity(self):
        taosBenchmark_thread_list = []
        # taosx_thread = []
        taosx_thread_list = []
        self.target_dbname = self.tdCom.get_long_name(5)
        for source in range(len(self.source_taosd_list)):
            host = self.source_taosd_list[source][0]
            port = self.source_taosd_list[source][1]
            self.tdTaosx.write_json(f'{self.test_root}/cases/taosx/stability/stability{source}.json', self.tdTaosx.get_json(f'{self.test_root}/cases/taosx/stability/basic.json',
                            host, int(port), self.dbname[source], self.stbname[source], self.tbname_m[source],self.tb_num,self.start_timestamp,self.row_num,self.drop_flag,self.child_table_exist_flag,replica=self.replica,vgroups=self.vgroups,interlace_rows=self.interlace_rows,insert_interval=self.insert_interval))
            self.remote.cmd(self.taosBenchmark_fqdn[0],f'rm -rf /tmp/basic_stability{source}')
            self.remote.cmd(self.taosBenchmark_fqdn[0],f'mkdir /tmp/basic_stability{source}')
            self.remote.put(
                self.taosBenchmark_fqdn[0], f'{self.test_root}/cases/taosx/stability/stability{source}.json', f'/tmp/basic_stability{source}/')
        for source in range(len(self.source_taosd_list)):   
            taosBenchmark_thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                self.taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/basic_stability{source}/stability{source}.json')))
            taosBenchmark_thread_list[source].start()
        time.sleep(5)
        for i in range(self.taosx_num):
            for source in range(len(self.source_taosd_list)):
                target_dbname = self.tdCom.get_long_name(10)
                group_id = self.tdCom.get_long_name(5)
                self.tdTaosx.run_taosx_db_from_native_to_native(taosx_thread_list,self.taosx_setting,'','',self.source_taosd_list,self.target_taosd,self.dbname,target_dbname,source,group_id,self.timeout)
                taosx_thread_list[i].start()
                print(f'taosx Thread:{i} start!')

        for i in range(self.taosx_num):
            for thread in taosx_thread_list:
                thread.join()

        for thread in taosBenchmark_thread_list:
            thread.join()
    def run(self):
        self.sync_stablity()
        
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