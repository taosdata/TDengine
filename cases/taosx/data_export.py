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
import sys
import threading
import time
import taos
from time import sleep
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taosx.taosxutil import taosx
from taostest import TDCase, T
from pandas import read_parquet
class DataExportTest(TDCase):
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
        self.dbname = ['db1','db2']
        self.stbname = ['stb1','stb2']
        self.tbname_m = ['d','t']
        self.tb_num = 100
        self.row_num = 1000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'

        self.ntb_dbname = ['test1','test2']
        self.ntb_name_m = ['nd','nt']
        self.ntb_num = 1000
        self.ntb_row_num = 10000
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
 
    def export_stb_check(self):
        for source_task in ['', '+ws']:
            for file_type in ['csv','parquet']:
                for source in range(len(self.source_taosd_list)):
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source]}').fetch_all_into_dict()
                    if source_task.lower() == '':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run \
                                            -f 'taos{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])}/{self.dbname[source]}?query=select * from {self.stbname[source]}&timeout=5s'\
                                            -t '{file_type}:{self.run_log_dir}/{self.stbname[source]}.{file_type}'")
                    elif source_task.lower() == '+ws':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run \
                                            -f 'taos{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])+11}/{self.dbname[source]}?query=select * from {self.stbname[source]}&timeout=5s'\
                                            -t '{file_type}:{self.run_log_dir}/{self.stbname[source]}.{file_type}'")
                    time.sleep(0.5)
                    if file_type.lower() == 'csv':
                        total = sum(1 for line in open(f"{self.run_log_dir}/{self.stbname[source]}.{file_type}")) - 1
                        self.tdSql.checkEqual(count_rows[0]['count(*)'],total)
                    elif file_type.lower() == 'parquet':
                        all_data = read_parquet(f'{self.run_log_dir}/{self.stbname[source]}.{file_type}')
                        self.tdSql.checkEqual(count_rows[0]['count(*)'],all_data.size / len(all_data.columns))

    def export_ctb_check(self):
        for source_task in ['', '+ws']:
            for file_type in ['csv','parquet']:
                for source in range(len(self.source_taosd_list)):
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.tbname_m[source]}').fetch_all_into_dict()
                    if source_task.lower() == '':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run \
                                            -f 'taos{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])}/{self.dbname[source]}?query=select * from {self.tbname_m[source]}0&timeout=5s'\
                                            -t '{file_type}:{self.run_log_dir}/{self.tbname_m[source]}0.{file_type}'")
                    elif source_task.lower() == '+ws':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run \
                                            -f 'taos{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])+11}/{self.dbname[source]}?query=select * from {self.tbname_m[source]}0&timeout=5s'\
                                            -t '{file_type}:{self.run_log_dir}/{self.tbname_m[source]}0.{file_type}'")
                    time.sleep(0.5)
                    if file_type.lower() == 'csv':
                        total = sum(1 for line in open(f"{self.run_log_dir}/{self.tbname_m[source]}.{file_type}")) - 1
                        self.tdSql.checkEqual(count_rows[0]['count(*)'],total)
                    elif file_type.lower() == 'parquet':
                        all_data = read_parquet(f'{self.run_log_dir}/{self.tbname_m[source]}0.{file_type}')
                        self.tdSql.checkEqual(count_rows[0]['count(*)'],all_data.size / len(all_data.columns))

    def export_ntb_check(self):
        for source_task in ['', '+ws']:
            for file_type in ['csv','parquet']:
                for source in range(len(self.source_taosd_list)):
                    taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                    count_rows = taosd_master.query(f'select count(*) from {self.ntb_dbname[source]}.{self.ntb_name_m[source]}').fetch_all_into_dict()
                    if source_task.lower() == '':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run \
                                            -f 'taos{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])}/{self.dbname[source]}?query=select * from {self.ntb_name_m[source]}0&timeout=5s'\
                                            -t '{file_type}:{self.run_log_dir}/{self.ntb_name_m[source]}0.{file_type}'")
                    elif source_task.lower() == '+ws':
                        self.remote.cmd(self.taosx_setting['fqdn'][0],f"taosx run \
                                            -f 'taos{source_task}://root:taosdata@{self.source_taosd_list[source][0]}:{int(self.source_taosd_list[source][1])+11}/{self.dbname[source]}?query=select * from {self.ntb_name_m[source]}0&timeout=5s'\
                                            -t '{file_type}:{self.run_log_dir}/{self.ntb_name_m[source]}0.{file_type}'")
                    time.sleep(0.5)
                    if file_type.lower() == 'csv':
                        total = sum(1 for line in open(f"{self.run_log_dir}/{self.ntb_name_m[source]}.{file_type}")) - 1
                        self.tdSql.checkEqual(count_rows[0]['count(*)'],total)
                    elif file_type.lower() == 'parquet':
                        all_data = read_parquet(f'{self.run_log_dir}/{self.ntb_name_m[source]}0.{file_type}')
                        self.tdSql.checkEqual(count_rows[0]['count(*)'],all_data.size / len(all_data.columns))
    def run(self):
        self.tdTaosx.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,self.taosBenchmark_fqdn,self.test_root)
        self.export_stb_check()
        self.export_ctb_check()
        
        self.data_insert_ntb(self.source_taosd_list,self.ntb_dbname,self.ntb_name_m,self.ntb_num,self.ntb_row_num)
        self.export_ntb_check()
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