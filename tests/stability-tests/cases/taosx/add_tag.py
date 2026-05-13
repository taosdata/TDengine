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

from audioop import add
import json
import os
import threading
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taosx.taosxutil import taosx

class RenameTable(TDCase):
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
        # param for taosBenchmark with db,stb and ctb check
        self.dbname = ['db1','db2']
        self.stbname = [['stb11','stb12'],['stb21','stb22']]
        self.tbname_m = [['d1d','t1t'],['d2d','t2t']]
        self.tb_num = 10
        self.row_num = 10000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        self.add_tag = [[{'name':'t11','value':'tag11'},{'name':'t12','value':'tag12'}],
        [{'name':'t21','value':'tag21'},{'name':'t22','value':'tag22'}]]
        # param for taosx
        self.timeout = '5s'
        self.target_dbname = 'target'
    def get_json(self,json_path,host,port,dbname,stbname,tbname_m,tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag):
        dict = {}
        with open(json_path,'rb') as file:
            params = json.load(file)
            params['host'] = host
            params['port'] = port
            for num in range(len(stbname)):
                params['databases'][0]['dbinfo']['name'] = dbname
                params['databases'][0]['dbinfo']['drop'] = drop_flag
                params['databases'][0]['super_tables'][num]['name'] = stbname[num]
                params['databases'][0]['super_tables'][num]['childtable_count'] = tb_num
                params['databases'][0]['super_tables'][num]['child_table_exists'] = child_table_exist_flag
                params['databases'][0]['super_tables'][num]['insert_rows'] = row_num
                params['databases'][0]['super_tables'][num]['childtable_prefix'] = tbname_m[num]
                params['databases'][0]['super_tables'][num]['start_timestamp'] = start_timestamp
            dict = params
        file.close()
        return dict
    def data_insert(self,source_taosd_list,dbname,stbname,tbname_m,tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        thread_list = []
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            self.tdTaosx.write_json(f'{self.test_root}/cases/taosx/two_stb{source}.json', self.get_json(f'{self.test_root}/cases/taosx/two_stb.json',
                            host, int(port), dbname[source], stbname[source], tbname_m[source],tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag))
            self.remote.put(
                taosBenchmark_fqdn[0], f'{self.test_root}/cases/taosx/two_stb{source}.json', f'/tmp/two_stb{source}')
        for source in range(len(source_taosd_list)):   
            thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/two_stb{source}/two_stb{source}.json')))
            thread_list[source].start()   
        for thread in thread_list:
            thread.join() 
    def set_add_tag_str(self,tag_list):
        add_tag_str = ''
        for tag in tag_list:
            add_tag_str += f''' -T 'add-tag:{tag["name"]}={tag["value"]}' '''
        return add_tag_str
    def add_tag_check(self):
        for source_task in ['', '+ws']:
            for target_task in ['', '+ws']:
                thread_list = []
                master_count_rows = []
                master_sum = []
                taosd_backup = taos.connect(
                    host=self.target_taosd[0], port=int(self.target_taosd[1]))
                taosd_backup.execute(f'drop database if exists {self.target_dbname}')
                taosd_backup.execute(f'create database if not exists {self.target_dbname}')
                for source in range(len(self.source_taosd_list)):
                    for tbname in range(len(self.stbname)):
                        taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                        count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source][tbname]}').fetch_all_into_dict()
                        master_count_rows.append(count_rows)
                        sum = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source][tbname]}').fetch_all_into_dict()
                        master_sum.append(sum)
                for source in range(len(self.source_taosd_list)):
                    group_id = self.tdCom.get_long_name(5)
                    add_tag_str = self.set_add_tag_str(self.add_tag[source])
                    if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                        self.tdTaosx.run_taosx_add_tag_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,add_tag_str)
                    elif source_task.lower() == '+ws' and target_task.lower() == '':
                        self.tdTaosx.run_taosx_add_tag_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,add_tag_str)
                    elif source_task.lower() == '' and target_task.lower() == '':
                        self.tdTaosx.run_taosx_add_tag_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,add_tag_str)
                    elif source_task.lower() == '' and target_task.lower() == '+ws':
                        self.tdTaosx.run_taosx_add_tag_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,add_tag_str)
                    thread_list[source].start()
                for thread in thread_list:
                    thread.join()
                backup_count_rows = []
                backup_sum = []
                for tag in self.add_tag:
                    for tag_num in range(len(tag)):
                        for source in range(len(self.source_taosd_list)):
                            for tbname in range(len(self.stbname)):
                                print(f'''select count(*) from {self.target_dbname}.{self.stbname[source][tbname]} where {tag[tag_num]["name"]} = '{tag[tag_num]["value"]}' ''')
                                count_rows = taosd_backup.query(f'''select count(*) from {self.target_dbname}.{self.stbname[source][tbname]} where {tag[tag_num]["name"]} = '{tag[tag_num]["value"]}' ''').fetch_all_into_dict()
                                backup_count_rows.append(count_rows)
                                sum_rows = taosd_backup.query(f'''select sum(voltage) from {self.target_dbname}.{self.stbname[source][tbname]} where {tag[tag_num]["name"]} = '{tag[tag_num]["value"]}' ''').fetch_all_into_dict()
                                backup_sum.append(sum_rows)
                            print(master_count_rows)
                            print(backup_count_rows)
                        for i in range(len(self.source_taosd_list) * len(self.stbname)):
                            self.tdSql.checkEqual(master_count_rows[i][0]['count(*)'], backup_count_rows[i][0]['count(*)'])
                            self.tdSql.checkEqual(master_sum[i][0]['sum(voltage)'], backup_sum[i][0]['sum(voltage)'])
                taosd_backup.execute(f'drop database {self.target_dbname}')
    
    
    def run(self):
        self.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag)
        self.add_tag_check()
    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            add-tag test of taosx <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaosSql.Update