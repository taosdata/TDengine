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
        self.stbname = [[self.tdCom.get_long_name(6),self.tdCom.get_long_name(6)],[self.tdCom.get_long_name(6),self.tdCom.get_long_name(6)]]
        self.tbname_m = [[self.tdCom.get_long_name(3),self.tdCom.get_long_name(3)],[self.tdCom.get_long_name(3),self.tdCom.get_long_name(3)]]
        self.tb_num = 1000
        self.row_num = 1000
        self.start_timestamp = "2020-10-01 00:00:00.000"
        self.drop_flag = 'yes'
        self.child_table_exist_flag = 'no'
        self.replica = [3]
        # param for taosBenchmark with ntb check
        self.ntb_dbname = [self.tdCom.get_long_name(6),self.tdCom.get_long_name(6)]
        self.ntb_name_m = [self.tdCom.get_long_name(2),self.tdCom.get_long_name(2)]
        self.ntb_num = 1000
        self.ntb_row_num = 1000
        self.prefix_list = ['first','second']
        self.suffix_list = ['one','two']
        self.template_list = [
            {'prefix':'first','suffix':'one'},
            {'prefix':'second','suffix':'two'}]
        # param for taosx
        self.timeout = '5s'
    def get_json(self,json_path,host,port,dbname,stbname,tbname_m,tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag,replica):
        dict = {}
        with open(json_path,'rb') as file:
            params = json.load(file)
            params['host'] = host
            params['port'] = port
            for num in range(len(stbname)):
                params['databases'][0]['dbinfo']['name'] = dbname
                params['databases'][0]['dbinfo']['drop'] = drop_flag
                params['databases'][0]['dbinfo']['replica'] = replica
                params['databases'][0]['super_tables'][num]['name'] = stbname[num]
                params['databases'][0]['super_tables'][num]['childtable_count'] = tb_num
                params['databases'][0]['super_tables'][num]['child_table_exists'] = child_table_exist_flag
                params['databases'][0]['super_tables'][num]['insert_rows'] = row_num
                params['databases'][0]['super_tables'][num]['childtable_prefix'] = tbname_m[num]
                params['databases'][0]['super_tables'][num]['start_timestamp'] = start_timestamp
            dict = params
        file.close()
        return dict
    def data_insert(self,source_taosd_list,dbname,stbname,tbname_m,tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag,replica):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        thread_list = []
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            self.tdTaosx.write_json(f'{self.test_root}/cases/taosx/two_stb{source}.json', self.get_json(f'{self.test_root}/cases/taosx/two_stb.json',
                            host, int(port), dbname[source], stbname[source], tbname_m[source],tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag,replica))
            self.remote.put(
                taosBenchmark_fqdn[0], f'{self.test_root}/cases/taosx/two_stb{source}.json', f'/tmp/two_stb{source}')
        for source in range(len(source_taosd_list)):   
            thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/two_stb{source}/two_stb{source}.json')))
            thread_list[source].start()   
        for thread in thread_list:
            thread.join() 
    def set_rename_str(self,rename_type,rename_kind,str):
        rename_str = ''
        if rename_type.lower() in['prefix','suffix'] :
            rename_str += f" -T '{rename_kind}:{rename_type}:{str}'"
        elif rename_type.lower() == 'template':
            rename_str += f''' -T "{rename_kind}:{rename_type}:{str['prefix']}{{name}}{str['suffix']}" '''
        return rename_str
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
    def rename_type_check(self,rename_type,source,rename_kind):
        if rename_type.lower() == 'prefix':
            rename_str = self.set_rename_str(rename_type,rename_kind,self.prefix_list[source])
        elif rename_type.lower() == 'suffix':
            rename_str = self.set_rename_str(rename_type,rename_kind,self.suffix_list[source])
        elif rename_type.lower() == 'template':
            rename_str = self.set_rename_str(rename_type,rename_kind,self.template_list[source])
        return rename_str
    def rename_table_check(self):
        #rename all table
        for source_task in ['', '+ws']:
            for target_task in ['', '+ws']:
                for rename_type in ['prefix','suffix','template']:
                    thread_list = []
                    master_count_rows = []
                    master_count_rows_ntb = []
                    master_sum_ntb = []
                    master_count_rows_ctb = []
                    master_sum_ctb = []
                    master_sum = []
                    taosd_backup = taos.connect(
                        host=self.target_taosd[0], port=int(self.target_taosd[1]))
                    taosd_backup.execute(f'drop database if  exists {self.target_dbname}')
                    taosd_backup.execute(f'create database if not exists {self.target_dbname}')
                    for source in range(len(self.source_taosd_list)):
                        for tbname in range(len(self.stbname)):
                            taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                            count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source][tbname]}').fetch_all_into_dict()
                            master_count_rows.append(count_rows)
                            sum_rows = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source][tbname]}').fetch_all_into_dict()
                            master_sum.append(sum_rows)
                            count_rows_ntb = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.ntb_name_m[source]}0').fetch_all_into_dict()
                            master_count_rows_ntb.append(count_rows_ntb)
                            sum_rows_ntb = taosd_master.query(f'select sum(c1) from {self.dbname[source]}.{self.ntb_name_m[source]}0').fetch_all_into_dict()
                            master_sum_ntb.append(sum_rows_ntb)
                            count_rows_ctb = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            master_count_rows_ctb.append(count_rows_ctb)
                            sum_rows_ctb = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            master_sum_ctb.append(sum_rows_ctb)
                    for source in range(len(self.source_taosd_list)):
                        group_id = self.tdCom.get_long_name(5)
                        rename_str = self.rename_type_check(rename_type,source,"rename-table")
                        if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_rename_db_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '+ws' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_rename_db_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_rename_db_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_rename_db_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        thread_list[source].start()
                    for thread in thread_list:
                        thread.join()
                    backup_count_rows = []
                    backup_sum = []
                    backup_count_rows_ntb = []
                    backup_count_rows_ctb = []
                    backup_sum_ntb = []
                    backup_sum_ctb = []
                    for source in range(len(self.source_taosd_list)):
                        for tbname in range(len(self.stbname)):
                            if rename_type.lower() == 'prefix':
                                count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.prefix_list[source]}{self.stbname[source][tbname]}').fetch_all_into_dict()
                                count_rows_ntb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.prefix_list[source]}{self.ntb_name_m[source]}0').fetch_all_into_dict()                   
                                count_rows_ctb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.prefix_list[source]}{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            elif rename_type.lower() == 'suffix':
                                count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.stbname[source][tbname]}{self.suffix_list[source]}').fetch_all_into_dict()
                                count_rows_ntb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.ntb_name_m[source]}0{self.suffix_list[source]}').fetch_all_into_dict()
                                count_rows_ctb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.tbname_m[source][tbname]}0{self.suffix_list[source]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'template':
                                count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.stbname[source][tbname]}{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                                count_rows_ntb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.ntb_name_m[source]}0{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                                count_rows_ctb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.tbname_m[source][tbname]}0{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                            backup_count_rows.append(count_rows)
                            backup_count_rows_ntb.append(count_rows_ntb)
                            backup_count_rows_ctb.append(count_rows_ctb)
                            if rename_type.lower() == 'prefix':
                                sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.prefix_list[source]}{self.stbname[source][tbname]}').fetch_all_into_dict()
                                sum_rows_ntb = taosd_backup.query(f'select sum(c1) from {self.target_dbname}.{self.prefix_list[source]}{self.ntb_name_m[source]}0').fetch_all_into_dict()
                                sum_rows_ctb = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.prefix_list[source]}{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            elif rename_type.lower() == 'suffix':
                                sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.stbname[source][tbname]}{self.suffix_list[source]}').fetch_all_into_dict()
                                sum_rows_ntb = taosd_backup.query(f'select sum(c1) from {self.target_dbname}.{self.ntb_name_m[source]}0{self.suffix_list[source]}').fetch_all_into_dict()
                                sum_rows_ctb = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.tbname_m[source][tbname]}0{self.suffix_list[source]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'template':
                                sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.stbname[source][tbname]}{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                                sum_rows_ntb = taosd_backup.query(f'select sum(c1) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.ntb_name_m[source]}0{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                                sum_rows_ctb = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.tbname_m[source][tbname]}0{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                            backup_sum.append(sum_rows)
                            backup_sum_ntb.append(sum_rows_ntb)
                            backup_sum_ctb.append(sum_rows_ctb)
                    for i in range(len(self.source_taosd_list) * len(self.stbname)):
                        self.tdSql.checkEqual(master_count_rows[i][0]['count(*)'], backup_count_rows[i][0]['count(*)'])
                        self.tdSql.checkEqual(master_count_rows_ntb[i][0]['count(*)'],backup_count_rows_ntb[i][0]['count(*)'])
                        self.tdSql.checkEqual(master_count_rows_ctb[i][0]['count(*)'],backup_count_rows_ctb[i][0]['count(*)'])
                        self.tdSql.checkEqual(master_sum[i][0]['sum(voltage)'], backup_sum[i][0]['sum(voltage)'])
                        self.tdSql.checkEqual(master_sum_ntb[i][0]['sum(c1)'], backup_sum_ntb[i][0]['sum(c1)'])
                        self.tdSql.checkEqual(master_sum_ctb[i][0]['sum(voltage)'], backup_sum_ctb[i][0]['sum(voltage)'])
                    taosd_backup.execute(f'drop database {self.target_dbname}')
    
    def rename_stable_check(self):
        for source_task in ['', '+ws']:
            for target_task in ['', '+ws']:
                for rename_type in ['prefix','suffix','template']:
                    thread_list = []
                    master_count_rows = []
                    master_sum = []
                    rename_str = ''
                    taosd_backup = taos.connect(
                        host=self.target_taosd[0], port=int(self.target_taosd[1]))
                    taosd_backup.execute(f'drop database if  exists {self.target_dbname}')
                    taosd_backup.execute(f'create database if not exists {self.target_dbname}')
                    for source in range(len(self.source_taosd_list)):
                        for tbname in range(len(self.stbname)):
                            taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                            count_rows = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.stbname[source][tbname]}').fetch_all_into_dict()
                            master_count_rows.append(count_rows)
                            sum_rows = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.stbname[source][tbname]}').fetch_all_into_dict()
                            master_sum.append(sum_rows)
                    for source in range(len(self.source_taosd_list)):
                        group_id = self.tdCom.get_long_name(5)
                        rename_str = self.rename_type_check(rename_type,source,"rename-super-table")
                        if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_rename_db_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '+ws' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_rename_db_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_rename_db_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_rename_db_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        thread_list[source].start()
                    for thread in thread_list:
                        thread.join()    
                    backup_count_rows = []
                    backup_sum = []
                    for source in range(len(self.source_taosd_list)):
                        for tbname in range(len(self.stbname)):
                            if rename_type.lower() == 'prefix':
                                count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.prefix_list[source]}{self.stbname[source][tbname]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'suffix':
                                count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.stbname[source][tbname]}{self.suffix_list[source]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'template':
                                count_rows = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.stbname[source][tbname]}{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                            backup_count_rows.append(count_rows)
                            if rename_type.lower() == 'prefix':
                                sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.prefix_list[source]}{self.stbname[source][tbname]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'suffix':
                                sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.stbname[source][tbname]}{self.suffix_list[source]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'template':
                                sum_rows = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.stbname[source][tbname]}{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                            backup_sum.append(sum_rows)
                    for i in range(len(self.source_taosd_list) * len(self.stbname)):
                        self.tdSql.checkEqual(master_count_rows[i][0]['count(*)'], backup_count_rows[i][0]['count(*)'])
                        self.tdSql.checkEqual(master_sum[i][0]['sum(voltage)'], backup_sum[i][0]['sum(voltage)'])
                    taosd_backup.execute(f'drop database {self.target_dbname}')
    def rename_ctable_check(self):
        for source_task in ['', '+ws']:
            for target_task in ['', '+ws']:
                for rename_type in ['prefix','suffix','template']:
                    thread_list = []
                    master_count_rows_ctb = []
                    master_sum_ctb = []
                    rename_str = ''
                    taosd_backup = taos.connect(
                        host=self.target_taosd[0], port=int(self.target_taosd[1]))
                    taosd_backup.execute(f'create database if not exists {self.target_dbname}')
                    for source in range(len(self.source_taosd_list)):
                        for tbname in range(len(self.stbname)):
                            taosd_master = taos.connect(host=self.source_taosd_list[source][0], port=int(self.source_taosd_list[source][1]))
                            count_rows_ctb = taosd_master.query(f'select count(*) from {self.dbname[source]}.{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            master_count_rows_ctb.append(count_rows_ctb)
                            sum_rows_ctb = taosd_master.query(f'select sum(voltage) from {self.dbname[source]}.{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            master_sum_ctb.append(sum_rows_ctb)
                    for source in range(len(self.source_taosd_list)):
                        group_id = self.tdCom.get_long_name(5)
                        rename_str = self.rename_type_check(rename_type,source,"rename-child-table")
                        if source_task.lower() == '+ws' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_rename_db_from_ws_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '+ws' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_rename_db_from_ws_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '' and target_task.lower() == '':
                            self.tdTaosx.run_taosx_rename_db_from_native_to_native(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        elif source_task.lower() == '' and target_task.lower() == '+ws':
                            self.tdTaosx.run_taosx_rename_db_from_native_to_ws(thread_list,self.taosx_setting,source_task,target_task,self.source_taosd_list,self.target_taosd,self.dbname,self.target_dbname,source,group_id,self.timeout,rename_str)
                        thread_list[source].start()
                    for thread in thread_list:
                        thread.join()
                    backup_count_rows_ctb = []
                    backup_sum_ctb = []
                    for source in range(len(self.source_taosd_list)):
                        for tbname in range(len(self.stbname)):
                            if rename_type.lower() == 'prefix':
                                count_rows_ctb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.prefix_list[source]}{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            elif rename_type.lower() == 'suffix':
                                count_rows_ctb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.tbname_m[source][tbname]}0{self.suffix_list[source]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'template':
                                count_rows_ctb = taosd_backup.query(f'select count(*) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.tbname_m[source][tbname]}0{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                            backup_count_rows_ctb.append(count_rows_ctb)
                            if rename_type.lower() == 'prefix':
                                sum_rows_ctb = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.prefix_list[source]}{self.tbname_m[source][tbname]}0').fetch_all_into_dict()
                            elif rename_type.lower() == 'suffix':
                                sum_rows_ctb = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.tbname_m[source][tbname]}0{self.suffix_list[source]}').fetch_all_into_dict()
                            elif rename_type.lower() == 'template':
                                sum_rows_ctb = taosd_backup.query(f'select sum(voltage) from {self.target_dbname}.{self.template_list[source]["prefix"]}{self.tbname_m[source][tbname]}0{self.template_list[source]["suffix"]}').fetch_all_into_dict()
                            backup_sum_ctb.append(sum_rows_ctb)
                    for i in range(len(self.source_taosd_list) * len(self.stbname)):
                        self.tdSql.checkEqual(master_count_rows_ctb[i][0]['count(*)'],backup_count_rows_ctb[i][0]['count(*)'])
                        self.tdSql.checkEqual(master_sum_ctb[i][0]['sum(voltage)'], backup_sum_ctb[i][0]['sum(voltage)'])
                    taosd_backup.execute(f'drop database {self.target_dbname}')
    def run(self):
        for replica in self.replica:
            self.dbname = [self.tdCom.get_long_name(5),self.tdCom.get_long_name(5)]
            self.data_insert(self.source_taosd_list,self.dbname,self.stbname,self.tbname_m,self.tb_num,self.row_num,self.start_timestamp,self.drop_flag,self.child_table_exist_flag,replica)
            self.target_dbname = self.tdCom.get_long_name(3)
            self.rename_stable_check()
            # self.rename_ctable_check()
        # self.data_insert_ntb(self.source_taosd_list,self.ntb_dbname,self.ntb_name_m,self.ntb_num,self.ntb_row_num)
        # self.rename_table_check()
    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            rename-table test of taosx <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaosSql.Update
