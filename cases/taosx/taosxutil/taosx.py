###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
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
import threading
from taostest.util.remote import Remote
class Runtaosx():
    def __init__(self,logger):
        self.logger = logger
        self.remote: Remote = Remote(self.logger)
    def get_json(self,json_path,host,port,dbname,stbname,tbname_m,tb_num,start_timestamp,row_num,drop_flag,child_table_exist):
        dict = {}
        with open(json_path,'rb') as file:
            params = json.load(file)
            params['host'] = host
            params['port'] = port
            params['databases'][0]['dbinfo']['name'] = dbname
            params['databases'][0]['dbinfo']['drop'] = drop_flag
            params['databases'][0]['super_tables'][0]['name'] = stbname
            params['databases'][0]['super_tables'][0]['childtable_count'] = tb_num
            params['databases'][0]['super_tables'][0]['child_table_exists'] = child_table_exist
            params['databases'][0]['super_tables'][0]['insert_rows'] = row_num
            params['databases'][0]['super_tables'][0]['childtable_prefix'] = tbname_m
            params['databases'][0]['super_tables'][0]['start_timestamp'] = start_timestamp
            dict = params
        file.close()
        return dict
    def write_json(self, json_path, dict):
        with open(json_path, 'w') as r:
            json.dump(dict, r)
        r.close()
    def data_insert(self,source_taosd_list,dbname,stbname,tbname_m,tb_num,row_num,start_timestamp,drop_flag,child_table_exist_flag,taosBenchmark_fqdn,test_root):
        thread_list = []
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            port = source_taosd_list[source][1]
            self.write_json(f'{test_root}/cases/taosx/basic{source}.json', self.get_json(f'{test_root}/cases/taosx/basic.json',
                            host, int(port), dbname[source], stbname[source], tbname_m[source],tb_num,start_timestamp,row_num,drop_flag,child_table_exist_flag))
            self.remote.put(
                taosBenchmark_fqdn[0], f'{test_root}/cases/taosx/basic{source}.json', f'/tmp/basic{source}')
        for source in range(len(source_taosd_list)):   
            thread_list.append(threading.Thread(target=self.remote.cmd,args=(
                taosBenchmark_fqdn[0], f'taosBenchmark -f /tmp/basic{source}/basic{source}.json')))
            thread_list[source].start()   
        for thread in thread_list:
            thread.join() 
    def run_taosx_db_from_native_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'")))
    def run_taosx_db_from_native_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'")))
    def run_taosx_db_from_ws_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'")))
    def run_taosx_db_from_ws_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'")))

    def run_taosx_stb_from_native_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{tbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'")))
    def run_taosx_stb_from_native_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{tbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'")))
    def run_taosx_stb_from_ws_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'")))
    def run_taosx_stb_from_ws_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'")))
    
    def run_taosx_tb_from_native_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}' -v")))
    def run_taosx_tb_from_native_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}' -v")))
    def run_taosx_tb_from_ws_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}' -v")))
    def run_taosx_tb_from_ws_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}' -v")))
    
    def run_taosx_rename_db_from_native_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,rename_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}' {rename_str}")))
    def run_taosx_rename_db_from_native_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,rename_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}' {rename_str}")))
    def run_taosx_rename_db_from_ws_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,rename_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'\
                                        {rename_str}")))
    def run_taosx_rename_db_from_ws_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,rename_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'\
                                        {rename_str}")))
    def run_taosx_add_tag_from_native_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,add_tag_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}' {add_tag_str}")))
    def run_taosx_add_tag_from_native_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,add_tag_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}' {add_tag_str}")))
    def run_taosx_add_tag_from_ws_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,add_tag_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'\
                                        {add_tag_str}")))
    def run_taosx_add_tag_from_ws_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,target_dbname,source,group_id,timeout,add_tag_str):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'\
                                        {add_tag_str}")))
    
    def run_backup_db_from_native_to_local(self,thread_list,taosx_setting,source_task,target_file_dir,source_taosd_list,dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'local:{target_file_dir}'")))
    
    def run_restore_from_local_to_native(self,thread_list,taosx_setting,target_task,target_file_dir,target_taosd,dbname,source):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'local:{target_file_dir}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{dbname[source]}' -y")))
    
    def run_backup_db_from_ws_to_local(self,thread_list,taosx_setting,source_task,target_file_dir,source_taosd_list,dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'local:{target_file_dir}'")))
    
    def run_restore_from_local_to_ws(self,thread_list,taosx_setting,target_task,target_file_dir,target_taosd,dbname,source):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'local:{target_file_dir}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{dbname[source]}' -y")))
    
    def run_backup_stb_from_native_to_local(self,thread_list,taosx_setting,source_task,target_file_dir,source_taosd_list,dbname,stbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{stbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'local:{target_file_dir}'")))

    def run_backup_stb_from_ws_to_local(self,thread_list,taosx_setting,source_task,target_file_dir,source_taosd_list,dbname,stbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{stbname[source]}?group.id={group_id}&timeout={timeout}'\
                                    -t 'local:{target_file_dir}'")))
    
    def run_backup_tb_from_native_to_local(self,thread_list,taosx_setting,source_task,target_file_dir,source_taosd_list,dbname,tbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'local:{target_file_dir}'")))

    def run_backup_tb_from_ws_to_local(self,thread_list,taosx_setting,source_task,target_file_dir,source_taosd_list,dbname,tbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'local:{target_file_dir}'")))
    