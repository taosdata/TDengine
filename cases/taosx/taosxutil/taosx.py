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




import threading
from taostest.util.remote import Remote

class Runtaosx():
    def __init__(self,logger):
        self.logger = logger
        self.remote: Remote = Remote(self.logger)
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
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'")))
    def run_taosx_tb_from_native_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'")))
    def run_taosx_tb_from_ws_to_native(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])}/{target_dbname}'")))
    def run_taosx_tb_from_ws_to_ws(self,thread_list,taosx_setting,source_task,target_task,source_taosd_list,target_taosd,dbname,tbname,target_dbname,source,group_id,timeout):
        thread_list.append(threading.Thread(target=self.remote.cmd, args=(
                                taosx_setting['fqdn'][0], f"taosx run \
                                    -f 'tmq{source_task}://root:taosdata@{source_taosd_list[source][0]}:{int(source_taosd_list[source][1])+11}/{dbname[source]}.{tbname[source]}0?group.id={group_id}&timeout={timeout}'\
                                    -t 'taos{target_task}://root:taosdata@{target_taosd[0]}:{int(target_taosd[1])+11}/{target_dbname}'")))