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

import sys
import time
import shutil
import socket
import taos
import frame
import frame.etool
import frame.eutil
import shutil

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.server.dnodes import *
from frame.srvCtl import *
from frame.taosadapter import *


global_count = 0

class Common:
    def __init__(self):
        pass

    def init_env(self):
        tdLog.info(f"check view like.")

        # start taosadapter
        tAdapter.init("")
        tAdapter.deploy()
        tAdapter.start()

        # start taoskeeper
        self.start_taoskeeper()

        # check if taos_slow_sql_detail exists
        count = 0
        while True:
            count = count + 1
            ret = tdSql.getResult('select * from information_schema.ins_stables where stable_name="taos_slow_sql_detail"')
            if len(ret) == 1:
                break
            time.sleep(1)
            if count == 5:
                tdLog.exit('can not find table "taos_slow_sql_detail"')


    def stop_taoskeeper(self):
         # stop taoskeeper
        cmd = 'killall taoskeeper'
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def start_taoskeeper(self):
         # start taoskeeper
        taoskeeper = os.path.join(os.getcwd(), 'monitor/taoskeeper')
        # cmd = f"nohup {taoskeeper} -c {self.cfg_path} > /dev/null & "
        cmd = f"nohup {taoskeeper} > /dev/null & "
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def start_private_taosd(self, cfg: str):
        cmd = f"nohup taosd -c {cfg} > /dev/null & "
        if os.system(cmd) != 0:
            tdLog.exit(cmd)
    
    def stop_private_taosd(self, cfg: str):
        # get process id of taosd
        psCmd = "ps -ef | grep -w taosd | grep 'root' | grep '{0}' | grep -v grep| grep -v defunct | awk '{{print $2}}' | xargs".format(cfg)
        process_id = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
        
        # stop taosd process
        if process_id != '':
            cmd = f"kill -9 {process_id}"
            os.system(cmd)

    def update_taos_cfg(self, idx, updatecfgDict: list):
        # stop dnode
        sc.dnodeStop(idx)

        # update taos.cfg and restart dnode
        sc.setTaosCfg(idx, updatecfgDict)

        # start dnode
        sc.dnodeStart(idx)

        time.sleep(2)

    def alter_variables(self, idx: int = 0, updatecfgDict: dict = None):
        if idx == 0:
            for key, value in updatecfgDict.items():
                sql = f"ALTER ALL DNODES  '{key} {value}'"
                tdSql.execute(sql, show=True)
        else:
            for key, value in updatecfgDict.items():
                sql = f"ALTER DNODE {idx} '{key} {value}'"
                tdSql.execute(sql, show=True)
        
        time.sleep(2)

    # check default value of slowLogScope
    def check_slow_query_table(self, db_name: str, query_log_exist: bool, insert_log_exist: bool, other_log_exist: bool, taosc_cfg: str = None):
        global global_count
        db_name = db_name + str(global_count)
        
        global_count = global_count + 1

        if taosc_cfg:
            eutil.runTaosShellWithSpecialCfg(sql=f'create database {db_name}', cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"create table {db_name}.t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f'insert into {db_name}.ct1 using {db_name}.t100 tags(1) values("2024-05-17 14:58:52.902", "a1", "100")', cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f'insert into {db_name}.ct1 using {db_name}.t100 tags(1) values("2024-05-17 14:58:52.902", "a2", "200")', cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"select * from {db_name}.t100 order by ts", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"select * from {db_name}.t100 order by ts", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"select * from {db_name}.t100 order by ts", cfg=taosc_cfg)
            eutil.runTaosShellWithSpecialCfg(sql=f"alter table {db_name}.t100 add column name varchar(10)", cfg=taosc_cfg)
        else:
            tdSql.execute(f"create database {db_name}", show=True)
            tdSql.execute(f"create table {db_name}.t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)", show=True)
            tdSql.execute(f"insert into {db_name}.ct1 using {db_name}.t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')", show=True)
            tdSql.execute(f"insert into {db_name}.ct1 using {db_name}.t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')", show=True)
            tdSql.query(f"select * from {db_name}.t100 order by ts", show=True)
            tdSql.query(f"select * from {db_name}.t100 order by ts", show=True)
            tdSql.query(f"select * from {db_name}.t100 order by ts", show=True)
            tdSql.execute(f"alter table {db_name}.t100 add column name varchar(10)", show=True)

            tdSql.query("select 1=1")
        time.sleep(2)
        
        # check data in taos_slow_sql_detail
        tdSql.query(f"select * from log.taos_slow_sql_detail where sql like '%{db_name}%' order by start_ts desc")
        row_count = 0
        if query_log_exist:
            row_count = row_count + 3
        if insert_log_exist:
            row_count = row_count + 2
        if other_log_exist:
            row_count = row_count + 3
        tdSql.checkRows(row_count)

        # check query records
        tdSql.query(f"select * from log.taos_slow_sql_detail where sql like '%{db_name}%' and `type`=1")
        if query_log_exist:
            tdSql.checkRows(3)
        else:
            tdSql.checkRows(0)
        
        # check insert records
        tdSql.query(f"select * from log.taos_slow_sql_detail where sql like '%{db_name}%' and `type`=2")
        if insert_log_exist:
            tdSql.checkRows(2)
        else:
            tdSql.checkRows(0)

        # check others records
        tdSql.query(f"select * from log.taos_slow_sql_detail where sql like '%{db_name}%' and `type`=4")
        if other_log_exist:
            tdSql.checkRows(3)
        else:
            tdSql.checkRows(0)

    def failed_to_start_taosd_with_special_cfg(self, cfg: str, expect_err: str):
        if not os.path.exists(cfg):
            tdLog.exit(f'config file does not exist, file: {cfg}')

        try:
            rets = frame.etool.runBinFile("taosd", f" -c {cfg}", timeout=3)
            if frame.etool.checkErrorFromBinFile(ret_list=rets, expect_err=expect_err):
                return True
            return False
        except subprocess.TimeoutExpired as err_msg:
            tdLog.exit(f"start taosd with special config file timeout, config: {cfg}")
        

    def failed_to_create_dnode_with_setting_not_match(self, cfg: str, taos_error_dir: str, endpoint: str, err_msg: str):
        if not os.path.exists(cfg):
            tdLog.exit(f'config file does not exist, file: {cfg}')

        # remove taos data folder
        if os.path.exists(taos_error_dir):
            shutil.rmtree(taos_error_dir)

        # stop taosd with special cfg
        self.stop_private_taosd(cfg)

        # drop old dnode if exists
        ret = tdSql.getResult(f"select id from information_schema.ins_dnodes where endpoint='{endpoint}'")

        if len(ret) == 1:
            node_id = ret[0][0]
            tdSql.execute(f"drop dnode '{node_id}'")

        # start taosd with special cfg
        self.start_private_taosd(cfg)
        
        time.sleep(1)

        # create new dnode
        tdSql.execute(f"create dnode '{endpoint}'")

        # check error msg
        count = 0
        while True:
            count = count + 1
            ret = tdSql.getResult(f"select note from information_schema.ins_dnodes where endpoint='{endpoint}'")
            
            if len(ret) == 1:
                note = ret[0][0]
                if err_msg in note:
                    tdLog.info(f"get the expected err_msg: {err_msg}")
                    break
            
            if count == 3:
                tdLog.exit(f"cannot get the expected err_msg: {err_msg}")
            
    # create private taos.cfg by special setting list
    def create_private_cfg(self, cfg_name: str, params: dict):
        taos_cfg = os.path.join(os.getcwd(), f'monitor/{cfg_name}')
        if os.path.exists(taos_cfg):
            os.remove(taos_cfg)
        
        with open(taos_cfg, 'w') as file:
            for key, value in params.items():
                file.write(f'{key} {value}\n')
        file.close()
        return taos_cfg
    
    def check_variable_setting(self, key: str, value: str):
        value = value.lstrip('0')
        result = tdSql.getResult("show cluster variables")
        for i in range(len(result)):
            if result[i][0].lower() == key.lower() and result[i][1].lower() == value.lower():
                return True
        tdLog.exit(f"check valid value '{value}' of variable '{key}' - FAIL")
      
    def check_maxlen(self, sql_length: str, less_then: bool, taosc_cfg: str= None):
        global global_count
        db_name = 'check_maxlen' + str(global_count)
        global_count = global_count + 1

        length = int(sql_length)
        sqls = [
            f"DROP DATABASE IF EXISTS {db_name}",
            f"CREATE DATABASE IF NOT EXISTS {db_name}",
            f"CREATE TABLE IF NOT EXISTS {db_name}.meters (ts timestamp, col int) tags(t1 int)"
        ]
        tdSql.executes(sqls)

        if length <= 50:
            count = 1
        else:
            quotient = (length - 32) // 20
            remainder = (length - 32) % 20
            
            if less_then:
                count = quotient
            else:
                count = quotient + 2

        sub_sql = ''
        for _ in range(count):
            sub_sql = sub_sql + f'col as {self.generate_random_string(length=11)}, '
        sub_sql = sub_sql[0: -2]

        sql = f'select {sub_sql} from {db_name}.meters'
        original_len = len(sql)
        if taosc_cfg:
            eutil.runTaosShellWithSpecialCfg(sql=sql, cfg=taosc_cfg)
        else:
            tdSql.query(sql)
            tdSql.checkRows(0)
            tdSql.query('select 1=1')
        time.sleep(2)

        sql = f"select CHAR_LENGTH(sql) from log.taos_slow_sql_detail where db='{db_name}'"
        ret = tdSql.getResult(sql)

        if len(ret) == 0:
            tdLog.exit(f'check slowLogMaxLen={sql_length} FAIL')
        actual_len = ret[0][0]
        if int(sql_length) < 51:
            if actual_len != int(sql_length):
                tdLog.exit(f'scenario: actual_sql_length < slowLogMaxLen={sql_length} FAIL')
            tdLog.info(f'scenario: actual_sql_length < slowLogMaxLen={sql_length} PASS')
        else:
            if less_then:
                if actual_len != original_len:
                    tdLog.exit(f'scenario: actual_sql_length < slowLogMaxLen={sql_length} FAIL')
                tdLog.info(f'scenario: actual_sql_length < slowLogMaxLen={sql_length} PASS')
            else:
                if actual_len != int(sql_length):
                    tdLog.exit(f'scenario: actual_sql_length > slowLogMaxLen={sql_length} FAIL')
                tdLog.info(f'scenario: actual_sql_length > slowLogMaxLen={sql_length} PASS')

        
        

    def generate_random_string(self, length):
        characters = string.ascii_letters
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string

monitor_common = Common()