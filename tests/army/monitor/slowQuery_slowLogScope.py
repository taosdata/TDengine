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

class TDTestCase(TBase):

    updatecfgDict = {
        "slowLogThresholdTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        # "monitorInterval": "1",
        "monitorFqdn": "localhost"
        # "monitorLogProtocol": "1"
        # "monitorPort": "6043"
    }


    def init_env(self):
        tdLog.info(f"check view like.")

        # start taosadapter
        tAdapter.init("")
        tAdapter.deploy()
        tAdapter.start()

        # start taoskeeper
        self.start_taoskeeper()

        # stop taoskeeper
        # self.stop_taoskeeper()

    
    def demo(self):
        updatecfgDict = {
            "slowLogScope":"ALL"   
        }

        # update taos.cfg and restart dnode
        self.update_taos_cfg(1, updatecfgDict)

        # alter dnode's variable
        self.alter_variables(1, updatecfgDict)

        # alter all dnode's variable
        self.alter_variables(updatecfgDict)

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
        time.sleep(5)
        
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
                note = ret['data'][0][0]
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
        # tdSql.execute(f"select value from information_schema.ins_configs where name='{key}'")
        result = tdSql.getResult("show cluster variables")
        for i in range(len(result)):
            if result[i][0].lower() == key.lower() and result[i][1].lower() == value.lower():
                return True
        return False

    def test_show_log_scope(self):
        tdLog.info(f"check_show_log_scope")

        # 1.check nagative value of show_log_scope
        VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES ={
            'INVALIDVALUE': 'Invalid slowLog scope value',
            'ALL|INSERT1': 'Invalid slowLog scope value',
            'QUERY,Insert,OTHERS': 'Invalid slowLog scope value',
            'NULL': 'Invalid slowLog scope value',
            '100': 'Invalid slowLog scope value'}
            # '': 'Invalid slowLog scope value'}
        # VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES =['INVALIDVALUE','ALL|INSERT1','QUERY,Insert,OTHERS','','NULL','100']
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')

        for value, err_info in VAR_SHOW_LOG_SCOPE_NAGATIVE_CASES.items():
            params = {
                'slowLogScope': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = self.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'slowLogScope' via config - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'slowLogScope' via config - FAIL")

            sql = f"ALTER ALL DNODES  'slowLogScope {value}'"
            tdSql.error(sql, expectErrInfo=err_info)

        # 2. check valid setting of show_log_scope
        VAR_SHOW_LOG_SCOPE_POSITIVE_CASES = {
            'ALL': {'query_log_exist': True, 'insert_log_exist': True, 'other_log_exist': True},
            'QUERY': {'query_log_exist': True, 'insert_log_exist': False, 'other_log_exist': False},
            'INSERT': {'query_log_exist': False, 'insert_log_exist': True, 'other_log_exist': False},
            'OTHERS': {'query_log_exist': False, 'insert_log_exist': False, 'other_log_exist': True},
            'NONE': {'query_log_exist': False, 'insert_log_exist': False, 'other_log_exist': False},
            'ALL|Query|INSERT|OTHERS|NONE': {'query_log_exist': True, 'insert_log_exist': True, 'other_log_exist': True},
            'QUERY|Insert|OTHERS': {'query_log_exist': True, 'insert_log_exist': True, 'other_log_exist': True},
            'INSERT|OThers': {'query_log_exist': False, 'insert_log_exist': True, 'other_log_exist': True},
            'QUERY|none': {'query_log_exist': True, 'insert_log_exist': False, 'other_log_exist': False}}

        for scope_value, verifications in VAR_SHOW_LOG_SCOPE_POSITIVE_CASES.items():
            updatecfgDict = {"slowLogScope": scope_value, 'monitorInterval': '1', "slowLogThresholdTest":"0"}

            # set slowLogScope via alter operation
            self.alter_variables(1, updatecfgDict)
            self.check_variable_setting(key='slowLogScope', value=scope_value)
            self.check_slow_query_table(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])

            # set slowLogScope via taos.cfg
            self.update_taos_cfg(1, updatecfgDict)
            self.check_variable_setting(key='slowLogScope', value=scope_value)
            self.check_slow_query_table(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])

            
        # 3.config in client is not available
        updatecfgDict = {"slowLogScope": "INSERT", 'monitorInterval': '1', "slowLogThresholdTest":"0"}
        self.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"slowLogScope":"QUERY", 'monitorInterval': '1', "slowLogThresholdTest":"0", "debugFlag":"135"}
        taosc_cfg = self.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        self.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=False, insert_log_exist=True, other_log_exist=False)

        # 4.add node failed if setting is different
        VAR_SHOW_LOG_SCOPE_DIFF_CASES =['ALL','ALL|INSERT','QUERY','OTHERS','NONE']
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')
        endpoint = f'{socket.gethostname()}:6630'
        for value in VAR_SHOW_LOG_SCOPE_DIFF_CASES:
            params = {
                'slowLogScope': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            self.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='slowLogScope not match')
        tdLog.info(f"check_show_log_scope is done")

    def test_show_log_threshold(self):
        tdLog.info(f"check_show_log_threshold")

        # 1.check nagative value of slowLogThreshold
        VAR_SHOW_LOG_THRESHOLD_NAGATIVE_CASES ={
            '-1': 'Out of range',
            '0': 'Out of range',
            'INVALIDVALUE': 'Invalid configuration value',
            # '001': 'Invalid configuration value',
            '0.1': 'Out of range',
            '1e6': 'Invalid configuration value',
            'NULL': 'Invalid configuration value',
            '2147483648': 'Out of range',
            'one': 'Invalid configuration value'}
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')

        for value, err_info in VAR_SHOW_LOG_THRESHOLD_NAGATIVE_CASES.items():
            params = {
                'slowLogThreshold': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = self.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'slowLogThreshold' - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'slowLogThreshold' - FAIL")

            sql = f"ALTER ALL DNODES  'slowLogThreshold {value}'"
            tdSql.error(sql, expectErrInfo=err_info)

        # 2. check valid setting of show_log_scope
        VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES = ['2147483647', '1']

        for threshold_value in VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES:
            updatecfgDict = {"slowLogThreshold": threshold_value}

            # set slowLogThreshold via alter operation
            self.alter_variables(1, updatecfgDict)
            self.check_variable_setting(key='slowLogThreshold', value=threshold_value)

            # set slowLogThreshold via taos.cfg
            self.update_taos_cfg(1, updatecfgDict)
            self.check_variable_setting(key='slowLogThreshold', value=threshold_value)

        # 3.config in client is not available
        updatecfgDict = {"slowLogThresholdTest": "1"}
        self.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"slowLogThreshold":"10"}
        taosc_cfg = self.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        self.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=False, insert_log_exist=True, other_log_exist=False)

        # 4.add node failed if setting is different
        VAR_SHOW_LOG_THRESHOLD_DIFF_CASES =['10086']
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')
        endpoint = f'{socket.gethostname()}:6630'
        for value in VAR_SHOW_LOG_THRESHOLD_DIFF_CASES:
            params = {
                'slowLogThreshold': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            
            self.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='slowLogThreshold not match')
        tdLog.info(f"check_show_log_threshold is done")

    def test_show_log_maxlen(self):
        tdLog.info(f"check_show_log_maxlen")

        # 1.check nagative value of slowLogMaxLen
        VAR_SHOW_LOG_MAXLEN_NAGATIVE_CASES ={
            '-1': 'Out of range',
            '0': 'Out of range',
            'INVALIDVALUE': 'Invalid configuration value',
            # '001': 'Invalid configuration value',
            '0.1': 'Out of range',
            '1e6': 'Invalid configuration value',
            'NULL': 'Invalid configuration value',
            '16385': 'Out of range',
            'one': 'Invalid configuration value'}
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')

        for value, err_info in VAR_SHOW_LOG_MAXLEN_NAGATIVE_CASES.items():
            params = {
                'slowLogMaxLen': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = self.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'slowLogMaxLen' - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'slowLogMaxLen' - FAIL")

            sql = f"ALTER ALL DNODES  'slowLogMaxLen {value}'"
            tdSql.error(sql, expectErrInfo=err_info)

        # 2. check valid setting of slowLogMaxLen
        VAR_SHOW_LOG_MAXLEN_POSITIVE_CASES = ['16384', '1']

        for maxlen_value in VAR_SHOW_LOG_MAXLEN_POSITIVE_CASES:
            updatecfgDict = {"slowLogMaxLen": maxlen_value, "slowLogScope": "QUERY", "monitorInterval": "1"}

            # set slowLogMaxLen via alter operation
            self.alter_variables(1, updatecfgDict)
            self.check_variable_setting(key='slowLogMaxLen', value=maxlen_value)
            self.check_maxlen(sql_length=maxlen_value, less_then=True, is_record=True)
            self.check_maxlen(sql_length=maxlen_value, more_then=True, is_record=False)

            # set slowLogMaxLen via taos.cfg
            self.update_taos_cfg(1, updatecfgDict)
            self.check_variable_setting(key='slowLogMaxLen', value=maxlen_value)
            self.check_maxlen(sql_length=maxlen_value, less_then=True, is_record=True)
            self.check_maxlen(sql_length=maxlen_value, more_then=True, is_record=False)

        # 3.config in client is not available
        updatecfgDict = {"slowLogMaxLen": "1"}
        self.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"slowLogMaxLen":"10"}
        taosc_cfg = self.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        self.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=False, insert_log_exist=True, other_log_exist=False)

        # 4.add node failed if setting is different
        VAR_SHOW_LOG_THRESHOLD_DIFF_CASES =['10086']
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')
        endpoint = f'{socket.gethostname()}:6630'
        for value in VAR_SHOW_LOG_THRESHOLD_DIFF_CASES:
            params = {
                'slowLogMaxLen': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'slowLogThresholdTest': '0',
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = self.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            
            self.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='slowLogMaxLen not match')
        tdLog.info(f"check_show_log_maxlen is done")
      
    def check_maxlen(self, sql_length: str, less_then: bool, is_record: bool):
        length = int(sql_length)
        db_name = 'check_maxlen' + str(global_count)
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
                count = quotient + 1

        sub_sql = ''
        for _ in range(count):
            sub_sql = sub_sql + f'col as {self.generate_random_string(length=11)}, '
        sub_sql = sub_sql[0: -2]

        sql = f'select {sub_sql} from {db_name}.meters'
        tdSql.execute(sql)
        tdSql.checkRows(0)

        sql = f"select * from log.taos_slow_sql_detail where db='{db_name}' and sql like '%from {db_name}.meter%'"
        tdSql.execute(sql)
        if is_record:
            tdSql.checkRows(1)
        else:
            tdSql.checkRows(0)
        
    def test_smoke_testing(self):
        # check default value
        VAR_SHOW_LOG_DEFAULT_VALUE_CASES = {
            'slowLogScope': 'QUERY',
            'slowLogThreshold': '10',
            'slowLogMaxLen': '4096',
            'monitorInterval': '30',
        }
        for key, value in VAR_SHOW_LOG_DEFAULT_VALUE_CASES.items():
            if self.check_variable_setting(key=key, value=value):
                tdLog.info(f"check default value '{value}' of variable '{key}' - PASS" )
            else:
                tdLog.exit(f"check default value '{value}' of variable '{key}' - FAIL" )

        # check basic slow query
        updatecfgDict = {"slowLogScope":"ALL", "slowLogThresholdTest":"0", "monitorInterval":"1"}
        self.alter_variables(updatecfgDict=updatecfgDict)
        self.check_slow_query_table(db_name='smoke_testing', query_log_exist=True, insert_log_exist=True, other_log_exist=True)

        # check sub-table name manually
        

    def generate_random_string(self, length):
        characters = string.ascii_letters
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string
    
    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        
        self.init_env()

        updatecfgDict = {"monitorInterval": "1"}
        self.update_taos_cfg(1, updatecfgDict)


        # self.test_smoke_testing()

        # self.check_maxlen(length=150, less_then=True, is_record=True)

        # self.test_show_log_scope()

        # self.test_show_log_threshold()

        self.test_show_log_maxlen()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
