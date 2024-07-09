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
from monitor.common import *

global_slowLogThreshold_count = 0

class TDTestCase(TBase):

    updatecfgDict = {
        # "slowLogThresholdTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        "monitorInterval": "1",
        "monitorFqdn": "localhost"
        # "monitorLogProtocol": "1"
        # "monitorPort": "6043"
    }
    
    def check_slowLogThreshold_setting(self, db_name, threshold_value: str):
        global global_slowLogThreshold_count
        db_name = db_name + str(global_slowLogThreshold_count)
        
        global_slowLogThreshold_count = global_slowLogThreshold_count + 1

        # monitor_common.install_taospy()
        
        udf_sleep = os.path.join(sc.getSimPath(), 'tests/army/monitor/udf_sleep.py')
        sqls = [
            f'create aggregate function if not exists my_sleep as "{udf_sleep}" outputtype int bufsize 10240 language "Python"',
            f'create database {db_name}',
            f'create table {db_name}.assistance (ts timestamp, n int)',
            f'insert into {db_name}.assistance values(now, 1), (now + 1s, 2), (now + 2s, 4), (now + 3s, 6)'
        ]
        tdSql.executes(sqls,queryTimes=1)
    
        if threshold_value == "1":
            tdSql.query(f'select my_sleep(n) from {db_name}.assistance where n=1', queryTimes=1)
            tdSql.query(f'select my_sleep(n) from {db_name}.assistance where n=2', queryTimes=1)
            tdSql.query(f'select 1=1', queryTimes=1)
            time.sleep(2)
            tdSql.query(f"select * from log.taos_slow_sql_detail where db ='{db_name}' and type=1", queryTimes=1)
            tdSql.checkRows(2)
        
        if threshold_value == "005":
            tdSql.query(f'select my_sleep(n) from {db_name}.assistance where n=4', queryTimes=1)
            tdSql.query(f'select my_sleep(n) from {db_name}.assistance where n=6', queryTimes=1)
            tdSql.query(f'select 1=1', queryTimes=1)
            time.sleep(2)
            tdSql.query(f"select * from log.taos_slow_sql_detail where db ='{db_name}' and type=1", queryTimes=1)
            tdSql.checkRows(1)

    # run
    def run(self):
        tdLog.info(f"check_show_log_threshold")

        monitor_common.init_env()  

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
        taos_error_dir = os.path.join(sc.getSimPath(), 'sim', 'dnode_err')
        # remove taos data folder
        if os.path.exists(taos_error_dir):
            shutil.rmtree(taos_error_dir)

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
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = monitor_common.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'slowLogThreshold' - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'slowLogThreshold' - FAIL")

            sql = f"ALTER ALL DNODES  'slowLogThreshold {value}'"
            tdSql.error(sql, expectErrInfo=err_info)

        # 2. check valid setting of show_log_scope
        VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES = ['2147483647', '005', '1']
        # VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES = ['002']

        for threshold_value in VAR_SHOW_LOG_THRESHOLD_POSITIVE_CASES:
            updatecfgDict = {"slowLogThreshold": threshold_value}

            # set slowLogThreshold via alter operation
            monitor_common.alter_variables(1, updatecfgDict)
            monitor_common.check_variable_setting(key='slowLogThreshold', value=threshold_value)
            if threshold_value != '2147483647':
                self.check_slowLogThreshold_setting(db_name='threshold_value_test', threshold_value=threshold_value)
            tdLog.info(f"check valid value '{threshold_value}' of variable 'slowLogThreshold' via alter - PASS")

            # set slowLogThreshold via taos.cfg
            monitor_common.update_taos_cfg(1, updatecfgDict)
            monitor_common.check_variable_setting(key='slowLogThreshold', value=threshold_value)
            if threshold_value != '2147483647':
                self.check_slowLogThreshold_setting(db_name='threshold_value_test', threshold_value=threshold_value)
            tdLog.info(f"check valid value '{threshold_value}' of variable 'slowLogThreshold' via cfg - PASS")

        # 3.config in client is not available
        updatecfgDict = {"slowLogThresholdTest": "0", "slowLogScope": "ALL"}
        monitor_common.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"slowLogThreshold":"10"}
        taosc_cfg = monitor_common.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        monitor_common.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=True, insert_log_exist=True, other_log_exist=True)

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
                'monitorInterval': '1',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            
            monitor_common.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='monitor slow log threshold not match')
        tdLog.info(f"check_show_log_threshold is done")


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
