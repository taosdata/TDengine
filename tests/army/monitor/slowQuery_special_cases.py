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


class TDTestCase(TBase):

    updatecfgDict = {
        "monitorIntervalTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        "monitorInterval": "1",
        "monitorFqdn": "localhost"
        # "monitorLogProtocol": "1"
        # "monitorPort": "6043"
    }
    
    # run
    def run(self):
        tdLog.info(f"check_show_log_threshold")

        monitor_common.init_env()
        # monitor_common.install_taospy()  

        # udf_sleep = os.path.join(sc.getSimPath(), 'tests/army/monitor/udf_sleep.py')
        # sqls = [
        #     f'create aggregate function my_sleep as "{udf_sleep}" outputtype int language "Python"',
        #     'create datab ase check_show_log_threshold',
        #     'create table check_show_log_threshold.assistance (ts timestamp, n int)',
        #     'insert into check_show_log_threshold.assistance values(now, 1), (now + 1s, 2), (now + 2s, 3)'
        # ]
        # tdSql.executes(sqls,queryTimes=1)
        # tdSql.query('select my_sleep(n) from check_show_log_threshold.assistance',queryTimes=1)

        # 1.check nagative value of monitorInterval
        VAR_MONITOR_INTERVAL_NAGATIVE_CASES ={
            '-1': 'Out of range',
            '0': 'Out of range',
            'INVALIDVALUE': 'Invalid configuration value',
            # '001': 'Invalid configuration value',
            '0.1': 'Out of range',
            '1e6': 'Invalid configuration value',
            'NULL': 'Invalid configuration value',
            '86401': 'Out of range',
            'one': 'Invalid configuration value'}
        taos_error_dir = os.path.join(sc.getSimPath(), 'sim', 'dnode_err')
        # remove taos data folder
        if os.path.exists(taos_error_dir):
            shutil.rmtree(taos_error_dir)

        for value, err_info in VAR_MONITOR_INTERVAL_NAGATIVE_CASES.items():
            params = {
                'monitorInterval': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'monitorInterval': value,
                'monitorFqdn': 'localhost'}
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = monitor_common.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'monitorInterval' - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'monitorInterval' - FAIL")

            sql = f"ALTER ALL DNODES  'monitorInterval {value}'"
            tdSql.error(sql, expectErrInfo=err_info)

        # 2. check valid setting of monitorInterval
        VAR_MONITOR_INTERVAL_POSITIVE_CASES = ['10', '86400', '1']

        for threshold_value in VAR_MONITOR_INTERVAL_POSITIVE_CASES:
            updatecfgDict = {"monitorInterval": threshold_value}

            # set monitorInterval via alter operation
            monitor_common.alter_variables(1, updatecfgDict)
            monitor_common.check_variable_setting(key='monitorInterval', value=threshold_value)
            tdLog.info(f"check valid value '{threshold_value}' of variable 'monitorInterval' via alter - PASS")

            # set monitorInterval via taos.cfg
            monitor_common.update_taos_cfg(1, updatecfgDict)
            monitor_common.check_variable_setting(key='monitorInterval', value=threshold_value)
            tdLog.info(f"check valid value '{threshold_value}' of variable 'monitorInterval' via cfg - PASS")

        # 3.config in client is not available
        updatecfgDict = {"monitorIntervalTest": "0", "slowLogScope": "ALL"}
        monitor_common.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"monitorInterval":"10"}
        taosc_cfg = monitor_common.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        monitor_common.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=True, insert_log_exist=True, other_log_exist=True)

        # 4.add node failed if setting is different
        VAR_SHOW_LOG_THRESHOLD_DIFF_CASES =['10086']
        taos_error_dir = os.path.join(sc.getSimPath(), 'dnode_err')
        endpoint = f'{socket.gethostname()}:6630'
        for value in VAR_SHOW_LOG_THRESHOLD_DIFF_CASES:
            params = {
                'monitorInterval': value,
                'fqdn': socket.gethostname(),
                'firstEp': f'{socket.gethostname()}:6030',
                'serverPort': '6630',
                'dataDir': f'{taos_error_dir}/data',
                'logDir': f'{taos_error_dir}/log',
                'monitorIntervalTest': '0',
                'monitorInterval': '1',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            
            monitor_common.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='monitor slow log threshold not match')
        tdLog.info(f"check_show_log_threshold is done")


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
