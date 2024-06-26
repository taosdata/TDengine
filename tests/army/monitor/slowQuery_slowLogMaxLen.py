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
        "slowLogThresholdTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        "monitorInterval": "1",
        "slowLogScope": "all",
        "monitorFqdn": "localhost"
        # "monitorLogProtocol": "1"
        # "monitorPort": "6043"
    }

    # run
    def run(self):
        tdLog.info(f"check_show_log_maxlen")

        monitor_common.init_env()

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
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = monitor_common.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'slowLogMaxLen' - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'slowLogMaxLen' - FAIL")

            sql = f"ALTER ALL DNODES  'slowLogMaxLen {value}'"
            tdSql.error(sql, expectErrInfo=err_info)

        # 2. check valid setting of slowLogMaxLen
        # VAR_SHOW_LOG_MAXLEN_POSITIVE_CASES = ['1']
        VAR_SHOW_LOG_MAXLEN_POSITIVE_CASES = ['16384', '200', '002', '1']

        for maxlen_value in VAR_SHOW_LOG_MAXLEN_POSITIVE_CASES:
            updatecfgDict = {"slowLogMaxLen": maxlen_value, "slowLogScope": "QUERY", "monitorInterval": "1"}

            # set slowLogMaxLen via alter operation
            monitor_common.alter_variables(1, updatecfgDict)
            monitor_common.check_variable_setting(key='slowLogMaxLen', value=maxlen_value)
            monitor_common.check_maxlen(sql_length=maxlen_value, less_then=True)
            monitor_common.check_maxlen(sql_length=maxlen_value, less_then=False)
            tdLog.info(f"check valid value '{maxlen_value}' of variable 'slowLogMaxLen' via alter - PASS")

            # set slowLogMaxLen via taos.cfg
            monitor_common.update_taos_cfg(1, updatecfgDict)
            monitor_common.check_variable_setting(key='slowLogMaxLen', value=maxlen_value)
            monitor_common.check_maxlen(sql_length=maxlen_value, less_then=True)
            monitor_common.check_maxlen(sql_length=maxlen_value, less_then=False)
            tdLog.info(f"check valid value '{maxlen_value}' of variable 'slowLogMaxLen' via cfg - PASS")

        # 3.config in client is not available
        updatecfgDict = {"slowLogMaxLen": "80"}
        monitor_common.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"slowLogMaxLen":"100"}
        taosc_cfg = monitor_common.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        # monitor_common.check_maxlen(sql_length=80, less_then=False, taosc_cfg=taosc_cfg)
        monitor_common.check_maxlen(sql_length=80, less_then=False, taosc_cfg=taosc_cfg)

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
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            
            monitor_common.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='slowLogMaxLen not match')
        tdLog.info(f"check_show_log_maxlen is done")


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
