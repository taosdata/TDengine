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
        "monitorFqdn": "localhost"
        # "monitorLogProtocol": "1"
        # "monitorPort": "6043"
    }

    # run
    def run(self):
        tdLog.info(f"check_show_log_scope")

        monitor_common.init_env()

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
                'monitorInterval': '5',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='invalid_taos.cfg', params=params)
            check_result = monitor_common.failed_to_start_taosd_with_special_cfg(cfg=new_taos_cfg, expect_err=err_info)
            if check_result:
                tdLog.info(f"check invalid value '{value}' of variable 'slowLogScope' via config - PASS")
            else:
                tdLog.exit(f"check invalid value '{value}' of variable 'slowLogScope' via config - FAIL")

            sql = f"ALTER ALL DNODES  'slowLogScope {value}'"
            tdSql.error(sql, expectErrInfo="Invalid config option")

        # sql = f"ALTER ALL DNODES  'slowLogScope '"
        # tdSql.error(sql, expectErrInfo="Invalid config option")

        # 2. check valid setting of show_log_scope
        VAR_SHOW_LOG_SCOPE_POSITIVE_CASES = {
            'ALL': {'expected_setting': 'Query|INSERT|OTHERS', 'query_log_exist': True, 'insert_log_exist': True, 'other_log_exist': True},
            'QUERY': {'expected_setting': 'Query', 'query_log_exist': True, 'insert_log_exist': False, 'other_log_exist': False},
            'INSERT': {'expected_setting': 'INSERT', 'query_log_exist': False, 'insert_log_exist': True, 'other_log_exist': False},
            'OTHERS': {'expected_setting': 'OTHERS', 'query_log_exist': False, 'insert_log_exist': False, 'other_log_exist': True},
            'NONE': {'expected_setting': 'NONE', 'query_log_exist': False, 'insert_log_exist': False, 'other_log_exist': False},
            'ALL|Query|INSERT|OTHERS|NONE': {'expected_setting': 'Query|INSERT|OTHERS', 'query_log_exist': True, 'insert_log_exist': True, 'other_log_exist': True},
            'QUERY|Insert|OTHERS': {'expected_setting': 'Query|INSERT|OTHERS', 'query_log_exist': True, 'insert_log_exist': True, 'other_log_exist': True},
            'INSERT|OThers': {'expected_setting': 'INSERT|OTHERS', 'query_log_exist': False, 'insert_log_exist': True, 'other_log_exist': True},
            'QUERY|none': {'expected_setting': 'Query', 'query_log_exist': True, 'insert_log_exist': False, 'other_log_exist': False}}

        for scope_value, verifications in VAR_SHOW_LOG_SCOPE_POSITIVE_CASES.items():
            updatecfgDict = {"slowLogScope": scope_value}

            # set slowLogScope via alter operation
            monitor_common.alter_variables(1, updatecfgDict)
            monitor_common.check_variable_setting(key='slowLogScope', value=verifications['expected_setting'])
            monitor_common.check_slow_query_table(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])
            tdLog.info(f"check valid value '{scope_value}' of variable 'slowLogMaxLen' via alter - PASS")

            # set slowLogScope via taos.cfg
            monitor_common.update_taos_cfg(1, updatecfgDict)
            monitor_common.check_variable_setting(key='slowLogScope', value=verifications['expected_setting'])
            monitor_common.check_slow_query_table(db_name='check_default', query_log_exist=verifications['query_log_exist'], insert_log_exist=verifications['insert_log_exist'], other_log_exist=verifications['other_log_exist'])
            tdLog.info(f"check valid value '{scope_value}' of variable 'slowLogMaxLen' via cfg - PASS")
            
        # 3.config in client is not available
        updatecfgDict = {"slowLogScope": "INSERT"}
        monitor_common.update_taos_cfg(1, updatecfgDict)

        updatecfgDict = {"slowLogScope":"QUERY"}
        taosc_cfg = monitor_common.create_private_cfg(cfg_name='taosc.cfg', params=updatecfgDict)
        monitor_common.check_slow_query_table(db_name='check_setting_in_client', taosc_cfg=taosc_cfg, query_log_exist=False, insert_log_exist=True, other_log_exist=False)

        # 4.add node failed if setting is different
        VAR_SHOW_LOG_SCOPE_DIFF_CASES =['ALL','ALL|INSERT','QUERY','OTHERS','NONE']
        # VAR_SHOW_LOG_SCOPE_DIFF_CASES =['ALL','QUERY','OTHERS','NONE']
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
                'monitorInterval': '1',
                'monitorFqdn': 'localhost'}
            new_taos_cfg = monitor_common.create_private_cfg(cfg_name='diff_taos.cfg', params=params)
            monitor_common.failed_to_create_dnode_with_setting_not_match(cfg=new_taos_cfg, taos_error_dir=taos_error_dir, endpoint=endpoint, err_msg='monitor slow log scopenot match')
        tdLog.info(f"check_show_log_scope is done")

        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
