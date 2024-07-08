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
        "monitorInterval": "1",
        "monitorFqdn": "localhost",
        "monitor": "0"
    }

    # run
    def run(self):
        tdLog.info(f"check_show_log_threshold")

        monitor_common.init_env()

        # 1. session - "monitor": 0, "slowLogScope": "NONE"
        updatecfgDict = {"slowLogScope": "NONE"}
        monitor_common.alter_variables(1, updatecfgDict)
        monitor_common.check_variable_setting(key='monitor', value="0")
        monitor_common.check_variable_setting(key='slowLogScope', value="NONE")
        time.sleep(2)
        monitor_common.check_slow_query_table(db_name='check_setting', query_log_exist=False, insert_log_exist=False, other_log_exist=False)
        tdLog.info(f"check valid value alter - 'monitor': 0, 'slowLogScope': 'NONE' - PASS")

        # 2. session - "monitor": 0, "slowLogScope": "ALL"
        updatecfgDict = {"slowLogScope": "ALL"}
        monitor_common.alter_variables(1, updatecfgDict)
        monitor_common.check_variable_setting(key='monitor', value="0")
        monitor_common.check_variable_setting(key='slowLogScope', value="ALL")
        time.sleep(2)
        monitor_common.check_slow_query_table(db_name='check_setting', query_log_exist=False, insert_log_exist=False, other_log_exist=False)
        tdLog.info(f"check valid value cfg - 'monitor': 0, 'slowLogScope': 'ALL' - PASS")

        # 3. cfg - "monitor": 1, session - "slowLogScope": "NONE"
        updatecfgDict1 = {"monitor": "1", "monitorInterval": "1", "monitorFqdn": "localhost"}
        updatecfgDict2 = {"slowLogScope": "NONE"}
        monitor_common.update_taos_cfg(1, updatecfgDict1)
        monitor_common.alter_variables(1, updatecfgDict2)
        monitor_common.check_variable_setting(key='monitor', value="1")
        monitor_common.check_variable_setting(key='slowLogScope', value="NONE")
        time.sleep(2)
        monitor_common.check_slow_query_table(db_name='check_setting', query_log_exist=False, insert_log_exist=False, other_log_exist=False)
        tdLog.info(f"check valid value cfg - 'monitor': 0, session - 'slowLogScope': 'ALL' - PASS")

        # 4. cfg - "monitor": 1, session - "slowLogScope": "ALL"
        updatecfgDict1 = {"monitor": "1", "monitorInterval": "1", "monitorFqdn": "localhost"}
        updatecfgDict2 = {"slowLogScope": "ALL"}
        monitor_common.update_taos_cfg(1, updatecfgDict1)
        monitor_common.alter_variables(1, updatecfgDict2)
        monitor_common.check_variable_setting(key='monitor', value="1")
        monitor_common.check_variable_setting(key='slowLogScope', value="ALL")
        time.sleep(2)
        monitor_common.check_slow_query_table(db_name='check_setting', query_log_exist=True, insert_log_exist=True, other_log_exist=True)
        tdLog.info(f"check valid value cfg - 'monitor': 0, session - 'slowLogScope': 'ALL' - PASS")


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
