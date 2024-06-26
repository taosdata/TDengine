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
from monitor.common import *

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.server.dnodes import *
from frame.srvCtl import *
from frame.taosadapter import *


class TDTestCase(TBase):

    updatecfgDict = {
        "slowLogThresholdTest": "0",  # special setting only for testing
        "slowLogExceptDb": "log",
        "monitor": "1",
        "monitorLogProtocol": "1",
        "monitorFqdn": "localhost"
    }
    
    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        monitor_common.init_env()

        # check default value
        VAR_SHOW_LOG_DEFAULT_VALUE_CASES = {
            'slowLogScope': 'QUERY',
            'slowLogThreshold': '10',
            'slowLogMaxLen': '4096',
            'monitorInterval': '30',
        }
        for key, value in VAR_SHOW_LOG_DEFAULT_VALUE_CASES.items():
            if monitor_common.check_variable_setting(key=key, value=value):
                tdLog.info(f"check default value '{value}' of variable '{key}' - PASS" )
            else:
                tdLog.exit(f"check default value '{value}' of variable '{key}' - FAIL" )
        
        # check basic slow query
        # updatecfgDict = {"slowLogScope":"ALL", "slowLogThresholdTest":"0", "monitorInterval":"1"}
        # monitor_common.update_taos_cfg(idx=1, updatecfgDict=updatecfgDict)
        # monitor_common.check_slow_query_table(db_name='smoke_testing', query_log_exist=True, insert_log_exist=True, other_log_exist=True)


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
