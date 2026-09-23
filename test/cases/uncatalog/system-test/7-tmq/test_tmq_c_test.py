
# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom
from taos.tmq import *

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_c(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: common,ci,integration,functional
        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_write_raw_test'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("error: tmq_write_raw_test failed")
        tdSql.query("select * from db_taosx.ct0")
        tdSql.checkRows(2)

        cmdStr = '%s/build/bin/tmq_ts5776'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("error: tmq_ts5776 failed")
        tdSql.query("select * from db_dst.st1")
        tdSql.checkRows(10)

        cmdStr = '%s/build/bin/tmq_td33798'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("error: tmq_td33798 failed")

        cmdStr = '%s/build/bin/tmq_poll_test'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("error: tmq_poll_test failed")



