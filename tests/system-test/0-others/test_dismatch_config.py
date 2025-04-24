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

import re
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    updatecfgDict = {'forceReadConfig':'1','timezone':'Asia/Shanghai','arbSetAssignedTimeoutSec':'10','rpcQueueMemoryAllowed':'10485760'}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def update_cfg_success(self):
        tdLog.info("start to update cfg")
        tdDnodes.stop(1)
        tdDnodes.cfg(1, 'timezone', 'UTC')
        tdDnodes.cfg(1, 'arbSetAssignedTimeoutSec', '17')
        tdDnodes.cfg(1, 'rpcQueueMemoryAllowed', '20971520')
        tdDnodes.start(1)
        time.sleep(10)
        # global cfg use values from cluster
        tdSql.query("show dnode 1 variables like 'timezone'")
        tdSql.checkData(0, 2, "Asia/Shanghai (CST, +0800)")
        tdSql.query("show dnode 1 variables like 'arbSetAssignedTimeoutSec'")
        tdSql.checkData(0, 2, "10")

        # dnode local cfg use values from cfg file while forceReadConfig is 1
        tdSql.query("show dnode 1 variables like 'rpcQueueMemoryAllowed'")
        tdSql.checkData(0, 2, "20971520")

        
    def run(self):
        self.update_cfg_success()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
