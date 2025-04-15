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
    updatecfgDict = {'forceReadConfig':'1','timezone':'Asia/Shanghai','arbSetAssignedTimeoutSec':'17'}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def update_cfg_err_timezone(self):
        tdDnodes.stop(1)
        tdDnodes.cfg(1, 'forceReadConfig', '1')
        tdDnodes.cfg(1, 'timezone', 'Asia/Shaghai')
        tdDnodes.cfg(1, 'arbSetAssignedTimeoutSec', '17')
        tdDnodes.start(1)
        time.sleep(10)
        tdSql.error("show databases")

    def update_cfg_success(self):
        tdDnodes.stop(1)
        tdDnodes.cfg(1, 'forceReadConfig', '1')
        tdDnodes.cfg(1, 'timezone', 'Asia/Shanghai')
        tdDnodes.cfg(1, 'arbSetAssignedTimeoutSec', '17')
        tdDnodes.start(1)
        time.sleep(10)
        tdSql.execute("show databases")
        tdSql.query("show dnode 1 variables like 'arbSetAssignedTimeoutSec'")
        tdSql.checkData(0, 2, 17)

        
    def run(self):
        self.update_cfg_err_timezone()
        self.update_cfg_success()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
