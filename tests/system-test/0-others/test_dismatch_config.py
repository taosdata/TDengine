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
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def update_cfg_err_timezone(self):
        tdDnodes.stop(1)
        tdDnodes.cfg(1, 'forceReadConfig', '1')
        tdDnodes.cfg(1, 'timezone', 'Asia/Shaghai')
        tdDnodes.start(1)
        tdSql.error("show databases")

    def update_cfg_success(self):
        tdDnodes.stop(1)
        tdDnodes.cfg(1, 'forceReadConfig', '1')
        tdDnodes.cfg(1, 'timezone', 'Asia/Shanghai')
        tdDnodes.start(1)
        tdSql.execute("show databases")

        
    def run(self):
        self.update_cfg_err_timezone()
        self.update_cfg_success()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
