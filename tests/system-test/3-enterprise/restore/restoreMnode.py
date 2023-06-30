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

from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
sys.path.append("./3-enterprise/restore")
from restoreBasic import *



class TDTestCase:
    # init
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        self.basic = RestoreBasic()
        self.basic.init(conn, logSql, replicaVar)
    
    # run
    def run(self):
        self.basic.restore_mnode(3)

    # stop
    def stop(self):
        self.basic.stop()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
