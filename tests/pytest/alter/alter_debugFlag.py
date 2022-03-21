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
import random
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        
        flagList=["debugflag", "cdebugflag", "tmrDebugFlag", "uDebugFlag", "rpcDebugFlag"]

        for flag in flagList:
            tdSql.execute("alter local %s 131" % flag)
            tdSql.execute("alter local %s 135" % flag)
            tdSql.execute("alter local %s 143" % flag)
            randomFlag = random.randint(100, 250)
            if randomFlag != 131 and randomFlag != 135 and randomFlag != 143:
                tdSql.error("alter local %s %d" % (flag, randomFlag))
        
        tdSql.query("show dnodes")
        dnodeId = tdSql.getData(0, 0)

        for flag in flagList:
            tdSql.execute("alter dnode %d %s 131" % (dnodeId, flag))
            tdSql.execute("alter dnode %d %s 135" % (dnodeId, flag))
            tdSql.execute("alter dnode %d %s 143" % (dnodeId, flag)) 

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase()) 
tdCases.addLinux(__file__, TDTestCase())
