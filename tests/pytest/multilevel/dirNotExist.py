###################################################################
#       Copyright (c) 2016 by TAOS Technologies, Inc.
#             All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        cfg={
            '/mnt/data1 0 0' : 'dataDir',
            '/mnt/data2 0 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        os.system('rm -rf /mnt/data2')
        

        tdLog.info("================= step1")
        tdDnodes.stop(1)
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdLog.info("================= step2")
        tdSql.taosdStatus(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
