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
        self.ntables = 10
        self.rowsPerTable = 10
        self.startTime = 1520000010000

        tdDnodes.stop(1)
        cfg={
            'dataDir': '/mnt/data1 0 0',
            'dataDir': '/mnt/data2 0 0',
            'dataDir': '/mnt/data3 0 0',
            'dataDir': '/mnt/data4 0 0',
            'dataDir': '/mnt/data5 0 0',
            'dataDir': '/mnt/data6 0 0',
            'dataDir': '/mnt/data7 0 0',
            'dataDir': '/mnt/data8 0 0',
            'dataDir': '/mnt/data9 0 0',
            'dataDir': '/mnt/data10 0 0',
            'dataDir': '/mnt/data11 0 0',
            'dataDir': '/mnt/data12 0 0',
            'dataDir': '/mnt/data13 0 0',
            'dataDir': '/mnt/data14 0 0',
            'dataDir': '/mnt/data15 0 0',
            'dataDir': '/mnt/data16 0 0',
            'dataDir': '/mnt/data17 0 0'
        }
        tdSql.createDir('/mnt/data1')
        tdSql.createDir('/mnt/data2')
        tdSql.createDir('/mnt/data3')
        tdSql.createDir('/mnt/data4')
        tdSql.createDir('/mnt/data5')
        tdSql.createDir('/mnt/data6')
        tdSql.createDir('/mnt/data7')
        tdSql.createDir('/mnt/data8')
        tdSql.createDir('/mnt/data9')
        tdSql.createDir('/mnt/data10')
        tdSql.createDir('/mnt/data11')
        tdSql.createDir('/mnt/data12')
        tdSql.createDir('/mnt/data13')
        tdSql.createDir('/mnt/data14')
        tdSql.createDir('/mnt/data15')
        tdSql.createDir('/mnt/data16')
        tdSql.createDir('/mnt/data17')
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdSql.taosdStatus(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
