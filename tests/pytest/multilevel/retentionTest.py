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
import os
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
        # Test1 1 dataDir
        cfg={
            '/mnt/data00 0 1' : 'dataDir',
            '/mnt/data01 0 0' : 'dataDir',
            '/mnt/data02 0 0' : 'dataDir',
            '/mnt/data10 1 0' : 'dataDir',
            '/mnt/data11 1 0' : 'dataDir',
            '/mnt/data12 1 0' : 'dataDir',
            '/mnt/data20 2 0' : 'dataDir',
            '/mnt/data21 2 0' : 'dataDir',
            '/mnt/data22 2 0' : 'dataDir'          
        }
        tdSql.createDir('/mnt/data00')
        tdSql.createDir('/mnt/data01')
        tdSql.createDir('/mnt/data02')
        tdSql.createDir('/mnt/data10')
        tdSql.createDir('/mnt/data11')
        tdSql.createDir('/mnt/data12')
        tdSql.createDir('/mnt/data20')
        tdSql.createDir('/mnt/data21')
        tdSql.createDir('/mnt/data22')
        
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdSql.execute("create database test days 1 keep 15,5,10")
        tdSql.execute("use test")

        tdSql.execute("create table tb(ts timestamp, c int)")

        count = 0
        os.system("sudo timedatectl set-ntp false")

        for i in range(self.rowsPerTable):
            tdSql.execute("insert into tb values(now, 1)")
            count += 1
            tdSql.query("select * from tb")
            tdSql.checkRows(count)
            tdDnodes.stop(1)
            os.system("sudo date -s $(date -d \"${DATE} 1 day\" \"+%Y%m%d\")")
            tdDnodes.start(1)

    def stop(self):
        os.system("sudo timedatectl set-ntp true")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
