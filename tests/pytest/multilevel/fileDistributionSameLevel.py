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
        self.ntables = 1000             
        self.ts = 1520000010000

        tdDnodes.stop(1)
        # Test1 1 dataDir
        cfg={
            '10' : 'maxVgroupsPerDb',
            '100' : 'maxTablesPerVnode',
            '/mnt/data00 0 1' : 'dataDir',
            '/mnt/data01 0 0' : 'dataDir',
            '/mnt/data02 0 0' : 'dataDir',
            '/mnt/data03 0 0' : 'dataDir',
            '/mnt/data04 0 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data00')
        tdSql.createDir('/mnt/data01')
        tdSql.createDir('/mnt/data02')
        tdSql.createDir('/mnt/data03')
        tdSql.createDir('/mnt/data04')   
        
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)

        tdSql.execute("create database test days 1")
        tdSql.execute("use test")

        tdSql.execute("create table stb(ts timestamp, c int) tags(t int)")

        for i in range(self.ntables):
            tdSql.execute("create table tb%d using stb tags(%d)" %(i, i))            
            tdSql.execute("insert into tb%d values(%d, 1)" % (self.ts + int (i / 100) * 86400000))
        
        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("select * from test.stb")
        tdSql.checkRows(1000)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
