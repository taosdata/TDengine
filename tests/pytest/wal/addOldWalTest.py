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
import os
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
    
    def createOldDir(self):
        oldDir = tdDnodes.getDnodesRootDir() + "dnode1/data/vnode/vnode2/wal/old"
        os.system("sudo mkdir -p %s" % oldDir)
    
    def createOldDirAndAddWal(self):
        oldDir = tdDnodes.getDnodesRootDir() + "dnode1/data/vnode/vnode2/wal/old"        
        os.system("sudo echo 'test' >> %s/wal" % oldDir)


    def run(self):
        tdSql.prepare()

        tdSql.execute("create table t1(ts timestamp, a int)") 
        tdSql.execute("insert into t1 values(now, 1)")

        # create old dir only
        self.createOldDir()
        os.system("sudo kill -9 $(pgrep taosd)")
        tdDnodes.start(1)

        tdSql.execute("use db")
        tdSql.query("select * from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        # create old dir and add wal under old dir
        self.createOldDir()
        self.createOldDirAndAddWal()        
        os.system("sudo kill -9 $(pgrep taosd)")
        tdDnodes.start(1)

        tdSql.query("select * from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
