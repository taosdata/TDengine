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
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        self.conn = conn
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000

    def createOldDir(self):        
        path = tdDnodes.dnodes[1].getDnodeRootDir(1)
        print(path)
        tdLog.info("sudo mkdir -p %s/data/vnode/vnode2/wal/old" % path)
        os.system("sudo mkdir -p %s/data/vnode/vnode2/wal/old" % path)       

    def run(self):
        # os.system("rm -rf %s/ " % tdDnodes.getDnodesRootDir())
        tdSql.prepare()

        tdSql.execute("create table st(ts timestamp, speed int)")
        tdSql.execute("insert into st values(now, 1)")
        tdSql.query("select count(*) from st")
        tdSql.checkRows(1)

        
        self.createOldDir()
        tdLog.sleep(10)

        print("force kill taosd")
        os.system("sudo kill -9 $(pgrep -x taosd)")
        os.system("")
        tdDnodes.start(1)

        tdSql.init(self.conn.cursor())
        tdSql.execute("use db")
        tdSql.query("select count(*) from st")
        tdSql.checkRows(1)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
