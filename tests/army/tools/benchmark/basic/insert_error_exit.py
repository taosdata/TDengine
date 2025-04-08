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
import time
import random

import taos
import frame
import frame.etool
import json
import threading

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.srvCtl import *

class TDTestCase(TBase):
    updatecfgDict = {
        'slowLogScope' : "others"
    }

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        # self.configJsonFile('insert_error_exit.json', 'db', 1, 1, 'splitVgroupByLearner.json', 100000)

    def dnodeNodeStopThread():
        event.wait()
        tdLog.debug("dnodeNodeStopThread start")
        time.sleep(10)
        sc.dnodeStop(2)
        time.sleep(2)
        if !self._rlist:
            self.checkListSting(self._rlist, "")

    def dbInsertThread(self, configFile):
        tdLog.debug(f"dbInsertThread start {configFile}")
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, configFile)
        cmd = f"-f {jfile}"
        self._rlist = self.benchmark(cmd, checkRun=False)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        t1 = threading.Thread(target=self.dbInsertThread, args=('splitVgroupByLearner.json'))
        t2 = threading.Thread(target=self.dnodeNodeStopThread)
        t1.start()
        t2.start()
        tdLog.success(f"{__file__} successfully executed")
        t1.join()
        t2.join()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())