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

from new_test_framework.utils import tdLog, tdSql, etool, sc
import os
import time
import threading

class TestInsertErrorExit:
    updatecfgDict = {
        'slowLogScope' : "others"
    }

    def init(self, conn, logSql, replicaVar=1):
        tdLog.info(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        self._rlist = None 
        # self.configJsonFile('insert_error_exit.json', 'db', 1, 1, 'splitVgroupByLearner.json', 100000)

    def dnodeNodeStopThread(self):
        tdLog.info("dnodeNodeStopThread start")
        time.sleep(10)
        sc.dnodeStop(2)
        time.sleep(10)
        if self._rlist:
            tdLog.info(self._rlist)
            self.checkListString(self._rlist, "failed to execute insert statement. reason: Vnode stopped")
        else:
            tdLog.exit("The benchmark process has not stopped!")


    def dbInsertThread(self):
        tdLog.info(f"dbInsertThread start")
        # taosBenchmark run
        cmd = "-d db -t 10000 -n 10000 -T 4 -I stmt -y"
        self._rlist = self.benchmark(cmd, checkRun=False)

    # run
    def test_insert_error_exit(self):
        tdLog.info(f"start to excute {__file__}")
        t1 = threading.Thread(target=self.dbInsertThread)
        t2 = threading.Thread(target=self.dnodeNodeStopThread)
        t1.start()
        t2.start()
        tdLog.success(f"{__file__} successfully executed")
        t1.join()
        t2.join()

        tdLog.success(f"{__file__} successfully executed")


