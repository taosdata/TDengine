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
import signal
import psutil

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
        tdLog.info(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        self._rlist = None 

    def get_pids_by_name(self, process_name):
        pids = []
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == process_name:
                pids.append(proc.pid)
        return pids

    def stopThread(self, isForceExit):
        tdLog.info("dnodeNodeStopThread start")
        time.sleep(10)
        pids = self.get_pids_by_name("taosBenchmark")
        if pids:
            tdLog.info(f"Find a process named taosBbenchmark with PID: {pids}")
        else:
            tdLog.exit("No process named taosBbenchmark was found.")

        os.kill(pids[0], signal.SIGINT)


        time.sleep(10)
        
        if self._rlist:
            tdLog.info(self._rlist)   
            self.checkListString(self._rlist, "Receive SIGINT or other signal, quit benchmark")
        else:
            tdLog.exit("The benchmark process has not stopped!")


    def dbInsert(self):
        tdLog.info(f"dbInsert start")
        # taosBenchmark run
        cmd = "-t 1000 -n 1000 -T 4 -I stmt -y"
        self.benchmark(cmd, checkRun=True)

    def dbTmqThread(self):
        binPath = etool.benchMarkFile()
        cmd = "-f ./tools/benchmark/basic/json/tmq_cancel.json"
        self._rlist = self.benchmark(cmd, checkRun=False)
        tdLog.info(self._rlist)

    # run
    def run(self):
        tdLog.info(f"start to excute {__file__}")
        tdSql.execute("drop topic if exists topic_benchmark_meters")
        self.dbInsert()
        tdLog.info(f"dbInsert finish！")
        

        t1 = threading.Thread(target=self.dbTmqThread)
        t2 = threading.Thread(target=self.stopThread, args=(False,))
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