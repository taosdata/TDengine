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
import signal
import psutil

class TestBenchmarkExcept:
    updatecfgDict = {
        'slowLogScope' : "others"
    }

    def setup_class(cls):
        tdLog.info(f"start to init {__file__}")
        cls._rlist = None

    #
    # ------------------- test_insert_cancel.py ----------------
    #
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
        if isForceExit:
            os.kill(pids[0], signal.SIGINT)

        time.sleep(10)
        
        if self._rlist:
            tdLog.info(self._rlist)
            if isForceExit:
                self.checkListString(self._rlist, "Benchmark process forced exit!")
            else:    
                self.checkListString(self._rlist, "Receive SIGINT or other signal, quit benchmark")
        else:
            tdLog.exit("The benchmark process has not stopped!")


    def dbInsertThread(self):
        tdLog.info(f"dbInsertThread start")
        # taosBenchmark run
        cmd = "-d db -t 10000 -n 10000 -T 8 -I stmt -y"
        self._rlist = self.benchmark(cmd, checkRun=False)

    # run
    def do_insert_cancel(self):
        tdLog.info(f"start to excute {__file__}")
        t1 = threading.Thread(target=self.dbInsertThread)
        t2 = threading.Thread(target=self.stopThread, args=(False,))
        t1.start()
        t2.start()
        tdLog.success(f"{__file__} successfully executed")
        t1.join()
        t2.join()

        t1 = threading.Thread(target=self.dbInsertThread)
        t2 = threading.Thread(target=self.stopThread, args=(True,))
        t1.start()
        t2.start()
        tdLog.success(f"{__file__} successfully executed")
        t1.join()
        t2.join()

        print("do insert canceled .................... [passed]")

    #
    # ------------------- test_insert_error_exit.py ----------------
    #
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
    def do_insert_error_exit(self):
        # init
        self._rlist = None
   
        tdLog.info(f"start to excute {__file__}")
        t1 = threading.Thread(target=self.dbInsertThread)
        t2 = threading.Thread(target=self.dnodeNodeStopThread)
        t1.start()
        t2.start()
        tdLog.success(f"{__file__} successfully executed")
        t1.join()
        t2.join()

        print("do insert error exit .................. [passed]")


    #
    # ------------------- main ----------------
    #
    def test_benchmark_except(self):
        """taosBenchmark exception

        1. Insert operator be canceled check expect
        2. Insert operator be forced exit check expect
        3. Insert operator meet dnode exit check expect
        
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_cancle.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_error_exit.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/

        """
        self.do_insert_cancel()
        self.do_insert_error_exit()