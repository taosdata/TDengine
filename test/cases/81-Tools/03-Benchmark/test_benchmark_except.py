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
    # ------------------- common helpers ----------------
    #
    def get_pids_by_name(self, process_name):
        pids = []
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == process_name:
                pids.append(proc.pid)
        return pids

    def wait_for_benchmark(self, timeout=30, interval=0.5):
        """Poll until taosBenchmark process appears, return pids."""
        elapsed = 0
        while elapsed < timeout:
            pids = self.get_pids_by_name("taosBenchmark")
            if pids:
                tdLog.info(f"Found taosBenchmark with PID: {pids}")
                return pids
            time.sleep(interval)
            elapsed += interval
        tdLog.exit("Timeout waiting for taosBenchmark process to start.")
        return []

    #
    # ------------------- test_insert_cancel ----------------
    #
    def stopThread(self, isForceExit):
        tdLog.info(f"stopThread start, force={isForceExit}")
        pids = self.wait_for_benchmark()
        if not pids:
            tdLog.exit("No process named taosBenchmark was found.")

        # Wait for benchmark to enter active insert phase
        time.sleep(3)

        os.kill(pids[0], signal.SIGINT)
        if isForceExit:
            # Send second SIGINT immediately (no delay!) to trigger forced exit
            os.kill(pids[0], signal.SIGINT)

        # Wait for benchmark to finish and write output
        time.sleep(10)

        if self._rlist:
            tdLog.info(self._rlist)
            if isForceExit:
                self.checkListString(self._rlist, "Benchmark process forced exit!")
            else:
                self.checkListString(self._rlist, "Receive SIGINT or other signal, quit benchmark")
        else:
            tdLog.exit("The benchmark process has not stopped!")

    def dbInsertThread(self, cmd):
        tdLog.info(f"dbInsertThread start, cmd: {cmd}")
        self._rlist = self.benchmark(cmd, checkRun=False)

    # run
    def do_insert_cancel(self):
        tdLog.info(f"start to execute {__file__} - insert cancel")

        # Use enough tables so benchmark is still running when SIGINT arrives
        cmd = "-d db -t 10000 -n 10000 -T 8 -I stmt -y"

        # Test 1: graceful cancel (single SIGINT)
        self._rlist = None
        t1 = threading.Thread(target=self.dbInsertThread, args=(cmd,))
        t2 = threading.Thread(target=self.stopThread, args=(False,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Test 2: forced exit (double SIGINT)
        self._rlist = None
        t1 = threading.Thread(target=self.dbInsertThread, args=(cmd,))
        t2 = threading.Thread(target=self.stopThread, args=(True,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        tdLog.info("do insert canceled .................... [passed]")

    #
    # ------------------- test_insert_error_exit ----------------
    #
    def dnodeNodeStopThread(self):
        tdLog.info("dnodeNodeStopThread start")
        pids = self.wait_for_benchmark()
        if not pids:
            tdLog.exit("No process named taosBenchmark was found.")

        # Wait for benchmark to enter active insert phase
        time.sleep(3)
        tdLog.info("stopping dnode 1 ...")
        sc.dnodeStop(1)

        # Wait for benchmark to detect error and exit
        time.sleep(10)

        if self._rlist:
            tdLog.info(self._rlist)
            self.checkListString(self._rlist, "failed to execute insert statement. reason: Vnode stopped")
        else:
            tdLog.exit("The benchmark process has not stopped!")

    # run
    def do_insert_error_exit(self):
        self._rlist = None
        tdLog.info(f"start to execute {__file__} - insert error exit")

        # Use enough tables so benchmark is still running when dnode stops
        cmd = "-d db -t 10000 -n 10000 -T 4 -I stmt -y"
        t1 = threading.Thread(target=self.dbInsertThread, args=(cmd,))
        t2 = threading.Thread(target=self.dnodeNodeStopThread)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        tdLog.info("do insert error exit .................. [passed]")


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