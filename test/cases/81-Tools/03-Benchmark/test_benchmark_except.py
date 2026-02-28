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

    def _insert_thread(self, cmd):
        """Run taosBenchmark with given command, store output in _rlist."""
        tdLog.info(f"_insert_thread start, cmd: {cmd}")
        self._rlist = self.benchmark(cmd, checkRun=False)

    #
    # ------------------- test_insert_cancel ----------------
    #
    def _stop_benchmark_thread(self, isForceExit):
        """Wait for taosBenchmark to start, then send SIGINT."""
        tdLog.info(f"_stop_benchmark_thread start, force={isForceExit}")
        pids = self.wait_for_benchmark()
        if not pids:
            return

        # Give benchmark a moment to enter active insert phase
        time.sleep(2)

        os.kill(pids[0], signal.SIGINT)
        if isForceExit:
            time.sleep(0.5)
            try:
                os.kill(pids[0], signal.SIGINT)
            except ProcessLookupError:
                pass

    def _run_cancel_test(self, isForceExit):
        """Run a single insert-cancel scenario and verify output."""
        self._rlist = None
        cmd = "-d db -t 10 -n 10000 -T 4 -I stmt -y"

        t1 = threading.Thread(target=self._insert_thread, args=(cmd,))
        t2 = threading.Thread(target=self._stop_benchmark_thread, args=(isForceExit,))
        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Verify output after both threads complete (no race condition)
        if self._rlist:
            tdLog.info(self._rlist)
            if isForceExit:
                self.checkListString(self._rlist, "Benchmark process forced exit!")
            else:
                self.checkListString(self._rlist, "Receive SIGINT or other signal, quit benchmark")
        else:
            tdLog.exit("The benchmark process output is empty!")

    # run
    def do_insert_cancel(self):
        tdLog.info(f"start to execute {__file__} - insert cancel")
        self._run_cancel_test(isForceExit=False)
        self._run_cancel_test(isForceExit=True)
        tdLog.info("do insert canceled .................... [passed]")

    #
    # ------------------- test_insert_error_exit ----------------
    #
    def _dnode_stop_thread(self):
        """Wait for taosBenchmark to start, then stop dnode 2."""
        tdLog.info("_dnode_stop_thread start")
        pids = self.wait_for_benchmark()
        if not pids:
            return
        # Wait long enough for benchmark to enter active insert phase,
        # but not so long that it finishes before dnode is stopped
        time.sleep(5)
        tdLog.info("stopping dnode 2 ...")
        sc.dnodeStop(2)

    # run
    def do_insert_error_exit(self):
        self._rlist = None
        tdLog.info(f"start to execute {__file__} - insert error exit")

        # Use enough data (-t 100 -n 10000) so benchmark is still inserting when dnode stops
        cmd = "-d db -t 100 -n 10000 -T 4 -I stmt -y"
        t1 = threading.Thread(target=self._insert_thread, args=(cmd,))
        t2 = threading.Thread(target=self._dnode_stop_thread)
        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Verify output after both threads complete (no race condition)
        if self._rlist:
            tdLog.info(self._rlist)
            self.checkListString(self._rlist, "failed to execute insert statement. reason: Vnode stopped")
        else:
            tdLog.exit("The benchmark process output is empty!")
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