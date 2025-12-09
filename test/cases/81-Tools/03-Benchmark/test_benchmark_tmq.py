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
# import os, signal
from new_test_framework.utils import tdLog, tdSql, etool, sc
import os
import time
import subprocess
import threading
import signal
import psutil

class TestBenchmarkTmq:
    updatecfgDict = {
        'slowLogScope' : "others"
    }    
    #
    # ------------------- test_tmp_basic.py ----------------
    #
    def do_tmq_basic(self):
        # insert data
        json = f"{os.path.dirname(__file__)}/json/tmqBasicInsert.json"
        db, stb, child_count, insert_rows = self.insertBenchJson(json, checkStep = True)

        # tmq Sequ
        json = f"{os.path.dirname(__file__)}/json/tmqBasicSequ.json"
        self.tmqBenchJson(json)

        # tmq Parallel
        json = f"{os.path.dirname(__file__)}/json/tmqBasicPara.json"
        self.tmqBenchJson(json)

        print("do tmq basic .......................... [passed]")

    #
    # ------------------- test_tmq_case.py ----------------
    #
    def do_tmq_case(self):
        tdSql.execute("drop topic if exists tmq_topic_0")
        tdSql.execute("drop topic if exists tmq_topic_1")
        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/default.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("alter database db WAL_RETENTION_PERIOD 3600000")
        tdSql.execute("reset query cache")
        cmd = "%s -f %s/json/tmq_basic.json " % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
                
        cmd = "%s -f %s/json/tmq_basic2.json " % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
        cmd = "%s -f %s/json/tmq_basic3.json " % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
#        try:
#            for line in os.popen("ps ax | grep taosBenchmark | grep -v grep"):
#                fields = line.split()

#                pid = fields[0]

#                os.kill(int(pid), signal.SIGINT)
#                time.sleep(3)
#            print("taosBenchmark be killed on purpose")
#        except:
#            tdLog.exit("failed to kill taosBenchmark")
   
        print("do tmq case .......................... [passed]")

    #
    # ------------------- test_tmq_cancle.py ----------------
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
        time.sleep(10)
        
        if self._rlist:
            tdLog.info(self._rlist)   
            self.checkListString(self._rlist, "Receive SIGINT or other signal, quit benchmark")
        else:
            tdLog.exit("The benchmark process has not stopped!")

    def dbInsert(self):
        tdLog.info(f"dbInsert start")
        # taosBenchmark run
        cmd = "-d dbcancel -t 1000 -n 1000 -T 4 -I stmt -y"
        self.benchmark(cmd, checkRun=True)

    def dbTmqThread(self):
        binPath = etool.benchMarkFile()
        cmd = f"-f {os.path.dirname(__file__)}/json/tmq_cancel.json"
        self._rlist = self.benchmark(cmd, checkRun=False)
        tdLog.info(self._rlist)

    # run
    def do_tmq_cancel(self):
        self._rlist = None 
        tdLog.info(f"start to excute {__file__}")
        tdSql.execute("drop topic if exists topic_benchmark_meters")
        self.dbInsert()
        tdLog.info(f"dbInsert finish！")   

        t1 = threading.Thread(target=self.dbTmqThread)
        t2 = threading.Thread(target=self.stopThread, args=(False,))
        t1.start()
        t2.start()
        # wait for threads to complete
        t1.join()
        t2.join()    
    
        print("do tmq cancel ......................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_tmq(self):
        """taosBenchmark tmq

        1. Check tmq basic insert data, tmq sequ, tmq parallel
        2. Check tmq case for different options
        3. Check coredump do restart taosd during consume data

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_tmp_basic.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_tmq_case.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_tmq_cancle.py

        """
        self.do_tmq_basic()
        self.do_tmq_case()
        self.do_tmq_cancel()