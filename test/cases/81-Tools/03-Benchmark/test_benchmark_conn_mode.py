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
from new_test_framework.utils import tdLog, tdSql, etool
import os
import platform

class TestConnMode:
    def caseDescription(self):
        """
        taosBenchmark public->connMode test cases
        """

    # expect cmd > evn > json
    def checkPriority(self):

        #
        #  cmd & json
        #
        
        # cmd first 6041 - valid
        options = "-X http://127.0.0.1:6041"
        # json 6042 - invalid
        json = f"{os.path.dirname(__file__)}/json/connModePriorityErrDsn.json"
        self.insertBenchJson(json, options, True)

        #
        #  env > json
        #

        # env 6041 - valid
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6041"
        # json 6042 - invalid
        json = f"{os.path.dirname(__file__)}/json/connModePriorityErrDsn.json"
        self.insertBenchJson(json, "", True)


        #
        # cmd & json & evn
        #

        # cmd  6041 - valid
        options = "-X http://127.0.0.1:6041"
        # env  6043 - invalid
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6043"
        # json 6042 - invalid
        json = f"{os.path.dirname(__file__)}/json/connModePriorityErrDsn.json"
        self.insertBenchJson(json, options, True)

        # clear env
        os.environ['TDENGINE_CLOUD_DSN'] = ""

    def checkCommandLine(self):
        # default CONN_MODE
        DEFAULT_CONN_MODE = "Native"

        # modes
        modes = ["", "-Z 1 -B 1", "-Z websocket", "-Z 0", "-Z native -B 2"]
        # result
        Rows = "insert rows: 9990"
        results1 = [
            [f"Connect mode is : {DEFAULT_CONN_MODE}", Rows],
            ["Connect mode is : WebSocket", Rows],
            ["Connect mode is : WebSocket", Rows],
            ["Connect mode is : Native", Rows],
            ["Connect mode is : Native", Rows],
        ]
        # iface todo add sml
        iface = ["taosc", "stmt", "stmt2"]

        # do check
        for face in iface:
            for i in range(len(modes)):
                self.benchmarkCmd(f"{modes[i]} -I {face}", 10, 999, 1000, results1[i])
                
        
    def checkExceptCmd(self):
        # exe
        bench   = etool.benchMarkFile()
        # option
        options = [
            "-Z native -X http://127.0.0.1:6041",
            "-Z 100",
            "-Z abcdefg",
            "-X",
            "-X 127.0.0.1:6041",
            "-X https://gw.cloud.taosdata.com?token617ffdf...",
            "-Z 1 -X https://gw.cloud.taosdata.com?token=617ffdf...",
            "-X http://127.0.0.1:6042"
        ]

        # do check
        for option in options:
            self.checkExcept(bench + " -y " + option)

    def checkHostPort(self):
        #
        # ommand
        # 
        self.benchmarkCmd("-h 127.0.0.1", 5, 100, 10, ["insert rows: 500"])
        user_opt = "-u root -p taosdata" if platform.system().lower() == "windows" else "-uroot -ptaosdata"

        self.benchmarkCmd(f"-h 127.0.0.1 {user_opt}", 5, 100, 10, ["insert rows: 500"])
        self.benchmarkCmd(f"-Z 0 -h 127.0.0.1 -P 6030 {user_opt}", 5, 100, 10, ["insert rows: 500"])

        #
        # command & json
        #

        # 6041 is default
        options = f"-Z 1 -h 127.0.0.1 -P 6041 {user_opt}"
        json = f"{os.path.dirname(__file__)}/json/connModePriorityErrHost.json"
        self.insertBenchJson(json, options, True)

        # cmd port first json port
        options = "-Z native -P 6030"
        json = f"{os.path.dirname(__file__)}/json/connModePriority.json"
        self.insertBenchJson(json, options, True)
        options = "-Z websocket -P 6041"
        json = f"{os.path.dirname(__file__)}/json/connModePriority.json"
        self.insertBenchJson(json, options, True)

    def test_benchmark_conn_mode(self):
        """taosBenchmark connect with Native and WebSocket

        1. Verify "-Z native -X http://127.0.0.1:6041"
        2. Verify "-Z websocket -X http://127.0.0.1:6042"
        3. Verify command line connection mode priority over json and env
        4. Verify env connection mode priority over json
        5. Verify except command line connection mode
        6. Verify host and port in command line and json

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_conn_mode.py

        """
        # init
        self.db  = "test"
        self.stb = "meters"

        # command line test
        self.checkCommandLine()

        # except
        self.checkExceptCmd()

        # cmd > json > env
        self.checkPriority()

        # host and port 
        self.checkHostPort()

        tdLog.success("%s successfully executed" % __file__)


