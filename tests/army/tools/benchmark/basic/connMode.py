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
import os
from time import sleep
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        taosBenchmark public->connMode test cases
        """

    # expect cmd > json > evn
    def checkPriority(self):

        #
        #  cmd & json
        #
        
        # json 6042 - invalid
        json = "tools/benchmark/basic/json/connModePriorityErr.json"
        # cmd  6041 - valid
        options = "-X http://127.0.0.1:6041"
        self.insertBenchJson(json, options, True)

        #
        # json & env
        #

        # env  6043 - invalid
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6043"
        # json 6042 - invalid
        json = "tools/benchmark/basic/json/connModePriority.json"
        self.insertBenchJson(json, "", True)


        #
        # cmd & json & evn
        #

        # env  6043 - invalid
        os.environ['TDENGINE_CLOUD_DSN'] = "http://127.0.0.1:6043"
        # json 6042 - invalid
        json = "tools/benchmark/basic/json/connModePriorityErr.json"
        # cmd  6041 - valid
        options = "-X http://127.0.0.1:6041"
        self.insertBenchJson(json, options, True)

        # clear env
        os.environ['TDENGINE_CLOUD_DSN'] = ""

    def checkCommandLine(self):
        # modes
        modes = ["", "-Z 1 -B 1", "-Z websocket", "-Z 0", "-Z native -B 2"]
        # result
        Rows = "insert rows: 9990"
        results1 = [
            ["Connect mode is : WebSocket", Rows],
            ["Connect mode is : WebSocket", Rows],
            ["Connect mode is : WebSocket", Rows],
            ["Connect mode is : Native", Rows],
            ["Connect mode is : Native", Rows],
        ]
        # iface
        iface = ["taosc", "stmt", "stmt2"]

        # do check
        for face in iface:
            for i in range(len(modes)):
                self.benchmarkCmd(f"{modes[i]} -I {face}", 10, 999, 1000, results1[i])

    def checkExceptCmd(self):
        # exe
        bench   = frame.etool.benchMarkFile()
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
            self.checkExcept(bench + " " + option + " -y")

    def checkHostPort(self):
        # host port
        self.benchmarkCmd("-h 127.0.0.1", 5, 100, 10, ["insert rows: 500"])
        self.benchmarkCmd("-h 127.0.0.1 -P 6041 -uroot -ptaosdata", 5, 100, 10, ["insert rows: 500"])
        self.benchmarkCmd("-Z 0 -h 127.0.0.1 -P 6030 -uroot -ptaosdata", 5, 100, 10, ["insert rows: 500"])

    def run(self):
        # command line test
        self.checkCommandLine()

        # host and port 
        self.checkHostPort()

        # except
        self.checkExceptCmd()

        # cmd > json > env
        self.checkPriority()


        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
