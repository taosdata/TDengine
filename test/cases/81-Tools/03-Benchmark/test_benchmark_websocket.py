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
from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestWebsocket:
    #
    # ------------------- test_websocket.py ----------------
    #
    def do_websocket(self):
        binPath = etool.benchMarkFile()
        cmd = "%s  -t 1 -n 1 -y -W http://localhost:6041 " % binPath
        etool.run(cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use test")
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        print("do websocket basic ................... [passed]")

    #
    # ------------------- main ----------------
    #
    def do_cloud_websocket(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -T 1 -t 2 -n 10  --driver='WebSocket' -y" % binPath
        out, err = etool.run(cmd)
        if out.find("Connect mode is : WebSocket") == -1:
            print(out)
            tdLog.exit("WebSocket driver not used, because not found 'Connect mode is : WebSocket'")
        
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = "%s -T 3 -t 5 -n 10 --cloud_dsn='http://localhost:6041' -y" % binPath
        etool.run(cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 50)

        print("do cloud .............................. [passed]")

    #
    # ------------------- main ----------------
    #
    def test_benchmark_websocket(self):
        """taosBenchmark websocket

        1. Check option -W http://localhost:6041
        2. Check option --driver='WebSocket'
        3. Check option --cloud_dsn='http://localhost:6041'

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/ws/test_websocket.py
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/cloud/test_cloud.py

        """
        self.do_websocket()
        self.do_cloud_websocket()