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


class TestStmt2InsertRetryJsonStb:
    def caseDescription(self):
        """
        [TD-19985] taosBenchmark retry test cases
        """

    def dbInsertThread(self):
        tdLog.info(f"dbInsertThread start")
        # taosBenchmark run
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/stmt2_insert_retry-stb.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

    def stopThread(self):
        time.sleep(3)
        sc.dnodeStopAll()
        time.sleep(5)
        sc.dnodeStart(1)    

    def test_stmt2_insert_retry_json_stb(self):
        tdLog.info(f"start to excute {__file__}")
        t1 = threading.Thread(target=self.dbInsertThread)
        t2 = threading.Thread(target=self.stopThread)
        
        t1.start()
        t2.start()
        tdLog.success(f"{__file__} successfully executed")
        t1.join()
        t2.join()
        
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 100)

        tdLog.success("%s successfully executed" % __file__)


