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
import os
import subprocess
import threading

import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.srvCtl import *


class TDTestCase(TBase):
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

    def run(self):
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

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
