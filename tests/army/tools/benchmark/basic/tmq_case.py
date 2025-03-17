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
import time
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
        [TD-11510] taosBenchmark test cases
        """



    def run(self):
        tdSql.execute("drop topic if exists tmq_topic_0")
        tdSql.execute("drop topic if exists tmq_topic_1")
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/default.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("alter database db WAL_RETENTION_PERIOD 3600000")
        tdSql.execute("reset query cache")
        cmd = "%s -f ./tools/benchmark/basic/json/tmq_basic.json " % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
                
        cmd = "%s -f ./tools/benchmark/basic/json/tmq_basic2.json " % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
        cmd = "%s -f ./tools/benchmark/basic/json/tmq_basic3.json " % binPath
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

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
