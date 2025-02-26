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
        taosBenchmark tmp->Basic test cases
        """

    def run(self):
        
        # insert data
        json = "tools/benchmark/basic/json/tmqBasicInsert.json"
        db, stb, child_count, insert_rows = self.insertBenchJson(json, checkStep = True)

        # tmq Sequ
        json = "tools/benchmark/basic/json/tmqBasicSequ.json"
        self.tmqBenchJson(json)

        # tmq Parallel
        json = "tools/benchmark/basic/json/tmqBasicPara.json"
        self.tmqBenchJson(json)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
