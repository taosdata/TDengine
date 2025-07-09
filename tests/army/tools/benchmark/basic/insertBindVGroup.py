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
import json
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
        taosBenchmark insert->BindVGroup test cases
        """

    # bugs ts
    def checkBasic(self):
        # thread equal vgroups
        self.insertBenchJson("./tools/benchmark/basic/json/insertBindVGroup.json", "-g", True)
        # thread is limited
        self.insertBenchJson("./tools/benchmark/basic/json/insertBindVGroup.json", "-T 2", True)

    def run(self):
        # basic
        self.checkBasic()
        # advance

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
