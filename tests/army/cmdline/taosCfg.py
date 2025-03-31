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

import sys
import time
import random

import frame.eos
import frame.etime
import frame.etool
import frame.etool
import frame.etool
import frame.etool
import taos
import frame.etool
import frame

from frame.log import *
from frame.sql import *
from frame.cases import *
from frame.caseBase import *
from frame.srvCtl import *
from frame import *


class TDTestCase(TBase):
    updatecfgDict = {
        'serverPort': 7030
    }

    def checkPort(self):
        rlist = self.taos(" -P 7030 -s 'show dnodes;'")
        results = [
            ":7030",
            "Query OK,"
        ]

        self.checkManyString(rlist, results) 

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # check show whole
        self.checkPort()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
