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

import taos
import frame
import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def alterSupportVnodes(self):
        tdLog.info(f"test function of altering supportVnodes")

        tdSql.execute("alter dnode 1 'supportVnodes' '128'")
        time.sleep(1)
        tdSql.query('show dnodes')
        tdSql.checkData(0, 3, "128")
        
        tdSql.execute("alter dnode 1 'supportVnodes' '64'")
        time.sleep(1)
        tdSql.query('show dnodes')
        tdSql.checkData(0, 3, "64")

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # TS-4721
        self.alterSupportVnodes()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
