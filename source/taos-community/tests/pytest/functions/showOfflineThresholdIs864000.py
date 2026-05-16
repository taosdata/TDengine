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
import numpy as np

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        # tdSql.query("show variables")
        # tdSql.checkData(54, 1, 864000)
        tdSql.execute("show variables")
        res = tdSql.cursor.fetchall()
        resList = np.array(res)
        index = np.where(resList == "offlineThreshold")
        index_value = np.dstack((index[0])).squeeze()
        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 864000)
        pass


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
