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
import os
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.numberOfTables = 10000
        self.numberOfRecords = 100

    def run(self):
        tdSql.prepare()
        
        os.system("yes | taosdemo -t %d -n %d" % (self.numberOfTables, self.numberOfRecords))

        tdSql.execute("use test")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, self.numberOfTables * self.numberOfRecords)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
