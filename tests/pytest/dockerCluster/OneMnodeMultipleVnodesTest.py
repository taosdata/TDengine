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
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from util.dockerNodes import *

class TDTestCase:
    
    updatecfgDict = {'numOfNodes': 3, '1':{'role': 1}, '2':{'role': 2}, '3':{'role': 2}}
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdLog.sleep(2)

        tdSql.query("show dnodes")
        tdSql.checkData(0, 5, 'mnode')
        tdSql.checkData(1, 5, 'vnode')
        tdSql.checkData(2, 5, 'vnode')
    
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())