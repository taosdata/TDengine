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
from util.log import *
from util.cases import *
from util.sql import *
from math import floor


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        sql = "select server_version()"
        ret = tdSql.query(sql)
        version = floor(float(tdSql.getData(0, 0)[0:3]))        
        expectedVersion = 2
        
        if(version == expectedVersion):
            tdLog.info("sql:%s, row:%d col:%d data:%d == expect" % (sql, 0, 0, version))
        else:
            tdLog.exit("sql:%s, row:%d col:%d data:%d != expect:%d " % (sql, 0, 0, version, expectedVersion))

        sql = "select client_version()"
        ret = tdSql.query(sql)
        version = floor(float(tdSql.getData(0, 0)[0:3]))        
        expectedVersion = 2
        if(version == expectedVersion):
            tdLog.info("sql:%s, row:%d col:%d data:%d == expect" % (sql, 0, 0, version))
        else:
            tdLog.exit("sql:%s, row:%d col:%d data:%d != expect:%d " % (sql, 0, 0, version, expectedVersion))
      

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
