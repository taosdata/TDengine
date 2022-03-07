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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        sql = "select server_version()"
        ret = tdSql.query(sql)
        version = tdSql.getData(0, 0)[0:3]        
        expectedVersion_dev = "2.0"
        expectedVersion_master = "2.1"
        if(version == expectedVersion_dev or version == expectedVersion_master):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect" % (sql, 0, 0, version))
        else:
            tdLog.exit("sql:%s, row:%d col:%d data:%s != expect:%s or %s " % (sql, 0, 0, version, expectedVersion_dev, expectedVersion_master))

        sql = "select client_version()"
        ret = tdSql.query(sql)
        version = tdSql.getData(0, 0)[0:3]        
        expectedVersion_dev = "2.0"
        expectedVersion_master = "2.1"
        if(version == expectedVersion_dev or version == expectedVersion_master):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect" % (sql, 0, 0, version))
        else:
            tdLog.exit("sql:%s, row:%d col:%d data:%s != expect:%s or %s " % (sql, 0, 0, version, expectedVersion_dev, expectedVersion_master))
      

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
