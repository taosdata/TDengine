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

        ret = tdSql.query('select database()')
        tdSql.checkData(0, 0, "db")        
        
        ret = tdSql.query('select server_status()')
        tdSql.checkData(0, 0, 1)

        ret = tdSql.query('select server_status() as result')
        tdSql.checkData(0, 0, 1)

        ret = tdSql.query('show dnodes')

        ret = tdSql.execute('alter dnode "%s" debugFlag 135' % tdSql.getData(0,0))
        tdLog.info('alter dnode "%s" debugFlag 135 -> ret: %d' % (tdSql.getData(0, 0), ret))

        ret = tdSql.query('show mnodes')
        tdSql.checkRows(1)

        ret = tdSql.query('show vgroups')
        tdSql.checkRows(0)        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
