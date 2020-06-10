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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        print("==========step1")
        print("drop built-in account")
        try:
            tdSql.execute("drop account root")
        except Exception as e:
            if len(e.args) > 0 and 'no rights' != e.args[0]:
                tdLog.exit(e)

        print("==========step2")
        print("drop built-in user")
        try:
            tdSql.execute("drop user root")
        except Exception as e:
            if len(e.args) > 0 and 'no rights' != e.args[0]:
                tdLog.exit(e)
            return

        tdLog.exit("drop built-in user is error.")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
