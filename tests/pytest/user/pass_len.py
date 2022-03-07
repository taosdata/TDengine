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
        print("==============step1")
        try:
            tdSql.execute("create user abc pass '123456'")
        except Exception as e:
            tdLog.exit(e)
        print("create user abc pass '123456'")

        print("==============step2")
        try:
            tdSql.execute("alter user abc pass 'taosdata'")
        except Exception as e:
            tdLog.exit(e)
        print("alter user abc pass 'taosdata'")

        print("==============step3")
        try:
            tdSql.execute("alter user abc pass ''")
        except Exception as e:
            print("alter user abc pass ''")
        else:
            tdLog.exit("Error: alert user abc pass''")

        print("==============step4")
        try:
            tdSql.execute("alter user abc pass null")
        except Exception as e:
            print("alter user abc pass null")
        else:
            tdLog.exit("Error: alter user abc pass null")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
