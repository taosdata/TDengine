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
            tdSql.execute("create user &abc PASS 'pass123'")
        except Exception as e:
            print(e)

        print("==============step2")
        try:
            tdSql.execute("create user a&bc PASS 'pass123'")
        except Exception as e:
            print(e)

        print("==============step3")
        try:
            tdSql.execute("create user '涛思' PASS 'pass123'")
        except Exception as e:
            print(e)
            return

        tdLog.exit("create user with special character.")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
