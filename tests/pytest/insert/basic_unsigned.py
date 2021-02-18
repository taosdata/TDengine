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

        ret = tdSql.execute('create table tb (ts timestamp, speed int unsigned)')

        insertRows = 10
        tdLog.info("insert %d rows" % (insertRows))
        for i in range(0, insertRows):
            ret = tdSql.execute(
                'insert into tb values (now + %dm, %d)' %
                (i, i))

        tdLog.info("insert earlier data")
        tdSql.execute('insert into tb values (now - 5m , 10)')
        tdSql.execute('insert into tb values (now - 6m , 10)')
        tdSql.execute('insert into tb values (now - 7m , 10)')
        tdSql.execute('insert into tb values (now - 8m , 4294967294)')

        tdSql.error('insert into tb values (now - 9m, -1)')
        tdSql.error('insert into tb values (now - 9m, 4294967295)')

        tdSql.query("select * from tb")
        tdSql.checkRows(insertRows + 4)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
