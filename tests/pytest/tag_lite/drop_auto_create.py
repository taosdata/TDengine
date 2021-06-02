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

        tdSql.execute('create table m1(ts timestamp, k int) tags(a binary(12), b int, c double);')
        tdSql.execute('insert into tm0 using m1(b,c) tags(1, 99) values(now, 1);')
        tdSql.execute('insert into tm1 using m1(b,c) tags(2, 100) values(now, 2);')
        tdLog.info("2 rows inserted")
        tdSql.query('select * from m1;')
        tdSql.checkRows(2)
        tdSql.query('select *,tbname from m1;')
        tdSql.execute("drop table tm0; ")
        tdSql.query('select * from m1')
        tdSql.checkRows(1)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
