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

        tdSql.execute('create table stb (ts timestamp, speed int) tags(t1 int);')

        for i in range(10):
            tdSql.execute(f'create table tb_{i} using stb tags({i});')
            tdSql.execute(f'insert into tb_{i} values(now, 0)')
            tdSql.execute(f'insert into tb_{i} values(now+10s, {i})')

        tdSql.query('select max(col3) from (select spread(speed) as col3 from stb group by tbname);')
        tdSql.checkData(0,0,'9.0')
        tdSql.error('select max(col3) from (select spread(col3) as col3 from stb group by tbname);')


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
