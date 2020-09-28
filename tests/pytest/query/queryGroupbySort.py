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

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        tdSql.execute(
            "create table stb(ts timestamp,i int) tags (p_id nchar(20));")
        tdSql.execute(
            "insert into tb using stb tags('11231') values (%d, %d) (%d, %d) (%d, %d) (%d, %d)" 
            % (self.ts, 12, self.ts + 1, 15, self.ts + 2, 15, self.ts + 3, 12))

        tdSql.query(''' select last(ts) p_time,i from stb where p_id='11231' and ts>=%d and ts <=%d 
            group by i order by time desc limit 100 ''' % (self.ts, self.ts + 4))
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00.003000")
        tdSql.checkData(1, 0, "2018-09-17 09:00:00.002000")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
