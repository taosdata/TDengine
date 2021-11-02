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
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info("=============== step1")
        tdSql.execute(
            'create stable properties_asch5snuykd (ts timestamp, create_time timestamp, number_value double, value int) tags (device_id nchar(8), property nchar(8))'
        )
        tdSql.execute(
            "insert into\n\t\t  \n\t\t\tdb.properties_b86ca7d11556e0fdd43fd12ac08651f9 using db.properties_asch5snuykd\n\t\t\t(\n\t\t\t  \n\t\t\t\tdevice_id\n\t\t\t , \n\t\t\t\tproperty\n\t\t\t \n\t\t\t)\n\t\t\ttags\n\t\t\t(\n\t\t\t  \n\t\t\t\t'dev1'\n\t\t\t , \n\t\t\t\t'pres'\n\t\t\t \n\t\t\t)\n\t\t\t(\n\t\t\t  \n\t\t\t\tts\n\t\t\t , \n\t\t\t\tcreate_time\n\t\t\t , \n\t\t\t\tnumber_value\n\t\t\t , \n\t\t\t\tvalue\n\t\t\t \n\t\t\t)\n\t\t\tvalues\n\t\t\t  \n\t\t\t\t(\n\t\t\t\t  \n\t\t\t\t\t1629443494659\n\t\t\t\t , \n\t\t\t\t\t1629443494660\n\t\t\t\t , \n\t\t\t\t\t-1000.0\n\t\t\t\t , \n\t\t\t\t\t'-1000'\n\t\t\t\t \n\t\t\t\t)\n;"
        )

        tdSql.query(
            "select * from db.properties_b86ca7d11556e0fdd43fd12ac08651f9")
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
