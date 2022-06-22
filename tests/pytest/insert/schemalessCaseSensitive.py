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

from util.log import *
from util.cases import *
from util.sql import *
from util.types import TDSmlProtocolType, TDSmlTimestampType

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self._conn = conn

    def run(self):

        # schemaless
        tdSql.execute("create database line_insert precision 'ns' ")
        tdSql.execute("use line_insert")
        lines = [   
                    "St,deviceId=1i voltage=1,phase=\"Test\" 1626006833639000000",
                    "St,DeviceId=3i voltage=2,phase=\"Test\" 1626006833639000000",
                    "St,deviceId=2i,DeviceId=3 Voltage=2,Phase=\"Test2\" 1626006833639000000",                    
                    "St,deviceId=4i,DeviceId=3 voltage=1,phase=\"Test\",Voltage=2,Phase=\"Test1\" 1626006833639000000",
                    "tbl,deviceId=\"sensor0\" Hello=3i 1646053743694400029",
                    "tbl,deviceId=\"sensor0\" n=3i,N=4i 1646053743694400030",
                    "tbl,deviceId=\"sensor0\" g=3i 1646053743694400031",
                    "tbl,deviceId=\"sensor0\" G=3i 1646053743694400032",
                    "tbl,deviceId=\"sensor0\" nice=2i,Nice=3i 1646053743694400033",
                    "tbl,deviceId=\"sensor0\" hello=3i 1646053743694400034",
                    "超级表,deviceId=\"sensor0\" 电压=3i 1646053743694400035",
                ]

        code = self._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.query("show stables")
        tdSql.checkRows(3)

        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("describe `St`")
        tdSql.checkRows(7)

        tdSql.query("select * from `St`")
        tdSql.checkRows(4)

        tdSql.query("select * from tbl")
        tdSql.checkRows(6)

        tdSql.query("select * from `超级表`")
        tdSql.checkRows(1)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())