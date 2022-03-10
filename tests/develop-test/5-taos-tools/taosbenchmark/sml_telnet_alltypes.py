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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        '''
        [TD-11510] taosBenchmark test cases
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/sml_telnet_alltypes.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb1")
        tdSql.checkData(1, 1, "BOOL")
        tdSql.query("describe db.stb2")
        tdSql.checkData(1, 1, "TINYINT")
        tdSql.query("describe db.stb3")
        tdSql.checkData(1, 1, "TINYINT UNSIGNED")
        tdSql.query("describe db.stb4")
        tdSql.checkData(1, 1, "SMALLINT")
        tdSql.query("describe db.stb5")
        tdSql.checkData(1, 1, "SMALLINT UNSIGNED")
        tdSql.query("describe db.stb6")
        tdSql.checkData(1, 1, "INT")
        tdSql.query("describe db.stb7")
        tdSql.checkData(1, 1, "INT UNSIGNED")
        tdSql.query("describe db.stb8")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.query("describe db.stb9")
        tdSql.checkData(1, 1, "BIGINT UNSIGNED")
        tdSql.query("describe db.stb10")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.query("describe db.stb11")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb12")
        tdSql.checkData(1, 1, "BINARY")
        tdSql.checkData(1, 2, 8)
        tdSql.query("describe db.stb13")
        tdSql.checkData(1, 1, "NCHAR")
        tdSql.checkData(1, 2, 8)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb5")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb6")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb7")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb8")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb9")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb11")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb12")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb13")
        tdSql.checkData(0, 0, 160)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())