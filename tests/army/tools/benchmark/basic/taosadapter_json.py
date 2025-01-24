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
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def run(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/sml_rest_telnet.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f ./tools/benchmark/basic/json/sml_rest_line.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db2")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db2.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db2.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f ./tools/benchmark/basic/json/sml_rest_json.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db3")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db3.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db3.stb2")
        tdSql.checkData(0, 0, 160)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
