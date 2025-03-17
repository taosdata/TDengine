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
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/taosc_auto_create_table.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select distinct(c5) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.stb1")
        tdSql.checkData(0, 0, None)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.`stb1-2`")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select distinct(c5) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)

        if major_ver == "3":
            tdSql.query(
                "select `ttl` from information_schema.ins_tables where db_name = 'db' and table_name like 'stb\_%' limit 1"
            )
            tdSql.checkData(0, 0, 360)

            tdSql.query(
                "select `ttl` from information_schema.ins_tables where db_name = 'db' and table_name like 'stb1-%' limit 1"
            )
            tdSql.checkData(0, 0, 180)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
