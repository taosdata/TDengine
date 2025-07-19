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
from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestSmlJsonAlltypesInterlace:
    def caseDescription(self):
        """
        [TD-21932] taosBenchmark schemaless refine
        """

    def test_sml_json_alltypes_interlace(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f %s/json/sml_json_alltypes-interlace.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb1")
        tdSql.checkData(1, 1, "BOOL")
        tdSql.query("describe db.stb2")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb3")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb4")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb5")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb6")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb7")
        tdSql.checkData(1, 1, "DOUBLE")
        tdSql.query("describe db.stb8")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 1, "NCHAR")
            tdSql.checkData(1, 2, 8)

        tdSql.query("describe db.stb9")
        if major_ver == "3":
            tdSql.checkData(1, 1, "VARCHAR")
            tdSql.checkData(1, 2, 16)
        else:
            tdSql.checkData(1, 2, 8)
            tdSql.checkData(1, 1, "NCHAR")

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

        tdLog.success("%s successfully executed" % __file__)


