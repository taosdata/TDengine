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
        """
        [TD-11510] taosBenchmark test cases
        """

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        elif "src" in selfPath:
            projPath = selfPath[: selfPath.find("src")]
        elif "/tools/" in selfPath:
            projPath = selfPath[: selfPath.find("/tools/")]
        elif "/tests/" in selfPath:
            projPath = selfPath[: selfPath.find("/tests/")]
        else:
            tdLog.info("cannot found %s in path: %s, use system's" % (tool, selfPath))
            projPath = "/usr/local/taos/bin/"

        paths = []
        for root, dummy, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def run(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = self.getPath()
        cmd = "%s -f ./taosbenchmark/json/sml_interlace.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb1)")
        else:
            tdSql.query("select count(tbname) from db.stb1")
        tdSql.checkData(0, 0, 8)
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb2)")
        else:
            tdSql.query("select count(tbname) from db.stb2")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb1")
        result = tdSql.getData(0, 0)
        assert result <= 160, "result is %s > expect: 160" % result
        tdSql.query("select count(*) from db.stb2")
        result = tdSql.getData(0, 0)
        assert result <= 160, "result is %s > expect: 160" % result

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
