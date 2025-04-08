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
        [TD-23292] taosBenchmark test cases
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

        tdLog.info("projPath: %s" % projPath)
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
        cmd = (
            "%s -f ./taosbenchmark/json/sml_telnet_insert_alltypes-same-min-max.json"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.stb")
        rows = tdSql.queryResult[0]
        tdSql.query("select * from db.stb")
        for row in range(rows[0]):
            if major_ver == "3":
                tdSql.checkData(row, 1, 1)
                tdSql.checkData(row, 2, "1i32")
                tdSql.checkData(row, 3, "3000000000i64")
                tdSql.checkData(row, 4, "1.000000f32")
                tdSql.checkData(row, 5, "1.000000f64")
                tdSql.checkData(row, 6, "1i16")
                tdSql.checkData(row, 7, "1i8")
                tdSql.checkData(row, 8, "true")
                tdSql.checkData(row, 9, "4000000000u32")
                tdSql.checkData(row, 10, "5000000000u64")
                tdSql.checkData(row, 11, "30u8")
                tdSql.checkData(row, 12, "60000u16")
            else:
                tdSql.checkData(row, 1, 1)
                tdSql.checkData(row, 2, "1i32")
                tdSql.checkData(row, 3, "60000u16")
                tdSql.checkData(row, 4, "3000000000i64")
                tdSql.checkData(row, 5, "1.000000f32")
                tdSql.checkData(row, 6, "1.000000f64")
                tdSql.checkData(row, 7, "1i16")
                tdSql.checkData(row, 8, "1i8")
                tdSql.checkData(row, 9, "true")
                tdSql.checkData(row, 10, "4000000000u32")
                tdSql.checkData(row, 11, "5000000000u64")
                tdSql.checkData(row, 12, "30u8")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
