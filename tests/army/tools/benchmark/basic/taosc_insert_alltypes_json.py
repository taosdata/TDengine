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
        cmd = "%s -f ./taosbenchmark/json/taosc_insert_alltypes.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 160)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkRows(29)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TIMESTAMP")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(4, 1, "FLOAT")
        tdSql.checkData(5, 1, "DOUBLE")
        tdSql.checkData(6, 1, "SMALLINT")
        tdSql.checkData(7, 1, "TINYINT")
        tdSql.checkData(8, 1, "BOOL")
        tdSql.checkData(9, 1, "NCHAR")
        tdSql.checkData(9, 2, 29)
        tdSql.checkData(10, 1, "INT UNSIGNED")
        tdSql.checkData(11, 1, "BIGINT UNSIGNED")
        tdSql.checkData(12, 1, "TINYINT UNSIGNED")
        tdSql.checkData(13, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(14, 1, "BINARY")
        tdSql.checkData(14, 2, 23)
        tdSql.checkData(15, 1, "TIMESTAMP")
        tdSql.checkData(16, 1, "INT")
        tdSql.checkData(17, 1, "BIGINT")
        tdSql.checkData(18, 1, "FLOAT")
        tdSql.checkData(19, 1, "DOUBLE")
        tdSql.checkData(20, 1, "SMALLINT")
        tdSql.checkData(21, 1, "TINYINT")
        tdSql.checkData(22, 1, "BOOL")
        tdSql.checkData(23, 1, "NCHAR")
        tdSql.checkData(23, 2, 17)
        tdSql.checkData(24, 1, "INT UNSIGNED")
        tdSql.checkData(25, 1, "BIGINT UNSIGNED")
        tdSql.checkData(26, 1, "TINYINT UNSIGNED")
        tdSql.checkData(27, 1, "SMALLINT UNSIGNED")
        # binary/varchar diff in 2.x/3.x
        # tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(28, 2, 19)
        tdSql.query("select count(*) from db.stb where c1 >= 0 and c1 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c2 >= 0 and c2 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c3 >= 0 and c3 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c4 >= 0 and c4 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c5 >= 0 and c5 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c6 >= 0 and c6 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c8 = 'd1' or c8 = 'd2'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c9 >= 0 and c9 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c10 >= 0 and c10 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c11 >= 0 and c11 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c12 >= 0 and c12 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where c13 = 'b1' or c13 = 'b2'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t1 >= 0 and t1 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t2 >= 0 and t2 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t3 >= 0 and t3 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t4 >= 0 and t4 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t5 >= 0 and t5 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t6 >= 0 and t6 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t8 = 'd1' or t8 = 'd2'")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t9 >= 0 and t9 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t10 >= 0 and t10 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t11 >= 0 and t11 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t12 >= 0 and t12 <= 10")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb where t13 = 'b1' or t13 = 'b2'")
        tdSql.checkData(0, 0, 160)

        if major_ver == "3":
            tdSql.query(
                "select `ttl` from information_schema.ins_tables where db_name = 'db' limit 1"
            )
            tdSql.checkData(0, 0, 360)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
