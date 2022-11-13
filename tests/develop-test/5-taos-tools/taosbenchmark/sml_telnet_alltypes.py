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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def run(self):
        binPath = self.getPath()
        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/sml_telnet_alltypes.json" %binPath
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
        tdSql.checkData(1, 1, "VARCHAR")
        tdSql.checkData(1, 2, 16)
        tdSql.query("describe db.stb13")
        tdSql.checkData(1, 1, "NCHAR")
        tdSql.checkData(1, 2, 16)
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
