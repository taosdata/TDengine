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
        [TD-13928] taosBenchmark improve user interface
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
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
        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/custom_col_tag.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "first_type")
        tdSql.checkData(2, 0, "second_type")
        tdSql.checkData(3, 0, "second_type_1")
        tdSql.checkData(4, 0, "second_type_2")
        tdSql.checkData(5, 0, "second_type_3")
        tdSql.checkData(6, 0, "second_type_4")
        tdSql.checkData(7, 0, "third_type")
        tdSql.checkData(8, 0, "forth_type")
        tdSql.checkData(9, 0, "forth_type_1")
        tdSql.checkData(10, 0, "forth_type_2")
        tdSql.checkData(11, 0, "single")
        tdSql.checkData(12, 0, "multiple")
        tdSql.checkData(13, 0, "multiple_1")
        tdSql.checkData(14, 0, "multiple_2")
        tdSql.checkData(15, 0, "multiple_3")
        tdSql.checkData(16, 0, "multiple_4")
        tdSql.checkData(17, 0, "thensingle")
        tdSql.checkData(18, 0, "thenmultiple")
        tdSql.checkData(19, 0, "thenmultiple_1")
        tdSql.checkData(20, 0, "thenmultiple_2")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
