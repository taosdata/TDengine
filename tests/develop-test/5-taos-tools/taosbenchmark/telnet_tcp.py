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
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def init(self, conn, logSql, replicaVar=1):
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
        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/sml_telnet_tcp.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(5)
        tdSql.execute("reset query cache")
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]
        if major_ver == "3":
            tdSql.query(
                "select count(*) from (select distinct(tbname) from opentsdb_telnet.stb1)"
            )
        else:
            tdSql.query("select count(tbname) from opentsdb_telnet.stb1")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from opentsdb_telnet.stb1")
        tdSql.checkData(0, 0, 160)
        if major_ver == "3":
            tdSql.query(
                "select count(*) from (select distinct(tbname) from opentsdb_telnet.stb2)"
            )
        else:
            tdSql.query("select count(tbname) from opentsdb_telnet.stb2")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from opentsdb_telnet.stb2")
        tdSql.checkData(0, 0, 160)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
