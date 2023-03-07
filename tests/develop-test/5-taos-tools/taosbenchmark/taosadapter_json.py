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
from util.taosadapter import *


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
        tAdapter.init("")
        adapter_cfg = {
            "influxdb": {
                "enable": True
            },
            "opentsdb": {
                "enable": True
            },
            "opentsdb_telnet": {
                "enable": True,
                "maxTCPConnection": 250,
                "tcpKeepAlive": True,
                "dbs": ["opentsdb_telnet", "collectd", "icinga2", "tcollector"],
                "ports": [6046, 6047, 6048, 6049],
                "user": "root",
                "password": "taosdata"
            }
        }
        binPath = self.getPath()
        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/sml_rest_telnet.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from db.stb1)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from (select distinct(tbname) from db.stb2)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/sml_rest_line.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from db2.stb1)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db2.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from (select distinct(tbname) from db2.stb2)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db2.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/sml_rest_json.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from db3.stb1)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db3.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from (select distinct(tbname) from db3.stb2)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db3.stb2")
        tdSql.checkData(0, 0, 160)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
