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
        tdSql.init(conn.cursor())

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
        tAdapter.deploy()
        tAdapter.start()
        binPath = self.getPath()
        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/taosc_auto_create_table.json" % binPath
        tdLog.info("%s" % cmd)
        os.system(cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from db.stb1)")
        tdSql.checkData(0, 0, 8)
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
        tdSql.query("select count(*) from (select distinct(tbname) from db.`stb1-2`)")
        tdSql.checkData(0, 0, 8)
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

        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/stmt_auto_create_table.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from stmt_db.stb2)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from stmt_db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select * from information_schema.ins_databases where name='stmt_db'")
        tdSql.checkData(0, 14, "us")

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from stmt_db.`stb2-2`)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from stmt_db.`stb2-2`")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/rest_auto_create_table.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from rest_db.stb3)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from rest_db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select * from information_schema.ins_databases where name='rest_db'")
        tdSql.checkData(0, 14, "ns")

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from rest_db.`stb3-2`)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from rest_db.`stb3-2`")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f ./5-taos-tools/taosbenchmark/json/sml_auto_create_table.json" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from sml_db.stb4)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from sml_db.stb4")
        tdSql.checkData(0, 0, 160)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from (select distinct(tbname) from sml_db.`stb4-2`)")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from sml_db.`stb4-2`")
        tdSql.checkData(0, 0, 160)

        tAdapter.stop()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
