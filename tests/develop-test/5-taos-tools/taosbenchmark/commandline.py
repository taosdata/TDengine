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
import subprocess
import time

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
        cmd = "%s -F 7 -n 10 -t 2 -x -y -M -C -d newtest -l 5 -A binary,nchar\(31\) -b tinyint,binary\(23\),bool,nchar -w 29 -E -m $%%^*" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use newtest")
        tdSql.query("select count(*) from newtest.meters")
        tdSql.checkData(0, 0, 20)
        tdSql.query("describe meters")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TINYINT")
        tdSql.checkData(2, 1, "VARCHAR")
        tdSql.checkData(2, 2, 23)
        tdSql.checkData(3, 1, "BOOL")
        tdSql.checkData(4, 1, "NCHAR")
        tdSql.checkData(4, 2, 29)
        tdSql.checkData(5, 1, "INT")
        tdSql.checkData(6, 1, "VARCHAR")
        tdSql.checkData(6, 2, 29)
        tdSql.checkData(6, 3, "TAG")
        tdSql.checkData(7, 1, "NCHAR")
        tdSql.checkData(7, 2, 31)
        tdSql.checkData(7, 3, "TAG")
        tdSql.query("select distinct(tbname) from meters where tbname like '$%^*%'")
        tdSql.checkRows(2)
        tdSql.execute("drop database if exists newtest")

        cmd = "%s -F 7 -n 10 -t 2 -y -M -I stmt" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from (select distinct(tbname) from test.meters)")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 2>&1 | grep sleep | wc -l" %binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 2):
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 2>&1 | grep sleep | wc -l" %binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 3):
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I sml 2>&1 | grep sleep | wc -l" %binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 2):
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I sml 2>&1 | grep sleep | wc -l" %binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 3):
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I stmt 2>&1 | grep sleep | wc -l" %binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 2):
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I stmt 2>&1 | grep sleep | wc -l" %binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 3):
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "%s -S 17 -n 3 -t 1 -y -x" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select last(ts) from test.meters")
        tdSql.checkData(0, 0 , "2017-07-14 10:40:00.034")

        cmd = "%s -N -I taosc -t 11 -n 11 -y -x -E" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from `d10`")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I rest -t 11 -n 11 -y -x" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I stmt -t 11 -n 11 -y -x" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I sml -y" %binPath
        tdLog.info("%s" % cmd)
        assert(os.system("%s" % cmd) !=0 )

        cmd = "%s -n 1 -t 1 -y -b bool" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BOOL")

        cmd = "%s -n 1 -t 1 -y -b tinyint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT")

        cmd = "%s -n 1 -t 1 -y -b utinyint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b smallint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT")

        cmd = "%s -n 1 -t 1 -y -b usmallint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b int" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT")

        cmd = "%s -n 1 -t 1 -y -b uint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b bigint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT")

        cmd = "%s -n 1 -t 1 -y -b ubigint" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b timestamp" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TIMESTAMP")

        cmd = "%s -n 1 -t 1 -y -b float" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "FLOAT")

        cmd = "%s -n 1 -t 1 -y -b double" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "DOUBLE")

        cmd = "%s -n 1 -t 1 -y -b nchar" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "%s -n 1 -t 1 -y -b nchar\(7\)" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "%s -n 1 -t 1 -y -b binary" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "VARCHAR")

        cmd = "%s -n 1 -t 1 -y -b binary\(7\)" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "VARCHAR")

        cmd = "%s -n 1 -t 1 -y -A json\(7\)" %binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(4, 1, "JSON")

        cmd = "%s -n 1 -t 1 -y -b int,x" %binPath
        tdLog.info("%s" % cmd)
        assert(os.system("%s" % cmd) != 0)

        cmd = "%s -n 1 -t 1 -y -A int,json" %binPath
        tdLog.info("%s" % cmd)
        assert(os.system("%s" % cmd) != 0)




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
