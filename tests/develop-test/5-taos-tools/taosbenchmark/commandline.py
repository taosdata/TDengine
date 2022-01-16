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

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        cmd = "taosBenchmark -F 7 -n 10 -t 2 -x -y -M -C -d newtest -l 5 -A binary,nchar\(31\) -b tinyint,binary\(23\),bool,nchar -w 29 -E -m $%^*"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use newtest")
        tdSql.query("select count(*) from newtest.meters")
        tdSql.checkData(0, 0, 20)
        tdSql.query("describe meters")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TINYINT")
        tdSql.checkData(2, 1, "BINARY")
        tdSql.checkData(2, 2, 23)
        tdSql.checkData(3, 1, "BOOL")
        tdSql.checkData(4, 1, "NCHAR")
        tdSql.checkData(4, 2, 29)
        tdSql.checkData(5, 1, "INT")
        tdSql.checkData(6, 1, "BINARY")
        tdSql.checkData(6, 2, 29)
        tdSql.checkData(6, 3, "TAG")
        tdSql.checkData(7, 1, "NCHAR")
        tdSql.checkData(7, 2, 31)
        tdSql.checkData(7, 3, "TAG")
        tdSql.query("select tbname from meters where tbname like '$%^*%'")
        tdSql.checkRows(2)
        tdSql.execute("drop database if exists newtest")

        cmd = "taosBenchmark -F 7 -n 10 -t 2 -y -M -I stmt"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(tbname) from test.meters")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = "taosBenchmark -n 3 -t 3 -B 2 -i 1 -G -y -T 1 2>&1 | grep sleep | wc -l"
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 2):
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = "taosBenchmark -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 2>&1 | grep sleep | wc -l"
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 3):
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "taosBenchmark -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I sml 2>&1 | grep sleep | wc -l"
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 2):
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = "taosBenchmark -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I sml 2>&1 | grep sleep | wc -l"
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 3):
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "taosBenchmark -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I stmt 2>&1 | grep sleep | wc -l"
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 2):
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = "taosBenchmark -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I stmt 2>&1 | grep sleep | wc -l"
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if (int(sleepTimes) != 3):
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "taosBenchmark -S 17 -n 3 -t 1 -y -x"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select last(ts) from test.meters")
        tdSql.checkData(0, 0 , "2017-07-14 10:40:00.034")

        cmd = "taosBenchmark -N -I taosc -t 11 -n 11 -y -x -E"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from `d10`")
        tdSql.checkData(0, 0, 11)

        cmd = "taosBenchmark -N -I rest -t 11 -n 11 -y -x"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "taosBenchmark -N -I stmt -t 11 -n 11 -y -x"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "taosBenchmark -N -I sml -y"
        tdLog.info("%s" % cmd)
        assert(os.system("%s" % cmd) !=0 )

        cmd = "taosBenchmark -n 1 -t 1 -y -b bool"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BOOL")

        cmd = "taosBenchmark -n 1 -t 1 -y -b tinyint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT")

        cmd = "taosBenchmark -n 1 -t 1 -y -b utinyint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT UNSIGNED")

        cmd = "taosBenchmark -n 1 -t 1 -y -b smallint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT")

        cmd = "taosBenchmark -n 1 -t 1 -y -b usmallint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT UNSIGNED")

        cmd = "taosBenchmark -n 1 -t 1 -y -b int"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT")

        cmd = "taosBenchmark -n 1 -t 1 -y -b uint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT UNSIGNED")

        cmd = "taosBenchmark -n 1 -t 1 -y -b bigint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT")

        cmd = "taosBenchmark -n 1 -t 1 -y -b ubigint"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT UNSIGNED")

        cmd = "taosBenchmark -n 1 -t 1 -y -b timestamp"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TIMESTAMP")

        cmd = "taosBenchmark -n 1 -t 1 -y -b float"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "FLOAT")

        cmd = "taosBenchmark -n 1 -t 1 -y -b double"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "DOUBLE")

        cmd = "taosBenchmark -n 1 -t 1 -y -b nchar"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "taosBenchmark -n 1 -t 1 -y -b nchar\(7\)"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "taosBenchmark -n 1 -t 1 -y -b binary"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BINARY")

        cmd = "taosBenchmark -n 1 -t 1 -y -b binary\(7\)"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BINARY")

        cmd = "taosBenchmark -n 1 -t 1 -y -A json\(7\)"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(4, 1, "JSON")

        cmd = "taosBenchmark -n 1 -t 1 -y -b int,x"
        tdLog.info("%s" % cmd)
        assert(os.system("%s" % cmd) != 0)

        cmd = "taosBenchmark -n 1 -t 1 -y -A int,json"
        tdLog.info("%s" % cmd)
        assert(os.system("%s" % cmd) != 0)




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())