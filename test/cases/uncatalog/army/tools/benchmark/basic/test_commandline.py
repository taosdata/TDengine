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
from new_test_framework.utils import tdLog, tdSql, etool
import os
import time
import subprocess

class TestCommandline:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def checkVersion(self):
        # run
        outputs = etool.runBinFile("taosBenchmark", "-V")
        print(outputs)
        if len(outputs) != 4:
            tdLog.exit(f"checkVersion return lines count {len(outputs)} != 4")
        # version string len
        assert len(outputs[1]) > 24
        # commit id
        assert len(outputs[2]) > 43
        assert outputs[2][:4] == "git:"
        # build info
        assert len(outputs[3]) > 36
        assert outputs[3][:6] == "build:"

        tdLog.info("check taosBenchmark version successfully.")        


    def test_commandline(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        # check version
        self.checkVersion()

        # command line
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -F 7 -n 10 -t 2 -x -y -M -C -d newtest -l 5 -A binary,nchar\(31\) -b tinyint,binary\(23\),bool,nchar -w 29 -E -m $%%^*"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use newtest")
        tdSql.query("select count(*) from newtest.meters")
        tdSql.checkData(0, 0, 20)
        tdSql.query("describe meters")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TINYINT")
        # 2.x is binary and 3.x is varchar
        # tdSql.checkData(2, 1, "BINARY")
        tdSql.checkData(2, 2, 23)
        tdSql.checkData(3, 1, "BOOL")
        tdSql.checkData(4, 1, "NCHAR")
        tdSql.checkData(4, 2, 29)
        tdSql.checkData(5, 1, "INT")
        # 2.x is binary and 3.x is varchar
        # tdSql.checkData(6, 1, "BINARY")
        tdSql.checkData(6, 2, 29)
        tdSql.checkData(6, 3, "TAG")
        tdSql.checkData(7, 1, "NCHAR")
        tdSql.checkData(7, 2, 31)
        tdSql.checkData(7, 3, "TAG")
        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.execute("drop database if exists newtest")

        cmd = (
            "%s -t 2 -n 10 -b bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -A bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -y"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = (
            "%s -I stmt -t 2 -n 10 -b bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -A bool,tinyint,smallint,int,bigint,float,double,utinyint,usmallint,uint,ubigint,binary,nchar,timestamp,varbinary,geometry -y"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        cmd = "%s -F 7 -n 10 -t 2 -y -M -I stmt" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 20)

        # add stmt2
        cmd = "%s -F 700 -n 1000 -t 4 -y -M -I stmt2" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("show test.tables")
        tdSql.checkRows(4)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4000)

        cmd = "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 2>&1 | grep sleep | wc -l" % binPath
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 2:
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 2>&1 | grep sleep | wc -l" % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 3:
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I sml 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 2:
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I sml 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 3:
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -I stmt 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 2:
            tdLog.exit("expected sleep times 2, actual %d" % int(sleepTimes))

        cmd = (
            "%s -n 3 -t 3 -B 2 -i 1 -G -y -T 1 -r 1 -I stmt 2>&1 | grep sleep | wc -l"
            % binPath
        )
        sleepTimes = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        if int(sleepTimes) != 3:
            tdLog.exit("expected sleep times 3, actual %d" % int(sleepTimes))

        cmd = "%s -S 17 -n 3 -t 1 -y -x" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)  # to avoid invalid vgroup id
        tdSql.query("select last(ts) from test.meters")
        tdSql.checkData(0, 0, "2017-07-14 10:40:00.034")

        cmd = "%s -N -I taosc -t 11 -n 11 -y -x -E -c abcde" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from `d10`")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I rest -t 11 -n 11 -y -x -c /etc/taos" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -N -I stmt -t 11 -n 11 -y -x" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(11)
        tdSql.query("select count(*) from d10")
        tdSql.checkData(0, 0, 11)

        cmd = "%s -n 1 -t 1 -y -b bool" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BOOL")

        cmd = "%s -n 1 -t 1 -y -b tinyint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT")

        cmd = "%s -n 1 -t 1 -y -b utinyint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TINYINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b smallint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT")

        cmd = "%s -n 1 -t 1 -y -b usmallint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "SMALLINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b int" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT")

        cmd = "%s -n 1 -t 1 -y -b uint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "INT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b bigint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT")

        cmd = "%s -n 1 -t 1 -y -b ubigint" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "BIGINT UNSIGNED")

        cmd = "%s -n 1 -t 1 -y -b timestamp" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "TIMESTAMP")

        cmd = "%s -n 1 -t 1 -y -b float" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "FLOAT")

        cmd = "%s -n 1 -t 1 -y -b double" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "DOUBLE")

        cmd = "%s -n 1 -t 1 -y -b nchar" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "%s -n 1 -t 1 -y -b nchar\(7\)" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(1, 1, "NCHAR")

        cmd = "%s -n 1 -t 1 -y -A json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe test.meters")
        tdSql.checkData(4, 1, "JSON")

        cmd = "%s -f ./tools/benchmark/basic/json/insert-sample.json -j ./insert_json_res.json" % binPath
        tdLog.info("%s" % cmd)
        ret = os.system("%s" % cmd)
        if not os.path.exists("./insert_json_res.json"):
            tdLog.info(f"expect failed, JSON file does not exist, cmd={cmd} ret={ret}")

        cmd = "%s -n 1 -t 1 -y -b int,x" % binPath
        ret = os.system("%s" % cmd)
        if ret == 0:
            tdLog.exit(f"expect failed, but successful cmd= {cmd} ")
        tdLog.info(f"test except ok, cmd={cmd} ret={ret}")

        tdLog.success("%s successfully executed" % __file__)


