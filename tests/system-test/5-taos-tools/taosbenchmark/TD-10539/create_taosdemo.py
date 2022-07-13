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

import sys
import taos
import time
import os
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)


    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        os.system("rm -rf tools/taosdemoAllTest/TD-10539/create_taosdemo.py.sql")
        tdSql.prepare()

        #print("==============taosdemo,#create stable,table; insert table; show table; select table; drop table")
        self.tsdemo = "tsdemo~!.@#$%^*[]-_=+{,?.}"
        #this escape character is not support in shell  . include  & () <> | /
        os.system("%staosBenchmark -d test -E -m %s -t 10 -n 100 -l 10 -y " % (binPath,self.tsdemo))
        tdSql.execute("use test ;" )
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("show test.tables like 'tsdemo%'" )
        tdSql.checkRows(10)
        tdSql.query("show test.tables like '%s_'" %self.tsdemo)
        tdSql.checkRows(10)
        tdSql.query("select _block_dist() from `%s1`" %self.tsdemo)
        tdSql.checkRows(1)
        tdSql.query("describe test.`%s1` ; " %self.tsdemo)
        tdSql.checkRows(13)
        tdSql.query("show create table test.`%s1` ; " %self.tsdemo)
        tdSql.checkData(0, 0, self.tsdemo+str(1))
        tdSql.checkData(0, 1, "CREATE TABLE `%s1` USING `meters` TAGS (1,\"beijing\")" %self.tsdemo)

        print("==============drop table\stable")
        try:
            tdSql.execute("drop table test.`%s1` ; " %self.tsdemo)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from test.`%s1` ; " %self.tsdemo)
        tdSql.query("show test.tables like '%s_'" %self.tsdemo)
        tdSql.checkRows(9)

        try:
            tdSql.execute("drop table test.meters ")
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from test.meters ")
        tdSql.error("select * from test.`%s2` ; " %self.tsdemo)

        # Exception
        os.system("%staosBenchmark -d test -m %s -t 10 -n 100 -l 10 -y " % (binPath,self.tsdemo))
        tdSql.query("show test.tables ")
        tdSql.checkRows(0)

        #print("==============taosdemo,#create regular table; insert table; show table; select table; drop table")
        self.tsdemo = "tsdemo~!.@#$%^*[]-_=+{,?.}"
        #this escape character is not support in shell  . include  & () <> | /
        os.system("%staosBenchmark -N -E -m %s -t 10 -n 100 -l 10 -y " % (binPath,self.tsdemo))
        tdSql.execute("use test ;" )
        tdSql.query("select count(*) from `%s1`" %self.tsdemo)
        tdSql.checkData(0, 0, 100)
        tdSql.query("show test.tables like 'tsdemo%'" )
        tdSql.checkRows(10)
        tdSql.query("show test.tables like '%s_'" %self.tsdemo)
        tdSql.checkRows(10)
        tdSql.query("select _block_dist() from `%s1`" %self.tsdemo)
        tdSql.checkRows(1)
        tdSql.query("describe test.`%s1` ; " %self.tsdemo)
        tdSql.checkRows(11)
        tdSql.query("show create table test.`%s1` ; " %self.tsdemo)
        tdSql.checkData(0, 0, self.tsdemo+str(1))
        tdSql.checkData(0, 1, "CREATE TABLE `%s1` (ts TIMESTAMP,c0 FLOAT,c1 INT,c2 FLOAT,c3 INT,c4 INT,c5 INT,c6 INT,c7 INT,c8 INT,c9 INT)" %self.tsdemo)

        print("==============drop table\stable")
        try:
            tdSql.execute("drop table test.`%s1` ; " %self.tsdemo)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from test.`%s1` ; " %self.tsdemo)
        tdSql.query("show test.tables like '%s_'" %self.tsdemo)
        tdSql.checkRows(9)

        # Exception
        os.system("%staosBenchmark -N -m %s -t 10 -n 100 -l 10 -y " % (binPath,self.tsdemo))
        tdSql.query("show test.tables ")
        tdSql.checkRows(0)


        #print("==============taosdemo——json_yes,#create stable,table; insert table; show table; select table; drop table")
        os.system("%staosBenchmark -f tools/taosdemoAllTest/TD-10539/create_taosdemo_yes.json -y " % binPath)
        tdSql.execute("use dbyes")

        self.tsdemo_stable = "tsdemo_stable~!.@#$%^*[]-_=+{,?.}"
        self.tsdemo = "tsdemo~!.@#$%^*[]-_=+{,?.}"

        tdSql.query("select count(*) from dbyes.`%s`" %self.tsdemo_stable)
        tdSql.checkData(0, 0, 1000)
        tdSql.query("show dbyes.tables like 'tsdemo%'" )
        tdSql.checkRows(10)
        tdSql.query("show dbyes.tables like '%s_'" %self.tsdemo)
        tdSql.checkRows(10)
        tdSql.query("select _block_dist() from `%s1`" %self.tsdemo)
        tdSql.checkRows(1)
        tdSql.query("describe dbyes.`%s1` ; " %self.tsdemo)
        tdSql.checkRows(13)
        tdSql.query("show create table dbyes.`%s1` ; " %self.tsdemo)
        tdSql.checkData(0, 0, self.tsdemo+str(1))
        tdSql.checkData(0, 1, "CREATE TABLE `%s1` USING `%s` TAGS (1,1)" %(self.tsdemo,self.tsdemo_stable))

        print("==============drop table\stable")
        try:
            tdSql.execute("drop table dbyes.`%s1` ; " %self.tsdemo)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from dbyes.`%s1` ; " %self.tsdemo)
        tdSql.query("show dbyes.tables like '%s_'" %self.tsdemo)
        tdSql.checkRows(9)

        try:
            tdSql.execute("drop table dbyes.`%s` ; " %self.tsdemo_stable)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from dbyes.`%s` ; " %self.tsdemo_stable)
        tdSql.error("select * from dbyes.`%s2` ; " %self.tsdemo)
       
        #print("==============taosdemo——json_no,#create stable,table; insert table; show table; select table; drop table")

        os.system("%staosBenchmark -f tools/taosdemoAllTest/TD-10539/create_taosdemo_no.json -y " % binPath) 
        tdSql.query("show dbno.tables;")
        tdSql.checkRows(0)
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
