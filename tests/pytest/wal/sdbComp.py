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

from distutils.log import debug
import sys
import os
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        global selfPath
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

        # set path para
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)

        binPath = buildPath+ "/build/bin/"
        testPath = selfPath+ "/../../../"
        walFilePath = testPath + "/sim/dnode1/data/mnode_bak/wal/"

        #new db and insert data
        os.system("rm -rf  %s/sim/dnode1/data/mnode_tmp/" % testPath)
        os.system("rm -rf  %s/sim/dnode1/data/mnode_bak/" % testPath)
        tdSql.execute("drop database if exists db2")
        os.system("%staosdemo -f wal/insertDataDb1.json -y " % binPath)
        tdSql.execute("drop database if exists db1")
        os.system("%staosdemo -f wal/insertDataDb2.json -y " % binPath)
        tdSql.execute("drop table if exists db2.stb0")
        os.system("%staosdemo -f wal/insertDataDb2Newstab.json -y " % binPath)
        query_pid1 = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
        print(query_pid1)
        tdSql.execute("use db2")
        tdSql.execute("drop table if exists stb1_0")
        tdSql.execute("drop table if exists stb1_1")
        tdSql.execute("insert into stb0_0 values(1614218412000,8637,78.861045,'R','bf3')(1614218422000,8637,98.861045,'R','bf3')")
        tdSql.execute("alter table db2.stb0 add column c4 int")
        tdSql.execute("alter table db2.stb0 drop column c2")
        tdSql.execute("alter table db2.stb0  add tag t3 int;")
        tdSql.execute("alter table db2.stb0 drop tag t1")
        tdSql.execute("create table  if not exists stb2_0 (ts timestamp, c0 int, c1 float)  ")
        tdSql.execute("insert into stb2_0 values(1614218412000,8637,78.861045)")
        tdSql.execute("alter table stb2_0 add column c2 binary(4)")
        tdSql.execute("alter table stb2_0 drop column c1")
        tdSql.execute("insert into stb2_0 values(1614218422000,8638,'R')")

        # stop taosd and compact wal file
        tdDnodes.stop(1)
        sleep(10)
        os.system("nohup %s/taosd  --compact-mnode-wal  -c %s/sim/dnode1/cfg/ & " %(binPath,testPath) )
        sleep(5)
        assert os.path.exists(walFilePath) , "%s is not generated, compact didn't  take effect " % walFilePath

        # use new wal file to start  taosd
        tdDnodes.start(1)
        sleep(5)
        tdSql.execute("reset query cache")
        query_pid2 = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
        print(query_pid2)

        # verify that the data is correct
        tdSql.execute("use db2")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkRows(0)
        tdSql.query("select count(*) from stb0_0")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from stb2_0")
        tdSql.checkData(0, 0, 2)

        # delete useless file
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf wal/%s.sql" % testcaseFilename)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
