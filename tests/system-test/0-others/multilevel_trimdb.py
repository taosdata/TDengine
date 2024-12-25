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


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import glob
import os

def scanFiles(pattern):
    res = []
    for f in glob.iglob(pattern):
        res += [f]
    return res

def checkFiles(pattern, state):
    res = scanFiles(pattern)
    tdLog.info(res)
    num = len(res)
    if num:
        if state:
            tdLog.info("%s: %d files exist. expect: files exist" % (pattern, num))
        else:
            tdLog.exit("%s: %d files exist. expect: files not exist." % (pattern, num))
    else:
        if state:
            tdLog.exit("%s: %d files exist. expect: files exist" % (pattern, num))
        else:
            tdLog.info("%s: %d files exist. expect: files not exist." % (pattern, num))

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):

        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        buildPath = ""
        for root, dirs, files in os.walk(projPath):
            if ("taosdump" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def trim_database(self):
        tdLog.info("============== trim_database test ===============")
        tdDnodes.stop(1)
        cfg = {
            '/mnt/data1 0 1' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)

        tdSql.execute('create database dbtest duration 60m keep 1d,365d,3650d')
        tdSql.execute('use dbtest')
        tdSql.execute('create table stb (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute('create table tb1 using stb tags(1)')
        for i in range(1,600, 30):
            tdSql.execute(f'insert into tb1 values(now-{i}d,10)')
        tdSql.execute('flush database dbtest')
        time.sleep(3)
        checkFiles('/mnt/data1/vnode/*/tsdb/v*',1)
        tdDnodes.stop(1)

        cfg={
            '/mnt/data1 0 1' : 'dataDir',
            '/mnt/data2 1 0 0' : 'dataDir',
            '/mnt/data3 1 0 1' : 'dataDir',
            '/mnt/data4 2 0' : 'dataDir',
        }
        tdSql.createDir('/mnt/data2')
        tdSql.createDir('/mnt/data3')
        tdSql.createDir('/mnt/data4')
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1) 

        checkFiles('/mnt/data1/vnode/*/tsdb/v*',1)
        checkFiles('/mnt/data2/vnode/*/tsdb/v*',0)
        checkFiles('/mnt/data3/vnode/*/tsdb/v*',0)
        checkFiles('/mnt/data4/vnode/*/tsdb/v*',0)
        tdSql.execute('alter database dbtest keep 1d,365d,3650d')

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        #-N:regular table  -d:database name   -t:table num  -n:rows num per table  -l:col num  -y:force
        #regular old && new
        os.system("%staosBenchmark -N -d dbtest -t 100 -n 5500000 -l 1023 -y" % binPath)

        #tdSql.execute('trim database dbtest')
        #time.sleep(3)
        #checkFiles('/mnt/data1/vnode/*/tsdb/v*',1)
        #checkFiles('/mnt/data2/vnode/*/tsdb/v*',1)
        #checkFiles('/mnt/data3/vnode/*/tsdb/v*',0)
        #checkFiles('/mnt/data4/vnode/*/tsdb/v*',1)

    def run(self):
        self.trim_database()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
