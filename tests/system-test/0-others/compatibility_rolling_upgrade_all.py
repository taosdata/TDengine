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

from urllib.parse import uses_relative
import os
import platform
from taos.tmq import Consumer
from taos.tmq import *
from .compatibility_basic import cb
import socket

from pathlib import Path
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import tdDnodes
from util.cluster import *


class TDTestCase:
    def caseDescription(self):
        f'''
        TDengine Data Compatibility Test 
        Testing compatibility from the following base versions to current version: {BASE_VERSIONS}
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def getDnodePath(self):
        buildPath = self.getBuildPath()
        dnodePaths = [buildPath + "/../sim/dnode1/", buildPath + "/../sim/dnode2/", buildPath + "/../sim/dnode3/"]
        return dnodePaths
    
    def getLastBigVersion(self):
        tdSql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdSql.queryResult[0][0]
        tdLog.info(f"Now server version is {nowServerVersion}")
        # get the last big version
        lastBigVersion = nowServerVersion.split(".")[0]+"."+nowServerVersion.split(".")[1]+"."+nowServerVersion.split(".")[2]+"."+"0"

        tdLog.info(f"Last big version is {lastBigVersion}")
        return lastBigVersion

    def run(self):
        hostname = socket.gethostname()
        tdLog.info(f"hostname: {hostname}")
        lastBigVersion = self.getLastBigVersion()

        tdDnodes.stopAll()
        
        cb.installTaosdForRollingUpgrade(self.getDnodePath(), lastBigVersion)
        
        tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
        tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

        time.sleep(10)

        cb.prepareDataOnOldVersion(lastBigVersion, self.getBuildPath(),corss_major_version=False)

        cb.updateNewVersion(self.getBuildPath(),self.getDnodePath(),1)

        time.sleep(10)

        cb.verifyData(corss_major_version=False)

        cb.verifyBackticksInTaosSql(self.getBuildPath())

        

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())