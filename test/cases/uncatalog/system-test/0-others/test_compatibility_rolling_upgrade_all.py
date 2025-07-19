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

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import time
import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from compatibility_basic import cb


class TestCompatibilityRollingUpgradeAll:
    def caseDescription(self):
        f'''
        TDengine Data Compatibility Test 
        Testing compatibility from the following base versions to current version: {BASE_VERSIONS}
        '''
        return

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    def getDnodePath(self):
        buildPath = tdCom.getBuildPath()
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

    def test_compatibility_rolling_upgrade_all(self):
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
        hostname = self.host
        tdLog.info(f"hostname: {hostname}")
        lastBigVersion = self.getLastBigVersion()

        tdDnodes.stopAll()
        
        cb.installTaosdForRollingUpgrade(self.getDnodePath(), lastBigVersion)
        
        tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
        tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

        time.sleep(10)

        cb.prepareDataOnOldVersion(lastBigVersion, tdCom.getBuildPath(),corss_major_version=False)

        cb.updateNewVersion(tdCom.getBuildPath(),self.getDnodePath(),1)

        time.sleep(10)

        cb.verifyData(corss_major_version=False)

        cb.verifyBackticksInTaosSql(tdCom.getBuildPath())

        tdLog.success(f"{__file__} successfully executed")


