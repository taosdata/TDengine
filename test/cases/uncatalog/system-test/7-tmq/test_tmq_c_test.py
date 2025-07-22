
import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom
from taos.tmq import *

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def checkData(self):
        tdSql.execute('use db_taosx')
        tdSql.query("select * from ct0")
        tdSql.checkRows(2)
        tdSql.checkData(0, 4, 23.23)

        return
    def test_tmq_c(self):
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
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_write_raw_test'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        cmdStr = '%s/build/bin/tmq_ts5776'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        cmdStr = '%s/build/bin/tmq_td33798'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        cmdStr = '%s/build/bin/tmq_poll_test'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()

        tdLog.success(f"{__file__} successfully executed")

