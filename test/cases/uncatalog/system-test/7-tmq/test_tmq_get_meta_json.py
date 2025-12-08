import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_get_meta_json(self):
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
        cmdStr = '%s/build/bin/tmq_get_meta_json'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("tmq_get_meta_json != 0")

        tdLog.success(f"{__file__} successfully executed")
