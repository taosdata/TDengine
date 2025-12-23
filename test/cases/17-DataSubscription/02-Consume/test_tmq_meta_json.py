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
        """Advanced: Metadata JSON
        
        1. Create topics with metadata
        2. Query metadata via JSON interface
        3. Verify JSON format correctness
        4. Check all metadata fields present
        5. Validate JSON schema
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_get_meta_json.py

        """
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_get_meta_json'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("tmq_get_meta_json != 0")

        tdLog.success(f"{__file__} successfully executed")
