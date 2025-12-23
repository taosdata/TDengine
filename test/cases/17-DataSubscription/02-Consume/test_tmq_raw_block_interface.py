
import taos
import sys
import time
import socket
import os
import platform
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def checkData(self):
        tdSql.execute('use db_raw')
        tdSql.query("select ts, current, voltage, phase from d1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 120)
        #tdSql.checkData(0, 4, 2.32) ## currently py conector does not support decimal

        tdSql.query("select ts, current, voltage, phase from d2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        return

    def check(self):
        buildPath = tdCom.getBuildPath()
        exe_file = 'write_raw_block_test' if platform.system().lower() != 'windows' else 'write_raw_block_test.exe'
        cmdStr = os.path.join(buildPath, 'build', 'bin', exe_file)
        tdLog.info(cmdStr)
        retCode = os.system(cmdStr)
        # run program code from system return , 0 is success
        runCode = retCode & 0xFF
        # program retur code from main function
        progCode = retCode >> 8

        tdLog.info(f"{cmdStr} ret={retCode} runCode={runCode} progCode={progCode}")

        if runCode != 0:
            tdLog.exit(f"run {cmdStr} failed, have system error.")
            return
        
        if progCode != 0:
            tdLog.exit(f"{cmdStr} found problem, return code = {progCode}.")
            return

        self.checkData()

        return

    def test_raw_block(self):
        """Advanced: raw block interface
        
        1. Use raw block API for consumption
        2. Fetch data in block format
        3. Parse block structure
        4. Verify block metadata
        5. Check performance benefit
        6. Validate block data
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_raw_block_interface_test.py

        """
        tdSql.prepare()
        self.check()
        tdLog.success(f"{__file__} successfully executed")

