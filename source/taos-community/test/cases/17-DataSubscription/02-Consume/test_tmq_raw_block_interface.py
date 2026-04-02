
import taos
import sys
import time
import socket
import os
import platform
import threading
import subprocess

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
        try:
            result = subprocess.run(
                cmdStr,
                shell=True,
                timeout=60,  # 防止卡死
                capture_output=True
            )
            retCode = result.returncode
            tdLog.info(f"{cmdStr} returncode={retCode}")
            if retCode != 0:
                tdLog.error(f"stdout: {result.stdout.decode(errors='ignore')}")
                tdLog.error(f"stderr: {result.stderr.decode(errors='ignore')}")
                tdLog.exit(f"run {cmdStr} failed, return code = {retCode}.")
                return
        except subprocess.TimeoutExpired:
            tdLog.exit(f"run {cmdStr} timeout, process killed.")
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


