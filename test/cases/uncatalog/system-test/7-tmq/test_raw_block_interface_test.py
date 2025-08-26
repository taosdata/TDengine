
import taos
import sys
import time
import socket
import os
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
        cmdStr = '%s/build/bin/write_raw_block_test'%(buildPath)
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
        tdSql.prepare()
        self.check()
        tdLog.success(f"{__file__} successfully executed")

