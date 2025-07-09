
import taos
import sys
import time
import socket
import os
import threading
import platform
from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_offset(self):
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

        if platform.system().lower() == 'windows':
            buildPath = tdCom.getBuildPath()
            cmdStr1 = ' mintty -h never %s/build/bin/taosBenchmark -i 50 -v 1 -B 1 -t 1000 -n 100000 -y '%(buildPath)
            tdLog.info(cmdStr1)
            os.system(cmdStr1)
            time.sleep(15)

            cmdStr2 = ' mintty -h never %s/build/bin/tmq_offset_test '%(buildPath)
            tdLog.info(cmdStr2)
            os.system(cmdStr2)
            time.sleep(20)

            # tdLog.info("ps -a | grep taos | awk \'{print $2}\' | xargs kill -9")
            os.system('ps -a | grep taosBenchmark | awk \'{print $2}\' | xargs kill -9')
            result = os.system('ps -a | grep tmq_offset_test | awk \'{print $2}\' | xargs kill -9')
            if result != 0:
                tdLog.exit("tmq_offset_test error!")
        else:
            buildPath = tdCom.getBuildPath()
            cmdStr0 = '%s/build/bin/tmq_offset_test 5679'%(buildPath)
            tdLog.info(cmdStr0)
            if os.system(cmdStr0) != 0:
                tdLog.exit(cmdStr0)

            cmdStr1 = '%s/build/bin/taosBenchmark -i 50 -v 1 -B 1 -t 1000 -n 100000 -y &'%(buildPath)
            tdLog.info(cmdStr1)
            os.system(cmdStr1)
            time.sleep(15)

            cmdStr2 = '%s/build/bin/tmq_offset_test &'%(buildPath)
            tdLog.info(cmdStr2)
            os.system(cmdStr2)
            time.sleep(20)

            os.system("kill -9 `pgrep taosBenchmark`")
            result = os.system("kill -9 `pgrep tmq_offset_test`")
            if result != 0:
                tdLog.exit("tmq_offset_test error!")

        tdLog.success(f"{__file__} successfully executed")

