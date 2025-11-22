import taos
import sys
import time
import socket
import os
import threading
import subprocess

# 基本用法
result = subprocess.run(['ls', '-l'], capture_output=True, text=True)
print(result.stdout)
print(result.returncode)

# 使用shell
result = subprocess.run('ls -l | grep txt', shell=True, capture_output=True, text=True)

from new_test_framework.utils import tdLog, tdSql, tdCom
from taos.tmq import *

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_ts7402(self):
        """summary: test subscribe topic of stable with tag filter

        description: fix with tag filter

        Since: 2025-11-20

        Labels: tmq

        Jira: https://jira.taosdata.com:18080/browse/TS-7662

        Catalog:
        - taosd:tmq

        History:
        - created in 2025-11-20 by markswang

        """
        tdSql.execute(f'create database if not exists db_7662 vgroups 1')
        tdSql.execute(f'use db_7662')
        tdSql.execute(f'create stable if not exists ts7662 (ts timestamp, c1 decimal(8,2)) tags (t binary(32))')
        tdSql.execute(f'create table t1 using ts7662 tags("t1") t2 using ts7662 tags("t2")')

        tdSql.execute("create topic topic_ts7662 with meta as stable ts7662 where t='t1'")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts7662'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("varbinary_test ret != 0")

        return

        tdLog.success(f"{__file__} successfully executed")
        