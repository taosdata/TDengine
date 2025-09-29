import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_ts5466(self):
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
        tdSql.execute(f'create database if not exists test')
        tdSql.execute(f'use test')
        tdSql.execute(f'create stable if not exists td37821 (ts timestamp, c1 int, c2 int) tags (t binary(32))')
        tdSql.execute(f'insert into t1 using td37821 tags("__devicid__") values(1669092069068, 0, 1)')

        tdSql.execute("create topic test_37821_topic with meta as stable td37821")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td37821'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        tdLog.success(f"{__file__} successfully executed")
