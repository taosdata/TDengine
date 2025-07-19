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

    def test_td_32187(self):
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
        tdSql.execute(f'create database if not exists db_32187')
        tdSql.execute(f'use db_32187')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))')
        tdSql.execute(f'insert into t1 using s5466 tags("__devicid__") values(1669092069068, 0, 1)')
        tdSql.execute(f'insert into t1(ts, c1, c2) values(1669092069067, 0, 1)')

        tdSql.execute("create topic topic_test with meta as database db_32187")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32187'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        tdLog.success(f"{__file__} successfully executed")

