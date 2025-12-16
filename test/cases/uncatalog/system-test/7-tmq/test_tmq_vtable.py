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
        tdSql.execute(f'create database if not exists db_taosx vgroups 1')
        tdSql.execute(f'create database if not exists db_5466  vgroups 1')
        tdSql.execute(f'use db_5466')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))')
        tdSql.execute(f'create stable if not exists vs5466 (ts timestamp, c1 int, c2 int) tags (t binary(32)) virtual 1')
        tdSql.execute(f'insert into t1 using s5466 tags("__devicid__") values(1669092069068, 0, 1)')
        tdSql.execute(f'create vtable vt1 (c1 from db_5466.t1.c1, c2 from db_5466.t1.c2) using vs5466 tags("__devicid__")')

        tdSql.execute(f'alter stable s5466 add column c5 nchar(16)')
        tdSql.execute(f'alter vtable vt1 alter column c1 set db_5466.t1.c2')
        tdSql.execute(f'alter stable s5466 drop column c1')

        tdSql.execute("create topic db_5466_topic with meta as database db_5466")
        buildPath = tdCom.getBuildPath()
        # cmdStr = '%s/build/bin/tmq_ts5466'%(buildPath)
        cmdStr = '/root/code/TDengine/debug/build/bin/tmq_ts5466'
        tdLog.info(cmdStr)
        os.system(cmdStr)

        tdLog.success(f"{__file__} successfully executed")
