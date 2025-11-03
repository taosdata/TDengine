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

    def test_tmq_td38404(self):
        """summary: tmq_get_json_meta

        description: tmq_get_json_meta behaves unexpectedly when the tags of subscribed meta messages contain empty strings.

        Since: v3.3.8.4

        Labels: taosx,tmq

        Jira: TD-38404

        Catalog:
        - taosc:tmq

        History:
        - 2025-10-27 Mark Wang Created

        """
        tdSql.execute(f'create database if not exists db_td38404 vgroups 1')
        tdSql.execute(f'use db_td38404')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32), t2 nchar(4))')
        tdSql.execute(f'insert into t1 using s5466 tags("","") values(1669092069068, 0, 1)')

        tdSql.execute("create topic db_38404_topic with meta as database db_td38404")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td38404'%(buildPath)
        # cmdStr = '/Users/mingming/code/TDengine2/debug/build/bin/tmq_td38404'
        tdLog.info(cmdStr)
        os.system(cmdStr)

        tdLog.success(f"{__file__} successfully executed")
