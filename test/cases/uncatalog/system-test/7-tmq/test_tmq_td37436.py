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

    def test_tmq_td37436(self):
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
        tdSql.execute(f'create snode on dnode 1');
        tdSql.execute(f'drop database if exists test');
        tdSql.execute(f'create database test vgroups 1');
        tdSql.execute(f'use test');
        tdSql.execute(f'create table stream_query (ts timestamp, id int)');
        tdSql.execute(f'create stream s1 session (ts, 1s) from stream_query stream_options(fill_history) into stream_out as select _twstart, avg(id) from stream_query')
        while 1:
            tdSql.query("show test.streams")
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getData(0, 1) == "Running":
                break
            else:
                time.sleep(0.5)

        tdSql.execute(f"insert into test.stream_query values ('2025-01-01 00:00:01', 0), ('2025-01-01 00:00:04', 1), ('2025-01-01 00:00:05', 2);")

        tdSql.execute("create topic tt with meta as database test")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td37436'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            raise Exception("run failed")

        tdLog.success(f"{__file__} successfully executed")
