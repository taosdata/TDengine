import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    updatecfgDict = {'debugFlag': 143, 'asynclog': 0}

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_td32471(self):
        """summary: test tmq connection show connections

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - created by Mark Wang 2025-11-15

        """
        tdSql.execute(f'create database if not exists db vgroups 1')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco1', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")


        tdSql.execute(f'create topic t0 as select * from meters')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)
        tdSql.query("show connections")
        tdSql.checkRows(2)
        time.sleep(10)
        tdSql.query("show connections")
        tdSql.checkRows(2)

        consumer.close()

        tdLog.success(f"{__file__} successfully executed")
