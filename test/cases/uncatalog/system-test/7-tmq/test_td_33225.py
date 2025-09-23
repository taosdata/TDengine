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

    def test_td_33225(self):
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
        tdSql.execute(f'create database if not exists db_33225')
        tdSql.execute(f'use db_33225')
        tdSql.execute(f'create stable if not exists s33225 (ts timestamp, c1 int, c2 int) tags (t binary(32), t2 int)')
        tdSql.execute(f'insert into t1 using s33225 tags("__devicid__", 1) values(1669092069068, 0, 1)')

        tdSql.execute("create topic db_33225_topic as select ts,c1,t2 from s33225")

        tdSql.execute(f'alter table s33225 modify column c2 COMPRESS "zlib"')
        tdSql.execute(f'create index dex1 on s33225(t2)')

        tdLog.success(f"{__file__} successfully executed")
