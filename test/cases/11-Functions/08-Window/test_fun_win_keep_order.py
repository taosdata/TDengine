import math
import numpy as np
import platform
import random
import re
import subprocess
import sys
import time
import os

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck,tdDnodes
from wsgiref.headers import tspecials

msec_per_min=60*1000


class TestKeepOrderFunc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------ main ------------------
    #
    def test_win_keep_order_func(self):
        """ Fun: KEEP_ORDER_FUNC
        
        1. Functionality test for KEEP_ORDER_FUNC
        2. Query on super/child table
        3. Query with interval/state_window/session_window/count_window
        4. Query with max/min/statecount/stateduration/mavg/tail/mode
        5. Query with top/bottom/sample

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
        History:
            - 2026-01-19 xsRen Create the KEEP_ORDER_FUNC function test case

        """
        
        self.prepare_data()
        testCases = [
            "test_win_keep_order_func_baisic",
        ]
        for testCase in testCases:
            tdLog.info(f"test {testCase} case")
            self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
            self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

            tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
     
    def join_with_const_condition(self):
        tdLog.info("test join with constant condition")
        
        # unique result more than 1 rows and no order by clause
        tdSql.error(
            f"select b.*, a.ats from (select ts ats, unique(f) from sst) as a inner join sst b on timetruncate(a.ats, 1s) = timetruncate(b.ts, 1s) and b.ts > a.ats-5a and b.ts < a.ats + 5a"
        )
        

    def prepare_data(self):
        ts = 1741757485230
        tdSql.execute("drop database if exists keeporderdb")
        tdSql.execute("create database keeporderdb vgroups 2 replica 1")
        tdSql.execute("use keeporderdb")
        tdSql.execute("CREATE STABLE keeporderdb.`meters` (`ts` TIMESTAMP, `ts2` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24))")
        for tableIndex in range(10):
            tdSql.execute(f"CREATE TABLE keeporderdb.t{tableIndex} USING keeporderdb.meters TAGS ({tableIndex}, 'tb{tableIndex}')")
            for num in range(700):
                if(num >= 10 and num < 11) or (num >= 30 and num < 32) or (num >= 50 and num < 53) or (num >= 80 and num < 84) or \
                    (num >= 120 and num < 125) or (num >= 170 and num < 176) or (num >= 230 and num < 237) or (num >= 300 and num < 308) or \
                    (num >= 380 and num < 389) or (num >= 470 and num < 480) or (num >= 570 and num < 581) or (num >= 680 and num < 692):
                    continue
                tdSql.execute(f"INSERT INTO keeporderdb.t{tableIndex} VALUES({ts + num * 1000}, {ts + (num % 13) * 1000}, {num * 1.0}, {215 + num/15}, 0.0)")
        