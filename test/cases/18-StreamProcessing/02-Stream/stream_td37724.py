import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestSnodeMgmt:
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Stream bug TD-37724
        
        1. Check stream td37724 

        Catalog:
            - Streams:create stream

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-5 mark wang Created

        """


        self.prepareData()
        # 创建一个 stream
        self.createOneStream()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "create snode on dnode 1;",
            "create database stream_from;",
            "create database stream_to;",
            "create table stream_from.stb (ts timestamp, c0 int, c1 int, c2 int, c3 int) tags(t1 int);"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")
    
    
    def createOneStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream stream_from.period_stb_agg_sourcetable_stb period(15s) from stream_from.stb into stream_to.period_stb_agg_sourcetable_stb as select cast(_tlocaltime/1000000 as timestamp) ts, avg(c0), avg(c1), avg(c2), avg(c3), max(c0), max(c1), max(c2), max(c3), min(c0), min(c1), min(c2), min(c3) from stream_from.stb where ts >= _tprev_localtime/1000000 and ts <= _tnext_localtime/1000000;"
        )
        tdLog.info(f"create stream:{sql}")
        try:
            tdSql.execute(sql)
        except Exception as e:
                if "No stream available snode now" not in str(e):
                    raise Exception(f" user cant  create stream no snode ,but create success")
    