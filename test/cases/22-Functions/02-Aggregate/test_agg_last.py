import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster,tdCom
from random import randint
import os
import subprocess
import json
import random
import time
import datetime

class Test_Last:
    caseName = "test_last"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test"
    stbname= "meters"
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_last(self):
        """Agg-basic: last

        Test the LAST function

        Catalog:
            - Function:Aggregate

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-18 Stephen Jin

        """
            
        self.prepareHistoryData()
        self.insertNowData()
        self.checkResult()
        
    def prepareHistoryData(self):
        cmd = f"taosBenchmark -t 100 -n 10000 -y"
        ret = os.system(cmd)
        if ret != 0:
            raise Exception("taosBenchmark run failed")
        time.sleep(5)
        tdLog.info(f"Prepare history data:taosBenchmark -t 100 -n 10000 -y")
        
    def insertNowData(self):
        tdSql.execute(f"use {self.dbname}")

        tdSql.execute(f"insert into {self.dbname}.d1 values (1759194759000, 1.1, 1.1, 245)")
        tdSql.execute(f"insert into {self.dbname}.d15 values (1759194759001, 1.1, 1.1, 245)")

    def checkResult(self):
        tdSql.query(f"select last(ts) from {self.dbname}.{self.stbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1759194759001)
        
        tdSql.query(f"select last(ts), first(ts) from {self.dbname}.{self.stbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1759194759001)
        
        tdSql.query(f"select first(ts), last(ts) from {self.dbname}.{self.stbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1759194759001)

