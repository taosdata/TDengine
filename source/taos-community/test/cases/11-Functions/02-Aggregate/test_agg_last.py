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

    def test_last_tag(self):
        """Agg: last/last_row with tag

        description: verify the behavior of selecting last/last_row with tag column outside.
                    For example: select last(ts), tag1, tag2 from stable group by tbname.
                    In this case, we should read cache data to get the tag column value.

        Since: ver-3.4.0.0

        Labels: last/last_row, tag

        Jira: TS-6146

        Catalog:
            - xxx:xxx

        History:
            - Tony Zhang, 2025/10/10, Created

        """
        tdSql.execute("create database test_last_tag cachemodel 'both' keep 3650;")
        tdSql.execute("use test_last_tag;")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags (tag1 int, tag2 float)")

        tdSql.execute("create table tb1 using stb tags (1, 1.1);")
        tdSql.execute("create table tb2 using stb tags (2, 2.2);")

        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:00', 0);")
        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:02', 2);")
        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:04', 4);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:01', 1);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:03', 3);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:05', null);")

        tdCom.compare_testcase_result(
            "cases/11-Functions/resource/in/last_tag.in",
            "cases/11-Functions/resource/ans/last_tag.csv",
            "test_last_tag")

    def test_last_pk(self):
        """Agg-basic: last with pk

        Test the LAST function with composite key outside.
        For example: select last(ts), pk from stb group by tbname.

        Catalog:
            - Function:Aggregate

        Since: v3.4.0.0

        Labels: last/last_row, composite key

        Jira: TD-38004

        History:
            - Tony zhang, 2025/10/10, created

        """
        tdSql.execute("create database if not exists test_last_pk cachemodel 'both' keep 3650")
        tdSql.execute("use test_last_pk")
        tdSql.execute("create table stb (ts timestamp,a int COMPOSITE key,b int,c int) tags(ta int,tb int,tc int)")
        tdSql.execute("create table aaat1 using stb tags(1,1,1)")
        tdSql.execute("create table bbbt2 using stb tags(2,2,2)")
        tdSql.execute("insert into aaat1 values('2024-06-05 11:00:00',1,2,3)")
        tdSql.execute("insert into aaat1 values('2024-06-05 12:00:00',2,2,3)")
        tdSql.execute("insert into bbbt2 values('2024-06-05 13:00:00',3,2,3)")
        tdSql.execute("insert into bbbt2 values('2024-06-05 14:00:00',4,2,3)")

        tdCom.compare_testcase_result(
            "cases/11-Functions/resource/in/last_pk.in",
            "cases/11-Functions/resource/ans/last_pk.csv",
            "test_last_pk"
        )
