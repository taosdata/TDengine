import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestQuantifiedSubQuery6b1:
    updatecfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    clientCfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    updatecfgDict["clientCfg"] = clientCfgDict
    caseName = "test_quantified_sub_query6b1"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    mainIdx = 0
    secondIdx = 0
    subIdx = 0
    fileIdx = 0
    saved_count = 0  # total number of queries saved so far
    maxFileQueryNum = 10000000  # max number of queries to save in a single file
    tableNames = ["tb3"] #["tb1", "tb3", "tbe", "st1"]

    subSqls = [
        # select 
        "select {quantifiedSql}",
        "select {quantifiedSql} from {tableName}",
        "select {quantifiedSql} a from {tableName}",
        "select tags {quantifiedSql} from {tableName}",
        "select distinct {quantifiedSql} from {tableName}",
        "select distinct {quantifiedSql}, f1 from {tableName}",
        "select distinct tags {quantifiedSql} from {tableName}",
        "select {quantifiedSql}, {quantifiedSql} from {tableName}",
        "select 1, {quantifiedSql} from {tableName} order by ts",
        "select {quantifiedSql}, * from {tableName} order by ts, f1",
        "select {quantifiedSql} from {tableName} partition by tbname",
        "select {quantifiedSql}, f1 from {tableName} partition by f1",
        "select case when {quantifiedSql} then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then {quantifiedSql} else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then 1 else {quantifiedSql} end, sum(f1) from {tableName} group by f1",
        "select _wstart, {quantifiedSql}, avg(f1) from {tableName} interval(1d)",
        "select {quantifiedSql}, avg(f1) from {tableName} interval(1d)",
        "select case {quantifiedSql} when 1 then 1 else 2 end, avg(f1) from {tableName} interval(1d)",
        "select _wstart, {quantifiedSql}, avg(f1) from {tableName} SESSION(ts, 1d)",
        "select {quantifiedSql}, avg(f1) from {tableName} SESSION(ts, 1d)",
        "select {quantifiedSql} from {tableName} SESSION(ts, 1d)",
        "select sum(case when {quantifiedSql} then 1 else 2 end) from {tableName} STATE_WINDOW(f1)",
        "select sum(case when f1 = 1 then cast({quantifiedSql} as bigint) else 2 end) from {tableName} EVENT_WINDOW START WITH f1 >= 1 END WITH f1 <= 2",
        "select sum(case when f1 = 1 then 1 else {quantifiedSql} end + 1) from {tableName} COUNT_WINDOW(2)",
        "select {quantifiedSql} from {tableName} partition by tbname interval(1d)",
        "select {quantifiedSql} from {tableName} partition by f1 interval(1d)",
        "select f1, {quantifiedSql} from {tableName} group by f1",

        # from
        "select b.f1, {quantifiedSql} from {tableName} a join {tableName} b on a.ts = b.ts and {quantifiedSql} order by b.f1",
        "select {quantifiedSql} from {tableName} a left join {tableName} b on a.ts = b.ts and {quantifiedSql}",
        "select a.*, {quantifiedSql}, b.* from {tableName} a left join {tableName} b on a.ts = b.ts and {quantifiedSql} order by a.ts, a.f1",

        # where
        "select f1 from {tableName} where {quantifiedSql} order by f1",
        "select f1, {quantifiedSql} from {tableName} where {quantifiedSql} order by f1",
        "select f1 from {tableName} where {quantifiedSql} order by f1",
        "select {quantifiedSql} from {tableName} where {quantifiedSql} order by f1",
        "select {quantifiedSql} from {tableName} where {quantifiedSql} order by f1",
        "select f1, cast({quantifiedSql} as varchar) from {tableName} where {quantifiedSql} order by f1",
        "select f1, {quantifiedSql} from {tableName} where {quantifiedSql} or {quantifiedSql} order by f1",
        "select f1, {quantifiedSql} from {tableName} where {quantifiedSql} and {quantifiedSql} order by f1",

        # partition
        "select f1 from {tableName} partition by f1, {quantifiedSql} order by f1",
        "select f1 from {tableName} partition by f1 having({quantifiedSql}) order by f1",
        "select f1 from {tableName} partition by f1 having(sum({quantifiedSql}) > 3) order by f1",

        # group
        "select sum(f1) from {tableName} group by {quantifiedSql}, f1 having({quantifiedSql}) order by 1",

        #union
        "select f1 from {tableName} union select {quantifiedSql} from {tableName} order by f1",
        "select {quantifiedSql} a from {tableName} union all select {quantifiedSql} b from {tableName} order by a",
    ]

    quantifiedSqls = [
        "f1 < any (select f1 from {tableName})",
        "f1 > any (select f1 from {tableName})",
        "f1 = all (select f1 from {tableName})",
        "f1 != all (select f1 from {tableName})",
        "exists (select f1 from {tableName} where f1 > 1)",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_quantified_sub_query6b1(self):
        """quantified sub query test case
        
        1. Prepare data.
        2. Explain execute various nested queries with different kind of quantified sub queries.
        
        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-02-28 dapan Created

        """

        self.prepareData()
        self.execCase()
     
    def prepareData(self):
        tdLog.info("create database db1")
        tdSql.execute(f"drop database if exists db1")
        tdSql.execute(f"create database db1")
        tdSql.execute(f"use db1")
        sqls = [
            "CREATE STABLE `st1` (`ts` TIMESTAMP, `f1` int, f2 int) TAGS (`tg1` int)",
            "CREATE TABLE tb1 USING st1 TAGS (1)",
            "CREATE TABLE tb2 USING st1 TAGS (2)",
            "CREATE TABLE tb3 USING st1 TAGS (3)",
            "CREATE TABLE tba (ts TIMESTAMP, f1 int)",
            "CREATE TABLE tbb (ts TIMESTAMP, f1 int)",
            "CREATE TABLE tbe (ts TIMESTAMP, f1 int)",
            "INSERT INTO tb1 VALUES ('2025-12-01 00:00:00.000', 1, 11)",
            "INSERT INTO tb2 VALUES ('2025-12-01 00:00:00.000', 2, 21)",
            "INSERT INTO tb2 VALUES ('2025-12-02 00:00:00.000', 3, 23)",
            "INSERT INTO tb3 VALUES ('2025-12-01 00:00:00.000', 4, 31)",
            "INSERT INTO tb3 VALUES ('2025-12-02 00:00:00.000', 5, 32)",
            "INSERT INTO tb3 VALUES ('2025-12-03 00:00:00.000', 6, 33)",
            "INSERT INTO tba VALUES ('2025-12-01 00:00:00.000', 0)",
            "INSERT INTO tbb VALUES ('2025-12-02 00:00:00.000', 1)",
        ]
        tdSql.executes(sqls)

    def checkResultWithResultFile(self, sqlFile, resFile):
        tdLog.info(f"check result with sql: {sqlFile}")
        #tdCom.compare_testcase_result(sqlFile, resFile, self.caseName)
        tdCom.execute_query_file(sqlFile)
        tdLog.info("check result with result file succeed")

    def openSqlTmpFile(self):
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        # open generated queries file in append mode so the loop can continuously write to it
        os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
        self.generated_queries_file = open(tmp_file, "w", encoding="utf-8")
        self.generated_queries_file.write("use db1;" + "\n\n")

    def rmoveSqlTmpFiles(self):
        for idx in range(0, self.fileIdx + 1):
            tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{idx}.sql")
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

    def execCase(self):
        tdLog.info(f"execCase begin")

        runnedCaseNum = 0

        self.openSqlTmpFile()

        for self.tableIdx in range(len(self.tableNames)):
            for self.mainIdx in range(len(self.subSqls)):
                for self.secondIdx in range(len(self.subSqls)):
                    for self.subIdx in range(len(self.quantifiedSqls)):
                        self.querySql = self.subSqls[self.mainIdx].replace("{quantifiedSql}", "(" + self.subSqls[self.secondIdx] + ")")
                        self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedSqls[self.subIdx])
                        self.querySql = self.querySql.replace("{tableName}", self.tableNames[self.tableIdx])
                        #self.querySql = self.querySql.replace("{ntableName}", self.tableNames[self.ntableIdx])

                        self.generated_queries_file.write("explain " + self.querySql.strip() + "\G;\n")
                        self.generated_queries_file.write("explain verbose true " + self.querySql.strip() + "\G;\n")
                        #self.generated_queries_file.write("explain analyze " + self.querySql.strip() + "\G\n")
                        #self.generated_queries_file.write("explain analyze verbose true " + self.querySql.strip() + "\G;\n")
                        self.generated_queries_file.flush()

        self.generated_queries_file.close()
        # iterate from file 0 to last file index self.fileIdx (inclusive)
        for idx in range(0, self.fileIdx + 1):
            tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{idx}.sql")
            res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{idx}.csv")
            self.checkResultWithResultFile(tmp_file, res_file)


        self.rmoveSqlTmpFiles()

        return True

