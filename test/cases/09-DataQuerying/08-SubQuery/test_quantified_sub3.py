import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestQuantifiedSubQuery3:
    updatecfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    clientCfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    updatecfgDict["clientCfg"] = clientCfgDict    
    caseName = "test_quantified_sub_query3"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    mainIdx = 0
    subIdx = 0
    fileIdx = 0
    saved_count = 0  # total number of queries saved so far
    maxFileQueryNum = 10000  # max number of queries to save in a single file
    tableNames = ["tb1", "tb2", "tb3", "tba", "tbe", "st1"]

    subSqls = [
        # select 
        "select {quantifiedSql}",
        "select {quantifiedSql} from {tableName} order by 1",
        "select {quantifiedSql} a from {tableName} order by 1",
        "select tags {quantifiedSql} from {tableName} order by 1",
        "select distinct {quantifiedSql} from {tableName} order by 1",
        "select distinct {quantifiedSql}, f1 from {tableName} order by 1, f1",
        "select distinct tags {quantifiedSql} + 1 from {tableName} order by 1",
        "select {quantifiedSql}, {quantifiedSql2} from {tableName} order by 1, 2",
        "select 1, {quantifiedSql} from {tableName} order by ts",
        "select {quantifiedSql}, * from {tableName} order by ts, f1",
        "select {quantifiedSql} * {quantifiedSql}, * from {tableName} order by ts, f1", 
        "select count({quantifiedSql}) from {tableName}",
        "select sum({quantifiedSql} + 1) from {tableName}",
        "select avg(({quantifiedSql}) + 1) + 2 from {tableName}",
        "select ({quantifiedSql}) + ({quantifiedSql}) from {tableName} partition by tbname",
        "select {quantifiedSql} + f1 from {tableName} partition by f1",
        "select case when {quantifiedSql} then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then {quantifiedSql} else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then 1 else {quantifiedSql} end, sum(f1) from {tableName} group by f1",
        "select _wstart, {quantifiedSql}, avg(f1) from {tableName} partition by f1 interval(1d)",
        "select {quantifiedSql}, avg(f1) from {tableName} partition by f1 interval(1d)",
        "select min({quantifiedSql}), avg(f1) from {tableName} partition by f1 interval(1d)",
        "select max(({quantifiedSql}) + 1), avg(f1) from {tableName} partition by f1 interval(1d)",
        "select case {quantifiedSql} when 1 then 1 else 2 end, avg(f1) from {tableName} partition by f1 interval(1d)",
        "select _wstart, {quantifiedSql}, avg(f1) from {tableName} partition by f1 SESSION(ts, 1d)",
        "select {quantifiedSql}, avg(f1) from {tableName} partition by f1 SESSION(ts, 1d)",
        "select {quantifiedSql} from {tableName} partition by f1 SESSION(ts, 1d)",
        "select sum(case when {quantifiedSql} then 1 else 2 end) from {tableName} STATE_WINDOW(f1)",
        "select sum(case when f1 = 1 then cast({quantifiedSql} as bigint) else 2 end) from {tableName} EVENT_WINDOW START WITH f1 >= 1 END WITH f1 <= 2",
        "select sum(case when f1 = 1 then 1 else {quantifiedSql} end + 1) from {tableName} COUNT_WINDOW(2)",
        "select {quantifiedSql} from {tableName} partition by tbname interval(1d)",
        "select avg(abs({quantifiedSql})) from {tableName} partition by f1 interval(1d)",
        "select f1, {quantifiedSql} from {tableName} group by f1",
        "select f1, {quantifiedSql}, avg({quantifiedSql}) from {tableName} group by f1",

        # from
        "select b.f1, {quantifiedSql} from {tableName} a join {tableName} b on a.ts = b.ts and {quantifiedSql} order by b.f1",
        "select {quantifiedSql} from {tableName} a left join {tableName} b on a.ts = b.ts and {quantifiedSql}  order by 1",
        "select a.*, {quantifiedSql}, b.* from {tableName} a left join {tableName} b on a.ts = b.ts and {quantifiedSql} order by a.ts, a.f1, b.ts, b.f1",

        # where
        "select f1 from {tableName} where {quantifiedSql} order by f1",
        "select f1 from {tableName} where f1 != ({quantifiedSql}) order by f1",
        "select f1, {quantifiedSql} from {tableName} where {quantifiedSql} and {quantifiedSql2} order by f1",
        "select f1, {quantifiedSql} from {tableName} where {quantifiedSql} or {quantifiedSql2} order by f1",
        "select f1 from {tableName} where ({quantifiedSql}) in (select 1 from {tableName}) order by f1",
        "select f1 from {tableName} where ({quantifiedSql}) not in (select 1 from {tableName}) order by f1",
        "select {quantifiedSql} from {tableName} where {quantifiedSql} is null order by f1",
        "select {quantifiedSql} from {tableName} where {quantifiedSql} is not null order by f1",
        "select f1, cast({quantifiedSql} as varchar) from {tableName} where abs({quantifiedSql}) > 0 order by f1",
        "select f1, {quantifiedSql} from {tableName} where f1 between ({quantifiedSql}) and ({quantifiedSql}) + 1 order by f1",

        # partition
        "select f1 from {tableName} partition by f1, ({quantifiedSql}) order by f1",
        "select f1 from {tableName} partition by f1 having({quantifiedSql}) order by f1",
        "select f1 from {tableName} partition by f1 having(({quantifiedSql}) > 1) order by f1",
        "select f1 from {tableName} partition by f1 having(sum({quantifiedSql}) > 3) order by f1",

        # group
        "select sum(f1) from {tableName} group by ({quantifiedSql}), f1 having(({quantifiedSql})) order by 1",

        #union
        "select f1 from {tableName} union select {quantifiedSql} from {tableName} order by f1",
        "select {quantifiedSql} a from {tableName} union all select {quantifiedSql} b from {tableName} order by a",
    ]

    quantifiedSqls = [
        "1 = any (select 1)",
        "f1 != some (select f1 from {tableName})",
        "f1 >= all (select count(*) from {tableName})",
        "f1 = any (select last(*) from {tableName})",
        "f1 = some (select f1, ts from {tableName} union all select f1, ts from {tableName})",
        "exists (select f1 from {tableName} where f1 is null)",
        "not exists (select f1 from {tableName} where f1 < 0)",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_quantified_sub_query3(self):
        """quantified expr sub query test case
        
        1. Prepare data.
        2. Execute various queries with different kind of quantified expr sub queries.
        3. Validate the results against expected result files.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-30 dapan Created

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
        tdCom.compare_testcase_result(sqlFile, resFile, self.caseName)
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
            self.ntableIdx = (self.tableIdx + 1) % len(self.tableNames)
            for self.mainIdx in range(len(self.subSqls)):
                for self.subIdx in range(len(self.quantifiedSqls)):
                    self.querySql = (
                        self.subSqls[self.mainIdx]
                        .replace("{quantifiedSql}", self.quantifiedSqls[self.subIdx])
                        .replace("{quantifiedSql2}", self.quantifiedSqls[(self.subIdx + 1) % len(self.quantifiedSqls)])
                        .replace("{tableName}", self.tableNames[self.tableIdx])
                    )
                    # ensure exactly one trailing semicolon
                    self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                    #tdLog.info(f"generated sql: {self.querySql}")

                    self.saved_count += 1
                    self._query_saved_count = self.saved_count

                    self.generated_queries_file.write(self.querySql.strip() + "\n")
                    self.generated_queries_file.flush()

                    if self.saved_count >= self.maxFileQueryNum:
                        self.generated_queries_file.close()
                        self.fileIdx += 1
                        self.saved_count = 0
                        self.openSqlTmpFile()

        self.generated_queries_file.close()
        # iterate from file 0 to last file index self.fileIdx (inclusive)
        for idx in range(0, self.fileIdx + 1):
            tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{idx}.sql")
            res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{idx}.csv")
            self.checkResultWithResultFile(tmp_file, res_file)


        self.rmoveSqlTmpFiles()

        return True

