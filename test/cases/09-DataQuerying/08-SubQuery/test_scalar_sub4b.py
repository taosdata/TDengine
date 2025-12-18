import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestScalarSubQuery4b:
    updatecfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    clientCfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    updatecfgDict["clientCfg"] = clientCfgDict
    caseName = "test_scalar_sub_query4b"
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
        "select {scalarSql}",
        "select {scalarSql} from {tableName}",
        "select {scalarSql} a from {tableName}",
        "select tags {scalarSql} from {tableName}",
        "select distinct {scalarSql} from {tableName}",
        "select distinct {scalarSql}, f1 from {tableName} order by f1",
        "select distinct tags {scalarSql} + 1 from {tableName}",
        "select {scalarSql}, {scalarSql} from {tableName}",
        "select 1, {scalarSql} from {tableName} order by ts",
        "select {scalarSql}, * from {tableName} order by ts, f1",
        "select {scalarSql} * {scalarSql}, * from {tableName} order by ts, f1", 
        "select count({scalarSql}) from {tableName}",
        "select sum({scalarSql} + 1) from {tableName}",
        "select avg({scalarSql} + 1) + 2 from {tableName}",
        "select {scalarSql} + {scalarSql} from {tableName} partition by tbname",
        "select {scalarSql} + f1 from {tableName} partition by f1",
        "select case when {scalarSql} > 0 then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then 2 / {scalarSql} else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then 1 else {scalarSql} is null end, sum(f1) from {tableName} group by f1",
        "select _wstart, {scalarSql}, avg(f1) from {tableName} interval(1d)",
        "select {scalarSql} + 1, avg(f1) from {tableName} interval(1d)",
        "select min({scalarSql}), avg(f1) from {tableName} interval(1d)",
        "select max({scalarSql} + 1), avg(f1) from {tableName} interval(1d)",
        "select case {scalarSql} when 1 then 1 else 2 end, avg(f1) from {tableName} interval(1d)",
        "select _wstart, {scalarSql}, avg(f1) from {tableName} SESSION(ts, 1d)",
        "select {scalarSql} + 1, avg(f1) from {tableName} SESSION(ts, 1d)",
        "select {scalarSql} > 0 from {tableName} SESSION(ts, 1d)",
        "select sum(case when {scalarSql} > 0 then 1 else 2 end) from {tableName} STATE_WINDOW(f1)",
        "select sum(case when f1 = 1 then cast(2 / {scalarSql} as bigint) else 2 end) from {tableName} EVENT_WINDOW START WITH f1 >= 1 END WITH f1 <= 2",
        "select sum(case when f1 = 1 then 1 else {scalarSql} is null end + 1) from {tableName} COUNT_WINDOW(2)",
        "select sum({scalarSql}) from {tableName} partition by tbname interval(1d)",
        "select avg(abs({scalarSql} * -1)) from {tableName} partition by f1 interval(1d)",
        "select f1, {scalarSql} from {tableName} group by f1",
        "select f1, {scalarSql}, avg({scalarSql}) from {tableName} group by f1",

        # from
        "select b.f1, {scalarSql} + 1 from {tableName} a join {tableName} b on a.ts = b.ts and a.f1 = {scalarSql} order by b.f1",
        "select {scalarSql} + 1, avg({scalarSql} + 2) from {tableName} a left join {tableName} b on a.ts = b.ts and a.f1 = {scalarSql}",
        "select a.*, {scalarSql}, b.* from {tableName} a left join {tableName} b on a.ts = b.ts and b.f1 = {scalarSql} order by a.ts, a.f1",

        # where
        "select f1 from {tableName} where f1 != {scalarSql} order by f1",
        "select f1, {scalarSql} from {tableName} where f1 = {scalarSql} order by f1",
        "select f1 from {tableName} where f1 in ({scalarSql})",
        "select f1 from {tableName} where f1 not in ({scalarSql})",
        "select {scalarSql} from {tableName} where {scalarSql} is null order by f1",
        "select {scalarSql} from {tableName} where {scalarSql} is not null order by f1",
        "select f1, cast({scalarSql} as varchar) from {tableName} where abs({scalarSql}) > 0 order by f1",
        "select f1, {scalarSql} from {tableName} where f1 > {scalarSql} or f1 < {scalarSql} order by f1",
        "select f1, {scalarSql} from {tableName} where f1 between {scalarSql} and {scalarSql} + 1 order by f1",
        "select {scalarSql}, {scalarSql} > 0 from {tableName} where {scalarSql} > 0 and f1 > {scalarSql} order by f1",

        # partition
        "select f1 from {tableName} partition by f1, {scalarSql} order by f1",
        "select f1 from {tableName} partition by f1 having({scalarSql} > 1) order by f1",
        "select f1 from {tableName} partition by f1 having(sum({scalarSql}) > 3) order by f1",

        # group
        "select sum(f1) from {tableName} group by {scalarSql}, f1 having({scalarSql} > 2) order by 1",

        #union
        "select f1 from {tableName} union select {scalarSql} from {tableName} order by f1",
        "select {scalarSql} a from {tableName} union all select {scalarSql} b from {tableName} order by a",
    ]

    scalarSqls = [
        "(select 1)",
        "(select f1 from {tableName})",
        "(select f1 from {tableName} order by ts, f1 limit 1)",
        "(select count(*) from {tableName})",
        "(select null from {tableName})",
        "(select * from {tableName})",
        "(select 2 from {tableName})",
        "(select avg(f1) from {tableName})",
        "(select last(*) from {tableName})",
        "(select max(f1) from {tableName} partition by f1)",
        "(select a.ts from {tableName} a join {tableName} b on a.ts = b.ts)",
        "(select sum(f1) from {tableName} interval(1d))",
        "(select f1 from {tableName} union select f1 from {tableName})",
        "(select f1 from {tableName} union all select f1 from {tableName})",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_scalar_sub_query4b(self):
        """scalar sub query test case
        
        1. Prepare data.
        2. Explain execute various nested queries with different kind of scalar sub queries.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-15 dapan Created

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
                    for self.subIdx in range(len(self.scalarSqls)):
                        self.querySql = self.subSqls[self.mainIdx].replace("{scalarSql}", "(" + self.subSqls[self.secondIdx] + ")")
                        self.querySql = self.querySql.replace("{scalarSql}", self.scalarSqls[self.subIdx])
                        self.querySql = self.querySql.replace("{tableName}", self.tableNames[self.tableIdx])
                        #self.querySql = self.querySql.replace("{ntableName}", self.tableNames[self.ntableIdx])

                        self.generated_queries_file.write("explain " + self.querySql.strip() + "\G;\n")
                        self.generated_queries_file.write("explain verbose true " + self.querySql.strip() + "\G;\n")
                        #self.generated_queries_file.write("explain analyze " + self.querySql.strip() + "\G\n")
                        self.generated_queries_file.write("explain analyze verbose true " + self.querySql.strip() + "\G;\n")
                        self.generated_queries_file.flush()

        self.generated_queries_file.close()
        # iterate from file 0 to last file index self.fileIdx (inclusive)
        for idx in range(0, self.fileIdx + 1):
            tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{idx}.sql")
            res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{idx}.csv")
            self.checkResultWithResultFile(tmp_file, res_file)


        self.rmoveSqlTmpFiles()

        return True

