import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestInSubQuery1:
    updatecfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    clientCfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    updatecfgDict["clientCfg"] = clientCfgDict    
    caseName = "test_in_sub_query1"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    mainIdx = 0
    subIdx = 0
    fileIdx = 0
    optIdx = 0
    targetIdx = 0
    tableIdx = 0
    tableNames = ["tb1", "st1"]
    fullTableNames = ["tb1", "tb2", "tb3", "tba", "tbb", "st1"]
    opStrs = ["IN", "NOT IN"]
    targetObjs = ["1", "'a'", "f1", "NULL", "123.45", "abs(-2)", "f1 + 1", "sum(f1)"]
    targetFullObjs = ["1", "'a'", "'d'", "f1", "NULL", "123.45", "abs(-2)", "f1 + 1", "sum(f1)", "(select 1)", "(select f1 from tba limit 0)"]
    targetFullTypeObjs = ["1", "1.0", "'a'", "'0'", "false", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "NULL"]

    subFullSqls = [
        "select {targetObj} {opStr} {colSql}, {targetObj} {opStr} {colSql} from {tableName} where {targetObj} {opStr} {colSql} order by 1",
    ]

    subFullTypeSqls = [
        "select {targetObj} {opStr} {colSql} from tba order by 1",
    ]
    subSqls = [
        # select 
        "select {targetObj} {opStr} {colSql} from {tableName} order by 1",
        "select tags {targetObj} {opStr} {colSql} from {tableName} order by 1",
        "select distinct {targetObj} {opStr} {colSql} from {tableName} order by 1",
        "select {targetObj} {opStr} {colSql}, {targetObj} {opStr} {colSql} from {tableName} order by 1",
        "select {targetObj} {opStr} {colSql}, * from {tableName} order by ts, f1",
        "select case when {targetObj} {opStr} {colSql} then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when f1 = 1 then 1 else {targetObj} {opStr} {colSql} end, sum(f1) from {tableName} group by f1",
        "select _wstart, {targetObj} {opStr} {colSql}, avg(f1) from {tableName} interval(1d)",
        "select case {targetObj} {opStr} {colSql} when 1 then 1 else 2 end, avg(f1) from {tableName} interval(1d)",
        "select sum(case when {targetObj} {opStr} {colSql} then 1 else 2 end) from {tableName} STATE_WINDOW(f1)",
        "select f1, {targetObj} {opStr} {colSql} from {tableName} group by f1",

        # from
        "select b.f1, {colSql} + 1 from {tableName} a join {tableName} b on a.ts = b.ts and {targetObj} {opStr} {colSql} order by b.f1",
        "select {colSql} + 1, avg({colSql} + 2) from {tableName} a left join {tableName} b on a.ts = b.ts and {targetObj} {opStr} {colSql}",

        # where
        "select f1 from {tableName} where {targetObj} {opStr} {colSql} order by f1",
        "select f1, {targetObj} {opStr} {colSql} from {tableName} where f1 > 0 and {targetObj} {opStr} {colSql} order by f1",
        "select f1 from {tableName} where f1 > 0 or {targetObj} {opStr} {colSql} order by f1",

        # partition
        "select f1 from {tableName} partition by f1 having({targetObj} {opStr} {colSql}) order by f1",

        # group
        "select sum(f1) from {tableName} group by f1 having({targetObj} {opStr} {colSql}) order by 1",

        #union
        "select true from {tableName} union select {targetObj} {opStr} {colSql} from {tableName} order by 1",
        "select {targetObj} {opStr} {colSql} a from {tableName} union all select {targetObj} {opStr} {colSql} b from {tableName} order by a",
    ]

    colSubQSqls = [
        "(select f1 from {tableName})",
        "(select f1 from {tableName} limit 0)",
        "(select null from {tableName})",
        "(1, null)",
        "(select f1 from tbb)"
    ]

    colSubQFullStmtSqls = [
        "(null)",
        "(select 1)",
        "(select f1 from {tableName})",
        "((select f1 from {tableName}))",
        "(select f1 from {tableName} limit 0)",
        "(select count(*) from {tableName})",
        "(select null from {tableName})",
        "(select * from {tableName})",
        "(select 2 from {tableName})",
        "(select last(*) from {tableName})",
        "(select max(f1) from {tableName} partition by f1)",
        "(select a.ts from {tableName} a join {tableName} b on a.ts = b.ts)",
        "(select sum(f1) from {tableName} interval(1d))",
        "(select f1 from {tableName} union select f1 from {tableName})",
        "(select f1 from {tableName} union all select f1 from {tableName})",
        "(select (select f1 from {tableName}) from {tableName})",
        "(select diff(f1) from {tableName})",
        "(select * from (select f1 from {tableName} where f1 > (select 1 from {tableName})) as subq)",
    ]

    colSubQFullTypeSqls = [
        "(select ts from tba)",
        "(select f1 from tba)",
        "(select f2 from tba)",
        "(select f3 from tba)",
        "(select f4 from tba)",
        "(select f5 from tba)",
        "(select f6 from tba)",
        "(select f7 from tba)",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_in_sub_query1(self):
        """IN sub query test case
        
        1. Prepare data.
        2. Execute various queries with different kind of IN sub queries.
        3. Validate the results against expected result files.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-09 dapan Created

        """

        self.prepareData()
        self.execNormalCase()
        self.fileIdx += 1
        self.execFullStmtCase()
        self.fileIdx += 1
        self.execFullTypeCase()
     
    def prepareData(self):
        tdLog.info("start to prepare data for in sub query test case")
        tdLog.info("create database db1")
        tdSql.execute(f"drop database if exists db1")
        tdSql.execute(f"create database db1")
        tdSql.execute(f"use db1")
        sqls = [
            "CREATE STABLE `st1` (`ts` TIMESTAMP, `f1` int, f2 int) TAGS (`tg1` int)",
            "CREATE TABLE tb1 USING st1 TAGS (1)",
            "CREATE TABLE tb2 USING st1 TAGS (2)",
            "CREATE TABLE tb3 USING st1 TAGS (3)",
            "CREATE TABLE tba (ts TIMESTAMP, f4 int, f2 bigint, f3 double, f1 varchar(20), f5 bool, f6 nchar(10), f7 tinyint)",
            "CREATE TABLE tbb (ts TIMESTAMP, f1 int)",
            "INSERT INTO tb1 VALUES ('2025-12-01 00:00:00.000', 1, 11)",
            "INSERT INTO tb2 VALUES ('2025-12-01 00:00:00.000', 2, 21)",
            "INSERT INTO tb2 VALUES ('2025-12-02 00:00:00.000', 3, 23)",
            "INSERT INTO tb3 VALUES ('2025-12-01 00:00:00.000', 4, 31)",
            "INSERT INTO tb3 VALUES ('2025-12-02 00:00:00.000', 5, 32)",
            "INSERT INTO tb3 VALUES ('2025-12-03 00:00:00.000', 6, 33)",
            "INSERT INTO tba VALUES ('2025-12-01 00:00:00.000', 0, 0, 0.0, 'a', false, 'a', 0)",
            "INSERT INTO tba VALUES ('2025-12-02 00:00:00.000', 1, 1, 1.0, 'b', true, 'b', 1)",
            "INSERT INTO tba VALUES ('2025-12-03 00:00:00.000', 2, 2, 2.0, 'c', false, 'c', 2)",
            "INSERT INTO tbb VALUES ('2025-12-04 00:00:00.000', 1)",
            "INSERT INTO tbb VALUES ('2025-12-05 00:00:00.000', null)",
            "INSERT INTO tbb VALUES ('2025-12-06 00:00:00.000', 3)",

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

    def execNormalCase(self):
        tdLog.info(f"execCase begin")

        self.openSqlTmpFile()

        for self.tableIdx in range(len(self.tableNames)):
            for self.opIdx in range(len(self.opStrs)):
                for self.targetIdx in range(len(self.targetObjs)):
                    for self.mainIdx in range(len(self.subSqls)):
                        for self.subIdx in range(len(self.colSubQSqls)):
                            self.querySql = self.subSqls[self.mainIdx].replace("{colSql}", self.colSubQSqls[self.subIdx])
                            self.querySql = self.querySql.replace("{tableName}", self.tableNames[self.tableIdx])
                            self.querySql = self.querySql.replace("{opStr}", self.opStrs[self.opIdx])
                            self.querySql = self.querySql.replace("{targetObj}", self.targetObjs[self.targetIdx])
                            # ensure exactly one trailing semicolon
                            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                            #tdLog.info(f"generated sql: {self.querySql}")

                            self.generated_queries_file.write(self.querySql.strip() + "\n")
                            self.generated_queries_file.flush()


        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        self.rmoveSqlTmpFiles()

        return True

    def execFullStmtCase(self):
        tdLog.info(f"execCase begin")

        self.openSqlTmpFile()

        for self.tableIdx in range(len(self.fullTableNames)):
            for self.opIdx in range(len(self.opStrs)):
                for self.targetIdx in range(len(self.targetFullObjs)):
                    for self.mainIdx in range(len(self.subFullSqls)):
                        for self.subIdx in range(len(self.colSubQFullStmtSqls)):
                            self.querySql = self.subFullSqls[self.mainIdx].replace("{colSql}", self.colSubQFullStmtSqls[self.subIdx])
                            self.querySql = self.querySql.replace("{tableName}", self.fullTableNames[self.tableIdx])
                            self.querySql = self.querySql.replace("{opStr}", self.opStrs[self.opIdx])
                            self.querySql = self.querySql.replace("{targetObj}", self.targetFullObjs[self.targetIdx])
                            # ensure exactly one trailing semicolon
                            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                            #tdLog.info(f"generated sql: {self.querySql}")

                            self.generated_queries_file.write(self.querySql.strip() + "\n")
                            self.generated_queries_file.flush()


        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        self.rmoveSqlTmpFiles()

        return True

    def execFullTypeCase(self):
        tdLog.info(f"execCase begin")

        self.openSqlTmpFile()

        for self.opIdx in range(len(self.opStrs)):
            for self.targetIdx in range(len(self.targetFullTypeObjs)):
                for self.mainIdx in range(len(self.subFullTypeSqls)):
                    for self.subIdx in range(len(self.colSubQFullTypeSqls)):
                        self.querySql = self.subFullTypeSqls[self.mainIdx].replace("{colSql}", self.colSubQFullTypeSqls[self.subIdx])
                        self.querySql = self.querySql.replace("{opStr}", self.opStrs[self.opIdx])
                        self.querySql = self.querySql.replace("{targetObj}", self.targetFullTypeObjs[self.targetIdx])
                        # ensure exactly one trailing semicolon
                        self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                        #tdLog.info(f"generated sql: {self.querySql}")

                        self.generated_queries_file.write(self.querySql.strip() + "\n")
                        self.generated_queries_file.flush()


        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        self.rmoveSqlTmpFiles()

        return True