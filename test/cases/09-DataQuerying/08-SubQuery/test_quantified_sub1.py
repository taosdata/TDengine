import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestQuantifiedSubQuery1:
    updatecfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    clientCfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    updatecfgDict["clientCfg"] = clientCfgDict    
    caseName = "test_quantified_sub_query1"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    fileIdx = 0
    colIdx = 0
    subColIdx = 0
    compareIdx = 0
    quantifiedIdx = 0
    filterIdx = 0
    tableIdx = 0
    existIdx = 0
    sqlIdx = 0
    maxColumnIdx = 20
    querySql = "select {row} {compareExpr} {quantifiedExpr} (select {row2} from {targetTableName} {filterExpr}) from {srcTableName};"
    queryTagsSql = "select tags {row} {compareExpr} {quantifiedExpr} (select {row2} from {targetTableName} {filterExpr}) from {srcTableName};"
    queryConstSql = "select {constExpr} {compareExpr} {quantifiedExpr} (select {row2} from {targetTableName} {filterExpr}) from {srcTableName};"
    queryExistsSqls = ["select {existExpr} (select f2 from ctt1 {filterExpr}) from cts1;",
                       "select f1 from cts1 where {existExpr} (select f2 from ctt1 {filterExpr});"
                      ]

    existExprs = [
        "EXISTS",
        "NOT EXISTS"
    ]

    compareExprs = [
        "=",
        "!=",
        "<",
        "<=",
        ">",
        ">="
    ]

    quantifiedExprs = [
        "ALL",
        "ANY",
        "SOME"
    ]

    filterExprs = [
        "",
        "WHERE 1 = 0",
        "WHERE {row2} IS NULL",
        "WHERE {row2} IS NOT NULL",
    ]

    constExprs = [
        "NULL",
        "1",
        "'a'",
        "true",
        "2.0",
        "CAST('2025-12-01 00:00:00.000' AS TIMESTAMP)",
        "abs(-1)",
        "(select 1)"
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_quantified_sub_query1(self):
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
            "CREATE STABLE `ss1` (f1 TIMESTAMP, f2 bool, f3 tinyint, f4 smallint, f5 int, f6 bigint, f7 float, f8 double, f9 varchar(5), f10 timestamp, "
            "f11 nchar(5), f12 tinyint unsigned, f13 smallint unsigned, f14 int unsigned, f15 bigint unsigned, f16 varbinary(5), f17 decimal(3, 1), f18 blob, f19 geometry(50)) "
            "TAGS (`tg1` json)",
            "CREATE TABLE cts1 USING ss1 TAGS ('{\"k1\": 1}')",
            "CREATE TABLE cts2 USING ss1 TAGS ('{\"k1\": 3}')",
            "CREATE TABLE cts3 USING ss1 TAGS (NULL)",

            "INSERT INTO cts1 VALUES ('2025-12-01 00:00:00.000',false,    1,   1,   1,   1, 1.0, 1.0, 'a','2025-12-01 00:00:00.000', 'a',   1,   1,   1,   1,'\x01', 1.0,'\x01','POINT(1 1)')",
            "INSERT INTO cts1 VALUES ('2025-12-03 00:00:00.000', true,    3,   3,   3,   3, 3.0, 3.0, 'c','2025-12-03 00:00:00.000', 'c',   3,   3,   3,   3,'\x03', 3.0,'\x03','POINT(3 3)')",
            "INSERT INTO cts1 VALUES ('2025-12-04 00:00:00.000', NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,                     NULL,NULL,NULL,NULL,NULL,NULL,  NULL,NULL,  NULL,        NULL)",

            "CREATE STABLE `st1` (f1 TIMESTAMP, f2 bool, f3 tinyint, f4 smallint, f5 int, f6 bigint, f7 float, f8 double, f9 varchar(5), f10 timestamp, "
            "f11 nchar(5), f12 tinyint unsigned, f13 smallint unsigned, f14 int unsigned, f15 bigint unsigned, f16 varbinary(5), f17 decimal(3, 1), f18 blob, f19 geometry(50)) "
            "TAGS (`tg1` json)",

            "CREATE TABLE ctt1 USING st1 TAGS ('{\"k1\": 1}')",
            "CREATE TABLE ctt2 USING st1 TAGS ('{\"k1\": 2}')",
            "CREATE TABLE ctt3 USING st1 TAGS (NULL)",

            "INSERT INTO ctt1 VALUES ('2025-12-01 00:00:00.000',false,    1,   1,   1,   1, 1.0, 1.0, 'a','2025-12-01 00:00:00.000', 'a',   1,   1,   1,   1,'\x01', 1.0,'\x01','POINT(1 1)')",
            "INSERT INTO ctt1 VALUES ('2025-12-02 00:00:00.000',false,    2,   2,   2,   2, 2.0, 2.0, 'b','2025-12-02 00:00:00.000', 'b',   2,   2,   2,   2,'\x02', 2.0,'\x02','POINT(2 2)')",
            "INSERT INTO ctt1 VALUES ('2025-12-04 00:00:00.000', NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,                     NULL,NULL,NULL,NULL,NULL,NULL,  NULL,NULL,  NULL,        NULL)",

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

        self.openSqlTmpFile()

        for self.colIdx in range(1, self.maxColumnIdx):
            for self.compareIdx in range(len(self.compareExprs)):
                for self.quantifiedIdx in range(len(self.quantifiedExprs)):
                    for self.subColIdx in range(1, self.maxColumnIdx + 1):
                        if (self.subColIdx != self.maxColumnIdx):
                            self.colName = f"f{self.subColIdx}"
                            self.targetTbName = "ctt1"
                        else:
                            self.colName = "tg1->'k1'"    
                            self.targetTbName = "st1"
                        for self.filterIdx in range(len(self.filterExprs)):
                            self.generatedSql = (
                                self.querySql
                                .replace("{row}", f"f{self.colIdx}")
                                .replace("{compareExpr}", self.compareExprs[self.compareIdx])
                                .replace("{quantifiedExpr}", self.quantifiedExprs[self.quantifiedIdx])
                                .replace("{filterExpr}", self.filterExprs[self.filterIdx])
                                .replace("{row2}", self.colName)
                                .replace("{targetTableName}", self.targetTbName)
                                .replace("{srcTableName}", "cts1")
                            )
                            #tdLog.info(f"generated sql: {self.generatedSql}")

                            self.generated_queries_file.write(self.generatedSql.strip() + "\n")
                            self.generated_queries_file.flush()

        for self.compareIdx in range(len(self.compareExprs)):
            for self.quantifiedIdx in range(len(self.quantifiedExprs)):
                for self.subColIdx in range(1, self.maxColumnIdx + 1):
                    if (self.subColIdx != self.maxColumnIdx):
                        self.colName = f"f{self.subColIdx}"
                        self.targetTbName = "ctt1"
                    else:
                        self.colName = "tg1->'k1'"    
                        self.targetTbName = "st1"                    
                    for self.filterIdx in range(len(self.filterExprs)):
                        self.generatedSql = (
                            self.querySql
                            .replace("{row}", "tg1->'k1'")
                            .replace("{compareExpr}", self.compareExprs[self.compareIdx])
                            .replace("{quantifiedExpr}", self.quantifiedExprs[self.quantifiedIdx])
                            .replace("{filterExpr}", self.filterExprs[self.filterIdx])
                            .replace("{row2}", self.colName)
                            .replace("{targetTableName}", self.targetTbName)
                            .replace("{srcTableName}", f"cts1")
                        )
                        #tdLog.info(f"generated sql: {self.generatedSql}")

                        self.generated_queries_file.write(self.generatedSql.strip() + "\n")
                        self.generated_queries_file.flush()


        self.colName = "tg1->'k1'"    
        self.targetTbName = "st1"                    
        for self.tableIdx in range(1, 4):
            for self.compareIdx in range(len(self.compareExprs)):
                for self.quantifiedIdx in range(len(self.quantifiedExprs)):
                    for self.filterIdx in range(len(self.filterExprs)):
                        self.generatedSql = (
                            self.queryTagsSql
                            .replace("{row}", "tg1->'k1'")
                            .replace("{compareExpr}", self.compareExprs[self.compareIdx])
                            .replace("{quantifiedExpr}", self.quantifiedExprs[self.quantifiedIdx])
                            .replace("{filterExpr}", self.filterExprs[self.filterIdx])
                            .replace("{row2}", self.colName)
                            .replace("{targetTableName}", self.targetTbName)
                            .replace("{srcTableName}", f"cts{self.tableIdx}")
                        )
                        #tdLog.info(f"generated sql: {self.generatedSql}")

                        self.generated_queries_file.write(self.generatedSql.strip() + "\n")
                        self.generated_queries_file.flush()

        for self.constExprIdx in range(len(self.constExprs)):
            for self.compareIdx in range(len(self.compareExprs)):
                for self.quantifiedIdx in range(len(self.quantifiedExprs)):
                    for self.subColIdx in range(1, self.maxColumnIdx + 1):
                        if (self.subColIdx != self.maxColumnIdx):
                            self.colName = f"f{self.subColIdx}"
                            self.targetTbName = "ctt1"
                        else:
                            self.colName = "tg1->'k1'"    
                            self.targetTbName = "st1"                    
                        for self.filterIdx in range(len(self.filterExprs)):
                            self.generatedSql = (
                                self.queryConstSql
                                .replace("{constExpr}", self.constExprs[self.constExprIdx])
                                .replace("{compareExpr}", self.compareExprs[self.compareIdx])
                                .replace("{quantifiedExpr}", self.quantifiedExprs[self.quantifiedIdx])
                                .replace("{filterExpr}", self.filterExprs[self.filterIdx])
                                .replace("{row2}", self.colName)
                                .replace("{targetTableName}", self.targetTbName)
                                .replace("{srcTableName}", f"cts1")
                            )
                            #tdLog.info(f"generated sql: {self.generatedSql}")

                            self.generated_queries_file.write(self.generatedSql.strip() + "\n")
                            self.generated_queries_file.flush()

        for self.existIdx in range(len(self.existExprs)):
            for self.sqlIdx in range(len(self.queryExistsSqls)):
                for self.filterIdx in range(len(self.filterExprs)):
                    self.generatedSql = (
                        self.queryExistsSqls[self.sqlIdx]
                        .replace("{existExpr}", self.existExprs[self.existIdx])
                        .replace("{filterExpr}", self.filterExprs[self.filterIdx])
                        .replace("{row2}", "f2")
                    )
                    #tdLog.info(f"generated sql: {self.generatedSql}")

                    self.generated_queries_file.write(self.generatedSql.strip() + "\n")
                    self.generated_queries_file.flush()

        self.generated_queries_file.close()
        # iterate from file 0 to last file index self.fileIdx (inclusive)
        for idx in range(0, self.fileIdx + 1):
            tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{idx}.sql")
            res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{idx}.csv")
            self.checkResultWithResultFile(tmp_file, res_file)

        self.rmoveSqlTmpFiles()

        return True

