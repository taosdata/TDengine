import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestScalarSubQuery2:
    caseName = "test_scalar_sub_query2"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    mainIdx = 0
    subIdx = 0
    fileIdx = 0
    funcIdx = 0
    saved_count = 0  # total number of queries saved so far
    maxFileQueryNum = 10000  # max number of queries to save in a single file
    tableNames = ["tb1", "tb2", "tb3", "tba", "tbb", "tbe", "st1"]
    func_1_param = ["abs", "acos", "asin", "atan", "ceil", "cos", "degrees", "exp", "floor", "ln", "log", "radians", "round", "sign", "sin", "sqrt", "tan", "crc32", "date", "dayofweek", "weekday", "week", "weekofyear", "csum", "diff", "irate", "twa"]
    func_1c_param = ["ascii", "char_length", "length", "lower", "ltrim", "rtrim", "trim", "upper", "to_base64"]
    func_2_param = ["pow", "truncate", "mod", "corr", "find_in_set", "like_in_set", "regexp_in_set", "timetruncate", "timediff", "bottom", "top", "ifnull", "nvl", "nullif"]
    func_3_param = ["percentile", "nvl2"]
    func_nc_param = ["concat", "concat_ws", "greatest", "least"]

    unsupportedSqls = [
        "create stream stm1 interval(1d) sliding(1d) from tb1 partition by {scalarSql} into out1 as select * from {tableName}",
        "create stream stm1 interval(1d) sliding(1d) from tb1 into out2 as select ts, {scalarSql} from {tableName}",
        "create topic topic1 as select {scalarSql} from {tableName}",
        "insert into tbb select now, {scalarSql} from tb1",        
    ]

    speCFuncSqls = [
        "select position({scalarSql} in 'abc') from {tableName}",
        "select position('a' in {scalarSql}) from {tableName}",
        "select repeat({scalarSql}, 2) from {tableName}",
        "select substring({scalarSql}, 1, 2) from {tableName}",
        "select substring({scalarSql} from 1 for 1) from {tableName}",
        "select cast({scalarSql} as int) from {tableName}",
        "select to_char(cast(1 as timestamp), {scalarSql}) from {tableName}",
        "select to_json({scalarSql}) from {tableName}",
        "select to_iso8601(cast(1 as timestamp), {scalarSql}) from {tableName}",
        "select replace({scalarSql}, 'a', 'b') from {tableName}",
        "select replace('ab', {scalarSql}, 'b') from {tableName}",
        "select replace('ab', 'a', {scalarSql}) from {tableName}",
        "select replace({scalarSql}, {scalarSql}, {scalarSql}) from {tableName}",
        "select substring_index({scalarSql}, 'a', 1) from {tableName}",
        "select substring_index('abc', {scalarSql}, 1) from {tableName}",
        "select apercentile(1, 1, {scalarSql}) from {tableName}",
        "select cols(last(f1), {scalarSql}) from {tableName}",
        "select to_timestamp('2025', {scalarSql}) from {tableName}",
        "select to_timestamp({scalarSql}, 'yy') from {tableName}",
        "select to_unixtimestamp({scalarSql}, 1) from {tableName}",
        "select statecount(1, {scalarSql}, 1) from {tableName}",
    ]

    speFuncSqls = [
        "select repeat('ab', {scalarSql}) from {tableName}",
        "select substring('abc', {scalarSql}) from {tableName}",
        "select substring('abc', 1, {scalarSql}) from {tableName}",
        "select substring('abc' from {scalarSql}) from {tableName}",
        "select substring('abc' from 1 for {scalarSql}) from {tableName}",
        "select cast({scalarSql} as bigint) from {tableName}",
        "select to_char(cast({scalarSql} as timestamp), 'am') from {tableName}",
        "select to_iso8601({scalarSql}, 'z') from {tableName}",
        "select substring_index('abc', 'b', {scalarSql}) from {tableName}",
        "select apercentile({scalarSql}, 1) from {tableName}",
        "select apercentile(f1, {scalarSql}) from {tableName}",
        "select histogram({scalarSql}, 'user_input', '\"user_input\": \"[1, 3, 5, 7]\"', 0) from {tableName}",
        "select histogram(f1, 'user_input', '\"user_input\": \"[1, 3, 5, 7]\"', {scalarSql}) from {tableName}",
        "select cols(last(f1), {scalarSql}) from {tableName}",
        "select rand({scalarSql}) from tb1",
        "select to_unixtimestamp('2025-10-01', {scalarSql}) from {tableName}",
        "select derivative({scalarSql}, 1s, 1) from {tableName}",
        "select derivative(1, 1s, {scalarSql}) from {tableName}",
        "select statecount({scalarSql}, 'GT', 1) from {tableName}",
        "select statecount(f1, 'GT', {scalarSql}) from {tableName}",
        "select stateduration(f1, 'GT', {scalarSql}, 1s) from {tableName}",
        "select sample(f1, {scalarSql}) from {tableName}",
        "select sample({scalarSql}, 1) from {tableName}",
        "select mavg(f1, {scalarSql}) from {tableName}",
        "select mavg({scalarSql}, 1) from {tableName}",
    ]

    funcSqls = [
        "select {funcName}({scalarSql}) from {tableName}",
        "select {funcName}({scalarSql}, {scalarSql}) from {tableName}",
        "select {funcName}({scalarSql}, {scalarSql}, {scalarSql}) from {tableName}",
        "select {funcName}({scalarSql}, {scalarSql}, {scalarSql}, {scalarSql}) from {tableName}",
    ]
    subSqls = [
        # select 
        "select {scalarSql} from {tableName}",
        "select {scalarSql} a from {tableName}",
        "select distinct {scalarSql} from {tableName}",
        "select distinct {scalarSql}, f1 from {tableName}",
        "select distinct tags {scalarSql} + 1 from {tableName}",
        "select {scalarSql}, {scalarSql} from {tableName}",
        "select abs({scalarSql}) from {tableName}",
        "select abs({scalarSql} + 1) from {tableName}",
        "select sum({scalarSql}) from {tableName}",
        "select case when {scalarSql} > 0 then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when 1 = 1 then 2 / {scalarSql} else 2 end, sum(f1) from {tableName} group by f1",
        "select case when 0 = 1 then 1 else {scalarSql} is null end, sum(f1) from {tableName} group by f1",

        # from
        "select a.ts from {tableName} a join {tableName} b on a.ts = b.ts and a.f1 = {scalarSql} order by 1",
        "select b.ts from {tableName} a left join {tableName} b on a.ts = b.ts and a.f1 > {scalarSql} order by 1",
        "select a.ts from {tableName} a right join {tableName} b on a.ts = b.ts and b.f1 = {scalarSql} order by 1",
        "select a.ts from {tableName} a full join {tableName} b on a.ts = b.ts and b.f1 = {scalarSql} order by 1",
        "select b.ts from {tableName} a left semi join {tableName} b on a.ts = b.ts and a.f1 = {scalarSql} order by 1",
        "select a.ts from {tableName} a right semi join {tableName} b on a.ts = b.ts and b.f1 > {scalarSql} order by 1",
        "select b.ts from {tableName} a left anti join {tableName} b on a.ts = b.ts and a.f1 != {scalarSql} order by 1",
        "select a.ts from {tableName} a right anti join {tableName} b on a.ts = b.ts and b.f1 <= {scalarSql} order by 1",
        "select b.ts from {tableName} a left asof join {tableName} b on a.ts = b.ts and a.f1 != {scalarSql} order by 1",
        "select a.ts from {tableName} a right asof join {tableName} b on a.ts = b.ts and b.f1 <= {scalarSql} order by 1",
        "select b.ts from {tableName} a left asof join {tableName} b on a.ts = b.ts jlimit {scalarSql} order by 1",
        "select a.ts from {tableName} a right asof join {tableName} b on a.ts = b.ts jlimit {scalarSql} order by 1",
        "select b.ts from {tableName} a left window join {tableName} b on a.f1 = {scalarSql} window_offset(-1d, 1d) order by 1",
        "select a.ts from {tableName} a right window join {tableName} b on b.f1 = {scalarSql} window_offset(-1d, 1d) order by 1",
        "select b.ts from {tableName} a left window join {tableName} b window_offset(-1d, 1d) jlimit {scalarSql} order by 1",
        "select a.ts from {tableName} a right window join {tableName} b window_offset(-1d, 1d) jlimit {scalarSql} order by 1",

        # where
        "select a.ts from {tableName} a , {tableName} b where a.ts = b.ts and a.f1 = {scalarSql} order by 1",
        "select f1 from {tableName} where f1 != {scalarSql} order by 1",
        "select f1 from {tableName} where f1 = {scalarSql} order by 1",
        "select f1 from {tableName} where f1 in ({scalarSql}) order by 1",
        "select f1 from {tableName} where f1 not in ({scalarSql}) order by 1",
        "select f1 from {tableName} where f1 like ({scalarSql}) order by 1",
        "select f1 from {tableName} where f1 not like ({scalarSql}) order by 1",
        "select f1 from {tableName} where f1 match ({scalarSql}) order by 1",
        "select f1 from {tableName} where f1 nmatch ({scalarSql}) order by 1",
        "select f1 from {tableName} where {scalarSql} is null order by 1",
        "select f1 from {tableName} where {scalarSql} is not null order by 1",
        "select f1 from {tableName} where abs({scalarSql}) > 0 order by 1",
        "select f1 from {tableName} where f1 > {scalarSql} or f1 < {scalarSql} order by 1",
        "select f1 from {tableName} where f1 between {scalarSql} and {scalarSql} + 1 order by 1",
        "select f1 from {tableName} where {scalarSql} > 0 and f1 > {scalarSql} order by 1",

        # partition
        "select f1 from {tableName} partition by {scalarSql} order by 1",
        "select f1 from {tableName} partition by {scalarSql} + 1 order by 1",
        "select f1 from {tableName} partition by tbname, {scalarSql} order by 1",

        # window
        "select avg(f1) from {tableName} INTERVAL({scalarSql})",
        "select avg(f1) from {tableName} INTERVAL({scalarSql}d)",
        "select avg(f1) from {tableName} INTERVAL(1d, {scalarSql})",
        "select avg(f1) from {tableName} INTERVAL(1d, {scalarSql}d)",
        "select avg(f1) from {tableName} INTERVAL(1d) FILL({scalarSql})",
        "select avg(f1) from {tableName} where ts >= '2025-12-01 00:00:00.000' and ts <= '2025-12-05 00:00:00.000' INTERVAL(1d) FILL(value, {scalarSql})",
        "select avg(f1) from {tableName} INTERVAL(1d) SLIDING({scalarSql})",
        "select avg(f1) from {tableName} INTERVAL(1d) SLIDING({scalarSql}h)",
        "select avg(f1) from {tableName} SESSION(ts, {scalarSql})",
        "select avg(f1) from {tableName} SESSION(ts, {scalarSql}h)",
        "select avg(f1) from {tableName} SESSION({scalarSql}, 1d)",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW({scalarSql})",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW(f1, {scalarSql})",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW(f1) TRUE_FOR({scalarSql})",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW(f1) TRUE_FOR({scalarSql}d)",
        "select avg(f1) from {tableName} where tbname = 'tb1' EVENT_WINDOW START WITH f1 >= {scalarSql} END WITH f1 <= {scalarSql} + 1",
        "select avg(f1) from {tableName} COUNT_WINDOW({scalarSql})",
        "select avg(f1) from {tableName} COUNT_WINDOW({scalarSql}, {scalarSql})",
        "select avg(f1) from {tableName} COUNT_WINDOW(3, 1, {scalarSql})",

        # group
        "select avg(f1) from {tableName} group by {scalarSql}",
        "select avg(f1) from {tableName} group by f1, {scalarSql}",
        "select avg(f1) from {tableName} group by {scalarSql} + 1",

        # having
        "select avg(f1) from {tableName} group by f1 having avg({scalarSql}) > 0",
        "select avg(f1) from {tableName} group by f1 having {scalarSql} > 0",
        "select avg(f1) from {tableName} group by f1 having avg({scalarSql} + 1) > 0",

        # interp
        "select interp(f1) from {tableName} range({scalarSql}) FILL(PREV)",
        "select interp(f1) from {tableName} range({scalarSql}, {scalarSql}) every(1d)",
        "select interp(f1) from {tableName} range(now - 1d, now, {scalarSql}) every(1d)", 
        "select interp(f1) from {tableName} range(now - 1d, now) every({scalarSql})",
        "select interp(f1) from {tableName} range(now - 1d, now) every({scalarSql}m)",
        "select interp(f1) from {tableName} range(now - 1d, now) every(1h) fill({scalarSql})",
        "select interp(f1) from {tableName} range(now - 1d, now) every(1h) fill(value, {scalarSql})",

        # order
        "select f1 from tb1 order by {scalarSql}",
        "select f1 from tb1 order by {scalarSql} desc",
        "select f1 from tb1 order by f1 + {scalarSql}",
        "select f1 from {tableName} order by f1, {scalarSql}",
        "select f1 from tb1 order by abs({scalarSql})",
        "select avg(f1) from tb1 order by sum({scalarSql})",

        # limit/slimit
        "select f1 from {tableName} limit {scalarSql}",
        "select f1 from {tableName} limit 1 offset {scalarSql}",
        "select f1 from {tableName} partition by tbname slimit {scalarSql}",
        "select f1 from {tableName} partition by tbname slimit 1 soffset {scalarSql}",

        # union
        "select f1 from {tableName} where f1 > {scalarSql} union select f1 from {tableName} order by f1",
        "select f1 from {tableName} union all select {scalarSql} from {tableName} order by f1",
        "select f1 from tb1 union select f1 from tb1 order by {scalarSql}",
        "select f1 from tb1 union all select f1 from tb1 limit {scalarSql}",

        # explain
        "explain select f1 from {tableName} where f1 = {scalarSql}\G",
        "explain verbose true select f1 from {tableName} where f1 = {scalarSql}\G",
        "explain select avg(f1), {scalarSql} from {tableName} group by f1, {scalarSql} having avg(f1) > {scalarSql}\G",
        "explain verbose true select avg(f1), {scalarSql} from {tableName} group by f1, {scalarSql} having avg(f1) > {scalarSql}\G",
    ]

    scalarcSqls = [
        "(select 'a' from {tableName} limit 1)",
        "(select 'am' from {tableName} limit 1)",
        "(select 'abc' from {tableName} limit 1)",
    ]

    scalarSqls = [
        "(select 0 from {tableName} limit 1)",
        "(select 1 from {tableName} limit 1)",
        "(select 2 from {tableName} limit 1)",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_scalar_sub_query2(self):
        """scalar sub query test case
        
        1. Prepare data.
        2. Execute various queries with scalar sub queries.
        3. Validate the results against expected result files.

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-11 dapan Created

        """

        self.prepareData()
        self.execSqlCase()
        self.fileIdx += 1
        self.execFuncCase()
        self.rmoveSqlTmpFiles()
     
    def prepareData(self):
        tdLog.info("start to prepare data for scalar sub query test case")
        tdSql.execute(f"create snode on dnode 1")
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
        tdCom.compare_testcase_result(sqlFile, resFile, self.caseName + "." + str(self.fileIdx))
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

    def execSqlCase(self):
        tdLog.info(f"execSqlCase begin")

        self.openSqlTmpFile()

        for self.mainIdx in range(len(self.subSqls)):
            for self.subIdx in range(len(self.scalarSqls)):
                self.querySql = self.subSqls[self.mainIdx].replace("{scalarSql}", self.scalarSqls[self.subIdx])
                self.querySql = self.querySql.replace("{tableName}", "st1")
                # ensure exactly one trailing semicolon
                self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                tdLog.info(f"generated sql: {self.querySql}")

                self.saved_count += 1
                self._query_saved_count = self.saved_count

                self.generated_queries_file.write(self.querySql.strip() + "\n")
                self.generated_queries_file.flush()

        for self.mainIdx in range(len(self.unsupportedSqls)):
            self.querySql = self.unsupportedSqls[self.mainIdx].replace("{scalarSql}", self.scalarSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        return True

    def execFuncCase(self):
        tdLog.info(f"execFuncCase begin")

        self.openSqlTmpFile()

        for self.funcIdx in range(len(self.func_1_param)):
            self.querySql = self.funcSqls[0].replace("{funcName}", self.func_1_param[self.funcIdx])
            self.querySql = self.querySql.replace("{scalarSql}", self.scalarSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_1c_param)):
            self.querySql = self.funcSqls[0].replace("{funcName}", self.func_1c_param[self.funcIdx])
            self.querySql = self.querySql.replace("{scalarSql}", self.scalarcSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_2_param)):
            self.querySql = self.funcSqls[1].replace("{funcName}", self.func_2_param[self.funcIdx])
            self.querySql = self.querySql.replace("{scalarSql}", self.scalarSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_3_param)):
            self.querySql = self.funcSqls[2].replace("{funcName}", self.func_3_param[self.funcIdx])
            self.querySql = self.querySql.replace("{scalarSql}", self.scalarSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_nc_param)):
            self.querySql = self.funcSqls[3].replace("{funcName}", self.func_nc_param[self.funcIdx])
            self.querySql = self.querySql.replace("{scalarSql}", self.scalarSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.speFuncSqls)):
            self.querySql = self.speFuncSqls[self.funcIdx].replace("{scalarSql}", self.scalarSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.speCFuncSqls)):
            self.querySql = self.speCFuncSqls[self.funcIdx].replace("{scalarSql}", self.scalarcSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1
            self._query_saved_count = self.saved_count

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        return True