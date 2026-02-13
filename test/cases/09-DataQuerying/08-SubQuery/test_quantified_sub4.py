import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime

class TestQuantifiedSubQuery4:
    updatecfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    clientCfgDict = {'debugFlag': 131, 'asyncLog': 1, 'qDebugFlag': 131, 'cDebugFlag': 131, 'rpcDebugFlag': 131}
    updatecfgDict["clientCfg"] = clientCfgDict    
    caseName = "test_quantified_sub_query4"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    mainIdx = 0
    subIdx = 0
    fileIdx = 0
    funcIdx = 0
    saved_count = 0  # total number of queries saved so far
    maxFileQueryNum = 10000000  # max number of queries to save in a single file
    tableNames = ["tb1", "tb2", "tb3", "tba", "tbb", "tbe", "st1"]
    func_1_param = ["abs", "acos", "asin", "atan", "ceil", "cos", "degrees", "exp", "floor", "ln", "log", "radians", "round", "sign", "sin", "sqrt", "tan", "crc32", "date", "dayofweek", "weekday", "week", "weekofyear", "csum", "diff", "irate", "twa"]
    func_1c_param = ["ascii", "char_length", "length", "lower", "ltrim", "rtrim", "trim", "upper", "to_base64"]
    func_2_param = ["pow", "truncate", "mod", "corr", "find_in_set", "like_in_set", "regexp_in_set", "timetruncate", "timediff", "bottom", "top", "ifnull", "nvl", "nullif"]
    func_3_param = ["percentile", "nvl2"]
    func_nc_param = ["concat", "concat_ws", "greatest", "least"]

    unsupportedSqls = [
        "create stream stm1 interval(1d) sliding(1d) from tb1 partition by {quantifiedSql} into out1 as select * from {tableName}",
        "create stream stm1 interval(1d) sliding(1d) from tb1 into out2 as select ts, {quantifiedSql} from {tableName}",
        "create topic topic1 as select {quantifiedSql} from {tableName}",      
        "insert into tb1 values(now, {quantifiedSql}, 1)",
        "alter table tb1 set tag tg1 = {quantifiedSql}",
    ]

    specSqls = [
        "select f1 from {tableName} where ts >= {quantifiedSql} and ts <= {quantifiedSql} order by 1",
        "select f1 from {tableName} where _c0 between {quantifiedSql} and {quantifiedSql} from {tableName}) order by 1",
    ]

    speCFuncSqls = [
        "select position({quantifiedSql} in 'abc') from {tableName}",
        "select position('a' in {quantifiedSql}) from {tableName}",
        "select repeat({quantifiedSql}, 2) from {tableName}",
        "select substring({quantifiedSql}, 1, 2) from {tableName}",
        "select substring({quantifiedSql} from 1 for 1) from {tableName}",
        "select cast({quantifiedSql} as int) from {tableName}",
        "select to_char(cast(1 as timestamp), {quantifiedSql}) from {tableName}",
        "select to_json({quantifiedSql}) from {tableName}",
        "select to_iso8601(cast(1 as timestamp), {quantifiedSql}) from {tableName}",
        "select replace({quantifiedSql}, 'a', 'b') from {tableName}",
        "select replace('ab', {quantifiedSql}, 'b') from {tableName}",
        "select replace('ab', 'a', {quantifiedSql}) from {tableName}",
        "select replace({quantifiedSql}, {quantifiedSql}, {quantifiedSql}) from {tableName}",
        "select substring_index({quantifiedSql}, 'a', 1) from {tableName}",
        "select substring_index('abc', {quantifiedSql}, 1) from {tableName}",
        "select apercentile(1, 1, {quantifiedSql}) from {tableName}",
        "select cols(last(f1), {quantifiedSql}) from {tableName}",
        "select to_timestamp('2025', {quantifiedSql}) from {tableName}",
        "select to_timestamp({quantifiedSql}, 'yy') from {tableName}",
        "select to_unixtimestamp({quantifiedSql}, 1) from {tableName}",
        "select statecount(1, {quantifiedSql}, 1) from {tableName}",
    ]

    speFuncSqls = [
        "select repeat('ab', {quantifiedSql}) from {tableName}",
        "select substring('abc', {quantifiedSql}) from {tableName}",
        "select substring('abc', 1, {quantifiedSql}) from {tableName}",
        "select substring('abc' from {quantifiedSql}) from {tableName}",
        "select substring('abc' from 1 for {quantifiedSql}) from {tableName}",
        "select cast({quantifiedSql} as bigint) from {tableName}",
        "select to_char(cast({quantifiedSql} as timestamp), 'am') from {tableName}",
        "select to_iso8601({quantifiedSql}, 'z') from {tableName}",
        "select substring_index('abc', 'b', {quantifiedSql}) from {tableName}",
        "select apercentile({quantifiedSql}, 1) from {tableName}",
        "select apercentile(f1, {quantifiedSql}) from {tableName}",
        "select histogram({quantifiedSql}, 'user_input', '\"user_input\": \"[1, 3, 5, 7]\"', 0) from {tableName}",
        "select histogram(f1, 'user_input', '\"user_input\": \"[1, 3, 5, 7]\"', {quantifiedSql}) from {tableName}",
        "select cols(last(f1), {quantifiedSql}) from {tableName}",
        "select rand({quantifiedSql}) from tb1",
        "select to_unixtimestamp('2025-10-01', {quantifiedSql}) from {tableName}",
        "select derivative({quantifiedSql}, 1s, 1) from {tableName}",
        "select derivative(1, 1s, {quantifiedSql}) from {tableName}",
        "select statecount({quantifiedSql}, 'GT', 1) from {tableName}",
        "select statecount(f1, 'GT', {quantifiedSql}) from {tableName}",
        "select stateduration(f1, 'GT', {quantifiedSql}, 1s) from {tableName}",
        "select sample(f1, {quantifiedSql}) from {tableName}",
        "select sample({quantifiedSql}, 1) from {tableName}",
        "select mavg(f1, {quantifiedSql}) from {tableName}",
        "select mavg({quantifiedSql}, 1) from {tableName}",
    ]

    funcSqls = [
        "select {funcName}({quantifiedSql}) from {tableName}",
        "select {funcName}({quantifiedSql}, {quantifiedSql}) from {tableName}",
        "select {funcName}({quantifiedSql}, {quantifiedSql}, {quantifiedSql}) from {tableName}",
        "select {funcName}({quantifiedSql}, {quantifiedSql}, {quantifiedSql}, {quantifiedSql}) from {tableName}",
    ]
    subSqls = [
        # select 
        "select {quantifiedSql} from {tableName}",
        "select {quantifiedSql} a from {tableName}",
        "select distinct {quantifiedSql} from {tableName}",
        "select distinct {quantifiedSql}, f1 from {tableName}",
        "select distinct tags {quantifiedSql} + 1 from {tableName}",
        "select {quantifiedSql}, {quantifiedSql} from {tableName}",
        "select abs({quantifiedSql}) from {tableName}",
        "select abs({quantifiedSql} + 1) from {tableName}",
        "select sum({quantifiedSql}) from {tableName}",
        "select case when {quantifiedSql} then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when {quantifiedSql} > 0 then 1 else 2 end, sum(f1) from {tableName} group by f1",
        "select case when 1 = 1 then {quantifiedSql} else 2 end, sum(f1) from {tableName} group by f1",
        "select case when 0 = 1 then 1 else {quantifiedSql} end, sum(f1) from {tableName} group by f1",
        "select {quantifiedSql} from information_schema.ins_tables limit 3",
        "select {quantifiedSql} from vtb1",

        # from
        "select a.ts from {tableName} a join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a join {tableName} b on a.ts = b.ts and a.f1 = {quantifiedSql} order by 1",
        "select b.ts from {tableName} a left join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a right join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a full join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select b.ts from {tableName} a left semi join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a right semi join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select b.ts from {tableName} a left anti join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a right anti join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select b.ts from {tableName} a left asof join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a right asof join {tableName} b on a.ts = b.ts and {quantifiedSql} order by 1",
        "select b.ts from {tableName} a left asof join {tableName} b on a.ts = b.ts jlimit {quantifiedSql} order by 1",
        "select a.ts from {tableName} a right asof join {tableName} b on a.ts = b.ts jlimit {quantifiedSql} order by 1",
        "select b.ts from {tableName} a left window join {tableName} b on a.f1 = {quantifiedSql} window_offset(-1d, 1d) order by 1",
        "select a.ts from {tableName} a right window join {tableName} b on b.f1 = {quantifiedSql} window_offset(-1d, 1d) order by 1",
        "select b.ts from {tableName} a left window join {tableName} b window_offset(-1d, 1d) jlimit {quantifiedSql} order by 1",
        "select a.ts from {tableName} a right window join {tableName} b window_offset(-1d, 1d) jlimit {quantifiedSql} order by 1",

        # where
        "select a.ts from {tableName} a , {tableName} b where a.ts = b.ts and {quantifiedSql} order by 1",
        "select a.ts from {tableName} a , {tableName} b where a.ts = b.ts and a.f1 = {quantifiedSql} order by 1",
        "select f1 from {tableName} where {quantifiedSql} order by 1",
        "select f1 from {tableName} where f1 = {quantifiedSql} order by 1",
        "select f1 from {tableName} where f1 in ({quantifiedSql}) order by 1",
        "select f1 from {tableName} where f1 not in ({quantifiedSql}) order by 1",
        "select f1 from {tableName} where f1 like ({quantifiedSql}) order by 1",
        "select f1 from {tableName} where f1 not like ({quantifiedSql}) order by 1",
        "select f1 from {tableName} where f1 match ({quantifiedSql}) order by 1",
        "select f1 from {tableName} where f1 nmatch ({quantifiedSql}) order by 1",
        "select f1 from {tableName} where {quantifiedSql} is null order by 1",
        "select f1 from {tableName} where {quantifiedSql} is not null order by 1",
        "select f1 from {tableName} where abs({quantifiedSql}) > 0 order by 1",
        "select f1 from {tableName} where f1 > {quantifiedSql} or f1 < {quantifiedSql} order by 1",
        "select f1 from {tableName} where f1 between {quantifiedSql} and {quantifiedSql} + 1 order by 1",
        "select f1 from {tableName} where {quantifiedSql} > 0 and f1 > {quantifiedSql} order by 1",
        "select f1 from {tableName} where tg1 = {quantifiedSql} order by 1",
        "select f1 from {tableName} where tbname = cast({quantifiedSql} as varchar) order by 1",
        "select f1 from {tableName} where ts >= {quantifiedSql} and ts <= {quantifiedSql} order by 1",
        "select f1 from {tableName} where _c0 between {quantifiedSql} and {quantifiedSql} order by 1",
        "select table_name from information_schema.ins_tables where table_name = cast({quantifiedSql} as varchar) order by 1 limit 3",
        "select * from information_schema.ins_tables where db_name = cast({quantifiedSql} as varchar) order by 1 limit 3",
        "select * from vtb1 where f1 = {quantifiedSql} order by 1",

        # partition
        "select f1 from {tableName} partition by {quantifiedSql} order by 1",
        "select f1 from {tableName} partition by {quantifiedSql} + 1 order by 1",
        "select f1 from {tableName} partition by tbname, {quantifiedSql} order by 1",

        # window
        "select avg(f1) from {tableName} INTERVAL({quantifiedSql})",
        "select avg(f1) from {tableName} INTERVAL({quantifiedSql}d)",
        "select avg(f1) from {tableName} INTERVAL(1d, {quantifiedSql})",
        "select avg(f1) from {tableName} INTERVAL(1d, {quantifiedSql}d)",
        "select avg(f1) from {tableName} INTERVAL(1d) FILL({quantifiedSql})",
        "select avg(f1) from {tableName} where ts >= '2025-12-01 00:00:00.000' and ts <= '2025-12-05 00:00:00.000' INTERVAL(1d) FILL(value, {quantifiedSql})",
        "select avg(f1) from {tableName} INTERVAL(1d) SLIDING({quantifiedSql})",
        "select avg(f1) from {tableName} INTERVAL(1d) SLIDING({quantifiedSql}h)",
        "select avg(f1) from {tableName} SESSION(ts, {quantifiedSql})",
        "select avg(f1) from {tableName} SESSION(ts, {quantifiedSql}h)",
        "select avg(f1) from {tableName} SESSION({quantifiedSql}, 1d)",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW({quantifiedSql})",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW(f1, {quantifiedSql})",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW(f1) TRUE_FOR({quantifiedSql})",
        "select avg(f1) from {tableName} where tbname = 'tb1' STATE_WINDOW(f1) TRUE_FOR({quantifiedSql}d)",
        "select avg(f1) from {tableName} where tbname = 'tb1' EVENT_WINDOW START WITH f1 >= {quantifiedSql} END WITH f1 <= {quantifiedSql} + 1",
        "select avg(f1) from {tableName} COUNT_WINDOW({quantifiedSql})",
        "select avg(f1) from {tableName} COUNT_WINDOW({quantifiedSql}, {quantifiedSql})",
        "select avg(f1) from {tableName} COUNT_WINDOW(3, 1, {quantifiedSql})",

        # group
        "select avg(f1) from {tableName} group by {quantifiedSql}",
        "select avg(f1) from {tableName} group by f1, {quantifiedSql}",
        "select avg(f1) from {tableName} group by {quantifiedSql} + 1",

        # having
        "select avg(f1) from {tableName} group by f1 having avg({quantifiedSql}) > 0",
        "select avg(f1) from {tableName} group by f1 having {quantifiedSql} > 0",
        "select avg(f1) from {tableName} group by f1 having avg({quantifiedSql} + 1) > 0",

        # interp
        "select interp(f1) from {tableName} range({quantifiedSql}) FILL(PREV)",
        "select interp(f1) from {tableName} range({quantifiedSql}, {quantifiedSql}) every(1d)",
        "select interp(f1) from {tableName} range(now - 1d, now, {quantifiedSql}) every(1d)", 
        "select interp(f1) from {tableName} range(now - 1d, now) every({quantifiedSql})",
        "select interp(f1) from {tableName} range(now - 1d, now) every({quantifiedSql}m)",
        "select interp(f1) from {tableName} range(now - 1d, now) every(1h) fill({quantifiedSql})",
        "select interp(f1) from {tableName} range(now - 1d, now) every(1h) fill(value, {quantifiedSql})",

        # order
        "select f1 from tb1 order by {quantifiedSql}",
        "select f1 from tb1 order by {quantifiedSql} desc",
        "select f1 from tb1 order by f1 + {quantifiedSql}",
        "select f1 from {tableName} order by f1, {quantifiedSql}",
        "select f1 from tb1 order by abs({quantifiedSql})",
        "select avg(f1) from tb1 order by sum({quantifiedSql})",

        # limit/slimit
        "select f1 from {tableName} limit {quantifiedSql}",
        "select f1 from {tableName} limit 1 offset {quantifiedSql}",
        "select f1 from {tableName} partition by tbname slimit {quantifiedSql}",
        "select f1 from {tableName} partition by tbname slimit 1 soffset {quantifiedSql}",

        # union
        "select f1 from {tableName} where f1 > {quantifiedSql} union select f1 from {tableName} order by f1",
        "select f1 from {tableName} union all select {quantifiedSql} from {tableName} order by f1",
        "select f1 from tb1 union select f1 from tb1 order by {quantifiedSql}",
        "select f1 from tb1 union all select f1 from tb1 limit {quantifiedSql}",

        # explain
        "explain select f1 from {tableName} where {quantifiedSql}\G",
        "explain select f1 from {tableName} where f1 = {quantifiedSql}\G",
        "explain verbose true select f1 from {tableName} where {quantifiedSql}\G",
        "explain analyze select f1 from {tableName} where {quantifiedSql}\G",
        "explain analyze verbose true select f1 from {tableName} where {quantifiedSql}\G",
        "explain select avg(f1), {quantifiedSql} from {tableName} group by f1 having {quantifiedSql}\G",
        "explain verbose true select avg(f1), {quantifiedSql} from {tableName} group by f1 having {quantifiedSql}\G",
        "explain analyze select avg(f1), {quantifiedSql} from {tableName} group by f1 having {quantifiedSql}\G",
        "explain analyze verbose true select avg(f1), {quantifiedSql} from {tableName} group by f1 having {quantifiedSql}\G",

        # view
        "select f1 from {tableName} where f1 = (select * from v1 where {quantifiedSql}) order by 1",
        "select f1 from v2 where {quantifiedSql} order by 1",
        "select f1 from v3 where {quantifiedSql} order by 1",
        "select f1 from v4 where {quantifiedSql} order by 1",

        # insert
        "insert into tbb select now, {quantifiedSql} from tb1",  
    ]


    correlatedSqls = [
        "select f1 from tb1 a where f1 = all (select a.f1 from tb2 limit 1)",
        "select f1 from tb1 a where f1 = any (select tb1.f1 from tb2 limit 1)",
        "select f1 from tb1 a where f1 = some (select f1 from tb2 where f1 = a.f1 limit 1)",
        "select f1 from tb1 a where f1 = all (select f1 from tb2 where f1 = tb1.f1 limit 1)",
        "select f1 from (select * from tb1) a where f1 = all (select f1 from tb2 where f1 = a.f1 limit 1)",
        "select f1 from (select * from (select * from tb1 where f1 = all (select f1 from b) a) b",
        "select f1 from (select * from (select * from tb1 where f1 = all (select f1 from a) a) b",
        "select a.f1 from (select * from tb1) a join (select * from tb2 where f1 = all (select f1 from a)) b on a.ts = b.ts and a.f1 = b.f1",
        "select a.f1 from (select * from tb1) a join (select * from tb2 where f1 = all (select f1 from b)) b on a.ts = b.ts and a.f1 = b.f1",
        "select (select avg(f1) from tb2 where f1 < all (select f1 from a)) from tb1 a",
        "select f1 from tb1 a where f1 = (select f1 from tb2 where f1 = (select f1 from tb3 where f1 = any (select f1 from a) limit 1) limit 1)",
    ]

    quantifiedcSqls = [
        "'a' = any (select 'a' from {tableName} limit 1)",
        "'am' != any (select 'am' from {tableName} limit 1)",
        "not exists (select 'abc' from {tableName} limit 1)",
    ]

    quantifiedSqls = [
        "1 < all (select 0 from {tableName})",
        "1 > all (select 0 from {tableName})",
        "NULL = any (select 2 from {tableName})",
        "exists (select f1 from db2.tba)",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_quantified_sub_query4(self):
        """quantified expr sub query test case
        
        1. Prepare data.
        2. Execute various queries with quantified expr sub queries.
        3. Validate the results against expected result files.

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-02-12 dapan Created

        """

        self.prepareData()
        self.execSqlCase()
        self.fileIdx += 1
        self.execFuncCase()
        self.fileIdx += 1
        self.execCorrelatedCase()
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
            "create view v1 as select f1 from tb1",
            "create view v2 as select f1 from st1 where f1 = all (select f1 from st1 order by ts, f1)",
            "create view v3 as select f1 from st1 where f1 = any (select f1 from st1 order by ts, f1)",
            "create view v4 as select f1 from st1 where exists (select f1 from st1 order by ts, f1)",
            "create vtable vtb1 (ts timestamp, f1 int from tb1.f1)",
        ]
        tdSql.executes(sqls)

        tdSql.execute(f"drop database if exists db2")
        tdSql.execute(f"create database db2")
        tdSql.execute(f"use db2")
        sqls = [
            "CREATE TABLE tba (ts TIMESTAMP, f1 int)",
            "INSERT INTO tba VALUES ('2025-12-11 00:00:00.000', 0)",
        ]
        tdSql.executes(sqls)
        tdSql.execute(f"use db1")

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
            for self.subIdx in range(len(self.quantifiedSqls)):
                self.querySql = self.subSqls[self.mainIdx].replace("{quantifiedSql}", self.quantifiedSqls[self.subIdx])
                self.querySql = self.querySql.replace("{tableName}", "st1")
                # ensure exactly one trailing semicolon
                self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                #tdLog.info(f"generated sql: {self.querySql}")

                self.saved_count += 1
                self._query_saved_count = self.saved_count

                self.generated_queries_file.write(self.querySql.strip() + "\n")
                self.generated_queries_file.flush()

        for self.mainIdx in range(len(self.specSqls)):
            for self.subIdx in range(len(self.quantifiedSqls)):
                self.querySql = self.specSqls[self.mainIdx].replace("{tableName}", "st1")
                self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedSqls[self.subIdx])
                # ensure exactly one trailing semicolon
                self.querySql = self.querySql.rstrip().rstrip(';') + ';'
                #tdLog.info(f"generated sql: {self.querySql}")

                self.saved_count += 1

                self.generated_queries_file.write(self.querySql.strip() + "\n")
                self.generated_queries_file.flush()

        for self.mainIdx in range(len(self.unsupportedSqls)):
            self.querySql = self.unsupportedSqls[self.mainIdx].replace("{quantifiedSql}", self.quantifiedSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

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
            self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_1c_param)):
            self.querySql = self.funcSqls[0].replace("{funcName}", self.func_1c_param[self.funcIdx])
            self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedcSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_2_param)):
            self.querySql = self.funcSqls[1].replace("{funcName}", self.func_2_param[self.funcIdx])
            self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_3_param)):
            self.querySql = self.funcSqls[2].replace("{funcName}", self.func_3_param[self.funcIdx])
            self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.func_nc_param)):
            self.querySql = self.funcSqls[3].replace("{funcName}", self.func_nc_param[self.funcIdx])
            self.querySql = self.querySql.replace("{quantifiedSql}", self.quantifiedSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.speFuncSqls)):
            self.querySql = self.speFuncSqls[self.funcIdx].replace("{quantifiedSql}", self.quantifiedSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        for self.funcIdx in range(len(self.speCFuncSqls)):
            self.querySql = self.speCFuncSqls[self.funcIdx].replace("{quantifiedSql}", self.quantifiedcSqls[1])
            self.querySql = self.querySql.replace("{tableName}", "tb3")
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            #tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        return True

    def execCorrelatedCase(self):
        tdLog.info(f"execCorrelatedCase begin")

        self.openSqlTmpFile()

        for self.mainIdx in range(len(self.correlatedSqls)):
            self.querySql = self.correlatedSqls[self.mainIdx]
            # ensure exactly one trailing semicolon
            self.querySql = self.querySql.rstrip().rstrip(';') + ';'
            tdLog.info(f"generated sql: {self.querySql}")

            self.saved_count += 1

            self.generated_queries_file.write(self.querySql.strip() + "\n")
            self.generated_queries_file.flush()

        self.generated_queries_file.close()
        tmp_file = os.path.join(self.currentDir, f"{self.caseName}_generated_queries{self.fileIdx}.sql")
        res_file = os.path.join(self.currentDir, f"ans/{self.caseName}.{self.fileIdx}.csv")
        self.checkResultWithResultFile(tmp_file, res_file)

        return True   