/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "mockCatalogService.h"
#include "parAst.h"
#include "parTestUtil.h"
#include "mockCatalogService.h"
#include "tglobal.h"

using namespace std;

namespace ParserTest {

class ParserSelectTest : public ParserTestBase {};
class ParserSelectDdlTest : public ParserDdlTest {};

namespace {

class FederatedFlagGuard {
 public:
  FederatedFlagGuard() : previous_(tsFederatedQueryEnable) { tsFederatedQueryEnable = true; }
  ~FederatedFlagGuard() { tsFederatedQueryEnable = previous_; }

 private:
  bool previous_;
};

void registerUnknownExternalColumn(const std::string& sourceName, int8_t sourceType,
                                   const std::string& timestampType, const std::string& integerType) {
  std::vector<MockCatalogService::MockExtColDef> columns;
  columns.push_back({"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, timestampType});
  columns.push_back({"id", TSDB_DATA_TYPE_BIGINT, false, true, false, integerType});
  columns.push_back({"unknown_value", TSDB_DATA_TYPE_NCHAR, false, true, false,
                     "UnknownExternalBoundaryType"});

  g_mockCatalogService->createExtSource(sourceName, sourceType, "127.0.0.1", 1, "fake_user", "fake_db", "public");
  g_mockCatalogService->createExtTable(sourceName, "", "", "metrics", columns, TSDB_TIME_PRECISION_MILLI);
}

void registerExternalTimelineTable(const std::string& sourceName) {
  std::vector<MockCatalogService::MockExtColDef> columns;
  columns.push_back({"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false,
                     "timestamp"});
  columns.push_back({"id", TSDB_DATA_TYPE_BIGINT, false, true, false,
                     "bigint"});

  g_mockCatalogService->createExtSource(
      sourceName, EXT_SOURCE_POSTGRESQL, "127.0.0.1", 1, "fake_user",
      "fake_db", "public");
  g_mockCatalogService->createExtTable(
      sourceName, "", "", "metrics", columns, TSDB_TIME_PRECISION_MILLI);
}

}  // namespace

TEST_F(ParserSelectDdlTest, externalColumnFilterValidation) {
  FederatedFlagGuard guard;
  login("root");
  g_mockCatalogService->createDatabase("vfilter", false, 0, TSDB_TIME_PRECISION_MICRO);
  useDb("root", "vfilter");

  std::vector<MockCatalogService::MockExtColDef> columns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "timestamp"},
      {"device", TSDB_DATA_TYPE_BIGINT, true, false, false, "bigint"},
      {"value", TSDB_DATA_TYPE_DOUBLE, false, true, false, "double precision"},
      {"active", TSDB_DATA_TYPE_BOOL, false, true, false, "boolean"},
      {"created_at", TSDB_DATA_TYPE_TIMESTAMP, false, true, false, "timestamp"},
      {"time_of_day", TSDB_DATA_TYPE_BIGINT, false, true, false, "time"},
  };
  g_mockCatalogService->createExtSource("ext_filter_pg", EXT_SOURCE_POSTGRESQL, "127.0.0.1", 5432,
                                        "fake_user", "fake_db", "public");
  g_mockCatalogService->createExtTable("ext_filter_pg", "fake_db", "", "metrics", columns,
                                       TSDB_TIME_PRECISION_MICRO);

  runAsyncOnly("create vtable v_filter_ok (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where (device = 7) and value >= 1.5 and active = true and "
               "created_at < '2026-01-01' and time_of_day >= 45000000000)");
  runAsyncOnly("create vtable test.v_filter_low_precision (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7)",
               TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE);
  runAsyncOnly("create vtable v_filter_bad_time (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7 and time_of_day >= '12:30:00')",
               TSDB_CODE_PAR_WRONG_VALUE_TYPE);
  runAsyncOnly("create vtable v_filter_missing_pk (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value)",
               TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
  runAsyncOnly("create vtable v_filter_pk_non_eq (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device > 7)",
               TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
  runAsyncOnly("create vtable v_filter_duplicate (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7 and device = 8)",
               TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN);
  runAsyncOnly("create vtable v_filter_or (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7 or active = true)",
               TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  runAsyncOnly("create vtable v_filter_rhs_col (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = value)",
               TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  runAsyncOnly("create vtable v_filter_qualified (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where metrics.device = 7)",
               TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  runAsyncOnly("create vtable v_filter_bad_timestamp (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7 and created_at = 1)",
               TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  runAsyncOnly("create vtable v_filter_bad_value (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 'abc')",
               TSDB_CODE_PAR_WRONG_VALUE_TYPE);
  runAsyncOnly("create vtable v_filter_bad_float (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7 and value = 'abc')",
               TSDB_CODE_PAR_WRONG_VALUE_TYPE);
  runAsyncOnly("create vtable v_filter_bad_bool (ts timestamp, value double from "
               "ext_filter_pg.fake_db.metrics.value where device = 7 and active = 'not_bool')",
               TSDB_CODE_PAR_WRONG_VALUE_TYPE);
  runAsyncOnly("create vtable v_filter_local (ts timestamp, value int from test.t1.c1 where c2 = 1)",
               TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED);

  std::vector<MockCatalogService::MockExtColDef> influxColumns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "Timestamp"},
      {"device", TSDB_DATA_TYPE_NCHAR, false, true, true, "Utf8"},
      {"value", TSDB_DATA_TYPE_DOUBLE, false, true, false, "Float64"},
  };
  g_mockCatalogService->createExtSource("ext_filter_influx", EXT_SOURCE_INFLUXDB, "127.0.0.1", 8086,
                                        "fake_user", "fake_db", "");
  g_mockCatalogService->createExtTable("ext_filter_influx", "fake_db", "", "metrics", influxColumns,
                                       TSDB_TIME_PRECISION_NANO);
  runAsyncOnly("create vtable v_filter_influx (ts timestamp, value double from "
               "ext_filter_influx.fake_db.metrics.value where device = 'd1')",
               TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED);

  ITableBuilder& alterTable =
      g_mockCatalogService->createTableBuilder("vfilter", "v_filter_alter", TSDB_VIRTUAL_NORMAL_TABLE, 2)
          .setPrecision(TSDB_TIME_PRECISION_MICRO)
          .setVgid(2)
          .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
          .addColumn("value", TSDB_DATA_TYPE_DOUBLE);
  alterTable.done();
  runAsyncOnly("alter vtable v_filter_alter add column value2 double from "
               "ext_filter_pg.fake_db.metrics.value where device = 8");
  runAsyncOnly("alter vtable v_filter_alter alter column value set "
               "ext_filter_pg.fake_db.metrics.value where device = 9");
  runAsyncOnly("alter vtable v_filter_alter alter column value set null");

  std::vector<MockCatalogService::MockExtColDef> timestampOnlyColumns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "timestamp"},
      {"value", TSDB_DATA_TYPE_DOUBLE, false, true, false, "double precision"},
  };
  g_mockCatalogService->createExtSource("ext_filter_single_pk", EXT_SOURCE_POSTGRESQL, "127.0.0.1", 5432,
                                        "fake_user", "fake_db", "public");
  g_mockCatalogService->createExtTable("ext_filter_single_pk", "fake_db", "", "metrics", timestampOnlyColumns,
                                       TSDB_TIME_PRECISION_MICRO);
  runAsyncOnly("create vtable v_filter_no_cond (ts timestamp, value double from "
               "ext_filter_single_pk.fake_db.metrics.value)");

  std::vector<MockCatalogService::MockExtColDef> noPrimaryKeyColumns = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, false, false, false, "timestamp"},
      {"value", TSDB_DATA_TYPE_DOUBLE, false, true, false, "double precision"},
  };
  g_mockCatalogService->createExtSource("ext_filter_no_pk", EXT_SOURCE_POSTGRESQL, "127.0.0.1", 5432,
                                        "fake_user", "fake_db", "public");
  g_mockCatalogService->createExtTable("ext_filter_no_pk", "fake_db", "", "metrics", noPrimaryKeyColumns,
                                       TSDB_TIME_PRECISION_MICRO);
  runAsyncOnly("create vtable v_filter_no_pk (ts timestamp, value double from "
               "ext_filter_no_pk.fake_db.metrics.value)",
               TSDB_CODE_EXT_NO_TS_PRIMARY_KEY);

  std::vector<MockCatalogService::MockExtColDef> twoTimestampPrimaryKeys = {
      {"ts", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "timestamp"},
      {"ts2", TSDB_DATA_TYPE_TIMESTAMP, true, false, false, "timestamp"},
      {"value", TSDB_DATA_TYPE_DOUBLE, false, true, false, "double precision"},
  };
  g_mockCatalogService->createExtSource("ext_filter_two_ts", EXT_SOURCE_POSTGRESQL, "127.0.0.1", 5432,
                                        "fake_user", "fake_db", "public");
  g_mockCatalogService->createExtTable("ext_filter_two_ts", "fake_db", "", "metrics", twoTimestampPrimaryKeys,
                                       TSDB_TIME_PRECISION_MICRO);
  runAsyncOnly("create vtable v_filter_two_ts (ts timestamp, value double from "
               "ext_filter_two_ts.fake_db.metrics.value)",
               TSDB_CODE_EXT_NO_TS_PRIMARY_KEY);
}

namespace {

SNodeList* createHint(SAstCreateContext* pCxt, std::string& hint) {
  SToken literal = {0};
  literal.z = &hint[0];
  literal.n = static_cast<uint32_t>(hint.size());
  return createHintNodeList(pCxt, &literal);
}

SNodeList* createHintWithParams(SAstCreateContext* pCxt, int32_t paramCount) {
  std::string hint = "/*+ BATCH_SCAN(";
  for (int32_t i = 0; i < paramCount; ++i) {
    if (0 != i) {
      hint += ',';
    }
    hint += "PARAM" + std::to_string(i);
  }
  hint += ") */";

  return createHint(pCxt, hint);
}

void checkHintParams(const SHintNode* pHint, int32_t paramCount) {
  ASSERT_NE(pHint->pParams, nullptr);
  ASSERT_EQ(taosArrayGetSize(pHint->pParams), paramCount);
  for (int32_t i = 0; i < paramCount; ++i) {
    const auto* param = static_cast<const SHintParam*>(taosArrayGet(pHint->pParams, i));
    ASSERT_NE(param, nullptr);
    EXPECT_EQ(param->type, TK_NK_ID);
    EXPECT_STREQ(param->value, ("PARAM" + std::to_string(i)).c_str());
  }
}

}  // namespace

TEST_F(ParserSelectTest, basic) {
  useDb("root", "test");

  run("SELECT * FROM t1");

  run("SELECT * FROM test.t1");

  run("SELECT ts, c1 FROM t1");

  run("SELECT ts, t.c1 FROM (SELECT * FROM t1) t");

  run("SELECT * FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1");

  run("SELECT * FROM st1");
}

TEST_F(ParserSelectTest, constant) {
  useDb("root", "test");

  run("SELECT 123, 20.4, 'abc', \"wxy\", timestamp '2022-02-09 17:30:20', true, false, 10s FROM t1");

  run("SELECT 1234567890123456789012345678901234567890, 20.1234567890123456789012345678901234567890, 'abc', \"wxy\", "
      "timestamp '2022-02-09 17:30:20', true, false, 15s FROM t1");

  run("SELECT 123 + 45 FROM t1 WHERE 2 - 1");

  run("SELECT * FROM t1 WHERE -2");
}

TEST_F(ParserSelectTest, expression) {
  useDb("root", "test");

  run("SELECT ts + 10s, c1 + 10, concat(c2, 'abc') FROM t1");

  run("SELECT ts > 0, c1 < 20 and c2 = 'qaz' FROM t1");

  run("SELECT ts > 0, c1 between 10 and 20 and c2 = 'qaz' FROM t1");

  run("SELECT c1 | 10, c2 & 20, c4 | c5 FROM t1");

  run("SELECT CASE WHEN ts > '2020-1-1 10:10:10' THEN c1 + 10 ELSE c1 - 10 END FROM t1 "
      "WHERE CASE c1 WHEN c3 + 20 THEN c3 - 1 WHEN c3 + 10 THEN c3 - 2 ELSE 10 END > 0");
}

TEST_F(ParserSelectTest, condition) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 WHERE ts in (true, false)");

  run("SELECT c1 FROM t1 WHERE NOT ts in (true, false)");

  run("SELECT * FROM t1 WHERE c1 > 10 and c1 is not null");

  run("SELECT * FROM t1 WHERE TBNAME like 'fda%' or TS > '2021-05-05 18:19:01.000'");
}

TEST_F(ParserSelectTest, pseudoColumn) {
  useDb("root", "test");

  run("SELECT _WSTART, _WEND, COUNT(*) FROM t1 INTERVAL(10s)");
}

TEST_F(ParserSelectTest, intervalNaturalOffsetOverflow) {
  useDb("root", "testus");

  g_mockCatalogService->createTableBuilder("testus", "t_nano_interval", TSDB_NORMAL_TABLE, 2)
      .setPrecision(TSDB_TIME_PRECISION_NANO)
      .setVgid(1)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT)
      .done();

  run("SELECT COUNT(*) FROM t_nano_interval INTERVAL(3504n, 3503n)", TSDB_CODE_PAR_INTER_VALUE_TOO_BIG);
}

TEST_F(ParserSelectTest, pseudoColumnSemanticCheck) {
  useDb("root", "test");

  run("SELECT TBNAME FROM (SELECT * FROM st1s1)", TSDB_CODE_PAR_INVALID_TBNAME, PARSER_STAGE_TRANSLATE);
  run("SELECT x.tbname FROM st1", TSDB_CODE_PAR_INVALID_TBNAME, PARSER_STAGE_TRANSLATE);
  run("SELECT * FROM (SELECT tbname, avg(c1) FROM st1 PARTITION BY tbname) a PARTITION BY a.tbname ORDER BY a.tbname");
}

TEST_F(ParserSelectTest, aggFunc) {
  useDb("root", "test");

  run("SELECT LEASTSQUARES(c1, -1, 1) FROM t1");
}

TEST_F(ParserSelectTest, multiResFunc) {
  useDb("root", "test");

  run("SELECT LAST(*), FIRST(*), LAST_ROW(*) FROM t1");

  run("SELECT LAST(c1, c2), FIRST(t1.*), LAST_ROW(c3) FROM t1");

  run("SELECT LAST(t2.*), FIRST(t1.c1, t2.*), LAST_ROW(t1.*, t2.*) FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts");
}

TEST_F(ParserSelectTest, timelineFunc) {
  useDb("root", "test");

  run("SELECT LAST(*), FIRST(*) FROM t1");

  run("SELECT FIRST(ts), FIRST(c1), FIRST(c2), FIRST(c3) FROM t1");

  run("SELECT LAST(*), FIRST(*) FROM t1 GROUP BY c1");

  run("SELECT LAST(*), FIRST(*) FROM t1 INTERVAL(10s)");

  run("SELECT diff(c1) FROM t1");

  // timeline fallback: the subquery projects a TIMESTAMP column (ts), which diff uses as a degraded timeline
  run("select diff(ts) from (select _wstart as ts, count(*) from st1 partition by tbname interval(1d))");

  run("select diff(ts) from (select _wstart as ts, count(*) from st1 partition by tbname interval(1d) order by ts)");

  run("select t1.* from st1s1 t1, (select _wstart as ts, count(*) from st1s2 partition by tbname interval(1d)) t2 WHERE t1.ts = t2.ts");

  run("select t1.* from st1s1 t1, (select _wstart as ts, count(*) from st1s2 partition by tbname interval(1d) order by ts) t2 WHERE t1.ts = t2.ts");

}

TEST_F(ParserSelectTest, implicitHavingMatchesExplicitPartitionTimeline) {
  useDb("root", "test");

  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM st1 PARTITION BY TRUE)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM st1 HAVING c1 > 0)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM st1 HAVING TRUE)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM t1 PARTITION BY TRUE)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM t1 HAVING c1 > 0)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM st1s1 PARTITION BY TRUE)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  run("SELECT IRATE(c1) FROM "
      "(SELECT ts, c1 FROM st1s1 HAVING c1 > 0)",
      TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
}

TEST_F(ParserSelectDdlTest,
       implicitHavingMatchesExplicitPartitionTimelineMode) {
  useDb("root", "test");

  ETimeLineMode expected = TIME_LINE_MULTI;
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (PARSER_STAGE_TRANSLATE != stage) {
      return;
    }
    ASSERT_EQ(QUERY_NODE_SELECT_STMT, nodeType(pQuery->pRoot));
    const SSelectStmt* pSelect = (const SSelectStmt*)pQuery->pRoot;
    EXPECT_EQ(expected, pSelect->timeLineResMode);
  });

  run("SELECT ts, c1 FROM t1 PARTITION BY TRUE");
  run("SELECT ts, c1 FROM t1 HAVING c1 > 0");
  run("SELECT ts, c1 FROM (SELECT ts, c1 FROM t1) PARTITION BY TRUE");
  run("SELECT ts, c1 FROM (SELECT ts, c1 FROM t1) HAVING c1 > 0");
  run("SELECT a.ts, a.c1 FROM t1 a JOIN st1s1 b ON a.ts = b.ts "
      "PARTITION BY TRUE");
  run("SELECT a.ts, a.c1 FROM t1 a JOIN st1s1 b ON a.ts = b.ts "
      "HAVING a.c1 > 0");

  expected = TIME_LINE_GLOBAL;
  run("SELECT 1 PARTITION BY TRUE");
  run("SELECT 1 HAVING TRUE");
}

TEST_F(ParserSelectDdlTest, implicitHavingSurvivesCloneAndJsonRoundTrip) {
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (PARSER_STAGE_TRANSLATE != stage) {
      return;
    }

    SNode* pClone = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesCloneNode(pQuery->pRoot, &pClone));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(
        pClone, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_SELECT_STMT, nodeType(pClone));
    EXPECT_TRUE(((SSelectStmt*)pClone)->implicitPartitionHaving);

    char*   pJson = nullptr;
    int32_t len = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesNodeToString(pQuery->pRoot, false, &pJson, &len));
    SNode* pRoundTrip = nullptr;
    int32_t code = nodesStringToNode(pJson, &pRoundTrip);
    taosMemoryFreeClear(pJson);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    unique_ptr<SNode, decltype(&nodesDestroyNode)> roundTripGuard(
        pRoundTrip, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_SELECT_STMT, nodeType(pRoundTrip));
    EXPECT_TRUE(((SSelectStmt*)pRoundTrip)->implicitPartitionHaving);
  });

  run("SELECT c1 FROM t1 HAVING c1 > 0");
}

TEST_F(ParserSelectTest, implicitHavingDoesNotPreserveExternalTimeline) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  useDb("root", "test");
  FederatedFlagGuard guard;
  registerExternalTimelineTable("ext_timeline");

  runAsyncOnly("SELECT IRATE(id) FROM "
               "(SELECT ts, id FROM ext_timeline.metrics HAVING id > 0)",
               TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
#endif
}

TEST_F(ParserSelectTest, selectFunc) {
  useDb("root", "test");

  // select function
  run("SELECT MAX(c1), MIN(c1) FROM t1");
  // select function for GROUP BY clause
  run("SELECT MAX(c1), MIN(c1) FROM t1 GROUP BY c1");
  // select function for INTERVAL clause
  run("SELECT MAX(c1), MIN(c1) FROM t1 INTERVAL(10s)");
  // select function along with the columns of select row
  run("SELECT MAX(c1), c2 FROM t1");
  run("SELECT MAX(c1), t1.* FROM t1");
  // select function along with the columns of select row, and with GROUP BY clause
  run("SELECT MAX(c1), c2 FROM t1 GROUP BY c3");
  run("SELECT MAX(c1), t1.* FROM t1 GROUP BY c3");
  // select function along with the columns of select row, and with window clause
  run("SELECT MAX(c1), c2 FROM t1 INTERVAL(10s)");
  run("SELECT MAX(c1), c2 FROM t1 SESSION(ts, 10s)");
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3)");
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3, 1)");
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3, 1, 0)");
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3, 1, NULL)",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3) EXTEND(1)");
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3) EXTEND(1) ZEROTH_STATE(0)");
  run("SELECT MAX(c1), c2 FROM t1 STATE_WINDOW(c3) ZEROTH_STATE(NO_ZEROTH)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(CAST(c1 AS INT))");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(CAST(1 AS INT))");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(1)",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(true)",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW('aaa')",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("SELECT COUNT(*) FROM st1 STATE_WINDOW(tag1)");
  run("SELECT COUNT(*) FROM st1 "
      "STATE_WINDOW(CASE WHEN tag1 > 1 THEN 1 ELSE 0 END)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(CASE WHEN c1 > 10 THEN 1 ELSE 0 END, c2)");
  run("SELECT COUNT(*) FROM st1 PARTITION BY tbname "
      "STATE_WINDOW(CASE WHEN tbname = 'st1s1' THEN 1 ELSE 0 END)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1, CASE WHEN c2 = 'abc' THEN 1 ELSE 0 END)");
    run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1, c1)",
      TSDB_CODE_PAR_INVALID_STATE_WIN_COL);
    run("SELECT COUNT(*) FROM t1 tt STATE_WINDOW(tt.c1, c1)",
      TSDB_CODE_PAR_INVALID_STATE_WIN_COL);
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1, c2, 1)",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // Logic expressions as state key
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 = 0)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 >= 10)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 <> 0)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c2 = 'abc')");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 IS NULL)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 IS NOT NULL)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 IN (1, 2, 3))");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 BETWEEN 1 AND 10)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5 AND c3 < 100)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5 OR c3 < 100)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(NOT c1 > 5)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW((c1 > 5 AND c3 < 100) OR c1 = 0)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5, c3)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c3, c1 > 5 AND c1 < 100)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5) EXTEND(1)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5) ZEROTH_STATE(true)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5) EXTEND(1) ZEROTH_STATE(false)");
  run("SELECT COUNT(*) FROM st1 STATE_WINDOW(tag1 > 1)");
  run("SELECT COUNT(*) FROM st1 PARTITION BY tbname STATE_WINDOW(tag1 > 1)");
  // Constant logic expressions → now allowed
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(1 > 0)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(1 = 1)");
  // Semi-constant logic expressions → now allowed
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5 AND 1 > 2)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5 OR 1 < 2)");
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(NOT 1 > 0)");
  // Duplicate logic expressions → reject
  run("SELECT COUNT(*) FROM t1 STATE_WINDOW(c1 > 5, c1 > 5)",
      TSDB_CODE_PAR_INVALID_STATE_WIN_COL);
}

TEST_F(ParserSelectTest, IndefiniteRowsFunc) {
  useDb("root", "test");

  run("SELECT DIFF(c1) FROM t1");
}

TEST_F(ParserSelectTest, IndefiniteRowsFuncSemanticCheck) {
  useDb("root", "test");

  run("SELECT DIFF(c1), c2 FROM t1");

  run("SELECT DIFF(c1), tbname FROM t1");

  run("SELECT DIFF(c1), count(*) FROM t1", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);

  run("SELECT DIFF(c1), CSUM(c1) FROM t1", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
}

TEST_F(ParserSelectTest, useDefinedFunc) {
  useDb("root", "test");

  run("SELECT udf1(c1) FROM t1");

  run("SELECT udf2(c1) FROM t1 GROUP BY c2");
}

TEST_F(ParserSelectTest, uniqueFunc) {
  useDb("root", "test");

  run("SELECT UNIQUE(c1) FROM t1");

  run("SELECT UNIQUE(c2 + 10) FROM t1 WHERE c1 > 10");

  run("SELECT UNIQUE(c2 + 10), ts, c2 FROM t1 WHERE c1 > 10");
}

TEST_F(ParserSelectTest, uniqueFuncSemanticCheck) {
  useDb("root", "test");

  run("SELECT UNIQUE(c1) FROM t1 INTERVAL(10S)", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);

  run("SELECT UNIQUE(c1) FROM t1 GROUP BY c2", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
}

TEST_F(ParserSelectTest, tailFunc) {
  useDb("root", "test");

  run("SELECT TAIL(c1, 10) FROM t1");

  run("SELECT TAIL(c2 + 10, 10, 80) FROM t1 WHERE c1 > 10");
}

TEST_F(ParserSelectTest, tailFuncSemanticCheck) {
  useDb("root", "test");

  run("SELECT TAIL(c1, 10) FROM t1 INTERVAL(10S)", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);

  run("SELECT TAIL(c1, 10) FROM t1 GROUP BY c2", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
}

TEST_F(ParserSelectTest, partitionBy) {
  useDb("root", "test");

  run("SELECT c1, c2 FROM t1 PARTITION BY c2");

  run("SELECT SUM(c1), c2 FROM t1 PARTITION BY c2");

  run("SELECT SUM(c1), c1 + 10 cc1 FROM t1 PARTITION BY cc1");

  run("SELECT c1 AS new_c1, COUNT(c1) AS c1 FROM t1 PARTITION BY c1");
}

TEST_F(ParserSelectTest, partitionBySemanticCheck) {
  useDb("root", "test");

  run("SELECT SUM(c1), c2, c3 FROM t1 PARTITION BY c2", TSDB_CODE_PAR_NOT_SINGLE_GROUP);
}

TEST_F(ParserSelectTest, groupBy) {
  useDb("root", "test");

  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0");

  run("SELECT COUNT(*), c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c2");

  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 having COUNT(c1) > 10");

  run("SELECT COUNT(*), c1, c2 + 10, c1 + c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c2, c1");

  run("SELECT COUNT(*), c1 + 10, c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c1 + 10, c2");

  run("SELECT COUNT(*), c1 + 10 cc1 FROM t1 GROUP BY cc1");

  run("SELECT c1 AS new_c1, COUNT(c1) AS c1 FROM t1 GROUP BY c1");
  run("SELECT c1 new_id, COUNT(c1) c1 FROM t1 GROUP BY c1 ORDER BY new_id");
}

TEST_F(ParserSelectTest, groupBySemanticCheck) {
  useDb("root", "test");

  run("SELECT COUNT(*) cnt, c1 FROM t1 WHERE c1 > 0", TSDB_CODE_PAR_NOT_SINGLE_GROUP);
  run("SELECT COUNT(*) cnt, c2 FROM t1 WHERE c1 > 0 GROUP BY c1", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);
}

TEST_F(ParserSelectTest, havingCheck) {
  useDb("root", "test");

  run("select tbname,count(*) from st1 partition by tbname having c1>0", TSDB_CODE_PAR_INVALID_OPTR_USAGE);

  run("select tbname,count(*) from st1 group by tbname having c1>0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  run("select max(c1) from st1 group by tbname having c1>0");

  run("select max(c1) from st1 partition by tbname having c1>0");
}


TEST_F(ParserSelectTest, orderBy) {
  useDb("root", "test");

  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 order by cnt");

  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 order by 1");
}

TEST_F(ParserSelectTest, distinct) {
  useDb("root", "test");

  run("SELECT distinct c1, c2 FROM t1 WHERE c1 > 0 order by c1");

  run("SELECT distinct c1 + 10, c2 FROM t1 WHERE c1 > 0 order by c1 + 10, c2");

  run("SELECT distinct c1 + 10 cc1, c2 cc2 FROM t1 WHERE c1 > 0 order by cc1, c2");

  run("SELECT distinct COUNT(c2) FROM t1 WHERE c1 > 0 GROUP BY c1 order by COUNT(c2)");
}

TEST_F(ParserSelectTest, limit) {
  useDb("root", "test");

  run("SELECT c1, c2 FROM t1 LIMIT 10");

  run("(SELECT c1, c2 FROM t1 LIMIT 10)");
}

TEST_F(ParserSelectTest, slimitWithoutFromMatchesLegacyBehavior) {
  useDb("root", "test");

  run("SELECT 1 SLIMIT 1");
  run("SELECT 1 WHERE TRUE SLIMIT 1 SOFFSET 1 LIMIT 1");
  run("SELECT 1 HAVING TRUE SLIMIT 1");
  run("SELECT c1 FROM t1 SLIMIT 1",
      TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_GROUP_BY);
}

TEST_F(ParserSelectTest, orderByWithoutFromMatchesLegacyBehavior) {
  useDb("root", "test");

  run("SELECT COUNT(1) ORDER BY c1");
  run("SELECT COUNT(1) HAVING COUNT(1) = 1 ORDER BY c1");
}

TEST_F(ParserSelectTest, implicitHavingWithOrdinaryColumns) {
  useDb("root", "test");

  run("SELECT c1, c2 FROM t1 HAVING c1 > 2 ORDER BY c1, c2");
}

TEST_F(ParserSelectTest, sqlWindowInlineRowsParse) {
  useDb("root", "test");
  run("SELECT avg(c1) OVER (PARTITION BY c2 ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1",
      TSDB_CODE_SUCCESS, PARSER_STAGE_PARSE);
}

TEST_F(ParserSelectTest, sqlWindowNamedParse) {
  useDb("root", "test");
  run("SELECT max(c1) OVER win FROM t1 WINDOW win AS (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
      TSDB_CODE_SUCCESS, PARSER_STAGE_PARSE);
}

TEST_F(ParserSelectTest, sqlWindowRangeDurationParse) {
  useDb("root", "test");
  run("SELECT avg(c1) OVER (ORDER BY ts RANGE BETWEEN 10m PRECEDING AND CURRENT ROW) FROM t1", TSDB_CODE_SUCCESS,
      PARSER_STAGE_PARSE);
  run("SELECT count(c1) OVER (ORDER BY c1 RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM t1");
  run("SELECT count(c1) OVER (ORDER BY c4 RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM t1");
  run("SELECT count(c1) OVER (ORDER BY ts RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM t1",
      TSDB_CODE_PAR_WINDOW_INVALID_BOUND, PARSER_STAGE_TRANSLATE);
  run("SELECT count(c1) OVER (ORDER BY c1 RANGE BETWEEN 10s PRECEDING AND CURRENT ROW) FROM t1",
      TSDB_CODE_PAR_WINDOW_INVALID_BOUND, PARSER_STAGE_TRANSLATE);
  run("SELECT count(c1) OVER (ORDER BY c2 RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM t1",
      TSDB_CODE_PAR_WINDOW_INVALID_BOUND, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserSelectTest, offsetWithoutLimitParse) {
  useDb("root", "test");
  run("SELECT ts, c1 FROM t1 OFFSET 10");
}

TEST_F(ParserSelectTest, sqlWindowKeywordsFallbackToIdentifiers) {
  useDb("root", "test");
  run("SELECT c1 current, c1 over, c1 rows, c1 preceding, c1 following, c1 unbounded FROM t1");
}

TEST_F(ParserSelectTest, sqlWindowFunctionAliasCompatibility) {
  useDb("root", "test");
  run("SELECT avg(c1) AS over FROM t1");
  run("SELECT avg(c1) over FROM t1", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

TEST_F(ParserSelectTest, sqlWindowDedicatedFunctionsRequireOver) {
  useDb("root", "test");
  run("SELECT first_value(c1) FROM t1", TSDB_CODE_PAR_NOT_ALLOWED_FUNC, PARSER_STAGE_TRANSLATE);
  run("SELECT nth_value(c1, 1) FROM t1", TSDB_CODE_PAR_NOT_ALLOWED_FUNC, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserSelectTest, sqlWindowLagLeadKeepsLegacyParamRules) {
  useDb("root", "test");
  run("SELECT lag(c1) FROM t1");
  run("SELECT lead(c1) FROM t1");
  run("SELECT lag(c1, 0) FROM t1");
  run("SELECT lead(c1, 0) FROM t1");
  run("SELECT lag(c1, -1) FROM t1", TSDB_CODE_FUNC_FUNTION_PARA_VALUE, PARSER_STAGE_TRANSLATE);
  run("SELECT lead(c1, -1) FROM t1", TSDB_CODE_FUNC_FUNTION_PARA_VALUE, PARSER_STAGE_TRANSLATE);
  run("SELECT lag(c1) OVER (ORDER BY ts) FROM t1");
  run("SELECT lead(c1, 0) OVER (ORDER BY ts) FROM t1");
}

TEST_F(ParserSelectTest, sqlWindowRejectsIllegalLocations) {
  useDb("root", "test");
  run("SELECT c1 FROM t1 WHERE row_number() OVER (ORDER BY ts) > 1", TSDB_CODE_PAR_INVALID_WINDOW_FUNC,
      PARSER_STAGE_TRANSLATE);
  run("SELECT c1 FROM t1 GROUP BY row_number() OVER (ORDER BY ts)", TSDB_CODE_PAR_INVALID_WINDOW_FUNC,
      PARSER_STAGE_TRANSLATE);
  run("SELECT c1 FROM t1 HAVING rank() OVER (ORDER BY ts) > 1", TSDB_CODE_PAR_INVALID_WINDOW_FUNC,
      PARSER_STAGE_TRANSLATE);
  run("SELECT row_number() OVER (PARTITION BY row_number() OVER (ORDER BY ts) ORDER BY ts) FROM t1",
      TSDB_CODE_PAR_INVALID_WINDOW_FUNC, PARSER_STAGE_TRANSLATE);
  run("SELECT row_number() OVER (PARTITION BY sum(c1) ORDER BY ts) FROM t1", TSDB_CODE_PAR_INVALID_WINDOW_FUNC,
      PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserSelectTest, sqlWindowAllowsPercentileAggregate) {
  useDb("root", "test");
  run("SELECT percentile(c1, 50) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t1");
}

TEST_F(ParserSelectTest, sqlWindowNamedValidation) {
  useDb("root", "test");
  run("SELECT avg(c1) OVER missing FROM t1", TSDB_CODE_PAR_WINDOW_NOT_DEFINED, PARSER_STAGE_TRANSLATE);
  run("SELECT avg(c1) OVER win FROM t1 WINDOW win AS (ORDER BY ts), win AS (ORDER BY c1)",
      TSDB_CODE_PAR_WINDOW_DUP_NAME, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserSelectTest, sqlWindowOrderAndFrameValidation) {
  useDb("root", "test");
  run("SELECT row_number() OVER (PARTITION BY c1) FROM t1", TSDB_CODE_PAR_WINDOW_ORDER_REQUIRED,
      PARSER_STAGE_TRANSLATE);
  run("SELECT avg(c1) OVER (ORDER BY ts, c2 RANGE BETWEEN 10m PRECEDING AND CURRENT ROW) FROM t1",
      TSDB_CODE_PAR_WINDOW_RANGE_MULTI_ORDER, PARSER_STAGE_TRANSLATE);
  run("SELECT avg(c1) OVER (ORDER BY ts, c2 RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM t1");
  run("SELECT count(c1) OVER (ORDER BY c2 RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM t1");
  run("SELECT avg(c1) OVER (ORDER BY ts DESC RANGE BETWEEN 10m PRECEDING AND CURRENT ROW) FROM t1");
  run("SELECT sum(c1) OVER (ORDER BY ts DESC) FROM t1");
  run("SELECT nth_value(c1, 0) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t1",
      TSDB_CODE_PAR_WINDOW_INVALID_ARGUMENT, PARSER_STAGE_TRANSLATE);
}

// INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
// fill_mod_and_val = { NONE | PREV | NULL | LINEAR | NEXT | value_mod }
// value_mod = VALUE , val ...
TEST_F(ParserSelectTest, interval) {
  useDb("root", "test");
  // INTERVAL(interval_val)
  run("SELECT COUNT(*) FROM t1 INTERVAL(10s)");
  // INTERVAL(interval_val, interval_offset)
  run("SELECT COUNT(*) FROM t1 INTERVAL(10s, 5s)");
  // INTERVAL(interval_val, interval_offset) SLIDING (sliding_val)
  run("SELECT COUNT(*) FROM t1 INTERVAL(10s, 5s) SLIDING(7s)");
  // INTERVAL(interval_val) FILL(NONE)
  run("SELECT COUNT(*) FROM t1 WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59' "
      "INTERVAL(10s) FILL(NONE)");
}

TEST_F(ParserSelectTest, intervalHints) {
  useDb("root", "test");
  run("SELECT /*+ FAST_INTERVAL() */ _WSTART, COUNT(*), AVG(c1) FROM st1 "
      "WHERE ts >= '2022-04-01 00:00:00' AND ts < '2022-04-01 01:00:00' INTERVAL(10s)");
  run("SELECT /*+ EX_INTERVAL() */ _WSTART, _WEND, _WDURATION, COUNT(*), SUM(c1), AVG(c1) FROM st1 "
      "WHERE ts >= '2022-04-01 00:00:00' AND ts < '2022-04-01 01:00:00' INTERVAL(10s)");
}

TEST_F(ParserSelectTest, hintParameterListDoesNotHaveFixedCapacity) {
  constexpr int32_t kLargeHintParamCount = 64;
  char          message[TSDB_ERROR_MSG_LEN] = {0};
  SParseContext query = {0};
  query.pMsg = message;
  query.msgLen = sizeof(message);

  SAstCreateContext context = {0};
  initAstCreateContext(&query, &context);

  SNodeList* result = createHintWithParams(&context, 0);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(LIST_LENGTH(result), 1);
  SNode* pNode = nodesListGetNode(result, 0);
  ASSERT_NE(pNode, nullptr);
  EXPECT_EQ(((SHintNode*)pNode)->option, HINT_BATCH_SCAN);
  EXPECT_EQ(((SHintNode*)pNode)->pParams, nullptr);
  EXPECT_EQ(context.errCode, TSDB_CODE_SUCCESS);
  nodesDestroyList(result);

  context = {0};
  initAstCreateContext(&query, &context);
  result = createHintWithParams(&context, kLargeHintParamCount);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(LIST_LENGTH(result), 1);
  pNode = nodesListGetNode(result, 0);
  ASSERT_NE(pNode, nullptr);
  const auto* pHint = (const SHintNode*)pNode;
  EXPECT_EQ(pHint->option, HINT_BATCH_SCAN);
  checkHintParams(pHint, kLargeHintParamCount);
  EXPECT_EQ(context.errCode, TSDB_CODE_SUCCESS);

  SNode* pClone = nullptr;
  ASSERT_EQ(nodesCloneNode(pNode, &pClone), TSDB_CODE_SUCCESS);
  ASSERT_NE(pClone, nullptr);
  const auto* pHintClone = (const SHintNode*)pClone;
  checkHintParams(pHintClone, kLargeHintParamCount);
  ASSERT_NE(pHintClone->pParams, pHint->pParams);
  for (int32_t i = 0; i < kLargeHintParamCount; ++i) {
    const auto* pSrcParam = static_cast<const SHintParam*>(taosArrayGet(pHint->pParams, i));
    const auto* pDstParam = static_cast<const SHintParam*>(taosArrayGet(pHintClone->pParams, i));
    ASSERT_NE(pSrcParam, nullptr);
    ASSERT_NE(pDstParam, nullptr);
    EXPECT_NE(pSrcParam->value, pDstParam->value);
  }
  nodesDestroyNode(pClone);
  nodesDestroyList(result);
}

TEST_F(ParserSelectTest, hintParametersPreserveTokenData) {
  char          message[TSDB_ERROR_MSG_LEN] = {0};
  SParseContext query = {0};
  query.pMsg = message;
  query.msgLen = sizeof(message);

  SAstCreateContext context = {0};
  initAstCreateContext(&query, &context);

  std::string hint = "/*+ BATCH_SCAN(PARAM,123,'value',1.5,true) */";
  SNodeList* result = createHint(&context, hint);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(LIST_LENGTH(result), 1);
  const auto* pHint = (const SHintNode*)nodesListGetNode(result, 0);
  ASSERT_NE(pHint, nullptr);
  ASSERT_NE(pHint->pParams, nullptr);
  ASSERT_EQ(taosArrayGetSize(pHint->pParams), 5);

  const uint32_t expectedTypes[] = {TK_NK_ID, TK_NK_INTEGER, TK_NK_STRING, TK_NK_FLOAT, TK_NK_BOOL};
  const char*    expectedValues[] = {"PARAM", "123", "'value'", "1.5", "true"};
  for (int32_t i = 0; i < 5; ++i) {
    const auto* param = static_cast<const SHintParam*>(taosArrayGet(pHint->pParams, i));
    ASSERT_NE(param, nullptr);
    EXPECT_EQ(param->type, expectedTypes[i]);
    EXPECT_STREQ(param->value, expectedValues[i]);
  }
  EXPECT_EQ(context.errCode, TSDB_CODE_SUCCESS);
  nodesDestroyList(result);
}

TEST_F(ParserSelectTest, intervalSemanticCheck) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM t1 INTERVAL(100s) SLIDING(1s)");
  run("SELECT COUNT(*) FROM t1 INTERVAL(100001a) SLIDING(1s)", TSDB_CODE_PAR_INTER_SLIDING_TOO_SMALL);

  // "SELECT c1 FROM t1 INTERVAL(10s)" is now valid in window projection (SCALAR) mode, negative test removed.
  run("SELECT DISTINCT c1, c2 FROM t1 WHERE c1 > 3 INTERVAL(1d) FILL(NEXT)",
      TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC);
  run("SELECT HISTOGRAM(c1, 'log_bin', '{\"start\": -33,\"factor\": 55,\"count\": 5,\"infinity\": false}', 1) FROM t1 "
      "WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59' INTERVAL(10s) FILL(NULL)",
      TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC);
  run("SELECT _WSTART, _WEND, _WDURATION, sum(c1) FROM t1", TSDB_CODE_PAR_INVALID_WINDOW_PC);
  // SCALAR/AGG keyword without a window clause is invalid
  run("SELECT SCALAR c1 FROM t1", TSDB_CODE_PAR_INVALID_OPTR_USAGE);
  run("SELECT AGG count(*) FROM t1", TSDB_CODE_PAR_INVALID_OPTR_USAGE);
}

TEST_F(ParserSelectTest, interp) {
  useDb("root", "test");

  run("SELECT INTERP(c1) FROM t1 RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");
}

// A statement reaching translate through a clone of its parsed AST must be
// accepted exactly like the statement itself. Views (clientView.c) and bound
// prepared statements (qStmtBindParams) only ever translate such a clone, so
// a clone losing RANGE/EVERY/FILL rejects a valid INTERP query there with
// "Missing RANGE clause, EVERY clause or FILL clause".
TEST_F(ParserSelectTest, clonedAstInterp) {
  useDb("root", "test");

  runClonedAst("SELECT INTERP(c1) FROM t1 RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");
}

TEST_F(ParserSelectTest, clonedAstIndefiniteRowsFunc) {
  useDb("root", "test");

  runClonedAst("SELECT CSUM(c1) FROM t1");
}

TEST_F(ParserSelectTest, subquery) {
  useDb("root", "test");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, ts FROM st1s1 INTERVAL(1m)) INTERVAL(1n)");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, _wstart FROM st1s1 INTERVAL(1m)) INTERVAL(1n)");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, ts FROM st1s1 PARTITION BY TBNAME INTERVAL(1m)) INTERVAL(1n)");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, _wstart FROM st1s1 PARTITION BY TBNAME INTERVAL(1m) ORDER BY _WSTART) "
      "INTERVAL(1n)");

  run("SELECT diff(a) FROM (SELECT _wstart, tag1, tag2, MAX(c1) a FROM st1 PARTITION BY tag1 INTERVAL(1m)) PARTITION BY tag1");

  // timeline fallback: the subquery projects a TIMESTAMP column (_wstart), which diff uses as a degraded timeline
  run("SELECT diff(a) FROM (SELECT _wstart, tag1, tag2, MAX(c1) a FROM st1 PARTITION BY tag1 INTERVAL(1m)) PARTITION BY tag2");

  run("SELECT _C0 FROM (SELECT _ROWTS, ts FROM st1s1)");

  run("SELECT ts FROM (SELECT t1.ts FROM st1s1 t1)");

  run("(((SELECT t1.ts FROM st1s1 t1)))");
}

TEST_F(ParserSelectTest, subquerySemanticCheck) {
  useDb("root", "test");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a FROM st1s1 INTERVAL(1m)) INTERVAL(1n)",
      TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);

  run("SELECT ts FROM (SELECT t1.ts AS ts, t2.ts FROM st1s1 t1, st1s2 t2 WHERE t1.ts = t2.ts)",
      TSDB_CODE_PAR_AMBIGUOUS_COLUMN);

  run("SELECT ts FROM (SELECT ts AS c1 FROM st1s1 t1)", TSDB_CODE_PAR_INVALID_COLUMN);
}

TEST_F(ParserSelectTest, semanticCheck) {
  useDb("root", "test");

  // TSDB_CODE_PAR_INVALID_COLUMN
  run("SELECT c1, cc1 FROM t1", TSDB_CODE_PAR_INVALID_COLUMN);

  run("SELECT t1.c1, t1.cc1 FROM t1", TSDB_CODE_PAR_INVALID_COLUMN);

  // TSDB_CODE_PAR_GET_META_ERROR
  run("SELECT * FROM t10", TSDB_CODE_PAR_TABLE_NOT_EXIST);

  run("SELECT * FROM test.t10", TSDB_CODE_PAR_TABLE_NOT_EXIST);

  // TSDB_CODE_PAR_TABLE_NOT_EXIST
  run("SELECT t2.c1 FROM t1", TSDB_CODE_PAR_TABLE_NOT_EXIST);

  // TSDB_CODE_PAR_AMBIGUOUS_COLUMN
  run("SELECT c2 FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1", TSDB_CODE_PAR_AMBIGUOUS_COLUMN);

  run("SELECT c2 FROM (SELECT c1 c2, c2 FROM t1)", TSDB_CODE_PAR_AMBIGUOUS_COLUMN);

  bool keepColumnName = tsKeepColumnName;
  tsKeepColumnName = true;
  run("SELECT c1 FROM (SELECT COUNT(c1) c1, FIRST(c1) FROM t1)");
  run("SELECT LAST(c1) c1, FIRST(c1) FROM t1 ORDER BY c1");
  tsKeepColumnName = keepColumnName;

  // TSDB_CODE_PAR_WRONG_VALUE_TYPE
  run("SELECT timestamp '2010a' FROM t1", TSDB_CODE_PAR_WRONG_VALUE_TYPE);

  run("SELECT LAST(*) + SUM(c1) FROM t1", TSDB_CODE_PAR_NOT_SUPPORT_MULTI_RESULT);

  run("SELECT CEIL(LAST(ts, c1)) FROM t1", TSDB_CODE_FUNC_FUNTION_PARA_NUM);

  // TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION
  run("SELECT c2 FROM t1 tt1 join t1 tt2 on COUNT(*) > 0", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);

  run("SELECT c2 FROM t1 WHERE COUNT(*) > 0", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);

  run("SELECT c2 FROM t1 GROUP BY COUNT(*)", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);

  // TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT
  run("SELECT c2 FROM t1 order by 0", TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT);

  run("SELECT c2 FROM t1 order by 2", TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT);

  // TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION
  run("SELECT COUNT(*) cnt FROM t1 having c1 > 0",
      TSDB_CODE_PAR_INVALID_OPTR_USAGE);

  run("SELECT COUNT(*) cnt FROM t1 GROUP BY c2 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  run("SELECT COUNT(*), c1 cnt FROM t1 GROUP BY c2 having c2 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  run("SELECT COUNT(*) cnt FROM t1 GROUP BY c2 having c2 > 0 order by c1", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  // TSDB_CODE_PAR_NOT_SINGLE_GROUP
  run("SELECT COUNT(*), c1 FROM t1", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT COUNT(*) FROM t1 order by c1", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT c1 FROM t1 order by COUNT(*)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);

  run("SELECT COUNT(*) FROM t1 order by COUNT(*)");

  run("SELECT COUNT(*) FROM t1 order by last(c2)");

  run("SELECT c1 FROM t1 order by last(ts)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);

  run("SELECT ts FROM t1 order by last(ts)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);

  run("SELECT c2 FROM t1 order by last(ts)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);

  run("SELECT * FROM t1 order by last(ts)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);

  run("SELECT last(ts) FROM t1 order by last(ts)");

  run("SELECT last(ts), ts, c1 FROM t1 order by last(ts)");

  run("SELECT ts, last(ts) FROM t1 order by last(ts)");

  run("SELECT first(ts), c2 FROM t1 order by last(c1)", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT c1 FROM t1 order by concat(c2, 'abc')");

  // TSDB_CODE_PAR_NOT_SELECT_EXPRESSION
  run("SELECT distinct c1, c2 FROM t1 WHERE c1 > 0 order by ts", TSDB_CODE_PAR_NOT_SELECT_EXPRESSION);

  run("SELECT distinct c1 FROM t1 WHERE c1 > 0 order by COUNT(c2)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);

  run("SELECT distinct c2 FROM t1 WHERE c1 > 0 order by COUNT(c2)", TSDB_CODE_PAR_ORDERBY_INVALID_EXPR);
}

TEST_F(ParserSelectTest, syntaxError) {
  useDb("root", "test");

  run("SELECT CAST(? AS BINARY(10)) FROM t1", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

TEST_F(ParserSelectTest, setOperator) {
  useDb("root", "test");

  run("SELECT * FROM t1 UNION ALL SELECT * FROM t1");

  run("(SELECT * FROM t1) UNION ALL (SELECT * FROM t1)");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t1)");

  run("SELECT c1, c2 FROM t1 UNION ALL SELECT c1 as a, c2 as b FROM t1 ORDER BY c1");
}

TEST_F(ParserSelectTest, setOperatorSemanticCheck) {
  useDb("root", "test");

  run("SELECT c1, c2 FROM t1 UNION ALL SELECT c1, c2 FROM t1 ORDER BY ts", TSDB_CODE_PAR_ORDERBY_UNKNOWN_EXPR);
}

TEST_F(ParserSelectTest, informationSchema) {
  useDb("root", "information_schema");

  run("SELECT * FROM ins_databases WHERE name = 'information_schema'");

  run("SELECT * FROM ins_tags WHERE db_name = 'test' and table_name = 'st1'");

  run("SELECT * FROM (SELECT table_name FROM ins_tables) t WHERE table_name = 'a'");
}

TEST_F(ParserSelectTest, withoutFrom) {
  useDb("root", "test");

  run("SELECT 1");

  run("SELECT DATABASE()");

  run("SELECT CLIENT_VERSION()");

  run("SELECT SERVER_VERSION()");

  run("SELECT SERVER_STATUS()");

  run("SELECT CURRENT_USER()");

  run("SELECT USER()");
}

TEST_F(ParserSelectTest, withoutFromSemanticCheck) {
  useDb("root", "test");

  run("SELECT c1", TSDB_CODE_PAR_INVALID_COLUMN);
  run("SELECT TBNAME", TSDB_CODE_PAR_INVALID_TBNAME);
}

TEST_F(ParserSelectTest, joinSemanticCheck) {
  useDb("root", "test");

  run("SELECT * FROM (SELECT tag1, SUM(c1) s FROM st1 GROUP BY tag1) t1, st1 t2 where t1.tag1 = t2.tag1");

  run("SELECT count(*) FROM t1 a join t1 b on a.ts=b.ts where a.ts=b.ts");
}

/*
 * Regression test for GHSA-v8cj-fw82-9jjf: createSimpleSubQStmt used pCxt->errCode instead
 * of the local `code` variable when checking initTranslateContext / setCurrLevelNsFromParent,
 * causing UAF/double-free when pCxt->errCode was stale and the local call succeeded (or failed).
 * Fix: replace pCxt->errCode with code in those two checks.
 *
 * Exercise the fixed code path via quantified-subquery expressions (ANY / ALL),
 * which are the only callers of createSimpleSubQStmt.
 */
TEST_F(ParserSelectTest, createSimpleSubQStmtNoUafOnInitError) {
  useDb("root", "test");

  // > ANY  => rewriteExprSubQResToMinMax(isMin=true) => createSimpleSubQStmt
  run("SELECT c1 FROM t1 WHERE c1 > ANY (SELECT MAX(c1) FROM st1)");

  // < ANY  => rewriteExprSubQResToMinMax(isMin=false) => createSimpleSubQStmt
  run("SELECT c1 FROM t1 WHERE c1 < ANY (SELECT MAX(c1) FROM st1)");

  // > ALL  => rewriteExprSubQResToMinMax(isMin=false) => createSimpleSubQStmt
  run("SELECT c1 FROM t1 WHERE c1 > ALL (SELECT MAX(c1) FROM st1)");

  // < ALL  => rewriteExprSubQResToMinMax(isMin=true) => createSimpleSubQStmt
  run("SELECT c1 FROM t1 WHERE c1 < ALL (SELECT MAX(c1) FROM st1)");
}

TEST_F(ParserSelectTest, FederatedUnknownColumnFailsAtEveryReferencedExpressionPosition) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  useDb("root", "test");
  FederatedFlagGuard guard;

  struct SourceCase {
    const char* source;
    int8_t      type;
    const char* timestampType;
    const char* integerType;
  };
  const SourceCase sources[] = {
      {"ext_unknown_mysql", EXT_SOURCE_MYSQL, "datetime", "bigint"},
      {"ext_unknown_pg", EXT_SOURCE_POSTGRESQL, "timestamp", "bigint"},
      {"ext_unknown_influx", EXT_SOURCE_INFLUXDB, "Timestamp", "Int64"},
  };
  const char* queries[] = {
      "select unknown_value from %s.metrics",
      "select * from %s.metrics",
      "select id from %s.metrics where unknown_value is not null",
      "select id from %s.metrics order by unknown_value",
      "select case when unknown_value is null then 0 else 1 end from %s.metrics",
      "select cast(unknown_value as varchar(16)) from %s.metrics",
      "select id from (select id, unknown_value from %s.metrics) q",
      "select id from %s.metrics union all select id from %s.metrics where unknown_value is null",
  };

  for (const SourceCase& source : sources) {
    registerUnknownExternalColumn(source.source, source.type, source.timestampType, source.integerType);
    for (const char* query : queries) {
      char sql[512] = {0};
      if (strstr(query, "%s.metrics where") != NULL) {
        snprintf(sql, sizeof(sql), query, source.source, source.source);
      } else {
        snprintf(sql, sizeof(sql), query, source.source);
      }
      runAsyncOnly(sql, TSDB_CODE_EXT_TYPE_NOT_MAPPABLE);
    }
  }
#endif
}

}  // namespace ParserTest
