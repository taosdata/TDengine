/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserSelectTest : public ParserTestBase {};

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

TEST_F(ParserSelectTest, pseudoColumnSemanticCheck) {
  useDb("root", "test");

  run("SELECT TBNAME FROM (SELECT * FROM st1s1)", TSDB_CODE_PAR_INVALID_TBNAME, PARSER_STAGE_TRANSLATE);
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

  run("select diff(ts) from (select _wstart as ts, count(*) from st1 partition by tbname interval(1d))", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);

  run("select diff(ts) from (select _wstart as ts, count(*) from st1 partition by tbname interval(1d) order by ts)");

  run("select t1.* from st1s1 t1, (select _wstart as ts, count(*) from st1s2 partition by tbname interval(1d)) WHERE t1.ts = t2.ts", TSDB_CODE_PAR_NOT_SUPPORT_JOIN);

  run("select t1.* from st1s1 t1, (select _wstart as ts, count(*) from st1s2 partition by tbname interval(1d) order by ts) t2 WHERE t1.ts = t2.ts");

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

  run("SELECT CSUM(c3) FROM t1 STATE_WINDOW(c1)", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);

  run("SELECT DIFF(c1) FROM t1 INTERVAL(10s)", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
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

TEST_F(ParserSelectTest, intervalSemanticCheck) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 INTERVAL(10s)", TSDB_CODE_PAR_INVALID_OPTR_USAGE);
  run("SELECT DISTINCT c1, c2 FROM t1 WHERE c1 > 3 INTERVAL(1d) FILL(NEXT)", TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  run("SELECT HISTOGRAM(c1, 'log_bin', '{\"start\": -33,\"factor\": 55,\"count\": 5,\"infinity\": false}', 1) FROM t1 "
      "WHERE ts > TIMESTAMP '2022-04-01 00:00:00' and ts < TIMESTAMP '2022-04-30 23:59:59' INTERVAL(10s) FILL(NULL)",
      TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC);
  run("SELECT _WSTART, _WEND, _WDURATION, sum(c1) FROM t1", TSDB_CODE_PAR_INVALID_WINDOW_PC);
}

TEST_F(ParserSelectTest, interp) {
  useDb("root", "test");

  run("SELECT INTERP(c1) FROM t1 RANGE('2017-7-14 18:00:00', '2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR)");
}

TEST_F(ParserSelectTest, subquery) {
  useDb("root", "test");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, ts FROM st1s1 INTERVAL(1m)) INTERVAL(1n)");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, _wstart FROM st1s1 INTERVAL(1m)) INTERVAL(1n)");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, ts FROM st1s1 PARTITION BY TBNAME INTERVAL(1m)) INTERVAL(1n)");

  run("SELECT SUM(a) FROM (SELECT MAX(c1) a, _wstart FROM st1s1 PARTITION BY TBNAME INTERVAL(1m) ORDER BY _WSTART) "
      "INTERVAL(1n)");

  run("SELECT diff(a) FROM (SELECT _wstart, tag1, tag2, MAX(c1) a FROM st1 PARTITION BY tag1 INTERVAL(1m)) PARTITION BY tag1");

  run("SELECT diff(a) FROM (SELECT _wstart, tag1, tag2, MAX(c1) a FROM st1 PARTITION BY tag1 INTERVAL(1m)) PARTITION BY tag2", TSDB_CODE_PAR_NOT_ALLOWED_FUNC);

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
  run("SELECT * FROM t10", TSDB_CODE_PAR_GET_META_ERROR);

  run("SELECT * FROM test.t10", TSDB_CODE_PAR_GET_META_ERROR);

  // TSDB_CODE_PAR_TABLE_NOT_EXIST
  run("SELECT t2.c1 FROM t1", TSDB_CODE_PAR_TABLE_NOT_EXIST);

  // TSDB_CODE_PAR_AMBIGUOUS_COLUMN
  run("SELECT c2 FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1", TSDB_CODE_PAR_AMBIGUOUS_COLUMN);

  run("SELECT c2 FROM (SELECT c1 c2, c2 FROM t1)", TSDB_CODE_PAR_AMBIGUOUS_COLUMN);

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
  run("SELECT COUNT(*) cnt FROM t1 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  run("SELECT COUNT(*) cnt FROM t1 GROUP BY c2 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  run("SELECT COUNT(*), c1 cnt FROM t1 GROUP BY c2 having c2 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  run("SELECT COUNT(*) cnt FROM t1 GROUP BY c2 having c2 > 0 order by c1", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);

  // TSDB_CODE_PAR_NOT_SINGLE_GROUP
  run("SELECT COUNT(*), c1 FROM t1", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT COUNT(*) FROM t1 order by c1", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT c1 FROM t1 order by COUNT(*)", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT COUNT(*) FROM t1 order by COUNT(*)");

  run("SELECT COUNT(*) FROM t1 order by last(c2)");

  run("SELECT c1 FROM t1 order by last(ts)");

  run("SELECT ts FROM t1 order by last(ts)");

  run("SELECT c2 FROM t1 order by last(ts)");

  run("SELECT * FROM t1 order by last(ts)");

  run("SELECT last(ts) FROM t1 order by last(ts)");

  run("SELECT last(ts), ts, c1 FROM t1 order by last(ts)");

  run("SELECT ts, last(ts) FROM t1 order by last(ts)");

  run("SELECT first(ts), c2 FROM t1 order by last(c1)", TSDB_CODE_PAR_NOT_SINGLE_GROUP);

  run("SELECT c1 FROM t1 order by concat(c2, 'abc')");

  // TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION
  run("SELECT distinct c1, c2 FROM t1 WHERE c1 > 0 order by ts", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION);

  run("SELECT distinct c1 FROM t1 WHERE c1 > 0 order by COUNT(c2)", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION);

  run("SELECT distinct c2 FROM t1 WHERE c1 > 0 order by COUNT(c2)", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION);
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

  run("SELECT c1, c2 FROM t1 UNION ALL SELECT c1, c2 FROM t1 ORDER BY ts", TSDB_CODE_PAR_INVALID_COLUMN);
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

  run("SELECT * FROM (SELECT tag1, SUM(c1) s FROM st1 GROUP BY tag1) t1, st1 t2 where t1.tag1 = t2.tag1",
      TSDB_CODE_PAR_NOT_SUPPORT_JOIN);

  run("SELECT count(*) FROM t1 a join t1 b on a.ts=b.ts where a.ts=b.ts");
}

}  // namespace ParserTest
