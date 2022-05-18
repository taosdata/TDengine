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
}

TEST_F(ParserSelectTest, constant) {
  useDb("root", "test");

  run("SELECT 123, 20.4, 'abc', \"wxy\", timestamp '2022-02-09 17:30:20', true, false, 10s FROM t1");

  run("SELECT 1234567890123456789012345678901234567890, 20.1234567890123456789012345678901234567890, 'abc', \"wxy\", "
      "timestamp '2022-02-09 17:30:20', true, false, 15s FROM t1");

  run("SELECT 123 + 45 FROM t1 WHERE 2 - 1");
}

TEST_F(ParserSelectTest, expression) {
  useDb("root", "test");

  run("SELECT ts + 10s, c1 + 10, concat(c2, 'abc') FROM t1");

  run("SELECT ts > 0, c1 < 20 and c2 = 'qaz' FROM t1");

  run("SELECT ts > 0, c1 between 10 and 20 and c2 = 'qaz' FROM t1");
}

TEST_F(ParserSelectTest, condition) {
  useDb("root", "test");

  run("SELECT c1 FROM t1 WHERE ts in (true, false)");

  run("SELECT * FROM t1 WHERE c1 > 10 and c1 is not null");
}

TEST_F(ParserSelectTest, pseudoColumn) {
  useDb("root", "test");

  run("SELECT _WSTARTTS, _WENDTS, COUNT(*) FROM t1 INTERVAL(10s)");
}

TEST_F(ParserSelectTest, pseudoColumnSemanticCheck) {
  useDb("root", "test");

  run("SELECT TBNAME FROM (SELECT * FROM st1s1)", TSDB_CODE_PAR_INVALID_TBNAME, PARSER_STAGE_TRANSLATE);
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

TEST_F(ParserSelectTest, clause) {
  useDb("root", "test");

  // GROUP BY clause
  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0");

  run("SELECT COUNT(*), c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c2");

  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 having COUNT(c1) > 10");

  run("SELECT COUNT(*), c1, c2 + 10, c1 + c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c2, c1");

  run("SELECT COUNT(*), c1 + 10, c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c1 + 10, c2");

  // order by clause
  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 order by cnt");

  run("SELECT COUNT(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 order by 1");

  // distinct clause
  // run("SELECT distinct c1, c2 FROM t1 WHERE c1 > 0 order by c1");

  // run("SELECT distinct c1 + 10, c2 FROM t1 WHERE c1 > 0 order by c1 + 10, c2");

  // run("SELECT distinct c1 + 10 cc1, c2 cc2 FROM t1 WHERE c1 > 0 order by cc1, c2");

  // run("SELECT distinct COUNT(c2) FROM t1 WHERE c1 > 0 GROUP BY c1 order by COUNT(c2)");
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

  run("SELECT c1 FROM t1 INTERVAL(10s)", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);
  run("SELECT DISTINCT c1, c2 FROM t1 WHERE c1 > 3 INTERVAL(1d) FILL(NEXT)", TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE,
      PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserSelectTest, semanticError) {
  useDb("root", "test");

  // TSDB_CODE_PAR_INVALID_COLUMN
  run("SELECT c1, cc1 FROM t1", TSDB_CODE_PAR_INVALID_COLUMN, PARSER_STAGE_TRANSLATE);

  run("SELECT t1.c1, t1.cc1 FROM t1", TSDB_CODE_PAR_INVALID_COLUMN, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_TABLE_NOT_EXIST
  run("SELECT * FROM t10", TSDB_CODE_PAR_TABLE_NOT_EXIST, PARSER_STAGE_TRANSLATE);

  run("SELECT * FROM test.t10", TSDB_CODE_PAR_TABLE_NOT_EXIST, PARSER_STAGE_TRANSLATE);

  run("SELECT t2.c1 FROM t1", TSDB_CODE_PAR_TABLE_NOT_EXIST, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_AMBIGUOUS_COLUMN
  run("SELECT c2 FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1", TSDB_CODE_PAR_AMBIGUOUS_COLUMN, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_WRONG_VALUE_TYPE
  run("SELECT timestamp '2010a' FROM t1", TSDB_CODE_PAR_WRONG_VALUE_TYPE, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION
  run("SELECT c2 FROM t1 tt1 join t1 tt2 on COUNT(*) > 0", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION,
      PARSER_STAGE_TRANSLATE);

  run("SELECT c2 FROM t1 WHERE COUNT(*) > 0", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION, PARSER_STAGE_TRANSLATE);

  run("SELECT c2 FROM t1 GROUP BY COUNT(*)", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT
  run("SELECT c2 FROM t1 order by 0", TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT, PARSER_STAGE_TRANSLATE);

  run("SELECT c2 FROM t1 order by 2", TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION
  run("SELECT COUNT(*) cnt FROM t1 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION, PARSER_STAGE_TRANSLATE);

  run("SELECT COUNT(*) cnt FROM t1 GROUP BY c2 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("SELECT COUNT(*), c1 cnt FROM t1 GROUP BY c2 having c2 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("SELECT COUNT(*) cnt FROM t1 GROUP BY c2 having c2 > 0 order by c1", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_NOT_SINGLE_GROUP
  run("SELECT COUNT(*), c1 FROM t1", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);

  run("SELECT COUNT(*) FROM t1 order by c1", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);

  run("SELECT c1 FROM t1 order by COUNT(*)", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION
  run("SELECT distinct c1, c2 FROM t1 WHERE c1 > 0 order by ts", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("SELECT distinct c1 FROM t1 WHERE c1 > 0 order by COUNT(c2)", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("SELECT distinct c2 FROM t1 WHERE c1 > 0 order by COUNT(c2)", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION,
      PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserSelectTest, setOperator) {
  useDb("root", "test");

  run("SELECT * FROM t1 UNION ALL SELECT * FROM t1");

  run("(SELECT * FROM t1) UNION ALL (SELECT * FROM t1)");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t1)");
}

TEST_F(ParserSelectTest, informationSchema) {
  useDb("root", "test");

  run("SELECT * FROM information_schema.user_databases WHERE name = 'information_schema'");
}

}  // namespace ParserTest
