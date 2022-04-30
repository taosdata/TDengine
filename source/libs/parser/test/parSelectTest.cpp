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

  run("select * from t1");

  run("select * from test.t1");

  run("select ts, c1 from t1");

  run("select ts, t.c1 from (select * from t1) t");

  run("select * from t1 tt1, t1 tt2 where tt1.c1 = tt2.c1");
}

TEST_F(ParserSelectTest, constant) {
  useDb("root", "test");

  run("select 123, 20.4, 'abc', \"wxy\", timestamp '2022-02-09 17:30:20', true, false, 10s from t1");

  run("select 1234567890123456789012345678901234567890, 20.1234567890123456789012345678901234567890, 'abc', \"wxy\", "
      "timestamp '2022-02-09 17:30:20', true, false, 15s from t1");

  run("select 123 + 45 from t1 where 2 - 1");
}

TEST_F(ParserSelectTest, expression) {
  useDb("root", "test");

  run("select ts + 10s, c1 + 10, concat(c2, 'abc') from t1");

  run("select ts > 0, c1 < 20 and c2 = 'qaz' from t1");

  run("select ts > 0, c1 between 10 and 20 and c2 = 'qaz' from t1");
}

TEST_F(ParserSelectTest, condition) {
  useDb("root", "test");

  run("select c1 from t1 where ts in (true, false)");

  run("select * from t1 where c1 > 10 and c1 is not null");
}

TEST_F(ParserSelectTest, pseudoColumn) {
  useDb("root", "test");

  run("select _wstartts, _wendts, count(*) from t1 interval(10s)");
}

TEST_F(ParserSelectTest, multiResFunc) {
  useDb("root", "test");

  run("select last(*), first(*), last_row(*) from t1");

  run("select last(c1, c2), first(t1.*), last_row(c3) from t1");

  run("select last(t2.*), first(t1.c1, t2.*), last_row(t1.*, t2.*) from st1s1 t1, st1s2 t2 where t1.ts = t2.ts");
}

TEST_F(ParserSelectTest, timelineFunc) {
  useDb("root", "test");

  run("select last(*), first(*) from t1");

  run("select last(*), first(*) from t1 group by c1");

  run("select last(*), first(*) from t1 interval(10s)");

  run("select diff(c1) from t1");
}

TEST_F(ParserSelectTest, clause) {
  useDb("root", "test");

  // group by clause
  run("select count(*) cnt from t1 where c1 > 0");

  run("select count(*), c2 cnt from t1 where c1 > 0 group by c2");

  run("select count(*) cnt from t1 where c1 > 0 group by c2 having count(c1) > 10");

  run("select count(*), c1, c2 + 10, c1 + c2 cnt from t1 where c1 > 0 group by c2, c1");

  run("select count(*), c1 + 10, c2 cnt from t1 where c1 > 0 group by c1 + 10, c2");

  // order by clause
  run("select count(*) cnt from t1 where c1 > 0 group by c2 order by cnt");

  run("select count(*) cnt from t1 where c1 > 0 group by c2 order by 1");

  // distinct clause
  // run("select distinct c1, c2 from t1 where c1 > 0 order by c1");

  // run("select distinct c1 + 10, c2 from t1 where c1 > 0 order by c1 + 10, c2");

  // run("select distinct c1 + 10 cc1, c2 cc2 from t1 where c1 > 0 order by cc1, c2");

  // run("select distinct count(c2) from t1 where c1 > 0 group by c1 order by count(c2)");
}

TEST_F(ParserSelectTest, window) {
  useDb("root", "test");

  run("select count(*) from t1 interval(10s)");
}

TEST_F(ParserSelectTest, semanticError) {
  useDb("root", "test");

  // TSDB_CODE_PAR_INVALID_COLUMN
  run("select c1, cc1 from t1", TSDB_CODE_PAR_INVALID_COLUMN, PARSER_STAGE_TRANSLATE);

  run("select t1.c1, t1.cc1 from t1", TSDB_CODE_PAR_INVALID_COLUMN, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_TABLE_NOT_EXIST
  run("select * from t10", TSDB_CODE_PAR_TABLE_NOT_EXIST, PARSER_STAGE_TRANSLATE);

  run("select * from test.t10", TSDB_CODE_PAR_TABLE_NOT_EXIST, PARSER_STAGE_TRANSLATE);

  run("select t2.c1 from t1", TSDB_CODE_PAR_TABLE_NOT_EXIST, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_AMBIGUOUS_COLUMN
  run("select c2 from t1 tt1, t1 tt2 where tt1.c1 = tt2.c1", TSDB_CODE_PAR_AMBIGUOUS_COLUMN, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_WRONG_VALUE_TYPE
  run("select timestamp '2010' from t1", TSDB_CODE_PAR_WRONG_VALUE_TYPE, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION
  run("select c2 from t1 tt1 join t1 tt2 on count(*) > 0", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION,
      PARSER_STAGE_TRANSLATE);

  run("select c2 from t1 where count(*) > 0", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION, PARSER_STAGE_TRANSLATE);

  run("select c2 from t1 group by count(*)", TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT
  run("select c2 from t1 order by 0", TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT, PARSER_STAGE_TRANSLATE);

  run("select c2 from t1 order by 2", TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION
  run("select count(*) cnt from t1 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION, PARSER_STAGE_TRANSLATE);

  run("select count(*) cnt from t1 group by c2 having c1 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("select count(*), c1 cnt from t1 group by c2 having c2 > 0", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("select count(*) cnt from t1 group by c2 having c2 > 0 order by c1", TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_NOT_SINGLE_GROUP
  run("select count(*), c1 from t1", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);

  run("select count(*) from t1 order by c1", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);

  run("select c1 from t1 order by count(*)", TSDB_CODE_PAR_NOT_SINGLE_GROUP, PARSER_STAGE_TRANSLATE);

  // TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION
  run("select distinct c1, c2 from t1 where c1 > 0 order by ts", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("select distinct c1 from t1 where c1 > 0 order by count(c2)", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION,
      PARSER_STAGE_TRANSLATE);

  run("select distinct c2 from t1 where c1 > 0 order by count(c2)", TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION,
      PARSER_STAGE_TRANSLATE);
}

}  // namespace ParserTest
