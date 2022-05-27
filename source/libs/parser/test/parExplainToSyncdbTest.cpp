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

class ParserExplainToSyncdbTest : public ParserTestBase {};

TEST_F(ParserExplainToSyncdbTest, explain) {
  useDb("root", "test");

  run("EXPLAIN SELECT * FROM t1");

  run("EXPLAIN ANALYZE SELECT * FROM t1");

  run("EXPLAIN ANALYZE VERBOSE true RATIO 0.01 SELECT * FROM t1");
}

TEST_F(ParserExplainToSyncdbTest, grant) {
  useDb("root", "test");

  run("GRANT ALL ON test.* TO wxy");
  run("GRANT READ ON test.* TO wxy");
  run("GRANT WRITE ON test.* TO wxy");
  run("GRANT READ, WRITE ON test.* TO wxy");
}

// todo kill connection
// todo kill query
// todo kill stream
// todo merge vgroup
// todo redistribute vgroup
// todo reset query cache

TEST_F(ParserExplainToSyncdbTest, revoke) {
  useDb("root", "test");

  run("REVOKE ALL ON test.* FROM wxy");
  run("REVOKE READ ON test.* FROM wxy");
  run("REVOKE WRITE ON test.* FROM wxy");
  run("REVOKE READ, WRITE ON test.* FROM wxy");
}

// todo syncdb

}  // namespace ParserTest
