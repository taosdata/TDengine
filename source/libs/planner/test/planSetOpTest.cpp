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

#include "planTestUtil.h"
#include "planner.h"

using namespace std;

class PlanSetOpTest : public PlannerTestBase {};

TEST_F(PlanSetOpTest, unionAll) {
  useDb("root", "test");

  // sql 1: single UNION ALL operator
  run("SELECT c1, c2 FROM t1 WHERE c1 > 10 UNION ALL SELECT c1, c2 FROM t1 WHERE c1 > 20");
  // sql 2: multi UNION ALL operator
  run("SELECT c1, c2 FROM t1 WHERE c1 > 10 "
      "UNION ALL SELECT c1, c2 FROM t1 WHERE c1 > 20 "
      "UNION ALL SELECT c1, c2 FROM t1 WHERE c1 > 30");
}

TEST_F(PlanSetOpTest, unionAllSubquery) {
  useDb("root", "test");

  run("SELECT * FROM (SELECT c1, c2 FROM t1 UNION ALL SELECT c1, c2 FROM t1)");
}

TEST_F(PlanSetOpTest, unionAllWithSubquery) {
  useDb("root", "test");

  // child table
  run("SELECT ts FROM (SELECT ts FROM st1s1) UNION ALL SELECT ts FROM (SELECT ts FROM st1s2)");
  // super table
  run("SELECT ts FROM (SELECT ts FROM st1) UNION ALL SELECT ts FROM (SELECT ts FROM st1)");
}

TEST_F(PlanSetOpTest, union) {
  useDb("root", "test");

  // single UNION operator
  run("SELECT c1, c2 FROM t1 WHERE c1 > 10 UNION SELECT c1, c2 FROM t1 WHERE c1 > 20");
  // multi UNION operator
  run("SELECT c1, c2 FROM t1 WHERE c1 > 10 "
      "UNION SELECT c1, c2 FROM t1 WHERE c1 > 20 "
      "UNION SELECT c1, c2 FROM t1 WHERE c1 > 30");
}

TEST_F(PlanSetOpTest, unionContainJoin) {
  useDb("root", "test");

  run("SELECT t1.c1 FROM st1s1 t1 join st1s2 t2 on t1.ts = t2.ts "
      "WHERE t1.c1 IS NOT NULL GROUP BY t1.c1 HAVING t1.c1 IS NOT NULL "
      "UNION "
      "SELECT t1.c1 FROM st1s1 t1 join st1s2 t2 on t1.ts = t2.ts "
      "WHERE t1.c1 IS NOT NULL GROUP BY t1.c1 HAVING t1.c1 IS NOT NULL");
}

TEST_F(PlanSetOpTest, unionSubquery) {
  useDb("root", "test");

  run("SELECT * FROM (SELECT c1, c2 FROM t1 UNION SELECT c1, c2 FROM t1)");
}

TEST_F(PlanSetOpTest, bug001) {
  useDb("root", "test");

  run("SELECT c2 FROM t1 WHERE c1 IS NOT NULL GROUP BY c2 "
      "UNION "
      "SELECT 'abcdefghijklmnopqrstuvwxyz' FROM t1 "
      "WHERE 'abcdefghijklmnopqrstuvwxyz' IS NOT NULL GROUP BY 'abcdefghijklmnopqrstuvwxyz'");
}
