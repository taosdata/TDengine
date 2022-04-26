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

class PlanStmtTest : public PlannerTestBase {
 public:
  void prepare(const string& sql) {
    run(sql);
    // todo calloc pBindParams_
  }

  void bindParam(int32_t val) {
    TAOS_BIND_v2* pBind = pBindParams_ + paramNo_++;
    pBind->buffer_type = TSDB_DATA_TYPE_INT;
    pBind->num = 1;
    pBind->buffer_length = sizeof(int32_t);
    pBind->buffer = taosMemoryCalloc(1, pBind->buffer_length);
    pBind->length = (int32_t*)taosMemoryCalloc(1, sizeof(int32_t));
    pBind->is_null = (char*)taosMemoryCalloc(1, sizeof(char));
    *((int32_t*)pBind->buffer) = val;
    *(pBind->length) = sizeof(int32_t);
    *(pBind->is_null) = 0;
  }

  void exec() {
    // todo
  }

 private:
  TAOS_BIND_v2* pBindParams_;
  int32_t       paramNo_;
};

TEST_F(PlanStmtTest, stmt) {
  useDb("root", "test");

  run("SELECT * FROM t1 where c1 = ?");
}
