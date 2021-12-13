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

#include <gtest/gtest.h>

#include "insertParser.h"
#include "mockCatalog.h"

using namespace std;
using namespace testing;

namespace {
  string toString(int32_t code) {
    return tstrerror(code);
  }
}

// syntax:
// INSERT INTO
//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
class InsertTest : public Test {
protected:
  void bind(const char* sql) {
    reset();
    cxt.pSql = sql;
    cxt.sqlLen = strlen(sql);
  }

  int32_t run() {
    code = parseInsertSql(&cxt, &res);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "code:" << toString(code) << ", msg:" << errMagBuf << endl;
    }
    return code;
  }

  SInsertStmtInfo* reslut() {
    return res;
  }

private:
  static const int max_err_len = 1024;

  void reset() {
    memset(&cxt, 0, sizeof(cxt));
    memset(errMagBuf, 0, max_err_len);
    cxt.pMsg = errMagBuf;
    cxt.msgLen = max_err_len;
    code = TSDB_CODE_SUCCESS;
    res = nullptr;
  }

  char errMagBuf[max_err_len];
  SParseContext cxt;
  int32_t code;
  SInsertStmtInfo* res;
};

// INSERT INTO tb_name VALUES (field1_value, ...)
TEST_F(InsertTest, simpleTest) {
  bind("insert into .. values (...)");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  SInsertStmtInfo* res = reslut();
  // todo check
}

TEST_F(InsertTest, toleranceTest) {
  bind("insert into");
  ASSERT_NE(run(), TSDB_CODE_SUCCESS);
  bind("insert into t");
  ASSERT_NE(run(), TSDB_CODE_SUCCESS);
}
