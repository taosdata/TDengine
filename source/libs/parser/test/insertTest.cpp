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

extern "C" {

#include <execinfo.h>

void *__real_malloc(size_t);

void *__wrap_malloc(size_t c) {  
  // printf("My MALLOC called: %d\n", c);
  // void *array[32]; 
  // int size = backtrace(array, 32); 
  // char **symbols = backtrace_symbols(array, size); 
  // for (int i = 0; i < size; ++i) { 
  //   cout << symbols[i] << endl;
  // } 
  // free(symbols); 

  return __real_malloc(c);
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
  void setDatabase(const string& acctId, const string& db) {
    acctId_ = acctId;
    db_ = db;
  }

  void bind(const char* sql) {
    reset();
    cxt_.pAcctId = acctId_.c_str();
    cxt_.pDbname = db_.c_str();
    strcpy(sqlBuf_, sql);
    cxt_.sqlLen = strlen(sql);
    sqlBuf_[cxt_.sqlLen] = '\0';
    cxt_.pSql = sqlBuf_;

  }

  int32_t run() {
    code_ = parseInsertSql(&cxt_, &res_);
    if (code_ != TSDB_CODE_SUCCESS) {
      cout << "code:" << toString(code_) << ", msg:" << errMagBuf_ << endl;
    }
    return code_;
  }

  SInsertStmtInfo* reslut() {
    return res_;
  }

private:
  static const int max_err_len = 1024;
  static const int max_sql_len = 1024 * 1024;

  void reset() {
    memset(&cxt_, 0, sizeof(cxt_));
    memset(errMagBuf_, 0, max_err_len);
    cxt_.pMsg = errMagBuf_;
    cxt_.msgLen = max_err_len;
    code_ = TSDB_CODE_SUCCESS;
    res_ = nullptr;
  }

  string acctId_;
  string db_;
  char errMagBuf_[max_err_len];
  char sqlBuf_[max_sql_len];
  SParseContext cxt_;
  int32_t code_;
  SInsertStmtInfo* res_;
};

// INSERT INTO tb_name VALUES (field1_value, ...)
TEST_F(InsertTest, simpleTest) {
  setDatabase("root", "test");

  bind("insert into t1 values (now, 1, \"wxy\")");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  SInsertStmtInfo* res = reslut();
  // todo check
  ASSERT_EQ(res->insertType, TSDB_QUERY_TYPE_INSERT);
  // ASSERT_EQ(taosArrayGetSize(res->pDataBlocks), 1);
}

TEST_F(InsertTest, toleranceTest) {
  setDatabase("root", "test");

  bind("insert into");
  ASSERT_NE(run(), TSDB_CODE_SUCCESS);
  bind("insert into t");
  ASSERT_NE(run(), TSDB_CODE_SUCCESS);
}
