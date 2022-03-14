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

#include "parInt.h"

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
  void setDatabase(const string& acctId, const string& db) {
    acctId_ = acctId;
    db_ = db;
  }

  void bind(const char* sql) {
    reset();
    cxt_.acctId = atoi(acctId_.c_str());
    cxt_.db = (char*) db_.c_str();
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

  void dumpReslut() {
    SVnodeModifOpStmt* pStmt = getVnodeModifStmt(res_);
    size_t num = taosArrayGetSize(pStmt->pDataBlocks);
    cout << "schemaAttache:" << (int32_t)pStmt->schemaAttache << ", payloadType:" << (int32_t)pStmt->payloadType << ", insertType:" << pStmt->insertType << ", numOfVgs:" << num << endl;    
    for (size_t i = 0; i < num; ++i) {
      SVgDataBlocks* vg = (SVgDataBlocks*)taosArrayGetP(pStmt->pDataBlocks, i);
      cout << "vgId:" << vg->vg.vgId << ", numOfTables:" << vg->numOfTables << ", dataSize:" << vg->size << endl;
      SSubmitReq* submit = (SSubmitReq*)vg->pData;
      cout << "length:" << ntohl(submit->length) << ", numOfBlocks:" << ntohl(submit->numOfBlocks) << endl;
      int32_t numOfBlocks = ntohl(submit->numOfBlocks);
      SSubmitBlk* blk = (SSubmitBlk*)(submit + 1);
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        cout << "Block:" << i << endl;
        cout << "\tuid:" << be64toh(blk->uid) << ", tid:" << ntohl(blk->tid) << ", padding:" << ntohl(blk->padding) << ", sversion:" << ntohl(blk->sversion)
            << ", dataLen:" << ntohl(blk->dataLen) << ", schemaLen:" << ntohl(blk->schemaLen) << ", numOfRows:" << ntohs(blk->numOfRows) << endl;
        blk = (SSubmitBlk*)(blk->data + ntohl(blk->dataLen));
      }
    }
  }

  void checkReslut(int32_t numOfTables, int16_t numOfRows1, int16_t numOfRows2 = -1) {
    SVnodeModifOpStmt* pStmt = getVnodeModifStmt(res_);
    ASSERT_EQ(pStmt->schemaAttache, 0);
    ASSERT_EQ(pStmt->payloadType, PAYLOAD_TYPE_KV);
    ASSERT_EQ(pStmt->insertType, TSDB_QUERY_TYPE_INSERT);
    size_t num = taosArrayGetSize(pStmt->pDataBlocks);
    ASSERT_GE(num, 0);
    for (size_t i = 0; i < num; ++i) {
      SVgDataBlocks* vg = (SVgDataBlocks*)taosArrayGetP(pStmt->pDataBlocks, i);
      ASSERT_EQ(vg->numOfTables, numOfTables);
      ASSERT_GE(vg->size, 0);
      SSubmitReq* submit = (SSubmitReq*)vg->pData;
      ASSERT_GE(ntohl(submit->length), 0);
      ASSERT_GE(ntohl(submit->numOfBlocks), 0);
      int32_t numOfBlocks = ntohl(submit->numOfBlocks);
      SSubmitBlk* blk = (SSubmitBlk*)(submit + 1);
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        ASSERT_EQ(ntohs(blk->numOfRows), (0 == i ? numOfRows1 : (numOfRows2 > 0 ? numOfRows2 : numOfRows1)));
        blk = (SSubmitBlk*)(blk->data + ntohl(blk->dataLen));
      }
    }
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
  }

  SVnodeModifOpStmt* getVnodeModifStmt(SQuery* pQuery) {
    return (SVnodeModifOpStmt*)pQuery->pRoot;
  }

  string acctId_;
  string db_;
  char errMagBuf_[max_err_len];
  char sqlBuf_[max_sql_len];
  SParseContext cxt_;
  int32_t code_;
  SQuery* res_;
};

// INSERT INTO tb_name VALUES (field1_value, ...)
TEST_F(InsertTest, singleTableSingleRowTest) {
  setDatabase("root", "test");

  bind("insert into t1 values (now, 1, 'beijing', 3, 4, 5)");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  dumpReslut();
  checkReslut(1, 1);
}

// INSERT INTO tb_name VALUES (field1_value, ...)(field1_value, ...)
TEST_F(InsertTest, singleTableMultiRowTest) {
  setDatabase("root", "test");

  bind("insert into t1 values (now, 1, 'beijing', 3, 4, 5)(now+1s, 2, 'shanghai', 6, 7, 8)(now+2s, 3, 'guangzhou', 9, 10, 11)");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  dumpReslut();
  checkReslut(1, 3);
}

// INSERT INTO tb1_name VALUES (field1_value, ...) tb2_name VALUES (field1_value, ...)
TEST_F(InsertTest, multiTableSingleRowTest) {
  setDatabase("root", "test");

  bind("insert into st1s1 values (now, 1, \"beijing\") st1s2 values (now, 10, \"131028\")");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  dumpReslut();
  checkReslut(2, 1);
}

// INSERT INTO tb1_name VALUES (field1_value, ...) tb2_name VALUES (field1_value, ...)
TEST_F(InsertTest, multiTableMultiRowTest) {
  setDatabase("root", "test");

  bind("insert into st1s1 values (now, 1, \"beijing\")(now+1s, 2, \"shanghai\")(now+2s, 3, \"guangzhou\")"
                  " st1s2 values (now, 10, \"131028\")(now+1s, 20, \"132028\")");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  dumpReslut();
  checkReslut(2, 3, 2);
}

TEST_F(InsertTest, toleranceTest) {
  setDatabase("root", "test");

  bind("insert into");
  ASSERT_NE(run(), TSDB_CODE_SUCCESS);
  bind("insert into t");
  ASSERT_NE(run(), TSDB_CODE_SUCCESS);
}
