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
#include <iostream>

#include "query.h"
#include "tmsg.h"
#include "trpc.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

namespace {
typedef struct SParam {
  int32_t v;
} SParam;
int32_t testPrint(void* p) {
  SParam* param = (SParam*)p;
  printf("hello world, %d\n", param->v);
  taosMemoryFreeClear(p);
  return 0;
}

int32_t testPrintError(void* p) {
  SParam* param = (SParam*)p;
  taosMemoryFreeClear(p);

  return -1;
}
}  // namespace

class QueryTestEnv : public testing::Environment {
 public:
  virtual void SetUp() { initTaskQueue(); }

  virtual void TearDown() { cleanupTaskQueue(); }

  QueryTestEnv() {}
  virtual ~QueryTestEnv() {}
};

int main(int argc, char** argv) {
  testing::AddGlobalTestEnvironment(new QueryTestEnv());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, async_task_test) {
  SParam* p = (SParam*)taosMemoryCalloc(1, sizeof(SParam));
  taosAsyncExec(testPrint, p, NULL);
  taosMsleep(5);
}

TEST(testCase, many_async_task_test) {
  for (int32_t i = 0; i < 50; ++i) {
    SParam* p = (SParam*)taosMemoryCalloc(1, sizeof(SParam));
    p->v = i;
    taosAsyncExec(testPrint, p, NULL);
  }

  taosMsleep(10);
}

TEST(testCase, error_in_async_test) {
  int32_t code = 0;
  SParam* p = (SParam*)taosMemoryCalloc(1, sizeof(SParam));
  taosAsyncExec(testPrintError, p, &code);
  taosMsleep(1);
  printf("Error code:%d after asynchronously exec function\n", code);
}

TEST(testCase, clone_normal_create_table_req_preserves_metadata) {
  SSchema schema[2] = {};
  schema[0].type = TSDB_DATA_TYPE_TIMESTAMP;
  schema[0].bytes = sizeof(int64_t);
  schema[1].type = TSDB_DATA_TYPE_DECIMAL;
  schema[1].bytes = 16;

  SColCmpr colCmpr[2] = {};
  colCmpr[0].id = 0;
  colCmpr[0].alg = 1;
  colCmpr[1].id = 1;
  colCmpr[1].alg = 2;

  SExtSchema extSchemas[2] = {};
  extSchemas[1].typeMod = 4;
  char       sql[] = "create table t0 (ts timestamp, v decimal(10, 2))";

  SVCreateTbReq src = {};
  src.name = (char*)"t0";
  src.type = TSDB_NORMAL_TABLE;
  src.sql = sql;
  src.sqlLen = strlen(sql);
  src.txnId = 9;
  src.txnStatus = 1;
  src.ntb.schemaRow.nCols = 2;
  src.ntb.schemaRow.version = 7;
  src.ntb.schemaRow.pSchema = schema;
  src.ntb.userId = 42;
  src.colCmpr.nCols = 2;
  src.colCmpr.version = 3;
  src.colCmpr.pColCmpr = colCmpr;
  src.pExtSchemas = extSchemas;

  SVCreateTbReq* dst = nullptr;
  ASSERT_EQ(cloneSVreateTbReq(&src, &dst), TSDB_CODE_SUCCESS);
  ASSERT_NE(dst, nullptr);
  ASSERT_EQ(dst->ntb.schemaRow.version, src.ntb.schemaRow.version);
  ASSERT_NE(dst->ntb.schemaRow.pSchema, src.ntb.schemaRow.pSchema);
  ASSERT_EQ(memcmp(dst->ntb.schemaRow.pSchema, src.ntb.schemaRow.pSchema, sizeof(schema)), 0);
  ASSERT_EQ(dst->colCmpr.nCols, src.colCmpr.nCols);
  ASSERT_EQ(dst->colCmpr.version, src.colCmpr.version);
  ASSERT_NE(dst->colCmpr.pColCmpr, src.colCmpr.pColCmpr);
  ASSERT_EQ(memcmp(dst->colCmpr.pColCmpr, src.colCmpr.pColCmpr, sizeof(colCmpr)), 0);
  ASSERT_NE(dst->pExtSchemas, src.pExtSchemas);
  ASSERT_EQ(memcmp(dst->pExtSchemas, src.pExtSchemas, sizeof(extSchemas)), 0);
  ASSERT_EQ(dst->ntb.userId, src.ntb.userId);
  ASSERT_EQ(dst->sqlLen, src.sqlLen);
  ASSERT_NE(dst->sql, src.sql);
  ASSERT_EQ(memcmp(dst->sql, src.sql, src.sqlLen), 0);
  ASSERT_EQ(dst->txnId, src.txnId);
  ASSERT_EQ(dst->txnStatus, src.txnStatus);

  tdDestroySVCreateTbReq(dst);
  taosMemoryFree(dst);
}

TEST(testCase, clone_create_table_req_rejects_inconsistent_sql) {
  SSchema schema = {};
  schema.type = TSDB_DATA_TYPE_TIMESTAMP;
  schema.bytes = sizeof(int64_t);

  SVCreateTbReq src = {};
  src.name = (char*)"t0";
  src.type = TSDB_NORMAL_TABLE;
  src.ntb.schemaRow.nCols = 1;
  src.ntb.schemaRow.pSchema = &schema;
  // sqlLen claims content while sql is absent
  src.sqlLen = 16;
  src.sql = nullptr;

  terrno = TSDB_CODE_SUCCESS;
  SVCreateTbReq* dst = nullptr;
  ASSERT_NE(cloneSVreateTbReq(&src, &dst), TSDB_CODE_SUCCESS);
  ASSERT_EQ(dst, nullptr);
}

#pragma GCC diagnostic pop
