/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "extTypeMap.h"
#include "tdef.h"
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

TEST(ExternalTypeMapTest, NormalizesBoundaryDeclarationsForAllProviders) {
  SDataType type = {0};

  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_MYSQL, "varchar(999999999999999999999)", "utf8mb4", &type));
  EXPECT_EQ(TSDB_DATA_TYPE_NCHAR, type.type);
  EXPECT_EQ(TSDB_MAX_NCHAR_LEN, type.bytes);
  EXPECT_EQ(0, type.precision);
  EXPECT_EQ(0, type.scale);

  type = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_POSTGRESQL, "numeric(999999999999999999999,999999999999999999999)",
                                       "UTF8", &type));
  EXPECT_EQ(TSDB_DECIMAL_MAX_PRECISION, type.precision);
  EXPECT_EQ(TSDB_DECIMAL_MAX_PRECISION, type.scale);
  EXPECT_EQ(DECIMAL128_BYTES, type.bytes);

  type = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_INFLUXDB, "LargeUtf8", NULL, &type));
  EXPECT_EQ(TSDB_DATA_TYPE_NCHAR, type.type);
  EXPECT_EQ(TSDB_MAX_NCHAR_LEN, type.bytes);

  type = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_INFLUXDB, "LargeBinary", NULL, &type));
  EXPECT_EQ(TSDB_DATA_TYPE_VARBINARY, type.type);
  EXPECT_EQ(TSDB_MAX_VARBINARY_LEN, type.bytes);

  type = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_INFLUXDB, "Decimal256(999999999999999999999,999999999999999999999)",
                                       NULL, &type));
  EXPECT_EQ(TSDB_DECIMAL_MAX_PRECISION, type.precision);
  EXPECT_EQ(TSDB_DECIMAL_MAX_PRECISION, type.scale);
  EXPECT_EQ(DECIMAL128_BYTES, type.bytes);
}

TEST(ExternalTypeMapTest, DecimalBoundariesRemainLegalAndUnknownTypesFail) {
  SDataType type = {0};

  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_MYSQL, "decimal(1,0)", NULL, &type));
  EXPECT_EQ(1, type.precision);
  EXPECT_EQ(0, type.scale);

  type = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            extTypeNameToTDengineType(EXT_SOURCE_POSTGRESQL, "numeric(38,39)", "UTF8", &type));
  EXPECT_EQ(TSDB_DECIMAL_MAX_PRECISION, type.precision);
  EXPECT_EQ(TSDB_DECIMAL_MAX_PRECISION, type.scale);

  type = {};
  EXPECT_EQ(TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
            extTypeNameToTDengineType(EXT_SOURCE_INFLUXDB, "UnknownArrowExtension", NULL, &type));
}

TEST(ExternalTypeMapTest, MapsMysqlInformationSchemaTablesColumnTypes) {
  struct TypeCase {
    const char* declaration;
    const char* charset;
  };

  // MySQL 8 INFORMATION_SCHEMA.TABLES combines text, enum, signed and unsigned
  // numeric, and temporal columns. Keep these declarations mapped so catalog
  // queries can select any individual field without rejecting the whole table.
  const TypeCase cases[] = {
      {"varchar(64)", "utf8mb3"},
      {"enum('BASE TABLE','VIEW','SYSTEM VIEW')", "utf8mb3"},
      {"int", NULL},
      {"enum('Fixed','Dynamic','Compressed','Redundant','Compact','Paged')", "utf8mb3"},
      {"bigint unsigned", NULL},
      {"timestamp", NULL},
      {"datetime", NULL},
      {"bigint", NULL},
      {"varchar(256)", "utf8mb3"},
      {"text", "utf8mb3"},
  };

  for (const TypeCase& test : cases) {
    SDataType type = {0};
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              extTypeNameToTDengineType(EXT_SOURCE_MYSQL, test.declaration, test.charset, &type))
        << test.declaration;
  }
}

TEST(ExternalTypeMapTest, InvalidModifiersNormalizeToLegalSchemaForAllProviders) {
  struct TypeCase {
    EExtSourceType source;
    const char*    declaration;
    const char*    charset;
    int8_t         expectedType;
    int32_t        expectedBytes;
    uint8_t        expectedPrecision;
    uint8_t        expectedScale;
  };

  const TypeCase cases[] = {
      {EXT_SOURCE_MYSQL, "varchar(-1)", "utf8mb4", TSDB_DATA_TYPE_NCHAR, TSDB_MAX_NCHAR_LEN, 0, 0},
      {EXT_SOURCE_MYSQL, "varchar(not-a-number)", "utf8mb4", TSDB_DATA_TYPE_NCHAR, TSDB_MAX_NCHAR_LEN, 0, 0},
      {EXT_SOURCE_POSTGRESQL, "numeric(-1,not-a-number)", "UTF8", TSDB_DATA_TYPE_DECIMAL,
       DECIMAL128_BYTES, TSDB_DECIMAL_MAX_PRECISION, 6},
      {EXT_SOURCE_POSTGRESQL, "numeric(1", "UTF8", TSDB_DATA_TYPE_DECIMAL64,
       DECIMAL64_BYTES, 1, 0},
      {EXT_SOURCE_INFLUXDB, "Decimal256(-1,broken)", NULL, TSDB_DATA_TYPE_DECIMAL,
       DECIMAL128_BYTES, TSDB_DECIMAL_MAX_PRECISION, 6},
  };

  for (const TypeCase& test : cases) {
    SDataType type = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              extTypeNameToTDengineType(test.source, test.declaration, test.charset, &type))
        << test.declaration;
    EXPECT_EQ(test.expectedType, type.type) << test.declaration;
    EXPECT_EQ(test.expectedBytes, type.bytes) << test.declaration;
    EXPECT_EQ(test.expectedPrecision, type.precision) << test.declaration;
    EXPECT_EQ(test.expectedScale, type.scale) << test.declaration;
  }
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
