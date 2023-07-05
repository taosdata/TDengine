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

#if 0
#include <gtest/gtest.h>

#include <taoserror.h>
#include <tdataformat.h>
#include <tglobal.h>
#include <tmsg.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#define NONE_CSTR "no"
#define NULL_CSTR "nu"
#define NONE_LEN  2
#define NULL_LEN  2
const static int16_t MAX_COLS = 14;

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 nchar(10), c7 tinyint, c8 smallint, c9 bool
STSchema *genSTSchema(int16_t nCols) {
  EXPECT_LE(nCols, MAX_COLS);
  SSchema *pSchema = (SSchema *)taosMemoryCalloc(nCols, sizeof(SSchema));
  EXPECT_NE(pSchema, nullptr);

  for (int16_t i = 0; i < nCols; ++i) {
    pSchema[i].colId = PRIMARYKEY_TIMESTAMP_COL_ID + i;
    char colName[TSDB_COL_NAME_LEN] = {0};
    snprintf(colName, TSDB_COL_NAME_LEN, "c%" PRIi16, i);
    strncpy(pSchema[i].name, colName, TSDB_COL_NAME_LEN);

    switch (i) {
      case 0: {
        pSchema[i].type = TSDB_DATA_TYPE_TIMESTAMP;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 1: {
        pSchema[i].type = TSDB_DATA_TYPE_INT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
        ;
      } break;
      case 2: {
        pSchema[i].type = TSDB_DATA_TYPE_BIGINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 3: {
        pSchema[i].type = TSDB_DATA_TYPE_FLOAT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 4: {
        pSchema[i].type = TSDB_DATA_TYPE_DOUBLE;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 5: {
        pSchema[i].type = TSDB_DATA_TYPE_BINARY;
        pSchema[i].bytes = 12;
      } break;
      case 6: {
        pSchema[i].type = TSDB_DATA_TYPE_NCHAR;
        pSchema[i].bytes = 42;
      } break;
      case 7: {
        pSchema[i].type = TSDB_DATA_TYPE_TINYINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 8: {
        pSchema[i].type = TSDB_DATA_TYPE_SMALLINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 9: {
        pSchema[i].type = TSDB_DATA_TYPE_BOOL;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 10: {
        pSchema[i].type = TSDB_DATA_TYPE_UTINYINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 11: {
        pSchema[i].type = TSDB_DATA_TYPE_USMALLINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 12: {
        pSchema[i].type = TSDB_DATA_TYPE_UINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;
      case 13: {
        pSchema[i].type = TSDB_DATA_TYPE_UBIGINT;
        pSchema[i].bytes = TYPE_BYTES[pSchema[i].type];
      } break;

      default:
        ASSERT(0);
        break;
    }
  }

  STSchema *pResult = NULL;
  pResult = tBuildTSchema(pSchema, nCols, 1);

  taosMemoryFree(pSchema);
  return pResult;
}

// ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 nchar(10), c7 tinyint, c8 smallint, c9 bool
static int32_t genTestData(const char **data, int16_t nCols, SArray **pArray) {
  if (!(*pArray)) {
    *pArray = taosArrayInit(nCols, sizeof(SColVal));
    if (!(*pArray)) return -1;
  }

  for (int16_t i = 0; i < nCols; ++i) {
    SColVal colVal = {0};
    colVal.cid = PRIMARYKEY_TIMESTAMP_COL_ID + i;
    if (strncasecmp(data[i], NONE_CSTR, NONE_LEN) == 0) {
      colVal.flag = CV_FLAG_NONE;
      taosArrayPush(*pArray, &colVal);
      continue;
    } else if (strncasecmp(data[i], NULL_CSTR, NULL_LEN) == 0) {
      colVal.flag = CV_FLAG_NULL;
      taosArrayPush(*pArray, &colVal);
      continue;
    }

    switch (i) {
      case 0:
        sscanf(data[i], "%" PRIi64, &colVal.value.ts);
        break;
      case 1:
        sscanf(data[i], "%" PRIi32, &colVal.value.i32);
        break;
      case 2:
        sscanf(data[i], "%" PRIi64, &colVal.value.i64);
        break;
      case 3:
        sscanf(data[i], "%f", &colVal.value.f);
        break;
      case 4:
        sscanf(data[i], "%lf", &colVal.value.d);
        break;
      case 5: {
        int16_t dataLen = strlen(data[i]) + 1;
        colVal.value.nData = dataLen < 10 ? dataLen : 10;
        colVal.value.pData = (uint8_t *)data[i];
      } break;
      case 6: {
        int16_t dataLen = strlen(data[i]) + 1;
        colVal.value.nData = dataLen < 40 ? dataLen : 40;
        colVal.value.pData = (uint8_t *)data[i];  // just for test, not real nchar
      } break;
      case 7:
      case 9: {
        int32_t d8;
        sscanf(data[i], "%" PRId32, &d8);
        colVal.value.i8 = (int8_t)d8;
      } break;
      case 8: {
        int32_t d16;
        sscanf(data[i], "%" PRId32, &d16);
        colVal.value.i16 = (int16_t)d16;
      } break;
      case 10: {
        uint32_t u8;
        sscanf(data[i], "%" PRId32, &u8);
        colVal.value.u8 = (uint8_t)u8;
      } break;
      case 11: {
        uint32_t u16;
        sscanf(data[i], "%" PRId32, &u16);
        colVal.value.u16 = (uint16_t)u16;
      } break;
      case 12: {
        sscanf(data[i], "%" PRIu32, &colVal.value.u32);
      } break;
      case 13: {
        sscanf(data[i], "%" PRIu64, &colVal.value.u64);
      } break;
      default:
        ASSERT(0);
    }
    taosArrayPush(*pArray, &colVal);
  }
  return 0;
}

int32_t debugPrintSColVal(SColVal *cv, int8_t type) {
  if (COL_VAL_IS_NONE(cv)) {
    printf("None ");
    return 0;
  }
  if (COL_VAL_IS_NULL(cv)) {
    printf("Null ");
    return 0;
  }
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      printf("%s ", cv->value.i8 == 0 ? "false" : "true");
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%" PRIi8 " ", cv->value.i8);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%" PRIi16 " ", cv->value.i16);
      break;
    case TSDB_DATA_TYPE_INT:
      printf("%" PRIi32 " ", cv->value.i32);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      printf("%" PRIi64 " ", cv->value.i64);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      printf("%f ", cv->value.f);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%lf ", cv->value.d);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
      char tv[15] = {0};
      snprintf(tv, 15, "%s", cv->value.pData);
      printf("%s ", tv);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      printf("%" PRIi64 " ", cv->value.i64);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      char tv[15] = {0};
      snprintf(tv, 15, "%s", cv->value.pData);
      printf("%s ", tv);
    } break;
    case TSDB_DATA_TYPE_UTINYINT:
      printf("%" PRIu8 " ", cv->value.u8);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      printf("%" PRIu16 " ", cv->value.u16);
      break;
    case TSDB_DATA_TYPE_UINT:
      printf("%" PRIu32 " ", cv->value.u32);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      printf("%" PRIu64 " ", cv->value.u64);
      break;
    case TSDB_DATA_TYPE_JSON:
      printf("JSON ");
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      printf("VARBIN ");
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      printf("DECIMAL ");
      break;
    case TSDB_DATA_TYPE_BLOB:
      printf("BLOB ");
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      printf("MedBLOB ");
      break;
    case TSDB_DATA_TYPE_MAX:
      printf("UNDEF ");
      break;
    default:
      printf("UNDEF ");
      break;
  }
  return 0;
}

void debugPrintTSRow(STSRow2 *row, STSchema *pTSchema, const char *tags, int32_t ln) {
  // printf("%s:%d %s:v%d:%d ", tags, ln, (row->flags & 0xf0) ? "KV" : "TP", row->sver, row->nData);
  for (int16_t i = 0; i < pTSchema->numOfCols; ++i) {
    SColVal cv = {0};
    tTSRowGet(row, pTSchema, i, &cv);
    debugPrintSColVal(&cv, pTSchema->columns[i].type);
  }
  printf("\n");
  fflush(stdout);
}

static int32_t checkSColVal(const char *rawVal, SColVal *cv, int8_t type) {
  ASSERT(rawVal);

  if (COL_VAL_IS_NONE(cv)) {
    EXPECT_STRCASEEQ(rawVal, NONE_CSTR);
    return 0;
  }
  if (COL_VAL_IS_NULL(cv)) {
    EXPECT_STRCASEEQ(rawVal, NULL_CSTR);
    return 0;
  }

  SValue rawSVal = {0};
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      int32_t d8;
      sscanf(rawVal, "%" PRId32, &d8);
      EXPECT_EQ(cv->value.i8, (int8_t)d8);
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      int32_t d16;
      sscanf(rawVal, "%" PRId32, &d16);
      EXPECT_EQ(cv->value.i16, (int16_t)d16);
    } break;
    case TSDB_DATA_TYPE_INT: {
      sscanf(rawVal, "%" PRId32, &rawSVal.i32);
      EXPECT_EQ(cv->value.i32, rawSVal.i32);
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      sscanf(rawVal, "%" PRIi64, &rawSVal.i64);
      EXPECT_EQ(cv->value.i64, rawSVal.i64);
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      sscanf(rawVal, "%f", &rawSVal.f);
      EXPECT_FLOAT_EQ(cv->value.f, rawSVal.f);
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      sscanf(rawVal, "%lf", &rawSVal.d);
      EXPECT_DOUBLE_EQ(cv->value.d, rawSVal.d);
    } break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
      EXPECT_STRCASEEQ(rawVal, (const char *)cv->value.pData);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      sscanf(rawVal, "%" PRIi64, &rawSVal.ts);
      EXPECT_DOUBLE_EQ(cv->value.ts, rawSVal.ts);
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      EXPECT_STRCASEEQ(rawVal, (const char *)cv->value.pData);  // informal nchar comparsion
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      uint32_t u8;
      sscanf(rawVal, "%" PRIu32, &u8);
      EXPECT_EQ(cv->value.u8, (uint8_t)u8);
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      uint32_t u16;
      sscanf(rawVal, "%" PRIu32, &u16);
      EXPECT_EQ(cv->value.u16, (uint16_t)u16);
    } break;
    case TSDB_DATA_TYPE_UINT: {
      sscanf(rawVal, "%" PRIu32, &rawSVal.u32);
      EXPECT_EQ(cv->value.u32, rawSVal.u32);
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      sscanf(rawVal, "%" PRIu64, &rawSVal.u64);
      EXPECT_EQ(cv->value.u64, rawSVal.u64);
    } break;
    case TSDB_DATA_TYPE_JSON:
      printf("JSON ");
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      printf("VARBIN ");
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      printf("DECIMAL ");
      break;
    case TSDB_DATA_TYPE_BLOB:
      printf("BLOB ");
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      printf("MedBLOB ");
      break;
    // case TSDB_DATA_TYPE_BINARY:
    //   printf("BINARY ");
    //   break;
    case TSDB_DATA_TYPE_MAX:
      printf("UNDEF ");
      break;
    default:
      printf("UNDEF ");
      break;
  }
  return 0;
}

static void checkTSRow(const char **data, STSRow2 *row, STSchema *pTSchema) {
  for (int16_t i = 0; i < pTSchema->numOfCols; ++i) {
    SColVal cv = {0};
    tTSRowGet(row, pTSchema, i, &cv);
    checkSColVal(data[i], &cv, pTSchema->columns[i].type);
  }
}

TEST(testCase, AllNormTest) {
  int16_t       nCols = 14;
  STSRowBuilder rb = {0};
  STSRow2      *row = nullptr;
  SArray       *pArray = taosArrayInit(nCols, sizeof(SColVal));
  EXPECT_NE(pArray, nullptr);

  STSchema *pTSchema = genSTSchema(nCols);
  EXPECT_NE(pTSchema, nullptr);

  // ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 nchar(10), c7 tinyint, c8 smallint,
  // c9 bool
  char *data[14] = {"1653694220000", "no", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "no", "no", "no", "no", "no"};

  genTestData((const char **)&data, nCols, &pArray);

  tTSRowNew(&rb, pArray, pTSchema, &row);

  debugPrintTSRow(row, pTSchema, __func__, __LINE__);
  checkTSRow((const char **)&data, row, pTSchema);

  tsRowBuilderClear(&rb);
  taosArrayDestroy(pArray);
  taosMemoryFree(pTSchema);
}

#if 1
TEST(testCase, NoneTest) {
  const static int nCols = 14;
  const static int nRows = 20;
  STSRow2         *row = nullptr;
  SArray          *pArray = taosArrayInit(nCols, sizeof(SColVal));
  EXPECT_NE(pArray, nullptr);

  STSchema *pTSchema = genSTSchema(nCols);
  EXPECT_NE(pTSchema, nullptr);

  // ts timestamp,  c1 int,  c2 bigint,  c3 float,  c4 double,  c5 binary(10),  c6 nchar(10),  c7 tinyint,  c8 smallint,
  // c9 bool c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned
  const char *data[nRows][nCols] = {
      {"1653694220000", "no", "20", "10.1", "10.1", "binary10", "no", "10", "10", "nu", "10", "20", "30", "40"},
      {"1653694220001", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no"},
      {"1653694220002", "10", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no"},
      {"1653694220003", "10", "10", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no", "no"},
      {"1653694220004", "no", "20", "no", "no", "no", "nchar10", "no", "no", "no", "no", "no", "no", "no"},
      {"1653694220005", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu"},
      {"1653694220006", "no", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "nu"},
      {"1653694220007", "no", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "no", "no", "no", "no", "no"},
      {"1653694220008", "no", "nu", "nu", "nu", "binary10", "nu", "nu", "nu", "nu", "nu", "nu", "nu", "no"},
      {"1653694220009", "no", "nu", "nu", "nu", "binary10", "nu", "nu", "10", "no", "nu", "nu", "nu", "100"},
      {"1653694220010", "-1", "-1", "-1", "-1", "binary10", "nu", "-1", "0", "0", "0", "0", "0", "0"},
      {"1653694220011", "-2147483648", "nu", "nu", "nu", "biy10", "nu", "nu", "32767", "no", "nu", "nu", "nu", "100"},
      {"1653694220012", "2147483647", "nu", "nu", "nu", "ary10", "nu", "nu", "-32768", "no", "nu", "nu", "nu", "100"},
      {"1653694220013", "no", "-9223372036854775818", "nu", "nu", "b1", "nu", "nu", "10", "no", "nu", "nu", "nu", "nu"},
      {"1653694220014", "no", "nu", "nu", "nu", "b0", "nu", "nu", "10", "no", "nu", "nu", "nu", "9223372036854775808"},
      {"1653694220015", "no", "nu", "nu", "nu", "binary30", "char4", "nu", "10", "no", "nu", "nu", "nu",
       "18446744073709551615"},
      {"1653694220016", "2147483647", "nu", "nu", "nu", "bin50", "nu", "nu", "10", "no", "nu", "nu", "nu", "100"},
      {"1653694220017", "2147483646", "0", "0", "0", "binary10", "0", "0", "0", "0", "255", "0", "0", "0"},
      {"1653694220018", "no", "-9223372036854775808", "nu", "nu", "binary10", "nu", "nu", "10", "no", "nu", "nu",
       "4294967295", "100"},
      {"1653694220019", "no", "9223372036854775807", "nu", "nu", "bin10", "nu", "nu", "10", "no", "254", "nu", "nu",
       "no"}};

  for (int r = 0; r < nRows; ++r) {
    genTestData((const char **)&data[r], nCols, &pArray);
    tTSRowNew(NULL, pArray, pTSchema, &row);
    debugPrintTSRow(row, pTSchema, __func__, __LINE__);  // debug print
    checkTSRow((const char **)&data[r], row, pTSchema);  // check
    tTSRowFree(row);
    taosArrayClear(pArray);
  }

  taosArrayDestroy(pArray);
  taosMemoryFree(pTSchema);
}
#endif
#endif