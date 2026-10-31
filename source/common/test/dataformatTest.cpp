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

// <algorithm> has to be parsed before osRand.h is pulled in: that header maps
// rand/srand to forbidden identifiers, and MSVC's <algorithm> still uses rand()
// in the deprecated random_shuffle definition, which then fails to compile.
#include <algorithm>
#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <random>
#include <utility>
#include <vector>

#include <taoserror.h>
#include <tdataformat.h>
#include <tglobal.h>
#include <tmsg.h>
#include <tdatablock.h>

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

// ts timestamp,  c1 int,  c2 bigint,  c3 float,  c4 double,  c5 binary(10),  c6 nchar(10),  c7 tinyint,  c8 smallint,
// c9 bool c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned
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
        colVal.value.type = TSDB_DATA_TYPE_TIMESTAMP;
        sscanf(data[i], "%" PRIi64, &colVal.value.val);
        break;
      case 1:
        colVal.value.type = TSDB_DATA_TYPE_INT;
        sscanf(data[i], "%" PRIi32, (int32_t *)&colVal.value.val);
        break;
      case 2:
        colVal.value.type = TSDB_DATA_TYPE_BIGINT;
        sscanf(data[i], "%" PRIi64, &colVal.value.val);
        break;
      case 3:
        colVal.value.type = TSDB_DATA_TYPE_FLOAT;
        sscanf(data[i], "%f", (float *)&colVal.value.val);
        break;
      case 4:
        colVal.value.type = TSDB_DATA_TYPE_DOUBLE;
        sscanf(data[i], "%lf", (double *)&colVal.value.val);
        break;
      case 5: {
        colVal.value.type = TSDB_DATA_TYPE_BINARY;
        int16_t dataLen = strlen(data[i]) + 1;
        colVal.value.nData = dataLen < 10 ? dataLen : 10;
        colVal.value.pData = (uint8_t *)data[i];
      } break;
      case 6: {
        colVal.value.type = TSDB_DATA_TYPE_NCHAR;
        int16_t dataLen = strlen(data[i]) + 1;
        colVal.value.nData = dataLen < 40 ? dataLen : 40;
        colVal.value.pData = (uint8_t *)data[i];  // just for test, not real nchar
      } break;
      case 7: {
        colVal.value.type = TSDB_DATA_TYPE_TINYINT;
        int32_t d8;
        sscanf(data[i], "%" PRId32, &d8);
        colVal.value.val = (int8_t)d8;
      }
      case 8: {
        colVal.value.type = TSDB_DATA_TYPE_SMALLINT;
        int32_t d16;
        sscanf(data[i], "%" PRId32, &d16);
        colVal.value.val = (int16_t)d16;
      } break;
      case 9: {
        colVal.value.type = TSDB_DATA_TYPE_BOOL;
        int32_t d8;
        sscanf(data[i], "%" PRId32, &d8);
        colVal.value.val = (int8_t)d8;
      } break;
      case 10: {
        colVal.value.type = TSDB_DATA_TYPE_UTINYINT;
        uint32_t u8;
        sscanf(data[i], "%" PRId32, &u8);
        colVal.value.val = (uint8_t)u8;
      } break;
      case 11: {
        colVal.value.type = TSDB_DATA_TYPE_USMALLINT;
        uint32_t u16;
        sscanf(data[i], "%" PRId32, &u16);
        colVal.value.val = (uint16_t)u16;
      } break;
      case 12: {
        colVal.value.type = TSDB_DATA_TYPE_UINT;
        sscanf(data[i], "%" PRIu32, (uint32_t *)&colVal.value.val);
      } break;
      case 13: {
        colVal.value.type = TSDB_DATA_TYPE_UBIGINT;
        sscanf(data[i], "%" PRIu64, (uint64_t *)&colVal.value.val);
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
      printf("%s ", cv->value.val == 0 ? "false" : "true");
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%" PRIi8 " ", *(int8_t *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%" PRIi16 " ", *(int16_t *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_INT:
      printf("%" PRIi32 " ", *(int32_t *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      printf("%" PRIi64 " ", cv->value.val);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      printf("%f ", *(float *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%lf ", *(double *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
      char tv[15] = {0};
      snprintf(tv, 15, "%s", cv->value.pData);
      printf("%s ", tv);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      printf("%" PRIi64 " ", cv->value.val);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      char tv[15] = {0};
      snprintf(tv, 15, "%s", cv->value.pData);
      printf("%s ", tv);
    } break;
    case TSDB_DATA_TYPE_UTINYINT:
      printf("%" PRIu8 " ", *(uint8_t *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      printf("%" PRIu16 " ", *(uint16_t *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_UINT:
      printf("%" PRIu32 " ", *(uint32_t *)&cv->value.val);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      printf("%" PRIu64 " ", *(uint64_t *)&cv->value.val);
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

void debugPrintTSRow(STSRow *row, STSchema *pTSchema, const char *tags, int32_t ln) {
  printf("%s:%d %s:v%d:len-%u ", tags, ln, (row->type) ? "KV" : "TP", row->sver, row->len);
  for (int16_t i = 0; i < pTSchema->numOfCols; ++i) {
    SColVal cv = {0};
    tTSRowGetVal(row, pTSchema, i, &cv);
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
      EXPECT_EQ((int8_t)cv->value.val, (int8_t)d8);
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      int32_t d16;
      sscanf(rawVal, "%" PRId32, &d16);
      EXPECT_EQ((int16_t)cv->value.val, (int16_t)d16);
    } break;
    case TSDB_DATA_TYPE_INT: {
      sscanf(rawVal, "%" PRId32, (int32_t *)&rawSVal.val);
      EXPECT_EQ((int32_t)cv->value.val, (int32_t)rawSVal.val);
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      sscanf(rawVal, "%" PRIi64, &rawSVal.val);
      EXPECT_EQ(cv->value.val, rawSVal.val);
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      sscanf(rawVal, "%f", (float *)&rawSVal.val);
      EXPECT_FLOAT_EQ((float)cv->value.val, (float)rawSVal.val);
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      sscanf(rawVal, "%lf", (double *)&rawSVal.val);
      EXPECT_DOUBLE_EQ((double)cv->value.val, (double)rawSVal.val);
    } break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
      EXPECT_STRCASEEQ(rawVal, (const char *)cv->value.pData);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      sscanf(rawVal, "%" PRIi64, &rawSVal.val);
      EXPECT_DOUBLE_EQ(cv->value.val, rawSVal.val);
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      EXPECT_STRCASEEQ(rawVal, (const char *)cv->value.pData);  // informal nchar comparsion
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      uint32_t u8;
      sscanf(rawVal, "%" PRIu32, &u8);
      EXPECT_EQ((uint8_t)cv->value.val, (uint8_t)u8);
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      uint32_t u16;
      sscanf(rawVal, "%" PRIu32, &u16);
      EXPECT_EQ((uint16_t)cv->value.val, (uint16_t)u16);
    } break;
    case TSDB_DATA_TYPE_UINT: {
      sscanf(rawVal, "%" PRIu32, (uint32_t *)&rawSVal.val);
      EXPECT_EQ((uint32_t)cv->value.val, (uint32_t)rawSVal.val);
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      sscanf(rawVal, "%" PRIu64, (uint64_t *)&rawSVal.val);
      EXPECT_EQ((uint64_t)cv->value.val, (uint64_t)rawSVal.val);
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

static void checkTSRow(const char **data, STSRow *row, STSchema *pTSchema) {
  for (int16_t i = 0; i < pTSchema->numOfCols; ++i) {
    SColVal cv = {0};
    tTSRowGetVal(row, pTSchema, i, &cv);
    checkSColVal(data[i], &cv, pTSchema->columns[i].type);
  }

  STSRowIter rowIter = {0};
  rowIter.pSchema = pTSchema;
  tdSTSRowIterReset(&rowIter, row);
  for (int32_t i = 0; i < pTSchema->numOfCols; ++i) {
    STColumn *pCol = pTSchema->columns + i;
    SColVal   colVal = {0};
    SCellVal  cv = {0};
    if (!tdSTSRowIterFetch(&rowIter, pCol->colId, pCol->type, &cv)) {
      break;
    }

    colVal.cid = pCol->colId;
    colVal.value.type = pCol->type;
    if (tdValTypeIsNone(cv.valType)) {
      colVal.flag = CV_FLAG_NONE;
    } else if (tdValTypeIsNull(cv.valType)) {
      colVal.flag = CV_FLAG_NULL;
    } else {
      colVal.flag = CV_FLAG_VALUE;

      if (IS_VAR_DATA_TYPE(pCol->type)) {
        colVal.value.nData = varDataLen(cv.val);
        colVal.value.pData = (uint8_t *)varDataVal(cv.val);
      } else {
        memcpy(&colVal.value.val, cv.val, tDataTypes[pCol->type].bytes);
      }
    }
    checkSColVal(data[i], &colVal, pCol->type);
  }
}
#ifndef WINDOWS
TEST(testCase, tColDataGetValue) {
  SColData pColData = {0};
  SColVal pColVal = {0};
  ASSERT_NE(tColDataGetValue(&pColData, 0, &pColVal),0);

  memset(&pColData, 0, sizeof(pColData));
  pColData.flag = 8;
  pColVal = {0};
  ASSERT_NE(tColDataGetValue(&pColData, 0, &pColVal),0);

  memset(&pColData, 0, sizeof(pColData));
  pColData.nVal = 1;
  pColData.flag = 8;
  ASSERT_NE(tColDataGetValue(&pColData, 0, &pColVal),0);
}
#endif

TEST(testCase, AllNormTest) {
  int16_t nCols = 14;
  STSRow *row = nullptr;
  SArray *pArray = taosArrayInit(nCols, sizeof(SColVal));
  EXPECT_NE(pArray, nullptr);

  STSchema *pTSchema = genSTSchema(nCols);
  EXPECT_NE(pTSchema, nullptr);

  // ts timestamp,  c1 int,  c2 bigint,  c3 float,  c4 double,  c5 binary(10),  c6 nchar(10),  c7 tinyint,  c8 smallint,
  // c9 bool c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned
  char *data[14] = {
      "1653694220000", "10", "20", "10.1", "10.1", "binary10", "nchar10", "10", "10", "10", "10", "20", "30", "40"};

  genTestData((const char **)&data, nCols, &pArray);

  tdSTSRowNew(pArray, pTSchema, &row, TD_ROW_TP);
  debugPrintTSRow(row, pTSchema, __func__, __LINE__);
  tdSRowPrint(row, pTSchema, __func__);
  checkTSRow((const char **)&data, row, pTSchema);

  taosMemoryFreeClear(row);
  taosArrayDestroy(pArray);
  taosMemoryFree(pTSchema);
}

TEST(testCase, StreamAllNormTest) {
  char ctbName[TSDB_TABLE_NAME_LEN] = {0};
  uint64_t groupId = 12345;

  buildCtbNameAddGroupId(NULL, ctbName, groupId, sizeof(ctbName));

  ASSERT_STREQ("_12345", ctbName);
}

TEST(testCase, StreamWithStbName) {
  char stbName[] = "1.table.stb";
  char ctbName[TSDB_TABLE_NAME_LEN] = {0};
  uint64_t groupId = 12345;

  buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName));

  ASSERT_STREQ("_stb_12345", ctbName);
}

TEST(testCase, StreamWithoutDotInStbName) {
  char stbName[] = "table";
  char ctbName[TSDB_TABLE_NAME_LEN] = {0};
  uint64_t groupId = 12345;

  buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName));

  ASSERT_STREQ("_table_12345", ctbName);
}

TEST(testCase, StreamWithoutDotInStbName2) {
  char stbName[] = "";
  char ctbName[TSDB_TABLE_NAME_LEN] = {0};
  uint64_t groupId = 12345;

  buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName));

  ASSERT_STREQ("__12345", ctbName);
}

TEST(testCase, StreamWithLongStbName) {
  char     ctbName[TSDB_TABLE_NAME_LEN];
  char    *stbName = "a_simle_stb_name";
  uint64_t groupId = UINT64_MAX;

  // test basic function
  strcpy(ctbName, "a_simple_ctb_name");
  EXPECT_EQ(buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName)), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(ctbName, "a_simple_ctb_name_a_simle_stb_name_18446744073709551615");

  // test null stbName
  strcpy(ctbName, "a_simple_ctb_name");
  stbName = NULL;
  EXPECT_EQ(buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName)), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(ctbName, "a_simple_ctb_name_18446744073709551615");

  // test buffer capcity check
  EXPECT_EQ(buildCtbNameAddGroupId(stbName, NULL, groupId, sizeof(ctbName)), TSDB_CODE_INTERNAL_ERROR);
  EXPECT_EQ(buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName) - 1), TSDB_CODE_INTERNAL_ERROR);

  // test truncation of long ctbName
  for (int32_t i = 0; i < 159; ++i) ctbName[i] = 'A';
  ctbName[159] = '\0';
  stbName = taosStrdup(ctbName);
  std::string expectName = std::string(ctbName) + "_" + std::string(stbName) + "_" + std::to_string(groupId);
  EXPECT_EQ(buildCtbNameAddGroupId(stbName, ctbName, groupId, sizeof(ctbName)), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(ctbName, expectName.c_str() + expectName.size() - TSDB_TABLE_NAME_LEN + 1);

  taosMemoryFree(stbName);
}

#if 1
TEST(testCase, NoneTest) {
  const static int nCols = 14;
  const static int nRows = 20;
  STSRow          *row = nullptr;
  SArray          *pArray = taosArrayInit(nCols, sizeof(SColVal));
  EXPECT_NE(pArray, nullptr);

  STSchema *pTSchema = genSTSchema(nCols);
  EXPECT_NE(pTSchema, nullptr);

  // ts timestamp,  c1 int,  c2 bigint,  c3 float,  c4 double,  c5 binary(10),  c6 nchar(10),  c7 tinyint,  c8 smallint,
  // c9 bool c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned
  const int8_t rowType[nRows] = {TD_ROW_TP, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV,
                                 TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_TP, TD_ROW_KV, TD_ROW_KV,
                                 TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_KV, TD_ROW_TP};

const char  *data[nRows][nCols] = {
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
  tdSTSRowNew(pArray, pTSchema, &row, rowType[r]);
  debugPrintTSRow(row, pTSchema, __func__, __LINE__);  // debug print
  tdSRowPrint(row, pTSchema, __func__);
  checkTSRow((const char **)&data[r], row, pTSchema);  // check
  taosMemoryFreeClear(row);
  taosArrayClear(pArray);
  }

  taosArrayDestroy(pArray);
  taosMemoryFree(pTSchema);
}
#endif

// tColDataAddValueByDataBlock is the append path used by taos_write_raw_block/tmq_write_raw
// (raw-block replication, backup/restore) to feed column bytes directly into storage, bypassing
// the taosMbsToUcs4 encoding validation that every other write path (SQL INSERT, stmt/stmt2 bind,
// schemaless) performs. It must reject NCHAR bytes that are not valid, decodable UCS4, instead of
// silently persisting them and only failing later at query time (0x3250/0x3251).
TEST(testCase, RawBlockNcharValidation) {
  const int32_t ncharBytes = 42;  // matches genSTSchema's NCHAR(10) column: 10*4 + VARSTR_HEADER_SIZE

  // -- valid UCS4 NCHAR value must be accepted --
  {
    SColData colData = {0};
    tColDataInit(&colData, 1, TSDB_DATA_TYPE_NCHAR, 0);

    const char *mbs = "ok";
    int32_t     ucs4Cap = ((int32_t)strlen(mbs) + 1) * (int32_t)TSDB_NCHAR_SIZE;
    char       *buf = (char *)taosMemoryCalloc(1, VARSTR_HEADER_SIZE + ucs4Cap);
    ASSERT_NE(buf, nullptr);

    int32_t ucs4Len = 0;
    bool    ok = taosMbsToUcs4(mbs, (int32_t)strlen(mbs), (TdUcs4 *)varDataVal(buf), ucs4Cap, &ucs4Len, nullptr);
    ASSERT_TRUE(ok);
    varDataSetLen(buf, ucs4Len);

    int32_t offset = 0;
    int32_t code = tColDataAddValueByDataBlock(&colData, TSDB_DATA_TYPE_NCHAR, ncharBytes, 1, (char *)&offset, buf);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    ASSERT_EQ(colData.numOfValue, 1);

    taosMemoryFree(buf);
    tColDataDestroy(&colData);
  }

  // -- illegal (non-decodable) UCS4 bytes must be rejected, not silently persisted --
  {
    SColData colData = {0};
    tColDataInit(&colData, 1, TSDB_DATA_TYPE_NCHAR, 0);

    // Length is a valid multiple of TSDB_NCHAR_SIZE (passes the existing length check), but the
    // bytes are not a valid UCS4 code point sequence (0xFFFFFFFF is not a valid Unicode scalar).
    char    buf[VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {0};
    int32_t badCode = 0xFFFFFFFF;
    memcpy(varDataVal(buf), &badCode, TSDB_NCHAR_SIZE);
    varDataSetLen(buf, TSDB_NCHAR_SIZE);

    int32_t offset = 0;
    int32_t code = tColDataAddValueByDataBlock(&colData, TSDB_DATA_TYPE_NCHAR, ncharBytes, 1, (char *)&offset, buf);
    ASSERT_EQ(code, TSDB_CODE_SCALAR_CONVERT_NCHAR_ERROR);
    ASSERT_EQ(colData.numOfValue, 0);

    tColDataDestroy(&colData);
  }

  // -- non-NCHAR var-data types (e.g. BINARY) must not be affected by the new check --
  {
    SColData colData = {0};
    tColDataInit(&colData, 1, TSDB_DATA_TYPE_BINARY, 0);

    char buf[VARSTR_HEADER_SIZE + 4] = {0};
    memcpy(varDataVal(buf), "\xFF\xFF\xFF\xFF", 4);
    varDataSetLen(buf, 4);

    int32_t offset = 0;
    int32_t code = tColDataAddValueByDataBlock(&colData, TSDB_DATA_TYPE_BINARY, 16, 1, (char *)&offset, buf);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    ASSERT_EQ(colData.numOfValue, 1);

    tColDataDestroy(&colData);
  }
}

TEST(BlockDataVariableValueValidationTest, ValidatesJsonScalarLayouts) {
  size_t valueLen = 0;

  char nullValue[] = {TSDB_DATA_TYPE_NULL};
  EXPECT_TRUE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, nullValue, sizeof(nullValue), &valueLen));
  EXPECT_EQ(valueLen, sizeof(nullValue));
  EXPECT_FALSE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, nullValue, 0, &valueLen));

  char boolValue[] = {TSDB_DATA_TYPE_BOOL, 1};
  EXPECT_TRUE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, boolValue, sizeof(boolValue), &valueLen));
  EXPECT_EQ(valueLen, sizeof(boolValue));
  EXPECT_FALSE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, boolValue, sizeof(boolValue) - 1, &valueLen));

  char doubleValue[CHAR_BYTES + DOUBLE_BYTES] = {TSDB_DATA_TYPE_DOUBLE};
  EXPECT_TRUE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, doubleValue, sizeof(doubleValue), &valueLen));
  EXPECT_EQ(valueLen, sizeof(doubleValue));
  EXPECT_FALSE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, doubleValue, sizeof(doubleValue) - 1, &valueLen));

  char ncharValue[CHAR_BYTES + VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE] = {TSDB_DATA_TYPE_NCHAR};
  varDataSetLen(ncharValue + CHAR_BYTES, TSDB_NCHAR_SIZE);
  EXPECT_TRUE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, ncharValue, sizeof(ncharValue), &valueLen));
  EXPECT_EQ(valueLen, sizeof(ncharValue));
  EXPECT_FALSE(blockDataValidateVariableValue(TSDB_DATA_TYPE_JSON, ncharValue, sizeof(ncharValue) - 1, &valueLen));
}

class BoundedBlockDecoderTest : public ::testing::Test {
 protected:
  enum { kRows = 3, kColumns = 3 };
  static constexpr size_t kHeaderSize = sizeof(int32_t) * 5 + sizeof(uint64_t);
  static constexpr size_t kSchemaSize = sizeof(int8_t) + sizeof(int32_t);

  void SetUp() override {
    ASSERT_EQ(createDataBlock(&source), TSDB_CODE_SUCCESS);
    ASSERT_EQ(createDataBlock(&decoded), TSDB_CODE_SUCCESS);

    SColumnInfoData timestamp = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0);
    SColumnInfoData fixed = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
    SColumnInfoData variable = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 2);
    ASSERT_EQ(blockDataAppendColInfo(source, &timestamp), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataAppendColInfo(source, &fixed), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataAppendColInfo(source, &variable), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataEnsureCapacity(source, kRows), TSDB_CODE_SUCCESS);

    const char *values[kRows] = {"one", "two", "six"};
    for (int32_t row = 0; row < kRows; ++row) {
      TSKEY   ts = 10 + row;
      int32_t integer = 100 + row;
      char    value[VARSTR_HEADER_SIZE + 5] = {0};
      varDataSetLen(value, strlen(values[row]));
      memcpy(varDataVal(value), values[row], strlen(values[row]));

      ASSERT_EQ(colDataSetVal(column(0), row, reinterpret_cast<const char *>(&ts), false), TSDB_CODE_SUCCESS);
      ASSERT_EQ(colDataSetVal(column(1), row, reinterpret_cast<const char *>(&integer), false), TSDB_CODE_SUCCESS);
      ASSERT_EQ(colDataSetVal(column(2), row, value, false), TSDB_CODE_SUCCESS);
    }
    source->info.rows = kRows;

    encoded.resize(blockGetEncodeSize(source));
    int32_t length = blockEncode(source, encoded.data(), encoded.size(), kColumns);
    ASSERT_GT(length, 0);
    encoded.resize(length);
  }

  void TearDown() override {
    blockDataDestroy(fourRowSource);
    blockDataDestroy(fourRowDecoded);
    blockDataDestroy(extraColumnsDecoded);
    blockDataDestroy(tooFewColumnsDecoded);
    blockDataDestroy(mismatchedColumnsDecoded);
    blockDataDestroy(source);
    blockDataDestroy(decoded);
  }

  SColumnInfoData *column(int32_t index) {
    return static_cast<SColumnInfoData *>(taosArrayGet(source->pDataBlock, index));
  }

  void setSourceTimestamp(int32_t row, TSKEY ts) {
    ASSERT_EQ(colDataSetVal(column(0), row, reinterpret_cast<const char *>(&ts), false), TSDB_CODE_SUCCESS);
  }

  void encodeUnfilledVariableColumn(std::vector<char> *result) {
    colDataDestroy(column(2));
    ASSERT_EQ(colInfoDataEnsureCapacity(column(2), kRows, false), TSDB_CODE_SUCCESS);
    ASSERT_EQ(column(2)->pData, nullptr);
    ASSERT_EQ(column(2)->varmeta.offset[0], 0);
    result->resize(blockGetEncodeSize(source));
    int32_t length = blockEncode(source, result->data(), result->size(), kColumns);
    ASSERT_GT(length, 0);
    result->resize(length);
  }

  void encodeVersion(int32_t version, bool internal, std::vector<char> *result) {
    result->resize(blockGetEncodeSize(source) + 64);
    int32_t length = internal ? blockEncodeInternal(source, result->data(), result->size(), kColumns)
                              : blockEncode(source, result->data(), result->size(), kColumns);
    ASSERT_GT(length, 0);
    result->resize(length);
    memcpy(result->data(), &version, sizeof(version));
    if (version == BLOCK_VERSION_2) {
      for (int32_t col = 0; col < kColumns; ++col) {
        size_t offset = kHeaderSize + kColumns * kSchemaSize + col * sizeof(int32_t);
        int32_t columnLength = 0;
        memcpy(&columnLength, result->data() + offset, sizeof(columnLength));
        columnLength = ntohl(columnLength);
        memcpy(result->data() + offset, &columnLength, sizeof(columnLength));
      }
    }
  }

  SSDataBlock      *source = nullptr;
  SSDataBlock      *decoded = nullptr;
  SSDataBlock      *fourRowSource = nullptr;
  SSDataBlock      *fourRowDecoded = nullptr;
  SSDataBlock      *extraColumnsDecoded = nullptr;
  SSDataBlock      *tooFewColumnsDecoded = nullptr;
  SSDataBlock      *mismatchedColumnsDecoded = nullptr;
  std::vector<char> encoded;
};

TEST_F(BoundedBlockDecoderTest, DecodesOneBlockWithinAContainerAndSupportsNullEndPointer) {
  const size_t blockLen = encoded.size();
  encoded.resize(blockLen + 16, 0);
  const char *end = nullptr;

  ASSERT_EQ(blockDecodeBounded(decoded, encoded.data(), encoded.size(), &end), TSDB_CODE_SUCCESS);
  EXPECT_EQ(end, encoded.data() + blockLen);
  ASSERT_EQ(decoded->info.rows, kRows);
  ASSERT_EQ(*reinterpret_cast<TSKEY *>(
                colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 0)), 2)),
            12);
  ASSERT_EQ(*reinterpret_cast<int32_t *>(
                colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 1)), 1)),
            101);
  const char *value = colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2)), 0);
  ASSERT_EQ(varDataLen(value), 3);
  EXPECT_EQ(memcmp(varDataVal(value), "one", 3), 0);

  ASSERT_EQ(blockDecodeBounded(decoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_SUCCESS);
}

TEST_F(BoundedBlockDecoderTest, RejectsTruncatedMalformedSchemaAndColumnLength) {
  for (size_t length = 0; length < encoded.size(); ++length) {
    SCOPED_TRACE(length);
    std::vector<char> truncated(encoded.begin(), encoded.begin() + length);
    EXPECT_EQ(blockDecodeBounded(decoded, truncated.data(), truncated.size(), nullptr), TSDB_CODE_INVALID_MSG);
  }

  std::vector<char> malformedSchema = encoded;
  malformedSchema[kHeaderSize] = TSDB_DATA_TYPE_MAX;
  EXPECT_EQ(blockDecodeBounded(decoded, malformedSchema.data(), malformedSchema.size(), nullptr),
            TSDB_CODE_INVALID_MSG);

  std::vector<char> malformedColumnLength = encoded;
  const int32_t     columnLength = htonl(INT32_MAX);
  memcpy(malformedColumnLength.data() + kHeaderSize + kColumns * kSchemaSize + sizeof(int32_t), &columnLength,
         sizeof(columnLength));
  EXPECT_EQ(blockDecodeBounded(decoded, malformedColumnLength.data(), malformedColumnLength.size(), nullptr),
            TSDB_CODE_INVALID_MSG);
}

TEST_F(BoundedBlockDecoderTest, DecodesBothVersionsThroughBoundedEntrypoints) {
  const char *values[kRows] = {"one", "two", "six"};
  for (int32_t version : {BLOCK_VERSION_1, BLOCK_VERSION_2}) {
    for (bool internal : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "version=" << version << ", internal=" << internal);
      std::vector<char> block;
      ASSERT_NO_FATAL_FAILURE(encodeVersion(version, internal, &block));
      const char *end = nullptr;
      int32_t code = internal ? blockDecodeInternalBounded(decoded, block.data(), block.size(), &end)
                              : blockDecodeBounded(decoded, block.data(), block.size(), &end);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
      EXPECT_EQ(end, block.data() + block.size());
      ASSERT_EQ(decoded->info.rows, kRows);
      auto *timestamp = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 0));
      auto *fixed = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 1));
      auto *variable = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2));
      for (int32_t row = 0; row < kRows; ++row) {
        EXPECT_EQ(*reinterpret_cast<TSKEY *>(colDataGetData(timestamp, row)), 10 + row);
        EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(fixed, row)), 100 + row);
        const char *value = colDataGetData(variable, row);
        ASSERT_EQ(varDataLen(value), strlen(values[row]));
        EXPECT_EQ(memcmp(varDataVal(value), values[row], varDataLen(value)), 0);
      }
    }
  }
}

TEST_F(BoundedBlockDecoderTest, SelectsBothVersionsWithTimestampAfterFixedColumn) {
  std::swap(*column(0), *column(1));
  for (int32_t version : {BLOCK_VERSION_1, BLOCK_VERSION_2}) {
    SCOPED_TRACE(version);
    std::vector<char> block;
    ASSERT_NO_FATAL_FAILURE(encodeVersion(version, false, &block));
    const char *end = nullptr;
    ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, block.data(), block.size(), 1, 11, 11, &end),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(end, block.data() + block.size());
    ASSERT_EQ(decoded->info.rows, 1);
    auto *fixed = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 0));
    auto *timestamp = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 1));
    auto *variable = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2));
    EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(fixed, 0)), 101);
    EXPECT_EQ(*reinterpret_cast<TSKEY *>(colDataGetData(timestamp, 0)), 11);
    const char *value = colDataGetData(variable, 0);
    ASSERT_EQ(varDataLen(value), 3);
    EXPECT_EQ(memcmp(varDataVal(value), "two", 3), 0);
  }
}

TEST_F(BoundedBlockDecoderTest, RejectsMalformedVersionTwoColumnLengths) {
  for (bool internal : {false, true}) {
    std::vector<char> block;
    ASSERT_NO_FATAL_FAILURE(encodeVersion(BLOCK_VERSION_2, internal, &block));
    int32_t columnLength = INT32_MAX;
    memcpy(block.data() + kHeaderSize + kColumns * kSchemaSize + sizeof(int32_t), &columnLength,
           sizeof(columnLength));
    int32_t code = internal ? blockDecodeInternalBounded(decoded, block.data(), block.size(), nullptr)
                            : blockDecodeBounded(decoded, block.data(), block.size(), nullptr);
    EXPECT_EQ(code, TSDB_CODE_INVALID_MSG);
    if (!internal) {
      EXPECT_EQ(blockSpecialDecodeLaterPartBounded(decoded, block.data(), block.size(), 0, 11, 11, nullptr),
                TSDB_CODE_INVALID_MSG);
    }
  }
}

TEST_F(BoundedBlockDecoderTest, AcceptsProductionReassignedVariableOffsets) {
  ASSERT_EQ(colDataReassignVal(column(2), 0, 1, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataReassignVal(column(2), 1, 2, nullptr), TSDB_CODE_SUCCESS);
  std::vector<char> reassigned(blockGetEncodeSize(source));
  int32_t           length = blockEncode(source, reassigned.data(), reassigned.size(), kColumns);
  ASSERT_GT(length, 0);
  reassigned.resize(length);

  ASSERT_EQ(blockDecodeBounded(decoded, reassigned.data(), reassigned.size(), nullptr), TSDB_CODE_SUCCESS);
  const char *value = colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2)), 0);
  ASSERT_EQ(varDataLen(value), 3);
  EXPECT_EQ(memcmp(varDataVal(value), "six", 3), 0);

  const char *end = nullptr;
  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, reassigned.data(), reassigned.size(), 0, 10, 10, &end),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(end, reassigned.data() + reassigned.size());
  ASSERT_EQ(decoded->info.rows, 1);
  value = colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2)), 0);
  ASSERT_EQ(varDataLen(value), 3);
  EXPECT_EQ(memcmp(varDataVal(value), "six", 3), 0);
}

TEST_F(BoundedBlockDecoderTest, DecodesReassignedUdfVariableOffsets) {
  colDataSetNULL(column(2), 1);
  ASSERT_EQ(colDataReassignVal(column(2), 2, 0, nullptr), TSDB_CODE_SUCCESS);

  std::vector<char> payload(tEncodeDataBlock(nullptr, source));
  void*             cursor = payload.data();
  ASSERT_EQ(tEncodeDataBlock(&cursor, source), payload.size());

  size_t offsetPosition = sizeof(int64_t) * 2 + sizeof(int16_t) * 2 + sizeof(int32_t);
  for (int32_t width : {static_cast<int32_t>(sizeof(TSKEY)), static_cast<int32_t>(sizeof(int32_t))}) {
    offsetPosition += sizeof(int16_t) + sizeof(int8_t) * 2 + sizeof(int32_t);
    offsetPosition += BitmapLen(kRows) + sizeof(int32_t) + kRows * width;
  }
  offsetPosition += sizeof(int16_t) + sizeof(int8_t) * 2 + sizeof(int32_t);
  int32_t reusedOffset = column(2)->varmeta.offset[2];
  memcpy(payload.data() + offsetPosition + 2 * sizeof(int32_t), &reusedOffset, sizeof(reusedOffset));

  blockDataFreeRes(decoded);
  int32_t consumed = 0;
  ASSERT_EQ(tDecodeDataBlockBounded(payload.data(), payload.size(), decoded, &consumed), TSDB_CODE_SUCCESS);
  ASSERT_EQ(consumed, payload.size());

  auto* variable = static_cast<SColumnInfoData*>(taosArrayGet(decoded->pDataBlock, 2));
  ASSERT_TRUE(variable->reassigned);
  const char* value = colDataGetData(variable, 2);
  ASSERT_EQ(varDataLen(value), 3);
  EXPECT_EQ(memcmp(varDataVal(value), "one", 3), 0);
  EXPECT_TRUE(colDataIsNull_s(variable, 1));
}

TEST_F(BoundedBlockDecoderTest, DecodesUdfNullPlaceholderWithOneByteWidth) {
  source->info.rows = 1;
  SColumnInfoData* placeholder = column(1);
  colDataSetNULL(placeholder, 0);
  placeholder->info.type = TSDB_DATA_TYPE_NULL;
  placeholder->info.bytes = CHAR_BYTES;

  std::vector<char> payload(tEncodeDataBlock(nullptr, source));
  void*             cursor = payload.data();
  ASSERT_EQ(tEncodeDataBlock(&cursor, source), payload.size());

  blockDataFreeRes(decoded);
  int32_t consumed = 0;
  ASSERT_EQ(tDecodeDataBlockBounded(payload.data(), payload.size(), decoded, &consumed), TSDB_CODE_SUCCESS);
  ASSERT_EQ(consumed, payload.size());
  auto* decodedPlaceholder = static_cast<SColumnInfoData*>(taosArrayGet(decoded->pDataBlock, 1));
  ASSERT_EQ(decodedPlaceholder->info.type, TSDB_DATA_TYPE_NULL);
  ASSERT_EQ(decodedPlaceholder->info.bytes, CHAR_BYTES);
  EXPECT_TRUE(colDataIsNull_s(decodedPlaceholder, 0));
}

TEST_F(BoundedBlockDecoderTest, EncodesReassignedUdfVariableOffsetsWithMixedWidths) {
  char value[VARSTR_HEADER_SIZE + 6] = {0};
  varDataSetLen(value, 6);
  memcpy(varDataVal(value), "longer", 6);
  ASSERT_EQ(colDataSetVal(column(2), 2, value, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataReassignVal(column(2), 0, 2, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataReassignVal(column(2), 1, 2, nullptr), TSDB_CODE_SUCCESS);

  std::vector<char> payload(tEncodeDataBlock(nullptr, source));
  void*             cursor = payload.data();
  ASSERT_EQ(tEncodeDataBlock(&cursor, source), payload.size());

  blockDataFreeRes(decoded);
  int32_t consumed = 0;
  ASSERT_EQ(tDecodeDataBlockBounded(payload.data(), payload.size(), decoded, &consumed), TSDB_CODE_SUCCESS);
  auto* variable = static_cast<SColumnInfoData*>(taosArrayGet(decoded->pDataBlock, 2));
  for (int32_t row = 0; row < kRows; ++row) {
    const char* decodedValue = colDataGetData(variable, row);
    ASSERT_EQ(varDataLen(decodedValue), 6);
    EXPECT_EQ(memcmp(varDataVal(decodedValue), "longer", 6), 0);
  }
}

TEST_F(BoundedBlockDecoderTest, EncodesUnfilledReassignedUdfColumnAsNull) {
  colDataDestroy(column(2));
  ASSERT_EQ(colInfoDataEnsureCapacity(column(2), kRows, false), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataReassignVal(column(2), 1, 0, nullptr), TSDB_CODE_SUCCESS);

  std::vector<char> payload(tEncodeDataBlock(nullptr, source));
  void*             cursor = payload.data();
  ASSERT_EQ(tEncodeDataBlock(&cursor, source), payload.size());

  blockDataFreeRes(decoded);
  int32_t consumed = 0;
  ASSERT_EQ(tDecodeDataBlockBounded(payload.data(), payload.size(), decoded, &consumed), TSDB_CODE_SUCCESS);
  auto* variable = static_cast<SColumnInfoData*>(taosArrayGet(decoded->pDataBlock, 2));
  for (int32_t row = 0; row < kRows; ++row) {
    EXPECT_TRUE(colDataIsNull_s(variable, row));
  }
}

TEST_F(BoundedBlockDecoderTest, AcceptsVirtualColumnValueWiderThanDeclaredWidth) {
  char value[VARSTR_HEADER_SIZE + 10] = {0};
  varDataSetLen(value, 10);
  memcpy(varDataVal(value), "0123456789", 10);
  ASSERT_EQ(colDataSetVal(column(2), 0, value, false), TSDB_CODE_SUCCESS);
  column(2)->info.bytes = VARSTR_HEADER_SIZE + 4;

  for (bool internal : {false, true}) {
    std::vector<char> block(blockGetEncodeSize(source) + 64);
    int32_t len = internal ? blockEncodeInternal(source, block.data(), block.size(), kColumns)
                           : blockEncode(source, block.data(), block.size(), kColumns);
    ASSERT_GT(len, 0);
    block.resize(len);

    SSDataBlock *result = nullptr;
    ASSERT_EQ(createDataBlock(&result), TSDB_CODE_SUCCESS);
    int32_t code = internal ? blockDecodeInternalBounded(result, block.data(), block.size(), nullptr)
                            : blockDecodeBounded(result, block.data(), block.size(), nullptr);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    auto *col = static_cast<SColumnInfoData *>(taosArrayGet(result->pDataBlock, 2));
    ASSERT_EQ(col->info.bytes, VARSTR_HEADER_SIZE + 4);
    const char *decodedValue = colDataGetData(col, 0);
    ASSERT_EQ(varDataLen(decodedValue), 10);
    EXPECT_EQ(memcmp(varDataVal(decodedValue), "0123456789", 10), 0);
    blockDataDestroy(result);
  }
}

TEST_F(BoundedBlockDecoderTest, AcceptsDifferentVariableWidthsInExistingDestination) {
  SColumnInfoData variable = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 10, 2);
  SColumnInfoData timestamp = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0);
  SColumnInfoData fixed = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(decoded, &timestamp), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(decoded, &fixed), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(decoded, &variable), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDecodeBounded(decoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_SUCCESS);
  auto *col = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2));
  EXPECT_EQ(col->info.bytes, 32);
  EXPECT_EQ(varDataLen(colDataGetData(col, 0)), 3);
}

TEST_F(BoundedBlockDecoderTest, EstimatesReassignedVariableDataForEncoding) {
  char value[VARSTR_HEADER_SIZE + 6] = {0};
  varDataSetLen(value, 6);
  memcpy(varDataVal(value), "longer", 6);
  ASSERT_EQ(colDataSetVal(column(2), 2, value, false), TSDB_CODE_SUCCESS);

  ASSERT_EQ(colDataReassignVal(column(2), 0, 2, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataReassignVal(column(2), 1, 2, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataReassignVal(column(2), 2, 2, nullptr), TSDB_CODE_SUCCESS);

  std::vector<char> reassigned(blockGetEncodeSize(source));
  int32_t           length = blockEncode(source, reassigned.data(), reassigned.size(), kColumns);
  ASSERT_EQ(length, reassigned.size());
}

TEST_F(BoundedBlockDecoderTest, EncodesReassignedUnfilledVariableColumn) {
  std::vector<char> unfilled;
  encodeUnfilledVariableColumn(&unfilled);

  ASSERT_EQ(colDataReassignVal(column(2), 1, 0, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(column(2)->reassigned);
  ASSERT_EQ(column(2)->pData, nullptr);

  ASSERT_EQ(blockDataToBuf(unfilled.data(), source), TSDB_CODE_SUCCESS);

  unfilled.resize(blockGetEncodeSize(source));
  int32_t length = blockEncode(source, unfilled.data(), unfilled.size(), kColumns);
  ASSERT_GT(length, 0);
  unfilled.resize(length);

  ASSERT_EQ(blockDecodeBounded(decoded, unfilled.data(), unfilled.size(), nullptr), TSDB_CODE_SUCCESS);
  auto* variable = static_cast<SColumnInfoData*>(taosArrayGet(decoded->pDataBlock, 2));
  for (int32_t row = 0; row < kRows; ++row) {
    EXPECT_TRUE(colDataIsNull_s(variable, row));
  }
}

TEST_F(BoundedBlockDecoderTest, DecodesUnfilledVariableColumnAsNull) {
  std::vector<char> unfilled;
  encodeUnfilledVariableColumn(&unfilled);
  for (bool reuse : {false, true}) {
    SCOPED_TRACE(reuse);
    blockDataFreeRes(decoded);
    if (reuse) {
      ASSERT_EQ(blockDecodeBounded(decoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_SUCCESS);
    }
    ASSERT_EQ(blockDecodeBounded(decoded, unfilled.data(), unfilled.size(), nullptr), TSDB_CODE_SUCCESS);
    ASSERT_EQ(decoded->info.rows, kRows);
    auto *variable = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2));
    for (int32_t row = 0; row < kRows; ++row) {
      EXPECT_TRUE(colDataIsNull_s(variable, row));
    }
  }
  const size_t offsetsStart = unfilled.size() - sizeof(bool) - kRows * sizeof(int32_t);
  for (int32_t offset : {-2, 1}) {
    memcpy(unfilled.data() + offsetsStart, &offset, sizeof(offset));
    EXPECT_EQ(blockDecodeBounded(decoded, unfilled.data(), unfilled.size(), nullptr), TSDB_CODE_INVALID_MSG);
  }
}

TEST_F(BoundedBlockDecoderTest, SelectsUnfilledVariableColumnAsNullWithReusedStorage) {
  std::vector<char> unfilled;
  encodeUnfilledVariableColumn(&unfilled);
  ASSERT_EQ(blockDecodeBounded(decoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, unfilled.data(), unfilled.size(), 0, 11, 11, nullptr),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(decoded->info.rows, 1);
  auto *variable = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 2));
  EXPECT_TRUE(colDataIsNull_s(variable, 0));
}

TEST_F(BoundedBlockDecoderTest, SelectsTimestampRangeWithoutDecodingPastTheBlock) {
  const size_t blockLen = encoded.size();
  encoded.resize(blockLen + 16, 0);
  const char *end = nullptr;

  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, encoded.data(), encoded.size(), 0, 11, 11, &end),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(end, encoded.data() + blockLen);
  ASSERT_EQ(decoded->info.rows, 1);
  EXPECT_EQ(*reinterpret_cast<TSKEY *>(
                colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 0)), 0)),
            11);

  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, encoded.data(), encoded.size(), 0, 20, 21, &end),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(end, encoded.data() + blockLen);
  EXPECT_EQ(decoded->info.rows, 0);
  EXPECT_EQ(blockSpecialDecodeLaterPartBounded(decoded, encoded.data(), encoded.size(), -1, 10, 12, nullptr),
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(blockSpecialDecodeLaterPartBounded(decoded, encoded.data(), encoded.size(), 1, 10, 12, nullptr),
            TSDB_CODE_INVALID_MSG);
}

TEST_F(BoundedBlockDecoderTest, SelectsGapRangeAsAnEmptyResult) {
  setSourceTimestamp(0, 10);
  setSourceTimestamp(1, 12);
  setSourceTimestamp(2, 14);
  std::vector<char> gapped(blockGetEncodeSize(source));
  int32_t           length = blockEncode(source, gapped.data(), gapped.size(), kColumns);
  ASSERT_GT(length, 0);
  gapped.resize(length);

  const char *end = nullptr;
  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, gapped.data(), gapped.size(), 0, 11, 11, &end),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(end, gapped.data() + gapped.size());
  EXPECT_EQ(decoded->info.rows, 0);

  ASSERT_EQ(blockDecodeBounded(decoded, gapped.data(), gapped.size(), nullptr), TSDB_CODE_SUCCESS);
  ASSERT_NE(colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 0)), 0), nullptr);
  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, gapped.data(), gapped.size(), 0, 11, 11, &end),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(end, gapped.data() + gapped.size());
  EXPECT_EQ(decoded->info.rows, 0);
}

TEST_F(BoundedBlockDecoderTest, CopiesOnlySelectedCanonicalVariableValues) {
  ASSERT_EQ(createDataBlock(&fourRowSource), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createDataBlock(&fourRowDecoded), TSDB_CODE_SUCCESS);

  SColumnInfoData timestamp = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0);
  SColumnInfoData variable = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 1);
  ASSERT_EQ(blockDataAppendColInfo(fourRowSource, &timestamp), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(fourRowSource, &variable), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(fourRowSource, 4), TSDB_CODE_SUCCESS);

  const char *values[4] = {"one", "two", "six", "ten"};
  for (int32_t row = 0; row < 4; ++row) {
    TSKEY             ts = 10 + row;
    std::vector<char> value(VARSTR_HEADER_SIZE + strlen(values[row]));
    varDataSetLen(value.data(), strlen(values[row]));
    memcpy(varDataVal(value.data()), values[row], strlen(values[row]));
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData *>(taosArrayGet(fourRowSource->pDataBlock, 0)), row,
                            reinterpret_cast<const char *>(&ts), false),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData *>(taosArrayGet(fourRowSource->pDataBlock, 1)), row,
                            value.data(), false),
              TSDB_CODE_SUCCESS);
  }
  fourRowSource->info.rows = 4;

  std::vector<char> canonical(blockGetEncodeSize(fourRowSource));
  int32_t           length = blockEncode(fourRowSource, canonical.data(), canonical.size(), 2);
  ASSERT_GT(length, 0);
  canonical.resize(length);

  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(fourRowDecoded, canonical.data(), canonical.size(), 0, 10, 10, nullptr),
            TSDB_CODE_SUCCESS);
  SColumnInfoData *selected = static_cast<SColumnInfoData *>(taosArrayGet(fourRowDecoded->pDataBlock, 1));
  ASSERT_EQ(fourRowDecoded->info.rows, 1);
  EXPECT_EQ(selected->varmeta.length, VARSTR_HEADER_SIZE + 3);

  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(fourRowDecoded, canonical.data(), canonical.size(), 0, 11, 11, nullptr),
            TSDB_CODE_SUCCESS);
  selected = static_cast<SColumnInfoData *>(taosArrayGet(fourRowDecoded->pDataBlock, 1));
  ASSERT_EQ(fourRowDecoded->info.rows, 1);
  EXPECT_EQ(selected->varmeta.length, VARSTR_HEADER_SIZE + 3);
  const char *value = colDataGetData(selected, 0);
  ASSERT_EQ(varDataLen(value), 3);
  EXPECT_EQ(memcmp(varDataVal(value), "two", 3), 0);
}

TEST_F(BoundedBlockDecoderTest, DecodesIntoOrdinaryDestinationWithTailColumns) {
  ASSERT_EQ(createDataBlock(&extraColumnsDecoded), TSDB_CODE_SUCCESS);
  SColumnInfoData timestamp = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0);
  SColumnInfoData fixed = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData variable = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 2);
  SColumnInfoData tailInt = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 3);
  SColumnInfoData tailBigint = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 4);
  ASSERT_EQ(blockDataAppendColInfo(extraColumnsDecoded, &timestamp), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(extraColumnsDecoded, &fixed), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(extraColumnsDecoded, &variable), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(extraColumnsDecoded, &tailInt), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(extraColumnsDecoded, &tailBigint), TSDB_CODE_SUCCESS);

  ASSERT_EQ(blockDecodeBounded(extraColumnsDecoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_SUCCESS);
  SColumnInfoData *tail = static_cast<SColumnInfoData *>(taosArrayGet(extraColumnsDecoded->pDataBlock, 3));
  ASSERT_EQ(blockDataEnsureCapacity(extraColumnsDecoded, kRows), TSDB_CODE_SUCCESS);
  int32_t tailValue = 999;
  ASSERT_EQ(colDataSetVal(tail, 0, reinterpret_cast<const char *>(&tailValue), false), TSDB_CODE_SUCCESS);
  char *tailData = tail->pData;

  ASSERT_EQ(blockDecodeBounded(extraColumnsDecoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_SUCCESS);
  EXPECT_EQ(extraColumnsDecoded->info.rows, kRows);
  EXPECT_EQ(tail->info.type, TSDB_DATA_TYPE_INT);
  EXPECT_EQ(tail->info.colId, 3);
  EXPECT_EQ(tail->pData, tailData);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(tail, 0)), tailValue);
  EXPECT_EQ(*reinterpret_cast<TSKEY *>(
                colDataGetData(static_cast<SColumnInfoData *>(taosArrayGet(extraColumnsDecoded->pDataBlock, 0)), 2)),
            12);

  ASSERT_EQ(createDataBlock(&tooFewColumnsDecoded), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(tooFewColumnsDecoded, &timestamp), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(tooFewColumnsDecoded, &fixed), TSDB_CODE_SUCCESS);
  EXPECT_EQ(blockDecodeBounded(tooFewColumnsDecoded, encoded.data(), encoded.size(), nullptr), TSDB_CODE_INVALID_MSG);

  ASSERT_EQ(createDataBlock(&mismatchedColumnsDecoded), TSDB_CODE_SUCCESS);
  SColumnInfoData wrongFixed = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(mismatchedColumnsDecoded, &timestamp), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(mismatchedColumnsDecoded, &wrongFixed), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(mismatchedColumnsDecoded, &variable), TSDB_CODE_SUCCESS);
  EXPECT_EQ(blockDecodeBounded(mismatchedColumnsDecoded, encoded.data(), encoded.size(), nullptr),
            TSDB_CODE_INVALID_MSG);
}

TEST_F(BoundedBlockDecoderTest, SelectsFixedColumnNullsWithoutPrefixBitmapBits) {
  ASSERT_EQ(colDataSetVal(column(1), 0, nullptr, true), TSDB_CODE_SUCCESS);
  std::vector<char> nullable(blockGetEncodeSize(source));
  int32_t           length = blockEncode(source, nullable.data(), nullable.size(), kColumns);
  ASSERT_GT(length, 0);
  nullable.resize(length);

  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, nullable.data(), nullable.size(), 0, 11, 11, nullptr),
            TSDB_CODE_SUCCESS);
  SColumnInfoData *fixed = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 1));
  EXPECT_FALSE(colDataIsNull_s(fixed, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(fixed, 0)), 101);

  ASSERT_EQ(blockDecodeBounded(decoded, nullable.data(), nullable.size(), nullptr), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, nullable.data(), nullable.size(), 0, 11, 11, nullptr),
            TSDB_CODE_SUCCESS);
  fixed = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 1));
  EXPECT_FALSE(colDataIsNull_s(fixed, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(fixed, 0)), 101);

  ASSERT_EQ(blockSpecialDecodeLaterPartBounded(decoded, nullable.data(), nullable.size(), 0, 10, 10, nullptr),
            TSDB_CODE_SUCCESS);
  fixed = static_cast<SColumnInfoData *>(taosArrayGet(decoded->pDataBlock, 1));
  EXPECT_TRUE(colDataIsNull_s(fixed, 0));
}

TEST(ColumnCopyTest, MergeAndAssignRejectWiderSource) {
  const int32_t   rows = 4;
  SColumnInfoData narrow = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, 1, 0);
  SColumnInfoData wide = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0);
  ASSERT_EQ(colInfoDataEnsureCapacity(&narrow, rows, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colInfoDataEnsureCapacity(&wide, rows, true), TSDB_CODE_SUCCESS);

  int32_t capacity = rows;
  EXPECT_EQ(colDataMergeCol(&narrow, 0, &capacity, &wide, rows), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(colDataAssign(&narrow, &wide, rows, nullptr), TSDB_CODE_INVALID_PARA);

  SColumnInfoData same = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0);
  ASSERT_EQ(colInfoDataEnsureCapacity(&same, rows, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(colDataAssign(&same, &wide, rows, nullptr), TSDB_CODE_SUCCESS);
  capacity = rows;
  EXPECT_EQ(colDataMergeCol(&same, rows, &capacity, &wide, rows), TSDB_CODE_SUCCESS);

  colDataDestroy(&narrow);
  colDataDestroy(&wide);
  colDataDestroy(&same);
}

// NULL-type columns legitimately differ in width (0 vs CHAR_BYTES). Only a source that actually carries data wider
// than the destination is refused; an all-NULL source without data must keep working.
static SColumnInfoData makeNullTypeCol(int32_t bytes, int32_t rows, bool withData) {
  SColumnInfoData col = createColumnInfoData(TSDB_DATA_TYPE_NULL, bytes, 0);
  col.nullbitmap = (char *)taosMemoryCalloc(1, BitmapLen(rows));
  if (withData) col.pData = (char *)taosMemoryCalloc(rows, CHAR_BYTES);
  return col;
}

TEST(ColumnCopyTest, MergeAndAssignNullTypeColumnsOfDifferentWidth) {
  const int32_t rows = 4;

  // without data nothing is copied, so the width difference is harmless
  SColumnInfoData dst = makeNullTypeCol(0, 2 * rows, false);
  SColumnInfoData src = makeNullTypeCol(CHAR_BYTES, rows, false);
  ASSERT_NE(dst.nullbitmap, nullptr);
  ASSERT_NE(src.nullbitmap, nullptr);
  int32_t capacity = 2 * rows;  // no reallocation needed
  EXPECT_EQ(colDataMergeCol(&dst, 0, &capacity, &src, rows), TSDB_CODE_SUCCESS);
  EXPECT_EQ(colDataAssign(&dst, &src, rows, nullptr), TSDB_CODE_SUCCESS);

  // with data, the copy would overrun the 0-byte destination
  SColumnInfoData dst2 = makeNullTypeCol(0, 2 * rows, false);
  SColumnInfoData src2 = makeNullTypeCol(CHAR_BYTES, rows, true);
  ASSERT_NE(dst2.nullbitmap, nullptr);
  ASSERT_NE(src2.pData, nullptr);
  capacity = 2 * rows;
  EXPECT_EQ(colDataMergeCol(&dst2, 0, &capacity, &src2, rows), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(colDataAssign(&dst2, &src2, rows, nullptr), TSDB_CODE_INVALID_PARA);

  colDataDestroy(&dst);
  colDataDestroy(&src);
  colDataDestroy(&dst2);
  colDataDestroy(&src2);
}

static STagVal makeTagVal(int16_t cid, int8_t type) {
  STagVal v;
  memset(&v, 0, sizeof(v));
  v.cid = cid;
  v.type = type;
  return v;
}

static STagVal makeJsonTagVal(char *key, int8_t type) {
  STagVal v;
  memset(&v, 0, sizeof(v));
  v.pKey = key;
  v.type = type;
  return v;
}

TEST(TagValidateTest, AcceptsBuiltTagsAndRejectsForgedHeaders) {
  SArray *pArray = taosArrayInit(2, sizeof(STagVal));
  ASSERT_NE(pArray, nullptr);
  char     str[] = "abc";
  STagVal  v1 = makeTagVal(2, TSDB_DATA_TYPE_INT);
  v1.i64 = 7;
  STagVal  v2 = makeTagVal(3, TSDB_DATA_TYPE_VARCHAR);
  v2.pData = (uint8_t *)str;
  v2.nData = 3;
  ASSERT_NE(taosArrayPush(pArray, &v1), nullptr);
  ASSERT_NE(taosArrayPush(pArray, &v2), nullptr);

  STag *pTag = nullptr;
  ASSERT_EQ(tTagNew(pArray, 1, 0, &pTag), TSDB_CODE_SUCCESS);
  EXPECT_EQ(tTagValidate(pTag, pTag->len), TSDB_CODE_SUCCESS);

  // len larger than the bytes actually transmitted
  EXPECT_NE(tTagValidate(pTag, pTag->len - 1), TSDB_CODE_SUCCESS);

  // the forgeries below assume two entries and a one-byte idx[]
  ASSERT_GE(pTag->nTag, 2);
  ASSERT_EQ(pTag->flags & TD_TAG_LARGE, 0);
  std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len);
  STag *pCopy = (STag *)buf.data();
  pCopy->len = INT16_MAX;  // header claims more than was sent
  EXPECT_NE(tTagValidate(pCopy, buf.size()), TSDB_CODE_SUCCESS);
  pCopy->len = pTag->len;
  pCopy->nTag = 100;  // idx[] runs past the blob
  EXPECT_NE(tTagValidate(pCopy, buf.size()), TSDB_CODE_SUCCESS);
  pCopy->nTag = pTag->nTag;
  pCopy->idx[1] = 100;  // offset outside the data area
  EXPECT_NE(tTagValidate(pCopy, buf.size()), TSDB_CODE_SUCCESS);

  tTagFree(pTag);
  taosArrayDestroy(pArray);
}

// An empty JSON tag blob (nTag == 0, len == sizeof(STag)) is the legitimate "no JSON value"
// state produced by tTagNew on an empty array. It must validate as well-formed and decode to
// an empty value array rather than being rejected as forged (regression: tTagToValArray used
// p >= pTagEnd, which fired when the data-area start coincides with the blob end).
TEST(TagValidateTest, EmptyJsonBlobIsValidAndDecodesToEmptyArray) {
  // STag ends with a flexible array member, which MSVC refuses to instantiate
  // by value (C2466); build the header-only blob in a byte buffer instead.
  std::vector<uint8_t> emptyBlob(sizeof(STag), 0);
  STag *empty = reinterpret_cast<STag *>(emptyBlob.data());
  empty->flags = TD_TAG_JSON;
  empty->len = sizeof(STag);
  empty->nTag = 0;
  EXPECT_EQ(tTagValidate(empty, sizeof(STag)), TSDB_CODE_SUCCESS);

  SArray *pVals = nullptr;
  ASSERT_EQ(tTagToValArray(empty, &pVals), TSDB_CODE_SUCCESS);
  ASSERT_NE(pVals, nullptr);
  EXPECT_EQ(taosArrayGetSize(pVals), 0);
  taosArrayDestroy(pVals);
}

TEST(TagValidateTest, TagNewRejectsOversizedValue) {
  SArray *pArray = taosArrayInit(1, sizeof(STagVal));
  ASSERT_NE(pArray, nullptr);
  std::vector<uint8_t> big(70000, 'x');
  STagVal v = makeTagVal(2, TSDB_DATA_TYPE_VARCHAR);
  v.pData = big.data();
  v.nData = big.size();
  ASSERT_NE(taosArrayPush(pArray, &v), nullptr);

  STag *pTag = nullptr;
  EXPECT_NE(tTagNew(pArray, 1, 0, &pTag), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pTag, nullptr);
  taosArrayDestroy(pArray);
}

// Header-consistent blobs whose entries are forged: validation must walk every entry, and readers must not trust them.
TEST(TagValidateTest, RejectsForgedEntryContent) {
  SArray *pArray = taosArrayInit(2, sizeof(STagVal));
  ASSERT_NE(pArray, nullptr);
  char    str[] = "abc";
  STagVal v1 = makeTagVal(2, TSDB_DATA_TYPE_INT);
  v1.i64 = 7;
  STagVal v2 = makeTagVal(3, TSDB_DATA_TYPE_VARCHAR);
  v2.pData = (uint8_t *)str;
  v2.nData = 3;
  ASSERT_NE(taosArrayPush(pArray, &v1), nullptr);
  ASSERT_NE(taosArrayPush(pArray, &v2), nullptr);
  STag *pTag = nullptr;
  ASSERT_EQ(tTagNew(pArray, 1, 0, &pTag), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tTagValidate(pTag, pTag->len), TSDB_CODE_SUCCESS);

  const size_t body = sizeof(STag) + pTag->nTag;  // small blob: 1-byte idx entries
  // entry layout for cid < 64: [cid:1][type:1][value]
  {  // type outside the type table
    std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len);
    buf[body + pTag->idx[0] + 1] = 100;
    EXPECT_EQ(tTagValidate(buf.data(), buf.size()), TSDB_CODE_INVALID_MSG);
    STagVal probe = makeTagVal(2, TSDB_DATA_TYPE_NULL);
    EXPECT_FALSE(tTagGet((STag *)buf.data(), &probe));
  }
  {  // var-length value claiming more bytes than the blob holds
    std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len);
    buf[body + pTag->idx[1] + 2] = 0x7f;
    EXPECT_EQ(tTagValidate(buf.data(), buf.size()), TSDB_CODE_INVALID_MSG);
    STagVal probe = makeTagVal(3, TSDB_DATA_TYPE_NULL);
    EXPECT_FALSE(tTagGet((STag *)buf.data(), &probe));
    SArray *pVals = nullptr;
    EXPECT_NE(tTagToValArray((STag *)buf.data(), &pVals), TSDB_CODE_SUCCESS);
    taosArrayDestroy(pVals);
  }
  {  // fixed-width value running past the blob end
    std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len - 2);
    STag *pCut = (STag *)buf.data();
    pCut->len = (int16_t)buf.size();
    pCut->nTag = 1;  // keep only the INT entry, then cut into its value
    buf.resize(body - 1 + pTag->idx[0] + 4);
    pCut->len = (int16_t)buf.size();
    EXPECT_EQ(tTagValidate(buf.data(), buf.size()), TSDB_CODE_INVALID_MSG);
  }
  tTagFree(pTag);
  taosArrayDestroy(pArray);
}

// DECIMAL128 is 16 bytes wide but STagVal holds fixed-width values in an 8-byte i64: a forged type must be rejected
// instead of overrunning the output, both by the validator and by the readers.
TEST(TagValidateTest, RejectsFixedTypeWiderThanTagValue) {
  SArray *pArray = taosArrayInit(1, sizeof(STagVal));
  ASSERT_NE(pArray, nullptr);
  STagVal v1 = makeTagVal(2, TSDB_DATA_TYPE_BIGINT);
  v1.i64 = 7;
  ASSERT_NE(taosArrayPush(pArray, &v1), nullptr);
  STag *pTag = nullptr;
  ASSERT_EQ(tTagNew(pArray, 1, 0, &pTag), TSDB_CODE_SUCCESS);

  // the offset of the type byte below assumes one entry and a one-byte idx[]
  ASSERT_EQ(pTag->nTag, 1);
  ASSERT_EQ(pTag->flags & TD_TAG_LARGE, 0);
  // pad so that enough bytes follow for a 16-byte value: the type, not the remaining length, must reject it
  std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len);
  buf.resize(buf.size() + 32, 0);
  STag *pForged = (STag *)buf.data();
  pForged->len = (int16_t)buf.size();
  buf[sizeof(STag) + pTag->nTag + 1] = TSDB_DATA_TYPE_DECIMAL;
  EXPECT_EQ(tTagValidate(buf.data(), buf.size()), TSDB_CODE_INVALID_MSG);
  STagVal probe = makeTagVal(2, TSDB_DATA_TYPE_NULL);
  EXPECT_FALSE(tTagGet(pForged, &probe));
  SArray *pVals = nullptr;
  EXPECT_NE(tTagToValArray(pForged, &pVals), TSDB_CODE_SUCCESS);
  taosArrayDestroy(pVals);

  tTagFree(pTag);
  taosArrayDestroy(pArray);
}

// A JSON tag holding a null value is legitimate and must not be rejected by the per-entry validation.
TEST(TagValidateTest, JsonTagWithNullValueIsValid) {
  SArray *pArray = taosArrayInit(2, sizeof(STagVal));
  ASSERT_NE(pArray, nullptr);
  STagVal v1 = makeJsonTagVal((char *)"a", TSDB_DATA_TYPE_NULL);
  const char *json = "x";
  STagVal v2 = makeJsonTagVal((char *)"b", TSDB_DATA_TYPE_NCHAR);
  v2.pData = (uint8_t *)json;
  v2.nData = 1;
  ASSERT_NE(taosArrayPush(pArray, &v1), nullptr);
  ASSERT_NE(taosArrayPush(pArray, &v2), nullptr);
  STag *pTag = nullptr;
  ASSERT_EQ(tTagNew(pArray, 1, 1, &pTag), TSDB_CODE_SUCCESS);
  EXPECT_EQ(tTagValidate(pTag, pTag->len), TSDB_CODE_SUCCESS);
  SArray *pVals = nullptr;
  EXPECT_EQ(tTagToValArray(pTag, &pVals), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(pVals), 2);
  taosArrayDestroy(pVals);
  tTagFree(pTag);
  taosArrayDestroy(pArray);
}

// JSON keys are compared with strcmp, so a key that lost its terminating NUL must be rejected, not read past the blob.
TEST(TagValidateTest, RejectsJsonKeyWithoutTerminator) {
  SArray *pArray = taosArrayInit(1, sizeof(STagVal));
  ASSERT_NE(pArray, nullptr);
  STagVal v = makeJsonTagVal((char *)"a", TSDB_DATA_TYPE_NULL);
  ASSERT_NE(taosArrayPush(pArray, &v), nullptr);
  STag *pTag = nullptr;
  ASSERT_EQ(tTagNew(pArray, 1, 1, &pTag), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tTagValidate(pTag, pTag->len), TSDB_CODE_SUCCESS);

  std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len);
  const size_t      key = sizeof(STag) + pTag->nTag + pTag->idx[0];  // [len:1]["a\0"][type]
  ASSERT_EQ(buf[key], 2);
  ASSERT_EQ(buf[key + 2], '\0');
  buf[key + 2] = 'x';
  EXPECT_EQ(tTagValidate(buf.data(), buf.size()), TSDB_CODE_INVALID_MSG);
  STagVal probe = makeJsonTagVal((char *)"a", TSDB_DATA_TYPE_NULL);
  EXPECT_FALSE(tTagGet((STag *)buf.data(), &probe));
  SArray *pVals = nullptr;
  EXPECT_NE(tTagToValArray((STag *)buf.data(), &pVals), TSDB_CODE_SUCCESS);
  taosArrayDestroy(pVals);

  buf[key] = 0;  // a zero-length key has no room for the terminator either
  EXPECT_EQ(tTagValidate(buf.data(), buf.size()), TSDB_CODE_INVALID_MSG);

  tTagFree(pTag);
  taosArrayDestroy(pArray);
}

// Micro benchmark for the per-blob cost of tag validation; run with --gtest_also_run_disabled_tests.
// "raw" is the decode cost before validation existed (tDecodeBinary), "decode" is tDecodeTag including tTagValidate.
static STag *benchBuildTag(int32_t nTags) {
  SArray *pArray = taosArrayInit(nTags, sizeof(STagVal));
  static char str[] = "tag-value-xx";
  for (int32_t i = 0; i < nTags; ++i) {
    STagVal v = makeTagVal((col_id_t)(i + 2), (i % 2) ? TSDB_DATA_TYPE_VARCHAR : TSDB_DATA_TYPE_BIGINT);
    if (v.type == TSDB_DATA_TYPE_VARCHAR) {
      v.pData = (uint8_t *)str;
      v.nData = sizeof(str) - 1;
    } else {
      v.i64 = i * 1000003LL;
    }
    taosArrayPush(pArray, &v);
  }
  STag *pTag = nullptr;
  EXPECT_EQ(tTagNew(pArray, 1, 0, &pTag), TSDB_CODE_SUCCESS);
  taosArrayDestroy(pArray);
  return pTag;
}

TEST(TagValidateBench, DISABLED_PerBlobCost) {
  using clk = std::chrono::steady_clock;
  const int32_t iters = 2000000;
  volatile int64_t sink = 0;
  printf("%8s %8s %12s %12s %12s %12s\n", "nTags", "bytes", "raw ns", "decode ns", "validate ns", "tTagGet ns");
  for (int32_t nTags : {1, 4, 16, 64}) {
    STag *pTag = benchBuildTag(nTags);
    ASSERT_NE(pTag, nullptr);
    std::vector<uint8_t> buf(pTag->len + 16);
    SEncoder enc;
    tEncoderInit(&enc, buf.data(), buf.size());
    ASSERT_EQ(tEncodeTag(&enc, pTag), 0);
    int32_t encLen = enc.pos;
    tEncoderClear(&enc);

    auto t0 = clk::now();
    for (int32_t i = 0; i < iters; ++i) {
      SDecoder dec;
      tDecoderInit(&dec, buf.data(), encLen);
      uint8_t *p = nullptr;
      uint32_t l = 0;
      tDecodeBinary(&dec, &p, &l);
      sink += l;
      tDecoderClear(&dec);
    }
    auto t1 = clk::now();
    for (int32_t i = 0; i < iters; ++i) {
      SDecoder dec;
      tDecoderInit(&dec, buf.data(), encLen);
      STag *p = nullptr;
      sink += tDecodeTag(&dec, &p);
      tDecoderClear(&dec);
    }
    auto t2 = clk::now();
    for (int32_t i = 0; i < iters; ++i) sink += tTagValidate(pTag, pTag->len);
    auto t3 = clk::now();
    STagVal probe = makeTagVal((col_id_t)(nTags / 2 + 2), TSDB_DATA_TYPE_NULL);
    for (int32_t i = 0; i < iters; ++i) sink += tTagGet(pTag, &probe);
    auto t4 = clk::now();

    auto ns = [&](clk::time_point a, clk::time_point b) {
      return (double)std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count() / iters;
    };
    printf("%8d %8d %12.1f %12.1f %12.1f %12.1f\n", nTags, (int)pTag->len, ns(t0, t1), ns(t1, t2), ns(t2, t3),
           ns(t3, t4));
    tTagFree(pTag);
  }
  (void)sink;
}

TEST(RowKeyTest, ColDataKeyCountIsBounded) {
  const int32_t nCols = 6;
  SColData      aColData[nCols];
  memset(aColData, 0, sizeof(aColData));
  for (int32_t i = 0; i < nCols; ++i) {
    tColDataInit(&aColData[i], i + 1, i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT,
                 i == 0 ? 0 : COL_IS_KEY);
    SValue v = {0};
    v.type = aColData[i].type;
    v.val = 10 + i;
    // COL_VAL_VALUE is a C compound literal; MSVC rejects it in C++ (C4576),
    // so the same value is assembled field by field here.
    SColVal cv;
    memset(&cv, 0, sizeof(cv));
    cv.cid = (int16_t)(i + 1);
    cv.flag = CV_FLAG_VALUE;
    cv.value = v;
    ASSERT_EQ(tColDataAppendValue(&aColData[i], &cv), TSDB_CODE_SUCCESS);
  }

  SRowKey key = {0};
  tColDataArrGetRowKey(aColData, nCols, 0, &key);
  EXPECT_EQ(key.numOfPKs, TD_MAX_PK_COLS);
  for (int32_t i = 0; i < nCols; ++i) tColDataDestroy(&aColData[i]);
}

// The tag readers parse entries with a branch-light fast path while enough bytes remain and a byte-by-byte slow path
// near the end of the blob. Both must accept exactly what a plain bounded reference parser accepts, whatever the
// bytes are: mutate and truncate valid blobs and compare the three public readers against the reference.
static bool refReadVarint(const uint8_t *p, const uint8_t *end, uint32_t maxBytes, uint32_t *pv) {
  uint32_t v = 0;
  for (uint32_t i = 0; i < maxBytes && p + i < end; ++i) {
    v |= (uint32_t)(p[i] & 0x7f) << (7 * i);
    if (!(p[i] & 0x80)) {
      *pv = v;
      return true;
    }
  }
  return false;
}

static bool refTagValid(const STag *pTag) {
  const bool     large = pTag->flags & TD_TAG_LARGE;
  const uint8_t *end = (const uint8_t *)pTag + pTag->len;
  const uint8_t *body = (const uint8_t *)pTag->idx + pTag->nTag * (large ? 2 : 1);
  if (body > end) return false;
  for (int32_t i = 0; i < pTag->nTag; ++i) {
    int32_t offset = large ? ((const int16_t *)pTag->idx)[i] : pTag->idx[i];
    if (offset < 0 || offset >= end - body) return false;
    const uint8_t *p = body + offset;
    uint32_t       v = 0;
    if (!refReadVarint(p, end, 3, &v)) return false;
    while (*p & 0x80) ++p;
    ++p;
    if (p >= end) return false;
    int8_t type = (int8_t)*p++;
    if (IS_INVALID_TYPE(type)) return false;
    if (IS_VAR_DATA_TYPE(type)) {
      if (!refReadVarint(p, end, 5, &v)) return false;
      while (*p & 0x80) ++p;
      ++p;
      if (v > (uint32_t)(end - p)) return false;
    } else {
      int32_t bytes = tDataTypes[type].bytes;
      if (bytes > 8 || bytes > end - p) return false;
    }
  }
  return true;
}

TEST(TagValidateTest, ReadersAgreeWithReferenceParserOnMutatedBlobs) {
  std::mt19937 rng(20260101);
  int32_t      accepted = 0, rejected = 0;
  for (int32_t nTags : {1, 4, 64}) {
    STag *pTag = benchBuildTag(nTags);
    ASSERT_NE(pTag, nullptr);
    const bool large = pTag->flags & TD_TAG_LARGE;
    ASSERT_EQ(large, nTags == 64);  // cover both idx widths
    const size_t bodyStart = sizeof(STag);
    for (int32_t iter = 0; iter < 20000; ++iter) {
      std::vector<char> buf((char *)pTag, (char *)pTag + pTag->len);
      int32_t           nMut = rng() % 4;
      for (int32_t k = 0; k < nMut; ++k) buf[bodyStart + rng() % (buf.size() - bodyStart)] = (char)rng();
      if (rng() % 3 == 0) buf.resize(bodyStart + rng() % (buf.size() - bodyStart + 1));
      STag *pBad = (STag *)buf.data();
      pBad->len = (int16_t)buf.size();  // keep the header self-consistent so only entry content is under test

      bool expect = (size_t)pBad->nTag * (large ? 2 : 1) <= buf.size() - sizeof(STag) && refTagValid(pBad);
      EXPECT_EQ(tTagValidate(buf.data(), buf.size()) == TSDB_CODE_SUCCESS, expect) << "iter " << iter;
      SArray *pVals = nullptr;
      EXPECT_EQ(tTagToValArray(pBad, &pVals) == TSDB_CODE_SUCCESS, expect) << "iter " << iter;
      taosArrayDestroy(pVals);
      expect ? ++accepted : ++rejected;
    }
    tTagFree(pTag);
  }
  EXPECT_GT(accepted, 1000);  // both outcomes must actually be exercised
  EXPECT_GT(rejected, 1000);
}

TEST(RowKeyTest, RejectsTooManyPrimaryKeys) {
  std::vector<uint8_t> buf(sizeof(SRow) + 64, 0);
  SRow *pRow = (SRow *)buf.data();
  pRow->flag = HAS_VALUE;
  pRow->len = buf.size();
  pRow->numOfPKs = 0xff;
  EXPECT_FALSE(tRowPrimaryKeyValid(pRow));

  SRowKey key = {0};
  tRowGetPrimaryKey(pRow, &key);  // must not write past key.pks
  EXPECT_EQ(key.numOfPKs, 0);

  // one key entry whose offset points past the row
  pRow->numOfPKs = 1;
  pRow->data[0] = TSDB_DATA_TYPE_INT;
  pRow->data[1] = 0x7f;
  EXPECT_FALSE(tRowPrimaryKeyValid(pRow));

  pRow->data[1] = 0;
  EXPECT_TRUE(tRowPrimaryKeyValid(pRow));

  // the limit itself is allowed, one more is not
  pRow->numOfPKs = TD_MAX_PK_COLS + 1;
  EXPECT_FALSE(tRowPrimaryKeyValid(pRow));
  std::fill(buf.begin() + sizeof(SRow), buf.end(), 0);
  for (int32_t i = 0; i < TD_MAX_PK_COLS; ++i) pRow->data[2 * i] = TSDB_DATA_TYPE_TINYINT;  // type, offset 0
  pRow->numOfPKs = TD_MAX_PK_COLS;
  EXPECT_TRUE(tRowPrimaryKeyValid(pRow));
}

// Rows built by tRowBuild with real primary keys must pass tRowPrimaryKeyValid and yield the same key through
// tRowGetKey, for both the dense (tuple) and the sparse (KV) layouts, with fixed and var-length key columns.
static void checkRowKeyRoundTrip(bool sparse, bool varKey, int32_t nPks, int32_t bigBytes = 0) {
  // ts, [pk0], [pk1], v
  std::vector<SSchema> cols;
  auto addCol = [&](int8_t type, int32_t bytes, int8_t flags) {
    SSchema c;
    memset(&c, 0, sizeof(c));
    c.colId = PRIMARYKEY_TIMESTAMP_COL_ID + (int16_t)cols.size();
    c.type = type;
    c.bytes = bytes;
    c.flags = flags;
    snprintf(c.name, sizeof(c.name), "c%d", (int)cols.size());
    cols.push_back(c);
  };
  addCol(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), COL_IS_KEY);
  if (nPks >= 1) varKey ? addCol(TSDB_DATA_TYPE_VARCHAR, 32, COL_IS_KEY) : addCol(TSDB_DATA_TYPE_INT, 4, COL_IS_KEY);
  if (nPks >= 2) addCol(TSDB_DATA_TYPE_BIGINT, 8, COL_IS_KEY);
  if (bigBytes > 0) addCol(TSDB_DATA_TYPE_VARCHAR, bigBytes + 8, 0);  // pushes the KV offsets past one byte
  addCol(TSDB_DATA_TYPE_INT, 4, 0);
  STSchema *pTSchema = tBuildTSchema(cols.data(), (int32_t)cols.size(), 1);
  ASSERT_NE(pTSchema, nullptr);

  static char str[] = "pk-string-value";
  std::vector<uint8_t> big(bigBytes > 0 ? bigBytes : 1, 'x');
  SArray     *pVals = taosArrayInit(cols.size(), sizeof(SColVal));
  for (size_t i = 0; i < cols.size(); ++i) {
    SValue v;
    memset(&v, 0, sizeof(v));
    v.type = cols[i].type;
    if (cols[i].type == TSDB_DATA_TYPE_VARCHAR && cols[i].flags == 0) {
      v.pData = big.data();
      v.nData = big.size();
    } else if (cols[i].type == TSDB_DATA_TYPE_VARCHAR) {
      v.pData = (uint8_t *)str;
      v.nData = sizeof(str) - 1;
    } else {
      v.val = 1000 + (int64_t)i;
    }
    // COL_VAL_VALUE / COL_VAL_NONE are C compound literals; MSVC rejects them
    // in C++ (C4576), so the same values are assembled field by field here.
    SColVal cv;
    memset(&cv, 0, sizeof(cv));
    cv.cid = cols[i].colId;
    cv.flag = CV_FLAG_VALUE;
    cv.value = v;
    if (sparse && i + 1 == cols.size()) {  // forces the KV layout
      cv.flag = CV_FLAG_NONE;
      memset(&cv.value, 0, sizeof(cv.value));
      cv.value.type = cols[i].type;
    }
    taosArrayPush(pVals, &cv);
  }

  SRow             *pRow = nullptr;
  SRowBuildScanInfo info;
  memset(&info, 0, sizeof(info));
  ASSERT_EQ(tRowBuild(pVals, pTSchema, &pRow, &info), TSDB_CODE_SUCCESS);
  ASSERT_NE(pRow, nullptr);
  EXPECT_EQ(pRow->numOfPKs, nPks) << "sparse=" << sparse << " var=" << varKey;
  EXPECT_TRUE(tRowPrimaryKeyValid(pRow)) << "sparse=" << sparse << " var=" << varKey << " pks=" << nPks;

  SRowKey key;
  memset(&key, 0, sizeof(key));
  tRowGetKey(pRow, &key);
  EXPECT_EQ(key.numOfPKs, nPks);
  EXPECT_EQ(key.ts, 1000);
  if (nPks >= 1) {
    if (varKey) {
      ASSERT_EQ(key.pks[0].nData, sizeof(str) - 1);
      EXPECT_EQ(memcmp(key.pks[0].pData, str, sizeof(str) - 1), 0);
    } else {
      EXPECT_EQ(key.pks[0].val, 1001);
    }
  }
  if (nPks >= 2) EXPECT_EQ(key.pks[1].val, 1002);

  // the row survives a wire round trip through the validating decoder
  std::vector<uint8_t> buf(pRow->len + 16);
  SEncoder             enc;
  tEncoderInit(&enc, buf.data(), (uint32_t)buf.size());
  ASSERT_EQ(tEncodeRow(&enc, pRow), 0);
  uint32_t encLen = enc.pos;
  tEncoderClear(&enc);
  SDecoder dec;
  tDecoderInit(&dec, buf.data(), encLen);
  SRow *pDecoded = nullptr;
  EXPECT_EQ(tDecodeRow(&dec, &pDecoded), TSDB_CODE_SUCCESS);
  tDecoderClear(&dec);

  tRowDestroy(pRow);
  taosArrayDestroy(pVals);
  taosMemoryFree(pTSchema);
}

TEST(RowKeyTest, RealRowsWithPrimaryKeysAreValid) {
  for (bool sparse : {false, true}) {
    for (bool varKey : {false, true}) {
      for (int32_t nPks : {0, 1, 2}) {
        if (nPks == 0 && varKey) continue;
        checkRowKeyRoundTrip(sparse, varKey, nPks);
        if (nPks > 0) checkRowKeyRoundTrip(sparse, varKey, nPks, 300);    // wider KV offsets
        if (nPks > 0) checkRowKeyRoundTrip(sparse, varKey, nPks, 40000);  // widest KV offsets
      }
    }
  }
}

// ==================== TD-SEC-2026-031: integer overflow in ColData/INSERT deserialization ====================
// Hand-crafted wire payloads; each malicious case must be rejected with TSDB_CODE_INVALID_MSG
// and must never hand back pointers beyond the decoder buffer.

// LEB128 varint writer, byte-compatible with tDecodeU32v
static int32_t secPutU32v(uint8_t *p, uint32_t v) {
  int32_t n = 0;
  while (v >= 0x80) {
    p[n++] = (uint8_t)(v | 0x80);
    v >>= 7;
  }
  p[n++] = (uint8_t)v;
  return n;
}

// zigzag + varint writer, byte-compatible with tDecodeI32v
static int32_t secPutI32v(uint8_t *p, int32_t v) {
  return secPutU32v(p, ZIGZAGE(int32_t, v));
}

static int32_t secDecodeColData(const uint8_t *buf, uint32_t len, uint8_t version, bool jump, SColData *pColData) {
  SDecoder dec = {0};
  tDecoderInit(&dec, (uint8_t *)buf, len);
  int32_t code = tDecodeColData(version, &dec, pColData, jump);
  tDecoderClear(&dec);
  return code;
}

// header shared by the crafted ColData payloads: cid/type/nVal/flag
static int32_t secPutColDataHeader(uint8_t *p, int8_t type, int32_t nVal, uint8_t flag) {
  int32_t n = 0;
  n += secPutI32v(p + n, 1);  // cid
  p[n++] = (uint8_t)type;
  n += secPutI32v(p + n, nVal);
  p[n++] = flag;
  return n;
}

TEST(testCase, secColDataRejectsNegativeNVal) {
  uint8_t buf[64] = {0};
  int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_INT, -1, HAS_NULL | HAS_NONE);

  SColData colData = {0};
  EXPECT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_INVALID_MSG);
}

TEST(testCase, secColDataRejectsOffsetSizeOverflow) {
  uint8_t buf[64] = {0};
  // nVal = 2^30: nVal << 2 overflows int32 and truncates to 0, so the bounds
  // check passes while the consumer later reads aOffset[0..nVal)
  int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_BINARY, 0x40000000, HAS_VALUE);
  n += secPutI32v(buf + n, 0);  // nData

  SColData colData = {0};
  EXPECT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_INVALID_MSG);
}

TEST(testCase, secColDataRejectsNegativeNData) {
  uint8_t buf[64] = {0};
  int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_BINARY, 1, HAS_VALUE);
  memset(buf + n, 0, 4);  // aOffset[0]: 4 raw bytes, not varint
  n += 4;
  n += secPutI32v(buf + n, -1);  // nData = -1: (uint32_t)-1 wraps the bounds check

  SColData colData = {0};
  EXPECT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_INVALID_MSG);
}

TEST(testCase, secColDataRejectsFixedDataSizeOverflow) {
  uint8_t buf[64] = {0};
  // nVal = INT32_MAX with an 8-byte fixed type: TYPE_BYTES * nVal overflows
  // int32 and wraps to -8, which bypasses the bounds check as 0xFFFFFFF8
  int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_BIGINT, 0x7FFFFFFF, HAS_VALUE);

  SColData colData = {0};
  EXPECT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_INVALID_MSG);
}

TEST(testCase, secColDataV2JumpRejectsBadLength) {
  {
    // negative length: pDecoder->pos += (len - INT_BYTES) underflows
    uint8_t buf[16] = {0};
    int32_t len = -8;
    memcpy(buf, &len, sizeof(len));

    SColData colData = {0};
    EXPECT_EQ(secDecodeColData(buf, 4, 2, true, &colData), TSDB_CODE_INVALID_MSG);
  }
  {
    // huge length: pos jumps past the end of the buffer
    uint8_t buf[16] = {0};
    int32_t len = 0x7FFFFFF0;
    memcpy(buf, &len, sizeof(len));

    SColData colData = {0};
    EXPECT_EQ(secDecodeColData(buf, 4, 2, true, &colData), TSDB_CODE_INVALID_MSG);
  }
}

TEST(testCase, secColDataStillAcceptsValidPayload) {
  {  // fixed-length values, no bitmap
    uint8_t buf[64] = {0};
    int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_INT, 2, HAS_VALUE);
    memset(buf + n, 0x11, 8);
    n += 8;

    SColData colData = {0};
    ASSERT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_SUCCESS);
    EXPECT_EQ(colData.nVal, 2);
    EXPECT_EQ(colData.nData, 8);
    EXPECT_EQ(((int32_t *)colData.pData)[1], 0x11111111);
  }
  {  // var-length values with offset array
    uint8_t buf[64] = {0};
    int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_BINARY, 1, HAS_VALUE);
    memset(buf + n, 0, 4);  // aOffset[0]: 4 raw bytes, not varint
    n += 4;
    n += secPutI32v(buf + n, 3);  // nData
    memcpy(buf + n, "abc", 3);
    n += 3;

    SColData colData = {0};
    ASSERT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_SUCCESS);
    EXPECT_EQ(colData.nVal, 1);
    EXPECT_EQ(colData.aOffset[0], 0);
    EXPECT_EQ(memcmp(colData.pData, "abc", 3), 0);
  }
  {  // 2-bit bitmap path
    uint8_t buf[64] = {0};
    int32_t n = secPutColDataHeader(buf, TSDB_DATA_TYPE_INT, 3, HAS_VALUE | HAS_NULL | HAS_NONE);
    buf[n++] = 0b00111111;  // BIT2_SIZE(3) == 1
    memset(buf + n, 0x22, 12);
    n += 12;

    SColData colData = {0};
    ASSERT_EQ(secDecodeColData(buf, n, 0, false, &colData), TSDB_CODE_SUCCESS);
    EXPECT_EQ(colData.nVal, 3);
    EXPECT_EQ(colData.nData, 12);
  }
  {  // v2 jump with a well-formed length
    uint8_t buf[8] = {0};
    int32_t len = 8;
    memcpy(buf, &len, sizeof(len));

    SColData colData = {0};
    ASSERT_EQ(secDecodeColData(buf, sizeof(buf), 2, true, &colData),
              TSDB_CODE_SUCCESS);
  }
}
