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

#include "tdatablock.h"
#include "geomFunc.h"

void setColumnInfo(SColumnInfo* info, int32_t colId, int32_t type, int32_t bytes) {
  memset(info, 0, sizeof(SColumnInfo));
  info->colId = colId;
  info->type = type;
  info->bytes = bytes;
}

void setScalarParam(SScalarParam* sclParam, int32_t type, void* valueArray, int32_t rowNum) {
  int32_t bytes = 0;
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      bytes = sizeof(int8_t);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      bytes = sizeof(int16_t);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      bytes = sizeof(int32_t);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      bytes = sizeof(int64_t);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      bytes = sizeof(float);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      bytes = sizeof(double);
      break;
    }
    case TSDB_DATA_TYPE_VARCHAR: {
      bytes = TSDB_MAX_BINARY_LEN;
      break;
    }
    case TSDB_DATA_TYPE_GEOMETRY: {
      bytes = TSDB_MAX_GEOMETRY_LEN;
      break;
    }
    default: {
      ASSERT(0);
      break;
    }
  }

  sclParam->columnData = (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  sclParam->numOfRows = rowNum;

  setColumnInfo(&sclParam->columnData->info, 0, type, bytes);
  colInfoDataEnsureCapacity(sclParam->columnData, rowNum);

  if (valueArray) {
    for (int32_t i = 0; i < rowNum; ++i) {
      colDataAppend(sclParam->columnData, i, (const char *)valueArray + (i * bytes), false);
    }
  }
}

void destroyScalarParam(SScalarParam *sclParam, int32_t colNum) {
  for (int32_t i = 0; i < colNum; ++i) {
    colDataDestroy((sclParam + i)->columnData);
    taosMemoryFree((sclParam + i)->columnData);
  }
  taosMemoryFree(sclParam);
}

void makeOneScalarParam(SScalarParam **pSclParam, int32_t type, void* valueArray, int32_t rowNum) {
  *pSclParam = (SScalarParam *)taosMemoryCalloc(1, sizeof(SScalarParam));
  setScalarParam(*pSclParam, type, valueArray, rowNum);
}

void outputGeomParamByGeomFromText(void* strArray, int32_t rowNum, SScalarParam **pOutputGeomFromText) {
  SScalarParam* pInputGeomFromText;
  makeOneScalarParam(&pInputGeomFromText, TSDB_DATA_TYPE_VARCHAR, strArray, rowNum);
  makeOneScalarParam(pOutputGeomFromText, TSDB_DATA_TYPE_GEOMETRY, 0, rowNum);

  int32_t code = geomFromTextFunction(pInputGeomFromText, 1, *pOutputGeomFromText);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  destroyScalarParam(pInputGeomFromText, 1);
}

bool compareVarData(unsigned char *input1, unsigned char *input2) {
  if (varDataLen(input1) == 0 || varDataLen(input2) == 0) {
    return false;
  }
  if(varDataLen(input1) != varDataLen(input2)) {
    return false;
  }

  return (memcmp(varDataVal(input1), varDataVal(input2), varDataLen(input1)) == 0);
}

void callMakePointAndCompareResult(int32_t type1, void* valueArray1, bool isConstant1,
                                   int32_t type2, void* valueArray2, bool isConstant2,
                                   int32_t rowNum, SScalarParam* pOutputGeomFromText) {
  int32_t rowNum1 = isConstant1 ? 1 : rowNum;
  int32_t rowNum2 = isConstant2 ? 1 : rowNum;

  SScalarParam* pInputMakePoint = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  setScalarParam(pInputMakePoint, type1, valueArray1, rowNum1);
  setScalarParam(pInputMakePoint + 1, type2, valueArray2, rowNum2);

  SScalarParam *pOutputMakePoint;
  makeOneScalarParam(&pOutputMakePoint, TSDB_DATA_TYPE_GEOMETRY, 0, rowNum);

  int32_t code = makePointFunction(pInputMakePoint, 2, pOutputMakePoint);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < rowNum; ++i) {
    bool res = compareVarData((unsigned char *)colDataGetData(pOutputMakePoint->columnData, i),
                              (unsigned char *)colDataGetData(pOutputGeomFromText->columnData, i));
    ASSERT_EQ(res, true);
  }

  destroyScalarParam(pInputMakePoint, 2);
  destroyScalarParam(pOutputMakePoint, 1);
}

#define MAKE_POINT_FIRST_COLUMN_VALUES {2, 3, -4}
#define MAKE_POINT_SECOND_COLUMN_VALUES {5, -6, -7}

TEST(GeomFuncTest, makePointFunctionTwoColumns) {
  int32_t rowNum = 3;
  SScalarParam* pOutputGeomFromText;

  // call GeomFromText(<POINT>) and generate pOutputGeomFromText to compare later
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray[0], "POINT(2.0 5.0)");
  STR_TO_VARSTR(strArray[1], "POINT(3.0 -6.0)");
  STR_TO_VARSTR(strArray[2], "POINT(-4.0 -7.0)");
  outputGeomParamByGeomFromText(strArray, rowNum, &pOutputGeomFromText);

  // call MakePoint() with TINYINT and SMALLINT, and compare with result of GeomFromText(<POINT>) 
  int8_t tinyIntArray1[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  int16_t smallIntArray2[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_TINYINT, tinyIntArray1, false,
                                TSDB_DATA_TYPE_SMALLINT, smallIntArray2, false,
                                rowNum, pOutputGeomFromText);

  // call MakePoint() with INT and BIGINT, and compare with result of GeomFromText(<POINT>) 
  int32_t intArray1[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  int64_t bigIntArray2[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_INT, intArray1, false,
                                TSDB_DATA_TYPE_BIGINT, bigIntArray2, false,
                                rowNum, pOutputGeomFromText);

  // call MakePoint() with FLOAT and DOUBLE, and compare with result of GeomFromText(<POINT>)
  float floatArray1[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  double doubleArray2[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_FLOAT, floatArray1, false,
                                TSDB_DATA_TYPE_DOUBLE, doubleArray2, false,
                                rowNum, pOutputGeomFromText);

  destroyScalarParam(pOutputGeomFromText, 1);
}

TEST(GeomFuncTest, makePointFunctionLeftConstant) {
  int32_t rowNum = 3;
  SScalarParam* pOutputGeomFromText;

  // call GeomFromText(<POINT>) and generate pOutputGeomFromText to compare later
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray[0], "POINT(3.0 5.0)");
  STR_TO_VARSTR(strArray[1], "POINT(3.0 -6.0)");
  STR_TO_VARSTR(strArray[2], "POINT(3.0 -7.0)");
  outputGeomParamByGeomFromText(strArray, rowNum, &pOutputGeomFromText);

  // call MakePoint() with TINYINT constant and BIGINT, and compare with result of GeomFromText(<POINT>) 
  int8_t constantValue = 3;
  int64_t bigIntArrayArray[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_TINYINT, &constantValue, true,
                                TSDB_DATA_TYPE_BIGINT, bigIntArrayArray, false,
                                rowNum, pOutputGeomFromText);
}

TEST(GeomFuncTest, makePointFunctionRightConstant) {
  int32_t rowNum = 3;
  SScalarParam* pOutputGeomFromText;

  // call GeomFromText(<POINT>) and generate pOutputGeomFromText to compare later
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray[0], "POINT(2.0 3.0)");
  STR_TO_VARSTR(strArray[1], "POINT(3.0 3.0)");
  STR_TO_VARSTR(strArray[2], "POINT(-4.0 3.0)");
  outputGeomParamByGeomFromText(strArray, rowNum, &pOutputGeomFromText);

  // call MakePoint() with INT and FLOAT constant, and compare with result of GeomFromText(<POINT>) 
  int32_t intArray[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  float constantValue = 3;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_INT, intArray, false,
                                TSDB_DATA_TYPE_FLOAT, &constantValue, true,
                                rowNum, pOutputGeomFromText);
}

TEST(GeomFuncTest, makePointFunctionWithLeftNull) {
}

TEST(GeomFuncTest, makePointFunctionWithRightNull) {
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
