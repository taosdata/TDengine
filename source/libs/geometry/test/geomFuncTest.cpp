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

void setScalarParam(SScalarParam* sclParam, int32_t type, void* val[], int32_t rowNum) {
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
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
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

  if (val) {
    for (int32_t i = 0; i < rowNum; ++i) {
      colDataAppend(sclParam->columnData, i, (const char *)val[i], false);
    }
  }
}

void makeOneScalarParam(SScalarParam **pSclParam, int32_t type, void* val[], int32_t rowNum) {
  *pSclParam = (SScalarParam *)taosMemoryCalloc(1, sizeof(SScalarParam));
  setScalarParam(*pSclParam, type, val, rowNum);
}

void destroyScalarParam(SScalarParam *sclParam, int32_t colNum) {
  for (int32_t i = 0; i < colNum; ++i) {
    colDataDestroy((sclParam + i)->columnData);
    taosMemoryFree((sclParam + i)->columnData);
  }
  taosMemoryFree(sclParam);
}

void* makeVarStr(const char* szString) {
  int32_t strSize = strlen(szString);
  void* varString = taosMemoryCalloc(1, strSize + VARSTR_HEADER_SIZE);

  memcpy(varDataVal(varString), szString, strSize);
  varDataSetLen(varString, strSize);

  return varString;
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

void CallMakePointAndCompareResult(SScalarParam *pInputMakePoint, int32_t rowNum, SScalarParam* pOutputGeomFromText) {
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

// compare geometry results of MakePoint() and GeomFromText(<POINT>)
TEST(GeomFuncTest, makePointFunction) {
  SScalarParam *pInputMakePoint;
  SScalarParam *pOutputGeomFromText;
  int32_t rowNum = 3;
  void* paramVal[rowNum];

  // call GeomFromText(<POINT>) and generate pOutputGeomFromText to compare later
  SScalarParam *pInputGeomFromText;
  paramVal[0] = makeVarStr("POINT(2.0 3.0)");
  paramVal[1] = makeVarStr("POINT(3.0 3.0)");
  paramVal[2] = makeVarStr("POINT(4.0 3.0)");
  makeOneScalarParam(&pInputGeomFromText, TSDB_DATA_TYPE_VARCHAR, paramVal, rowNum);
  makeOneScalarParam(&pOutputGeomFromText, TSDB_DATA_TYPE_GEOMETRY, 0, rowNum);
  for (int32_t i = 0; i < rowNum; ++i) {
    taosMemoryFree(paramVal[i]);
  }
  int32_t code = geomFromTextFunction(pInputGeomFromText, 1, pOutputGeomFromText);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  destroyScalarParam(pInputGeomFromText, 1);

  // call MakePoint() with TINYINT, and compare with result of GeomFromText(<POINT>) 
  int8_t valTinyInt[2][rowNum] = {{2, 3, 4}, {3, 3, 3}};
  pInputMakePoint = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  for (int32_t i = 0; i < rowNum; ++i) {
    paramVal[i] = &valTinyInt[0][i];
  }
  setScalarParam(pInputMakePoint, TSDB_DATA_TYPE_TINYINT, paramVal, rowNum);
  for (int32_t i = 0; i < rowNum; ++i) {
    paramVal[i] = &valTinyInt[1][i];
  }
  setScalarParam(pInputMakePoint + 1, TSDB_DATA_TYPE_TINYINT, paramVal, rowNum);
  CallMakePointAndCompareResult(pInputMakePoint, rowNum, pOutputGeomFromText);

  // call MakePoint() with FLOAT, and compare with result of GeomFromText(<POINT>)
  float valFloat[2][rowNum] = {{2.0, 3.0, 4.0}, {3.0, 3.0, 3.0}};
  pInputMakePoint = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  for (int32_t i = 0; i < rowNum; ++i) {
    paramVal[i] = &valFloat[0][i];
  }
  setScalarParam(pInputMakePoint, TSDB_DATA_TYPE_FLOAT, paramVal, rowNum);
  for (int32_t i = 0; i < rowNum; ++i) {
    paramVal[i] = &valFloat[1][i];
  }
  setScalarParam(pInputMakePoint + 1, TSDB_DATA_TYPE_FLOAT, paramVal, rowNum);
  CallMakePointAndCompareResult(pInputMakePoint, rowNum, pOutputGeomFromText);

  // call MakePoint() with TINYINT AND FLOAT, and compare with result of GeomFromText(<POINT>)
  int8_t val1TinyInt[] = {2, 3, 4};
  float  val2Float[] = {3.0, 3.0, 3.0};
  pInputMakePoint = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  for (int32_t i = 0; i < rowNum; ++i) {
    paramVal[i] = &val1TinyInt[i];
  }
  setScalarParam(pInputMakePoint + 0, TSDB_DATA_TYPE_TINYINT, paramVal, rowNum);
  for (int32_t i = 0; i < rowNum; ++i) {
    paramVal[i] = &val2Float[i];
  }
  setScalarParam(pInputMakePoint + 1, TSDB_DATA_TYPE_FLOAT, paramVal, rowNum);
  CallMakePointAndCompareResult(pInputMakePoint, rowNum, pOutputGeomFromText);

  destroyScalarParam(pOutputGeomFromText, 1);
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
