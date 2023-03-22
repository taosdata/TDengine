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

#include "geomFuncTestUtil.h"

void callGeomRelationFuncAndCompareResult(FScalarExecProcess geomRelationFunc,
                                          SScalarParam *pInputIntersects, int32_t rowNum,
                                          int32_t expectedCode, int8_t expectedResult[]) {
  SScalarParam *pOutputIntersects;
  makeOneScalarParam(&pOutputIntersects, TSDB_DATA_TYPE_BOOL, 0, rowNum);
  int32_t code = geomRelationFunc(pInputIntersects, 2, pOutputIntersects);
  ASSERT_EQ(code, expectedCode);

  if (code == TSDB_CODE_SUCCESS) {
    int8_t res = -1;
    for (int32_t i = 0; i < rowNum; ++i) {
      bool isNull1 = colDataIsNull_s(pOutputIntersects->columnData, i);
      if (isNull1) {
        res = -1;
      }
      else {
        res = *(bool*)colDataGetData(pOutputIntersects->columnData, i);
      }

      ASSERT_EQ(res, expectedResult[i]);
    }
  }

  destroyScalarParam(pOutputIntersects, 1);
  destroyScalarParam(pInputIntersects, 2);
}

void callIntersectsAndCompareResult(SScalarParam *pInputIntersects, int32_t rowNum, int32_t expectedCode, int8_t expectedResult[]) {
  callGeomRelationFuncAndCompareResult(intersectsFunction,
                                       pInputIntersects, rowNum,
                                       expectedCode, expectedResult);
}

TEST(GeomFuncTest, intersectsFunction) {
  int32_t rowNum = 6;

  char strArray1[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray1[0], "POINT(3.5 7.0)");
  STR_TO_VARSTR(strArray1[1], "POINT(3.0 3.0)");
  STR_TO_VARSTR(strArray1[2], "POINT(4.0 7.0)");
  STR_TO_VARSTR(strArray1[3], "LINESTRING(1.0 1.0, 2.0 2.0, 5.0 5.0)");
  STR_TO_VARSTR(strArray1[4], "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 5.0)");
  STR_TO_VARSTR(strArray1[5], "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))");

  char strArray2[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray2[0], "POINT(3.5 7.0)");
  STR_TO_VARSTR(strArray2[1], "LINESTRING(1.0 1.0, 2.0 2.0, 5.0 6.0)");
  STR_TO_VARSTR(strArray2[2], "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))");
  STR_TO_VARSTR(strArray2[3], "LINESTRING(1.0 4.0, 2.0 3.0, 5.0 0.0)");
  STR_TO_VARSTR(strArray2[4], "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))");
  STR_TO_VARSTR(strArray2[5], "POLYGON((5.0 6.0, 7.0 6.0, 7.0 8.0, 5.0 8.0, 5.0 6.0))");

  // two columns input
  SScalarParam *pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, rowNum, pInputIntersects);  //pInputIntersects come from GeomFromText()
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects + 1);
  int8_t expectedResultTwoCol[rowNum] = {1, 0, 1, 1, 0, 1}; // 1: true, 0: false, -1: null
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultTwoCol);

  // swap two columns
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects);
  callGeomFromTextWrapper5(strArray1, rowNum, pInputIntersects + 1);
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultTwoCol);

  // constant and column input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, 1, pInputIntersects);
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects + 1);
  int8_t expectedResultFirstConst[rowNum] = {1, 0, 1, 0, 1, 0};
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultFirstConst);

  // column and constant input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, rowNum, pInputIntersects);
  callGeomFromTextWrapper5(strArray2, 1, pInputIntersects + 1);
  int8_t expectedResultSecondConst[rowNum] = {1, 0, 0, 0, 0, 1};
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultSecondConst);

  // two constants input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, 1, pInputIntersects);
  callGeomFromTextWrapper5(strArray2, 1, pInputIntersects + 1);
  int8_t expectedResultTwoConst[1] = {1};
  callIntersectsAndCompareResult(pInputIntersects, 1, TSDB_CODE_SUCCESS, expectedResultTwoConst);

  // two columns with NULL value input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  STR_TO_VARSTR(strArray1[2], "");
  STR_TO_VARSTR(strArray2[4], "");
  callGeomFromTextWrapper5(strArray1, rowNum, pInputIntersects);
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects + 1);
  int8_t expectedResultWithNull[rowNum] = {1, 0, -1, 1, -1, 1};
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultWithNull);

  // first NULL type input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  setScalarParam(pInputIntersects, TSDB_DATA_TYPE_NULL, 0, 1);
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects + 1);
  int8_t expectedResultNullType[rowNum] = {-1, -1, -1, -1, -1, -1};
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultNullType);

  // second NULL type input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, rowNum, pInputIntersects);
  setScalarParam(pInputIntersects + 1, TSDB_DATA_TYPE_NULL, 0, 1);
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultNullType);

  // first empty content input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  char strInput[TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strInput, "");
  setScalarParam(pInputIntersects, TSDB_DATA_TYPE_GEOMETRY, strInput, 1);
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects + 1);
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_SUCCESS, expectedResultNullType);

  // first wrong type input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  int32_t intInput = 3;
  setScalarParam(pInputIntersects, TSDB_DATA_TYPE_INT, &intInput, 1);
  callGeomFromTextWrapper5(strArray2, rowNum, pInputIntersects + 1);
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_FUNC_FUNTION_PARA_VALUE, 0);

  // second wrong content input
  pInputIntersects = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  STR_TO_VARSTR(strInput, "XXX");
  callGeomFromTextWrapper5(strArray1, rowNum, pInputIntersects);
  setScalarParam(pInputIntersects + 1, TSDB_DATA_TYPE_GEOMETRY, strInput, 1);
  callIntersectsAndCompareResult(pInputIntersects, rowNum, TSDB_CODE_FUNC_FUNTION_PARA_VALUE, 0);
}
