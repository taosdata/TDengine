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
                                          SScalarParam *pInput, int32_t rowNum,
                                          int32_t expectedCode, int8_t expectedResult[]) {
  SScalarParam *pOutput;
  makeOneScalarParam(&pOutput, TSDB_DATA_TYPE_BOOL, 0, 0, rowNum);
  int32_t code = geomRelationFunc(pInput, 2, pOutput);
  ASSERT_EQ(code, expectedCode);

  if (code == TSDB_CODE_SUCCESS) {
    int8_t res = -1;
    for (int32_t i = 0; i < rowNum; ++i) {
      bool isNull1 = colDataIsNull_s(pOutput->columnData, i);
      if (isNull1) {
        res = -1;
      }
      else {
        res = *(bool*)colDataGetData(pOutput->columnData, i);
      }

      ASSERT_EQ(res, expectedResult[i]);
    }
  }

  destroyScalarParam(pOutput, 1);
  destroyScalarParam(pInput, 2);
}

/*
-- Use the following SQL to get expected results for all relation functions in PostgreSQL with PostGIS
WITH geom_str AS
(SELECT 'POINT(3.5 7.0)' AS g1, 'POINT(3.5 7.0)' AS g2
UNION ALL
SELECT 'POINT(3.0 3.0)' AS g1, 'LINESTRING(1.0 1.0, 2.0 2.0, 5.0 6.0)' AS g2
UNION ALL
SELECT 'POINT(3.0 6.0)' AS g1, 'POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))' AS g2
UNION ALL
SELECT 'LINESTRING(1.0 1.0, 2.0 2.0, 5.0 5.0)' AS g1, 'LINESTRING(1.0 4.0, 2.0 3.0, 5.0 0.0)' AS g2
UNION ALL
SELECT 'LINESTRING(3.0 7.0, 4.0 7.0, 5.0 7.0)' AS g1, 'POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))' AS g2
UNION ALL
SELECT 'POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))' AS g1, 'POLYGON((5.0 6.0, 7.0 6.0, 7.0 8.0, 5.0 8.0, 5.0 6.0))' AS g2
)
SELECT ST_Intersects(g1, g2), ST_Equals(g1, g2), ST_Touches(g1, g2), ST_Covers(g1, g2), ST_Contains(g1, g2), ST_ContainsProperly(g1, g2) FROM geom_str
*/
void geomRelationFuncTest(FScalarExecProcess geomRelationFunc, int8_t expectedResults[6][6]) {
  const int32_t rowNum = 6;

  char strArray1[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray1[0], "POINT(3.5 7.0)");
  STR_TO_VARSTR(strArray1[1], "POINT(3.0 3.0)");
  STR_TO_VARSTR(strArray1[2], "POINT(3.0 6.0)");
  STR_TO_VARSTR(strArray1[3], "LINESTRING(1.0 1.0, 2.0 2.0, 5.0 5.0)");
  STR_TO_VARSTR(strArray1[4], "LINESTRING(3.0 7.0, 4.0 7.0, 5.0 7.0)");
  STR_TO_VARSTR(strArray1[5], "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))");
  TDRowValT valTypeArray1[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  char strArray2[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray2[0], "POINT(3.5 7.0)");
  STR_TO_VARSTR(strArray2[1], "LINESTRING(1.0 1.0, 2.0 2.0, 5.0 6.0)");
  STR_TO_VARSTR(strArray2[2], "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))");
  STR_TO_VARSTR(strArray2[3], "LINESTRING(1.0 4.0, 2.0 3.0, 5.0 0.0)");
  STR_TO_VARSTR(strArray2[4], "POLYGON((3.0 6.0, 5.0 6.0, 5.0 8.0, 3.0 8.0, 3.0 6.0))");
  STR_TO_VARSTR(strArray2[5], "POLYGON((5.0 6.0, 7.0 6.0, 7.0 8.0, 5.0 8.0, 5.0 6.0))");
  TDRowValT valTypeArray2[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  // two columns input
  SScalarParam *pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, valTypeArray1, rowNum, pInput);  //pInput come from GeomFromText()
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResults[0]);

  // swap two columns
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput);
  callGeomFromTextWrapper5(strArray1, valTypeArray1, rowNum, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResults[1]);

  // constant and column input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, valTypeArray1, 1, pInput);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResults[2]);

  // column and constant input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, valTypeArray1, rowNum, pInput);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, 1, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResults[3]);

  // two constants input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, valTypeArray1, 1, pInput);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, 1, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, 1, TSDB_CODE_SUCCESS, expectedResults[4]);

  // two columns with NULL value input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  valTypeArray1[2] = TD_VTYPE_NULL;
  valTypeArray2[4] = TD_VTYPE_NULL;
  callGeomFromTextWrapper5(strArray1, valTypeArray1, rowNum, pInput);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResults[5]);

  // first NULL type input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  setScalarParam(pInput, TSDB_DATA_TYPE_NULL, 0, 0, 1);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput + 1);
  int8_t expectedResultNullType[rowNum] = {-1, -1, -1, -1, -1, -1};
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResultNullType);

  // second NULL type input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  callGeomFromTextWrapper5(strArray1, valTypeArray1, rowNum, pInput);
  setScalarParam(pInput + 1, TSDB_DATA_TYPE_NULL, 0, 0, 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResultNullType);

  // first empty content input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  char strInput[TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strInput, "");
  setScalarParam(pInput, TSDB_DATA_TYPE_GEOMETRY, strInput, valTypeArray1, 1);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_SUCCESS, expectedResultNullType);

  // first wrong type input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  int32_t intInput = 3;
  setScalarParam(pInput, TSDB_DATA_TYPE_INT, &intInput, valTypeArray1, 1);
  callGeomFromTextWrapper5(strArray2, valTypeArray2, rowNum, pInput + 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_FUNC_FUNTION_PARA_VALUE, 0);

  // second wrong content input
  pInput = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  STR_TO_VARSTR(strInput, "XXX");
  callGeomFromTextWrapper5(strArray1, valTypeArray1, rowNum, pInput);
  setScalarParam(pInput + 1, TSDB_DATA_TYPE_GEOMETRY, strInput, valTypeArray2, 1);
  callGeomRelationFuncAndCompareResult(geomRelationFunc, pInput, rowNum, TSDB_CODE_FUNC_FUNTION_PARA_VALUE, 0);
}

TEST(GeomRelationFuncTest, intersectsFunction) {
  // 1: true, 0: false, -1: null
  int8_t expectedResults[6][6] = {
    {1, 0, 1, 1, 1, 1},   // two columns
    {1, 0, 1, 1, 1, 1},   // two columns swpped
    {1, 0, 1, 0, 1, 0},   // first constant
    {1, 0, 0, 0, 1, 1},   // second constant
    {1},                  // two constant
    {1, 0, -1, 1, -1, 1}  // with Null value
  };

  geomRelationFuncTest(intersectsFunction, expectedResults);
}

TEST(GeomRelationFuncTest, equalsFunction) {
  // 1: true, 0: false, -1: null
  int8_t expectedResults[6][6] = {
    {1, 0, 0, 0, 0, 0},   // two columns
    {1, 0, 0, 0, 0, 0},   // two columns swapped
    {1, 0, 0, 0, 0, 0},   // first constant
    {1, 0, 0, 0, 0, 0},   // second constant
    {1},                  // two constant
    {1, 0, -1, 0, -1, 0}  // with Null value
  };

  geomRelationFuncTest(equalsFunction, expectedResults);
}

TEST(GeomRelationFuncTest, touchesFunction) {
  // 1: true, 0: false, -1: null
  int8_t expectedResults[6][6] = {
    {0, 0, 1, 0, 0, 1},   // two columns
    {0, 0, 1, 0, 0, 1},   // two columns swapped
    {0, 0, 0, 0, 0, 0},   // first constant
    {0, 0, 0, 0, 0, 0},   // second constant
    {0},                  // two constant
    {0, 0, -1, 0, -1, 1}  // with Null value
  };

  geomRelationFuncTest(touchesFunction, expectedResults);
}

TEST(GeomRelationFuncTest, coversFunction) {
  // 1: true, 0: false, -1: null
  int8_t expectedResults[6][6] = {
    {1, 0, 0, 0, 0, 0},   // two columns
    {1, 0, 1, 0, 1, 0},   // two columns swapped
    {1, 0, 0, 0, 0, 0},   // first constant
    {1, 0, 0, 0, 1, 1},   // second constant
    {1},                  // two constant
    {1, 0, -1, 0, -1, 0}  // with Null value
  };

  geomRelationFuncTest(coversFunction, expectedResults);
}

TEST(GeomRelationFuncTest, containsFunction) {
  // 1: true, 0: false, -1: null
  int8_t expectedResults[6][6] = {
    {1, 0, 0, 0, 0, 0},   // two columns
    {1, 0, 0, 0, 1, 0},   // two columns swapped
    {1, 0, 0, 0, 0, 0},   // first constant
    {1, 0, 0, 0, 1, 1},   // second constant
    {1},                  // two constant
    {1, 0, -1, 0, -1, 0}  // with Null value
  };

  geomRelationFuncTest(containsFunction, expectedResults);
}

TEST(GeomRelationFuncTest, containsProperlyFunction) {
  // 1: true, 0: false, -1: null
  int8_t expectedResults[6][6] = {
    {1, 0, 0, 0, 0, 0},   // two columns
    {1, 0, 0, 0, 0, 0},   // two columns swapped
    {1, 0, 0, 0, 0, 0},   // first constant
    {1, 0, 0, 0, 1, 1},   // second constant
    {1},                  // two constant
    {1, 0, -1, 0, -1, 0}  // with Null value
  };

  geomRelationFuncTest(containsProperlyFunction, expectedResults);
}
