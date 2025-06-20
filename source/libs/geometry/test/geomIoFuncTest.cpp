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

void callGeomFromText(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum,
                      SScalarParam **pInputGeomFromText, SScalarParam **pOutputGeomFromText,
                      int32_t expectedCode) {
  makeOneScalarParam(pInputGeomFromText, inputType, strArray, valTypeArray, rowNum);
  makeOneScalarParam(pOutputGeomFromText, TSDB_DATA_TYPE_GEOMETRY, 0, 0, rowNum);

  int32_t code = geomFromTextFunction(*pInputGeomFromText, 1, *pOutputGeomFromText);
  ASSERT_EQ(code, expectedCode);
}

void callGeomFromTextWrapper1(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam **pOutputGeomFromText) {
  SScalarParam *pInputGeomFromText;
  callGeomFromText(inputType, strArray, valTypeArray, rowNum, &pInputGeomFromText, pOutputGeomFromText, TSDB_CODE_SUCCESS);
  destroyScalarParam(pInputGeomFromText, 1);
}

void callGeomFromTextWrapper2(void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam **pOutputGeomFromText) {
  callGeomFromTextWrapper1(TSDB_DATA_TYPE_VARCHAR, strArray, valTypeArray, rowNum, pOutputGeomFromText);
}

void callGeomFromTextWrapper3(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, int32_t expectedCode) {
  SScalarParam *pInputGeomFromText;
  SScalarParam *pOutputGeomFromText;

  callGeomFromText(inputType, strArray, valTypeArray, rowNum, &pInputGeomFromText, &pOutputGeomFromText, expectedCode);

  destroyScalarParam(pInputGeomFromText, 1);
  destroyScalarParam(pOutputGeomFromText, 1);
}

void callGeomFromTextWrapper4(void *strArray, TDRowValT valTypeArray[], int32_t rowNum, int32_t expectedCode) {
  callGeomFromTextWrapper3(TSDB_DATA_TYPE_VARCHAR, strArray, valTypeArray, rowNum, expectedCode);
}

void callGeomFromTextWrapper5(void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam *pOutputGeomFromText) {
  SScalarParam *pInputGeomFromText;
  makeOneScalarParam(&pInputGeomFromText, TSDB_DATA_TYPE_VARCHAR, strArray, valTypeArray, rowNum);

  setScalarParam(pOutputGeomFromText, TSDB_DATA_TYPE_GEOMETRY, 0, 0, rowNum);

  int32_t code = geomFromTextFunction(pInputGeomFromText, 1, pOutputGeomFromText);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  destroyScalarParam(pInputGeomFromText, 1);
}

void callAsText(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum,
                SScalarParam **pInputAsText, SScalarParam **pOutputAsText,
                int32_t expectedCode) {
  makeOneScalarParam(pInputAsText, inputType, strArray, valTypeArray, rowNum);
  makeOneScalarParam(pOutputAsText, TSDB_DATA_TYPE_VARCHAR, 0, 0, rowNum);

  int32_t code = geomFromTextFunction(*pInputAsText, 1, *pOutputAsText);
  ASSERT_EQ(code, expectedCode);
}

void callAsTextWrapper1(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam **pOutputAsText) {
  SScalarParam *pInputAsText;
  callAsText(inputType, strArray, valTypeArray, rowNum, &pInputAsText, pOutputAsText, TSDB_CODE_SUCCESS);
  destroyScalarParam(pInputAsText, 1);
}

void callAsTextWrapper2(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, int32_t expectedCode) {
  SScalarParam *pInputAsText;
  SScalarParam *pOutputAsText;

  callAsText(inputType, strArray, valTypeArray, rowNum, &pInputAsText, &pOutputAsText, expectedCode);

  destroyScalarParam(pInputAsText, 1);
  destroyScalarParam(pOutputAsText, 1);
}

void callGeomFromGeoJSON(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum,
                      SScalarParam **pInputGeomFromGeoJSON, SScalarParam **pOutputGeomFromGeoJSON,
                      int32_t expectedCode) {
  makeOneScalarParam(pInputGeomFromGeoJSON, inputType, strArray, valTypeArray, rowNum);
  makeOneScalarParam(pOutputGeomFromGeoJSON, TSDB_DATA_TYPE_GEOMETRY, 0, 0, rowNum);

  int32_t code = geomFromGeoJSONFunction(*pInputGeomFromGeoJSON, 1, *pOutputGeomFromGeoJSON);
  ASSERT_EQ(code, expectedCode);
}

void callGeomFromGeoJSONWrapper1(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam **pOutputGeomFromGeoJSON) {
  SScalarParam *pInputGeomFromGeoJSON;
  callGeomFromGeoJSON(inputType, strArray, valTypeArray, rowNum, &pInputGeomFromGeoJSON, pOutputGeomFromGeoJSON, TSDB_CODE_SUCCESS);
  destroyScalarParam(pInputGeomFromGeoJSON, 1);
}

void callGeomFromGeoJSONWrapper2(void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam **pOutputGeomFromGeoJSON) {
  callGeomFromGeoJSONWrapper1(TSDB_DATA_TYPE_VARCHAR, strArray, valTypeArray, rowNum, pOutputGeomFromGeoJSON);
}

void callGeomFromGeoJSONWrapper3(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, int32_t expectedCode) {
  SScalarParam *pInputGeomFromGeoJSON;
  SScalarParam *pOutputGeomFromGeoJSON;

  callGeomFromGeoJSON(inputType, strArray, valTypeArray, rowNum, &pInputGeomFromGeoJSON, &pOutputGeomFromGeoJSON, expectedCode);

  destroyScalarParam(pInputGeomFromGeoJSON, 1);
  destroyScalarParam(pOutputGeomFromGeoJSON, 1);
}

void callGeomFromGeoJSONWrapper4(void *strArray, TDRowValT valTypeArray[], int32_t rowNum, int32_t expectedCode) {
  callGeomFromGeoJSONWrapper3(TSDB_DATA_TYPE_VARCHAR, strArray, valTypeArray, rowNum, expectedCode);
}

void callAsGeoJSON(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum,
                   SScalarParam **pInputAsGeoJSON, SScalarParam **pOutputAsGeoJSON,
                   int32_t expectedCode) {
  makeOneScalarParam(pInputAsGeoJSON, inputType, strArray, valTypeArray, rowNum);
  makeOneScalarParam(pOutputAsGeoJSON, TSDB_DATA_TYPE_VARCHAR, 0, 0, rowNum);

  int32_t code = geomFromGeoJSONFunction(*pInputAsGeoJSON, 1, *pOutputAsGeoJSON);
  ASSERT_EQ(code, expectedCode);
}

void callAsGeoJSONWrapper1(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam **pOutputAsGeoJSON) {
  SScalarParam *pInputAsGeoJSON;
  callAsGeoJSON(inputType, strArray, valTypeArray, rowNum, &pInputAsGeoJSON, pOutputAsGeoJSON, TSDB_CODE_SUCCESS);
  destroyScalarParam(pInputAsGeoJSON, 1);
}

void callAsGeoJSONWrapper2(int32_t inputType, void *strArray, TDRowValT valTypeArray[], int32_t rowNum, int32_t expectedCode) {
  SScalarParam *pInputAsGeoJSON;
  SScalarParam *pOutputAsGeoJSON;

  callAsGeoJSON(inputType, strArray, valTypeArray, rowNum, &pInputAsGeoJSON, &pOutputAsGeoJSON, expectedCode);

  destroyScalarParam(pInputAsGeoJSON, 1);
  destroyScalarParam(pOutputAsGeoJSON, 1);
}

void callMakePointAndCompareResult(int32_t type1, void *valueArray1, TDRowValT valTypeArray1[], bool isConstant1,
                                   int32_t type2, void *valueArray2, TDRowValT valTypeArray2[], bool isConstant2,
                                   SScalarParam *pExpectedResult, int32_t rowNum) {
  int32_t rowNum1 = isConstant1 ? 1 : rowNum;
  int32_t rowNum2 = isConstant2 ? 1 : rowNum;

  SScalarParam *pInputMakePoint = (SScalarParam *)taosMemoryCalloc(2, sizeof(SScalarParam));
  setScalarParam(pInputMakePoint, type1, valueArray1, valTypeArray1, rowNum1);
  setScalarParam(pInputMakePoint + 1, type2, valueArray2, valTypeArray2, rowNum2);

  SScalarParam *pOutputMakePoint;
  makeOneScalarParam(&pOutputMakePoint, TSDB_DATA_TYPE_GEOMETRY, 0, 0, rowNum);

  int32_t code = makePointFunction(pInputMakePoint, 2, pOutputMakePoint);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  ASSERT_EQ(pOutputMakePoint->columnData->info.type, TSDB_DATA_TYPE_GEOMETRY);
  ASSERT_EQ(pExpectedResult->columnData->info.type, TSDB_DATA_TYPE_GEOMETRY);

  compareVarDataColumn(pOutputMakePoint->columnData, pExpectedResult->columnData, rowNum);

  destroyScalarParam(pInputMakePoint, 2);
  destroyScalarParam(pOutputMakePoint, 1);
}

#define MAKE_POINT_FIRST_COLUMN_VALUES {2, 3, -4}
#define MAKE_POINT_SECOND_COLUMN_VALUES {5, -6, -7}

void geomIoFuncTestMakePointFunctionTwoColumns() {
  const int32_t rowNum = 3;
  SScalarParam *pExpectedResult;
  TDRowValT valTypeArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  // call GeomFromText(<POINT>) and generate pExpectedResult to compare later
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray[0], "POINT(2.0 5.0)");
  STR_TO_VARSTR(strArray[1], "POINT(3.0 -6.0)");
  STR_TO_VARSTR(strArray[2], "POINT(-4.0 -7.0)");
  callGeomFromTextWrapper2(strArray, valTypeArray, rowNum, &pExpectedResult);

  // call MakePoint() with TINYINT and SMALLINT, and compare with result of GeomFromText(<POINT>)
  int8_t tinyIntArray1[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  int16_t smallIntArray2[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_TINYINT, tinyIntArray1, valTypeArray, false,
                                TSDB_DATA_TYPE_SMALLINT, smallIntArray2, valTypeArray, false,
                                pExpectedResult, rowNum);

  // call MakePoint() with INT and BIGINT, and compare with result of GeomFromText(<POINT>)
  int32_t intArray1[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  int64_t bigIntArray2[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_INT, intArray1, valTypeArray, false,
                                TSDB_DATA_TYPE_BIGINT, bigIntArray2, valTypeArray, false,
                                pExpectedResult, rowNum);

  // call MakePoint() with FLOAT and DOUBLE, and compare with result of GeomFromText(<POINT>)
  float floatArray1[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  double doubleArray2[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_FLOAT, floatArray1, valTypeArray, false,
                                TSDB_DATA_TYPE_DOUBLE, doubleArray2, valTypeArray, false,
                                pExpectedResult, rowNum);

  destroyScalarParam(pExpectedResult, 1);
}

void geomIoFuncTestMakePointFunctionConstant() {
  const int32_t rowNum = 3;
  SScalarParam *pExpectedResult;
  TDRowValT valTypeArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  // 1. call GeomFromText(<POINT>) and generate pExpectedResult with first constant
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strArray[0], "POINT(3.0 5.0)");
  STR_TO_VARSTR(strArray[1], "POINT(3.0 -6.0)");
  STR_TO_VARSTR(strArray[2], "POINT(3.0 -7.0)");
  callGeomFromTextWrapper2(strArray, valTypeArray, rowNum, &pExpectedResult);

  // call MakePoint() with TINYINT constant and BIGINT column, and compare with result of GeomFromText(<POINT>)
  int8_t tinyIntConstant = 3;
  int64_t bigIntArray[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_TINYINT, &tinyIntConstant, valTypeArray, true,
                                TSDB_DATA_TYPE_BIGINT, bigIntArray, valTypeArray, false,
                                pExpectedResult, rowNum);

  destroyScalarParam(pExpectedResult, 1);

  // 2. call GeomFromText(<POINT>) and generate pExpectedResult with second constant
  STR_TO_VARSTR(strArray[0], "POINT(2.0 3.0)");
  STR_TO_VARSTR(strArray[1], "POINT(3.0 3.0)");
  STR_TO_VARSTR(strArray[2], "POINT(-4.0 3.0)");
  callGeomFromTextWrapper2(strArray, valTypeArray, rowNum, &pExpectedResult);

  // call MakePoint() with INT column and FLOAT constant, and compare with result of GeomFromText(<POINT>)
  int32_t intArray[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  float floatConstant = 3;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_INT, intArray, valTypeArray, false,
                                TSDB_DATA_TYPE_FLOAT, &floatConstant, valTypeArray, true,
                                pExpectedResult, rowNum);

  destroyScalarParam(pExpectedResult, 1);
}

void geomIoFuncTestMakePointFunctionWithNull() {
  const int32_t rowNum = 3;
  SScalarParam *pExpectedResult;
  TDRowValT valTypeNormArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  // call GeomFromText(<POINT>) and generate pExpectedResult with all NULL values
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  TDRowValT valTypeNullArray[rowNum] = {TD_VTYPE_NULL, TD_VTYPE_NULL, TD_VTYPE_NULL};
  callGeomFromTextWrapper2(strArray, valTypeNullArray, rowNum, &pExpectedResult);

  // 1. call MakePoint() with NULL type and INT column, and compare all NULL results
  int64_t intArray[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_NULL, 0, 0, true,
                                TSDB_DATA_TYPE_INT, intArray, valTypeNormArray, false,
                                pExpectedResult, rowNum);
  // swap params and compare
  callMakePointAndCompareResult(TSDB_DATA_TYPE_INT, intArray, valTypeNormArray, false,
                                TSDB_DATA_TYPE_NULL, 0, 0, true,
                                pExpectedResult, rowNum);

  // call MakePoint() with SMALLINT NULL constant and BIGINT column, and compare all NULL results
  int16_t smallIntConstant = 0;
  int64_t bigIntArray[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_SMALLINT, &smallIntConstant, valTypeNullArray, true,
                                TSDB_DATA_TYPE_BIGINT, bigIntArray, valTypeNormArray, false,
                                pExpectedResult, rowNum);
  // swap params and compare
  callMakePointAndCompareResult(TSDB_DATA_TYPE_BIGINT, bigIntArray, valTypeNormArray, false,
                                TSDB_DATA_TYPE_SMALLINT, &smallIntConstant, valTypeNullArray, true,
                                pExpectedResult, rowNum);

  destroyScalarParam(pExpectedResult, 1);

  // 2. call GeomFromText(<POINT>) and generate pExpectedResult with NULL value
  STR_TO_VARSTR(strArray[0], "POINT(2.0 5.0)");
  STR_TO_VARSTR(strArray[2], "POINT(-4.0 -7.0)");
  TDRowValT valTypeWithNullArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NULL, TD_VTYPE_NORM};
  callGeomFromTextWrapper2(strArray, valTypeWithNullArray, rowNum, &pExpectedResult);

  // call MakePoint() with TINYINT column with NULL value and FLOAT column, and compare results with NULL value
  int8_t tinyIntArray[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  float floatArray[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_TINYINT, tinyIntArray, valTypeWithNullArray, false,
                                TSDB_DATA_TYPE_FLOAT, floatArray, valTypeNormArray, false,
                                pExpectedResult, rowNum);

  // call MakePoint() with SMALLINT column and DOUBLE column with NULL value, and compare results with NULL value
  int16_t smallIntArray[rowNum] = MAKE_POINT_FIRST_COLUMN_VALUES;
  double doubleArray[rowNum] = MAKE_POINT_SECOND_COLUMN_VALUES;
  callMakePointAndCompareResult(TSDB_DATA_TYPE_SMALLINT, smallIntArray, valTypeNormArray, false,
                                TSDB_DATA_TYPE_DOUBLE, doubleArray, valTypeWithNullArray, false,
                                pExpectedResult, rowNum);

  destroyScalarParam(pExpectedResult, 1);
}

void geomIoFuncTestGeomFromTextFunction() {
  const int32_t rowNum = 4;
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  TDRowValT valTypeNormArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  // column input
  // input of GeomFromText (with NULL value) and output of AsText should be same after calling GeomFromText() and AsText()
  SScalarParam *pInputGeomFromText;
  SScalarParam *pOutputGeomFromText;
  SScalarParam *pOutputAsText;

  STR_TO_VARSTR(strArray[0], "POINT (2.000000 5.000000)");
  STR_TO_VARSTR(strArray[2], "LINESTRING (3.000000 -6.000000, -71.160837 42.259113)");
  STR_TO_VARSTR(strArray[3], "POLYGON ((-71.177658 42.390290, -71.177682 42.390370, -71.177606 42.390382, -71.177582 42.390303, -71.177658 42.390290))");
  TDRowValT valTypeWithNullArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NULL, TD_VTYPE_NORM, TD_VTYPE_NORM};
  callGeomFromText(TSDB_DATA_TYPE_VARCHAR, strArray, valTypeWithNullArray, rowNum, &pInputGeomFromText, &pOutputGeomFromText, TSDB_CODE_SUCCESS);

  makeOneScalarParam(&pOutputAsText, TSDB_DATA_TYPE_VARCHAR, 0, 0, rowNum);
  int32_t code = asTextFunction(pOutputGeomFromText, 1, pOutputAsText);   // pOutputGeomFromText is input for AsText()
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  compareVarDataColumn(pInputGeomFromText->columnData, pOutputAsText->columnData, rowNum);

  destroyScalarParam(pInputGeomFromText, 1);
  destroyScalarParam(pOutputGeomFromText, 1);
  destroyScalarParam(pOutputAsText, 1);

  // empty input
  STR_TO_VARSTR(strArray[0], "");
  callGeomFromTextWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_SUCCESS);

  // NULL type input
  callGeomFromTextWrapper1(TSDB_DATA_TYPE_NULL, 0, 0, 1, &pOutputGeomFromText);
  ASSERT_EQ(colDataIsNull_s(pOutputGeomFromText->columnData, 0), true);
  destroyScalarParam(pOutputGeomFromText, 1);

  // wrong type input [ToDo] make sure it is handled in geomFunc
  int32_t intInput = 3;
  callGeomFromTextWrapper3(TSDB_DATA_TYPE_INT, &intInput, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);

  // wrong content input
  STR_TO_VARSTR(strArray[0], "POIN(2 5)"); // lack of the last letter of 'POINT'
  callGeomFromTextWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
  STR_TO_VARSTR(strArray[0], "LINESTRING(3 -6.1,-7.1 4.2,)"); // redundant comma at the end
  callGeomFromTextWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
  STR_TO_VARSTR(strArray[0], "POLYGON((-71.1 42.3,-71.2 42.4,-71.3 42.5,-71.1 42.8))"); // the first point and last one are not same
  callGeomFromTextWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
}

void geomIoFuncTestAsTextFunction() {
  // column input has been tested in geomFromTextFunction

  TDRowValT valTypeArray[1] = {TD_VTYPE_NORM};

  // empty input
  char strInput[TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strInput, "");
  SScalarParam *pOutputAsText;
  callAsTextWrapper1(TSDB_DATA_TYPE_GEOMETRY, strInput, valTypeArray, 1, &pOutputAsText);
  ASSERT_EQ(colDataIsNull_s(pOutputAsText->columnData, 0), true);
  destroyScalarParam(pOutputAsText, 1);

  // NULL type input
  callAsTextWrapper1(TSDB_DATA_TYPE_NULL, 0, 0, 1, &pOutputAsText);
  ASSERT_EQ(colDataIsNull_s(pOutputAsText->columnData, 0), true);
  destroyScalarParam(pOutputAsText, 1);

  // wrong type input  [ToDo] make sure it is handled in geomFunc
  int32_t intInput = 3;
  callAsTextWrapper2(TSDB_DATA_TYPE_INT, &intInput, valTypeArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);

  // wrong content input
  STR_TO_VARSTR(strInput, "XXX");
  callAsTextWrapper2(TSDB_DATA_TYPE_GEOMETRY, strInput, valTypeArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
}

void geomIoFuncTestGeomFromGeoJSONFunction() {
  const int32_t rowNum = 4;
  char strArray[rowNum][TSDB_MAX_BINARY_LEN];
  TDRowValT valTypeNormArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM, TD_VTYPE_NORM};

  // column input
  // input of GeomFromGeoJSON (with NULL value) and output of AsGeoJSON should be same after calling GeomFromGeoJSON() and AsGeoJSON()
  SScalarParam *pInputGeomFromGeoJSON;
  SScalarParam *pOutputGeomFromGeoJSON;
  SScalarParam *pOutputAsGeoJSON;

  // Test valid GeoJSON inputs
  STR_TO_VARSTR(strArray[0], "{\"type\":\"Point\",\"coordinates\":[2.0,5.0]}");
  STR_TO_VARSTR(strArray[2], "{\"type\":\"LineString\",\"coordinates\":[[3.0,-6.0],[-71.160837,42.259113]]}");
  STR_TO_VARSTR(strArray[3], "{\"type\":\"Polygon\",\"coordinates\":[[[-71.177658,42.39029],[-71.177682,42.39037],[-71.177606,42.390382],[-71.177582,42.390303],[-71.177658,42.39029]]]}");
  TDRowValT valTypeWithNullArray[rowNum] = {TD_VTYPE_NORM, TD_VTYPE_NULL, TD_VTYPE_NORM, TD_VTYPE_NORM};
  callGeomFromGeoJSON(TSDB_DATA_TYPE_VARCHAR, strArray, valTypeWithNullArray, rowNum, &pInputGeomFromGeoJSON, &pOutputGeomFromGeoJSON, TSDB_CODE_SUCCESS);

  makeOneScalarParam(&pOutputAsGeoJSON, TSDB_DATA_TYPE_VARCHAR, 0, 0, rowNum);
  int32_t code = asGeoJSONFunction(pOutputGeomFromGeoJSON, 1, pOutputAsGeoJSON);   // pOutputGeomFromGeoJSON is input for AsText()
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  compareVarDataColumn(pInputGeomFromGeoJSON->columnData, pOutputAsGeoJSON->columnData, rowNum);

  destroyScalarParam(pInputGeomFromGeoJSON, 1);
  destroyScalarParam(pOutputGeomFromGeoJSON, 1);
  destroyScalarParam(pOutputAsGeoJSON, 1);

  // empty input
  STR_TO_VARSTR(strArray[0], "");
  callGeomFromGeoJSONWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_SUCCESS);

  // NULL type input
  callGeomFromGeoJSONWrapper1(TSDB_DATA_TYPE_NULL, 0, 0, 1, &pOutputGeomFromGeoJSON);
  ASSERT_EQ(colDataIsNull_s(pOutputGeomFromGeoJSON->columnData, 0), true);
  destroyScalarParam(pOutputGeomFromGeoJSON, 1);

  // wrong type input
  int32_t intInput = 3;
  callGeomFromGeoJSONWrapper3(TSDB_DATA_TYPE_INT, &intInput, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);

  // invalid GeoJSON formats
  STR_TO_VARSTR(strArray[0], "{\"type\":\"Point\",\"coordinates\":[2.0]}"); // Missing y coordinate
  callGeomFromGeoJSONWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
  
  STR_TO_VARSTR(strArray[0], "{\"type\":\"LineString\",\"coordinates\":[[3.0,-6.0]]}"); // Only one point
  callGeomFromGeoJSONWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
  
  STR_TO_VARSTR(strArray[0], "{\"type\":\"Polygon\",\"coordinates\":[[[-71.1,42.3],[-71.2,42.4],[-71.3,42.5],[-71.1,42.8]]]}"); // Not closed
  callGeomFromGeoJSONWrapper4(strArray, valTypeNormArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
}

void geomIoFuncTestAsGeoJSONFunction() {
  // column input has been tested in geomFromGeoJSONFunction

  TDRowValT valTypeArray[1] = {TD_VTYPE_NORM};

  // empty input
  char strInput[TSDB_MAX_BINARY_LEN];
  STR_TO_VARSTR(strInput, "");
  SScalarParam *pOutputGeoJSON;
  callAsGeoJSONWrapper1(TSDB_DATA_TYPE_GEOMETRY, strInput, valTypeArray, 1, &pOutputGeoJSON);
  ASSERT_EQ(colDataIsNull_s(pOutputGeoJSON->columnData, 0), true);
  destroyScalarParam(pOutputGeoJSON, 1);

  // NULL type input
  callAsGeoJSONWrapper1(TSDB_DATA_TYPE_NULL, 0, 0, 1, &pOutputGeoJSON);
  ASSERT_EQ(colDataIsNull_s(pOutputGeoJSON->columnData, 0), true);
  destroyScalarParam(pOutputGeoJSON, 1);

  // wrong type input  [ToDo] make sure it is handled in geomFunc
  int32_t intInput = 3;
  callAsGeoJSONWrapper2(TSDB_DATA_TYPE_INT, &intInput, valTypeArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);

  // wrong content input
  STR_TO_VARSTR(strInput, "XXX");
  callAsGeoJSONWrapper2(TSDB_DATA_TYPE_GEOMETRY, strInput, valTypeArray, 1, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
}

static void geomIoFuncTestImpl() {
  geomIoFuncTestMakePointFunctionTwoColumns();
  geomIoFuncTestMakePointFunctionConstant();
  geomIoFuncTestMakePointFunctionWithNull();
  geomIoFuncTestGeomFromTextFunction();
  geomIoFuncTestAsTextFunction();
  geomIoFuncTestGeomFromGeoJSONFunction();
  geomIoFuncTestAsGeoJSONFunction();
}

static void *geomIoFuncTestFunc(void *arg) {
  geomIoFuncTestImpl();
  return nullptr;
}

static void geomIoFuncTestInThread() {
  TdThread thread;
  ASSERT_EQ(taosThreadCreate(&thread, nullptr, geomIoFuncTestFunc, NULL), 0);
  ASSERT_EQ(taosThreadJoin(thread, nullptr), 0);
}

TEST(threadGeomFuncTest, threadFuncTest) {
  geomIoFuncTestImpl();
  geomIoFuncTestInThread();
}