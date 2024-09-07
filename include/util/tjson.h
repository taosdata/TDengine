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

#ifndef _TD_UTIL_JSON_H_
#define _TD_UTIL_JSON_H_

#include "os.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

#define tjsonGetNumberValue(pJson, pName, val, code) \
  do {                                               \
    uint64_t _tmp = 0;                               \
    code = tjsonGetBigIntValue(pJson, pName, &_tmp); \
    val = _tmp;                                      \
  } while (0)

#define tjsonGetInt32ValueFromDouble(pJson, pName, val, code) \
  do {                                                        \
    double _tmp = 0;                                          \
    code = tjsonGetDoubleValue(pJson, pName, &_tmp);          \
    val = (int32_t)_tmp;                                      \
  } while (0)

#define tjsonGetInt8ValueFromDouble(pJson, pName, val, code) \
  do {                                                       \
    double _tmp = 0;                                         \
    code = tjsonGetDoubleValue(pJson, pName, &_tmp);         \
    val = (int8_t)_tmp;                                      \
  } while (0)

#define tjsonGetUInt16ValueFromDouble(pJson, pName, val, code) \
  do {                                                         \
    double _tmp = 0;                                           \
    code = tjsonGetDoubleValue(pJson, pName, &_tmp);           \
    val = (uint16_t)_tmp;                                      \
  } while (0)

typedef void SJson;

SJson* tjsonCreateObject();
SJson* tjsonCreateArray();
void   tjsonDelete(SJson* pJson);

SJson*  tjsonAddArrayToObject(SJson* pJson, const char* pName);
int32_t tjsonAddIntegerToObject(SJson* pJson, const char* pName, const uint64_t number);
int32_t tjsonAddDoubleToObject(SJson* pJson, const char* pName, const double number);
int32_t tjsonAddBoolToObject(SJson* pJson, const char* pName, const bool boolean);
int32_t tjsonAddStringToObject(SJson* pJson, const char* pName, const char* pVal);
int32_t tjsonAddItemToObject(SJson* pJson, const char* pName, SJson* pItem);
int32_t tjsonAddItemToArray(SJson* pJson, SJson* pItem);

SJson*  tjsonGetObjectItem(const SJson* pJson, const char* pName);
int32_t tjsonGetObjectName(const SJson* pJson, char** pName);
int32_t tjsonGetObjectValueString(const SJson* pJson, char** pStringValue);
int32_t tjsonGetStringValue(const SJson* pJson, const char* pName, char* pVal);
int32_t tjsonDupStringValue(const SJson* pJson, const char* pName, char** pVal);
int32_t tjsonGetBigIntValue(const SJson* pJson, const char* pName, int64_t* pVal);
int32_t tjsonGetIntValue(const SJson* pJson, const char* pName, int32_t* pVal);
int32_t tjsonGetSmallIntValue(const SJson* pJson, const char* pName, int16_t* pVal);
int32_t tjsonGetTinyIntValue(const SJson* pJson, const char* pName, int8_t* pVal);
int32_t tjsonGetUBigIntValue(const SJson* pJson, const char* pName, uint64_t* pVal);
int32_t tjsonGetUIntValue(const SJson* pJson, const char* pName, uint32_t* pVal);
int32_t tjsonGetUTinyIntValue(const SJson* pJson, const char* pName, uint8_t* pVal);
int32_t tjsonGetBoolValue(const SJson* pJson, const char* pName, bool* pVal);
int32_t tjsonGetDoubleValue(const SJson* pJson, const char* pName, double* pVal);

int32_t tjsonGetArraySize(const SJson* pJson);
SJson*  tjsonGetArrayItem(const SJson* pJson, int32_t index);

typedef int32_t (*FToJson)(const void* pObj, SJson* pJson);

int32_t tjsonAddObject(SJson* pJson, const char* pName, FToJson func, const void* pObj);
int32_t tjsonAddItem(SJson* pJson, FToJson func, const void* pObj);
int32_t tjsonAddArray(SJson* pJson, const char* pName, FToJson func, const void* pArray, int32_t itemSize, int32_t num);
int32_t tjsonAddTArray(SJson* pJson, const char* pName, FToJson func, const SArray* pArray);

typedef int32_t (*FToObject)(const SJson* pJson, void* pObj);

int32_t tjsonToObject(const SJson* pJson, const char* pName, FToObject func, void* pObj);
int32_t tjsonMakeObject(const SJson* pJson, const char* pName, FToObject func, void** pObj, int32_t objSize);
int32_t tjsonToArray(const SJson* pJson, const char* pName, FToObject func, void* pArray, int32_t itemSize);
int32_t tjsonToTArray(const SJson* pJson, const char* pName, FToObject func, SArray** pArray, int32_t itemSize);

char* tjsonToString(const SJson* pJson);
char* tjsonToUnformattedString(const SJson* pJson);

SJson*      tjsonParse(const char* pStr);
bool        tjsonValidateJson(const char* pJson);
const char* tjsonGetError();
void        tjsonDeleteItemFromObject(const SJson* pJson, const char* pName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_JSON_H_*/
