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

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

typedef void SJson;

SJson* tjsonCreateObject();
void tjsonDelete(SJson* pJson);

SJson* tjsonAddArrayToObject(SJson* pJson, const char* pName);

int32_t tjsonAddIntegerToObject(SJson* pJson, const char* pName, const uint64_t number);
int32_t tjsonAddDoubleToObject(SJson* pJson, const char* pName, const double number);
int32_t tjsonAddStringToObject(SJson* pJson, const char* pName, const char* pVal);
int32_t tjsonAddItemToObject(SJson* pJson, const char* pName, SJson* pItem);
int32_t tjsonAddItemToArray(SJson* pJson, SJson* pItem);

typedef int32_t (*FToJson)(const void* pObj, SJson* pJson);

int32_t tjsonAddObject(SJson* pJson, const char* pName, FToJson func, const void* pObj);
int32_t tjsonAddItem(SJson* pJson, FToJson func, const void* pObj);

typedef int32_t (*FFromJson)(const SJson* pJson, void* pObj);

char* tjsonToString(const SJson* pJson);
char* tjsonToUnformattedString(const SJson* pJson);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_JSON_H_*/
