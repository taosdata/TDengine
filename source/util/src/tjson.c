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

#include "tjson.h"

#include "taoserror.h"
#include "cJSON.h"

SJson* tjsonCreateObject() {
  return cJSON_CreateObject();
}

void tjsonDelete(SJson* pJson) {
  cJSON_Delete((cJSON*)pJson);
}

int32_t tjsonAddIntegerToObject(SJson* pJson, const char* pName, const uint64_t number) {
  char tmp[40] = {0};
  snprintf(tmp, tListLen(tmp), "%"PRId64, number);
  return tjsonAddStringToObject(pJson, pName, tmp);
}

int32_t tjsonAddDoubleToObject(SJson* pJson, const char* pName, const double number) {
  return (NULL == cJSON_AddNumberToObject((cJSON*)pJson, pName, number) ? TSDB_CODE_FAILED : TSDB_CODE_SUCCESS);
}

int32_t tjsonAddStringToObject(SJson* pJson, const char* pName, const char* pVal) {
  return (NULL == cJSON_AddStringToObject((cJSON*)pJson, pName, pVal) ? TSDB_CODE_FAILED : TSDB_CODE_SUCCESS);
}

SJson* tjsonAddArrayToObject(SJson* pJson, const char* pName) {
  return cJSON_AddArrayToObject((cJSON*)pJson, pName);
}

int32_t tjsonAddItemToObject(SJson *pJson, const char* pName, SJson* pItem) {
  return (cJSON_AddItemToObject((cJSON*)pJson, pName, pItem) ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED);
}

int32_t tjsonAddItemToArray(SJson* pJson, SJson* pItem) {
  return (cJSON_AddItemToArray((cJSON*)pJson, pItem) ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED);
}

int32_t tjsonAddObject(SJson* pJson, const char* pName, FToJson func, const void* pObj) {
  if (NULL == pObj) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* pJobj = tjsonCreateObject();
  if (NULL == pJobj || TSDB_CODE_SUCCESS != func(pObj, pJobj)) {
    printf("%s:%d code = %d\n", __FUNCTION__, __LINE__, TSDB_CODE_FAILED);
    tjsonDelete(pJobj);
    return TSDB_CODE_FAILED;
  }
  return tjsonAddItemToObject(pJson, pName, pJobj);
}

int32_t tjsonAddItem(SJson* pJson, FToJson func, const void* pObj) {
  SJson* pJobj = tjsonCreateObject();
  if (NULL == pJobj || TSDB_CODE_SUCCESS != func(pObj, pJobj)) {
    tjsonDelete(pJobj);
    return TSDB_CODE_FAILED;
  }
  return tjsonAddItemToArray(pJson, pJobj);
}

char* tjsonToString(const SJson* pJson) {
  return cJSON_Print((cJSON*)pJson);
}

char* tjsonToUnformattedString(const SJson* pJson) {
  return cJSON_PrintUnformatted((cJSON*)pJson);
}