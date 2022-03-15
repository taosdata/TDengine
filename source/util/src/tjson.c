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

#define _DEFAULT_SOURCE
#include "tjson.h"
#include "cJSON.h"
#include "taoserror.h"

SJson* tjsonCreateObject() {
  SJson* pJson = cJSON_CreateObject();
  if (pJson == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return pJson;
}

SJson* tjsonCreateArray() {
  SJson* pJson = cJSON_CreateArray();
  if (pJson == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return pJson;
}

void tjsonDelete(SJson* pJson) {
  if (pJson != NULL) {
    cJSON_Delete((cJSON*)pJson);
  }
}

int32_t tjsonAddIntegerToObject(SJson* pJson, const char* pName, const uint64_t number) {
  char tmp[40] = {0};
  snprintf(tmp, sizeof(tmp), "%" PRId64, number);
  return tjsonAddStringToObject(pJson, pName, tmp);
}

int32_t tjsonAddDoubleToObject(SJson* pJson, const char* pName, const double number) {
  if (NULL == cJSON_AddNumberToObject((cJSON*)pJson, pName, number)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tjsonAddBoolToObject(SJson* pJson, const char* pName, const bool boolean) {
  if (NULL == cJSON_AddBoolToObject((cJSON*)pJson, pName, boolean)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tjsonAddStringToObject(SJson* pJson, const char* pName, const char* pVal) {
  if (NULL == cJSON_AddStringToObject((cJSON*)pJson, pName, pVal)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

SJson* tjsonAddArrayToObject(SJson* pJson, const char* pName) {
  SJson* ret = (SJson*)cJSON_AddArrayToObject((cJSON*)pJson, pName);
  if (ret == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return ret;
}

int32_t tjsonAddItemToObject(SJson* pJson, const char* pName, SJson* pItem) {
  if (cJSON_AddItemToObject((cJSON*)pJson, pName, pItem)) {
    return TSDB_CODE_SUCCESS;
  }

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return TSDB_CODE_FAILED;
}

int32_t tjsonAddItemToArray(SJson* pJson, SJson* pItem) {
  if (cJSON_AddItemToArray((cJSON*)pJson, pItem)) {
    return TSDB_CODE_SUCCESS;
  }

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return TSDB_CODE_FAILED;
}

int32_t tjsonAddObject(SJson* pJson, const char* pName, FToJson func, const void* pObj) {
  if (NULL == pObj) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* pJobj = tjsonCreateObject();
  if (NULL == pJobj || TSDB_CODE_SUCCESS != func(pObj, pJobj)) {
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

int32_t tjsonAddArray(SJson* pJson, const char* pName, FToJson func, const void* pArray, int32_t itemSize, int32_t num) {
  if (num > 0) {
    SJson* pJsonArray = tjsonAddArrayToObject(pJson, pName);
    if (NULL == pJsonArray) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < num; ++i) {
      int32_t code = tjsonAddItem(pJsonArray, func, (const char*)pArray + itemSize * i);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

char* tjsonToString(const SJson* pJson) { return cJSON_Print((cJSON*)pJson); }

char* tjsonToUnformattedString(const SJson* pJson) { return cJSON_PrintUnformatted((cJSON*)pJson); }

SJson* tjsonGetObjectItem(const SJson* pJson, const char* pName) { return cJSON_GetObjectItem(pJson, pName); }

int32_t tjsonGetStringValue(const SJson* pJson, const char* pName, char* pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_FAILED;
  }
  strcpy(pVal, p);
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonDupStringValue(const SJson* pJson, const char* pName, char** pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_FAILED;
  }
  *pVal = strdup(p);
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetBigIntValue(const SJson* pJson, const char* pName, int64_t* pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_FAILED;
  }

  *pVal = strtol(p, NULL, 10);
  return (errno == EINVAL || errno == ERANGE) ? TSDB_CODE_FAILED:TSDB_CODE_SUCCESS;
}

int32_t tjsonGetIntValue(const SJson* pJson, const char* pName, int32_t* pVal) {
  int64_t val = 0;
  int32_t code = tjsonGetBigIntValue(pJson, pName, &val);
  *pVal = val;
  return code;
}

int32_t tjsonGetSmallIntValue(const SJson* pJson, const char* pName, int16_t* pVal) {
  int64_t val = 0;
  int32_t code = tjsonGetBigIntValue(pJson, pName, &val);
  *pVal = val;
  return code;
}

int32_t tjsonGetTinyIntValue(const SJson* pJson, const char* pName, int8_t* pVal) {
  int64_t val = 0;
  int32_t code = tjsonGetBigIntValue(pJson, pName, &val);
  *pVal = val;
  return code;
}

int32_t tjsonGetUBigIntValue(const SJson* pJson, const char* pName, uint64_t* pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_FAILED;
  }

  *pVal = strtoul(p, NULL, 10);
  return (errno == ERANGE||errno == EINVAL) ? TSDB_CODE_FAILED:TSDB_CODE_SUCCESS;
}

int32_t tjsonGetUTinyIntValue(const SJson* pJson, const char* pName, uint8_t* pVal) {
  uint64_t val = 0;
  int32_t  code = tjsonGetUBigIntValue(pJson, pName, &val);
  *pVal = val;
  return code;
}

int32_t tjsonGetBoolValue(const SJson* pJson, const char* pName, bool* pVal) {
  const SJson* pObject = tjsonGetObjectItem(pJson, pName);
  if (!cJSON_IsBool(pObject)) {
    return TSDB_CODE_FAILED;
  }
  *pVal = cJSON_IsTrue(pObject) ? true : false;
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetDoubleValue(const SJson* pJson, const char* pName, double* pVal) {
  const SJson* pObject = tjsonGetObjectItem(pJson, pName);
  if (!cJSON_IsNumber(pObject)) {
    return TSDB_CODE_FAILED;
  }
  *pVal = cJSON_GetNumberValue(pObject);
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetArraySize(const SJson* pJson) { return cJSON_GetArraySize(pJson); }

SJson* tjsonGetArrayItem(const SJson* pJson, int32_t index) { return cJSON_GetArrayItem(pJson, index); }

int32_t tjsonToObject(const SJson* pJson, const char* pName, FToObject func, void* pObj) {
  SJson* pJsonObj = tjsonGetObjectItem(pJson, pName);
  if (NULL == pJsonObj) {
    return TSDB_CODE_FAILED;
  }
  return func(pJsonObj, pObj);
}

int32_t tjsonToArray(const SJson* pJson, const char* pName, FToObject func, void* pArray, int32_t itemSize) {
  const cJSON* jArray = tjsonGetObjectItem(pJson, pName);
  int32_t size = (NULL == jArray ? 0 : tjsonGetArraySize(jArray));
  for (int32_t i = 0; i < size; ++i) {
    int32_t code = func(tjsonGetArrayItem(jArray, i), (char*)pArray + itemSize * i);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

SJson* tjsonParse(const char* pStr) { return cJSON_Parse(pStr); }
