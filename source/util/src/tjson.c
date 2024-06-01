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
#include "taoserror.h"
#include "tref.h"

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

int32_t tjsonAddArray(SJson* pJson, const char* pName, FToJson func, const void* pArray, int32_t itemSize,
                      int32_t num) {
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

int32_t tjsonAddTArray(SJson* pJson, const char* pName, FToJson func, const SArray* pArray) {
  int32_t num = taosArrayGetSize(pArray);
  if (num > 0) {
    SJson* pJsonArray = tjsonAddArrayToObject(pJson, pName);
    if (NULL == pJsonArray) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (int32_t i = 0; i < num; ++i) {
      int32_t code = tjsonAddItem(pJsonArray, func, taosArrayGet(pArray, i));
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

int32_t tjsonGetObjectName(const SJson* pJson, char** pName) {
  *pName = ((cJSON*)pJson)->string;
  if (NULL == *pName) {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetObjectValueString(const SJson* pJson, char** pValueString) {
  *pValueString = ((cJSON*)pJson)->valuestring;
  if (NULL == *pValueString) {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetStringValue(const SJson* pJson, const char* pName, char* pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_SUCCESS;
  }
  strcpy(pVal, p);
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonDupStringValue(const SJson* pJson, const char* pName, char** pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_SUCCESS;
  }
  *pVal = taosStrdup(p);
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetBigIntValue(const SJson* pJson, const char* pName, int64_t* pVal) {
  char* p = cJSON_GetStringValue(tjsonGetObjectItem((cJSON*)pJson, pName));
  if (NULL == p) {
    return TSDB_CODE_SUCCESS;
  }
#ifdef WINDOWS
  sscanf(p, "%" PRId64, pVal);
#else
  *pVal = taosStr2Int64(p, NULL, 10);
#endif
  return TSDB_CODE_SUCCESS;
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
    return TSDB_CODE_SUCCESS;
  }
#ifdef WINDOWS
  sscanf(p, "%" PRIu64, pVal);
#else
  *pVal = taosStr2UInt64(p, NULL, 10);
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetUIntValue(const SJson* pJson, const char* pName, uint32_t* pVal) {
  uint64_t val = 0;
  int32_t  code = tjsonGetUBigIntValue(pJson, pName, &val);
  *pVal = val;
  return code;
}

int32_t tjsonGetUTinyIntValue(const SJson* pJson, const char* pName, uint8_t* pVal) {
  uint64_t val = 0;
  int32_t  code = tjsonGetUBigIntValue(pJson, pName, &val);
  *pVal = val;
  return code;
}

int32_t tjsonGetBoolValue(const SJson* pJson, const char* pName, bool* pVal) {
  const SJson* pObject = tjsonGetObjectItem(pJson, pName);
  if (NULL == pObject) {
    return TSDB_CODE_SUCCESS;
  }
  if (!cJSON_IsBool(pObject)) {
    return TSDB_CODE_FAILED;
  }
  *pVal = cJSON_IsTrue(pObject) ? true : false;
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetDoubleValue(const SJson* pJson, const char* pName, double* pVal) {
  const SJson* pObject = tjsonGetObjectItem(pJson, pName);
  if (NULL == pObject) {
    return TSDB_CODE_SUCCESS;
  }
  if (!cJSON_IsNumber(pObject)) {
    return TSDB_CODE_FAILED;
  }
  *pVal = cJSON_GetNumberValue(pObject);
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonGetArraySize(const SJson* pJson) { return cJSON_GetArraySize(pJson); }

SJson* tjsonGetArrayItem(const SJson* pJson, int32_t index) { return cJSON_GetArrayItem(pJson, index); }

int32_t tjsonToObject(const SJson* pJson, const char* pName, FToObjectJson func, void* pObj) {
  SJson* pJsonObj = tjsonGetObjectItem(pJson, pName);
  if (NULL == pJsonObj) {
    return TSDB_CODE_SUCCESS;
  }
  return func(pJsonObj, pObj);
}

int32_t tjsonMakeObject(const SJson* pJson, const char* pName, FToObjectJson func, void** pObj, int32_t objSize) {
  if (objSize <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* pJsonObj = tjsonGetObjectItem(pJson, pName);
  if (NULL == pJsonObj) {
    return TSDB_CODE_SUCCESS;
  }
  *pObj = taosMemoryCalloc(1, objSize);
  if (NULL == *pObj) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return func(pJsonObj, *pObj);
}

int32_t tjsonToArray(const SJson* pJson, const char* pName, FToObjectJson func, void* pArray, int32_t itemSize) {
  const cJSON* jArray = tjsonGetObjectItem(pJson, pName);
  int32_t      size = (NULL == jArray ? 0 : tjsonGetArraySize(jArray));
  for (int32_t i = 0; i < size; ++i) {
    int32_t code = func(tjsonGetArrayItem(jArray, i), (char*)pArray + itemSize * i);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tjsonToTArray(const SJson* pJson, const char* pName, FToObjectJson func, SArray** pArray, int32_t itemSize) {
  const cJSON* jArray = tjsonGetObjectItem(pJson, pName);
  int32_t      size = tjsonGetArraySize(jArray);
  if (size > 0) {
    *pArray = taosArrayInit_s(itemSize, size);
    if (NULL == *pArray) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (int32_t i = 0; i < size; ++i) {
      int32_t code = func(tjsonGetArrayItem(jArray, i), taosArrayGet(*pArray, i));
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

SJson* tjsonParse(const char* pStr) { return cJSON_Parse(pStr); }

bool tjsonValidateJson(const char* jIn) {
  if (!jIn) {
    return false;
  }

  // set json real data
  cJSON* root = cJSON_Parse(jIn);
  if (root == NULL) {
    return false;
  }

  if (!cJSON_IsObject(root)) {
    return false;
  }
  int size = cJSON_GetArraySize(root);
  for (int i = 0; i < size; i++) {
    cJSON* item = cJSON_GetArrayItem(root, i);
    if (!item) {
      return false;
    }

    char* jsonKey = item->string;
    if (!jsonKey) return false;
    for (size_t j = 0; j < strlen(jsonKey); ++j) {
      if (isprint(jsonKey[j]) == 0) return false;
    }

    if (item->type == cJSON_Object || item->type == cJSON_Array) {
      return false;
    }
  }
  return true;
}

const char* tjsonGetError() { return cJSON_GetErrorPtr(); }

void tjsonDeleteItemFromObject(const SJson* pJson, const char* pName) {
  cJSON_DeleteItemFromObject((cJSON*)pJson, pName);
}

static bool isInteger(double value){
  return value == (int64_t)value;
}

cJSON* transformValue2Array(cJSON* data){
  cJSON* arrayDst = cJSON_CreateArray();

  switch ((data->type) & 0xFF)
  {
    case cJSON_NULL:{
      ADD_TYPE_2_ARRAY(arrayDst, "null");
      break;
    }

    case cJSON_False:
    case cJSON_True:{
      ADD_TYPE_2_ARRAY(arrayDst, "bool");
      break;
    }

    case cJSON_Number: {
      if (isInteger(data->valuedouble)) {
        ADD_TYPE_2_ARRAY(arrayDst, "bigint");
      } else {
        ADD_TYPE_2_ARRAY(arrayDst, "double");
      }
      break;
    }

    case cJSON_Raw:
    case cJSON_String:
    {
      ADD_TYPE_2_ARRAY(arrayDst, "string");
      break;
    }

    case cJSON_Array:{
      cJSON* current_item = data->child;
      if (current_item){
        cJSON* arr = transformValue2Array(current_item);
        cJSON_AddItemToArray(arrayDst, arr);
      }
      break;
    }

    case cJSON_Object:{
      cJSON* object = transformJson2JsonTemplate(data);
      cJSON_AddItemToArray(arrayDst, object);
      break;
    }

    default:
      break;
  }
  return arrayDst;
}

cJSON* transformJson2JsonTemplate(cJSON* objectSrc){
  cJSON* object = cJSON_CreateObject();
  cJSON *data = objectSrc->child;
  while (data){
    switch ((data->type) & 0xFF)
    {
      case cJSON_NULL:{
        ADD_TYPE_2_OBJECT(object, data->string, "null");
        break;
      }

      case cJSON_False:
      case cJSON_True:{
        ADD_TYPE_2_OBJECT(object, data->string, "boolean");
        break;
      }

      case cJSON_Number: {
        if (isInteger(data->valuedouble)) {
          ADD_TYPE_2_OBJECT(object, data->string, "long");
        } else {
          ADD_TYPE_2_OBJECT(object, data->string, "double");
        }
        break;
      }

      case cJSON_Raw:
      case cJSON_String:
      {
        ADD_TYPE_2_OBJECT(object, data->string, "string");
        break;
      }

      case cJSON_Array:{
        cJSON* current_item = data->child;
        if (current_item){
          cJSON* arr = transformValue2Array(current_item);
          cJSON_AddItemToObject(object, data->string, arr);
        }
        break;
      }

      case cJSON_Object:{
        cJSON* objectDst = transformJson2JsonTemplate(data);
        cJSON_AddItemToObject(object, data->string, objectDst);
        break;
      }

      default:
        break;
    }
    data = data->next;
  }
  return object;
}

#define ADD_AVRO_TYPE(avro,type,name) cJSON* t = cJSON_CreateString(type);\
                           cJSON_AddItemToObject(avro, "type", t);\
                           cJSON* n = cJSON_CreateString(name);\
                           cJSON_AddItemToObject(avro, "name", n)

cJSON* transformArray2AvroRecord(cJSON* template);

cJSON* objectElement2Avro(cJSON* data){
  cJSON* avroSchema = cJSON_CreateObject();
  switch ((data->type) & 0xFF)
  {
    case cJSON_String:
    {
      ADD_AVRO_TYPE(avroSchema, data->valuestring, data->string);
      break;
    }

    case cJSON_Array:{
      cJSON* n = cJSON_CreateString(data->string);
      cJSON_AddItemToObject(avroSchema, "name", n);
      cJSON* array = transformArray2AvroRecord(data);
      cJSON_AddItemToObject(avroSchema, "type", array);
      break;
    }

    case cJSON_Object:{
      cJSON* n = cJSON_CreateString(data->string);
      cJSON_AddItemToObject(avroSchema, "name", n);
      cJSON* record = transformJsonTemplate2AvroRecord(data);
      cJSON_AddItemToObject(avroSchema, "type", record);
      break;
    }

    default:{
      uError("Unsupported avro type:%d", data->type);
      ASSERTS(0, "Unsupported avro type");
      break;
    }
  }
  return avroSchema;
}

cJSON* arrayElement2Avro(cJSON* data){
  switch ((data->type) & 0xFF)
  {
    case cJSON_String:
    {
      return cJSON_CreateString(data->valuestring);
    }

    case cJSON_Array:{
      return transformArray2AvroRecord(data);
    }

    case cJSON_Object:{
      return transformJsonTemplate2AvroRecord(data);
    }

    default:{
      uError("Unsupported avro type:%d", data->type);
      ASSERTS(0, "Unsupported avro type");
      break;
    }
  }
  return NULL;
}

cJSON* transformJsonTemplate2AvroRecord(cJSON* cTemplate){
  cJSON* record = cJSON_CreateObject();
  char name[256] = {0};
  sprintf(name, "schema_%p", cTemplate);
  ADD_AVRO_TYPE(record, "record", name);
  cJSON* fields = cJSON_CreateArray();

  cJSON *data = cTemplate->child;
  while (data){
    cJSON* avro = objectElement2Avro(data);
    cJSON_AddItemToArray(fields, avro);
    data = data->next;
  }

  cJSON_AddItemToObject(record, "fields", fields);
  return record;
}

cJSON* transformArray2AvroRecord(cJSON* cTemplate){
  cJSON* array = cJSON_CreateObject();
  cJSON* t = cJSON_CreateString("array");
  cJSON_AddItemToObject(array, "type", t);

  cJSON* current_item = cTemplate->child;
  if (current_item){
    cJSON* avro = arrayElement2Avro(current_item);
    cJSON_AddItemToObject(array, "items", avro);
  }
  return array;
}

int encodeBySchema(const avro_schema_t schema, cJSON *json, avro_datum_t current_val) {
  switch (schema->type) {
    case AVRO_RECORD:
    {
      ASSERT(cJSON_IsObject(json));

      int len = avro_schema_record_size(schema), i;
      for (i=0; i<len; i++) {
        const char *name = avro_schema_record_field_name(schema, i);
        avro_schema_t field_schema = avro_schema_record_field_get_by_index(schema, i);

        cJSON *json_val = cJSON_GetObjectItem(json, name);

        avro_datum_t  subrec = NULL;
        avro_record_get(current_val, name, &subrec);
        if (encodeBySchema(field_schema, json_val, subrec) != 0){
          return -1;
        }
      }
    } break;

    case AVRO_STRING:
      ASSERT(cJSON_IsString(json));

      const char *js = cJSON_GetStringValue(json);
      avro_string_set(current_val, js);
      break;

    case AVRO_INT32:
      ASSERT(0);
      break;

    case AVRO_INT64:
      ASSERT(cJSON_IsNumber(json));
      avro_int64_set(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_FLOAT:
      ASSERT(cJSON_IsNumber(json));
      avro_float_set(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_DOUBLE:
      ASSERT(cJSON_IsNumber(json));
      avro_double_set(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_BOOLEAN:
      ASSERT(cJSON_IsBool(json));
      if (cJSON_IsTrue(json))
        avro_boolean_set(current_val, 1);
      else
        avro_boolean_set(current_val, 0);
      break;

    case AVRO_NULL:
      ASSERT(cJSON_IsNull(json));
      break;

    case AVRO_ARRAY:
      ASSERT(cJSON_IsArray(json));
      int i, len = cJSON_GetArraySize(json);
      avro_schema_t items = avro_schema_array_items(schema);
      for (i=0; i<len; i++) {
        avro_datum_t  element = avro_datum_from_schema(items);
        avro_array_append_datum(current_val, element);
        if (encodeBySchema(items, cJSON_GetArrayItem(json, i), element)){
          avro_datum_decref(element);
          return -1;
        }
        avro_datum_decref(element);
      }
      break;

    case AVRO_BYTES:
    case AVRO_ENUM:
    case AVRO_MAP:
    case AVRO_UNION:
    case AVRO_FIXED:
    case AVRO_LINK:
      ASSERT(0);
      break;

    default:
    uError("ERROR: avro Unknown type: %d", schema->type);
      return -1;
  }
  return 0;
}

avro_schema_t getAvroSchema(cJSON* avro) {
  avro_schema_t      schema;
  char*              schema_str = cJSON_PrintUnformatted(avro);
  if(!schema_str){
    return NULL;
  }
  if (avro_schema_from_json_length(schema_str, strlen(schema_str), &schema)) {
    uError("%s Failed to parse schema, err:%s", __FUNCTION__, avro_strerror());
    taosMemoryFree(schema_str);
    return NULL;
  }
  taosMemoryFree(schema_str);
  return schema;
}

int32_t encodeJson2Avro(cJSON* data, avro_schema_t schema, void* encodeData, int64_t* len) {
  int32_t        ret = 0;
  avro_datum_t   in = NULL;
  avro_writer_t  writer = NULL;

  writer = avro_writer_memory(encodeData, *len);
  if(writer == NULL){
    uError("%s Failed to create writer", __FUNCTION__);
    goto END;

  }
  in = avro_datum_from_schema(schema);
  if(in == NULL){
    uError("%s Failed to create datum", __FUNCTION__);
    goto END;
  }

  check(ret,encodeBySchema(schema, data, in))
  avro_writer_reset(writer);
  check(ret, avro_write_data(writer, schema, in))
  *len = avro_writer_tell(writer);
END:
  avro_datum_decref(in);
  avro_writer_free(writer);
  return ret;
}

//cJSON* decodeRecord2Json(const avro_schema_t schema, avro_value_t *current_val);
//
//cJSON* decodeBySchema(const avro_schema_t schema, avro_value_t *current_val) {
//  switch (schema->type) {
//    case AVRO_RECORD:
//    {
//      return decodeRecord2Json(schema, current_val);
//    }
//
//    case AVRO_STRING:{
//      const char *p = NULL;
//      size_t size = 0;
//      avro_value_get_string(current_val, &p, &size);
//      return cJSON_CreateString(p);
//    }
//
//
//    case AVRO_INT32:
//      ASSERT(0);
//      break;
//
//    case AVRO_INT64:{
//      int64_t num = 0;
//      avro_value_get_long(current_val, &num);
//      return cJSON_CreateNumber(num);
//    }
//
//    case AVRO_FLOAT:{
//      float num = 0;
//      avro_value_get_float(current_val, &num);
//      return cJSON_CreateNumber(num);
//    }
//
//    case AVRO_DOUBLE:{
//      double num = 0;
//      avro_value_get_double(current_val, &num);
//      return cJSON_CreateNumber(num);
//    }
//
//    case AVRO_BOOLEAN:{
//      int num = 0;
//      avro_value_get_boolean(current_val, &num);
//      return cJSON_CreateBool(num);
//    }
//
//    case AVRO_NULL:{
//      return cJSON_CreateNull();
//    }
//
//    case AVRO_ARRAY:
//      ASSERT(cJSON_IsArray(json));
//      int i, len = cJSON_GetArraySize(json);
//      avro_schema_t items = avro_schema_array_items(schema);
//      avro_value_t val;
//      for (i=0; i<len; i++) {
//        avro_value_append(current_val, &val, NULL);
//        if (decodeBySchema(items, cJSON_GetArrayItem(json, i), &val)){
//          return -1;
//        }
//      }
//      break;
//
//    case AVRO_BYTES:
//    case AVRO_ENUM:
//    case AVRO_MAP:
//    case AVRO_UNION:
//    case AVRO_FIXED:
//    case AVRO_LINK:
//      ASSERT(0);
//      break;
//
//    default:
//    uError("ERROR: avro Unknown type: %d", schema->type);
//      return -1;
//  }
//  return 0;
//}
//
//cJSON* decodeRecord2Json(const avro_schema_t schema, avro_value_t *current_val) {
//  cJSON* json = cJSON_CreateObject();
//
//  int len = avro_schema_record_size(schema), i;
//  for (i=0; i<len; i++) {
//    const char *name = avro_schema_record_field_name(schema, i);
//    avro_schema_t field_schema = avro_schema_record_field_get_by_index(schema, i);
//
//    avro_value_t field;
//    avro_value_get_by_index(current_val, i, &field, NULL);
//    cJSON* tmp = decodeBySchema(field_schema, &field);
//    cJSON_AddItemToObject(json, name, tmp);
//  }
//  return json;
//}

avro_datum_t decodeAvro2Datum(const avro_schema_t schema, void* data, int64_t len) {
  int32_t ret = 0;
  avro_datum_t  out = NULL;
  avro_reader_t  reader = avro_reader_memory(data, len);
  if(reader == NULL){
    return NULL;
  }
  avro_reader_reset(reader);
  check(ret, avro_read_data(reader, schema, schema, &out))

  END:
//  avro_datum_decref(out);
  avro_reader_free(reader);

  if(ret != 0){
    return NULL;
  }
  return out;
}

char* datum2Json(avro_datum_t  out) {
  char* jsonStr = NULL;
  if(avro_datum_to_json(out, 1, &jsonStr) != 0){
    return NULL;
  }else {
    return jsonStr;
  }
}

//int32_t getElementFromRoot(const avro_schema_t schema, void* data, int64_t len, const char* key) {
//  int ret = 0;
//  if (!is_avro_record(schema)) {
//    return -1;
//  }
//  const char* record_name = avro_schema_name(schema);
//
//  avro_schema_t projection_schema = avro_schema_record(record_name,NULL);
//  avro_schema_t t = avro_schema_record_field_get(schema, key);
//  avro_schema_t projection_schame_key_schema = avro_schema_copy(t);
//  avro_schema_record_field_append(projection_schema, key, projection_schame_key_schema);
//
//  avro_datum_t  out = NULL;
//  avro_reader_t  reader = avro_reader_memory(data, len);
//  if(reader == NULL){
//    return -1;
//  }
//  avro_reader_reset(reader);
//  check(ret, avro_read_data(reader, schema, projection_schema, &out))
//  check(ret, avro_datum_to_json(out, 1, jsonStr))
//
//          END:
//  avro_datum_decref(out);
//}
//
//avro_datum_t getElementFromDatum(avro_datum_t schema, const char* key) {
//  int ret = 0;
//  if (!is_avro_record(schema)) {
//    return NULL;
//  }
//  const char* record_name = avro_schema_name(schema);
//
//  avro_schema_t projection_schema = avro_schema_record(record_name,NULL);
//  avro_schema_t t = avro_schema_record_field_get(schema, key);
//  avro_schema_t projection_schame_key_schema = avro_schema_copy(t);
//  avro_schema_record_field_append(projection_schema, key, projection_schame_key_schema);
//
//  avro_datum_t  out = NULL;
//  avro_reader_t  reader = avro_reader_memory(data, len);
//  if(reader == NULL){
//    return -1;
//  }
//  avro_reader_reset(reader);
//  check(ret, avro_read_data(reader, schema, projection_schema, &out))
//  check(ret, avro_datum_to_json(out, 1, jsonStr))
//
//  END:
//  avro_datum_decref(out);
//}

int32_t testJsonAvro(const char* json) {
  if (json == NULL) {
    return -1;
  }

  cJSON* root = cJSON_Parse(json);
  if (root == NULL) {
    return -1;
  }

  if (!cJSON_IsObject(root)) {
    return -1;
  }
  cJSON* object = transformJson2JsonTemplate(root);
  cJSON* avro = transformJsonTemplate2AvroRecord(object);
  char* in = cJSON_PrintUnformatted(root);
//  char* out = cJSON_PrintUnformatted(object);

//  char* in = cJSON_Print(root);
  char* out = cJSON_Print(object);
  char* avroout = cJSON_Print(avro);


  printf("%s\n\n\n%s\n\n\n%s\n", in, out, avroout);

  avro_schema_t schema = getAvroSchema(avro);
  if (schema == NULL) {
    return -1;
  }

  int64_t len = strlen(json);
  void* encodeData = taosMemoryMalloc(len);
  encodeJson2Avro(root, schema, encodeData, &len);
  char* decodeStr = datum2Json(decodeAvro2Datum(schema, encodeData, len));
  printf("decode str: %s\n", decodeStr);
  taosMemoryFree(decodeStr);
  taosMemoryFree(encodeData);

  taosMemoryFree(in);
  taosMemoryFree(out);
  taosMemoryFree(avroout);

  cJSON_Delete(avro);
  cJSON_Delete(root);
  cJSON_Delete(object);
  avro_schema_decref(schema);
  return 0;
}

static bool valueValidate(const char* value){
  if(strcmp(value, "string") == 0 || strcmp(value, "long") == 0 ||
     strcmp(value, "double") == 0 || strcmp(value, "boolean") == 0){
    return true;
  }
  return false;
}

int32_t checkJsonTemplate(SJson *pJson){
  cJSON *template = (cJSON*)pJson;
  if (template->type == cJSON_Array) {
    int array_size = cJSON_GetArraySize(template);
    if(array_size != 1){
      return TSDB_CODE_TEMPLATE_ARRAY_ONLY_ONE_TYPE;
    }
    for (int i = 0; i < array_size; i++) {
      cJSON *item = cJSON_GetArrayItem(template, i);
      int code = checkJsonTemplate(item);
      if (code != TSDB_CODE_SUCCESS)
        return code;
    }
  } else if (template->type == cJSON_Object) {
    cJSON *item = template->child;
    while (item != NULL) {
      int code = checkJsonTemplate(item);
      if (code != TSDB_CODE_SUCCESS)
        return code;
      item = item->next;
    }
  } else {
    if (template->type != cJSON_String || !valueValidate(template->valuestring)) {
      return TSDB_CODE_TEMPLATE_VALUE_INVALIDATE;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t checkJsonTemplateString(const char *jsonTemplate){
  int32_t code = TSDB_CODE_SUCCESS;
  if (0 == strlen(jsonTemplate)) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  cJSON *root = cJSON_Parse(jsonTemplate);
  if (root == NULL){
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  if (root->type != cJSON_Object || root->child == NULL){
    code = TSDB_CODE_TEMPLATE_MUST_BE_OBJECT;
    goto END;
  }

  code = checkJsonTemplate(root);

END:
  cJSON_Delete(root);
  return code;
}

uint8_t decodeTemplateId(uint8_t* data, int32_t *value){
  *value = 0;
  uint8_t b;
  uint8_t offset = 0;
  do {
    if (offset == 10) {
      return 0;
    }
    b = *data;
    *value |= (int32_t) (b & 0x7F) << (7 * offset);
    ++offset;
    ++data;
  }
  while (b & 0x80);
  return offset;
}

uint8_t encodeTemplateId(uint8_t* buf, int32_t data){
  uint8_t bytes = 0;
  while (data & ~0x7F) {
    buf[bytes++] = (char)((((uint8_t) data) & 0x7F) | 0x80);
    data >>= 7;
  }
  buf[bytes++] = data;
  return bytes;
}

int32_t               jsonTemplateRef = 0;
static TdThreadOnce   jsonTemplateInit = PTHREAD_ONCE_INIT;  // initialize only once

static void jsonTemplateFree(void* handle) {
  SArray *tmp = (SArray *)handle;
  for(int i = 0; i < taosArrayGetSize(tmp); i++){
    avro_schema_decref(taosArrayGetP(tmp, i));
  }
  taosArrayDestroy(tmp);
}

static void jsonTemplateMgmtInit(void) {
  jsonTemplateRef = taosOpenRef(10000, jsonTemplateFree);
}

void initJsonTemplateMeta(){
  taosThreadOnce(&jsonTemplateInit, jsonTemplateMgmtInit);
}