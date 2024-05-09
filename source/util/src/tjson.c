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
#include "avro.h"

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

int32_t tjsonToObject(const SJson* pJson, const char* pName, FToObject func, void* pObj) {
  SJson* pJsonObj = tjsonGetObjectItem(pJson, pName);
  if (NULL == pJsonObj) {
    return TSDB_CODE_SUCCESS;
  }
  return func(pJsonObj, pObj);
}

int32_t tjsonMakeObject(const SJson* pJson, const char* pName, FToObject func, void** pObj, int32_t objSize) {
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

int32_t tjsonToArray(const SJson* pJson, const char* pName, FToObject func, void* pArray, int32_t itemSize) {
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

int32_t tjsonToTArray(const SJson* pJson, const char* pName, FToObject func, SArray** pArray, int32_t itemSize) {
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

bool isInteger(double value){
  return value == (int64_t)value;
}

#define ADD_TYPE_2_OBJECT(t,k,v) cJSON* type = cJSON_CreateString(v);\
                                 cJSON_AddItemToObject(t, k, type)

#define ADD_TYPE_2_ARRAY(t,v) cJSON* type = cJSON_CreateString(v);\
                              cJSON_AddItemToArray(t, type)


cJSON* transformObject2Object(cJSON* objectSrc);

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
      cJSON* object = transformObject2Object(data);
      cJSON_AddItemToArray(arrayDst, object);
      break;
    }

    default:
      break;
  }
  return arrayDst;
}

cJSON* transformObject2Object(cJSON* objectSrc){
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
        cJSON* objectDst = transformObject2Object(data);
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

cJSON* transformObject2AvroRecord(cJSON* template);

cJSON* json2Avro(cJSON* data){
  cJSON* avroSchema = cJSON_CreateObject();
  switch ((data->type) & 0xFF)
  {
    case cJSON_String:
    {
      ADD_AVRO_TYPE(avroSchema, data->valuestring, data->string);
      break;
    }

    case cJSON_Array:{
      ADD_AVRO_TYPE(avroSchema, "array", data->string);

      cJSON* current_item = data->child;
      if (current_item){
        cJSON* avro = json2Avro(current_item);
        cJSON_AddItemToObject(avroSchema, "items", avro);
      }

      break;
    }

    case cJSON_Object:{
      cJSON* n = cJSON_CreateString(data->string);
      cJSON_AddItemToObject(avroSchema, "name", n);
      cJSON* record = transformObject2AvroRecord(data);
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

cJSON* transformObject2AvroRecord(cJSON* template){
  cJSON* record = cJSON_CreateObject();
  ADD_AVRO_TYPE(record, "record", "schema");
  cJSON* fields = cJSON_CreateArray();

  cJSON *data = template->child;
  while (data){
    cJSON* avro = json2Avro(data);
    cJSON_AddItemToArray(fields, avro);
    data = data->next;
  }

  cJSON_AddItemToObject(record, "fields", fields);
  return record;
}

int schema_traverse(const avro_schema_t schema, cJSON *json, avro_value_t *current_val) {
  switch (schema->type) {
    case AVRO_RECORD:
    {
      ASSERT(cJSON_IsObject(json));

      int len = avro_schema_record_size(schema), i;
      for (i=0; i<len; i++) {
        const char *name = avro_schema_record_field_name(schema, i);
        avro_schema_t field_schema = avro_schema_record_field_get_by_index(schema, i);

        cJSON *json_val = cJSON_GetObjectItem(json, name);

        avro_value_t field;
        avro_value_get_by_index(current_val, i, &field, NULL);

        if (schema_traverse(field_schema, json_val, &field) != 0){
          return -1;
        }
      }
    } break;

    case AVRO_STRING:
      ASSERT(cJSON_IsString(json));

      const char *js = cJSON_GetStringValue(json);
      avro_value_set_string(current_val, js);
      break;

    case AVRO_INT32:
      ASSERT(0);
      break;

    case AVRO_INT64:
      ASSERT(cJSON_IsNumber(json));
      avro_value_set_long(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_FLOAT:
      ASSERT(cJSON_IsNumber(json));
      avro_value_set_float(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_DOUBLE:
      ASSERT(cJSON_IsNumber(json));
      avro_value_set_double(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_BOOLEAN:
      ASSERT(cJSON_IsBool(json));
      avro_value_set_boolean(current_val, cJSON_GetNumberValue(json));
      break;

    case AVRO_NULL:
      ASSERT(cJSON_IsNull(json));
      avro_value_set_null(current_val);
      break;

    case AVRO_ARRAY:
      ASSERT(cJSON_IsArray(json));
      int i, len = cJSON_GetArraySize(json);
      avro_schema_t items = avro_schema_array_items(schema);
      avro_value_t val;
      for (i=0; i<len; i++) {
        avro_value_append(current_val, &val, NULL);
        if (schema_traverse(items, cJSON_GetArrayItem(json, i), &val)){
          return -1;
        }
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

int32_t json2avro(cJSON* data, cJSON* avro, void** encodeData, int64_t* len) {
  avro_file_writer_t db;
  avro_schema_t schema;
  char*         shcema_str = cJSON_PrintUnformatted(avro);
  if (avro_schema_from_json_length(shcema_str, strlen(shcema_str), &schema)) {
    uError("%s Failed to parse schema", __FUNCTION__);
    return -1;
  }
  int32_t rval = avro_file_writer_create_with_codec("/tmp/avro.db", schema, &db, "deflate", 0);
  if (rval) {
    uError("%s There was an error creating writer %s, error message: %s\n", __FUNCTION__, shcema_str, avro_strerror());
    return -1;
  }

  avro_value_t        record;
  avro_value_iface_t* iface = avro_generic_class_from_schema(schema);
  avro_generic_value_new(iface, &record);

  if (schema_traverse(schema, data, &record) != 0) {
    uError("%s Failed to process record %s", __FUNCTION__, shcema_str);
    return -1;
  }

  avro_file_writer_get_encode_data(db, &record, encodeData, len);
  avro_value_iface_decref(iface);
  avro_value_decref(&record);

  avro_file_writer_close(db);
  return 0;
}

int32_t encodeJsonData2Avro(const char* json) {
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
  cJSON* object = transformObject2Object(root);
  cJSON* avro = transformObject2AvroRecord(object);
//  char* in = cJSON_PrintUnformatted(root);
//  char* out = cJSON_PrintUnformatted(object);

  char* in = cJSON_Print(root);
  char* out = cJSON_Print(object);
  char* avroout = cJSON_Print(avro);


  printf("%s\n\n\n%s\n\n\n%s", in, out, avroout);

  void** encodeData = NULL;
  int64_t len = 0;
  json2avro(root, avro, encodeData, &len);


  taosMemoryFree(out);
  cJSON_Delete(root);
  cJSON_Delete(object);
  return 0;
}