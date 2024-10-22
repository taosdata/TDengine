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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "clientSml.h"

#define OTD_JSON_SUB_FIELDS_NUM 2

static inline int32_t smlParseMetricFromJSON(SSmlHandle *info, cJSON *metric, SSmlLineInfo *elements) {
  elements->measureLen = strlen(metric->valuestring);
  if (IS_INVALID_TABLE_LEN(elements->measureLen)) {
    uError("OTD:0x%" PRIx64 " Metric length is 0 or large than 192", info->id);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  elements->measure = metric->valuestring;
  return TSDB_CODE_SUCCESS;
}

const char    *jsonName[OTD_JSON_FIELDS_NUM] = {"metric", "timestamp", "value", "tags"};
static int32_t smlGetJsonElements(cJSON *root, cJSON ***marks) {
  for (int i = 0; i < OTD_JSON_FIELDS_NUM; ++i) {
    cJSON *child = root->child;
    while (child != NULL) {
      if (strcasecmp(child->string, jsonName[i]) == 0) {
        *marks[i] = child;
        break;
      }
      child = child->next;
    }
    if (*marks[i] == NULL) {
      uError("smlGetJsonElements error, not find mark:%d:%s", i, jsonName[i]);
      return TSDB_CODE_TSC_INVALID_JSON;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlConvertJSONBool(SSmlKv *pVal, char *typeStr, cJSON *value) {
  if (strcasecmp(typeStr, "bool") != 0) {
    uError("OTD:invalid type(%s) for JSON Bool", typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->type = TSDB_DATA_TYPE_BOOL;
  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
  pVal->i = value->valueint;

  return TSDB_CODE_SUCCESS;
}

static int32_t smlConvertJSONNumber(SSmlKv *pVal, char *typeStr, cJSON *value) {
  // tinyint
  if (strcasecmp(typeStr, "i8") == 0 || strcasecmp(typeStr, "tinyint") == 0) {
    if (!IS_VALID_TINYINT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(tinyint)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // smallint
  if (strcasecmp(typeStr, "i16") == 0 || strcasecmp(typeStr, "smallint") == 0) {
    if (!IS_VALID_SMALLINT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(smallint)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // int
  if (strcasecmp(typeStr, "i32") == 0 || strcasecmp(typeStr, "int") == 0) {
    if (!IS_VALID_INT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(int)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // bigint
  if (strcasecmp(typeStr, "i64") == 0 || strcasecmp(typeStr, "bigint") == 0) {
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    if (value->valuedouble >= (double)INT64_MAX) {
      pVal->i = INT64_MAX;
    } else if (value->valuedouble <= (double)INT64_MIN) {
      pVal->i = INT64_MIN;
    } else {
      pVal->i = value->valuedouble;
    }
    return TSDB_CODE_SUCCESS;
  }
  // float
  if (strcasecmp(typeStr, "f32") == 0 || strcasecmp(typeStr, "float") == 0) {
    if (!IS_VALID_FLOAT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(float)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->f = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // double
  if (strcasecmp(typeStr, "f64") == 0 || strcasecmp(typeStr, "double") == 0) {
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->d = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }

  // if reach here means type is unsupported
  uError("OTD:invalid type(%s) for JSON Number", typeStr);
  return TSDB_CODE_TSC_INVALID_JSON_TYPE;
}

static int32_t smlConvertJSONString(SSmlKv *pVal, char *typeStr, cJSON *value) {
  if (strcasecmp(typeStr, "binary") == 0) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
  } else if (strcasecmp(typeStr, "varbinary") == 0) {
    pVal->type = TSDB_DATA_TYPE_VARBINARY;
  } else if (strcasecmp(typeStr, "nchar") == 0) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
  } else {
    uError("OTD:invalid type(%s) for JSON String", typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->length = strlen(value->valuestring);

  if ((pVal->type == TSDB_DATA_TYPE_BINARY || pVal->type == TSDB_DATA_TYPE_VARBINARY) && pVal->length > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
    return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
  }
  if (pVal->type == TSDB_DATA_TYPE_NCHAR &&
      pVal->length > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
    return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
  }

  pVal->value = value->valuestring;
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseValueFromJSONObj(cJSON *root, SSmlKv *kv) {
  int32_t ret = TSDB_CODE_SUCCESS;
  int32_t size = cJSON_GetArraySize(root);

  if (size != OTD_JSON_SUB_FIELDS_NUM) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (value == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (!cJSON_IsString(type)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  switch (value->type) {
    case cJSON_True:
    case cJSON_False: {
      ret = smlConvertJSONBool(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_Number: {
      ret = smlConvertJSONNumber(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_String: {
      ret = smlConvertJSONString(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseValueFromJSON(cJSON *root, SSmlKv *kv) {
  switch (root->type) {
    case cJSON_True:
    case cJSON_False: {
      kv->type = TSDB_DATA_TYPE_BOOL;
      kv->length = (int16_t)tDataTypes[kv->type].bytes;
      kv->i = root->valueint;
      break;
    }
    case cJSON_Number: {
      kv->type = TSDB_DATA_TYPE_DOUBLE;
      kv->length = (int16_t)tDataTypes[kv->type].bytes;
      kv->d = root->valuedouble;
      break;
    }
    case cJSON_String: {
      int32_t ret = smlConvertJSONString(kv, "binary", root);
      if (ret != TSDB_CODE_SUCCESS) {
        uError("OTD:Failed to parse binary value from JSON Obj");
        return ret;
      }
      break;
    }
    case cJSON_Object: {
      int32_t ret = smlParseValueFromJSONObj(root, kv);
      if (ret != TSDB_CODE_SUCCESS) {
        uError("OTD:Failed to parse value from JSON Obj");
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlProcessTagJson(SSmlHandle *info, cJSON *tags){
  SArray *preLineKV = info->preLineTagKV;
  taosArrayClearEx(preLineKV, freeSSmlKv);
  int     cnt = 0;

  int32_t tagNum = cJSON_GetArraySize(tags);
  if (unlikely(tagNum == 0)) {
    uError("SML:Tag should not be empty");
    return TSDB_CODE_TSC_INVALID_JSON;
  }
  for (int32_t i = 0; i < tagNum; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (unlikely(tag == NULL)) {
      return TSDB_CODE_TSC_INVALID_JSON;
    }
    size_t keyLen = strlen(tag->string);
    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      uError("OTD:Tag key length is 0 or too large than 64");
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }

    // add kv to SSmlKv
    SSmlKv kv = {0};
    kv.key = tag->string;
    kv.keyLen = keyLen;

    // value
    int32_t ret = smlParseValueFromJSON(tag, &kv);
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      return ret;
    }
    if (taosArrayPush(preLineKV, &kv) == NULL) {
      return terrno;
    }

    if (info->dataFormat && !isSmlTagAligned(info, cnt, &kv)) {
      return TSDB_CODE_TSC_INVALID_JSON;
    }

    cnt++;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTagsFromJSON(SSmlHandle *info, cJSON *tags, SSmlLineInfo *elements) {
  int32_t code = 0;
  if(info->dataFormat){
    SML_CHECK_CODE(smlProcessSuperTable(info, elements));
  }
  SML_CHECK_CODE(smlProcessTagJson(info, tags));
  SML_CHECK_CODE(smlJoinMeasureTag(elements));
  return smlProcessChildTable(info, elements);

END:
  if(info->reRun){
    return TSDB_CODE_SUCCESS;
  }
  return code;
}

static int64_t smlParseTSFromJSONObj(SSmlHandle *info, cJSON *root, int32_t toPrecision) {
  int32_t size = cJSON_GetArraySize(root);
  if (unlikely(size != OTD_JSON_SUB_FIELDS_NUM)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (unlikely(!cJSON_IsNumber(value))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (unlikely(!cJSON_IsString(type))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  double timeDouble = value->valuedouble;
  if (unlikely(smlDoubleToInt64OverFlow(timeDouble))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  if (timeDouble == 0) {
    return taosGetTimestampNs() / smlFactorNS[toPrecision];
  }

  if (timeDouble < 0) {
    return timeDouble;
  }

  int64_t tsInt64 = timeDouble;
  size_t  typeLen = strlen(type->valuestring);
  if (typeLen == 1 && (type->valuestring[0] == 's' || type->valuestring[0] == 'S')) {
    // seconds
    if (smlFactorS[toPrecision] < INT64_MAX / tsInt64) {
      return tsInt64 * smlFactorS[toPrecision];
    }
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  } else if (typeLen == 2 && (type->valuestring[1] == 's' || type->valuestring[1] == 'S')) {
    switch (type->valuestring[0]) {
      case 'm':
      case 'M':
        // milliseconds
        return convertTimePrecision(tsInt64, TSDB_TIME_PRECISION_MILLI, toPrecision);
      case 'u':
      case 'U':
        // microseconds
        return convertTimePrecision(tsInt64, TSDB_TIME_PRECISION_MICRO, toPrecision);
      case 'n':
      case 'N':
        return convertTimePrecision(tsInt64, TSDB_TIME_PRECISION_NANO, toPrecision);
      default:
        return TSDB_CODE_TSC_INVALID_JSON_TYPE;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
}

uint8_t smlGetTimestampLen(int64_t num) {
  uint8_t len = 0;
  while ((num /= 10) != 0) {
    len++;
  }
  len++;
  return len;
}

static int64_t smlParseTSFromJSON(SSmlHandle *info, cJSON *timestamp) {
  // Timestamp must be the first KV to parse
  int32_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;
  if (cJSON_IsNumber(timestamp)) {
    // timestamp value 0 indicates current system time
    double timeDouble = timestamp->valuedouble;
    if (unlikely(smlDoubleToInt64OverFlow(timeDouble))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }

    if (unlikely(timeDouble < 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is negative", NULL);
      return timeDouble;
    } else if (unlikely(timeDouble == 0)) {
      return taosGetTimestampNs() / smlFactorNS[toPrecision];
    }

    uint8_t tsLen = smlGetTimestampLen((int64_t)timeDouble);

    int8_t fromPrecision = smlGetTsTypeByLen(tsLen);
    if (unlikely(fromPrecision == -1)) {
      smlBuildInvalidDataMsg(&info->msgBuf,
                             "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    int64_t tsInt64 = timeDouble;
    if (fromPrecision == TSDB_TIME_PRECISION_SECONDS) {
      if (smlFactorS[toPrecision] < INT64_MAX / tsInt64) {
        return tsInt64 * smlFactorS[toPrecision];
      }
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    } else {
      return convertTimePrecision(timeDouble, fromPrecision, toPrecision);
    }
  } else if (cJSON_IsObject(timestamp)) {
    return smlParseTSFromJSONObj(info, timestamp, toPrecision);
  } else {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return TSDB_CODE_TSC_INVALID_JSON;
  }
}

static int32_t smlParseJSONStringExt(SSmlHandle *info, cJSON *root, SSmlLineInfo *elements) {
  int32_t ret = TSDB_CODE_SUCCESS;

  cJSON *metricJson = NULL;
  cJSON *tsJson = NULL;
  cJSON *valueJson = NULL;
  cJSON *tagsJson = NULL;

  int32_t size = cJSON_GetArraySize(root);
  // outmost json fields has to be exactly 4
  if (size != OTD_JSON_FIELDS_NUM) {
    uError("OTD:0x%" PRIx64 " Invalid number of JSON fields in data point %d", info->id, size);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON **marks[OTD_JSON_FIELDS_NUM] = {&metricJson, &tsJson, &valueJson, &tagsJson};
  ret = smlGetJsonElements(root, marks);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    return ret;
  }

  // Parse metric
  ret = smlParseMetricFromJSON(info, metricJson, elements);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    uError("OTD:0x%" PRIx64 " Unable to parse metric from JSON payload", info->id);
    return ret;
  }

  // Parse metric value
  SSmlKv kv = {.key = VALUE, .keyLen = VALUE_LEN};
  ret = smlParseValueFromJSON(valueJson, &kv);
  if (unlikely(ret)) {
    uError("OTD:0x%" PRIx64 " Unable to parse metric value from JSON payload", info->id);
    return ret;
  }

  // Parse tags
  bool needFree = info->dataFormat;
  elements->tags = cJSON_PrintUnformatted(tagsJson);
  if (elements->tags == NULL){
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  elements->tagsLen = strlen(elements->tags);
  if (is_same_child_table_telnet(elements, &info->preLine) != 0) {
    ret = smlParseTagsFromJSON(info, tagsJson, elements);
    if (unlikely(ret)) {
      uError("OTD:0x%" PRIx64 " Unable to parse tags from JSON payload", info->id);
      taosMemoryFree(elements->tags);
      elements->tags = NULL;
      return ret;
    }
  } else {
    elements->measureTag = info->preLine.measureTag;
  }

  if (needFree) {
    taosMemoryFree(elements->tags);
    elements->tags = NULL;
  }

  if (unlikely(info->reRun)) {
    return TSDB_CODE_SUCCESS;
  }

  // Parse timestamp
  // notice!!! put ts back to tag to ensure get meta->precision
  int64_t ts = smlParseTSFromJSON(info, tsJson);
  if (unlikely(ts < 0)) {
    char* tmp = cJSON_PrintUnformatted(tsJson);
    if (tmp == NULL) {
      uError("cJSON_PrintUnformatted failed since %s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      uError("OTD:0x%" PRIx64 " Unable to parse timestamp from JSON payload %s %" PRId64, info->id, info->msgBuf.buf, ts);
    } else {
      uError("OTD:0x%" PRIx64 " Unable to parse timestamp from JSON payload %s %s %" PRId64, info->id, info->msgBuf.buf,tmp, ts);
      taosMemoryFree(tmp);
    }
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  SSmlKv kvTs = {0};
  smlBuildTsKv(&kvTs, ts);
  if (info->dataFormat){
    ret = smlParseEndTelnetJsonFormat(info, elements, &kvTs, &kv);
  } else {
    ret = smlParseEndTelnetJsonUnFormat(info, elements, &kvTs, &kv);
  }
  return ret;
}

int32_t smlParseJSONExt(SSmlHandle *info, char *payload) {
  int32_t payloadNum = 0;
  int32_t ret = TSDB_CODE_SUCCESS;

  info->root = cJSON_Parse(payload);
  if (unlikely(info->root == NULL)) {
    uError("SML:0x%" PRIx64 " parse json failed:%s", info->id, payload);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  // multiple data points must be sent in JSON array
  if (cJSON_IsArray(info->root)) {
    payloadNum = cJSON_GetArraySize(info->root);
  } else if (cJSON_IsObject(info->root)) {
    payloadNum = 1;
  } else {
    uError("SML:0x%" PRIx64 " Invalid JSON Payload 3:%s", info->id, payload);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  if (unlikely(info->lines != NULL)) {
    for (int i = 0; i < info->lineNum; i++) {
      taosArrayDestroyEx(info->lines[i].colArray, freeSSmlKv);
      if (info->lines[i].measureTagsLen != 0) taosMemoryFree(info->lines[i].measureTag);
    }
    taosMemoryFree(info->lines);
    info->lines = NULL;
  }
  info->lineNum = payloadNum;
  info->dataFormat = true;

  ret = smlClearForRerun(info);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  info->parseJsonByLib = true;
  cJSON *head = (payloadNum == 1 && cJSON_IsObject(info->root)) ? info->root : info->root->child;

  int    cnt = 0;
  cJSON *dataPoint = head;
  while (dataPoint) {
    if (info->dataFormat) {
      SSmlLineInfo element = {0};
      ret = smlParseJSONStringExt(info, dataPoint, &element);
    } else {
      ret = smlParseJSONStringExt(info, dataPoint, info->lines + cnt);
    }
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      uError("SML:0x%" PRIx64 " Invalid JSON Payload 2:%s", info->id, payload);
      return ret;
    }

    if (unlikely(info->reRun)) {
      cnt = 0;
      dataPoint = head;
      info->lineNum = payloadNum;
      ret = smlClearForRerun(info);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      continue;
    }
    cnt++;
    dataPoint = dataPoint->next;
  }

  return TSDB_CODE_SUCCESS;
}

