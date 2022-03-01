#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "hash.h"
#include "taos.h"

#include "tscUtil.h"
#include "tsclient.h"
#include "tscLog.h"

#include "tscParseLine.h"

#define OTD_MAX_FIELDS_NUM 2
#define OTD_JSON_SUB_FIELDS_NUM 2
#define OTD_JSON_FIELDS_NUM 4

#define OTD_TIMESTAMP_COLUMN_NAME "ts"
#define OTD_METRIC_VALUE_COLUMN_NAME "value"

/* telnet style API parser */
static uint64_t HandleId = 0;

static uint64_t genUID() {
  uint64_t id;

  do {
    id = atomic_add_fetch_64(&HandleId, 1);
  } while (id == 0);

  return id;
}

static int32_t parseTelnetMetric(TAOS_SML_DATA_POINT *pSml, const char **index, SSmlLinesInfo* info) {
  const char *cur = *index;
  uint16_t len = 0;

  pSml->stableName = tcalloc(TSDB_TABLE_NAME_LEN + TS_BACKQUOTE_CHAR_SIZE, 1);
  if (pSml->stableName == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  /*
  if (isdigit(*cur)) {
    tscError("OTD:0x%"PRIx64" Metric cannot start with digit", info->id);
    tfree(pSml->stableName);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  */

  while (*cur != '\0') {
    if (len > TSDB_TABLE_NAME_LEN - 1) {
      tscError("OTD:0x%"PRIx64" Metric cannot exceeds %d characters", info->id, TSDB_TABLE_NAME_LEN - 1);
      tfree(pSml->stableName);
      return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
    }

    if (*cur == ' ') {
      if (*(cur + 1) != ' ') {
        break;
      } else {
        cur++;
        continue;
      }
    }

    pSml->stableName[len] = *cur;

    cur++;
    len++;
  }
  if (len == 0 || *cur == '\0') {
    tfree(pSml->stableName);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  addEscapeCharToString(pSml->stableName, len);
  *index = cur + 1;
  tscDebug("OTD:0x%"PRIx64" Stable name in metric:%s|len:%d", info->id, pSml->stableName, len);

  return TSDB_CODE_SUCCESS;
}

static int32_t parseTelnetTimeStamp(TAOS_SML_KV **pTS, int *num_kvs, const char **index, SSmlLinesInfo* info) {
  //Timestamp must be the first KV to parse
  assert(*num_kvs == 0);

  const char *start, *cur;
  int32_t ret = TSDB_CODE_SUCCESS;
  int len = 0;
  char key[] = OTD_TIMESTAMP_COLUMN_NAME;
  char *value = NULL;

  start = cur = *index;
  //allocate fields for timestamp and value
  *pTS = tcalloc(OTD_MAX_FIELDS_NUM, sizeof(TAOS_SML_KV));

  while(*cur != '\0') {
    if (*cur == ' ') {
      if (*(cur + 1) != ' ') {
        break;
      } else {
        cur++;
        continue;
      }
    }
    cur++;
    len++;
  }

  if (len > 0 && *cur != '\0') {
    value = tcalloc(len + 1, 1);
    memcpy(value, start, len);
  } else {
    tfree(*pTS);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  ret = convertSmlTimeStamp(*pTS, value, len, info);
  if (ret) {
    tfree(value);
    tfree(*pTS);
    return ret;
  }
  tfree(value);

  (*pTS)->key = tcalloc(sizeof(key) + TS_BACKQUOTE_CHAR_SIZE, 1);
  memcpy((*pTS)->key, key, sizeof(key));
  addEscapeCharToString((*pTS)->key, (int32_t)strlen(key));

  *num_kvs += 1;
  *index = cur + 1;

  return ret;
}

static int32_t parseTelnetMetricValue(TAOS_SML_KV **pKVs, int *num_kvs, const char **index, SSmlLinesInfo* info) {
  //skip timestamp
  TAOS_SML_KV *pVal = *pKVs + 1;
  const char *start, *cur;
  int32_t ret = TSDB_CODE_SUCCESS;
  int len = 0;
  bool searchQuote = false;
  char key[] = OTD_METRIC_VALUE_COLUMN_NAME;
  char *value = NULL;

  start = cur = *index;

  //if metric value is string
  if (*cur == '"') {
    searchQuote = true;
    cur += 1;
    len += 1;
  } else if (*cur == 'L' && *(cur + 1) == '"') {
    searchQuote = true;
    cur += 2;
    len += 2;
  }

  while(*cur != '\0') {
    if (*cur == ' ') {
      if (searchQuote == true) {
        if (*(cur - 1) == '"' && len != 1 && len != 2) {
          searchQuote = false;
        } else {
          cur++;
          len++;
          continue;
        }
      }

      if (*(cur + 1) != ' ') {
        break;
      } else {
        cur++;
        continue;
      }
    }
    cur++;
    len++;
  }

  if (len > 0 && *cur != '\0') {
    value = tcalloc(len + 1, 1);
    memcpy(value, start, len);
  } else {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  if (!convertSmlValueType(pVal, value, len, info, false)) {
    tscError("OTD:0x%"PRIx64" Failed to convert metric value string(%s) to any type",
            info->id, value);
    tfree(value);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }
  tfree(value);

  pVal->key = tcalloc(sizeof(key) + TS_BACKQUOTE_CHAR_SIZE, 1);
  memcpy(pVal->key, key, sizeof(key));
  addEscapeCharToString(pVal->key, (int32_t)strlen(pVal->key));
  *num_kvs += 1;

  *index = cur + 1;
  return ret;
}

static int32_t parseTelnetTagKey(TAOS_SML_KV *pKV, const char **index, SHashObj *pHash, SSmlLinesInfo* info) {
  const char *cur = *index;
  char key[TSDB_COL_NAME_LEN];
  uint16_t len = 0;

  //key field cannot start with digit
  //if (isdigit(*cur)) {
  //  tscError("OTD:0x%"PRIx64" Tag key cannot start with digit", info->id);
  //  return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  //}
  while (*cur != '\0') {
    if (len > TSDB_COL_NAME_LEN - 1) {
      tscError("OTD:0x%"PRIx64" Tag key cannot exceeds %d characters", info->id, TSDB_COL_NAME_LEN - 1);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    if (*cur == ' ') {
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }
    if (*cur == '=') {
      break;
    }

    key[len] = *cur;
    cur++;
    len++;
  }
  if (len == 0 || *cur == '\0') {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  key[len] = '\0';

  if (checkDuplicateKey(key, pHash, info)) {
    return TSDB_CODE_TSC_DUP_TAG_NAMES;
  }

  pKV->key = tcalloc(len + TS_BACKQUOTE_CHAR_SIZE + 1, 1);
  memcpy(pKV->key, key, len + 1);
  addEscapeCharToString(pKV->key, len);
  //tscDebug("OTD:0x%"PRIx64" Key:%s|len:%d", info->id, pKV->key, len);
  *index = cur + 1;
  return TSDB_CODE_SUCCESS;
}


static int32_t parseTelnetTagValue(TAOS_SML_KV *pKV, const char **index,
                          bool *is_last_kv, SSmlLinesInfo* info) {
  const char *start, *cur;
  char *value = NULL;
  uint16_t len = 0;
  start = cur = *index;

  while (1) {
    // whitespace or '\0' identifies a value
    if (*cur == ' ' || *cur == '\0') {
      // '\0' indicates end of value
      *is_last_kv = (*cur == '\0') ? true : false;
      if (*cur == ' ' && *(cur + 1) == ' ') {
        cur++;
        continue;
      } else {
        break;
      }
    }
    cur++;
    len++;
  }

  if (len == 0) {
    tfree(pKV->key);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  value = tcalloc(len + 1, 1);
  memcpy(value, start, len);
  value[len] = '\0';
  if (!convertSmlValueType(pKV, value, len, info, true)) {
    tscError("OTD:0x%"PRIx64" Failed to convert sml value string(%s) to any type",
            info->id, value);
    //free previous alocated key field
    tfree(pKV->key);
    tfree(value);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }
  tfree(value);

  *index = (*cur == '\0') ? cur : cur + 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t parseTelnetTagKvs(TAOS_SML_KV **pKVs, int *num_kvs,
                               const char **index,  char **childTableName,
                               SHashObj *pHash, SSmlLinesInfo* info) {
  const char *cur = *index;
  int32_t ret = TSDB_CODE_SUCCESS;
  TAOS_SML_KV *pkv;
  bool is_last_kv = false;

  int32_t capacity = 4;
  *pKVs = tcalloc(capacity, sizeof(TAOS_SML_KV));
  pkv = *pKVs;

  size_t childTableNameLen = strlen(tsSmlChildTableName);
  char childTbName[TSDB_TABLE_NAME_LEN + TS_BACKQUOTE_CHAR_SIZE] = {0};
  if (childTableNameLen != 0) {
    memcpy(childTbName, tsSmlChildTableName, childTableNameLen);
    addEscapeCharToString(childTbName, (int32_t)(childTableNameLen));
  }
  while (*cur != '\0') {
    ret = parseTelnetTagKey(pkv, &cur, pHash, info);
    if (ret) {
      tscError("OTD:0x%"PRIx64" Unable to parse key", info->id);
      return ret;
    }
    ret = parseTelnetTagValue(pkv, &cur, &is_last_kv, info);
    if (ret) {
      tscError("OTD:0x%"PRIx64" Unable to parse value", info->id);
      return ret;
    }
    if (childTableNameLen != 0 && strcasecmp(pkv->key, childTbName) == 0) {
      *childTableName = tcalloc(pkv->length + TS_BACKQUOTE_CHAR_SIZE + 1, 1);
      memcpy(*childTableName, pkv->value, pkv->length);
      (*childTableName)[pkv->length] = '\0';
      addEscapeCharToString(*childTableName, pkv->length);
      tfree(pkv->key);
      tfree(pkv->value);
    } else {
      *num_kvs += 1;
    }

    if (is_last_kv) {
      break;
    }

    //reallocate addtional memory for more kvs
    if ((*num_kvs + 1) > capacity) {
      TAOS_SML_KV *more_kvs = NULL;
      capacity *= 3; capacity /= 2;
      more_kvs = realloc(*pKVs, capacity * sizeof(TAOS_SML_KV));
      if (!more_kvs) {
        return TSDB_CODE_TSC_OUT_OF_MEMORY;
      }
      *pKVs = more_kvs;
    }

    //move pKV points to next TAOS_SML_KV block
    pkv = *pKVs + *num_kvs;
  }

  return ret;
}

static int32_t tscParseTelnetLine(const char* line, TAOS_SML_DATA_POINT* smlData, SSmlLinesInfo* info) {
  const char* index = line;
  int32_t ret = TSDB_CODE_SUCCESS;

  //Parse metric
  ret = parseTelnetMetric(smlData, &index, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse metric", info->id);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse metric finished", info->id);

  //Parse timestamp
  ret = parseTelnetTimeStamp(&smlData->fields, &smlData->fieldNum, &index, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse timestamp", info->id);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse timestamp finished", info->id);

  //Parse value
  ret = parseTelnetMetricValue(&smlData->fields, &smlData->fieldNum, &index, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse metric value", info->id);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse metric value finished", info->id);

  //Parse tagKVs
  SHashObj *keyHashTable = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  ret = parseTelnetTagKvs(&smlData->tags, &smlData->tagNum, &index, &smlData->childTableName, keyHashTable, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse tags", info->id);
    taosHashCleanup(keyHashTable);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse tags finished", info->id);
  taosHashCleanup(keyHashTable);


  return TSDB_CODE_SUCCESS;
}

static int32_t tscParseTelnetLines(char* lines[], int numLines, SArray* points, SArray* failedLines, SSmlLinesInfo* info) {
  for (int32_t i = 0; i < numLines; ++i) {
    TAOS_SML_DATA_POINT point = {0};
    int32_t code = tscParseTelnetLine(lines[i], &point, info);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("OTD:0x%"PRIx64" data point line parse failed. line %d : %s", info->id, i, lines[i]);
      destroySmlDataPoint(&point);
      return code;
    } else {
      tscDebug("OTD:0x%"PRIx64" data point line parse success. line %d", info->id, i);
    }

    taosArrayPush(points, &point);
  }
  return TSDB_CODE_SUCCESS;
}

int taos_insert_telnet_lines(TAOS* taos, char* lines[], int numLines, SMLProtocolType protocol, SMLTimeStampType tsType, int* affectedRows) {
  int32_t code = 0;

  SSmlLinesInfo* info = tcalloc(1, sizeof(SSmlLinesInfo));
  info->id = genUID();
  info->tsType = tsType;
  info->protocol = protocol;

  if (numLines <= 0 || numLines > 65536) {
    tscError("OTD:0x%"PRIx64" taos_insert_telnet_lines numLines should be between 1 and 65536. numLines: %d", info->id, numLines);
    tfree(info);
    code = TSDB_CODE_TSC_APP_ERROR;
    return code;
  }

  for (int i = 0; i < numLines; ++i) {
    if (lines[i] == NULL) {
      tscError("OTD:0x%"PRIx64" taos_insert_telnet_lines line %d is NULL", info->id, i);
      tfree(info);
      code = TSDB_CODE_TSC_APP_ERROR;
      return code;
    }
  }

  SArray* lpPoints = taosArrayInit(numLines, sizeof(TAOS_SML_DATA_POINT));
  if (lpPoints == NULL) {
    tscError("OTD:0x%"PRIx64" taos_insert_telnet_lines failed to allocate memory", info->id);
    tfree(info);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscDebug("OTD:0x%"PRIx64" taos_insert_telnet_lines begin inserting %d lines, first line: %s", info->id, numLines, lines[0]);
  code = tscParseTelnetLines(lines, numLines, lpPoints, NULL, info);
  size_t numPoints = taosArrayGetSize(lpPoints);

  if (code != 0) {
    goto cleanup;
  }

  TAOS_SML_DATA_POINT* points = TARRAY_GET_START(lpPoints);
  code = tscSmlInsert(taos, points, (int)numPoints, info);
  if (code != 0) {
    tscError("OTD:0x%"PRIx64" taos_insert_telnet_lines error: %s", info->id, tstrerror((code)));
  }
  if (affectedRows != NULL) {
    *affectedRows = info->affectedRows;
  }

cleanup:
  tscDebug("OTD:0x%"PRIx64" taos_insert_telnet_lines finish inserting %d lines. code: %d", info->id, numLines, code);
  points = TARRAY_GET_START(lpPoints);
  numPoints = taosArrayGetSize(lpPoints);
  for (int i = 0; i < numPoints; ++i) {
    destroySmlDataPoint(points+i);
  }

  taosArrayDestroy(&lpPoints);

  tfree(info);
  return code;
}

int taos_telnet_insert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint) {
  SSmlLinesInfo* info = tcalloc(1, sizeof(SSmlLinesInfo));
  info->id = genUID();
  int code = tscSmlInsert(taos, points, numPoint, info);
  tfree(info);
  return code;
}


/* telnet style API parser */
static int32_t parseMetricFromJSON(cJSON *root, TAOS_SML_DATA_POINT* pSml, SSmlLinesInfo* info) {
  cJSON *metric = cJSON_GetObjectItem(root, "metric");
  if (!cJSON_IsString(metric)) {
    return  TSDB_CODE_TSC_INVALID_JSON;
  }

  size_t stableLen = strlen(metric->valuestring);
  if (stableLen > TSDB_TABLE_NAME_LEN - 1) {
      tscError("OTD:0x%"PRIx64" Metric cannot exceeds %d characters in JSON", info->id, TSDB_TABLE_NAME_LEN - 1);
      return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  pSml->stableName = tcalloc(stableLen + TS_BACKQUOTE_CHAR_SIZE + 1, sizeof(char));
  if (pSml->stableName == NULL){
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  /*
  if (isdigit(metric->valuestring[0])) {
    tscError("OTD:0x%"PRIx64" Metric cannot start with digit in JSON", info->id);
    tfree(pSml->stableName);
    return TSDB_CODE_TSC_INVALID_JSON;
  }
  */

  tstrncpy(pSml->stableName, metric->valuestring, stableLen + 1);
  addEscapeCharToString(pSml->stableName, (int32_t)stableLen);

  return TSDB_CODE_SUCCESS;

}

static int32_t parseTimestampFromJSONObj(cJSON *root, int64_t *tsVal, SSmlLinesInfo* info) {
  int32_t size = cJSON_GetArraySize(root);
  if (size != OTD_JSON_SUB_FIELDS_NUM) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (!cJSON_IsNumber(value)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (!cJSON_IsString(type)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  *tsVal = strtoll(value->numberstring, NULL, 10);
  //if timestamp value is 0 use current system time
  if (*tsVal == 0) {
    *tsVal = taosGetTimestampNs();
    return TSDB_CODE_SUCCESS;
  }

  size_t typeLen = strlen(type->valuestring);
  if (typeLen == 1 && type->valuestring[0] == 's') {
    //seconds
    *tsVal = (int64_t)(*tsVal * 1e9);
  } else if (typeLen == 2 && type->valuestring[1] == 's') {
    switch (type->valuestring[0]) {
      case 'm':
        //milliseconds
        *tsVal = convertTimePrecision(*tsVal, TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_NANO);
        break;
      case 'u':
        //microseconds
        *tsVal = convertTimePrecision(*tsVal, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO);
        break;
      case 'n':
        //nanoseconds
        *tsVal = *tsVal * 1;
        break;
      default:
        return TSDB_CODE_TSC_INVALID_JSON;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t parseTimestampFromJSON(cJSON *root, TAOS_SML_KV **pTS, int *num_kvs, SSmlLinesInfo* info) {
  //Timestamp must be the first KV to parse
  assert(*num_kvs == 0);
  int64_t tsVal;
  char key[] = OTD_TIMESTAMP_COLUMN_NAME;

  cJSON *timestamp = cJSON_GetObjectItem(root, "timestamp");
  if (cJSON_IsNumber(timestamp)) {
    //timestamp value 0 indicates current system time
    if (timestamp->valueint == 0) {
      tsVal = taosGetTimestampNs();
    } else {
      tsVal = strtoll(timestamp->numberstring, NULL, 10);
      size_t tsLen = strlen(timestamp->numberstring);
      if (tsLen == SML_TIMESTAMP_SECOND_DIGITS) {
        tsVal = (int64_t)(tsVal * 1e9);
      } else if (tsLen == SML_TIMESTAMP_MILLI_SECOND_DIGITS) {
        tsVal = convertTimePrecision(tsVal, TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_NANO);
      } else {
        return TSDB_CODE_TSC_INVALID_TIME_STAMP;
      }
    }
  } else if (cJSON_IsObject(timestamp)) {
    int32_t ret = parseTimestampFromJSONObj(timestamp, &tsVal, info);
    if (ret != TSDB_CODE_SUCCESS) {
      tscError("OTD:0x%"PRIx64" Failed to parse timestamp from JSON Obj", info->id);
      return ret;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  //allocate fields for timestamp and value
  *pTS = tcalloc(OTD_MAX_FIELDS_NUM, sizeof(TAOS_SML_KV));


  (*pTS)->key = tcalloc(sizeof(key), 1);
  memcpy((*pTS)->key, key, sizeof(key));

  (*pTS)->type = TSDB_DATA_TYPE_TIMESTAMP;
  (*pTS)->length = (int16_t)tDataTypes[(*pTS)->type].bytes;
  (*pTS)->value = tcalloc((*pTS)->length, 1);
  memcpy((*pTS)->value, &tsVal, (*pTS)->length);

  *num_kvs += 1;
  return TSDB_CODE_SUCCESS;

}

static int32_t convertJSONBool(TAOS_SML_KV *pVal, char* typeStr, int64_t valueInt, SSmlLinesInfo* info) {
  if (strcasecmp(typeStr, "bool") != 0) {
    tscError("OTD:0x%"PRIx64" invalid type(%s) for JSON Bool", info->id, typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->type = TSDB_DATA_TYPE_BOOL;
  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
  pVal->value = tcalloc(pVal->length, 1);
  *(bool *)(pVal->value) = valueInt ? true : false;

  return TSDB_CODE_SUCCESS;
}

static int32_t convertJSONNumber(TAOS_SML_KV *pVal, char* typeStr, cJSON *value, SSmlLinesInfo* info) {
  //tinyint
  if (strcasecmp(typeStr, "i8") == 0 ||
      strcasecmp(typeStr, "tinyint") == 0) {
    if (!IS_VALID_TINYINT(value->valueint)) {
      tscError("OTD:0x%"PRIx64" JSON value(%"PRId64") cannot fit in type(tinyint)", info->id, value->valueint);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = tcalloc(pVal->length, 1);
    *(int8_t *)(pVal->value) = (int8_t)(value->valueint);
    return TSDB_CODE_SUCCESS;
  }
  //smallint
  if (strcasecmp(typeStr, "i16") == 0 ||
      strcasecmp(typeStr, "smallint") == 0) {
    if (!IS_VALID_SMALLINT(value->valueint)) {
      tscError("OTD:0x%"PRIx64" JSON value(%"PRId64") cannot fit in type(smallint)", info->id, value->valueint);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = tcalloc(pVal->length, 1);
    *(int16_t *)(pVal->value) = (int16_t)(value->valueint);
    return TSDB_CODE_SUCCESS;
  }
  //int
  if (strcasecmp(typeStr, "i32") == 0 ||
      strcasecmp(typeStr, "int") == 0) {
    if (!IS_VALID_INT(value->valueint)) {
      tscError("OTD:0x%"PRIx64" JSON value(%"PRId64") cannot fit in type(int)", info->id, value->valueint);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = tcalloc(pVal->length, 1);
    *(int32_t *)(pVal->value) = (int32_t)(value->valueint);
    return TSDB_CODE_SUCCESS;
  }
  //bigint
  if (strcasecmp(typeStr, "i64") == 0 ||
      strcasecmp(typeStr, "bigint") == 0) {
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = tcalloc(pVal->length, 1);
    /* cJSON conversion of legit BIGINT may overflow,
     * use original string to do the conversion.
     */
    errno = 0;
    int64_t val = (int64_t)strtoll(value->numberstring, NULL, 10);
    if (errno == ERANGE || !IS_VALID_BIGINT(val)) {
      tscError("OTD:0x%"PRIx64" JSON value(%s) cannot fit in type(bigint)", info->id, value->numberstring);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    *(int64_t *)(pVal->value) = val;
    return TSDB_CODE_SUCCESS;
  }
  //float
  if (strcasecmp(typeStr, "f32") == 0 ||
      strcasecmp(typeStr, "float") == 0) {
    if (!IS_VALID_FLOAT(value->valuedouble)) {
      tscError("OTD:0x%"PRIx64" JSON value(%f) cannot fit in type(float)", info->id, value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = tcalloc(pVal->length, 1);
    *(float *)(pVal->value) = (float)(value->valuedouble);
    return TSDB_CODE_SUCCESS;
  }
  //double
  if (strcasecmp(typeStr, "f64") == 0 ||
      strcasecmp(typeStr, "double") == 0) {
    if (!IS_VALID_DOUBLE(value->valuedouble)) {
      tscError("OTD:0x%"PRIx64" JSON value(%f) cannot fit in type(double)", info->id, value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = tcalloc(pVal->length, 1);
    *(double *)(pVal->value) = (double)(value->valuedouble);
    return TSDB_CODE_SUCCESS;
  }

  //if reach here means type is unsupported
  tscError("OTD:0x%"PRIx64" invalid type(%s) for JSON Number", info->id, typeStr);
  return TSDB_CODE_TSC_INVALID_JSON_TYPE;
}

static int32_t convertJSONString(TAOS_SML_KV *pVal, char* typeStr, cJSON *value, SSmlLinesInfo* info) {
  if (strcasecmp(typeStr, "binary") == 0) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
  } else if (strcasecmp(typeStr, "nchar") == 0) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
  } else {
    tscError("OTD:0x%"PRIx64" invalid type(%s) for JSON String", info->id, typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->length = (int16_t)strlen(value->valuestring);
  pVal->value = tcalloc(pVal->length + 1, 1);
  memcpy(pVal->value, value->valuestring, pVal->length);
  return TSDB_CODE_SUCCESS;
}

static int32_t parseValueFromJSONObj(cJSON *root, TAOS_SML_KV *pVal, SSmlLinesInfo* info) {
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
      ret = convertJSONBool(pVal, type->valuestring, value->valueint, info);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_Number: {
      ret = convertJSONNumber(pVal, type->valuestring, value, info);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_String: {
      ret = convertJSONString(pVal, type->valuestring, value, info);
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

static int32_t parseValueFromJSON(cJSON *root, TAOS_SML_KV *pVal, SSmlLinesInfo* info) {
  int type = root->type;

  switch (type) {
    case cJSON_True:
    case cJSON_False: {
      pVal->type = TSDB_DATA_TYPE_BOOL;
      pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
      pVal->value = tcalloc(pVal->length, 1);
      *(bool *)(pVal->value) = root->valueint ? true : false;
      break;
    }
    case cJSON_Number: {
      //convert default JSON Number type to BIGINT/DOUBLE
      //if (isValidInteger(root->numberstring)) {
      //  pVal->type = TSDB_DATA_TYPE_BIGINT;
      //  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
      //  pVal->value = tcalloc(pVal->length, 1);
      //  /* cJSON conversion of legit BIGINT may overflow,
      //   * use original string to do the conversion.
      //   */
      //  errno = 0;
      //  int64_t val = (int64_t)strtoll(root->numberstring, NULL, 10);
      //  if (errno == ERANGE || !IS_VALID_BIGINT(val)) {
      //    tscError("OTD:0x%"PRIx64" JSON value(%s) cannot fit in type(bigint)", info->id, root->numberstring);
      //    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
      //  }
      //  *(int64_t *)(pVal->value) = val;
      //} else if (isValidFloat(root->numberstring)) {
      //  pVal->type = TSDB_DATA_TYPE_DOUBLE;
      //  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
      //  pVal->value = tcalloc(pVal->length, 1);
      //  *(double *)(pVal->value) = (double)(root->valuedouble);
      //} else {
      //  return TSDB_CODE_TSC_INVALID_JSON_TYPE;
      //}
      if (isValidInteger(root->numberstring) || isValidFloat(root->numberstring)) {
        pVal->type = TSDB_DATA_TYPE_DOUBLE;
        pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
        pVal->value = tcalloc(pVal->length, 1);
        *(double *)(pVal->value) = (double)(root->valuedouble);
      }

      break;
    }
    case cJSON_String: {
      /* set default JSON type to binary/nchar according to
       * user configured parameter tsDefaultJSONStrType
       */
      if (strcasecmp(tsDefaultJSONStrType, "binary") == 0) {
        pVal->type = TSDB_DATA_TYPE_BINARY;
      } else if (strcasecmp(tsDefaultJSONStrType, "nchar") == 0) {
        pVal->type = TSDB_DATA_TYPE_NCHAR;
      } else {
        tscError("OTD:0x%"PRIx64" Invalid default JSON string type set from config %s", info->id, tsDefaultJSONStrType);
        return TSDB_CODE_TSC_INVALID_JSON_CONFIG;
      }
      //pVal->length = wcslen((wchar_t *)root->valuestring) * TSDB_NCHAR_SIZE;
      pVal->length = (int16_t)strlen(root->valuestring);
      pVal->value = tcalloc(pVal->length + 1, 1);
      memcpy(pVal->value, root->valuestring, pVal->length);
      break;
    }
    case cJSON_Object: {
      int32_t ret = parseValueFromJSONObj(root, pVal, info);
      if (ret != TSDB_CODE_SUCCESS) {
        tscError("OTD:0x%"PRIx64" Failed to parse timestamp from JSON Obj", info->id);
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t parseMetricValueFromJSON(cJSON *root, TAOS_SML_KV **pKVs, int *num_kvs, SSmlLinesInfo* info) {
  //skip timestamp
  TAOS_SML_KV *pVal = *pKVs + 1;
  char key[] = OTD_METRIC_VALUE_COLUMN_NAME;

  cJSON *metricVal = cJSON_GetObjectItem(root, "value");
  if (metricVal == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  int32_t ret = parseValueFromJSON(metricVal, pVal, info);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  pVal->key = tcalloc(sizeof(key) + TS_BACKQUOTE_CHAR_SIZE, 1);
  memcpy(pVal->key, key, sizeof(key));
  addEscapeCharToString(pVal->key, (int32_t)strlen(pVal->key));

  *num_kvs += 1;
  return TSDB_CODE_SUCCESS;

}


static int32_t parseTagsFromJSON(cJSON *root, TAOS_SML_KV **pKVs, int *num_kvs, char **childTableName,
                                 SHashObj *pHash, SSmlLinesInfo* info) {
  int32_t ret = TSDB_CODE_SUCCESS;

  cJSON *tags = cJSON_GetObjectItem(root, "tags");
  if (tags == NULL || tags->type != cJSON_Object) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  //handle child table name
  size_t childTableNameLen = strlen(tsSmlChildTableName);
  char childTbName[TSDB_TABLE_NAME_LEN] = {0};
  if (childTableNameLen != 0) {
    memcpy(childTbName, tsSmlChildTableName, childTableNameLen);
    cJSON *id = cJSON_GetObjectItem(tags, childTbName);
    if (id != NULL) {
      if (!cJSON_IsString(id)) {
        tscError("OTD:0x%"PRIx64" ID must be JSON string", info->id);
        return TSDB_CODE_TSC_INVALID_JSON;
      }
      size_t idLen = strlen(id->valuestring);
      *childTableName = tcalloc(idLen + TS_BACKQUOTE_CHAR_SIZE + 1, sizeof(char));
      memcpy(*childTableName, id->valuestring, idLen);
      addEscapeCharToString(*childTableName, (int32_t)idLen);

      //check duplicate IDs
      cJSON_DeleteItemFromObject(tags, childTbName);
      id = cJSON_GetObjectItem(tags, childTbName);
      if (id != NULL) {
        return TSDB_CODE_TSC_DUP_TAG_NAMES;
      }
    }
  }

  int32_t tagNum = cJSON_GetArraySize(tags);
  //at least one tag pair required
  if (tagNum <= 0) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  //allocate memory for tags
  *pKVs = tcalloc(tagNum, sizeof(TAOS_SML_KV));
  TAOS_SML_KV *pkv = *pKVs;

  for (int32_t i = 0; i < tagNum; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      return TSDB_CODE_TSC_INVALID_JSON;
    }
    //check duplicate keys
    if (checkDuplicateKey(tag->string, pHash, info)) {
      return TSDB_CODE_TSC_DUP_TAG_NAMES;
    }
    //key
    size_t keyLen = strlen(tag->string);
    if (keyLen > TSDB_COL_NAME_LEN - 1) {
      tscError("OTD:0x%"PRIx64" Tag key cannot exceeds %d characters in JSON", info->id, TSDB_COL_NAME_LEN - 1);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    pkv->key = tcalloc(keyLen + TS_BACKQUOTE_CHAR_SIZE + 1, sizeof(char));
    strncpy(pkv->key, tag->string, keyLen);
    addEscapeCharToString(pkv->key, (int32_t)keyLen);
    //value
    ret = parseValueFromJSON(tag, pkv, info);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
    *num_kvs += 1;
    pkv++;

  }

  return ret;

}

static int32_t tscParseJSONPayload(cJSON *root, TAOS_SML_DATA_POINT* pSml, SSmlLinesInfo* info) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (!cJSON_IsObject(root)) {
    tscError("OTD:0x%"PRIx64" data point needs to be JSON object", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  int32_t size = cJSON_GetArraySize(root);
  //outmost json fields has to be exactly 4
  if (size != OTD_JSON_FIELDS_NUM) {
    tscError("OTD:0x%"PRIx64" Invalid number of JSON fields in data point %d", info->id, size);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  //Parse metric
  ret = parseMetricFromJSON(root, pSml, info);
  if (ret != TSDB_CODE_SUCCESS) {
    tscError("OTD:0x%"PRIx64" Unable to parse metric from JSON payload", info->id);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse metric from JSON payload finished", info->id);

  //Parse timestamp
  ret = parseTimestampFromJSON(root, &pSml->fields, &pSml->fieldNum, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse timestamp from JSON payload", info->id);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse timestamp from JSON payload finished", info->id);

  //Parse metric value
  ret = parseMetricValueFromJSON(root, &pSml->fields, &pSml->fieldNum, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse metric value from JSON payload", info->id);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse metric value from JSON payload finished", info->id);

  //Parse tags
  SHashObj *keyHashTable = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  ret = parseTagsFromJSON(root, &pSml->tags, &pSml->tagNum, &pSml->childTableName, keyHashTable, info);
  if (ret) {
    tscError("OTD:0x%"PRIx64" Unable to parse tags from JSON payload", info->id);
    taosHashCleanup(keyHashTable);
    return ret;
  }
  tscDebug("OTD:0x%"PRIx64" Parse tags from JSON payload finished", info->id);
  taosHashCleanup(keyHashTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t tscParseMultiJSONPayload(char* payload, SArray* points, SSmlLinesInfo* info) {
  int32_t payloadNum, ret;
  ret = TSDB_CODE_SUCCESS;

  if (payload == NULL) {
    tscError("OTD:0x%"PRIx64" empty JSON Payload", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *root = cJSON_Parse(payload);
  //multiple data points must be sent in JSON array
  if (cJSON_IsObject(root)) {
    payloadNum = 1;
  } else if (cJSON_IsArray(root)) {
    payloadNum = cJSON_GetArraySize(root);
  } else {
    tscError("OTD:0x%"PRIx64" Invalid JSON Payload", info->id);
    ret = TSDB_CODE_TSC_INVALID_JSON;
    goto PARSE_JSON_OVER;
  }

  for (int32_t i = 0; i < payloadNum; ++i) {
    TAOS_SML_DATA_POINT point = {0};
    cJSON *dataPoint = (payloadNum == 1 && cJSON_IsObject(root)) ? root : cJSON_GetArrayItem(root, i);

    ret = tscParseJSONPayload(dataPoint, &point, info);
    if (ret != TSDB_CODE_SUCCESS) {
      tscError("OTD:0x%"PRIx64" JSON data point parse failed", info->id);
      destroySmlDataPoint(&point);
      goto PARSE_JSON_OVER;
    } else {
      tscDebug("OTD:0x%"PRIx64" JSON data point parse success", info->id);
    }
    taosArrayPush(points, &point);
  }

PARSE_JSON_OVER:
  cJSON_Delete(root);
  return ret;
}

int taos_insert_json_payload(TAOS* taos, char* payload, SMLProtocolType protocol, SMLTimeStampType tsType, int* affectedRows) {
  int32_t code = 0;

  SSmlLinesInfo* info = tcalloc(1, sizeof(SSmlLinesInfo));
  info->id = genUID();
  info->tsType = tsType;
  info->protocol = protocol;

  if (payload == NULL) {
    tscError("OTD:0x%"PRIx64" taos_insert_json_payload payload is NULL", info->id);
    tfree(info);
    code = TSDB_CODE_TSC_APP_ERROR;
    return code;
  }

  SArray* lpPoints = taosArrayInit(1, sizeof(TAOS_SML_DATA_POINT));
  if (lpPoints == NULL) {
    tscError("OTD:0x%"PRIx64" taos_insert_json_payload failed to allocate memory", info->id);
    tfree(info);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscDebug("OTD:0x%"PRIx64" taos_insert_telnet_lines begin inserting %d points", info->id, 1);
  code = tscParseMultiJSONPayload(payload, lpPoints, info);
  size_t numPoints = taosArrayGetSize(lpPoints);

  if (code != 0) {
    goto cleanup;
  }

  TAOS_SML_DATA_POINT* points = TARRAY_GET_START(lpPoints);
  code = tscSmlInsert(taos, points, (int)numPoints, info);
  if (code != 0) {
    tscError("OTD:0x%"PRIx64" taos_insert_json_payload error: %s", info->id, tstrerror((code)));
  }
  if (affectedRows != NULL) {
    *affectedRows = info->affectedRows;
  }

cleanup:
  tscDebug("OTD:0x%"PRIx64" taos_insert_json_payload finish inserting 1 Point. code: %d", info->id, code);
  points = TARRAY_GET_START(lpPoints);
  numPoints = taosArrayGetSize(lpPoints);
  for (int i = 0; i < numPoints; ++i) {
    destroySmlDataPoint(points+i);
  }

  taosArrayDestroy(&lpPoints);

  tfree(info);
  return code;
}
