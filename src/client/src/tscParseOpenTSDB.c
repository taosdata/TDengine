#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "taos.h"

#include "tscUtil.h"
#include "tsclient.h"
#include "tscLog.h"

#include "tscParseLine.h"

#define MAX_TELNET_FILEDS_NUM 2

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

  pSml->stableName = tcalloc(TSDB_TABLE_NAME_LEN + 1, 1);    // +1 to avoid 1772 line over write
  if (pSml->stableName == NULL){
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  if (isdigit(*cur)) {
    tscError("OTD:0x%"PRIx64" Metric cannnot start with digit", info->id);
    tfree(pSml->stableName);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  while (*cur != '\0') {
    if (len > TSDB_TABLE_NAME_LEN) {
      tscError("OTD:0x%"PRIx64" Metric cannot exceeds 193 characters", info->id);
      tfree(pSml->stableName);
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }

    if (*cur == ' ') {
      break;
    }

    pSml->stableName[len] = *cur;
    cur++;
    len++;
  }
  if (len == 0) {
    tfree(pSml->stableName);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  pSml->stableName[len] = '\0';
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
  char key[] = "_ts";
  char *value = NULL;

  start = cur = *index;
  //allocate fields for timestamp and value
  *pTS = tcalloc(MAX_TELNET_FILEDS_NUM, sizeof(TAOS_SML_KV));

  while(*cur != '\0') {
    if (*cur == ' ') {
      break;
    }
    cur++;
    len++;
  }

  if (len > 0) {
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

  (*pTS)->key = tcalloc(sizeof(key), 1);
  memcpy((*pTS)->key, key, sizeof(key));

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
  char key[] = "value";
  char *value = NULL;

  start = cur = *index;

  while(*cur != '\0') {
    if (*cur == ' ') {
      break;
    }
    cur++;
    len++;
  }

  if (len > 0) {
    value = tcalloc(len + 1, 1);
    memcpy(value, start, len);
  } else {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  if (!convertSmlValueType(pVal, value, len, info)) {
    tscError("OTD:0x%"PRIx64" Failed to convert metric value string(%s) to any type",
            info->id, value);
    tfree(value);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  tfree(value);

  pVal->key = tcalloc(sizeof(key), 1);
  memcpy(pVal->key, key, sizeof(key));
  *num_kvs += 1;

  *index = cur + 1;
  return ret;
}

static int32_t parseTelnetTagKey(TAOS_SML_KV *pKV, const char **index, SHashObj *pHash, SSmlLinesInfo* info) {
  const char *cur = *index;
  char key[TSDB_COL_NAME_LEN + 1];  // +1 to avoid key[len] over write
  uint16_t len = 0;

  //key field cannot start with digit
  if (isdigit(*cur)) {
    tscError("OTD:0x%"PRIx64" Tag key cannnot start with digit", info->id);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  while (*cur != '\0') {
    if (len > TSDB_COL_NAME_LEN) {
      tscError("OTD:0x%"PRIx64" Tag key cannot exceeds 65 characters", info->id);
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }
    if (*cur == '=') {
      break;
    }

    key[len] = *cur;
    cur++;
    len++;
  }
  if (len == 0) {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  key[len] = '\0';

  if (checkDuplicateKey(key, pHash, info)) {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  pKV->key = tcalloc(len + 1, 1);
  memcpy(pKV->key, key, len + 1);
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
    // ',' or '\0' identifies a value
    if (*cur == ',' || *cur == '\0') {
      // '\0' indicates end of value
      *is_last_kv = (*cur == '\0') ? true : false;
      break;
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
  if (!convertSmlValueType(pKV, value, len, info)) {
    tscError("OTD:0x%"PRIx64" Failed to convert sml value string(%s) to any type",
            info->id, value);
    //free previous alocated key field
    tfree(pKV->key);
    tfree(value);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
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
    if ((strcasecmp(pkv->key, "ID") == 0) && pkv->type == TSDB_DATA_TYPE_BINARY) {
      ret = isValidChildTableName(pkv->value, pkv->length);
      if (ret) {
        return ret;
      }
      *childTableName = malloc(pkv->length + 1);
      memcpy(*childTableName, pkv->value, pkv->length);
      (*childTableName)[pkv->length] = '\0';
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

int32_t tscParseTelnetLine(const char* line, TAOS_SML_DATA_POINT* smlData, SSmlLinesInfo* info) {
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

int32_t tscParseTelnetLines(char* lines[], int numLines, SArray* points, SArray* failedLines, SSmlLinesInfo* info) {
  for (int32_t i = 0; i < numLines; ++i) {
    TAOS_SML_DATA_POINT point = {0};
    int32_t code = tscParseTelnetLine(lines[i], &point, info);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("OTD:0x%"PRIx64" data point line parse failed. line %d : %s", info->id, i, lines[i]);
      destroySmlDataPoint(&point);
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    } else {
      tscDebug("OTD:0x%"PRIx64" data point line parse success. line %d", info->id, i);
    }

    taosArrayPush(points, &point);
  }
  return 0;
}

int taos_insert_telnet_lines(TAOS* taos, char* lines[], int numLines) {
  int32_t code = 0;

  SSmlLinesInfo* info = tcalloc(1, sizeof(SSmlLinesInfo));
  info->id = genUID();

  if (numLines <= 0 || numLines > 65536) {
    tscError("OTD:0x%"PRIx64" taos_insert_telnet_lines numLines should be between 1 and 65536. numLines: %d", info->id, numLines);
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

cleanup:
  tscDebug("OTD:0x%"PRIx64" taos_insert_telnet_lines finish inserting %d lines. code: %d", info->id, numLines, code);
  points = TARRAY_GET_START(lpPoints);
  numPoints = taosArrayGetSize(lpPoints);
  for (int i=0; i<numPoints; ++i) {
    destroySmlDataPoint(points+i);
  }

  taosArrayDestroy(lpPoints);

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
