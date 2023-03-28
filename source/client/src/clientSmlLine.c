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

// comma ,
// #define IS_SLASH_COMMA(sql) (*(sql) == COMMA && *((sql)-1) == SLASH)
#define IS_COMMA(sql) (*(sql) == COMMA && *((sql)-1) != SLASH)
// space
// #define IS_SLASH_SPACE(sql) (*(sql) == SPACE && *((sql)-1) == SLASH)
#define IS_SPACE(sql) (*(sql) == SPACE && *((sql)-1) != SLASH)
// equal =
// #define IS_SLASH_EQUAL(sql) (*(sql) == EQUAL && *((sql)-1) == SLASH)
#define IS_EQUAL(sql) (*(sql) == EQUAL && *((sql)-1) != SLASH)
// quote "
// #define IS_SLASH_QUOTE(sql) (*(sql) == QUOTE && *((sql)-1) == SLASH)
#define IS_QUOTE(sql) (*(sql) == QUOTE && *((sql)-1) != SLASH)
// SLASH
// #define IS_SLASH_SLASH(sql) (*(sql) == SLASH && *((sql)-1) == SLASH)

#define IS_SLASH_LETTER(sql)                                                                           \
  (*((sql)-1) == SLASH && (*(sql) == COMMA || *(sql) == SPACE || *(sql) == EQUAL || *(sql) == QUOTE || \
                           *(sql) == SLASH))  //  (IS_SLASH_COMMA(sql) || IS_SLASH_SPACE(sql) || IS_SLASH_EQUAL(sql) ||
                                              //  IS_SLASH_QUOTE(sql) || IS_SLASH_SLASH(sql))

#define MOVE_FORWARD_ONE(sql, len) (memmove((void *)((sql)-1), (sql), len))

#define PROCESS_SLASH(key, keyLen)           \
  for (int i = 1; i < keyLen; ++i) {         \
    if (IS_SLASH_LETTER(key + i)) {          \
      MOVE_FORWARD_ONE(key + i, keyLen - i); \
      i--;                                   \
      keyLen--;                              \
    }                                        \
  }

#define BINARY_ADD_LEN 2  // "binary"   2 means " "
#define NCHAR_ADD_LEN  3  // L"nchar"   3 means L" "

uint8_t smlPrecisionConvert[7] = {TSDB_TIME_PRECISION_NANO,    TSDB_TIME_PRECISION_HOURS, TSDB_TIME_PRECISION_MINUTES,
                                  TSDB_TIME_PRECISION_SECONDS, TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO,
                                  TSDB_TIME_PRECISION_NANO};

static int64_t smlParseInfluxTime(SSmlHandle *info, const char *data, int32_t len) {
  uint8_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;

  if (unlikely(len == 0 || (len == 1 && data[0] == '0'))) {
    return taosGetTimestampNs() / smlFactorNS[toPrecision];
  }

  uint8_t fromPrecision = smlPrecisionConvert[info->precision];

  int64_t ts = smlGetTimeValue(data, len, fromPrecision, toPrecision);
  if (unlikely(ts == -1)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

int32_t smlParseValue(SSmlKv *pVal, SSmlMsgBuf *msg) {
  if (pVal->value[0] == '"') {  // binary
    if (pVal->length >= 2 && pVal->value[pVal->length - 1] == '"') {
      pVal->type = TSDB_DATA_TYPE_BINARY;
      pVal->length -= BINARY_ADD_LEN;
      if (pVal->length > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
        return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
      }
      pVal->value += (BINARY_ADD_LEN - 1);
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pVal->value[0] == 'l' || pVal->value[0] == 'L') {  // nchar
    if (pVal->value[1] == '"' && pVal->value[pVal->length - 1] == '"' && pVal->length >= 3) {
      pVal->type = TSDB_DATA_TYPE_NCHAR;
      pVal->length -= NCHAR_ADD_LEN;
      if (pVal->length > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
      }
      pVal->value += (NCHAR_ADD_LEN - 1);
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pVal->value[0] == 't' || pVal->value[0] == 'T') {
    if (pVal->length == 1 ||
        (pVal->length == 4 && (pVal->value[1] == 'r' || pVal->value[1] == 'R') &&
         (pVal->value[2] == 'u' || pVal->value[2] == 'U') && (pVal->value[3] == 'e' || pVal->value[3] == 'E'))) {
      pVal->i = TSDB_TRUE;
      pVal->type = TSDB_DATA_TYPE_BOOL;
      pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pVal->value[0] == 'f' || pVal->value[0] == 'F') {
    if (pVal->length == 1 ||
        (pVal->length == 5 && (pVal->value[1] == 'a' || pVal->value[1] == 'A') &&
         (pVal->value[2] == 'l' || pVal->value[2] == 'L') && (pVal->value[3] == 's' || pVal->value[3] == 'S') &&
         (pVal->value[4] == 'e' || pVal->value[4] == 'E'))) {
      pVal->i = TSDB_FALSE;
      pVal->type = TSDB_DATA_TYPE_BOOL;
      pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  // number
  if (smlParseNumber(pVal, msg)) {
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_TSC_INVALID_VALUE;
}

static int32_t smlParseTagKv(SSmlHandle *info, char **sql, char *sqlEnd, SSmlLineInfo *currElement, bool isSameMeasure,
                             bool isSameCTable) {
  if (isSameCTable) {
    return TSDB_CODE_SUCCESS;
  }

  int     cnt = 0;
  SArray *preLineKV = info->preLineTagKV;
  if (info->dataFormat) {
    if (unlikely(!isSameMeasure)) {
      SSmlSTableMeta **tmp =
          (SSmlSTableMeta **)taosHashGet(info->superTables, currElement->measure, currElement->measureLen);

      SSmlSTableMeta *sMeta = NULL;
      if (unlikely(tmp == NULL)) {
        STableMeta *pTableMeta = smlGetMeta(info, currElement->measure, currElement->measureLen);
        if (pTableMeta == NULL) {
          info->dataFormat = false;
          info->reRun = true;
          return TSDB_CODE_SUCCESS;
        }
        sMeta = smlBuildSTableMeta(info->dataFormat);
        sMeta->tableMeta = pTableMeta;
        taosHashPut(info->superTables, currElement->measure, currElement->measureLen, &sMeta, POINTER_BYTES);
        for(int i = pTableMeta->tableInfo.numOfColumns; i < pTableMeta->tableInfo.numOfTags + pTableMeta->tableInfo.numOfColumns; i++){
          SSchema *tag = pTableMeta->schema + i;
          SSmlKv kv = {.key = tag->name, .keyLen = strlen(tag->name), .type = tag->type, .length = (tag->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE };
          taosArrayPush(sMeta->tags, &kv);
        }
        tmp = &sMeta;
      }
      info->currSTableMeta = (*tmp)->tableMeta;
      info->maxTagKVs = (*tmp)->tags;
    }
  }
  taosArrayClear(preLineKV);

  while (*sql < sqlEnd) {
    if (unlikely(IS_SPACE(*sql))) {
      break;
    }

    bool hasSlash = false;
    // parse key
    const char *key = *sql;
    size_t      keyLen = 0;
    while (*sql < sqlEnd) {
      if (unlikely(IS_COMMA(*sql))) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (unlikely(IS_EQUAL(*sql))) {
        keyLen = *sql - key;
        (*sql)++;
        break;
      }
      if (!hasSlash) {
        hasSlash = (*(*sql) == SLASH);
      }
      (*sql)++;
    }
    if (unlikely(hasSlash)) {
      PROCESS_SLASH(key, keyLen)
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }

    // parse value
    const char *value = *sql;
    size_t      valueLen = 0;
    hasSlash = false;
    while (*sql < sqlEnd) {
      // parse value
      if (unlikely(IS_SPACE(*sql) || IS_COMMA(*sql))) {
        break;
      } else if (unlikely(IS_EQUAL(*sql))) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }

      if (!hasSlash) {
        hasSlash = (*(*sql) == SLASH);
      }

      (*sql)++;
    }
    valueLen = *sql - value;

    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if (unlikely(hasSlash)) {
      PROCESS_SLASH(value, valueLen)
    }

    if (unlikely(valueLen > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE)) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }

    SSmlKv kv = {.key = key, .keyLen = keyLen, .type = TSDB_DATA_TYPE_NCHAR, .value = value, .length = valueLen};
    if (info->dataFormat) {
      if (unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }

      if (unlikely(cnt >= taosArrayGetSize(info->maxTagKVs))) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }
      SSmlKv *maxKV = (SSmlKv *)taosArrayGet(info->maxTagKVs, cnt);

      if (unlikely(!IS_SAME_KEY)) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }

      if (unlikely(kv.length > maxKV->length)) {
        maxKV->length = kv.length;
        info->needModifySchema = true;
      }
    }
    taosArrayPush(preLineKV, &kv);

    cnt++;
    if (IS_SPACE(*sql)) {
      break;
    }
    (*sql)++;
  }

  void *oneTable = taosHashGet(info->childTables, currElement->measure, currElement->measureTagsLen);
  if ((oneTable != NULL)) {
    return TSDB_CODE_SUCCESS;
  }

  SSmlTableInfo *tinfo = smlBuildTableInfo(1, currElement->measure, currElement->measureLen);
  if (unlikely(!tinfo)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tinfo->tags = taosArrayDup(preLineKV, NULL);

  smlSetCTableName(tinfo);
  tinfo->uid = info->uid++;
  if (info->dataFormat) {
    info->currSTableMeta->uid = tinfo->uid;
    tinfo->tableDataCtx = smlInitTableDataCtx(info->pQuery, info->currSTableMeta);
    if (tinfo->tableDataCtx == NULL) {
      smlDestroyTableInfo(info, tinfo);
      smlBuildInvalidDataMsg(&info->msgBuf, "smlInitTableDataCtx error", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }
  }

  taosHashPut(info->childTables, currElement->measure, currElement->measureTagsLen, &tinfo, POINTER_BYTES);

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseColKv(SSmlHandle *info, char **sql, char *sqlEnd, SSmlLineInfo *currElement, bool isSameMeasure,
                             bool isSameCTable) {
  int     cnt = 0;
  if (info->dataFormat) {
    if (unlikely(!isSameCTable)) {
      SSmlTableInfo **oneTable =
          (SSmlTableInfo **)taosHashGet(info->childTables, currElement->measure, currElement->measureTagsLen);
      if (unlikely(oneTable == NULL)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "child table should inside", currElement->measure);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      info->currTableDataCtx = (*oneTable)->tableDataCtx;
    }

    if (unlikely(!isSameMeasure)) {
      SSmlSTableMeta **tmp =
          (SSmlSTableMeta **)taosHashGet(info->superTables, currElement->measure, currElement->measureLen);
      if (unlikely(tmp == NULL)) {
        STableMeta *pTableMeta = smlGetMeta(info, currElement->measure, currElement->measureLen);
        if (pTableMeta == NULL) {
          info->dataFormat = false;
          info->reRun = true;
          return TSDB_CODE_SUCCESS;
        }
        *tmp = smlBuildSTableMeta(info->dataFormat);
        (*tmp)->tableMeta = pTableMeta;
        taosHashPut(info->superTables, currElement->measure, currElement->measureLen, tmp, POINTER_BYTES);

        for(int i = 0; i < pTableMeta->tableInfo.numOfColumns; i++){
          SSchema *tag = pTableMeta->schema + i;
          SSmlKv kv = {.key = tag->name, .keyLen = strlen(tag->name), .type = tag->type };
          if(tag->type == TSDB_DATA_TYPE_NCHAR){
            kv.length = (tag->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
          }else if(tag->type == TSDB_DATA_TYPE_BINARY){
            kv.length = tag->bytes - VARSTR_HEADER_SIZE;
          }
          taosArrayPush((*tmp)->cols, &kv);
        }
      }
      info->currSTableMeta = (*tmp)->tableMeta;
      info->masColKVs = (*tmp)->cols;
    }
  }

  while (*sql < sqlEnd) {
    if (unlikely(IS_SPACE(*sql))) {
      break;
    }

    bool hasSlash = false;
    // parse key
    const char *key = *sql;
    size_t      keyLen = 0;
    while (*sql < sqlEnd) {
      if (unlikely(IS_COMMA(*sql))) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (unlikely(IS_EQUAL(*sql))) {
        keyLen = *sql - key;
        (*sql)++;
        break;
      }
      if (!hasSlash) {
        hasSlash = (*(*sql) == SLASH);
      }
      (*sql)++;
    }
    if (unlikely(hasSlash)) {
      PROCESS_SLASH(key, keyLen)
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }

    // parse value
    const char *value = *sql;
    size_t      valueLen = 0;
    hasSlash = false;
    bool isInQuote = false;
    while (*sql < sqlEnd) {
      // parse value
      if (unlikely(IS_QUOTE(*sql))) {
        isInQuote = !isInQuote;
        (*sql)++;
        continue;
      }
      if (!isInQuote) {
        if (unlikely(IS_SPACE(*sql) || IS_COMMA(*sql))) {
          break;
        } else if (unlikely(IS_EQUAL(*sql))) {
          smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
          return TSDB_CODE_SML_INVALID_DATA;
        }
      }
      if (!hasSlash) {
        hasSlash = (*(*sql) == SLASH);
      }

      (*sql)++;
    }
    valueLen = *sql - value;

    if (unlikely(isInQuote)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "only one quote", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if (unlikely(hasSlash)) {
      PROCESS_SLASH(value, valueLen)
    }

    SSmlKv  kv = {.key = key, .keyLen = keyLen, .value = value, .length = valueLen};
    int32_t ret = smlParseValue(&kv, &info->msgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "smlParseValue error", value);
      return ret;
    }

    if (info->dataFormat) {
      // cnt begin 0, add ts so + 2
      if (unlikely(cnt + 2 > info->currSTableMeta->tableInfo.numOfColumns)) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }
      // bind data
      ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, cnt + 1);
      if (unlikely(ret != TSDB_CODE_SUCCESS)) {
        uDebug("smlBuildCol error, retry");
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }
      if (cnt >= taosArrayGetSize(info->masColKVs)) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }
      SSmlKv *maxKV = (SSmlKv *)taosArrayGet(info->masColKVs, cnt);
      if (kv.type != maxKV->type) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }
      if (unlikely(!IS_SAME_KEY)) {
        info->dataFormat = false;
        info->reRun = true;
        return TSDB_CODE_SUCCESS;
      }

      if (unlikely(IS_VAR_DATA_TYPE(kv.type) && kv.length > maxKV->length)) {
        maxKV->length = kv.length;
        info->needModifySchema = true;
      }
    } else {
      if (currElement->colArray == NULL) {
        currElement->colArray = taosArrayInit_s(sizeof(SSmlKv), 1);
      }
      taosArrayPush(currElement->colArray, &kv);  // reserve for timestamp
    }

    cnt++;
    if (IS_SPACE(*sql)) {
      break;
    }
    (*sql)++;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t smlParseInfluxString(SSmlHandle *info, char *sql, char *sqlEnd, SSmlLineInfo *elements) {
  if (!sql) return TSDB_CODE_SML_INVALID_DATA;
  JUMP_SPACE(sql, sqlEnd)
  if (unlikely(*sql == COMMA)) return TSDB_CODE_SML_INVALID_DATA;
  elements->measure = sql;

  // parse measure
  while (sql < sqlEnd) {
    if (unlikely((sql != elements->measure) && IS_SLASH_LETTER(sql))) {
      MOVE_FORWARD_ONE(sql, sqlEnd - sql);
      sqlEnd--;
      continue;
    }
    if (unlikely(IS_COMMA(sql))) {
      break;
    }

    if (unlikely(IS_SPACE(sql))) {
      break;
    }
    sql++;
  }
  elements->measureLen = sql - elements->measure;
  if (unlikely(IS_INVALID_TABLE_LEN(elements->measureLen))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "measure is empty or too large than 192", NULL);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  // to get measureTagsLen before
  const char *tmp = sql;
  while (tmp < sqlEnd) {
    if (unlikely(IS_SPACE(tmp))) {
      break;
    }
    tmp++;
  }
  elements->measureTagsLen = tmp - elements->measure;

  bool isSameCTable = false;
  bool isSameMeasure = false;
  if (IS_SAME_CHILD_TABLE) {
    isSameCTable = true;
    isSameMeasure = true;
  } else if (info->dataFormat) {
    isSameMeasure = IS_SAME_SUPER_TABLE;
  }
  // parse tag
  if (*sql == COMMA) sql++;
  elements->tags = sql;

  int ret = smlParseTagKv(info, &sql, sqlEnd, elements, isSameMeasure, isSameCTable);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    return ret;
  }
  if (unlikely(info->reRun)) {
    return TSDB_CODE_SUCCESS;
  }

  sql = elements->measure + elements->measureTagsLen;
  elements->tagsLen = sql - elements->tags;

  // parse cols
  JUMP_SPACE(sql, sqlEnd)
  elements->cols = sql;

  ret = smlParseColKv(info, &sql, sqlEnd, elements, isSameMeasure, isSameCTable);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    return ret;
  }

  if (unlikely(info->reRun)) {
    return TSDB_CODE_SUCCESS;
  }

  elements->colsLen = sql - elements->cols;
  if (unlikely(elements->colsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "cols is empty", NULL);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse timestamp
  JUMP_SPACE(sql, sqlEnd)
  elements->timestamp = sql;
  while (sql < sqlEnd) {
    if (unlikely(isspace(*sql))) {
      break;
    }
    sql++;
  }
  elements->timestampLen = sql - elements->timestamp;

  int64_t ts = smlParseInfluxTime(info, elements->timestamp, elements->timestampLen);
  if (unlikely(ts <= 0)) {
    uError("SML:0x%" PRIx64 " smlParseTS error:%" PRId64, info->id, ts);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  // add ts to
  SSmlKv kv = {.key = TS,
               .keyLen = TS_LEN,
               .type = TSDB_DATA_TYPE_TIMESTAMP,
               .i = ts,
               .length = (size_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};
  if (info->dataFormat) {
    uDebug("SML:0x%" PRIx64 " smlParseInfluxString format true, ts:%" PRId64, info->id, ts);
    ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, 0);
    if(ret != TSDB_CODE_SUCCESS){return ret;}
    ret = smlBuildRow(info->currTableDataCtx);
    if(ret != TSDB_CODE_SUCCESS){return ret;}
    clearColValArray(info->currTableDataCtx->pValues);
  } else {
    uDebug("SML:0x%" PRIx64 " smlParseInfluxString format false, ts:%" PRId64, info->id, ts);
    taosArraySet(elements->colArray, 0, &kv);
  }
  info->preLine = *elements;

  return ret;
}
