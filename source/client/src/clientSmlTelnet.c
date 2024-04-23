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

int32_t is_same_child_table_telnet(const void *a, const void *b) {
  SSmlLineInfo *t1 = (SSmlLineInfo *)a;
  SSmlLineInfo *t2 = (SSmlLineInfo *)b;
  if (t1 == NULL || t2 == NULL || t1->measure == NULL || t2->measure == NULL || t1->tags == NULL || t2->tags == NULL)
    return 1;
  return (((t1->measureLen == t2->measureLen) && memcmp(t1->measure, t2->measure, t1->measureLen) == 0) &&
          ((t1->tagsLen == t2->tagsLen) && memcmp(t1->tags, t2->tags, t1->tagsLen) == 0))
             ? 0
             : 1;
}

int64_t smlParseOpenTsdbTime(SSmlHandle *info, const char *data, int32_t len) {
  uint8_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;

  if (unlikely(!data)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
    return -1;
  }
  if (unlikely(len == 1 && data[0] == '0')) {
    return taosGetTimestampNs() / smlFactorNS[toPrecision];
  }
  int8_t fromPrecision = smlGetTsTypeByLen(len);
  if (unlikely(fromPrecision == -1)) {
    smlBuildInvalidDataMsg(&info->msgBuf,
                           "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", data);
    return -1;
  }
  int64_t ts = smlGetTimeValue(data, len, fromPrecision, toPrecision);
  if (unlikely(ts == -1)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static void smlParseTelnetElement(char **sql, char *sqlEnd, char **data, int32_t *len) {
  while (*sql < sqlEnd) {
    if (unlikely((**sql != SPACE && !(*data)))) {
      *data = *sql;
    } else if (unlikely(**sql == SPACE && *data)) {
      *len = *sql - *data;
      break;
    }
    (*sql)++;
  }
}

static int32_t smlProcessTagTelnet(SSmlHandle *info, char *data, char *sqlEnd){
  SArray *preLineKV = info->preLineTagKV;
  taosArrayClearEx(preLineKV, freeSSmlKv);
  int     cnt = 0;

  const char *sql = data;
  while (sql < sqlEnd) {
    JUMP_SPACE(sql, sqlEnd)
    if (unlikely(*sql == '\0')) break;

    const char *key = sql;
    size_t      keyLen = 0;

    // parse key
    while (sql < sqlEnd) {
      if (unlikely(*sql == SPACE)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
        terrno = TSDB_CODE_SML_INVALID_DATA;
        return -1;
      }
      if (unlikely(*sql == EQUAL)) {
        keyLen = sql - key;
        sql++;
        break;
      }
      sql++;
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid key or key is too long than 64", key);
      terrno = TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
      return -1;
    }

    // parse value
    const char *value = sql;
    size_t      valueLen = 0;
    while (sql < sqlEnd) {
      // parse value
      if (unlikely(*sql == SPACE)) {
        break;
      }
      if (unlikely(*sql == EQUAL)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
        terrno = TSDB_CODE_SML_INVALID_DATA;
        return -1;
      }
      sql++;
    }
    valueLen = sql - value;

    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", value);
      terrno = TSDB_CODE_TSC_INVALID_VALUE;
      return -1;
    }

    if (unlikely(valueLen > (TSDB_MAX_TAGS_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE)) {
      terrno = TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
      return -1;
    }

    SSmlKv kv = {.key = key,
        .keyLen = keyLen,
        .type = TSDB_DATA_TYPE_NCHAR,
        .value = value,
        .length = valueLen,
        .keyEscaped = false,
        .valueEscaped = false};
    taosArrayPush(preLineKV, &kv);
    if (info->dataFormat && !isSmlTagAligned(info, cnt, &kv)) {
      terrno = TSDB_CODE_SUCCESS;
      return -1;
    }
    cnt++;
  }
  return 0;
}

static int32_t smlParseTelnetTags(SSmlHandle *info, char *data, char *sqlEnd, SSmlLineInfo *elements) {
  if (is_same_child_table_telnet(elements, &info->preLine) == 0) {
    elements->measureTag = info->preLine.measureTag;
    return TSDB_CODE_SUCCESS;
  }

  int32_t ret = 0;
  if(info->dataFormat){
    ret = smlProcessSuperTable(info, elements);
    if(ret != 0){
      return terrno;
    }
  }

  ret = smlProcessTagTelnet(info, data, sqlEnd);
  if(ret != 0){
    return terrno;
  }

  ret = smlJoinMeasureTag(elements);
  if(ret != 0){
    return ret;
  }

  return smlProcessChildTable(info, elements);
}

// format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
int32_t smlParseTelnetString(SSmlHandle *info, char *sql, char *sqlEnd, SSmlLineInfo *elements) {
  if (!sql) return TSDB_CODE_SML_INVALID_DATA;

  // parse metric
  smlParseTelnetElement(&sql, sqlEnd, &elements->measure, &elements->measureLen);
  if (unlikely((!(elements->measure) || IS_INVALID_TABLE_LEN(elements->measureLen)))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  // parse timestamp
  smlParseTelnetElement(&sql, sqlEnd, &elements->timestamp, &elements->timestampLen);
  if (unlikely(!elements->timestamp || elements->timestampLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  bool needConverTime = false;  // get TS before parse tag(get meta), so need convert time
  if (info->dataFormat && info->currSTableMeta == NULL) {
    needConverTime = true;
  }
  int64_t ts = smlParseOpenTsdbTime(info, elements->timestamp, elements->timestampLen);
  if (unlikely(ts < 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }

  // parse value
  smlParseTelnetElement(&sql, sqlEnd, &elements->cols, &elements->colsLen);
  if (unlikely(!elements->cols || elements->colsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", sql);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  SSmlKv kv = {.key = VALUE, .keyLen = VALUE_LEN, .value = elements->cols, .length = (size_t)elements->colsLen};
  if (smlParseValue(&kv, &info->msgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  JUMP_SPACE(sql, sqlEnd)

  elements->tags = sql;
  elements->tagsLen = sqlEnd - sql;
  if (unlikely(!elements->tags || elements->tagsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", sql);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  int ret = smlParseTelnetTags(info, sql, sqlEnd, elements);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    return ret;
  }

  if (unlikely(info->reRun)) {
    return TSDB_CODE_SUCCESS;
  }

  SSmlKv kvTs = {0};
  smlBuildTsKv(&kvTs, ts);
  if (needConverTime) {
    kvTs.i = convertTimePrecision(kvTs.i, TSDB_TIME_PRECISION_NANO, info->currSTableMeta->tableInfo.precision);
  }

  return smlParseEndTelnetJson(info, elements, &kvTs, &kv);
}