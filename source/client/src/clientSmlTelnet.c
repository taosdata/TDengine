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

int32_t is_same_child_table_telnet(const void *a, const void *b){
  SSmlLineInfo *t1 = (SSmlLineInfo *)a;
  SSmlLineInfo *t2 = (SSmlLineInfo *)b;
  return (((t1->measureLen == t2->measureLen) && memcmp(t1->measure, t2->measure, t1->measureLen) == 0)
          && ((t1->tagsLen == t2->tagsLen) && memcmp(t1->tags, t2->tags, t1->tagsLen) == 0)) ? 0 : 1;
}

int64_t smlParseOpenTsdbTime(SSmlHandle *info, const char *data, int32_t len) {
  uint8_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;

  if (unlikely(!data)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
    return -1;
  }
  if (unlikely(len == 1 && data[0] == '0')) {
    return taosGetTimestampNs()/smlFactorNS[toPrecision];
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

static int32_t smlParseTelnetTags(SSmlHandle *info, char *data, char *sqlEnd, SSmlLineInfo *elements, SSmlMsgBuf *msg) {
  if(is_same_child_table_telnet(elements, &info->preLine) == 0){
    return TSDB_CODE_SUCCESS;
  }

  bool isSameMeasure = IS_SAME_SUPER_TABLE;

  int     cnt = 0;
  SArray *preLineKV = info->preLineTagKV;
  bool    isSuperKVInit = true;
  SArray *superKV = NULL;
  if(info->dataFormat){
    if(!isSameMeasure){
      SSmlSTableMeta *sMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);

      if(unlikely(sMeta == NULL)){
        sMeta = smlBuildSTableMeta(info->dataFormat);
        STableMeta * pTableMeta = smlGetMeta(info, elements->measure, elements->measureLen);
        sMeta->tableMeta = pTableMeta;
        if(pTableMeta == NULL){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        nodeListSet(&info->superTables, elements->measure, elements->measureLen, sMeta, NULL);
      }
      info->currSTableMeta = sMeta->tableMeta;
      superKV = sMeta->tags;

      if(unlikely(taosArrayGetSize(superKV) == 0)){
        isSuperKVInit = false;
      }
      taosArraySetSize(preLineKV, 0);
    }
  }else{
    taosArraySetSize(preLineKV, 0);
  }

  const char *sql = data;
  while (sql < sqlEnd) {
    JUMP_SPACE(sql, sqlEnd)
    if (unlikely(*sql == '\0')) break;

    const char *key = sql;
    size_t     keyLen = 0;

    // parse key
    while (sql < sqlEnd) {
      if (unlikely(*sql == SPACE)) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (unlikely(*sql == EQUAL)) {
        keyLen = sql - key;
        sql++;
        break;
      }
      sql++;
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
//    if (smlCheckDuplicateKey(key, keyLen, dumplicateKey)) {
//      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
//      return TSDB_CODE_TSC_DUP_NAMES;
//    }

    // parse value
    const char *value = sql;
    size_t     valueLen = 0;
    while (sql < sqlEnd) {
      // parse value
      if (unlikely(*sql == SPACE)) {
        break;
      }
      if (unlikely(*sql == EQUAL)) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      sql++;
    }
    valueLen = sql - value;

    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    if (unlikely(valueLen > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE)) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }

    SSmlKv kv = {.key = key, .keyLen = keyLen, .type = TSDB_DATA_TYPE_NCHAR, .value = value, .length = valueLen};

    if(info->dataFormat){
      if(unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)){
        info->dataFormat = false;
        info->reRun      = true;
        return TSDB_CODE_SUCCESS;
      }

      if(isSameMeasure){
        if(unlikely(cnt >= taosArrayGetSize(preLineKV))) {
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        SSmlKv *preKV = (SSmlKv *)taosArrayGet(preLineKV, cnt);
        if(unlikely(kv.length > preKV->length)){
          preKV->length = kv.length;
          SSmlSTableMeta *tableMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);
          ASSERT(tableMeta != NULL);

          SSmlKv *oldKV = (SSmlKv *)taosArrayGet(tableMeta->tags, cnt);
          oldKV->length = kv.length;
          info->needModifySchema = true;
        }
        if(unlikely(!IS_SAME_KEY)){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
      }else{
        if(isSuperKVInit){
          if(unlikely(cnt >= taosArrayGetSize(superKV))) {
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
          SSmlKv *preKV = (SSmlKv *)taosArrayGet(superKV, cnt);
          if(unlikely(kv.length > preKV->length)) {
            preKV->length = kv.length;
          }else{
            kv.length = preKV->length;
          }
          info->needModifySchema = true;

          if(unlikely(!IS_SAME_KEY)){
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
        }else{
          taosArrayPush(superKV, &kv);
        }
        taosArrayPush(preLineKV, &kv);
      }
    }else{
      taosArrayPush(preLineKV, &kv);
    }
    cnt++;
  }
  SSmlTableInfo *tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements, POINTER_BYTES, is_same_child_table_telnet);
  if (unlikely(tinfo == NULL)) {
    tinfo = smlBuildTableInfo(1, elements->measure, elements->measureLen);
    if (!tinfo) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tinfo->tags = taosArrayDup(preLineKV, NULL);

    smlSetCTableName(tinfo);
    if (info->dataFormat) {
      info->currSTableMeta->uid = tinfo->uid;
      tinfo->tableDataCtx = smlInitTableDataCtx(info->pQuery, info->currSTableMeta);
      if (tinfo->tableDataCtx == NULL) {
        smlBuildInvalidDataMsg(&info->msgBuf, "smlInitTableDataCtx error", NULL);
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }

    SSmlLineInfo *key = (SSmlLineInfo *)taosMemoryMalloc(sizeof(SSmlLineInfo));
    *key = *elements;
    tinfo->key = key;
    nodeListSet(&info->childTables, key, POINTER_BYTES, tinfo, is_same_child_table_telnet);
  }
  if (info->dataFormat) info->currTableDataCtx = tinfo->tableDataCtx;

  return TSDB_CODE_SUCCESS;
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

  bool needConverTime = false;  // get TS before parse tag(get meta), so need conver time
  if(info->dataFormat && info->currSTableMeta == NULL){
    needConverTime = true;
  }
  int64_t ts = smlParseOpenTsdbTime(info, elements->timestamp, elements->timestampLen);
  if (unlikely(ts < 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  SSmlKv kvTs = { .key = TS, .keyLen = TS_LEN, .type = TSDB_DATA_TYPE_TIMESTAMP, .i = ts, .length = (size_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};

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

  int ret = smlParseTelnetTags(info, sql, sqlEnd, elements, &info->msgBuf);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    return ret;
  }

  if(unlikely(info->reRun)){
    return TSDB_CODE_SUCCESS;
  }

  if(info->dataFormat){
    if(needConverTime) {
      kvTs.i = convertTimePrecision(kvTs.i, TSDB_TIME_PRECISION_NANO, info->currSTableMeta->tableInfo.precision);
    }
    ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kvTs, 0);
    if(ret == TSDB_CODE_SUCCESS){
      ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, 1);
    }
    if(ret == TSDB_CODE_SUCCESS){
      ret = smlBuildRow(info->currTableDataCtx);
    }
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "smlBuildCol error", NULL);
      return ret;
    }
  }else{
    if(elements->colArray == NULL){
      elements->colArray = taosArrayInit(16, sizeof(SSmlKv));
    }
    taosArrayPush(elements->colArray, &kvTs);
    taosArrayPush(elements->colArray, &kv);
  }
  info->preLine = *elements;

  return TSDB_CODE_SUCCESS;
}