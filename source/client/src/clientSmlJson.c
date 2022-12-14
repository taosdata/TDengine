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

#define JUMP_JSON_SPACE(start,end) \
while(start < end){\
  if(unlikely(isspace(*start) == 0))\
    break;\
  else\
    start++;\
  }

static SArray *smlJsonParseTags(char *start, char *end){
  SArray *tags = taosArrayInit(4, sizeof(SSmlKv));
  while(start < end){
    SSmlKv kv = {0};
    kv.type = TSDB_DATA_TYPE_NCHAR;
    bool isInQuote = false;
    while(start < end){
      if(!isInQuote && *start == '"'){
        start++;
        kv.key = start;
        isInQuote = true;
        continue;
      }
      if(isInQuote && *start == '"'){
        kv.keyLen = start - kv.key;
        start++;
        break;
      }
      start++;
    }
    bool hasColon = false;
    while(start < end){
      if(!hasColon && *start == ':'){
        start++;
        hasColon = true;
        continue;
      }
      if(hasColon && kv.value == NULL && (isspace(*start) == 0 && *start != '"')){
        kv.value = start;
        start++;
        continue;
      }

      if(hasColon && kv.value != NULL && (*start == '"' || *start == ',' || *start == '}')){
        kv.length = start - kv.value;
        taosArrayPush(tags, &kv);
        start++;
        break;
      }
      start++;
    }
  }
  return tags;
}

static int32_t smlParseTagsFromJSON(SSmlHandle *info, SSmlLineInfo *elements) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if(is_same_child_table_telnet(elements, &info->preLine) == 0){
    return TSDB_CODE_SUCCESS;
  }

  bool isSameMeasure = IS_SAME_SUPER_TABLE;

  int     cnt = 0;
  SArray *preLineKV = info->preLineTagKV;
  bool    isSuperKVInit = true;
  SArray *superKV = NULL;
  if(info->dataFormat){
    if(unlikely(!isSameMeasure)){
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

  SArray *tags = smlJsonParseTags(elements->tags, elements->tags + elements->tagsLen);
  int32_t tagNum = taosArrayGetSize(tags);
  for (int32_t i = 0; i < tagNum; ++i) {
    SSmlKv kv = *(SSmlKv*)taosArrayGet(tags, i);

    if(info->dataFormat){
      if(unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)){
        info->dataFormat = false;
        info->reRun      = true;
        taosArrayDestroy(tags);
        return TSDB_CODE_SUCCESS;
      }

      if(isSameMeasure){
        if(unlikely(cnt >= taosArrayGetSize(preLineKV))) {
          info->dataFormat = false;
          info->reRun      = true;
          taosArrayDestroy(tags);
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
          taosArrayDestroy(tags);
          return TSDB_CODE_SUCCESS;
        }
      }else{
        if(isSuperKVInit){
          if(unlikely(cnt >= taosArrayGetSize(superKV))) {
            info->dataFormat = false;
            info->reRun      = true;
            taosArrayDestroy(tags);
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
            taosArrayDestroy(tags);
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
  taosArrayDestroy(tags);

  SSmlTableInfo *tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements, POINTER_BYTES, is_same_child_table_telnet);
  if (unlikely(tinfo == NULL)) {
    tinfo = smlBuildTableInfo(1, elements->measure, elements->measureLen);
    if (unlikely(!tinfo)) {
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

  return ret;
}

static char* smlJsonGetObj(char *payload){
  int   leftBracketCnt = 0;
  while(*payload) {
    if (*payload == '{') {
      leftBracketCnt++;
      payload++;
      continue;
    }
    if (*payload == '}') {
      leftBracketCnt--;
      payload++;
      if (leftBracketCnt == 0) {
        return payload;
      } else if (leftBracketCnt < 0) {
        return NULL;
      }
      continue;
    }
    payload++;
  }
  return NULL;
}

static void smlJsonParseObj(char *start, char *end, SSmlLineInfo *element){
  while(start < end){
    if(start[0]== '"' && start[1] == 'm' && start[2] == 'e' && start[3] == 't'
       && start[4] == 'r' &&  start[5] == 'i' && start[6] == 'c' && start[7] == '"'){

      start += 8;
      bool isInQuote = false;
      while(start < end){
        if(!isInQuote && *start == '"'){
          start++;
          element->measure = start;
          isInQuote = true;
          continue;
        }
        if(isInQuote && *start == '"'){
          element->measureLen = start - element->measure;
          start++;
          break;
        }
        start++;
      }
    }else if(start[0] == '"' && start[1] == 't' && start[2] == 'i' && start[3] == 'm'
             && start[4] == 'e' &&  start[5] == 's' && start[6] == 't'
             && start[7] == 'a' &&  start[8] == 'm' && start[9] == 'p' && start[10] == '"'){

      start += 11;
      bool hasColon = false;
      while(start < end){
        if(!hasColon && *start == ':'){
          start++;
          JUMP_JSON_SPACE(start,end)
          element->timestamp = start;
          hasColon = true;
          continue;
        }
        if(hasColon && (*start == ',' || *start == '}' || isspace(*start) != 0)){
          element->timestampLen = start - element->timestamp;
          start++;
          break;
        }
        start++;
      }
    }else if(start[0]== '"' && start[1] == 'v' && start[2] == 'a' && start[3] == 'l'
             && start[4] == 'u' &&  start[5] == 'e' && start[6] == '"'){

      start += 7;

      bool hasColon = false;
      while(start < end){
        if(!hasColon && *start == ':'){
          start++;
          JUMP_JSON_SPACE(start,end)
          element->cols = start;
          hasColon = true;
          continue;
        }
        if(hasColon && (*start == ',' || *start == '}' || isspace(*start) != 0)){
          element->colsLen = start - element->cols;
          start++;
          break;
        }
        start++;
      }
    }else if(start[0] == '"' && start[1] == 't' && start[2] == 'a' && start[3] == 'g'
             && start[4] == 's' && start[5] == '"'){
      start += 6;

      while(start < end){
        if(*start == ':'){
          start++;
          JUMP_JSON_SPACE(start,end)
          element->tags = start;
          element->tagsLen = smlJsonGetObj(start) - start;
          break;
        }
        start++;
      }
    }else{
      start++;
    }
  }
}

static int32_t smlParseJSONString(SSmlHandle *info, char *start, char *end, SSmlLineInfo *elements) {
  int32_t ret = TSDB_CODE_SUCCESS;

  smlJsonParseObj(start, end, elements);
  if(unlikely(elements->measure == NULL || elements->measureLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid measure data", start);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  if(unlikely(elements->tags == NULL || elements->tagsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid tags data", start);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  if(unlikely(elements->cols == NULL || elements->colsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid cols data", start);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  if(unlikely(elements->timestamp == NULL || elements->timestampLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp data", start);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  SSmlKv kv = {.key = VALUE, .keyLen = VALUE_LEN, .value = elements->cols, .length = (size_t)elements->colsLen};
  if (smlParseNumber(&kv, &info->msgBuf)) {
    kv.length = (int16_t)tDataTypes[kv.type].bytes;
  }else{
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  // Parse tags
  ret = smlParseTagsFromJSON(info, elements);
  if (unlikely(ret)) {
    uError("OTD:0x%" PRIx64 " Unable to parse tags from JSON payload", info->id);
    return ret;
  }

  if(unlikely(info->reRun)){
    return TSDB_CODE_SUCCESS;
  }

  // Parse timestamp
  // notice!!! put ts back to tag to ensure get meta->precision
  int64_t ts = smlParseOpenTsdbTime(info, elements->timestamp, elements->timestampLen);
  if (unlikely(ts < 0)) {
    uError("OTD:0x%" PRIx64 " Unable to parse timestamp from JSON payload", info->id);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  SSmlKv kvTs = { .key = TS, .keyLen = TS_LEN, .type = TSDB_DATA_TYPE_TIMESTAMP, .i = ts, .length = (size_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};

  if(info->dataFormat){
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

//#define JUMP_TO_QUOTE(sql) \
//  while (sql){                   \
//    if (unlikely(isspace(*(sql))) != 0) \
//      (sql)++;                   \
//    else                        \
//      break;                    \
//  }
//


int32_t smlParseJSON(SSmlHandle *info, char *payload) {
  int32_t payloadNum = 1 << 15;
  int32_t ret = TSDB_CODE_SUCCESS;

  int cnt = 0;
  char *dataPointStart = payload;
  char *dataPointEnd = NULL;
  while (1) {
    dataPointEnd = smlJsonGetObj(dataPointStart);
    if(dataPointEnd == NULL) break;

    if(info->dataFormat) {
      SSmlLineInfo element = {0};
      ret = smlParseJSONString(info, dataPointStart, dataPointEnd, &element);
    }else{
      if(cnt >= payloadNum){
        payloadNum = payloadNum << 1;
        void* tmp = taosMemoryRealloc(info->lines, payloadNum * sizeof(SSmlLineInfo));
        if(tmp != NULL){
          info->lines = (SSmlLineInfo*)tmp;
        }
      }
      ret = smlParseJSONString(info, dataPointStart, dataPointEnd, info->lines + cnt);
    }
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      uError("SML:0x%" PRIx64 " Invalid JSON Payload", info->id);
      return ret;
    }

    if(unlikely(info->reRun)){
      cnt = 0;
      dataPointStart = payload;
      info->lineNum = payloadNum;
      ret = smlClearForRerun(info);
      if(ret != TSDB_CODE_SUCCESS){
        return ret;
      }
      continue;
    }
    cnt++;
    dataPointStart = dataPointEnd;
  }
  info->lineNum = cnt;

  return TSDB_CODE_SUCCESS;
}
