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

#define RETURN_FALSE                                 \
  smlBuildInvalidDataMsg(msg, "invalid data", pVal); \
  return false;

#define SET_DOUBLE                     \
  kvVal->type = TSDB_DATA_TYPE_DOUBLE; \
  kvVal->d = result;

#define SET_FLOAT                                                                              \
  if (!IS_VALID_FLOAT(result)) {                                                               \
    smlBuildInvalidDataMsg(msg, "float out of range[-3.402823466e+38,3.402823466e+38]", pVal); \
    return false;                                                                              \
  }                                                                                            \
  kvVal->type = TSDB_DATA_TYPE_FLOAT;                                                          \
  kvVal->f = (float)result;

#define SET_BIGINT                                                                                       \
  errno = 0;                                                                                             \
  int64_t tmp = taosStr2Int64(pVal, &endptr, 10);                                                        \
  if (errno == ERANGE) {                                                                                 \
    smlBuildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", pVal); \
    return false;                                                                                        \
  }                                                                                                      \
  kvVal->type = TSDB_DATA_TYPE_BIGINT;                                                                   \
  kvVal->i = tmp;

#define SET_INT                                                                    \
  if (!IS_VALID_INT(result)) {                                                     \
    smlBuildInvalidDataMsg(msg, "int out of range[-2147483648,2147483647]", pVal); \
    return false;                                                                  \
  }                                                                                \
  kvVal->type = TSDB_DATA_TYPE_INT;                                                \
  kvVal->i = result;

#define SET_SMALL_INT                                                          \
  if (!IS_VALID_SMALLINT(result)) {                                            \
    smlBuildInvalidDataMsg(msg, "small int our of range[-32768,32767]", pVal); \
    return false;                                                              \
  }                                                                            \
  kvVal->type = TSDB_DATA_TYPE_SMALLINT;                                       \
  kvVal->i = result;

#define SET_UBIGINT                                                                             \
  errno = 0;                                                                                    \
  uint64_t tmp = taosStr2UInt64(pVal, &endptr, 10);                                             \
  if (errno == ERANGE || result < 0) {                                                          \
    smlBuildInvalidDataMsg(msg, "unsigned big int out of range[0,18446744073709551615]", pVal); \
    return false;                                                                               \
  }                                                                                             \
  kvVal->type = TSDB_DATA_TYPE_UBIGINT;                                                         \
  kvVal->u = tmp;

#define SET_UINT                                                                  \
  if (!IS_VALID_UINT(result)) {                                                   \
    smlBuildInvalidDataMsg(msg, "unsigned int out of range[0,4294967295]", pVal); \
    return false;                                                                 \
  }                                                                               \
  kvVal->type = TSDB_DATA_TYPE_UINT;                                              \
  kvVal->u = result;

#define SET_USMALL_INT                                                            \
  if (!IS_VALID_USMALLINT(result)) {                                              \
    smlBuildInvalidDataMsg(msg, "unsigned small int out of rang[0,65535]", pVal); \
    return false;                                                                 \
  }                                                                               \
  kvVal->type = TSDB_DATA_TYPE_USMALLINT;                                         \
  kvVal->u = result;

#define SET_TINYINT                                                       \
  if (!IS_VALID_TINYINT(result)) {                                        \
    smlBuildInvalidDataMsg(msg, "tiny int out of range[-128,127]", pVal); \
    return false;                                                         \
  }                                                                       \
  kvVal->type = TSDB_DATA_TYPE_TINYINT;                                   \
  kvVal->i = result;

#define SET_UTINYINT                                                            \
  if (!IS_VALID_UTINYINT(result)) {                                             \
    smlBuildInvalidDataMsg(msg, "unsigned tiny int out of range[0,255]", pVal); \
    return false;                                                               \
  }                                                                             \
  kvVal->type = TSDB_DATA_TYPE_UTINYINT;                                        \
  kvVal->u = result;

int64_t smlToMilli[] = {3600000LL, 60000LL, 1000LL};
int64_t smlFactorNS[] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
int64_t smlFactorS[] = {1000LL, 1000000LL, 1000000000LL};

static int32_t smlCheckAuth(SSmlHandle *info,  SRequestConnInfo* conn, const char* pTabName, AUTH_TYPE type){
  SUserAuthInfo pAuth = {0};
  snprintf(pAuth.user, sizeof(pAuth.user), "%s", info->taos->user);
  if (NULL == pTabName) {
    tNameSetDbName(&pAuth.tbName, info->taos->acctId, info->pRequest->pDb, strlen(info->pRequest->pDb));
  } else {
    toName(info->taos->acctId, info->pRequest->pDb, pTabName, &pAuth.tbName);
  }
  pAuth.type = type;

  int32_t      code = TSDB_CODE_SUCCESS;
  SUserAuthRes authRes = {0};

  code = catalogChkAuth(info->pCatalog, conn, &pAuth, &authRes);
  nodesDestroyNode(authRes.pCond[AUTH_RES_BASIC]);

  return (code == TSDB_CODE_SUCCESS) ? (authRes.pass[AUTH_RES_BASIC] ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_PERMISSION_DENIED) : code;

}

int32_t smlBuildInvalidDataMsg(SSmlMsgBuf *pBuf, const char *msg1, const char *msg2) {
  if (pBuf->buf) {
    memset(pBuf->buf, 0, pBuf->len);
    if (msg1) strncat(pBuf->buf, msg1, pBuf->len);
    int32_t left = pBuf->len - strlen(pBuf->buf);
    if (left > 2 && msg2) {
      strncat(pBuf->buf, ":", left - 1);
      strncat(pBuf->buf, msg2, left - 2);
    }
  }
  return TSDB_CODE_SML_INVALID_DATA;
}

int64_t smlGetTimeValue(const char *value, int32_t len, uint8_t fromPrecision, uint8_t toPrecision) {
  char   *endPtr = NULL;
  int64_t tsInt64 = taosStr2Int64(value, &endPtr, 10);
  if (unlikely(value + len != endPtr)) {
    return -1;
  }

  if (unlikely(fromPrecision >= TSDB_TIME_PRECISION_HOURS)) {
    int64_t unit = smlToMilli[fromPrecision - TSDB_TIME_PRECISION_HOURS];
    if (tsInt64 != 0 && unit > INT64_MAX / tsInt64) {
      return -1;
    }
    tsInt64 *= unit;
    fromPrecision = TSDB_TIME_PRECISION_MILLI;
  }

  return convertTimePrecision(tsInt64, fromPrecision, toPrecision);
}

SSmlTableInfo *smlBuildTableInfo(int numRows, const char *measure, int32_t measureLen) {
  SSmlTableInfo *tag = (SSmlTableInfo *)taosMemoryCalloc(sizeof(SSmlTableInfo), 1);
  if (!tag) {
    return NULL;
  }

  tag->sTableName = measure;
  tag->sTableNameLen = measureLen;

  tag->cols = taosArrayInit(numRows, POINTER_BYTES);
  if (tag->cols == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }

  return tag;

cleanup:
  taosMemoryFree(tag);
  return NULL;
}

void smlBuildTsKv(SSmlKv *kv, int64_t ts){
  kv->key = tsSmlTsDefaultName;
  kv->keyLen = strlen(tsSmlTsDefaultName);
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->i = ts;
  kv->length = (size_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
}

SSmlSTableMeta* smlBuildSuperTableInfo(SSmlHandle *info, SSmlLineInfo *currElement){
  SSmlSTableMeta* sMeta = NULL;
  char *measure = currElement->measure;
  int   measureLen = currElement->measureLen;
  if (currElement->measureEscaped) {
    measure = (char *)taosMemoryMalloc(measureLen);
    memcpy(measure, currElement->measure, measureLen);
    PROCESS_SLASH_IN_MEASUREMENT(measure, measureLen);
    smlStrReplace(measure, measureLen);
  }
  STableMeta *pTableMeta = smlGetMeta(info, measure, measureLen);
  if (currElement->measureEscaped) {
    taosMemoryFree(measure);
  }
  if (pTableMeta == NULL) {
    info->dataFormat = false;
    info->reRun = true;
    terrno = TSDB_CODE_SUCCESS;
    return sMeta;
  }
  sMeta = smlBuildSTableMeta(info->dataFormat);
  if(sMeta == NULL){
    taosMemoryFreeClear(pTableMeta);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return sMeta;
  }
  sMeta->tableMeta = pTableMeta;
  taosHashPut(info->superTables, currElement->measure, currElement->measureLen, &sMeta, POINTER_BYTES);
  for (int i = 1; i < pTableMeta->tableInfo.numOfTags + pTableMeta->tableInfo.numOfColumns; i++) {
    SSchema *col = pTableMeta->schema + i;
    SSmlKv   kv = {.key = col->name, .keyLen = strlen(col->name), .type = col->type};
    if (col->type == TSDB_DATA_TYPE_NCHAR) {
      kv.length = (col->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    } else if (col->type == TSDB_DATA_TYPE_BINARY || col->type == TSDB_DATA_TYPE_GEOMETRY || col->type == TSDB_DATA_TYPE_VARBINARY) {
      kv.length = col->bytes - VARSTR_HEADER_SIZE;
    } else{
      kv.length = col->bytes;
    }

    if(i < pTableMeta->tableInfo.numOfColumns){
      taosArrayPush(sMeta->cols, &kv);
    }else{
      taosArrayPush(sMeta->tags, &kv);
    }
  }
  return sMeta;
}

bool isSmlColAligned(SSmlHandle *info, int cnt, SSmlKv *kv){
  // cnt begin 0, add ts so + 2
  if (unlikely(cnt + 2 > info->currSTableMeta->tableInfo.numOfColumns)) {
    info->dataFormat = false;
    info->reRun = true;
    return false;
  }
  // bind data
  int32_t ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kv, cnt + 1);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    uDebug("smlBuildCol error, retry");
    info->dataFormat = false;
    info->reRun = true;
    return false;
  }
  if (cnt >= taosArrayGetSize(info->maxColKVs)) {
    info->dataFormat = false;
    info->reRun = true;
    return false;
  }
  SSmlKv *maxKV = (SSmlKv *)taosArrayGet(info->maxColKVs, cnt);

  if (unlikely(!IS_SAME_KEY)) {
    info->dataFormat = false;
    info->reRun = true;
    return false;
  }

  if (unlikely(IS_VAR_DATA_TYPE(kv->type) && kv->length > maxKV->length)) {
    maxKV->length = kv->length;
    info->needModifySchema = true;
  }
  return true;
}

bool isSmlTagAligned(SSmlHandle *info, int cnt, SSmlKv *kv){
  if (unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)) {
    goto END;
  }

  if (unlikely(cnt >= taosArrayGetSize(info->maxTagKVs))) {
    goto END;
  }
  SSmlKv *maxKV = (SSmlKv *)taosArrayGet(info->maxTagKVs, cnt);

  if (unlikely(!IS_SAME_KEY)) {
    goto END;
  }

  if (unlikely(kv->length > maxKV->length)) {
    maxKV->length = kv->length;
    info->needModifySchema = true;
  }
  return true;

END:
  info->dataFormat = false;
  info->reRun = true;
  return false;
}

int32_t smlJoinMeasureTag(SSmlLineInfo *elements){
  elements->measureTag = (char *)taosMemoryMalloc(elements->measureLen + elements->tagsLen);
  if(elements->measureTag == NULL){
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(elements->measureTag, elements->measure, elements->measureLen);
  memcpy(elements->measureTag + elements->measureLen, elements->tags, elements->tagsLen);
  elements->measureTagsLen = elements->measureLen + elements->tagsLen;
  return TSDB_CODE_SUCCESS;
}

int32_t smlProcessSuperTable(SSmlHandle *info, SSmlLineInfo *elements) {
  bool isSameMeasure = IS_SAME_SUPER_TABLE;
  if(isSameMeasure) {
    return 0;
  }
  SSmlSTableMeta **tmp = (SSmlSTableMeta **)taosHashGet(info->superTables, elements->measure, elements->measureLen);

  SSmlSTableMeta *sMeta = NULL;
  if (unlikely(tmp == NULL)) {
    sMeta = smlBuildSuperTableInfo(info, elements);
    if(sMeta == NULL) return -1;
  }else{
    sMeta = *tmp;
  }
  ASSERT(sMeta != NULL);
  info->currSTableMeta = sMeta->tableMeta;
  info->maxTagKVs = sMeta->tags;
  info->maxColKVs = sMeta->cols;
  return 0;
}

int32_t smlProcessChildTable(SSmlHandle *info, SSmlLineInfo *elements){
  SSmlTableInfo **oneTable =
      (SSmlTableInfo **)taosHashGet(info->childTables, elements->measureTag, elements->measureTagsLen);
  SSmlTableInfo *tinfo = NULL;
  if (unlikely(oneTable == NULL)) {
    tinfo = smlBuildTableInfo(1, elements->measure, elements->measureLen);
    if (unlikely(!tinfo)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosHashPut(info->childTables, elements->measureTag, elements->measureTagsLen, &tinfo, POINTER_BYTES);

    tinfo->tags = taosArrayDup(info->preLineTagKV, NULL);
    for (size_t i = 0; i < taosArrayGetSize(info->preLineTagKV); i++) {
      SSmlKv *kv = (SSmlKv *)taosArrayGet(info->preLineTagKV, i);
      if (kv->keyEscaped) kv->key = NULL;
      if (kv->valueEscaped) kv->value = NULL;
    }

    smlSetCTableName(tinfo);
    getTableUid(info, elements, tinfo);
    if (info->dataFormat) {
      info->currSTableMeta->uid = tinfo->uid;
      tinfo->tableDataCtx = smlInitTableDataCtx(info->pQuery, info->currSTableMeta);
      if (tinfo->tableDataCtx == NULL) {
        smlBuildInvalidDataMsg(&info->msgBuf, "smlInitTableDataCtx error", NULL);
        smlDestroyTableInfo(&tinfo);
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }
  }else{
    tinfo = *oneTable;
  }
  ASSERT(tinfo != NULL);
  if (info->dataFormat) info->currTableDataCtx = tinfo->tableDataCtx;
  return TSDB_CODE_SUCCESS;
}

int32_t smlParseEndTelnetJson(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs, SSmlKv *kv){
  if (info->dataFormat) {
    uDebug("SML:0x%" PRIx64 " smlParseEndTelnetJson format true, ts:%" PRId64, info->id, kvTs->i);
    int32_t ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kvTs, 0);
    if (ret == TSDB_CODE_SUCCESS) {
      ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kv, 1);
    }
    if (ret == TSDB_CODE_SUCCESS) {
      ret = smlBuildRow(info->currTableDataCtx);
    }
    clearColValArraySml(info->currTableDataCtx->pValues);
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "smlBuildCol error", NULL);
      return ret;
    }
  } else {
    uDebug("SML:0x%" PRIx64 " smlParseEndTelnetJson format false, ts:%" PRId64, info->id, kvTs->i);
    if (elements->colArray == NULL) {
      elements->colArray = taosArrayInit(16, sizeof(SSmlKv));
    }
    taosArrayPush(elements->colArray, kvTs);
    taosArrayPush(elements->colArray, kv);
  }
  info->preLine = *elements;

  return TSDB_CODE_SUCCESS;
}

int32_t smlParseEndLine(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs){
  if (info->dataFormat) {
    uDebug("SML:0x%" PRIx64 " smlParseEndLine format true, ts:%" PRId64, info->id, kvTs->i);
    int32_t ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kvTs, 0);
    if (ret == TSDB_CODE_SUCCESS) {
      ret = smlBuildRow(info->currTableDataCtx);
    }

    clearColValArraySml(info->currTableDataCtx->pValues);
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "smlBuildCol error", NULL);
      return ret;
    }
  } else {
    uDebug("SML:0x%" PRIx64 " smlParseEndLine format false, ts:%" PRId64, info->id, kvTs->i);
    taosArraySet(elements->colArray, 0, kvTs);
  }
  info->preLine = *elements;

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTableName(SArray *tags, char *childTableName) {
  bool autoChildName = false;
  size_t delimiter = strlen(tsSmlAutoChildTableNameDelimiter);
  if(delimiter > 0){
    size_t totalNameLen = delimiter * (taosArrayGetSize(tags) - 1);
    for (int i = 0; i < taosArrayGetSize(tags); i++) {
      SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
      totalNameLen += tag->length;
    }
    if(totalNameLen < TSDB_TABLE_NAME_LEN){
      autoChildName = true;
    }
  }
  if(autoChildName){
    memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
    for (int i = 0; i < taosArrayGetSize(tags); i++) {
      SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
      strncat(childTableName, tag->value, tag->length);
      if(i != taosArrayGetSize(tags) - 1){
        strcat(childTableName, tsSmlAutoChildTableNameDelimiter);
      }
    }
    if(tsSmlDot2Underline){
      smlStrReplace(childTableName, strlen(childTableName));
    }
  }else{
    size_t childTableNameLen = strlen(tsSmlChildTableName);
    if (childTableNameLen <= 0) return TSDB_CODE_SUCCESS;

    for (int i = 0; i < taosArrayGetSize(tags); i++) {
      SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
      // handle child table name
      if (childTableNameLen == tag->keyLen && strncmp(tag->key, tsSmlChildTableName, tag->keyLen) == 0) {
        memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
        strncpy(childTableName, tag->value, (tag->length < TSDB_TABLE_NAME_LEN ? tag->length : TSDB_TABLE_NAME_LEN));
        if(tsSmlDot2Underline){
          smlStrReplace(childTableName, strlen(childTableName));
        }
        taosArrayRemove(tags, i);
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t smlSetCTableName(SSmlTableInfo *oneTable) {
  smlParseTableName(oneTable->tags, oneTable->childTableName);

  if (strlen(oneTable->childTableName) == 0) {
    SArray       *dst = taosArrayDup(oneTable->tags, NULL);
    ASSERT(oneTable->sTableNameLen < TSDB_TABLE_NAME_LEN);
    char superName[TSDB_TABLE_NAME_LEN] = {0};
    RandTableName rName = {dst, NULL, (uint8_t)oneTable->sTableNameLen, oneTable->childTableName};
    if(tsSmlDot2Underline){
      memcpy(superName, oneTable->sTableName, oneTable->sTableNameLen);
      smlStrReplace(superName, oneTable->sTableNameLen);
      rName.stbFullName = superName;
    }else{
      rName.stbFullName = oneTable->sTableName;
    }

    buildChildTableName(&rName);
    taosArrayDestroy(dst);
  }
  return TSDB_CODE_SUCCESS;
}

void getTableUid(SSmlHandle *info, SSmlLineInfo *currElement, SSmlTableInfo *tinfo) {
  char   key[TSDB_TABLE_NAME_LEN * 2 + 1] = {0};
  size_t nLen = strlen(tinfo->childTableName);
  memcpy(key, currElement->measure, currElement->measureLen);
  if(tsSmlDot2Underline){
    smlStrReplace(key, currElement->measureLen);
  }
  memcpy(key + currElement->measureLen + 1, tinfo->childTableName, nLen);
  void *uid =
      taosHashGet(info->tableUids, key,
                  currElement->measureLen + 1 + nLen);  // use \0 as separator for stable name and child table name
  if (uid == NULL) {
    tinfo->uid = info->uid++;
    taosHashPut(info->tableUids, key, currElement->measureLen + 1 + nLen, &tinfo->uid, sizeof(uint64_t));
  } else {
    tinfo->uid = *(uint64_t *)uid;
  }
}

static void smlDestroySTableMeta(void *para) {
  SSmlSTableMeta *meta = *(SSmlSTableMeta**)para;
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->colHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFreeClear(meta->tableMeta);
  taosMemoryFree(meta);
}

SSmlSTableMeta *smlBuildSTableMeta(bool isDataFormat) {
  SSmlSTableMeta *meta = (SSmlSTableMeta *)taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
  if (!meta) {
    return NULL;
  }

  if (unlikely(!isDataFormat)) {
    meta->tagHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (meta->tagHash == NULL) {
      uError("SML:smlBuildSTableMeta failed to allocate memory");
      goto cleanup;
    }

    meta->colHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (meta->colHash == NULL) {
      uError("SML:smlBuildSTableMeta failed to allocate memory");
      goto cleanup;
    }
  }

  meta->tags = taosArrayInit(32, sizeof(SSmlKv));
  if (meta->tags == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->cols = taosArrayInit(32, sizeof(SSmlKv));
  if (meta->cols == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }
  return meta;

cleanup:
  smlDestroySTableMeta(meta);
  return NULL;
}

bool smlParseNumber(SSmlKv *kvVal, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t     len = kvVal->length;
  char       *endptr = NULL;
  double      result = taosStr2Double(pVal, &endptr);
  if (pVal == endptr) {
    RETURN_FALSE
  }

  int32_t left = len - (endptr - pVal);
  if (left == 0) {
    SET_DOUBLE
  } else if (left == 3) {
    if (endptr[0] == 'f' || endptr[0] == 'F') {
      if (endptr[1] == '6' && endptr[2] == '4') {
        SET_DOUBLE
      } else if (endptr[1] == '3' && endptr[2] == '2') {
        SET_FLOAT
      } else {
        RETURN_FALSE
      }
    } else if (endptr[0] == 'i' || endptr[0] == 'I') {
      if (endptr[1] == '6' && endptr[2] == '4') {
        SET_BIGINT
      } else if (endptr[1] == '3' && endptr[2] == '2') {
        SET_INT
      } else if (endptr[1] == '1' && endptr[2] == '6') {
        SET_SMALL_INT
      } else {
        RETURN_FALSE
      }
    } else if (endptr[0] == 'u' || endptr[0] == 'U') {
      if (endptr[1] == '6' && endptr[2] == '4') {
        SET_UBIGINT
      } else if (endptr[1] == '3' && endptr[2] == '2') {
        SET_UINT
      } else if (endptr[1] == '1' && endptr[2] == '6') {
        SET_USMALL_INT
      } else {
        RETURN_FALSE
      }
    } else {
      RETURN_FALSE
    }
  } else if (left == 2) {
    if (endptr[0] == 'i' || endptr[0] == 'I') {
      if (endptr[1] == '8') {
        SET_TINYINT
      } else {
        RETURN_FALSE
      }
    } else if (endptr[0] == 'u' || endptr[0] == 'U') {
      if (endptr[1] == '8') {
        SET_UTINYINT
      } else {
        RETURN_FALSE
      }
    } else {
      RETURN_FALSE
    }
  } else if (left == 1) {
    if (endptr[0] == 'i' || endptr[0] == 'I') {
      SET_BIGINT
    } else if (endptr[0] == 'u' || endptr[0] == 'U') {
      SET_UBIGINT
    } else {
      RETURN_FALSE
    }
  } else {
    RETURN_FALSE;
  }
  return true;
}

bool smlParseNumberOld(SSmlKv *kvVal, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t     len = kvVal->length;
  char       *endptr = NULL;
  double      result = taosStr2Double(pVal, &endptr);
  if (pVal == endptr) {
    smlBuildInvalidDataMsg(msg, "invalid data", pVal);
    return false;
  }

  int32_t left = len - (endptr - pVal);
  if (left == 0 || (left == 3 && strncasecmp(endptr, "f64", left) == 0)) {
    kvVal->type = TSDB_DATA_TYPE_DOUBLE;
    kvVal->d = result;
  } else if ((left == 3 && strncasecmp(endptr, "f32", left) == 0)) {
    if (!IS_VALID_FLOAT(result)) {
      smlBuildInvalidDataMsg(msg, "float out of range[-3.402823466e+38,3.402823466e+38]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_FLOAT;
    kvVal->f = (float)result;
  } else if ((left == 1 && *endptr == 'i') || (left == 3 && strncasecmp(endptr, "i64", left) == 0)) {
    if (smlDoubleToInt64OverFlow(result)) {
      errno = 0;
      int64_t tmp = taosStr2Int64(pVal, &endptr, 10);
      if (errno == ERANGE) {
        smlBuildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", pVal);
        return false;
      }
      kvVal->type = TSDB_DATA_TYPE_BIGINT;
      kvVal->i = tmp;
      return true;
    }
    kvVal->type = TSDB_DATA_TYPE_BIGINT;
    kvVal->i = (int64_t)result;
  } else if ((left == 1 && *endptr == 'u') || (left == 3 && strncasecmp(endptr, "u64", left) == 0)) {
    if (result >= (double)UINT64_MAX || result < 0) {
      errno = 0;
      uint64_t tmp = taosStr2UInt64(pVal, &endptr, 10);
      if (errno == ERANGE || result < 0) {
        smlBuildInvalidDataMsg(msg, "unsigned big int out of range[0,18446744073709551615]", pVal);
        return false;
      }
      kvVal->type = TSDB_DATA_TYPE_UBIGINT;
      kvVal->u = tmp;
      return true;
    }
    kvVal->type = TSDB_DATA_TYPE_UBIGINT;
    kvVal->u = result;
  } else if (left == 3 && strncasecmp(endptr, "i32", left) == 0) {
    if (!IS_VALID_INT(result)) {
      smlBuildInvalidDataMsg(msg, "int out of range[-2147483648,2147483647]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_INT;
    kvVal->i = result;
  } else if (left == 3 && strncasecmp(endptr, "u32", left) == 0) {
    if (!IS_VALID_UINT(result)) {
      smlBuildInvalidDataMsg(msg, "unsigned int out of range[0,4294967295]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UINT;
    kvVal->u = result;
  } else if (left == 3 && strncasecmp(endptr, "i16", left) == 0) {
    if (!IS_VALID_SMALLINT(result)) {
      smlBuildInvalidDataMsg(msg, "small int our of range[-32768,32767]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_SMALLINT;
    kvVal->i = result;
  } else if (left == 3 && strncasecmp(endptr, "u16", left) == 0) {
    if (!IS_VALID_USMALLINT(result)) {
      smlBuildInvalidDataMsg(msg, "unsigned small int out of rang[0,65535]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_USMALLINT;
    kvVal->u = result;
  } else if (left == 2 && strncasecmp(endptr, "i8", left) == 0) {
    if (!IS_VALID_TINYINT(result)) {
      smlBuildInvalidDataMsg(msg, "tiny int out of range[-128,127]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_TINYINT;
    kvVal->i = result;
  } else if (left == 2 && strncasecmp(endptr, "u8", left) == 0) {
    if (!IS_VALID_UTINYINT(result)) {
      smlBuildInvalidDataMsg(msg, "unsigned tiny int out of range[0,255]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UTINYINT;
    kvVal->u = result;
  } else {
    smlBuildInvalidDataMsg(msg, "invalid data", pVal);
    return false;
  }
  return true;
}

STableMeta *smlGetMeta(SSmlHandle *info, const void *measure, int32_t measureLen) {
  STableMeta *pTableMeta = NULL;

  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));

  SRequestConnInfo conn = {0};
  conn.pTrans = info->taos->pAppInfo->pTransporter;
  conn.requestId = info->pRequest->requestId;
  conn.requestObjRefId = info->pRequest->self;
  conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
  memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
  memcpy(pName.tname, measure, measureLen);

  int32_t code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
  if (code != TSDB_CODE_SUCCESS) {
    return NULL;
  }
  return pTableMeta;
}

static int64_t smlGenId() {
  static volatile int64_t linesSmlHandleId = 0;

  int64_t id = 0;
  do {
    id = atomic_add_fetch_64(&linesSmlHandleId, 1);
  } while (id == 0);

  return id;
}

static int32_t smlGenerateSchemaAction(SSchema *colField, SHashObj *colHash, SSmlKv *kv, bool isTag,
                                       ESchemaAction *action, SSmlHandle *info) {
  uint16_t *index = colHash ? (uint16_t *)taosHashGet(colHash, kv->key, kv->keyLen) : NULL;
  if (index) {
    if (colField[*index].type != kv->type) {
      uError("SML:0x%" PRIx64 " point type and db type mismatch. db type: %d, point type: %d, key: %s", info->id,
             colField[*index].type, kv->type, kv->key);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if (((colField[*index].type == TSDB_DATA_TYPE_VARCHAR  || colField[*index].type == TSDB_DATA_TYPE_VARBINARY || colField[*index].type == TSDB_DATA_TYPE_GEOMETRY) &&
         (colField[*index].bytes - VARSTR_HEADER_SIZE) < kv->length) ||
        (colField[*index].type == TSDB_DATA_TYPE_NCHAR &&
         ((colField[*index].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE < kv->length))) {
      if (isTag) {
        *action = SCHEMA_ACTION_CHANGE_TAG_SIZE;
      } else {
        *action = SCHEMA_ACTION_CHANGE_COLUMN_SIZE;
      }
    }
  } else {
    if (isTag) {
      *action = SCHEMA_ACTION_ADD_TAG;
    } else {
      *action = SCHEMA_ACTION_ADD_COLUMN;
    }
  }
  return 0;
}

#define BOUNDARY 1024
static int32_t smlFindNearestPowerOf2(int32_t length, uint8_t type) {
  int32_t result = 1;
  if (length >= BOUNDARY) {
    result = length;
  } else {
    while (result <= length) {
      result <<= 1;
    }
  }

  if ((type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_GEOMETRY) && result > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
    result = TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_NCHAR && result > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
    result = (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
  }

  if (type == TSDB_DATA_TYPE_NCHAR) {
    result = result * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_GEOMETRY) {
    result = result + VARSTR_HEADER_SIZE;
  }
  return result;
}

static int32_t smlProcessSchemaAction(SSmlHandle *info, SSchema *schemaField, SHashObj *schemaHash, SArray *cols,
                                      ESchemaAction *action, bool isTag) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int j = 0; j < taosArrayGetSize(cols); ++j) {
    if (j == 0 && !isTag) continue;
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, j);
    code = smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, action, info);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlCheckMeta(SSchema *schema, int32_t length, SArray *cols, bool isTag) {
  SHashObj *hashTmp = taosHashInit(length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  int32_t   i = 0;
  for (; i < length; i++) {
    taosHashPut(hashTmp, schema[i].name, strlen(schema[i].name), &i, SHORT_BYTES);
  }

  if (isTag) {
    i = 0;
  } else {
    i = 1;
  }
  for (; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    if (taosHashGet(hashTmp, kv->key, kv->keyLen) == NULL) {
      taosHashCleanup(hashTmp);
      return TSDB_CODE_SML_INVALID_DATA;
    }
  }
  taosHashCleanup(hashTmp);
  return 0;
}

static int32_t getBytes(uint8_t type, int32_t length) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_GEOMETRY) {
    return smlFindNearestPowerOf2(length, type);
  } else {
    return tDataTypes[type].bytes;
  }
}

static int32_t smlBuildFieldsList(SSmlHandle *info, SSchema *schemaField, SHashObj *schemaHash, SArray *cols,
                                  SArray *results, int32_t numOfCols, bool isTag) {
  for (int j = 0; j < taosArrayGetSize(cols); ++j) {
    SSmlKv       *kv = (SSmlKv *)taosArrayGet(cols, j);
    ESchemaAction action = SCHEMA_ACTION_NULL;
    int           code = smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, &action, info);
    if (code != 0) {
      return code;
    }
    if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_ADD_TAG) {
      SField field = {0};
      field.type = kv->type;
      field.bytes = getBytes(kv->type, kv->length);
      memcpy(field.name, kv->key, kv->keyLen);
      taosArrayPush(results, &field);
    } else if (action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
      uint16_t *index = (uint16_t *)taosHashGet(schemaHash, kv->key, kv->keyLen);
      if (index == NULL) {
        uError("smlBuildFieldsList get error, key:%s", kv->key);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      uint16_t newIndex = *index;
      if (isTag) newIndex -= numOfCols;
      SField *field = (SField *)taosArrayGet(results, newIndex);
      field->bytes = getBytes(kv->type, kv->length);
    }
  }

  int32_t maxLen = isTag ? TSDB_MAX_TAGS_LEN : TSDB_MAX_BYTES_PER_ROW;
  int32_t len = 0;
  for (int j = 0; j < taosArrayGetSize(results); ++j) {
    SField *field = taosArrayGet(results, j);
    len += field->bytes;
  }
  if(len > maxLen){
    return isTag ? TSDB_CODE_PAR_INVALID_TAGS_LENGTH : TSDB_CODE_PAR_INVALID_ROW_LENGTH;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlSendMetaMsg(SSmlHandle *info, SName *pName, SArray *pColumns, SArray *pTags, STableMeta *pTableMeta,
                              ESchemaAction action) {
  SRequestObj   *pRequest = NULL;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SCmdMsgInfo    pCmdMsg = {0};
  char          *pSql = NULL;

  // put front for free
  pReq.numOfColumns = taosArrayGetSize(pColumns);
  pReq.pColumns = pColumns;
  pReq.numOfTags = taosArrayGetSize(pTags);
  pReq.pTags = pTags;

  if (action == SCHEMA_ACTION_CREATE_STABLE) {
    pReq.colVer = 1;
    pReq.tagVer = 1;
    pReq.suid = 0;
    pReq.source = TD_REQ_FROM_APP;
    pSql = "sml_create_stable";
  } else if (action == SCHEMA_ACTION_ADD_TAG || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
    pReq.colVer = pTableMeta->sversion;
    pReq.tagVer = pTableMeta->tversion + 1;
    pReq.suid = pTableMeta->uid;
    pReq.source = TD_REQ_FROM_TAOX;
    pSql = (action == SCHEMA_ACTION_ADD_TAG) ? "sml_add_tag" : "sml_modify_tag_size";
  } else if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE) {
    pReq.colVer = pTableMeta->sversion + 1;
    pReq.tagVer = pTableMeta->tversion;
    pReq.suid = pTableMeta->uid;
    pReq.source = TD_REQ_FROM_TAOX;
    pSql = (action == SCHEMA_ACTION_ADD_COLUMN) ? "sml_add_column" : "sml_modify_column_size";
  } else{
    uError("SML:0x%" PRIx64 " invalid action:%d", info->id, action);
    goto end;
  }

  code = buildRequest(info->taos->id, pSql, strlen(pSql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  if (pReq.numOfTags == 0) {
    pReq.numOfTags = 1;
    SField field = {0};
    field.type = TSDB_DATA_TYPE_NCHAR;
    field.bytes = TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
    strcpy(field.name, tsSmlTagName);
    taosArrayPush(pReq.pTags, &field);
  }

  pReq.commentLen = -1;
  pReq.igExists = true;
  tNameExtractFullName(pName, pReq.name);

  pCmdMsg.epSet = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  if (NULL == pCmdMsg.pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);

  SQuery pQuery;
  memset(&pQuery, 0, sizeof(pQuery));
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    catalogRemoveTableMeta(info->pCatalog, pName);
  }
  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

end:
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  return code;
}

static int32_t smlModifyDBSchemas(SSmlHandle *info) {
  uDebug("SML:0x%" PRIx64 " smlModifyDBSchemas start, format:%d, needModifySchema:%d", info->id, info->dataFormat,
         info->needModifySchema);
  if (info->dataFormat && !info->needModifySchema) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t     code = 0;
  SHashObj   *hashTmp = NULL;
  STableMeta *pTableMeta = NULL;

  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));

  SRequestConnInfo conn = {0};
  conn.pTrans = info->taos->pAppInfo->pTransporter;
  conn.requestId = info->pRequest->requestId;
  conn.requestObjRefId = info->pRequest->self;
  conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

  SSmlSTableMeta **tmp = (SSmlSTableMeta **)taosHashIterate(info->superTables, NULL);
  while (tmp) {
    SSmlSTableMeta *sTableData = *tmp;
    bool            needCheckMeta = false;  // for multi thread

    size_t superTableLen = 0;
    void  *superTable = taosHashGetKey(tmp, &superTableLen);
    char  *measure = taosMemoryMalloc(superTableLen);
    memcpy(measure, superTable, superTableLen);
    PROCESS_SLASH_IN_MEASUREMENT(measure, superTableLen);
    smlStrReplace(measure, superTableLen);
    memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
    memcpy(pName.tname, measure, superTableLen);
    taosMemoryFree(measure);

    code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);

    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_MND_STB_NOT_EXIST) {
      code = smlCheckAuth(info, &conn, NULL, AUTH_TYPE_WRITE);
      if(code != TSDB_CODE_SUCCESS){
        goto end;
      }
      uDebug("SML:0x%" PRIx64 " smlModifyDBSchemas create table:%s", info->id, pName.tname);
      SArray *pColumns = taosArrayInit(taosArrayGetSize(sTableData->cols), sizeof(SField));
      SArray *pTags = taosArrayInit(taosArrayGetSize(sTableData->tags), sizeof(SField));
      code = smlBuildFieldsList(info, NULL, NULL, sTableData->tags, pTags, 0, true);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlBuildFieldsList tag1 failed. %s", info->id, pName.tname);
        taosArrayDestroy(pColumns);
        taosArrayDestroy(pTags);
        goto end;
      }
      code = smlBuildFieldsList(info, NULL, NULL, sTableData->cols, pColumns, 0, false);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlBuildFieldsList col1 failed. %s", info->id, pName.tname);
        taosArrayDestroy(pColumns);
        taosArrayDestroy(pTags);
        goto end;
      }
      code = smlSendMetaMsg(info, &pName, pColumns, pTags, NULL, SCHEMA_ACTION_CREATE_STABLE);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlSendMetaMsg failed. can not create %s", info->id, pName.tname);
        goto end;
      }
      info->cost.numOfCreateSTables++;
      taosMemoryFreeClear(pTableMeta);

      code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " catalogGetSTableMeta failed. super table name %s", info->id, pName.tname);
        goto end;
      }
    } else if (code == TSDB_CODE_SUCCESS) {
      hashTmp = taosHashInit(pTableMeta->tableInfo.numOfTags, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                             HASH_NO_LOCK);
      for (uint16_t i = pTableMeta->tableInfo.numOfColumns;
           i < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; i++) {
        taosHashPut(hashTmp, pTableMeta->schema[i].name, strlen(pTableMeta->schema[i].name), &i, SHORT_BYTES);
      }

      ESchemaAction action = SCHEMA_ACTION_NULL;
      code = smlProcessSchemaAction(info, pTableMeta->schema, hashTmp, sTableData->tags, &action, true);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
      if (action != SCHEMA_ACTION_NULL) {
        code = smlCheckAuth(info, &conn, pName.tname, AUTH_TYPE_WRITE);
        if(code != TSDB_CODE_SUCCESS){
          goto end;
        }
        uDebug("SML:0x%" PRIx64 " smlModifyDBSchemas change table tag, table:%s, action:%d", info->id, pName.tname,
               action);
        SArray *pColumns =
            taosArrayInit(taosArrayGetSize(sTableData->cols) + pTableMeta->tableInfo.numOfColumns, sizeof(SField));
        SArray *pTags =
            taosArrayInit(taosArrayGetSize(sTableData->tags) + pTableMeta->tableInfo.numOfTags, sizeof(SField));

        for (uint16_t i = 0; i < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; i++) {
          SField field = {0};
          field.type = pTableMeta->schema[i].type;
          field.bytes = pTableMeta->schema[i].bytes;
          strcpy(field.name, pTableMeta->schema[i].name);
          if (i < pTableMeta->tableInfo.numOfColumns) {
            taosArrayPush(pColumns, &field);
          } else {
            taosArrayPush(pTags, &field);
          }
        }
        code = smlBuildFieldsList(info, pTableMeta->schema, hashTmp, sTableData->tags, pTags,
                                  pTableMeta->tableInfo.numOfColumns, true);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " smlBuildFieldsList tag2 failed. %s", info->id, pName.tname);
          taosArrayDestroy(pColumns);
          taosArrayDestroy(pTags);
          goto end;
        }

        if (taosArrayGetSize(pTags) + pTableMeta->tableInfo.numOfColumns > TSDB_MAX_COLUMNS) {
          uError("SML:0x%" PRIx64 " too many columns than 4096", info->id);
          code = TSDB_CODE_PAR_TOO_MANY_COLUMNS;
          taosArrayDestroy(pColumns);
          taosArrayDestroy(pTags);
          goto end;
        }
        if (taosArrayGetSize(pTags) > TSDB_MAX_TAGS) {
          uError("SML:0x%" PRIx64 " too many tags than 128", info->id);
          code = TSDB_CODE_PAR_INVALID_TAGS_NUM;
          taosArrayDestroy(pColumns);
          taosArrayDestroy(pTags);
          goto end;
        }

        code = smlSendMetaMsg(info, &pName, pColumns, pTags, pTableMeta, action);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " smlSendMetaMsg failed. can not create %s", info->id, pName.tname);
          goto end;
        }

        info->cost.numOfAlterTagSTables++;
        taosMemoryFreeClear(pTableMeta);
        code = catalogRefreshTableMeta(info->pCatalog, &conn, &pName, -1);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
        code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
      }

      taosHashClear(hashTmp);
      for (uint16_t i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
        taosHashPut(hashTmp, pTableMeta->schema[i].name, strlen(pTableMeta->schema[i].name), &i, SHORT_BYTES);
      }
      action = SCHEMA_ACTION_NULL;
      code = smlProcessSchemaAction(info, pTableMeta->schema, hashTmp, sTableData->cols, &action, false);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
      if (action != SCHEMA_ACTION_NULL) {
        code = smlCheckAuth(info, &conn, pName.tname, AUTH_TYPE_WRITE);
        if(code != TSDB_CODE_SUCCESS){
          goto end;
        }
        uDebug("SML:0x%" PRIx64 " smlModifyDBSchemas change table col, table:%s, action:%d", info->id, pName.tname,
               action);
        SArray *pColumns =
            taosArrayInit(taosArrayGetSize(sTableData->cols) + pTableMeta->tableInfo.numOfColumns, sizeof(SField));
        SArray *pTags =
            taosArrayInit(taosArrayGetSize(sTableData->tags) + pTableMeta->tableInfo.numOfTags, sizeof(SField));

        for (uint16_t i = 0; i < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; i++) {
          SField field = {0};
          field.type = pTableMeta->schema[i].type;
          field.bytes = pTableMeta->schema[i].bytes;
          strcpy(field.name, pTableMeta->schema[i].name);
          if (i < pTableMeta->tableInfo.numOfColumns) {
            taosArrayPush(pColumns, &field);
          } else {
            taosArrayPush(pTags, &field);
          }
        }

        code = smlBuildFieldsList(info, pTableMeta->schema, hashTmp, sTableData->cols, pColumns,
                                  pTableMeta->tableInfo.numOfColumns, false);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " smlBuildFieldsList col2 failed. %s", info->id, pName.tname);
          taosArrayDestroy(pColumns);
          taosArrayDestroy(pTags);
          goto end;
        }

        if (taosArrayGetSize(pColumns) + pTableMeta->tableInfo.numOfTags > TSDB_MAX_COLUMNS) {
          uError("SML:0x%" PRIx64 " too many columns than 4096", info->id);
          code = TSDB_CODE_PAR_TOO_MANY_COLUMNS;
          taosArrayDestroy(pColumns);
          taosArrayDestroy(pTags);
          goto end;
        }

        code = smlSendMetaMsg(info, &pName, pColumns, pTags, pTableMeta, action);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " smlSendMetaMsg failed. can not create %s", info->id, pName.tname);
          goto end;
        }

        info->cost.numOfAlterColSTables++;
        taosMemoryFreeClear(pTableMeta);
        code = catalogRefreshTableMeta(info->pCatalog, &conn, &pName, -1);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
        code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " catalogGetSTableMeta failed. super table name %s", info->id, pName.tname);
          goto end;
        }
      }

      needCheckMeta = true;
      taosHashCleanup(hashTmp);
      hashTmp = NULL;
    } else {
      uError("SML:0x%" PRIx64 " load table meta error: %s", info->id, tstrerror(code));
      goto end;
    }

    if (needCheckMeta) {
      code = smlCheckMeta(&(pTableMeta->schema[pTableMeta->tableInfo.numOfColumns]), pTableMeta->tableInfo.numOfTags,
                          sTableData->tags, true);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " check tag failed. super table name %s", info->id, pName.tname);
        goto end;
      }
      code = smlCheckMeta(&(pTableMeta->schema[0]), pTableMeta->tableInfo.numOfColumns, sTableData->cols, false);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " check cols failed. super table name %s", info->id, pName.tname);
        goto end;
      }
    }

    taosMemoryFreeClear(sTableData->tableMeta);
    sTableData->tableMeta = pTableMeta;
    uDebug("SML:0x%" PRIx64 "modify schema uid:%" PRIu64 ", sversion:%d, tversion:%d", info->id, pTableMeta->uid,
           pTableMeta->sversion, pTableMeta->tversion);
    tmp = (SSmlSTableMeta **)taosHashIterate(info->superTables, tmp);
  }
  uDebug("SML:0x%" PRIx64 " smlModifyDBSchemas end success, format:%d, needModifySchema:%d", info->id, info->dataFormat,
         info->needModifySchema);

  return 0;

end:
  taosHashCancelIterate(info->superTables, tmp);
  taosHashCleanup(hashTmp);
  taosMemoryFreeClear(pTableMeta);
  catalogRefreshTableMeta(info->pCatalog, &conn, &pName, 1);
  uError("SML:0x%" PRIx64 " smlModifyDBSchemas end failed:%d:%s, format:%d, needModifySchema:%d", info->id, code,
         tstrerror(code), info->dataFormat, info->needModifySchema);

  return code;
}

static void smlInsertMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols) {
  for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    int     ret = taosHashPut(metaHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
    if (ret == 0) {
      taosArrayPush(metaArray, kv);
    }
  }
}

static int32_t smlUpdateMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols, bool isTag, SSmlMsgBuf *msg) {
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);

    int16_t *index = (int16_t *)taosHashGet(metaHash, kv->key, kv->keyLen);
    if (index) {
      SSmlKv *value = (SSmlKv *)taosArrayGet(metaArray, *index);
      if (isTag) {
        if (kv->length > value->length) {
          value->length = kv->length;
        }
        continue;
      }
      if (kv->type != value->type) {
        smlBuildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
        return TSDB_CODE_SML_NOT_SAME_TYPE;
      }

      if (IS_VAR_DATA_TYPE(kv->type) && (kv->length > value->length)) {  // update string len, if bigger
        value->length = kv->length;
      }
    } else {
      size_t tmp = taosArrayGetSize(metaArray);
      if (tmp > INT16_MAX) {
        smlBuildInvalidDataMsg(msg, "too many cols or tags", kv->key);
        uError("too many cols or tags");
        return TSDB_CODE_SML_INVALID_DATA;
      }
      int16_t size = tmp;
      int     ret = taosHashPut(metaHash, kv->key, kv->keyLen, &size, SHORT_BYTES);
      if (ret == 0) {
        taosArrayPush(metaArray, kv);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

void smlDestroyTableInfo(void *para) {
  SSmlTableInfo *tag = *(SSmlTableInfo**)para;
  for (size_t i = 0; i < taosArrayGetSize(tag->cols); i++) {
    SHashObj *kvHash = (SHashObj *)taosArrayGetP(tag->cols, i);
    taosHashCleanup(kvHash);
  }

  taosArrayDestroy(tag->cols);
  taosArrayDestroyEx(tag->tags, freeSSmlKv);
  taosMemoryFree(tag);
}

void freeSSmlKv(void *data) {
  SSmlKv *kv = (SSmlKv *)data;
  if (kv->keyEscaped) taosMemoryFreeClear(kv->key);
  if (kv->valueEscaped) taosMemoryFreeClear(kv->value);
  if (kv->type == TSDB_DATA_TYPE_GEOMETRY) geosFreeBuffer((void *)(kv->value));
  if (kv->type == TSDB_DATA_TYPE_VARBINARY) taosMemoryFreeClear(kv->value);
}

void smlDestroyInfo(SSmlHandle *info) {
  if (!info) return;
  qDestroyQuery(info->pQuery);

  taosHashCleanup(info->pVgHash);
  taosHashCleanup(info->childTables);
  taosHashCleanup(info->superTables);
  taosHashCleanup(info->tableUids);

  for (int i = 0; i < taosArrayGetSize(info->tagJsonArray); i++) {
    cJSON *tags = (cJSON *)taosArrayGetP(info->tagJsonArray, i);
    cJSON_Delete(tags);
  }
  taosArrayDestroy(info->tagJsonArray);

  for (int i = 0; i < taosArrayGetSize(info->valueJsonArray); i++) {
    cJSON *value = (cJSON *)taosArrayGetP(info->valueJsonArray, i);
    cJSON_Delete(value);
  }
  taosArrayDestroy(info->valueJsonArray);

  taosArrayDestroyEx(info->preLineTagKV, freeSSmlKv);

  if (!info->dataFormat) {
    for (int i = 0; i < info->lineNum; i++) {
      taosArrayDestroyEx(info->lines[i].colArray, freeSSmlKv);
      if (info->parseJsonByLib) {
        taosMemoryFree(info->lines[i].tags);
      }
      if (info->lines[i].measureTagsLen != 0 && info->protocol != TSDB_SML_LINE_PROTOCOL) {
        taosMemoryFree(info->lines[i].measureTag);
      }
    }
    taosMemoryFree(info->lines);
  }

  cJSON_Delete(info->root);
  taosMemoryFreeClear(info);
}

SSmlHandle *smlBuildSmlInfo(TAOS *taos) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SSmlHandle *info = (SSmlHandle *)taosMemoryCalloc(1, sizeof(SSmlHandle));
  if (NULL == info) {
    return NULL;
  }
  if (taos != NULL) {
    info->taos = acquireTscObj(*(int64_t *)taos);
    if (info->taos == NULL) {
      goto cleanup;
    }
    code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " get catalog error %d", info->id, code);
      goto cleanup;
    }
  }

  info->pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  info->childTables = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  info->tableUids = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  info->superTables = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  taosHashSetFreeFp(info->superTables, smlDestroySTableMeta);
  taosHashSetFreeFp(info->childTables, smlDestroyTableInfo);

  info->id = smlGenId();
  info->pQuery = smlInitHandle();
  info->dataFormat = true;

  info->tagJsonArray = taosArrayInit(8, POINTER_BYTES);
  info->valueJsonArray = taosArrayInit(8, POINTER_BYTES);
  info->preLineTagKV = taosArrayInit(8, sizeof(SSmlKv));

  if (NULL == info->pVgHash || NULL == info->childTables || NULL == info->superTables || NULL == info->tableUids) {
    uError("create SSmlHandle failed");
    goto cleanup;
  }

  return info;

cleanup:
  smlDestroyInfo(info);
  return NULL;
}

static int32_t smlPushCols(SArray *colsArray, SArray *cols) {
  SHashObj *kvHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!kvHash) {
    uError("SML:smlDealCols failed to allocate memory");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    terrno = 0;
    taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
    if (terrno == TSDB_CODE_DUP_KEY) {
      taosHashCleanup(kvHash);
      return terrno;
    }
  }

  taosArrayPush(colsArray, &kvHash);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseLineBottom(SSmlHandle *info) {
  uDebug("SML:0x%" PRIx64 " smlParseLineBottom start, format:%d, linenum:%d", info->id, info->dataFormat,
         info->lineNum);
  if (info->dataFormat) return TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < info->lineNum; i++) {
    SSmlLineInfo  *elements = info->lines + i;
    SSmlTableInfo *tinfo = NULL;
    if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
      SSmlTableInfo **tmp =
          (SSmlTableInfo **)taosHashGet(info->childTables, elements->measure, elements->measureTagsLen);
      if (tmp) tinfo = *tmp;
    } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
      SSmlTableInfo **tmp = (SSmlTableInfo **)taosHashGet(info->childTables, elements->measureTag,
                                                          elements->measureLen + elements->tagsLen);
      if (tmp) tinfo = *tmp;
    } else {
      SSmlTableInfo **tmp = (SSmlTableInfo **)taosHashGet(info->childTables, elements->measureTag,
                                                          elements->measureLen + elements->tagsLen);
      if (tmp) tinfo = *tmp;
    }

    if (tinfo == NULL) {
      uError("SML:0x%" PRIx64 "get oneTable failed, line num:%d", info->id, i);
      smlBuildInvalidDataMsg(&info->msgBuf, "get oneTable failed", elements->measure);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if (taosArrayGetSize(tinfo->tags) > TSDB_MAX_TAGS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "too many tags than 128", NULL);
      return TSDB_CODE_PAR_INVALID_TAGS_NUM;
    }

    if (taosArrayGetSize(elements->colArray) + taosArrayGetSize(tinfo->tags) > TSDB_MAX_COLUMNS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "too many columns than 4096", NULL);
      return TSDB_CODE_PAR_TOO_MANY_COLUMNS;
    }

    int ret = smlPushCols(tinfo->cols, elements->colArray);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    SSmlSTableMeta **tableMeta =
        (SSmlSTableMeta **)taosHashGet(info->superTables, elements->measure, elements->measureLen);
    if (tableMeta) {  // update meta
      uDebug("SML:0x%" PRIx64 " smlParseLineBottom update meta, format:%d, linenum:%d", info->id, info->dataFormat,
             info->lineNum);
      ret = smlUpdateMeta((*tableMeta)->colHash, (*tableMeta)->cols, elements->colArray, false, &info->msgBuf);
      if (ret == TSDB_CODE_SUCCESS) {
        ret = smlUpdateMeta((*tableMeta)->tagHash, (*tableMeta)->tags, tinfo->tags, true, &info->msgBuf);
      }
      if (ret != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
        return ret;
      }
    } else {
      uDebug("SML:0x%" PRIx64 " smlParseLineBottom add meta, format:%d, linenum:%d", info->id, info->dataFormat,
             info->lineNum);
      SSmlSTableMeta *meta = smlBuildSTableMeta(info->dataFormat);
      if(meta == NULL){
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      taosHashPut(info->superTables, elements->measure, elements->measureLen, &meta, POINTER_BYTES);
      terrno = 0;
      smlInsertMeta(meta->tagHash, meta->tags, tinfo->tags);
      if (terrno == TSDB_CODE_DUP_KEY) {
        return terrno;
      }
      smlInsertMeta(meta->colHash, meta->cols, elements->colArray);
    }
  }
  uDebug("SML:0x%" PRIx64 " smlParseLineBottom end, format:%d, linenum:%d", info->id, info->dataFormat, info->lineNum);

  return TSDB_CODE_SUCCESS;
}

static int32_t smlInsertData(SSmlHandle *info) {
  int32_t code = TSDB_CODE_SUCCESS;
  uDebug("SML:0x%" PRIx64 " smlInsertData start, format:%d", info->id, info->dataFormat);

  if (info->pRequest->dbList == NULL) {
    info->pRequest->dbList = taosArrayInit(1, TSDB_DB_FNAME_LEN);
  }
  char *data = (char *)taosArrayReserve(info->pRequest->dbList, 1);
  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));
  tNameGetFullDbName(&pName, data);

  SSmlTableInfo **oneTable = (SSmlTableInfo **)taosHashIterate(info->childTables, NULL);
  while (oneTable) {
    SSmlTableInfo *tableData = *oneTable;

    int   measureLen = tableData->sTableNameLen;
    char *measure = (char *)taosMemoryMalloc(tableData->sTableNameLen);
    memcpy(measure, tableData->sTableName, tableData->sTableNameLen);
    PROCESS_SLASH_IN_MEASUREMENT(measure, measureLen);
    smlStrReplace(measure, measureLen);
    memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
    memcpy(pName.tname, measure, measureLen);

    if (info->pRequest->tableList == NULL) {
      info->pRequest->tableList = taosArrayInit(1, sizeof(SName));
    }
    taosArrayPush(info->pRequest->tableList, &pName);

    strcpy(pName.tname, tableData->childTableName);

    SRequestConnInfo conn = {0};
    conn.pTrans = info->taos->pAppInfo->pTransporter;
    conn.requestId = info->pRequest->requestId;
    conn.requestObjRefId = info->pRequest->self;
    conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    code = smlCheckAuth(info, &conn, pName.tname, AUTH_TYPE_WRITE);
    if(code != TSDB_CODE_SUCCESS){
      taosMemoryFree(measure);
      taosHashCancelIterate(info->childTables, oneTable);
      return code;
    }

    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(info->pCatalog, &conn, &pName, &vg);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " catalogGetTableHashVgroup failed. table name: %s", info->id, tableData->childTableName);
      taosMemoryFree(measure);
      taosHashCancelIterate(info->childTables, oneTable);
      return code;
    }
    taosHashPut(info->pVgHash, (const char *)&vg.vgId, sizeof(vg.vgId), (char *)&vg, sizeof(vg));

    SSmlSTableMeta **pMeta =
        (SSmlSTableMeta **)taosHashGet(info->superTables, tableData->sTableName, tableData->sTableNameLen);
    if (unlikely(NULL == pMeta || NULL == (*pMeta)->tableMeta)) {
      uError("SML:0x%" PRIx64 " NULL == pMeta. table name: %s", info->id, tableData->childTableName);
      taosMemoryFree(measure);
      taosHashCancelIterate(info->childTables, oneTable);
      return TSDB_CODE_SML_INTERNAL_ERROR;
    }

    // use tablemeta of stable to save vgid and uid of child table
    (*pMeta)->tableMeta->vgId = vg.vgId;
    (*pMeta)->tableMeta->uid = tableData->uid;  // one table merge data block together according uid
    uDebug("SML:0x%" PRIx64 " smlInsertData table:%s, uid:%" PRIu64 ", format:%d", info->id, pName.tname,
           tableData->uid, info->dataFormat);

    code = smlBindData(info->pQuery, info->dataFormat, tableData->tags, (*pMeta)->cols, tableData->cols,
                       (*pMeta)->tableMeta, tableData->childTableName, measure, measureLen, info->ttl, info->msgBuf.buf,
                       info->msgBuf.len);
    taosMemoryFree(measure);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlBindData failed", info->id);
      taosHashCancelIterate(info->childTables, oneTable);
      return code;
    }
    oneTable = (SSmlTableInfo **)taosHashIterate(info->childTables, oneTable);
  }

  code = smlBuildOutput(info->pQuery, info->pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " smlBuildOutput failed", info->id);
    return code;
  }
  info->cost.insertRpcTime = taosGetTimestampUs();

  SAppClusterSummary *pActivity = &info->taos->pAppInfo->summary;
  atomic_add_fetch_64((int64_t *)&pActivity->numOfInsertsReq, 1);

  launchQueryImpl(info->pRequest, info->pQuery, true, NULL);
  uDebug("SML:0x%" PRIx64 " smlInsertData end, format:%d, code:%d,%s", info->id, info->dataFormat, info->pRequest->code,
         tstrerror(info->pRequest->code));

  return info->pRequest->code;
}

static void smlPrintStatisticInfo(SSmlHandle *info) {
  uDebug(
      "SML:0x%" PRIx64
      " smlInsertLines result, code:%d, msg:%s, lineNum:%d,stable num:%d,ctable num:%d,create stable num:%d,alter stable tag num:%d,alter stable col num:%d \
        parse cost:%" PRId64 ",schema cost:%" PRId64 ",bind cost:%" PRId64 ",rpc cost:%" PRId64 ",total cost:%" PRId64,
      info->id, info->cost.code, tstrerror(info->cost.code), info->cost.lineNum, info->cost.numOfSTables,
      info->cost.numOfCTables, info->cost.numOfCreateSTables, info->cost.numOfAlterTagSTables,
      info->cost.numOfAlterColSTables, info->cost.schemaTime - info->cost.parseTime,
      info->cost.insertBindTime - info->cost.schemaTime, info->cost.insertRpcTime - info->cost.insertBindTime,
      info->cost.endTime - info->cost.insertRpcTime, info->cost.endTime - info->cost.parseTime);

}

int32_t smlClearForRerun(SSmlHandle *info) {
  info->reRun = false;

  taosHashClear(info->childTables);
  taosHashClear(info->superTables);
  taosHashClear(info->tableUids);

  if (!info->dataFormat) {
    if (unlikely(info->lines != NULL)) {
      uError("SML:0x%" PRIx64 " info->lines != NULL", info->id);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    info->lines = (SSmlLineInfo *)taosMemoryCalloc(info->lineNum, sizeof(SSmlLineInfo));
  }

  memset(&info->preLine, 0, sizeof(SSmlLineInfo));
  info->currSTableMeta = NULL;
  info->currTableDataCtx = NULL;

  SVnodeModifyOpStmt *stmt = (SVnodeModifyOpStmt *)(info->pQuery->pRoot);
  stmt->freeHashFunc(stmt->pTableBlockHashObj);
  stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  return TSDB_CODE_SUCCESS;
}

static bool getLine(SSmlHandle *info, char *lines[], char **rawLine, char *rawLineEnd,
                    int numLines, int i, char** tmp, int *len){
  if (lines) {
    *tmp = lines[i];
    *len = strlen(*tmp);
  } else if (*rawLine) {
    *tmp = *rawLine;
    while (*rawLine < rawLineEnd) {
      if (*((*rawLine)++) == '\n') {
        break;
      }
      (*len)++;
    }
    if (info->protocol == TSDB_SML_LINE_PROTOCOL && (*tmp)[0] == '#') {  // this line is comment
      return false;
    }
  }

  if(*rawLine != NULL && (uDebugFlag & DEBUG_DEBUG)){
    char* print = taosMemoryCalloc(*len + 1, 1);
    memcpy(print, *tmp, *len);
    uDebug("SML:0x%" PRIx64 " smlParseLine is raw, numLines:%d, protocol:%d, len:%d, data:%s", info->id,
            numLines, info->protocol, *len, print);
    taosMemoryFree(print);
  }else{
    uDebug("SML:0x%" PRIx64 " smlParseLine is not numLines:%d, protocol:%d, len:%d, data:%s", info->id,
           numLines, info->protocol, *len, *tmp);
  }
  return true;
}

static int32_t smlParseLine(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
  uDebug("SML:0x%" PRIx64 " smlParseLine start", info->id);
  int32_t code = TSDB_CODE_SUCCESS;
  if (info->protocol == TSDB_SML_JSON_PROTOCOL) {
    if (lines) {
      code = smlParseJSON(info, *lines);
    } else if (rawLine) {
      code = smlParseJSON(info, rawLine);
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseJSON failed:%s", info->id, lines ? *lines : rawLine);
      return code;
    }
    return code;
  }

  char   *oldRaw = rawLine;
  int32_t i = 0;
  while (i < numLines) {
    char *tmp = NULL;
    int   len = 0;
    if(!getLine(info, lines, &rawLine, rawLineEnd, numLines, i, &tmp, &len)){
      continue;
    }
    if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
      if (info->dataFormat) {
        SSmlLineInfo element = {0};
        code = smlParseInfluxString(info, tmp, tmp + len, &element);
      } else {
        code = smlParseInfluxString(info, tmp, tmp + len, info->lines + i);
      }
    } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
      if (info->dataFormat) {
        SSmlLineInfo element = {0};
        code = smlParseTelnetString(info, (char *)tmp, (char *)tmp + len, &element);
        if (element.measureTagsLen != 0) taosMemoryFree(element.measureTag);
      } else {
        code = smlParseTelnetString(info, (char *)tmp, (char *)tmp + len, info->lines + i);
      }
    } else {
      code = TSDB_CODE_SML_INVALID_PROTOCOL_TYPE;
    }
    if (code != TSDB_CODE_SUCCESS) {
      if(rawLine != NULL){
        char* print = taosMemoryCalloc(len + 1, 1);
        memcpy(print, tmp, len);
        uError("SML:0x%" PRIx64 " smlParseLine failed. line %d : %s", info->id, i, print);
        taosMemoryFree(print);
      }else{
        uError("SML:0x%" PRIx64 " smlParseLine failed. line %d : %s", info->id, i, tmp);
      }
      return code;
    }
    if (info->reRun) {
      uDebug("SML:0x%" PRIx64 " smlParseLine re run", info->id);
      i = 0;
      rawLine = oldRaw;
      code = smlClearForRerun(info);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      continue;
    }
    i++;
  }
  uDebug("SML:0x%" PRIx64 " smlParseLine end", info->id);

  return code;
}

static int smlProcess(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t retryNum = 0;

  info->cost.parseTime = taosGetTimestampUs();

  code = smlParseLine(info, lines, rawLine, rawLineEnd, numLines);
  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlParseLine error : %s", info->id, tstrerror(code));
    return code;
  }
  code = smlParseLineBottom(info);
  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlParseLineBottom error : %s", info->id, tstrerror(code));
    return code;
  }

  info->cost.lineNum = info->lineNum;
  info->cost.numOfSTables = taosHashGetSize(info->superTables);
  info->cost.numOfCTables = taosHashGetSize(info->childTables);

  info->cost.schemaTime = taosGetTimestampUs();

  do {
    code = smlModifyDBSchemas(info);
    if (code != TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER && code != TSDB_CODE_SDB_OBJ_CREATING && code != TSDB_CODE_MND_TRANS_CONFLICT) {
      break;
    }
    taosMsleep(100);
    uInfo("SML:0x%" PRIx64 " smlModifyDBSchemas retry code:%s, times:%d", info->id, tstrerror(code), retryNum);
  } while (retryNum++ < taosHashGetSize(info->superTables) * MAX_RETRY_TIMES);

  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlModifyDBSchemas error : %s", info->id, tstrerror(code));
    return code;
  }

  info->cost.insertBindTime = taosGetTimestampUs();
  code = smlInsertData(info);
  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlInsertData error : %s", info->id, tstrerror(code));
    return code;
  }

  return code;
}

void smlSetReqSQL(SRequestObj *request, char *lines[], char *rawLine, char *rawLineEnd) {
  if (tsSlowLogScope & SLOW_LOG_TYPE_INSERT) {
    int32_t len = 0;
    int32_t rlen = 0;
    char* p = NULL;

    if (lines && lines[0]) {
      len = strlen(lines[0]);
      p = lines[0];
    } else if (rawLine) {
      if (rawLineEnd) {
        len = rawLineEnd - rawLine;
      } else {
        len = strlen(rawLine);
      }
      p = rawLine;
    }

    if (NULL == p) {
      return;
    }

    rlen = TMIN(len, TSDB_MAX_ALLOWED_SQL_LEN);
    rlen = TMAX(rlen, 0);

    char *sql = taosMemoryMalloc(rlen + 1);
    if (NULL == sql) {
      uError("malloc %d for sml sql failed", rlen + 1);
      return;
    }
    memcpy(sql, p, rlen);
    sql[rlen] = 0;

    request->sqlstr = sql;
    request->sqlLen = rlen;
  }
}

TAOS_RES *taos_schemaless_insert_inner(TAOS *taos, char *lines[], char *rawLine, char *rawLineEnd, int numLines,
                                       int protocol, int precision, int32_t ttl, int64_t reqid) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  SRequestObj *request = NULL;
  SSmlHandle  *info = NULL;
  int          cnt = 0;
  while (1) {
    request = (SRequestObj *)createRequest(*(int64_t *)taos, TSDB_SQL_INSERT, reqid);
    if (request == NULL) {
      uError("SML:taos_schemaless_insert error request is null");
      return NULL;
    }

    info = smlBuildSmlInfo(taos);
    if (info == NULL) {
      request->code = TSDB_CODE_OUT_OF_MEMORY;
      uError("SML:taos_schemaless_insert error SSmlHandle is null");
      return (TAOS_RES *)request;
    }
    info->pRequest = request;
    info->ttl = ttl;
    info->precision = precision;
    info->protocol = (TSDB_SML_PROTOCOL_TYPE)protocol;
    info->msgBuf.buf = info->pRequest->msgBuf;
    info->msgBuf.len = ERROR_MSG_BUF_DEFAULT_SIZE;
    info->lineNum = numLines;

    smlSetReqSQL(request, lines, rawLine, rawLineEnd);

    SSmlMsgBuf msg = {ERROR_MSG_BUF_DEFAULT_SIZE, request->msgBuf};
    if (request->pDb == NULL) {
      request->code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
      smlBuildInvalidDataMsg(&msg, "Database not specified", NULL);
      goto end;
    }

    if (protocol < TSDB_SML_LINE_PROTOCOL || protocol > TSDB_SML_JSON_PROTOCOL) {
      request->code = TSDB_CODE_SML_INVALID_PROTOCOL_TYPE;
      smlBuildInvalidDataMsg(&msg, "protocol invalidate", NULL);
      goto end;
    }

    if (protocol == TSDB_SML_LINE_PROTOCOL &&
        (precision < TSDB_SML_TIMESTAMP_NOT_CONFIGURED || precision > TSDB_SML_TIMESTAMP_NANO_SECONDS)) {
      request->code = TSDB_CODE_SML_INVALID_PRECISION_TYPE;
      smlBuildInvalidDataMsg(&msg, "precision invalidate for line protocol", NULL);
      goto end;
    }

    if (protocol == TSDB_SML_JSON_PROTOCOL) {
      numLines = 1;
    } else if (numLines <= 0) {
      request->code = TSDB_CODE_SML_INVALID_DATA;
      smlBuildInvalidDataMsg(&msg, "line num is invalid", NULL);
      goto end;
    }

    code = smlProcess(info, lines, rawLine, rawLineEnd, numLines);
    request->code = code;
    info->cost.endTime = taosGetTimestampUs();
    info->cost.code = code;
    if (NEED_CLIENT_HANDLE_ERROR(code) || code == TSDB_CODE_SDB_OBJ_CREATING ||
        code == TSDB_CODE_PAR_VALUE_TOO_LONG || code == TSDB_CODE_MND_TRANS_CONFLICT) {
      if (cnt++ >= 10) {
        uInfo("SML:%" PRIx64 " retry:%d/10 end code:%d, msg:%s", info->id, cnt, code, tstrerror(code));
        break;
      }
      taosMsleep(100);
      refreshMeta(request->pTscObj, request);
      uInfo("SML:%" PRIx64 " retry:%d/10,ver is old retry or object is creating code:%d, msg:%s", info->id, cnt, code,
            tstrerror(code));
      smlDestroyInfo(info);
      info = NULL;
      taos_free_result(request);
      request = NULL;
      continue;
    }
    smlPrintStatisticInfo(info);
    break;
  }

end:
  smlDestroyInfo(info);
  return (TAOS_RES *)request;
}

/**
 * taos_schemaless_insert() parse and insert data points into database according to
 * different protocol.
 *
 * @param $lines input array may contain multiple lines, each line indicates a data point.
 *               If protocol=2 is used input array should contain single JSON
 *               string(e.g. char *lines[] = {"$JSON_string"}). If need to insert
 *               multiple data points in JSON format, should include them in $JSON_string
 *               as a JSON array.
 * @param $numLines indicates how many data points in $lines.
 *                  If protocol = 2 is used this param will be ignored as $lines should
 *                  contain single JSON string.
 * @param $protocol indicates which protocol to use for parsing:
 *                  0 - influxDB line protocol
 *                  1 - OpenTSDB telnet line protocol
 *                  2 - OpenTSDB JSON format protocol
 * @return TAOS_RES
 */

TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl, int64_t reqid) {
  return taos_schemaless_insert_inner(taos, lines, NULL, NULL, numLines, protocol, precision, ttl, reqid);
}

TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  return taos_schemaless_insert_ttl_with_reqid(taos, lines, numLines, protocol, precision, TSDB_DEFAULT_TABLE_TTL, 0);
}

TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                     int32_t ttl) {
  return taos_schemaless_insert_ttl_with_reqid(taos, lines, numLines, protocol, precision, ttl, 0);
}

TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                            int64_t reqid) {
  return taos_schemaless_insert_ttl_with_reqid(taos, lines, numLines, protocol, precision, TSDB_DEFAULT_TABLE_TTL,
                                               reqid);
}

static void getRawLineLen(char *lines, int len, int32_t *totalRows, int protocol){
  int numLines = 0;
  *totalRows = 0;
  char *tmp = lines;
  for (int i = 0; i < len; i++) {
    if (lines[i] == '\n' || i == len - 1) {
      numLines++;
      if (tmp[0] != '#' || protocol != TSDB_SML_LINE_PROTOCOL) {  // ignore comment
        (*totalRows)++;
      }
      tmp = lines + i + 1;
    }
  }
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl, int64_t reqid) {
  getRawLineLen(lines, len, totalRows, protocol);
  return taos_schemaless_insert_inner(taos, NULL, lines, lines + len, *totalRows, protocol, precision, ttl, reqid);
}

TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                int precision, int64_t reqid) {
  return taos_schemaless_insert_raw_ttl_with_reqid(taos, lines, len, totalRows, protocol, precision,
                                                   TSDB_DEFAULT_TABLE_TTL, reqid);
}
TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                         int precision, int32_t ttl) {
  return taos_schemaless_insert_raw_ttl_with_reqid(taos, lines, len, totalRows, protocol, precision, ttl, 0);
}

TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                     int precision) {
  return taos_schemaless_insert_raw_ttl_with_reqid(taos, lines, len, totalRows, protocol, precision,
                                                   TSDB_DEFAULT_TABLE_TTL, 0);
}
