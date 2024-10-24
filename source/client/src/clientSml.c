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

#define IS_COMMENT(protocol,data)                             \
  (protocol == TSDB_SML_LINE_PROTOCOL && data == '#')

int64_t smlToMilli[] = {3600000LL, 60000LL, 1000LL};
int64_t smlFactorNS[] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
int64_t smlFactorS[] = {1000LL, 1000000LL, 1000000000LL};

static int32_t smlCheckAuth(SSmlHandle *info, SRequestConnInfo *conn, const char *pTabName, AUTH_TYPE type) {
  SUserAuthInfo pAuth = {0};
  (void)snprintf(pAuth.user, sizeof(pAuth.user), "%s", info->taos->user);
  if (NULL == pTabName) {
    if (tNameSetDbName(&pAuth.tbName, info->taos->acctId, info->pRequest->pDb, strlen(info->pRequest->pDb)) != 0) {
      return TSDB_CODE_SML_INVALID_DATA;
    }
  } else {
    toName(info->taos->acctId, info->pRequest->pDb, pTabName, &pAuth.tbName);
  }
  pAuth.type = type;

  int32_t      code = TSDB_CODE_SUCCESS;
  SUserAuthRes authRes = {0};

  code = catalogChkAuth(info->pCatalog, conn, &pAuth, &authRes);
  nodesDestroyNode(authRes.pCond[AUTH_RES_BASIC]);

  return (code == TSDB_CODE_SUCCESS)
             ? (authRes.pass[AUTH_RES_BASIC] ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_PERMISSION_DENIED)
             : code;
}

void smlBuildInvalidDataMsg(SSmlMsgBuf *pBuf, const char *msg1, const char *msg2) {
  if (pBuf->buf == NULL) {
    return;
  }
  (void)memset(pBuf->buf, 0, pBuf->len);
  if (msg1) {
    (void)strncat(pBuf->buf, msg1, pBuf->len - 1);
  }
  int32_t left = pBuf->len - strlen(pBuf->buf);
  if (left > 2 && msg2) {
    (void)strncat(pBuf->buf, ":", left - 1);
    (void)strncat(pBuf->buf, msg2, left - 2);
  }
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

int32_t smlBuildTableInfo(int numRows, const char *measure, int32_t measureLen, SSmlTableInfo **tInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  SSmlTableInfo *tag = (SSmlTableInfo *)taosMemoryCalloc(sizeof(SSmlTableInfo), 1);
  SML_CHECK_NULL(tag)

  tag->sTableName = measure;
  tag->sTableNameLen = measureLen;

  tag->cols = taosArrayInit(numRows, POINTER_BYTES);
  SML_CHECK_NULL(tag->cols)
  *tInfo = tag;
  return code;

END:
  taosMemoryFree(tag);
  uError("%s failed code:%d line:%d", __FUNCTION__ , code, lino);
  return code;
}

void smlBuildTsKv(SSmlKv *kv, int64_t ts) {
  kv->key = tsSmlTsDefaultName;
  kv->keyLen = strlen(tsSmlTsDefaultName);
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->i = ts;
  kv->length = (size_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
}

static void smlDestroySTableMeta(void *para) {
  if (para == NULL) {
    return;
  }
  SSmlSTableMeta *meta = *(SSmlSTableMeta **)para;
  if (meta == NULL) {
    return;
  }
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->colHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFreeClear(meta->tableMeta);
  taosMemoryFree(meta);
}

int32_t smlBuildSuperTableInfo(SSmlHandle *info, SSmlLineInfo *currElement, SSmlSTableMeta **sMeta) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  STableMeta *pTableMeta = NULL;

  int   measureLen = currElement->measureLen;
  char *measure = (char *)taosMemoryMalloc(measureLen);
  SML_CHECK_NULL(measure);
  (void)memcpy(measure, currElement->measure, measureLen);
  if (currElement->measureEscaped) {
    PROCESS_SLASH_IN_MEASUREMENT(measure, measureLen);
  }
  smlStrReplace(measure, measureLen);
  code = smlGetMeta(info, measure, measureLen, &pTableMeta);
  taosMemoryFree(measure);
  if (code != TSDB_CODE_SUCCESS) {
    info->dataFormat = false;
    info->reRun = true;
    goto END;
  }
  SML_CHECK_CODE(smlBuildSTableMeta(info->dataFormat, sMeta));
  for (int i = 1; i < pTableMeta->tableInfo.numOfTags + pTableMeta->tableInfo.numOfColumns; i++) {
    SSchema *col = pTableMeta->schema + i;
    SSmlKv   kv = {.key = col->name, .keyLen = strlen(col->name), .type = col->type};
    if (col->type == TSDB_DATA_TYPE_NCHAR) {
      kv.length = (col->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    } else if (col->type == TSDB_DATA_TYPE_BINARY || col->type == TSDB_DATA_TYPE_GEOMETRY ||
               col->type == TSDB_DATA_TYPE_VARBINARY) {
      kv.length = col->bytes - VARSTR_HEADER_SIZE;
    } else {
      kv.length = col->bytes;
    }

    if (i < pTableMeta->tableInfo.numOfColumns) {
      SML_CHECK_NULL(taosArrayPush((*sMeta)->cols, &kv));
    } else {
      SML_CHECK_NULL(taosArrayPush((*sMeta)->tags, &kv));
    }
  }
  SML_CHECK_CODE(taosHashPut(info->superTables, currElement->measure, currElement->measureLen, sMeta, POINTER_BYTES));
  (*sMeta)->tableMeta = pTableMeta;
  return code;

END:
  smlDestroySTableMeta(sMeta);
  taosMemoryFreeClear(pTableMeta);
  RETURN
}

bool isSmlColAligned(SSmlHandle *info, int cnt, SSmlKv *kv) {
  // cnt begin 0, add ts so + 2
  if (unlikely(cnt + 2 > info->currSTableMeta->tableInfo.numOfColumns)) {
    goto END;
  }
  // bind data
  int32_t ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kv, cnt + 1);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    uDebug("smlBuildCol error, retry");
    goto END;
  }
  if (cnt >= taosArrayGetSize(info->maxColKVs)) {
    goto END;
  }
  SSmlKv *maxKV = (SSmlKv *)taosArrayGet(info->maxColKVs, cnt);
  if (maxKV == NULL) {
    goto END;
  }
  if (unlikely(!IS_SAME_KEY)) {
    goto END;
  }

  if (unlikely(IS_VAR_DATA_TYPE(kv->type) && kv->length > maxKV->length)) {
    maxKV->length = kv->length;
    info->needModifySchema = true;
  }
  return true;

END:
  info->dataFormat = false;
  info->reRun = true;
  return false;
}

bool isSmlTagAligned(SSmlHandle *info, int cnt, SSmlKv *kv) {
  if (unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)) {
    goto END;
  }

  if (unlikely(cnt >= taosArrayGetSize(info->maxTagKVs))) {
    goto END;
  }
  SSmlKv *maxKV = (SSmlKv *)taosArrayGet(info->maxTagKVs, cnt);
  if (maxKV == NULL) {
    goto END;
  }
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

int32_t smlJoinMeasureTag(SSmlLineInfo *elements) {
  elements->measureTag = (char *)taosMemoryMalloc(elements->measureLen + elements->tagsLen);
  if (elements->measureTag == NULL) {
    return terrno;
  }
  (void)memcpy(elements->measureTag, elements->measure, elements->measureLen);
  (void)memcpy(elements->measureTag + elements->measureLen, elements->tags, elements->tagsLen);
  elements->measureTagsLen = elements->measureLen + elements->tagsLen;
  return TSDB_CODE_SUCCESS;
}

static bool smlIsPKTable(STableMeta *pTableMeta) {
  for (int i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
    if (pTableMeta->schema[i].flags & COL_IS_KEY) {
      return true;
    }
  }

  return false;
}

int32_t smlProcessSuperTable(SSmlHandle *info, SSmlLineInfo *elements) {
  bool isSameMeasure = IS_SAME_SUPER_TABLE;
  if (isSameMeasure) {
    return TSDB_CODE_SUCCESS;
  }
  SSmlSTableMeta **tmp = (SSmlSTableMeta **)taosHashGet(info->superTables, elements->measure, elements->measureLen);

  SSmlSTableMeta *sMeta = NULL;
  if (unlikely(tmp == NULL)) {
    int32_t code = smlBuildSuperTableInfo(info, elements, &sMeta);
    if (code != 0) return code;
  } else {
    sMeta = *tmp;
  }
  if (sMeta == NULL) {
    uError("smlProcessSuperTable failed to get super table meta");
    return TSDB_CODE_SML_INTERNAL_ERROR;
  }
  info->currSTableMeta = sMeta->tableMeta;
  info->maxTagKVs = sMeta->tags;
  info->maxColKVs = sMeta->cols;

  if (smlIsPKTable(sMeta->tableMeta)) {
    return TSDB_CODE_SML_NOT_SUPPORT_PK;
  }
  return 0;
}

int32_t smlProcessChildTable(SSmlHandle *info, SSmlLineInfo *elements) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SSmlTableInfo **oneTable = (SSmlTableInfo **)taosHashGet(info->childTables, elements->measureTag, elements->measureTagsLen);
  SSmlTableInfo *tinfo = NULL;
  if (unlikely(oneTable == NULL)) {
    SML_CHECK_CODE(smlBuildTableInfo(1, elements->measure, elements->measureLen, &tinfo));
    SML_CHECK_CODE(taosHashPut(info->childTables, elements->measureTag, elements->measureTagsLen, &tinfo, POINTER_BYTES));

    tinfo->tags = taosArrayDup(info->preLineTagKV, NULL);
    SML_CHECK_NULL(tinfo->tags);
    for (size_t i = 0; i < taosArrayGetSize(info->preLineTagKV); i++) {
      SSmlKv *kv = (SSmlKv *)taosArrayGet(info->preLineTagKV, i);
      SML_CHECK_NULL(kv);
      if (kv->keyEscaped) kv->key = NULL;
      if (kv->valueEscaped) kv->value = NULL;
    }

    SML_CHECK_CODE(smlSetCTableName(tinfo, info->tbnameKey));
    SML_CHECK_CODE(getTableUid(info, elements, tinfo));
    if (info->dataFormat) {
      info->currSTableMeta->uid = tinfo->uid;
      SML_CHECK_CODE(smlInitTableDataCtx(info->pQuery, info->currSTableMeta, &tinfo->tableDataCtx));
    }
  } else {
    tinfo = *oneTable;
  }
  if (info->dataFormat) info->currTableDataCtx = tinfo->tableDataCtx;
  return TSDB_CODE_SUCCESS;

END:
  smlDestroyTableInfo(&tinfo);
  RETURN
}

int32_t smlParseEndTelnetJsonFormat(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs, SSmlKv *kv) {
  int32_t code = 0;
  int32_t lino = 0;
  uDebug("SML:0x%" PRIx64 " %s format true, ts:%" PRId64, info->id, __FUNCTION__ , kvTs->i);
  SML_CHECK_CODE(smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kvTs, 0));
  SML_CHECK_CODE(smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kv, 1));
  SML_CHECK_CODE(smlBuildRow(info->currTableDataCtx));

END:
  clearColValArraySml(info->currTableDataCtx->pValues);
  RETURN
}

int32_t smlParseEndTelnetJsonUnFormat(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs, SSmlKv *kv) {
  int32_t code = 0;
  int32_t lino = 0;
  uDebug("SML:0x%" PRIx64 " %s format false, ts:%" PRId64, info->id, __FUNCTION__, kvTs->i);
  if (elements->colArray == NULL) {
    elements->colArray = taosArrayInit(16, sizeof(SSmlKv));
    SML_CHECK_NULL(elements->colArray);
  }
  SML_CHECK_NULL(taosArrayPush(elements->colArray, kvTs));
  SML_CHECK_NULL (taosArrayPush(elements->colArray, kv));

END:
  RETURN
}

int32_t smlParseEndLine(SSmlHandle *info, SSmlLineInfo *elements, SSmlKv *kvTs) {
  if (info->dataFormat) {
    uDebug("SML:0x%" PRIx64 " %s format true, ts:%" PRId64, info->id, __FUNCTION__, kvTs->i);
    int32_t ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, kvTs, 0);
    if (ret == TSDB_CODE_SUCCESS) {
      ret = smlBuildRow(info->currTableDataCtx);
    }

    clearColValArraySml(info->currTableDataCtx->pValues);
    taosArrayClearP(info->escapedStringList, taosMemoryFree);
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      uError("SML:0x%" PRIx64 " %s smlBuildCol error:%d", info->id, __FUNCTION__, ret);
      return ret;
    }
  } else {
    uDebug("SML:0x%" PRIx64 " %s format false, ts:%" PRId64, info->id, __FUNCTION__, kvTs->i);
    taosArraySet(elements->colArray, 0, kvTs);
  }
  info->preLine = *elements;

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTableName(SArray *tags, char *childTableName, char *tbnameKey) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    autoChildName = false;
  size_t  delimiter = strlen(tsSmlAutoChildTableNameDelimiter);
  if (delimiter > 0 && tbnameKey == NULL) {
    size_t totalNameLen = delimiter * (taosArrayGetSize(tags) - 1);
    for (int i = 0; i < taosArrayGetSize(tags); i++) {
      SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
      SML_CHECK_NULL(tag);
      totalNameLen += tag->length;
    }
    if (totalNameLen < TSDB_TABLE_NAME_LEN) {
      autoChildName = true;
    }
  }
  if (autoChildName) {
    (void)memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
    for (int i = 0; i < taosArrayGetSize(tags); i++) {
      SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
      SML_CHECK_NULL(tag);
      (void)strncat(childTableName, tag->value, TMIN(tag->length, TSDB_TABLE_NAME_LEN - 1 - strlen(childTableName)));
      if (i != taosArrayGetSize(tags) - 1) {
        (void)strncat(childTableName, tsSmlAutoChildTableNameDelimiter, TSDB_TABLE_NAME_LEN - 1 - strlen(childTableName));
      }
    }
    if (tsSmlDot2Underline) {
      smlStrReplace(childTableName, strlen(childTableName));
    }
  } else {
    if (tbnameKey == NULL) {
      tbnameKey = tsSmlChildTableName;
    }
    size_t childTableNameLen = strlen(tbnameKey);
    if (childTableNameLen <= 0) return TSDB_CODE_SUCCESS;

    for (int i = 0; i < taosArrayGetSize(tags); i++) {
      SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
      SML_CHECK_NULL(tag);
      // handle child table name
      if (childTableNameLen == tag->keyLen && strncmp(tag->key, tbnameKey, tag->keyLen) == 0) {
        (void)memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
        tstrncpy(childTableName, tag->value, TMIN(TSDB_TABLE_NAME_LEN, tag->length + 1));
        if (tsSmlDot2Underline) {
          smlStrReplace(childTableName, strlen(childTableName));
        }
        taosArrayRemove(tags, i);
        break;
      }
    }
  }

END:
  RETURN
}

int32_t smlSetCTableName(SSmlTableInfo *oneTable, char *tbnameKey) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray *dst  = NULL;
  SML_CHECK_CODE(smlParseTableName(oneTable->tags, oneTable->childTableName, tbnameKey));

  if (strlen(oneTable->childTableName) == 0) {
    dst = taosArrayDup(oneTable->tags, NULL);
    SML_CHECK_NULL(dst);
    if (oneTable->sTableNameLen >= TSDB_TABLE_NAME_LEN) {
      code = TSDB_CODE_SML_INTERNAL_ERROR;
      goto END;
    }
    char          superName[TSDB_TABLE_NAME_LEN] = {0};
    RandTableName rName = {dst, NULL, (uint8_t)oneTable->sTableNameLen, oneTable->childTableName};
    if (tsSmlDot2Underline) {
      (void)memcpy(superName, oneTable->sTableName, oneTable->sTableNameLen);
      smlStrReplace(superName, oneTable->sTableNameLen);
      rName.stbFullName = superName;
    } else {
      rName.stbFullName = oneTable->sTableName;
    }

    SML_CHECK_CODE(buildChildTableName(&rName));
  }

END:
  taosArrayDestroy(dst);
  RETURN
}

int32_t getTableUid(SSmlHandle *info, SSmlLineInfo *currElement, SSmlTableInfo *tinfo) {
  char   key[TSDB_TABLE_NAME_LEN * 2 + 1] = {0};
  size_t nLen = strlen(tinfo->childTableName);
  (void)memcpy(key, currElement->measure, currElement->measureLen);
  if (tsSmlDot2Underline) {
    smlStrReplace(key, currElement->measureLen);
  }
  (void)memcpy(key + currElement->measureLen + 1, tinfo->childTableName, nLen);
  void *uid =
      taosHashGet(info->tableUids, key,
                  currElement->measureLen + 1 + nLen);  // use \0 as separator for stable name and child table name
  if (uid == NULL) {
    tinfo->uid = info->uid++;
    return taosHashPut(info->tableUids, key, currElement->measureLen + 1 + nLen, &tinfo->uid, sizeof(uint64_t));
  } else {
    tinfo->uid = *(uint64_t *)uid;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t smlBuildSTableMeta(bool isDataFormat, SSmlSTableMeta **sMeta) {
  int32_t code = 0;
  int32_t lino = 0;
  SSmlSTableMeta *meta = (SSmlSTableMeta *)taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
  SML_CHECK_NULL(meta);
  if (unlikely(!isDataFormat)) {
    meta->tagHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    SML_CHECK_NULL(meta->tagHash);
    meta->colHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    SML_CHECK_NULL(meta->colHash);
  }

  meta->tags = taosArrayInit(32, sizeof(SSmlKv));
  SML_CHECK_NULL(meta->tags);

  meta->cols = taosArrayInit(32, sizeof(SSmlKv));
  SML_CHECK_NULL(meta->cols);
  *sMeta = meta;
  return TSDB_CODE_SUCCESS;

END:
  smlDestroySTableMeta(&meta);
  uError("%s failed code:%d line:%d", __FUNCTION__ , code, lino);
  return code;
}

int32_t smlParseNumber(SSmlKv *kvVal, SSmlMsgBuf *msg) {
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

int32_t smlGetMeta(SSmlHandle *info, const void *measure, int32_t measureLen, STableMeta **pTableMeta) {
  *pTableMeta = NULL;

  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));

  SRequestConnInfo conn = {0};
  conn.pTrans = info->taos->pAppInfo->pTransporter;
  conn.requestId = info->pRequest->requestId;
  conn.requestObjRefId = info->pRequest->self;
  conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
  (void)memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
  (void)memcpy(pName.tname, measure, measureLen);

  return catalogGetSTableMeta(info->pCatalog, &conn, &pName, pTableMeta);
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
      snprintf(info->msgBuf.buf, info->msgBuf.len, "SML:0x%" PRIx64 " %s point type and db type mismatch. db type: %d, point type: %d, key: %s",
               info->id, __FUNCTION__, colField[*index].type, kv->type, kv->key);
      uError("%s", info->msgBuf.buf);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if (((colField[*index].type == TSDB_DATA_TYPE_VARCHAR || colField[*index].type == TSDB_DATA_TYPE_VARBINARY ||
          colField[*index].type == TSDB_DATA_TYPE_GEOMETRY) &&
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
  return TSDB_CODE_SUCCESS;
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

  if ((type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_GEOMETRY) &&
      result > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
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
                                      SArray *checkDumplicateCols, ESchemaAction *action, bool isTag) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  for (int j = 0; j < taosArrayGetSize(cols); ++j) {
    if (j == 0 && !isTag) continue;
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, j);
    SML_CHECK_NULL(kv);
    SML_CHECK_CODE(smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, action, info));
  }

  for (int j = 0; j < taosArrayGetSize(checkDumplicateCols); ++j) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(checkDumplicateCols, j);
    SML_CHECK_NULL(kv);
    if (taosHashGet(schemaHash, kv->key, kv->keyLen) != NULL) {
      code = TSDB_CODE_PAR_DUPLICATED_COLUMN;
      goto END;
    }
  }
END:
  RETURN
}

static int32_t smlCheckMeta(SSchema *schema, int32_t length, SArray *cols, bool isTag) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SHashObj *hashTmp = taosHashInit(length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SML_CHECK_NULL(hashTmp);
  int32_t i = 0;
  for (; i < length; i++) {
    SML_CHECK_CODE(taosHashPut(hashTmp, schema[i].name, strlen(schema[i].name), &i, SHORT_BYTES));
  }
  i = isTag ? 0 : 1;
  for (; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    SML_CHECK_NULL(kv);
    if (taosHashGet(hashTmp, kv->key, kv->keyLen) == NULL) {
      code = TSDB_CODE_SML_INVALID_DATA;
      goto END;
    }
  }

END:
  taosHashCleanup(hashTmp);
  RETURN
}

static int32_t getBytes(uint8_t type, int32_t length) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_NCHAR ||
      type == TSDB_DATA_TYPE_GEOMETRY) {
    return smlFindNearestPowerOf2(length, type);
  } else {
    return tDataTypes[type].bytes;
  }
}

static int32_t smlBuildFieldsList(SSmlHandle *info, SSchema *schemaField, SHashObj *schemaHash, SArray *cols,
                                  SArray *results, int32_t numOfCols, bool isTag) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = TSDB_CODE_SUCCESS;
  for (int j = 0; j < taosArrayGetSize(cols); ++j) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, j);
    SML_CHECK_NULL(kv);
    ESchemaAction action = SCHEMA_ACTION_NULL;
    SML_CHECK_CODE(smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, &action, info));
    if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_ADD_TAG) {
      SField field = {0};
      field.type = kv->type;
      field.bytes = getBytes(kv->type, kv->length);
      (void)memcpy(field.name, kv->key, TMIN(kv->keyLen, sizeof(field.name) - 1));
      SML_CHECK_NULL(taosArrayPush(results, &field));
    } else if (action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
      uint16_t *index = (uint16_t *)taosHashGet(schemaHash, kv->key, kv->keyLen);
      if (index == NULL) {
        uError("smlBuildFieldsList get error, key:%s", kv->key);
        code = TSDB_CODE_SML_INVALID_DATA;
        goto END;
      }
      uint16_t newIndex = *index;
      if (isTag) newIndex -= numOfCols;
      SField *field = (SField *)taosArrayGet(results, newIndex);
      SML_CHECK_NULL(field);
      field->bytes = getBytes(kv->type, kv->length);
    }
  }

  int32_t maxLen = isTag ? TSDB_MAX_TAGS_LEN : TSDB_MAX_BYTES_PER_ROW;
  int32_t len = 0;
  for (int j = 0; j < taosArrayGetSize(results); ++j) {
    SField *field = taosArrayGet(results, j);
    SML_CHECK_NULL(field);
    len += field->bytes;
  }
  if (len > maxLen) {
    return isTag ? TSDB_CODE_PAR_INVALID_TAGS_LENGTH : TSDB_CODE_PAR_INVALID_ROW_LENGTH;
  }

END:
  RETURN
}

static FORCE_INLINE void smlBuildCreateStbReq(SMCreateStbReq *pReq, int32_t colVer, int32_t tagVer, tb_uid_t suid, int8_t source){
  pReq->colVer = colVer;
  pReq->tagVer = tagVer;
  pReq->suid = suid;
  pReq->source = source;
}
static int32_t smlSendMetaMsg(SSmlHandle *info, SName *pName, SArray *pColumns, SArray **pTags, STableMeta *pTableMeta,
                              ESchemaAction action) {
  SRequestObj   *pRequest = NULL;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SCmdMsgInfo    pCmdMsg = {0};
  char          *pSql = NULL;

  // put front for free
  pReq.numOfColumns = taosArrayGetSize(pColumns);
  pReq.pTags = *pTags;
  pReq.numOfTags = taosArrayGetSize(*pTags);
  *pTags = NULL;

  pReq.pColumns = taosArrayInit(pReq.numOfColumns, sizeof(SFieldWithOptions));
  SML_CHECK_NULL(pReq.pColumns);
  for (int32_t i = 0; i < pReq.numOfColumns; ++i) {
    SField *pField = taosArrayGet(pColumns, i);
    SML_CHECK_NULL(pField);
    SFieldWithOptions fieldWithOption = {0};
    setFieldWithOptions(&fieldWithOption, pField);
    setDefaultOptionsForField(&fieldWithOption);
    SML_CHECK_NULL(taosArrayPush(pReq.pColumns, &fieldWithOption));
  }

  if (action == SCHEMA_ACTION_CREATE_STABLE) {
    pSql = "sml_create_stable";
    smlBuildCreateStbReq(&pReq, 1, 1, 0, TD_REQ_FROM_APP);
  } else if (action == SCHEMA_ACTION_ADD_TAG || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
    pSql = (action == SCHEMA_ACTION_ADD_TAG) ? "sml_add_tag" : "sml_modify_tag_size";
    smlBuildCreateStbReq(&pReq, pTableMeta->sversion, pTableMeta->tversion + 1, pTableMeta->uid, TD_REQ_FROM_TAOX);
  } else if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE) {
    pSql = (action == SCHEMA_ACTION_ADD_COLUMN) ? "sml_add_column" : "sml_modify_column_size";
    smlBuildCreateStbReq(&pReq, pTableMeta->sversion + 1, pTableMeta->tversion, pTableMeta->uid, TD_REQ_FROM_TAOX);
  } else {
    uError("SML:0x%" PRIx64 " invalid action:%d", info->id, action);
    code = TSDB_CODE_SML_INVALID_DATA;
    goto END;
  }

  SML_CHECK_CODE(buildRequest(info->taos->id, pSql, strlen(pSql), NULL, false, &pRequest, 0));

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto END;
  }

  if (pReq.numOfTags == 0) {
    pReq.numOfTags = 1;
    SField field = {0};
    field.type = TSDB_DATA_TYPE_NCHAR;
    field.bytes = TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
    tstrncpy(field.name, tsSmlTagName, sizeof(field.name));
    SML_CHECK_NULL(taosArrayPush(pReq.pTags, &field));
  }

  pReq.commentLen = -1;
  pReq.igExists = true;
  SML_CHECK_CODE(tNameExtractFullName(pName, pReq.name));

  pCmdMsg.epSet = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  if (pCmdMsg.msgLen < 0) {
    code = pCmdMsg.msgLen;
    goto END;
  }
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  SML_CHECK_NULL(pCmdMsg.pMsg);
  code = tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);
  if (code < 0) {
    taosMemoryFree(pCmdMsg.pMsg);
    goto END;
  }

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);  // no need to check return value

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SML_CHECK_CODE(catalogRemoveTableMeta(info->pCatalog, pName));
  }
  code = pRequest->code;

END:
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  RETURN
}

static int32_t smlCreateTable(SSmlHandle *info, SRequestConnInfo *conn, SSmlSTableMeta *sTableData,
                              SName *pName, STableMeta **pTableMeta){
  int32_t code = 0;
  int32_t lino = 0;
  SArray *pColumns = NULL;
  SArray *pTags = NULL;
  SML_CHECK_CODE(smlCheckAuth(info, conn, NULL, AUTH_TYPE_WRITE));
  uDebug("SML:0x%" PRIx64 " %s create table:%s", info->id, __FUNCTION__, pName->tname);
  pColumns = taosArrayInit(taosArrayGetSize(sTableData->cols), sizeof(SField));
  SML_CHECK_NULL(pColumns);
  pTags = taosArrayInit(taosArrayGetSize(sTableData->tags), sizeof(SField));
  SML_CHECK_NULL(pTags);
  SML_CHECK_CODE(smlBuildFieldsList(info, NULL, NULL, sTableData->tags, pTags, 0, true));
  SML_CHECK_CODE(smlBuildFieldsList(info, NULL, NULL, sTableData->cols, pColumns, 0, false));
  SML_CHECK_CODE(smlSendMetaMsg(info, pName, pColumns, &pTags, NULL, SCHEMA_ACTION_CREATE_STABLE));
  info->cost.numOfCreateSTables++;
  taosMemoryFreeClear(*pTableMeta);

  SML_CHECK_CODE(catalogGetSTableMeta(info->pCatalog, conn, pName, pTableMeta));

END:
  taosArrayDestroy(pColumns);
  taosArrayDestroy(pTags);
  RETURN
}

static int32_t smlBuildFields(SArray **pColumns, SArray **pTags, STableMeta *pTableMeta, SSmlSTableMeta *sTableData){
  int32_t code = 0;
  int32_t lino = 0;
  *pColumns = taosArrayInit(taosArrayGetSize(sTableData->cols) + (pTableMeta)->tableInfo.numOfColumns, sizeof(SField));
  SML_CHECK_NULL(pColumns);
  *pTags = taosArrayInit(taosArrayGetSize(sTableData->tags) + (pTableMeta)->tableInfo.numOfTags, sizeof(SField));
  SML_CHECK_NULL(pTags);
  for (uint16_t i = 0; i < (pTableMeta)->tableInfo.numOfColumns + (pTableMeta)->tableInfo.numOfTags; i++) {
    SField field = {0};
    field.type = (pTableMeta)->schema[i].type;
    field.bytes = (pTableMeta)->schema[i].bytes;
    tstrncpy(field.name, (pTableMeta)->schema[i].name, sizeof(field.name));
    if (i < (pTableMeta)->tableInfo.numOfColumns) {
      SML_CHECK_NULL(taosArrayPush(*pColumns, &field));
    } else {
      SML_CHECK_NULL(taosArrayPush(*pTags, &field));
    }
  }
END:
  RETURN
}
static int32_t smlModifyTag(SSmlHandle *info, SHashObj* hashTmp, SRequestConnInfo *conn,
                            SSmlSTableMeta *sTableData, SName *pName, STableMeta **pTableMeta){
  ESchemaAction action = SCHEMA_ACTION_NULL;
  SArray *pColumns = NULL;
  SArray *pTags = NULL;
  int32_t code = 0;
  int32_t lino = 0;
  SML_CHECK_CODE(smlProcessSchemaAction(info, (*pTableMeta)->schema, hashTmp, sTableData->tags, sTableData->cols, &action, true));

  if (action != SCHEMA_ACTION_NULL) {
    SML_CHECK_CODE(smlCheckAuth(info, conn, pName->tname, AUTH_TYPE_WRITE));
    uDebug("SML:0x%" PRIx64 " %s change table tag, table:%s, action:%d", info->id, __FUNCTION__, pName->tname,
           action);
    SML_CHECK_CODE(smlBuildFields(&pColumns, &pTags, *pTableMeta, sTableData));
    SML_CHECK_CODE(smlBuildFieldsList(info, (*pTableMeta)->schema, hashTmp, sTableData->tags, pTags,
                              (*pTableMeta)->tableInfo.numOfColumns, true));

    SML_CHECK_CODE(smlSendMetaMsg(info, pName, pColumns, &pTags, (*pTableMeta), action));

    info->cost.numOfAlterTagSTables++;
    taosMemoryFreeClear(*pTableMeta);
    SML_CHECK_CODE(catalogRefreshTableMeta(info->pCatalog, conn, pName, -1));
    SML_CHECK_CODE(catalogGetSTableMeta(info->pCatalog, conn, pName, pTableMeta));
  }

END:
  taosArrayDestroy(pColumns);
  taosArrayDestroy(pTags);
  RETURN
}

static int32_t smlModifyCols(SSmlHandle *info, SHashObj* hashTmp, SRequestConnInfo *conn,
                            SSmlSTableMeta *sTableData, SName *pName, STableMeta **pTableMeta){
  ESchemaAction action = SCHEMA_ACTION_NULL;
  SArray *pColumns = NULL;
  SArray *pTags = NULL;
  int32_t code = 0;
  int32_t lino = 0;
  SML_CHECK_CODE(smlProcessSchemaAction(info, (*pTableMeta)->schema, hashTmp, sTableData->cols, sTableData->tags, &action, false));

  if (action != SCHEMA_ACTION_NULL) {
    SML_CHECK_CODE(smlCheckAuth(info, conn, pName->tname, AUTH_TYPE_WRITE));
    uDebug("SML:0x%" PRIx64 " %s change table col, table:%s, action:%d", info->id, __FUNCTION__, pName->tname,
           action);
    SML_CHECK_CODE(smlBuildFields(&pColumns, &pTags, *pTableMeta, sTableData));
    SML_CHECK_CODE(smlBuildFieldsList(info, (*pTableMeta)->schema, hashTmp, sTableData->cols, pColumns,
                                      (*pTableMeta)->tableInfo.numOfColumns, false));

    SML_CHECK_CODE(smlSendMetaMsg(info, pName, pColumns, &pTags, (*pTableMeta), action));

    info->cost.numOfAlterColSTables++;
    taosMemoryFreeClear(*pTableMeta);
    SML_CHECK_CODE(catalogRefreshTableMeta(info->pCatalog, conn, pName, -1));
    SML_CHECK_CODE(catalogGetSTableMeta(info->pCatalog, conn, pName, pTableMeta));
  }

END:
  taosArrayDestroy(pColumns);
  taosArrayDestroy(pTags);
  RETURN
}

static int32_t smlBuildTempHash(SHashObj *hashTmp, STableMeta *pTableMeta, uint16_t start, uint16_t end){
  int32_t code = 0;
  int32_t lino = 0;
  for (uint16_t i = start; i < end; i++) {
    SML_CHECK_CODE(taosHashPut(hashTmp, pTableMeta->schema[i].name, strlen(pTableMeta->schema[i].name), &i, SHORT_BYTES));
  }

END:
  return code;
}

static int32_t smlModifyDBSchemas(SSmlHandle *info) {
  uDebug("SML:0x%" PRIx64 " %s start, format:%d, needModifySchema:%d", info->id, __FUNCTION__, info->dataFormat,
         info->needModifySchema);
  if (info->dataFormat && !info->needModifySchema) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t     code = 0;
  int32_t     lino = 0;
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
    SML_CHECK_NULL(measure);
    (void)memcpy(measure, superTable, superTableLen);
    if (info->protocol == TSDB_SML_LINE_PROTOCOL){
      PROCESS_SLASH_IN_MEASUREMENT(measure, superTableLen);
    }
    smlStrReplace(measure, superTableLen);
    (void)memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
    (void)memcpy(pName.tname, measure, TMIN(superTableLen, TSDB_TABLE_NAME_LEN - 1));
    taosMemoryFree(measure);

    code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);

    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_MND_STB_NOT_EXIST) {
      SML_CHECK_CODE(smlCreateTable(info, &conn, sTableData, &pName, &pTableMeta));
    } else if (code == TSDB_CODE_SUCCESS) {
      if (smlIsPKTable(pTableMeta)) {
        code = TSDB_CODE_SML_NOT_SUPPORT_PK;
        goto END;
      }

      hashTmp = taosHashInit(pTableMeta->tableInfo.numOfTags, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
      SML_CHECK_NULL(hashTmp);
      SML_CHECK_CODE(smlBuildTempHash(hashTmp, pTableMeta, pTableMeta->tableInfo.numOfColumns, pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags));
      SML_CHECK_CODE(smlModifyTag(info, hashTmp, &conn, sTableData, &pName, &pTableMeta));
      taosHashClear(hashTmp);
      SML_CHECK_CODE(smlBuildTempHash(hashTmp, pTableMeta, 0, pTableMeta->tableInfo.numOfColumns));
      SML_CHECK_CODE(smlModifyCols(info, hashTmp, &conn, sTableData, &pName, &pTableMeta));

      needCheckMeta = true;
      taosHashCleanup(hashTmp);
      hashTmp = NULL;
    } else {
      uError("SML:0x%" PRIx64 " %s load table meta error: %s", info->id, __FUNCTION__, tstrerror(code));
      goto END;
    }

    if (needCheckMeta) {
      SML_CHECK_CODE(smlCheckMeta(&(pTableMeta->schema[pTableMeta->tableInfo.numOfColumns]), pTableMeta->tableInfo.numOfTags, sTableData->tags, true));
      SML_CHECK_CODE(smlCheckMeta(&(pTableMeta->schema[0]), pTableMeta->tableInfo.numOfColumns, sTableData->cols, false));
    }

    taosMemoryFreeClear(sTableData->tableMeta);
    sTableData->tableMeta = pTableMeta;
    uDebug("SML:0x%" PRIx64 " %s modify schema uid:%" PRIu64 ", sversion:%d, tversion:%d", info->id, __FUNCTION__, pTableMeta->uid,
           pTableMeta->sversion, pTableMeta->tversion);
    tmp = (SSmlSTableMeta **)taosHashIterate(info->superTables, tmp);
  }
  uDebug("SML:0x%" PRIx64 " %s end success, format:%d, needModifySchema:%d", info->id, __FUNCTION__, info->dataFormat,
         info->needModifySchema);

  return TSDB_CODE_SUCCESS;

END:
  taosHashCancelIterate(info->superTables, tmp);
  taosHashCleanup(hashTmp);
  taosMemoryFreeClear(pTableMeta);
  (void)catalogRefreshTableMeta(info->pCatalog, &conn, &pName, 1);  // ignore refresh meta code if there is an error
  uError("SML:0x%" PRIx64 " %s end failed:%d:%s, format:%d, needModifySchema:%d", info->id, __FUNCTION__, code,
         tstrerror(code), info->dataFormat, info->needModifySchema);

  return code;
}

static int32_t smlInsertMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols, SHashObj *checkDuplicate) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = 0;
  for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    SML_CHECK_NULL(kv);
    int ret = taosHashPut(metaHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
    if (ret == 0) {
      SML_CHECK_NULL(taosArrayPush(metaArray, kv));
      if (taosHashGet(checkDuplicate, kv->key, kv->keyLen) != NULL) {
        code = TSDB_CODE_PAR_DUPLICATED_COLUMN;
        goto END;
      }
    } else if (terrno == TSDB_CODE_DUP_KEY) {
      return TSDB_CODE_PAR_DUPLICATED_COLUMN;
    }
  }

END:
  RETURN
}

static int32_t smlUpdateMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols, bool isTag, SSmlMsgBuf *msg,
                             SHashObj *checkDuplicate) {
  int32_t code = 0;
  int32_t lino = 0;
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    SML_CHECK_NULL(kv);
    int16_t *index = (int16_t *)taosHashGet(metaHash, kv->key, kv->keyLen);
    if (index) {
      SSmlKv *value = (SSmlKv *)taosArrayGet(metaArray, *index);
      SML_CHECK_NULL(value);

      if (isTag) {
        if (kv->length > value->length) {
          value->length = kv->length;
        }
        continue;
      }
      if (kv->type != value->type) {
        smlBuildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
        code = TSDB_CODE_SML_NOT_SAME_TYPE;
        goto END;
      }

      if (IS_VAR_DATA_TYPE(kv->type) && (kv->length > value->length)) {  // update string len, if bigger
        value->length = kv->length;
      }
    } else {
      size_t tmp = taosArrayGetSize(metaArray);
      if (tmp > INT16_MAX) {
        smlBuildInvalidDataMsg(msg, "too many cols or tags", kv->key);
        uError("too many cols or tags");
        code = TSDB_CODE_SML_INVALID_DATA;
        goto END;
      }
      int16_t size = tmp;
      SML_CHECK_CODE(taosHashPut(metaHash, kv->key, kv->keyLen, &size, SHORT_BYTES));
      SML_CHECK_NULL(taosArrayPush(metaArray, kv));
      if (taosHashGet(checkDuplicate, kv->key, kv->keyLen) != NULL) {
        code = TSDB_CODE_PAR_DUPLICATED_COLUMN;
        goto END;
      }
    }
  }

END:
  RETURN
}

void smlDestroyTableInfo(void *para) {
  SSmlTableInfo *tag = *(SSmlTableInfo **)para;
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
  if (info == NULL) return;

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
  taosArrayDestroyP(info->escapedStringList, taosMemoryFree);

  if (!info->dataFormat) {
    for (int i = 0; i < info->lineNum; i++) {
      taosArrayDestroyEx(info->lines[i].colArray, freeSSmlKv);
      if (info->lines[i].measureTagsLen != 0 && info->protocol != TSDB_SML_LINE_PROTOCOL) {
        taosMemoryFree(info->lines[i].measureTag);
      }
    }
    taosMemoryFree(info->lines);
  }
  if(info->protocol == TSDB_SML_JSON_PROTOCOL)  {
    taosMemoryFreeClear(info->preLine.tags);
  }
  cJSON_Delete(info->root);
  taosMemoryFreeClear(info);
}

int32_t smlBuildSmlInfo(TAOS *taos, SSmlHandle **handle) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SSmlHandle *info = (SSmlHandle *)taosMemoryCalloc(1, sizeof(SSmlHandle));
  SML_CHECK_NULL(info);
  if (taos != NULL){
    info->taos = acquireTscObj(*(int64_t *)taos);
    SML_CHECK_NULL(info->taos);
    SML_CHECK_CODE(catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog));
  }

  info->pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  SML_CHECK_NULL(info->pVgHash);
  info->childTables = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SML_CHECK_NULL(info->childTables);
  info->tableUids = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SML_CHECK_NULL(info->tableUids);
  info->superTables = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SML_CHECK_NULL(info->superTables);
  taosHashSetFreeFp(info->superTables, smlDestroySTableMeta);
  taosHashSetFreeFp(info->childTables, smlDestroyTableInfo);

  info->id = smlGenId();
  SML_CHECK_CODE(smlInitHandle(&info->pQuery));
  info->dataFormat = true;
  info->tagJsonArray = taosArrayInit(8, POINTER_BYTES);
  SML_CHECK_NULL(info->tagJsonArray);
  info->valueJsonArray = taosArrayInit(8, POINTER_BYTES);
  SML_CHECK_NULL(info->valueJsonArray);
  info->preLineTagKV = taosArrayInit(8, sizeof(SSmlKv));
  SML_CHECK_NULL(info->preLineTagKV);
  info->escapedStringList = taosArrayInit(8, POINTER_BYTES);
  SML_CHECK_NULL(info->escapedStringList);

  *handle = info;
  info = NULL;

END:
  smlDestroyInfo(info);
  RETURN
}

static int32_t smlPushCols(SArray *colsArray, SArray *cols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj *kvHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  SML_CHECK_NULL(kvHash);
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    SML_CHECK_NULL(kv);
    terrno = 0;
    code = taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
    if (terrno == TSDB_CODE_DUP_KEY) {
      code = TSDB_CODE_PAR_DUPLICATED_COLUMN;
      goto END;
    }
    SML_CHECK_CODE(code);
  }

  SML_CHECK_NULL(taosArrayPush(colsArray, &kvHash));
  return code;
END:
  taosHashCleanup(kvHash);
  RETURN
}

static int32_t smlParseEnd(SSmlHandle *info) {
  uDebug("SML:0x%" PRIx64 " %s start, format:%d, linenum:%d", info->id, __FUNCTION__, info->dataFormat,
         info->lineNum);
  int32_t code = 0;
  int32_t lino = 0;
  if (info->dataFormat) return TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < info->lineNum; i++) {
    SSmlLineInfo  *elements = info->lines + i;
    SSmlTableInfo *tinfo = NULL;
    if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
      SSmlTableInfo **tmp =
          (SSmlTableInfo **)taosHashGet(info->childTables, elements->measure, elements->measureTagsLen);
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

    SML_CHECK_CODE(smlPushCols(tinfo->cols, elements->colArray));

    SSmlSTableMeta **tableMeta =
        (SSmlSTableMeta **)taosHashGet(info->superTables, elements->measure, elements->measureLen);
    if (tableMeta) {  // update meta
      uDebug("SML:0x%" PRIx64 " %s update meta, format:%d, linenum:%d", info->id, __FUNCTION__, info->dataFormat,
             info->lineNum);
      SML_CHECK_CODE(smlUpdateMeta((*tableMeta)->colHash, (*tableMeta)->cols, elements->colArray, false, &info->msgBuf,
                          (*tableMeta)->tagHash));
      SML_CHECK_CODE(smlUpdateMeta((*tableMeta)->tagHash, (*tableMeta)->tags, tinfo->tags, true, &info->msgBuf,
                          (*tableMeta)->colHash));
    } else {
      uDebug("SML:0x%" PRIx64 " %s add meta, format:%d, linenum:%d", info->id, __FUNCTION__, info->dataFormat,
             info->lineNum);
      SSmlSTableMeta *meta = NULL;
      SML_CHECK_CODE(smlBuildSTableMeta(info->dataFormat, &meta));
      code = taosHashPut(info->superTables, elements->measure, elements->measureLen, &meta, POINTER_BYTES);
      if (code != TSDB_CODE_SUCCESS) {
        smlDestroySTableMeta(&meta);
        uError("SML:0x%" PRIx64 " put measure to hash failed", info->id);
        goto END;
      }
      SML_CHECK_CODE(smlInsertMeta(meta->tagHash, meta->tags, tinfo->tags, NULL));
      SML_CHECK_CODE(smlInsertMeta(meta->colHash, meta->cols, elements->colArray, meta->tagHash));
    }
  }
  uDebug("SML:0x%" PRIx64 " %s end, format:%d, linenum:%d", info->id, __FUNCTION__, info->dataFormat, info->lineNum);

END:
  RETURN
}

static int32_t smlInsertData(SSmlHandle *info) {
  int32_t         code      = TSDB_CODE_SUCCESS;
  int32_t         lino      = 0;
  char           *measure   = NULL;
  SSmlTableInfo **oneTable  = NULL;
  uDebug("SML:0x%" PRIx64 " %s start, format:%d", info->id, __FUNCTION__, info->dataFormat);

  if (info->pRequest->dbList == NULL) {
    info->pRequest->dbList = taosArrayInit(1, TSDB_DB_FNAME_LEN);
    SML_CHECK_NULL(info->pRequest->dbList);
  }
  char *data = (char *)taosArrayReserve(info->pRequest->dbList, 1);
  SML_CHECK_NULL(data);
  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));
  (void)tNameGetFullDbName(&pName, data);  // ignore

  oneTable = (SSmlTableInfo **)taosHashIterate(info->childTables, NULL);
  while (oneTable) {
    SSmlTableInfo *tableData = *oneTable;

    int   measureLen = tableData->sTableNameLen;
    measure = (char *)taosMemoryMalloc(tableData->sTableNameLen);
    SML_CHECK_NULL(measure);
    (void)memcpy(measure, tableData->sTableName, tableData->sTableNameLen);
    if (info->protocol == TSDB_SML_LINE_PROTOCOL){
      PROCESS_SLASH_IN_MEASUREMENT(measure, measureLen);
    }
    smlStrReplace(measure, measureLen);
    (void)memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
    (void)memcpy(pName.tname, measure, measureLen);

    if (info->pRequest->tableList == NULL) {
      info->pRequest->tableList = taosArrayInit(1, sizeof(SName));
      SML_CHECK_NULL(info->pRequest->tableList);
    }
    SML_CHECK_NULL(taosArrayPush(info->pRequest->tableList, &pName));
    tstrncpy(pName.tname, tableData->childTableName, sizeof(pName.tname));

    SRequestConnInfo conn = {0};
    conn.pTrans = info->taos->pAppInfo->pTransporter;
    conn.requestId = info->pRequest->requestId;
    conn.requestObjRefId = info->pRequest->self;
    conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    SML_CHECK_CODE(smlCheckAuth(info, &conn, pName.tname, AUTH_TYPE_WRITE));

    SVgroupInfo vg = {0};
    SML_CHECK_CODE(catalogGetTableHashVgroup(info->pCatalog, &conn, &pName, &vg));
    SML_CHECK_CODE(taosHashPut(info->pVgHash, (const char *)&vg.vgId, sizeof(vg.vgId), (char *)&vg, sizeof(vg)));

    SSmlSTableMeta **pMeta =
        (SSmlSTableMeta **)taosHashGet(info->superTables, tableData->sTableName, tableData->sTableNameLen);
    if (unlikely(NULL == pMeta || NULL == *pMeta || NULL == (*pMeta)->tableMeta)) {
      uError("SML:0x%" PRIx64 " %s NULL == pMeta. table name: %s", info->id, __FUNCTION__, tableData->childTableName);
      code = TSDB_CODE_SML_INTERNAL_ERROR;
      goto END;
    }

    // use tablemeta of stable to save vgid and uid of child table
    (*pMeta)->tableMeta->vgId = vg.vgId;
    (*pMeta)->tableMeta->uid = tableData->uid;  // one table merge data block together according uid
    uDebug("SML:0x%" PRIx64 " %s table:%s, uid:%" PRIu64 ", format:%d", info->id, __FUNCTION__, pName.tname,
           tableData->uid, info->dataFormat);

    SML_CHECK_CODE(smlBindData(info->pQuery, info->dataFormat, tableData->tags, (*pMeta)->cols, tableData->cols,
                       (*pMeta)->tableMeta, tableData->childTableName, measure, measureLen, info->ttl, info->msgBuf.buf,
                       info->msgBuf.len));
    taosMemoryFreeClear(measure);
    oneTable = (SSmlTableInfo **)taosHashIterate(info->childTables, oneTable);
  }

  SML_CHECK_CODE(smlBuildOutput(info->pQuery, info->pVgHash));
  info->cost.insertRpcTime = taosGetTimestampUs();

  SAppClusterSummary *pActivity = &info->taos->pAppInfo->summary;
  (void)atomic_add_fetch_64((int64_t *)&pActivity->numOfInsertsReq, 1);  // no need to check return code

  launchQueryImpl(info->pRequest, info->pQuery, true, NULL);  // no need to check return code

  uDebug("SML:0x%" PRIx64 " %s end, format:%d, code:%d,%s", info->id, __FUNCTION__, info->dataFormat, info->pRequest->code,
         tstrerror(info->pRequest->code));

  return info->pRequest->code;

END:
  taosMemoryFree(measure);
  taosHashCancelIterate(info->childTables, oneTable);
  RETURN
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
  int32_t code = 0;
  int32_t lino = 0;
  info->reRun = false;

  taosHashClear(info->childTables);
  taosHashClear(info->superTables);
  taosHashClear(info->tableUids);

  if (!info->dataFormat) {
    info->lines = (SSmlLineInfo *)taosMemoryCalloc(info->lineNum, sizeof(SSmlLineInfo));
    SML_CHECK_NULL(info->lines);
  }

  taosArrayClearP(info->escapedStringList, taosMemoryFree);
  if(info->protocol == TSDB_SML_JSON_PROTOCOL)  {
    taosMemoryFreeClear(info->preLine.tags);
  }
  (void)memset(&info->preLine, 0, sizeof(SSmlLineInfo));
  info->currSTableMeta = NULL;
  info->currTableDataCtx = NULL;

  SVnodeModifyOpStmt *stmt = (SVnodeModifyOpStmt *)(info->pQuery->pRoot);
  stmt->freeHashFunc(stmt->pTableBlockHashObj);
  stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  SML_CHECK_NULL(stmt->pTableBlockHashObj);

END:
  RETURN
}

static void printRaw(int64_t id, int lineNum, int numLines, ELogLevel level, char* data, int32_t len){
  char *print = taosMemoryCalloc(len + 1, 1);
  if (print == NULL) {
    uError("SML:0x%" PRIx64 " smlParseLine failed. code : %d", id, terrno);
    return;
  }
  (void)memcpy(print, data, len);
  if (level == DEBUG_DEBUG){
    uDebug("SML:0x%" PRIx64 " smlParseLine is raw, line %d/%d : %s", id, lineNum, numLines, print);
  }else if (level == DEBUG_ERROR){
    uError("SML:0x%" PRIx64 " smlParseLine failed. line %d/%d : %s", id, lineNum, numLines, print);
  }
  taosMemoryFree(print);
}

static bool getLine(SSmlHandle *info, char *lines[], char **rawLine, char *rawLineEnd, int numLines, int i, char **tmp,
                    int *len) {
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
    if (IS_COMMENT(info->protocol,(*tmp)[0])) {  // this line is comment
      return false;
    }
  }

  if (*rawLine != NULL && (uDebugFlag & DEBUG_DEBUG)) {
    printRaw(info->id, i, numLines, DEBUG_DEBUG, *tmp, *len);
  } else {
    uDebug("SML:0x%" PRIx64 " smlParseLine is not raw, line %d/%d : %s", info->id, i, numLines, *tmp);
  }
  return true;
}


static int32_t smlParseJson(SSmlHandle *info, char *lines[], char *rawLine) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (lines) {
    code = smlParseJSONExt(info, *lines);
  } else if (rawLine) {
    code = smlParseJSONExt(info, rawLine);
  }
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed code:%d", __FUNCTION__ , code);
  }
  return code;
}

static int32_t smlParseStart(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
  uDebug("SML:0x%" PRIx64 " %s start", info->id, __FUNCTION__);
  int32_t code = TSDB_CODE_SUCCESS;
  if (info->protocol == TSDB_SML_JSON_PROTOCOL) {
    return smlParseJson(info, lines, rawLine);
  }

  char   *oldRaw = rawLine;
  int32_t i = 0;
  while (i < numLines) {
    char *tmp = NULL;
    int   len = 0;
    if (!getLine(info, lines, &rawLine, rawLineEnd, numLines, i, &tmp, &len)) {
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
    }
    if (code != TSDB_CODE_SUCCESS) {
      if (rawLine != NULL) {
        printRaw(info->id, i, numLines, DEBUG_ERROR, tmp, len);
      } else {
        uError("SML:0x%" PRIx64 " %s failed. line %d : %s", info->id, __FUNCTION__, i, tmp);
      }
      return code;
    }
    if (info->reRun) {
      uDebug("SML:0x%" PRIx64 " %s re run", info->id, __FUNCTION__);
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
  uDebug("SML:0x%" PRIx64 " %s end", info->id, __FUNCTION__);

  return code;
}

static int smlProcess(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t retryNum = 0;

  info->cost.parseTime = taosGetTimestampUs();

  SML_CHECK_CODE(smlParseStart(info, lines, rawLine, rawLineEnd, numLines));
  SML_CHECK_CODE(smlParseEnd(info));

  info->cost.lineNum = info->lineNum;
  info->cost.numOfSTables = taosHashGetSize(info->superTables);
  info->cost.numOfCTables = taosHashGetSize(info->childTables);
  info->cost.schemaTime = taosGetTimestampUs();

  do {
    code = smlModifyDBSchemas(info);
    if (code != TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER && code != TSDB_CODE_SDB_OBJ_CREATING &&
        code != TSDB_CODE_MND_TRANS_CONFLICT) {
      break;
    }
    taosMsleep(100);
    uInfo("SML:0x%" PRIx64 " smlModifyDBSchemas retry code:%s, times:%d", info->id, tstrerror(code), retryNum);
  } while (retryNum++ < taosHashGetSize(info->superTables) * MAX_RETRY_TIMES);

  SML_CHECK_CODE(code);
  info->cost.insertBindTime = taosGetTimestampUs();
  SML_CHECK_CODE(smlInsertData(info));

END:
  RETURN
}

void smlSetReqSQL(SRequestObj *request, char *lines[], char *rawLine, char *rawLineEnd) {
  if (request->pTscObj->pAppInfo->monitorParas.tsSlowLogScope & SLOW_LOG_TYPE_INSERT) {
    int32_t len = 0;
    int32_t rlen = 0;
    char   *p = NULL;

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
    (void)memcpy(sql, p, rlen);
    sql[rlen] = 0;

    request->sqlstr = sql;
    request->sqlLen = rlen;
  }
}

TAOS_RES *taos_schemaless_insert_inner(TAOS *taos, char *lines[], char *rawLine, char *rawLineEnd, int numLines,
                                       int protocol, int precision, int32_t ttl, int64_t reqid, char *tbnameKey) {
  int32_t      code    = TSDB_CODE_SUCCESS;
  int32_t      lino    = 0;
  SRequestObj *request = NULL;
  SSmlHandle  *info    = NULL;
  int          cnt     = 0;
  while (1) {
    SML_CHECK_CODE(createRequest(*(int64_t *)taos, TSDB_SQL_INSERT, reqid, &request));
    SSmlMsgBuf msg = {request->msgBufLen, request->msgBuf};
    request->code = smlBuildSmlInfo(taos, &info);
    SML_CHECK_CODE(request->code);

    info->pRequest = request;
    info->pRequest->pQuery = info->pQuery;
    info->ttl = ttl;
    info->precision = precision;
    info->protocol = (TSDB_SML_PROTOCOL_TYPE)protocol;
    info->msgBuf.buf = info->pRequest->msgBuf;
    info->msgBuf.len = ERROR_MSG_BUF_DEFAULT_SIZE;
    info->lineNum = numLines;
    info->tbnameKey = tbnameKey;

    smlSetReqSQL(request, lines, rawLine, rawLineEnd);

    if (request->pDb == NULL) {
      request->code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
      smlBuildInvalidDataMsg(&msg, "Database not specified", NULL);
      goto END;
    }

    if (protocol < TSDB_SML_LINE_PROTOCOL || protocol > TSDB_SML_JSON_PROTOCOL) {
      request->code = TSDB_CODE_SML_INVALID_PROTOCOL_TYPE;
      smlBuildInvalidDataMsg(&msg, "protocol invalidate", NULL);
      goto END;
    }

    if (protocol == TSDB_SML_LINE_PROTOCOL &&
        (precision < TSDB_SML_TIMESTAMP_NOT_CONFIGURED || precision > TSDB_SML_TIMESTAMP_NANO_SECONDS)) {
      request->code = TSDB_CODE_SML_INVALID_PRECISION_TYPE;
      smlBuildInvalidDataMsg(&msg, "precision invalidate for line protocol", NULL);
      goto END;
    }

    if (protocol == TSDB_SML_JSON_PROTOCOL) {
      numLines = 1;
    } else if (numLines <= 0) {
      request->code = TSDB_CODE_SML_INVALID_DATA;
      smlBuildInvalidDataMsg(&msg, "line num is invalid", NULL);
      goto END;
    }

    code = smlProcess(info, lines, rawLine, rawLineEnd, numLines);
    request->code = code;
    info->cost.endTime = taosGetTimestampUs();
    info->cost.code = code;
    if (NEED_CLIENT_HANDLE_ERROR(code) || code == TSDB_CODE_SDB_OBJ_CREATING || code == TSDB_CODE_PAR_VALUE_TOO_LONG ||
        code == TSDB_CODE_MND_TRANS_CONFLICT) {
      if (cnt++ >= 10) {
        uInfo("SML:%" PRIx64 " retry:%d/10 end code:%d, msg:%s", info->id, cnt, code, tstrerror(code));
        break;
      }
      taosMsleep(100);
      uInfo("SML:%" PRIx64 " retry:%d/10,ver is old retry or object is creating code:%d, msg:%s", info->id, cnt, code,
            tstrerror(code));
      code = refreshMeta(request->pTscObj, request);
      if (code != 0) {
        uInfo("SML:%" PRIx64 " refresh meta error code:%d, msg:%s", info->id, code, tstrerror(code));
      }
      smlDestroyInfo(info);
      info = NULL;
      taos_free_result(request);
      request = NULL;
      continue;
    }
    smlPrintStatisticInfo(info);
    break;
  }

END:
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

TAOS_RES *taos_schemaless_insert_ttl_with_reqid_tbname_key(TAOS *taos, char *lines[], int numLines, int protocol,
                                                           int precision, int32_t ttl, int64_t reqid, char *tbnameKey) {
  if (taos == NULL || lines == NULL || numLines <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  for (int i = 0; i < numLines; i++){
    if (lines[i] == NULL){
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
    }
  }
  return taos_schemaless_insert_inner(taos, lines, NULL, NULL, numLines, protocol, precision, ttl, reqid, tbnameKey);
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl, int64_t reqid) {
  return taos_schemaless_insert_ttl_with_reqid_tbname_key(taos, lines, numLines, protocol, precision, ttl, reqid, NULL);
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

static int32_t getRawLineLen(char *lines, int len, int protocol) {
  int numLines = 0;
  char *tmp = lines;
  for (int i = 0; i < len; i++) {
    if (lines[i] == '\n' || i == len - 1) {
      if (!IS_COMMENT(protocol, tmp[0])) {  // ignore comment
        numLines++;
      }
      tmp = lines + i + 1;
    }
  }
  return numLines;
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(TAOS *taos, char *lines, int len, int32_t *totalRows,
                                                               int protocol, int precision, int32_t ttl, int64_t reqid,
                                                               char *tbnameKey) {
  if (taos == NULL || lines == NULL || len <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  int numLines = getRawLineLen(lines, len, protocol);
  if (totalRows != NULL){
    *totalRows = numLines;
  }
  return taos_schemaless_insert_inner(taos, NULL, lines, lines + len, numLines, protocol, precision, ttl, reqid,
                                      tbnameKey);
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl, int64_t reqid) {
  return taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(taos, lines, len, totalRows, protocol, precision, ttl,
                                                              reqid, NULL);
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
