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

int64_t smlToMilli[3] = {3600000LL, 60000LL, 1000LL};
int64_t smlFactorNS[3] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
int64_t smlFactorS[3] = {1000LL, 1000000LL, 1000000000LL};

void *nodeListGet(NodeList *list, const void *key, int32_t len, _equal_fn_sml fn) {
  NodeList *tmp = list;
  while (tmp) {
    if (fn == NULL) {
      if (tmp->data.used && tmp->data.keyLen == len && memcmp(tmp->data.key, key, len) == 0) {
        return tmp->data.value;
      }
    } else {
      if (tmp->data.used && fn(tmp->data.key, key) == 0) {
        return tmp->data.value;
      }
    }

    tmp = tmp->next;
  }
  return NULL;
}

int nodeListSet(NodeList **list, const void *key, int32_t len, void *value, _equal_fn_sml fn) {
  NodeList *tmp = *list;
  while (tmp) {
    if (!tmp->data.used) break;
    if (fn == NULL) {
      if (tmp->data.keyLen == len && memcmp(tmp->data.key, key, len) == 0) {
        return -1;
      }
    } else {
      if (tmp->data.keyLen == len && fn(tmp->data.key, key) == 0) {
        return -1;
      }
    }

    tmp = tmp->next;
  }
  if (tmp) {
    tmp->data.key = key;
    tmp->data.keyLen = len;
    tmp->data.value = value;
    tmp->data.used = true;
  } else {
    NodeList *newNode = (NodeList *)taosMemoryCalloc(1, sizeof(NodeList));
    if (newNode == NULL) {
      return -1;
    }
    newNode->data.key = key;
    newNode->data.keyLen = len;
    newNode->data.value = value;
    newNode->data.used = true;
    newNode->next = *list;
    *list = newNode;
  }
  return 0;
}

int nodeListSize(NodeList *list) {
  int cnt = 0;
  while (list) {
    if (list->data.used)
      cnt++;
    else
      break;
    list = list->next;
  }
  return cnt;
}

inline bool smlDoubleToInt64OverFlow(double num) {
  if (num >= (double)INT64_MAX || num <= (double)INT64_MIN) return true;
  return false;
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
    if (unit > INT64_MAX / tsInt64) {
      return -1;
    }
    tsInt64 *= unit;
    fromPrecision = TSDB_TIME_PRECISION_MILLI;
  }

  return convertTimePrecision(tsInt64, fromPrecision, toPrecision);
}

int8_t smlGetTsTypeByLen(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI;
  } else {
    return -1;
  }
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

  //  tag->tags = taosArrayInit(16, sizeof(SSmlKv));
  //  if (tag->tags == NULL) {
  //    uError("SML:smlBuildTableInfo failed to allocate memory");
  //    goto cleanup;
  //  }
  return tag;

cleanup:
  taosMemoryFree(tag);
  return NULL;
}

static int32_t smlParseTableName(SArray *tags, char *childTableName) {
  size_t childTableNameLen = strlen(tsSmlChildTableName);
  if (childTableNameLen <= 0) return TSDB_CODE_SUCCESS;

  for (int i = 0; i < taosArrayGetSize(tags); i++) {
    SSmlKv *tag = (SSmlKv *)taosArrayGet(tags, i);
    // handle child table name
    if (childTableNameLen == tag->keyLen && strncmp(tag->key, tsSmlChildTableName, tag->keyLen) == 0) {
      memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
      strncpy(childTableName, tag->value, (tag->length < TSDB_TABLE_NAME_LEN ? tag->length : TSDB_TABLE_NAME_LEN));
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t smlSetCTableName(SSmlTableInfo *oneTable) {
  smlParseTableName(oneTable->tags, oneTable->childTableName);

  if (strlen(oneTable->childTableName) == 0) {
    SArray       *dst = taosArrayDup(oneTable->tags, NULL);
    RandTableName rName = {dst, oneTable->sTableName, (uint8_t)oneTable->sTableNameLen, oneTable->childTableName, 0};

    buildChildTableName(&rName);
    taosArrayDestroy(dst);
    oneTable->uid = rName.uid;
  } else {
    oneTable->uid = *(uint64_t *)(oneTable->childTableName);
  }
  return TSDB_CODE_SUCCESS;
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
  taosMemoryFree(meta);
  return NULL;
}

// uint16_t smlCalTypeSum(char* endptr, int32_t left){
//   uint16_t sum = 0;
//   for(int i = 0; i < left; i++){
//     sum += endptr[i];
//   }
//   return sum;
// }

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

#define SET_BIGINT                                                                                         \
  if (smlDoubleToInt64OverFlow(result)) {                                                                  \
    errno = 0;                                                                                             \
    int64_t tmp = taosStr2Int64(pVal, &endptr, 10);                                                        \
    if (errno == ERANGE) {                                                                                 \
      smlBuildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", pVal); \
      return false;                                                                                        \
    }                                                                                                      \
    kvVal->type = TSDB_DATA_TYPE_BIGINT;                                                                   \
    kvVal->i = tmp;                                                                                        \
    return true;                                                                                           \
  }                                                                                                        \
  kvVal->type = TSDB_DATA_TYPE_BIGINT;                                                                     \
  kvVal->i = (int64_t)result;

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

#define SET_UBIGINT                                                                               \
  if (result >= (double)UINT64_MAX || result < 0) {                                               \
    errno = 0;                                                                                    \
    uint64_t tmp = taosStr2UInt64(pVal, &endptr, 10);                                             \
    if (errno == ERANGE || result < 0) {                                                          \
      smlBuildInvalidDataMsg(msg, "unsigned big int out of range[0,18446744073709551615]", pVal); \
      return false;                                                                               \
    }                                                                                             \
    kvVal->type = TSDB_DATA_TYPE_UBIGINT;                                                         \
    kvVal->u = tmp;                                                                               \
    return true;                                                                                  \
  }                                                                                               \
  kvVal->type = TSDB_DATA_TYPE_UBIGINT;                                                           \
  kvVal->u = result;

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

  catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
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
      uError("SML:0x%" PRIx64 " point type and db type mismatch. key: %s. point type: %d, db type: %d", info->id,
             kv->key, colField[*index].type, kv->type);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    if ((colField[*index].type == TSDB_DATA_TYPE_VARCHAR &&
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

static int32_t smlFindNearestPowerOf2(int32_t length, uint8_t type) {
  int32_t result = 1;
  while (result <= length) {
    result *= 2;
  }
  if (type == TSDB_DATA_TYPE_BINARY && result > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
    result = TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_NCHAR && result > (TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
    result = (TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
  }

  if (type == TSDB_DATA_TYPE_NCHAR) {
    result = result * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_BINARY) {
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
      return -1;
    }
  }
  taosHashCleanup(hashTmp);
  return 0;
}

static int32_t getBytes(uint8_t type, int32_t length) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
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
    smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, &action, info);
    if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_ADD_TAG) {
      SField field = {0};
      field.type = kv->type;
      field.bytes = getBytes(kv->type, kv->length);
      memcpy(field.name, kv->key, kv->keyLen);
      taosArrayPush(results, &field);
    } else if (action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
      uint16_t *index = (uint16_t *)taosHashGet(schemaHash, kv->key, kv->keyLen);
      uint16_t  newIndex = *index;
      if (isTag) newIndex -= numOfCols;
      SField *field = (SField *)taosArrayGet(results, newIndex);
      field->bytes = getBytes(kv->type, kv->length);
    }
  }
  return TSDB_CODE_SUCCESS;
}

// static int32_t smlSendMetaMsg(SSmlHandle *info, SName *pName, SSmlSTableMeta *sTableData,
//                               int32_t colVer, int32_t tagVer, int8_t source, uint64_t suid){
static int32_t smlSendMetaMsg(SSmlHandle *info, SName *pName, SArray *pColumns, SArray *pTags, STableMeta *pTableMeta,
                              ESchemaAction action) {
  SRequestObj   *pRequest = NULL;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SCmdMsgInfo    pCmdMsg = {0};

  // put front for free
  pReq.numOfColumns = taosArrayGetSize(pColumns);
  pReq.pColumns = pColumns;
  pReq.numOfTags = taosArrayGetSize(pTags);
  pReq.pTags = pTags;

  code = buildRequest(info->taos->id, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  if (action == SCHEMA_ACTION_CREATE_STABLE) {
    pReq.colVer = 1;
    pReq.tagVer = 1;
    pReq.suid = 0;
    pReq.source = TD_REQ_FROM_APP;
  } else if (action == SCHEMA_ACTION_ADD_TAG || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
    pReq.colVer = pTableMeta->sversion;
    pReq.tagVer = pTableMeta->tversion + 1;
    pReq.suid = pTableMeta->uid;
    pReq.source = TD_REQ_FROM_TAOX;
  } else if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE) {
    pReq.colVer = pTableMeta->sversion + 1;
    pReq.tagVer = pTableMeta->tversion;
    pReq.suid = pTableMeta->uid;
    pReq.source = TD_REQ_FROM_TAOX;
  }

  if (pReq.numOfTags == 0) {
    pReq.numOfTags = 1;
    SField field = {0};
    field.type = TSDB_DATA_TYPE_NCHAR;
    field.bytes = 1;
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

  NodeList *tmp = info->superTables;
  while (tmp) {
    SSmlSTableMeta *sTableData = (SSmlSTableMeta *)tmp->data.value;
    bool            needCheckMeta = false;  // for multi thread

    size_t      superTableLen = (size_t)tmp->data.keyLen;
    const void *superTable = tmp->data.key;
    memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
    memcpy(pName.tname, superTable, superTableLen);

    code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);

    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_MND_STB_NOT_EXIST) {
      SArray *pColumns = taosArrayInit(taosArrayGetSize(sTableData->cols), sizeof(SField));
      SArray *pTags = taosArrayInit(taosArrayGetSize(sTableData->tags), sizeof(SField));
      smlBuildFieldsList(info, NULL, NULL, sTableData->tags, pTags, 0, true);
      smlBuildFieldsList(info, NULL, NULL, sTableData->cols, pColumns, 0, false);

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
        smlBuildFieldsList(info, pTableMeta->schema, hashTmp, sTableData->tags, pTags,
                           pTableMeta->tableInfo.numOfColumns, true);

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

        smlBuildFieldsList(info, pTableMeta->schema, hashTmp, sTableData->cols, pColumns,
                           pTableMeta->tableInfo.numOfColumns, false);

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

    sTableData->tableMeta = pTableMeta;

    tmp = tmp->next;
  }
  return 0;

end:
  taosHashCleanup(hashTmp);
  taosMemoryFreeClear(pTableMeta);
  //  catalogRefreshTableMeta(info->pCatalog, &conn, &pName, 1);
  return code;
}

/*
static int32_t smlCheckDupUnit(SHashObj *dumplicateKey, SArray *tags, SSmlMsgBuf *msg){
  for(int i = 0; i < taosArrayGetSize(tags); i++) {
    SSmlKv *tag = taosArrayGet(tags, i);
    if (smlCheckDuplicateKey(tag->key, tag->keyLen, dumplicateKey)) {
      smlBuildInvalidDataMsg(msg, "dumplicate key", tag->key);
      return TSDB_CODE_TSC_DUP_NAMES;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlJudgeDupColName(SArray *cols, SArray *tags, SSmlMsgBuf *msg) {
  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  int ret = smlCheckDupUnit(dumplicateKey, cols, msg);
  if(ret != TSDB_CODE_SUCCESS){
    goto end;
  }
  ret = smlCheckDupUnit(dumplicateKey, tags, msg);
  if(ret != TSDB_CODE_SUCCESS){
    goto end;
  }

  end:
  taosHashCleanup(dumplicateKey);
  return ret;
}
*/

static void smlInsertMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols) {
  for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    int     ret = taosHashPut(metaHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
    if (ret == 0) {
      taosArrayPush(metaArray, kv);
    }
  }
}

static void smlDestroySTableMeta(SSmlSTableMeta *meta) {
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->colHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFree(meta->tableMeta);
  taosMemoryFree(meta);
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
      ASSERT(tmp <= INT16_MAX);
      int16_t size = tmp;
      int     ret = taosHashPut(metaHash, kv->key, kv->keyLen, &size, SHORT_BYTES);
      if (ret == 0) {
        taosArrayPush(metaArray, kv);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void smlDestroyTableInfo(SSmlTableInfo *tag) {
  for (size_t i = 0; i < taosArrayGetSize(tag->cols); i++) {
    SHashObj *kvHash = (SHashObj *)taosArrayGetP(tag->cols, i);
    taosHashCleanup(kvHash);
  }

  taosMemoryFree(tag->key);
  taosArrayDestroy(tag->cols);
  taosArrayDestroy(tag->tags);
  taosMemoryFree(tag);
}

void smlDestroyInfo(SSmlHandle *info) {
  if (!info) return;
  qDestroyQuery(info->pQuery);

  // destroy info->childTables
  NodeList *tmp = info->childTables;
  while (tmp) {
    if (tmp->data.used) {
      smlDestroyTableInfo((SSmlTableInfo *)tmp->data.value);
    }
    NodeList *t = tmp->next;
    taosMemoryFree(tmp);
    tmp = t;
  }

  // destroy info->superTables
  tmp = info->superTables;
  while (tmp) {
    if (tmp->data.used) {
      smlDestroySTableMeta((SSmlSTableMeta *)tmp->data.value);
    }
    NodeList *t = tmp->next;
    taosMemoryFree(tmp);
    tmp = t;
  }

  // destroy info->pVgHash
  taosHashCleanup(info->pVgHash);

  taosArrayDestroy(info->preLineTagKV);
  taosArrayDestroy(info->preLineColKV);

  if (!info->dataFormat) {
    for (int i = 0; i < info->lineNum; i++) {
      taosArrayDestroy(info->lines[i].colArray);
    }
    taosMemoryFree(info->lines);
  }

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
    code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " get catalog error %d", info->id, code);
      goto cleanup;
    }
  }

  info->pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  info->id = smlGenId();
  info->pQuery = smlInitHandle();
  info->dataFormat = true;

  info->preLineTagKV = taosArrayInit(8, sizeof(SSmlKv));
  info->preLineColKV = taosArrayInit(8, sizeof(SSmlKv));

  if (NULL == info->pVgHash) {
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
    taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
  }

  taosArrayPush(colsArray, &kvHash);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseLineBottom(SSmlHandle *info) {
  if (info->dataFormat) return TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < info->lineNum; i++) {
    SSmlLineInfo  *elements = info->lines + i;
    SSmlTableInfo *tinfo = NULL;
    if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
      tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements->measure, elements->measureTagsLen, NULL);
    } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
      tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements, POINTER_BYTES, is_same_child_table_telnet);
    } else {
      tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements, POINTER_BYTES, is_same_child_table_telnet);
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

    SSmlSTableMeta *tableMeta =
        (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);
    if (tableMeta) {  // update meta
      ret = smlUpdateMeta(tableMeta->colHash, tableMeta->cols, elements->colArray, false, &info->msgBuf);
      if (ret == TSDB_CODE_SUCCESS) {
        ret = smlUpdateMeta(tableMeta->tagHash, tableMeta->tags, tinfo->tags, true, &info->msgBuf);
      }
      if (ret != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
        return ret;
      }
    } else {
      //      ret = smlJudgeDupColName(elements->colArray, tinfo->tags, &info->msgBuf);
      //      if (ret != TSDB_CODE_SUCCESS) {
      //        uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
      //        return ret;
      //      }

      SSmlSTableMeta *meta = smlBuildSTableMeta(info->dataFormat);
      smlInsertMeta(meta->tagHash, meta->tags, tinfo->tags);
      smlInsertMeta(meta->colHash, meta->cols, elements->colArray);
      nodeListSet(&info->superTables, elements->measure, elements->measureLen, meta, NULL);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlInsertData(SSmlHandle *info) {
  int32_t code = TSDB_CODE_SUCCESS;

  NodeList *tmp = info->childTables;
  while (tmp) {
    SSmlTableInfo *tableData = (SSmlTableInfo *)tmp->data.value;

    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));
    memcpy(pName.tname, tableData->childTableName, strlen(tableData->childTableName));

    SRequestConnInfo conn = {0};
    conn.pTrans = info->taos->pAppInfo->pTransporter;
    conn.requestId = info->pRequest->requestId;
    conn.requestObjRefId = info->pRequest->self;
    conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(info->pCatalog, &conn, &pName, &vg);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " catalogGetTableHashVgroup failed. table name: %s", info->id, tableData->childTableName);
      return code;
    }
    taosHashPut(info->pVgHash, (const char *)&vg.vgId, sizeof(vg.vgId), (char *)&vg, sizeof(vg));

    SSmlSTableMeta *pMeta =
        (SSmlSTableMeta *)nodeListGet(info->superTables, tableData->sTableName, tableData->sTableNameLen, NULL);
    ASSERT(NULL != pMeta);

    // use tablemeta of stable to save vgid and uid of child table
    pMeta->tableMeta->vgId = vg.vgId;
    pMeta->tableMeta->uid = tableData->uid;  // one table merge data block together according uid

    code = smlBindData(info->pQuery, info->dataFormat, tableData->tags, pMeta->cols, tableData->cols, pMeta->tableMeta,
                       tableData->childTableName, tableData->sTableName, tableData->sTableNameLen, info->ttl,
                       info->msgBuf.buf, info->msgBuf.len);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlBindData failed", info->id);
      return code;
    }
    tmp = tmp->next;
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
  return info->pRequest->code;
}

static void smlPrintStatisticInfo(SSmlHandle *info) {
  uError(
      "SML:0x%" PRIx64
      " smlInsertLines result, code:%d,lineNum:%d,stable num:%d,ctable num:%d,create stable num:%d,alter stable tag num:%d,alter stable col num:%d \
        parse cost:%" PRId64 ",schema cost:%" PRId64 ",bind cost:%" PRId64 ",rpc cost:%" PRId64 ",total cost:%" PRId64
      "",
      info->id, info->cost.code, info->cost.lineNum, info->cost.numOfSTables, info->cost.numOfCTables,
      info->cost.numOfCreateSTables, info->cost.numOfAlterTagSTables, info->cost.numOfAlterColSTables,
      info->cost.schemaTime - info->cost.parseTime, info->cost.insertBindTime - info->cost.schemaTime,
      info->cost.insertRpcTime - info->cost.insertBindTime, info->cost.endTime - info->cost.insertRpcTime,
      info->cost.endTime - info->cost.parseTime);
}

int32_t smlClearForRerun(SSmlHandle *info) {
  info->reRun = false;
  // clear info->childTables
  NodeList *pList = info->childTables;
  while (pList) {
    if (pList->data.used) {
      smlDestroyTableInfo((SSmlTableInfo *)pList->data.value);
      pList->data.used = false;
    }
    pList = pList->next;
  }

  // clear info->superTables
  pList = info->superTables;
  while (pList) {
    if (pList->data.used) {
      smlDestroySTableMeta((SSmlSTableMeta *)pList->data.value);
      pList->data.used = false;
    }
    pList = pList->next;
  }

  if (unlikely(info->lines != NULL)) {
    uError("SML:0x%" PRIx64 " info->lines != NULL", info->id);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  info->lines = (SSmlLineInfo *)taosMemoryCalloc(info->lineNum, sizeof(SSmlLineInfo));

  memset(&info->preLine, 0, sizeof(SSmlLineInfo));
  info->currSTableMeta = NULL;
  info->currTableDataCtx = NULL;

  SVnodeModifyOpStmt *stmt = (SVnodeModifyOpStmt *)(info->pQuery->pRoot);
  stmt->freeHashFunc(stmt->pTableBlockHashObj);
  stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseLine(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
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
    if (lines) {
      tmp = lines[i];
      len = strlen(tmp);
    } else if (rawLine) {
      tmp = rawLine;
      while (rawLine < rawLineEnd) {
        if (*(rawLine++) == '\n') {
          break;
        }
        len++;
      }
      if (info->protocol == TSDB_SML_LINE_PROTOCOL && tmp[0] == '#') {  // this line is comment
        continue;
      }
    }

    uDebug("SML:0x%" PRIx64 " smlParseLine israw:%d, len:%d, sql:%s", info->id, info->isRawLine, len,
           (info->isRawLine ? "rawdata" : tmp));

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
      } else {
        code = smlParseTelnetString(info, (char *)tmp, (char *)tmp + len, info->lines + i);
      }

    } else {
      ASSERT(0);
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseLine failed. line %d : %s", info->id, i, tmp);
      return code;
    }
    if (info->reRun) {
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
  info->cost.numOfSTables = nodeListSize(info->superTables);
  info->cost.numOfCTables = nodeListSize(info->childTables);

  info->cost.schemaTime = taosGetTimestampUs();

  do {
    code = smlModifyDBSchemas(info);
    if (code == 0) break;
  } while (retryNum++ < nodeListSize(info->superTables) * MAX_RETRY_TIMES);

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

TAOS_RES *taos_schemaless_insert_inner(TAOS *taos, char *lines[], char *rawLine, char *rawLineEnd, int numLines,
                                       int protocol, int precision, int32_t ttl, int64_t reqid) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  SRequestObj *request = (SRequestObj *)createRequest(*(int64_t *)taos, TSDB_SQL_INSERT, reqid);
  if (request == NULL) {
    uError("SML:taos_schemaless_insert error request is null");
    return NULL;
  }

  SSmlHandle *info = smlBuildSmlInfo(taos);
  if (info == NULL) {
    request->code = TSDB_CODE_OUT_OF_MEMORY;
    uError("SML:taos_schemaless_insert error SSmlHandle is null");
    return (TAOS_RES *)request;
  }
  info->pRequest = request;
  info->isRawLine = rawLine != NULL;
  info->ttl = ttl;
  info->precision = precision;
  info->protocol = (TSDB_SML_PROTOCOL_TYPE)protocol;
  info->msgBuf.buf = info->pRequest->msgBuf;
  info->msgBuf.len = ERROR_MSG_BUF_DEFAULT_SIZE;
  info->lineNum = numLines;

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
  //  smlPrintStatisticInfo(info);

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

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                    int precision, int32_t ttl, int64_t reqid) {
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
