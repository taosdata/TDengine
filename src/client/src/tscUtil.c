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

#include <assert.h>
#include <math.h>
#include <time.h>

#include "ihash.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tkey.h"
#include "tmd5.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tsqldef.h"
#include "ttimer.h"

/*
 * the detailed information regarding metric meta key is:
 * fullmetername + '.' + querycond  + '.' + [tagId1, tagId2,...] + '.' + group_orderType
 *
 * if querycond is null, its format is:
 * fullmetername + '.' + '(nil)' + '.' + [tagId1, tagId2,...] + '.' + group_orderType
 */
void tscGetMetricMetaCacheKey(SSqlCmd* pCmd, char* keyStr) {
  char*         pTagCondStr = NULL;
  const int32_t RESERVED_SIZE = 100;

  char    tagIdBuf[128] = {0};
  int32_t offset = 0;
  for (int32_t i = 0; i < pCmd->numOfReqTags; ++i) {
    offset += sprintf(&tagIdBuf[offset], "%d,", pCmd->tagColumnIndex[i]);
  }

  assert(offset < tListLen(tagIdBuf));
  size_t len = strlen(pCmd->name);

  /* for too long key, we use the md5 to generated the key for local cache */
  if (pCmd->tagCond.len >= TSDB_MAX_TAGS_LEN - RESERVED_SIZE - offset) {
    MD5_CTX ctx;
    MD5Init(&ctx);
    MD5Update(&ctx, (uint8_t*)tsGetMetricQueryCondPos(&pCmd->tagCond), pCmd->tagCond.len);
    MD5Final(&ctx);

    pTagCondStr = base64_encode(ctx.digest, tListLen(ctx.digest));
  } else if (pCmd->tagCond.len + len + offset <= TSDB_MAX_TAGS_LEN && pCmd->tagCond.len > 0) {
    pTagCondStr = strdup(tsGetMetricQueryCondPos(&pCmd->tagCond));
  }

  int32_t keyLen = sprintf(keyStr, "%s.%s.[%s].%d", pCmd->name, pTagCondStr, tagIdBuf, pCmd->groupbyExpr.orderType);

  free(pTagCondStr);
  assert(keyLen <= TSDB_MAX_TAGS_LEN);
}

char* tsGetMetricQueryCondPos(STagCond* pTagCond) { return pTagCond->pData; }

bool tscQueryOnMetric(SSqlCmd* pCmd) { return UTIL_METER_IS_METRIC(pCmd) && pCmd->msgType == TSDB_MSG_TYPE_QUERY; }

void tscGetDBInfoFromMeterId(char* meterId, char* db) {
  char* st = strstr(meterId, TS_PATH_DELIMITER);
  if (st != NULL) {
    char* end = strstr(st + 1, TS_PATH_DELIMITER);
    if (end != NULL) {
      memcpy(db, meterId, (end - meterId));
      db[end - meterId] = 0;
      return;
    }
  }

  db[0] = 0;
}

SVnodeSidList* tscGetVnodeSidList(SMetricMeta* pMetricmeta, int32_t vnodeIdx) {
  if (pMetricmeta == NULL) {
    tscError("illegal metricmeta");
    return 0;
  }

  if (pMetricmeta->numOfVnodes == 0 || pMetricmeta->numOfMeters == 0) {
    return 0;
  }

  if (vnodeIdx < 0 || vnodeIdx >= pMetricmeta->numOfVnodes) {
    int32_t vnodeRange = (pMetricmeta->numOfVnodes > 0) ? (pMetricmeta->numOfVnodes - 1) : 0;
    tscError("illegal vnodeIdx:%d, reset to 0, vnodeIdx range:%d-%d", vnodeIdx, 0, vnodeRange);

    vnodeIdx = 0;
  }

  return (SVnodeSidList*)(pMetricmeta->list[vnodeIdx] + (char*)pMetricmeta);
}

SMeterSidExtInfo* tscGetMeterSidInfo(SVnodeSidList* pSidList, int32_t idx) {
  if (pSidList == NULL) {
    tscError("illegal sidlist");
    return 0;
  }

  if (idx < 0 || idx >= pSidList->numOfSids) {
    int32_t sidRange = (pSidList->numOfSids > 0) ? (pSidList->numOfSids - 1) : 0;

    tscError("illegal sidIdx:%d, reset to 0, sidIdx range:%d-%d", idx, 0, sidRange);
    idx = 0;
  }
  return (SMeterSidExtInfo*)(pSidList->pSidExtInfoList[idx] + (char*)pSidList);
}

bool tscIsTwoStageMergeMetricQuery(SSqlObj* pSql) {
  assert(pSql != NULL);

  SSqlCmd* pCmd = &pSql->cmd;
  if (pCmd->pMeterMeta == NULL) {
    return false;
  }

  if (pCmd->vnodeIdx == 0 && pCmd->command == TSDB_SQL_SELECT && (tscSqlExprGet(pCmd, 0)->sqlFuncId != TSDB_FUNC_PRJ)) {
    return UTIL_METER_IS_METRIC(pCmd);
  }

  return false;
}

bool tscProjectionQueryOnMetric(SSqlObj* pSql) {
  assert(pSql != NULL);

  SSqlCmd* pCmd = &pSql->cmd;

  /*
   * In following cases, return false for project query on metric
   * 1. failed to get metermeta from server; 2. not a metric; 3. limit 0; 4. show query, instead of a select query
   */
  if (pCmd->pMeterMeta == NULL || !UTIL_METER_IS_METRIC(pCmd) || pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pCmd->exprsInfo.numOfExprs == 0) {
    return false;
  }

  /*
   * Note:if there is COLPRJ_FUNCTION, only TAGPRJ_FUNCTION is allowed simultaneous
   * for interp query, the query routine will action the same as projection query on metric
   */
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(&pSql->cmd, i);
    if (pExpr->sqlFuncId == TSDB_FUNC_PRJ) {
      return true;
    }
  }

  return false;
}

bool tscIsPointInterpQuery(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr == NULL) {
      return false;
    }

    int32_t functionId = pExpr->sqlFuncId;
    if (functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (functionId != TSDB_FUNC_INTERP) {
      return false;
    }
  }

  return true;
}

bool tscIsFirstProjQueryOnMetric(SSqlObj* pSql) {
  return (tscProjectionQueryOnMetric(pSql) && (pSql->cmd.vnodeIdx == 0));
}

void tscClearInterpInfo(SSqlCmd* pCmd) {
  if (!tscIsPointInterpQuery(pCmd)) {
    return;
  }

  pCmd->interpoType = TSDB_INTERPO_NONE;
  memset(pCmd->defaultVal, 0, sizeof(pCmd->defaultVal));
}

void tscClearSqlMetaInfo(SSqlCmd* pCmd) {
  /* remove the metermeta/metricmeta in cache */
  taosRemoveDataFromCache(tscCacheHandle, (void**)&(pCmd->pMeterMeta), false);
  taosRemoveDataFromCache(tscCacheHandle, (void**)&(pCmd->pMetricMeta), false);
}

void tscClearSqlMetaInfoForce(SSqlCmd* pCmd) {
  /* remove the metermeta/metricmeta in cache */
  taosRemoveDataFromCache(tscCacheHandle, (void**)&(pCmd->pMeterMeta), true);
  taosRemoveDataFromCache(tscCacheHandle, (void**)&(pCmd->pMetricMeta), true);
}

int32_t tscCreateResPointerInfo(SSqlCmd* pCmd, SSqlRes* pRes) {
  if (pRes->tsrow == NULL) {
    pRes->numOfnchar = 0;
    int32_t numOfOutputCols = pCmd->fieldsInfo.numOfOutputCols;

    for (int32_t i = 0; i < numOfOutputCols; ++i) {
      TAOS_FIELD* pField = tscFieldInfoGetField(pCmd, i);
      if (pField->type == TSDB_DATA_TYPE_NCHAR) {
        pRes->numOfnchar++;
      }
    }

    pRes->tsrow = calloc(1, (POINTER_BYTES + sizeof(short)) * numOfOutputCols + POINTER_BYTES * pRes->numOfnchar);
    if (pRes->tsrow == NULL) {
      pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      return pRes->code;
    }

    pRes->bytes = (short*)((char*)pRes->tsrow + POINTER_BYTES * numOfOutputCols);
    if (pRes->numOfnchar > 0) {
      pRes->buffer = (char**)((char*)pRes->bytes + sizeof(short) * numOfOutputCols);
    }
  }

  return TSDB_CODE_SUCCESS;
}

void tscDestroyResPointerInfo(SSqlRes* pRes) {
  // free all buffers containing the multibyte string
  for (int i = 0; i < pRes->numOfnchar; i++) {
    if (pRes->buffer[i] != NULL) {
      tfree(pRes->buffer[i]);
    }
  }

  tfree(pRes->tsrow);

  pRes->numOfnchar = 0;
  pRes->buffer = NULL;
  pRes->bytes = NULL;
}

void tscfreeSqlCmdData(SSqlCmd* pCmd) {
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

  tscTagCondRelease(&pCmd->tagCond);
  tscClearFieldInfo(pCmd);

  tfree(pCmd->exprsInfo.pExprs);
  memset(&pCmd->exprsInfo, 0, sizeof(pCmd->exprsInfo));

  tfree(pCmd->colList.pColList);
  memset(&pCmd->colList, 0, sizeof(pCmd->colList));
}

void tscFreeSqlObjPartial(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) return;

  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  STscObj* pObj = pSql->pTscObj;

  int32_t cmd = pCmd->command;
  if (cmd < TSDB_SQL_INSERT || cmd == TSDB_SQL_RETRIEVE_METRIC || cmd == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    tscRemoveFromSqlList(pSql);
  }

  // pSql->sqlstr will be used by tscBuildQueryStreamDesc
  pthread_mutex_lock(&pObj->mutex);
  tfree(pSql->sqlstr);
  pthread_mutex_unlock(&pObj->mutex);

  tfree(pSql->res.pRsp);
  pSql->res.row = 0;
  pSql->res.numOfRows = 0;
  pSql->res.numOfTotal = 0;

  pSql->res.numOfGroups = 0;
  tfree(pSql->res.pGroupRec);

  tscDestroyLocalReducer(pSql);

  tfree(pSql->pSubs);
  pSql->numOfSubs = 0;
  tscDestroyResPointerInfo(pRes);

  tscfreeSqlCmdData(&pSql->cmd);
  tscClearSqlMetaInfo(pCmd);
}

void tscFreeSqlObj(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) return;

  tscTrace("%p start to free sql object", pSql);
  tscFreeSqlObjPartial(pSql);

  pSql->signature = NULL;
  pSql->fp = NULL;

  SSqlCmd* pCmd = &pSql->cmd;

  memset(pCmd->payload, 0, (size_t)tsRpcHeadSize);
  tfree(pCmd->payload);

  pCmd->allocSize = 0;

  if (pSql->res.buffer != NULL) {
    for (int i = 0; i < pCmd->fieldsInfo.numOfOutputCols; i++) {
      if (pSql->res.buffer[i] != NULL) {
        tfree(pSql->res.buffer[i]);
      }
    }

    tfree(pSql->res.buffer);
  }

  if (pSql->fp == NULL) {
    sem_destroy(&pSql->rspSem);
    sem_destroy(&pSql->emptyRspSem);
  }

  free(pSql);
}

STableDataBlocks* tscCreateDataBlock(int32_t size) {
  STableDataBlocks* dataBuf = (STableDataBlocks*)calloc(1, sizeof(STableDataBlocks));
  dataBuf->nAllocSize = (uint32_t)size;
  dataBuf->pData = calloc(1, dataBuf->nAllocSize);
  dataBuf->ordered = true;
  dataBuf->prevTS = INT64_MIN;
  return dataBuf;
}

void tscDestroyDataBlock(STableDataBlocks* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  tfree(pDataBlock->pData);
  tfree(pDataBlock);
}

SDataBlockList* tscCreateBlockArrayList() {
  const int32_t DEFAULT_INITIAL_NUM_OF_BLOCK = 16;

  SDataBlockList* pDataBlockArrayList = calloc(1, sizeof(SDataBlockList));
  pDataBlockArrayList->nAlloc = DEFAULT_INITIAL_NUM_OF_BLOCK;
  pDataBlockArrayList->pData = calloc(1, POINTER_BYTES * pDataBlockArrayList->nAlloc);

  return pDataBlockArrayList;
}

void* tscDestroyBlockArrayList(SDataBlockList* pList) {
  if (pList == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < pList->nSize; i++) {
    tscDestroyDataBlock(pList->pData[i]);
  }

  tfree(pList->pData);
  tfree(pList);

  return NULL;
}

int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, STableDataBlocks* pDataBlock) {
  SSqlCmd* pCmd = &pSql->cmd;

  pCmd->count = pDataBlock->numOfMeters;
  strncpy(pCmd->name, pDataBlock->meterId, TSDB_METER_ID_LEN);

  tscAllocPayloadWithSize(pCmd, pDataBlock->nAllocSize);
  memcpy(pCmd->payload, pDataBlock->pData, pDataBlock->nAllocSize);

  // set the message length
  pCmd->payloadLen = pDataBlock->nAllocSize;
  return tscGetMeterMeta(pSql, pCmd->name);
}

void tscFreeUnusedDataBlocks(SDataBlockList* pList) {
  /* release additional memory consumption */
  for (int32_t i = 0; i < pList->nSize; ++i) {
    STableDataBlocks* pDataBlock = pList->pData[i];
    pDataBlock->pData = realloc(pDataBlock->pData, pDataBlock->size);
    pDataBlock->nAllocSize = (uint32_t)pDataBlock->size;
  }
}

STableDataBlocks* tscCreateDataBlockEx(size_t size, int32_t rowSize, int32_t startOffset, char* name) {
  STableDataBlocks* dataBuf = tscCreateDataBlock(size);

  dataBuf->rowSize = rowSize;
  dataBuf->size = startOffset;
  strncpy(dataBuf->meterId, name, TSDB_METER_ID_LEN);
  return dataBuf;
}

STableDataBlocks* tscGetDataBlockFromList(void* pHashList, SDataBlockList* pDataBlockList, int64_t id, int32_t size,
                                          int32_t startOffset, int32_t rowSize, char* tableId) {
  STableDataBlocks* dataBuf = NULL;

  STableDataBlocks** t1 = (STableDataBlocks**)taosGetIntHashData(pHashList, id);
  if (t1 != NULL) {
    dataBuf = *t1;
  }

  if (dataBuf == NULL) {
    dataBuf = tscCreateDataBlockEx((size_t)size, rowSize, startOffset, tableId);
    dataBuf = *(STableDataBlocks**)taosAddIntHash(pHashList, id, (char*)&dataBuf);
    tscAppendDataBlock(pDataBlockList, dataBuf);
  }

  return dataBuf;
}

void tscMergeTableDataBlocks(SSqlObj* pSql, SDataBlockList* pTableDataBlockList) {
  SSqlCmd*        pCmd = &pSql->cmd;
  void*           pVnodeDataBlockHashList = taosInitIntHash(8, sizeof(void*), taosHashInt);
  SDataBlockList* pVnodeDataBlockList = tscCreateBlockArrayList();

  for (int32_t i = 0; i < pTableDataBlockList->nSize; ++i) {
    STableDataBlocks* pOneTableBlock = pTableDataBlockList->pData[i];
    STableDataBlocks* dataBuf =
        tscGetDataBlockFromList(pVnodeDataBlockHashList, pVnodeDataBlockList, pOneTableBlock->vgid, TSDB_PAYLOAD_SIZE,
                                tsInsertHeadSize, 0, pOneTableBlock->meterId);

    int64_t destSize = dataBuf->size + pOneTableBlock->size;
    if (dataBuf->nAllocSize < destSize) {
      while (dataBuf->nAllocSize < destSize) {
        dataBuf->nAllocSize = dataBuf->nAllocSize * 1.5;
      }

      char* tmp = realloc(dataBuf->pData, dataBuf->nAllocSize);
      if (tmp != NULL) {
        dataBuf->pData = tmp;
        memset(dataBuf->pData + dataBuf->size, 0, dataBuf->nAllocSize - dataBuf->size);
      } else {
        // to do handle error
      }
    }

    SShellSubmitBlock* pBlocks = (SShellSubmitBlock*)pOneTableBlock->pData;
    sortRemoveDuplicates(pOneTableBlock);

    tscTrace("%p meterId:%s, sid:%d, rows:%d, sversion:%d", pSql, pOneTableBlock->meterId, pBlocks->sid,
             pBlocks->numOfRows, pBlocks->sversion);

    pBlocks->sid = htonl(pBlocks->sid);
    pBlocks->uid = htobe64(pBlocks->uid);
    pBlocks->sversion = htonl(pBlocks->sversion);
    pBlocks->numOfRows = htons(pBlocks->numOfRows);

    memcpy(dataBuf->pData + dataBuf->size, pOneTableBlock->pData, pOneTableBlock->size);

    dataBuf->size += pOneTableBlock->size;
    dataBuf->numOfMeters += 1;
  }

  tscDestroyBlockArrayList(pTableDataBlockList);

  // free the table data blocks;
  pCmd->pDataBlocks = pVnodeDataBlockList;

  tscFreeUnusedDataBlocks(pCmd->pDataBlocks);
  taosCleanUpIntHash(pVnodeDataBlockHashList);
}

void tscCloseTscObj(STscObj* pObj) {
  pObj->signature = NULL;
  SSqlObj* pSql = pObj->pSql;
  globalCode = pSql->res.code;

  taosTmrStopA(&(pObj->pTimer));
  tscFreeSqlObj(pSql);

  pthread_mutex_destroy(&pObj->mutex);
  tscTrace("%p DB connection is closed", pObj);
  tfree(pObj);
}

bool tscIsInsertOrImportData(char* sqlstr) {
  SSQLToken t0 = {0};
  while (1) {
    t0.n = tSQLGetToken(sqlstr, &t0.type);
    if (t0.type != TK_SPACE) {
      break;
    }

    sqlstr += t0.n;
  }

  return t0.type == TK_INSERT || t0.type == TK_IMPORT;
}

int tscAllocPayloadWithSize(SSqlCmd* pCmd, int size) {
  assert(size > 0);

  if (pCmd->payload == NULL) {
    assert(pCmd->allocSize == 0);

    pCmd->payload = (char*)calloc(1, size);
    if (pCmd->payload == NULL) return TSDB_CODE_CLI_OUT_OF_MEMORY;

    pCmd->allocSize = size;
  } else {
    if (pCmd->allocSize < size) {
      pCmd->payload = realloc(pCmd->payload, size);
      if (pCmd->payload == NULL) return TSDB_CODE_CLI_OUT_OF_MEMORY;
      pCmd->allocSize = size;
    }
  }

  memset(pCmd->payload, 0, pCmd->allocSize);
  assert(pCmd->allocSize >= size);

  return TSDB_CODE_SUCCESS;
}

static void ensureSpace(SFieldInfo* pFieldInfo, int32_t size) {
  if (size > pFieldInfo->numOfAlloc) {
    int32_t oldSize = pFieldInfo->numOfAlloc;

    int32_t newSize = (oldSize <= 0) ? 8 : (oldSize << 1);
    while (newSize < size) {
      newSize = (newSize << 1);
    }

    if (newSize > TSDB_MAX_COLUMNS) {
      newSize = TSDB_MAX_COLUMNS;
    }

    int32_t inc = newSize - oldSize;

    pFieldInfo->pFields = realloc(pFieldInfo->pFields, newSize * sizeof(TAOS_FIELD));
    memset(&pFieldInfo->pFields[oldSize], 0, inc * sizeof(TAOS_FIELD));

    pFieldInfo->pOffset = realloc(pFieldInfo->pOffset, newSize * sizeof(int16_t));
    memset(&pFieldInfo->pOffset[oldSize], 0, inc * sizeof(int16_t));

    pFieldInfo->numOfAlloc = newSize;
  }
}

static void evic(SFieldInfo* pFieldInfo, int32_t index) {
  if (index < pFieldInfo->numOfOutputCols) {
    memmove(&pFieldInfo->pFields[index + 1], &pFieldInfo->pFields[index],
            sizeof(pFieldInfo->pFields[0]) * (pFieldInfo->numOfOutputCols - index));
  }
}

static void setValueImpl(TAOS_FIELD* pField, int8_t type, char* name, int16_t bytes) {
  pField->type = type;
  strncpy(pField->name, name, TSDB_COL_NAME_LEN);
  pField->bytes = bytes;
}

void tscFieldInfoSetValFromSchema(SFieldInfo* pFieldInfo, int32_t index, SSchema* pSchema) {
  ensureSpace(pFieldInfo, pFieldInfo->numOfOutputCols + 1);
  evic(pFieldInfo, index);

  TAOS_FIELD* pField = &pFieldInfo->pFields[index];
  setValueImpl(pField, pSchema->type, pSchema->name, pSchema->bytes);
  pFieldInfo->numOfOutputCols++;
}

void tscFieldInfoSetValFromField(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* pField) {
  ensureSpace(pFieldInfo, pFieldInfo->numOfOutputCols + 1);
  evic(pFieldInfo, index);

  memcpy(&pFieldInfo->pFields[index], pField, sizeof(TAOS_FIELD));
  pFieldInfo->numOfOutputCols++;
}

void tscFieldInfoSetValue(SFieldInfo* pFieldInfo, int32_t index, int8_t type, char* name, int16_t bytes) {
  ensureSpace(pFieldInfo, pFieldInfo->numOfOutputCols + 1);
  evic(pFieldInfo, index);

  TAOS_FIELD* pField = &pFieldInfo->pFields[index];
  setValueImpl(pField, type, name, bytes);
  pFieldInfo->numOfOutputCols++;
}

void tscFieldInfoCalOffset(SSqlCmd* pCmd) {
  SFieldInfo* pFieldInfo = &pCmd->fieldsInfo;
  pFieldInfo->pOffset[0] = 0;

  for (int32_t i = 1; i < pFieldInfo->numOfOutputCols; ++i) {
    pFieldInfo->pOffset[i] = pFieldInfo->pOffset[i - 1] + pFieldInfo->pFields[i - 1].bytes;
  }
}

void tscFieldInfoRenewOffsetForInterResult(SSqlCmd* pCmd) {
  SFieldInfo* pFieldInfo = &pCmd->fieldsInfo;
  if (pFieldInfo->numOfOutputCols == 0) {
    return;
  }

  pFieldInfo->pOffset[0] = 0;

  /*
   * the retTypeLen is used to store the intermediate result length
   * for potential secondary merge exists
   */
  for (int32_t i = 1; i < pFieldInfo->numOfOutputCols; ++i) {
    pFieldInfo->pOffset[i] = pFieldInfo->pOffset[i - 1] + tscSqlExprGet(pCmd, i - 1)->resBytes;
  }
}

void tscFieldInfoClone(SFieldInfo* src, SFieldInfo* dst) {
  if (src == NULL) {
    return;
  }

  *dst = *src;

  dst->pFields = malloc(sizeof(TAOS_FIELD) * dst->numOfAlloc);
  dst->pOffset = malloc(sizeof(short) * dst->numOfAlloc);

  memcpy(dst->pFields, src->pFields, sizeof(TAOS_FIELD) * dst->numOfOutputCols);
  memcpy(dst->pOffset, src->pOffset, sizeof(short) * dst->numOfOutputCols);
}

TAOS_FIELD* tscFieldInfoGetField(SSqlCmd* pCmd, int32_t index) {
  if (index >= pCmd->fieldsInfo.numOfOutputCols) {
    return NULL;
  }

  return &pCmd->fieldsInfo.pFields[index];
}

int16_t tscFieldInfoGetOffset(SSqlCmd* pCmd, int32_t index) {
  if (index >= pCmd->fieldsInfo.numOfOutputCols) {
    return 0;
  }

  return pCmd->fieldsInfo.pOffset[index];
}

int32_t tscGetResRowLength(SSqlCmd* pCmd) {
  SFieldInfo* pFieldInfo = &pCmd->fieldsInfo;
  if (pFieldInfo->numOfOutputCols <= 0) {
    return 0;
  }

  return pFieldInfo->pOffset[pFieldInfo->numOfOutputCols - 1] +
         pFieldInfo->pFields[pFieldInfo->numOfOutputCols - 1].bytes;
}

void tscClearFieldInfo(SSqlCmd* pCmd) {
  if (pCmd == NULL) {
    return;
  }

  tfree(pCmd->fieldsInfo.pOffset);
  tfree(pCmd->fieldsInfo.pFields);
  memset(&pCmd->fieldsInfo, 0, sizeof(pCmd->fieldsInfo));
}

static void _exprCheckSpace(SSqlExprInfo* pExprInfo, int32_t size) {
  if (size > pExprInfo->numOfAlloc) {
    int32_t oldSize = pExprInfo->numOfAlloc;

    int32_t newSize = (oldSize <= 0) ? 8 : (oldSize << 1);
    while (newSize < size) {
      newSize = (newSize << 1);
    }

    if (newSize > TSDB_MAX_COLUMNS) {
      newSize = TSDB_MAX_COLUMNS;
    }

    int32_t inc = newSize - oldSize;

    pExprInfo->pExprs = realloc(pExprInfo->pExprs, newSize * sizeof(SSqlExpr));
    memset(&pExprInfo->pExprs[oldSize], 0, inc * sizeof(SSqlExpr));

    pExprInfo->numOfAlloc = newSize;
  }
}

static void _exprEvic(SSqlExprInfo* pExprInfo, int32_t index) {
  if (index < pExprInfo->numOfExprs) {
    memmove(&pExprInfo->pExprs[index + 1], &pExprInfo->pExprs[index],
            sizeof(pExprInfo->pExprs[0]) * (pExprInfo->numOfExprs - index));
  }
}

SSqlExpr* tscSqlExprInsert(SSqlCmd* pCmd, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size) {
  SSqlExprInfo* pExprInfo = &pCmd->exprsInfo;
  SSchema*      pSchema = tsGetSchema(pCmd->pMeterMeta);

  _exprCheckSpace(pExprInfo, pExprInfo->numOfExprs + 1);
  _exprEvic(pExprInfo, index);

  SSqlExpr* pExpr = &pExprInfo->pExprs[index];

  pExpr->sqlFuncId = functionId;

  pExpr->colInfo.colIdx = srcColumnIndex;
  if (srcColumnIndex == -1) {
    pExpr->colInfo.colId = -1;
  } else {
    pExpr->colInfo.colId = pSchema[srcColumnIndex].colId;
  }

  pExpr->colInfo.isTag = false;
  pExpr->resType = type;
  pExpr->resBytes = size;

  pExprInfo->numOfExprs++;
  return pExpr;
}

SSqlExpr* tscSqlExprUpdate(SSqlCmd* pCmd, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size) {
  SSqlExprInfo* pExprInfo = &pCmd->exprsInfo;
  if (index > pExprInfo->numOfExprs) {
    return NULL;
  }

  SSqlExpr* pExpr = &pExprInfo->pExprs[index];

  pExpr->sqlFuncId = functionId;

  pExpr->colInfo.colIdx = srcColumnIndex;
  pExpr->colInfo.colId = tsGetSchemaColIdx(pCmd->pMeterMeta, srcColumnIndex)->colId;

  pExpr->resType = type;
  pExpr->resBytes = size;

  return pExpr;
}

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes) {
  if (pExpr == NULL || argument == NULL || bytes == 0) {
    return;
  }

  // set parameter value
  // transfer to tVariant from byte data/no ascii data
  tVariantCreateB(&pExpr->param[pExpr->numOfParams], argument, bytes, type);

  pExpr->numOfParams += 1;
  assert(pExpr->numOfParams <= 3);
}

SSqlExpr* tscSqlExprGet(SSqlCmd* pCmd, int32_t index) {
  if (pCmd->exprsInfo.numOfExprs <= index) {
    return NULL;
  }

  return &pCmd->exprsInfo.pExprs[index];
}

void tscSqlExprClone(SSqlExprInfo* src, SSqlExprInfo* dst) {
  if (src == NULL) {
    return;
  }

  *dst = *src;

  dst->pExprs = malloc(sizeof(SSqlExpr) * dst->numOfAlloc);
  memcpy(dst->pExprs, src->pExprs, sizeof(SSqlExpr) * dst->numOfExprs);

  for (int32_t i = 0; i < dst->numOfExprs; ++i) {
    for (int32_t j = 0; j < src->pExprs[i].numOfParams; ++j) {
      tVariantAssign(&dst->pExprs[i].param[j], &src->pExprs[i].param[j]);
    }
  }
}

static void _cf_ensureSpace(SColumnsInfo* pcolList, int32_t size) {
  if (pcolList->numOfAlloc < size) {
    int32_t oldSize = pcolList->numOfAlloc;

    int32_t newSize = (oldSize <= 0) ? 8 : (oldSize << 1);
    while (newSize < size) {
      newSize = (newSize << 1);
    }

    if (newSize > TSDB_MAX_COLUMNS) {
      newSize = TSDB_MAX_COLUMNS;
    }

    int32_t inc = newSize - oldSize;

    pcolList->pColList = realloc(pcolList->pColList, newSize * sizeof(SColumnBase));
    memset(&pcolList->pColList[oldSize], 0, inc * sizeof(SColumnBase));

    pcolList->numOfAlloc = newSize;
  }
}

static void _cf_evic(SColumnsInfo* pcolList, int32_t index) {
  if (index < pcolList->numOfCols) {
    memmove(&pcolList->pColList[index + 1], &pcolList->pColList[index],
            sizeof(SColumnBase) * (pcolList->numOfCols - index));

    memset(&pcolList->pColList[index], 0, sizeof(SColumnBase));
  }
}

SColumnBase* tscColumnInfoGet(SSqlCmd* pCmd, int32_t index) {
  if (pCmd->colList.numOfCols < index) {
    return NULL;
  }

  return &pCmd->colList.pColList[index];
}

SColumnBase* tscColumnInfoInsert(SSqlCmd* pCmd, int32_t colIndex) {
  SColumnsInfo* pcolList = &pCmd->colList;

  if (colIndex < 0) {
    /* ignore the tbname column to be inserted into source list */
    return NULL;
  }

  int32_t i = 0;
  while (i < pcolList->numOfCols && pcolList->pColList[i].colIndex < colIndex) {
    i++;
  }

  if ((i < pcolList->numOfCols && pcolList->pColList[i].colIndex > colIndex) || (i >= pcolList->numOfCols)) {
    _cf_ensureSpace(pcolList, pcolList->numOfCols + 1);
    _cf_evic(pcolList, i);

    pcolList->pColList[i].colIndex = (int16_t)colIndex;
    pcolList->numOfCols++;
    pCmd->numOfCols++;
  }

  return &pcolList->pColList[i];
}

void tscColumnInfoClone(SColumnsInfo* src, SColumnsInfo* dst) {
  if (src == NULL) {
    return;
  }

  *dst = *src;

  dst->pColList = malloc(sizeof(SColumnBase) * dst->numOfAlloc);
  memcpy(dst->pColList, src->pColList, sizeof(SColumnBase) * dst->numOfCols);
}

void tscColumnInfoReserve(SSqlCmd* pCmd, int32_t size) { _cf_ensureSpace(&pCmd->colList, size); }

/*
 * 1. normal name, not a keyword or number
 * 2. name with quote
 * 3. string with only one delimiter '.'.
 *
 * only_one_part
 * 'only_one_part'
 * first_part.second_part
 * first_part.'second_part'
 * 'first_part'.second_part
 * 'first_part'.'second_part'
 * 'first_part.second_part'
 *
 */

static int32_t validateQuoteToken(SSQLToken* pToken) {
  pToken->n = strdequote(pToken->z);
  strtrim(pToken->z);
  pToken->n = (uint32_t)strlen(pToken->z);

  int32_t k = tSQLGetToken(pToken->z, &pToken->type);

  if (pToken->type == TK_STRING) {
    return tscValidateName(pToken);
  }

  if (k != pToken->n || pToken->type != TK_ID) {
    return TSDB_CODE_INVALID_SQL;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tscValidateName(SSQLToken* pToken) {
  if (pToken->type != TK_STRING && pToken->type != TK_ID) {
    return TSDB_CODE_INVALID_SQL;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep == NULL) {  // single part
    if (pToken->type == TK_STRING) {
      pToken->n = strdequote(pToken->z);
      strtrim(pToken->z);
      pToken->n = (uint32_t)strlen(pToken->z);

      int len = tSQLGetToken(pToken->z, &pToken->type);

      // single token, validate it
      if (len == pToken->n) {
        return validateQuoteToken(pToken);
      } else {
        sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
        if (sep == NULL) {
          return TSDB_CODE_INVALID_SQL;
        }

        return tscValidateName(pToken);
      }
    } else {
      if (isNumber(pToken)) {
        return TSDB_CODE_INVALID_SQL;
      }
    }
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;

    if (pToken->type == TK_SPACE) {
      strtrim(pToken->z);
      pToken->n = (uint32_t)strlen(pToken->z);
    }

    pToken->n = tSQLGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (pToken->type != TK_STRING && pToken->type != TK_ID) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (pToken->type == TK_STRING && validateQuoteToken(pToken) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = oldLen - (sep - pStr) - 1;
    int32_t len = tSQLGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || (pToken->type != TK_STRING && pToken->type != TK_ID)) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (pToken->type == TK_STRING && validateQuoteToken(pToken) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    // re-build the whole name string
    if (pStr[firstPartLen] == TS_PATH_DELIMITER[0]) {
      // first part do not have quote
      // do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      pStr[firstPartLen + sizeof(TS_PATH_DELIMITER[0]) + pToken->n] = 0;
    }
    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;
  }

  return TSDB_CODE_SUCCESS;
}

void tscIncStreamExecutionCount(void* pStream) {
  if (pStream == NULL) {
    return;
  }

  SSqlStream* ps = (SSqlStream*)pStream;
  ps->num += 1;
}

bool tscValidateColumnId(SSqlCmd* pCmd, int32_t colId) {
  if (pCmd->pMeterMeta == NULL) {
    return false;
  }

  if (colId == -1 && UTIL_METER_IS_METRIC(pCmd)) {
    return true;
  }

  SSchema* pSchema = tsGetSchema(pCmd->pMeterMeta);
  int32_t  numOfTotal = pCmd->pMeterMeta->numOfTags + pCmd->pMeterMeta->numOfColumns;

  for (int32_t i = 0; i < numOfTotal; ++i) {
    if (pSchema[i].colId == colId) {
      return true;
    }
  }

  return false;
}

void tscTagCondAssign(STagCond* pDst, STagCond* pSrc) {
  if (pSrc->len == 0) {
    memset(pDst, 0, sizeof(STagCond));
    return;
  }

  pDst->pData = strdup(pSrc->pData);
  pDst->allocSize = pSrc->len + 1;
  pDst->type = pSrc->type;
  pDst->len = pSrc->len;
}

void tscTagCondRelease(STagCond* pCond) {
  if (pCond->allocSize > 0) {
    assert(pCond->pData != NULL);
    tfree(pCond->pData);
  }

  memset(pCond, 0, sizeof(STagCond));
}

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SSqlCmd* pCmd) {
  SSchema* pSchema = tsGetSchema(pCmd->pMeterMeta);

  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    pColInfo[i].functionId = pExpr->sqlFuncId;

    if (pExpr->colInfo.isTag) {
      SSchema* pTagSchema = tsGetTagSchema(pCmd->pMeterMeta);
      int16_t  actualTagIndex = pCmd->tagColumnIndex[pExpr->colInfo.colIdx];

      pColInfo[i].type = (actualTagIndex != -1) ? pTagSchema[actualTagIndex].type : TSDB_DATA_TYPE_BINARY;
    } else {
      pColInfo[i].type = pSchema[pExpr->colInfo.colIdx].type;
    }
  }
}

void tscSetFreeHeatBeat(STscObj* pObj) {
  if (pObj == NULL || pObj->signature != pObj || pObj->pHb == NULL) {
    return;
  }

  SSqlObj* pHeatBeat = pObj->pHb;
  assert(pHeatBeat == pHeatBeat->signature);

  pHeatBeat->cmd.type = 1;  // to denote the heart-beat timer close connection and free all allocated resources
}

bool tscShouldFreeHeatBeat(SSqlObj* pHb) {
  assert(pHb == pHb->signature);

  return pHb->cmd.type == 1;
}

void tscCleanSqlCmd(SSqlCmd* pCmd) {
  tscfreeSqlCmdData(pCmd);

  uint32_t     allocSize = pCmd->allocSize;
  char*        allocPtr = pCmd->payload;
  SMeterMeta*  pMeterMeta = pCmd->pMeterMeta;
  SMetricMeta* pMetricMeta = pCmd->pMetricMeta;

  memset(pCmd, 0, sizeof(SSqlCmd));

  // restore values
  pCmd->allocSize = allocSize;
  pCmd->payload = allocPtr;
  pCmd->pMeterMeta = pMeterMeta;
  pCmd->pMetricMeta = pMetricMeta;
}

/*
 * the following three kinds of SqlObj should not be freed
 * 1. SqlObj for stream computing
 * 2. main SqlObj
 * 3. heartbeat SqlObj
 *
 * If res code is error and SqlObj does not belong to above types, it should be
 * automatically freed for async query, ignoring that connection should be kept.
 *
 * If connection need to be recycled, the SqlObj also should be freed.
 */
bool tscShouldFreeAsyncSqlObj(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql || pSql->fp == NULL) {
    return false;
  }

  STscObj* pTscObj = pSql->pTscObj;
  if (pSql->pStream != NULL || pTscObj->pHb == pSql) {
    return false;
  }

  int32_t command = pSql->cmd.command;
  if (pTscObj->pSql == pSql) {
    /*
     * in case of taos_connect_a query, the object should all be released, even it is the
     * master sql object. Otherwise, the master sql should not be released
     */
    if (command == TSDB_SQL_CONNECT && pSql->res.code != TSDB_CODE_SUCCESS) {
      return true;
    }

    return false;
  }

  if (command == TSDB_SQL_INSERT) {
    SSqlCmd* pCmd = &pSql->cmd;

    /*
     * in case of multi-vnode insertion, the object should not be released until all
     * data blocks have been submit to vnode.
     */
    SDataBlockList* pDataBlocks = pCmd->pDataBlocks;
    if (pDataBlocks == NULL || pCmd->vnodeIdx >= pDataBlocks->nSize) {
      tscTrace("%p object should be release since all data blocks have been submit", pSql);
      return true;
    } else {
      return false;
    }
  } else {
    return tscKeepConn[command] == 0 ||
           (pSql->res.code != TSDB_CODE_ACTION_IN_PROGRESS && pSql->res.code != TSDB_CODE_SUCCESS);
  }
}

void tscDoQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
  } else {
    if (pCmd->command == TSDB_SQL_SELECT) {
      tscAddIntoSqlList(pSql);
    }

    if (tscIsFirstProjQueryOnMetric(pSql)) {
      pSql->cmd.vnodeIdx += 1;
    }

    void* fp = pSql->fp;

    if (pCmd->isInsertFromFile == 1) {
      tscProcessMultiVnodesInsertForFile(pSql);
    } else {
      // pSql may be released in this function if it is a async insertion.
      tscProcessSql(pSql);

      // handle the multi-vnode insertion for sync model
      if (fp == NULL) {
        assert(pSql->signature == pSql);
        tscProcessMultiVnodesInsert(pSql);
      }
    }
  }
}
