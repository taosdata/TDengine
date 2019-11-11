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

#include "os.h"
#include "ihash.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tkey.h"
#include "tmd5.h"
#include "tscJoinProcess.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tsqldef.h"
#include "ttimer.h"

/*
 * the detailed information regarding metric meta key is:
 * fullmetername + '.' + tagQueryCond + '.' + tableNameCond + '.' + joinCond +
 * '.' + relation + '.' + [tagId1, tagId2,...] + '.' + group_orderType
 *
 * if querycond/tablenameCond/joinCond is null, its format is:
 * fullmetername + '.' + '(nil)' + '.' + '(nil)' + relation + '.' + [tagId1,
 * tagId2,...] + '.' + group_orderType
 */
void tscGetMetricMetaCacheKey(SSqlCmd* pCmd, char* str, uint64_t uid) {
  int32_t         index = -1;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoByUid(pCmd, uid, &index);

  int32_t len = 0;
  char    tagIdBuf[128] = {0};
  for (int32_t i = 0; i < pMeterMetaInfo->numOfTags; ++i) {
    len += sprintf(&tagIdBuf[len], "%d,", pMeterMetaInfo->tagColumnIndex[i]);
  }

  STagCond* pTagCond = &pCmd->tagCond;
  assert(len < tListLen(tagIdBuf));

  const int32_t maxKeySize = TSDB_MAX_TAGS_LEN;  // allowed max key size
  char*         tmp = calloc(1, TSDB_MAX_SQL_LEN);

  SCond* cond = tsGetMetricQueryCondPos(pTagCond, uid);

  char join[512] = {0};
  if (pTagCond->joinInfo.hasJoin) {
    sprintf(join, "%s,%s", pTagCond->joinInfo.left.meterId, pTagCond->joinInfo.right.meterId);
  }

  int32_t keyLen =
      snprintf(tmp, TSDB_MAX_SQL_LEN, "%s,%s,%s,%d,%s,[%s],%d", pMeterMetaInfo->name,
               (cond != NULL ? cond->cond.z : NULL), pTagCond->tbnameCond.cond.n > 0 ? pTagCond->tbnameCond.cond.z : NULL,
               pTagCond->relType, join, tagIdBuf, pCmd->groupbyExpr.orderType);

  assert(keyLen <= TSDB_MAX_SQL_LEN);

  if (keyLen < maxKeySize) {
    strcpy(str, tmp);
  } else {  // using md5 to hash
    MD5_CTX ctx;
    MD5Init(&ctx);

    MD5Update(&ctx, (uint8_t*) tmp, keyLen);
    char* pStr = base64_encode(ctx.digest, tListLen(ctx.digest));
    strcpy(str, pStr);
  }

  free(tmp);
}

SCond* tsGetMetricQueryCondPos(STagCond* pTagCond, uint64_t uid) {
  for (int32_t i = 0; i < TSDB_MAX_JOIN_TABLE_NUM; ++i) {
    if (uid == pTagCond->cond[i].uid) {
      return &pTagCond->cond[i];
    }
  }

  return NULL;
}

void tsSetMetricQueryCond(STagCond* pTagCond, uint64_t uid, const char* str) {
  size_t len = strlen(str);
  if (len == 0) {
    return;
  }

  SCond* pDest = &pTagCond->cond[pTagCond->numOfTagCond];
  pDest->uid = uid;
  pDest->cond = SStringCreate(str);

  pTagCond->numOfTagCond += 1;
}

bool tscQueryOnMetric(SSqlCmd* pCmd) {
  return ((pCmd->type & TSDB_QUERY_TYPE_STABLE_QUERY) == TSDB_QUERY_TYPE_STABLE_QUERY) &&
         (pCmd->msgType == TSDB_MSG_TYPE_QUERY);
}

bool tscQueryMetricTags(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    if (tscSqlExprGet(pCmd, i)->functionId != TSDB_FUNC_TAGPRJ) {
      return false;
    }
  }

  return true;
}

bool tscIsSelectivityWithTagQuery(SSqlCmd* pCmd) {
  bool    hasTags = false;
  int32_t numOfSelectivity = 0;

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functId = tscSqlExprGet(pCmd, i)->functionId;
    if (functId == TSDB_FUNC_TAG_DUMMY) {
      hasTags = true;
      continue;
    }

    if ((aAggs[functId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    }
  }

  if (numOfSelectivity > 0 && hasTags) {
    return true;
  }

  return false;
}


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

bool tscIsTwoStageMergeMetricQuery(SSqlCmd* pCmd) {
  assert(pCmd != NULL);

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (pMeterMetaInfo == NULL || pMeterMetaInfo->pMetricMeta == NULL) {
    return false;
  }

  // for projection query, iterate all qualified vnodes sequentially
  if (tscProjectionQueryOnMetric(pCmd)) {
    return false;
  }

  if (((pCmd->type & TSDB_QUERY_TYPE_STABLE_SUBQUERY) != TSDB_QUERY_TYPE_STABLE_SUBQUERY) &&
      pCmd->command == TSDB_SQL_SELECT) {
    return UTIL_METER_IS_METRIC(pMeterMetaInfo);
  }

  return false;
}

bool tscProjectionQueryOnMetric(SSqlCmd* pCmd) {
  assert(pCmd != NULL);

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  /*
   * In following cases, return false for project query on metric
   * 1. failed to get metermeta from server; 2. not a metric; 3. limit 0; 4. show query, instead of a select query
   */
  if (pMeterMetaInfo == NULL || !UTIL_METER_IS_METRIC(pMeterMetaInfo) ||
      pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pCmd->exprsInfo.numOfExprs == 0) {
    return false;
  }

  // only query on tag, not a projection query
  if (tscQueryMetricTags(pCmd)) {
    return false;
  }

  //for project query, only the following two function is allowed
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    int32_t functionId = pExpr->functionId;
    if (functionId != TSDB_FUNC_PRJ && functionId != TSDB_FUNC_TAGPRJ &&
        functionId != TSDB_FUNC_TAG && functionId != TSDB_FUNC_TS) {
      return false;
    }
  }

  return true;
}

bool tscIsPointInterpQuery(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr == NULL) {
      return false;
    }

    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (functionId != TSDB_FUNC_INTERP) {
      return false;
    }
  }
  return true;
}

bool tscIsTWAQuery(SSqlCmd* pCmd) {
  for(int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TWA) {
      return true;
    }
  }

  return false;
}

void tscClearInterpInfo(SSqlCmd* pCmd) {
  if (!tscIsPointInterpQuery(pCmd)) {
    return;
  }

  pCmd->interpoType = TSDB_INTERPO_NONE;
  memset(pCmd->defaultVal, 0, sizeof(pCmd->defaultVal));
}

void tscClearSqlMetaInfoForce(SSqlCmd* pCmd) {
  /* remove the metermeta/metricmeta in cache */
  //    taosRemoveDataFromCache(tscCacheHandle, (void**)&(pCmd->pMeterMeta),
  //    true);
  //    taosRemoveDataFromCache(tscCacheHandle, (void**)&(pCmd->pMetricMeta),
  //    true);
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

void tscFreeSqlCmdData(SSqlCmd* pCmd) {
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

  tscTagCondRelease(&pCmd->tagCond);
  tscClearFieldInfo(&pCmd->fieldsInfo);

  tfree(pCmd->exprsInfo.pExprs);
  memset(&pCmd->exprsInfo, 0, sizeof(pCmd->exprsInfo));

  tscColumnBaseInfoDestroy(&pCmd->colList);
  memset(&pCmd->colList, 0, sizeof(pCmd->colList));

  if (pCmd->tsBuf != NULL) {
    tsBufDestory(pCmd->tsBuf);
    pCmd->tsBuf = NULL;
  }
}

void tscFreeSqlObjPartial(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  STscObj* pObj = pSql->pTscObj;

  int32_t cmd = pCmd->command;
  if (cmd < TSDB_SQL_INSERT || cmd == TSDB_SQL_RETRIEVE_METRIC || cmd == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      cmd == TSDB_SQL_METRIC_JOIN_RETRIEVE) {
    tscRemoveFromSqlList(pSql);
  }

  pCmd->command = -1;

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
  tfree(pSql->res.pColumnIndex);

  tscFreeSqlCmdData(pCmd);
  tscRemoveAllMeterMetaInfo(pCmd, false);
}

void tscFreeSqlObj(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) return;

  tscTrace("%p start to free sql object", pSql);
  tscFreeSqlObjPartial(pSql);

  pSql->signature = NULL;
  pSql->fp = NULL;
  SSqlCmd* pCmd = &pSql->cmd;

  memset(pCmd->payload, 0, (size_t)pCmd->allocSize);
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
    tsem_destroy(&pSql->rspSem);
    tsem_destroy(&pSql->emptyRspSem);
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
  tfree(pDataBlock->params);
  tfree(pDataBlock);
}

SParamInfo* tscAddParamToDataBlock(STableDataBlocks* pDataBlock, char type, uint8_t timePrec, short bytes, uint32_t offset) {
  uint32_t needed = pDataBlock->numOfParams + 1;
  if (needed > pDataBlock->numOfAllocedParams) {
    needed *= 2;
    void* tmp = realloc(pDataBlock->params, needed * sizeof(SParamInfo));
    if (tmp == NULL) {
      return NULL;
    }
    pDataBlock->params = (SParamInfo*)tmp;
    pDataBlock->numOfAllocedParams = needed;
  }

  SParamInfo* param = pDataBlock->params + pDataBlock->numOfParams;
  param->idx = -1;
  param->type = type;
  param->timePrec = timePrec;
  param->bytes = bytes;
  param->offset = offset;

  ++pDataBlock->numOfParams;
  return param;
}

SDataBlockList* tscCreateBlockArrayList() {
  const int32_t DEFAULT_INITIAL_NUM_OF_BLOCK = 16;

  SDataBlockList* pDataBlockArrayList = calloc(1, sizeof(SDataBlockList));
  if (pDataBlockArrayList == NULL) {
    return NULL;
  }
  pDataBlockArrayList->nAlloc = DEFAULT_INITIAL_NUM_OF_BLOCK;
  pDataBlockArrayList->pData = calloc(1, POINTER_BYTES * pDataBlockArrayList->nAlloc);
  if (pDataBlockArrayList->pData == NULL) {
    free(pDataBlockArrayList);
    return NULL;
  }

  return pDataBlockArrayList;
}

void tscAppendDataBlock(SDataBlockList *pList, STableDataBlocks *pBlocks) {
  if (pList->nSize >= pList->nAlloc) {
    pList->nAlloc = pList->nAlloc << 1;
    pList->pData = realloc(pList->pData, sizeof(void *) * (size_t)pList->nAlloc);

    // reset allocated memory
    memset(pList->pData + pList->nSize, 0, sizeof(void *) * (pList->nAlloc - pList->nSize));
  }

  pList->pData[pList->nSize++] = pBlocks;
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
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  strcpy(pMeterMetaInfo->name, pDataBlock->meterId);

  /*
   * the submit message consists of : [RPC header|message body|digest]
   * the dataBlock only includes the RPC Header buffer and actual submit messsage body, space for digest needs
   * additional space.
   */
  int ret = tscAllocPayload(pCmd, pDataBlock->nAllocSize + sizeof(STaosDigest));
  if (TSDB_CODE_SUCCESS != ret) return ret;
  memcpy(pCmd->payload, pDataBlock->pData, pDataBlock->nAllocSize);

  /*
   * the payloadLen should be actual message body size
   * the old value of payloadLen is the allocated payload size
   */
  pCmd->payloadLen = pDataBlock->nAllocSize - tsRpcHeadSize;

  assert(pCmd->allocSize >= pCmd->payloadLen + tsRpcHeadSize + sizeof(STaosDigest));
  return tscGetMeterMeta(pSql, pMeterMetaInfo->name, 0);
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
  STableDataBlocks *dataBuf = tscCreateDataBlock(size);

  dataBuf->rowSize = rowSize;
  dataBuf->size = startOffset;
  dataBuf->tsSource = -1;

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
    dataBuf = tscCreateDataBlockEx((size_t) size, rowSize, startOffset, tableId);
    dataBuf = *(STableDataBlocks**)taosAddIntHash(pHashList, id, (char*)&dataBuf);
    tscAppendDataBlock(pDataBlockList, dataBuf);
  }

  return dataBuf;
}

int32_t tscMergeTableDataBlocks(SSqlObj* pSql, SDataBlockList* pTableDataBlockList) {
  SSqlCmd* pCmd = &pSql->cmd;

  void*           pVnodeDataBlockHashList = taosInitIntHash(8, POINTER_BYTES, taosHashInt);
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
      } else { // failed to allocate memory, free already allocated memory and return error code
        tscError("%p failed to allocate memory for merging submit block, size:%d", pSql, dataBuf->nAllocSize);

        taosCleanUpIntHash(pVnodeDataBlockHashList);
        tfree(dataBuf->pData);
        tscDestroyBlockArrayList(pVnodeDataBlockList);

        return TSDB_CODE_CLI_OUT_OF_MEMORY;
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

  return TSDB_CODE_SUCCESS;
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
  int32_t index = 0;
  SSQLToken t0 = tStrGetToken(sqlstr, &index, false, 0, NULL);
  return t0.type == TK_INSERT || t0.type == TK_IMPORT;
}

int tscAllocPayload(SSqlCmd* pCmd, int size) {
  assert(size > 0);

  if (pCmd->payload == NULL) {
    assert(pCmd->allocSize == 0);

    pCmd->payload = (char*)malloc(size);
    if (pCmd->payload == NULL) return TSDB_CODE_CLI_OUT_OF_MEMORY;
    pCmd->allocSize = size;
  } else {
    if (pCmd->allocSize < size) {
      char* b = realloc(pCmd->payload, size);      
      if (b == NULL) return TSDB_CODE_CLI_OUT_OF_MEMORY;
      pCmd->payload = b;
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

    pFieldInfo->pVisibleCols = realloc(pFieldInfo->pVisibleCols, newSize * sizeof(bool));

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
  pFieldInfo->pVisibleCols[index] = true;

  pFieldInfo->numOfOutputCols++;
}

void tscFieldInfoUpdateVisible(SFieldInfo* pFieldInfo, int32_t index, bool visible) {
  if (index < 0 || index > pFieldInfo->numOfOutputCols) {
    return;
  }

  bool oldVisible = pFieldInfo->pVisibleCols[index];
  pFieldInfo->pVisibleCols[index] = visible;

  if (oldVisible != visible) {
    if (!visible) {
      pFieldInfo->numOfHiddenCols += 1;
    } else {
      if (pFieldInfo->numOfHiddenCols > 0) {
        pFieldInfo->numOfHiddenCols -= 1;
      }
    }
  }
}

void tscFieldInfoSetValue(SFieldInfo* pFieldInfo, int32_t index, int8_t type, char* name, int16_t bytes) {
  ensureSpace(pFieldInfo, pFieldInfo->numOfOutputCols + 1);
  evic(pFieldInfo, index);

  TAOS_FIELD* pField = &pFieldInfo->pFields[index];
  setValueImpl(pField, type, name, bytes);

  pFieldInfo->pVisibleCols[index] = true;
  pFieldInfo->numOfOutputCols++;
}

void tscFieldInfoCalOffset(SSqlCmd* pCmd) {
  SFieldInfo* pFieldInfo = &pCmd->fieldsInfo;
  pFieldInfo->pOffset[0] = 0;

  for (int32_t i = 1; i < pFieldInfo->numOfOutputCols; ++i) {
    pFieldInfo->pOffset[i] = pFieldInfo->pOffset[i - 1] + pFieldInfo->pFields[i - 1].bytes;
  }
}

void tscFieldInfoUpdateOffset(SSqlCmd* pCmd) {
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

void tscFieldInfoCopy(SFieldInfo* src, SFieldInfo* dst, const int32_t* indexList, int32_t size) {
  if (src == NULL) {
    return;
  }

  if (size <= 0) {
    *dst = *src;
    tscFieldInfoCopyAll(src, dst);
  } else {  // only copy the required column
    for (int32_t i = 0; i < size; ++i) {
      assert(indexList[i] >= 0 && indexList[i] <= src->numOfOutputCols);
      tscFieldInfoSetValFromField(dst, i, &src->pFields[indexList[i]]);
    }
  }
}

void tscFieldInfoCopyAll(SFieldInfo* src, SFieldInfo* dst) {
  *dst = *src;

  dst->pFields = malloc(sizeof(TAOS_FIELD) * dst->numOfAlloc);
  dst->pOffset = malloc(sizeof(short) * dst->numOfAlloc);
  dst->pVisibleCols = malloc(sizeof(bool) * dst->numOfAlloc);

  memcpy(dst->pFields, src->pFields, sizeof(TAOS_FIELD) * dst->numOfOutputCols);
  memcpy(dst->pOffset, src->pOffset, sizeof(short) * dst->numOfOutputCols);
  memcpy(dst->pVisibleCols, src->pVisibleCols, sizeof(bool) * dst->numOfOutputCols);
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

void tscClearFieldInfo(SFieldInfo* pFieldInfo) {
  if (pFieldInfo == NULL) {
    return;
  }

  tfree(pFieldInfo->pOffset);
  tfree(pFieldInfo->pFields);
  tfree(pFieldInfo->pVisibleCols);

  memset(pFieldInfo, 0, sizeof(SFieldInfo));
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

SSqlExpr* tscSqlExprInsert(SSqlCmd* pCmd, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t interSize) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pColIndex->tableIndex);

  SSqlExprInfo* pExprInfo = &pCmd->exprsInfo;

  _exprCheckSpace(pExprInfo, pExprInfo->numOfExprs + 1);
  _exprEvic(pExprInfo, index);

  SSqlExpr* pExpr = &pExprInfo->pExprs[index];

  pExpr->functionId = functionId;
  int16_t numOfCols = pMeterMetaInfo->pMeterMeta->numOfColumns;

  // set the correct column index
  if (pColIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    pExpr->colInfo.colId = TSDB_TBNAME_COLUMN_INDEX;
  } else {
    SSchema* pSchema = tsGetColumnSchema(pMeterMetaInfo->pMeterMeta, pColIndex->columnIndex);
    pExpr->colInfo.colId = pSchema->colId;
  }

  // tag columns require the column index revised.
  if (pColIndex->columnIndex >= numOfCols) {
    pColIndex->columnIndex -= numOfCols;
    pExpr->colInfo.flag = TSDB_COL_TAG;
  } else {
    if (pColIndex->columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
      pExpr->colInfo.flag = TSDB_COL_NORMAL;
    } else {
      pExpr->colInfo.flag = TSDB_COL_TAG;
    }
  }

  pExpr->colInfo.colIdx = pColIndex->columnIndex;
  pExpr->resType = type;
  pExpr->resBytes = size;
  pExpr->interResBytes = interSize;
  pExpr->uid = pMeterMetaInfo->pMeterMeta->uid;

  pExprInfo->numOfExprs++;
  return pExpr;
}

SSqlExpr* tscSqlExprUpdate(SSqlCmd* pCmd, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  SSqlExprInfo*   pExprInfo = &pCmd->exprsInfo;
  if (index > pExprInfo->numOfExprs) {
    return NULL;
  }

  SSqlExpr* pExpr = &pExprInfo->pExprs[index];

  pExpr->functionId = functionId;

  pExpr->colInfo.colIdx = srcColumnIndex;
  pExpr->colInfo.colId = tsGetColumnSchema(pMeterMetaInfo->pMeterMeta, srcColumnIndex)->colId;

  pExpr->resType = type;
  pExpr->resBytes = size;

  return pExpr;
}

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes, int16_t tableIndex) {
  if (pExpr == NULL || argument == NULL || bytes == 0) {
    return;
  }

  // set parameter value
  // transfer to tVariant from byte data/no ascii data
  tVariantCreateFromBinary(&pExpr->param[pExpr->numOfParams], argument, bytes, type);

  pExpr->numOfParams += 1;
  assert(pExpr->numOfParams <= 3);
}

SSqlExpr* tscSqlExprGet(SSqlCmd* pCmd, int32_t index) {
  if (pCmd->exprsInfo.numOfExprs <= index) {
    return NULL;
  }

  return &pCmd->exprsInfo.pExprs[index];
}

void tscSqlExprCopy(SSqlExprInfo* dst, const SSqlExprInfo* src, uint64_t tableuid) {
  if (src == NULL) {
    return;
  }

  *dst = *src;

  dst->pExprs = malloc(sizeof(SSqlExpr) * dst->numOfAlloc);
  int16_t num = 0;
  for (int32_t i = 0; i < src->numOfExprs; ++i) {
    if (src->pExprs[i].uid == tableuid) {
      dst->pExprs[num++] = src->pExprs[i];
    }
  }

  dst->numOfExprs = num;
  for (int32_t i = 0; i < dst->numOfExprs; ++i) {
    for (int32_t j = 0; j < src->pExprs[i].numOfParams; ++j) {
      tVariantAssign(&dst->pExprs[i].param[j], &src->pExprs[i].param[j]);
    }
  }
}

static void clearVal(SColumnBase* pBase) {
  memset(pBase, 0, sizeof(SColumnBase));

  pBase->colIndex.tableIndex = -2;
  pBase->colIndex.columnIndex = -2;
}

static void _cf_ensureSpace(SColumnBaseInfo* pcolList, int32_t size) {
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

static void _cf_evic(SColumnBaseInfo* pcolList, int32_t index) {
  if (index < pcolList->numOfCols) {
    memmove(&pcolList->pColList[index + 1], &pcolList->pColList[index],
            sizeof(SColumnBase) * (pcolList->numOfCols - index));

    clearVal(&pcolList->pColList[index]);
  }
}

SColumnBase* tscColumnBaseInfoGet(SColumnBaseInfo* pColumnBaseInfo, int32_t index) {
  if (pColumnBaseInfo == NULL || pColumnBaseInfo->numOfCols < index) {
    return NULL;
  }

  return &pColumnBaseInfo->pColList[index];
}

void tscColumnBaseInfoUpdateTableIndex(SColumnBaseInfo* pColList, int16_t tableIndex) {
  for (int32_t i = 0; i < pColList->numOfCols; ++i) {
    pColList->pColList[i].colIndex.tableIndex = tableIndex;
  }
}

// todo refactor
SColumnBase* tscColumnBaseInfoInsert(SSqlCmd* pCmd, SColumnIndex* pColIndex) {
  SColumnBaseInfo* pcolList = &pCmd->colList;

  // ignore the tbname column to be inserted into source list
  if (pColIndex->columnIndex < 0) {
    return NULL;
  }

  int16_t col = pColIndex->columnIndex;

  int32_t i = 0;
  while (i < pcolList->numOfCols) {
    if (pcolList->pColList[i].colIndex.columnIndex < col) {
      i++;
    } else if (pcolList->pColList[i].colIndex.tableIndex < pColIndex->tableIndex) {
      i++;
    } else {
      break;
    }
  }

  SColumnIndex* pIndex = &pcolList->pColList[i].colIndex;
  if ((i < pcolList->numOfCols && (pIndex->columnIndex > col || pIndex->tableIndex != pColIndex->tableIndex)) ||
      (i >= pcolList->numOfCols)) {
    _cf_ensureSpace(pcolList, pcolList->numOfCols + 1);
    _cf_evic(pcolList, i);

    pcolList->pColList[i].colIndex = *pColIndex;
    pcolList->numOfCols++;
    pCmd->numOfCols++;
  }

  return &pcolList->pColList[i];
}

void tscColumnFilterInfoCopy(SColumnFilterInfo* dst, const SColumnFilterInfo* src) {
  assert (src != NULL && dst != NULL);

  assert(src->filterOnBinary == 0 || src->filterOnBinary == 1);
  if (src->lowerRelOptr == TSDB_RELATION_INVALID && src->upperRelOptr == TSDB_RELATION_INVALID) {
    assert(0);
  }

  *dst = *src;
  if (dst->filterOnBinary) {
    size_t len = (size_t) dst->len + 1;
    dst->pz = calloc(1, len);
    memcpy((char*) dst->pz, (char*) src->pz, (size_t) len);
  }
}

void tscColumnBaseCopy(SColumnBase* dst, const SColumnBase* src) {
  assert (src != NULL && dst != NULL);

  *dst = *src;

  if (src->numOfFilters > 0) {
    dst->filterInfo = calloc(1, src->numOfFilters * sizeof(SColumnFilterInfo));

    for (int32_t j = 0; j < src->numOfFilters; ++j) {
      tscColumnFilterInfoCopy(&dst->filterInfo[j], &src->filterInfo[j]);
    }
  } else {
    assert(src->filterInfo == NULL);
  }
}

void tscColumnBaseInfoCopy(SColumnBaseInfo* dst, const SColumnBaseInfo* src, int16_t tableIndex) {
  if (src == NULL) {
    return;
  }

  *dst = *src;
  dst->pColList = calloc(1, sizeof(SColumnBase) * dst->numOfAlloc);

  int16_t num = 0;
  for (int32_t i = 0; i < src->numOfCols; ++i) {
    if (src->pColList[i].colIndex.tableIndex == tableIndex || tableIndex < 0) {
      dst->pColList[num] = src->pColList[i];

      if (dst->pColList[num].numOfFilters > 0) {
        dst->pColList[num].filterInfo = calloc(1, dst->pColList[num].numOfFilters * sizeof(SColumnFilterInfo));

        for (int32_t j = 0; j < dst->pColList[num].numOfFilters; ++j) {
          tscColumnFilterInfoCopy(&dst->pColList[num].filterInfo[j], &src->pColList[i].filterInfo[j]);
        }
      }

      num += 1;
    }
  }

  dst->numOfCols = num;
}

void tscColumnBaseInfoDestroy(SColumnBaseInfo* pColumnBaseInfo) {
  if (pColumnBaseInfo == NULL) {
    return;
  }

  assert(pColumnBaseInfo->numOfCols <= TSDB_MAX_COLUMNS);

  for (int32_t i = 0; i < pColumnBaseInfo->numOfCols; ++i) {
    SColumnBase *pColBase = &(pColumnBaseInfo->pColList[i]);

    if (pColBase->numOfFilters > 0) {
      for (int32_t j = 0; j < pColBase->numOfFilters; ++j) {
        assert(pColBase->filterInfo[j].filterOnBinary == 0 || pColBase->filterInfo[j].filterOnBinary == 1);

        if (pColBase->filterInfo[j].filterOnBinary) {
          tfree(pColBase->filterInfo[j].pz);
        }
      }
    }

    tfree(pColBase->filterInfo);
  }

  tfree(pColumnBaseInfo->pColList);
}


void tscColumnBaseInfoReserve(SColumnBaseInfo* pColumnBaseInfo, int32_t size) { _cf_ensureSpace(pColumnBaseInfo, size); }

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
      if (len == pToken->n){
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
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (pMeterMetaInfo->pMeterMeta == NULL) {
    return false;
  }

  if (colId == -1 && UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    return true;
  }

  SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);
  int32_t  numOfTotal = pMeterMetaInfo->pMeterMeta->numOfTags + pMeterMetaInfo->pMeterMeta->numOfColumns;

  for (int32_t i = 0; i < numOfTotal; ++i) {
    if (pSchema[i].colId == colId) {
      return true;
    }
  }

  return false;
}

void tscTagCondCopy(STagCond* dest, const STagCond* src) {
  memset(dest, 0, sizeof(STagCond));

  SStringCopy(&dest->tbnameCond.cond, &src->tbnameCond.cond);
  dest->tbnameCond.uid = src->tbnameCond.uid;

  memcpy(&dest->joinInfo, &src->joinInfo, sizeof(SJoinInfo));

  for (int32_t i = 0; i < src->numOfTagCond; ++i) {
    SStringCopy(&dest->cond[i].cond, &src->cond[i].cond);
    dest->cond[i].uid = src->cond[i].uid;
  }

  dest->relType = src->relType;
  dest->numOfTagCond = src->numOfTagCond;
}

void tscTagCondRelease(STagCond* pCond) {
  SStringFree(&pCond->tbnameCond.cond);

  for (int32_t i = 0; i < pCond->numOfTagCond; ++i) {
    SStringFree(&pCond->cond[i].cond);
  }

  memset(pCond, 0, sizeof(STagCond));
}

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SSqlCmd* pCmd) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  SSchema*        pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    pColInfo[i].functionId = pExpr->functionId;

    if (TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
      SSchema* pTagSchema = tsGetTagSchema(pMeterMetaInfo->pMeterMeta);
      int16_t  actualTagIndex = pMeterMetaInfo->tagColumnIndex[pExpr->colInfo.colIdx];

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

  // to denote the heart-beat timer close connection and free all allocated resources
  pHeatBeat->cmd.type = TSDB_QUERY_TYPE_FREE_RESOURCE;
}

bool tscShouldFreeHeatBeat(SSqlObj* pHb) {
  assert(pHb == pHb->signature);
  return pHb->cmd.type == TSDB_QUERY_TYPE_FREE_RESOURCE;
}

void tscCleanSqlCmd(SSqlCmd* pCmd) {
  tscFreeSqlCmdData(pCmd);

  assert(pCmd->pMeterInfo == NULL);

  uint32_t allocSize = pCmd->allocSize;
  char*    allocPtr = pCmd->payload;

  memset(pCmd, 0, sizeof(SSqlCmd));

  // restore values
  pCmd->allocSize = allocSize;
  pCmd->payload = allocPtr;
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

SMeterMetaInfo* tscGetMeterMetaInfo(SSqlCmd* pCmd, int32_t index) {
  if (pCmd == NULL || index >= pCmd->numOfTables || index < 0) {
    return NULL;
  }

  return pCmd->pMeterInfo[index];
}

SMeterMetaInfo* tscGetMeterMetaInfoByUid(SSqlCmd* pCmd, uint64_t uid, int32_t* index) {
  int32_t k = -1;
  for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
    if (pCmd->pMeterInfo[i]->pMeterMeta->uid == uid) {
      k = i;
      break;
    }
  }

  if (index != NULL) {
    *index = k;
  }

  return tscGetMeterMetaInfo(pCmd, k);
}

SMeterMetaInfo* tscAddMeterMetaInfo(SSqlCmd* pCmd, const char* name, SMeterMeta* pMeterMeta, SMetricMeta* pMetricMeta,
                                    int16_t numOfTags, int16_t* tags) {
  void* pAlloc = realloc(pCmd->pMeterInfo, (pCmd->numOfTables + 1) * POINTER_BYTES);
  if (pAlloc == NULL) {
    return NULL;
  }

  pCmd->pMeterInfo = pAlloc;
  pCmd->pMeterInfo[pCmd->numOfTables] = calloc(1, sizeof(SMeterMetaInfo));

  SMeterMetaInfo* pMeterMetaInfo = pCmd->pMeterInfo[pCmd->numOfTables];
  assert(pMeterMetaInfo != NULL);

  if (name != NULL) {
    assert(strlen(name) <= TSDB_METER_ID_LEN);
    strcpy(pMeterMetaInfo->name, name);
  }

  pMeterMetaInfo->pMeterMeta = pMeterMeta;
  pMeterMetaInfo->pMetricMeta = pMetricMeta;
  pMeterMetaInfo->numOfTags = numOfTags;

  if (tags != NULL) {
    memcpy(pMeterMetaInfo->tagColumnIndex, tags, sizeof(int16_t) * numOfTags);
  }

  pCmd->numOfTables += 1;

  return pMeterMetaInfo;
}

SMeterMetaInfo* tscAddEmptyMeterMetaInfo(SSqlCmd* pCmd) { return tscAddMeterMetaInfo(pCmd, NULL, NULL, NULL, 0, NULL); }

void tscRemoveMeterMetaInfo(SSqlCmd* pCmd, int32_t index, bool removeFromCache) {
  if (index < 0 || index >= pCmd->numOfTables) {
    return;
  }

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index);

  tscClearMeterMetaInfo(pMeterMetaInfo, removeFromCache);
  free(pMeterMetaInfo);

  int32_t after = pCmd->numOfTables - index - 1;
  if (after > 0) {
    memmove(&pCmd->pMeterInfo[index], &pCmd->pMeterInfo[index + 1], after * sizeof(void*));
  }

  pCmd->numOfTables -= 1;
}

void tscRemoveAllMeterMetaInfo(SSqlCmd* pCmd, bool removeFromCache) {
  int64_t addr = offsetof(SSqlObj, cmd);

  tscTrace("%p deref the metric/meter meta in cache, numOfTables:%d", ((char*)pCmd - addr), pCmd->numOfTables);

  while (pCmd->numOfTables > 0) {
    tscRemoveMeterMetaInfo(pCmd, pCmd->numOfTables - 1, removeFromCache);
  }

  tfree(pCmd->pMeterInfo);
}

void tscClearMeterMetaInfo(SMeterMetaInfo* pMeterMetaInfo, bool removeFromCache) {
  if (pMeterMetaInfo == NULL) {
    return;
  }

  taosRemoveDataFromCache(tscCacheHandle, (void**)&(pMeterMetaInfo->pMeterMeta), removeFromCache);
  taosRemoveDataFromCache(tscCacheHandle, (void**)&(pMeterMetaInfo->pMetricMeta), removeFromCache);
}

void tscResetForNextRetrieve(SSqlRes* pRes) {
  pRes->row = 0;
  pRes->numOfRows = 0;
}

SString SStringCreate(const char* str) {
  size_t len = strlen(str);

  SString dest = {.n = len, .alloc = len + 1};
  dest.z = calloc(1, dest.alloc);
  strcpy(dest.z, str);

  return dest;
}

void SStringCopy(SString* pDest, const SString* pSrc) {
  if (pSrc->n > 0) {
    pDest->n = pSrc->n;
    pDest->alloc = pDest->n + 1;  // one additional space for null terminate

    pDest->z = calloc(1, pDest->alloc);

    memcpy(pDest->z, pSrc->z, pDest->n);
  } else {
    memset(pDest, 0, sizeof(SString));
  }
}

void SStringFree(SString* pStr) {
  if (pStr->alloc > 0) {
    tfree(pStr->z);
    pStr->alloc = 0;
  }
}

void SStringShrink(SString* pStr) {
  if (pStr->alloc > (pStr->n + 1) && pStr->alloc > (pStr->n * 2)) {
    pStr->z = realloc(pStr->z, pStr->n + 1);
    assert(pStr->z != NULL);

    pStr->alloc = pStr->n + 1;
  }
}

int32_t SStringAlloc(SString* pStr, int32_t size) {
  if (pStr->alloc >= size) {
    return TSDB_CODE_SUCCESS;
  }

  size = ALIGN8(size);

  char* tmp = NULL;
  if (pStr->z != NULL) {
    tmp = realloc(pStr->z, size);
    memset(pStr->z + pStr->n, 0, size - pStr->n);
  } else {
    tmp = calloc(1, size);
  }

  if (tmp == NULL) {
#ifdef WINDOWS
    LPVOID lpMsgBuf;
    FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
      GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),  // Default language
      (LPTSTR)&lpMsgBuf, 0, NULL);
    tscTrace("failed to allocate memory, reason:%s", lpMsgBuf);
    LocalFree(lpMsgBuf);
#else
    char errmsg[256] = {0};
    strerror_r(errno, errmsg, tListLen(errmsg));
    tscTrace("failed to allocate memory, reason:%s", errmsg);
#endif
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pStr->z = tmp;
  pStr->alloc = size;

  return TSDB_CODE_SUCCESS;
}

#define MIN_ALLOC_SIZE 8

int32_t SStringEnsureRemain(SString* pStr, int32_t size) {
  if (pStr->alloc - pStr->n > size) {
    return TSDB_CODE_SUCCESS;
  }

  // remain space is insufficient, allocate more spaces
  int32_t inc = (size < MIN_ALLOC_SIZE) ? size : MIN_ALLOC_SIZE;
  if (inc < (pStr->alloc >> 1)) {
    inc = (pStr->alloc >> 1);
  }

  // get the new size
  int32_t newsize = pStr->alloc + inc;

  char* tmp = realloc(pStr->z, newsize);
  if (tmp == NULL) {

#ifdef WINDOWS
    LPVOID lpMsgBuf;
    FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
      GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),  // Default language
      (LPTSTR)&lpMsgBuf, 0, NULL);
    tscTrace("failed to allocate memory, reason:%s", lpMsgBuf);
    LocalFree(lpMsgBuf);
#else
    char errmsg[256] = {0};
    strerror_r(errno, errmsg, tListLen(errmsg));
    tscTrace("failed to allocate memory, reason:%s", errmsg);
#endif

    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  memset(tmp + pStr->n, 0, inc);
  pStr->z = tmp;

  return TSDB_CODE_SUCCESS;
}

SSqlObj* createSubqueryObj(SSqlObj* pSql, int32_t vnodeIndex, int16_t tableIndex, void (*fp)(), void* param,
                           SSqlObj* pPrevSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("%p new subquery failed, vnodeIdx:%d, tableIndex:%d", pSql, vnodeIndex, tableIndex);
    return NULL;
  }

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->sqlstr = strdup(pSql->sqlstr);
  if (pNew->sqlstr == NULL) {
    tscError("%p new subquery failed, vnodeIdx:%d, tableIndex:%d", pSql, vnodeIndex, tableIndex);

    free(pNew);
    return NULL;
  }

  memcpy(&pNew->cmd, pCmd, sizeof(SSqlCmd));

  pNew->cmd.command = TSDB_SQL_SELECT;
  pNew->cmd.payload = NULL;
  pNew->cmd.allocSize = 0;

  pNew->cmd.pMeterInfo = NULL;

  pNew->cmd.colList.pColList = NULL;
  pNew->cmd.colList.numOfAlloc = 0;
  pNew->cmd.colList.numOfCols = 0;

  pNew->cmd.numOfTables = 0;
  pNew->cmd.tsBuf = NULL;

  memset(&pNew->cmd.fieldsInfo, 0, sizeof(SFieldInfo));
  tscTagCondCopy(&pNew->cmd.tagCond, &pCmd->tagCond);

  if (tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE) != TSDB_CODE_SUCCESS) {
    tscError("%p new subquery failed, vnodeIdx:%d, tableIndex:%d", pSql, vnodeIndex, tableIndex);
    tscFreeSqlObj(pNew);
    return NULL;
  }

  tscColumnBaseInfoCopy(&pNew->cmd.colList, &pCmd->colList, (int16_t)tableIndex);

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, tableIndex);

  // set the correct query type
  if (pPrevSql != NULL) {
    pNew->cmd.type = pPrevSql->cmd.type;
  } else {
    pNew->cmd.type |= TSDB_QUERY_TYPE_SUBQUERY; // it must be the subquery
  }

  uint64_t uid = pMeterMetaInfo->pMeterMeta->uid;
  tscSqlExprCopy(&pNew->cmd.exprsInfo, &pCmd->exprsInfo, uid);

  int32_t numOfOutputCols = pNew->cmd.exprsInfo.numOfExprs;

  if (numOfOutputCols > 0) {
    int32_t* indexList = calloc(1, numOfOutputCols * sizeof(int32_t));
    for (int32_t i = 0, j = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
      if (pExpr->uid == uid) {
        indexList[j++] = i;
      }
    }

    tscFieldInfoCopy(&pCmd->fieldsInfo, &pNew->cmd.fieldsInfo, indexList, numOfOutputCols);
    free(indexList);

    tscFieldInfoUpdateOffset(&pNew->cmd);
  }

  pNew->fp = fp;

  pNew->param = param;
  pNew->cmd.vnodeIdx = vnodeIndex;
  SMeterMetaInfo* pMetermetaInfo = tscGetMeterMetaInfo(pCmd, tableIndex);

  char key[TSDB_MAX_TAGS_LEN + 1] = {0};
  tscGetMetricMetaCacheKey(pCmd, key, pMetermetaInfo->pMeterMeta->uid);

  char* name = pMeterMetaInfo->name;
  SMeterMetaInfo* pFinalInfo = NULL;

  if (pPrevSql == NULL) {
    SMeterMeta*  pMeterMeta = taosGetDataFromCache(tscCacheHandle, name);
    SMetricMeta* pMetricMeta = taosGetDataFromCache(tscCacheHandle, key);

    pFinalInfo = tscAddMeterMetaInfo(&pNew->cmd, name, pMeterMeta, pMetricMeta, pMeterMetaInfo->numOfTags,
                        pMeterMetaInfo->tagColumnIndex);
  } else {
    SMeterMetaInfo* pPrevInfo = tscGetMeterMetaInfo(&pPrevSql->cmd, 0);
    pFinalInfo = tscAddMeterMetaInfo(&pNew->cmd, name, pPrevInfo->pMeterMeta, pPrevInfo->pMetricMeta, pMeterMetaInfo->numOfTags,
                        pMeterMetaInfo->tagColumnIndex);

    pPrevInfo->pMeterMeta = NULL;
    pPrevInfo->pMetricMeta = NULL;
  }

  assert(pFinalInfo->pMeterMeta != NULL);
  if (UTIL_METER_IS_METRIC(pMetermetaInfo)) {
    assert(pFinalInfo->pMetricMeta != NULL);
  }

  tscTrace("%p new subquery %p, vnodeIdx:%d, tableIndex:%d, type:%d", pSql, pNew, vnodeIndex, tableIndex, pNew->cmd.type);
  return pNew;
}

void tscDoQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  void* fp = pSql->fp;

  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
  } else {
    if (pCmd->command == TSDB_SQL_SELECT) {
      tscAddIntoSqlList(pSql);
    }

    if (pCmd->isInsertFromFile == 1) {
      tscProcessMultiVnodesInsertForFile(pSql);
    } else {
      // pSql may be released in this function if it is a async insertion.
      tscProcessSql(pSql);
      if (NULL == fp) tscProcessMultiVnodesInsert(pSql);
    }
  }
}

int16_t tscGetJoinTagColIndexByUid(SSqlCmd* pCmd, uint64_t uid) {
  STagCond* pTagCond = &pCmd->tagCond;

  if (pTagCond->joinInfo.left.uid == uid) {
    return pTagCond->joinInfo.left.tagCol;
  } else {
    return pTagCond->joinInfo.right.tagCol;
  }
}

bool tscIsUpdateQuery(STscObj* pObj) {
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlCmd* pCmd = &pObj->pSql->cmd;
  return ((pCmd->command >= TSDB_SQL_INSERT && pCmd->command <= TSDB_SQL_DROP_DNODE) ||
      TSDB_SQL_USE_DB == pCmd->command) ? 1 : 0;

}
