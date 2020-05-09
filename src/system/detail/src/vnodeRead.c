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

#define _DEFAULT_SOURCE
#include "os.h"

#include "ihash.h"
#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "tscJoinProcess.h"
#include "tscompression.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"
#include "hash.h"
#include "hashutil.h"

int (*pQueryFunc[])(SMeterObj *, SQuery *) = {vnodeQueryFromCache, vnodeQueryFromFile};

int vnodeInterpolationSearchKey(char *pValue, int num, TSKEY key, int order) {
  int    firstPos, lastPos, midPos = -1;
  int    delta, numOfPoints;
  TSKEY *keyList;

  keyList = (TSKEY *)pValue;
  firstPos = 0;
  lastPos = num - 1;

  if (order == 0) {
    // from latest to oldest
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfPoints = lastPos - firstPos + 1;
      delta = keyList[lastPos] - keyList[firstPos];
      midPos = (key - keyList[firstPos]) / delta * numOfPoints + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // from oldest to latest
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num) return -1;
      }

      numOfPoints = lastPos - firstPos + 1;
      delta = keyList[lastPos] - keyList[firstPos];
      midPos = (key - keyList[firstPos]) / delta * numOfPoints + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

int vnodeBinarySearchKey(char *pValue, int num, TSKEY key, int order) {
  int    firstPos, lastPos, midPos = -1;
  int    numOfPoints;
  TSKEY *keyList;

  if (num <= 0) return -1;

  keyList = (TSKEY *)pValue;
  firstPos = 0;
  lastPos = num - 1;

  if (order == 0) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger than the key
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

int (*vnodeSearchKeyFunc[])(char *pValue, int num, TSKEY key, int order) = {vnodeBinarySearchKey,
                                                                            vnodeInterpolationSearchKey};

static SQInfo *vnodeAllocateQInfoCommon(SQueryMeterMsg *pQueryMsg, SMeterObj *pMeterObj, SSqlFunctionExpr *pExprs) {
  SQInfo *pQInfo = (SQInfo *)calloc(1, sizeof(SQInfo));
  if (pQInfo == NULL) {
    return NULL;
  }

  SQuery *pQuery = &(pQInfo->query);

  SColumnInfo *colList = pQueryMsg->colList;

  short numOfCols = pQueryMsg->numOfCols;
  short numOfOutputCols = pQueryMsg->numOfOutputCols;

  pQuery->numOfCols = numOfCols;
  pQuery->numOfOutputCols = numOfOutputCols;

  pQuery->limit.limit = pQueryMsg->limit;
  pQuery->limit.offset = pQueryMsg->offset;

  pQuery->order.order = pQueryMsg->order;
  pQuery->order.orderColId = pQueryMsg->orderColId;

  pQuery->colList = calloc(1, sizeof(SSingleColumnFilterInfo) * numOfCols);
  if (pQuery->colList == NULL) {
    goto _clean_memory;
  }

  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i].req[0] = 1;  // column required during mater scan of data blocks
    pQuery->colList[i].colIdxInBuf = i;

    pQuery->colList[i].data = colList[i];
    SColumnInfo *pColInfo = &pQuery->colList[i].data;

    pColInfo->filters = NULL;

    if (colList[i].numOfFilters > 0) {
      pColInfo->filters = calloc(1, colList[i].numOfFilters * sizeof(SColumnFilterInfo));

      for (int32_t j = 0; j < colList[i].numOfFilters; ++j) {
        tscColumnFilterInfoCopy(&pColInfo->filters[j], &colList[i].filters[j]);
      }
    } else {
      pQuery->colList[i].data.filters = NULL;
    }
  }

  vnodeUpdateQueryColumnIndex(pQuery, pMeterObj);

  for (int16_t col = 0; col < numOfOutputCols; ++col) {
    assert(pExprs[col].resBytes > 0);

    pQuery->rowSize += pExprs[col].resBytes;
    if (TSDB_COL_IS_TAG(pExprs[col].pBase.colInfo.flag)) {
      continue;
    }

    int16_t colId = pExprs[col].pBase.colInfo.colId;
    int16_t functId = pExprs[col].pBase.functionId;

    // build the projection of actual column data in buffer and the real column index
    for (int32_t k = 0; k < numOfCols; ++k) {
      if (pQuery->colList[k].data.colId == colId) {
        pExprs[col].pBase.colInfo.colIdxInBuf = (int16_t)k;
        pExprs[col].pBase.colInfo.colIdx = pQuery->colList[k].colIdx;

        if (((functId == TSDB_FUNC_FIRST_DST || functId == TSDB_FUNC_FIRST) && pQuery->order.order == TSQL_SO_DESC) ||
            ((functId == TSDB_FUNC_LAST_DST || functId == TSDB_FUNC_LAST) && pQuery->order.order == TSQL_SO_ASC)) {
          pQuery->colList[k].req[1] = 1;
        } else if (functId == TSDB_FUNC_STDDEV) {
          pQuery->colList[k].req[1] = 1;
        }
        break;
      }
    }
  }

  pQuery->pSelectExpr = pExprs;

  int32_t ret = vnodeCreateFilterInfo(pQInfo, pQuery);
  if (ret != TSDB_CODE_SUCCESS) {
    goto _clean_memory;
  }

  vnodeUpdateFilterColumnIndex(pQuery);
  pQuery->precision = vnodeList[pMeterObj->vnode].cfg.precision;

  return pQInfo;

_clean_memory:
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);
  tfree(pQInfo);

  return NULL;
}

static SQInfo *vnodeAllocateQInfoEx(SQueryMeterMsg *pQueryMsg, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *pExprs,
                                    SMeterObj *pMeterObj) {
  SQInfo *pQInfo = vnodeAllocateQInfoCommon(pQueryMsg, pMeterObj, pExprs);
  if (pQInfo == NULL) {
    tfree(pExprs);
    tfree(pGroupbyExpr);

    return NULL;
  }

  SQuery *pQuery = &(pQInfo->query);

  /* pQuery->sdata is the results output buffer. */
  pQuery->sdata = (SData **)calloc(pQuery->numOfOutputCols, sizeof(SData *));
  if (pQuery->sdata == NULL) {
    goto sign_clean_memory;
  }

  pQuery->pGroupbyExpr = pGroupbyExpr;
  pQuery->intervalTime = pQueryMsg->intervalTime;
  pQuery->slidingTime = pQueryMsg->slidingTime;
  pQuery->interpoType = pQueryMsg->interpoType;
  pQuery->slidingTimeUnit = pQueryMsg->slidingTimeUnit;

  pQInfo->query.pointsToRead = vnodeList[pMeterObj->vnode].cfg.rowsInFileBlock;

  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
    assert(pExprs[col].interResBytes >= pExprs[col].resBytes);

    // allocate additional memory for interResults that are usually larger then final results
    size_t size = (pQInfo->query.pointsToRead + 1) * pExprs[col].resBytes + pExprs[col].interResBytes + sizeof(SData);
    pQuery->sdata[col] = (SData *)calloc(1, size);
    if (pQuery->sdata[col] == NULL) {
      goto sign_clean_memory;
    }
  }

  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    pQuery->defaultVal = malloc(sizeof(int64_t) * pQuery->numOfOutputCols);
    if (pQuery->defaultVal == NULL) {
      goto sign_clean_memory;
    }

    // the first column is the timestamp
    memcpy(pQuery->defaultVal, (char *)pQueryMsg->defaultVal, pQuery->numOfOutputCols * sizeof(int64_t));
  }

  // to make sure third party won't overwrite this structure
  pQInfo->signature = (uint64_t)pQInfo;
  pQInfo->pObj = pMeterObj;
  pQuery->slot = -1;
  pQuery->pos = -1;
  pQuery->hfd = -1;
  pQuery->dfd = -1;
  pQuery->lfd = -1;

  dTrace("vid:%d sid:%d meterId:%s, QInfo is allocated:%p", pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId,
         pQInfo);

  return pQInfo;

sign_clean_memory:
  tfree(pQuery->defaultVal);

  if (pQuery->sdata != NULL) {
    for (int16_t col = 0; col < pQuery->numOfOutputCols; ++col) {
      tfree(pQuery->sdata[col]);
    }
  }

  tfree(pQuery->sdata);
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);

  tfree(pExprs);
  tfree(pGroupbyExpr);

  tfree(pQInfo);

  return NULL;
}

SQInfo *vnodeAllocateQInfo(SQueryMeterMsg *pQueryMsg, SMeterObj *pObj, SSqlFunctionExpr *pExprs) {
  SQInfo *pQInfo = vnodeAllocateQInfoCommon(pQueryMsg, pObj, pExprs);
  if (pQInfo == NULL) {
    tfree(pExprs);
    return NULL;
  }

  SQuery *pQuery = &(pQInfo->query);

  pQuery->sdata = (SData **)calloc(1, sizeof(SData *) * pQuery->numOfOutputCols);
  if (pQuery->sdata == NULL) {
    goto __clean_memory;
  }

  size_t  size = 0;
  int32_t numOfRows = vnodeList[pObj->vnode].cfg.rowsInFileBlock;
  for (int col = 0; col < pQuery->numOfOutputCols; ++col) {
    size = 2 * (numOfRows * pQuery->pSelectExpr[col].resBytes + sizeof(SData));
    pQuery->sdata[col] = (SData *)malloc(size);
    if (pQuery->sdata[col] == NULL) {
      goto __clean_memory;
    }
  }

  if (pQuery->colList[0].data.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    size = 2 * (numOfRows * TSDB_KEYSIZE + sizeof(SData));
    pQuery->tsData = (SData *)malloc(size);
    if (pQuery->tsData == NULL) {
      goto __clean_memory;
    }
  }

  // to make sure third party won't overwrite this structure
  pQInfo->signature = (uint64_t)pQInfo;
  pQInfo->pObj = pObj;
  pQuery->slot = -1;
  pQuery->hfd = -1;
  pQuery->dfd = -1;
  pQuery->lfd = -1;
  pQuery->pos = -1;
  pQuery->interpoType = TSDB_INTERPO_NONE;

  dTrace("vid:%d sid:%d meterId:%s, QInfo is allocated:%p", pObj->vnode, pObj->sid, pObj->meterId, pQInfo);
  return pQInfo;

__clean_memory:

  tfree(pQuery->tsData);
  if (pQuery->sdata != NULL) {
    for (int col = 0; col < pQuery->numOfOutputCols; ++col) {
      tfree(pQuery->sdata[col]);
    }
  }
  tfree(pQuery->sdata);
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);

  tfree(pExprs);

  tfree(pQInfo);

  return NULL;
}

void vnodeFreeQInfoInQueue(void *param) {
  SQInfo *pQInfo = (SQInfo *)param;

  if (!vnodeIsQInfoValid(pQInfo)) return;

  pQInfo->killed = 1;
  dTrace("QInfo:%p set kill flag to free QInfo", pQInfo);
  
  vnodeDecRefCount(pQInfo);
}

void vnodeFreeQInfo(void *param, bool decQueryRef) {
  SQInfo *pQInfo = (SQInfo *)param;
  if (!vnodeIsQInfoValid(param)) return;

  pQInfo->killed = 1;
  SMeterObj *pObj = pQInfo->pObj;
  dTrace("QInfo:%p start to free SQInfo", pQInfo);

  if (decQueryRef) {
    vnodeDecMeterRefcnt(pQInfo);
  }

  SQuery *pQuery = &(pQInfo->query);
  tclose(pQuery->hfd);
  tclose(pQuery->dfd);
  tclose(pQuery->lfd);

  vnodeFreeFields(pQuery);

  tfree(pQuery->pBlock);

  for (int col = 0; col < pQuery->numOfOutputCols; ++col) {
    tfree(pQuery->sdata[col]);
  }

  for (int col = 0; col < pQuery->numOfCols; ++col) {
    vnodeFreeColumnInfo(&pQuery->colList[col].data);
  }

  if (pQuery->colList[0].colIdx != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    tfree(pQuery->tsData);
  }

  sem_destroy(&(pQInfo->dataReady));
  vnodeQueryFreeQInfoEx(pQInfo);

  for (int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
    SSingleColumnFilterInfo *pColFilter = &pQuery->pFilterInfo[i];
    if (pColFilter->numOfFilters > 0) {
      tfree(pColFilter->pFilters);
    }
  }

  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);
  tfree(pQuery->sdata);

  if (pQuery->pSelectExpr != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlBinaryExprInfo *pBinExprInfo = &pQuery->pSelectExpr[i].binExprInfo;

      if (pBinExprInfo->numOfCols > 0) {
        tfree(pBinExprInfo->pReqColumns);
        tSQLBinaryExprDestroy(&pBinExprInfo->pBinExpr, NULL);
      }
    }

    tfree(pQuery->pSelectExpr);
  }

  if (pQuery->defaultVal != NULL) {
    tfree(pQuery->defaultVal);
  }

  tfree(pQuery->pGroupbyExpr);
  dTrace("QInfo:%p vid:%d sid:%d meterId:%s, QInfo is freed", pQInfo, pObj->vnode, pObj->sid, pObj->meterId);

  //destroy signature, in order to avoid the query process pass the object safety check
  memset(pQInfo, 0, sizeof(SQInfo));
  tfree(pQInfo);
}

bool vnodeIsQInfoValid(void *param) {
  SQInfo *pQInfo = (SQInfo *)param;
  if (pQInfo == NULL) {
    return false;
  }

  /*
   * pQInfo->signature may be changed by another thread, so we assign value of signature
   * into local variable, then compare by using local variable
   */
  uint64_t sig = pQInfo->signature;
  return (sig == (uint64_t)pQInfo);
}

void vnodeDecRefCount(void *param) {
  SQInfo *pQInfo = (SQInfo*) param;
  
  assert(vnodeIsQInfoValid(pQInfo));
  
  int32_t ref = atomic_sub_fetch_32(&pQInfo->refCount, 1);
  if (ref < 0) {
    return; // avoid two threads dec ref count
  }
  assert(ref >= 0);
  
  dTrace("QInfo:%p decrease obj refcount, %d", pQInfo, ref);
  if (ref == 0) {
    vnodeFreeQInfo(pQInfo, true);
  }
}

void vnodeAddRefCount(void *param) {
  SQInfo *pQInfo = (SQInfo*) param;
  
  assert(vnodeIsQInfoValid(pQInfo));
  
  int32_t ref = atomic_add_fetch_32(&pQInfo->refCount, 1);
  dTrace("QInfo:%p add refcount, %d", pQInfo, ref);
}

void vnodeQueryData(SSchedMsg *pMsg) {
  SQuery *pQuery;
  SQInfo *pQInfo;

  pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo->killed) {
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    vnodeDecRefCount(pQInfo);
    return;
  }

  pQuery = &(pQInfo->query);

  SMeterObj *pObj = pQInfo->pObj;

  dTrace("QInfo:%p vid:%d sid:%d id:%s, query thread is created, numOfQueries:%d, func:%s", pQInfo, pObj->vnode,
         pObj->sid, pObj->meterId, pObj->numOfQueries, __FUNCTION__);

  pQuery->pointsToRead = vnodeList[pObj->vnode].cfg.rowsInFileBlock;
  pQuery->pointsOffset = pQInfo->bufIndex * pQuery->pointsToRead;

  int64_t st = taosGetTimestampUs();

  while (1) {
    int64_t potentNumOfRes = pQInfo->pointsRead + pQuery->pointsToRead;
    /* limit the potential overflow data */
    if (pQuery->limit.limit > 0 && potentNumOfRes > pQuery->limit.limit) {
      pQuery->pointsToRead = pQuery->limit.limit - pQInfo->pointsRead;

      if (pQuery->pointsToRead == 0) {
        /* reach the limitation, abort */
        pQuery->pointsRead = 0;
        pQInfo->over = 1;
        break;
      }
    }

    pQInfo->code = (*pQInfo->fp)(pObj, pQuery);  // <0:error

    // has read at least one point
    if (pQuery->pointsRead > 0 || pQInfo->code < 0) break;

    if (pQuery->pointsRead == 0 && pQuery->over == 0) continue;

    if (pQInfo->changed) {
      pQInfo->over = 1;
      break;
    }

    // has read all data in file, check data in cache
    pQInfo->fp = pQueryFunc[pQuery->order.order ^ 1];
    pQInfo->changed = 1;

    pQuery->slot = -1;  // reset the handle
    pQuery->over = 0;

    dTrace("vid:%d sid:%d id:%s, query in other media, order:%d, skey:%" PRId64 " query:%p", pObj->vnode, pObj->sid,
           pObj->meterId, pQuery->order.order, pQuery->skey, pQuery);
  }

  pQInfo->pointsRead += pQuery->pointsRead;

  dTrace("vid:%d sid:%d id:%s, %d points returned, totalRead:%d totalReturn:%d last key:%" PRId64 ", query:%p", pObj->vnode,
         pObj->sid, pObj->meterId, pQuery->pointsRead, pQInfo->pointsRead, pQInfo->pointsReturned, pQuery->lastKey,
         pQuery);

  int64_t et = taosGetTimestampUs();
  pQInfo->useconds += et - st;

  // close FDs as soon as possible
  if (pQInfo->over) {
    dTrace("vid:%d sid:%d id:%s, query over, %d points are returned", pObj->vnode, pObj->sid, pObj->meterId,
           pQInfo->pointsRead);
    tclose(pQInfo->query.hfd);
    tclose(pQInfo->query.dfd);
    tclose(pQInfo->query.lfd);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}

void *vnodeQueryOnSingleTable(SMeterObj **pMetersObj, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *pSqlExprs,
                            SQueryMeterMsg *pQueryMsg, int32_t *code) {
  SQInfo *pQInfo;
  SQuery *pQuery;

  SMeterObj *pMeterObj = pMetersObj[0];
  bool       isProjQuery = vnodeIsProjectionQuery(pSqlExprs, pQueryMsg->numOfOutputCols);

  // todo pass the correct error code
  if (isProjQuery && pQueryMsg->tsLen == 0) {
    pQInfo = vnodeAllocateQInfo(pQueryMsg, pMeterObj, pSqlExprs);
  } else {
    pQInfo = vnodeAllocateQInfoEx(pQueryMsg, pGroupbyExpr, pSqlExprs, pMetersObj[0]);
  }

  if (pQInfo == NULL) {
    *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    goto _error;
  }

  pQuery = &(pQInfo->query);
  dTrace("qmsg:%p create QInfo:%p, QInfo created", pQueryMsg, pQInfo);

  SMeterSidExtInfo** pSids = (SMeterSidExtInfo**)pQueryMsg->pSidExtInfo;
  if (pSids != NULL && pSids[0]->key > 0) {
    pQuery->skey = pSids[0]->key;
  } else {
    pQuery->skey = pQueryMsg->skey;
  }

  pQuery->ekey = pQueryMsg->ekey;
  pQuery->lastKey = pQuery->skey;

  pQInfo->fp = pQueryFunc[pQueryMsg->order];

  if (sem_init(&(pQInfo->dataReady), 0, 0) != 0) {
    dError("QInfo:%p vid:%d sid:%d meterId:%s, init dataReady sem failed, reason:%s", pQInfo, pMeterObj->vnode,
           pMeterObj->sid, pMeterObj->meterId, strerror(errno));
    *code = TSDB_CODE_APP_ERROR;
    goto _error;
  }

  SSchedMsg schedMsg = {0};

  if (isProjQuery && pQueryMsg->tsLen == 0) {
    schedMsg.fp = vnodeQueryData;
  } else {
    if (vnodeParametersSafetyCheck(pQuery) == false) {
      *code = TSDB_CODE_APP_ERROR;
      goto _error;
    }

    STableQuerySupportObj *pSupporter = (STableQuerySupportObj *)calloc(1, sizeof(STableQuerySupportObj));
    if (pSupporter == NULL) {
        *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        goto _error;
    }
    pSupporter->numOfMeters = 1;
    pSupporter->pSidSet = NULL;
    pSupporter->subgroupIdx = -1;
    pSupporter->pMeterSidExtInfo = NULL;

    pQInfo->pTableQuerySupporter = pSupporter;

    pSupporter->pMetersHashTable = taosInitHashTable(pSupporter->numOfMeters, taosIntHash_32, false);
    if (pSupporter->pMetersHashTable == NULL) {
        *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        goto _error;
    }
    if (taosAddToHashTable(pSupporter->pMetersHashTable, (const char*) &pMetersObj[0]->sid, sizeof(pMeterObj[0].sid),
                (char *)&pMetersObj[0], POINTER_BYTES) != 0) {
        *code = TSDB_CODE_APP_ERROR;
        goto _error;
    }

    STSBuf *pTSBuf = NULL;
    if (pQueryMsg->tsLen > 0) {
      // open new file to save the result
      char *tsBlock = (char *)pQueryMsg + pQueryMsg->tsOffset;
      pTSBuf = tsBufCreateFromCompBlocks(tsBlock, pQueryMsg->tsNumOfBlocks, pQueryMsg->tsLen, pQueryMsg->tsOrder);
      tsBufResetPos(pTSBuf);
      tsBufNextPos(pTSBuf);
    }

    if (((*code) = vnodeQueryTablePrepare(pQInfo, pQInfo->pObj, pSupporter, pTSBuf)) != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    if (pQInfo->over == 1) {
      vnodeAddRefCount(pQInfo);  // for retrieve procedure
      return pQInfo;
    }

    schedMsg.fp = vnodeSingleTableQuery;
  }

  /*
   * The reference count, which is 2, is for both the current query thread and the future retrieve request,
   * which will always be issued by client to acquire data or free SQInfo struct.
   */
  vnodeAddRefCount(pQInfo);
  vnodeAddRefCount(pQInfo);
  
  schedMsg.msg = NULL;
  schedMsg.thandle = (void *)1;
  schedMsg.ahandle = pQInfo;

  dTrace("QInfo:%p set query flag and prepare runtime environment completed, ref:%d, wait for schedule", pQInfo,
      pQInfo->refCount);
  
  taosScheduleTask(queryQhandle, &schedMsg);
  return pQInfo;

_error:
  // table query ref will be decrease during error handling
  vnodeFreeQInfo(pQInfo, false);
  return NULL;
}

/*
 * query on multi-meters
 */
void *vnodeQueryOnMultiMeters(SMeterObj **pMetersObj, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *pSqlExprs,
                              SQueryMeterMsg *pQueryMsg, int32_t *code) {
  SQInfo *pQInfo;
  SQuery *pQuery;

  assert(QUERY_IS_STABLE_QUERY(pQueryMsg->queryType) && pQueryMsg->numOfCols > 0 && pQueryMsg->pSidExtInfo != 0 &&
         pQueryMsg->numOfSids >= 1);

  pQInfo = vnodeAllocateQInfoEx(pQueryMsg, pGroupbyExpr, pSqlExprs, *pMetersObj);
  if (pQInfo == NULL) {
    *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    goto _error;
  }

  pQuery = &(pQInfo->query);
  dTrace("qmsg:%p create QInfo:%p, QInfo created", pQueryMsg, pQInfo);

  pQuery->skey = pQueryMsg->skey;
  pQuery->ekey = pQueryMsg->ekey;

  pQInfo->fp = pQueryFunc[pQueryMsg->order];

  if (sem_init(&(pQInfo->dataReady), 0, 0) != 0) {
    dError("QInfo:%p vid:%d sid:%d id:%s, init dataReady sem failed, reason:%s", pQInfo, pMetersObj[0]->vnode,
           pMetersObj[0]->sid, pMetersObj[0]->meterId, strerror(errno));
    *code = TSDB_CODE_APP_ERROR;
    goto _error;
  }

  SSchedMsg schedMsg = {0};

  STableQuerySupportObj *pSupporter = (STableQuerySupportObj *)calloc(1, sizeof(STableQuerySupportObj));
  pSupporter->numOfMeters = pQueryMsg->numOfSids;

  pSupporter->pMetersHashTable = taosInitHashTable(pSupporter->numOfMeters, taosIntHash_32, false);
  for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
    taosAddToHashTable(pSupporter->pMetersHashTable, (const char*) &pMetersObj[i]->sid, sizeof(pMetersObj[i]->sid), (char *)&pMetersObj[i],
                       POINTER_BYTES);
  }

  int32_t sidElemLen = pQueryMsg->tagLength + sizeof(SMeterSidExtInfo);

  int32_t size = POINTER_BYTES * pQueryMsg->numOfSids + sidElemLen * pQueryMsg->numOfSids;
  pSupporter->pMeterSidExtInfo = (SMeterSidExtInfo **)malloc(size);
  if (pSupporter->pMeterSidExtInfo == NULL) {
    *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    dError("QInfo:%p failed to allocate memory for meterSid info, size:%d, abort", pQInfo, size);
    goto _error;
  }

  char *px = ((char *)pSupporter->pMeterSidExtInfo) + POINTER_BYTES * pQueryMsg->numOfSids;

  for (int32_t i = 0; i < pQueryMsg->numOfSids; ++i) {
    SMeterSidExtInfo* pSrc = ((SMeterSidExtInfo **)pQueryMsg->pSidExtInfo)[i];
    SMeterSidExtInfo* pDst = (SMeterSidExtInfo *)px;

    pSupporter->pMeterSidExtInfo[i] = pDst;
    pDst->sid = pSrc->sid;
    pDst->uid = pSrc->uid;
    pDst->key = pSrc->key;

    if (pQueryMsg->tagLength > 0) {
      memcpy(pDst->tags, pSrc->tags, pQueryMsg->tagLength);
    }
    px += sidElemLen;
  }

  if (pGroupbyExpr != NULL && pGroupbyExpr->numOfGroupCols > 0) {
    pSupporter->pSidSet =
        tSidSetCreate(pSupporter->pMeterSidExtInfo, pQueryMsg->numOfSids, (SSchema *)pQueryMsg->pTagSchema,
                      pQueryMsg->numOfTagsCols, pGroupbyExpr->columnInfo, pGroupbyExpr->numOfGroupCols);
  } else {
    pSupporter->pSidSet = tSidSetCreate(pSupporter->pMeterSidExtInfo, pQueryMsg->numOfSids,
                                        (SSchema *)pQueryMsg->pTagSchema, pQueryMsg->numOfTagsCols, NULL, 0);
  }

  pQInfo->pTableQuerySupporter = pSupporter;

  STSBuf *pTSBuf = NULL;
  if (pQueryMsg->tsLen > 0) {
    // open new file to save the result
    char *tsBlock = (char *)pQueryMsg + pQueryMsg->tsOffset;
    pTSBuf = tsBufCreateFromCompBlocks(tsBlock, pQueryMsg->tsNumOfBlocks, pQueryMsg->tsLen, pQueryMsg->tsOrder);
    tsBufResetPos(pTSBuf);
  }

  if (((*code) = vnodeSTableQueryPrepare(pQInfo, pQuery, pTSBuf)) != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  vnodeAddRefCount(pQInfo);
  if (pQInfo->over == 1) {
    return pQInfo;
  }

  vnodeAddRefCount(pQInfo);
  
  schedMsg.msg = NULL;
  schedMsg.thandle = (void *)1;
  schedMsg.ahandle = pQInfo;
  schedMsg.fp = vnodeMultiMeterQuery;

  dTrace("QInfo:%p set query flag and prepare runtime environment completed, wait for schedule", pQInfo);

  taosScheduleTask(queryQhandle, &schedMsg);
  return pQInfo;

_error:
  // table query ref will be decrease during error handling
  vnodeFreeQInfo(pQInfo, false);
  return NULL;
}

/* engine provides the storage, the app has to save the data before next
   retrieve, *pNum is the number of points retrieved, and argv[] is
   the point to retrieved column
*/

int vnodeRetrieveQueryInfo(void *handle, int *numOfRows, int *rowSize, int16_t *timePrec) {
  SQInfo *pQInfo;
  SQuery *pQuery;

  *numOfRows = 0;
  *rowSize = 0;

  pQInfo = (SQInfo *)handle;
  if (pQInfo == NULL) {
    return TSDB_CODE_INVALID_QHANDLE;
  }

  pQuery = &(pQInfo->query);
  if (!vnodeIsQInfoValid(pQInfo) || (pQuery->sdata == NULL)) {
    dError("QInfo:%p %p retrieve memory is corrupted!!! QInfo:%p, sign:%p, sdata:%p", pQInfo, pQuery, pQInfo,
           pQInfo->signature, pQuery->sdata);
    return TSDB_CODE_INVALID_QHANDLE;
  }

  if (pQInfo->killed) {
    dTrace("QInfo:%p query is killed, %p, code:%d", pQInfo, pQuery, pQInfo->code);
    if (pQInfo->code == TSDB_CODE_SUCCESS) {
      return TSDB_CODE_QUERY_CANCELLED;
    } else { // in case of not TSDB_CODE_SUCCESS, return the code to client
      return abs(pQInfo->code);
    }
  }

  sem_wait(&pQInfo->dataReady);
  *numOfRows = pQInfo->pointsRead - pQInfo->pointsReturned;
  *rowSize = pQuery->rowSize;

  *timePrec = vnodeList[pQInfo->pObj->vnode].cfg.precision;
  
  dTrace("QInfo:%p, retrieve data info completed, precision:%d, rowsize:%d, rows:%d, code:%d", pQInfo, *timePrec,
      *rowSize, *numOfRows, pQInfo->code);
  
  if (pQInfo->code < 0) {  // less than 0 means there are error existed.
    return -pQInfo->code;
  }

  return TSDB_CODE_SUCCESS;
}

// vnodeRetrieveQueryInfo must be called first
int vnodeSaveQueryResult(void *handle, char *data, int32_t *size) {
  SQInfo *pQInfo = (SQInfo *)handle;

  // the remained number of retrieved rows, not the interpolated result
  int numOfRows = pQInfo->pointsRead - pQInfo->pointsReturned;

  int32_t numOfFinal = vnodeCopyQueryResultToMsg(pQInfo, data, numOfRows);
  pQInfo->pointsReturned += numOfFinal;

  dTrace("QInfo:%p %d are returned, totalReturned:%d totalRead:%d", pQInfo, numOfFinal, pQInfo->pointsReturned,
         pQInfo->pointsRead);

  if (pQInfo->over == 0) {
    #ifdef _TD_ARM_
      dTrace("QInfo:%p set query flag, sig:%" PRIu64 ", func:vnodeSaveQueryResult", pQInfo, pQInfo->signature);
    #else
      dTrace("QInfo:%p set query flag, sig:%" PRIu64 ", func:%s", pQInfo, pQInfo->signature, __FUNCTION__);
    #endif

    if (pQInfo->killed == 1) {
      dTrace("%p freed or killed, abort query", pQInfo);
    } else {
      vnodeAddRefCount(pQInfo);
      dTrace("%p add query into task queue for schedule", pQInfo);
      
      SSchedMsg schedMsg = {0};

      if (pQInfo->pTableQuerySupporter != NULL) {
        if (pQInfo->pTableQuerySupporter->pSidSet == NULL) {
          schedMsg.fp = vnodeSingleTableQuery;
        } else {  // group by tag
          schedMsg.fp = vnodeMultiMeterQuery;
        }
      } else {
        pQInfo->bufIndex = pQInfo->bufIndex ^ 1;  // exchange between 0 and 1
        schedMsg.fp = vnodeQueryData;
      }

      schedMsg.msg = NULL;
      schedMsg.thandle = (void *)1;
      schedMsg.ahandle = pQInfo;
      taosScheduleTask(queryQhandle, &schedMsg);
    }
  }

  return numOfFinal;
}

static int32_t validateQueryMeterMsg(SQueryMeterMsg *pQueryMsg) {
  if (pQueryMsg->intervalTime < 0) {
    dError("qmsg:%p illegal value of aggTimeInterval %" PRId64 "", pQueryMsg, pQueryMsg->intervalTime);
    return -1;
  }

  if (pQueryMsg->numOfTagsCols < 0 || pQueryMsg->numOfTagsCols > TSDB_MAX_TAGS + 1) {
    dError("qmsg:%p illegal value of numOfTagsCols %d", pQueryMsg, pQueryMsg->numOfTagsCols);
    return -1;
  }

  if (pQueryMsg->numOfCols <= 0 || pQueryMsg->numOfCols > TSDB_MAX_COLUMNS) {
    dError("qmsg:%p illegal value of numOfCols %d", pQueryMsg, pQueryMsg->numOfCols);
    return -1;
  }

  if (pQueryMsg->numOfSids <= 0) {
    dError("qmsg:%p illegal value of numOfSids %d", pQueryMsg, pQueryMsg->numOfSids);
    return -1;
  }

  if (pQueryMsg->numOfGroupCols < 0) {
    dError("qmsg:%p illegal value of numOfGroupbyCols %d", pQueryMsg, pQueryMsg->numOfGroupCols);
    return -1;
  }

  if (pQueryMsg->numOfOutputCols > TSDB_MAX_COLUMNS || pQueryMsg->numOfOutputCols <= 0) {
    dError("qmsg:%p illegal value of output columns %d", pQueryMsg, pQueryMsg->numOfOutputCols);
    return -1;
  }

  if (pQueryMsg->tagLength < 0) {
    dError("qmsg:%p illegal value of tag length %d", pQueryMsg, pQueryMsg->tagLength);
    return -1;
  }

  return 0;
}

int32_t vnodeConvertQueryMeterMsg(SQueryMeterMsg *pQueryMsg) {
  pQueryMsg->vnode = htons(pQueryMsg->vnode);
  pQueryMsg->numOfSids = htonl(pQueryMsg->numOfSids);

#ifdef TSKEY32
  pQueryMsg->skey = htonl(pQueryMsg->skey);
  pQueryMsg->ekey = htonl(pQueryMsg->ekey);
#else
  pQueryMsg->skey = htobe64(pQueryMsg->skey);
  pQueryMsg->ekey = htobe64(pQueryMsg->ekey);
#endif

  pQueryMsg->order = htons(pQueryMsg->order);
  pQueryMsg->orderColId = htons(pQueryMsg->orderColId);

  pQueryMsg->queryType = htons(pQueryMsg->queryType);

  pQueryMsg->intervalTime = htobe64(pQueryMsg->intervalTime);
  pQueryMsg->slidingTime = htobe64(pQueryMsg->slidingTime);
  
  pQueryMsg->numOfTagsCols = htons(pQueryMsg->numOfTagsCols);
  pQueryMsg->numOfCols = htons(pQueryMsg->numOfCols);
  pQueryMsg->numOfOutputCols = htons(pQueryMsg->numOfOutputCols);
  pQueryMsg->numOfGroupCols = htons(pQueryMsg->numOfGroupCols);
  pQueryMsg->tagLength = htons(pQueryMsg->tagLength);

  pQueryMsg->limit = htobe64(pQueryMsg->limit);
  pQueryMsg->offset = htobe64(pQueryMsg->offset);
  pQueryMsg->tsOffset = htonl(pQueryMsg->tsOffset);
  pQueryMsg->tsLen = htonl(pQueryMsg->tsLen);
  pQueryMsg->tsNumOfBlocks = htonl(pQueryMsg->tsNumOfBlocks);
  pQueryMsg->tsOrder = htonl(pQueryMsg->tsOrder);

  // query msg safety check
  if (validateQueryMeterMsg(pQueryMsg) != 0) {
    return TSDB_CODE_INVALID_QUERY_MSG;
  }

  SMeterSidExtInfo **pSids = NULL;
  char *             pMsg = (char *)(pQueryMsg->colList) + sizeof(SColumnInfo) * pQueryMsg->numOfCols;

  for (int32_t col = 0; col < pQueryMsg->numOfCols; ++col) {
    pQueryMsg->colList[col].colId = htons(pQueryMsg->colList[col].colId);
    pQueryMsg->colList[col].type = htons(pQueryMsg->colList[col].type);
    pQueryMsg->colList[col].bytes = htons(pQueryMsg->colList[col].bytes);
    pQueryMsg->colList[col].numOfFilters = htons(pQueryMsg->colList[col].numOfFilters);

    assert(pQueryMsg->colList[col].type >= TSDB_DATA_TYPE_BOOL && pQueryMsg->colList[col].type <= TSDB_DATA_TYPE_NCHAR);

    int32_t numOfFilters = pQueryMsg->colList[col].numOfFilters;

    if (numOfFilters > 0) {
      pQueryMsg->colList[col].filters = calloc(numOfFilters, sizeof(SColumnFilterInfo));
    }

    for (int32_t f = 0; f < numOfFilters; ++f) {
      SColumnFilterInfo *pFilterInfo = (SColumnFilterInfo *)pMsg;
      SColumnFilterInfo *pDestFilterInfo = &pQueryMsg->colList[col].filters[f];

      pDestFilterInfo->filterOnBinary = htons(pFilterInfo->filterOnBinary);

      pMsg += sizeof(SColumnFilterInfo);

      if (pDestFilterInfo->filterOnBinary) {
        pDestFilterInfo->len = htobe64(pFilterInfo->len);

        pDestFilterInfo->pz = (int64_t)calloc(1, pDestFilterInfo->len + 1);
        memcpy((void*)pDestFilterInfo->pz, pMsg, pDestFilterInfo->len + 1);
        pMsg += (pDestFilterInfo->len + 1);
      } else {
        pDestFilterInfo->lowerBndi = htobe64(pFilterInfo->lowerBndi);
        pDestFilterInfo->upperBndi = htobe64(pFilterInfo->upperBndi);
      }

      pDestFilterInfo->lowerRelOptr = htons(pFilterInfo->lowerRelOptr);
      pDestFilterInfo->upperRelOptr = htons(pFilterInfo->upperRelOptr);
    }
  }

  bool hasArithmeticFunction = false;

  /*
   * 1. simple projection query on meters, we only record the pSqlFuncExprs[i].colIdx value
   * 2. for complex queries, whole SqlExprs object is required.
   */
  pQueryMsg->pSqlFuncExprs = (int64_t)malloc(POINTER_BYTES * pQueryMsg->numOfOutputCols);
  SSqlFuncExprMsg *pExprMsg = (SSqlFuncExprMsg *)pMsg;

  for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
    ((SSqlFuncExprMsg **)pQueryMsg->pSqlFuncExprs)[i] = pExprMsg;

    pExprMsg->colInfo.colIdx = htons(pExprMsg->colInfo.colIdx);
    pExprMsg->colInfo.colId = htons(pExprMsg->colInfo.colId);
    pExprMsg->colInfo.flag = htons(pExprMsg->colInfo.flag);

    pExprMsg->functionId = htons(pExprMsg->functionId);
    pExprMsg->numOfParams = htons(pExprMsg->numOfParams);

    pMsg += sizeof(SSqlFuncExprMsg);

    for (int32_t j = 0; j < pExprMsg->numOfParams; ++j) {
      pExprMsg->arg[j].argType = htons(pExprMsg->arg[j].argType);
      pExprMsg->arg[j].argBytes = htons(pExprMsg->arg[j].argBytes);

      if (pExprMsg->arg[j].argType == TSDB_DATA_TYPE_BINARY) {
        pExprMsg->arg[j].argValue.pz = pMsg;
        pMsg += pExprMsg->arg[j].argBytes + 1;  // one more for the string terminated char.
      } else {
        pExprMsg->arg[j].argValue.i64 = htobe64(pExprMsg->arg[j].argValue.i64);
      }
    }

    if (pExprMsg->functionId == TSDB_FUNC_ARITHM) {
      hasArithmeticFunction = true;
    } else if (pExprMsg->functionId == TSDB_FUNC_TAG ||
        pExprMsg->functionId == TSDB_FUNC_TAGPRJ ||
        pExprMsg->functionId == TSDB_FUNC_TAG_DUMMY) {
      if (pExprMsg->colInfo.flag != TSDB_COL_TAG) { // ignore the column  index check for arithmetic expression.
        return TSDB_CODE_INVALID_QUERY_MSG;
      }
    } else {
      if (!vnodeValidateExprColumnInfo(pQueryMsg, pExprMsg)) {
        return TSDB_CODE_INVALID_QUERY_MSG;
      }
    }

    pExprMsg = (SSqlFuncExprMsg *)pMsg;
  }

  pQueryMsg->colNameLen = htonl(pQueryMsg->colNameLen);
  if (hasArithmeticFunction) {  // column name array
    assert(pQueryMsg->colNameLen > 0);
    pQueryMsg->colNameList = (int64_t)pMsg;
    pMsg += pQueryMsg->colNameLen;
  }

  pSids = (SMeterSidExtInfo **)calloc(pQueryMsg->numOfSids, sizeof(SMeterSidExtInfo *));
  pQueryMsg->pSidExtInfo = (uint64_t)pSids;

  pSids[0] = (SMeterSidExtInfo *)pMsg;
  pSids[0]->sid = htonl(pSids[0]->sid);
  pSids[0]->uid = htobe64(pSids[0]->uid);
  pSids[0]->key = htobe64(pSids[0]->key);
  
  for (int32_t j = 1; j < pQueryMsg->numOfSids; ++j) {
    pSids[j] = (SMeterSidExtInfo *)((char *)pSids[j - 1] + sizeof(SMeterSidExtInfo) + pQueryMsg->tagLength);
    pSids[j]->sid = htonl(pSids[j]->sid);
    pSids[j]->uid = htobe64(pSids[j]->uid);
    pSids[j]->key = htobe64(pSids[j]->key);
  }

  pMsg = (char *)pSids[pQueryMsg->numOfSids - 1];
  pMsg += sizeof(SMeterSidExtInfo) + pQueryMsg->tagLength;

  if (pQueryMsg->numOfGroupCols > 0 || pQueryMsg->numOfTagsCols > 0) {  // group by tag columns
    pQueryMsg->pTagSchema = (uint64_t)pMsg;
    SSchema *pTagSchema = (SSchema *)pQueryMsg->pTagSchema;
    pMsg += sizeof(SSchema) * pQueryMsg->numOfTagsCols;

    if (pQueryMsg->numOfGroupCols > 0) {
      pQueryMsg->groupbyTagIds = (uint64_t) & (pTagSchema[pQueryMsg->numOfTagsCols]);
    } else {
      pQueryMsg->groupbyTagIds = 0;
    }
    pQueryMsg->orderByIdx = htons(pQueryMsg->orderByIdx);
    pQueryMsg->orderType = htons(pQueryMsg->orderType);

    pMsg += sizeof(SColIndexEx) * pQueryMsg->numOfGroupCols;
  } else {
    pQueryMsg->pTagSchema = 0;
    pQueryMsg->groupbyTagIds = 0;
  }

  pQueryMsg->interpoType = htons(pQueryMsg->interpoType);
  if (pQueryMsg->interpoType != TSDB_INTERPO_NONE) {
    pQueryMsg->defaultVal = (uint64_t)(pMsg);

    int64_t *v = (int64_t *)pMsg;
    for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
      v[i] = htobe64(v[i]);
    }
  }

  dTrace("qmsg:%p query on %d meter(s), qrange:%" PRId64 "-%" PRId64 ", numOfGroupbyTagCols:%d, numOfTagCols:%d, timestamp order:%d, "
      "tags order:%d, tags order col:%d, numOfOutputCols:%d, numOfCols:%d, interval:%" PRId64 ", fillType:%d, comptslen:%d, limit:%" PRId64 ", "
      "offset:%" PRId64,
      pQueryMsg, pQueryMsg->numOfSids, pQueryMsg->skey, pQueryMsg->ekey, pQueryMsg->numOfGroupCols,
      pQueryMsg->numOfTagsCols, pQueryMsg->order, pQueryMsg->orderType, pQueryMsg->orderByIdx,
      pQueryMsg->numOfOutputCols, pQueryMsg->numOfCols, pQueryMsg->intervalTime, pQueryMsg->interpoType,
      pQueryMsg->tsLen, pQueryMsg->limit, pQueryMsg->offset);

  return 0;
}
