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

#include <arpa/inet.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>

#include "ihash.h"
#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"
#pragma GCC diagnostic ignored "-Wint-conversion"

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

  SColumnFilterMsg *colList = pQueryMsg->colList;

  short numOfCols = pQueryMsg->numOfCols;
  short numOfOutputCols = pQueryMsg->numOfOutputCols;

  pQuery->numOfCols = numOfCols;
  pQuery->numOfOutputCols = numOfOutputCols;

  pQuery->limit.limit = pQueryMsg->limit;
  pQuery->limit.offset = pQueryMsg->offset;

  pQuery->order.order = pQueryMsg->order;
  pQuery->order.orderColId = pQueryMsg->orderColId;

  pQuery->colList = calloc(1, sizeof(SColumnFilter) * numOfCols);
  if (pQuery->colList == NULL) {
    goto _clean_memory;
  }

  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i].req[0] = 1;  // column required during mater scan of data blocks
    pQuery->colList[i].colIdxInBuf = i;
    pQuery->colList[i].data = colList[i];
    pQuery->dataRowSize += colList[i].bytes;
  }

  vnodeUpdateQueryColumnIndex(pQuery, pMeterObj);

  for (int16_t col = 0; col < numOfOutputCols; ++col) {
    assert(pExprs[col].resBytes > 0);

    pQuery->rowSize += pExprs[col].resBytes;
    if (pExprs[col].pBase.colInfo.isTag) {
      continue;
    }

    int16_t colId = pExprs[col].pBase.colInfo.colId;
    int16_t functId = pExprs[col].pBase.functionId;

    // build the projection of actual column data in buffer and the real column
    // index
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
  pQuery->nAggTimeInterval = pQueryMsg->nAggTimeInterval;
  pQuery->interpoType = pQueryMsg->interpoType;
  pQuery->intervalTimeUnit = pQueryMsg->intervalTimeUnit;

  pQInfo->query.pointsToRead = vnodeList[pMeterObj->vnode].cfg.rowsInFileBlock;

  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
    size_t size = (pQInfo->query.pointsToRead + 1) * pExprs[col].resBytes + sizeof(SData);
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

  pQuery->sdata = (SData **)malloc(sizeof(SData *) * pQuery->numOfOutputCols);
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

static void vnodeFreeQInfoInQueueImpl(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;
  vnodeFreeQInfo(pQInfo, true);
}

void vnodeFreeQInfoInQueue(void *param) {
  SQInfo *pQInfo = (SQInfo *)param;

  if (!vnodeIsQInfoValid(pQInfo)) return;

  pQInfo->killed = 1;

  dTrace("QInfo:%p set kill flag and add to queue, stop query ASAP", pQInfo);
  SSchedMsg schedMsg = {0};
  schedMsg.fp = vnodeFreeQInfoInQueueImpl;

  schedMsg.msg = NULL;
  schedMsg.thandle = (void *)1;
  schedMsg.ahandle = param;
  taosScheduleTask(queryQhandle, &schedMsg);
}

void vnodeFreeQInfo(void *param, bool decQueryRef) {
  SQInfo *pQInfo = (SQInfo *)param;
  if (!vnodeIsQInfoValid(param)) return;

  pQInfo->killed = 1;
  TSDB_WAIT_TO_SAFE_DROP_QINFO(pQInfo);

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
    if (pQuery->colList[col].data.filterOnBinary == 1 && pQuery->colList[col].data.filterOn) {
      tfree(pQuery->colList[col].data.pz);
      pQuery->colList[col].data.len = 0;
    }
  }

  if (pQuery->colList[0].colIdx != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    tfree(pQuery->tsData);
  }

  sem_destroy(&(pQInfo->dataReady));
  vnodeQueryFreeQInfoEx(pQInfo);

  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);
  tfree(pQuery->sdata);

  if (pQuery->pSelectExpr != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlBinaryExprInfo *pBinExprInfo = &pQuery->pSelectExpr[i].pBinExprInfo;

      if (pBinExprInfo->numOfCols > 0) {
        tfree(pBinExprInfo->pReqColumns);
        tSQLBinaryExprDestroy(&pBinExprInfo->pBinExpr);
      }
    }

    tfree(pQuery->pSelectExpr);
  }

  if (pQuery->defaultVal != NULL) {
    tfree(pQuery->defaultVal);
  }

  tfree(pQuery->pGroupbyExpr);

  dTrace("QInfo:%p vid:%d sid:%d meterId:%s, QInfo is freed", pQInfo, pObj->vnode, pObj->sid, pObj->meterId);

  /*
   * destory signature, in order to avoid the query process pass the object
   * safety check
   */
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
  return (sig == (uint64_t)pQInfo) || (sig == TSDB_QINFO_QUERY_FLAG);
}

void vnodeQueryData(SSchedMsg *pMsg) {
  SQuery *pQuery;
  SQInfo *pQInfo;

  pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo->killed) {
    TSDB_QINFO_RESET_SIG(pQInfo);
    dTrace("QInfo:%p it is already killed, reset signature and abort", pQInfo);
    return;
  }

  assert(pQInfo->signature == TSDB_QINFO_QUERY_FLAG);
  pQuery = &(pQInfo->query);

  SMeterObj *pObj = pQInfo->pObj;

  dTrace("QInfo:%p vid:%d sid:%d id:%s, query thread is created, numOfQueries:%d", pQInfo, pObj->vnode, pObj->sid,
         pObj->meterId, pObj->numOfQueries);

  pQuery->pointsToRead = vnodeList[pObj->vnode].cfg.rowsInFileBlock;
  pQuery->pointsOffset = pQInfo->bufIndex * pQuery->pointsToRead;

  // dTrace("id:%s, start to query data", pQInfo->pObj->meterId);
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

    dTrace("vid:%d sid:%d id:%s, query in other media, order:%d, skey:%lld query:%p",
           pObj->vnode, pObj->sid, pObj->meterId, pQuery->order.order, pQuery->skey, pQuery);
  }

  pQInfo->pointsRead += pQuery->pointsRead;

  dTrace("vid:%d sid:%d id:%s, %d points returned, totalRead:%d totalReturn:%d last key:%lld, query:%p",
         pObj->vnode, pObj->sid, pObj->meterId, pQuery->pointsRead, pQInfo->pointsRead, pQInfo->pointsReturned,
         pQuery->lastKey, pQuery);

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

  /* reset QInfo signature */
  dTrace("QInfo:%p reset signature", pQInfo);
  TSDB_QINFO_RESET_SIG(pQInfo);
  sem_post(&pQInfo->dataReady);
}

void *vnodeQueryInTimeRange(SMeterObj **pMetersObj, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *pSqlExprs,
                            SQueryMeterMsg *pQueryMsg, int32_t *code) {
  SQInfo *pQInfo;
  SQuery *pQuery;

  SMeterObj *pMeterObj = pMetersObj[0];
  bool       isProjQuery = vnodeIsProjectionQuery(pSqlExprs, pQueryMsg->numOfOutputCols);

  if (isProjQuery) {
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

  pQuery->skey = pQueryMsg->skey;
  pQuery->ekey = pQueryMsg->ekey;
  pQuery->lastKey = pQuery->skey;

  pQInfo->fp = pQueryFunc[pQueryMsg->order];
  pQInfo->num = pQueryMsg->num;

  if (sem_init(&(pQInfo->dataReady), 0, 0) != 0) {
    dError("QInfo:%p vid:%d sid:%d meterId:%s, init dataReady sem failed, reason:%s", pQInfo, pMeterObj->vnode,
           pMeterObj->sid, pMeterObj->meterId, strerror(errno));
    *code = TSDB_CODE_APP_ERROR;
    goto _error;
  }

  SSchedMsg schedMsg = {0};

  if (!isProjQuery) {
    if (vnodeParametersSafetyCheck(pQuery) == false) {
      *code = TSDB_CODE_APP_ERROR;
      goto _error;
    }

    SMeterQuerySupportObj *pSupporter = (SMeterQuerySupportObj *)calloc(1, sizeof(SMeterQuerySupportObj));
    pSupporter->numOfMeters = 1;

    pSupporter->pMeterObj = taosInitIntHash(pSupporter->numOfMeters, POINTER_BYTES, taosHashInt);
    taosAddIntHash(pSupporter->pMeterObj, pMetersObj[0]->sid, (char *)&pMetersObj[0]);

    pSupporter->pSidSet = NULL;
    pSupporter->subgroupIdx = -1;
    pSupporter->pMeterSidExtInfo = NULL;

    pQInfo->pMeterQuerySupporter = pSupporter;

    if (((*code) = vnodeQuerySingleMeterPrepare(pQInfo, pQInfo->pObj, pSupporter)) != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    if (pQInfo->over == 1) {
      return pQInfo;
    }

    schedMsg.fp = vnodeSingleMeterQuery;
  } else {
    schedMsg.fp = vnodeQueryData;
  }

  // set in query flag
  pQInfo->signature = TSDB_QINFO_QUERY_FLAG;

  schedMsg.msg = NULL;
  schedMsg.thandle = (void *)1;
  schedMsg.ahandle = pQInfo;

  dTrace("QInfo:%p set query flag and prepare runtime environment completed, wait for schedule", pQInfo);

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

  assert(pQueryMsg->metricQuery == 1 && pQueryMsg->numOfCols > 0 && pQueryMsg->pSidExtInfo != 0 &&
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
  pQInfo->num = pQueryMsg->num;

  if (sem_init(&(pQInfo->dataReady), 0, 0) != 0) {
    dError("QInfo:%p vid:%d sid:%d id:%s, init dataReady sem failed, reason:%s", pQInfo, pMetersObj[0]->vnode,
           pMetersObj[0]->sid, pMetersObj[0]->meterId, strerror(errno));
    *code = TSDB_CODE_APP_ERROR;
    goto _error;
  }

  SSchedMsg schedMsg = {0};

  SMeterQuerySupportObj *pSupporter = (SMeterQuerySupportObj *)calloc(1, sizeof(SMeterQuerySupportObj));
  pSupporter->numOfMeters = pQueryMsg->numOfSids;

  pSupporter->pMeterObj = taosInitIntHash(pSupporter->numOfMeters, POINTER_BYTES, taosHashInt);
  for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
    taosAddIntHash(pSupporter->pMeterObj, pMetersObj[i]->sid, (char *)&pMetersObj[i]);
  }

  pSupporter->pMeterSidExtInfo = (SMeterSidExtInfo **)pQueryMsg->pSidExtInfo;
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
    pSupporter->pMeterSidExtInfo[i] = (SMeterSidExtInfo *)px;
    pSupporter->pMeterSidExtInfo[i]->sid = ((SMeterSidExtInfo **)pQueryMsg->pSidExtInfo)[i]->sid;

    if (pQueryMsg->tagLength > 0) {
      memcpy(pSupporter->pMeterSidExtInfo[i]->tags, ((SMeterSidExtInfo **)pQueryMsg->pSidExtInfo)[i]->tags,
             pQueryMsg->tagLength);
    }
    px += sidElemLen;
  }

  if (pGroupbyExpr != NULL && pGroupbyExpr->numOfGroupbyCols > 0) {
    pSupporter->pSidSet =
        tSidSetCreate(pSupporter->pMeterSidExtInfo, pQueryMsg->numOfSids, (SSchema *)pQueryMsg->pTagSchema,
                      pQueryMsg->numOfTagsCols, pGroupbyExpr->tagIndex, pGroupbyExpr->numOfGroupbyCols);
  } else {
    pSupporter->pSidSet = tSidSetCreate(pSupporter->pMeterSidExtInfo, pQueryMsg->numOfSids,
                                        (SSchema *)pQueryMsg->pTagSchema, pQueryMsg->numOfTagsCols, NULL, 0);
  }

  pQInfo->pMeterQuerySupporter = pSupporter;

  if (((*code) = vnodeMultiMeterQueryPrepare(pQInfo, pQuery)) != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pQInfo->over == 1) {
    return pQInfo;
  }

  pQInfo->signature = TSDB_QINFO_QUERY_FLAG;

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
  if (pQInfo == NULL || pQInfo->killed) return -1;
  pQuery = &(pQInfo->query);

  if (!vnodeIsQInfoValid(pQInfo) || (pQuery->sdata == NULL)) {
    dError("%p retrieve memory is corrupted!!! QInfo:%p, sign:%p, sdata:%p", pQuery, pQInfo, pQInfo->signature,
           pQuery->sdata);
    return TSDB_CODE_APP_ERROR;
  }

  if (pQInfo->killed) {
    dTrace("%p it is already killed", pQuery);
    return TSDB_CODE_APP_ERROR;
  }

  sem_wait(&pQInfo->dataReady);
  *numOfRows = pQInfo->pointsRead - pQInfo->pointsReturned;
  *rowSize = pQuery->rowSize;

  *timePrec = vnodeList[pQInfo->pObj->vnode].cfg.precision;

  if (pQInfo->code < 0) return -pQInfo->code;

  return 0;
}

// vnodeRetrieveQueryInfo must be called first
int vnodeSaveQueryResult(void *handle, char *data) {
  SQInfo *pQInfo = (SQInfo *)handle;

  // the remained number of retrieved rows, not the interpolated result
  int numOfRows = pQInfo->pointsRead - pQInfo->pointsReturned;

  int32_t numOfFinal = vnodeCopyQueryResultToMsg(pQInfo, data, numOfRows);
  pQInfo->pointsReturned += numOfFinal;  //(pQInfo->pointsRead + pQInfo->pointsInterpo);

  dTrace("QInfo:%p %d are returned, totalReturned:%d totalRead:%d", pQInfo, numOfFinal, pQInfo->pointsReturned,
         pQInfo->pointsRead);

  if (pQInfo->over == 0) {
    dTrace("QInfo:%p set query flag, oldSig:%p, func:%s", pQInfo, pQInfo->signature, __FUNCTION__);
    uint64_t oldSignature = TSDB_QINFO_SET_QUERY_FLAG(pQInfo);

    /*
     * If SQInfo has been released, the value of signature cannot be equalled to
     * the address of pQInfo, since in release function, the original value has
     * been
     * destroyed. However, this memory area may be reused by another function.
     * It may be 0 or any value, but it is rarely still be equalled to the address
     * of SQInfo.
     */
    if (oldSignature == 0 || oldSignature != (uint64_t)pQInfo) {
      dTrace("%p freed or killed, old sig:%p abort query", pQInfo, oldSignature);
    } else {
      dTrace("%p add query into task queue for schedule", pQInfo);

      SSchedMsg schedMsg;

      if (pQInfo->pMeterQuerySupporter != NULL) {
        if (pQInfo->pMeterQuerySupporter->pSidSet == NULL) {
          schedMsg.fp = vnodeSingleMeterQuery;
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
  if (pQueryMsg->nAggTimeInterval < 0) {
    dError("qmsg:%p illegal value of aggTimeInterval %ld", pQueryMsg, pQueryMsg->nAggTimeInterval);
    return -1;
  }

  if (pQueryMsg->numOfTagsCols < 0 || pQueryMsg->numOfTagsCols > TSDB_MAX_TAGS) {
    dError("qmsg:%p illegal value of numOfTagsCols %ld", pQueryMsg, pQueryMsg->numOfTagsCols);
    return -1;
  }

  if (pQueryMsg->numOfCols <= 0 || pQueryMsg->numOfCols > TSDB_MAX_COLUMNS) {
    dError("qmsg:%p illegal value of numOfCols %ld", pQueryMsg, pQueryMsg->numOfCols);
    return -1;
  }

  if (pQueryMsg->numOfSids <= 0) {
    dError("qmsg:%p illegal value of numOfSids %ld", pQueryMsg, pQueryMsg->numOfSids);
    return -1;
  }

  if (pQueryMsg->numOfGroupbyCols < 0) {
    dError("qmsg:%p illegal value of numOfGroupbyCols %ld", pQueryMsg, pQueryMsg->numOfGroupbyCols);
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

  pQueryMsg->num = htonl(pQueryMsg->num);

  pQueryMsg->order = htons(pQueryMsg->order);
  pQueryMsg->orderColId = htons(pQueryMsg->orderColId);

  pQueryMsg->metricQuery = htons(pQueryMsg->metricQuery);

  pQueryMsg->nAggTimeInterval = htobe64(pQueryMsg->nAggTimeInterval);
  pQueryMsg->numOfTagsCols = htons(pQueryMsg->numOfTagsCols);
  pQueryMsg->numOfCols = htons(pQueryMsg->numOfCols);
  pQueryMsg->numOfOutputCols = htons(pQueryMsg->numOfOutputCols);
  pQueryMsg->numOfGroupbyCols = htons(pQueryMsg->numOfGroupbyCols);
  pQueryMsg->tagLength = htons(pQueryMsg->tagLength);

  pQueryMsg->limit = htobe64(pQueryMsg->limit);
  pQueryMsg->offset = htobe64(pQueryMsg->offset);

  // query msg safety check
  if (validateQueryMeterMsg(pQueryMsg) != 0) {
    return TSDB_CODE_INVALID_QUERY_MSG;
  }

  SMeterSidExtInfo **pSids = NULL;
  char *             strBuf = (char *)&pQueryMsg->colList[pQueryMsg->numOfCols];
  for (int32_t col = 0; col < pQueryMsg->numOfCols; ++col) {
    pQueryMsg->colList[col].colId = htons(pQueryMsg->colList[col].colId);

    pQueryMsg->colList[col].type = htons(pQueryMsg->colList[col].type);
    pQueryMsg->colList[col].bytes = htons(pQueryMsg->colList[col].bytes);

    assert(pQueryMsg->colList[col].type >= TSDB_DATA_TYPE_BOOL && pQueryMsg->colList[col].type <= TSDB_DATA_TYPE_NCHAR);

    pQueryMsg->colList[col].filterOn = htons(pQueryMsg->colList[col].filterOn);
    pQueryMsg->colList[col].filterOnBinary = htons(pQueryMsg->colList[col].filterOnBinary);

    if (pQueryMsg->colList[col].filterOn && pQueryMsg->colList[col].filterOnBinary) {
      pQueryMsg->colList[col].len = htobe64(pQueryMsg->colList[col].len);
      pQueryMsg->colList[col].pz = calloc(1, pQueryMsg->colList[col].len + 1);
      strcpy(pQueryMsg->colList[col].pz, strBuf);
      strBuf += pQueryMsg->colList[col].len + 1;
    } else {
      pQueryMsg->colList[col].lowerBndi = htobe64(pQueryMsg->colList[col].lowerBndi);
      pQueryMsg->colList[col].upperBndi = htobe64(pQueryMsg->colList[col].upperBndi);
    }

    pQueryMsg->colList[col].lowerRelOptr = htons(pQueryMsg->colList[col].lowerRelOptr);
    pQueryMsg->colList[col].upperRelOptr = htons(pQueryMsg->colList[col].upperRelOptr);
  }

  bool hasArithmeticFunction = false;

  /*
   * 1. simple projection query on meters, we only record the pSqlFuncExprs[i].colIdx value
   * 2. for complex queries, whole SqlExprs object is required.
   */
  pQueryMsg->pSqlFuncExprs = malloc(POINTER_BYTES * pQueryMsg->numOfOutputCols);

  char *           pMsg = strBuf;
  SSqlFuncExprMsg *pExprMsg = (SSqlFuncExprMsg *)pMsg;

  for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
    ((SSqlFuncExprMsg **)pQueryMsg->pSqlFuncExprs)[i] = pExprMsg;

    pExprMsg->colInfo.colIdx = htons(pExprMsg->colInfo.colIdx);
    pExprMsg->colInfo.colId = htons(pExprMsg->colInfo.colId);

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
    } else if (pExprMsg->functionId == TSDB_FUNC_TAG || pExprMsg->functionId == TSDB_FUNC_TAGPRJ) {
      // ignore the column  index check for arithmetic expression.
      if (!pExprMsg->colInfo.isTag) {
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
    pQueryMsg->colNameList = pMsg;
    pMsg += pQueryMsg->colNameLen;
  }

  pSids = (SMeterSidExtInfo **)calloc(pQueryMsg->numOfSids, sizeof(SMeterSidExtInfo *));
  pQueryMsg->pSidExtInfo = (uint64_t)pSids;

  pSids[0] = (SMeterSidExtInfo *)pMsg;
  pSids[0]->sid = htonl(pSids[0]->sid);

  for (int32_t j = 1; j < pQueryMsg->numOfSids; ++j) {
    pSids[j] = (SMeterSidExtInfo *)((char *)pSids[j - 1] + sizeof(SMeterSidExtInfo) + pQueryMsg->tagLength);
    pSids[j]->sid = htonl(pSids[j]->sid);
  }

  pMsg = (char *)pSids[pQueryMsg->numOfSids - 1];
  pMsg += sizeof(SMeterSidExtInfo) + pQueryMsg->tagLength;

  if (pQueryMsg->numOfGroupbyCols > 0 || pQueryMsg->numOfTagsCols > 0) {  // group by tag columns
    pQueryMsg->pTagSchema = (uint64_t)pMsg;
    SSchema *pTagSchema = (SSchema *)pQueryMsg->pTagSchema;
    pMsg += sizeof(SSchema) * pQueryMsg->numOfTagsCols;

    if (pQueryMsg->numOfGroupbyCols > 0) {
      pQueryMsg->groupbyTagIds = (uint64_t) & (pTagSchema[pQueryMsg->numOfTagsCols]);
    } else {
      pQueryMsg->groupbyTagIds = 0;
    }
    pQueryMsg->orderByIdx = htons(pQueryMsg->orderByIdx);
    pQueryMsg->orderType = htons(pQueryMsg->orderType);

    pMsg += sizeof(int16_t) * pQueryMsg->numOfGroupbyCols;
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

  dTrace("qmsg:%p query on %d meter(s), qrange:%lld-%lld, numOfGroupbyTagCols:%d, numOfTagCols:%d, timestamp order:%d, "
      "tags order:%d, tags order col:%d, numOfOutputCols:%d, numOfCols:%d, interval:%lld, fillType:%d",
      pQueryMsg, pQueryMsg->numOfSids, pQueryMsg->skey, pQueryMsg->ekey, pQueryMsg->numOfGroupbyCols,
      pQueryMsg->numOfTagsCols, pQueryMsg->order, pQueryMsg->orderType, pQueryMsg->orderByIdx,
      pQueryMsg->numOfOutputCols, pQueryMsg->numOfCols, pQueryMsg->nAggTimeInterval, pQueryMsg->interpoType);

  return 0;
}
