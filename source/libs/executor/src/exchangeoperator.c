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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

typedef struct SFetchRspHandleWrapper {
  uint32_t exchangeId;
  int32_t  sourceIndex;
  int64_t  seqId;
} SFetchRspHandleWrapper;

typedef struct SSourceDataInfo {
  int32_t            index;
  int64_t            seqId;
  SRWLatch           lock;
  SRetrieveTableRsp* pRsp;
  uint64_t           totalRows;
  int64_t            startTime;
  int32_t            code;
  EX_SOURCE_STATUS   status;
  const char*        taskId;
  SArray*            pSrcUidList;
  int32_t            srcOpType;
  bool               tableSeq;
  char*              decompBuf;
  int32_t            decompBufSize;
  SOrgTbInfo*        colMap;
  bool               isVtbRefScan;
  bool               isVtbTagScan;
  STimeWindow        window;
  bool               fetchSent; // need reset
} SSourceDataInfo;

static void destroyExchangeOperatorInfo(void* param);
static void freeBlock(void* pParam);
static void freeSourceDataInfo(void* param);
static void setAllSourcesCompleted(SOperatorInfo* pOperator);

static int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code);
static int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex);
static int32_t getCompletedSources(const SArray* pArray, int32_t* pRes);
static int32_t prepareConcurrentlyLoad(SOperatorInfo* pOperator);
static int32_t seqLoadRemoteData(SOperatorInfo* pOperator);
static int32_t prepareLoadRemoteData(SOperatorInfo* pOperator);
static int32_t handleLimitOffset(SOperatorInfo* pOperator, SLimitInfo* pLimitInfo, SSDataBlock* pBlock,
                                 bool holdDataInBuf);
static int32_t doExtractResultBlocks(SExchangeInfo* pExchangeInfo, SSourceDataInfo* pDataInfo);

static int32_t exchangeWait(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo);


static void streamConcurrentlyLoadRemoteData(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                           SExecTaskInfo* pTaskInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t startTs = taosGetTimestampUs();  
  int32_t  totalSources = (int32_t)taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int32_t completed = 0;
  code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &completed);
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (completed == totalSources) {
    qDebug("%s no load since all sources completed, completed:%d, totalSources:%d", pTaskInfo->id.str, completed, totalSources);
    setAllSourcesCompleted(pOperator);
    return;
  }

  SSourceDataInfo* pDataInfo = NULL;

  while (1) {
    if (pExchangeInfo->current < 0) {
      qDebug("current %d and all sources complted, totalSources:%d", pExchangeInfo->current, totalSources);
      setAllSourcesCompleted(pOperator);
      return;
    }
    
    if (pExchangeInfo->current >= totalSources) {
      completed = 0;
      code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &completed);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
      if (completed == totalSources) {
        qDebug("stop to load since all sources complted, completed:%d, totalSources:%d", completed, totalSources);
        setAllSourcesCompleted(pOperator);
        return;
      }
      
      pExchangeInfo->current = 0;
    }

    qDebug("%s start stream exchange %p idx:%d fetch", GET_TASKID(pTaskInfo), pExchangeInfo, pExchangeInfo->current);

    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    if (!pDataInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
      pExchangeInfo->current++;
      continue;
    }

    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;

    code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    while (true) {
      code = exchangeWait(pOperator, pExchangeInfo);
      if (code != TSDB_CODE_SUCCESS || isTaskKilled(pTaskInfo)) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }

      int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
      if (pDataInfo->seqId != currSeqId) {
        qDebug("%s seq rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", 
            GET_TASKID(pTaskInfo), pDataInfo->seqId, pExchangeInfo, currSeqId);
        taosMemoryFreeClear(pDataInfo->pRsp);
        continue;
      }

      break;
    }

    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);
    if (!pSource) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64 " execId:%d error happens, code:%s",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             tstrerror(pDataInfo->code));
      pTaskInfo->code = pDataInfo->code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;

    if (pRsp->numOfRows == 0) {
      qDebug("exhausted %p,%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64
             " execId:%d idx %d of total completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 " try next", pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pExchangeInfo->current + 1, pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (pDataInfo->isVtbRefScan || pDataInfo->isVtbTagScan) {
        pExchangeInfo->current = -1;
      } else {
        pExchangeInfo->current += 1;
      }
      taosMemoryFreeClear(pDataInfo->pRsp);
      continue;
    }

    code = doExtractResultBlocks(pExchangeInfo, pDataInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
    if (pRsp->completed == 1) {
      qDebug("exhausted %p,%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%d", pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pRetrieveRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize,
             pExchangeInfo->current + 1, totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (pDataInfo->isVtbRefScan) {
        pExchangeInfo->current = -1;
        taosMemoryFreeClear(pDataInfo->pRsp);
        continue;
      }
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d idx:%d numOfRows:%" PRId64
             ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pExchangeInfo->current, pRetrieveRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize);
    }

    updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, startTs, pOperator);
    pDataInfo->totalRows += pRetrieveRsp->numOfRows;

    pExchangeInfo->current++;

    taosMemoryFreeClear(pDataInfo->pRsp);
    return;
  }

_exit:

  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
}


static void concurrentlyLoadRemoteDataImpl(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                           SExecTaskInfo* pTaskInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int32_t completed = 0;
  code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &completed);
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (completed == totalSources) {
    setAllSourcesCompleted(pOperator);
    return;
  }

  SSourceDataInfo* pDataInfo = NULL;

  while (1) {
    qDebug("prepare wait for ready, %p, %s", pExchangeInfo, GET_TASKID(pTaskInfo));
    code = exchangeWait(pOperator, pExchangeInfo);

    if (code != TSDB_CODE_SUCCESS || isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    for (int32_t i = 0; i < totalSources; ++i) {
      pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
      QUERY_CHECK_NULL(pDataInfo, code, lino, _exit, terrno);
      if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
        continue;
      }

      if (pDataInfo->status != EX_SOURCE_DATA_READY) {
        continue;
      }

      int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
      if (pDataInfo->seqId != currSeqId) {
        qDebug("concurrent rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", pDataInfo->seqId, pExchangeInfo, currSeqId);
        taosMemoryFreeClear(pDataInfo->pRsp);
        break;
      }

      if (pDataInfo->code != TSDB_CODE_SUCCESS) {
        code = pDataInfo->code;
        TAOS_CHECK_EXIT(code);
      }

      tmemory_barrier();
      SRetrieveTableRsp*     pRsp = pDataInfo->pRsp;
      SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pDataInfo->index);
      QUERY_CHECK_NULL(pSource, code, lino, _exit, terrno);

      // todo
      SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
      if (pRsp->numOfRows == 0) {
        if (NULL != pDataInfo->pSrcUidList && (!pDataInfo->isVtbRefScan)) {
          pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
          code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pDataInfo->pRsp);
            TAOS_CHECK_EXIT(code);
          }
        } else {
          pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
          qDebug("exhausted %p,%s vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
                 " execId:%d index:%d completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", try next %d/%" PRIzu, pDataInfo,
                 GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId, i,
                 pDataInfo->totalRows, pExchangeInfo->loadInfo.totalRows, i + 1, totalSources);
          taosMemoryFreeClear(pDataInfo->pRsp);
        }
        break;
      }

      TAOS_CHECK_EXIT(doExtractResultBlocks(pExchangeInfo, pDataInfo));

      SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
      updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, pDataInfo->startTime, pOperator);
      pDataInfo->totalRows += pRetrieveRsp->numOfRows;

      if (pRsp->completed == 1) {
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
        qDebug("exhausted %p,%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
               " execId:%d index:%d completed, blocks:%d, numOfRows:%" PRId64 ", rowsOfSource:%" PRIu64
               ", totalRows:%" PRIu64 ", total:%.2f Kb, try next %d/%" PRIzu, pDataInfo,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId, i,
               pRsp->numOfBlocks, pRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows,
               pLoadInfo->totalSize / 1024.0, i + 1, totalSources);
      } else {
        qDebug("%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
               " execId:%d blocks:%d, numOfRows:%" PRId64 ", totalRows:%" PRIu64 ", total:%.2f Kb",
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
               pRsp->numOfBlocks, pRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0);
      }

      taosMemoryFreeClear(pDataInfo->pRsp);

      if ((pDataInfo->status != EX_SOURCE_DATA_EXHAUSTED || NULL != pDataInfo->pSrcUidList) && !pDataInfo->isVtbRefScan) {
        pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pDataInfo->pRsp);
          TAOS_CHECK_EXIT(code);
        }
      }
      
      return;
    }  // end loop

    int32_t complete1 = 0;
    code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &complete1);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (complete1 == totalSources) {
      qDebug("all sources are completed, %s", GET_TASKID(pTaskInfo));
      return;
    }
  }

_exit:

  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
}

static SSDataBlock* doLoadRemoteDataImpl(SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    qDebug("%s all %" PRIzu " source(s) are exhausted, total rows:%" PRIu64 " bytes:%" PRIu64 ", elapsed:%.2f ms",
           GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize,
           pLoadInfo->totalElapsed / 1000.0);
    return NULL;
  }

  // we have buffered retrieved datablock, return it directly
  SSDataBlock* p = NULL;
  if (taosArrayGetSize(pExchangeInfo->pResultBlockList) > 0) {
    p = taosArrayGetP(pExchangeInfo->pResultBlockList, 0);
    taosArrayRemove(pExchangeInfo->pResultBlockList, 0);
  }

  if (p != NULL) {
    void* tmp = taosArrayPush(pExchangeInfo->pRecycledBlocks, &p);
    if (!tmp) {
      code = terrno;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    return p;
  } else {
    if (pExchangeInfo->seqLoadData) {
      code = seqLoadRemoteData(pOperator);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    } else if (IS_STREAM_MODE(pOperator->pTaskInfo))   {
      streamConcurrentlyLoadRemoteData(pOperator, pExchangeInfo, pTaskInfo);
    } else {
      concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
    }
    if (TSDB_CODE_SUCCESS != pOperator->pTaskInfo->code) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(pOperator->pTaskInfo->code));
      T_LONG_JMP(pTaskInfo->env, pOperator->pTaskInfo->code);
    }
    
    if (taosArrayGetSize(pExchangeInfo->pResultBlockList) == 0) {
      qDebug("empty resultBlockList");
      return NULL;
    } else {
      p = taosArrayGetP(pExchangeInfo->pResultBlockList, 0);
      taosArrayRemove(pExchangeInfo->pResultBlockList, 0);
      void* tmp = taosArrayPush(pExchangeInfo->pRecycledBlocks, &p);
      if (!tmp) {
        code = terrno;
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }

      qDebug("block with rows:%" PRId64 " loaded", p->info.rows);
      return p;
    }
}
}

static int32_t loadRemoteDataNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  qDebug("%s start to load from exchange %p", pTaskInfo->id.str, pExchangeInfo);

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  while (1) {
    SSDataBlock* pBlock = doLoadRemoteDataImpl(pOperator);
    if (pBlock == NULL) {
      (*ppRes) = NULL;
      return code;
    }

    code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    if (blockDataGetNumOfRows(pBlock) == 0) {
      qDebug("rows 0 block got, continue next load");
      continue;
    }

    SLimitInfo* pLimitInfo = &pExchangeInfo->limitInfo;
    if (hasLimitOffsetInfo(pLimitInfo)) {
      int32_t status = handleLimitOffset(pOperator, pLimitInfo, pBlock, false);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        qDebug("limit retrieve continue");
        continue;
      } else if (status == PROJECT_RETRIEVE_DONE) {
        if (pBlock->info.rows == 0) {
          setOperatorCompleted(pOperator);
          (*ppRes) = NULL;
          return code;
        } else {
          (*ppRes) = pBlock;
          return code;
        }
      }
    } else {
      (*ppRes) = pBlock;
      qDebug("block with rows %" PRId64 " returned in exechange", pBlock->info.rows);
      return code;
    }
  }

_end:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  } else {
    qDebug("empty block returned in exchange");
  }
  
  (*ppRes) = NULL;
  return code;
}

static int32_t initDataSource(int32_t numOfSources, SExchangeInfo* pInfo, const char* id) {
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL) {
    return terrno;
  }

  if (pInfo->dynamicOp) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t len = strlen(id) + 1;
  pInfo->pTaskId = taosMemoryCalloc(1, len);
  if (!pInfo->pTaskId) {
    return terrno;
  }
  tstrncpy(pInfo->pTaskId, id, len);
  for (int32_t i = 0; i < numOfSources; ++i) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = pInfo->pTaskId;
    dataInfo.index = i;
    SSourceDataInfo* pDs = taosArrayPush(pInfo->pSourceDataInfo, &dataInfo);
    if (pDs == NULL) {
      taosArrayDestroyEx(pInfo->pSourceDataInfo, freeSourceDataInfo);
      return terrno;
    }
    qDebug("init source data info %d, pDs:%p, status:%d", i, pDs, pDs->status);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initExchangeOperator(SExchangePhysiNode* pExNode, SExchangeInfo* pInfo, const char* id) {
  size_t numOfSources = LIST_LENGTH(pExNode->pSrcEndPoints);

  if (numOfSources == 0) {
    qError("%s invalid number: %d of sources in exchange operator", id, (int32_t)numOfSources);
    return TSDB_CODE_INVALID_PARA;
  }
  pInfo->pFetchRpcHandles = taosArrayInit(numOfSources, sizeof(int64_t));
  if (!pInfo->pFetchRpcHandles) {
    return terrno;
  }
  void* ret = taosArrayReserve(pInfo->pFetchRpcHandles, numOfSources);
  if (!ret) {
    return terrno;
  }

  pInfo->pSources = taosArrayInit(numOfSources, sizeof(SDownstreamSourceNode));
  if (pInfo->pSources == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }

  if (pExNode->node.dynamicOp) {
    pInfo->pHashSources = tSimpleHashInit(numOfSources * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pInfo->pHashSources) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)nodesListGetNode((SNodeList*)pExNode->pSrcEndPoints, i);
    if (!pNode) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    void* tmp = taosArrayPush(pInfo->pSources, pNode);
    if (!tmp) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    SExchangeSrcIndex idx = {.srcIdx = i, .inUseIdx = -1};
    int32_t           code =
        tSimpleHashPut(pInfo->pHashSources, &pNode->addr.nodeId, sizeof(pNode->addr.nodeId), &idx, sizeof(idx));
    if (pInfo->pHashSources && code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return code;
    }
  }

  initLimitInfo(pExNode->node.pLimit, pExNode->node.pSlimit, &pInfo->limitInfo);
  int64_t refId = taosAddRef(exchangeObjRefPool, pInfo);
  if (refId < 0) {
    int32_t code = terrno;
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  } else {
    pInfo->self = refId;
  }

  return initDataSource(numOfSources, pInfo, id);
}

int32_t resetExchangeOperState(SOperatorInfo* pOper) {
  SExchangeInfo* pInfo = pOper->info;
  SExchangePhysiNode* pPhynode = (SExchangePhysiNode*)pOper->pPhyNode;

  qDebug("%s reset exchange op:%p info:%p", pOper->pTaskInfo->id.str, pOper, pInfo);

  atomic_add_fetch_64(&pInfo->seqId, 1);
  pOper->status = OP_NOT_OPENED;
  pInfo->current = 0;
  pInfo->loadInfo.totalElapsed = 0;
  pInfo->loadInfo.totalRows = 0;
  pInfo->loadInfo.totalSize = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSourceDataInfo); ++i) {
    SSourceDataInfo* pDataInfo = taosArrayGet(pInfo->pSourceDataInfo, i);
    taosWLockLatch(&pDataInfo->lock);
    taosMemoryFreeClear(pDataInfo->decompBuf);
    taosMemoryFreeClear(pDataInfo->pRsp);

    pDataInfo->totalRows = 0;
    pDataInfo->code = 0;
    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
    pDataInfo->fetchSent = false;
    taosWUnLockLatch(&pDataInfo->lock);
  }

  if (pInfo->dynamicOp) {
    taosArrayClearEx(pInfo->pSourceDataInfo, freeSourceDataInfo);
  } 

  taosArrayClearEx(pInfo->pResultBlockList, freeBlock);
  taosArrayClearEx(pInfo->pRecycledBlocks, freeBlock);

  blockDataCleanup(pInfo->pDummyBlock);

  void   *data = NULL;
  int32_t iter = 0;
  while ((data = tSimpleHashIterate(pInfo->pHashSources, data, &iter))) {
    ((SExchangeSrcIndex *)data)->inUseIdx = -1;
  }
  
  pInfo->limitInfo = (SLimitInfo){0};
  initLimitInfo(pPhynode->node.pLimit, pPhynode->node.pSlimit, &pInfo->limitInfo);

  return 0;
}

int32_t createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo,
                                   SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t        code = 0;
  int32_t        lino = 0;
  SExchangeInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pExNode;
  pInfo->dynamicOp = pExNode->node.dynamicOp;
  code = initExchangeOperator(pExNode, pInfo, GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _error);

  code = tsem_init(&pInfo->ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pDummyBlock = createDataBlockFromDescNode(pExNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pDummyBlock, code, lino, _error, terrno);

  pInfo->pResultBlockList = taosArrayInit(64, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pResultBlockList, code, lino, _error, terrno);
  pInfo->pRecycledBlocks = taosArrayInit(64, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pRecycledBlocks, code, lino, _error, terrno);

  SExchangeOpStopInfo stopInfo = {QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, pInfo->self};
  code = qAppendTaskStopInfo(pTaskInfo, &stopInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->seqLoadData = pExNode->seqRecvData;
  pInfo->dynTbname = pExNode->dynTbname;
  if (pInfo->dynTbname) {
    pInfo->seqLoadData = true;
  }
  pInfo->pTransporter = pTransporter;

  setOperatorInfo(pOperator, "ExchangeOperator", QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pDummyBlock->pDataBlock);

  code = filterInitFromNode((SNode*)pExNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);
  qTrace("%s exchange op:%p", __func__, pOperator);
  pOperator->fpSet = createOperatorFpSet(prepareLoadRemoteData, loadRemoteDataNext, NULL, destroyExchangeOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetExchangeOperState);
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  if (pInfo != NULL) {
    doDestroyExchangeOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

void destroyExchangeOperatorInfo(void* param) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  int32_t        code = taosRemoveRef(exchangeObjRefPool, pExInfo->self);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
}

void freeBlock(void* pParam) {
  SSDataBlock* pBlock = *(SSDataBlock**)pParam;
  blockDataDestroy(pBlock);
}

void freeSourceDataInfo(void* p) {
  SSourceDataInfo* pInfo = (SSourceDataInfo*)p;
  taosMemoryFreeClear(pInfo->decompBuf);
  taosMemoryFreeClear(pInfo->pRsp);

  pInfo->decompBufSize = 0;
}

void doDestroyExchangeOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  if (pExInfo->pFetchRpcHandles) {
    for (int32_t i = 0; i < pExInfo->pFetchRpcHandles->size; ++i) {
      int64_t* pRpcHandle = taosArrayGet(pExInfo->pFetchRpcHandles, i);
      if (*pRpcHandle > 0) {
        SDownstreamSourceNode* pSource = taosArrayGet(pExInfo->pSources, i);
        (void)asyncFreeConnById(pExInfo->pTransporter, *pRpcHandle);
      }
    }
    taosArrayDestroy(pExInfo->pFetchRpcHandles);
  }

  taosArrayDestroy(pExInfo->pSources);
  taosArrayDestroyEx(pExInfo->pSourceDataInfo, freeSourceDataInfo);

  taosArrayDestroyEx(pExInfo->pResultBlockList, freeBlock);
  taosArrayDestroyEx(pExInfo->pRecycledBlocks, freeBlock);

  blockDataDestroy(pExInfo->pDummyBlock);
  tSimpleHashCleanup(pExInfo->pHashSources);

  int32_t code = tsem_destroy(&pExInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  taosMemoryFreeClear(pExInfo->pTaskId);

  taosMemoryFreeClear(param);
}

int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SFetchRspHandleWrapper* pWrapper = (SFetchRspHandleWrapper*)param;

  taosMemoryFreeClear(pMsg->pEpSet);
  SExchangeInfo* pExchangeInfo = taosAcquireRef(exchangeObjRefPool, pWrapper->exchangeId);
  if (pExchangeInfo == NULL) {
    qWarn("failed to acquire exchange operator, since it may have been released, %p", pExchangeInfo);
    taosMemoryFree(pMsg->pData);
    return TSDB_CODE_SUCCESS;
  }

  int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
  if (pWrapper->seqId != currSeqId) {
    qDebug("rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", pWrapper->seqId, pExchangeInfo, currSeqId);
    taosMemoryFree(pMsg->pData);
    code = taosReleaseRef(exchangeObjRefPool, pWrapper->exchangeId);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t          index = pWrapper->sourceIndex;

  qDebug("%s exchange %p %dth source got rsp, code:%d, rsp:%p", pExchangeInfo->pTaskId, pExchangeInfo, index, code, pMsg->pData);

  int64_t* pRpcHandle = taosArrayGet(pExchangeInfo->pFetchRpcHandles, index);
  if (pRpcHandle != NULL) {
    int32_t ret = asyncFreeConnById(pExchangeInfo->pTransporter, *pRpcHandle);
    if (ret != 0) {
      qDebug("failed to free rpc handle, code:%s, %p", tstrerror(ret), pExchangeInfo);
    }
    *pRpcHandle = -1;
  }

  SSourceDataInfo* pSourceDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, index);
  if (!pSourceDataInfo) {
    return terrno;
  }

  if (0 == code && NULL == pMsg->pData) {
    qError("invalid rsp msg, msgType:%d, len:%d", pMsg->msgType, pMsg->len);
    code = TSDB_CODE_QRY_INVALID_MSG;
  }

  taosWLockLatch(&pSourceDataInfo->lock);
  if (code == TSDB_CODE_SUCCESS) {
    pSourceDataInfo->seqId = pWrapper->seqId;
    pSourceDataInfo->pRsp = pMsg->pData;

    SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
    pRsp->numOfRows = htobe64(pRsp->numOfRows);
    pRsp->compLen = htonl(pRsp->compLen);
    pRsp->payloadLen = htonl(pRsp->payloadLen);
    pRsp->numOfCols = htonl(pRsp->numOfCols);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->numOfBlocks = htonl(pRsp->numOfBlocks);

    qDebug("%s fetch rsp received, index:%d, blocks:%d, rows:%" PRId64 ", %p", pSourceDataInfo->taskId, index,
           pRsp->numOfBlocks, pRsp->numOfRows, pExchangeInfo);
  } else {
    taosMemoryFree(pMsg->pData);
    pSourceDataInfo->code = rpcCvtErrCode(code);
    if (pSourceDataInfo->code != code) {
      qError("%s fetch rsp received, index:%d, error:%s, cvted error: %s, %p", pSourceDataInfo->taskId, index,
             tstrerror(code), tstrerror(pSourceDataInfo->code), pExchangeInfo);
    } else {
      qError("%s fetch rsp received, index:%d, error:%s, %p", pSourceDataInfo->taskId, index, tstrerror(code),
             pExchangeInfo);
    }
  }

  tmemory_barrier();
  pSourceDataInfo->status = EX_SOURCE_DATA_READY;
  taosWUnLockLatch(&pSourceDataInfo->lock);
  
  code = tsem_post(&pExchangeInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to invoke post when fetch rsp is ready, code:%s, %p", tstrerror(code), pExchangeInfo);
    return code;
  }

  code = taosReleaseRef(exchangeObjRefPool, pWrapper->exchangeId);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  return code;
}

int32_t buildTableScanOperatorParam(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, bool tableSeq) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return terrno;
  }

  STableScanOperatorParam* pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
  if (NULL == pScan) {
    taosMemoryFreeClear(*ppRes);
    return terrno;
  }

  pScan->pUidList = taosArrayDup(pUidList, NULL);
  if (NULL == pScan->pUidList) {
    taosMemoryFree(pScan);
    taosMemoryFreeClear(*ppRes);
    return terrno;
  }
  pScan->tableSeq = tableSeq;
  pScan->pOrgTbInfo = NULL;
  pScan->window.skey = INT64_MAX;
  pScan->window.ekey = INT64_MIN;

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}

int32_t buildTableScanOperatorParamEx(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, SOrgTbInfo *pMap, bool tableSeq, STimeWindow *window) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  STableScanOperatorParam* pScan = NULL;

  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
  QUERY_CHECK_NULL(pScan, code, lino, _return, terrno);

  pScan->pUidList = taosArrayDup(pUidList, NULL);
  QUERY_CHECK_NULL(pScan->pUidList, code, lino, _return, terrno);

  pScan->pOrgTbInfo = taosMemoryMalloc(sizeof(SOrgTbInfo));
  QUERY_CHECK_NULL(pScan->pOrgTbInfo, code, lino, _return, terrno);

  pScan->pOrgTbInfo->vgId = pMap->vgId;
  tstrncpy(pScan->pOrgTbInfo->tbName, pMap->tbName, TSDB_TABLE_FNAME_LEN);

  pScan->pOrgTbInfo->colMap = taosArrayDup(pMap->colMap, NULL);
  QUERY_CHECK_NULL(pScan->pOrgTbInfo->colMap, code, lino, _return, terrno);

  pScan->tableSeq = tableSeq;
  pScan->window.skey = window->skey;
  pScan->window.ekey = window->ekey;

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at %d, failed to build scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  taosMemoryFreeClear(*ppRes);
  if (pScan) {
    taosArrayDestroy(pScan->pUidList);
    if (pScan->pOrgTbInfo) {
      taosArrayDestroy(pScan->pOrgTbInfo->colMap);
      taosMemoryFreeClear(pScan->pOrgTbInfo);
    }
    taosMemoryFree(pScan);
  }
  return code;
}

int32_t buildTagScanOperatorParam(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  STagScanOperatorParam*   pScan = NULL;

  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  pScan = taosMemoryMalloc(sizeof(STagScanOperatorParam));
  QUERY_CHECK_NULL(pScan, code, lino, _return, terrno);
  pScan->vcUid = *(tb_uid_t*)taosArrayGet(pUidList, 0);

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at %d, failed to build scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  taosMemoryFreeClear(*ppRes);
  if (pScan) {
    taosMemoryFree(pScan);
  }
  return code;
}

int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);
  if (!pDataInfo) {
    return terrno;
  }

  if (EX_SOURCE_DATA_NOT_READY != pDataInfo->status) {
    return TSDB_CODE_SUCCESS;
  }

  pDataInfo->status = EX_SOURCE_DATA_STARTED;
  SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pDataInfo->index);
  if (!pSource) {
    return terrno;
  }

  pDataInfo->startTime = taosGetTimestampUs();
  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SFetchRspHandleWrapper* pWrapper = taosMemoryCalloc(1, sizeof(SFetchRspHandleWrapper));
  QUERY_CHECK_NULL(pWrapper, code, lino, _end, terrno);
  pWrapper->exchangeId = pExchangeInfo->self;
  pWrapper->sourceIndex = sourceIndex;
  pWrapper->seqId = pExchangeInfo->seqId;

  if (pSource->localExec) {
    SDataBuf pBuf = {0};
    int32_t  code = (*pTaskInfo->localFetch.fp)(pTaskInfo->localFetch.handle, pSource->sId, pTaskInfo->id.queryId,
                                               pSource->clientId, pSource->taskId, 0, pSource->execId, &pBuf.pData,
                                               pTaskInfo->localFetch.explainRes);
    code = loadRemoteDataCallback(pWrapper, &pBuf, code);
    taosMemoryFree(pWrapper);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    SResFetchReq req = {0};
    req.header.vgId = pSource->addr.nodeId;
    req.sId = pSource->sId;
    req.clientId = pSource->clientId;
    req.taskId = pSource->taskId;
    req.queryId = pTaskInfo->id.queryId;
    req.execId = pSource->execId;
    if (pTaskInfo->pStreamRuntimeInfo) {
      req.dynTbname = pExchangeInfo->dynTbname;
      req.execId = pTaskInfo->pStreamRuntimeInfo->execId;
      if (pSource->fetchMsgType == TDMT_STREAM_FETCH_FROM_RUNNER)
        qDebug("doSendFetchDataRequest to execId:%d, %p", req.execId, pTaskInfo->pStreamRuntimeInfo);
      req.pStRtFuncInfo = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
      if (!pDataInfo->fetchSent) {
        req.reset = pDataInfo->fetchSent = true;
      }
    }
    if (pDataInfo->isVtbRefScan) {
      code = buildTableScanOperatorParamEx(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType, pDataInfo->colMap, pDataInfo->tableSeq, &pDataInfo->window);
      taosArrayDestroy(pDataInfo->colMap->colMap);
      taosMemoryFreeClear(pDataInfo->colMap);
      taosArrayDestroy(pDataInfo->pSrcUidList);
      pDataInfo->pSrcUidList = NULL;
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        taosMemoryFree(pWrapper);
        return pTaskInfo->code;
      }
    } else if (pDataInfo->isVtbTagScan) {
      code = buildTagScanOperatorParam(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType);
      taosArrayDestroy(pDataInfo->pSrcUidList);
      pDataInfo->pSrcUidList = NULL;
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        taosMemoryFree(pWrapper);
        return pTaskInfo->code;
      }
    } else {
      if (pDataInfo->pSrcUidList) {
        code = buildTableScanOperatorParam(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType, pDataInfo->tableSeq);
        taosArrayDestroy(pDataInfo->pSrcUidList);
        pDataInfo->pSrcUidList = NULL;
        if (TSDB_CODE_SUCCESS != code) {
          pTaskInfo->code = code;
          taosMemoryFree(pWrapper);
          return pTaskInfo->code;
        }
      }
    }

    int32_t msgSize = tSerializeSResFetchReq(NULL, 0, &req);
    if (msgSize < 0) {
      pTaskInfo->code = msgSize;
      taosMemoryFree(pWrapper);
      freeOperatorParam(req.pOpParam, OP_GET_PARAM);
      return pTaskInfo->code;
    }

    void* msg = taosMemoryCalloc(1, msgSize);
    if (NULL == msg) {
      pTaskInfo->code = terrno;
      taosMemoryFree(pWrapper);
      freeOperatorParam(req.pOpParam, OP_GET_PARAM);
      return pTaskInfo->code;
    }

    msgSize = tSerializeSResFetchReq(msg, msgSize, &req);
    if (msgSize < 0) {
      pTaskInfo->code = msgSize;
      taosMemoryFree(pWrapper);
      taosMemoryFree(msg);
      freeOperatorParam(req.pOpParam, OP_GET_PARAM);
      return pTaskInfo->code;
    }

    freeOperatorParam(req.pOpParam, OP_GET_PARAM);

    qDebug("%s build fetch msg and send to vgId:%d, ep:%s, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
           ", seqId:%" PRId64 ", execId:%d, %p, %d/%" PRIzu,
           GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->addr.epSet.eps[0].fqdn, pSource->clientId,
           pSource->taskId, pExchangeInfo->seqId, pSource->execId, pExchangeInfo, sourceIndex, totalSources);

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      taosMemoryFreeClear(msg);
      taosMemoryFree(pWrapper);
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = terrno;
      return pTaskInfo->code;
    }

    pMsgSendInfo->param = pWrapper;
    pMsgSendInfo->paramFreeFp = taosAutoMemoryFree;
    pMsgSendInfo->msgInfo.pData = msg;
    pMsgSendInfo->msgInfo.len = msgSize;
    pMsgSendInfo->msgType = pSource->fetchMsgType;
    pMsgSendInfo->fp = loadRemoteDataCallback;
    pMsgSendInfo->requestId = pTaskInfo->id.queryId;

    int64_t transporterId = 0;
    void* poolHandle = NULL;
    code = asyncSendMsgToServer(pExchangeInfo->pTransporter, &pSource->addr.epSet, &transporterId, pMsgSendInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    int64_t* pRpcHandle = taosArrayGet(pExchangeInfo->pFetchRpcHandles, sourceIndex);
    *pRpcHandle = transporterId;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void updateLoadRemoteInfo(SLoadRemoteDataInfo* pInfo, int64_t numOfRows, int32_t dataLen, int64_t startTs,
                          SOperatorInfo* pOperator) {
  pInfo->totalRows += numOfRows;
  pInfo->totalSize += dataLen;
  pInfo->totalElapsed += (taosGetTimestampUs() - startTs);
  pOperator->resultInfo.totalRows += numOfRows;
}

int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, char* pData, SArray* pColList, char** pNextStart) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;
  if (pColList == NULL) {  // data from other sources
    blockDataCleanup(pRes);
    code = blockDecode(pRes, pData, (const char**)pNextStart);
    if (code) {
      return code;
    }
  } else {  // extract data according to pColList
    char* pStart = pData;

    int32_t numOfCols = htonl(*(int32_t*)pStart);
    pStart += sizeof(int32_t);

    // todo refactor:extract method
    SSysTableSchema* pSchema = (SSysTableSchema*)pStart;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SSysTableSchema* p = (SSysTableSchema*)pStart;

      p->colId = htons(p->colId);
      p->bytes = htonl(p->bytes);
      pStart += sizeof(SSysTableSchema);
    }

    pBlock = NULL;
    code = createDataBlock(&pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData idata = createColumnInfoData(pSchema[i].type, pSchema[i].bytes, pSchema[i].colId);
      code = blockDataAppendColInfo(pBlock, &idata);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = blockDecode(pBlock, pStart, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataEnsureCapacity(pRes, pBlock->info.rows);
    QUERY_CHECK_CODE(code, lino, _end);

    // data from mnode
    pRes->info.dataLoad = 1;
    pRes->info.rows = pBlock->info.rows;
    pRes->info.scanFlag = MAIN_SCAN;
    code = relocateColumnData(pRes, pColList, pBlock->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataDestroy(pBlock);
    pBlock = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void setAllSourcesCompleted(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  size_t               totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  qDebug("%s all %" PRIzu " sources are exhausted, total rows: %" PRIu64 ", %.2f Kb, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0,
         pLoadInfo->totalElapsed / 1000.0);

  setOperatorCompleted(pOperator);
}

int32_t getCompletedSources(const SArray* pArray, int32_t* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  total = taosArrayGetSize(pArray);

  int32_t completed = 0;
  for (int32_t k = 0; k < total; ++k) {
    SSourceDataInfo* p = taosArrayGet(pArray, k);
    QUERY_CHECK_NULL(p, code, lino, _end, terrno);
    if (p->status == EX_SOURCE_DATA_EXHAUSTED) {
      qDebug("source %d is completed, info:%p %p", k, pArray, p);
      completed += 1;
    }
  }

  *pRes = completed;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t prepareConcurrentlyLoad(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int64_t startTs = taosGetTimestampUs();

  // Asynchronously send all fetch requests to all sources.
  for (int32_t i = 0; i < totalSources; ++i) {
    int32_t code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      return code;
    }
  }

  int64_t endTs = taosGetTimestampUs();
  qDebug("%s send all fetch requests to %" PRIzu " sources completed, elapsed:%.2fms", GET_TASKID(pTaskInfo),
         totalSources, (endTs - startTs) / 1000.0);

  pOperator->status = OP_RES_TO_RETURN;
  pOperator->cost.openCost = taosGetTimestampUs() - startTs;
  if (isTaskKilled(pTaskInfo)) {
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doExtractResultBlocks(SExchangeInfo* pExchangeInfo, SSourceDataInfo* pDataInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
  SSDataBlock*       pb = NULL;

  char* pNextStart = pRetrieveRsp->data;
  char* pStart = pNextStart;

  int32_t index = 0;

  if (pRetrieveRsp->compressed) {  // decompress the data
    if (pDataInfo->decompBuf == NULL) {
      pDataInfo->decompBuf = taosMemoryMalloc(pRetrieveRsp->payloadLen);
      QUERY_CHECK_NULL(pDataInfo->decompBuf, code, lino, _end, terrno);
      pDataInfo->decompBufSize = pRetrieveRsp->payloadLen;
    } else {
      if (pDataInfo->decompBufSize < pRetrieveRsp->payloadLen) {
        char* p = taosMemoryRealloc(pDataInfo->decompBuf, pRetrieveRsp->payloadLen);
        QUERY_CHECK_NULL(p, code, lino, _end, terrno);
        if (p != NULL) {
          pDataInfo->decompBuf = p;
          pDataInfo->decompBufSize = pRetrieveRsp->payloadLen;
        }
      }
    }
  }

  while (index++ < pRetrieveRsp->numOfBlocks) {
    pStart = pNextStart;

    if (taosArrayGetSize(pExchangeInfo->pRecycledBlocks) > 0) {
      pb = *(SSDataBlock**)taosArrayPop(pExchangeInfo->pRecycledBlocks);
      blockDataCleanup(pb);
    } else {
      code = createOneDataBlock(pExchangeInfo->pDummyBlock, false, &pb);
      QUERY_CHECK_NULL(pb, code, lino, _end, code);
    }

    int32_t compLen = *(int32_t*)pStart;
    pStart += sizeof(int32_t);

    int32_t rawLen = *(int32_t*)pStart;
    pStart += sizeof(int32_t);
    QUERY_CHECK_CONDITION((compLen <= rawLen && compLen != 0), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

    pNextStart = pStart + compLen;
    if (pRetrieveRsp->compressed && (compLen < rawLen)) {
      int32_t t = tsDecompressString(pStart, compLen, 1, pDataInfo->decompBuf, rawLen, ONE_STAGE_COMP, NULL, 0);
      QUERY_CHECK_CONDITION((t == rawLen), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      pStart = pDataInfo->decompBuf;
    }

    code = extractDataBlockFromFetchRsp(pb, pStart, NULL, &pStart);
    if (code != 0) {
      taosMemoryFreeClear(pDataInfo->pRsp);
      goto _end;
    }

    void* tmp = taosArrayPush(pExchangeInfo->pResultBlockList, &pb);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    qDebug("%dth block added to resultBlockList, rows:%" PRId64, index, pb->info.rows);
    pb = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pb);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t seqLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int32_t code = 0;
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  int32_t vgId = 0;
  if (pExchangeInfo->dynTbname) {
    SArray* vals = pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPartColVals;
    for (int32_t i = 0; i < taosArrayGetSize(vals); ++i) {
      SStreamGroupValue* pValue = taosArrayGet(vals, i);
      if (pValue != NULL && pValue->isTbname) {
        vgId = pValue->vgId;
        break;
      }
    }
  }

  while (1) {
    if (pExchangeInfo->current >= totalSources) {
      setAllSourcesCompleted(pOperator);
      return TSDB_CODE_SUCCESS;
    }

    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);
    if (!pSource) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (vgId != 0 && pSource->addr.nodeId != vgId){
      pExchangeInfo->current += 1;
      continue;
    }

    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    if (!pDataInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }
    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;

    code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    while (true) {
      code = exchangeWait(pOperator, pExchangeInfo);
      if (code != TSDB_CODE_SUCCESS || isTaskKilled(pTaskInfo)) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }

      int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
      if (pDataInfo->seqId != currSeqId) {
        qDebug("seq rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", pDataInfo->seqId, pExchangeInfo, currSeqId);
        taosMemoryFreeClear(pDataInfo->pRsp);
        continue;
      }

      break;
    }

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64 " execId:%d error happens, code:%s",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             tstrerror(pDataInfo->code));
      pOperator->pTaskInfo->code = pDataInfo->code;
      return pOperator->pTaskInfo->code;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;

    if (pRsp->numOfRows == 0) {
      qDebug("exhausted %p,%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64
             " execId:%d %d of total completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 " try next", pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pExchangeInfo->current + 1, pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (pDataInfo->isVtbRefScan || pDataInfo->isVtbTagScan) {
        pExchangeInfo->current = totalSources;
      } else {
        pExchangeInfo->current += 1;
      }
      taosMemoryFreeClear(pDataInfo->pRsp);
      continue;
    }

    code = doExtractResultBlocks(pExchangeInfo, pDataInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
    if (pRsp->completed == 1) {
      qDebug("exhausted %p,%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu, pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pRetrieveRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize,
             pExchangeInfo->current + 1, totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (pDataInfo->isVtbRefScan) {
        pExchangeInfo->current = totalSources;
      } else {
        pExchangeInfo->current += 1;
      }
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pRetrieveRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize);
    }
    if (pExchangeInfo->dynamicOp && pExchangeInfo->seqLoadData) {
      taosArrayClear(pExchangeInfo->pSourceDataInfo);
    }
    updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, startTs, pOperator);
    pDataInfo->totalRows += pRetrieveRsp->numOfRows;

    taosMemoryFreeClear(pDataInfo->pRsp);
    return TSDB_CODE_SUCCESS;
  }

_error:
  pTaskInfo->code = code;
  return code;
}

void clearVtbScanDataInfo(void* pItem) {
  SSourceDataInfo *pInfo = (SSourceDataInfo *)pItem;
  if (pInfo->colMap) {
    taosArrayDestroy(pInfo->colMap->colMap);
    taosMemoryFreeClear(pInfo->colMap);
  }
  taosArrayDestroy(pInfo->pSrcUidList);
}

int32_t addSingleExchangeSource(SOperatorInfo* pOperator, SExchangeOperatorBasicParam* pBasicParam) {
  SExchangeInfo*     pExchangeInfo = pOperator->info;
  SExchangeSrcIndex* pIdx = tSimpleHashGet(pExchangeInfo->pHashSources, &pBasicParam->vgId, sizeof(pBasicParam->vgId));
  if (NULL == pIdx) {
    qError("No exchange source for vgId: %d", pBasicParam->vgId);
    return TSDB_CODE_INVALID_PARA;
  }

  qDebug("start to add single exchange source");

  if (pBasicParam->isVtbRefScan) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = pExchangeInfo->pTaskId;
    dataInfo.index = pIdx->srcIdx;
    dataInfo.window = pBasicParam->window;
    dataInfo.colMap = taosMemoryMalloc(sizeof(SOrgTbInfo));
    dataInfo.colMap->vgId = pBasicParam->colMap->vgId;
    tstrncpy(dataInfo.colMap->tbName, pBasicParam->colMap->tbName, TSDB_TABLE_FNAME_LEN);
    dataInfo.colMap->colMap = taosArrayDup(pBasicParam->colMap->colMap, NULL);

    dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
    if (dataInfo.pSrcUidList == NULL) {
      return terrno;
    }

    dataInfo.isVtbRefScan = pBasicParam->isVtbRefScan;
    dataInfo.isVtbTagScan = pBasicParam->isVtbTagScan;
    dataInfo.srcOpType = pBasicParam->srcOpType;
    dataInfo.tableSeq = pBasicParam->tableSeq;

    taosArrayClearEx(pExchangeInfo->pSourceDataInfo, clearVtbScanDataInfo);
    void* tmp = taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo);
    if (!tmp) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
  } else if (pBasicParam->isVtbTagScan) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = pExchangeInfo->pTaskId;
    dataInfo.index = pIdx->srcIdx;
    dataInfo.window = pBasicParam->window;
    dataInfo.colMap = NULL;

    dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
    if (dataInfo.pSrcUidList == NULL) {
      return terrno;
    }

    dataInfo.isVtbRefScan = pBasicParam->isVtbRefScan;
    dataInfo.isVtbTagScan = pBasicParam->isVtbTagScan;
    dataInfo.srcOpType = pBasicParam->srcOpType;
    dataInfo.tableSeq = pBasicParam->tableSeq;

    taosArrayClearEx(pExchangeInfo->pSourceDataInfo, clearVtbScanDataInfo);
    void* tmp = taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo);
    if (!tmp) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
  } else {
    if (pIdx->inUseIdx < 0) {
      SSourceDataInfo dataInfo = {0};
      dataInfo.status = EX_SOURCE_DATA_NOT_READY;
      dataInfo.taskId = pExchangeInfo->pTaskId;
      dataInfo.index = pIdx->srcIdx;
      if (pBasicParam->isVtbRefScan) {
        dataInfo.window = pBasicParam->window;
        dataInfo.colMap = taosMemoryMalloc(sizeof(SOrgTbInfo));
        dataInfo.colMap->vgId = pBasicParam->colMap->vgId;
        tstrncpy(dataInfo.colMap->tbName, pBasicParam->colMap->tbName, TSDB_TABLE_FNAME_LEN);
        dataInfo.colMap->colMap = taosArrayDup(pBasicParam->colMap->colMap, NULL);
      }

      dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
      if (dataInfo.pSrcUidList == NULL) {
        return terrno;
      }

      dataInfo.isVtbRefScan = pBasicParam->isVtbRefScan;
      dataInfo.isVtbTagScan = pBasicParam->isVtbTagScan;
      dataInfo.srcOpType = pBasicParam->srcOpType;
      dataInfo.tableSeq = pBasicParam->tableSeq;

      void* tmp = taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo);
      if (!tmp) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
        return terrno;
      }
      pIdx->inUseIdx = taosArrayGetSize(pExchangeInfo->pSourceDataInfo) - 1;
    } else {
      SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pIdx->inUseIdx);
      if (!pDataInfo) {
        return terrno;
      }
      if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
        pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
      }

      if (pBasicParam->isVtbRefScan) {
        pDataInfo->window = pBasicParam->window;
        if (!pDataInfo->colMap) {
          pDataInfo->colMap = taosMemoryMalloc(sizeof(SOrgTbInfo));
        }
        pDataInfo->colMap->vgId = pBasicParam->colMap->vgId;
        tstrncpy(pDataInfo->colMap->tbName, pBasicParam->colMap->tbName, TSDB_TABLE_FNAME_LEN);
        pDataInfo->colMap->colMap = taosArrayDup(pBasicParam->colMap->colMap, NULL);
      }

      pDataInfo->pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
      if (pDataInfo->pSrcUidList == NULL) {
        return terrno;
      }

      pDataInfo->isVtbRefScan = pBasicParam->isVtbRefScan;
      pDataInfo->isVtbTagScan = pBasicParam->isVtbTagScan;
      pDataInfo->srcOpType = pBasicParam->srcOpType;
      pDataInfo->tableSeq = pBasicParam->tableSeq;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t addDynamicExchangeSource(SOperatorInfo* pOperator) {
  SExchangeInfo*               pExchangeInfo = pOperator->info;
  int32_t                      code = TSDB_CODE_SUCCESS;
  SExchangeOperatorBasicParam* pBasicParam = NULL;
  SExchangeOperatorParam*      pParam = (SExchangeOperatorParam*)pOperator->pOperatorGetParam->value;
  if (pParam->multiParams) {
    SExchangeOperatorBatchParam* pBatch = (SExchangeOperatorBatchParam*)pOperator->pOperatorGetParam->value;
    int32_t                      iter = 0;
    while (NULL != (pBasicParam = tSimpleHashIterate(pBatch->pBatchs, pBasicParam, &iter))) {
      code = addSingleExchangeSource(pOperator, pBasicParam);
      if (code) {
        return code;
      }
    }
  } else {
    pBasicParam = &pParam->basic;
    code = addSingleExchangeSource(pOperator, pBasicParam);
  }

  freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
  pOperator->pOperatorGetParam = NULL;

  return code;
}

int32_t prepareLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  
  if ((OPTR_IS_OPENED(pOperator) && !pExchangeInfo->dynamicOp) ||
      (pExchangeInfo->dynamicOp && NULL == pOperator->pOperatorGetParam)) {
    qDebug("skip prepare, opened:%d, dynamicOp:%d, getParam:%p", OPTR_IS_OPENED(pOperator), pExchangeInfo->dynamicOp, pOperator->pOperatorGetParam);
    return TSDB_CODE_SUCCESS;
  }

  if (pExchangeInfo->dynamicOp) {
    code = addDynamicExchangeSource(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pOperator->status == OP_NOT_OPENED && (pExchangeInfo->dynamicOp && pExchangeInfo->seqLoadData) || IS_STREAM_MODE(pOperator->pTaskInfo)) {
    pExchangeInfo->current = 0;
  }

  int64_t st = taosGetTimestampUs();

  if (!IS_STREAM_MODE(pOperator->pTaskInfo) && !pExchangeInfo->seqLoadData) {
    code = prepareConcurrentlyLoad(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
    pExchangeInfo->openedTs = taosGetTimestampUs();
  }

  OPTR_SET_OPENED(pOperator);
  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  qDebug("%s prepare load complete", pOperator->pTaskInfo->id.str);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t handleLimitOffset(SOperatorInfo* pOperator, SLimitInfo* pLimitInfo, SSDataBlock* pBlock, bool holdDataInBuf) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pLimitInfo->remainGroupOffset > 0) {
    if (pLimitInfo->currentGroupId == 0) {  // it is the first group
      pLimitInfo->currentGroupId = pBlock->info.id.groupId;
      blockDataCleanup(pBlock);
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
      // now it is the data from a new group
      pLimitInfo->remainGroupOffset -= 1;

      // ignore data block in current group
      if (pLimitInfo->remainGroupOffset > 0) {
        blockDataCleanup(pBlock);
        return PROJECT_RETRIEVE_CONTINUE;
      }
    }

    // set current group id of the project operator
    pLimitInfo->currentGroupId = pBlock->info.id.groupId;
  }

  // here check for a new group data, we need to handle the data of the previous group.
  if (pLimitInfo->currentGroupId != 0 && pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
    pLimitInfo->numOfOutputGroups += 1;
    if ((pLimitInfo->slimit.limit > 0) && (pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      pOperator->status = OP_EXEC_DONE;
      blockDataCleanup(pBlock);

      return PROJECT_RETRIEVE_DONE;
    }

    // reset the value for a new group data
    resetLimitInfoForNextGroup(pLimitInfo);
    // existing rows that belongs to previous group.
    if (pBlock->info.rows > 0) {
      return PROJECT_RETRIEVE_DONE;
    }
  }

  // here we reach the start position, according to the limit/offset requirements.

  // set current group id
  pLimitInfo->currentGroupId = pBlock->info.id.groupId;

  bool limitReached = applyLimitOffset(pLimitInfo, pBlock, pTaskInfo);
  if (pBlock->info.rows == 0) {
    return PROJECT_RETRIEVE_CONTINUE;
  } else {
    if (limitReached && (pLimitInfo->slimit.limit > 0 && pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      setOperatorCompleted(pOperator);
      return PROJECT_RETRIEVE_DONE;
    }
  }

  // todo optimize performance
  // If there are slimit/soffset value exists, multi-round result can not be packed into one group, since the
  // they may not belong to the same group the limit/offset value is not valid in this case.
  if ((!holdDataInBuf) || (pBlock->info.rows >= pOperator->resultInfo.threshold) || hasSlimitOffsetInfo(pLimitInfo)) {
    return PROJECT_RETRIEVE_DONE;
  } else {  // not full enough, continue to accumulate the output data in the buffer.
    return PROJECT_RETRIEVE_CONTINUE;
  }
}

static int32_t exchangeWait(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo) {
  SExecTaskInfo* pTask = pOperator->pTaskInfo;
  int32_t        code = TSDB_CODE_SUCCESS;
  if (pTask->pWorkerCb) {
    code = pTask->pWorkerCb->beforeBlocking(pTask->pWorkerCb->pPool);
    if (code != TSDB_CODE_SUCCESS) {
      pTask->code = code;
      return pTask->code;
    }
  }

  code = tsem_wait(&pExchangeInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTask->code = code;
    return pTask->code;
  }

  if (pTask->pWorkerCb) {
    code = pTask->pWorkerCb->afterRecoverFromBlocking(pTask->pWorkerCb->pPool);
    if (code != TSDB_CODE_SUCCESS) {
      pTask->code = code;
      return pTask->code;
    }
  }
  return TSDB_CODE_SUCCESS;
}
