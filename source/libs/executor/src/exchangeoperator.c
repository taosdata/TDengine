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
} SFetchRspHandleWrapper;

typedef struct SSourceDataInfo {
  int32_t            index;
  SRetrieveTableRsp* pRsp;
  uint64_t           totalRows;
  int64_t            startTime;
  int32_t            code;
  EX_SOURCE_STATUS   status;
  const char*        taskId;
  SArray*            pSrcUidList;
  int32_t            srcOpType;
  bool               tableSeq;
} SSourceDataInfo;

static void  destroyExchangeOperatorInfo(void* param);
static void  freeBlock(void* pParam);
static void  freeSourceDataInfo(void* param);
static void* setAllSourcesCompleted(SOperatorInfo* pOperator);

static int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code);
static int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex);
static int32_t getCompletedSources(const SArray* pArray);
static int32_t prepareConcurrentlyLoad(SOperatorInfo* pOperator);
static int32_t seqLoadRemoteData(SOperatorInfo* pOperator);
static int32_t prepareLoadRemoteData(SOperatorInfo* pOperator);
static int32_t handleLimitOffset(SOperatorInfo* pOperator, SLimitInfo* pLimitInfo, SSDataBlock* pBlock,
                                 bool holdDataInBuf);
static int32_t doExtractResultBlocks(SExchangeInfo* pExchangeInfo, SSourceDataInfo* pDataInfo);

static void concurrentlyLoadRemoteDataImpl(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                           SExecTaskInfo* pTaskInfo) {
  int32_t code = 0;
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int32_t completed = getCompletedSources(pExchangeInfo->pSourceDataInfo);
  if (completed == totalSources) {
    setAllSourcesCompleted(pOperator);
    return;
  }

  SSourceDataInfo* pDataInfo = NULL;

  while (1) {
    qDebug("prepare wait for ready, %p, %s", pExchangeInfo, GET_TASKID(pTaskInfo));
    tsem_wait(&pExchangeInfo->ready);

    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    for (int32_t i = 0; i < totalSources; ++i) {
      pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
      if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
        continue;
      }

      if (pDataInfo->status != EX_SOURCE_DATA_READY) {
        continue;
      }

      if (pDataInfo->code != TSDB_CODE_SUCCESS) {
        code = pDataInfo->code;
        goto _error;
      }

      SRetrieveTableRsp*     pRsp = pDataInfo->pRsp;
      SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pDataInfo->index);

      // todo
      SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
      if (pRsp->numOfRows == 0) {
        if (NULL != pDataInfo->pSrcUidList) {
          pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
          code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFreeClear(pDataInfo->pRsp);
            goto _error;
          }
        } else {
          pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
          qDebug("%s vgId:%d, taskId:0x%" PRIx64 " execId:%d index:%d completed, rowsOfSource:%" PRIu64
                 ", totalRows:%" PRIu64 ", try next %d/%" PRIzu,
                 GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, i, pDataInfo->totalRows,
                 pExchangeInfo->loadInfo.totalRows, i + 1, totalSources);
          taosMemoryFreeClear(pDataInfo->pRsp);
        }
        break;
      }

      code = doExtractResultBlocks(pExchangeInfo, pDataInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
      updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, pDataInfo->startTime, pOperator);
      pDataInfo->totalRows += pRetrieveRsp->numOfRows;

      if (pRsp->completed == 1) {
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64
               " execId:%d index:%d completed, blocks:%d, numOfRows:%" PRId64 ", rowsOfSource:%" PRIu64
               ", totalRows:%" PRIu64 ", total:%.2f Kb, try next %d/%" PRIzu,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, i, pRsp->numOfBlocks,
               pRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0, i + 1,
               totalSources);
      } else {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " execId:%d blocks:%d, numOfRows:%" PRId64
               ", totalRows:%" PRIu64 ", total:%.2f Kb",
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pRsp->numOfBlocks,
               pRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0);
      }

      taosMemoryFreeClear(pDataInfo->pRsp);

      if (pDataInfo->status != EX_SOURCE_DATA_EXHAUSTED || NULL != pDataInfo->pSrcUidList) {
        pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pDataInfo->pRsp);
          goto _error;
        }
      }
      return;
    }  // end loop

    int32_t complete1 = getCompletedSources(pExchangeInfo->pSourceDataInfo);
    if (complete1 == totalSources) {
      qDebug("all sources are completed, %s", GET_TASKID(pTaskInfo));
      return;
    }
  }

_error:
  pTaskInfo->code = code;
}

static SSDataBlock* doLoadRemoteDataImpl(SOperatorInfo* pOperator) {
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
    taosArrayPush(pExchangeInfo->pRecycledBlocks, &p);
    return p;
  } else {
    if (pExchangeInfo->seqLoadData) {
      seqLoadRemoteData(pOperator);
    } else {
      concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
    }

    if (taosArrayGetSize(pExchangeInfo->pResultBlockList) == 0) {
      return NULL;
    } else {
      p = taosArrayGetP(pExchangeInfo->pResultBlockList, 0);
      taosArrayRemove(pExchangeInfo->pResultBlockList, 0);
      taosArrayPush(pExchangeInfo->pRecycledBlocks, &p);
      return p;
    }
  }
}

static SSDataBlock* loadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  while (1) {
    SSDataBlock* pBlock = doLoadRemoteDataImpl(pOperator);
    if (pBlock == NULL) {
      return NULL;
    }

    doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL);
    if (blockDataGetNumOfRows(pBlock) == 0) {
      continue;
    }

    SLimitInfo* pLimitInfo = &pExchangeInfo->limitInfo;
    if (hasLimitOffsetInfo(pLimitInfo)) {
      int32_t status = handleLimitOffset(pOperator, pLimitInfo, pBlock, false);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      } else if (status == PROJECT_RETRIEVE_DONE) {
        if (pBlock->info.rows == 0) {
          setOperatorCompleted(pOperator);
          return NULL;
        } else {
          return pBlock;
        }
      }
    } else {
      return pBlock;
    }
  }
}

static int32_t initDataSource(int32_t numOfSources, SExchangeInfo* pInfo, const char* id) {
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (pInfo->dynamicOp) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t len = strlen(id) + 1;
  pInfo->pTaskId = taosMemoryCalloc(1, len);
  strncpy(pInfo->pTaskId, id, len);
  for (int32_t i = 0; i < numOfSources; ++i) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = pInfo->pTaskId;
    dataInfo.index = i;
    SSourceDataInfo* pDs = taosArrayPush(pInfo->pSourceDataInfo, &dataInfo);
    if (pDs == NULL) {
      taosArrayDestroyEx(pInfo->pSourceDataInfo, freeSourceDataInfo);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initExchangeOperator(SExchangePhysiNode* pExNode, SExchangeInfo* pInfo, const char* id) {
  size_t numOfSources = LIST_LENGTH(pExNode->pSrcEndPoints);

  if (numOfSources == 0) {
    qError("%s invalid number: %d of sources in exchange operator", id, (int32_t)numOfSources);
    return TSDB_CODE_INVALID_PARA;
  }

  pInfo->pSources = taosArrayInit(numOfSources, sizeof(SDownstreamSourceNode));
  if (pInfo->pSources == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (pExNode->node.dynamicOp) {
    pInfo->pHashSources = tSimpleHashInit(numOfSources * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pInfo->pHashSources) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)nodesListGetNode((SNodeList*)pExNode->pSrcEndPoints, i);
    taosArrayPush(pInfo->pSources, pNode);
    SExchangeSrcIndex idx = {.srcIdx = i, .inUseIdx = -1};
    tSimpleHashPut(pInfo->pHashSources, &pNode->addr.nodeId, sizeof(pNode->addr.nodeId), &idx, sizeof(idx));
  }

  initLimitInfo(pExNode->node.pLimit, pExNode->node.pSlimit, &pInfo->limitInfo);
  pInfo->self = taosAddRef(exchangeObjRefPool, pInfo);

  return initDataSource(numOfSources, pInfo, id);
}

SOperatorInfo* createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo) {
  SExchangeInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->dynamicOp = pExNode->node.dynamicOp;
  int32_t code = initExchangeOperator(pExNode, pInfo, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  tsem_init(&pInfo->ready, 0, 0);
  pInfo->pDummyBlock = createDataBlockFromDescNode(pExNode->node.pOutputDataBlockDesc);
  pInfo->pResultBlockList = taosArrayInit(64, POINTER_BYTES);
  pInfo->pRecycledBlocks = taosArrayInit(64, POINTER_BYTES);

  SExchangeOpStopInfo stopInfo = {QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, pInfo->self};
  qAppendTaskStopInfo(pTaskInfo, &stopInfo);

  pInfo->seqLoadData = pExNode->seqRecvData;
  pInfo->pTransporter = pTransporter;

  setOperatorInfo(pOperator, "ExchangeOperator", QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pDummyBlock->pDataBlock);

  code = filterInitFromNode((SNode*)pExNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(prepareLoadRemoteData, loadRemoteData, NULL, destroyExchangeOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  return pOperator;

_error:
  if (pInfo != NULL) {
    doDestroyExchangeOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyExchangeOperatorInfo(void* param) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  taosRemoveRef(exchangeObjRefPool, pExInfo->self);
}

void freeBlock(void* pParam) {
  SSDataBlock* pBlock = *(SSDataBlock**)pParam;
  blockDataDestroy(pBlock);
}

void freeSourceDataInfo(void* p) {
  SSourceDataInfo* pInfo = (SSourceDataInfo*)p;
  taosMemoryFreeClear(pInfo->pRsp);
}

void doDestroyExchangeOperatorInfo(void* param) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;

  taosArrayDestroy(pExInfo->pSources);
  taosArrayDestroyEx(pExInfo->pSourceDataInfo, freeSourceDataInfo);

  taosArrayDestroyEx(pExInfo->pResultBlockList, freeBlock);
  taosArrayDestroyEx(pExInfo->pRecycledBlocks, freeBlock);

  blockDataDestroy(pExInfo->pDummyBlock);
  tSimpleHashCleanup(pExInfo->pHashSources);

  tsem_destroy(&pExInfo->ready);
  taosMemoryFreeClear(pExInfo->pTaskId);

  taosMemoryFreeClear(param);
}

int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SFetchRspHandleWrapper* pWrapper = (SFetchRspHandleWrapper*)param;

  SExchangeInfo* pExchangeInfo = taosAcquireRef(exchangeObjRefPool, pWrapper->exchangeId);
  if (pExchangeInfo == NULL) {
    qWarn("failed to acquire exchange operator, since it may have been released, %p", pExchangeInfo);
    taosMemoryFree(pMsg->pData);
    return TSDB_CODE_SUCCESS;
  }

  int32_t          index = pWrapper->sourceIndex;
  SSourceDataInfo* pSourceDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, index);

  if (code == TSDB_CODE_SUCCESS) {
    pSourceDataInfo->pRsp = pMsg->pData;

    SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
    pRsp->numOfRows = htobe64(pRsp->numOfRows);
    pRsp->compLen = htonl(pRsp->compLen);
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

  pSourceDataInfo->status = EX_SOURCE_DATA_READY;
  code = tsem_post(&pExchangeInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    code = TAOS_SYSTEM_ERROR(code);
    qError("failed to invoke post when fetch rsp is ready, code:%s, %p", tstrerror(code), pExchangeInfo);
  }

  taosReleaseRef(exchangeObjRefPool, pWrapper->exchangeId);
  return code;
}

int32_t buildTableScanOperatorParam(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, bool tableSeq) {
  *ppRes = taosMemoryMalloc(sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  STableScanOperatorParam* pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
  if (NULL == pScan) {
    taosMemoryFreeClear(*ppRes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pScan->pUidList = taosArrayDup(pUidList, NULL);
  if (NULL == pScan->pUidList) {
    taosMemoryFree(pScan);
    taosMemoryFreeClear(*ppRes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pScan->tableSeq = tableSeq;

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex) {
  SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);
  if (EX_SOURCE_DATA_NOT_READY != pDataInfo->status) {
    return TSDB_CODE_SUCCESS;
  }

  pDataInfo->status = EX_SOURCE_DATA_STARTED;
  SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pDataInfo->index);
  pDataInfo->startTime = taosGetTimestampUs();
  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SFetchRspHandleWrapper* pWrapper = taosMemoryCalloc(1, sizeof(SFetchRspHandleWrapper));
  pWrapper->exchangeId = pExchangeInfo->self;
  pWrapper->sourceIndex = sourceIndex;

  if (pSource->localExec) {
    SDataBuf pBuf = {0};
    int32_t  code =
        (*pTaskInfo->localFetch.fp)(pTaskInfo->localFetch.handle, pSource->schedId, pTaskInfo->id.queryId,
                                    pSource->taskId, 0, pSource->execId, &pBuf.pData, pTaskInfo->localFetch.explainRes);
    loadRemoteDataCallback(pWrapper, &pBuf, code);
    taosMemoryFree(pWrapper);
  } else {
    SResFetchReq req = {0};
    req.header.vgId = pSource->addr.nodeId;
    req.sId = pSource->schedId;
    req.taskId = pSource->taskId;
    req.queryId = pTaskInfo->id.queryId;
    req.execId = pSource->execId;
    if (pDataInfo->pSrcUidList) {
      int32_t code =
          buildTableScanOperatorParam(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType, pDataInfo->tableSeq);
      taosArrayDestroy(pDataInfo->pSrcUidList);
      pDataInfo->pSrcUidList = NULL;
      if (TSDB_CODE_SUCCESS != code) {
        pTaskInfo->code = code;
        taosMemoryFree(pWrapper);
        return pTaskInfo->code;
      }
    }

    int32_t msgSize = tSerializeSResFetchReq(NULL, 0, &req);
    if (msgSize < 0) {
      pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pWrapper);
      freeOperatorParam(req.pOpParam, OP_GET_PARAM);
      return pTaskInfo->code;
    }

    void* msg = taosMemoryCalloc(1, msgSize);
    if (NULL == msg) {
      pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pWrapper);
      freeOperatorParam(req.pOpParam, OP_GET_PARAM);
      return pTaskInfo->code;
    }

    if (tSerializeSResFetchReq(msg, msgSize, &req) < 0) {
      pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pWrapper);
      taosMemoryFree(msg);
      freeOperatorParam(req.pOpParam, OP_GET_PARAM);
      return pTaskInfo->code;
    }

    freeOperatorParam(req.pOpParam, OP_GET_PARAM);

    qDebug("%s build fetch msg and send to vgId:%d, ep:%s, taskId:0x%" PRIx64 ", execId:%d, %p, %d/%" PRIzu,
           GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->addr.epSet.eps[0].fqdn, pSource->taskId,
           pSource->execId, pExchangeInfo, sourceIndex, totalSources);

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      taosMemoryFreeClear(msg);
      taosMemoryFree(pWrapper);
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
      return pTaskInfo->code;
    }

    pMsgSendInfo->param = pWrapper;
    pMsgSendInfo->paramFreeFp = taosMemoryFree;
    pMsgSendInfo->msgInfo.pData = msg;
    pMsgSendInfo->msgInfo.len = msgSize;
    pMsgSendInfo->msgType = pSource->fetchMsgType;
    pMsgSendInfo->fp = loadRemoteDataCallback;

    int64_t transporterId = 0;
    int32_t code =
        asyncSendMsgToServer(pExchangeInfo->pTransporter, &pSource->addr.epSet, &transporterId, pMsgSendInfo);
  }

  return TSDB_CODE_SUCCESS;
}

void updateLoadRemoteInfo(SLoadRemoteDataInfo* pInfo, int64_t numOfRows, int32_t dataLen, int64_t startTs,
                          SOperatorInfo* pOperator) {
  pInfo->totalRows += numOfRows;
  pInfo->totalSize += dataLen;
  pInfo->totalElapsed += (taosGetTimestampUs() - startTs);
  pOperator->resultInfo.totalRows += numOfRows;
}

int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, char* pData, SArray* pColList, char** pNextStart) {
  if (pColList == NULL) {  // data from other sources
    blockDataCleanup(pRes);
    *pNextStart = (char*)blockDecode(pRes, pData);
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

    SSDataBlock* pBlock = createDataBlock();
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData idata = createColumnInfoData(pSchema[i].type, pSchema[i].bytes, pSchema[i].colId);
      blockDataAppendColInfo(pBlock, &idata);
    }

    blockDecode(pBlock, pStart);
    blockDataEnsureCapacity(pRes, pBlock->info.rows);

    // data from mnode
    pRes->info.dataLoad = 1;
    pRes->info.rows = pBlock->info.rows;
    pRes->info.scanFlag = MAIN_SCAN;
    relocateColumnData(pRes, pColList, pBlock->pDataBlock, false);
    blockDataDestroy(pBlock);
  }

  return TSDB_CODE_SUCCESS;
}

void* setAllSourcesCompleted(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  size_t               totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  qDebug("%s all %" PRIzu " sources are exhausted, total rows: %" PRIu64 ", %.2f Kb, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0,
         pLoadInfo->totalElapsed / 1000.0);

  setOperatorCompleted(pOperator);
  return NULL;
}

int32_t getCompletedSources(const SArray* pArray) {
  size_t total = taosArrayGetSize(pArray);

  int32_t completed = 0;
  for (int32_t k = 0; k < total; ++k) {
    SSourceDataInfo* p = taosArrayGet(pArray, k);
    if (p->status == EX_SOURCE_DATA_EXHAUSTED) {
      completed += 1;
    }
  }

  return completed;
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
  SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;

  char*   pStart = pRetrieveRsp->data;
  int32_t index = 0;
  int32_t code = 0;
  while (index++ < pRetrieveRsp->numOfBlocks) {
    SSDataBlock* pb = NULL;
    if (taosArrayGetSize(pExchangeInfo->pRecycledBlocks) > 0) {
      pb = *(SSDataBlock**)taosArrayPop(pExchangeInfo->pRecycledBlocks);
      blockDataCleanup(pb);
    } else {
      pb = createOneDataBlock(pExchangeInfo->pDummyBlock, false);
    }

    code = extractDataBlockFromFetchRsp(pb, pStart, NULL, &pStart);
    if (code != 0) {
      taosMemoryFreeClear(pDataInfo->pRsp);
      return code;
    }

    taosArrayPush(pExchangeInfo->pResultBlockList, &pb);
  }

  return code;
}

int32_t seqLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int32_t code = 0;
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  while (1) {
    if (pExchangeInfo->current >= totalSources) {
      setAllSourcesCompleted(pOperator);
      return TSDB_CODE_SUCCESS;
    }

    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;

    doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    tsem_wait(&pExchangeInfo->ready);
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, taskID:0x%" PRIx64 " execId:%d error happens, code:%s", GET_TASKID(pTaskInfo),
             pSource->addr.nodeId, pSource->taskId, pSource->execId, tstrerror(pDataInfo->code));
      pOperator->pTaskInfo->code = pDataInfo->code;
      return pOperator->pTaskInfo->code;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;

    if (pRsp->numOfRows == 0) {
      qDebug("%s vgId:%d, taskID:0x%" PRIx64 " execId:%d %d of total completed, rowsOfSource:%" PRIu64
             ", totalRows:%" PRIu64 " try next",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pExchangeInfo->current + 1,
             pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
      taosMemoryFreeClear(pDataInfo->pRsp);
      continue;
    }

    code = doExtractResultBlocks(pExchangeInfo, pDataInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
    if (pRsp->completed == 1) {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pRetrieveRsp->numOfRows,
             pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize, pExchangeInfo->current + 1,
             totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64 ", totalRows:%" PRIu64
             ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pRetrieveRsp->numOfRows,
             pLoadInfo->totalRows, pLoadInfo->totalSize);
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

int32_t addSingleExchangeSource(SOperatorInfo* pOperator, SExchangeOperatorBasicParam* pBasicParam) {
  SExchangeInfo*     pExchangeInfo = pOperator->info;
  SExchangeSrcIndex* pIdx = tSimpleHashGet(pExchangeInfo->pHashSources, &pBasicParam->vgId, sizeof(pBasicParam->vgId));
  if (NULL == pIdx) {
    qError("No exchange source for vgId: %d", pBasicParam->vgId);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pIdx->inUseIdx < 0) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = pExchangeInfo->pTaskId;
    dataInfo.index = pIdx->srcIdx;
    dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
    dataInfo.srcOpType = pBasicParam->srcOpType;
    dataInfo.tableSeq = pBasicParam->tableSeq;

    taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo);
    pIdx->inUseIdx = taosArrayGetSize(pExchangeInfo->pSourceDataInfo) - 1;
  } else {
    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pIdx->inUseIdx);
    if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
      pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
    }
    pDataInfo->pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
    pDataInfo->srcOpType = pBasicParam->srcOpType;
    pDataInfo->tableSeq = pBasicParam->tableSeq;
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

  return TSDB_CODE_SUCCESS;
}

int32_t prepareLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  int32_t        code = TSDB_CODE_SUCCESS;
  if ((OPTR_IS_OPENED(pOperator) && !pExchangeInfo->dynamicOp) ||
      (pExchangeInfo->dynamicOp && NULL == pOperator->pOperatorGetParam)) {
    return TSDB_CODE_SUCCESS;
  }

  if (pExchangeInfo->dynamicOp) {
    code = addDynamicExchangeSource(pOperator);
    if (code) {
      return code;
    }
  }

  int64_t st = taosGetTimestampUs();

  if (!pExchangeInfo->seqLoadData) {
    int32_t code = prepareConcurrentlyLoad(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pExchangeInfo->openedTs = taosGetTimestampUs();
  }

  OPTR_SET_OPENED(pOperator);
  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
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
