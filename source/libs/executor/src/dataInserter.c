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

#include "dataSinkInt.h"
#include "dataSinkMgt.h"
#include "executorimpl.h"
#include "planner.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tqueue.h"

extern SDataSinkStat gDataSinkStat;

typedef struct SSubmitRes {
  int64_t     affectedRows;
  int32_t     code;
  SSubmitRsp *pRsp;
} SSubmitRes;

typedef struct SDataInserterHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  STSchema*           pSchema;
  SQueryInserterNode* pNode;
  SSubmitRes          submitRes;
  SInserterParam*     pParam;
  SArray*             pDataBlocks;
  int32_t             status;
  bool                queryEnd;
  uint64_t            useconds;
  uint64_t            cachedSize;
  TdThreadMutex       mutex;
  tsem_t              ready;  
} SDataInserterHandle;

typedef struct SSubmitRspParam {
  SDataInserterHandle* pInserter;
} SSubmitRspParam;

static int32_t updateStatus(SDataInserterHandle* pInserter) {
  taosThreadMutexLock(&pInserter->mutex);
  int32_t blockNums = taosQueueItemSize(pInserter->pDataBlocks);
  int32_t status =
      (0 == blockNums ? DS_BUF_EMPTY
                      : (blockNums < pInserter->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pInserter->status = status;
  taosThreadMutexUnlock(&pInserter->mutex);
  return status;
}

static int32_t getStatus(SDataInserterHandle* pInserter) {
  taosThreadMutexLock(&pInserter->mutex);
  int32_t status = pInserter->status;
  taosThreadMutexUnlock(&pInserter->mutex);
  return status;
}

int32_t inserterCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SSubmitRspParam* pParam = (SSubmitRspParam*)param;
  SDataInserterHandle* pInserter = pParam->pInserter;

  pInserter->submitRes.code = code;
  
  if (code == TSDB_CODE_SUCCESS) {
    pInserter->submitRes.pRsp = taosMemoryCalloc(1, sizeof(SSubmitRsp));
    SDecoder    coder = {0};
    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    code = tDecodeSSubmitRsp(&coder, pInserter->submitRes.pRsp);
    if (code) {
      tFreeSSubmitRsp(pInserter->submitRes.pRsp);
      pInserter->submitRes.code = code;
      goto _return;
    }
    
    if (pInserter->submitRes.pRsp->nBlocks > 0) {
      for (int32_t i = 0; i < pInserter->submitRes.pRsp->nBlocks; ++i) {
        SSubmitBlkRsp *blk = pInserter->submitRes.pRsp->pBlocks + i;
        if (TSDB_CODE_SUCCESS != blk->code) {
          code = blk->code;
          tFreeSSubmitRsp(pInserter->submitRes.pRsp);
          pInserter->submitRes.code = code;
          goto _return;
        }
      }
    }
    
    pInserter->submitRes.affectedRows += pInserter->submitRes.pRsp->affectedRows;
    qDebug("submit rsp received, affectedRows:%d, total:%d", pInserter->submitRes.pRsp->affectedRows, pInserter->submitRes.affectedRows);

    tFreeSSubmitRsp(pInserter->submitRes.pRsp);
  }

_return:

  tsem_post(&pInserter->ready);

  taosMemoryFree(param);
  
  return TSDB_CODE_SUCCESS;
}


static int32_t sendSubmitRequest(SDataInserterHandle* pInserter, SSubmitReq* pMsg, void* pTransporter, SEpSet* pEpset) {
  // send the fetch remote task result reques
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    taosMemoryFreeClear(pMsg);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return terrno;
  }

  SSubmitRspParam* pParam = taosMemoryCalloc(1, sizeof(SSubmitRspParam));
  pParam->pInserter = pInserter;

  pMsgSendInfo->param = pParam;
  pMsgSendInfo->msgInfo.pData = pMsg;
  pMsgSendInfo->msgInfo.len = sizeof(SSubmitReq);
  pMsgSendInfo->msgType = TDMT_VND_SUBMIT;
  pMsgSendInfo->fp = inserterCallback;

  int64_t transporterId = 0;
  return asyncSendMsgToServer(pTransporter, pEpset, &transporterId, pMsgSendInfo);
}


static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  taosArrayPush(pInserter->pDataBlocks, pInput->pData);
  SSubmitReq* pMsg = dataBlockToSubmit(pInserter->pDataBlocks, pInserter->pSchema, pInserter->pNode->tableId, pInserter->pNode->suid, pInserter->pNode->vgId);

  int32_t code = sendSubmitRequest(pInserter, pMsg, pInserter->pParam->readHandle->pMsgCb->clientRpc, &pInserter->pNode->epSet);
  if (code) {
    return code;
  }

  tsem_wait(&pInserter->ready);

  if (pInserter->submitRes.code) {
    return pInserter->submitRes.code;
  }

  *pContinue = true;
  
  return TSDB_CODE_SUCCESS;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  taosThreadMutexLock(&pInserter->mutex);
  pInserter->queryEnd = true;
  pInserter->useconds = useconds;
  taosThreadMutexUnlock(&pInserter->mutex);
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pInserter->cachedSize);
  taosArrayDestroy(pInserter->pDataBlocks);
  taosMemoryFree(pInserter->pSchema);
  taosThreadMutexDestroy(&pInserter->mutex);
  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataInserter(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle, void *pParam) {
  SDataInserterHandle* inserter = taosMemoryCalloc(1, sizeof(SDataInserterHandle));
  if (NULL == inserter) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  SDataDeleterNode* pInserterNode = (SQueryInserterNode *)pDataSink;
  inserter->sink.fPut = putDataBlock;
  inserter->sink.fEndPut = endPut;
  inserter->sink.fGetLen = NULL;
  inserter->sink.fGetData = NULL;
  inserter->sink.fDestroy = destroyDataSinker;
  inserter->sink.fGetCacheSize = getCacheSize;
  inserter->pManager = pManager;
  inserter->pNode = pInserterNode;
  inserter->pParam = pParam;
  inserter->status = DS_BUF_EMPTY;
  inserter->queryEnd = false;

  int64_t suid = 0;
  int32_t code = tsdbGetTableSchema(inserter->pParam->readHandle->vnode, pInserterNode->tableId, &inserter->pSchema, &suid);
  if (code) {
    return code;
  }

  if (pInserterNode->suid != suid) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    return terrno;
  }

  inserter->pDataBlocks = taosArrayInit(1, POINTER_BYTES);
  taosThreadMutexInit(&inserter->mutex, NULL);
  if (NULL == inserter->pDataBlocks) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  tsem_init(&inserter->ready, 0, 0);
  
  *pHandle = inserter;
  return TSDB_CODE_SUCCESS;
}
