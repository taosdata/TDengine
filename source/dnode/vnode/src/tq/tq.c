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

#include "tq.h"

int32_t tqInit() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tqMgmt.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    tqMgmt.timer = taosTmrInit(10000, 100, 10000, "TQ");
    if (tqMgmt.timer == NULL) {
      atomic_store_8(&tqMgmt.inited, 0);
      return -1;
    }
    atomic_store_8(&tqMgmt.inited, 1);
  }
  return 0;
}

void tqCleanUp() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tqMgmt.inited, 1, 2);
    if (old != 2) break;
  }

  if (old == 1) {
    taosTmrCleanUp(tqMgmt.timer);
    atomic_store_8(&tqMgmt.inited, 0);
  }
}

STQ* tqOpen(const char* path, SVnode* pVnode, SWal* pWal) {
  STQ* pTq = taosMemoryMalloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->pVnode = pVnode;
  pTq->pWal = pWal;

  pTq->handles = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);

  pTq->pStreamTasks = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  pTq->pushMgr = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);

  if (tqMetaOpen(pTq) < 0) {
    ASSERT(0);
  }

  return pTq;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    taosMemoryFreeClear(pTq->path);
    taosHashCleanup(pTq->handles);
    taosHashCleanup(pTq->pStreamTasks);
    taosHashCleanup(pTq->pushMgr);
    tqMetaClose(pTq);
    taosMemoryFree(pTq);
  }
  // TODO
}

int32_t tqSendPollRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataBlkRsp* pRsp) {
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqDataBlkRsp(NULL, pRsp);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;
  ((SMqRspHead*)buf)->consumerId = pReq->consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqDataBlkRsp(&abuf, pRsp);

  SRpcMsg resp = {
      .info = pMsg->info,
      .pCont = buf,
      .contLen = tlen,
      .code = 0,
  };
  tmsgSendRsp(&resp);

  tqDebug("vg %d from consumer %ld (epoch %d) send rsp, block num: %d, reqOffset: %ld, rspOffset: %ld",
          TD_VID(pTq->pVnode), pReq->consumerId, pReq->epoch, pRsp->blockNum, pRsp->reqOffset, pRsp->rspOffset);

  return 0;
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg, int32_t workerId) {
  SMqPollReq* pReq = pMsg->pCont;
  int64_t     consumerId = pReq->consumerId;
  int64_t     timeout = pReq->timeout;
  int32_t     reqEpoch = pReq->epoch;
  int64_t     fetchOffset;
  int32_t     code = 0;

  // get offset to fetch message
  if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__EARLIEAST) {
    fetchOffset = walGetFirstVer(pTq->pWal);
  } else if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__LATEST) {
    fetchOffset = walGetCommittedVer(pTq->pWal);
  } else {
    fetchOffset = pReq->currentOffset + 1;
  }

  tqDebug("tmq poll: consumer %ld (epoch %d) recv poll req in vg %d, req %ld %ld", consumerId, pReq->epoch,
          TD_VID(pTq->pVnode), pReq->currentOffset, fetchOffset);

  STqHandle* pHandle = taosHashGet(pTq->handles, pReq->subKey, strlen(pReq->subKey));
  ASSERT(pHandle);

  int32_t consumerEpoch = atomic_load_32(&pHandle->epoch);
  while (consumerEpoch < reqEpoch) {
    consumerEpoch = atomic_val_compare_exchange_32(&pHandle->epoch, consumerEpoch, reqEpoch);
  }

  SWalHead* pHeadWithCkSum = taosMemoryMalloc(sizeof(SWalHead) + 2048);
  if (pHeadWithCkSum == NULL) {
    return -1;
  }

  walSetReaderCapacity(pHandle->pWalReader, 2048);

  SMqDataBlkRsp rsp = {0};
  rsp.reqOffset = pReq->currentOffset;

  rsp.blockData = taosArrayInit(0, sizeof(void*));
  rsp.blockDataLen = taosArrayInit(0, sizeof(int32_t));

  rsp.withTbName = pReq->withTbName;
  if (rsp.withTbName) {
    rsp.blockTbName = taosArrayInit(0, sizeof(void*));
  }
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    rsp.withSchema = false;
    rsp.withTag = false;
  } else {
    rsp.withSchema = true;
    rsp.withTag = false;
    rsp.blockSchema = taosArrayInit(0, sizeof(void*));
  }

  while (1) {
    consumerEpoch = atomic_load_32(&pHandle->epoch);
    if (consumerEpoch > reqEpoch) {
      tqWarn("tmq poll: consumer %ld (epoch %d) vg %d offset %ld, found new consumer epoch %d, discard req epoch %d",
             consumerId, pReq->epoch, TD_VID(pTq->pVnode), fetchOffset, consumerEpoch, reqEpoch);
      break;
    }

    if (tqFetchLog(pTq, pHandle, &fetchOffset, &pHeadWithCkSum) < 0) {
      // TODO add push mgr
      break;
    }

    SWalReadHead* pHead = &pHeadWithCkSum->head;

    tqDebug("tmq poll: consumer %ld (epoch %d) iter log, vg %d offset %ld msgType %d", consumerId, pReq->epoch,
            TD_VID(pTq->pVnode), fetchOffset, pHead->msgType);

    if (pHead->msgType == TDMT_VND_SUBMIT) {
      SSubmitReq* pCont = (SSubmitReq*)&pHead->body;

      if (tqDataExec(pTq, &pHandle->execHandle, pCont, &rsp, workerId) < 0) {
        /*ASSERT(0);*/
      }
    } else {
      // TODO
      ASSERT(0);
    }

    // TODO batch optimization:
    // TODO continue scan until meeting batch requirement
    if (rsp.blockNum > 0 /* threshold */) {
      break;
    } else {
      fetchOffset++;
    }
  }

  taosMemoryFree(pHeadWithCkSum);

  ASSERT(taosArrayGetSize(rsp.blockData) == rsp.blockNum);
  ASSERT(taosArrayGetSize(rsp.blockDataLen) == rsp.blockNum);
  if (rsp.withSchema) {
    ASSERT(taosArrayGetSize(rsp.blockSchema) == rsp.blockNum);
  }

  rsp.rspOffset = fetchOffset;

  if (tqSendPollRsp(pTq, pMsg, pReq, &rsp) < 0) {
    code = -1;
  }

  // TODO wrap in destroy func
  taosArrayDestroy(rsp.blockData);
  taosArrayDestroy(rsp.blockDataLen);

  if (rsp.withSchema) {
    taosArrayDestroyP(rsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
  }

  if (rsp.withTbName) {
    taosArrayDestroyP(rsp.blockTbName, (FDelete)taosMemoryFree);
  }

  return code;
}

int32_t tqProcessVgDeleteReq(STQ* pTq, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;

  int32_t code = taosHashRemove(pTq->handles, pReq->subKey, strlen(pReq->subKey));
  ASSERT(code == 0);

  if (tqMetaDeleteHandle(pTq, pReq->subKey) < 0) {
    ASSERT(0);
  }
  return 0;
}

// TODO: persist meta into tdb
int32_t tqProcessVgChangeReq(STQ* pTq, char* msg, int32_t msgLen) {
  SMqRebVgReq req = {0};
  tDecodeSMqRebVgReq(msg, &req);
  // todo lock
  STqHandle* pHandle = taosHashGet(pTq->handles, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    ASSERT(req.oldConsumerId == -1);
    ASSERT(req.newConsumerId != -1);
    STqHandle tqHandle = {0};
    pHandle = &tqHandle;
    /*taosInitRWLatch(&pExec->lock);*/

    memcpy(pHandle->subKey, req.subKey, TSDB_SUBSCRIBE_KEY_LEN);
    pHandle->consumerId = req.newConsumerId;
    pHandle->epoch = -1;

    pHandle->execHandle.subType = req.subType;
    /*pExec->withTbName = req.withTbName;*/
    /*pExec->withSchema = req.withSchema;*/
    /*pExec->withTag = req.withTag;*/

    pHandle->pWalReader = walOpenReadHandle(pTq->pVnode->pWal);
    for (int32_t i = 0; i < 5; i++) {
      pHandle->execHandle.pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
    }
    if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      pHandle->execHandle.exec.execCol.qmsg = req.qmsg;
      req.qmsg = NULL;
      for (int32_t i = 0; i < 5; i++) {
        SReadHandle handle = {
            .reader = pHandle->execHandle.pExecReader[i],
            .meta = pTq->pVnode->pMeta,
            .pMsgCb = &pTq->pVnode->msgCb,
        };
        pHandle->execHandle.exec.execCol.task[i] =
            qCreateStreamExecTaskInfo(pHandle->execHandle.exec.execCol.qmsg, &handle);
        ASSERT(pHandle->execHandle.exec.execCol.task[i]);
      }
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      pHandle->execHandle.exec.execDb.pFilterOutTbUid =
          taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      int64_t suid = 0;
      /*pHandle->execHandle.exec.execTb.suid = req.suid;*/
      SArray* tbUidList = taosArrayInit(0, sizeof(int16_t));
      tsdbGetAllTableList(pTq->pVnode->pMeta, suid, tbUidList);
      for (int32_t i = 0; i < 5; i++) {
        tqReadHandleSetTbUidList(pHandle->execHandle.pExecReader[i], tbUidList);
      }
    }
    taosHashPut(pTq->handles, req.subKey, strlen(req.subKey), pHandle, sizeof(STqHandle));
  } else {
    /*ASSERT(pExec->consumerId == req.oldConsumerId);*/
    // TODO handle qmsg and exec modification
    atomic_store_32(&pHandle->epoch, -1);
    atomic_store_64(&pHandle->consumerId, req.newConsumerId);
    atomic_add_fetch_32(&pHandle->epoch, 1);
  }

  if (tqMetaSaveHandle(pTq, req.subKey, pHandle) < 0) {
    // TODO
  }
  return 0;
}

int32_t tqProcessTaskDeploy(STQ* pTq, char* msg, int32_t msgLen) {
  SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeSStreamTask(&decoder, pTask) < 0) {
    ASSERT(0);
  }
  tDecoderClear(&decoder);

  pTask->status = TASK_STATUS__IDLE;
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;

  pTask->inputQ = taosOpenQueue();
  pTask->outputQ = taosOpenQueue();
  pTask->inputQAll = taosAllocateQall();
  pTask->outputQAll = taosAllocateQall();

  if (pTask->inputQ == NULL || pTask->outputQ == NULL || pTask->inputQAll == NULL || pTask->outputQAll == NULL)
    goto FAIL;

  // exec
  if (pTask->execType != TASK_EXEC__NONE) {
    // expand runners
    STqReadHandle* pStreamReader = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
    SReadHandle    handle = {
           .reader = pStreamReader,
           .meta = pTq->pVnode->pMeta,
           .pMsgCb = &pTq->pVnode->msgCb,
           .vnode = pTq->pVnode,
    };
    pTask->exec.inputHandle = pStreamReader;
    pTask->exec.executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle);
    ASSERT(pTask->exec.executor);
  }

  // sink
  pTask->ahandle = pTq->pVnode;
  if (pTask->sinkType == TASK_SINK__SMA) {
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->sinkType == TASK_SINK__TABLE) {
    pTask->tbSink.vnode = pTq->pVnode;
    pTask->tbSink.tbSinkFunc = tqTableSink;

    ASSERT(pTask->tbSink.pSchemaWrapper);
    ASSERT(pTask->tbSink.pSchemaWrapper->pSchema);

    pTask->tbSink.pTSchema =
        tdGetSTSChemaFromSSChema(&pTask->tbSink.pSchemaWrapper->pSchema, pTask->tbSink.pSchemaWrapper->nCols);
    ASSERT(pTask->tbSink.pTSchema);
  }

  taosHashPut(pTq->pStreamTasks, &pTask->taskId, sizeof(int32_t), pTask, sizeof(SStreamTask));

  return 0;
FAIL:
  if (pTask->inputQ) taosCloseQueue(pTask->inputQ);
  if (pTask->outputQ) taosCloseQueue(pTask->outputQ);
  if (pTask->inputQAll) taosFreeQall(pTask->inputQAll);
  if (pTask->outputQAll) taosFreeQall(pTask->outputQAll);
  if (pTask) taosMemoryFree(pTask);
  return -1;
}

int32_t tqProcessStreamTrigger(STQ* pTq, SSubmitReq* pReq) {
  void*              pIter = NULL;
  bool               failed = false;
  SStreamDataSubmit* pSubmit = NULL;

  pSubmit = streamDataSubmitNew(pReq);
  if (pSubmit == NULL) {
    failed = true;
  }

  while (1) {
    pIter = taosHashIterate(pTq->pStreamTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = (SStreamTask*)pIter;
    if (pTask->inputType != STREAM_INPUT__DATA_SUBMIT) continue;

    int8_t inputStatus = atomic_load_8(&pTask->inputStatus);
    if (inputStatus == TASK_INPUT_STATUS__NORMAL) {
      if (failed) {
        atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
        continue;
      }

      SStreamDataSubmit* pSubmitClone = streamSubmitRefClone(pSubmit);
      if (pSubmitClone == NULL) {
        atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
        continue;
      }
      taosWriteQitem(pTask->inputQ, pSubmitClone);

      int8_t execStatus = atomic_load_8(&pTask->status);
      if (execStatus == TASK_STATUS__IDLE || execStatus == TASK_STATUS__CLOSING) {
        SStreamTaskRunReq* pRunReq = taosMemoryMalloc(sizeof(SStreamTaskRunReq));
        if (pRunReq == NULL) continue;
        // TODO: do we need htonl?
        pRunReq->head.vgId = pTq->pVnode->config.vgId;
        pRunReq->streamId = pTask->streamId;
        pRunReq->taskId = pTask->taskId;
        SRpcMsg msg = {
            .msgType = TDMT_VND_TASK_RUN,
            .pCont = pRunReq,
            .contLen = sizeof(SStreamTaskRunReq),
        };
        tmsgPutToQueue(&pTq->pVnode->msgCb, FETCH_QUEUE, &msg);
      }

    } else {
      // blocked or stopped, do nothing
    }
  }

  if (pSubmit) {
    streamDataSubmitRefDec(pSubmit);
    taosFreeQitem(pSubmit);
  }

  return failed ? -1 : 0;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  //
  SStreamTaskRunReq* pReq = pMsg->pCont;
  int32_t            taskId = pReq->taskId;
  SStreamTask*       pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamTaskProcessRunReq(pTask, &pTq->pVnode->msgCb);
  return 0;
}

int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamDispatchReq* pReq = pMsg->pCont;
  int32_t             taskId = pReq->taskId;
  SStreamTask*        pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessDispatchReq(pTask, &pTq->pVnode->msgCb, pReq, pMsg);
  return 0;
}

int32_t tqProcessTaskRecoverReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRecoverReq* pReq = pMsg->pCont;
  int32_t                taskId = pReq->taskId;
  SStreamTask*           pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessRecoverReq(pTask, &pTq->pVnode->msgCb, pReq, pMsg);
  return 0;
}

int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = pMsg->pCont;
  int32_t             taskId = pRsp->taskId;
  SStreamTask*        pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessDispatchRsp(pTask, &pTq->pVnode->msgCb, pRsp);
  return 0;
}

int32_t tqProcessTaskRecoverRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRecoverRsp* pRsp = pMsg->pCont;
  int32_t                taskId = pRsp->taskId;
  SStreamTask*           pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessRecoverRsp(pTask, pRsp);
  return 0;
}
