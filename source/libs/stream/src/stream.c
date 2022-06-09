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

#include "streamInc.h"

int32_t streamTriggerByWrite(SStreamTask* pTask, int32_t vgId, SMsgCb* pMsgCb) {
  int8_t execStatus = atomic_load_8(&pTask->status);
  if (execStatus == TASK_STATUS__IDLE || execStatus == TASK_STATUS__CLOSING) {
    SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
    if (pRunReq == NULL) return -1;

    // TODO: do we need htonl?
    pRunReq->head.vgId = vgId;
    pRunReq->streamId = pTask->streamId;
    pRunReq->taskId = pTask->taskId;
    SRpcMsg msg = {
        .msgType = TDMT_STREAM_TASK_RUN,
        .pCont = pRunReq,
        .contLen = sizeof(SStreamTaskRunReq),
    };
    tmsgPutToQueue(pMsgCb, FETCH_QUEUE, &msg);
  }
  return 0;
}

int32_t streamTaskEnqueue(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
  int8_t            status;

  // enqueue
  if (pData != NULL) {
    pData->type = STREAM_DATA_TYPE_SSDATA_BLOCK;
    pData->sourceVg = pReq->sourceVg;
    // decode
    /*pData->blocks = pReq->data;*/
    /*pBlock->sourceVer = pReq->sourceVer;*/
    streamDispatchReqToData(pReq, pData);
    if (streamTaskInput(pTask, (SStreamQueueItem*)pData) == 0) {
      status = TASK_INPUT_STATUS__NORMAL;
    } else {
      status = TASK_INPUT_STATUS__FAILED;
    }
  } else {
    streamTaskInputFail(pTask);
    status = TASK_INPUT_STATUS__FAILED;
  }

  // rsp by input status
  void* buf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  ((SMsgHead*)buf)->vgId = htonl(pReq->sourceVg);
  SStreamDispatchRsp* pCont = POINTER_SHIFT(buf, sizeof(SMsgHead));
  pCont->inputStatus = status;
  pCont->streamId = pReq->streamId;
  pCont->taskId = pReq->sourceTaskId;
  pRsp->pCont = buf;
  pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
  tmsgSendRsp(pRsp);
  return status == TASK_INPUT_STATUS__NORMAL ? 0 : -1;
}

int32_t streamProcessDispatchReq(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  // 1. handle input
  streamTaskEnqueue(pTask, pReq, pRsp);

  // 2. try exec
  // 2.1. idle: exec
  // 2.2. executing: return
  // 2.3. closing: keep trying
  streamExec(pTask, pMsgCb);

  // 3. handle output
  // 3.1 check and set status
  // 3.2 dispatch / sink
  if (pTask->dispatchType != TASK_DISPATCH__NONE) {
    streamDispatch(pTask, pMsgCb);
  }

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamDispatchRsp* pRsp) {
  ASSERT(pRsp->inputStatus == TASK_OUTPUT_STATUS__NORMAL || pRsp->inputStatus == TASK_OUTPUT_STATUS__BLOCKED);
  int8_t old = atomic_exchange_8(&pTask->outputStatus, pRsp->inputStatus);
  ASSERT(old == TASK_OUTPUT_STATUS__WAIT);
  if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
    // TODO: init recover timer
    return 0;
  }
  // continue dispatch
  if (pTask->dispatchType != TASK_DISPATCH__NONE) {
    streamDispatch(pTask, pMsgCb);
  }
  return 0;
}

int32_t streamTaskProcessRunReq(SStreamTask* pTask, SMsgCb* pMsgCb) {
  streamExec(pTask, pMsgCb);
  if (pTask->dispatchType != TASK_DISPATCH__NONE) {
    streamDispatch(pTask, pMsgCb);
  }
  return 0;
}

int32_t streamProcessRecoverReq(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamTaskRecoverReq* pReq, SRpcMsg* pMsg) {
  //
  return 0;
}

int32_t streamProcessRecoverRsp(SStreamTask* pTask, SStreamTaskRecoverRsp* pRsp) {
  //
  return 0;
}
