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
#include "vnd.h"

int32_t tqProcessSubmitReqForSubscribe(STQ* pTq) {
  if (pTq == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (taosHashGetSize(pTq->pPushMgr) <= 0) {
    return 0;
  }
  SRpcMsg msg = {.msgType = TDMT_VND_TMQ_CONSUME_PUSH};
  msg.pCont = rpcMallocCont(sizeof(SMsgHead));
  if (msg.pCont == NULL) {
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }
  msg.contLen = sizeof(SMsgHead);
  SMsgHead *pHead = msg.pCont;
  pHead->vgId = TD_VID(pTq->pVnode);
  pHead->contLen = msg.contLen;
  int32_t code = tmsgPutToQueue(&pTq->pVnode->msgCb, QUERY_QUEUE, &msg);
  if (code != 0){
    tqError("vgId:%d failed to push msg to queue, code:%d", TD_VID(pTq->pVnode), code);
    rpcFreeCont(msg.pCont);
  }
  return code;
}

int32_t tqPushMsg(STQ* pTq, tmsg_t msgType) {
  int32_t code = 0;
  if (msgType == TDMT_VND_SUBMIT) {
    code = tqProcessSubmitReqForSubscribe(pTq);
    if (code != 0){
      tqError("vgId:%d failed to process submit request for subscribe, code:%d", TD_VID(pTq->pVnode), code);
    }
  }

  return code;
}

int32_t tqRegisterPushHandle(STQ* pTq, void* handle, SRpcMsg* pMsg) {
  if (pTq == NULL || handle == NULL || pMsg == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t    vgId = TD_VID(pTq->pVnode);
  STqHandle* pHandle = (STqHandle*)handle;

  if (pHandle->msg == NULL) {
    pHandle->msg = taosMemoryCalloc(1, sizeof(SRpcMsg));
    if (pHandle->msg == NULL) {
      return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memcpy(pHandle->msg, pMsg, sizeof(SRpcMsg));
    pHandle->msg->pCont = rpcMallocCont(pMsg->contLen);
    if (pHandle->msg->pCont == NULL) {
      taosMemoryFree(pHandle->msg);
      pHandle->msg = NULL;
      return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    }
  } else {
    tqPushEmptyDataRsp(pHandle, vgId);

    void* tmp = pHandle->msg->pCont;
    (void)memcpy(pHandle->msg, pMsg, sizeof(SRpcMsg));
    pHandle->msg->pCont = tmp;
  }

  (void)memcpy(pHandle->msg->pCont, pMsg->pCont, pMsg->contLen);
  pHandle->msg->contLen = pMsg->contLen;
  int32_t ret = taosHashPut(pTq->pPushMgr, pHandle->subKey, strlen(pHandle->subKey), &pHandle, POINTER_BYTES);
  tqDebug("vgId:%d data is over, ret:%d, consumerId:0x%" PRIx64 ", register to pHandle:%p, pCont:%p, len:%d", vgId, ret,
          pHandle->consumerId, pHandle, pHandle->msg->pCont, pHandle->msg->contLen);
  if (ret != 0) {
    rpcFreeCont(pHandle->msg->pCont);
    taosMemoryFree(pHandle->msg);
    pHandle->msg = NULL;
  }
  return ret;
}

void tqUnregisterPushHandle(STQ* pTq, void *handle) {
  if (pTq == NULL || handle == NULL) {
    return;
  }
  STqHandle *pHandle = (STqHandle*)handle;
  int32_t    vgId = TD_VID(pTq->pVnode);

  if(taosHashGetSize(pTq->pPushMgr) <= 0) {
    return;
  }
  int32_t ret = taosHashRemove(pTq->pPushMgr, pHandle->subKey, strlen(pHandle->subKey));
  tqInfo("vgId:%d remove pHandle:%p,ret:%d consumer Id:0x%" PRIx64, vgId, pHandle, ret, pHandle->consumerId);

  if(ret == 0 && pHandle->msg != NULL) {
    tqPushEmptyDataRsp(pHandle, vgId);

    rpcFreeCont(pHandle->msg->pCont);
    taosMemoryFree(pHandle->msg);
    pHandle->msg = NULL;
  }
}
