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

#include "mndQuery.h"
#include "executor.h"
#include "mndMnode.h"
#include "qworker.h"

int32_t mndPreProcessQueryMsg(SRpcMsg *pMsg) {
  if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) return 0;
  SMnode *pMnode = pMsg->info.node;
  return qWorkerPreprocessQueryMsg(pMnode->pQuery, pMsg);
}

void mndPostProcessQueryMsg(SRpcMsg *pMsg) {
  if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) return;
  SMnode *pMnode = pMsg->info.node;
  qWorkerAbortPreprocessQueryMsg(pMnode->pQuery, pMsg);
}

int32_t mndProcessQueryMsg(SRpcMsg *pMsg) {
  int32_t     code = -1;
  SMnode     *pMnode = pMsg->info.node;
  SReadHandle handle = {.mnd = pMnode, .pMsgCb = &pMnode->msgCb};

  mTrace("msg:%p, in query queue is processing", pMsg);
  switch (pMsg->msgType) {
    case TDMT_SCH_QUERY:
    case TDMT_SCH_MERGE_QUERY:
      code = qWorkerProcessQueryMsg(&handle, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_SCH_QUERY_CONTINUE:
      code = qWorkerProcessCQueryMsg(&handle, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_SCH_FETCH:
    case TDMT_SCH_MERGE_FETCH:
      code = qWorkerProcessFetchMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_SCH_DROP_TASK:
      code = qWorkerProcessDropMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_SCH_QUERY_HEARTBEAT:
      code = qWorkerProcessHbMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    default:
      terrno = TSDB_CODE_VND_APP_ERROR;
      mError("unknown msg type:%d in query queue", pMsg->msgType);
  }

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  return code;
}

int32_t mndProcessBatchMetaMsg(SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t offset = 0;
  int32_t rspSize = 0;
  SBatchReq *batchReq = (SBatchReq*)pMsg->pCont;
  int32_t msgNum = ntohl(batchReq->msgNum);
  offset += sizeof(SBatchReq);
  SBatchMsg req = {0};
  SBatchRsp rsp = {0};
  SRpcMsg reqMsg = *pMsg;
  SRpcMsg rspMsg = {0};
  void* pRsp = NULL;
  SMnode *pMnode = pMsg->info.node;

  SArray* batchRsp = taosArrayInit(msgNum, sizeof(SBatchRsp));
  if (NULL == batchRsp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  
  for (int32_t i = 0; i < msgNum; ++i) {
    req.msgIdx = ntohl(*(int32_t*)((char*)pMsg->pCont + offset));
    offset += sizeof(req.msgIdx);

    req.msgType = ntohl(*(int32_t*)((char*)pMsg->pCont + offset));
    offset += sizeof(req.msgType);

    req.msgLen = ntohl(*(int32_t*)((char*)pMsg->pCont + offset));
    offset += sizeof(req.msgLen);

    req.msg = (char*)pMsg->pCont + offset;
    offset += req.msgLen;

    reqMsg.msgType = req.msgType;
    reqMsg.pCont = req.msg;
    reqMsg.contLen = req.msgLen;
    reqMsg.info.rsp = NULL;
    reqMsg.info.rspLen = 0;

    MndMsgFp fp = pMnode->msgFp[TMSG_INDEX(req.msgType)];
    if (fp == NULL) {
      mError("msg:%p, failed to get msg handle, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      return -1;
    }

    if ((*fp)(&reqMsg)) {
      rsp.rspCode = terrno;
    } else {
      rsp.rspCode = 0;
    }
    rsp.msgIdx = req.msgIdx;
    rsp.reqType = reqMsg.msgType;
    rsp.msgLen = reqMsg.info.rspLen;
    rsp.msg = reqMsg.info.rsp;
    
    taosArrayPush(batchRsp, &rsp);

    rspSize += sizeof(rsp) + rsp.msgLen - POINTER_BYTES;
  }

  rspSize += sizeof(int32_t);
  offset = 0;
  
  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  *(int32_t*)((char*)pRsp + offset) = htonl(msgNum);
  offset += sizeof(msgNum);
  for (int32_t i = 0; i < msgNum; ++i) {
    SBatchRsp *p = taosArrayGet(batchRsp, i);
    
    *(int32_t*)((char*)pRsp + offset) = htonl(p->reqType);
    offset += sizeof(p->reqType);
    *(int32_t*)((char*)pRsp + offset) = htonl(p->msgIdx);
    offset += sizeof(p->msgIdx);
    *(int32_t*)((char*)pRsp + offset) = htonl(p->msgLen);
    offset += sizeof(p->msgLen);
    *(int32_t*)((char*)pRsp + offset) = htonl(p->rspCode);
    offset += sizeof(p->rspCode);
    memcpy((char*)pRsp + offset, p->msg, p->msgLen);
    offset += p->msgLen;

    rpcFreeCont(p->msg);
  }

  taosArrayDestroy(batchRsp);
  batchRsp = NULL;

_exit:

  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspSize;

  if (code) {
    mError("mnd get batch meta failed cause of %s", tstrerror(code));
  }

  taosArrayDestroyEx(batchRsp, tFreeSBatchRsp);

  return code;
}

int32_t mndInitQuery(SMnode *pMnode) {
  if (qWorkerInit(NODE_TYPE_MNODE, MNODE_HANDLE, NULL, (void **)&pMnode->pQuery, &pMnode->msgCb) != 0) {
    mError("failed to init qworker in mnode since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TDMT_SCH_QUERY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_MERGE_QUERY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_QUERY_CONTINUE, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_FETCH, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_MERGE_FETCH, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_DROP_TASK, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_QUERY_HEARTBEAT, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_BATCH_META, mndProcessBatchMetaMsg);

  return 0;
}

void mndCleanupQuery(SMnode *pMnode) { qWorkerDestroy((void **)&pMnode->pQuery); }
