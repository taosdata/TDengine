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
  return qWorkerPreprocessQueryMsg(pMnode->pQuery, pMsg, false);
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
    case TDMT_SCH_TASK_NOTIFY:
      code = qWorkerProcessNotifyMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    default:
      terrno = TSDB_CODE_APP_ERROR;
      mError("unknown msg type:%d in query queue", pMsg->msgType);
  }

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  return code;
}


static FORCE_INLINE void mnodeFreeSBatchRspMsg(void* p) {
  if (NULL == p) {
    return;
  }

  SBatchRspMsg* pRsp = (SBatchRspMsg*)p;
  rpcFreeCont(pRsp->msg);
}

int32_t mndProcessBatchMetaMsg(SRpcMsg *pMsg) {
  int32_t    code = 0;
  int32_t    rspSize = 0;
  SBatchReq  batchReq = {0};
  SBatchMsg req = {0};
  SBatchRspMsg rsp = {0};
  SBatchRsp batchRsp = {0};
  SRpcMsg   reqMsg = *pMsg;
  void     *pRsp = NULL;
  SMnode   *pMnode = pMsg->info.node;

  if (tDeserializeSBatchReq(pMsg->pCont, pMsg->contLen, &batchReq)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    mError("tDeserializeSBatchReq failed");
    goto _exit;
  }

  int32_t    msgNum = taosArrayGetSize(batchReq.pMsgs);
  if (msgNum >= MAX_META_MSG_IN_BATCH) {
    code = TSDB_CODE_INVALID_MSG;
    mError("too many msgs %d in mnode batch meta req", msgNum);
    goto _exit;
  }

  batchRsp.pRsps = taosArrayInit(msgNum, sizeof(SBatchRspMsg));
  if (NULL == batchRsp.pRsps) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t i = 0; i < msgNum; ++i) {
    SBatchMsg* req = taosArrayGet(batchReq.pMsgs, i);

    reqMsg.msgType = req->msgType;
    reqMsg.pCont = req->msg;
    reqMsg.contLen = req->msgLen;
    reqMsg.info.rsp = NULL;
    reqMsg.info.rspLen = 0;

    MndMsgFp fp = pMnode->msgFp[TMSG_INDEX(req->msgType)];
    if (fp == NULL) {
      mError("msg:%p, failed to get msg handle, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      taosArrayDestroy(batchRsp.pRsps);
      return -1;
    }

    if ((*fp)(&reqMsg)) {
      rsp.rspCode = terrno;
    } else {
      rsp.rspCode = 0;
    }
    rsp.msgIdx = req->msgIdx;
    rsp.reqType = reqMsg.msgType;
    rsp.msgLen = reqMsg.info.rspLen;
    rsp.msg = reqMsg.info.rsp;

    taosArrayPush(batchRsp.pRsps, &rsp);
  }

  rspSize = tSerializeSBatchRsp(NULL, 0, &batchRsp);
  if (rspSize < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (tSerializeSBatchRsp(pRsp, rspSize, &batchRsp) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:

  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspSize;

  if (code) {
    mError("mnd get batch meta failed cause of %s", tstrerror(code));
  }

  taosArrayDestroyEx(batchReq.pMsgs, tFreeSBatchReqMsg);
  taosArrayDestroyEx(batchRsp.pRsps, mnodeFreeSBatchRspMsg);

  return code;
}

int32_t mndInitQuery(SMnode *pMnode) {
  if (qWorkerInit(NODE_TYPE_MNODE, MNODE_HANDLE, (void **)&pMnode->pQuery, &pMnode->msgCb) != 0) {
    mError("failed to init qworker in mnode since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TDMT_SCH_QUERY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_MERGE_QUERY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_QUERY_CONTINUE, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_FETCH, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_MERGE_FETCH, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_TASK_NOTIFY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_DROP_TASK, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_SCH_QUERY_HEARTBEAT, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_BATCH_META, mndProcessBatchMetaMsg);

  return 0;
}

void mndCleanupQuery(SMnode *pMnode) { qWorkerDestroy((void **)&pMnode->pQuery); }
