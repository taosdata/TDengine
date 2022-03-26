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
#include "mndMnode.h"
#include "executor.h"
#include "qworker.h"

int32_t mndProcessQueryMsg(SNodeMsg *pReq) {
  mTrace("message in query queue is processing");
  SMnode *pMnode = pReq->pNode;
  SReadHandle handle = {0};

  switch (pReq->rpcMsg.msgType) {
    case TDMT_VND_QUERY:
      return qWorkerProcessQueryMsg(&handle, pMnode->pQuery, &pReq->rpcMsg);
    case TDMT_VND_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pMnode->pQuery, &pReq->rpcMsg);
    default:
      mError("unknown msg type:%d in query queue", pReq->rpcMsg.msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

int32_t mndProcessFetchMsg(SNodeMsg *pReq) {
  mTrace("message in fetch queue is processing");
  SMnode *pMnode = pReq->pNode;
  
  switch (pReq->rpcMsg.msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pMnode, pMnode->pQuery, &pReq->rpcMsg);
    case TDMT_VND_DROP_TASK:
      return qWorkerProcessDropMsg(pMnode, pMnode->pQuery, &pReq->rpcMsg);
    case TDMT_VND_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pMnode, pMnode->pQuery, &pReq->rpcMsg);
    default:
      mError("unknown msg type:%d in fetch queue", pReq->rpcMsg.msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

int32_t mndInitQuery(SMnode *pMnode) {
  int32_t code = qWorkerInit(NODE_TYPE_MNODE, MND_VGID, NULL, (void **)&pMnode->pQuery, &pMnode->msgCb);
  if (code) {
    return code;
  }

  mndSetMsgHandle(pMnode, TDMT_VND_QUERY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_CONTINUE, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_FETCH, mndProcessFetchMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TASK, mndProcessFetchMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_HEARTBEAT, mndProcessFetchMsg);

  return 0;
}

void mndCleanupQuery(SMnode *pMnode) { qWorkerDestroy((void **)&pMnode->pQuery); }

