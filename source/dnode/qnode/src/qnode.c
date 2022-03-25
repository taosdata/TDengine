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

#include "executor.h"
#include "qndInt.h"
#include "query.h"
#include "qworker.h"

SQnode *qndOpen(const SQnodeOpt *pOption) {
  SQnode *pQnode = taosMemoryCalloc(1, sizeof(SQnode));
  if (NULL == pQnode) {
    qError("calloc SQnode failed");
    return NULL;
  }

  if (qWorkerInit(NODE_TYPE_QNODE, pQnode->qndId, NULL, (void **)&pQnode->pQuery, &pOption->msgCb)) {
    taosMemoryFreeClear(pQnode);
    return NULL;
  }

  pQnode->msgCb = pOption->msgCb;
  return pQnode;
}

void qndClose(SQnode *pQnode) {
  qWorkerDestroy((void **)&pQnode->pQuery);

  taosMemoryFree(pQnode);
}

int32_t qndGetLoad(SQnode *pQnode, SQnodeLoad *pLoad) { return 0; }

int32_t qndProcessQueryMsg(SQnode *pQnode, SRpcMsg *pMsg) {
  qTrace("message in query queue is processing");
  SReadHandle handle = {0};

  switch (pMsg->msgType) {
    case TDMT_VND_QUERY: {
      return qWorkerProcessQueryMsg(&handle, pQnode->pQuery, pMsg);
    }
    case TDMT_VND_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pQnode->pQuery, pMsg);
    default:
      qError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

int32_t qndProcessFetchMsg(SQnode *pQnode, SRpcMsg *pMsg) {
  qTrace("message in fetch queue is processing");
  switch (pMsg->msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_FETCH_RSP:
      return qWorkerProcessFetchRsp(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_RES_READY:
      return qWorkerProcessReadyMsg(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_TASKS_STATUS:
      return qWorkerProcessStatusMsg(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_CANCEL_TASK:
      return qWorkerProcessCancelMsg(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_DROP_TASK:
      return qWorkerProcessDropMsg(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_SHOW_TABLES:
      return qWorkerProcessShowMsg(pQnode, pQnode->pQuery, pMsg);
    case TDMT_VND_SHOW_TABLES_FETCH:
      // return vnodeGetTableList(pQnode, pMsg);
    case TDMT_VND_TABLE_META:
      // return vnodeGetTableMeta(pQnode, pMsg);
    case TDMT_VND_CONSUME:
      // return tqProcessConsumeReq(pQnode->pTq, pMsg);
    default:
      qError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}
