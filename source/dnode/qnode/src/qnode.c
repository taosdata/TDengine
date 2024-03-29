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

  if (qWorkerInit(NODE_TYPE_QNODE, pQnode->qndId, (void **)&pQnode->pQuery, &pOption->msgCb)) {
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

int32_t qndGetLoad(SQnode *pQnode, SQnodeLoad *pLoad) {
  SReadHandle  handle = {.pMsgCb = &pQnode->msgCb};
  SQWorkerStat stat = {0};

  int32_t code = qWorkerGetStat(&handle, pQnode->pQuery, &stat);
  if (code) {
    return code;
  }

  pLoad->numOfQueryInQueue = stat.numOfQueryInQueue;
  pLoad->numOfFetchInQueue = stat.numOfFetchInQueue;
  pLoad->timeInQueryQueue = stat.timeInQueryQueue;
  pLoad->timeInFetchQueue = stat.timeInFetchQueue;
  pLoad->cacheDataSize = stat.cacheDataSize;
  pLoad->numOfProcessedQuery = stat.queryProcessed;
  pLoad->numOfProcessedCQuery = stat.cqueryProcessed;
  pLoad->numOfProcessedFetch = stat.fetchProcessed;
  pLoad->numOfProcessedDrop = stat.dropProcessed;
  pLoad->numOfProcessedNotify = stat.notifyProcessed;
  pLoad->numOfProcessedHb = stat.hbProcessed;
  pLoad->numOfProcessedDelete = stat.deleteProcessed;

  return 0;
}

int32_t qndPreprocessQueryMsg(SQnode *pQnode, SRpcMsg *pMsg) {
  if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) {
    return 0;
  }

  return qWorkerPreprocessQueryMsg(pQnode->pQuery, pMsg, false);
}

int32_t qndProcessQueryMsg(SQnode *pQnode, int64_t ts, SRpcMsg *pMsg) {
  int32_t     code = -1;
  SReadHandle handle = {.pMsgCb = &pQnode->msgCb};
  qTrace("message in qnode queue is processing");

  switch (pMsg->msgType) {
    case TDMT_SCH_QUERY:
    case TDMT_SCH_MERGE_QUERY:
      code = qWorkerProcessQueryMsg(&handle, pQnode->pQuery, pMsg, ts);
      break;
    case TDMT_SCH_QUERY_CONTINUE:
      code = qWorkerProcessCQueryMsg(&handle, pQnode->pQuery, pMsg, ts);
      break;
    case TDMT_SCH_FETCH:
    case TDMT_SCH_MERGE_FETCH:
      code = qWorkerProcessFetchMsg(pQnode, pQnode->pQuery, pMsg, ts);
      break;
    case TDMT_SCH_CANCEL_TASK:
      // code = qWorkerProcessCancelMsg(pQnode, pQnode->pQuery, pMsg, ts);
      break;
    case TDMT_SCH_DROP_TASK:
      code = qWorkerProcessDropMsg(pQnode, pQnode->pQuery, pMsg, ts);
      break;
    case TDMT_VND_TMQ_CONSUME:
      // code =  tqProcessConsumeReq(pQnode->pTq, pMsg);
      // break;
    case TDMT_SCH_QUERY_HEARTBEAT:
      code = qWorkerProcessHbMsg(pQnode, pQnode->pQuery, pMsg, ts);
      break;
    case TDMT_SCH_TASK_NOTIFY:
      code = qWorkerProcessNotifyMsg(pQnode, pQnode->pQuery, pMsg, ts);
      break;
    default:
      qError("unknown msg type:%d in qnode queue", pMsg->msgType);
      terrno = TSDB_CODE_APP_ERROR;
  }

  if (code == 0) return TSDB_CODE_ACTION_IN_PROGRESS;
  return code;
}
