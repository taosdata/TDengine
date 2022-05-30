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

int32_t mndProcessQueryMsg(SRpcMsg *pMsg) {
  int32_t     code = -1;
  SMnode     *pMnode = pMsg->info.node;
  SReadHandle handle = {.mnd = pMnode, .pMsgCb = &pMnode->msgCb};

  mTrace("msg:%p, in query queue is processing", pMsg);
  switch (pMsg->msgType) {
    case TDMT_VND_QUERY:
      code = qWorkerProcessQueryMsg(&handle, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_VND_QUERY_CONTINUE:
      code = qWorkerProcessCQueryMsg(&handle, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_VND_FETCH:
      code = qWorkerProcessFetchMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_VND_DROP_TASK:
      code = qWorkerProcessDropMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    case TDMT_VND_QUERY_HEARTBEAT:
      code = qWorkerProcessHbMsg(pMnode, pMnode->pQuery, pMsg, 0);
      break;
    default:
      terrno = TSDB_CODE_VND_APP_ERROR;
      mError("unknown msg type:%d in query queue", pMsg->msgType);
  }

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  return code;
}

int32_t mndInitQuery(SMnode *pMnode) {
  if (qWorkerInit(NODE_TYPE_MNODE, MNODE_HANDLE, NULL, (void **)&pMnode->pQuery, &pMnode->msgCb) != 0) {
    mError("failed to init qworker in mnode since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TDMT_VND_QUERY, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_CONTINUE, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_FETCH, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TASK, mndProcessQueryMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_HEARTBEAT, mndProcessQueryMsg);

  return 0;
}

void mndCleanupQuery(SMnode *pMnode) { qWorkerDestroy((void **)&pMnode->pQuery); }
