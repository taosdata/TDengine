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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "qmInt.h"

int32_t qmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDCreateQnodeReq createReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to create qnode since %s, input:%d cur:%d", terrstr(), createReq.dnodeId, pDnode->dnodeId);
    return -1;
  } else {
    return qmOpen(pWrapper);
  }
}

int32_t qmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDDropQnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  } else {
    return qmDrop(pWrapper);
  }
}

void qmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY, (NodeMsgFp)qmProcessQueryMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, (NodeMsgFp)qmProcessQueryMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH_RSP, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);

  dndSetMsgHandle(pWrapper, TDMT_VND_RES_READY, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_TASKS_STATUS, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_CANCEL_TASK, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);
  dndSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES, (NodeMsgFp)qmProcessFetchMsg, QND_VGID);
}
