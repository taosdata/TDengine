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
#include "smInt.h"

int32_t smProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to create snode since %s, input:%d cur:%d", terrstr(), createReq.dnodeId, pDnode->dnodeId);
    return -1;
  } else {
    return smOpen(pWrapper);
  }
}

int32_t smProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to drop snode since %s", terrstr());
    return -1;
  } else {
    return smDrop(pWrapper);
  }
}

void smInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by SNODE
  dndSetMsgHandle(pWrapper, TDMT_SND_TASK_DEPLOY, (NodeMsgFp)smProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_SND_TASK_EXEC, (NodeMsgFp)smProcessExecMsg, 0);
}
