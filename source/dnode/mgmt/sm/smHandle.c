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

void smGetMonitorInfo(SMgmtWrapper *pWrapper, SMonSmInfo *smInfo) {}

int32_t smProcessGetMonSmInfoReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq) {
  SMonSmInfo smInfo = {0};
  smGetMonitorInfo(pWrapper, &smInfo);
  dmGetMonitorSysInfo(&smInfo.sys);
  monGetLogs(&smInfo.log);

  int32_t rspLen = tSerializeSMonSmInfo(NULL, 0, &smInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonSmInfo(pRsp, rspLen, &smInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonSmInfo(&smInfo);
  return 0;
}

int32_t smProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create snode since %s", terrstr());
    return -1;
  } else {
    return smOpen(pWrapper);
  }
}

int32_t smProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop snode since %s", terrstr());
    return -1;
  } else {
    return smDrop(pWrapper);
  }
}

void smInitMsgHandle(SMgmtWrapper *pWrapper) {
  dndSetMsgHandle(pWrapper, TDMT_MON_SM_INFO, smProcessMonitorMsg, DEFAULT_HANDLE);

  // Requests handled by SNODE
  dndSetMsgHandle(pWrapper, TDMT_SND_TASK_DEPLOY, smProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_SND_TASK_EXEC, smProcessExecMsg, DEFAULT_HANDLE);
}
