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
#include "bmInt.h"

void bmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonBmInfo *bmInfo) {
}

int32_t bmProcessGetMonBmInfoReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq) {
  SMonBmInfo bmInfo = {0};
  bmGetMonitorInfo(pWrapper, &bmInfo);
  dmGetMonitorSysInfo(&bmInfo.sys);
  monGetLogs(&bmInfo.log);

  int32_t rspLen = tSerializeSMonBmInfo(NULL, 0, &bmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonBmInfo(pRsp, rspLen, &bmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonBmInfo(&bmInfo);
  return 0;
}

int32_t bmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDCreateBnodeReq createReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to create bnode since %s, input:%d cur:%d", terrstr(), createReq.dnodeId, pDnode->dnodeId);
    return -1;
  } else {
    return bmOpen(pWrapper);
  }
}

int32_t bmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDDropBnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  } else {
    return bmDrop(pWrapper);
  }
}

void bmInitMsgHandle(SMgmtWrapper *pWrapper) {
  dndSetMsgHandle(pWrapper, TDMT_MON_BM_INFO, bmProcessMonitorMsg, DEFAULT_HANDLE);
}
