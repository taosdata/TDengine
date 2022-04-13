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

void qmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonQmInfo *qmInfo) {}

int32_t qmProcessGetMonQmInfoReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq) {
  SMonQmInfo qmInfo = {0};
  qmGetMonitorInfo(pWrapper, &qmInfo);
  dmGetMonitorSysInfo(&qmInfo.sys);
  monGetLogs(&qmInfo.log);

  int32_t rspLen = tSerializeSMonQmInfo(NULL, 0, &qmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonQmInfo(pRsp, rspLen, &qmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonQmInfo(&qmInfo);
  return 0;
}

int32_t qmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDCreateQnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create qnode since %s", terrstr());
    return -1;
  } else {
    return dndOpenNode(pWrapper);
  }
}

int32_t qmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDDropQnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  } else {
    // dndCloseNode(pWrapper);
    return qmDrop(pWrapper);
  }
}

void qmInitMsgHandle(SMgmtWrapper *pWrapper) {
  dndSetMsgHandle(pWrapper, TDMT_MON_QM_INFO, qmProcessMonitorMsg, DEFAULT_HANDLE);

  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY, qmProcessQueryMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, qmProcessQueryMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH, qmProcessFetchMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH_RSP, qmProcessFetchMsg, QNODE_HANDLE);

  dndSetMsgHandle(pWrapper, TDMT_VND_RES_READY, qmProcessFetchMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_TASKS_STATUS, qmProcessFetchMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_CANCEL_TASK, qmProcessFetchMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, qmProcessFetchMsg, QNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES, qmProcessFetchMsg, QNODE_HANDLE);
}
