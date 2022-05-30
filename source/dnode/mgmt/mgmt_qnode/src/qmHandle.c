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

void qmGetMonitorInfo(SQnodeMgmt *pMgmt, SMonQmInfo *qmInfo) {
  SQnodeLoad qload = {0};
  qndGetLoad(pMgmt->pQnode, &qload);

}

int32_t qmProcessGetMonitorInfoReq(SQnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMonQmInfo qmInfo = {0};
  qmGetMonitorInfo(pMgmt, &qmInfo);
  dmGetMonitorSystemInfo(&qmInfo.sys);
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
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  tFreeSMonQmInfo(&qmInfo);
  return 0;
}

int32_t qmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDCreateQnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create qnode since %s", terrstr());
    return -1;
  }

  bool deployed = true;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write qnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t qmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDDropQnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  }

  bool deployed = false;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write qnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

SArray *qmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MON_QM_INFO, qmPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;

  // Requests handled by VNODE
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY, qmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_CONTINUE, qmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FETCH, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FETCH_RSP, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_HEARTBEAT, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_CANCEL_TASK, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TASK, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
