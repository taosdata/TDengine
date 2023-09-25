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

  qload.dnodeId = pMgmt->pData->dnodeId;
}

void qmGetQnodeLoads(SQnodeMgmt *pMgmt, SQnodeLoad *pInfo) {
  qndGetLoad(pMgmt->pQnode, pInfo);

  pInfo->dnodeId = pMgmt->pData->dnodeId;
}

int32_t qmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDCreateQnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create qnode since %s", terrstr());
    tFreeSMCreateQnodeReq(&createReq);
    return -1;
  }

  bool deployed = true;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write qnode file since %s", terrstr());
    tFreeSMCreateQnodeReq(&createReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&createReq);
  return 0;
}

int32_t qmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDDropQnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop qnode since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  bool deployed = false;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write qnode file since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&dropReq);
  return 0;
}

SArray *qmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  // Requests handled by VNODE
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY, qmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_QUERY, qmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_CONTINUE, qmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_FETCH, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH_RSP, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_HEARTBEAT, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SCH_CANCEL_TASK, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_DROP_TASK, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_TASK_NOTIFY, qmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
