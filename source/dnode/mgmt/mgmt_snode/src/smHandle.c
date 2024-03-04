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

void smGetMonitorInfo(SSnodeMgmt *pMgmt, SMonSmInfo *smInfo) {}

int32_t smProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create snode since %s", terrstr());
    tFreeSMCreateQnodeReq(&createReq);
    return -1;
  }

  bool deployed = true;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write snode file since %s", terrstr());
    tFreeSMCreateQnodeReq(&createReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&createReq);
  return 0;
}

int32_t smProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop snode since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  bool deployed = false;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write snode file since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&dropReq);
  return 0;
}

SArray *smGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_UPDATE, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DEPLOY, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DROP, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_RUN, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DISPATCH, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DISPATCH_RSP, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_RETRIEVE, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_RETRIEVE_RSP, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_PAUSE, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_RESUME, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_STOP, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_CHECK, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_CHECK_RSP, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_CHECKPOINT_READY, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_CHECKPOINT_READY_RSP, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_RESET, smPutNodeMsgToMgmtQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_HEARTBEAT_RSP, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_REQ_CHKPT_RSP, smPutNodeMsgToStreamQueue, 1) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
