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

int32_t smProcessGetMonitorInfoReq(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMonSmInfo smInfo = {0};
  smGetMonitorInfo(pMgmt, &smInfo);
  dmGetMonitorSystemInfo(&smInfo.sys);
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
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  tFreeSMonSmInfo(&smInfo);
  return 0;
}

int32_t smProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create snode since %s", terrstr());
    return -1;
  }

  bool deployed = true;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write snode file since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t smProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop snode since %s", terrstr());
    return -1;
  }

  bool deployed = false;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write snode file since %s", terrstr());
    return -1;
  }

  return 0;
}

SArray *smGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MON_SM_INFO, smPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;

  // Requests handled by SNODE
  if (dmSetMgmtHandle(pArray, TDMT_SND_TASK_DEPLOY, smPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  /*if (dmSetMgmtHandle(pArray, TDMT_SND_TASK_EXEC, smPutNodeMsgToExecQueue, 0) == NULL) goto _OVER;*/

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
