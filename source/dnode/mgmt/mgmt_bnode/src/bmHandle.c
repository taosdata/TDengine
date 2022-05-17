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

static void bmGetMonitorInfo(SBnodeMgmt *pMgmt, SMonBmInfo *bmInfo) {}

int32_t bmProcessGetMonBmInfoReq(SBnodeMgmt *pMgmt, SRpcMsg *pReq) {
  SMonBmInfo bmInfo = {0};
  bmGetMonitorInfo(pMgmt, &bmInfo);
  dmGetMonitorSystemInfo(&bmInfo.sys);
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
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;
  tFreeSMonBmInfo(&bmInfo);
  return 0;
}

int32_t bmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SRpcMsg *pReq = pMsg;

  SDCreateBnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create bnode since %s, input:%d cur:%d", terrstr(), createReq.dnodeId, pInput->pData->dnodeId);
    return -1;
  }

  bool deployed = true;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write bnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t bmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SRpcMsg *pReq = pMsg;

  SDDropBnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  }

  bool deployed = false;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write bnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

SArray *bmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(2, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MON_BM_INFO, bmPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
