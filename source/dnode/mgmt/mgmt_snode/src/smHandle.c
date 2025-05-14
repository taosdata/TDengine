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
  int32_t          code = 0;
  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSDCreateSNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if (pInput->pData->dnodeId != 0 && createReq.snodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to create snode since %s", tstrerror(code));
    goto _exit;
  }

  bool deployed = true;
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = terrno;
    dError("failed to create json object since %s", tstrerror(code));
    goto _exit;
  }

  if (tjsonAddDoubleToObject(pJson, "deployed", deployed) < 0) {
    code = terrno;
    dError("failed to add deployed to json object since %s", tstrerror(code));
    goto _exit;
  }
  if (tjsonAddIntegerToObject(pJson, "replicaId", createReq.replicaId) < 0) {
    code = terrno;
    dError("failed to add replicaId to json object since %s", tstrerror(code));
    goto _exit;
  }

  char path[TSDB_FILENAME_LEN];
  snprintf(path, TSDB_FILENAME_LEN, "%s%ssnode%d", pInput->path, TD_DIRSEP, createReq.snodeId);
  
  if ((code = dmWriteFileJson(path, pInput->name, pJson)) != 0) {
    dError("failed to write snode file since %s", tstrerror(code));
    goto _exit;
  }

_exit:

  tFreeSDCreateSnodeReq(&createReq);
  
  return code;
}

int32_t smProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t        code = 0;
  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;

    return code;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop snode since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&dropReq);
    return code;
  }

  char path[TSDB_FILENAME_LEN];
  snprintf(path, TSDB_FILENAME_LEN, "%s%ssnode%d", pInput->path, TD_DIRSEP, dropReq.dnodeId);

  bool deployed = false;
  if ((code = dmWriteFile(path, pInput->name, deployed)) != 0) {
    dError("failed to write snode file since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&dropReq);
    return code;
  }

  tFreeSMCreateQnodeReq(&dropReq);
  return 0;
}

SArray *smGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_CALC, smPutMsgToRunnerQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_FETCH_FROM_RUNNER, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_FETCH_FROM_CACHE, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_PULL_RSP, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_CALC_RSP, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;

  code = 0;
  
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
