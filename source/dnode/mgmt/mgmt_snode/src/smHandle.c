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
#include "stream.h"

SSnodeInfo gSnode = {0};


void smGetMonitorInfo(SSnodeMgmt *pMgmt, SMonSmInfo *smInfo) {}

static int32_t epToJson(const void* pObj, SJson* pJson) {
  const SEp* pNode = (const SEp*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, "fqdn", pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, "port", pNode->port);
  }

  return code;
}

static int32_t jsonToEp(const SJson* pJson, void* pObj) {
  SEp* pNode = (SEp*)pObj;

  int32_t code = tjsonGetStringValue(pJson, "fqdn", pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonGetSmallIntValue(pJson, "port", &pNode->port);
  }

  return code;
}


void smUpdateSnodeInfo(SDCreateSnodeReq* pReq) {
  taosWLockLatch(&gSnode.snodeLock);
  gSnode.snodeId = pReq->snodeId;
  gSnode.snodeLeaders[0] = pReq->leaders[0];
  gSnode.snodeLeaders[1] = pReq->leaders[1];  
  gSnode.snodeReplica = pReq->replica;
  taosWUnLockLatch(&gSnode.snodeLock);
}

SEpSet* dmGetSynEpset(int32_t leaderId) {
  if (gSnode.snodeId == leaderId && gSnode.snodeReplica.nodeId > 0) {
    return &gSnode.snodeReplica.epSet;
  } 
  for (int32_t i = 0; i < 2; ++i) {
    if (gSnode.snodeLeaders[i].nodeId == leaderId) {
      return &gSnode.snodeLeaders[i].epSet;
    }
  }
  return NULL;
}

int32_t smBuildCreateReqFromJson(SJson *pJson, SDCreateSnodeReq *pReq) {
  SJson* pLeader0 = NULL;
  SJson* pLeader1 = NULL;
  SJson* pReplica = NULL;
  int32_t code = tjsonGetIntValue(pJson, "snodeId", &pReq->snodeId);
  if (TSDB_CODE_SUCCESS == code) {
    pLeader0 = tjsonGetObjectItem(pJson, "leader0");
    if (pLeader0) {
      code = tjsonGetIntValue(pLeader0, "nodeId", &pReq->leaders[0].nodeId);
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonGetTinyIntValue(pLeader0, "inUse", &pReq->leaders[0].epSet.inUse);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonGetTinyIntValue(pLeader0, "numOfEps", &pReq->leaders[0].epSet.numOfEps);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonToArray(pLeader0, "eps", jsonToEp, pReq->leaders[0].epSet.eps, sizeof(SEp));
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pLeader1 = tjsonGetObjectItem(pJson, "leader1");
    if (pLeader1) {
      code = tjsonGetIntValue((pLeader1), "nodeId", &pReq->leaders[1].nodeId);
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonGetTinyIntValue((pLeader1), "inUse", &pReq->leaders[1].epSet.inUse);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonGetTinyIntValue((pLeader1), "numOfEps", &pReq->leaders[1].epSet.numOfEps);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonToArray(pLeader1, "eps", jsonToEp, pReq->leaders[1].epSet.eps, sizeof(SEp));
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pReplica = tjsonGetObjectItem(pJson, "replica");
    if (pReplica) {
      code = tjsonGetIntValue((pReplica), "nodeId", &pReq->replica.nodeId);
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonGetTinyIntValue((pReplica), "inUse", &pReq->replica.epSet.inUse);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonGetTinyIntValue((pReplica), "numOfEps", &pReq->replica.epSet.numOfEps);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = tjsonToArray(pReplica, "eps", jsonToEp, pReq->replica.epSet.eps, sizeof(SEp));
      }
    }
  }

  return code;
}

int32_t smProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t          code = 0;
  int32_t          lino = 0;
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

  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "deployed", deployed));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "snodeId", createReq.snodeId));

  SJson *leader0 = tjsonCreateObject();
  TAOS_CHECK_EXIT(tjsonAddItemToObject(pJson, "leader0", leader0));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(leader0, "nodeId", createReq.leaders[0].nodeId));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(leader0, "inUse", createReq.leaders[0].epSet.inUse));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(leader0, "numOfEps", createReq.leaders[0].epSet.numOfEps));
  TAOS_CHECK_EXIT(tjsonAddArray(leader0, "eps", epToJson, createReq.leaders[0].epSet.eps, sizeof(SEp), createReq.leaders[0].epSet.numOfEps));

  SJson *leader1 = tjsonCreateObject();
  TAOS_CHECK_EXIT(tjsonAddItemToObject(pJson, "leader1", leader1));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(leader1, "nodeId", createReq.leaders[1].nodeId));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(leader1, "inUse", createReq.leaders[1].epSet.inUse));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(leader1, "numOfEps", createReq.leaders[1].epSet.numOfEps));
  TAOS_CHECK_EXIT(tjsonAddArray(leader1, "eps", epToJson, createReq.leaders[1].epSet.eps, sizeof(SEp), createReq.leaders[1].epSet.numOfEps));

  SJson *replica = tjsonCreateObject();
  TAOS_CHECK_EXIT(tjsonAddItemToObject(pJson, "replica", replica));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(replica, "nodeId", createReq.replica.nodeId));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(replica, "inUse", createReq.replica.epSet.inUse));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(replica, "numOfEps", createReq.replica.epSet.numOfEps));
  TAOS_CHECK_EXIT(tjsonAddArray(replica, "eps", epToJson, createReq.replica.epSet.eps, sizeof(SEp), createReq.replica.epSet.numOfEps));

  char path[TSDB_FILENAME_LEN];
  snprintf(path, TSDB_FILENAME_LEN, "%s%ssnode%d", pInput->path, TD_DIRSEP, createReq.snodeId);

  if (taosMulMkDir(path) != 0) {
    code = terrno;
    dError("failed to create dir:%s since %s", path, tstrerror(code));
    goto _exit;
  }

  dInfo("path %s created", path);
  
  if ((code = dmWriteFileJson(path, pInput->name, pJson)) != 0) {
    dError("failed to write snode file since %s", tstrerror(code));
    goto _exit;
  }

  if (createReq.replica.nodeId != gSnode.snodeReplica.nodeId && createReq.replica.nodeId != 0) {
    int32_t ret = streamSyncAllCheckpoints(&createReq.replica.epSet);
    dInfo("[checkpoint] sync all checkpoint from snode %d to replicaId:%d, return:%d", createReq.snodeId, createReq.replica.nodeId, ret);
  }
  smUpdateSnodeInfo(&createReq);

  dInfo("snode %d created, replicaId:%d", createReq.snodeId, createReq.replica.nodeId);

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

  streamDeleteAllCheckpoints();

  bool deployed = false;
  if ((code = dmWriteFile(path, pInput->name, deployed)) != 0) {
    dError("failed to write snode file since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&dropReq);
    return code;
  }

  smUndeploySnodeTasks(true);

  tFreeSMCreateQnodeReq(&dropReq);
  return 0;
}

SArray *smGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_SYNC_CHECKPOINT, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_SYNC_CHECKPOINT_RSP, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;
  
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_DELETE_CHECKPOINT, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_CALC, smPutMsgToRunnerQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_FETCH_FROM_RUNNER, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;
  //if (dmSetMgmtHandle(pArray, TDMT_STREAM_FETCH_FROM_CACHE, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_DROP, smPutMsgToRunnerQueue, 1) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_PULL_RSP, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_CALC_RSP, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_DROP_RSP, smPutMsgToTriggerQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SNODE_DROP_TABLE_RSP, smPutMsgToTriggerQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SND_BATCH_META, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;

  code = 0;
  
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
