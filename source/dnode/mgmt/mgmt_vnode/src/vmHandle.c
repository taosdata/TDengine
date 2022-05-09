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
#include "vmInt.h"

void vmGetVnodeLoads(SMgmtWrapper *pWrapper, SMonVloadInfo *pInfo) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;

  pInfo->pVloads = taosArrayInit(pMgmt->state.totalVnodes, sizeof(SVnodeLoad));
  if (pInfo->pVloads == NULL) return;

  taosRLockLatch(&pMgmt->latch);

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    SVnodeLoad vload = {0};
    vnodeGetLoad(pVnode->pImpl, &vload);
    taosArrayPush(pInfo->pVloads, &vload);
    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosRUnLockLatch(&pMgmt->latch);
}

void vmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonVmInfo *pInfo) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;

  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pWrapper, &vloads);

  SArray *pVloads = vloads.pVloads;
  if (pVloads == NULL) return;

  int32_t totalVnodes = 0;
  int32_t masterNum = 0;
  int64_t numOfSelectReqs = 0;
  int64_t numOfInsertReqs = 0;
  int64_t numOfInsertSuccessReqs = 0;
  int64_t numOfBatchInsertReqs = 0;
  int64_t numOfBatchInsertSuccessReqs = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pVloads); ++i) {
    SVnodeLoad *pLoad = taosArrayGet(pVloads, i);
    numOfSelectReqs += pLoad->numOfSelectReqs;
    numOfInsertReqs += pLoad->numOfInsertReqs;
    numOfInsertSuccessReqs += pLoad->numOfInsertSuccessReqs;
    numOfBatchInsertReqs += pLoad->numOfBatchInsertReqs;
    numOfBatchInsertSuccessReqs += pLoad->numOfBatchInsertSuccessReqs;
    if (pLoad->syncState == TAOS_SYNC_STATE_LEADER) masterNum++;
    totalVnodes++;
  }

  pInfo->vstat.totalVnodes = totalVnodes;
  pInfo->vstat.masterNum = masterNum;
  pInfo->vstat.numOfSelectReqs = numOfSelectReqs - pMgmt->state.numOfSelectReqs;
  pInfo->vstat.numOfInsertReqs = numOfInsertReqs - pMgmt->state.numOfInsertReqs;
  pInfo->vstat.numOfInsertSuccessReqs = numOfInsertSuccessReqs - pMgmt->state.numOfInsertSuccessReqs;
  pInfo->vstat.numOfBatchInsertReqs = numOfBatchInsertReqs - pMgmt->state.numOfBatchInsertReqs;
  pInfo->vstat.numOfBatchInsertSuccessReqs = numOfBatchInsertSuccessReqs - pMgmt->state.numOfBatchInsertSuccessReqs;
  pMgmt->state.totalVnodes = totalVnodes;
  pMgmt->state.masterNum = masterNum;
  pMgmt->state.numOfSelectReqs = numOfSelectReqs;
  pMgmt->state.numOfInsertReqs = numOfInsertReqs;
  pMgmt->state.numOfInsertSuccessReqs = numOfInsertSuccessReqs;
  pMgmt->state.numOfBatchInsertReqs = numOfBatchInsertReqs;
  pMgmt->state.numOfBatchInsertSuccessReqs = numOfBatchInsertSuccessReqs;

  tfsGetMonitorInfo(pMgmt->pTfs, &pInfo->tfs);
  taosArrayDestroy(pVloads);
}

int32_t vmProcessGetMonVmInfoReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq) {
  SMonVmInfo vmInfo = {0};
  vmGetMonitorInfo(pWrapper, &vmInfo);
  dmGetMonitorSysInfo(&vmInfo.sys);
  monGetLogs(&vmInfo.log);

  int32_t rspLen = tSerializeSMonVmInfo(NULL, 0, &vmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonVmInfo(pRsp, rspLen, &vmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonVmInfo(&vmInfo);
  return 0;
}

int32_t vmProcessGetVnodeLoadsReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq) {
  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pWrapper, &vloads);

  int32_t rspLen = tSerializeSMonVloadInfo(NULL, 0, &vloads);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonVloadInfo(pRsp, rspLen, &vloads);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonVloadInfo(&vloads);
  return 0;
}

static void vmGenerateVnodeCfg(SCreateVnodeReq *pCreate, SVnodeCfg *pCfg) {
  memcpy(pCfg, &vnodeCfgDefault, sizeof(SVnodeCfg));

  pCfg->vgId = pCreate->vgId;
  tstrncpy(pCfg->dbname, pCreate->db, sizeof(pCfg->dbname));
  pCfg->dbId = pCreate->dbUid;
  pCfg->isWeak = true;
  pCfg->tsdbCfg.days = 10;
  pCfg->tsdbCfg.keep0 = 3650;
  pCfg->tsdbCfg.keep1 = 3650;
  pCfg->tsdbCfg.keep2 = 3650;
  for (size_t i = 0; i < taosArrayGetSize(pCreate->pRetensions); ++i) {
    memcpy(&pCfg->tsdbCfg.retentions[i], taosArrayGet(pCreate->pRetensions, i), sizeof(SRetention));
  }
  pCfg->walCfg.vgId = pCreate->vgId;
  pCfg->hashBegin = pCreate->hashBegin;
  pCfg->hashEnd = pCreate->hashEnd;
  pCfg->hashMethod = pCreate->hashMethod;

  pCfg->syncCfg.myIndex = pCreate->selfIndex;
  pCfg->syncCfg.replicaNum = pCreate->replica;
  memset(&pCfg->syncCfg.nodeInfo, 0, sizeof(pCfg->syncCfg.nodeInfo));
  for (int i = 0; i < pCreate->replica; ++i) {
    pCfg->syncCfg.nodeInfo[i].nodePort = pCreate->replicas[i].port;
    snprintf(pCfg->syncCfg.nodeInfo[i].nodeFqdn, sizeof(pCfg->syncCfg.nodeInfo[i].nodeFqdn), "%s",
             pCreate->replicas[i].fqdn);
  }
}

static void vmGenerateWrapperCfg(SVnodesMgmt *pMgmt, SCreateVnodeReq *pCreate, SWrapperCfg *pCfg) {
  pCfg->vgId = pCreate->vgId;
  pCfg->vgVersion = pCreate->vgVersion;
  pCfg->dropped = 0;
  pCfg->dbUid = pCreate->dbUid;
  tstrncpy(pCfg->db, pCreate->db, TSDB_DB_FNAME_LEN);
  snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCreate->vgId);
}

int32_t vmProcessCreateVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg        *pReq = &pMsg->rpcMsg;
  SCreateVnodeReq createReq = {0};
  int32_t         code = -1;
  char            path[TSDB_FILENAME_LEN] = {0};

  if (tDeserializeSCreateVnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dDebug("vgId:%d, create vnode req is received", createReq.vgId);

  SVnodeCfg vnodeCfg = {0};
  vmGenerateVnodeCfg(&createReq, &vnodeCfg);

  SWrapperCfg wrapperCfg = {0};
  vmGenerateWrapperCfg(pMgmt, &createReq, &wrapperCfg);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, createReq.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist", createReq.vgId);
    tFreeSCreateVnodeReq(&createReq);
    vmReleaseVnode(pMgmt, pVnode);
    terrno = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    return -1;
  }

  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vnodeCfg.vgId);
  if (vnodeCreate(path, &vnodeCfg, pMgmt->pTfs) < 0) {
    tFreeSCreateVnodeReq(&createReq);
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    return -1;
  }

  SMsgCb msgCb = pMgmt->pDnode->data.msgCb;
  msgCb.pWrapper = pMgmt->pWrapper;
  msgCb.queueFps[WRITE_QUEUE] = vmPutMsgToWriteQueue;
  msgCb.queueFps[SYNC_QUEUE] = vmPutMsgToSyncQueue;
  msgCb.queueFps[APPLY_QUEUE] = vmPutMsgToApplyQueue;
  msgCb.queueFps[QUERY_QUEUE] = vmPutMsgToQueryQueue;
  msgCb.queueFps[FETCH_QUEUE] = vmPutMsgToFetchQueue;
  msgCb.queueFps[MERGE_QUEUE] = vmPutMsgToMergeQueue;
  msgCb.qsizeFp = vmGetQueueSize;

  SVnode *pImpl = vnodeOpen(path, pMgmt->pTfs, msgCb);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    goto _OVER;
  }

  code = vmOpenVnode(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to open vnode since %s", createReq.vgId, terrstr());
    goto _OVER;
  }

  code = vnodeStart(pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to start sync since %s", createReq.vgId, terrstr());
    goto _OVER;
  }

  code = vmWriteVnodeListToFile(pMgmt);
  if (code != 0) goto _OVER;

_OVER:
  if (code != 0) {
    vnodeClose(pImpl);
    vnodeDestroy(path, pMgmt->pTfs);
  }

  tFreeSCreateVnodeReq(&createReq);
  terrno = code;
  return code;
}

int32_t vmProcessDropVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg      *pReq = &pMsg->rpcMsg;
  SDropVnodeReq dropReq = {0};
  if (tDeserializeSDropVnodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t vgId = dropReq.vgId;
  dDebug("vgId:%d, drop vnode req is received", vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to drop since %s", vgId, terrstr());
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    return -1;
  }

  pVnode->dropped = 1;
  if (vmWriteVnodeListToFile(pMgmt) != 0) {
    pVnode->dropped = 0;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  vmCloseVnode(pMgmt, pVnode);
  vmWriteVnodeListToFile(pMgmt);

  return 0;
}

void vmInitMsgHandle(SMgmtWrapper *pWrapper) {
  dmSetMsgHandle(pWrapper, TDMT_MON_VM_INFO, vmProcessMonitorMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_MON_VM_LOAD, vmProcessMonitorMsg, DEFAULT_HANDLE);

  // Requests handled by VNODE
  dmSetMsgHandle(pWrapper, TDMT_VND_SUBMIT, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_QUERY, vmProcessQueryMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, vmProcessQueryMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_FETCH, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_FETCH_RSP, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_UPDATE_TAG_VAL, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TABLE_META, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TABLES_META, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_MQ_CONSUME, vmProcessQueryMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_MQ_QUERY, vmProcessQueryMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_MQ_CONNECT, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_MQ_DISCONNECT, vmProcessWriteMsg, DEFAULT_HANDLE);
  // dmSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_RES_READY, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TASKS_STATUS, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_CANCEL_TASK, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_DROP_STB, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_CREATE_TABLE, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_DROP_TABLE, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_CREATE_SMA, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_CANCEL_SMA, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_DROP_SMA, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SUBMIT_RSMA, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_MQ_VG_CHANGE, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_CONSUME, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TASK_DEPLOY, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_QUERY_HEARTBEAT, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TASK_PIPE_EXEC, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TASK_MERGE_EXEC, vmProcessMergeMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_TASK_WRITE_EXEC, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_STREAM_TRIGGER, vmProcessFetchMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_ALTER_VNODE, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_COMPACT_VNODE, vmProcessWriteMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE, vmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE, vmProcessMgmtMsg, DEFAULT_HANDLE);

  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_TIMEOUT, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_PING, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_PING_REPLY, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_CLIENT_REQUEST, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_CLIENT_REQUEST_REPLY, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_REQUEST_VOTE, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_REQUEST_VOTE_REPLY, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_APPEND_ENTRIES, vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_APPEND_ENTRIES_REPLY, vmProcessSyncMsg, DEFAULT_HANDLE);
}
