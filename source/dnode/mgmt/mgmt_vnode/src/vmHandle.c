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

void vmGetVnodeLoads(SVnodeMgmt *pMgmt, SMonVloadInfo *pInfo) {
  pInfo->pVloads = taosArrayInit(pMgmt->state.totalVnodes, sizeof(SVnodeLoad));
  if (pInfo->pVloads == NULL) return;

  taosThreadRwlockRdlock(&pMgmt->lock);

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

  taosThreadRwlockUnlock(&pMgmt->lock);
}

void vmGetMonitorInfo(SVnodeMgmt *pMgmt, SMonVmInfo *pInfo) {
  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pMgmt, &vloads);

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

int32_t vmProcessGetMonitorInfoReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMonVmInfo vmInfo = {0};
  vmGetMonitorInfo(pMgmt, &vmInfo);
  dmGetMonitorSystemInfo(&vmInfo.sys);
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
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  tFreeSMonVmInfo(&vmInfo);
  return 0;
}

int32_t vmProcessGetLoadsReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pMgmt, &vloads);

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
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  tFreeSMonVloadInfo(&vloads);
  return 0;
}

static void vmGenerateVnodeCfg(SCreateVnodeReq *pCreate, SVnodeCfg *pCfg) {
  memcpy(pCfg, &vnodeCfgDefault, sizeof(SVnodeCfg));

  pCfg->vgId = pCreate->vgId;
  tstrncpy(pCfg->dbname, pCreate->db, sizeof(pCfg->dbname));
  pCfg->dbId = pCreate->dbUid;
  pCfg->szPage = pCreate->pageSize * 1024;
  pCfg->szCache = pCreate->pages;
  pCfg->szBuf = (uint64_t)pCreate->buffer * 1024 * 1024;
  pCfg->isWeak = true;
  pCfg->tsdbCfg.compression = pCreate->compression;
  pCfg->tsdbCfg.precision = pCreate->precision;
  pCfg->tsdbCfg.days = pCreate->daysPerFile;
  pCfg->tsdbCfg.keep0 = pCreate->daysToKeep0;
  pCfg->tsdbCfg.keep1 = pCreate->daysToKeep1;
  pCfg->tsdbCfg.keep2 = pCreate->daysToKeep2;
  pCfg->tsdbCfg.minRows = pCreate->minRows;
  pCfg->tsdbCfg.maxRows = pCreate->maxRows;
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

static void vmGenerateWrapperCfg(SVnodeMgmt *pMgmt, SCreateVnodeReq *pCreate, SWrapperCfg *pCfg) {
  pCfg->vgId = pCreate->vgId;
  pCfg->vgVersion = pCreate->vgVersion;
  pCfg->dropped = 0;
  snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCreate->vgId);
}

int32_t vmProcessCreateVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SCreateVnodeReq createReq = {0};
  int32_t         code = -1;
  char            path[TSDB_FILENAME_LEN] = {0};

  if (tDeserializeSCreateVnodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dDebug("vgId:%d, create vnode req is received, tsma:%d", createReq.vgId, createReq.isTsma);

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

  SVnode *pImpl = vnodeOpen(path, pMgmt->pTfs, pMgmt->msgCb);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    code = terrno;
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

int32_t vmProcessDropVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SDropVnodeReq dropReq = {0};
  if (tDeserializeSDropVnodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
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

SArray *vmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(32, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MON_VM_INFO, vmPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MON_VM_LOAD, vmPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;

  // Requests handled by VNODE
  if (dmSetMgmtHandle(pArray, TDMT_VND_SUBMIT, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY, vmPutNodeMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_CONTINUE, vmPutNodeMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FETCH, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_TABLE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_UPDATE_TAG_VAL, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLE_META, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLES_META, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_CONSUME, vmPutNodeMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_QUERY, vmPutNodeMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_CONNECT, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_DISCONNECT, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  // if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_SET_CUR, vmPutNodeMsgToWriteQueue, 0)== NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_RES_READY, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASKS_STATUS, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CANCEL_TASK, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TASK, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_STB, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_STB, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_STB, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_TABLE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TABLE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_SMA, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CANCEL_SMA, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_SMA, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SUBMIT_RSMA, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_VG_CHANGE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_VG_DELETE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CONSUME, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_DEPLOY, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_HEARTBEAT, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_PIPE_EXEC, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_MERGE_EXEC, vmPutNodeMsgToMergeQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_WRITE_EXEC, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TRIGGER, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_RUN, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_DISPATCH, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_RECOVER, vmPutNodeMsgToFetchQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_VNODE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMPACT_VNODE, vmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_VNODE, vmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_VNODE, vmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_TIMEOUT, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_PING, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_PING_REPLY, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_CLIENT_REQUEST, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_CLIENT_REQUEST_REPLY, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_REQUEST_VOTE, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_REQUEST_VOTE_REPLY, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_APPEND_ENTRIES, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_APPEND_ENTRIES_REPLY, vmPutNodeMsgToSyncQueue, 0) == NULL) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
