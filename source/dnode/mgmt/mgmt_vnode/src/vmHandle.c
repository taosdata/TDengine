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

void vmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonVmInfo *pInfo) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;

  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pWrapper, &vloads);
  if (vloads.pVloads == NULL) return;

  int32_t totalVnodes = 0;
  int32_t masterNum = 0;
  int64_t numOfSelectReqs = 0;
  int64_t numOfInsertReqs = 0;
  int64_t numOfInsertSuccessReqs = 0;
  int64_t numOfBatchInsertReqs = 0;
  int64_t numOfBatchInsertSuccessReqs = 0;

  for (int32_t i = 0; i < taosArrayGetSize(vloads.pVloads); ++i) {
    SVnodeLoad *pLoad = taosArrayGet(vloads.pVloads, i);
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
  pMgmt->state = pInfo->vstat;

  taosArrayDestroy(vloads.pVloads);
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
  strcpy(pCfg->dbname, pCreate->db);
  pCfg->wsize = 16 * 1024 * 1024;
  pCfg->ssize = 1024;
  pCfg->lsize = 1024 * 1024;
  pCfg->streamMode = pCreate->streamMode;
  pCfg->isWeak = true;
  pCfg->tsdbCfg.keep2 = pCreate->durationToKeep0;
  pCfg->tsdbCfg.keep0 = pCreate->durationToKeep2;
  pCfg->tsdbCfg.keep1 = pCreate->durationToKeep0;
  pCfg->tsdbCfg.lruCacheSize = 16;
  pCfg->tsdbCfg.retentions = pCreate->pRetensions;
  pCfg->walCfg.vgId = pCreate->vgId;
  pCfg->hashBegin = pCreate->hashBegin;
  pCfg->hashEnd = pCreate->hashEnd;
  pCfg->hashMethod = pCreate->hashMethod;

  // sync integration
  pCfg->syncCfg.myIndex = pCreate->selfIndex;
  pCfg->syncCfg.replicaNum = pCreate->replica;
  memset(&(pCfg->syncCfg.nodeInfo), 0, sizeof(pCfg->syncCfg.nodeInfo));
  for (int i = 0; i < pCreate->replica; ++i) {
    (pCfg->syncCfg.nodeInfo)[i].nodePort = (pCreate->replicas)[i].port;
    snprintf((pCfg->syncCfg.nodeInfo)[i].nodeFqdn, sizeof((pCfg->syncCfg.nodeInfo)[i].nodeFqdn), "%s",
             (pCreate->replicas)[i].fqdn);
  }
}

static void vmGenerateWrapperCfg(SVnodesMgmt *pMgmt, SCreateVnodeReq *pCreate, SWrapperCfg *pCfg) {
  memcpy(pCfg->db, pCreate->db, TSDB_DB_FNAME_LEN);
  pCfg->dbUid = pCreate->dbUid;
  pCfg->dropped = 0;
  snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCreate->vgId);
  pCfg->vgId = pCreate->vgId;
  pCfg->vgVersion = pCreate->vgVersion;
}

int32_t vmProcessCreateVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg        *pReq = &pMsg->rpcMsg;
  SCreateVnodeReq createReq = {0};
  char            path[TSDB_FILENAME_LEN];

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
    tFreeSCreateVnodeReq(&createReq);
    dDebug("vgId:%d, already exist", createReq.vgId);
    vmReleaseVnode(pMgmt, pVnode);
    terrno = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    return -1;
  }

  // create vnode
  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vnodeCfg.vgId);
  if (vnodeCreate(path, &vnodeCfg, pMgmt->pTfs) < 0) {
    tFreeSCreateVnodeReq(&createReq);
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    return -1;
  }

  SMsgCb msgCb = pMgmt->pDnode->data.msgCb;
  msgCb.pWrapper = pMgmt->pWrapper;
  msgCb.queueFps[QUERY_QUEUE] = vmPutMsgToQueryQueue;
  msgCb.queueFps[FETCH_QUEUE] = vmPutMsgToFetchQueue;
  msgCb.queueFps[APPLY_QUEUE] = vmPutMsgToApplyQueue;
  msgCb.queueFps[SYNC_QUEUE] = vmPutMsgToSyncQueue;  // sync integration
  msgCb.qsizeFp = vmGetQueueSize;

  SVnode *pImpl = vnodeOpen(path, pMgmt->pTfs, msgCb);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    tFreeSCreateVnodeReq(&createReq);
    return -1;
  }

  int32_t code = vmOpenVnode(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    tFreeSCreateVnodeReq(&createReq);
    dError("vgId:%d, failed to open vnode since %s", createReq.vgId, terrstr());
    vnodeClose(pImpl);
    vnodeDestroy(path, pMgmt->pTfs);
    terrno = code;
    return code;
  }

  code = vmWriteVnodesToFile(pMgmt);
  if (code != 0) {
    tFreeSCreateVnodeReq(&createReq);
    vnodeClose(pImpl);
    vnodeDestroy(path, pMgmt->pTfs);
    terrno = code;
    return code;
  }

  return 0;
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
  if (vmWriteVnodesToFile(pMgmt) != 0) {
    pVnode->dropped = 0;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  vmCloseVnode(pMgmt, pVnode);
  vmWriteVnodesToFile(pMgmt);

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
  dmSetMsgHandle(pWrapper, TDMT_VND_MQ_VG_CHANGE, (NodeMsgFp)vmProcessWriteMsg, DEFAULT_HANDLE);
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
  // dmSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE, vmProcessMgmtMsg, DEFAULT_HANDLE);
  // dmSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE, vmProcessMgmtMsg, DEFAULT_HANDLE);

  // sync integration
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_TIMEOUT, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_PING, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_PING_REPLY, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_CLIENT_REQUEST, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_CLIENT_REQUEST_REPLY, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_REQUEST_VOTE, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_REQUEST_VOTE_REPLY, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_APPEND_ENTRIES, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_VND_SYNC_APPEND_ENTRIES_REPLY, (NodeMsgFp)vmProcessSyncMsg, DEFAULT_HANDLE);
}
