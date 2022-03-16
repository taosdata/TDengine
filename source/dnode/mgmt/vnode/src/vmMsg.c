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
#include "vmMsg.h"
#include "vmFile.h"
#include "vmWorker.h"
#include "dmInt.h"

static void vmGenerateVnodeCfg(SCreateVnodeReq *pCreate, SVnodeCfg *pCfg) {
  pCfg->vgId = pCreate->vgId;
  pCfg->wsize = pCreate->cacheBlockSize;
  pCfg->ssize = pCreate->cacheBlockSize;
  pCfg->lsize = pCreate->cacheBlockSize;
  pCfg->isHeapAllocator = true;
  pCfg->ttl = 4;
  pCfg->keep = pCreate->daysToKeep0;
  pCfg->streamMode = pCreate->streamMode;
  pCfg->isWeak = true;
  pCfg->tsdbCfg.keep = pCreate->daysToKeep0;
  pCfg->tsdbCfg.keep1 = pCreate->daysToKeep2;
  pCfg->tsdbCfg.keep2 = pCreate->daysToKeep0;
  pCfg->tsdbCfg.lruCacheSize = pCreate->cacheBlockSize;
  pCfg->metaCfg.lruSize = pCreate->cacheBlockSize;
  pCfg->walCfg.fsyncPeriod = pCreate->fsyncPeriod;
  pCfg->walCfg.level = pCreate->walLevel;
  pCfg->walCfg.retentionPeriod = 10;
  pCfg->walCfg.retentionSize = 128;
  pCfg->walCfg.rollPeriod = 128;
  pCfg->walCfg.segSize = 128;
  pCfg->walCfg.vgId = pCreate->vgId;
  pCfg->hashBegin = pCreate->hashBegin;
  pCfg->hashEnd = pCreate->hashEnd;
  pCfg->hashMethod = pCreate->hashMethod;
}

static void vmGenerateWrapperCfg(SVnodesMgmt *pMgmt, SCreateVnodeReq *pCreate, SWrapperCfg *pCfg) {
  memcpy(pCfg->db, pCreate->db, TSDB_DB_FNAME_LEN);
  pCfg->dbUid = pCreate->dbUid;
  pCfg->dropped = 0;
  snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCreate->vgId);
  pCfg->vgId = pCreate->vgId;
  pCfg->vgVersion = pCreate->vgVersion;
}

int32_t vmProcessCreateVnodeReq(SVnodesMgmt *pMgmt, SRpcMsg *pReq) {
  SCreateVnodeReq createReq = {0};
  if (tDeserializeSCreateVnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dDebug("vgId:%d, create vnode req is received", createReq.vgId);

  SVnodeCfg vnodeCfg = {0};
  vmGenerateVnodeCfg(&createReq, &vnodeCfg);

  SWrapperCfg wrapperCfg = {0};
  vmGenerateWrapperCfg(pMgmt, &createReq, &wrapperCfg);

  if (createReq.dnodeId != dmGetDnodeId(pMgmt->pDnode)) {
    terrno = TSDB_CODE_DND_VNODE_INVALID_OPTION;
    dDebug("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    return -1;
  }

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, createReq.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist", createReq.vgId);
    vmReleaseVnode(pMgmt, pVnode);
    terrno = TSDB_CODE_DND_VNODE_ALREADY_DEPLOYED;
    return -1;
  }

  vnodeCfg.pMgmt = pMgmt;
  vnodeCfg.pTfs = pMgmt->pTfs;
  vnodeCfg.dbId = wrapperCfg.dbUid;
  SVnode *pImpl = vnodeOpen(wrapperCfg.path, &vnodeCfg);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    return -1;
  }

  int32_t code = vmOpenVnode(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to open vnode since %s", createReq.vgId, terrstr());
    vnodeClose(pImpl);
    vnodeDestroy(wrapperCfg.path);
    terrno = code;
    return code;
  }

  code = vmWriteVnodesToFile(pMgmt);
  if (code != 0) {
    vnodeClose(pImpl);
    vnodeDestroy(wrapperCfg.path);
    terrno = code;
    return code;
  }

  return 0;
}

int32_t vmProcessAlterVnodeReq(SVnodesMgmt *pMgmt, SRpcMsg *pReq) {
  SAlterVnodeReq alterReq = {0};
  if (tDeserializeSCreateVnodeReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dDebug("vgId:%d, alter vnode req is received", alterReq.vgId);

  SVnodeCfg vnodeCfg = {0};
  vmGenerateVnodeCfg(&alterReq, &vnodeCfg);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, alterReq.vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to alter vnode since %s", alterReq.vgId, terrstr());
    return -1;
  }

  if (alterReq.vgVersion == pVnode->vgVersion) {
    vmReleaseVnode(pMgmt, pVnode);
    dDebug("vgId:%d, no need to alter vnode cfg for version unchanged ", alterReq.vgId);
    return 0;
  }

  if (vnodeAlter(pVnode->pImpl, &vnodeCfg) != 0) {
    dError("vgId:%d, failed to alter vnode since %s", alterReq.vgId, terrstr());
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  int32_t oldVersion = pVnode->vgVersion;
  pVnode->vgVersion = alterReq.vgVersion;
  int32_t code = vmWriteVnodesToFile(pMgmt);
  if (code != 0) {
    pVnode->vgVersion = oldVersion;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmProcessDropVnodeReq(SVnodesMgmt *pMgmt, SRpcMsg *pReq) {
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
    terrno = TSDB_CODE_DND_VNODE_NOT_DEPLOYED;
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

int32_t vmProcessSyncVnodeReq(SVnodesMgmt *pMgmt, SRpcMsg *pReq) {
  SSyncVnodeReq syncReq = {0};
  tDeserializeSDropVnodeReq(pReq->pCont, pReq->contLen, &syncReq);

  int32_t vgId = syncReq.vgId;
  dDebug("vgId:%d, sync vnode req is received", vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to sync since %s", vgId, terrstr());
    return -1;
  }

  if (vnodeSync(pVnode->pImpl) != 0) {
    dError("vgId:%d, failed to sync vnode since %s", vgId, terrstr());
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return 0;
}

int32_t vmProcessCompactVnodeReq(SVnodesMgmt *pMgmt, SRpcMsg *pReq) {
  SCompactVnodeReq compatcReq = {0};
  tDeserializeSDropVnodeReq(pReq->pCont, pReq->contLen, &compatcReq);

  int32_t vgId = compatcReq.vgId;
  dDebug("vgId:%d, compact vnode req is received", vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to compact since %s", vgId, terrstr());
    return -1;
  }

  if (vnodeCompact(pVnode->pImpl) != 0) {
    dError("vgId:%d, failed to compact vnode since %s", vgId, terrstr());
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return 0;
}

void vmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_SUBMIT, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH_RSP, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_UPDATE_TAG_VAL, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_TABLE_META, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_TABLES_META, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_CONSUME, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_QUERY, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_CONNECT, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_DISCONNECT, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_RES_READY, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_TASKS_STATUS, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CANCEL_TASK, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_STB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES_FETCH, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_REB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CONSUME, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_HEARTBEAT, vmProcessFetchMsg);

  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE, vmProcessCreateVnodeReq);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE, vmProcessAlterVnodeReq);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE, vmProcessDropVnodeReq);
  dndSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE, vmProcessSyncVnodeReq);
  dndSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE, vmProcessCompactVnodeReq);
}
