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

void vmGetVnodeLoads(SVnodeMgmt *pMgmt, SMonVloadInfo *pInfo, bool isReset) {
  pInfo->pVloads = taosArrayInit(pMgmt->state.totalVnodes, sizeof(SVnodeLoad));
  if (pInfo->pVloads == NULL) return;

  tfsUpdateSize(pMgmt->pTfs);

  taosThreadRwlockRdlock(&pMgmt->lock);

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    SVnodeLoad vload = {.vgId = pVnode->vgId};
    if (!pVnode->failed) {
      vnodeGetLoad(pVnode->pImpl, &vload);
      if (isReset) vnodeResetLoad(pVnode->pImpl, &vload);
    }
    taosArrayPush(pInfo->pVloads, &vload);
    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosThreadRwlockUnlock(&pMgmt->lock);
}

void vmGetVnodeLoadsLite(SVnodeMgmt *pMgmt, SMonVloadInfo *pInfo) {
  pInfo->pVloads = taosArrayInit(pMgmt->state.totalVnodes, sizeof(SVnodeLoadLite));
  if (!pInfo->pVloads) return;

  taosThreadRwlockRdlock(&pMgmt->lock);

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj     *pVnode = *ppVnode;
    if (!pVnode->failed) {
      SVnodeLoadLite vload = {0};
      if (vnodeGetLoadLite(pVnode->pImpl, &vload) == 0) {
        taosArrayPush(pInfo->pVloads, &vload);
      }
    }
    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosThreadRwlockUnlock(&pMgmt->lock);
}

void vmGetMonitorInfo(SVnodeMgmt *pMgmt, SMonVmInfo *pInfo) {
  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pMgmt, &vloads, true);

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
  pInfo->vstat.numOfSelectReqs = numOfSelectReqs;
  pInfo->vstat.numOfInsertReqs = numOfInsertReqs;                          // delta
  pInfo->vstat.numOfInsertSuccessReqs = numOfInsertSuccessReqs;            // delta
  pInfo->vstat.numOfBatchInsertReqs = numOfBatchInsertReqs;                // delta
  pInfo->vstat.numOfBatchInsertSuccessReqs = numOfBatchInsertSuccessReqs;  // delta
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

static void vmGenerateVnodeCfg(SCreateVnodeReq *pCreate, SVnodeCfg *pCfg) {
  memcpy(pCfg, &vnodeCfgDefault, sizeof(SVnodeCfg));

  pCfg->vgId = pCreate->vgId;
  tstrncpy(pCfg->dbname, pCreate->db, sizeof(pCfg->dbname));
  pCfg->dbId = pCreate->dbUid;
  pCfg->szPage = pCreate->pageSize * 1024;
  pCfg->szCache = pCreate->pages;
  pCfg->cacheLast = pCreate->cacheLast;
  pCfg->cacheLastSize = pCreate->cacheLastSize;
  pCfg->szBuf = (uint64_t)pCreate->buffer * 1024 * 1024;
  pCfg->isWeak = true;
  pCfg->isTsma = pCreate->isTsma;
  pCfg->tsdbCfg.compression = pCreate->compression;
  pCfg->tsdbCfg.precision = pCreate->precision;
  pCfg->tsdbCfg.days = pCreate->daysPerFile;
  pCfg->tsdbCfg.keep0 = pCreate->daysToKeep0;
  pCfg->tsdbCfg.keep1 = pCreate->daysToKeep1;
  pCfg->tsdbCfg.keep2 = pCreate->daysToKeep2;
  pCfg->tsdbCfg.keepTimeOffset = pCreate->keepTimeOffset;
  pCfg->tsdbCfg.minRows = pCreate->minRows;
  pCfg->tsdbCfg.maxRows = pCreate->maxRows;
  for (size_t i = 0; i < taosArrayGetSize(pCreate->pRetensions); ++i) {
    SRetention *pRetention = &pCfg->tsdbCfg.retentions[i];
    memcpy(pRetention, taosArrayGet(pCreate->pRetensions, i), sizeof(SRetention));
    if (i == 0) {
      if ((pRetention->freq >= 0 && pRetention->keep > 0)) pCfg->isRsma = 1;
    }
  }

  pCfg->walCfg.vgId = pCreate->vgId;
  pCfg->walCfg.fsyncPeriod = pCreate->walFsyncPeriod;
  pCfg->walCfg.retentionPeriod = pCreate->walRetentionPeriod;
  pCfg->walCfg.rollPeriod = pCreate->walRollPeriod;
  pCfg->walCfg.retentionSize = pCreate->walRetentionSize;
  pCfg->walCfg.segSize = pCreate->walSegmentSize;
  pCfg->walCfg.level = pCreate->walLevel;

  pCfg->sttTrigger = pCreate->sstTrigger;
  pCfg->hashBegin = pCreate->hashBegin;
  pCfg->hashEnd = pCreate->hashEnd;
  pCfg->hashMethod = pCreate->hashMethod;
  pCfg->hashPrefix = pCreate->hashPrefix;
  pCfg->hashSuffix = pCreate->hashSuffix;
  pCfg->tsdbPageSize = pCreate->tsdbPageSize * 1024;

  pCfg->standby = 0;
  pCfg->syncCfg.replicaNum = 0;
  pCfg->syncCfg.totalReplicaNum = 0;
  pCfg->syncCfg.changeVersion = pCreate->changeVersion;

  memset(&pCfg->syncCfg.nodeInfo, 0, sizeof(pCfg->syncCfg.nodeInfo));
  for (int32_t i = 0; i < pCreate->replica; ++i) {
    SNodeInfo *pNode = &pCfg->syncCfg.nodeInfo[i];
    pNode->nodeId = pCreate->replicas[pCfg->syncCfg.replicaNum].id;
    pNode->nodePort = pCreate->replicas[pCfg->syncCfg.replicaNum].port;
    pNode->nodeRole = TAOS_SYNC_ROLE_VOTER;
    tstrncpy(pNode->nodeFqdn, pCreate->replicas[pCfg->syncCfg.replicaNum].fqdn, TSDB_FQDN_LEN);
    tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
    pCfg->syncCfg.replicaNum++;
  }
  if (pCreate->selfIndex != -1) {
    pCfg->syncCfg.myIndex = pCreate->selfIndex;
  }
  for (int32_t i = pCfg->syncCfg.replicaNum; i < pCreate->replica + pCreate->learnerReplica; ++i) {
    SNodeInfo *pNode = &pCfg->syncCfg.nodeInfo[i];
    pNode->nodeId = pCreate->learnerReplicas[pCfg->syncCfg.totalReplicaNum].id;
    pNode->nodePort = pCreate->learnerReplicas[pCfg->syncCfg.totalReplicaNum].port;
    pNode->nodeRole = TAOS_SYNC_ROLE_LEARNER;
    tstrncpy(pNode->nodeFqdn, pCreate->learnerReplicas[pCfg->syncCfg.totalReplicaNum].fqdn, TSDB_FQDN_LEN);
    tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
    pCfg->syncCfg.totalReplicaNum++;
  }
  pCfg->syncCfg.totalReplicaNum += pCfg->syncCfg.replicaNum;
  if (pCreate->learnerSelfIndex != -1) {
    pCfg->syncCfg.myIndex = pCreate->replica + pCreate->learnerSelfIndex;
  }
}

static void vmGenerateWrapperCfg(SVnodeMgmt *pMgmt, SCreateVnodeReq *pCreate, SWrapperCfg *pCfg) {
  pCfg->vgId = pCreate->vgId;
  pCfg->vgVersion = pCreate->vgVersion;
  pCfg->dropped = 0;
  snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCreate->vgId);
}

static int32_t vmTsmaAdjustDays(SVnodeCfg *pCfg, SCreateVnodeReq *pReq) {
  if (pReq->isTsma) {
    SMsgHead *smaMsg = pReq->pTsma;
    uint32_t  contLen = (uint32_t)(htonl(smaMsg->contLen) - sizeof(SMsgHead));
    return smaGetTSmaDays(pCfg, POINTER_SHIFT(smaMsg, sizeof(SMsgHead)), contLen, &pCfg->tsdbCfg.days);
  }
  return 0;
}

#if 0
static int32_t vmTsmaProcessCreate(SVnode *pVnode, SCreateVnodeReq *pReq) {
  if (pReq->isTsma) {
    SMsgHead *smaMsg = pReq->pTsma;
    uint32_t  contLen = (uint32_t)(htonl(smaMsg->contLen) - sizeof(SMsgHead));
    return vnodeProcessCreateTSma(pVnode, POINTER_SHIFT(smaMsg, sizeof(SMsgHead)), contLen);
  }
  return 0;
}
#endif

int32_t vmProcessCreateVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SCreateVnodeReq req = {0};
  SVnodeCfg       vnodeCfg = {0};
  SWrapperCfg     wrapperCfg = {0};
  int32_t         code = -1;
  char            path[TSDB_FILENAME_LEN] = {0};

  if (tDeserializeSCreateVnodeReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (req.learnerReplica == 0) {
    req.learnerSelfIndex = -1;
  }

  dInfo(
      "vgId:%d, vnode management handle msgType:%s, start to create vnode, page:%d pageSize:%d buffer:%d szPage:%d "
      "szBuf:%" PRIu64 ", cacheLast:%d cacheLastSize:%d sstTrigger:%d tsdbPageSize:%d %d dbname:%s dbId:%" PRId64
      ", days:%d keep0:%d keep1:%d keep2:%d keepTimeOffset%d tsma:%d precision:%d compression:%d minRows:%d maxRows:%d"
      ", wal fsync:%d level:%d retentionPeriod:%d retentionSize:%" PRId64 " rollPeriod:%d segSize:%" PRId64
      ", hash method:%d begin:%u end:%u prefix:%d surfix:%d replica:%d selfIndex:%d "
      "learnerReplica:%d learnerSelfIndex:%d strict:%d changeVersion:%d",
      req.vgId, TMSG_INFO(pMsg->msgType), req.pages, req.pageSize, req.buffer, req.pageSize * 1024,
      (uint64_t)req.buffer * 1024 * 1024, req.cacheLast, req.cacheLastSize, req.sstTrigger, req.tsdbPageSize,
      req.tsdbPageSize * 1024, req.db, req.dbUid, req.daysPerFile, req.daysToKeep0, req.daysToKeep1, req.daysToKeep2,
      req.keepTimeOffset, req.isTsma, req.precision, req.compression, req.minRows, req.maxRows, req.walFsyncPeriod,
      req.walLevel, req.walRetentionPeriod, req.walRetentionSize, req.walRollPeriod, req.walSegmentSize, req.hashMethod,
      req.hashBegin, req.hashEnd, req.hashPrefix, req.hashSuffix, req.replica, req.selfIndex, req.learnerReplica,
      req.learnerSelfIndex, req.strict, req.changeVersion);

  for (int32_t i = 0; i < req.replica; ++i) {
    dInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d", req.vgId, i, req.replicas[i].fqdn, req.replicas[i].port,
          req.replicas[i].id);
  }
  for (int32_t i = 0; i < req.learnerReplica; ++i) {
    dInfo("vgId:%d, learnerReplica:%d ep:%s:%u dnode:%d", req.vgId, i, req.learnerReplicas[i].fqdn,
          req.learnerReplicas[i].port, req.replicas[i].id);
  }

  SReplica *pReplica = NULL;
  if (req.selfIndex != -1) {
    pReplica = &req.replicas[req.selfIndex];
  } else {
    pReplica = &req.learnerReplicas[req.learnerSelfIndex];
  }
  if (pReplica->id != pMgmt->pData->dnodeId || pReplica->port != tsServerPort ||
      strcmp(pReplica->fqdn, tsLocalFqdn) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("vgId:%d, dnodeId:%d ep:%s:%u not matched with local dnode", req.vgId, pReplica->id, pReplica->fqdn,
           pReplica->port);
    return -1;
  }

  vmGenerateVnodeCfg(&req, &vnodeCfg);

  if (vmTsmaAdjustDays(&vnodeCfg, &req) < 0) {
    dError("vgId:%d, failed to adjust tsma days since %s", req.vgId, terrstr());
    code = terrno;
    goto _OVER;
  }

  vmGenerateWrapperCfg(pMgmt, &req, &wrapperCfg);

  SVnodeObj *pVnode = vmAcquireVnodeImpl(pMgmt, req.vgId, false);
  if (pVnode != NULL && (req.replica == 1 || !pVnode->failed)) {
    dError("vgId:%d, already exist", req.vgId);
    tFreeSCreateVnodeReq(&req);
    vmReleaseVnode(pMgmt, pVnode);
    terrno = TSDB_CODE_VND_ALREADY_EXIST;
    code = terrno;
    return 0;
  }

  int32_t diskPrimary = vmGetPrimaryDisk(pMgmt, vnodeCfg.vgId);
  if (diskPrimary < 0) {
    diskPrimary = vmAllocPrimaryDisk(pMgmt, vnodeCfg.vgId);
  }
  wrapperCfg.diskPrimary = diskPrimary;

  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vnodeCfg.vgId);

  if (vnodeCreate(path, &vnodeCfg, diskPrimary, pMgmt->pTfs) < 0) {
    dError("vgId:%d, failed to create vnode since %s", req.vgId, terrstr());
    vmReleaseVnode(pMgmt, pVnode);
    tFreeSCreateVnodeReq(&req);
    code = terrno;
    return code;
  }

  SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, pMgmt->msgCb, true);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to open vnode since %s", req.vgId, terrstr());
    code = terrno;
    goto _OVER;
  }

  code = vmOpenVnode(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to open vnode since %s", req.vgId, terrstr());
    code = terrno;
    goto _OVER;
  }

#if 0
  code = vmTsmaProcessCreate(pImpl, &req);
  if (code != 0) {
    dError("vgId:%d, failed to create tsma since %s", req.vgId, terrstr());
    code = terrno;
    goto _OVER;
  }
#endif

  code = vnodeStart(pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to start sync since %s", req.vgId, terrstr());
    goto _OVER;
  }

  code = vmWriteVnodeListToFile(pMgmt);
  if (code != 0) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (code != 0) {
    vnodeClose(pImpl);
    vnodeDestroy(0, path, pMgmt->pTfs);
  } else {
    dInfo("vgId:%d, vnode management handle msgType:%s, end to create vnode, vnode is created", req.vgId,
          TMSG_INFO(pMsg->msgType));
  }

  tFreeSCreateVnodeReq(&req);
  terrno = code;
  return code;
}

//alter replica doesn't use this, but restore dnode still use this
int32_t vmProcessAlterVnodeTypeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SAlterVnodeTypeReq req = {0};
  if (tDeserializeSAlterVnodeReplicaReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (req.learnerReplicas == 0) {
    req.learnerSelfIndex = -1;
  }

  dInfo("vgId:%d, vnode management handle msgType:%s, start to process alter-node-type-request", req.vgId,
        TMSG_INFO(pMsg->msgType));

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, req.vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to alter vnode type since %s", req.vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    if (pVnode) vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  ESyncRole role = vnodeGetRole(pVnode->pImpl);
  dInfo("vgId:%d, checking node role:%d", req.vgId, role);
  if (role == TAOS_SYNC_ROLE_VOTER) {
    dError("vgId:%d, failed to alter vnode type since node already is role:%d", req.vgId, role);
    terrno = TSDB_CODE_VND_ALREADY_IS_VOTER;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("vgId:%d, checking node catch up", req.vgId);
  if (vnodeIsCatchUp(pVnode->pImpl) != 1) {
    terrno = TSDB_CODE_VND_NOT_CATCH_UP;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("node:%s, catched up leader, continue to process alter-node-type-request", pMgmt->name);

  int32_t vgId = req.vgId;
  dInfo("vgId:%d, start to alter vnode type replica:%d selfIndex:%d strict:%d changeVersion:%d",
        vgId, req.replica, req.selfIndex, req.strict, req.changeVersion);
  for (int32_t i = 0; i < req.replica; ++i) {
    SReplica *pReplica = &req.replicas[i];
    dInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d", vgId, i, pReplica->fqdn, pReplica->port, pReplica->id);
  }
  for (int32_t i = 0; i < req.learnerReplica; ++i) {
    SReplica *pReplica = &req.learnerReplicas[i];
    dInfo("vgId:%d, learnerReplicas:%d ep:%s:%u dnode:%d", vgId, i, pReplica->fqdn, pReplica->port, pReplica->id);
  }

  if (req.replica <= 0 || (req.selfIndex < 0 && req.learnerSelfIndex < 0) || req.selfIndex >= req.replica ||
      req.learnerSelfIndex >= req.learnerReplica) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("vgId:%d, failed to alter replica since invalid msg", vgId);
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  SReplica *pReplica = NULL;
  if (req.selfIndex != -1) {
    pReplica = &req.replicas[req.selfIndex];
  } else {
    pReplica = &req.learnerReplicas[req.learnerSelfIndex];
  }

  if (pReplica->id != pMgmt->pData->dnodeId || pReplica->port != tsServerPort ||
      strcmp(pReplica->fqdn, tsLocalFqdn) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("vgId:%d, dnodeId:%d ep:%s:%u not matched with local dnode", vgId, pReplica->id, pReplica->fqdn,
           pReplica->port);
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("vgId:%d, start to close vnode", vgId);
  SWrapperCfg wrapperCfg = {
      .dropped = pVnode->dropped,
      .vgId = pVnode->vgId,
      .vgVersion = pVnode->vgVersion,
      .diskPrimary = pVnode->diskPrimary,
  };
  tstrncpy(wrapperCfg.path, pVnode->path, sizeof(wrapperCfg.path));
  vmCloseVnode(pMgmt, pVnode, false);

  int32_t diskPrimary = wrapperCfg.diskPrimary;
  char    path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vgId);

  dInfo("vgId:%d, start to alter vnode replica at %s", vgId, path);
  if (vnodeAlterReplica(path, &req, diskPrimary, pMgmt->pTfs) < 0) {
    dError("vgId:%d, failed to alter vnode at %s since %s", vgId, path, terrstr());
    return -1;
  }

  dInfo("vgId:%d, begin to open vnode", vgId);
  SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, pMgmt->msgCb, false);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to open vnode at %s since %s", vgId, path, terrstr());
    return -1;
  }

  if (vmOpenVnode(pMgmt, &wrapperCfg, pImpl) != 0) {
    dError("vgId:%d, failed to open vnode mgmt since %s", vgId, terrstr());
    return -1;
  }

  if (vnodeStart(pImpl) != 0) {
    dError("vgId:%d, failed to start sync since %s", vgId, terrstr());
    return -1;
  }

  dInfo("vgId:%d, vnode management handle msgType:%s, end to process alter-node-type-request, vnode config is altered",
        req.vgId, TMSG_INFO(pMsg->msgType));
  return 0;
}

int32_t vmProcessCheckLearnCatchupReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SCheckLearnCatchupReq req = {0};
  if (tDeserializeSAlterVnodeReplicaReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if(req.learnerReplicas == 0){
    req.learnerSelfIndex = -1;
  }

  dInfo("vgId:%d, vnode management handle msgType:%s, start to process check-learner-catchup-request",
          req.vgId, TMSG_INFO(pMsg->msgType));

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, req.vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to alter vnode type since %s", req.vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    if (pVnode) vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  ESyncRole role = vnodeGetRole(pVnode->pImpl);
  dInfo("vgId:%d, checking node role:%d", req.vgId, role);
  if(role == TAOS_SYNC_ROLE_VOTER){
    dError("vgId:%d, failed to alter vnode type since node already is role:%d", req.vgId, role);
    terrno = TSDB_CODE_VND_ALREADY_IS_VOTER;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("vgId:%d, checking node catch up", req.vgId);
  if(vnodeIsCatchUp(pVnode->pImpl) != 1){
    terrno = TSDB_CODE_VND_NOT_CATCH_UP;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("node:%s, catched up leader, continue to process alter-node-type-request", pMgmt->name);

  vmReleaseVnode(pMgmt, pVnode);

  dInfo("vgId:%d, vnode management handle msgType:%s, end to process check-learner-catchup-request",
          req.vgId, TMSG_INFO(pMsg->msgType));

  return 0;
}

int32_t vmProcessDisableVnodeWriteReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SDisableVnodeWriteReq req = {0};
  if (tDeserializeSDisableVnodeWriteReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dInfo("vgId:%d, vnode write disable:%d", req.vgId, req.disable);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, req.vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to disable write since %s", req.vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    if (pVnode) vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  pVnode->disable = req.disable;
  vmReleaseVnode(pMgmt, pVnode);
  return 0;
}

int32_t vmProcessAlterHashRangeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SAlterVnodeHashRangeReq req = {0};
  if (tDeserializeSAlterVnodeHashRangeReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t srcVgId = req.srcVgId;
  int32_t dstVgId = req.dstVgId;

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, dstVgId);
  if (pVnode != NULL) {
    dError("vgId:%d, vnode already exist", dstVgId);
    vmReleaseVnode(pMgmt, pVnode);
    terrno = TSDB_CODE_VND_ALREADY_EXIST;
    return -1;
  }

  dInfo("vgId:%d, start to alter vnode hashrange:[%u, %u], dstVgId:%d", req.srcVgId, req.hashBegin, req.hashEnd,
        req.dstVgId);
  pVnode = vmAcquireVnode(pMgmt, srcVgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to alter hashrange since %s", srcVgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    if (pVnode) vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  SWrapperCfg wrapperCfg = {
      .dropped = pVnode->dropped,
      .vgId = dstVgId,
      .vgVersion = pVnode->vgVersion,
      .diskPrimary = pVnode->diskPrimary,
  };
  tstrncpy(wrapperCfg.path, pVnode->path, sizeof(wrapperCfg.path));

  // prepare alter
  pVnode->toVgId = dstVgId;
  if (vmWriteVnodeListToFile(pMgmt) != 0) {
    dError("vgId:%d, failed to write vnode list since %s", dstVgId, terrstr());
    return -1;
  }

  dInfo("vgId:%d, close vnode", srcVgId);
  vmCloseVnode(pMgmt, pVnode, true);

  int32_t diskPrimary = wrapperCfg.diskPrimary;
  char    srcPath[TSDB_FILENAME_LEN] = {0};
  char    dstPath[TSDB_FILENAME_LEN] = {0};
  snprintf(srcPath, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, srcVgId);
  snprintf(dstPath, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, dstVgId);

  dInfo("vgId:%d, alter vnode hashrange at %s", srcVgId, srcPath);
  if (vnodeAlterHashRange(srcPath, dstPath, &req, diskPrimary, pMgmt->pTfs) < 0) {
    dError("vgId:%d, failed to alter vnode hashrange since %s", srcVgId, terrstr());
    return -1;
  }

  dInfo("vgId:%d, open vnode", dstVgId);
  SVnode *pImpl = vnodeOpen(dstPath, diskPrimary, pMgmt->pTfs, pMgmt->msgCb, false);

  if (pImpl == NULL) {
    dError("vgId:%d, failed to open vnode at %s since %s", dstVgId, dstPath, terrstr());
    return -1;
  }

  if (vmOpenVnode(pMgmt, &wrapperCfg, pImpl) != 0) {
    dError("vgId:%d, failed to open vnode mgmt since %s", dstVgId, terrstr());
    return -1;
  }

  if (vnodeStart(pImpl) != 0) {
    dError("vgId:%d, failed to start sync since %s", dstVgId, terrstr());
    return -1;
  }

  // complete alter
  if (vmWriteVnodeListToFile(pMgmt) != 0) {
    dError("vgId:%d, failed to write vnode list since %s", dstVgId, terrstr());
    return -1;
  }

  dInfo("vgId:%d, vnode hashrange is altered", dstVgId);
  return 0;
}

int32_t vmProcessAlterVnodeReplicaReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SAlterVnodeReplicaReq alterReq = {0};
  if (tDeserializeSAlterVnodeReplicaReq(pMsg->pCont, pMsg->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (alterReq.learnerReplica == 0) {
    alterReq.learnerSelfIndex = -1;
  }

  int32_t vgId = alterReq.vgId;
  dInfo(
      "vgId:%d,vnode management handle msgType:%s, start to alter vnode replica:%d selfIndex:%d leanerReplica:%d "
      "learnerSelfIndex:%d strict:%d changeVersion:%d",
      vgId, TMSG_INFO(pMsg->msgType), alterReq.replica, alterReq.selfIndex, alterReq.learnerReplica,
      alterReq.learnerSelfIndex, alterReq.strict, alterReq.changeVersion);

  for (int32_t i = 0; i < alterReq.replica; ++i) {
    SReplica *pReplica = &alterReq.replicas[i];
    dInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d", vgId, i, pReplica->fqdn, pReplica->port, pReplica->port);
  }
  for (int32_t i = 0; i < alterReq.learnerReplica; ++i) {
    SReplica *pReplica = &alterReq.learnerReplicas[i];
    dInfo("vgId:%d, learnerReplicas:%d ep:%s:%u dnode:%d", vgId, i, pReplica->fqdn, pReplica->port, pReplica->port);
  }

  if (alterReq.replica <= 0 || (alterReq.selfIndex < 0 && alterReq.learnerSelfIndex < 0) ||
      alterReq.selfIndex >= alterReq.replica || alterReq.learnerSelfIndex >= alterReq.learnerReplica) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("vgId:%d, failed to alter replica since invalid msg", vgId);
    return -1;
  }

  SReplica *pReplica = NULL;
  if (alterReq.selfIndex != -1) {
    pReplica = &alterReq.replicas[alterReq.selfIndex];
  } else {
    pReplica = &alterReq.learnerReplicas[alterReq.learnerSelfIndex];
  }

  if (pReplica->id != pMgmt->pData->dnodeId || pReplica->port != tsServerPort ||
      strcmp(pReplica->fqdn, tsLocalFqdn) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("vgId:%d, dnodeId:%d ep:%s:%u not matched with local dnode", vgId, pReplica->id, pReplica->fqdn,
           pReplica->port);
    return -1;
  }

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to alter replica since %s", vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    if (pVnode) vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("vgId:%d, start to close vnode", vgId);
  SWrapperCfg wrapperCfg = {
      .dropped = pVnode->dropped,
      .vgId = pVnode->vgId,
      .vgVersion = pVnode->vgVersion,
      .diskPrimary = pVnode->diskPrimary,
  };
  tstrncpy(wrapperCfg.path, pVnode->path, sizeof(wrapperCfg.path));
  vmCloseVnode(pMgmt, pVnode, false);

  int32_t diskPrimary = wrapperCfg.diskPrimary;
  char    path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vgId);

  dInfo("vgId:%d, start to alter vnode replica at %s", vgId, path);
  if (vnodeAlterReplica(path, &alterReq, diskPrimary, pMgmt->pTfs) < 0) {
    dError("vgId:%d, failed to alter vnode at %s since %s", vgId, path, terrstr());
    return -1;
  }

  dInfo("vgId:%d, begin to open vnode", vgId);
  SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, pMgmt->msgCb, false);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to open vnode at %s since %s", vgId, path, terrstr());
    return -1;
  }

  if (vmOpenVnode(pMgmt, &wrapperCfg, pImpl) != 0) {
    dError("vgId:%d, failed to open vnode mgmt since %s", vgId, terrstr());
    return -1;
  }

  if (vnodeStart(pImpl) != 0) {
    dError("vgId:%d, failed to start sync since %s", vgId, terrstr());
    return -1;
  }

  dInfo(
      "vgId:%d, vnode management handle msgType:%s, end to alter vnode replica:%d selfIndex:%d leanerReplica:%d "
      "learnerSelfIndex:%d strict:%d",
      vgId, TMSG_INFO(pMsg->msgType), alterReq.replica, alterReq.selfIndex, alterReq.learnerReplica,
      alterReq.learnerSelfIndex, alterReq.strict);
  return 0;
}

int32_t vmProcessDropVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SDropVnodeReq dropReq = {0};
  if (tDeserializeSDropVnodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t vgId = dropReq.vgId;
  dInfo("vgId:%d, start to drop vnode", vgId);

  if (dropReq.dnodeId != pMgmt->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("vgId:%d, dnodeId:%d not matched with local dnode", dropReq.vgId, dropReq.dnodeId);
    return -1;
  }

  SVnodeObj *pVnode = vmAcquireVnodeImpl(pMgmt, vgId, false);
  if (pVnode == NULL) {
    dInfo("vgId:%d, failed to drop since %s", vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    return -1;
  }

  pVnode->dropped = 1;
  if (vmWriteVnodeListToFile(pMgmt) != 0) {
    pVnode->dropped = 0;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  vmCloseVnode(pMgmt, pVnode, false);
  vmWriteVnodeListToFile(pMgmt);

  dInfo("vgId:%d, is dropped", vgId);
  return 0;
}

SArray *vmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(32, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_SUBMIT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_QUERY, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_CONTINUE, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FETCH_RSMA, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_EXEC_RSMA, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_FETCH, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_UPDATE_TAG_VAL, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLE_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLE_CFG, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_BATCH_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLES_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_CANCEL_TASK, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_DROP_TASK, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_TASK_NOTIFY, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_STB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TTL_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_STB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_STB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_SMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CANCEL_SMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_SMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SUBMIT_RSMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_SUBSCRIBE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_DELETE_SUB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_COMMIT_OFFSET, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_SEEK, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_ADD_CHECKINFO, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_DEL_CHECKINFO, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_CONSUME, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_CONSUME_PUSH, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_VG_WALINFO, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_VG_COMMITTEDINFO, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DELETE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_BATCH_DEL, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMMIT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_HEARTBEAT, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_INDEX, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_INDEX, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_COMPACT_PROGRESS, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_KILL_COMPACT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DEPLOY, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DROP, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_RUN, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DISPATCH, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DISPATCH_RSP, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_RETRIEVE, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_RETRIEVE_RSP, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_CHECK, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_CHECK_RSP, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_PAUSE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_RESUME, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_STOP, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_CHECK_POINT_SOURCE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_CHECKPOINT_READY, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_CHECKPOINT_READY_RSP, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_UPDATE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_RESET, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_HEARTBEAT_RSP, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_REQ_CHKPT_RSP, vmPutMsgToStreamQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_REPLICA, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIG, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIRM, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DISABLE_WRITE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_HASHRANGE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMPACT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TRIM, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_VNODE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_VNODE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_VNODE_TYPE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CHECK_VNODE_LEARNER_CATCHUP, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CONFIG_CHANGE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_TIMEOUT_ELECTION, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST_BATCH, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST_REPLY, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_REQUEST_VOTE, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_REQUEST_VOTE_REPLY, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES_BATCH, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES_REPLY, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SNAPSHOT_SEND, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_PREP_SNAPSHOT, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_FORCE_FOLLOWER, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_TIMEOUT, vmPutMsgToSyncRdQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_HEARTBEAT, vmPutMsgToSyncRdQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_HEARTBEAT_REPLY, vmPutMsgToSyncRdQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SNAPSHOT_RSP, vmPutMsgToSyncRdQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_PREP_SNAPSHOT_REPLY, vmPutMsgToSyncRdQueue, 0) == NULL) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
