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
#include "metrics.h"
#include "taos_monitor.h"
#include "vmInt.h"
#include "vnd.h"
#include "vnodeInt.h"

extern taos_counter_t *tsInsertCounter;

// Forward declaration for function defined in metrics.c
extern int32_t addWriteMetrics(int32_t vgId, int32_t dnodeId, int64_t clusterId, const char *dnodeEp,
                               const char *dbname, const SRawWriteMetrics *pRawMetrics);

void vmGetVnodeLoads(SVnodeMgmt *pMgmt, SMonVloadInfo *pInfo, bool isReset) {
  pInfo->pVloads = taosArrayInit(pMgmt->state.totalVnodes, sizeof(SVnodeLoad));
  if (pInfo->pVloads == NULL) return;

  tfsUpdateSize(pMgmt->pTfs);

  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    SVnodeLoad vload = {.vgId = pVnode->vgId};
    if (!pVnode->failed) {
      if (vnodeGetLoad(pVnode->pImpl, &vload) != 0) {
        dError("failed to get vnode load");
      }
      if (isReset) vnodeResetLoad(pVnode->pImpl, &vload);
    }
    if (taosArrayPush(pInfo->pVloads, &vload) == NULL) {
      dError("failed to push vnode load");
    }
    pIter = taosHashIterate(pMgmt->runngingHash, pIter);
  }

  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
}

void vmSetVnodeSyncTimeout(SVnodeMgmt *pMgmt) {
  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;

    if (vnodeSetSyncTimeout(pVnode->pImpl, tsVnodeElectIntervalMs) != 0) {
      dError("vgId:%d, failed to vnodeSetSyncTimeout", pVnode->vgId);
    }
    pIter = taosHashIterate(pMgmt->runngingHash, pIter);
  }

  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
}

void vmGetVnodeLoadsLite(SVnodeMgmt *pMgmt, SMonVloadInfo *pInfo) {
  pInfo->pVloads = taosArrayInit(pMgmt->state.totalVnodes, sizeof(SVnodeLoadLite));
  if (!pInfo->pVloads) return;

  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    if (!pVnode->failed) {
      SVnodeLoadLite vload = {0};
      if (vnodeGetLoadLite(pVnode->pImpl, &vload) == 0) {
        if (taosArrayPush(pInfo->pVloads, &vload) == NULL) {
          taosArrayDestroy(pInfo->pVloads);
          pInfo->pVloads = NULL;
          break;
        }
      }
    }
    pIter = taosHashIterate(pMgmt->runngingHash, pIter);
  }

  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
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
    if (pLoad->syncState == TAOS_SYNC_STATE_LEADER || pLoad->syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
      masterNum++;
    }
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

  if (tfsGetMonitorInfo(pMgmt->pTfs, &pInfo->tfs) != 0) {
    dError("failed to get tfs monitor info");
  }
  taosArrayDestroy(pVloads);
}

void vmCleanExpriedSamples(SVnodeMgmt *pMgmt) {
  int list_size = taos_counter_get_keys_size(tsInsertCounter);
  if (list_size == 0) return;
  int32_t *vgroup_ids;
  char   **keys;
  int      r = 0;
  r = taos_counter_get_vgroup_ids(tsInsertCounter, &keys, &vgroup_ids, &list_size);
  if (r) {
    dError("failed to get vgroup ids");
    return;
  }
  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);
  for (int i = 0; i < list_size; i++) {
    int32_t vgroup_id = vgroup_ids[i];
    void   *vnode = taosHashGet(pMgmt->runngingHash, &vgroup_id, sizeof(int32_t));
    if (vnode == NULL) {
      r = taos_counter_delete(tsInsertCounter, keys[i]);
      if (r) {
        dError("failed to delete monitor sample key:%s", keys[i]);
      }
    }
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
  if (vgroup_ids) taosMemoryFree(vgroup_ids);
  if (keys) taosMemoryFree(keys);
  return;
}

void vmCleanExpiredMetrics(SVnodeMgmt *pMgmt) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0 || !tsEnableMetrics) {
    return;
  }

  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);
  void     *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pValidVgroups == NULL) {
    (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
    return;
  }

  while (pIter != NULL) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode && *ppVnode) {
      int32_t vgId = (*ppVnode)->vgId;
      char    dummy = 1;  // hash table value (we only care about the key)
      if (taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char)) != 0) {
        dError("failed to put vgId:%d to valid vgroups hash", vgId);
      }
    }
    pIter = taosHashIterate(pMgmt->runngingHash, pIter);
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);

  // Clean expired metrics by removing metrics for non-existent vgroups
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to clean expired metrics, code:%d", code);
  }

  taosHashCleanup(pValidVgroups);
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
#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
  pCfg->tsdbCfg.encryptAlgorithm = pCreate->encryptAlgorithm;
  if (pCfg->tsdbCfg.encryptAlgorithm == DND_CA_SM4) {
    tstrncpy(pCfg->tsdbCfg.encryptKey, tsEncryptKey, ENCRYPT_KEY_LEN + 1);
  }
#else
  pCfg->tsdbCfg.encryptAlgorithm = 0;
#endif

  pCfg->walCfg.vgId = pCreate->vgId;  // pCreate->mountVgId ? pCreate->mountVgId : pCreate->vgId;
  pCfg->walCfg.fsyncPeriod = pCreate->walFsyncPeriod;
  pCfg->walCfg.retentionPeriod = pCreate->walRetentionPeriod;
  pCfg->walCfg.rollPeriod = pCreate->walRollPeriod;
  pCfg->walCfg.retentionSize = pCreate->walRetentionSize;
  pCfg->walCfg.segSize = pCreate->walSegmentSize;
  pCfg->walCfg.level = pCreate->walLevel;
#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
  pCfg->walCfg.encryptAlgorithm = pCreate->encryptAlgorithm;
  if (pCfg->walCfg.encryptAlgorithm == DND_CA_SM4) {
    tstrncpy(pCfg->walCfg.encryptKey, tsEncryptKey, ENCRYPT_KEY_LEN + 1);
  }
#else
  pCfg->walCfg.encryptAlgorithm = 0;
#endif

#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
  pCfg->tdbEncryptAlgorithm = pCreate->encryptAlgorithm;
  if (pCfg->tdbEncryptAlgorithm == DND_CA_SM4) {
    tstrncpy(pCfg->tdbEncryptKey, tsEncryptKey, ENCRYPT_KEY_LEN + 1);
  }
#else
  pCfg->tdbEncryptAlgorithm = 0;
#endif

  pCfg->sttTrigger = pCreate->sstTrigger;
  pCfg->hashBegin = pCreate->hashBegin;
  pCfg->hashEnd = pCreate->hashEnd;
  pCfg->hashMethod = pCreate->hashMethod;
  pCfg->hashPrefix = pCreate->hashPrefix;
  pCfg->hashSuffix = pCreate->hashSuffix;
  pCfg->tsdbPageSize = pCreate->tsdbPageSize * 1024;

  pCfg->ssChunkSize = pCreate->ssChunkSize;
  pCfg->ssKeepLocal = pCreate->ssKeepLocal;
  pCfg->ssCompact = pCreate->ssCompact;

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
    bool ret = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
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
    bool ret = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
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

int32_t vmProcessCreateVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SCreateVnodeReq req = {0};
  SVnodeCfg       vnodeCfg = {0};
  SWrapperCfg     wrapperCfg = {0};
  int32_t         code = -1;
  char            path[TSDB_FILENAME_LEN] = {0};

  if (tDeserializeSCreateVnodeReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  if (req.learnerReplica == 0) {
    req.learnerSelfIndex = -1;
  }

  dInfo(
      "vgId:%d, vnode management handle msgType:%s, start to create vnode, page:%d pageSize:%d buffer:%d szPage:%d "
      "szBuf:%" PRIu64 ", cacheLast:%d cacheLastSize:%d sstTrigger:%d tsdbPageSize:%d %d dbname:%s dbId:%" PRId64
      ", days:%d keep0:%d keep1:%d keep2:%d keepTimeOffset%d ssChunkSize:%d ssKeepLocal:%d ssCompact:%d tsma:%d "
      "precision:%d compression:%d minRows:%d maxRows:%d"
      ", wal fsync:%d level:%d retentionPeriod:%d retentionSize:%" PRId64 " rollPeriod:%d segSize:%" PRId64
      ", hash method:%d begin:%u end:%u prefix:%d surfix:%d replica:%d selfIndex:%d "
      "learnerReplica:%d learnerSelfIndex:%d strict:%d changeVersion:%d encryptAlgorithm:%d",
      req.vgId, TMSG_INFO(pMsg->msgType), req.pages, req.pageSize, req.buffer, req.pageSize * 1024,
      (uint64_t)req.buffer * 1024 * 1024, req.cacheLast, req.cacheLastSize, req.sstTrigger, req.tsdbPageSize,
      req.tsdbPageSize * 1024, req.db, req.dbUid, req.daysPerFile, req.daysToKeep0, req.daysToKeep1, req.daysToKeep2,
      req.keepTimeOffset, req.ssChunkSize, req.ssKeepLocal, req.ssCompact, req.isTsma, req.precision, req.compression,
      req.minRows, req.maxRows, req.walFsyncPeriod, req.walLevel, req.walRetentionPeriod, req.walRetentionSize,
      req.walRollPeriod, req.walSegmentSize, req.hashMethod, req.hashBegin, req.hashEnd, req.hashPrefix, req.hashSuffix,
      req.replica, req.selfIndex, req.learnerReplica, req.learnerSelfIndex, req.strict, req.changeVersion,
      req.encryptAlgorithm);

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
    (void)tFreeSCreateVnodeReq(&req);

    code = TSDB_CODE_DNODE_NOT_MATCH_WITH_LOCAL;
    dError("vgId:%d, dnodeId:%d ep:%s:%u in request, ep:%s:%u in local, %s", req.vgId, pReplica->id,
           pReplica->fqdn, pReplica->port, tsLocalFqdn, tsServerPort, tstrerror(code));
    return code;
  }

  if (req.encryptAlgorithm == DND_CA_SM4) {
    if (strlen(tsEncryptKey) == 0) {
      (void)tFreeSCreateVnodeReq(&req);
      code = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
      dError("vgId:%d, failed to create vnode since encrypt key is empty, reason:%s", req.vgId, tstrerror(code));
      return code;
    }
  }

  vmGenerateVnodeCfg(&req, &vnodeCfg);

  vmGenerateWrapperCfg(pMgmt, &req, &wrapperCfg);

  SVnodeObj *pVnode = vmAcquireVnodeImpl(pMgmt, req.vgId, false);
  if (pVnode != NULL && (req.replica == 1 || !pVnode->failed)) {
    dError("vgId:%d, already exist", req.vgId);
    (void)tFreeSCreateVnodeReq(&req);
    vmReleaseVnode(pMgmt, pVnode);
    code = TSDB_CODE_VND_ALREADY_EXIST;
    return 0;
  }

  int32_t diskPrimary = vmGetPrimaryDisk(pMgmt, vnodeCfg.vgId);
  if (diskPrimary < 0) {
    diskPrimary = vmAllocPrimaryDisk(pMgmt, vnodeCfg.vgId);
  }
  wrapperCfg.diskPrimary = diskPrimary;

  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vnodeCfg.vgId);

  if ((code = vnodeCreate(path, &vnodeCfg, diskPrimary, pMgmt->pTfs)) < 0) {
    dError("vgId:%d, failed to create vnode since %s", req.vgId, tstrerror(code));
    vmReleaseVnode(pMgmt, pVnode);
    vmCleanPrimaryDisk(pMgmt, req.vgId);
    (void)tFreeSCreateVnodeReq(&req);
    return code;
  }

  SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, NULL, pMgmt->msgCb, true);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to open vnode since %s", req.vgId, terrstr());
    code = terrno != 0 ? terrno : -1;
    goto _OVER;
  }

  code = vmOpenVnode(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to open vnode since %s", req.vgId, terrstr());
    code = terrno != 0 ? terrno : code;
    goto _OVER;
  }

  code = vnodeStart(pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to start sync since %s", req.vgId, terrstr());
    goto _OVER;
  }

  code = vmWriteVnodeListToFile(pMgmt);
  if (code != 0) {
    code = terrno != 0 ? terrno : code;
    goto _OVER;
  }

_OVER:
  vmCleanPrimaryDisk(pMgmt, req.vgId);

  if (code != 0) {
    vmCloseFailedVnode(pMgmt, req.vgId);

    vnodeClose(pImpl);
    vnodeDestroy(0, path, pMgmt->pTfs, 0);
  } else {
    dInfo("vgId:%d, vnode management handle msgType:%s, end to create vnode, vnode is created", req.vgId,
          TMSG_INFO(pMsg->msgType));
  }

  (void)tFreeSCreateVnodeReq(&req);
  terrno = code;
  return code;
}

#ifdef USE_MOUNT
typedef struct {
  int64_t dbId;
  int32_t vgId;
  int32_t diskPrimary;
} SMountDbVgId;
extern int32_t vnodeLoadInfo(const char *dir, SVnodeInfo *pInfo);
extern int32_t mndFetchSdbStables(const char *mntName, const char *path, void *output);

static int compareVnodeInfo(const void *p1, const void *p2) {
  SVnodeInfo *v1 = (SVnodeInfo *)p1;
  SVnodeInfo *v2 = (SVnodeInfo *)p2;

  if (v1->config.dbId == v2->config.dbId) {
    if (v1->config.vgId == v2->config.vgId) {
      return 0;
    }
    return v1->config.vgId > v2->config.vgId ? 1 : -1;
  }

  return v1->config.dbId > v2->config.dbId ? 1 : -1;
}
static int compareVgDiskPrimary(const void *p1, const void *p2) {
  SMountDbVgId *v1 = (SMountDbVgId *)p1;
  SMountDbVgId *v2 = (SMountDbVgId *)p2;

  if (v1->dbId == v2->dbId) {
    if (v1->vgId == v2->vgId) {
      return 0;
    }
    return v1->vgId > v2->vgId ? 1 : -1;
  }

  return v1->dbId > v2->dbId ? 1 : -1;
}

static int32_t vmRetrieveMountDnode(SVnodeMgmt *pMgmt, SRetrieveMountPathReq *pReq, SMountInfo *pMountInfo) {
  int32_t   code = 0, lino = 0;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  int64_t   size = 0;
  int64_t   clusterId = 0, dropped = 0, encryptScope = 0;
  char      file[TSDB_MOUNT_FPATH_LEN] = {0};
  SArray   *pDisks = NULL;
  // step 1: fetch clusterId from dnode.json
  (void)snprintf(file, sizeof(file), "%s%s%s%sdnode.json", pReq->mountPath, TD_DIRSEP, dmNodeName(DNODE), TD_DIRSEP);
  TAOS_CHECK_EXIT(taosStatFile(file, &size, NULL, NULL));
  TSDB_CHECK_NULL((pFile = taosOpenFile(file, TD_FILE_READ)), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((content = taosMemoryMalloc(size + 1)), code, lino, _exit, terrno);
  if (taosReadFile(pFile, content, size) != size) {
    TAOS_CHECK_EXIT(terrno);
  }
  content[size] = '\0';
  pJson = tjsonParse(content);
  if (pJson == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  tjsonGetNumberValue(pJson, "dropped", dropped, code);
  TAOS_CHECK_EXIT(code);
  if (dropped == 1) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_MOUNT_DNODE_DROPPED);
  }
  tjsonGetNumberValue(pJson, "encryptScope", encryptScope, code);
  TAOS_CHECK_EXIT(code);
  if (encryptScope != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG);
  }
  tjsonGetNumberValue(pJson, "clusterId", clusterId, code);
  TAOS_CHECK_EXIT(code);
  if (clusterId == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_INVALID_CLUSTER_ID);
  }
  pMountInfo->clusterId = clusterId;
  if (content != NULL) taosMemoryFreeClear(content);
  if (pJson != NULL) {
    cJSON_Delete(pJson);
    pJson = NULL;
  }
  if (pFile != NULL) taosCloseFile(&pFile);
  // step 2: fetch dataDir from dnode/config/local.json
  TAOS_CHECK_EXIT(vmGetMountDisks(pMgmt, pReq->mountPath, &pDisks));
  int32_t nDisks = taosArrayGetSize(pDisks);
  if (nDisks < 1 || nDisks > TFS_MAX_DISKS) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  for (int32_t i = 0; i < nDisks; ++i) {
    SDiskCfg *pDisk = TARRAY_GET_ELEM(pDisks, i);
    if (!pMountInfo->pDisks[pDisk->level]) {
      pMountInfo->pDisks[pDisk->level] = taosArrayInit(1, sizeof(char *));
      if (!pMountInfo->pDisks[pDisk->level]) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    char *pDir = taosStrdup(pDisk->dir);
    if (pDir == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    if (pDisk->primary == 1 && taosArrayGetSize(pMountInfo->pDisks[0])) {
      // put the primary disk to the first position of level 0
      if (!taosArrayInsert(pMountInfo->pDisks[0], 0, &pDir)) {
        taosMemFree(pDir);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    } else if (!taosArrayPush(pMountInfo->pDisks[pDisk->level], &pDir)) {
      taosMemFree(pDir);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  for (int32_t i = 0; i < TFS_MAX_TIERS; ++i) {
    int32_t nDisk = taosArrayGetSize(pMountInfo->pDisks[i]);
    if (nDisk < (i == 0 ? 1 : 0) || nDisk > TFS_MAX_DISKS_PER_TIER) {
      dError("mount:%s, invalid disk number:%d at level:%d", pReq->mountName, nDisk, i);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
  }
_exit:
  if (content != NULL) taosMemoryFreeClear(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (pDisks != NULL) {
    taosArrayDestroy(pDisks);
    pDisks = NULL;
  }
  if (code != 0) {
    dError("mount:%s, failed to retrieve mount dnode at line %d on dnode:%d since %s, path:%s", pReq->mountName, lino,
           pReq->dnodeId, tstrerror(code), pReq->mountPath);
  } else {
    dInfo("mount:%s, success to retrieve mount dnode on dnode:%d, clusterId:%" PRId64 ", path:%s", pReq->mountName,
          pReq->dnodeId, pMountInfo->clusterId, pReq->mountPath);
  }
  TAOS_RETURN(code);
}

static int32_t vmRetrieveMountVnodes(SVnodeMgmt *pMgmt, SRetrieveMountPathReq *pReq, SMountInfo *pMountInfo) {
  int32_t       code = 0, lino = 0;
  SWrapperCfg  *pCfgs = NULL;
  int32_t       numOfVnodes = 0;
  char          path[TSDB_MOUNT_FPATH_LEN] = {0};
  TdDirPtr      pDir = NULL;
  TdDirEntryPtr de = NULL;
  SVnodeMgmt    vnodeMgmt = {0};
  SArray       *pVgCfgs = NULL;
  SArray       *pDbInfos = NULL;
  SArray       *pDiskPrimarys = NULL;

  snprintf(path, sizeof(path), "%s%s%s", pReq->mountPath, TD_DIRSEP, dmNodeName(VNODE));
  vnodeMgmt.path = path;
  TAOS_CHECK_EXIT(vmGetVnodeListFromFile(&vnodeMgmt, &pCfgs, &numOfVnodes));
  dInfo("mount:%s, num of vnodes is %d in path:%s", pReq->mountName, numOfVnodes, vnodeMgmt.path);
  TSDB_CHECK_NULL((pVgCfgs = taosArrayInit_s(sizeof(SVnodeInfo), numOfVnodes)), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((pDiskPrimarys = taosArrayInit(numOfVnodes, sizeof(SMountDbVgId))), code, lino, _exit, terrno);

  int32_t nDiskLevel0 = taosArrayGetSize(pMountInfo->pDisks[0]);
  int32_t nVgDropped = 0, j = 0;
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SWrapperCfg *pCfg = &pCfgs[i];
    // in order to support multi-tier disk, the pCfg->path should be adapted according to the diskPrimary firstly
    if (nDiskLevel0 > 1) {
      char *pDir = taosArrayGet(pMountInfo->pDisks[0], pCfg->diskPrimary);
      if (!pDir) TAOS_CHECK_EXIT(TSDB_CODE_INTERNAL_ERROR);
      (void)snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%svnode%d", *(char **)pDir, TD_DIRSEP, TD_DIRSEP,
                     pCfg->vgId);
    }
    dInfo("mount:%s, vnode path:%s, dropped:%" PRIi8, pReq->mountName, pCfg->path, pCfg->dropped);
    if (pCfg->dropped) {
      ++nVgDropped;
      continue;
    }
    if (!taosCheckAccessFile(pCfg->path, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
      dError("mount:%s, vnode path:%s, no r/w authority", pReq->mountName, pCfg->path);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_NO_RIGHTS);
    }
    SVnodeInfo *pInfo = TARRAY_GET_ELEM(pVgCfgs, j++);
    TAOS_CHECK_EXIT(vnodeLoadInfo(pCfg->path, pInfo));
    if (pInfo->config.syncCfg.replicaNum > 1) {
      dError("mount:%s, vnode path:%s, invalid replica:%d", pReq->mountName, pCfg->path,
             pInfo->config.syncCfg.replicaNum);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_INVALID_REPLICA);
    } else if (pInfo->config.vgId != pCfg->vgId) {
      dError("mount:%s, vnode path:%s, vgId:%d not match:%d", pReq->mountName, pCfg->path, pInfo->config.vgId,
             pCfg->vgId);
      TAOS_CHECK_EXIT(TSDB_CODE_FILE_CORRUPTED);
    } else if (pInfo->config.tdbEncryptAlgorithm || pInfo->config.tsdbCfg.encryptAlgorithm ||
               pInfo->config.walCfg.encryptAlgorithm) {
      dError("mount:%s, vnode path:%s, invalid encrypt algorithm, tdb:%d wal:%d tsdb:%d", pReq->mountName, pCfg->path,
             pInfo->config.tdbEncryptAlgorithm, pInfo->config.walCfg.encryptAlgorithm,
             pInfo->config.tsdbCfg.encryptAlgorithm);
      TAOS_CHECK_EXIT(TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG);
    }
    SMountDbVgId dbVgId = {.dbId = pInfo->config.dbId, .vgId = pInfo->config.vgId, .diskPrimary = pCfg->diskPrimary};
    TSDB_CHECK_NULL(taosArrayPush(pDiskPrimarys, &dbVgId), code, lino, _exit, terrno);
  }
  if (nVgDropped > 0) {
    dInfo("mount:%s, %d vnodes are dropped", pReq->mountName, nVgDropped);
    int32_t nVgToDrop = taosArrayGetSize(pVgCfgs) - nVgDropped;
    if (nVgToDrop > 0) taosArrayRemoveBatch(pVgCfgs, nVgToDrop - 1, nVgToDrop, NULL);
  }
  int32_t nVgCfg = taosArrayGetSize(pVgCfgs);
  int32_t nDiskPrimary = taosArrayGetSize(pDiskPrimarys);
  if (nVgCfg != nDiskPrimary) {
    dError("mount:%s, nVgCfg:%d not match nDiskPrimary:%d", pReq->mountName, nVgCfg, nDiskPrimary);
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }
  if (nVgCfg > 1) {
    taosArraySort(pVgCfgs, compareVnodeInfo);
    taosArraySort(pDiskPrimarys, compareVgDiskPrimary);
  }

  int64_t clusterId = pMountInfo->clusterId;
  int64_t dbId = 0, vgId = 0, nDb = 0;
  for (int32_t i = 0; i < nVgCfg; ++i) {
    SVnodeInfo *pInfo = TARRAY_GET_ELEM(pVgCfgs, i);
    if (clusterId != pInfo->config.syncCfg.nodeInfo->clusterId) {
      dError("mount:%s, clusterId:%" PRId64 " not match:%" PRId64, pReq->mountName, clusterId,
             pInfo->config.syncCfg.nodeInfo->clusterId);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_INVALID_CLUSTER_ID);
    }
    if (dbId != pInfo->config.dbId) {
      dbId = pInfo->config.dbId;
      ++nDb;
    }
    if (vgId == pInfo->config.vgId) {
      TAOS_CHECK_EXIT(TSDB_CODE_FILE_CORRUPTED);
    } else {
      vgId = pInfo->config.vgId;
    }
  }

  if (nDb > 0) {
    TSDB_CHECK_NULL((pDbInfos = taosArrayInit_s(sizeof(SMountDbInfo), nDb)), code, lino, _exit, terrno);
    int32_t dbIdx = -1;
    for (int32_t i = 0; i < nVgCfg; ++i) {
      SVnodeInfo   *pVgCfg = TARRAY_GET_ELEM(pVgCfgs, i);
      SMountDbVgId *pDiskPrimary = TARRAY_GET_ELEM(pDiskPrimarys, i);
      SMountDbInfo *pDbInfo = NULL;
      if (i == 0 || ((SMountDbInfo *)TARRAY_GET_ELEM(pDbInfos, dbIdx))->dbId != pVgCfg->config.dbId) {
        pDbInfo = TARRAY_GET_ELEM(pDbInfos, ++dbIdx);
        pDbInfo->dbId = pVgCfg->config.dbId;
        snprintf(pDbInfo->dbName, sizeof(pDbInfo->dbName), "%s", pVgCfg->config.dbname);
        TSDB_CHECK_NULL((pDbInfo->pVgs = taosArrayInit(nVgCfg / nDb, sizeof(SMountVgInfo))), code, lino, _exit, terrno);
      } else {
        pDbInfo = TARRAY_GET_ELEM(pDbInfos, dbIdx);
      }
      SMountVgInfo vgInfo = {
          .diskPrimary = pDiskPrimary->diskPrimary,
          .vgId = pVgCfg->config.vgId,
          .dbId = pVgCfg->config.dbId,
          .cacheLastSize = pVgCfg->config.cacheLastSize,
          .szPage = pVgCfg->config.szPage,
          .szCache = pVgCfg->config.szCache,
          .szBuf = pVgCfg->config.szBuf,
          .cacheLast = pVgCfg->config.cacheLast,
          .standby = pVgCfg->config.standby,
          .hashMethod = pVgCfg->config.hashMethod,
          .hashBegin = pVgCfg->config.hashBegin,
          .hashEnd = pVgCfg->config.hashEnd,
          .hashPrefix = pVgCfg->config.hashPrefix,
          .hashSuffix = pVgCfg->config.hashSuffix,
          .sttTrigger = pVgCfg->config.sttTrigger,
          .replications = pVgCfg->config.syncCfg.replicaNum,
          .precision = pVgCfg->config.tsdbCfg.precision,
          .compression = pVgCfg->config.tsdbCfg.compression,
          .slLevel = pVgCfg->config.tsdbCfg.slLevel,
          .daysPerFile = pVgCfg->config.tsdbCfg.days,
          .keep0 = pVgCfg->config.tsdbCfg.keep0,
          .keep1 = pVgCfg->config.tsdbCfg.keep1,
          .keep2 = pVgCfg->config.tsdbCfg.keep2,
          .keepTimeOffset = pVgCfg->config.tsdbCfg.keepTimeOffset,
          .minRows = pVgCfg->config.tsdbCfg.minRows,
          .maxRows = pVgCfg->config.tsdbCfg.maxRows,
          .tsdbPageSize = pVgCfg->config.tsdbPageSize / 1024,
          .ssChunkSize = pVgCfg->config.ssChunkSize,
          .ssKeepLocal = pVgCfg->config.ssKeepLocal,
          .ssCompact = pVgCfg->config.ssCompact,
          .walFsyncPeriod = pVgCfg->config.walCfg.fsyncPeriod,
          .walRetentionPeriod = pVgCfg->config.walCfg.retentionPeriod,
          .walRollPeriod = pVgCfg->config.walCfg.rollPeriod,
          .walRetentionSize = pVgCfg->config.walCfg.retentionSize,
          .walSegSize = pVgCfg->config.walCfg.segSize,
          .walLevel = pVgCfg->config.walCfg.level,
          .encryptAlgorithm = pVgCfg->config.walCfg.encryptAlgorithm,
          .committed = pVgCfg->state.committed,
          .commitID = pVgCfg->state.commitID,
          .commitTerm = pVgCfg->state.commitTerm,
          .numOfSTables = pVgCfg->config.vndStats.numOfSTables,
          .numOfCTables = pVgCfg->config.vndStats.numOfCTables,
          .numOfNTables = pVgCfg->config.vndStats.numOfNTables,
      };
      TSDB_CHECK_NULL(taosArrayPush(pDbInfo->pVgs, &vgInfo), code, lino, _exit, terrno);
    }
  }

  pMountInfo->pDbs = pDbInfos;

_exit:
  if (code != 0) {
    dError("mount:%s, failed to retrieve mount vnode at line %d on dnode:%d since %s, path:%s", pReq->mountName, lino,
           pReq->dnodeId, tstrerror(code), pReq->mountPath);
  }
  taosArrayDestroy(pDiskPrimarys);
  taosArrayDestroy(pVgCfgs);
  taosMemoryFreeClear(pCfgs);
  TAOS_RETURN(code);
}

/**
 *   Retrieve the stables from vnode meta.
 */
static int32_t vmRetrieveMountStbs(SVnodeMgmt *pMgmt, SRetrieveMountPathReq *pReq, SMountInfo *pMountInfo) {
  int32_t code = 0, lino = 0;
  char    path[TSDB_MOUNT_FPATH_LEN] = {0};
  int32_t nDb = taosArrayGetSize(pMountInfo->pDbs);
  SArray *suidList = NULL;
  SArray *pCols = NULL;
  SArray *pTags = NULL;
  SArray *pColExts = NULL;
  SArray *pTagExts = NULL;

  snprintf(path, sizeof(path), "%s%s%s", pReq->mountPath, TD_DIRSEP, dmNodeName(VNODE));
  for (int32_t i = 0; i < nDb; ++i) {
    SMountDbInfo *pDbInfo = TARRAY_GET_ELEM(pMountInfo->pDbs, i);
    int32_t       nVg = taosArrayGetSize(pDbInfo->pVgs);
    for (int32_t j = 0; j < nVg; ++j) {
      SMountVgInfo *pVgInfo = TARRAY_GET_ELEM(pDbInfo->pVgs, j);
      SVnode        vnode = {
                 .config.vgId = pVgInfo->vgId,
                 .config.dbId = pVgInfo->dbId,
                 .config.cacheLastSize = pVgInfo->cacheLastSize,
                 .config.szPage = pVgInfo->szPage,
                 .config.szCache = pVgInfo->szCache,
                 .config.szBuf = pVgInfo->szBuf,
                 .config.cacheLast = pVgInfo->cacheLast,
                 .config.standby = pVgInfo->standby,
                 .config.hashMethod = pVgInfo->hashMethod,
                 .config.hashBegin = pVgInfo->hashBegin,
                 .config.hashEnd = pVgInfo->hashEnd,
                 .config.hashPrefix = pVgInfo->hashPrefix,
                 .config.hashSuffix = pVgInfo->hashSuffix,
                 .config.sttTrigger = pVgInfo->sttTrigger,
                 .config.syncCfg.replicaNum = pVgInfo->replications,
                 .config.tsdbCfg.precision = pVgInfo->precision,
                 .config.tsdbCfg.compression = pVgInfo->compression,
                 .config.tsdbCfg.slLevel = pVgInfo->slLevel,
                 .config.tsdbCfg.days = pVgInfo->daysPerFile,
                 .config.tsdbCfg.keep0 = pVgInfo->keep0,
                 .config.tsdbCfg.keep1 = pVgInfo->keep1,
                 .config.tsdbCfg.keep2 = pVgInfo->keep2,
                 .config.tsdbCfg.keepTimeOffset = pVgInfo->keepTimeOffset,
                 .config.tsdbCfg.minRows = pVgInfo->minRows,
                 .config.tsdbCfg.maxRows = pVgInfo->maxRows,
                 .config.tsdbPageSize = pVgInfo->tsdbPageSize,
                 .config.ssChunkSize = pVgInfo->ssChunkSize,
                 .config.ssKeepLocal = pVgInfo->ssKeepLocal,
                 .config.ssCompact = pVgInfo->ssCompact,
                 .config.walCfg.fsyncPeriod = pVgInfo->walFsyncPeriod,
                 .config.walCfg.retentionPeriod = pVgInfo->walRetentionPeriod,
                 .config.walCfg.rollPeriod = pVgInfo->walRollPeriod,
                 .config.walCfg.retentionSize = pVgInfo->walRetentionSize,
                 .config.walCfg.segSize = pVgInfo->walSegSize,
                 .config.walCfg.level = pVgInfo->walLevel,
                 .config.walCfg.encryptAlgorithm = pVgInfo->encryptAlgorithm,
                 .diskPrimary = pVgInfo->diskPrimary,
      };
      void *vnodePath = taosArrayGet(pMountInfo->pDisks[0], pVgInfo->diskPrimary);
      snprintf(path, sizeof(path), "%s%s%s%svnode%d", *(char **)vnodePath, TD_DIRSEP, dmNodeName(VNODE), TD_DIRSEP,
               pVgInfo->vgId);
      vnode.path = path;

      int32_t rollback = vnodeShouldRollback(&vnode);
      if ((code = metaOpen(&vnode, &vnode.pMeta, rollback)) != 0) {
        dError("mount:%s, failed to retrieve stbs of vnode:%d for db:%" PRId64 " on dnode:%d since %s, path:%s",
               pReq->mountName, pVgInfo->vgId, pVgInfo->dbId, pReq->dnodeId, tstrerror(code), path);
        TAOS_CHECK_EXIT(code);
      } else {
        dInfo("mount:%s, success to retrieve stbs of vnode:%d for db:%" PRId64 " on dnode:%d, path:%s", pReq->mountName,
              pVgInfo->vgId, pVgInfo->dbId, pReq->dnodeId, path);

        SMetaReader mr = {0};
        tb_uid_t    suid = 0;
        SMeta      *pMeta = vnode.pMeta;

        metaReaderDoInit(&mr, pMeta, META_READER_LOCK);
        if (!suidList && !(suidList = taosArrayInit(1, sizeof(tb_uid_t)))) {
          TSDB_CHECK_CODE(terrno, lino, _exit0);
        }
        taosArrayClear(suidList);
        TSDB_CHECK_CODE(vnodeGetStbIdList(&vnode, 0, suidList), lino, _exit0);
        dInfo("mount:%s, vnode:%d, db:%" PRId64 ", stbs num:%d on dnode:%d", pReq->mountName, pVgInfo->vgId,
              pVgInfo->dbId, (int32_t)taosArrayGetSize(suidList), pReq->dnodeId);
        int32_t nStbs = taosArrayGetSize(suidList);
        if (!pDbInfo->pStbs && !(pDbInfo->pStbs = taosArrayInit(nStbs, sizeof(void *)))) {
          TSDB_CHECK_CODE(terrno, lino, _exit0);
        }
        for (int32_t i = 0; i < nStbs; ++i) {
          suid = *(tb_uid_t *)taosArrayGet(suidList, i);
          dInfo("mount:%s, vnode:%d, db:%" PRId64 ", stb suid:%" PRIu64 " on dnode:%d", pReq->mountName, pVgInfo->vgId,
                pVgInfo->dbId, suid, pReq->dnodeId);
          if ((code = metaReaderGetTableEntryByUidCache(&mr, suid)) < 0) {
            TSDB_CHECK_CODE(code, lino, _exit0);
          }
          if (mr.me.uid != suid || mr.me.type != TSDB_SUPER_TABLE ||
              mr.me.colCmpr.nCols != mr.me.stbEntry.schemaRow.nCols) {
            dError("mount:%s, vnode:%d, db:%" PRId64 ", stb info not match, suid:%" PRIu64 " expected:%" PRIu64
                   ", type:%" PRIi8 " expected:%d, nCmprCols:%d nCols:%d on dnode:%d",
                   pReq->mountName, pVgInfo->vgId, pVgInfo->dbId, mr.me.uid, suid, mr.me.type, TSDB_SUPER_TABLE,
                   mr.me.colCmpr.nCols, mr.me.stbEntry.schemaRow.nCols, pReq->dnodeId);
            TSDB_CHECK_CODE(TSDB_CODE_FILE_CORRUPTED, lino, _exit0);
          }
          SMountStbInfo stbInfo = {
              .req.source = TD_REQ_FROM_APP,
              .req.suid = suid,
              .req.colVer = mr.me.stbEntry.schemaRow.version,
              .req.tagVer = mr.me.stbEntry.schemaTag.version,
              .req.numOfColumns = mr.me.stbEntry.schemaRow.nCols,
              .req.numOfTags = mr.me.stbEntry.schemaTag.nCols,
              .req.virtualStb = TABLE_IS_VIRTUAL(mr.me.flags) ? 1 : 0,
          };
          snprintf(stbInfo.req.name, sizeof(stbInfo.req.name), "%s", mr.me.name);
          if (!pCols && !(pCols = taosArrayInit(stbInfo.req.numOfColumns, sizeof(SFieldWithOptions)))) {
            TSDB_CHECK_CODE(terrno, lino, _exit0);
          }
          if (!pTags && !(pTags = taosArrayInit(stbInfo.req.numOfTags, sizeof(SField)))) {
            TSDB_CHECK_CODE(terrno, lino, _exit0);
          }

          if (!pColExts && !(pColExts = taosArrayInit(stbInfo.req.numOfColumns, sizeof(col_id_t)))) {
            TSDB_CHECK_CODE(terrno, lino, _exit0);
          }
          if (!pTagExts && !(pTagExts = taosArrayInit(stbInfo.req.numOfTags, sizeof(col_id_t)))) {
            TSDB_CHECK_CODE(terrno, lino, _exit0);
          }
          taosArrayClear(pCols);
          taosArrayClear(pTags);
          taosArrayClear(pColExts);
          taosArrayClear(pTagExts);
          stbInfo.req.pColumns = pCols;
          stbInfo.req.pTags = pTags;
          stbInfo.pColExts = pColExts;
          stbInfo.pTagExts = pTagExts;

          for (int32_t c = 0; c < stbInfo.req.numOfColumns; ++c) {
            SSchema          *pSchema = mr.me.stbEntry.schemaRow.pSchema + c;
            SColCmpr         *pColComp = mr.me.colCmpr.pColCmpr + c;
            SFieldWithOptions col = {
                .type = pSchema->type,
                .flags = pSchema->flags,
                .bytes = pSchema->bytes,
                .compress = pColComp->alg,
            };
            (void)snprintf(col.name, sizeof(col.name), "%s", pSchema->name);
            if (pSchema->colId != pColComp->id) {
              TSDB_CHECK_CODE(TSDB_CODE_FILE_CORRUPTED, lino, _exit0);
            }
            if (mr.me.pExtSchemas) {
              col.typeMod = (mr.me.pExtSchemas + c)->typeMod;
            }
            TSDB_CHECK_NULL(taosArrayPush(pCols, &col), code, lino, _exit0, terrno);
            TSDB_CHECK_NULL(taosArrayPush(pColExts, &pSchema->colId), code, lino, _exit0, terrno);
          }
          for (int32_t t = 0; t < stbInfo.req.numOfTags; ++t) {
            SSchema *pSchema = mr.me.stbEntry.schemaTag.pSchema + t;
            SField   tag = {
                  .type = pSchema->type,
                  .flags = pSchema->flags,
                  .bytes = pSchema->bytes,
            };
            (void)snprintf(tag.name, sizeof(tag.name), "%s", pSchema->name);
            TSDB_CHECK_NULL(taosArrayPush(pTags, &tag), code, lino, _exit0, terrno);
            TSDB_CHECK_NULL(taosArrayPush(pTagExts, &pSchema->colId), code, lino, _exit0, terrno);
          }
          tDecoderClear(&mr.coder);

          // serialize the SMountStbInfo
          int32_t firstPartLen = 0;
          int32_t msgLen = tSerializeSMountStbInfo(NULL, 0, &firstPartLen, &stbInfo);
          if (msgLen <= 0) {
            TSDB_CHECK_CODE(msgLen < 0 ? msgLen : TSDB_CODE_INTERNAL_ERROR, lino, _exit0);
          }
          void *pBuf = taosMemoryMalloc((sizeof(int32_t) << 1) + msgLen);  // totalLen(4)|1stPartLen(4)|1stPart|2ndPart
          if (!pBuf) TSDB_CHECK_CODE(TSDB_CODE_OUT_OF_MEMORY, lino, _exit0);
          *(int32_t *)pBuf = (sizeof(int32_t) << 1) + msgLen;
          *(int32_t *)POINTER_SHIFT(pBuf, sizeof(int32_t)) = firstPartLen;
          if (tSerializeSMountStbInfo(POINTER_SHIFT(pBuf, (sizeof(int32_t) << 1)), msgLen, NULL, &stbInfo) <= 0) {
            taosMemoryFree(pBuf);
            TSDB_CHECK_CODE(msgLen < 0 ? msgLen : TSDB_CODE_INTERNAL_ERROR, lino, _exit0);
          }
          if (!taosArrayPush(pDbInfo->pStbs, &pBuf)) {
            taosMemoryFree(pBuf);
            TSDB_CHECK_CODE(terrno, lino, _exit0);
          }
        }
      _exit0:
        metaReaderClear(&mr);
        metaClose(&vnode.pMeta);
        TAOS_CHECK_EXIT(code);
      }
      break;  // retrieve stbs from one vnode is enough
    }
  }
_exit:
  if (code != 0) {
    dError("mount:%s, failed to retrieve mount stbs at line %d on dnode:%d since %s, path:%s", pReq->mountName, lino,
           pReq->dnodeId, tstrerror(code), path);
  }
  taosArrayDestroy(suidList);
  taosArrayDestroy(pCols);
  taosArrayDestroy(pTags);
  taosArrayDestroy(pColExts);
  taosArrayDestroy(pTagExts);
  TAOS_RETURN(code);
}

int32_t vmMountCheckRunning(const char *mountName, const char *mountPath, TdFilePtr *pFile, int32_t retryLimit) {
  int32_t code = 0, lino = 0;
  int32_t retryTimes = 0;
  char    filepath[PATH_MAX] = {0};
  (void)snprintf(filepath, sizeof(filepath), "%s%s.running", mountPath, TD_DIRSEP);
  TSDB_CHECK_NULL((*pFile = taosOpenFile(
                       filepath, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CLOEXEC)),
                  code, lino, _exit, terrno);
  int32_t ret = 0;
  do {
    ret = taosLockFile(*pFile);
    if (ret == 0) break;
    taosMsleep(1000);
    ++retryTimes;
    dError("mount:%s, failed to lock file:%s since %s, retryTimes:%d", mountName, filepath, tstrerror(ret), retryTimes);
  } while (retryTimes < retryLimit);
  TAOS_CHECK_EXIT(ret);
_exit:
  if (code != 0) {
    (void)taosCloseFile(pFile);
    *pFile = NULL;
    dError("mount:%s, failed to check running at line %d since %s, path:%s", mountName, lino, tstrerror(code),
           filepath);
  }
  TAOS_RETURN(code);
}

static int32_t vmRetrieveMountPreCheck(SVnodeMgmt *pMgmt, SRetrieveMountPathReq *pReq, SMountInfo *pMountInfo) {
  int32_t code = 0, lino = 0;
  char    path[TSDB_MOUNT_FPATH_LEN] = {0};
  TSDB_CHECK_CONDITION(taosCheckAccessFile(pReq->mountPath, O_RDONLY), code, lino, _exit, TAOS_SYSTEM_ERROR(errno));
  TAOS_CHECK_EXIT(vmMountCheckRunning(pReq->mountName, pReq->mountPath, &pMountInfo->pFile, 3));
  (void)snprintf(path, sizeof(path), "%s%s%s%sdnode.json", pReq->mountPath, TD_DIRSEP, dmNodeName(DNODE), TD_DIRSEP);
  TSDB_CHECK_CONDITION(taosCheckAccessFile(path, O_RDONLY), code, lino, _exit, TAOS_SYSTEM_ERROR(errno));
  (void)snprintf(path, sizeof(path), "%s%s%s", pReq->mountPath, TD_DIRSEP, dmNodeName(MNODE));
  TSDB_CHECK_CONDITION(taosCheckAccessFile(path, O_RDONLY), code, lino, _exit, TAOS_SYSTEM_ERROR(errno));
  (void)snprintf(path, sizeof(path), "%s%s%s", pReq->mountPath, TD_DIRSEP, dmNodeName(VNODE));
  TSDB_CHECK_CONDITION(taosCheckAccessFile(path, O_RDONLY), code, lino, _exit, TAOS_SYSTEM_ERROR(errno));
  (void)snprintf(path, sizeof(path), "%s%s%s%sconfig%slocal.json", pReq->mountPath, TD_DIRSEP, dmNodeName(DNODE), TD_DIRSEP,
           TD_DIRSEP);
  TSDB_CHECK_CONDITION(taosCheckAccessFile(path, O_RDONLY), code, lino, _exit, TAOS_SYSTEM_ERROR(errno));
_exit:
  if (code != 0) {
    dError("mount:%s, failed to retrieve mount at line %d on dnode:%d since %s, path:%s", pReq->mountName, lino,
           pReq->dnodeId, tstrerror(code), path);
  }
  TAOS_RETURN(code);
}

static int32_t vmRetrieveMountPathImpl(SVnodeMgmt *pMgmt, SRpcMsg *pMsg, SRetrieveMountPathReq *pReq,
                                       SMountInfo *pMountInfo) {
  int32_t code = 0, lino = 0;
  pMountInfo->dnodeId = pReq->dnodeId;
  pMountInfo->mountUid = pReq->mountUid;
  (void)tsnprintf(pMountInfo->mountName, sizeof(pMountInfo->mountName), "%s", pReq->mountName);
  (void)tsnprintf(pMountInfo->mountPath, sizeof(pMountInfo->mountPath), "%s", pReq->mountPath);
  pMountInfo->ignoreExist = pReq->ignoreExist;
  pMountInfo->valLen = pReq->valLen;
  pMountInfo->pVal = pReq->pVal;
  TAOS_CHECK_EXIT(vmRetrieveMountPreCheck(pMgmt, pReq, pMountInfo));
  TAOS_CHECK_EXIT(vmRetrieveMountDnode(pMgmt, pReq, pMountInfo));
  TAOS_CHECK_EXIT(vmRetrieveMountVnodes(pMgmt, pReq, pMountInfo));
  TAOS_CHECK_EXIT(vmRetrieveMountStbs(pMgmt, pReq, pMountInfo));
_exit:
  if (code != 0) {
    dError("mount:%s, failed to retrieve mount at line %d on dnode:%d since %s, path:%s", pReq->mountName, lino,
           pReq->dnodeId, tstrerror(code), pReq->mountPath);
  }
  TAOS_RETURN(code);
}

int32_t vmProcessRetrieveMountPathReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t               code = 0, lino = 0;
  int32_t               rspCode = 0;
  SVnodeMgmt            vndMgmt = {0};
  SMountInfo            mountInfo = {0};
  void                 *pBuf = NULL;
  int32_t               bufLen = 0;
  SRetrieveMountPathReq req = {0};

  vndMgmt = *pMgmt;
  vndMgmt.path = NULL;
  TAOS_CHECK_GOTO(tDeserializeSRetrieveMountPathReq(pMsg->pCont, pMsg->contLen, &req), &lino, _end);
  dInfo("mount:%s, start to retrieve path:%s", req.mountName, req.mountPath);
  TAOS_CHECK_GOTO(vmRetrieveMountPathImpl(&vndMgmt, pMsg, &req, &mountInfo), &lino, _end);
_end:
  TSDB_CHECK_CONDITION((bufLen = tSerializeSMountInfo(NULL, 0, &mountInfo)) >= 0, rspCode, lino, _exit, bufLen);
  TSDB_CHECK_CONDITION((pBuf = rpcMallocCont(bufLen)), rspCode, lino, _exit, terrno);
  TSDB_CHECK_CONDITION((bufLen = tSerializeSMountInfo(pBuf, bufLen, &mountInfo)) >= 0, rspCode, lino, _exit, bufLen);
  pMsg->info.rsp = pBuf;
  pMsg->info.rspLen = bufLen;
_exit:
  if (rspCode != 0) {
    // corner case: if occurs, the client will not receive the response, and the client should be killed manually
    dError("mount:%s, failed to retrieve mount at line %d since %s, dnode:%d, path:%s", req.mountName, lino,
           tstrerror(rspCode), req.dnodeId, req.mountPath);
    rpcFreeCont(pBuf);
    code = rspCode;
  } else if (code != 0) {
    // the client would receive the response with error msg
    dError("mount:%s, failed to retrieve mount at line %d on dnode:%d since %s, path:%s", req.mountName, lino,
           req.dnodeId, tstrerror(code), req.mountPath);
  } else {
    int32_t nVgs = 0;
    int32_t nDbs = taosArrayGetSize(mountInfo.pDbs);
    for (int32_t i = 0; i < nDbs; ++i) {
      SMountDbInfo *pDb = TARRAY_GET_ELEM(mountInfo.pDbs, i);
      nVgs += taosArrayGetSize(pDb->pVgs);
    }
    dInfo("mount:%s, success to retrieve mount, nDbs:%d, nVgs:%d, path:%s", req.mountName, nDbs, nVgs, req.mountPath);
  }
  taosMemFreeClear(vndMgmt.path);
  tFreeMountInfo(&mountInfo, false);
  TAOS_RETURN(code);
}

static int32_t vmMountVnode(SVnodeMgmt *pMgmt, const char *path, SVnodeCfg *pCfg, int32_t diskPrimary,
                            SMountVnodeReq *req, STfs *pMountTfs) {
  int32_t    code = 0;
  SVnodeInfo info = {0};
  char       hostDir[TSDB_FILENAME_LEN] = {0};
  char       mountDir[TSDB_FILENAME_LEN] = {0};
  char       mountVnode[32] = {0};

  if ((code = vnodeCheckCfg(pCfg)) < 0) {
    vError("vgId:%d, mount:%s, failed to mount vnode since:%s", pCfg->vgId, req->mountName, tstrerror(code));
    return code;
  }

  vnodeGetPrimaryDir(path, 0, pMgmt->pTfs, hostDir, TSDB_FILENAME_LEN);
  if ((code = taosMkDir(hostDir))) {
    vError("vgId:%d, mount:%s, failed to prepare vnode dir since %s, host path: %s", pCfg->vgId, req->mountName,
           tstrerror(code), hostDir);
    return code;
  }

  info.config = *pCfg;  // copy the config
  info.state.committed = req->committed;
  info.state.commitID = req->commitID;
  info.state.commitTerm = req->commitTerm;
  info.state.applied = req->committed;
  info.state.applyTerm = req->commitTerm;
  info.config.vndStats.numOfSTables = req->numOfSTables;
  info.config.vndStats.numOfCTables = req->numOfCTables;
  info.config.vndStats.numOfNTables = req->numOfNTables;

  SVnodeInfo oldInfo = {0};
  oldInfo.config = vnodeCfgDefault;
  if (vnodeLoadInfo(hostDir, &oldInfo) == 0) {
    if (oldInfo.config.dbId != info.config.dbId) {
      code = TSDB_CODE_VND_ALREADY_EXIST_BUT_NOT_MATCH;
      vError("vgId:%d, mount:%s, vnode config info already exists at %s. oldDbId:%" PRId64 "(%s) at cluster:%" PRId64
             ", newDbId:%" PRId64 "(%s) at cluser:%" PRId64 ", code:%s",
             oldInfo.config.vgId, req->mountName, hostDir, oldInfo.config.dbId, oldInfo.config.dbname,
             oldInfo.config.syncCfg.nodeInfo[oldInfo.config.syncCfg.myIndex].clusterId, info.config.dbId,
             info.config.dbname, info.config.syncCfg.nodeInfo[info.config.syncCfg.myIndex].clusterId, tstrerror(code));

    } else {
      vWarn("vgId:%d, mount:%s, vnode config info already exists at %s.", oldInfo.config.vgId, req->mountName, hostDir);
    }
    return code;
  }

  char hostSubDir[TSDB_FILENAME_LEN] = {0};
  char mountSubDir[TSDB_FILENAME_LEN] = {0};
  (void)snprintf(mountVnode, sizeof(mountVnode), "vnode%svnode%d", TD_DIRSEP, req->mountVgId);
  vnodeGetPrimaryDir(mountVnode, diskPrimary, pMountTfs, mountDir, TSDB_FILENAME_LEN);
  static const char *vndSubDirs[] = {"meta", "sync", "tq", "tsdb", "wal"};
  for (int32_t i = 0; i < tListLen(vndSubDirs); ++i) {
    (void)snprintf(hostSubDir, sizeof(hostSubDir), "%s%s%s", hostDir, TD_DIRSEP, vndSubDirs[i]);
    (void)snprintf(mountSubDir, sizeof(mountSubDir), "%s%s%s", mountDir, TD_DIRSEP, vndSubDirs[i]);
    if ((code = taosSymLink(mountSubDir, hostSubDir)) != 0) {
      vError("vgId:%d, mount:%s, failed to create vnode symlink %s -> %s since %s", info.config.vgId, req->mountName,
             mountSubDir, hostSubDir, tstrerror(code));
      return code;
    }
  }
  vInfo("vgId:%d, mount:save vnode config while create", info.config.vgId);
  if ((code = vnodeSaveInfo(hostDir, &info)) < 0 || (code = vnodeCommitInfo(hostDir)) < 0) {
    vError("vgId:%d, mount:%s, failed to save vnode config since %s, mount path: %s", pCfg ? pCfg->vgId : 0,
           req->mountName, tstrerror(code), hostDir);
    return code;
  }
  vInfo("vgId:%d, mount:%s, vnode is mounted from %s to %s", info.config.vgId, req->mountName, mountDir, hostDir);
  return 0;
}

int32_t vmProcessMountVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t          code = 0, lino = 0;
  SMountVnodeReq   req = {0};
  SCreateVnodeReq *pCreateReq = &req.createReq;
  SVnodeCfg        vnodeCfg = {0};
  SWrapperCfg      wrapperCfg = {0};
  SVnode          *pImpl = NULL;
  STfs            *pMountTfs = NULL;
  char             path[TSDB_FILENAME_LEN] = {0};
  bool             releaseTfs = false;

  if (tDeserializeSMountVnodeReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    dError("vgId:%d, failed to mount vnode since deserialize request error", pCreateReq->vgId);
    return TSDB_CODE_INVALID_MSG;
  }

  if (pCreateReq->learnerReplica == 0) {
    pCreateReq->learnerSelfIndex = -1;
  }
  for (int32_t i = 0; i < pCreateReq->replica; ++i) {
    dInfo("mount:%s, vgId:%d, replica:%d ep:%s:%u dnode:%d", req.mountName, pCreateReq->vgId, i,
          pCreateReq->replicas[i].fqdn, pCreateReq->replicas[i].port, pCreateReq->replicas[i].id);
  }
  for (int32_t i = 0; i < pCreateReq->learnerReplica; ++i) {
    dInfo("mount:%s, vgId:%d, learnerReplica:%d ep:%s:%u dnode:%d", req.mountName, pCreateReq->vgId, i,
          pCreateReq->learnerReplicas[i].fqdn, pCreateReq->learnerReplicas[i].port, pCreateReq->replicas[i].id);
  }

  SReplica *pReplica = NULL;
  if (pCreateReq->selfIndex != -1) {
    pReplica = &pCreateReq->replicas[pCreateReq->selfIndex];
  } else {
    pReplica = &pCreateReq->learnerReplicas[pCreateReq->learnerSelfIndex];
  }
  if (pReplica->id != pMgmt->pData->dnodeId || pReplica->port != tsServerPort ||
      strcmp(pReplica->fqdn, tsLocalFqdn) != 0) {
    (void)tFreeSMountVnodeReq(&req);
    code = TSDB_CODE_INVALID_MSG;
    dError("mount:%s, vgId:%d, dnodeId:%d ep:%s:%u not matched with local dnode, reason:%s", req.mountName,
           pCreateReq->vgId, pReplica->id, pReplica->fqdn, pReplica->port, tstrerror(code));
    return code;
  }
  vmGenerateVnodeCfg(pCreateReq, &vnodeCfg);
  vnodeCfg.mountVgId = req.mountVgId;
  vmGenerateWrapperCfg(pMgmt, pCreateReq, &wrapperCfg);
  wrapperCfg.mountId = req.mountId;

  SVnodeObj *pVnode = vmAcquireVnodeImpl(pMgmt, pCreateReq->vgId, false);
  if (pVnode != NULL && (pCreateReq->replica == 1 || !pVnode->failed)) {
    dError("mount:%s, vgId:%d, already exist", req.mountName, pCreateReq->vgId);
    (void)tFreeSMountVnodeReq(&req);
    vmReleaseVnode(pMgmt, pVnode);
    code = TSDB_CODE_VND_ALREADY_EXIST;
    return 0;
  }
  vmReleaseVnode(pMgmt, pVnode);

  wrapperCfg.diskPrimary = req.diskPrimary;
  (void)snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vnodeCfg.vgId);
  TAOS_CHECK_EXIT(vmAcquireMountTfs(pMgmt, req.mountId, req.mountName, req.mountPath, &pMountTfs));
  releaseTfs = true;

  TAOS_CHECK_EXIT(vmMountVnode(pMgmt, path, &vnodeCfg, wrapperCfg.diskPrimary, &req, pMountTfs));
  if (!(pImpl = vnodeOpen(path, 0, pMgmt->pTfs, pMountTfs, pMgmt->msgCb, true))) {
    TAOS_CHECK_EXIT(terrno != 0 ? terrno : -1);
  }
  if ((code = vmOpenVnode(pMgmt, &wrapperCfg, pImpl)) != 0) {
    TAOS_CHECK_EXIT(terrno != 0 ? terrno : code);
  }
  TAOS_CHECK_EXIT(vnodeStart(pImpl));
  TAOS_CHECK_EXIT(vmWriteVnodeListToFile(pMgmt));
  TAOS_CHECK_EXIT(vmWriteMountListToFile(pMgmt));
_exit:
  vmCleanPrimaryDisk(pMgmt, pCreateReq->vgId);
  if (code != 0) {
    dError("mount:%s, vgId:%d, msgType:%s, failed at line %d to mount vnode since %s", req.mountName, pCreateReq->vgId,
           TMSG_INFO(pMsg->msgType), lino, tstrerror(code));
    vmCloseFailedVnode(pMgmt, pCreateReq->vgId);
    vnodeClose(pImpl);
    vnodeDestroy(0, path, pMgmt->pTfs, 0);
    if (releaseTfs) vmReleaseMountTfs(pMgmt, req.mountId, 1);
  } else {
    dInfo("mount:%s, vgId:%d, msgType:%s, success to mount vnode", req.mountName, pCreateReq->vgId,
          TMSG_INFO(pMsg->msgType));
  }

  pMsg->code = code;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  (void)tFreeSMountVnodeReq(&req);
  TAOS_RETURN(code);
}
#endif  // USE_MOUNT

// alter replica doesn't use this, but restore dnode still use this
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
  dInfo("vgId:%d, start to alter vnode type replica:%d selfIndex:%d strict:%d changeVersion:%d", vgId, req.replica,
        req.selfIndex, req.strict, req.changeVersion);
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
    terrno = TSDB_CODE_DNODE_NOT_MATCH_WITH_LOCAL;
    dError("vgId:%d, dnodeId:%d ep:%s:%u in request, ep:%s:%u in local, %s", vgId, pReplica->id, pReplica->fqdn,
           pReplica->port, tsLocalFqdn, tsServerPort, tstrerror(terrno));
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

  bool commitAndRemoveWal = vnodeShouldRemoveWal(pVnode->pImpl);
  vmCloseVnode(pMgmt, pVnode, commitAndRemoveWal, true);

  int32_t diskPrimary = wrapperCfg.diskPrimary;
  char    path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vgId);

  dInfo("vgId:%d, start to alter vnode replica at %s", vgId, path);
  if (vnodeAlterReplica(path, &req, diskPrimary, pMgmt->pTfs) < 0) {
    dError("vgId:%d, failed to alter vnode at %s since %s", vgId, path, terrstr());
    return -1;
  }

  dInfo("vgId:%d, begin to open vnode", vgId);
  SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, NULL, pMgmt->msgCb, false);
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

  if (req.learnerReplicas == 0) {
    req.learnerSelfIndex = -1;
  }

  dInfo("vgId:%d, vnode management handle msgType:%s, start to process check-learner-catchup-request", req.vgId,
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

  vmReleaseVnode(pMgmt, pVnode);

  dInfo("vgId:%d, vnode management handle msgType:%s, end to process check-learner-catchup-request", req.vgId,
        TMSG_INFO(pMsg->msgType));

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

int32_t vmProcessSetKeepVersionReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  SVndSetKeepVersionReq req = {0};
  if (tDeserializeSVndSetKeepVersionReq(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), pMsg->contLen - sizeof(SMsgHead),
                                        &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dInfo("vgId:%d, set wal keep version to %" PRId64, pHead->vgId, req.keepVersion);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to set keep version since %s", pHead->vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    return -1;
  }

  // Directly call vnodeSetWalKeepVersion for immediate effect (< 1ms)
  // This bypasses Raft to avoid timing issues where WAL might be deleted
  // before keepVersion is set through the Raft consensus process
  int32_t code = vnodeSetWalKeepVersion(pVnode->pImpl, req.keepVersion);
  if (code != TSDB_CODE_SUCCESS) {
    dError("vgId:%d, failed to set keepVersion to %" PRId64 " since %s", pHead->vgId, req.keepVersion, tstrerror(code));
    terrno = code;
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  dInfo("vgId:%d, successfully set keepVersion to %" PRId64, pHead->vgId, req.keepVersion);

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
  vmCloseVnode(pMgmt, pVnode, true, false);

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
  SVnode *pImpl = vnodeOpen(dstPath, diskPrimary, pMgmt->pTfs, NULL, pMgmt->msgCb, false);

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
      "vgId:%d, vnode management handle msgType:%s, start to alter vnode replica:%d selfIndex:%d leanerReplica:%d "
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
    terrno = TSDB_CODE_DNODE_NOT_MATCH_WITH_LOCAL;
    dError("vgId:%d, dnodeId:%d ep:%s:%u in request, ep:%s:%u in lcoal, %s", vgId, pReplica->id, pReplica->fqdn,
           pReplica->port, tsLocalFqdn, tsServerPort, tstrerror(terrno));
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

  bool commitAndRemoveWal = vnodeShouldRemoveWal(pVnode->pImpl);
  vmCloseVnode(pMgmt, pVnode, commitAndRemoveWal, true);

  int32_t diskPrimary = wrapperCfg.diskPrimary;
  char    path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, vgId);

  dInfo("vgId:%d, start to alter vnode replica at %s", vgId, path);
  if (vnodeAlterReplica(path, &alterReq, diskPrimary, pMgmt->pTfs) < 0) {
    dError("vgId:%d, failed to alter vnode at %s since %s", vgId, path, terrstr());
    return -1;
  }

  dInfo("vgId:%d, begin to open vnode", vgId);
  SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, NULL, pMgmt->msgCb, false);
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

int32_t vmProcessAlterVnodeElectBaselineReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SAlterVnodeElectBaselineReq alterReq = {0};
  if (tDeserializeSAlterVnodeReplicaReq(pMsg->pCont, pMsg->contLen, &alterReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t vgId = alterReq.vgId;
  dInfo(
      "vgId:%d, process alter vnode elect-base-line msgType:%s, electBaseLine:%d",
      vgId, TMSG_INFO(pMsg->msgType), alterReq.electBaseLine);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to alter replica since %s", vgId, terrstr());
    return terrno;
  }

  if(vnodeSetElectBaseline(pVnode->pImpl, alterReq.electBaseLine) != 0){
    vmReleaseVnode(pMgmt, pVnode);
    return -1;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return 0;
}

int32_t vmProcessDropVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t       code = 0;
  SDropVnodeReq dropReq = {0};
  if (tDeserializeSDropVnodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return terrno;
  }

  int32_t vgId = dropReq.vgId;
  dInfo("vgId:%d, start to drop vnode", vgId);

  if (dropReq.dnodeId != pMgmt->pData->dnodeId) {
    terrno = TSDB_CODE_DNODE_NOT_MATCH_WITH_LOCAL;
    dError("vgId:%d, dnodeId:%d, %s", dropReq.vgId, dropReq.dnodeId, tstrerror(terrno));
    return terrno;
  }

  SVnodeObj *pVnode = vmAcquireVnodeImpl(pMgmt, vgId, false);
  if (pVnode == NULL) {
    dInfo("vgId:%d, failed to drop since %s", vgId, terrstr());
    terrno = TSDB_CODE_VND_NOT_EXIST;
    return terrno;
  }

  pVnode->dropped = 1;
  if ((code = vmWriteVnodeListToFile(pMgmt)) != 0) {
    pVnode->dropped = 0;
    vmReleaseVnode(pMgmt, pVnode);
    return code;
  }

  vmCloseVnode(pMgmt, pVnode, false, false);
  if (vmWriteVnodeListToFile(pMgmt) != 0) {
    dError("vgId:%d, failed to write vnode list since %s", vgId, terrstr());
  }

  dInfo("vgId:%d, is dropped", vgId);
  return 0;
}

int32_t vmProcessArbHeartBeatReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SVArbHeartBeatReq arbHbReq = {0};
  SVArbHeartBeatRsp arbHbRsp = {0};
  if (tDeserializeSVArbHeartBeatReq(pMsg->pCont, pMsg->contLen, &arbHbReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (arbHbReq.dnodeId != pMgmt->pData->dnodeId) {
    terrno = TSDB_CODE_DNODE_NOT_MATCH_WITH_LOCAL;
    dError("dnodeId:%d, %s", arbHbReq.dnodeId, tstrerror(terrno));
    goto _OVER;
  }

  if (strlen(arbHbReq.arbToken) == 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("dnodeId:%d arbToken is empty", arbHbReq.dnodeId);
    goto _OVER;
  }

  size_t size = taosArrayGetSize(arbHbReq.hbMembers);

  arbHbRsp.dnodeId = pMgmt->pData->dnodeId;
  tstrncpy(arbHbRsp.arbToken, arbHbReq.arbToken, TSDB_ARB_TOKEN_SIZE);
  arbHbRsp.hbMembers = taosArrayInit(size, sizeof(SVArbHbRspMember));
  if (arbHbRsp.hbMembers == NULL) {
    goto _OVER;
  }

  for (int32_t i = 0; i < size; i++) {
    SVArbHbReqMember *pReqMember = taosArrayGet(arbHbReq.hbMembers, i);
    SVnodeObj        *pVnode = vmAcquireVnode(pMgmt, pReqMember->vgId);
    if (pVnode == NULL) {
      dError("dnodeId:%d vgId:%d not found failed to process arb hb req", arbHbReq.dnodeId, pReqMember->vgId);
      continue;
    }

    SVArbHbRspMember rspMember = {0};
    rspMember.vgId = pReqMember->vgId;
    rspMember.hbSeq = pReqMember->hbSeq;
    if (vnodeGetArbToken(pVnode->pImpl, rspMember.memberToken) != 0) {
      dError("dnodeId:%d vgId:%d failed to get arb token", arbHbReq.dnodeId, pReqMember->vgId);
      vmReleaseVnode(pMgmt, pVnode);
      continue;
    }

    if (vnodeUpdateArbTerm(pVnode->pImpl, arbHbReq.arbTerm) != 0) {
      dError("dnodeId:%d vgId:%d failed to update arb term", arbHbReq.dnodeId, pReqMember->vgId);
      vmReleaseVnode(pMgmt, pVnode);
      continue;
    }

    if (taosArrayPush(arbHbRsp.hbMembers, &rspMember) == NULL) {
      dError("dnodeId:%d vgId:%d failed to push arb hb rsp member", arbHbReq.dnodeId, pReqMember->vgId);
      vmReleaseVnode(pMgmt, pVnode);
      goto _OVER;
    }

    vmReleaseVnode(pMgmt, pVnode);
  }

  SRpcMsg rspMsg = {.info = pMsg->info};
  int32_t rspLen = tSerializeSVArbHeartBeatRsp(NULL, 0, &arbHbRsp);
  if (rspLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = terrno;
    goto _OVER;
  }

  if (tSerializeSVArbHeartBeatRsp(pRsp, rspLen, &arbHbRsp) <= 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rpcFreeCont(pRsp);
    goto _OVER;
  }
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;

  terrno = TSDB_CODE_SUCCESS;

_OVER:
  tFreeSVArbHeartBeatReq(&arbHbReq);
  tFreeSVArbHeartBeatRsp(&arbHbRsp);
  return terrno;
}

SArray *vmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(32, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_SUBMIT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_QUERY, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_CONTINUE, vmPutMsgToQueryQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_FETCH, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_UPDATE_TAG_VAL, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLE_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_VSUBTABLES_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_VSTB_REF_DBS, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLE_CFG, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_BATCH_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLES_META, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_CANCEL_TASK, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_DROP_TASK, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_TASK_NOTIFY, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_STB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TTL_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FETCH_TTL_EXPIRED_TBS, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_STB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_STB, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SNODE_DROP_TABLE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
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
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_TRIM_PROGRESS, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_SCAN_PROGRESS, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_KILL_COMPACT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_KILL_SCAN, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_KILL_TRIM, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TABLE_NAME, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_RSMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_RSMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_RSMA, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_REPLICA, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIG, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIRM, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DISABLE_WRITE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SET_KEEP_VERSION, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_HASHRANGE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMPACT, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SCAN, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TRIM, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_LIST_SSMIGRATE_FILESETS, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SSMIGRATE_FILESET, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_SSMIGRATE_PROGRESS, vmPutMsgToFetchQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FOLLOWER_SSMIGRATE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_KILL_SSMIGRATE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TRIM_WAL, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_VNODE, vmPutMsgToMultiMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_MOUNT_VNODE, vmPutMsgToMultiMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_RETRIEVE_MOUNT_PATH, vmPutMsgToMultiMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_VNODE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_VNODE_TYPE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CHECK_VNODE_LEARNER_CATCHUP, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CONFIG_CHANGE, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_ELECTBASELINE, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;

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

  if (dmSetMgmtHandle(pArray, TDMT_VND_ARB_HEARTBEAT, vmPutMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ARB_CHECK_SYNC, vmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SET_ASSIGNED_LEADER, vmPutMsgToSyncQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_FETCH, vmPutMsgToStreamReaderQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_PULL, vmPutMsgToStreamReaderQueue, 0) == NULL) goto _OVER;
  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}

void vmUpdateMetricsInfo(SVnodeMgmt *pMgmt, int64_t clusterId) {
  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) {
      continue;
    }

    SVnodeObj *pVnode = *ppVnode;
    if (!pVnode->failed) {
      SRawWriteMetrics metrics = {0};
      if (vnodeGetRawWriteMetrics(pVnode->pImpl, &metrics) == 0) {
        // Add the metrics to the global metrics system with cluster ID
        SName   name = {0};
        int32_t code = tNameFromString(&name, pVnode->pImpl->config.dbname, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
        if (code < 0) {
          dError("failed to get db name since %s", tstrerror(code));
          continue;
        }
        code = addWriteMetrics(pVnode->vgId, pMgmt->pData->dnodeId, clusterId, tsLocalEp, name.dbname, &metrics);
        if (code != TSDB_CODE_SUCCESS) {
          dError("Failed to add write metrics for vgId: %d, code: %d", pVnode->vgId, code);
        } else {
          // After successfully adding metrics, reset the vnode's write metrics using atomic operations
          if (vnodeResetRawWriteMetrics(pVnode->pImpl, &metrics) != 0) {
            dError("Failed to reset write metrics for vgId: %d", pVnode->vgId);
          }
        }
      } else {
        dError("Failed to get write metrics for vgId: %d", pVnode->vgId);
      }
    }
    pIter = taosHashIterate(pMgmt->runngingHash, pIter);
  }

  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
}