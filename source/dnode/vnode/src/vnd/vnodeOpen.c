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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "cos.h"
#include "sync.h"
#include "tsdb.h"
#include "vnd.h"

int32_t vnodeGetPrimaryDir(const char *relPath, int32_t diskPrimary, STfs *pTfs, char *buf, size_t bufLen) {
  if (pTfs) {
    SDiskID diskId = {0};
    diskId.id = diskPrimary;
    snprintf(buf, bufLen - 1, "%s%s%s", tfsGetDiskPath(pTfs, diskId), TD_DIRSEP, relPath);
  } else {
    snprintf(buf, bufLen - 1, "%s", relPath);
  }
  buf[bufLen - 1] = '\0';
  return 0;
}

static int32_t vnodeMkDir(STfs *pTfs, const char *path) {
  if (pTfs) {
    return tfsMkdirRecur(pTfs, path);
  } else {
    return taosMkDir(path);
  }
}

int32_t vnodeCreate(const char *path, SVnodeCfg *pCfg, int32_t diskPrimary, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};

  // check config
  if (vnodeCheckCfg(pCfg) < 0) {
    vError("vgId:%d, failed to create vnode since:%s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  // create vnode env
  if (vnodeMkDir(pTfs, path)) {
    vError("vgId:%d, failed to prepare vnode dir since %s, path: %s", pCfg->vgId, strerror(errno), path);
    return TAOS_SYSTEM_ERROR(errno);
  }
  vnodeGetPrimaryDir(path, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);

  if (pCfg) {
    info.config = *pCfg;
  } else {
    info.config = vnodeCfgDefault;
  }
  info.state.committed = -1;
  info.state.applied = -1;
  info.state.commitID = 0;

  SVnodeInfo oldInfo = {0};
  oldInfo.config = vnodeCfgDefault;
  if (vnodeLoadInfo(dir, &oldInfo) == 0) {
    vWarn("vgId:%d, vnode config info already exists at %s.", oldInfo.config.vgId, dir);
    return (oldInfo.config.dbId == info.config.dbId) ? 0 : -1;
  }

  vInfo("vgId:%d, save config while create", info.config.vgId);
  if (vnodeSaveInfo(dir, &info) < 0 || vnodeCommitInfo(dir) < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pCfg ? pCfg->vgId : 0, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, vnode is created", info.config.vgId);
  return 0;
}

int32_t vnodeAlterReplica(const char *path, SAlterVnodeReplicaReq *pReq, int32_t diskPrimary, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  int32_t    ret = 0;

  vnodeGetPrimaryDir(path, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);

  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to read vnode config from %s since %s", pReq->vgId, path, tstrerror(terrno));
    return -1;
  }

  SSyncCfg *pCfg = &info.config.syncCfg;

  pCfg->replicaNum = 0;
  pCfg->totalReplicaNum = 0;
  memset(&pCfg->nodeInfo, 0, sizeof(pCfg->nodeInfo));

  for (int i = 0; i < pReq->replica; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    pNode->nodeId = pReq->replicas[i].id;
    pNode->nodePort = pReq->replicas[i].port;
    tstrncpy(pNode->nodeFqdn, pReq->replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodeRole = TAOS_SYNC_ROLE_VOTER;
    (void)tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
    vInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d", pReq->vgId, i, pNode->nodeFqdn, pNode->nodePort, pNode->nodeId);
    pCfg->replicaNum++;
  }
  if (pReq->selfIndex != -1) {
    pCfg->myIndex = pReq->selfIndex;
  }
  for (int i = pCfg->replicaNum; i < pReq->replica + pReq->learnerReplica; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    pNode->nodeId = pReq->learnerReplicas[pCfg->totalReplicaNum].id;
    pNode->nodePort = pReq->learnerReplicas[pCfg->totalReplicaNum].port;
    pNode->nodeRole = TAOS_SYNC_ROLE_LEARNER;
    tstrncpy(pNode->nodeFqdn, pReq->learnerReplicas[pCfg->totalReplicaNum].fqdn, sizeof(pNode->nodeFqdn));
    (void)tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
    vInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d", pReq->vgId, i, pNode->nodeFqdn, pNode->nodePort, pNode->nodeId);
    pCfg->totalReplicaNum++;
  }
  pCfg->totalReplicaNum += pReq->replica;
  if (pReq->learnerSelfIndex != -1) {
    pCfg->myIndex = pReq->replica + pReq->learnerSelfIndex;
  }
  pCfg->changeVersion = pReq->changeVersion;

  vInfo("vgId:%d, save config while alter, replicas:%d totalReplicas:%d selfIndex:%d changeVersion:%d", pReq->vgId,
        pCfg->replicaNum, pCfg->totalReplicaNum, pCfg->myIndex, pCfg->changeVersion);

  info.config.syncCfg = *pCfg;
  ret = vnodeSaveInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pReq->vgId, tstrerror(terrno));
    return -1;
  }

  ret = vnodeCommitInfo(dir);
  if (ret < 0) {
    vError("vgId:%d, failed to commit vnode config since %s", pReq->vgId, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, vnode config is saved", info.config.vgId);
  return 0;
}

static int32_t vnodeVgroupIdLen(int32_t vgId) {
  char tmp[TSDB_FILENAME_LEN];
  sprintf(tmp, "%d", vgId);
  return strlen(tmp);
}

int32_t vnodeRenameVgroupId(const char *srcPath, const char *dstPath, int32_t srcVgId, int32_t dstVgId,
                            int32_t diskPrimary, STfs *pTfs) {
  int32_t ret = 0;

  char oldRname[TSDB_FILENAME_LEN] = {0};
  char newRname[TSDB_FILENAME_LEN] = {0};
  char tsdbPath[TSDB_FILENAME_LEN] = {0};
  char tsdbFilePrefix[TSDB_FILENAME_LEN] = {0};
  snprintf(tsdbPath, TSDB_FILENAME_LEN, "%s%stsdb", srcPath, TD_DIRSEP);
  snprintf(tsdbFilePrefix, TSDB_FILENAME_LEN, "tsdb%sv", TD_DIRSEP);
  int32_t prefixLen = strlen(tsdbFilePrefix);

  STfsDir *tsdbDir = tfsOpendir(pTfs, tsdbPath);
  if (tsdbDir == NULL) return 0;

  while (1) {
    const STfsFile *tsdbFile = tfsReaddir(tsdbDir);
    if (tsdbFile == NULL) break;
    if (tsdbFile->rname[0] == '\0') continue;
    tstrncpy(oldRname, tsdbFile->rname, TSDB_FILENAME_LEN);

    char *tsdbFilePrefixPos = strstr(oldRname, tsdbFilePrefix);
    if (tsdbFilePrefixPos == NULL) continue;

    int32_t tsdbFileVgId = atoi(tsdbFilePrefixPos + prefixLen);
    if (tsdbFileVgId == srcVgId) {
      char *tsdbFileSurfixPos = tsdbFilePrefixPos + prefixLen + vnodeVgroupIdLen(srcVgId);

      tsdbFilePrefixPos[prefixLen] = 0;
      snprintf(newRname, TSDB_FILENAME_LEN, "%s%d%s", oldRname, dstVgId, tsdbFileSurfixPos);
      vInfo("vgId:%d, rename file from %s to %s", dstVgId, tsdbFile->rname, newRname);

      ret = tfsRename(pTfs, diskPrimary, tsdbFile->rname, newRname);
      if (ret != 0) {
        vError("vgId:%d, failed to rename file from %s to %s since %s", dstVgId, tsdbFile->rname, newRname, terrstr());
        tfsClosedir(tsdbDir);
        return ret;
      }
    }
  }

  tfsClosedir(tsdbDir);

  vInfo("vgId:%d, rename dir from %s to %s", dstVgId, srcPath, dstPath);
  ret = tfsRename(pTfs, diskPrimary, srcPath, dstPath);
  if (ret != 0) {
    vError("vgId:%d, failed to rename dir from %s to %s since %s", dstVgId, srcPath, dstPath, terrstr());
  }
  return ret;
}

int32_t vnodeAlterHashRange(const char *srcPath, const char *dstPath, SAlterVnodeHashRangeReq *pReq,
                            int32_t diskPrimary, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  int32_t    ret = 0;

  vnodeGetPrimaryDir(srcPath, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);

  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to read vnode config from %s since %s", pReq->srcVgId, srcPath, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, alter hashrange from [%u, %u] to [%u, %u]", pReq->srcVgId, info.config.hashBegin, info.config.hashEnd,
        pReq->hashBegin, pReq->hashEnd);
  info.config.vgId = pReq->dstVgId;
  info.config.hashBegin = pReq->hashBegin;
  info.config.hashEnd = pReq->hashEnd;
  info.config.hashChange = true;
  info.config.walCfg.vgId = pReq->dstVgId;
  info.config.syncCfg.changeVersion = pReq->changeVersion;

  SSyncCfg *pCfg = &info.config.syncCfg;
  pCfg->myIndex = 0;
  pCfg->replicaNum = 1;
  pCfg->totalReplicaNum = 1;
  memset(&pCfg->nodeInfo, 0, sizeof(pCfg->nodeInfo));

  vInfo("vgId:%d, alter vnode replicas to 1", pReq->srcVgId);
  SNodeInfo *pNode = &pCfg->nodeInfo[0];
  pNode->nodePort = tsServerPort;
  tstrncpy(pNode->nodeFqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  (void)tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
  vInfo("vgId:%d, ep:%s:%u dnode:%d", pReq->srcVgId, pNode->nodeFqdn, pNode->nodePort, pNode->nodeId);

  info.config.syncCfg = *pCfg;

  ret = vnodeSaveInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pReq->dstVgId, tstrerror(terrno));
    return -1;
  }

  ret = vnodeCommitInfo(dir);
  if (ret < 0) {
    vError("vgId:%d, failed to commit vnode config since %s", pReq->dstVgId, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, rename %s to %s", pReq->dstVgId, srcPath, dstPath);
  ret = vnodeRenameVgroupId(srcPath, dstPath, pReq->srcVgId, pReq->dstVgId, diskPrimary, pTfs);
  if (ret < 0) {
    vError("vgId:%d, failed to rename vnode from %s to %s since %s", pReq->dstVgId, srcPath, dstPath,
           tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, vnode hashrange is altered", info.config.vgId);
  return 0;
}

int32_t vnodeRestoreVgroupId(const char *srcPath, const char *dstPath, int32_t srcVgId, int32_t dstVgId,
                             int32_t diskPrimary, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};

  vnodeGetPrimaryDir(dstPath, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);
  if (vnodeLoadInfo(dir, &info) == 0) {
    if (info.config.vgId != dstVgId) {
      vError("vgId:%d, unexpected vnode config.vgId:%d", dstVgId, info.config.vgId);
      return -1;
    }
    return dstVgId;
  }

  vnodeGetPrimaryDir(srcPath, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);
  if (vnodeLoadInfo(dir, &info) < 0) {
    vError("vgId:%d, failed to read vnode config from %s since %s", srcVgId, srcPath, tstrerror(terrno));
    return -1;
  }

  if (info.config.vgId == srcVgId) {
    vInfo("vgId:%d, rollback alter hashrange", srcVgId);
    return srcVgId;
  } else if (info.config.vgId != dstVgId) {
    vError("vgId:%d, unexpected vnode config.vgId:%d", dstVgId, info.config.vgId);
    return -1;
  }

  vInfo("vgId:%d, rename %s to %s", dstVgId, srcPath, dstPath);
  if (vnodeRenameVgroupId(srcPath, dstPath, srcVgId, dstVgId, diskPrimary, pTfs) < 0) {
    vError("vgId:%d, failed to rename vnode from %s to %s since %s", dstVgId, srcPath, dstPath, tstrerror(terrno));
    return -1;
  }

  return dstVgId;
}

void vnodeDestroy(int32_t vgId, const char *path, STfs *pTfs) {
  vInfo("path:%s is removed while destroy vnode", path);
  tfsRmdir(pTfs, path);

  int32_t nlevel = tfsGetLevel(pTfs);
  if (vgId > 0 && nlevel > 1 && tsS3Enabled) {
    char vnode_prefix[TSDB_FILENAME_LEN];
    snprintf(vnode_prefix, TSDB_FILENAME_LEN, "v%df", vgId);
    s3DeleteObjectsByPrefix(vnode_prefix);
  }
}

static int32_t vnodeCheckDisk(int32_t diskPrimary, STfs *pTfs) {
  int32_t ndisk = 1;
  if (pTfs) {
    ndisk = tfsGetDisksAtLevel(pTfs, 0);
  }
  if (diskPrimary < 0 || diskPrimary >= ndisk) {
    vError("disk:%d is unavailable from the %d disks mounted at level 0", diskPrimary, ndisk);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }
  return 0;
}

SVnode *vnodeOpen(const char *path, int32_t diskPrimary, STfs *pTfs, SMsgCb msgCb, bool force) {
  SVnode    *pVnode = NULL;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  char       tdir[TSDB_FILENAME_LEN * 2] = {0};
  int32_t    ret = 0;
  terrno = TSDB_CODE_SUCCESS;

  if (vnodeCheckDisk(diskPrimary, pTfs)) {
    vError("failed to open vnode from %s since %s. diskPrimary:%d", path, terrstr(), diskPrimary);
    return NULL;
  }
  vnodeGetPrimaryDir(path, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);

  info.config = vnodeCfgDefault;

  // load vnode info
  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("failed to open vnode from %s since %s", path, tstrerror(terrno));
    terrno = TSDB_CODE_NEED_RETRY;
    return NULL;
  }

  if (vnodeMkDir(pTfs, path)) {
    vError("vgId:%d, failed to prepare vnode dir since %s, path: %s", info.config.vgId, strerror(errno), path);
    return NULL;
  }
  // save vnode info on dnode ep changed
  bool      updated = false;
  SSyncCfg *pCfg = &info.config.syncCfg;
  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    if (tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort)) {
      updated = true;
    }
  }
  if (updated) {
    vInfo("vgId:%d, save vnode info since dnode info changed", info.config.vgId);
    (void)vnodeSaveInfo(dir, &info);
    (void)vnodeCommitInfo(dir);
  }

  // create handle
  pVnode = taosMemoryCalloc(1, sizeof(*pVnode) + strlen(path) + 1);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    vError("vgId:%d, failed to open vnode since %s", info.config.vgId, tstrerror(terrno));
    return NULL;
  }

  pVnode->path = (char *)&pVnode[1];
  strcpy(pVnode->path, path);
  pVnode->config = info.config;
  pVnode->state.committed = info.state.committed;
  pVnode->state.commitTerm = info.state.commitTerm;
  pVnode->state.commitID = info.state.commitID;
  pVnode->state.applied = info.state.committed;
  pVnode->state.applyTerm = info.state.commitTerm;
  pVnode->pTfs = pTfs;
  pVnode->diskPrimary = diskPrimary;
  pVnode->msgCb = msgCb;
  taosThreadMutexInit(&pVnode->lock, NULL);
  pVnode->blocked = false;

  tsem_init(&pVnode->syncSem, 0, 0);
  taosThreadMutexInit(&pVnode->mutex, NULL);
  taosThreadCondInit(&pVnode->poolNotEmpty, NULL);

  if (vnodeAChannelInit(vnodeAsyncHandle[0], &pVnode->commitChannel) != 0) {
    vError("vgId:%d, failed to init commit channel", TD_VID(pVnode));
    goto _err;
  }

  int8_t rollback = vnodeShouldRollback(pVnode);

  // open buffer pool
  if (vnodeOpenBufPool(pVnode) < 0) {
    vError("vgId:%d, failed to open vnode buffer pool since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open meta
  if (metaOpen(pVnode, &pVnode->pMeta, rollback) < 0) {
    vError("vgId:%d, failed to open vnode meta since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  if (metaUpgrade(pVnode, &pVnode->pMeta) < 0) {
    vError("vgId:%d, failed to upgrade meta since %s", TD_VID(pVnode), tstrerror(terrno));
  }

  // open tsdb
  if (!VND_IS_RSMA(pVnode) && tsdbOpen(pVnode, &VND_TSDB(pVnode), VNODE_TSDB_DIR, NULL, rollback, force) < 0) {
    vError("vgId:%d, failed to open vnode tsdb since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open wal
  sprintf(tdir, "%s%s%s", dir, TD_DIRSEP, VNODE_WAL_DIR);
  taosRealPath(tdir, NULL, sizeof(tdir));

  pVnode->pWal = walOpen(tdir, &(pVnode->config.walCfg));
  if (pVnode->pWal == NULL) {
    vError("vgId:%d, failed to open vnode wal since %s. wal:%s", TD_VID(pVnode), tstrerror(terrno), tdir);
    goto _err;
  }

  // open tq
  sprintf(tdir, "%s%s%s", dir, TD_DIRSEP, VNODE_TQ_DIR);
  taosRealPath(tdir, NULL, sizeof(tdir));

  // open query
  if (vnodeQueryOpen(pVnode)) {
    vError("vgId:%d, failed to open vnode query since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // sma required the tq is initialized before the vnode open
  pVnode->pTq = tqOpen(tdir, pVnode);
  if (pVnode->pTq == NULL) {
    vError("vgId:%d, failed to open vnode tq since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open sma
  if (smaOpen(pVnode, rollback, force)) {
    vError("vgId:%d, failed to open vnode sma since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // vnode begin
  if (vnodeBegin(pVnode) < 0) {
    vError("vgId:%d, failed to begin since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // open sync
  vInfo("vgId:%d, start to open sync, changeVersion:%d", TD_VID(pVnode), info.config.syncCfg.changeVersion);
  if (vnodeSyncOpen(pVnode, dir, info.config.syncCfg.changeVersion)) {
    vError("vgId:%d, failed to open sync since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  if (rollback) {
    vnodeRollback(pVnode);
  }

  snprintf(pVnode->monitor.strClusterId, TSDB_CLUSTER_ID_LEN, "%"PRId64, pVnode->config.syncCfg.nodeInfo[0].clusterId);
  snprintf(pVnode->monitor.strDnodeId, TSDB_NODE_ID_LEN, "%"PRId32, pVnode->config.syncCfg.nodeInfo[0].nodeId);
  snprintf(pVnode->monitor.strVgId, TSDB_VGROUP_ID_LEN, "%"PRId32, pVnode->config.vgId);

  if(pVnode->monitor.insertCounter == NULL){
    int32_t label_count = 7;
    const char *sample_labels[] = {VNODE_METRIC_TAG_NAME_SQL_TYPE, VNODE_METRIC_TAG_NAME_CLUSTER_ID,
                                   VNODE_METRIC_TAG_NAME_DNODE_ID, VNODE_METRIC_TAG_NAME_DNODE_EP,
                                   VNODE_METRIC_TAG_NAME_VGROUP_ID, VNODE_METRIC_TAG_NAME_USERNAME,
                                   VNODE_METRIC_TAG_NAME_RESULT};
    taos_counter_t *counter = taos_counter_new(VNODE_METRIC_SQL_COUNT, "counter for insert sql",  
                                                label_count, sample_labels);
    vInfo("vgId:%d, new metric:%p",TD_VID(pVnode), counter);
    if(taos_collector_registry_register_metric(counter) == 1){
      taos_counter_destroy(counter);
      counter = taos_collector_registry_get_metric(VNODE_METRIC_SQL_COUNT);
      vInfo("vgId:%d, get metric from registry:%p",TD_VID(pVnode), counter);
    }
    pVnode->monitor.insertCounter = counter;
    vInfo("vgId:%d, succeed to set metric:%p",TD_VID(pVnode), counter);
  }

  return pVnode;

_err:
  if (pVnode->pQuery) vnodeQueryClose(pVnode);
  if (pVnode->pTq) tqClose(pVnode->pTq);
  if (pVnode->pWal) walClose(pVnode->pWal);
  if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
  if (pVnode->pSma) smaClose(pVnode->pSma);
  if (pVnode->pMeta) metaClose(&pVnode->pMeta);
  if (pVnode->freeList) vnodeCloseBufPool(pVnode);

  taosMemoryFree(pVnode);
  return NULL;
}

void vnodePreClose(SVnode *pVnode) {
  vnodeSyncPreClose(pVnode);
  vnodeQueryPreClose(pVnode);
}

void vnodePostClose(SVnode *pVnode) { vnodeSyncPostClose(pVnode); }

void vnodeClose(SVnode *pVnode) {
  if (pVnode) {
    vnodeAWait(vnodeAsyncHandle[0], pVnode->commitTask);
    vnodeAChannelDestroy(vnodeAsyncHandle[0], pVnode->commitChannel, true);
    vnodeSyncClose(pVnode);
    vnodeQueryClose(pVnode);
    tqClose(pVnode->pTq);
    walClose(pVnode->pWal);
    if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
    smaClose(pVnode->pSma);
    if (pVnode->pMeta) metaClose(&pVnode->pMeta);
    vnodeCloseBufPool(pVnode);

    // destroy handle
    tsem_destroy(&pVnode->syncSem);
    taosThreadCondDestroy(&pVnode->poolNotEmpty);
    taosThreadMutexDestroy(&pVnode->mutex);
    taosThreadMutexDestroy(&pVnode->lock);
    taosMemoryFree(pVnode);
  }
}

// start the sync timer after the queue is ready
int32_t vnodeStart(SVnode *pVnode) {
  ASSERT(pVnode);
  return vnodeSyncStart(pVnode);
}

int32_t vnodeIsCatchUp(SVnode *pVnode) { return syncIsCatchUp(pVnode->sync); }

ESyncRole vnodeGetRole(SVnode *pVnode) { return syncGetRole(pVnode->sync); }

void vnodeStop(SVnode *pVnode) {}

int64_t vnodeGetSyncHandle(SVnode *pVnode) { return pVnode->sync; }
