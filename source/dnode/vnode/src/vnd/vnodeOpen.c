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

#include "vnd.h"

int32_t vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};

  // check config
  if (vnodeCheckCfg(pCfg) < 0) {
    vError("vgId:%d, failed to create vnode since:%s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  // create vnode env
  if (pTfs) {
    if (tfsMkdirAt(pTfs, path, (SDiskID){0}) < 0) {
      vError("vgId:%d, failed to create vnode since:%s", pCfg->vgId, tstrerror(terrno));
      return -1;
    }
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  } else {
    if (taosMkDir(path)) {
      return TAOS_SYSTEM_ERROR(errno);
    }
    snprintf(dir, TSDB_FILENAME_LEN, "%s", path);
  }

  if (pCfg) {
    info.config = *pCfg;
  } else {
    info.config = vnodeCfgDefault;
  }
  info.state.committed = -1;
  info.state.applied = -1;
  info.state.commitID = 0;

  vInfo("vgId:%d, save config while create", info.config.vgId);
  if (vnodeSaveInfo(dir, &info) < 0 || vnodeCommitInfo(dir) < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pCfg ? pCfg->vgId : 0, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, vnode is created", info.config.vgId);
  return 0;
}

int32_t vnodeAlterReplica(const char *path, SAlterVnodeReplicaReq *pReq, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  int32_t    ret = 0;

  if (pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", path);
  }

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
  if(pReq->selfIndex != -1){
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
  if(pReq->learnerSelfIndex != -1){
    pCfg->myIndex = pReq->replica + pReq->learnerSelfIndex;
  }

  vInfo("vgId:%d, save config while alter, replicas:%d totalReplicas:%d selfIndex:%d", 
            pReq->vgId, pCfg->replicaNum, pCfg->totalReplicaNum, pCfg->myIndex);

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

int32_t vnodeRenameVgroupId(const char *srcPath, const char *dstPath, int32_t srcVgId, int32_t dstVgId, STfs *pTfs) {
  int32_t ret = tfsRename(pTfs, srcPath, dstPath);
  if (ret != 0) return ret;

  char oldRname[TSDB_FILENAME_LEN] = {0};
  char newRname[TSDB_FILENAME_LEN] = {0};
  char tsdbPath[TSDB_FILENAME_LEN] = {0};
  char tsdbFilePrefix[TSDB_FILENAME_LEN] = {0};
  snprintf(tsdbPath, TSDB_FILENAME_LEN, "%s%stsdb", dstPath, TD_DIRSEP);
  snprintf(tsdbFilePrefix, TSDB_FILENAME_LEN, "tsdb%sv", TD_DIRSEP);

  STfsDir *tsdbDir = tfsOpendir(pTfs, tsdbPath);
  if (tsdbDir == NULL) return 0;

  while (1) {
    const STfsFile *tsdbFile = tfsReaddir(tsdbDir);
    if (tsdbFile == NULL) break;
    if (tsdbFile->rname[0] == '\0') continue;
    tstrncpy(oldRname, tsdbFile->rname, TSDB_FILENAME_LEN);

    char *tsdbFilePrefixPos = strstr(oldRname, tsdbFilePrefix);
    if (tsdbFilePrefixPos == NULL) continue;

    int32_t tsdbFileVgId = atoi(tsdbFilePrefixPos + 6);
    if (tsdbFileVgId == srcVgId) {
      char *tsdbFileSurfixPos = strstr(tsdbFilePrefixPos, "f");
      if (tsdbFileSurfixPos == NULL) continue;

      tsdbFilePrefixPos[6] = 0;
      snprintf(newRname, TSDB_FILENAME_LEN, "%s%d%s", oldRname, dstVgId, tsdbFileSurfixPos);
      vInfo("vgId:%d, rename file from %s to %s", dstVgId, tsdbFile->rname, newRname);

      ret = tfsRename(pTfs, tsdbFile->rname, newRname);
      if (ret != 0) {
        vInfo("vgId:%d, failed to rename file from %s to %s since %s", dstVgId, tsdbFile->rname, newRname, terrstr());
        tfsClosedir(tsdbDir);
        return ret;
      }
    }
  }

  tfsClosedir(tsdbDir);
  return 0;
}

int32_t vnodeAlterHashRange(const char *srcPath, const char *dstPath, SAlterVnodeHashRangeReq *pReq, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  int32_t    ret = 0;

  if (pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, srcPath);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", srcPath);
  }

  // todo add stat file to handle exception while vnode open

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
  ret = vnodeRenameVgroupId(srcPath, dstPath, pReq->srcVgId, pReq->dstVgId, pTfs);
  if (ret < 0) {
    vError("vgId:%d, failed to rename vnode from %s to %s since %s", pReq->dstVgId, srcPath, dstPath,
           tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d, vnode hashrange is altered", info.config.vgId);
  return 0;
}

void vnodeDestroy(const char *path, STfs *pTfs) {
  vInfo("path:%s is removed while destroy vnode", path);
  tfsRmdir(pTfs, path);
}

SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb) {
  SVnode    *pVnode = NULL;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  char       tdir[TSDB_FILENAME_LEN * 2] = {0};
  int32_t    ret = 0;

  if (pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", path);
  }

  info.config = vnodeCfgDefault;

  // load vnode info
  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("failed to open vnode from %s since %s", path, tstrerror(terrno));
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
  pVnode->msgCb = msgCb;
  taosThreadMutexInit(&pVnode->lock, NULL);
  pVnode->blocked = false;

  tsem_init(&pVnode->syncSem, 0, 0);
  tsem_init(&(pVnode->canCommit), 0, 1);
  taosThreadMutexInit(&pVnode->mutex, NULL);
  taosThreadCondInit(&pVnode->poolNotEmpty, NULL);

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

  // open tsdb
  if (!VND_IS_RSMA(pVnode) && tsdbOpen(pVnode, &VND_TSDB(pVnode), VNODE_TSDB_DIR, NULL, rollback) < 0) {
    vError("vgId:%d, failed to open vnode tsdb since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open sma
  if (smaOpen(pVnode, rollback)) {
    vError("vgId:%d, failed to open vnode sma since %s", TD_VID(pVnode), tstrerror(terrno));
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
  pVnode->pTq = tqOpen(tdir, pVnode);
  if (pVnode->pTq == NULL) {
    vError("vgId:%d, failed to open vnode tq since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open query
  if (vnodeQueryOpen(pVnode)) {
    vError("vgId:%d, failed to open vnode query since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // vnode begin
  if (vnodeBegin(pVnode) < 0) {
    vError("vgId:%d, failed to begin since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // open sync
  if (vnodeSyncOpen(pVnode, dir)) {
    vError("vgId:%d, failed to open sync since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  if (rollback) {
    vnodeRollback(pVnode);
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

  tsem_destroy(&(pVnode->canCommit));
  taosMemoryFree(pVnode);
  return NULL;
}

void vnodePreClose(SVnode *pVnode) {
  vnodeQueryPreClose(pVnode);
  vnodeSyncPreClose(pVnode);
}

void vnodePostClose(SVnode *pVnode) { vnodeSyncPostClose(pVnode); }

void vnodeClose(SVnode *pVnode) {
  if (pVnode) {
    tsem_wait(&pVnode->canCommit);
    vnodeSyncClose(pVnode);
    vnodeQueryClose(pVnode);
    walClose(pVnode->pWal);
    tqClose(pVnode->pTq);
    if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
    smaClose(pVnode->pSma);
    if (pVnode->pMeta) metaClose(&pVnode->pMeta);
    vnodeCloseBufPool(pVnode);
    tsem_post(&pVnode->canCommit);

    // destroy handle
    tsem_destroy(&(pVnode->canCommit));
    tsem_destroy(&pVnode->syncSem);
    taosThreadCondDestroy(&pVnode->poolNotEmpty);
    taosThreadMutexDestroy(&pVnode->mutex);
    taosThreadMutexDestroy(&pVnode->lock);
    taosMemoryFree(pVnode);
  }
}

// start the sync timer after the queue is ready
int32_t vnodeStart(SVnode *pVnode) { return vnodeSyncStart(pVnode); }

int32_t vnodeIsCatchUp(SVnode *pVnode){
  return syncIsCatchUp(pVnode->sync);
}

ESyncRole vnodeGetRole(SVnode *pVnode){
  return syncGetRole(pVnode->sync);
}

void vnodeStop(SVnode *pVnode) {}

int64_t vnodeGetSyncHandle(SVnode *pVnode) { return pVnode->sync; }

void vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnapshot) {
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = pVnode->state.committed;
  pSnapshot->lastApplyTerm = pVnode->state.commitTerm;
  pSnapshot->lastConfigIndex = -1;
}
