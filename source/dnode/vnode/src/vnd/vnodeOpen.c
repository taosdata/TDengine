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

#include "sync.h"
#include "tss.h"
#include "tq.h"
#include "tsdb.h"
#include "vnd.h"

void vnodeGetPrimaryDir(const char *relPath, int32_t diskPrimary, STfs *pTfs, char *buf, size_t bufLen) {
  if (pTfs) {
    SDiskID diskId = {0};
    diskId.id = diskPrimary;
    snprintf(buf, bufLen - 1, "%s%s%s", tfsGetDiskPath(pTfs, diskId), TD_DIRSEP, relPath);
  } else {
    snprintf(buf, bufLen - 1, "%s", relPath);
  }
  buf[bufLen - 1] = '\0';
}

void vnodeGetPrimaryPath(SVnode *pVnode, bool mount, char *buf, size_t bufLen) {
  if (pVnode->mounted) {
    if (mount) {  // mount path
      SDiskID diskId = {0};
      diskId.id = pVnode->diskPrimary;
      snprintf(buf, bufLen - 1, "%s%svnode%svnode%d", tfsGetDiskPath(pVnode->pMountTfs, diskId), TD_DIRSEP, TD_DIRSEP,
               pVnode->config.mountVgId);
    } else {  // host path
      vnodeGetPrimaryDir(pVnode->path, 0, pVnode->pTfs, buf, bufLen);
    }
    buf[bufLen - 1] = '\0';

  } else {
    vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, buf, bufLen);
  }
}

static int32_t vnodeMkDir(STfs *pTfs, const char *path) {
  if (pTfs) {
    return tfsMkdirRecur(pTfs, path);
  } else {
    return taosMkDir(path);
  }
}

int32_t vnodeCreate(const char *path, SVnodeCfg *pCfg, int32_t diskPrimary, STfs *pTfs) {
  int32_t    code = 0;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};

  // check config
  if ((code = vnodeCheckCfg(pCfg)) < 0) {
    vError("vgId:%d, failed to create vnode since:%s", pCfg->vgId, tstrerror(code));
    return code;
  }

  // create vnode env
  if (vnodeMkDir(pTfs, path)) {
    vError("vgId:%d, failed to prepare vnode dir since %s, path: %s", pCfg->vgId, strerror(ERRNO), path);
    return TAOS_SYSTEM_ERROR(ERRNO);
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
    code = (oldInfo.config.dbId == info.config.dbId) ? 0 : TSDB_CODE_VND_ALREADY_EXIST_BUT_NOT_MATCH;
    if (code == 0) {
      vWarn("vgId:%d, vnode config info already exists at %s.", oldInfo.config.vgId, dir);
    } else {
      vError("vgId:%d, vnode config info already exists at %s. oldDbId:%" PRId64 "(%s) at cluster:%" PRId64
             ", newDbId:%" PRId64 "(%s) at cluser:%" PRId64 ", code:%s",
             oldInfo.config.vgId, dir, oldInfo.config.dbId, oldInfo.config.dbname,
             oldInfo.config.syncCfg.nodeInfo[oldInfo.config.syncCfg.myIndex].clusterId, info.config.dbId,
             info.config.dbname, info.config.syncCfg.nodeInfo[info.config.syncCfg.myIndex].clusterId, tstrerror(code));
    }
    return code;
  }

  vInfo("vgId:%d, save config while create", info.config.vgId);
  if ((code = vnodeSaveInfo(dir, &info)) < 0 || (code = vnodeCommitInfo(dir)) < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pCfg ? pCfg->vgId : 0, tstrerror(code));
    return code;
  }

  vInfo("vgId:%d, vnode is created", info.config.vgId);
  return 0;
}

bool vnodeShouldRemoveWal(SVnode *pVnode) { return pVnode->config.walCfg.clearFiles == 1; }

int32_t vnodeAlterReplica(const char *path, SAlterVnodeReplicaReq *pReq, int32_t diskPrimary, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  int32_t    ret = 0;

  vnodeGetPrimaryDir(path, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);

  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to read vnode config from %s since %s", pReq->vgId, path, tstrerror(terrno));
    return ret;
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
    bool ret = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
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
    bool ret = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
    vInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d", pReq->vgId, i, pNode->nodeFqdn, pNode->nodePort, pNode->nodeId);
    pCfg->totalReplicaNum++;
  }
  pCfg->totalReplicaNum += pReq->replica;
  if (pReq->learnerSelfIndex != -1) {
    pCfg->myIndex = pReq->replica + pReq->learnerSelfIndex;
  }
  pCfg->changeVersion = pReq->changeVersion;

  if (info.config.walCfg.clearFiles) {
    info.config.walCfg.clearFiles = 0;

    vInfo("vgId:%d, reset wal clearFiles", pReq->vgId);
  }

  vInfo("vgId:%d, save config while alter, replicas:%d totalReplicas:%d selfIndex:%d changeVersion:%d", pReq->vgId,
        pCfg->replicaNum, pCfg->totalReplicaNum, pCfg->myIndex, pCfg->changeVersion);

  info.config.syncCfg = *pCfg;
  ret = vnodeSaveInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pReq->vgId, tstrerror(terrno));
    return ret;
  }

  ret = vnodeCommitInfo(dir);
  if (ret < 0) {
    vError("vgId:%d, failed to commit vnode config since %s", pReq->vgId, tstrerror(terrno));
    return ret;
  }

  vInfo("vgId:%d, vnode config is saved", info.config.vgId);
  return 0;
}

static int32_t vnodeVgroupIdLen(int32_t vgId) {
  char tmp[TSDB_FILENAME_LEN];
  (void)tsnprintf(tmp, TSDB_FILENAME_LEN, "%d", vgId);
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

  STfsDir *tsdbDir = NULL;
  int32_t  tret = tfsOpendir(pTfs, tsdbPath, &tsdbDir);
  if (tsdbDir == NULL) {
    return 0;
  }

  while (1) {
    const STfsFile *tsdbFile = tfsReaddir(tsdbDir);
    if (tsdbFile == NULL) break;
    if (tsdbFile->rname[0] == '\0') continue;
    tstrncpy(oldRname, tsdbFile->rname, TSDB_FILENAME_LEN);

    char *tsdbFilePrefixPos = strstr(oldRname, tsdbFilePrefix);
    if (tsdbFilePrefixPos == NULL) continue;

    int32_t tsdbFileVgId = 0;
    ret = taosStr2int32(tsdbFilePrefixPos + prefixLen, &tsdbFileVgId);
    if (ret != 0) {
      vError("vgId:%d, failed to get tsdb file vgid since %s", dstVgId, tstrerror(ret));
      tfsClosedir(tsdbDir);
      return ret;
    }

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
    return ret;
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
  bool ret1 = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
  vInfo("vgId:%d, ep:%s:%u dnode:%d", pReq->srcVgId, pNode->nodeFqdn, pNode->nodePort, pNode->nodeId);

  info.config.syncCfg = *pCfg;

  ret = vnodeSaveInfo(dir, &info);
  if (ret < 0) {
    vError("vgId:%d, failed to save vnode config since %s", pReq->dstVgId, tstrerror(terrno));
    return ret;
  }

  ret = vnodeCommitInfo(dir);
  if (ret < 0) {
    vError("vgId:%d, failed to commit vnode config since %s", pReq->dstVgId, tstrerror(terrno));
    return ret;
  }

  vInfo("vgId:%d, rename %s to %s", pReq->dstVgId, srcPath, dstPath);
  ret = vnodeRenameVgroupId(srcPath, dstPath, pReq->srcVgId, pReq->dstVgId, diskPrimary, pTfs);
  if (ret < 0) {
    vError("vgId:%d, failed to rename vnode from %s to %s since %s", pReq->dstVgId, srcPath, dstPath,
           tstrerror(terrno));
    return ret;
  }

  vInfo("vgId:%d, vnode hashrange is altered", info.config.vgId);
  return 0;
}

int32_t vnodeRestoreVgroupId(const char *srcPath, const char *dstPath, int32_t srcVgId, int32_t dstVgId,
                             int32_t diskPrimary, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  int32_t    code = 0;

  vnodeGetPrimaryDir(dstPath, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);
  if (vnodeLoadInfo(dir, &info) == 0) {
    if (info.config.vgId != dstVgId) {
      vError("vgId:%d, unexpected vnode config.vgId:%d", dstVgId, info.config.vgId);
      return TSDB_CODE_FAILED;
    }
    return dstVgId;
  }

  vnodeGetPrimaryDir(srcPath, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);
  if ((code = vnodeLoadInfo(dir, &info)) < 0) {
    vError("vgId:%d, failed to read vnode config from %s since %s", srcVgId, srcPath, tstrerror(terrno));
    return code;
  }

  if (info.config.vgId == srcVgId) {
    vInfo("vgId:%d, rollback alter hashrange", srcVgId);
    return srcVgId;
  } else if (info.config.vgId != dstVgId) {
    vError("vgId:%d, unexpected vnode config.vgId:%d", dstVgId, info.config.vgId);
    return TSDB_CODE_FAILED;
  }

  vInfo("vgId:%d, rename %s to %s", dstVgId, srcPath, dstPath);
  if (vnodeRenameVgroupId(srcPath, dstPath, srcVgId, dstVgId, diskPrimary, pTfs) < 0) {
    vError("vgId:%d, failed to rename vnode from %s to %s since %s", dstVgId, srcPath, dstPath, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  return dstVgId;
}

void vnodeDestroy(int32_t vgId, const char *path, STfs *pTfs, int32_t nodeId) {
  vInfo("path:%s is removed while destroy vnode", path);
  if (tfsRmdir(pTfs, path) < 0) {
    vError("failed to remove path:%s since %s", path, tstrerror(terrno));
  }

#ifdef USE_SHARED_STORAGE
  if (nodeId > 0 && vgId > 0 && tsSsEnabled) {
    // we should only do this on the leader node, but it is ok to do this on all nodes
    char prefix[TSDB_FILENAME_LEN];
    snprintf(prefix, TSDB_FILENAME_LEN, "vnode%d/", vgId);
    int32_t code = tssDeleteFileByPrefixFromDefault(prefix);
    if (code < 0) {
      vError("vgId:%d, failed to remove vnode files from shared storage since %s", vgId, tstrerror(code));
    } else {
      vInfo("vgId:%d, removed vnode files from shared storage", vgId);
    }
  }
#endif
}

static int32_t vnodeCheckDisk(int32_t diskPrimary, STfs *pTfs) {
  int32_t ndisk = 1;
  if (pTfs) {
    ndisk = tfsGetDisksAtLevel(pTfs, 0);
  }
  if (diskPrimary < 0 || diskPrimary >= ndisk) {
    vError("disk:%d is unavailable from the %d disks mounted at level 0", diskPrimary, ndisk);
    return terrno = TSDB_CODE_FS_INVLD_CFG;
  }
  return 0;
}

SVnode *vnodeOpen(const char *path, int32_t diskPrimary, STfs *pTfs, STfs *pMountTfs, SMsgCb msgCb, bool force) {
  SVnode    *pVnode = NULL;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN] = {0};
  char       tdir[TSDB_FILENAME_LEN * 2] = {0};
  int32_t    ret = 0;
  bool       mounted = pMountTfs != NULL;
  terrno = TSDB_CODE_SUCCESS;

  if (vnodeCheckDisk(diskPrimary, pTfs)) {
    vError("failed to open vnode from %s since %s. diskPrimary:%d", path, terrstr(), diskPrimary);
    return NULL;
  }
  vnodeGetPrimaryDir(path, diskPrimary, pTfs, dir, TSDB_FILENAME_LEN);

  info.config = vnodeCfgDefault;

  // load vnode info
  vInfo("vgId:%d, start to vnode load info %s", info.config.vgId, dir);
  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("failed to open vnode from %s since %s", path, tstrerror(terrno));
    terrno = TSDB_CODE_NEED_RETRY;
    return NULL;
  }

  if (!mounted && vnodeMkDir(pTfs, path)) {
    vError("vgId:%d, failed to prepare vnode dir since %s, path: %s", info.config.vgId, strerror(ERRNO), path);
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
    if (vnodeSaveInfo(dir, &info) < 0) {
      vError("vgId:%d, failed to save vnode info since %s", info.config.vgId, tstrerror(terrno));
    }

    if (vnodeCommitInfo(dir) < 0) {
      vError("vgId:%d, failed to commit vnode info since %s", info.config.vgId, tstrerror(terrno));
    }
  }

  // create handle
  pVnode = taosMemoryCalloc(1, sizeof(*pVnode) + strlen(path) + 1);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    vError("vgId:%d, failed to open vnode since %s", info.config.vgId, tstrerror(terrno));
    return NULL;
  }

  pVnode->path = (char *)&pVnode[1];
  memcpy(pVnode->path, path, strlen(path) + 1);
  pVnode->config = info.config;
  pVnode->state.committed = info.state.committed;
  pVnode->state.commitTerm = info.state.commitTerm;
  pVnode->state.commitID = info.state.commitID;
  pVnode->state.applied = info.state.committed;
  pVnode->state.applyTerm = info.state.commitTerm;
  pVnode->pTfs = pTfs;
  pVnode->pMountTfs = pMountTfs;
  pVnode->mounted = mounted;
  pVnode->diskPrimary = diskPrimary;
  pVnode->msgCb = msgCb;
  (void)taosThreadMutexInit(&pVnode->lock, NULL);
  pVnode->blocked = false;
  pVnode->disableWrite = false;

  if (tsem_init(&pVnode->syncSem, 0, 0) != 0) {
    vError("vgId:%d, failed to init semaphore", TD_VID(pVnode));
    goto _err;
  }
  (void)taosThreadMutexInit(&pVnode->mutex, NULL);
  (void)taosThreadCondInit(&pVnode->poolNotEmpty, NULL);

  vInfo("vgId:%d, finished vnode load info %s, vnode committed:%" PRId64, info.config.vgId, dir,
        pVnode->state.committed);

  int8_t rollback = vnodeShouldRollback(pVnode);

  // open buffer pool
  vInfo("vgId:%d, start to open vnode buffer pool", TD_VID(pVnode));
  if (vnodeOpenBufPool(pVnode) < 0) {
    vError("vgId:%d, failed to open vnode buffer pool since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open meta
  (void)taosThreadRwlockInit(&pVnode->metaRWLock, NULL);
  vInfo("vgId:%d, start to open vnode meta", TD_VID(pVnode));
  if (metaOpen(pVnode, &pVnode->pMeta, rollback) < 0) {
    vError("vgId:%d, failed to open vnode meta since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  vInfo("vgId:%d, start to upgrade meta", TD_VID(pVnode));
  if (!mounted && metaUpgrade(pVnode, &pVnode->pMeta) < 0) {
    vError("vgId:%d, failed to upgrade meta since %s", TD_VID(pVnode), tstrerror(terrno));
  }

  // open tsdb
  vInfo("vgId:%d, start to open vnode tsdb", TD_VID(pVnode));
  if ((terrno = tsdbOpen(pVnode, &VND_TSDB(pVnode), VNODE_TSDB_DIR, NULL, rollback, force)) < 0) {
    vError("vgId:%d, failed to open vnode tsdb since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  if (TSDB_CACHE_RESET(pVnode->config)) {
    // flag vnode tsdb cache
    vInfo("vgId:%d, start to flag vnode tsdb cache", TD_VID(pVnode));

    if (metaFlagCache(pVnode) < 0) {
      vError("vgId:%d, failed to flag tsdb cache since %s", TD_VID(pVnode), tstrerror(terrno));
      goto _err;
    }
  }

  // open wal
  (void)tsnprintf(tdir, sizeof(tdir), "%s%s%s", dir, TD_DIRSEP, VNODE_WAL_DIR);
  ret = taosRealPath(tdir, NULL, sizeof(tdir));
  TAOS_UNUSED(ret);

  vInfo("vgId:%d, start to open vnode wal", TD_VID(pVnode));
  pVnode->pWal = walOpen(tdir, &(pVnode->config.walCfg));
  if (pVnode->pWal == NULL) {
    vError("vgId:%d, failed to open vnode wal since %s. wal:%s", TD_VID(pVnode), tstrerror(terrno), tdir);
    goto _err;
  }

  // open tq
  (void)tsnprintf(tdir, sizeof(tdir), "%s%s%s", dir, TD_DIRSEP, VNODE_TQ_DIR);
  ret = taosRealPath(tdir, NULL, sizeof(tdir));
  TAOS_UNUSED(ret);

  // open query
  vInfo("vgId:%d, start to open vnode query", TD_VID(pVnode));
  if (vnodeQueryOpen(pVnode)) {
    vError("vgId:%d, failed to open vnode query since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // sma required the tq is initialized before the vnode open
  vInfo("vgId:%d, start to open vnode tq", TD_VID(pVnode));
  if (tqOpen(tdir, pVnode)) {
    vError("vgId:%d, failed to open vnode tq since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open blob store engine
  vInfo("vgId:%d, start to open blob store engine", TD_VID(pVnode));
  (void)tsnprintf(tdir, sizeof(tdir), "%s%s%s", dir, TD_DIRSEP, VNODE_BSE_DIR);

  SBseCfg cfg = {
      .vgId = pVnode->config.vgId,
      .keepDays = pVnode->config.tsdbCfg.days,
      .keeps = pVnode->config.tsdbCfg.keep0,
      .retention = pVnode->config.tsdbCfg.retentions[0],
      .precision = pVnode->config.tsdbCfg.precision,
  };
  ret = bseOpen(tdir, &cfg, &pVnode->pBse);
  if (ret != 0) {
    vError("vgId:%d, failed to open blob store engine since %s", TD_VID(pVnode), tstrerror(ret));
    terrno = ret;
    goto _err;
  }

  // vnode begin
  vInfo("vgId:%d, start to begin vnode", TD_VID(pVnode));
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

  snprintf(pVnode->monitor.strClusterId, TSDB_CLUSTER_ID_LEN, "%" PRId64, pVnode->config.syncCfg.nodeInfo[0].clusterId);
  snprintf(pVnode->monitor.strDnodeId, TSDB_NODE_ID_LEN, "%" PRId32, pVnode->config.syncCfg.nodeInfo[0].nodeId);
  snprintf(pVnode->monitor.strVgId, TSDB_VGROUP_ID_LEN, "%" PRId32, pVnode->config.vgId);

  return pVnode;

_err:
  if (pVnode->pQuery) vnodeQueryClose(pVnode);
  if (pVnode->pTq) tqClose(pVnode->pTq);
  if (pVnode->pWal) walClose(pVnode->pWal);
  if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
  if (pVnode->pMeta) metaClose(&pVnode->pMeta);
  if (pVnode->freeList) vnodeCloseBufPool(pVnode);

  (void)taosThreadRwlockDestroy(&pVnode->metaRWLock);
  taosMemoryFree(pVnode);
  return NULL;
}

void vnodePreClose(SVnode *pVnode) {
  streamRemoveVnodeLeader(pVnode->config.vgId);
  vnodeSyncPreClose(pVnode);
  vnodeQueryPreClose(pVnode);
}

void vnodePostClose(SVnode *pVnode) { vnodeSyncPostClose(pVnode); }

void vnodeClose(SVnode *pVnode) {
  if (pVnode) {
    vInfo("start to close vnode");
    vnodeAWait(&pVnode->commitTask2);
    vnodeAWait(&pVnode->commitTask);
    vnodeSyncClose(pVnode);
    vnodeQueryClose(pVnode);
    tqClose(pVnode->pTq);
    walClose(pVnode->pWal);
    if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
    if (pVnode->pMeta) metaClose(&pVnode->pMeta);
    vnodeCloseBufPool(pVnode);

    if (pVnode->pBse) {
      bseClose(pVnode->pBse);
    }

    // destroy handle
    if (tsem_destroy(&pVnode->syncSem) != 0) {
      vError("vgId:%d, failed to destroy semaphore", TD_VID(pVnode));
    }
    (void)taosThreadCondDestroy(&pVnode->poolNotEmpty);
    (void)taosThreadMutexDestroy(&pVnode->mutex);
    (void)taosThreadMutexDestroy(&pVnode->lock);
    taosMemoryFree(pVnode);
  }
}

// start the sync timer after the queue is ready
int32_t vnodeStart(SVnode *pVnode) {
  if (pVnode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  return vnodeSyncStart(pVnode);
}

int32_t vnodeIsCatchUp(SVnode *pVnode) { return syncIsCatchUp(pVnode->sync); }

ESyncRole vnodeGetRole(SVnode *pVnode) { return syncGetRole(pVnode->sync); }

int32_t vnodeUpdateArbTerm(SVnode *pVnode, int64_t arbTerm) { return syncUpdateArbTerm(pVnode->sync, arbTerm); }
int32_t vnodeGetArbToken(SVnode *pVnode, char *outToken) { return syncGetArbToken(pVnode->sync, outToken); }

int32_t vnodeSetWalKeepVersion(SVnode *pVnode, int64_t keepVersion) {
  if (pVnode == NULL || pVnode->pWal == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  return walSetKeepVersion(pVnode->pWal, keepVersion);
}

void vnodeStop(SVnode *pVnode) {}

int64_t vnodeGetSyncHandle(SVnode *pVnode) { return pVnode->sync; }
