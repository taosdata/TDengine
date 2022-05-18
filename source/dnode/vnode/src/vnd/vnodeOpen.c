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

int vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN];

  // TODO: check if directory exists

  // check config
  if (vnodeCheckCfg(pCfg) < 0) {
    vError("vgId:%d failed to create vnode since: %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  // create vnode env
  if (tfsMkdir(pTfs, path) < 0) {
    vError("vgId:%d failed to create vnode since: %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  info.config = *pCfg;
  info.state.committed = -1;
  info.state.applied = -1;

  if (vnodeSaveInfo(dir, &info) < 0 || vnodeCommitInfo(dir, &info) < 0) {
    vError("vgId:%d failed to save vnode config since %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId:%d vnode is created", pCfg->vgId);

  return 0;
}

void vnodeDestroy(const char *path, STfs *pTfs) { tfsRmdir(pTfs, path); }

SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb) {
  SVnode    *pVnode = NULL;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN];
  char       tdir[TSDB_FILENAME_LEN * 2];
  int        ret;

  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);

  // load vnode info
  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("failed to open vnode from %s since %s", path, tstrerror(terrno));
    return NULL;
  }

  // create handle
  pVnode = (SVnode *)taosMemoryCalloc(1, sizeof(*pVnode) + strlen(path) + 1);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    vError("vgId:%d failed to open vnode since %s", info.config.vgId, tstrerror(terrno));
    return NULL;
  }

  pVnode->path = (char *)&pVnode[1];
  strcpy(pVnode->path, path);
  pVnode->config = info.config;
  pVnode->state = info.state;
  pVnode->pTfs = pTfs;
  pVnode->msgCb = msgCb;

  tsem_init(&(pVnode->canCommit), 0, 1);

  // open buffer pool
  if (vnodeOpenBufPool(pVnode, pVnode->config.isHeap ? 0 : pVnode->config.szBuf / 3) < 0) {
    vError("vgId:%d failed to open vnode buffer pool since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open meta
  if (metaOpen(pVnode, &pVnode->pMeta) < 0) {
    vError("vgId:%d failed to open vnode meta since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open tsdb
  if (!vnodeIsRollup(pVnode) && tsdbOpen(pVnode, &VND_TSDB(pVnode), VNODE_TSDB_DIR, TSDB_TYPE_TSDB) < 0) {
    vError("vgId:%d failed to open vnode tsdb since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open sma
  if (smaOpen(pVnode)) {
    vError("vgId:%d failed to open vnode sma since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open wal
  sprintf(tdir, "%s%s%s", dir, TD_DIRSEP, VNODE_WAL_DIR);
  pVnode->pWal = walOpen(tdir, &(pVnode->config.walCfg));
  if (pVnode->pWal == NULL) {
    vError("vgId:%d failed to open vnode wal since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open tq
  sprintf(tdir, "%s%s%s", dir, TD_DIRSEP, VNODE_TQ_DIR);
  pVnode->pTq = tqOpen(tdir, pVnode, pVnode->pWal);
  if (pVnode->pTq == NULL) {
    vError("vgId:%d failed to open vnode tq since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open query
  if (vnodeQueryOpen(pVnode)) {
    vError("vgId:%d failed to open vnode query since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // vnode begin
  if (vnodeBegin(pVnode) < 0) {
    vError("vgId:%d failed to begin since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // open sync
  if (vnodeSyncOpen(pVnode, dir)) {
    vError("vgId:%d failed to open sync since %s", TD_VID(pVnode), tstrerror(terrno));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  return pVnode;

_err:
  if (pVnode->pQuery) vnodeQueryClose(pVnode);
  if (pVnode->pTq) tqClose(pVnode->pTq);
  if (pVnode->pWal) walClose(pVnode->pWal);
  if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
  if (pVnode->pMeta) metaClose(pVnode->pMeta);
  if (pVnode->pSma) smaClose(pVnode->pSma);

  tsem_destroy(&(pVnode->canCommit));
  taosMemoryFree(pVnode);
  return NULL;
}

void vnodeClose(SVnode *pVnode) {
  if (pVnode) {
    vnodeCommit(pVnode);
    vnodeSyncClose(pVnode);
    vnodeQueryClose(pVnode);
    walClose(pVnode->pWal);
    tqClose(pVnode->pTq);
    if (pVnode->pTsdb) tsdbClose(&pVnode->pTsdb);
    smaClose(pVnode->pSma);
    metaClose(pVnode->pMeta);
    vnodeCloseBufPool(pVnode);
    // destroy handle
    tsem_destroy(&(pVnode->canCommit));
    taosMemoryFree(pVnode);
  }
}

// start the sync timer after the queue is ready
int32_t vnodeStart(SVnode *pVnode) {
  vnodeSyncSetQ(pVnode, NULL);
  vnodeSyncSetRpc(pVnode, NULL);
  vnodeSyncStart(pVnode);
  return 0;
}

void vnodeStop(SVnode *pVnode) {}

int64_t vnodeGetSyncHandle(SVnode *pVnode) { return pVnode->sync; }

void vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnapshot) { pSnapshot->lastApplyIndex = pVnode->state.committed; }