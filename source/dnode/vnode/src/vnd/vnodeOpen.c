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

#include "vnodeInt.h"

static SVnode *vnodeNew(const char *path, const SVnodeCfg *pVnodeCfg);
static void    vnodeFree(SVnode *pVnode);
static int     vnodeOpenImpl(SVnode *pVnode);
static void    vnodeCloseImpl(SVnode *pVnode);

int vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs) {
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN];

  // TODO: check if directory exists

  // check config
  if (vnodeCheckCfg(pCfg) < 0) {
    vError("vgId: %d failed to create vnode since: %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  // create vnode env
  if (tfsMkdir(pTfs, path) < 0) {
    vError("vgId: %d failed to create vnode since: %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  info.config = *pCfg;

  if (vnodeSaveInfo(dir, &info) < 0 || vnodeCommitInfo(dir, &info) < 0) {
    vError("vgId: %d failed to save vnode config since %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  vInfo("vgId: %d vnode is created", pCfg->vgId);

  return 0;
}

void vnodeDestroy(const char *path, STfs *pTfs) { tfsRmdir(pTfs, path); }

SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb) {
  SVnode    *pVnode = NULL;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN];
  int        ret;

  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);

  // load vnode info
  ret = vnodeLoadInfo(dir, &info);
  if (ret < 0) {
    vError("failed to open vnode from %s since %s", path, tstrerror(terrno));
    return NULL;
  }

  info.config.pTfs = pTfs;
  info.config.msgCb = msgCb;

  // crate handle
  pVnode = vnodeNew(dir, &info.config);
  if (pVnode == NULL) {
    // TODO: handle error
    return NULL;
  }

  // Open the vnode
  if (vnodeOpenImpl(pVnode) < 0) {
    // TODO: handle error
    return NULL;
  }

  return pVnode;
}

void vnodeClose(SVnode *pVnode) {
  if (pVnode) {
    vnodeCloseImpl(pVnode);
    vnodeFree(pVnode);
  }
}

/* ------------------------ STATIC METHODS ------------------------ */
static SVnode *vnodeNew(const char *path, const SVnodeCfg *pVnodeCfg) {
  SVnode *pVnode = NULL;

  pVnode = (SVnode *)taosMemoryCalloc(1, sizeof(*pVnode));
  if (pVnode == NULL) {
    // TODO
    return NULL;
  }

  pVnode->vgId = pVnodeCfg->vgId;
  pVnode->msgCb = pVnodeCfg->msgCb;
  pVnode->pTfs = pVnodeCfg->pTfs;
  pVnode->path = strdup(path);
  vnodeOptionsCopy(&(pVnode->config), pVnodeCfg);

  tsem_init(&(pVnode->canCommit), 0, 1);

  return pVnode;
}

static void vnodeFree(SVnode *pVnode) {
  if (pVnode) {
    tsem_destroy(&(pVnode->canCommit));
    taosMemoryFreeClear(pVnode->path);
    taosMemoryFree(pVnode);
  }
}

static int vnodeOpenImpl(SVnode *pVnode) {
  char dir[TSDB_FILENAME_LEN];

  if (vnodeOpenBufPool(pVnode) < 0) {
    // TODO: handle error
    return -1;
  }

  // Open meta
  sprintf(dir, "%s/meta", pVnode->path);
  pVnode->pMeta = metaOpen(dir, &(pVnode->config.metaCfg), vBufPoolGetMAF(pVnode));
  if (pVnode->pMeta == NULL) {
    // TODO: handle error
    return -1;
  }

  // Open tsdb
  sprintf(dir, "%s/tsdb", pVnode->path);
  pVnode->pTsdb =
      tsdbOpen(dir, pVnode->vgId, &(pVnode->config.tsdbCfg), vBufPoolGetMAF(pVnode), pVnode->pMeta, pVnode->pTfs);
  if (pVnode->pTsdb == NULL) {
    // TODO: handle error
    return -1;
  }

  // Open WAL
  sprintf(dir, "%s/wal", pVnode->path);
  pVnode->pWal = walOpen(dir, &(pVnode->config.walCfg));
  if (pVnode->pWal == NULL) {
    // TODO: handle error
    return -1;
  }

  // Open TQ
  sprintf(dir, "%s/tq", pVnode->path);
  pVnode->pTq = tqOpen(dir, pVnode, pVnode->pWal, pVnode->pMeta, &(pVnode->config.tqCfg), vBufPoolGetMAF(pVnode));
  if (pVnode->pTq == NULL) {
    // TODO: handle error
    return -1;
  }

  // Open Query
  if (vnodeQueryOpen(pVnode)) {
    return -1;
  }

  // TODO
  return 0;
}

static void vnodeCloseImpl(SVnode *pVnode) {
  vnodeSyncCommit(pVnode);
  if (pVnode) {
    vnodeCloseBufPool(pVnode);
    metaClose(pVnode->pMeta);
    tsdbClose(pVnode->pTsdb);
    tqClose(pVnode->pTq);
    walClose(pVnode->pWal);
    vnodeQueryClose(pVnode);
  }
}
