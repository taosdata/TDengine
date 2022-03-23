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

static SVnode *vnodeNew(const char *path, const SVnodeCfg *pVnodeCfg);
static void    vnodeFree(SVnode *pVnode);
static int     vnodeOpenImpl(SVnode *pVnode);
static void    vnodeCloseImpl(SVnode *pVnode);

SVnode *vnodeOpen(const char *path, const SVnodeCfg *pVnodeCfg) {
  SVnode *pVnode = NULL;

  // Set default options
  SVnodeCfg cfg = defaultVnodeOptions;
  if (pVnodeCfg != NULL) {
    cfg.vgId = pVnodeCfg->vgId;
    cfg.msgCb = pVnodeCfg->msgCb;
    cfg.pTfs = pVnodeCfg->pTfs;
    cfg.dbId = pVnodeCfg->dbId;
    cfg.hashBegin = pVnodeCfg->hashBegin;
    cfg.hashEnd = pVnodeCfg->hashEnd;
    cfg.hashMethod = pVnodeCfg->hashMethod;
  }

  // Validate options
  if (vnodeValidateOptions(&cfg) < 0) {
    // TODO
    return NULL;
  }

  // Create the handle
  pVnode = vnodeNew(path, &cfg);
  if (pVnode == NULL) {
    // TODO: handle error
    return NULL;
  }

  taosMkDir(path);

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

void vnodeDestroy(const char *path) { taosRemoveDir(path); }

/* ------------------------ STATIC METHODS ------------------------ */
static SVnode *vnodeNew(const char *path, const SVnodeCfg *pVnodeCfg) {
  SVnode *pVnode = NULL;

  pVnode = (SVnode *)calloc(1, sizeof(*pVnode));
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
    tfree(pVnode->path);
    free(pVnode);
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
