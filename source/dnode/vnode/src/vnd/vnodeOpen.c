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

void vnodeDestroy(const char *path) { taosRemoveDir(path); }

SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb) {
  char       dir[TSDB_FILENAME_LEN];
  char       tdir[TSDB_FILENAME_LEN * 2];
  SVnodeInfo info = {0};
  SVnode    *pVnode = NULL;

  // load info
  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  if (vnodeLoadInfo(dir, &info) < 0) {
    vError("failed to open vnode from %s since %s", path, tstrerror(terrno));
    return NULL;
  }

  // create handle
  pVnode = (SVnode *)taosMemoryCalloc(1, sizeof(*pVnode) + strlen(path) + 1);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    vError("vgId: %d failed to open vnode since %s", info.config.vgId, tstrerror(terrno));
    return NULL;
  }

  pVnode->path = (char *)&pVnode[1];
  strcpy(pVnode->path, path);
  pVnode->config = info.config;
  pVnode->state = info.state;
  pVnode->state.processed = pVnode->state.applied = pVnode->state.committed;
  pVnode->pTfs = pTfs;
  pVnode->msgCb = msgCb;

  tsem_init(&pVnode->canCommit, 0, 1);

  // open buffer pool
  if (vnodeOpenBufPool(pVnode) < 0) {
    vError("vgId: %d failed to open vnode pool since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open meta
  snprintf(tdir, 2 * TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_META_DIR);
  pVnode->pMeta = metaOpen(tdir, vBufPoolGetMAF(pVnode));
  if (pVnode->pMeta == NULL) {
    vError("vgId: %d failed to open vnode meta since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open tsdb
  snprintf(tdir, 2 * TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_TSDB_DIR);
  pVnode->pTsdb = tsdbOpen(tdir, TD_VNODE_ID(pVnode), &(pVnode->config.tsdbCfg), vBufPoolGetMAF(pVnode), pVnode->pMeta,
                           pVnode->pTfs);
  if (pVnode->pTsdb == NULL) {
    vError("vgId: %d failed to open vnode tsdb since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open wal
  snprintf(tdir, 2 * TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_WAL_DIR);
  pVnode->pWal = walOpen(tdir, &(pVnode->config.walCfg));
  if (pVnode->pWal == NULL) {
    vError("vgId: %d failed to open vnode wal since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open tq
  snprintf(tdir, 2 * TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_TQ_DIR);
  pVnode->pTq = tqOpen(tdir, pVnode, pVnode->pWal, pVnode->pMeta, NULL, vBufPoolGetMAF(pVnode));
  if (pVnode->pTq == NULL) {
    vError("vgId: %d failed to open vnode tq since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open query
  if (vnodeQueryOpen(pVnode)) {
    vError("vgId: %d failed to open vnode query since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // // vnode begin
  // if (vnodeBegin(pVnode, 0) < 0) {
  //   // TODO
  // }

  return pVnode;

_err:
  if (pVnode->pTq) tqClose(pVnode->pTq);
  if (pVnode->pWal) walClose(pVnode->pWal);
  if (pVnode->pTsdb) tsdbClose(pVnode->pTsdb);
  if (pVnode->pMeta) metaClose(pVnode->pMeta);
  if (pVnode->pBufPool) vnodeCloseBufPool(pVnode);
  tsem_destroy(&pVnode->canCommit);
  taosMemoryFree(pVnode);
  return NULL;
}

void vnodeClose(SVnode *pVnode) {
  vnodeSyncCommit(pVnode);
  tqClose(pVnode->pTq);
  walClose(pVnode->pWal);
  tsdbClose(pVnode->pTsdb);
  metaClose(pVnode->pMeta);
  vnodeCloseBufPool(pVnode);
  tsem_destroy(&pVnode->canCommit);
  taosMemoryFree(pVnode);
}