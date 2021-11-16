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

#include "vnodeDef.h"

static SVnode *vnodeNew(const char *path, const SVnodeOptions *pVnodeOptions);
static void    vnodeFree(SVnode *pVnode);
static int     vnodeOpenImpl(SVnode *pVnode);
static void    vnodeCloseImpl(SVnode *pVnode);

SVnode *vnodeOpen(const char *path, const SVnodeOptions *pVnodeOptions) {
  SVnode *pVnode = NULL;

  // Set default options
  if (pVnodeOptions == NULL) {
    pVnodeOptions = &defaultVnodeOptions;
  }

  // Validate options
  if (vnodeValidateOptions(pVnodeOptions) < 0) {
    // TODO
    return NULL;
  }

  // Create the handle
  pVnode = vnodeNew(path, pVnodeOptions);
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
static SVnode *vnodeNew(const char *path, const SVnodeOptions *pVnodeOptions) {
  SVnode *pVnode = NULL;

  pVnode = (SVnode *)calloc(1, sizeof(*pVnode));
  if (pVnode == NULL) {
    // TODO
    return NULL;
  }

  pVnode->path = strdup(path);
  vnodeOptionsCopy(&(pVnode->options), pVnodeOptions);

  return pVnode;
}

static void vnodeFree(SVnode *pVnode) {
  if (pVnode) {
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
  pVnode->pMeta = metaOpen(dir, &(pVnode->options.metaOptions));
  if (pVnode->pMeta == NULL) {
    // TODO: handle error
    return -1;
  }

  // Open tsdb
  sprintf(dir, "%s/tsdb", pVnode->path);
  pVnode->pTsdb = tsdbOpen(dir, &(pVnode->options.tsdbOptions));
  if (pVnode->pTsdb == NULL) {
    // TODO: handle error
    return -1;
  }

  // TODO: Open TQ
  sprintf(dir, "%s/wal", pVnode->path);
  // pVnode->pTq = tqOpen(dir, NULL /* TODO */);
  // if (pVnode->pTq == NULL) {
  //   // TODO: handle error
  //   return -1;
  // }

  // Open WAL
  sprintf(dir, "%s/wal", pVnode->path);
  pVnode->pWal = walOpen(dir, NULL /* TODO */);
  if (pVnode->pWal == NULL) {
    // TODO: handle error
    return -1;
  }

  // TODO
  return 0;
}

static void vnodeCloseImpl(SVnode *pVnode) {
  if (pVnode) {
    vnodeCloseBufPool(pVnode);
    tsdbClose(pVnode->pTsdb);
    metaClose(pVnode->pMeta);
  }
}