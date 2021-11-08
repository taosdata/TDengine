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

  pVnode = vnodeNew(path, pVnodeOptions);
  if (pVnode == NULL) {
    // TODO: handle error
    return NULL;
  }

  taosMkDir(path);

  return pVnode;
}

void vnodeClose(SVnode *pVnode) { /* TODO */
}

void vnodeDestroy(const char *path) { taosRemoveDir(path); }

/* ------------------------ STATIC METHODS ------------------------ */
static SVnode *vnodeNew(const char *path, const SVnodeOptions *pVnodeOptions) {
  // TODO
  return NULL;
}

static void vnodeFree(SVnode *pVnode) {
  if (pVnode) {
    // TODO
  }
}