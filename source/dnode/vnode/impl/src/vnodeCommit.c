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

static int vnodeStartCommit(SVnode *pVnode);
static int vnodeEndCommit(SVnode *pVnode);

bool vnodeShouldCommit(SVnode *pVnode) { return false; }

int vnodeAsyncCommit(SVnode *pVnode) {
#if 0
  if (vnodeStartCommit(pVnode) < 0) {
    // TODO
  }

  if (tqCommit(pVnode->pTQ) < 0) {
    // TODO
  }

  if (metaCommit(pVnode->pMeta) < 0) {
    // TODO
  }

  if (tsdbCommit(pVnode->pTsdb) < 0) {
    // TODO
  }

  if (vnodeEndCommit(pVnode) < 0) {
    // TODO
  }

#endif
  return 0;
}

static int vnodeStartCommit(SVnode *pVnode) {
  // TODO
  return 0;
}

static int vnodeEndCommit(SVnode *pVnode) {
  // TODO
  return 0;
}