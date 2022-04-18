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

int metaOpen(SVnode *pVnode, SMeta **ppMeta) {
  SMeta *pMeta = NULL;
  int    slen;

  *ppMeta = NULL;

  // create handle
  slen = strlen(tfsGetPrimaryPath(pVnode->pTfs)) + strlen(pVnode->path) + strlen(VNODE_META_DIR) + 3;
  if ((pMeta = taosMemoryCalloc(1, sizeof(*pMeta) + slen)) == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMeta->path = (char *)&pMeta[1];
  sprintf(pMeta->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP,
          VNODE_META_DIR);
  pMeta->pVnode = pVnode;

  // create path if not created yet
  taosMkDir(pMeta->path);

  // open meta
  if (metaOpenDB(pMeta) < 0) {
    goto _err;
  }

  if (metaOpenIdx(pMeta) < 0) {
    goto _err;
  }

  *ppMeta = pMeta;
  return 0;

_err:
  if (pMeta->pIdx) metaCloseIdx(pMeta);
  if (pMeta->pDB) metaCloseDB(pMeta);
  taosMemoryFree(pMeta);
  return -1;
}

int metaClose(SMeta *pMeta) {
  if (pMeta) {
    metaCloseIdx(pMeta);
    metaCloseDB(pMeta);
    taosMemoryFree(pMeta);
  }

  return 0;
}