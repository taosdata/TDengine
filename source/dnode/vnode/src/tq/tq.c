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

#include "tq.h"
#include <string.h>
#include "osDef.h"
#include "taoserror.h"

static int32_t tqInitialize(STQ* pTq) {
  if (pTq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  return tqMetaOpen(pTq);
}

int32_t tqOpen(const char* path, SVnode* pVnode) {
  if (path == NULL || pVnode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool ignoreTq = pVnode->mounted && !taosCheckExistFile(path);
  if (ignoreTq) {
    return 0;
  }

  STQ* pTq = taosMemoryCalloc(1, sizeof(STQ));
  if (pTq == NULL) {
    return terrno;
  }

  pVnode->pTq = pTq;
  pTq->pVnode = pVnode;

  pTq->path = taosStrdup(path);
  if (pTq->path == NULL) {
    return terrno;
  }

  pTq->pHandle = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (pTq->pHandle == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pTq->pHandle, tqDestroySTqHandle);

  taosInitRWLatch(&pTq->lock);

  pTq->pOffset = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  if (pTq->pOffset == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pTq->pOffset, (FDelete)tDeleteSTqOffset);

  return tqInitialize(pTq);
}

void tqClose(STQ* pTq) {
  qDebug("start to close tq");
  if (pTq == NULL) {
    return;
  }

  int32_t vgId = 0;
  if (pTq->pVnode != NULL) {
    vgId = TD_VID(pTq->pVnode);
  }

  taosHashCleanup(pTq->pHandle);
  taosHashCleanup(pTq->pOffset);
  taosMemoryFree(pTq->path);
  tqMetaClose(pTq);
  qDebug("vgId:%d end to close tq", vgId);

  taosMemoryFree(pTq);
}