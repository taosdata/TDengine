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

#include "meta.h"

static FORCE_INLINE void *metaMalloc(void *pPool, size_t size) { return vnodeBufPoolMalloc((SVBufPool *)pPool, size); }
static FORCE_INLINE void  metaFree(void *pPool, void *p) { vnodeBufPoolFree((SVBufPool *)pPool, p); }

// begin a meta txn
int metaBegin(SMeta *pMeta, int8_t fromSys) {
  if (fromSys) {
    tdbTxnOpen(&pMeta->txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  } else {
    tdbTxnOpen(&pMeta->txn, 0, metaMalloc, metaFree, pMeta->pVnode->inUse, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  }
  if (tdbBegin(pMeta->pEnv, &pMeta->txn) < 0) {
    return -1;
  }

  return 0;
}

// commit the meta txn
int metaCommit(SMeta *pMeta) { return tdbCommit(pMeta->pEnv, &pMeta->txn); }

// abort the meta txn
int metaAbort(SMeta *pMeta) { return tdbAbort(pMeta->pEnv, &pMeta->txn); }
