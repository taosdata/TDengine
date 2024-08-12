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

static FORCE_INLINE void *metaMalloc(void *pPool, size_t size) {
  return vnodeBufPoolMallocAligned((SVBufPool *)pPool, size);
}
static FORCE_INLINE void metaFree(void *pPool, void *p) { vnodeBufPoolFree((SVBufPool *)pPool, p); }

// begin a meta txn
int32_t metaBegin(SMeta *pMeta, int8_t heap) {
  int32_t code = 0;
  int32_t lino;

  void *(*xMalloc)(void *, size_t) = NULL;
  void (*xFree)(void *, void *) = NULL;
  void *xArg = NULL;

  // default heap to META_BEGIN_HEAP_NIL
  if (heap == META_BEGIN_HEAP_OS) {
    xMalloc = tdbDefaultMalloc;
    xFree = tdbDefaultFree;
  } else if (heap == META_BEGIN_HEAP_BUFFERPOOL) {
    xMalloc = metaMalloc;
    xFree = metaFree;
    xArg = pMeta->pVnode->inUse;
  }

  code = tdbBegin(pMeta->pEnv, &pMeta->txn, xMalloc, xFree, xArg, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdbCommit(pMeta->pEnv, pMeta->txn);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(terrno));
  } else {
    metaDebug("vgId:%d %s success", TD_VID(pMeta->pVnode), __func__);
  }
  return 0;
}

// commit the meta txn
TXN *metaGetTxn(SMeta *pMeta) { return pMeta->txn; }
int  metaCommit(SMeta *pMeta, TXN *txn) { return tdbCommit(pMeta->pEnv, txn); }
int  metaFinishCommit(SMeta *pMeta, TXN *txn) { return tdbPostCommit(pMeta->pEnv, txn); }

int metaPrepareAsyncCommit(SMeta *pMeta) {
  // return tdbPrepareAsyncCommit(pMeta->pEnv, pMeta->txn);
  int     code = 0;
  int32_t lino;

  metaWLock(pMeta);
  TAOS_UNUSED(ttlMgrFlush(pMeta->pTtlMgr, pMeta->txn));
  metaULock(pMeta);

  code = tdbCommit(pMeta->pEnv, pMeta->txn);
  TSDB_CHECK_CODE(code, lino, _exit);
  pMeta->changed = false;

  pMeta->txn = NULL;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(terrno));
  } else {
    metaDebug("vgId:%d %s success", TD_VID(pMeta->pVnode), __func__);
  }
  return code;
}

// abort the meta txn
int metaAbort(SMeta *pMeta) {
  if (!pMeta->txn) {
    return 0;
  }

  int code = tdbAbort(pMeta->pEnv, pMeta->txn);
  if (code) {
    metaError("vgId:%d, failed to abort meta since %s", TD_VID(pMeta->pVnode), tstrerror(terrno));
  } else {
    pMeta->txn = NULL;
  }

  return code;
}
