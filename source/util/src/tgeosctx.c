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

#include "tgeosctx.h"
#include "tarray.h"
#include "tdef.h"
#include "tlockfree.h"
#include "tlog.h"

#define GEOS_POOL_CAPACITY 64
typedef struct {
  SArray       *poolArray;  // totalSize: (GEOS_POOL_CAPACITY *  (taosArrayGetSize(poolArray) - 1)) + size
  SGeosContext *pool;       // current SGeosContext pool
  int32_t       size;       // size of current SGeosContext pool, size <= GEOS_POOL_CAPACITY
  SRWLatch      lock;
} SGeosContextPool;

static SGeosContextPool          sGeosPool = {0};
static threadlocal SGeosContext *tlGeosCtx = NULL;

SGeosContext *acquireThreadLocalGeosCtx() { return tlGeosCtx; }

SGeosContext *getThreadLocalGeosCtx() {
  if (tlGeosCtx) return tlGeosCtx;

  taosWLockLatch(&sGeosPool.lock);
  if (!sGeosPool.pool || sGeosPool.size >= GEOS_POOL_CAPACITY) {
    if (!(sGeosPool.pool = (SGeosContext *)taosMemoryCalloc(GEOS_POOL_CAPACITY, sizeof(SGeosContext)))) {
      taosWUnLockLatch(&sGeosPool.lock);
      return NULL;
    }
    if (!sGeosPool.poolArray) {
      if (!(sGeosPool.poolArray = taosArrayInit(16, POINTER_BYTES))) {
        taosMemoryFreeClear(sGeosPool.pool);
        taosWUnLockLatch(&sGeosPool.lock);
        return NULL;
      }
    }
    if (!taosArrayPush(sGeosPool.poolArray, &sGeosPool.pool)) {
      taosMemoryFreeClear(sGeosPool.pool);
      taosWUnLockLatch(&sGeosPool.lock);
      return NULL;
    }
    sGeosPool.size = 0;
  }
  tlGeosCtx = sGeosPool.pool + sGeosPool.size;
  ++sGeosPool.size;
  taosWUnLockLatch(&sGeosPool.lock);

  return tlGeosCtx;
}

const char *getGeosErrMsg(int32_t code) { return tlGeosCtx ? tlGeosCtx->errMsg : (code != 0 ? tstrerror(code) : ""); }

static void destroyGeosCtx(SGeosContext *pCtx) {
  if (pCtx) {
    if (pCtx->WKTReader) {
      GEOSWKTReader_destroy_r(pCtx->handle, pCtx->WKTReader);
      pCtx->WKTReader = NULL;
    }

    if (pCtx->WKTWriter) {
      GEOSWKTWriter_destroy_r(pCtx->handle, pCtx->WKTWriter);
      pCtx->WKTWriter = NULL;
    }

    if (pCtx->WKBReader) {
      GEOSWKBReader_destroy_r(pCtx->handle, pCtx->WKBReader);
      pCtx->WKBReader = NULL;
    }

    if (pCtx->WKBWriter) {
      GEOSWKBWriter_destroy_r(pCtx->handle, pCtx->WKBWriter);
      pCtx->WKBWriter = NULL;
    }

    if (pCtx->WKTRegex) {
      destroyRegexes(pCtx->WKTRegex, pCtx->WKTMatchData);
      pCtx->WKTRegex = NULL;
      pCtx->WKTMatchData = NULL;
    }

    if (pCtx->handle) {
      GEOS_finish_r(pCtx->handle);
      pCtx->handle = NULL;
    }
  }
}

void taosGeosDestroy() {
  uInfo("geos is cleaned up");
  int32_t size = taosArrayGetSize(sGeosPool.poolArray);
  for (int32_t i = 0; i < size; ++i) {
    SGeosContext *pool = *(SGeosContext **)TARRAY_GET_ELEM(sGeosPool.poolArray, i);
    int32_t       poolSize = i == size - 1 ? sGeosPool.size : GEOS_POOL_CAPACITY;
    for (int32_t j = 0; j < poolSize; ++j) {
      destroyGeosCtx(pool + j);
    }
    taosMemoryFree(pool);
  }
  taosArrayDestroy(sGeosPool.poolArray);
  sGeosPool.poolArray = NULL;
}