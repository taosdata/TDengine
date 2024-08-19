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
#include "tdef.h"
#include "tlockfree.h"
#include "tlog.h"

typedef struct {
  SGeosContext *pool;
  int32_t       capacity;
  int32_t       size;
  SRWLatch      lock;
} SGeosContextPool;

static SGeosContextPool          sGeosPool = {0};
static threadlocal SGeosContext *tlGeosCtx = NULL;

SGeosContext *acquireThreadLocalGeosCtx() { return tlGeosCtx; }

SGeosContext *getThreadLocalGeosCtx() {
  if (tlGeosCtx) {
    return tlGeosCtx;
  }

  taosWLockLatch(&sGeosPool.lock);
  if (sGeosPool.size >= sGeosPool.capacity) {
    sGeosPool.capacity += 128;
    void *tmp = taosMemoryRealloc(sGeosPool.pool, sGeosPool.capacity * sizeof(SGeosContext));
    if (!tmp) {
      taosWUnLockLatch(&sGeosPool.lock);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    sGeosPool.pool = tmp;
    TAOS_MEMSET(sGeosPool.pool + sGeosPool.size, 0, (sGeosPool.capacity - sGeosPool.size) * sizeof(SGeosContext));
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
  if (!sGeosPool.pool) return;
  for (int32_t i = 0; i < sGeosPool.size; ++i) {
    destroyGeosCtx(sGeosPool.pool + i);
  }
  taosMemoryFreeClear(sGeosPool.pool);
}