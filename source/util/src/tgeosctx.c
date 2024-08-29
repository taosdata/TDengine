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
#include "tlog.h"
#include "tutil.h"

static TdThreadKey tlGeosCtxKey = 0;
static int8_t      tlGeosCtxKeyInited = 0;

static threadlocal SGeosContext *tlGeosCtx = NULL;

static void destroyThreadLocalGeosCtx(void *param) {
#ifdef WINDOWS
  if (taosThreadIsMain()) return;
#endif

  SGeosContext *pGeosCtx = (SGeosContext *)param;
  if (!pGeosCtx) {
    return;
  }
  if (pGeosCtx->WKTReader) {
    GEOSWKTReader_destroy_r(pGeosCtx->handle, pGeosCtx->WKTReader);
    pGeosCtx->WKTReader = NULL;
  }

  if (pGeosCtx->WKTWriter) {
    GEOSWKTWriter_destroy_r(pGeosCtx->handle, pGeosCtx->WKTWriter);
    pGeosCtx->WKTWriter = NULL;
  }

  if (pGeosCtx->WKBReader) {
    GEOSWKBReader_destroy_r(pGeosCtx->handle, pGeosCtx->WKBReader);
    pGeosCtx->WKBReader = NULL;
  }

  if (pGeosCtx->WKBWriter) {
    GEOSWKBWriter_destroy_r(pGeosCtx->handle, pGeosCtx->WKBWriter);
    pGeosCtx->WKBWriter = NULL;
  }

  if (pGeosCtx->WKTRegex) {
    destroyRegexes(pGeosCtx->WKTRegex, pGeosCtx->WKTMatchData);
    pGeosCtx->WKTRegex = NULL;
    pGeosCtx->WKTMatchData = NULL;
  }

  if (pGeosCtx->handle) {
    GEOS_finish_r(pGeosCtx->handle);
    pGeosCtx->handle = NULL;
  }
  taosMemoryFree(pGeosCtx);
}

SGeosContext *acquireThreadLocalGeosCtx() { return tlGeosCtx; }

int32_t getThreadLocalGeosCtx(SGeosContext **ppCtx) {
  if ((*ppCtx = tlGeosCtx)) {
    return 0;
  }

  int32_t code = 0, lino = 0;
  if (atomic_val_compare_exchange_8(&tlGeosCtxKeyInited, 0, 1) == 0) {
    if ((taosThreadKeyCreate(&tlGeosCtxKey, destroyThreadLocalGeosCtx)) != 0) {
      atomic_store_8(&tlGeosCtxKeyInited, 0);
      TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
    }
  }

  SGeosContext *tlGeosCtxObj = (SGeosContext *)taosMemoryCalloc(1, sizeof(SGeosContext));
  if (!tlGeosCtxObj) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  if ((taosThreadSetSpecific(tlGeosCtxKey, (const void *)tlGeosCtxObj)) != 0) {
    taosMemoryFreeClear(tlGeosCtxObj);
    TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
  }

  *ppCtx = tlGeosCtx = tlGeosCtxObj;

_exit:
  if (code != 0) {
    *ppCtx = NULL;
    uError("failed to get geos context at line:%d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

const char *getGeosErrMsg(int32_t code) {
  return (tlGeosCtx && tlGeosCtx->errMsg[0] != 0) ? tlGeosCtx->errMsg : (code ? tstrerror(code) : "");
}

