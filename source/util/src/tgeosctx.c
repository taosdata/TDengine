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

static threadlocal TdThreadKey   tlGeosCtxKey = 0;
static threadlocal SGeosContext  tlGeosCtxObj = {0};

static void destroyThreadLocalGeosCtx(void *param) {
  SGeosContext *tlGeosCtx = &tlGeosCtxObj;
  if (tlGeosCtx->WKTReader) {
    GEOSWKTReader_destroy_r(tlGeosCtx->handle, tlGeosCtx->WKTReader);
    tlGeosCtx->WKTReader = NULL;
  }

  if (tlGeosCtx->WKTWriter) {
    GEOSWKTWriter_destroy_r(tlGeosCtx->handle, tlGeosCtx->WKTWriter);
    tlGeosCtx->WKTWriter = NULL;
  }

  if (tlGeosCtx->WKBReader) {
    GEOSWKBReader_destroy_r(tlGeosCtx->handle, tlGeosCtx->WKBReader);
    tlGeosCtx->WKBReader = NULL;
  }

  if (tlGeosCtx->WKBWriter) {
    GEOSWKBWriter_destroy_r(tlGeosCtx->handle, tlGeosCtx->WKBWriter);
    tlGeosCtx->WKBWriter = NULL;
  }

  if (tlGeosCtx->WKTRegex) {
    destroyRegexes(tlGeosCtx->WKTRegex, tlGeosCtx->WKTMatchData);
    tlGeosCtx->WKTRegex = NULL;
    tlGeosCtx->WKTMatchData = NULL;
  }

  if (tlGeosCtx->handle) {
    GEOS_finish_r(tlGeosCtx->handle);
    tlGeosCtx->handle = NULL;
  }
}

SGeosContext *acquireThreadLocalGeosCtx() { return taosThreadGetSpecific(tlGeosCtxKey); }

int32_t getThreadLocalGeosCtx(SGeosContext **ppCtx) {
  if ((*ppCtx = taosThreadGetSpecific(tlGeosCtxKey))) {
    return 0;
  }

  int32_t code = 0, lino = 0;
  if ((taosThreadKeyCreate(&tlGeosCtxKey, destroyThreadLocalGeosCtx)) != 0) {
    TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
  }

  if ((taosThreadSetSpecific(tlGeosCtxKey, &tlGeosCtxObj)) != 0) {
    TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
  }

  if (!(*ppCtx = taosThreadGetSpecific(tlGeosCtxKey))) {
    if (errno != 0) {
      TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_NOT_FOUND);
    }
  }
_exit:
  if (code != 0) {
    *ppCtx = NULL;
    uError("failed to get geos context at lino:%d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

const char *getGeosErrMsg(int32_t code) {
  SGeosContext *tlGeosCtx = taosThreadGetSpecific(tlGeosCtxKey);
  return (tlGeosCtx && tlGeosCtx->errMsg[0] != 0) ? tlGeosCtx->errMsg : (code ? tstrerror(code) : "");
}
