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

#include "tdef.h"
#include "tgeosctx.h"

static threadlocal SGeosContext tlGeosCtx = {0};

SGeosContext* getThreadLocalGeosCtx() {
  return &tlGeosCtx;
}

void destroyThreadLocalGeosCtx() {
  if (tlGeosCtx.WKTReader) {
    GEOSWKTReader_destroy_r(tlGeosCtx.handle, tlGeosCtx.WKTReader);
    tlGeosCtx.WKTReader = NULL;
  }

  if (tlGeosCtx.WKTWriter) {
    GEOSWKTWriter_destroy_r(tlGeosCtx.handle, tlGeosCtx.WKTWriter);
    tlGeosCtx.WKTWriter = NULL;
  }

  if (tlGeosCtx.WKBReader) {
    GEOSWKBReader_destroy_r(tlGeosCtx.handle, tlGeosCtx.WKBReader);
    tlGeosCtx.WKBReader = NULL;
  }

  if (tlGeosCtx.WKBWriter) {
    GEOSWKBWriter_destroy_r(tlGeosCtx.handle, tlGeosCtx.WKBWriter);
    tlGeosCtx.WKBWriter = NULL;
  }

  if(tlGeosCtx.handle) {
    GEOS_finish_r(tlGeosCtx.handle);
    tlGeosCtx.handle = NULL;
  }
}
