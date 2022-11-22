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

#include "geosWrapper.h"
#include "tdef.h"
#include "types.h"

int32_t prepareGeomFromText(SGEOSGeomFromTextContext *context) {
  int32_t code = TSDB_CODE_FAILED;

  if (context->handle == NULL) {
    context->handle = GEOS_init_r();
  }
  if (context->handle == NULL) {
    return code;
  }

  if (context->reader == NULL) {
    context->reader = GEOSWKTReader_create_r(context->handle);
  }
  if (context->reader == NULL) {
    return code;
  }

  if (context->writer == NULL) {
    context->writer = GEOSWKBWriter_create_r(context->handle);
  }
  if (context->writer == NULL) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

// inputWKT is a zero ending string
// need to call GEOSFree_r(handle, outputGeom) later
int32_t doGeomFromText(SGEOSGeomFromTextContext *context, const char *inputWKT, unsigned char **outputGeom, size_t *size) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = NULL;
  unsigned char *wkb = NULL;

  geom = GEOSWKTReader_read_r(context->handle, context->reader, inputWKT);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkb = GEOSWKBWriter_write_r(context->handle, context->writer, geom, size);
  if (wkb == NULL) {
    goto _exit;
  }
  *outputGeom = wkb;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(context->handle, geom);
    geom = NULL;
  }

  return code;
}

void cleanGeomFromText(SGEOSGeomFromTextContext *context) {
  if (context->writer) {
    GEOSWKBWriter_destroy_r(context->handle, context->writer);
    context->writer = NULL;
  }

  if (context->reader) {
    GEOSWKTReader_destroy_r(context->handle, context->reader);
    context->reader = NULL;
  }

  if(context->handle) {
    GEOS_finish_r(context->handle);
    context->handle = NULL;
  }
}
