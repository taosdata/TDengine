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

#ifndef TDENGINE_GEOS_WRAPPER_H
#define TDENGINE_GEOS_WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <geos_c.h>

typedef struct SGEOSGeomFromTextContext {
  GEOSContextHandle_t handle;
  GEOSWKTReader *reader;
  GEOSWKBWriter *writer;
} SGEOSGeomFromTextContext;

int32_t prepareGeomFromText(SGEOSGeomFromTextContext *context);
int32_t doGeomFromText(SGEOSGeomFromTextContext *context, const char *inputWKT, unsigned char **outputGeom, size_t *size);
void cleanGeomFromText(SGEOSGeomFromTextContext *context);

#ifdef __cplusplus
}
#endif

#endif /*TDENGINE_GEOS_WRAPPER_H*/
