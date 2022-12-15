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

typedef struct SGeosContext {
  GEOSContextHandle_t handle;

  GEOSWKTReader *WKTReader;
  GEOSWKTWriter *WKTWriter;

  GEOSWKBReader *WKBReader;
  GEOSWKBWriter *WKBWriter;

  char errMsg[512];
} SGeosContext;

SGeosContext *getGlobleGeosCtx();

void destroyGeosContext(SGeosContext *context);

void geosErrMsgeHandler(const char *errMsg, void *userData);

int32_t prepareGeomFromText(SGeosContext *context);
int32_t doGeomFromText(SGeosContext *context, const char *inputWKT, unsigned char **outputGeom, size_t *size);

int32_t prepareAsText(SGeosContext *context);
int32_t doAsText(SGeosContext *context, const unsigned char *inputGeom, size_t size, char **outputWKT);

int32_t prepareMakePoint(SGeosContext *context);
int32_t doMakePoint(SGeosContext *context, double x, double y, unsigned char **outputGeom, size_t *size);

int32_t prepareIntersects(SGeosContext *context);
int32_t doIntersects(SGeosContext *context,
                     const unsigned char *inputGeom1, size_t size1,
                     const unsigned char *inputGeom2, size_t size2,
                     char *res);

#ifdef __cplusplus
}
#endif

#endif /*TDENGINE_GEOS_WRAPPER_H*/
