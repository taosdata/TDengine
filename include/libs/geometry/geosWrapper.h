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

SGeosContext* getThreadLocalGeosCtx();
void destroyThreadLocalGeosCtx();

void geosFreeBuffer(void *buffer);

int32_t prepareGeomFromText();
int32_t doGeomFromText(const char *inputWKT, unsigned char **outputGeom, size_t *size);

int32_t prepareAsText();
int32_t doAsText(const unsigned char *inputGeom, size_t size, char **outputWKT);

int32_t prepareMakePoint();
int32_t doMakePoint(double x, double y, unsigned char **outputGeom, size_t *size);

int32_t prepareIntersects();
int32_t doIntersects(const unsigned char *inputGeom1, size_t size1,
                     const unsigned char *inputGeom2, size_t size2,
                     char *res);

int32_t makePreparedGeometry(unsigned char *input, GEOSGeometry **outputGeom, const GEOSPreparedGeometry **outputPreparedGeom);
void destroyPreparedGeometry(const GEOSPreparedGeometry **preparedGeom, GEOSGeometry **geom);

int32_t doPreparedIntersects(const GEOSPreparedGeometry *preparedGeom1,
                             const unsigned char *inputGeom2, size_t size2,
                             char *res);

#ifdef __cplusplus
}
#endif

#endif /*TDENGINE_GEOS_WRAPPER_H*/
