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
#include "os.h"

#include "tgeosctx.h"

void geosFreeBuffer(void *buffer);

int32_t initCtxMakePoint();
int32_t doMakePoint(double x, double y, unsigned char **outputGeom, size_t *size);

int32_t initCtxGeomFromText();
int32_t doGeomFromText(const char *inputWKT, unsigned char **outputGeom, size_t *size);

int32_t initCtxAsText();
int32_t doAsText(const unsigned char *inputGeom, size_t size, char **outputWKT);

int32_t initCtxRelationFunc();
int32_t doIntersects(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                     bool swapped, char *res);
int32_t doEquals(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                 bool swapped, char *res);
int32_t doTouches(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                  bool swapped, char *res);
int32_t doCovers(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                 bool swapped, char *res);
int32_t doContains(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                   bool swapped, char *res);
int32_t doContainsProperly(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                           bool swapped, char *res);

int32_t readGeometry(const unsigned char *input, GEOSGeometry **outputGeom, const GEOSPreparedGeometry **outputPreparedGeom);
void destroyGeometry(GEOSGeometry **geom, const GEOSPreparedGeometry **preparedGeom);

#ifdef __cplusplus
}
#endif

#endif /*TDENGINE_GEOS_WRAPPER_H*/
