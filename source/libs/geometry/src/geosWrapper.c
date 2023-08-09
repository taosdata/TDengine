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

typedef char (*_geosRelationFunc_t)(GEOSContextHandle_t handle, const GEOSGeometry *g1, const GEOSGeometry *g2);
typedef char (*_geosPreparedRelationFunc_t)(GEOSContextHandle_t handle, const GEOSPreparedGeometry *pg1, const GEOSGeometry *g2);

void geosFreeBuffer(void *buffer) {
  if (buffer) {
    GEOSFree_r(getThreadLocalGeosCtx()->handle, buffer);
  }
}

void geosErrMsgeHandler(const char *errMsg, void *userData) {
  char* targetErrMsg = userData;
  snprintf(targetErrMsg, 512, "%s", errMsg);
}

int32_t initCtxMakePoint() {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
  }

  if (geosCtx->WKBWriter == NULL) {
    geosCtx->WKBWriter = GEOSWKBWriter_create_r(geosCtx->handle);
    if (geosCtx->WKBWriter == NULL) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// outputWKT is a zero ending string
// need to call geosFreeBuffer(*outputGeom) later
int32_t doMakePoint(double x, double y, unsigned char **outputGeom, size_t *size) {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  GEOSGeometry *geom = NULL;
  unsigned char *wkb = NULL;

  geom = GEOSGeom_createPointFromXY_r(geosCtx->handle, x, y);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkb = GEOSWKBWriter_write_r(geosCtx->handle, geosCtx->WKBWriter, geom, size);
  if (wkb == NULL) {
    goto _exit;
  }
  *outputGeom = wkb;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(geosCtx->handle, geom);
    geom = NULL;
  }

  return code;
}

int32_t initCtxGeomFromText() {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
  }

  if (geosCtx->WKTReader == NULL) {
    geosCtx->WKTReader = GEOSWKTReader_create_r(geosCtx->handle);
    if (geosCtx->WKTReader == NULL) {
      return code;
    }
  }

  if (geosCtx->WKBWriter == NULL) {
    geosCtx->WKBWriter = GEOSWKBWriter_create_r(geosCtx->handle);
    if (geosCtx->WKBWriter == NULL) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// inputWKT is a zero ending string
// need to call geosFreeBuffer(*outputGeom) later
int32_t doGeomFromText(const char *inputWKT, unsigned char **outputGeom, size_t *size) {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  GEOSGeometry *geom = NULL;
  unsigned char *wkb = NULL;

  geom = GEOSWKTReader_read_r(geosCtx->handle, geosCtx->WKTReader, inputWKT);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkb = GEOSWKBWriter_write_r(geosCtx->handle, geosCtx->WKBWriter, geom, size);
  if (wkb == NULL) {
    goto _exit;
  }
  *outputGeom = wkb;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(geosCtx->handle, geom);
    geom = NULL;
  }

  return code;
}

int32_t initCtxAsText() {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
  }

  if (geosCtx->WKBReader == NULL) {
    geosCtx->WKBReader = GEOSWKBReader_create_r(geosCtx->handle);
    if (geosCtx->WKBReader == NULL) {
      return code;
    }
  }

  if (geosCtx->WKTWriter == NULL) {
    geosCtx->WKTWriter = GEOSWKTWriter_create_r(geosCtx->handle);

    if (geosCtx->WKTWriter) {
      GEOSWKTWriter_setRoundingPrecision_r(geosCtx->handle, geosCtx->WKTWriter, 6);
      GEOSWKTWriter_setTrim_r(geosCtx->handle, geosCtx->WKTWriter, 0);
    } else {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// outputWKT is a zero ending string
// need to call geosFreeBuffer(*outputWKT) later
int32_t doAsText(const unsigned char *inputGeom, size_t size, char **outputWKT) {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  GEOSGeometry *geom = NULL;
  unsigned char *wkt = NULL;

  geom = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, inputGeom, size);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkt = GEOSWKTWriter_write_r(geosCtx->handle, geosCtx->WKTWriter, geom);
  if (wkt == NULL) {
    goto _exit;
  }
  *outputWKT = wkt;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(geosCtx->handle, geom);
    geom = NULL;
  }

  return code;
}

int32_t initCtxRelationFunc() {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
  }

  if (geosCtx->WKBReader == NULL) {
    geosCtx->WKBReader = GEOSWKBReader_create_r(geosCtx->handle);
    if (geosCtx->WKBReader == NULL) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doGeosRelation(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                       bool swapped, char *res,
                       _geosRelationFunc_t relationFn,
                       _geosRelationFunc_t swappedRelationFn,
                       _geosPreparedRelationFunc_t preparedRelationFn,
                       _geosPreparedRelationFunc_t swappedPreparedRelationFn) {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (!preparedGeom1) {
    if (!swapped) {
      ASSERT(relationFn);
      *res = relationFn(geosCtx->handle, geom1, geom2);
    }
    else {
      ASSERT(swappedRelationFn);
      *res = swappedRelationFn(geosCtx->handle, geom1, geom2);
    }
  }
  else {
    if (!swapped) {
      ASSERT(preparedRelationFn);
      *res = preparedRelationFn(geosCtx->handle, preparedGeom1, geom2);
    }
    else {
      ASSERT(swappedPreparedRelationFn);
      *res = swappedPreparedRelationFn(geosCtx->handle, preparedGeom1, geom2);
    }
  }

  code = TSDB_CODE_SUCCESS;
  return code;
}

int32_t doIntersects(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                     bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res,
                        GEOSIntersects_r, GEOSIntersects_r, GEOSPreparedIntersects_r, GEOSPreparedIntersects_r);
}

int32_t doEquals(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                 bool swapped, char *res) {
  return doGeosRelation(geom1, NULL, geom2, swapped, res,
                        GEOSEquals_r, GEOSEquals_r, NULL, NULL);  // no prepared version for eguals()
}

int32_t doTouches(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                  bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res,
                        GEOSTouches_r, GEOSTouches_r, GEOSPreparedTouches_r, GEOSPreparedTouches_r);
}

int32_t doCovers(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                 bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res,
                        GEOSCovers_r, GEOSCoveredBy_r, GEOSPreparedCovers_r, GEOSPreparedCoveredBy_r);
}

int32_t doContains(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                   bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res,
                        GEOSContains_r, GEOSWithin_r, GEOSPreparedContains_r, GEOSPreparedWithin_r);
}

int32_t doContainsProperly(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                           bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res,
                        NULL, NULL, GEOSPreparedContainsProperly_r, NULL);
}

// input is with VARSTR format
// need to call destroyGeometry(outputGeom, outputPreparedGeom) later
int32_t readGeometry(const unsigned char *input, GEOSGeometry **outputGeom, const GEOSPreparedGeometry **outputPreparedGeom) {
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  ASSERT(outputGeom); //it is not allowed if outputGeom is NULL
  *outputGeom = NULL;

  if (outputPreparedGeom) {  //it means not to generate PreparedGeometry if outputPreparedGeom is NULL
    *outputPreparedGeom = NULL;
  }

  if (varDataLen(input) == 0) { //empty value
    return TSDB_CODE_SUCCESS;
  }

  *outputGeom = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, varDataVal(input), varDataLen(input));
  if (*outputGeom == NULL) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

  if (outputPreparedGeom) {
    *outputPreparedGeom = GEOSPrepare_r(geosCtx->handle, *outputGeom);
    if (*outputPreparedGeom == NULL) {
      return TSDB_CODE_FAILED;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void destroyGeometry(GEOSGeometry **geom, const GEOSPreparedGeometry **preparedGeom) {
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (preparedGeom && *preparedGeom) {
    GEOSPreparedGeom_destroy_r(geosCtx->handle, *preparedGeom);
    *preparedGeom = NULL;
  }

  if (geom && *geom) {
    GEOSGeom_destroy_r(geosCtx->handle, *geom);
    *geom = NULL;
  }
}
