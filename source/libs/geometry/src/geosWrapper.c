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

void geosFreeBuffer(void *buffer) {
  if (buffer) {
    GEOSFree_r(getThreadLocalGeosCtx()->handle, buffer);
  }
}

void geosErrMsgeHandler(const char *errMsg, void *userData) {
  char* targetErrMsg = userData;
  snprintf(targetErrMsg, 512, errMsg);
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

    if (geosCtx->WKTWriter != NULL) {
      GEOSWKTWriter_setRoundingPrecision_r(geosCtx->handle, geosCtx->WKTWriter, 6);
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

int32_t initCtxIntersects() {
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

int32_t doIntersects(const unsigned char *inputGeom1, size_t size1,
                     const unsigned char *inputGeom2, size_t size2,
                     char *res) {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  GEOSGeometry *geom1 = NULL;
  GEOSGeometry *geom2 = NULL;

  geom1 = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, inputGeom1, size1);
  if (geom1 == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  geom2 = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, inputGeom2, size2);
  if (geom2 == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  *res = GEOSIntersects_r(geosCtx->handle, geom1, geom2);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom1) {
    GEOSGeom_destroy_r(geosCtx->handle, geom1);
    geom1 = NULL;
  }

  if (geom2) {
    GEOSGeom_destroy_r(geosCtx->handle, geom2);
    geom2 = NULL;
  }

  return code;
}

// input is with VARSTR format
// need to call destroyPreparedGeometry(preparedGeom, outputGeom) later
int32_t makePreparedGeometry(unsigned char *input, GEOSGeometry **outputGeom, const GEOSPreparedGeometry **outputPreparedGeom) {
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  *outputGeom = NULL;
  *outputPreparedGeom = NULL;

  if (varDataLen(input) == 0) { //empty value
    return TSDB_CODE_SUCCESS;
  }

  *outputGeom = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, varDataVal(input), varDataLen(input));
  if (*outputGeom == NULL) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

  *outputPreparedGeom = GEOSPrepare_r(geosCtx->handle, *outputGeom);
  if (*outputPreparedGeom == NULL) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

void destroyPreparedGeometry(const GEOSPreparedGeometry **preparedGeom, GEOSGeometry **geom) {
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  if (*preparedGeom) {
    GEOSPreparedGeom_destroy_r(geosCtx->handle, *preparedGeom);
    *preparedGeom = NULL;
  }

  if (*geom) {
    GEOSGeom_destroy_r(geosCtx->handle, *geom);
    *geom = NULL;
  }
}

int32_t doPreparedIntersects(const GEOSPreparedGeometry *preparedGeom1,
                             const unsigned char *inputGeom2, size_t size2,
                             char *res) {
  int32_t code = TSDB_CODE_FAILED;
  SGeosContext* geosCtx = getThreadLocalGeosCtx();

  GEOSGeometry *geom2 = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, inputGeom2, size2);
  if (geom2 == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  *res = GEOSPreparedIntersects_r(geosCtx->handle, preparedGeom1, geom2);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom2) {
    GEOSGeom_destroy_r(geosCtx->handle, geom2);
    geom2 = NULL;
  }

  return code;
}
