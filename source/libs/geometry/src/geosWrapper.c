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

void geosFreeBuffer(void *buffer) {
  if (buffer) {
    GEOSFree_r(tlGeosCtx.handle, buffer);
  }
}

void geosErrMsgeHandler(const char *errMsg, void *userData) {
  char* targetErrMsg = userData;
  snprintf(targetErrMsg, 512, errMsg);
}

int32_t prepareGeomFromText() {
  int32_t code = TSDB_CODE_FAILED;

  if (tlGeosCtx.handle == NULL) {
    tlGeosCtx.handle = GEOS_init_r();
    if (tlGeosCtx.handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(tlGeosCtx.handle, geosErrMsgeHandler, tlGeosCtx.errMsg);
  }

  if (tlGeosCtx.WKTReader == NULL) {
    tlGeosCtx.WKTReader = GEOSWKTReader_create_r(tlGeosCtx.handle);
    if (tlGeosCtx.WKTReader == NULL) {
      return code;
    }
  }

  if (tlGeosCtx.WKBWriter == NULL) {
    tlGeosCtx.WKBWriter = GEOSWKBWriter_create_r(tlGeosCtx.handle);
    if (tlGeosCtx.WKBWriter == NULL) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// inputWKT is a zero ending string
// need to call geosFreeBuffer(*outputGeom) later
int32_t doGeomFromText(const char *inputWKT, unsigned char **outputGeom, size_t *size) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = NULL;
  unsigned char *wkb = NULL;

  geom = GEOSWKTReader_read_r(tlGeosCtx.handle, tlGeosCtx.WKTReader, inputWKT);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkb = GEOSWKBWriter_write_r(tlGeosCtx.handle, tlGeosCtx.WKBWriter, geom, size);
  if (wkb == NULL) {
    goto _exit;
  }
  *outputGeom = wkb;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, geom);
    geom = NULL;
  }

  return code;
}

int32_t prepareAsText() {
  int32_t code = TSDB_CODE_FAILED;

  if (tlGeosCtx.handle == NULL) {
    tlGeosCtx.handle = GEOS_init_r();
    if (tlGeosCtx.handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(tlGeosCtx.handle, geosErrMsgeHandler, tlGeosCtx.errMsg);
  }

  if (tlGeosCtx.WKBReader == NULL) {
    tlGeosCtx.WKBReader = GEOSWKBReader_create_r(tlGeosCtx.handle);
    if (tlGeosCtx.WKBReader == NULL) {
      return code;
    }
  }

  if (tlGeosCtx.WKTWriter == NULL) {
    tlGeosCtx.WKTWriter = GEOSWKTWriter_create_r(tlGeosCtx.handle);

    if (tlGeosCtx.WKTWriter != NULL) {
      GEOSWKTWriter_setRoundingPrecision_r(tlGeosCtx.handle, tlGeosCtx.WKTWriter, 6);
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

  GEOSGeometry *geom = NULL;
  unsigned char *wkt = NULL;

  geom = GEOSWKBReader_read_r(tlGeosCtx.handle, tlGeosCtx.WKBReader, inputGeom, size);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkt = GEOSWKTWriter_write_r(tlGeosCtx.handle, tlGeosCtx.WKTWriter, geom);
  if (wkt == NULL) {
    goto _exit;
  }
  *outputWKT = wkt;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, geom);
    geom = NULL;
  }

  return code;
}

int32_t prepareMakePoint() {
  int32_t code = TSDB_CODE_FAILED;

  if (tlGeosCtx.handle == NULL) {
    tlGeosCtx.handle = GEOS_init_r();
    if (tlGeosCtx.handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(tlGeosCtx.handle, geosErrMsgeHandler, tlGeosCtx.errMsg);
  }

  if (tlGeosCtx.WKBWriter == NULL) {
    tlGeosCtx.WKBWriter = GEOSWKBWriter_create_r(tlGeosCtx.handle);
    if (tlGeosCtx.WKBWriter == NULL) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// outputWKT is a zero ending string
// need to call geosFreeBuffer(*outputGeom) later
int32_t doMakePoint(double x, double y, unsigned char **outputGeom, size_t *size) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = NULL;
  unsigned char *wkb = NULL;

  geom = GEOSGeom_createPointFromXY_r(tlGeosCtx.handle, x, y);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkb = GEOSWKBWriter_write_r(tlGeosCtx.handle, tlGeosCtx.WKBWriter, geom, size);
  if (wkb == NULL) {
    goto _exit;
  }
  *outputGeom = wkb;

  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, geom);
    geom = NULL;
  }

  return code;
}

int32_t prepareIntersects() {
  int32_t code = TSDB_CODE_FAILED;

  if (tlGeosCtx.handle == NULL) {
    tlGeosCtx.handle = GEOS_init_r();
    if (tlGeosCtx.handle == NULL) {
      return code;
    }

    GEOSContext_setErrorMessageHandler_r(tlGeosCtx.handle, geosErrMsgeHandler, tlGeosCtx.errMsg);
  }

  if (tlGeosCtx.WKBReader == NULL) {
    tlGeosCtx.WKBReader = GEOSWKBReader_create_r(tlGeosCtx.handle);
    if (tlGeosCtx.WKBReader == NULL) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doIntersects(const unsigned char *inputGeom1, size_t size1,
                     const unsigned char *inputGeom2, size_t size2,
                     char *res) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom1 = NULL;
  GEOSGeometry *geom2 = NULL;

  geom1 = GEOSWKBReader_read_r(tlGeosCtx.handle, tlGeosCtx.WKBReader, inputGeom1, size1);
  if (geom1 == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  geom2 = GEOSWKBReader_read_r(tlGeosCtx.handle, tlGeosCtx.WKBReader, inputGeom2, size2);
  if (geom2 == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  *res = GEOSIntersects_r(tlGeosCtx.handle, geom1, geom2);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom1) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, geom1);
    geom1 = NULL;
  }

  if (geom2) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, geom2);
    geom2 = NULL;
  }

  return code;
}

// input is with VARSTR format
// need to call destroyPreparedGeometry(preparedGeom, outputGeom) later
int32_t makePreparedGeometry(unsigned char *input, GEOSGeometry **outputGeom, const GEOSPreparedGeometry **outputPreparedGeom) {
  *outputGeom = NULL;
  *outputPreparedGeom = NULL;

  if (varDataLen(input) == 0) { //empty value
    return TSDB_CODE_SUCCESS;
  }

  *outputGeom = GEOSWKBReader_read_r(tlGeosCtx.handle, tlGeosCtx.WKBReader, varDataVal(input), varDataLen(input));
  if (*outputGeom == NULL) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

  *outputPreparedGeom = GEOSPrepare_r(tlGeosCtx.handle, *outputGeom);
  if (*outputPreparedGeom == NULL) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

void destroyPreparedGeometry(const GEOSPreparedGeometry **preparedGeom, GEOSGeometry **geom) {
  if (*preparedGeom) {
    GEOSPreparedGeom_destroy_r(tlGeosCtx.handle, *preparedGeom);
    *preparedGeom = NULL;
  }

  if (*geom) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, *geom);
    *geom = NULL;
  }
}

int32_t doPreparedIntersects(const GEOSPreparedGeometry *preparedGeom1,
                             const unsigned char *inputGeom2, size_t size2,
                             char *res) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom2 = GEOSWKBReader_read_r(tlGeosCtx.handle, tlGeosCtx.WKBReader, inputGeom2, size2);
  if (geom2 == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  *res = GEOSPreparedIntersects_r(tlGeosCtx.handle, preparedGeom1, geom2);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (geom2) {
    GEOSGeom_destroy_r(tlGeosCtx.handle, geom2);
    geom2 = NULL;
  }

  return code;
}
