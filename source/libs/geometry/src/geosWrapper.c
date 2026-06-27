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

#ifdef USE_GEOS
#include "geosWrapper.h"
#include <ctype.h>
#include "tutil.h"
#include "types.h"

typedef char (*_geosRelationFunc_t)(GEOSContextHandle_t handle, const GEOSGeometry *g1, const GEOSGeometry *g2);
typedef char (*_geosPreparedRelationFunc_t)(GEOSContextHandle_t handle, const GEOSPreparedGeometry *pg1,
                                            const GEOSGeometry *g2);

void geosFreeBuffer(void *buffer) {
  if (buffer) {
    SGeosContext *pCtx = acquireThreadLocalGeosCtx();
    if (pCtx) GEOSFree_r(pCtx->handle, buffer);
  }
}

void geosErrMsgeHandler(const char *errMsg, void *userData) {
  char *targetErrMsg = userData;
  (void)snprintf(targetErrMsg, 512, "%s", errMsg);
}

typedef struct {
  double x;
  double y;
} SWktCoord;

static const char *skipWktSpaces(const char *p) {
  while ('\0' != *p && isspace((unsigned char)*p)) {
    ++p;
  }
  return p;
}

static bool isWktTokenBoundary(char c) {
  return '\0' == c || isspace((unsigned char)c) || '(' == c || ')' == c || ',' == c;
}

static bool consumeWktChar(const char **pp, char c) {
  const char *p = skipWktSpaces(*pp);
  if (*p != c) {
    return false;
  }
  *pp = p + 1;
  return true;
}

static bool consumeWktWord(const char **pp, const char *word) {
  const char *p = skipWktSpaces(*pp);
  size_t      len = strlen(word);
  if (0 != strncasecmp(p, word, len) || !isWktTokenBoundary(p[len])) {
    return false;
  }
  *pp = p + len;
  return true;
}

static void consumeWktDimension(const char **pp) {
  const char *p = skipWktSpaces(*pp);

  if (0 == strncasecmp(p, "ZM", 2) && isWktTokenBoundary(p[2])) {
    *pp = p + 2;
  } else if ((('Z' == *p) || ('z' == *p) || ('M' == *p) || ('m' == *p)) && isWktTokenBoundary(p[1])) {
    *pp = p + 1;
  }
}

static bool parseWktNumber(const char **pp, double *value) {
  const char *p = skipWktSpaces(*pp);
  char       *end = NULL;
  double      parsed = taosStr2Double(p, &end);
  if (end == p) {
    return false;
  }
  *value = parsed;
  *pp = end;
  return true;
}

static bool parseWktCoord(const char **pp, SWktCoord *coord) {
  double  vals[2] = {0};
  double  ignored = 0;
  int32_t count = 0;

  while (count < 4) {
    const char *p = skipWktSpaces(*pp);
    if (count >= 2 && (',' == *p || ')' == *p)) {
      break;
    }
    if (!parseWktNumber(pp, count < 2 ? &vals[count] : &ignored)) {
      return false;
    }
    ++count;
  }

  if (count < 2) {
    return false;
  }

  const char *p = skipWktSpaces(*pp);
  if (',' != *p && ')' != *p) {
    return false;
  }

  coord->x = vals[0];
  coord->y = vals[1];
  return true;
}

static bool isSameWktCoord(SWktCoord left, SWktCoord right) { return left.x == right.x && left.y == right.y; }

static bool validateWktLinearRing(const char **pp) {
  if (consumeWktWord(pp, "EMPTY")) {
    return true;
  }
  if (!consumeWktChar(pp, '(')) {
    return false;
  }

  int32_t   count = 0;
  SWktCoord first = {0};
  SWktCoord last = {0};

  while (true) {
    SWktCoord coord = {0};
    if (!parseWktCoord(pp, &coord)) {
      return false;
    }
    if (0 == count) {
      first = coord;
    }
    last = coord;
    ++count;

    const char *p = skipWktSpaces(*pp);
    if (',' == *p) {
      *pp = p + 1;
      continue;
    }
    if (')' == *p) {
      *pp = p + 1;
      return count > 0 && isSameWktCoord(first, last);
    }
    return false;
  }
}

static bool skipWktBody(const char **pp) {
  if (consumeWktWord(pp, "EMPTY")) {
    return true;
  }

  const char *p = skipWktSpaces(*pp);
  if ('(' != *p) {
    return false;
  }

  int32_t depth = 0;
  while ('\0' != *p) {
    if ('(' == *p) {
      ++depth;
    } else if (')' == *p) {
      --depth;
      if (0 == depth) {
        *pp = p + 1;
        return true;
      }
    }
    ++p;
  }

  return false;
}

static bool validateWktGeometry(const char **pp);

static bool validateWktPolygonBody(const char **pp) {
  if (consumeWktWord(pp, "EMPTY")) {
    return true;
  }
  if (!consumeWktChar(pp, '(')) {
    return false;
  }
  if (!validateWktLinearRing(pp)) {
    return false;
  }

  while (true) {
    const char *p = skipWktSpaces(*pp);
    if (',' != *p) {
      break;
    }
    *pp = p + 1;
    if (!validateWktLinearRing(pp)) {
      return false;
    }
  }

  return consumeWktChar(pp, ')');
}

static bool validateWktMultiPolygonBody(const char **pp) {
  if (consumeWktWord(pp, "EMPTY")) {
    return true;
  }
  if (!consumeWktChar(pp, '(')) {
    return false;
  }

  while (true) {
    if (!validateWktPolygonBody(pp)) {
      return false;
    }

    const char *p = skipWktSpaces(*pp);
    if (',' == *p) {
      *pp = p + 1;
      continue;
    }
    if (')' == *p) {
      *pp = p + 1;
      return true;
    }
    return false;
  }
}

static bool validateWktCollectionBody(const char **pp) {
  if (consumeWktWord(pp, "EMPTY")) {
    return true;
  }
  if (!consumeWktChar(pp, '(')) {
    return false;
  }

  while (true) {
    if (!validateWktGeometry(pp)) {
      return false;
    }

    const char *p = skipWktSpaces(*pp);
    if (',' == *p) {
      *pp = p + 1;
      continue;
    }
    if (')' == *p) {
      *pp = p + 1;
      return true;
    }
    return false;
  }
}

static bool validateWktGeometry(const char **pp) {
  if (consumeWktWord(pp, "POINT") || consumeWktWord(pp, "LINESTRING") || consumeWktWord(pp, "MULTIPOINT") ||
      consumeWktWord(pp, "MULTILINESTRING")) {
    consumeWktDimension(pp);
    return skipWktBody(pp);
  }

  if (consumeWktWord(pp, "POLYGON")) {
    consumeWktDimension(pp);
    return validateWktPolygonBody(pp);
  }

  if (consumeWktWord(pp, "MULTIPOLYGON")) {
    consumeWktDimension(pp);
    return validateWktMultiPolygonBody(pp);
  }

  if (consumeWktWord(pp, "GEOMETRYCOLLECTION") || consumeWktWord(pp, "GEOCOLLECTION")) {
    consumeWktDimension(pp);
    return validateWktCollectionBody(pp);
  }

  return false;
}

/*
 * GEOS may throw on malformed polygon rings. Under our ASAN build this turns
 * into SIGABRT before the C API can hand back a normal error code, so reject
 * those structures before calling the reader.
 */
static bool validateWktPolygonStructure(const char *inputWKT) {
  const char *p = inputWKT;
  if (!validateWktGeometry(&p)) {
    return false;
  }
  p = skipWktSpaces(p);
  return '\0' == *p;
}

int32_t initCtxMakePoint() {
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    (void)GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
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
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  GEOSGeometry  *geom = NULL;
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

static int32_t initWktRegex(pcre2_code **ppRegex, pcre2_match_data **ppMatchData) {
  int32_t code = 0;
  char   *wktPatternWithSpace = taosMemoryCalloc(4, 1024);
  if (NULL == wktPatternWithSpace) {
    return terrno;
  }

  (void)snprintf(
      wktPatternWithSpace, 4 * 1024,
      "^( *)point( *)z?m?( *)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *)\\)))|linestring( *)z?m?( "
      "*)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))|polygon( *)z?m?( "
      "*)((empty)|(\\(( *)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))(( *)(,)( "
      "*)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))( *))*( "
      "*)\\)))|multipoint( *)z?m?( *)((empty)|(\\(( "
      "*)((([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}|((empty)|(\\(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *)\\))))(( *)(,)( "
      "*)((([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}|((empty)|(\\(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *)\\))))( *))*( "
      "*)\\)))|multilinestring( *)z?m?( *)((empty)|(\\(( *)((empty)|(\\(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))(( *)(,)( "
      "*)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))( *))*( "
      "*)\\)))|multipolygon( *)z?m?( *)((empty)|(\\(( *)((empty)|(\\(( *)((empty)|(\\(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))(( *)(,)( "
      "*)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))( *))*( *)\\)))(( *)(,)( "
      "*)((empty)|(\\(( *)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))(( *)(,)( "
      "*)((empty)|(\\(( *)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}(( *)(,)( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?(( "
      "*)(([-+]?[0-9]+\\.?[0-9]*)|([-+]?[0-9]*\\.?[0-9]+))(e[-+]?[0-9]+)?){1,3}( *))*( *)\\)))( *))*( *)\\)))( *))*( "
      "*)\\)))|(GEOCOLLECTION\\((?R)(( *)(,)( *)(?R))*( *)\\))( *)$");

  pcre2_code       *pRegex = NULL;
  pcre2_match_data *pMatchData = NULL;
  code = doRegComp(&pRegex, &pMatchData, wktPatternWithSpace);
  if (code < 0) {
    taosMemoryFree(wktPatternWithSpace);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *ppRegex = pRegex;
  *ppMatchData = pMatchData;

  taosMemoryFree(wktPatternWithSpace);
  return code;
}

int32_t initCtxGeomFromText() {
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    (void)GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
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

  if (geosCtx->WKTRegex == NULL) {
    if (initWktRegex(&geosCtx->WKTRegex, &geosCtx->WKTMatchData) != 0) return code;
  }

  return TSDB_CODE_SUCCESS;
}

// inputWKT is a zero ending string
// need to call geosFreeBuffer(*outputGeom) later
int32_t doGeomFromText(const char *inputWKT, unsigned char **outputGeom, size_t *size) {
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  GEOSGeometry  *geom = NULL;
  unsigned char *wkb = NULL;

  if (doRegExec(inputWKT, geosCtx->WKTRegex, geosCtx->WKTMatchData) != 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  if (!validateWktPolygonStructure(inputWKT)) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

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
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    (void)GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
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
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  GEOSGeometry *geom = NULL;
  char         *wkt = NULL;

  geom = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, inputGeom, size);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkt = GEOSWKTWriter_write_r(geosCtx->handle, geosCtx->WKTWriter, geom);
  if (wkt == NULL) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
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

int32_t checkWKB(const unsigned char *wkb, size_t size) {
  int32_t       code = TSDB_CODE_SUCCESS;
  GEOSGeometry *geom = NULL;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  geom = GEOSWKBReader_read_r(geosCtx->handle, geosCtx->WKBReader, wkb, size);
  if (geom == NULL) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

_exit:
  if (geom) {
    GEOSGeom_destroy_r(geosCtx->handle, geom);
    geom = NULL;
  }
  return code;
}

int32_t initCtxRelationFunc() {
  int32_t       code = TSDB_CODE_FAILED;
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  if (geosCtx->handle == NULL) {
    geosCtx->handle = GEOS_init_r();
    if (geosCtx->handle == NULL) {
      return code;
    }

    (void)GEOSContext_setErrorMessageHandler_r(geosCtx->handle, geosErrMsgeHandler, geosCtx->errMsg);
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
                       bool swapped, char *res, _geosRelationFunc_t relationFn, _geosRelationFunc_t swappedRelationFn,
                       _geosPreparedRelationFunc_t preparedRelationFn,
                       _geosPreparedRelationFunc_t swappedPreparedRelationFn) {
  SGeosContext *geosCtx = NULL;

  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));

  if (!preparedGeom1) {
    if (!swapped) {
      if (!relationFn) {
        return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
      }
      *res = relationFn(geosCtx->handle, geom1, geom2);
    } else {
      if (!swappedRelationFn) {
        return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
      }
      *res = swappedRelationFn(geosCtx->handle, geom1, geom2);
    }
  } else {
    if (!swapped) {
      if (!preparedRelationFn) {
        return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
      }
      *res = preparedRelationFn(geosCtx->handle, preparedGeom1, geom2);
    } else {
      if (!swappedPreparedRelationFn) {
        return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
      }
      *res = swappedPreparedRelationFn(geosCtx->handle, preparedGeom1, geom2);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doIntersects(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                     bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res, GEOSIntersects_r, GEOSIntersects_r,
                        GEOSPreparedIntersects_r, GEOSPreparedIntersects_r);
}

int32_t doEquals(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                 bool swapped, char *res) {
  return doGeosRelation(geom1, NULL, geom2, swapped, res, GEOSEquals_r, GEOSEquals_r, NULL,
                        NULL);  // no prepared version for eguals()
}

int32_t doTouches(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                  bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res, GEOSTouches_r, GEOSTouches_r, GEOSPreparedTouches_r,
                        GEOSPreparedTouches_r);
}

int32_t doCovers(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                 bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res, GEOSCovers_r, GEOSCoveredBy_r, GEOSPreparedCovers_r,
                        GEOSPreparedCoveredBy_r);
}

int32_t doContains(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1, const GEOSGeometry *geom2,
                   bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res, GEOSContains_r, GEOSWithin_r, GEOSPreparedContains_r,
                        GEOSPreparedWithin_r);
}

int32_t doContainsProperly(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1,
                           const GEOSGeometry *geom2, bool swapped, char *res) {
  return doGeosRelation(geom1, preparedGeom1, geom2, swapped, res, NULL, NULL, GEOSPreparedContainsProperly_r, NULL);
}

// input is with VARSTR format
// need to call destroyGeometry(outputGeom, outputPreparedGeom) later
int32_t readGeometry(const unsigned char *input, GEOSGeometry **outputGeom,
                     const GEOSPreparedGeometry **outputPreparedGeom) {
  if (!outputGeom) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

  *outputGeom = NULL;

  if (outputPreparedGeom) {  // it means not to generate PreparedGeometry if outputPreparedGeom is NULL
    *outputPreparedGeom = NULL;
  }

  if (varDataLen(input) == 0) {  // empty value
    return TSDB_CODE_SUCCESS;
  }

  SGeosContext *geosCtx = NULL;
  TAOS_CHECK_RETURN(getThreadLocalGeosCtx(&geosCtx));
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
  SGeosContext *geosCtx = acquireThreadLocalGeosCtx();
  if (!geosCtx) return;

  if (preparedGeom && *preparedGeom) {
    GEOSPreparedGeom_destroy_r(geosCtx->handle, *preparedGeom);
    *preparedGeom = NULL;
  }

  if (geom && *geom) {
    GEOSGeom_destroy_r(geosCtx->handle, *geom);
    *geom = NULL;
  }
}
#endif
