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

// extTypeMap.c — external data source type name → TDengine type mapping
//
// DS §5.3.2: type mapping rules for MySQL, PostgreSQL, and InfluxDB 3.x.
//
// Design principles:
//  - extTypeNameToTDengineType() is called by Parser and Planner only.
//  - External Connector does NOT call this; it only uses the SExtColTypeMapping
//    array already embedded in SFederatedScanPhysiNode for value conversion.
//  - Unknown types return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE (no silent fallback).

#include "extTypeMap.h"

#include <string.h>
#include <strings.h>  // strcasecmp / strncasecmp

#include "taosdef.h"
#include "taoserror.h"
#include "tcommon.h"  // VARSTR_HEADER_SIZE
#include "osString.h" // taosStr2Int32
#include "ttypes.h"   // SDataType, decimalTypeFromPrecision
#include "tdef.h"     // DECIMAL*_BYTES, TSDB_DECIMAL_*

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// Case-insensitive prefix match, optionally followed by '(' or whitespace.
static bool typeHasPrefix(const char *typeName, const char *prefix) {
  size_t prefixLen = strlen(prefix);
  if (strncasecmp(typeName, prefix, prefixLen) != 0) {
    return false;
  }
  char c = typeName[prefixLen];
  return c == '\0' || c == '(' || c == ' ';
}

// Parse the length parameter from a type string like "VARCHAR(255)".
// Returns 0 if no parameter found or on parse error.
static int32_t parseTypeLength(const char *typeName) {
  const char *p = strchr(typeName, '(');
  if (!p) return 0;
  return taosStr2Int32(p + 1, NULL, 10);
}

// Parse precision and scale from "DECIMAL(p)" or "DECIMAL(p,s)".
// Clamps both values to the valid TDengine range.
// If no explicit parameters are found, defaults to maximum precision and scale=0.
static void parsePrecisionScale(const char *typeName, uint8_t *pPrec, uint8_t *pScale) {
  const char *p = strchr(typeName, '(');
  if (!p) {
    *pPrec  = TSDB_DECIMAL_MAX_PRECISION;
    *pScale = 0;
    return;
  }
  char   *endp;
  int32_t prec = taosStr2Int32(p + 1, &endp, 10);
  if (prec < TSDB_DECIMAL_MIN_PRECISION) prec = TSDB_DECIMAL_MIN_PRECISION;
  if (prec > TSDB_DECIMAL_MAX_PRECISION) prec = TSDB_DECIMAL_MAX_PRECISION;
  *pPrec  = (uint8_t)prec;
  *pScale = 0;
  if (*endp == ',') {
    int32_t scale = taosStr2Int32(endp + 1, NULL, 10);
    if (scale < TSDB_DECIMAL_MIN_SCALE) scale = TSDB_DECIMAL_MIN_SCALE;
    if (scale > prec)                   scale = prec;
    *pScale = (uint8_t)scale;
  }
}

// Fill pTd for a DECIMAL/NUMERIC type, parsing precision/scale from typeName.
static void setDecimalMapping(const char *typeName, SDataType *pTd) {
  uint8_t prec = 0, scale = 0;
  parsePrecisionScale(typeName, &prec, &scale);
  pTd->type      = decimalTypeFromPrecision(prec);
  pTd->precision = prec;
  pTd->scale     = scale;
  pTd->bytes     = (pTd->type == TSDB_DATA_TYPE_DECIMAL64) ? DECIMAL64_BYTES : DECIMAL128_BYTES;
}

// Convenience: fill pTd for a non-decimal fixed-width type.
#define SET_TD(pTd, t, b)  do { (pTd)->type = (t); (pTd)->precision = 0; \
                                 (pTd)->scale = 0;  (pTd)->bytes = (b); } while (0)

// Default VARCHAR/NCHAR column length used when no explicit width is given.
#define EXT_DEFAULT_VARCHAR_LEN 65535

// ---------------------------------------------------------------------------
// MySQL type mapping  (DS §5.3.2 — MySQL → TDengine)
// ---------------------------------------------------------------------------
static int32_t mysqlTypeMap(const char *typeName, SDataType *pTd) {
  // --- integer types ---
  if (strcasecmp(typeName, "TINYINT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TINYINT, 1);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "TINYINT UNSIGNED") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_UTINYINT, 1);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "SMALLINT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "SMALLINT UNSIGNED") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_USMALLINT, 2);
    return TSDB_CODE_SUCCESS;
  }
  // MEDIUMINT → INT (value domain fits)
  if (strcasecmp(typeName, "MEDIUMINT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "MEDIUMINT UNSIGNED") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_UINT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "INT") == 0 || strcasecmp(typeName, "INTEGER") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "INT UNSIGNED") == 0 || strcasecmp(typeName, "INTEGER UNSIGNED") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_UINT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "BIGINT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "BIGINT UNSIGNED") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_UBIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  // BIT(n) → BIGINT (n≤64) or VARBINARY (n>64)
  if (typeHasPrefix(typeName, "BIT")) {
    int32_t n = parseTypeLength(typeName);
    if (n == 0 || n <= 64) {
      SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    } else {
      SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, (n / 8 + 1) + VARSTR_HEADER_SIZE);
    }
    return TSDB_CODE_SUCCESS;
  }
  // YEAR → SMALLINT
  if (strcasecmp(typeName, "YEAR") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
    return TSDB_CODE_SUCCESS;
  }
  // --- boolean ---
  if (strcasecmp(typeName, "BOOLEAN") == 0 || strcasecmp(typeName, "BOOL") == 0 ||
      strcasecmp(typeName, "TINYINT(1)") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
    return TSDB_CODE_SUCCESS;
  }
  // --- floating point ---
  if (strcasecmp(typeName, "FLOAT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "DOUBLE") == 0 || strcasecmp(typeName, "DOUBLE PRECISION") == 0 ||
      strcasecmp(typeName, "REAL") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
    return TSDB_CODE_SUCCESS;
  }
  // DECIMAL / NUMERIC → DECIMAL(p,s) — precision/scale extracted from type name
  if (typeHasPrefix(typeName, "DECIMAL") || typeHasPrefix(typeName, "NUMERIC")) {
    setDecimalMapping(typeName, pTd);
    return TSDB_CODE_SUCCESS;
  }
  // --- temporal ---
  if (strcasecmp(typeName, "DATE") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "DATETIME") == 0 || typeHasPrefix(typeName, "DATETIME(") ||
      strcasecmp(typeName, "TIMESTAMP") == 0 || typeHasPrefix(typeName, "TIMESTAMP(")) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "TIME") == 0 || typeHasPrefix(typeName, "TIME(")) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  // --- character types ---
  // CHAR / NCHAR / NVARCHAR → NCHAR or BINARY
  if (typeHasPrefix(typeName, "NCHAR") || typeHasPrefix(typeName, "NVARCHAR")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // VARCHAR → VARCHAR (ASCII) or NCHAR (multibyte: caller decides by charset)
  // We default to NCHAR to be safe; precise charset detection is at connector level.
  if (typeHasPrefix(typeName, "VARCHAR")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (typeHasPrefix(typeName, "CHAR")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = 1;
    SET_TD(pTd, TSDB_DATA_TYPE_BINARY, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // TINYTEXT
  if (strcasecmp(typeName, "TINYTEXT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 255 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "TEXT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, 65535 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "MEDIUMTEXT") == 0 || strcasecmp(typeName, "LONGTEXT") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // --- binary types ---
  if (typeHasPrefix(typeName, "BINARY")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = 1;
    SET_TD(pTd, TSDB_DATA_TYPE_BINARY, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (typeHasPrefix(typeName, "VARBINARY")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "TINYBLOB") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BINARY, 255 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "BLOB") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, 65535 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "MEDIUMBLOB") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "LONGBLOB") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BLOB, 4 * 1024 * 1024 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // --- misc ---
  if (typeHasPrefix(typeName, "ENUM") || typeHasPrefix(typeName, "SET")) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "JSON") == 0) {
    // JSON is only valid as a Tag column in TDengine; for ordinary columns we
    // use NCHAR.  The caller (Parser) decides which applies based on context.
    SET_TD(pTd, TSDB_DATA_TYPE_JSON, TSDB_MAX_JSON_TAG_LEN);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "GEOMETRY") == 0 || strcasecmp(typeName, "POINT") == 0 ||
      strcasecmp(typeName, "LINESTRING") == 0 || strcasecmp(typeName, "POLYGON") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);  // variable; Connector fills actual wkb bytes
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// PostgreSQL type mapping  (DS §5.3.2 — PostgreSQL → TDengine)
// ---------------------------------------------------------------------------
static int32_t pgTypeMap(const char *typeName, SDataType *pTd) {
  // --- boolean ---
  if (strcasecmp(typeName, "boolean") == 0 || strcasecmp(typeName, "bool") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
    return TSDB_CODE_SUCCESS;
  }
  // --- integer ---
  if (strcasecmp(typeName, "smallint") == 0 || strcasecmp(typeName, "int2") == 0 ||
      strcasecmp(typeName, "smallserial") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "integer") == 0 || strcasecmp(typeName, "int4") == 0 ||
      strcasecmp(typeName, "int") == 0 || strcasecmp(typeName, "serial") == 0 ||
      strcasecmp(typeName, "serial4") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "bigint") == 0 || strcasecmp(typeName, "int8") == 0 ||
      strcasecmp(typeName, "bigserial") == 0 || strcasecmp(typeName, "serial8") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  // --- floating point ---
  if (strcasecmp(typeName, "real") == 0 || strcasecmp(typeName, "float4") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "double precision") == 0 || strcasecmp(typeName, "float8") == 0 ||
      strcasecmp(typeName, "float") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (typeHasPrefix(typeName, "numeric") || typeHasPrefix(typeName, "decimal")) {
    setDecimalMapping(typeName, pTd);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "money") == 0) {
    // money has 2 implicit decimal places; treat as DECIMAL(19,2)
    pTd->type      = TSDB_DATA_TYPE_DECIMAL64;
    pTd->precision = 19;
    pTd->scale     = 2;
    pTd->bytes     = DECIMAL64_BYTES;
    return TSDB_CODE_SUCCESS;
  }
  // --- character ---
  if (typeHasPrefix(typeName, "char") || typeHasPrefix(typeName, "character")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = 1;
    // Default to NCHAR (UTF-8); single‐byte charset is uncommon in modern PG.
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (typeHasPrefix(typeName, "varchar") || typeHasPrefix(typeName, "character varying")) {
    int32_t len = parseTypeLength(typeName);
    if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "text") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "bytea") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // --- temporal ---
  if (strcasecmp(typeName, "timestamp") == 0 ||
      strcasecmp(typeName, "timestamp without time zone") == 0 ||
      strcasecmp(typeName, "timestamptz") == 0 ||
      strcasecmp(typeName, "timestamp with time zone") == 0 ||
      strcasecmp(typeName, "date") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "time") == 0 || strcasecmp(typeName, "timetz") == 0 ||
      strcasecmp(typeName, "interval") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  // --- misc ---
  if (strcasecmp(typeName, "uuid") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 36 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "json") == 0 || strcasecmp(typeName, "jsonb") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_JSON, TSDB_MAX_JSON_TAG_LEN);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "xml") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "inet") == 0 || strcasecmp(typeName, "cidr") == 0 ||
      strcasecmp(typeName, "macaddr") == 0 || strcasecmp(typeName, "macaddr8") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 64 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (typeHasPrefix(typeName, "bit")) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "geometry") == 0 || strcasecmp(typeName, "point") == 0 ||
      strcasecmp(typeName, "path") == 0 || strcasecmp(typeName, "polygon") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "hstore") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // array types (e.g. "integer[]", "text[]") and range / tsvector types → NCHAR
  if (strstr(typeName, "[]") || typeHasPrefix(typeName, "array") ||
      typeHasPrefix(typeName, "int4range") || typeHasPrefix(typeName, "int8range") ||
      typeHasPrefix(typeName, "numrange") || typeHasPrefix(typeName, "tsrange") ||
      typeHasPrefix(typeName, "tstzrange") || typeHasPrefix(typeName, "daterange") ||
      strcasecmp(typeName, "tsvector") == 0 || strcasecmp(typeName, "tsquery") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // user-defined ENUM from information_schema (reported as "USER-DEFINED" or enum name)
  if (strcasecmp(typeName, "USER-DEFINED") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// InfluxDB 3.x (Arrow type names) → TDengine  (DS §5.3.2)
// ---------------------------------------------------------------------------
static int32_t influxTypeMap(const char *typeName, SDataType *pTd) {
  if (strcasecmp(typeName, "Timestamp") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Int64") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "UInt64") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_UBIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Float64") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Utf8") == 0 || strcasecmp(typeName, "LargeUtf8") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Boolean") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Binary") == 0 || strcasecmp(typeName, "LargeBinary") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  // Arrow Decimal128/256: parse precision/scale from "Decimal128(p,s)" format
  if (typeHasPrefix(typeName, "Decimal128") || typeHasPrefix(typeName, "Decimal256")) {
    setDecimalMapping(typeName, pTd);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Dictionary") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Date32") == 0 || strcasecmp(typeName, "Date64") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "Time32") == 0 || strcasecmp(typeName, "Time64") == 0 ||
      strcasecmp(typeName, "Duration") == 0 || strcasecmp(typeName, "Interval") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcasecmp(typeName, "List") == 0 || strcasecmp(typeName, "LargeList") == 0 ||
      strcasecmp(typeName, "Struct") == 0 || strcasecmp(typeName, "Map") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------
int32_t extTypeNameToTDengineType(EExtSourceType srcType, const char *extTypeName, SDataType *pTdType) {
  if (!extTypeName || !pTdType) return TSDB_CODE_INVALID_PARA;
  switch (srcType) {
    case EXT_SOURCE_MYSQL:
      return mysqlTypeMap(extTypeName, pTdType);
    case EXT_SOURCE_POSTGRESQL:
      return pgTypeMap(extTypeName, pTdType);
    case EXT_SOURCE_INFLUXDB:
      return influxTypeMap(extTypeName, pTdType);
    default:
      return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }
}
