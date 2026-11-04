/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// extTypeMap.c  -  external data source type name  ->  TDengine type mapping
//
// DS section 5.3.2: type mapping rules for MySQL, PostgreSQL, and InfluxDB 3.x.
//
// Design principles:
//  - extTypeNameToTDengineType() is called by Parser and Planner only.
//  - External Connector does NOT call this; it only uses the SExtColTypeMapping
//    array already embedded in SFederatedScanPhysiNode for value conversion.
//  - Unknown types return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE (no silent fallback).

#include "extTypeMap.h"

#include <ctype.h>    // toupper / tolower
#include <string.h>
#ifdef WINDOWS
#  define strcasecmp  _stricmp
#  define strncasecmp _strnicmp
#else
#  include <strings.h>  // strcasecmp / strncasecmp
#endif

#include "query.h"                 // qError
#include "taosdef.h"
#include "taoserror.h"
#include "tcommon.h"  // VARSTR_HEADER_SIZE
#include "osString.h" // taosStr2Int32
#include "ttypes.h"   // SDataType, decimalTypeFromPrecision
#include "tdef.h"     // DECIMAL*_BYTES, TSDB_DECIMAL_*

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// Case-insensitive match on a whole type token: the name must be the prefix
// itself, or continue with '(' / whitespace.  Use this when the prefix is the
// complete type name, so that "date" does not match "datetime".
static bool typeHasTokenPrefix(const char *typeName, const char *prefix) {
  size_t prefixLen = strlen(prefix);
  if (strncasecmp(typeName, prefix, prefixLen) != 0) {
    return false;
  }
  char c = typeName[prefixLen];
  return c == '\0' || c == '(' || c == ' ';
}

// Case-insensitive match on a type family prefix, without a token boundary:
// the timezone classifier groups "datetime2(6)" / "smalldatetime" / "datetime"
// under one family, so "date" has to match "datetime" as well.  Use
// typeHasTokenPrefix() when the prefix names a single type.
static bool typeFamilyStartsWith(const char *typeName, const char *prefix) {
  return strncasecmp(typeName, prefix, strlen(prefix)) == 0;
}

// Parse the length parameter from a type string like "VARCHAR(255)".
// Returns 0 if no parameter is present or syntactically valid. A declared
// value above the TDengine field limit is saturated before later arithmetic,
// then normalizeExternalResultType() converts it to the supported result
// schema limit. This keeps an oversized external declaration from overflowing
// an int32_t while still deferring value rejection to execution time.
static int32_t parseTypeLength(const char *typeName) {
  const char *p = strchr(typeName, '(');
  if (!p) return 0;
  ++p;
  if (!isdigit((unsigned char)*p)) return 0;

  int64_t value = 0;
  while (isdigit((unsigned char)*p)) {
    if (value >= TSDB_MAX_FIELD_LEN) return TSDB_MAX_FIELD_LEN;
    value = value * 10 + (*p - '0');
    if (value >= TSDB_MAX_FIELD_LEN) return TSDB_MAX_FIELD_LEN;
    ++p;
  }
  return (int32_t)value;
}

// Parse one non-negative modifier without allowing external text to overflow
// the arithmetic used by the mapper. The value is deliberately capped above
// every supported decimal precision so callers can still distinguish an
// oversized declaration from an omitted modifier.
static int32_t parseBoundedTypeModifier(const char *p, const char **ppEnd) {
  int64_t value = 0;
  const char *cur = p;
  while (isdigit((unsigned char)*cur)) {
    if (value >= TSDB_MAX_FIELD_LEN) {
      while (isdigit((unsigned char)*cur)) ++cur;
      if (ppEnd != NULL) *ppEnd = cur;
      return TSDB_MAX_FIELD_LEN;
    }
    value = value * 10 + (*cur - '0');
    ++cur;
  }
  if (ppEnd != NULL) *ppEnd = cur;
  return (int32_t)value;
}

// Parse precision and scale from "DECIMAL(p)" or "DECIMAL(p,s)".
// Normalizes both values to the valid TDengine result-schema range while
// preserving as many source integer digits as the target representation allows.
// If no explicit parameters are found, defaults to maximum precision and a
// reasonable default scale (6) to preserve decimal digits in unbound NUMERIC.
static void parsePrecisionScale(const char *typeName, uint8_t *pPrec, uint8_t *pScale) {
  const char *p = strchr(typeName, '(');
  if (!p) {
    *pPrec  = TSDB_DECIMAL_MAX_PRECISION;
    *pScale = 6;  // Reasonable default for unbound NUMERIC/DECIMAL (preserves decimal digits)
    return;
  }
  const char *endp = NULL;
  int32_t declaredPrec = parseBoundedTypeModifier(p + 1, &endp);
  if (endp == p + 1) {
    *pPrec = TSDB_DECIMAL_MAX_PRECISION;
    *pScale = 6;
    return;
  }
  int32_t declaredScale = 0;
  if (*endp == ',') {
    declaredScale = parseBoundedTypeModifier(endp + 1, NULL);
    if (declaredScale < TSDB_DECIMAL_MIN_SCALE) {
      declaredScale = TSDB_DECIMAL_MIN_SCALE;
    }
    if (declaredScale > declaredPrec) {
      declaredScale = declaredPrec;
    }
  }

  int32_t prec = declaredPrec;
  if (prec < TSDB_DECIMAL_MIN_PRECISION) prec = TSDB_DECIMAL_MIN_PRECISION;
  if (prec > TSDB_DECIMAL_MAX_PRECISION) prec = TSDB_DECIMAL_MAX_PRECISION;
  *pPrec  = (uint8_t)prec;
  int32_t scale = declaredScale;
  if (declaredPrec > prec) {
    int32_t integerDigits = declaredPrec - declaredScale;
    // DECIMAL(65,30), for example, becomes DECIMAL(38,3): retaining the
    // original 35 integer digits is more useful and avoids value overflow.
    scale = integerDigits >= prec ? 0 : prec - integerDigits;
  }
  if (scale > prec) scale = prec;
  *pScale = (uint8_t)scale;
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

// Normalize variable-length output schemas after provider-specific mapping.
// SDataType.bytes always includes the TDengine value header. An external
// declaration may be much larger, but result metadata must remain valid for
// every downstream parser, planner, TLV, executor, and client path.
static void normalizeExternalResultType(SDataType *pTd) {
  int32_t maxBytes = 0;

  switch (pTd->type) {
    case TSDB_DATA_TYPE_VARCHAR:
      maxBytes = TSDB_MAX_BINARY_LEN;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      maxBytes = TSDB_MAX_NCHAR_LEN;
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      maxBytes = TSDB_MAX_VARBINARY_LEN;
      break;
    case TSDB_DATA_TYPE_GEOMETRY:
      maxBytes = TSDB_MAX_GEOMETRY_LEN;
      break;
    case TSDB_DATA_TYPE_BLOB:
      pTd->bytes = TSDB_MAX_BLOB_LEN + BLOBSTR_HEADER_SIZE;
      return;
    default:
      return;
  }

  if (pTd->bytes <= VARSTR_HEADER_SIZE || pTd->bytes > maxBytes) {
    pTd->bytes = maxBytes;
  }
}

// Convenience: fill pTd for a non-decimal fixed-width type.
#define SET_TD(pTd, t, b)  do { (pTd)->type = (t); (pTd)->precision = 0; \
                                 (pTd)->scale = 0;  (pTd)->bytes = (b); } while (0)

// Default VARCHAR/NCHAR column length used when no explicit width is given.
// Must not exceed TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE (65517) to avoid
// CAST overflow when UNION reconciles column types across sources.
#define EXT_DEFAULT_VARCHAR_LEN 65517

// ---------------------------------------------------------------------------
// MySQL type mapping  (DS section 5.3.2  -  MySQL  ->  TDengine)
// ---------------------------------------------------------------------------

// Returns true when the MySQL column charset requires NCHAR mapping.
// The MySQL connector forces a utf8mb4 client connection, so the *received* bytes
// are always valid UTF-8 regardless of this charset; but the charset indicates
// the *intended* character set of the column:
//   Multi-byte charsets (utf8*, ucs2, utf16*, utf32)  ->  NCHAR (UCS-4 stored).
//   Single-byte charsets (ascii, latin1, binary, cp*, ...)   ->  VARCHAR (raw bytes).
static bool mysqlCharsetNeedsNchar(const char *cs) {
  if (!cs || !cs[0]) return false;
  if (strncasecmp(cs, "utf8", 4) == 0) return true;   // utf8, utf8mb3, utf8mb4, utf8unicode_ci...
  if (strncasecmp(cs, "ucs",  3) == 0) return true;   // ucs2, ucs4
  if (strncasecmp(cs, "utf16", 5) == 0) return true;  // utf16, utf16le
  if (strncasecmp(cs, "utf32", 5) == 0) return true;
  return false;
}

static int32_t mysqlTypeMap(const char *typeName, const char *charset, SDataType *pTd) {
  // Compute base length (before '(') with trailing spaces stripped, and first char.
  const char *paren = strchr(typeName, '(');
  size_t      blen  = paren ? (size_t)(paren - typeName) : strlen(typeName);
  while (blen > 0 && typeName[blen - 1] == ' ') blen--;
  char fc = (char)toupper((unsigned char)typeName[0]);

  switch (fc) {
    case 'B':
      switch (blen) {
        case 3:  // BIT
        {
          int32_t n = parseTypeLength(typeName);
          if (n == 0 || n < 64) {
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          } else if (n == 64) {
            SET_TD(pTd, TSDB_DATA_TYPE_UBIGINT, 8);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, (n / 8 + 1) + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        }
        case 4:  // BLOB vs BOOL
          if (strncasecmp(typeName, "blob", 4) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, 65535 + VARSTR_HEADER_SIZE);
          } else {  // BOOL
            SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
          }
          return TSDB_CODE_SUCCESS;
        case 6:  // BIGINT vs BINARY
          if (strncasecmp(typeName, "bigint", 6) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          } else {  // BINARY  -  fixed-length binary, use VARBINARY to avoid UTF-8 decode errors
            int32_t len = parseTypeLength(typeName);
            if (len == 0) len = 1;
            SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, len + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        case 7:  // BOOLEAN
          SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
          return TSDB_CODE_SUCCESS;
        case 15:  // BIGINT UNSIGNED
          SET_TD(pTd, TSDB_DATA_TYPE_UBIGINT, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'C':
      switch (blen) {
        case 4:  // CHAR  -  charset-aware: utf8mb4/utf8/...  ->  NCHAR; ascii/latin1/...  ->  BINARY (DS section 5.3.2)
        {
          int32_t len = parseTypeLength(typeName);
          if (len == 0) len = 1;
          if (mysqlCharsetNeedsNchar(charset)) {
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_BINARY, len + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        }
        default: break;
      }
      break;

    case 'D':
      switch (blen) {
        case 4:  // DATE
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 6:  // DOUBLE
          SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
          return TSDB_CODE_SUCCESS;
        case 7:  // DECIMAL
          setDecimalMapping(typeName, pTd);
          return TSDB_CODE_SUCCESS;
        case 8:  // DATETIME
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 16:  // DOUBLE PRECISION
          SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'E':  // ENUM (blen=4)  -  charset-aware (DS section 5.3.2)
      if (mysqlCharsetNeedsNchar(charset)) {
        SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      } else {
        SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      }
      return TSDB_CODE_SUCCESS;

    case 'F':  // FLOAT (blen=5)
      SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
      return TSDB_CODE_SUCCESS;

    case 'G':  // GEOMETRY (blen=8)
      SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
      return TSDB_CODE_SUCCESS;

    case 'I':
      switch (blen) {
        case 3:  // INT
          SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          return TSDB_CODE_SUCCESS;
        case 7:  // INTEGER
          SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          return TSDB_CODE_SUCCESS;
        case 12:  // INT UNSIGNED
          SET_TD(pTd, TSDB_DATA_TYPE_UINT, 4);
          return TSDB_CODE_SUCCESS;
        case 16:  // INTEGER UNSIGNED
          SET_TD(pTd, TSDB_DATA_TYPE_UINT, 4);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'J':  // JSON (blen=4)  -  no native JSON column in external tables; serialize to string
      SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      return TSDB_CODE_SUCCESS;

    case 'L':
      switch (blen) {
        case 8:  // LONGBLOB  ->  BLOB; LONGTEXT  ->  NCHAR
          if (strncasecmp(typeName, "longblob", 8) == 0) {
            // TSDB_MAX_BLOB_LEN already includes BLOBSTR_HEADER_SIZE, so this is the
            // maximum BLOB column capacity; extConnectorQuery derives the payload
            // limit as bytes - BLOBSTR_HEADER_SIZE.
            SET_TD(pTd, TSDB_DATA_TYPE_BLOB, TSDB_MAX_BLOB_LEN);
          } else {  // LONGTEXT
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        case 10:  // LINESTRING
          SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'M':
      switch (blen) {
        case 9:  // MEDIUMINT
          SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          return TSDB_CODE_SUCCESS;
        case 10:  // MEDIUMBLOB  ->  VARBINARY; MEDIUMTEXT  ->  NCHAR
          if (toupper((unsigned char)typeName[6]) == 'B') {
            SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        case 18:  // MEDIUMINT UNSIGNED
          SET_TD(pTd, TSDB_DATA_TYPE_UINT, 4);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'N':
      switch (blen) {
        case 5:  // NCHAR
        case 8:  // NVARCHAR
        {
          int32_t len = parseTypeLength(typeName);
          if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
          SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        }
        case 7:  // NUMERIC
          setDecimalMapping(typeName, pTd);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'P':
      switch (blen) {
        case 5:  // POINT
          SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
          return TSDB_CODE_SUCCESS;
        case 7:  // POLYGON
          SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'R':  // REAL (blen=4)
      SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
      return TSDB_CODE_SUCCESS;

    case 'S':
      switch (blen) {
        case 3:  // SET  -  charset-aware (DS section 5.3.2)
          if (mysqlCharsetNeedsNchar(charset)) {
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        case 8:  // SMALLINT (strlen("SMALLINT") = 8)
          SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
          return TSDB_CODE_SUCCESS;
        case 17:  // SMALLINT UNSIGNED  (strlen("SMALLINT UNSIGNED") = 17)
          SET_TD(pTd, TSDB_DATA_TYPE_USMALLINT, 2);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'T':
      switch (blen) {
        case 4:  // TEXT vs TIME  -  TEXT is charset-aware (DS section 5.3.2: ASCII -> VARCHAR, utf8mb4 -> NCHAR)
          if (strncasecmp(typeName, "text", 4) == 0) {
            if (mysqlCharsetNeedsNchar(charset)) {
              SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, 65535 + VARSTR_HEADER_SIZE);
            } else {
              SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 65535 + VARSTR_HEADER_SIZE);
            }
          } else {  // TIME
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          }
          return TSDB_CODE_SUCCESS;
        case 7:  // TINYINT (may be TINYINT(1)  ->  BOOL)
          if (paren && parseTypeLength(typeName) == 1) {
            SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_TINYINT, 1);
          }
          return TSDB_CODE_SUCCESS;
        case 8:  // TINYBLOB  ->  VARBINARY; TINYTEXT  ->  charset-aware (DS section 5.3.2)
          if (toupper((unsigned char)typeName[4]) == 'B') {
            SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, 255 + VARSTR_HEADER_SIZE);
          } else {  // TINYTEXT
            if (mysqlCharsetNeedsNchar(charset)) {
              SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, 255 + VARSTR_HEADER_SIZE);
            } else {
              SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 255 + VARSTR_HEADER_SIZE);
            }
          }
          return TSDB_CODE_SUCCESS;
        case 9:  // TIMESTAMP
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 16:  // TINYINT UNSIGNED
          SET_TD(pTd, TSDB_DATA_TYPE_UTINYINT, 1);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'V':
      switch (blen) {
        case 7:  // VARCHAR  -  charset-aware: utf8mb4/utf8/...  ->  NCHAR; ascii/latin1/...  ->  VARCHAR
        {
          int32_t len = parseTypeLength(typeName);
          if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
          if (mysqlCharsetNeedsNchar(charset)) {
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        }
        case 9:  // VARBINARY
        {
          int32_t len = parseTypeLength(typeName);
          if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
          SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, len + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        }
        default: break;
      }
      break;

    case 'Y':  // YEAR (blen=4)
      SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
      return TSDB_CODE_SUCCESS;

    default: break;
  }
  qError("MySQL type not mappable to TDengine: '%s'", typeName);
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// PostgreSQL type mapping  (DS section 5.3.2  -  PostgreSQL  ->  TDengine)
// ---------------------------------------------------------------------------

// Returns true when the PG database server encoding requires NCHAR mapping.
// PG has a single database-level encoding (not per-column).  The encoding
// string comes from current_setting('server_encoding') (e.g. "UTF8", "SQL_ASCII").
// Only UTF-8 requires NCHAR; SQL_ASCII and single-byte encodings use VARCHAR.
static bool pgEncodingNeedsNchar(const char *enc) {
  if (!enc || !enc[0]) return false;
  return (strcasecmp(enc, "UTF8") == 0 || strcasecmp(enc, "UTF-8") == 0);
}

static int32_t pgTypeMap(const char *typeName, const char *encoding, SDataType *pTd) {
  // Handle "[]" array suffix and array/range/tsvector prefix early.
  if (strstr(typeName, "[]") || typeHasTokenPrefix(typeName, "array") ||
      typeHasTokenPrefix(typeName, "int4range") || typeHasTokenPrefix(typeName, "int8range") ||
      typeHasTokenPrefix(typeName, "numrange") || typeHasTokenPrefix(typeName, "tsrange") ||
      typeHasTokenPrefix(typeName, "tstzrange") || typeHasTokenPrefix(typeName, "daterange") ||
      strcasecmp(typeName, "tsvector") == 0 || strcasecmp(typeName, "tsquery") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }

  // Compute base length and first char for two-level dispatch.
  const char *paren = strchr(typeName, '(');
  size_t      blen  = paren ? (size_t)(paren - typeName) : strlen(typeName);
  while (blen > 0 && typeName[blen - 1] == ' ') blen--;
  char fc = (char)toupper((unsigned char)typeName[0]);

  switch (fc) {
    case 'B':
      switch (blen) {
        case 3:   // bit
        case 11:  // bit varying
          SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 4:  // bool
          SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
          return TSDB_CODE_SUCCESS;
        case 5:  // bytea
          SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 6:  // bigint vs bigserial
          if (strncasecmp(typeName, "bigint", 6) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          } else {  // bigserial
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          }
          return TSDB_CODE_SUCCESS;
        case 7:  // boolean
          SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
          return TSDB_CODE_SUCCESS;
        case 9:  // bigserial (full name)
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'C':
      switch (blen) {
        case 4:  // char vs cidr  -  char is encoding-aware (DS section 5.3.2: ASCII -> BINARY, UTF-8 -> NCHAR)
          if (strncasecmp(typeName, "char", 4) == 0) {
            int32_t len = parseTypeLength(typeName);
            if (len == 0) len = 1;
            if (pgEncodingNeedsNchar(encoding)) {
              SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
            } else {
              SET_TD(pTd, TSDB_DATA_TYPE_BINARY, len + VARSTR_HEADER_SIZE);
            }
          } else {  // cidr
            SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 64 + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        case 9:   // character  -  encoding-aware (DS section 5.3.2: ASCII -> BINARY, UTF-8 -> NCHAR)
        case 17:  // character varying
        {
          int32_t len = parseTypeLength(typeName);
          if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
          if (blen == 9) {  // "character" (fixed-length CHAR)  -  encoding-aware
            if (pgEncodingNeedsNchar(encoding)) {
              SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
            } else {
              SET_TD(pTd, TSDB_DATA_TYPE_BINARY, len + VARSTR_HEADER_SIZE);
            }
          } else {  // "character varying"  -  encoding-aware
            if (pgEncodingNeedsNchar(encoding)) {
              SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
            } else {
              SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
            }
          }
          return TSDB_CODE_SUCCESS;
        }
        default: break;
      }
      break;

    case 'D':
      switch (blen) {
        case 4:  // date
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 7:  // decimal
          setDecimalMapping(typeName, pTd);
          return TSDB_CODE_SUCCESS;
        case 16:  // double precision
          SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'F':
      switch (blen) {
        case 5:  // float
          SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
          return TSDB_CODE_SUCCESS;
        case 6:  // float4 vs float8
          if (typeName[5] == '4') {
            SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
          } else {  // float8
            SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
          }
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'G':  // geometry / point / path / polygon handled under P
      SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
      return TSDB_CODE_SUCCESS;

    case 'H':  // hstore (blen=6)
      SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      return TSDB_CODE_SUCCESS;

    case 'I':
      switch (blen) {
        case 3:  // int
          SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          return TSDB_CODE_SUCCESS;
        case 4:  // int2 / int4 / int8 / inet
        {
          char c4 = (char)tolower((unsigned char)typeName[3]);
          if (c4 == '2') {
            SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
          } else if (c4 == '4') {
            SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          } else if (c4 == '8') {
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          } else {  // inet
            SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 64 + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        }
        case 7:  // integer
          SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          return TSDB_CODE_SUCCESS;
        case 8:  // interval
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'J':
      switch (blen) {
        case 4:  // json  -  serialize to string; no native JSON column in external tables
          SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 5:  // jsonb  -  serialize to string; no native JSON column in external tables
          SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'M':
      switch (blen) {
        case 5:  // money  ->  DECIMAL(18,2)  DS section 5.3.2
          // PG money range  <=  92233720368547758.07; DECIMAL64 max precision = 18.
          pTd->type      = TSDB_DATA_TYPE_DECIMAL64;
          pTd->precision = 18;
          pTd->scale     = 2;
          pTd->bytes     = DECIMAL64_BYTES;
          return TSDB_CODE_SUCCESS;
        case 7:  // macaddr (explicit check to avoid collision with user-defined types)
          if (strncasecmp(typeName, "macaddr", 7) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 64 + VARSTR_HEADER_SIZE);
            return TSDB_CODE_SUCCESS;
          }
          break;
        case 8:  // macaddr8 (explicit check to avoid collision with user-defined types like my_point)
          if (strncasecmp(typeName, "macaddr8", 8) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 64 + VARSTR_HEADER_SIZE);
            return TSDB_CODE_SUCCESS;
          }
          break;
        default: break;
      }
      break;

    case 'N':  // numeric (blen=7)
      setDecimalMapping(typeName, pTd);
      return TSDB_CODE_SUCCESS;

    case 'P':
      switch (blen) {
        case 4:  // path
          SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
          return TSDB_CODE_SUCCESS;
        case 5:  // point
          SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
          return TSDB_CODE_SUCCESS;
        case 7:  // polygon
          SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'R':  // real (blen=4)
      SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
      return TSDB_CODE_SUCCESS;

    case 'S':
      switch (blen) {
        case 6:  // serial
          SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          return TSDB_CODE_SUCCESS;
        case 7:  // serial4 vs serial8
          if (typeName[6] == '8') {
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          } else {
            SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
          }
          return TSDB_CODE_SUCCESS;
        case 8:  // smallint
          SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
          return TSDB_CODE_SUCCESS;
        case 11:  // smallserial
          SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'T':
      switch (blen) {
        case 4:  // text vs time
          if (strncasecmp(typeName, "text", 4) == 0) {
            // PG text is variable-length Unicode  ->  NCHAR per DS section 5.3.2.
            // Functions that receive NCHAR input (to_timestamp, st_geomfromtext, etc.) handle
            // the UCS-4  ->  UTF-8 conversion internally.
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          } else {  // time
            SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          }
          return TSDB_CODE_SUCCESS;
        case 6:  // timetz
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        case 9:  // timestamp
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 11:  // timestamptz
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 19:  // time with time zone
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        case 22:  // time without time zone
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        case 24:  // timestamp with time zone
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 27:  // timestamp without time zone
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'U':
      switch (blen) {
        case 4:  // uuid
          SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 36 + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 12:  // USER-DEFINED
          SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'V':  // varchar (blen=7)  -  encoding-aware
    {
      int32_t len = parseTypeLength(typeName);
      if (len == 0) len = EXT_DEFAULT_VARCHAR_LEN;
      if (pgEncodingNeedsNchar(encoding)) {
        SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
      } else {
        SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
      }
      return TSDB_CODE_SUCCESS;
    }

    case 'X':  // xml (blen=3)
      SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      return TSDB_CODE_SUCCESS;

    default: break;
  }
  qError("PostgreSQL type not mappable to TDengine: '%s'", typeName);
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// InfluxDB 3.x (Arrow type names)  ->  TDengine  (DS section 5.3.2)
// ---------------------------------------------------------------------------
/* Copy the base type name (before any '(' modifier) into buf, lowercased. */
static void sqlserverBaseTypeName(const char *typeName, char *buf, int32_t bufLen) {
  int32_t n = 0;
  for (const char *p = typeName; *p != '\0' && *p != '(' && n < bufLen - 1; ++p) {
    buf[n++] = (char)tolower((unsigned char)*p);
  }
  while (n > 0 && (buf[n - 1] == ' ' || buf[n - 1] == '\t')) {
    --n;
  }
  buf[n] = '\0';
}

// SQL Server type map (DS 5.3.2.1).  Type names come from
// INFORMATION_SCHEMA.DATA_TYPE, normalized by the provider to carry their
// length/precision modifiers ("varchar(50)", "nvarchar(max)", "decimal(18,2)",
// "datetime2(3)").  Unicode strings are converted by the connector, so the
// charset argument is unused.
static int32_t sqlserverTypeMap(const char *typeName, const char *charset, SDataType *pTd) {
  (void)charset;

  char base[64] = {0};
  sqlserverBaseTypeName(typeName, base, sizeof(base));
  if ('\0' == base[0]) {
    return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }

  /* Integer family */
  if (strcmp(base, "bit") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "tinyint") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TINYINT, 1);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "smallint") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_SMALLINT, 2);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "int") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_INT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "bigint") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }

  /* Floating point */
  if (strcmp(base, "real") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "float") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
    return TSDB_CODE_SUCCESS;
  }

  /* Exact numerics */
  if (strcmp(base, "decimal") == 0 || strcmp(base, "numeric") == 0) {
    setDecimalMapping(typeName, pTd);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "money") == 0 || strcmp(base, "smallmoney") == 0) {
    /* Money keeps value semantics; its scale is fixed at 4. */
    setDecimalMapping(strcmp(base, "money") == 0 ? "decimal(19,4)" : "decimal(10,4)", pTd);
    return TSDB_CODE_SUCCESS;
  }

  /* Strings */
  if (strcmp(base, "char") == 0 || strcmp(base, "varchar") == 0) {
    int32_t len = parseTypeLength(typeName);
    if (len <= 0) {
      len = EXT_DEFAULT_VARCHAR_LEN;
    }
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "nchar") == 0 || strcmp(base, "nvarchar") == 0) {
    int32_t len = parseTypeLength(typeName);
    if (len <= 0) {
      len = EXT_DEFAULT_VARCHAR_LEN / TSDB_NCHAR_SIZE;
    }
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "uniqueidentifier") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 36 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "xml") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }

  /* Binary family */
  if (strcmp(base, "binary") == 0 || strcmp(base, "varbinary") == 0) {
    int32_t len = parseTypeLength(typeName);
    if (len <= 0) {
      len = EXT_DEFAULT_VARCHAR_LEN;
    }
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "image") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BLOB, TSDB_MAX_BLOB_LEN);
    return TSDB_CODE_SUCCESS;
  }
  /* timestamp / rowversion is a row-version counter, not a time value: it maps
   * to VARBINARY and can never be a virtual-table timeline (FS 4.3). */
  if (strcmp(base, "timestamp") == 0 || strcmp(base, "rowversion") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, 8 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }

  /* Temporal types (DS 5.3.2.1) */
  if (strcmp(base, "date") == 0 || strcmp(base, "datetime") == 0 ||
      strcmp(base, "smalldatetime") == 0 || strcmp(base, "datetime2") == 0 ||
      strcmp(base, "datetimeoffset") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "time") == 0) {
    /* Microseconds since midnight, matching PG's time mapping. */
    SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
    return TSDB_CODE_SUCCESS;
  }

  /* Spatial */
  if (strcmp(base, "geography") == 0 || strcmp(base, "geometry") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_GEOMETRY, 0);
    return TSDB_CODE_SUCCESS;
  }

  /* Deliberately unsupported: referencing them is an error, never a silent
   * string fallback (DS 5.3.2.1). */
  if (strcmp(base, "sql_variant") == 0 || strcmp(base, "hierarchyid") == 0 ||
      strcmp(base, "vector") == 0) {
    return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }

  qError("sqlserverTypeMap: unknown SQL Server type '%s'", typeName);
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

static int32_t influxTypeMap(const char *typeName, SDataType *pTd) {
  // Compute base length and first char for two-level dispatch.
  // Strip both '(...)' and '[...]' suffix notations so that type strings like
  // "timestamp[ns, tz=UTC]" or "Decimal(10,2)" are dispatched correctly.
  const char *paren   = strchr(typeName, '(');
  const char *bracket = strchr(typeName, '[');
  const char *end = NULL;
  if (paren && bracket) end = (paren < bracket) ? paren : bracket;
  else if (paren)        end = paren;
  else if (bracket)      end = bracket;
  size_t blen = end ? (size_t)(end - typeName) : strlen(typeName);
  while (blen > 0 && typeName[blen - 1] == ' ') blen--;
  char fc = (char)toupper((unsigned char)typeName[0]);

  switch (fc) {
    case 'B':
      switch (blen) {
        case 6:  // Binary
          SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 7:  // Boolean
          SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'D':
      switch (blen) {
        case 6:  // Date32 / Date64  -  both map to TIMESTAMP
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        case 8:  // Duration
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        case 10:  // Decimal128 / Decimal256 / Dictionary
          if (strncasecmp(typeName, "dict", 4) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          } else {  // Decimal128 / Decimal256
            setDecimalMapping(typeName, pTd);
          }
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'F':  // Float64 (blen=7)
      SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
      return TSDB_CODE_SUCCESS;

    case 'I':
      switch (blen) {
        case 5:  // Int64
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        case 8:  // Interval
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'L':
      switch (blen) {
        case 4:  // List
          SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 9:  // LargeUtf8 vs LargeList
          if (strncasecmp(typeName, "largel", 6) == 0) {
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          } else {  // LargeUtf8
            SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          }
          return TSDB_CODE_SUCCESS;
        case 11:  // LargeBinary
          SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'M':  // Map (blen=3)
      SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      return TSDB_CODE_SUCCESS;

    case 'S':  // Struct (blen=6)
      SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
      return TSDB_CODE_SUCCESS;

    case 'T':
      switch (blen) {
        case 6:  // Time32 / Time64  -  both map to BIGINT
          SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
          return TSDB_CODE_SUCCESS;
        case 9:  // Timestamp
          SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    case 'U':
      switch (blen) {
        case 4:  // Utf8
          SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
          return TSDB_CODE_SUCCESS;
        case 6:  // UInt64
          SET_TD(pTd, TSDB_DATA_TYPE_UBIGINT, 8);
          return TSDB_CODE_SUCCESS;
        default: break;
      }
      break;

    default: break;
  }
  qError("InfluxDB type not mappable to TDengine: '%s'", typeName);
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// Oracle type map  (DS section 5.3.2.1  -  Oracle  ->  TDengine)
// ---------------------------------------------------------------------------
/* Copy the base type name of an Oracle type, lowercased, stopping at '(' or at
 * a space: the temporal types append a qualifier after the modifier
 * ("TIMESTAMP(6) WITH TIME ZONE"), and INTERVAL types carry two words before
 * their modifier. */
static void oracleBaseTypeName(const char *typeName, char *buf, int32_t bufLen) {
  int32_t n = 0;
  for (const char *p = typeName; *p != '\0' && *p != '(' && n < bufLen - 1; ++p) {
    buf[n++] = (char)tolower((unsigned char)*p);
  }
  while (n > 0 && (buf[n - 1] == ' ' || buf[n - 1] == '\t')) {
    --n;
  }
  buf[n] = '\0';
}

/* Parse the precision and scale of NUMBER(p) / NUMBER(p,s).
 * *pHasScale distinguishes "NUMBER(p)" from "NUMBER(p,0)"; both are integer
 * forms but only the explicit form states a scale. */
static bool oracleParseNumberModifier(const char *typeName, int32_t *pPrec, int32_t *pScale,
                                      bool *pHasScale) {
  const char *p = strchr(typeName, '(');
  if (NULL == p) {
    return false;
  }
  const char *endp = NULL;
  int32_t     prec = parseBoundedTypeModifier(p + 1, &endp);
  if (endp == p + 1) {
    return false;
  }
  int32_t scale = 0;
  bool    hasScale = false;
  if (*endp == ',') {
    scale = parseBoundedTypeModifier(endp + 1, NULL);
    hasScale = true;
  }
  *pPrec = prec;
  *pScale = scale;
  *pHasScale = hasScale;
  return true;
}

// Oracle type map (DS 5.3.2.1).  Type names come from ALL_TAB_COLUMNS.DATA_TYPE,
// re-composed by the provider so the numeric modifier is part of the text
// ("NUMBER(10,2)", "FLOAT(126)", "VARCHAR2(50)", "TIMESTAMP(6) WITH TIME ZONE").
// Only lossless mappings are produced: an unrepresentable declaration returns
// TSDB_CODE_EXT_TYPE_NOT_MAPPABLE and is never silently widened or truncated.
static int32_t oracleTypeMap(const char *typeName, const char *charset, SDataType *pTd) {
  (void)charset;  // text always arrives as UTF-8 and is converted by the connector

  char base[64] = {0};
  oracleBaseTypeName(typeName, base, sizeof(base));
  if ('\0' == base[0]) {
    return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }

  /* NUMBER family.  Oracle has no machine integer in DDL: every numeric column
   * is a NUMBER with a declared precision, so the mapping is driven entirely by
   * that declaration. */
  if (strcmp(base, "number") == 0 || strcmp(base, "numeric") == 0 ||
      strcmp(base, "decimal") == 0) {
    int32_t prec = 0, scale = 0;
    bool    hasScale = false;
    if (!oracleParseNumberModifier(typeName, &prec, &scale, &hasScale)) {
      /* An unqualified NUMBER holds 38 digits with an arbitrary exponent; there
       * is no lossless TDengine representation (DS 5.3.2.1). */
      qError("oracleTypeMap: unqualified NUMBER has no lossless mapping: '%s'", typeName);
      return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
    }
    if (scale != 0) {
      if (prec > TSDB_DECIMAL_MAX_PRECISION) {
        return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
      }
      setDecimalMapping(typeName, pTd);
      return TSDB_CODE_SUCCESS;
    }
    if (prec == 1) {
      /* NUMBER(1,0) is a numeric column, never a boolean: mapping it to BOOL
       * would reject values 2..9 that the column can legitimately hold. */
      SET_TD(pTd, TSDB_DATA_TYPE_TINYINT, 1);
      return TSDB_CODE_SUCCESS;
    }
    if (prec <= 18) {
      SET_TD(pTd, TSDB_DATA_TYPE_BIGINT, 8);
      return TSDB_CODE_SUCCESS;
    }
    if (prec <= TSDB_DECIMAL_MAX_PRECISION) {
      setDecimalMapping(typeName, pTd);
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }

  /* INTEGER / INT are aliases of NUMBER(38,0), not machine integers: mapping
   * them to INT would silently overflow. */
  if (strcmp(base, "integer") == 0 || strcmp(base, "int") == 0 ||
      strcmp(base, "smallint") == 0) {
    setDecimalMapping("NUMBER(38,0)", pTd);
    return TSDB_CODE_SUCCESS;
  }

  /* FLOAT(b) carries a BINARY precision in DATA_PRECISION. */
  if (strcmp(base, "float") == 0) {
    int32_t binaryPrec = 0;
    int32_t dummy = 0;
    bool    hasScale = false;
    if (oracleParseNumberModifier(typeName, &binaryPrec, &dummy, &hasScale)) {
      if (binaryPrec <= 24) {
        SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
        return TSDB_CODE_SUCCESS;
      }
      if (binaryPrec <= 53) {
        SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
        return TSDB_CODE_SUCCESS;
      }
      /* 54..126 exceeds IEEE double precision: no lossless mapping. */
      qError("oracleTypeMap: FLOAT(%d) exceeds IEEE double precision: '%s'", binaryPrec, typeName);
      return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
    }
    /* Unqualified FLOAT is FLOAT(126). */
    return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }
  if (strcmp(base, "real") == 0 || strncmp(base, "double", 6) == 0) {
    /* REAL is FLOAT(63) and DOUBLE PRECISION is FLOAT(126); both exceed IEEE
     * double precision. */
    return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }
  if (strcmp(base, "binary_float") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_FLOAT, 4);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "binary_double") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_DOUBLE, 8);
    return TSDB_CODE_SUCCESS;
  }

  /* Character family */
  if (strcmp(base, "char") == 0 || strcmp(base, "varchar2") == 0 ||
      strcmp(base, "varchar") == 0) {
    int32_t len = parseTypeLength(typeName);
    if (len <= 0) {
      len = EXT_DEFAULT_VARCHAR_LEN;
    }
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  /* LONG RAW is binary; test it before the character family, whose base name for
   * it would be the bare word "long". */
  if (strncasecmp(typeName, "long raw", 8) == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, EXT_DEFAULT_VARCHAR_LEN + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "nchar") == 0 || strcmp(base, "nvarchar2") == 0 ||
      strcmp(base, "clob") == 0 || strcmp(base, "nclob") == 0 || strcmp(base, "long") == 0 ||
      strcmp(base, "xmltype") == 0) {
    int32_t len = parseTypeLength(typeName);
    if (len <= 0) {
      len = EXT_DEFAULT_VARCHAR_LEN / TSDB_NCHAR_SIZE;
    }
    SET_TD(pTd, TSDB_DATA_TYPE_NCHAR, len * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "rowid") == 0 || strcmp(base, "urowid") == 0) {
    /* ROWID is an 18-character base-64 identifier; read as text. */
    SET_TD(pTd, TSDB_DATA_TYPE_VARCHAR, 40 + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }

  /* Binary family.  RAW is limited to 2000 bytes in Oracle itself. */
  if (strcmp(base, "raw") == 0) {
    int32_t len = parseTypeLength(typeName);
    if (len <= 0) {
      len = 2000;
    }
    SET_TD(pTd, TSDB_DATA_TYPE_VARBINARY, len + VARSTR_HEADER_SIZE);
    return TSDB_CODE_SUCCESS;
  }
  if (strcmp(base, "blob") == 0 || strcmp(base, "bfile") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BLOB, TSDB_MAX_BLOB_LEN);
    return TSDB_CODE_SUCCESS;
  }

  /* Temporal family.  DATE carries a time of day at second precision, and both
   * the WITH TIME ZONE and the WITH LOCAL TIME ZONE forms are aware types; the
   * connector distinguishes them by the full type name (DS 5.3.7). */
  if (strcmp(base, "date") == 0 || strcmp(base, "timestamp") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_TIMESTAMP, 8);
    return TSDB_CODE_SUCCESS;
  }

  /* 26ai native boolean.  19c has no SQL-level boolean, so no existing column
   * can be affected by this mapping. */
  if (strcmp(base, "boolean") == 0) {
    SET_TD(pTd, TSDB_DATA_TYPE_BOOL, 1);
    return TSDB_CODE_SUCCESS;
  }

  /* Deliberately unsupported: intervals, object types, collections, REFs,
   * ANYDATA and spatial types.  ODBC cannot return them as column values, so
   * they are not mappable rather than serialised (DS 5.3.2.1). */
  qError("oracleTypeMap: type not mappable to TDengine: '%s'", typeName);
  return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------
int32_t extTypeNameToTDengineType(EExtSourceType srcType, const char *extTypeName,
                                   const char *extCharsetName, SDataType *pTdType) {
  if (!extTypeName || !pTdType) {
    qError("extTypeNameToTDengineType: invalid param, extTypeName:%p pTdType:%p", extTypeName, pTdType);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  switch (srcType) {
    case EXT_SOURCE_MYSQL:
      code = mysqlTypeMap(extTypeName, extCharsetName, pTdType);
      break;
    case EXT_SOURCE_POSTGRESQL:
      code = pgTypeMap(extTypeName, extCharsetName, pTdType);
      break;
    case EXT_SOURCE_INFLUXDB:
      code = influxTypeMap(extTypeName, pTdType);  // charset unused; Utf8 always maps to NCHAR
      break;
    case EXT_SOURCE_SQLSERVER:
      code = sqlserverTypeMap(extTypeName, extCharsetName, pTdType);
      break;
    case EXT_SOURCE_ORACLE:
      code = oracleTypeMap(extTypeName, extCharsetName, pTdType);
      break;
    default:
      qError("extTypeNameToTDengineType: unknown source type %d for type '%s'", srcType, extTypeName);
      return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
  }

  if (code == TSDB_CODE_SUCCESS) {
    normalizeExternalResultType(pTdType);
  }
  return code;
}

int8_t extTimestampTypePrecision(EExtSourceType srcType, const char *extTypeName,
                                  int8_t fallbackPrecision) {
  switch (srcType) {
    case EXT_SOURCE_MYSQL:
      if (extTypeName != NULL &&
          (typeHasTokenPrefix(extTypeName, "datetime") || typeHasTokenPrefix(extTypeName, "timestamp") ||
           typeHasTokenPrefix(extTypeName, "date"))) {
        return parseTypeLength(extTypeName) > 3 ? TSDB_TIME_PRECISION_MICRO : TSDB_TIME_PRECISION_MILLI;
      }
      break;
    case EXT_SOURCE_POSTGRESQL:
      if (extTypeName != NULL && typeHasTokenPrefix(extTypeName, "timestamp")) {
        const char* pParen = strchr(extTypeName, '(');
        return pParen != NULL && parseTypeLength(extTypeName) <= 3 ? TSDB_TIME_PRECISION_MILLI
                                                                   : TSDB_TIME_PRECISION_MICRO;
      }
      break;
    case EXT_SOURCE_INFLUXDB:
      return TSDB_TIME_PRECISION_NANO;
    case EXT_SOURCE_SQLSERVER:
      // datetime2/datetimeoffset declare their own fractional-seconds precision
      // (0..7, 7 when the modifier is omitted).  A declaration with at most 3
      // fractional digits is millisecond aligned, so it can back a millisecond
      // virtual table; anything finer keeps the microsecond transport precision
      // the connector reads at (DS 5.3.7).  `datetime` also stays microsecond:
      // its 1/300 s ticks are not millisecond exact.  smalldatetime/date are
      // minute/day aligned and therefore millisecond safe.
      if (extTypeName != NULL) {
        if (typeHasTokenPrefix(extTypeName, "datetime2") || typeHasTokenPrefix(extTypeName, "datetimeoffset")) {
          const char* pParen = strchr(extTypeName, '(');
          return pParen != NULL && parseTypeLength(extTypeName) <= 3 ? TSDB_TIME_PRECISION_MILLI
                                                                     : TSDB_TIME_PRECISION_MICRO;
        }
        if (typeHasTokenPrefix(extTypeName, "smalldatetime") || typeHasTokenPrefix(extTypeName, "date")) {
          return TSDB_TIME_PRECISION_MILLI;
        }
      }
      return TSDB_TIME_PRECISION_MICRO;
    case EXT_SOURCE_ORACLE:
      // TIMESTAMP(p) declares its own fractional-second precision (0..9, 6
      // when the modifier is omitted), so the column - not the table's
      // transport precision - decides what a virtual table may declare.  A
      // declaration with at most 3 fractional digits is millisecond aligned
      // and can back a millisecond virtual table; 4..6 stays microsecond and
      // 7..9 nanosecond.  DATE holds whole seconds and is therefore
      // millisecond safe as well.  Returning microsecond for everything made
      // a millisecond virtual table over DATE / TIMESTAMP(p <= 3) fail the
      // precision check with 0x268f, the same defect the SQL Server branch
      // had and fixed in the same way.
      if (extTypeName != NULL && typeHasTokenPrefix(extTypeName, "timestamp")) {
        const char* pParen = strchr(extTypeName, '(');
        int32_t     fracDigits = (pParen != NULL) ? parseTypeLength(extTypeName) : 6;
        if (fracDigits >= 7) {
          return TSDB_TIME_PRECISION_NANO;
        }
        return fracDigits <= 3 ? TSDB_TIME_PRECISION_MILLI : TSDB_TIME_PRECISION_MICRO;
      }
      if (extTypeName != NULL && typeHasTokenPrefix(extTypeName, "date")) {
        return TSDB_TIME_PRECISION_MILLI;
      }
      break;
    default:
      break;
  }

  return fallbackPrecision >= TSDB_TIME_PRECISION_MILLI && fallbackPrecision <= TSDB_TIME_PRECISION_NANO
             ? fallbackPrecision
             : TSDB_TIME_PRECISION_MILLI;
}

/* Oracle's SQL parser folds an unquoted identifier to upper case before it
 * looks the name up, while a name written in double quotes is looked up
 * exactly and can therefore be a different object (DS 5.3.6 R1).
 *
 * TDengine has already applied its own rule by the time a reference gets here:
 * an unquoted identifier was lower-cased, a quoted one keeps the spelling the
 * user wrote.  An upper-case letter in refName therefore proves the reference
 * was quoted, and only the all-lower-case form can be an unquoted reference -
 * the one form Oracle folds to upper case.  Folding every reference would make
 * the quoted "Val" resolve onto the column VAL, so a filter that names both
 * VAL and "Val" would look like one column named twice, and a stored filter
 * over "Val" would silently follow a rename of VAL.  Callers that can see both
 * a folded and an exact candidate still try the exact spelling first -
 * findExternalFilterColumn() does. */
static bool oracleIdentMatches(const char *storedName, const char *refName) {
  if (NULL == storedName || NULL == refName) {
    return false;
  }
  if (0 == strcmp(storedName, refName)) {
    return true;
  }
  char    folded[TSDB_COL_NAME_LEN];
  int32_t i = 0;
  for (; refName[i] != '\0' && i < (int32_t)sizeof(folded) - 1; ++i) {
    char c = refName[i];
    if (c >= 'A' && c <= 'Z') {
      return false;
    }
    folded[i] = (c >= 'a' && c <= 'z') ? (char)(c - 'a' + 'A') : c;
  }
  folded[i] = '\0';
  return 0 == strcmp(storedName, folded);
}

bool extColumnNameEqual(EExtSourceType srcType, const char *lhs, const char *rhs) {
  // MySQL and SQL Server expose case-insensitive catalogs by default, so name
  // variants fold onto the stored column and the resolved column echoes the
  // stored case; PostgreSQL and InfluxDB compare identifiers literally.
  // Oracle folds an unquoted reference to upper case and then compares exactly,
  // so a stored "VAL" matches "val" and "VAL" but not "Val".
  if (lhs == NULL || rhs == NULL) {
    return lhs == rhs;
  }
  if (srcType == EXT_SOURCE_ORACLE) {
    return oracleIdentMatches(lhs, rhs);
  }
  bool caseInsensitive = (srcType == EXT_SOURCE_MYSQL || srcType == EXT_SOURCE_SQLSERVER);
  return caseInsensitive ? strcasecmp(lhs, rhs) == 0 : strcmp(lhs, rhs) == 0;
}

bool extTimestampTypeHasSubMicroFraction(EExtSourceType srcType, const char *extTypeName) {
  // Only SQL Server can declare a timestamp finer than the microsecond the
  // connectors transport (DS 5.3.7): MySQL/PostgreSQL stop at 6 fractional
  // digits, Oracle reads TIMESTAMP(7..9) at its native nanosecond resolution
  // and InfluxDB is read at its native nanosecond resolution too, so none of
  // them loses the sub-microsecond digits.
  if (srcType != EXT_SOURCE_SQLSERVER || extTypeName == NULL) {
    return false;
  }
  if (!typeHasTokenPrefix(extTypeName, "datetime2") && !typeHasTokenPrefix(extTypeName, "datetimeoffset")) {
    return false;
  }
  // An omitted modifier means the default precision of 7.
  const char *pParen = strchr(extTypeName, '(');
  int32_t     digits = (pParen != NULL) ? parseTypeLength(extTypeName) : 7;
  return digits > 6;
}

int8_t extTimestampTypeTzClass(EExtSourceType srcType, const char *extTypeName) {
  if (extTypeName == NULL || extTypeName[0] == '\0') {
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  switch (srcType) {
    case EXT_SOURCE_MYSQL:
      if (typeFamilyStartsWith(extTypeName, "timestamp")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (typeFamilyStartsWith(extTypeName, "datetime") || typeFamilyStartsWith(extTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SOURCE_POSTGRESQL:
      // "timestamp(6) with time zone" also starts with "timestamp", so the
      // aware forms must be tested first.
      if (typeFamilyStartsWith(extTypeName, "timestamptz") ||
          taosStrCaseStr(extTypeName, "with time zone") != NULL) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (typeFamilyStartsWith(extTypeName, "timestamp") || typeFamilyStartsWith(extTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SOURCE_SQLSERVER:
      // DS 5.3.7: datetimeoffset carries an absolute instant (tz-aware), the
      // other temporal types store the client wall clock (tz-naive).
      // "datetimeoffset" must be tested before the "datetime" prefix, which
      // also covers datetime2.
      if (typeFamilyStartsWith(extTypeName, "datetimeoffset")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (typeFamilyStartsWith(extTypeName, "datetime") || typeFamilyStartsWith(extTypeName, "smalldatetime") ||
          typeFamilyStartsWith(extTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SOURCE_ORACLE:
      // DS 5.3.7: the WITH [LOCAL] TIME ZONE forms carry an absolute instant;
      // DATE and the bare TIMESTAMP(n) hold the client wall clock.  The aware
      // forms must be tested first - "timestamp(6) with time zone" also starts
      // with "timestamp".
      if (taosStrCaseStr(extTypeName, "with local time zone") != NULL ||
          taosStrCaseStr(extTypeName, "with time zone") != NULL) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (typeFamilyStartsWith(extTypeName, "timestamp") || typeFamilyStartsWith(extTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SOURCE_INFLUXDB:
      if (typeFamilyStartsWith(extTypeName, "timestamp")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (typeFamilyStartsWith(extTypeName, "date32") || typeFamilyStartsWith(extTypeName, "date64")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    default:
      break;
  }

  return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
}
