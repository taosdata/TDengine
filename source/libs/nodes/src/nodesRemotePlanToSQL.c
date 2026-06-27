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

// nodesRemotePlanToSQL.c — converts a federated-query physical plan (or fallback) to remote SQL.
//
// DS §5.2.6 mandates that this function lives in the `nodes` module so that
// both the Connector (Module B) and the Executor (Module F) call the exact same
// code path.  The EXPLAIN output therefore matches the SQL actually sent to the
// remote database.
//
// All internal rendering uses SDynSQL — a growable heap buffer — so the
// generated SQL has no fixed-length limit.

#include "nodes.h"
#include "os.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taoserror.h"
#include "osMemory.h"
#include "tdef.h"    // TSDB_DATA_TYPE_*
#include "thash.h"  // taosHashIterate / taosHashGetKey
#include "tlog.h"   // qDebug / qError

static const char* dialectName(EExtSQLDialect d) {
  switch (d) {
    case EXT_SQL_DIALECT_MYSQL:    return "MySQL";
    case EXT_SQL_DIALECT_POSTGRES: return "PG";
    case EXT_SQL_DIALECT_INFLUXQL: return "InfluxDB";
    default:                      return "Unknown";
  }
}

// ---------------------------------------------------------------------------
// SDynSQL — growable SQL string buffer (no fixed size limit)
// ---------------------------------------------------------------------------
typedef struct {
  char*       buf;     // heap-allocated; NULL until first grow
  int32_t     pos;     // bytes written so far
  int32_t     cap;     // current capacity
  int32_t     err;     // first error code encountered (0 = OK)
  // DS §5.2.6: client timezone name for timestamp→calendar conversion.
  // Pointer into the SNodesRemoteSQLCtx.tzName string; NOT owned by SDynSQL.
  // NULL means "use server global default timezone" (legacy behaviour).
  const char* tzName;
} SDynSQL;

#define DYN_SQL_INIT_CAP 512

static void dynSQLInit(SDynSQL* s) {
  s->buf    = NULL;
  s->pos    = 0;
  s->cap    = 0;
  s->err    = 0;
  s->tzName = NULL;
}

static void dynSQLFree(SDynSQL* s) {
  taosMemoryFree(s->buf);
  s->buf = NULL;
  s->pos = 0;
  s->cap = 0;
}

// Ensure at least `extra` bytes of free space are available.
static void dynSQLEnsure(SDynSQL* s, int32_t extra) {
  if (s->err) return;
  int32_t needed = s->pos + extra + 1;  // +1 for null terminator
  if (needed <= s->cap) return;
  int32_t newCap = s->cap < DYN_SQL_INIT_CAP ? DYN_SQL_INIT_CAP : s->cap;
  while (newCap < needed) newCap *= 2;
  char* tmp = (char*)taosMemoryRealloc(s->buf, newCap);
  if (!tmp) {
    s->err = terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    return;
  }
  s->buf = tmp;
  s->cap = newCap;
}

// Append a single character.
static void dynSQLAppendChar(SDynSQL* s, char c) {
  dynSQLEnsure(s, 1);
  if (s->err) return;
  s->buf[s->pos++] = c;
}

// Append a string of known length.
static void dynSQLAppendLen(SDynSQL* s, const char* str, int32_t len) {
  if (len <= 0) return;
  dynSQLEnsure(s, len);
  if (s->err) return;
  memcpy(s->buf + s->pos, str, len);
  s->pos += len;
}

// Append a NUL-terminated string.
static void dynSQLAppendStr(SDynSQL* s, const char* str) {
  if (str) dynSQLAppendLen(s, str, (int32_t)strlen(str));
}

// Append a formatted string (varargs snprintf).
static void dynSQLAppendf(SDynSQL* s, const char* fmt, ...) {
  if (s->err) return;
  va_list args;
  // First: measure
  va_start(args, fmt);
  int32_t needed = (int32_t)vsnprintf(NULL, 0, fmt, args);
  va_end(args);
  if (needed <= 0) return;
  // Grow if needed
  dynSQLEnsure(s, needed);
  if (s->err) return;
  // Write
  va_start(args, fmt);
  (void)vsnprintf(s->buf + s->pos, (size_t)(needed + 1), fmt, args);
  va_end(args);
  s->pos += needed;
}

// Detach the buffer from SDynSQL (caller takes ownership; NUL-termination guaranteed).
// Returns NULL if an error occurred (s->err != 0).
static char* dynSQLDetach(SDynSQL* s) {
  if (s->err) {
    dynSQLFree(s);
    return NULL;
  }
  dynSQLEnsure(s, 0);  // ensure buf is allocated even for empty SQL
  if (s->err) return NULL;
  s->buf[s->pos] = '\0';
  char* result = s->buf;
  s->buf = NULL;
  s->pos = 0;
  s->cap = 0;
  return result;
}

// ---------------------------------------------------------------------------
// Internal rendering helpers — all write to SDynSQL*
// ---------------------------------------------------------------------------

static void dynAppendQuotedId(SDynSQL* s, const char* name, EExtSQLDialect dialect) {
  char q = (dialect == EXT_SQL_DIALECT_MYSQL) ? '`' : '"';
  size_t len = strlen(name);
  // Strip surrounding backticks or double-quotes if the identifier is already quoted,
  // to avoid double-quoting (e.g. when TDengine parser preserves backticks verbatim).
  if (len >= 2 && (name[0] == '`' || name[0] == '"') && name[len-1] == name[0]) {
    dynSQLAppendChar(s, q);
    dynSQLAppendLen(s, name + 1, (int32_t)(len - 2));
    dynSQLAppendChar(s, q);
  } else {
    dynSQLAppendChar(s, q);
    dynSQLAppendStr(s, name);
    dynSQLAppendChar(s, q);
  }
}

static void dynAppendTablePath(SDynSQL* s, const SExtTableNode* pExtTable, EExtSQLDialect dialect) {
  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL: {
      // `database`.`table`
      // Prefer table.dbName (set by parser); fall back to srcDatabase (always populated).
      const char* dbToUse = pExtTable->table.dbName[0] ? pExtTable->table.dbName : pExtTable->srcDatabase;
      if (dbToUse && dbToUse[0]) {
        dynAppendQuotedId(s, dbToUse, dialect);
        dynSQLAppendChar(s, '.');
      }
      // Use the actual remote table name (preserving original case) if available.
      // remoteTableName is serialized and populated from catalog metadata.
      const char* tblToUse = pExtTable->remoteTableName[0]
                               ? pExtTable->remoteTableName
                               : ((pExtTable->pExtMeta && pExtTable->pExtMeta->remoteTableName[0])
                                   ? pExtTable->pExtMeta->remoteTableName
                                   : pExtTable->table.tableName);
      dynAppendQuotedId(s, tblToUse, dialect);
      break;
    }
      break;
    case EXT_SQL_DIALECT_POSTGRES:
      // "schema"."table"
      if (pExtTable->schemaName[0]) {
        dynAppendQuotedId(s, pExtTable->schemaName, dialect);
        dynSQLAppendChar(s, '.');
      }
      dynAppendQuotedId(s, pExtTable->table.tableName, dialect);
      break;
    case EXT_SQL_DIALECT_INFLUXQL:
    default:
      // "measurement"
      dynAppendQuotedId(s, pExtTable->table.tableName, dialect);
      break;
  }
}

// Append a SQL-escaped string literal.
// Single quotes → double single-quotes; MySQL also escapes backslashes.
static void dynAppendEscapedString(SDynSQL* s, const char* str, EExtSQLDialect dialect) {
  dynSQLAppendChar(s, '\'');
  for (const char* p = str; *p; p++) {
    if (*p == '\'') {
      dynSQLAppendChar(s, '\'');
      dynSQLAppendChar(s, '\'');
    } else if (*p == '\\' && dialect == EXT_SQL_DIALECT_MYSQL) {
      dynSQLAppendChar(s, '\\');
      dynSQLAppendChar(s, '\\');
    } else {
      dynSQLAppendChar(s, *p);
    }
  }
  dynSQLAppendChar(s, '\'');
}

static void dynAppendValueLiteral(SDynSQL* s, const SValueNode* pVal, EExtSQLDialect dialect) {
  if (pVal->isNull) {
    dynSQLAppendStr(s, "NULL");
    return;
  }
  // Raw SQL fragment — emit datum.p verbatim without quoting or escaping.
  // datum.p is a TDengine varstring (uint16_t length prefix + char content).
  if (pVal->flag & VALUE_FLAG_RAW_SQL_FRAG) {
    if (pVal->datum.p) dynSQLAppendStr(s, varDataVal(pVal->datum.p));
    return;
  }
  switch (pVal->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      dynSQLAppendStr(s, pVal->datum.b ? "TRUE" : "FALSE");
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      dynSQLAppendf(s, "%" PRId64, pVal->datum.i);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      dynSQLAppendf(s, "%" PRIu64, pVal->datum.u);
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      char numBuf[64] = {0};
      (void)snprintf(numBuf, sizeof(numBuf), "%.17g", pVal->datum.d);
      dynSQLAppendStr(s, numBuf);
      // Keep floating literals as floating tokens (e.g. 4.0, 2.0) so remote
      // engines don't downgrade division/mod semantics to integer arithmetic.
      if (NULL == strchr(numBuf, '.') && NULL == strchr(numBuf, 'e') && NULL == strchr(numBuf, 'E')) {
        dynSQLAppendStr(s, ".0");
      }
      break;
    }
    case TSDB_DATA_TYPE_BINARY:   // TSDB_DATA_TYPE_VARCHAR has the same integer value
      // datum.p is VARSTR format (2-byte length header + UTF-8 content, null-terminated).
      // varDataVal() skips the header to return the actual string content.
      // Fall back to literal when datum.p is NULL (untranslated value nodes, e.g. tag conditions).
      if (pVal->datum.p) {
        dynAppendEscapedString(s, varDataVal(pVal->datum.p), dialect);
      } else {
        dynAppendEscapedString(s, pVal->literal ? pVal->literal : "", dialect);
      }
      break;
    case TSDB_DATA_TYPE_NCHAR:
      // datum.p is VARSTR (UCS-4 encoded, 4 bytes/char).  Use the original
      // source literal (UTF-8) for the remote SQL string literal instead, so
      // the external DB receives a valid UTF-8 comparison value.
      dynAppendEscapedString(s, pVal->literal ? pVal->literal : "", dialect);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      // Convert TDengine ms/us/ns timestamp to ISO-8601 string literal.
      // All external databases (MySQL, PG, InfluxDB) accept 'YYYY-MM-DD HH:MM:SS.fff'.
      int64_t ts   = pVal->datum.i;
      int64_t ms;
      switch (pVal->node.resType.precision) {
        case TSDB_TIME_PRECISION_MICRO: ms = ts / 1000LL;       break;
        case TSDB_TIME_PRECISION_NANO:  ms = ts / 1000000LL;    break;
        default:                        ms = ts;                 break;  // MILLI
      }
      time_t    sec  = (time_t)(ms / 1000LL);
      int32_t   frac = (int32_t)(ms % 1000LL);
      if (frac < 0) { frac += 1000; sec -= 1; }
      struct tm tmBuf;
      // DS §5.2.6: MySQL DATETIME and PG TIMESTAMP (without tz) are timezone-naive;
      // format the epoch using the client timezone so that filter values match the
      // external DB's interpretation.  InfluxDB stores epoch UTC — always use UTC.
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        taosGmTimeR(&sec, &tmBuf);
      } else {
        // Use client timezone when available; fall back to server global default.
        timezone_t tz = (s->tzName && s->tzName[0]) ? tzalloc(s->tzName) : NULL;
        taosLocalTime(&sec, &tmBuf, NULL, 0, tz);
        if (tz) tzfree(tz);
      }
      dynSQLAppendf(s, "'%04d-%02d-%02d %02d:%02d:%02d.%03d'",
                    tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
                    tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, frac);
      qDebug("FQ-SQL: val ts->iso dialect=%s tz=%s epoch=%" PRId64 " prec=%d -> '%04d-%02d-%02d %02d:%02d:%02d.%03d'",
             dialectName(dialect), s->tzName ? s->tzName : "(global)",
             ts, (int)pVal->node.resType.precision,
             tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
             tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, frac);
      break;
    }
    default:
      s->err = TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      break;
  }
}

// ---------------------------------------------------------------------------
// resolveExtColName — map TDengine column name to the remote source column name.
// If pExtMeta is NULL or the column has no remoteColName, returns tdColName as-is.
// ---------------------------------------------------------------------------
static const char* resolveExtColName(const SExtTableMeta* pExtMeta, const char* tdColName) {
  if (!pExtMeta) return tdColName;
  for (int32_t i = 0; i < pExtMeta->numOfCols; i++) {
    if (strcasecmp(pExtMeta->pCols[i].colName, tdColName) == 0 &&
        pExtMeta->pCols[i].remoteColName[0] != '\0') {
      return pExtMeta->pCols[i].remoteColName;
    }
  }
  return tdColName;
}

// Forward declaration for mutual recursion
static int32_t dynAppendExpr(SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect,
                              const SExtTableMeta* pExtMeta,
                              const SNodesRemoteSQLCtx* pCtx);

// Render an integer value as an ISO-8601 timestamp string literal.
// Used when an integer value is compared against a TIMESTAMP column in a WHERE clause.
static void dynAppendIntAsTimestamp(SDynSQL* s, int64_t ts, int8_t precision,
                                   EExtSQLDialect dialect) {
  int64_t ms;
  switch (precision) {
    case TSDB_TIME_PRECISION_MICRO: ms = ts / 1000LL;    break;
    case TSDB_TIME_PRECISION_NANO:  ms = ts / 1000000LL; break;
    default:                        ms = ts;             break;
  }
  time_t    sec  = (time_t)(ms / 1000LL);
  int32_t   frac = (int32_t)(ms % 1000LL);
  if (frac < 0) { frac += 1000; sec -= 1; }
  struct tm tmBuf;
  // DS §5.2.6: same timezone logic as dynAppendValueLiteral TIMESTAMP case.
  if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
    taosGmTimeR(&sec, &tmBuf);
  } else {
    timezone_t tz = (s->tzName && s->tzName[0]) ? tzalloc(s->tzName) : NULL;
    taosLocalTime(&sec, &tmBuf, NULL, 0, tz);
    if (tz) tzfree(tz);
  }
  dynSQLAppendf(s, "'%04d-%02d-%02d %02d:%02d:%02d.%03d'",
                tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
                tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, frac);
  qDebug("FQ-SQL: int->ts dialect=%s tz=%s epoch=%" PRId64 " prec=%d -> '%04d-%02d-%02d %02d:%02d:%02d.%03d'",
         dialectName(dialect), s->tzName ? s->tzName : "(global)",
         ts, (int)precision,
         tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
         tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, frac);
}

// Check if pNode is an integer value that should be rendered as ISO timestamp
// because it is being compared to a TIMESTAMP-typed counterpart pOther.
static bool isIntValueForTimestamp(const SNode* pNode, const SNode* pOther) {
  if (nodeType(pNode) != QUERY_NODE_VALUE || !pOther) return false;
  const SValueNode* pVal = (const SValueNode*)pNode;
  if (pVal->isNull) return false;
  uint8_t vt = pVal->node.resType.type;
  if (vt != TSDB_DATA_TYPE_TINYINT && vt != TSDB_DATA_TYPE_SMALLINT &&
      vt != TSDB_DATA_TYPE_INT     && vt != TSDB_DATA_TYPE_BIGINT)
    return false;
  uint8_t ot = ((const SExprNode*)pOther)->resType.type;
  return (ot == TSDB_DATA_TYPE_TIMESTAMP);
}

// Render an expression, converting integer values to ISO timestamp if needed.
static int32_t dynAppendExprTS(SDynSQL* s, const SNode* pExpr, const SNode* pOther,
                               EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                               const SNodesRemoteSQLCtx* pCtx) {
  if (isIntValueForTimestamp(pExpr, pOther)) {
    const SValueNode* pVal = (const SValueNode*)pExpr;
    int8_t prec = ((const SExprNode*)pOther)->resType.precision;
    if (prec == 0) prec = TSDB_TIME_PRECISION_MILLI;
    dynAppendIntAsTimestamp(s, pVal->datum.i, prec, dialect);
    return TSDB_CODE_SUCCESS;
  }
  return dynAppendExpr(s, pExpr, dialect, pExtMeta, pCtx);
}

// ---------------------------------------------------------------------------
// dynAppendRemoteValueList — resolve REMOTE_VALUE_LIST and emit IN (...) list
// ---------------------------------------------------------------------------
static int32_t dynAppendRemoteListLiteral(SDynSQL* s, const SRemoteValueListNode* pRemote,
                                          const void* pKey, EExtSQLDialect dialect,
                                          int8_t tsPrecision) {
  switch (pRemote->filterValueType) {
    case TSDB_DATA_TYPE_BOOL:
      dynSQLAppendStr(s, (*(int8_t*)pKey) ? "TRUE" : "FALSE");
      break;
    case TSDB_DATA_TYPE_TINYINT:
      dynSQLAppendf(s, "%" PRId64, (int64_t)(*(int8_t*)pKey));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      dynSQLAppendf(s, "%" PRId64, (int64_t)(*(int16_t*)pKey));
      break;
    case TSDB_DATA_TYPE_INT:
      dynSQLAppendf(s, "%" PRId64, (int64_t)(*(int32_t*)pKey));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      dynSQLAppendf(s, "%" PRId64, *(int64_t*)pKey);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int8_t prec = tsPrecision;
      if (prec != TSDB_TIME_PRECISION_MILLI &&
          prec != TSDB_TIME_PRECISION_MICRO &&
          prec != TSDB_TIME_PRECISION_NANO) {
        prec = TSDB_TIME_PRECISION_MILLI;
      }
      dynAppendIntAsTimestamp(s, *(int64_t*)pKey, prec, dialect);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT:
      dynSQLAppendf(s, "%" PRIu64, (uint64_t)(*(uint8_t*)pKey));
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      dynSQLAppendf(s, "%" PRIu64, (uint64_t)(*(uint16_t*)pKey));
      break;
    case TSDB_DATA_TYPE_UINT:
      dynSQLAppendf(s, "%" PRIu64, (uint64_t)(*(uint32_t*)pKey));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      dynSQLAppendf(s, "%" PRIu64, *(uint64_t*)pKey);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      dynSQLAppendf(s, "%.9g", (double)(*(float*)pKey));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      dynSQLAppendf(s, "%.17g", *(double*)pKey);
      break;
    case TSDB_DATA_TYPE_BINARY:   // VARCHAR
    case TSDB_DATA_TYPE_NCHAR:
      // Key is a NUL-terminated string stored inline in the hash.
      dynAppendEscapedString(s, (const char*)pKey, dialect);
      break;
    default:
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendRemoteValueList(SDynSQL* s, const SRemoteValueListNode* pRemote,
                                         EExtSQLDialect dialect,
                                         const SNodesRemoteSQLCtx* pCtx,
                                         int8_t tsPrecision) {
  if (!pCtx || !pCtx->fp) {
    // No resolve context — cannot expand; caller will skip WHERE clause.
    qError("FQ-SQL: dynAppendRemoteValueList no resolve ctx (pCtx=%p)", pCtx);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  // Call the executor callback to fill pRemote->pHashFilter with actual values.
  int32_t code = pCtx->fp(pCtx->pCtx, pRemote->subQIdx, (SNode*)pRemote);
  if (code != TSDB_CODE_SUCCESS) return code;

  // Empty set — "col IN ()" is invalid SQL; emit FALSE instead.
  if (!pRemote->pHashFilter || taosHashGetSize(pRemote->pHashFilter) == 0) {
    dynSQLAppendStr(s, "FALSE");
    return TSDB_CODE_SUCCESS;
  }

  dynSQLAppendChar(s, '(');
  bool first = true;
  void* pVal = taosHashIterate(pRemote->pHashFilter, NULL);
  while (pVal != NULL) {
    if (!first) dynSQLAppendStr(s, ", ");
    first = false;

    size_t keyLen = 0;
    const void* pKey = taosHashGetKey(pVal, &keyLen);

    // Emit a SQL literal based on the value type stored in the hash key.
    int32_t litCode = dynAppendRemoteListLiteral(s, pRemote, pKey, dialect, tsPrecision);
    if (litCode != TSDB_CODE_SUCCESS) {
      // Unsupported type — stop iteration and report error.
      qError("FQ-SQL: dynAppendRemoteValueList unsupported hash key type=%d", pRemote->filterValueType);
      taosHashCancelIterate(pRemote->pHashFilter, pVal);
      return litCode;
    }

    pVal = taosHashIterate(pRemote->pHashFilter, pVal);
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

// InfluxDB does not reliably accept "time IN (...)" / "time NOT IN (...)".
// Rewrite REMOTE_VALUE_LIST predicates into OR/AND chains:
//   col IN (v1,v2,...)      -> (col=v1 OR col=v2 ...)
//   col NOT IN (v1,v2,...)  -> (col!=v1 AND col!=v2 ...)
static int32_t dynAppendInfluxRemoteInChain(SDynSQL* s, const SOperatorNode* pOp,
                                             const SRemoteValueListNode* pRemote,
                                             bool notIn,
                                             EExtSQLDialect dialect,
                                             const SExtTableMeta* pExtMeta,
                                             const SNodesRemoteSQLCtx* pCtx) {
  if (!pCtx || !pCtx->fp) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  int32_t code = pCtx->fp(pCtx->pCtx, pRemote->subQIdx, (SNode*)pRemote);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t sz = pRemote->pHashFilter ? taosHashGetSize(pRemote->pHashFilter) : 0;
  if (sz <= 0) {
    dynSQLAppendStr(s, notIn ? "(1=1)" : "(1=0)");
    return TSDB_CODE_SUCCESS;
  }

  int8_t leftPrec = ((const SExprNode*)pOp->pLeft)->resType.precision;
  dynSQLAppendChar(s, '(');

  bool first = true;
  void* pVal = taosHashIterate(pRemote->pHashFilter, NULL);
  while (pVal != NULL) {
    if (!first) {
      dynSQLAppendStr(s, notIn ? " AND " : " OR ");
    }
    first = false;

    dynSQLAppendChar(s, '(');
    code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pRemote->pHashFilter, pVal);
      return code;
    }
    dynSQLAppendStr(s, notIn ? " != " : " = ");

    size_t keyLen = 0;
    const void* pKey = taosHashGetKey(pVal, &keyLen);
    code = dynAppendRemoteListLiteral(s, pRemote, pKey, dialect, leftPrec);
    if (code != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pRemote->pHashFilter, pVal);
      return code;
    }
    dynSQLAppendChar(s, ')');

    pVal = taosHashIterate(pRemote->pHashFilter, pVal);
  }

  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

// Influx compatibility rewrite for constant IN/NOT IN lists:
//   col IN (v1,v2,...)      -> (col=v1 OR col=v2 ...)
//   col NOT IN (v1,v2,...)  -> (col!=v1 AND col!=v2 ...)
static int32_t dynAppendInfluxNodeInChain(SDynSQL* s, const SOperatorNode* pOp,
                                          const SNodeListNode* pList,
                                          bool notIn,
                                          EExtSQLDialect dialect,
                                          const SExtTableMeta* pExtMeta,
                                          const SNodesRemoteSQLCtx* pCtx) {
  if (!pList->pNodeList || LIST_LENGTH(pList->pNodeList) == 0) {
    dynSQLAppendStr(s, notIn ? "(1=1)" : "(1=0)");
    return TSDB_CODE_SUCCESS;
  }

  dynSQLAppendChar(s, '(');

  bool first = true;
  SNode* pItem = NULL;
  FOREACH(pItem, pList->pNodeList) {
    if (!first) {
      dynSQLAppendStr(s, notIn ? " AND " : " OR ");
    }
    first = false;

    dynSQLAppendChar(s, '(');
    int32_t code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendStr(s, notIn ? " != " : " = ");
    code = dynAppendExpr(s, pItem, dialect, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendChar(s, ')');
  }

  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// dynAppendNodeList — render constant value list as "(v1, v2, ...)" for IN/NOT IN
// ---------------------------------------------------------------------------
static int32_t dynAppendNodeList(SDynSQL* s, const SNodeListNode* pList, EExtSQLDialect dialect,
                                  const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  if (!pList->pNodeList || LIST_LENGTH(pList->pNodeList) == 0) {
    dynSQLAppendStr(s, "(NULL)");
    return TSDB_CODE_SUCCESS;
  }
  dynSQLAppendChar(s, '(');
  bool first = true;
  SNode* pItem = NULL;
  FOREACH(pItem, pList->pNodeList) {
    if (!first) dynSQLAppendStr(s, ", ");
    first = false;
    int32_t code = dynAppendExpr(s, pItem, dialect, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static bool dynIsZeroConstValue(const SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_VALUE != nodeType(pNode)) {
    return false;
  }

  const SValueNode* pVal = (const SValueNode*)pNode;
  if (pVal->isNull) {
    return false;
  }

  switch (pVal->node.resType.type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      return (0 == pVal->datum.i);
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return (0 == pVal->datum.u);
    default:
      return false;
  }
}

static const char* dynGetRemoteZeroRowsSQLHint(const SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_REMOTE_ZERO_ROWS != nodeType(pNode)) {
    return NULL;
  }

  const SRemoteZeroRowsNode* pRows = (const SRemoteZeroRowsNode*)pNode;
  if (NULL == pRows->val.literal || '\0' == pRows->val.literal[0]) {
    return NULL;
  }

  return pRows->val.literal;
}

static int32_t dynAppendOperatorExpr(SDynSQL* s, const SOperatorNode* pOp, EExtSQLDialect dialect,
                                      const SExtTableMeta* pExtMeta,
                                      const SNodesRemoteSQLCtx* pCtx) {
  // Rewritten EXISTS/NOT EXISTS shape:
  //   EXISTS(subq)     -> REMOTE_ZERO_ROWS(subq) > 0
  //   NOT EXISTS(subq) -> REMOTE_ZERO_ROWS(subq) <= 0
  // Parser may preserve the original inner SQL as a hint in pLeft->val.literal.
  // Prefer rendering back to EXISTS SQL for pushdown if the hint is present.
  const char* pExistsHint = dynGetRemoteZeroRowsSQLHint(pOp->pLeft);
  if (pExistsHint && dynIsZeroConstValue(pOp->pRight)) {
    if (pOp->opType == OP_TYPE_GREATER_THAN) {
      dynSQLAppendStr(s, "EXISTS (");
      dynSQLAppendStr(s, pExistsHint);
      dynSQLAppendChar(s, ')');
      return TSDB_CODE_SUCCESS;
    }
    if (pOp->opType == OP_TYPE_LOWER_EQUAL) {
      dynSQLAppendStr(s, "NOT EXISTS (");
      dynSQLAppendStr(s, pExistsHint);
      dynSQLAppendChar(s, ')');
      return TSDB_CODE_SUCCESS;
    }
  }

  const char* opStr = NULL;
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:         opStr = " = ";    break;
    case OP_TYPE_NOT_EQUAL:     opStr = " <> ";   break;
    case OP_TYPE_GREATER_THAN:  opStr = " > ";    break;
    case OP_TYPE_GREATER_EQUAL: opStr = " >= ";   break;
    case OP_TYPE_LOWER_THAN:    opStr = " < ";    break;
    case OP_TYPE_LOWER_EQUAL:   opStr = " <= ";   break;
    case OP_TYPE_LIKE:          opStr = " LIKE ";     break;
    case OP_TYPE_NOT_LIKE:      opStr = " NOT LIKE "; break;
    case OP_TYPE_ADD:           opStr = " + ";        break;
    case OP_TYPE_SUB:           opStr = " - ";        break;
    case OP_TYPE_MULTI:         opStr = " * ";        break;
    case OP_TYPE_DIV:           opStr = " / ";        break;
    case OP_TYPE_REM:           opStr = " % ";        break;
    case OP_TYPE_MINUS:
      // Unary minus: -(expr)
      dynSQLAppendStr(s, "(-(");
      {
        int32_t code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        if (code) return code;
      }
      dynSQLAppendStr(s, "))");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_MATCH:
      // TDengine MATCH → MySQL REGEXP / PostgreSQL ~ / InfluxDB(DataFusion) ~
      (void)dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      if (dialect == EXT_SQL_DIALECT_MYSQL)
        dynSQLAppendStr(s, " REGEXP ");
      else if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL)
        dynSQLAppendStr(s, " ~ ");
      else {
        qError("FQ-SQL: OP_TYPE_MATCH unsupported dialect=%d", dialect);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      (void)dynAppendExpr(s, pOp->pRight, dialect, pExtMeta, pCtx);
      (void)dynSQLAppendChar(s, ')');
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_NMATCH:
      // TDengine NMATCH → MySQL NOT REGEXP / PostgreSQL !~ / InfluxDB(DataFusion) !~
      (void)dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      if (dialect == EXT_SQL_DIALECT_MYSQL)
        dynSQLAppendStr(s, " NOT REGEXP ");
      else if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL)
        dynSQLAppendStr(s, " !~ ");
      else {
        qError("FQ-SQL: OP_TYPE_NMATCH unsupported dialect=%d", dialect);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      (void)dynAppendExpr(s, pOp->pRight, dialect, pExtMeta, pCtx);
      (void)dynSQLAppendChar(s, ')');
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IN:
      // col IN (constant list) or col IN REMOTE_VALUE_LIST(subquery)
      // InfluxDB: rewrite IN to OR-chain; must branch before emitting "col IN"
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_NODE_LIST) {
          return dynAppendInfluxNodeInChain(s, pOp,
                                            (const SNodeListNode*)pOp->pRight,
                                            false, dialect, pExtMeta, pCtx);
        }
        if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_VALUE_LIST) {
          return dynAppendInfluxRemoteInChain(s, pOp,
                                              (const SRemoteValueListNode*)pOp->pRight,
                                              false, dialect, pExtMeta, pCtx);
        }
      }
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IN ");
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_NODE_LIST) {
        return dynAppendNodeList(s, (const SNodeListNode*)pOp->pRight, dialect, pExtMeta, pCtx);
      }
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_VALUE_LIST) {
        int8_t leftPrec = ((const SExprNode*)pOp->pLeft)->resType.precision;
        return dynAppendRemoteValueList(s, (const SRemoteValueListNode*)pOp->pRight,
                                        dialect, pCtx, leftPrec);
      }
      qError("FQ-SQL: OP_TYPE_IN unsupported right node type=%d", pOp->pRight ? nodeType(pOp->pRight) : -1);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    case OP_TYPE_NOT_IN: {
      // col NOT IN (constant list) or col NOT IN REMOTE_VALUE_LIST(subquery)
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_NODE_LIST) {
        if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
          return dynAppendInfluxNodeInChain(s, pOp,
                                            (const SNodeListNode*)pOp->pRight,
                                            true, dialect, pExtMeta, pCtx);
        }
        (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        dynSQLAppendStr(s, " NOT IN ");
        return dynAppendNodeList(s, (const SNodeListNode*)pOp->pRight, dialect, pExtMeta, pCtx);
      }
      if (!pOp->pRight || nodeType(pOp->pRight) != QUERY_NODE_REMOTE_VALUE_LIST) {
        qError("FQ-SQL: OP_TYPE_NOT_IN unsupported right node type=%d", pOp->pRight ? nodeType(pOp->pRight) : -1);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }

      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        // Influx: rewrite NOT IN(list) to AND-chain to avoid dialect syntax issues.
        return dynAppendInfluxRemoteInChain(s, pOp,
                    (const SRemoteValueListNode*)pOp->pRight,
                    true, dialect, pExtMeta, pCtx);
      }

      const SRemoteValueListNode* pRemote = (const SRemoteValueListNode*)pOp->pRight;
      if (!pCtx || !pCtx->fp) {
        qError("FQ-SQL: OP_TYPE_NOT_IN no resolve ctx (pCtx=%p)", pCtx);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      int32_t notInCode = pCtx->fp(pCtx->pCtx, pRemote->subQIdx, (SNode*)pRemote);
      if (notInCode != TSDB_CODE_SUCCESS) return notInCode;
      // Empty set: col NOT IN (empty) is always TRUE — emit (1=1) to pass all rows.
      if (!pRemote->pHashFilter || taosHashGetSize(pRemote->pHashFilter) == 0) {
        dynSQLAppendStr(s, "(1=1)");
        return TSDB_CODE_SUCCESS;
      }
      // Emit: col NOT IN (v1, v2, ...)
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " NOT IN (");
      bool notInFirst = true;
      void* pNotInVal = taosHashIterate(pRemote->pHashFilter, NULL);
      while (pNotInVal != NULL) {
        if (!notInFirst) dynSQLAppendStr(s, ", ");
        notInFirst = false;
        size_t notInKeyLen = 0;
        const void* pNotInKey = taosHashGetKey(pNotInVal, &notInKeyLen);
        int8_t leftPrec = ((const SExprNode*)pOp->pLeft)->resType.precision;
        int32_t litCode = dynAppendRemoteListLiteral(s, pRemote, pNotInKey, dialect, leftPrec);
        if (litCode != TSDB_CODE_SUCCESS) {
          qError("FQ-SQL: OP_TYPE_NOT_IN unsupported hash key type=%d", pRemote->filterValueType);
          taosHashCancelIterate(pRemote->pHashFilter, pNotInVal);
          return litCode;
        }
        pNotInVal = taosHashIterate(pRemote->pHashFilter, pNotInVal);
      }
      dynSQLAppendChar(s, ')');
      return TSDB_CODE_SUCCESS;
    }
    case OP_TYPE_IS_NULL:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS NULL)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_NOT_NULL:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS NOT NULL)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_EXISTS:
      // pLeft must be a VALUE with VALUE_FLAG_RAW_SQL_FRAG set — a pre-rendered
      // EXISTS body SQL string produced by nodesRenderCorrelatedExistsBody.
      if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_VALUE &&
          (((const SValueNode*)pOp->pLeft)->flag & VALUE_FLAG_RAW_SQL_FRAG)) {
        dynSQLAppendStr(s, "EXISTS (");
        dynSQLAppendStr(s, varDataVal(((const SValueNode*)pOp->pLeft)->datum.p));
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }
      // REMOTE_ZERO_ROWS fallback: inner subquery was evaluated by TDengine;
      // emit the resolved row count as a boolean expression (count > 0).
      if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_REMOTE_ZERO_ROWS) {
        dynSQLAppendChar(s, '(');
        int32_t code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " > 0)");
        return TSDB_CODE_SUCCESS;
      }
      qError("FQ-SQL: OP_TYPE_EXISTS unsupported left node type=%d", pOp->pLeft ? nodeType(pOp->pLeft) : -1);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    case OP_TYPE_NOT_EXISTS:
      if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_VALUE &&
          (((const SValueNode*)pOp->pLeft)->flag & VALUE_FLAG_RAW_SQL_FRAG)) {
        dynSQLAppendStr(s, "NOT EXISTS (");
        dynSQLAppendStr(s, varDataVal(((const SValueNode*)pOp->pLeft)->datum.p));
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }
      // REMOTE_ZERO_ROWS fallback: inner subquery was evaluated by TDengine;
      // emit the resolved row count as a boolean expression (count = 0).
      if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_REMOTE_ZERO_ROWS) {
        dynSQLAppendChar(s, '(');
        int32_t code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " = 0)");
        return TSDB_CODE_SUCCESS;
      }
      qError("FQ-SQL: OP_TYPE_NOT_EXISTS unsupported left node type=%d", pOp->pLeft ? nodeType(pOp->pLeft) : -1);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    case OP_TYPE_JSON_CONTAINS:
      // CONTAINS requires a TDengine JSON tag column (TSDB_DATA_TYPE_JSON / STag format).
      // External source columns (MySQL JSON, PG JSONB, InfluxDB string) are all mapped to
      // NCHAR, not the native JSON type, so CONTAINS is rejected at the parser type-check
      // stage (PAR_INVALID_COL_JSON) before reaching here.  This branch is unreachable for
      // external sources; return unsupported defensively.
      qError("FQ-SQL: OP_TYPE_JSON_CONTAINS not supported for external sources");
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    default:
      qError("FQ-SQL: dynAppendOperatorExpr unsupported opType=%d", pOp->opType);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  dynSQLAppendChar(s, '(');
  int32_t code = dynAppendExprTS(s, pOp->pLeft, pOp->pRight, dialect, pExtMeta, pCtx);
  if (code) return code;
  dynSQLAppendStr(s, opStr);
  code = dynAppendExprTS(s, pOp->pRight, pOp->pLeft, dialect, pExtMeta, pCtx);
  if (code) return code;
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendLogicCondition(SDynSQL* s, const SLogicConditionNode* pLogic,
                                        EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                                        const SNodesRemoteSQLCtx* pCtx) {
  // NOT(EXISTS(RAW_SQL_FRAG)) — special-case to emit NOT EXISTS (...) directly.
  if (pLogic->condType == LOGIC_COND_TYPE_NOT &&
      pLogic->pParameterList && LIST_LENGTH(pLogic->pParameterList) == 1) {
    SNode* pInner = (SNode*)pLogic->pParameterList->pHead->pNode;
    if (pInner && nodeType(pInner) == QUERY_NODE_OPERATOR) {
      SOperatorNode* pOp = (SOperatorNode*)pInner;
      if (pOp->opType == OP_TYPE_EXISTS &&
          pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_VALUE &&
          (((const SValueNode*)pOp->pLeft)->flag & VALUE_FLAG_RAW_SQL_FRAG)) {
        dynSQLAppendStr(s, "NOT EXISTS (");
        dynSQLAppendStr(s, varDataVal(((const SValueNode*)pOp->pLeft)->datum.p));
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }
      // NOT(EXISTS(REMOTE_ZERO_ROWS)) — subquery evaluated TDengine-side; emit (count = 0).
      if (pOp->opType == OP_TYPE_EXISTS &&
          pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_REMOTE_ZERO_ROWS) {
        dynSQLAppendChar(s, '(');
        int32_t code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " = 0)");
        return TSDB_CODE_SUCCESS;
      }
    }
    // Generic NOT: emit NOT (expr)
    dynSQLAppendStr(s, "NOT ");
    SNode* pNode = (SNode*)pLogic->pParameterList->pHead->pNode;
    dynSQLAppendChar(s, '(');
    int32_t code = dynAppendExpr(s, pNode, dialect, pExtMeta, pCtx);
    if (code) return code;
    dynSQLAppendChar(s, ')');
    return TSDB_CODE_SUCCESS;
  }
  const char* sep = (pLogic->condType == LOGIC_COND_TYPE_AND) ? " AND " : " OR ";
  bool first = true;
  dynSQLAppendChar(s, '(');
  SNode* pNode = NULL;
  FOREACH(pNode, pLogic->pParameterList) {
    if (!first) dynSQLAppendStr(s, sep);
    int32_t code = dynAppendExpr(s, pNode, dialect, pExtMeta, pCtx);
    if (code) return code;
    first = false;
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendExpr(SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect,
                              const SExtTableMeta* pExtMeta,
                              const SNodesRemoteSQLCtx* pCtx) {
  if (!pExpr) return TSDB_CODE_SUCCESS;
  switch (nodeType(pExpr)) {
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pExpr;
      // When includeTableName is requested (e.g. for correlated EXISTS body),
      // prefix the column with its table name so the generated SQL is
      // self-contained when embedded as a subquery.
      if (pCtx && pCtx->includeTableName && pCol->tableName[0] != '\0') {
        dynAppendQuotedId(s, pCol->tableName, dialect);
        dynSQLAppendChar(s, '.');
      }
      dynAppendQuotedId(s, resolveExtColName(pExtMeta, pCol->colName), dialect);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_VALUE:
      dynAppendValueLiteral(s, (const SValueNode*)pExpr, dialect);
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_OPERATOR:
      return dynAppendOperatorExpr(s, (const SOperatorNode*)pExpr, dialect, pExtMeta, pCtx);
    case QUERY_NODE_LOGIC_CONDITION:
      return dynAppendLogicCondition(s, (const SLogicConditionNode*)pExpr, dialect, pExtMeta, pCtx);
    case QUERY_NODE_REMOTE_VALUE_LIST:
      // Standalone REMOTE_VALUE_LIST (rare, but handle gracefully).
      return dynAppendRemoteValueList(s, (const SRemoteValueListNode*)pExpr, dialect,
                                      pCtx, ((const SExprNode*)pExpr)->resType.precision);
    case QUERY_NODE_REMOTE_VALUE: {
      // Scalar subquery result (single scalar) — resolve and emit as literal value.
      const SRemoteValueNode* pVal = (const SRemoteValueNode*)pExpr;
      if (IS_VAL_UNSET(pVal->val.flag)) {
        if (!pCtx || !pCtx->fp) {
          qError("FQ-SQL: REMOTE_VALUE no resolve ctx (pCtx=%p)", pCtx);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        int32_t valCode = pCtx->fp(pCtx->pCtx, pVal->subQIdx, (SNode*)pVal);
        if (valCode != TSDB_CODE_SUCCESS) return valCode;
      }
      dynAppendValueLiteral(s, &pVal->val, dialect);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ZERO_ROWS: {
      // EXISTS/NOT EXISTS subquery row-count — resolve and emit as literal integer.
      // Used when EXISTS cannot be pushed to the remote source (e.g. InfluxDB).
      // The inner subquery is executed by TDengine; the row count becomes a constant
      // in the remote SQL (e.g. WHERE (3 > 0) instead of WHERE EXISTS (...)).
      const SRemoteZeroRowsNode* pRows = (const SRemoteZeroRowsNode*)pExpr;
      if (IS_VAL_UNSET(pRows->val.flag)) {
        if (!pCtx || !pCtx->fp) {
          qError("FQ-SQL: REMOTE_ZERO_ROWS no resolve ctx (pCtx=%p)", pCtx);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        int32_t rowCode = pCtx->fp(pCtx->pCtx, pRows->subQIdx, (SNode*)pRows);
        if (rowCode != TSDB_CODE_SUCCESS) return rowCode;
      }
      dynAppendValueLiteral(s, &pRows->val, dialect);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ROW: {
      // Scalar subquery result (ANY/ALL/scalar) — resolve and emit as literal value.
      const SRemoteRowNode* pRow = (const SRemoteRowNode*)pExpr;
      if (!pRow->valSet) {
        if (!pCtx || !pCtx->fp) {
          qError("FQ-SQL: REMOTE_ROW no resolve ctx (pCtx=%p)", pCtx);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        int32_t rowCode = pCtx->fp(pCtx->pCtx, pRow->subQIdx, (SNode*)pRow);
        if (rowCode != TSDB_CODE_SUCCESS) return rowCode;
      }
      // Emit the resolved scalar value; NULL if subquery returned no rows.
      dynAppendValueLiteral(s, &pRow->val, dialect);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_CASE_WHEN: {
      // CASE [expr] WHEN cond THEN result [...] [ELSE result] END
      const SCaseWhenNode* pCW = (const SCaseWhenNode*)pExpr;
      dynSQLAppendStr(s, "CASE");
      if (pCW->pCase) {
        dynSQLAppendChar(s, ' ');
        int32_t code = dynAppendExpr(s, pCW->pCase, dialect, pExtMeta, pCtx);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
      SNode* pItem = NULL;
      FOREACH(pItem, pCW->pWhenThenList) {
        const SWhenThenNode* pWT = (const SWhenThenNode*)pItem;
        dynSQLAppendStr(s, " WHEN ");
        int32_t code = dynAppendExpr(s, pWT->pWhen, dialect, pExtMeta, pCtx);
        if (code != TSDB_CODE_SUCCESS) return code;
        dynSQLAppendStr(s, " THEN ");
        code = dynAppendExpr(s, pWT->pThen, dialect, pExtMeta, pCtx);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
      if (pCW->pElse) {
        dynSQLAppendStr(s, " ELSE ");
        int32_t code = dynAppendExpr(s, pCW->pElse, dialect, pExtMeta, pCtx);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
      dynSQLAppendStr(s, " END");
      return TSDB_CODE_SUCCESS;
    }
    default:
      qError("FQ-SQL: dynAppendExpr unsupported nodeType=%d", nodeType(pExpr));
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
}

// ---------------------------------------------------------------------------
// nodesExprToExtSQL — public API (fixed-buffer; kept for external callers)
// ---------------------------------------------------------------------------
int32_t nodesExprToExtSQL(const SNode* pExpr, EExtSQLDialect dialect, char* buf, int32_t bufLen,
                           int32_t* pLen) {
  if (!pExpr) {
    if (pLen) *pLen = 0;
    return TSDB_CODE_SUCCESS;
  }
  SDynSQL s;
  dynSQLInit(&s);
  int32_t code = dynAppendExpr(&s, pExpr, dialect, NULL, NULL);  // no ext table / resolve context for public API
  if (code) {
    dynSQLFree(&s);
    return code;
  }
  if (s.err) {
    int32_t err = s.err;
    dynSQLFree(&s);
    return err;
  }
  // Copy into caller-provided buffer (truncate silently if too small — caller responsibility)
  int32_t written = s.pos < bufLen - 1 ? s.pos : (bufLen > 0 ? bufLen - 1 : 0);
  if (bufLen > 0 && buf) {
    memcpy(buf, s.buf ? s.buf : "", written);
    buf[written] = '\0';
  }
  if (pLen) *pLen = written;
  dynSQLFree(&s);
  return TSDB_CODE_SUCCESS;
}

// Forward declaration — renderNode is recursive (subquery case calls itself).
static int32_t renderNode(const SPhysiNode* pNode, EExtSQLDialect dialect,
                           const SNodesRemoteSQLCtx* pCtx, SDynSQL* s);

// Render SELECT column list from a SNodeList (pProjections or pScanCols).
// pExtMeta is used for column name resolution (TDengine name → remote name).
static void renderSelectList(SDynSQL* s, const SNodeList* pCols, EExtSQLDialect dialect,
                              const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  if (!pCols || LIST_LENGTH(pCols) == 0) {
    if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
      dynSQLAppendChar(s, '*');
    } else {
      dynSQLAppendChar(s, '1');
    }
    return;
  }
  bool first = true;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pCols) {
    const SNode* pCol = pExpr;
    if (nodeType(pCol) == QUERY_NODE_TARGET) {
      pCol = ((const STargetNode*)pCol)->pExpr;
    }
    if (NULL == pCol) continue;
    if (!first) dynSQLAppendStr(s, ", ");
    first = false;
    if (nodeType(pCol) == QUERY_NODE_COLUMN) {
      const char* tdName = ((const SColumnNode*)pCol)->colName;
      const char* colName = resolveExtColName(pExtMeta, tdName);
      dynAppendQuotedId(s, colName, dialect);
      // Add AS alias when:
      // (1) The column has an explicit aliasName (e.g. TAIL(val,2) rewrites val
      //     with aliasName="expr_1") that differs from the rendered colName.
      //     Without this, a nested-subquery case emits "SELECT expr_1 FROM
      //     (SELECT val FROM ...) _sq" where "expr_1" is unknown in the inner
      //     result — causing MySQL error 1054, PG error 42703, etc.
      // (2) Remote column name differs from TDengine name (column rename), so
      //     parent nodes (Project/Sort) can reference columns by TDengine name
      //     through the subquery boundary.
      const char* alias = ((const SExprNode*)pCol)->aliasName;
      if (alias && alias[0] != '\0' && strcasecmp(alias, colName) != 0) {
        dynSQLAppendStr(s, " AS ");
        dynAppendQuotedId(s, alias, dialect);
      } else if (pExtMeta && strcasecmp(colName, tdName) != 0) {
        dynSQLAppendStr(s, " AS ");
        dynAppendQuotedId(s, tdName, dialect);
      }
    } else {
      (void)dynAppendExpr(s, pCol, dialect, pExtMeta, pCtx);
      const char* alias = ((const SExprNode*)pCol)->aliasName;
      if (alias && alias[0] != '\0') {
        dynSQLAppendStr(s, " AS ");
        dynAppendQuotedId(s, alias, dialect);
      }
    }
  }
  if (first) {
    if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
      dynSQLAppendChar(s, '*');
    } else {
      dynSQLAppendChar(s, '1');
    }
  }
}

// Render WHERE clause from pConditions.
static void renderWhere(SDynSQL* s, const SNode* pConditions, EExtSQLDialect dialect,
                         const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  if (!pConditions) return;
  SDynSQL cond;
  dynSQLInit(&cond);
  cond.tzName = s->tzName;
  int32_t code = dynAppendExpr(&cond, pConditions, dialect, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS || cond.err) {
    if (!s->err) s->err = cond.err ? cond.err : code;
    dynSQLFree(&cond);
    return;
  }
  if (cond.pos > 0) {
    dynSQLAppendStr(s, " WHERE ");
    dynSQLAppendLen(s, cond.buf, cond.pos);
  }
  dynSQLFree(&cond);
}

// Render ORDER BY clause from pSortKeys.
static void renderOrderBy(SDynSQL* s, const SNodeList* pSortKeys, EExtSQLDialect dialect,
                            const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  if (!pSortKeys || LIST_LENGTH(pSortKeys) == 0) return;
  bool firstKey = true;
  SNode* pKey = NULL;
  FOREACH(pKey, pSortKeys) {
    const SOrderByExprNode* pOrd = (const SOrderByExprNode*)pKey;
    if (dialect == EXT_SQL_DIALECT_MYSQL) {
      bool needNullKey = false;
      bool nullKeyDesc = false;
      if (pOrd->order == ORDER_ASC && pOrd->nullOrder == NULL_ORDER_LAST) {
        needNullKey = true; nullKeyDesc = false;
      } else if (pOrd->order == ORDER_DESC && pOrd->nullOrder == NULL_ORDER_FIRST) {
        needNullKey = true; nullKeyDesc = true;
      }
      if (needNullKey) {
        dynSQLAppendStr(s, firstKey ? " ORDER BY (" : ", (");
        (void)dynAppendExpr(s, pOrd->pExpr, dialect, pExtMeta, pCtx);
        dynSQLAppendStr(s, nullKeyDesc ? " IS NULL) DESC" : " IS NULL) ASC");
        firstKey = false;
      }
    }
    dynSQLAppendStr(s, firstKey ? " ORDER BY " : ", ");
    firstKey = false;
    (void)dynAppendExpr(s, pOrd->pExpr, dialect, pExtMeta, pCtx);
    dynSQLAppendStr(s, (pOrd->order == ORDER_DESC) ? " DESC" : " ASC");
    if (dialect != EXT_SQL_DIALECT_MYSQL) {
      if (pOrd->nullOrder == NULL_ORDER_FIRST)
        dynSQLAppendStr(s, " NULLS FIRST");
      else if (pOrd->nullOrder == NULL_ORDER_LAST)
        dynSQLAppendStr(s, " NULLS LAST");
    }
  }
}

// Render LIMIT / OFFSET clause.
static void renderLimit(SDynSQL* s, const SLimitNode* pLimit) {
  if (!pLimit || !pLimit->limit) return;
  dynSQLAppendf(s, " LIMIT %" PRId64, pLimit->limit->datum.i);
  if (pLimit->offset && pLimit->offset->datum.i > 0)
    dynSQLAppendf(s, " OFFSET %" PRId64, pLimit->offset->datum.i);
}

// Get first child of a physi node.
static const SPhysiNode* getFirstChild(const SPhysiNode* pNode) {
  if (!pNode || !pNode->pChildren || LIST_LENGTH(pNode->pChildren) == 0) return NULL;
  return (const SPhysiNode*)nodesListGetNode(pNode->pChildren, 0);
}

// Walk down to find the FederatedScan leaf.
static const SFederatedScanPhysiNode* findLeafScan(const SPhysiNode* pNode) {
  while (pNode) {
    if (nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN)
      return (const SFederatedScanPhysiNode*)pNode;
    pNode = getFirstChild(pNode);
  }
  return NULL;
}

// ---------------------------------------------------------------------------
// renderNode — translate a physical plan subtree to SQL.
//
// Strategy: walk the chain collecting Sort and the first Project encountered.
// If the chain reaches FederatedScan without hitting a second Project, merge
// everything into a single flat query (no subqueries):
//   SELECT <proj|scanCols> FROM table [WHERE] [ORDER BY] [LIMIT]
//
// When a second Project is detected (nested Projects), only the portion from
// the second Project downward is rendered as a subquery:
//   SELECT <outerProj> FROM (<innerSQL>) _sq [ORDER BY] 
//
// This avoids subqueries for the common Sort→Project→FederatedScan chains
// (critical for InfluxDB which does not support subqueries) while correctly
// handling Project→Project→FederatedScan where column sets differ.
// ---------------------------------------------------------------------------
static int32_t renderNode(const SPhysiNode* pNode, EExtSQLDialect dialect,
                           const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  if (!pNode) {
    qError("FQ-SQL: renderNode called with NULL pNode");
    return TSDB_CODE_INVALID_PARA;
  }

  // ── Phase 1: walk down collecting Sort and innermost Project ──
  // When multiple Project nodes are stacked (e.g. TAIL rewrite creates
  // Project_outer(expr_1) → Project_inner(val) → Sort → FedScan), always
  // use the innermost Project's columns for the remote SELECT.  The outer
  // Project's renaming (val → expr_1) is applied locally by TDengine after
  // receiving the remote result.  This matches the behaviour of the old
  // collectRemoteParts() which simply overwrote pProjections on each Project
  // node it visited.
  const SSortPhysiNode*    pSort = NULL;
  const SProjectPhysiNode* pProj = NULL;  // innermost Project seen so far
  const SPhysiNode*        pCur = pNode;

  while (pCur) {
    int16_t type = nodeType(pCur);
    if (type == QUERY_NODE_PHYSICAL_PLAN_SORT) {
      if (!pSort) pSort = (const SSortPhysiNode*)pCur;
      pCur = getFirstChild(pCur);
    } else if (type == QUERY_NODE_PHYSICAL_PLAN_PROJECT) {
      pProj = (const SProjectPhysiNode*)pCur;  // overwrite → innermost wins
      pCur = getFirstChild(pCur);
    } else if (type == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
      break;
    } else {
      qError("FQ-SQL: renderNode unexpected nodeType=%d in chain", type);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
  }

  // Find the FederatedScan leaf (always present)
  const SFederatedScanPhysiNode* pScan = findLeafScan(pNode);
  if (!pScan || !pScan->pExtTable) {
    qError("FQ-SQL: renderNode no FederatedScan leaf found");
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  const SExtTableNode* pExtTable = (const SExtTableNode*)pScan->pExtTable;
  const SExtTableMeta* pMeta = pExtTable->pExtMeta;

  // ── Phase 2: render ──
  // SELECT <innermost proj or scanCols> FROM table [WHERE] [ORDER BY] [LIMIT]
  dynSQLAppendStr(s, "SELECT ");
  if (pProj) {
    renderSelectList(s, pProj->pProjections, dialect, pMeta, pCtx);
  } else {
    renderSelectList(s, pScan->pScanCols, dialect, pMeta, pCtx);
  }
  dynSQLAppendStr(s, " FROM ");
  dynAppendTablePath(s, pExtTable, dialect);
  renderWhere(s, pScan->node.pConditions, dialect, pMeta, pCtx);
  if (pSort) {
    // ORDER BY uses TDengine names (from subquery boundary)
    renderOrderBy(s, pSort->pSortKeys, dialect, NULL, pCtx);
  } else if (pScan->underVTableScan) {
    // No explicit ORDER BY from planner — always order by the primary timestamp
    // column ASC to guarantee deterministic results from all external sources.
    SExtColumnDef pTsCol = pMeta->pCols[0];
    const char* tsName = pTsCol.remoteColName[0] ? pTsCol.remoteColName : pTsCol.colName;
    dynSQLAppendStr(s, " ORDER BY ");
    dynAppendQuotedId(s, tsName, dialect);
    dynSQLAppendStr(s, " ASC");
  }
  renderLimit(s, (const SLimitNode*)pScan->node.pLimit);
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// nodesRenderCorrelatedExistsBody — public API
// ---------------------------------------------------------------------------
// Renders the body SQL of a correlated EXISTS subquery for pushdown to an
// external data source.
//
// pInnerSelect : the inner SSelectStmt.  Its pFromTable must be a
//                SRealTableNode with pExtTableNode set.
// sourceType   : EExtSourceType — determines the SQL dialect.
// ppSQL        : OUT — heap-allocated SQL string (NULL-terminated plain C
//                string); caller must taosMemoryFree() when done.
//
// Column references in the WHERE clause are rendered as "tableName"."colName"
// (or `tableName`.`colName` for MySQL) so that:
//   - Columns from the inner table reference the table in the EXISTS FROM.
//   - Columns from the outer table (correlated references) reference the
//     outer table in the outer FROM clause of the containing query.
//
// The generated string can be embedded directly as:
//   EXISTS (<ppSQL>)  or  NOT EXISTS (<ppSQL>)
// ---------------------------------------------------------------------------
int32_t nodesRenderCorrelatedExistsBody(const SSelectStmt* pInnerSelect,
                                        int8_t             sourceType,
                                        char**             ppSQL) {
  if (!pInnerSelect || !ppSQL) {
    qError("FQ-SQL: nodesRenderCorrelatedExistsBody NULL input (pInnerSelect=%p ppSQL=%p)", pInnerSelect, ppSQL);
    return TSDB_CODE_INVALID_PARA;
  }

  EExtSQLDialect dialect;
  switch ((EExtSourceType)sourceType) {
    case EXT_SOURCE_MYSQL:      dialect = EXT_SQL_DIALECT_MYSQL;    break;
    case EXT_SOURCE_POSTGRESQL: dialect = EXT_SQL_DIALECT_POSTGRES; break;
    case EXT_SOURCE_INFLUXDB:   dialect = EXT_SQL_DIALECT_INFLUXQL; break;
    default:                    dialect = EXT_SQL_DIALECT_MYSQL;    break;
  }

  // Context that enables "tableName"."colName" rendering.
  SNodesRemoteSQLCtx fullCtx = { .includeTableName = true };

  SDynSQL s;
  dynSQLInit(&s);

  // ── SELECT clause ──────────────────────────────────────────────────────
  dynSQLAppendStr(&s, "SELECT ");
  bool first = true;
  if (pInnerSelect->pProjectionList) {
    SNode* pProj = NULL;
    FOREACH(pProj, pInnerSelect->pProjectionList) {
      if (!first) dynSQLAppendStr(&s, ", ");
      first = false;
      int32_t code = dynAppendExpr(&s, pProj, dialect, NULL, &fullCtx);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FQ-SQL: nodesRenderCorrelatedExistsBody SELECT expr failed code=0x%x", code);
        dynSQLFree(&s);
        return code;
      }
      if (s.err) {
        int32_t err = s.err;
        qError("FQ-SQL: nodesRenderCorrelatedExistsBody SELECT dynSQL err=0x%x", err);
        dynSQLFree(&s);
        return err;
      }
    }
  }
  if (first) dynSQLAppendChar(&s, '1');  // default: SELECT 1

  // ── FROM clause ────────────────────────────────────────────────────────
  if (pInnerSelect->pFromTable &&
      QUERY_NODE_REAL_TABLE == nodeType(pInnerSelect->pFromTable)) {
    SRealTableNode* pReal = (SRealTableNode*)pInnerSelect->pFromTable;
    SExtTableNode*  pExt  = (SExtTableNode*)pReal->pExtTableNode;
    if (pExt) {
      dynSQLAppendStr(&s, " FROM ");
      dynAppendTablePath(&s, pExt, dialect);
    }
  }

  // ── WHERE clause ───────────────────────────────────────────────────────
  if (pInnerSelect->pWhere) {
    SDynSQL condSql;
    dynSQLInit(&condSql);
    int32_t code = dynAppendExpr(&condSql, pInnerSelect->pWhere, dialect, NULL, &fullCtx);
    if (code != TSDB_CODE_SUCCESS || condSql.err) {
      int32_t err = condSql.err ? condSql.err : code;
      qError("FQ-SQL: nodesRenderCorrelatedExistsBody WHERE failed code=0x%x", err);
      dynSQLFree(&condSql);
      dynSQLFree(&s);
      return err;
    }
    if (condSql.pos > 0) {
      dynSQLAppendStr(&s, " WHERE ");
      dynSQLAppendLen(&s, condSql.buf, condSql.pos);
    }
    dynSQLFree(&condSql);
  }

  if (s.err) {
    int32_t err = s.err;
    qError("FQ-SQL: nodesRenderCorrelatedExistsBody final dynSQL err=0x%x", err);
    dynSQLFree(&s);
    return err;
  }

  char* plainSQL = dynSQLDetach(&s);
  if (!plainSQL) {
    qError("FQ-SQL: nodesRenderCorrelatedExistsBody OOM on dynSQLDetach");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *ppSQL = plainSQL;
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// nodesRemotePlanToSQL — public API
// ---------------------------------------------------------------------------
// pRemotePlan MUST be non-NULL (the Mode 1 outer node's .pRemotePlan field).
// The function walks the mini physi-plan tree rooted at pRemotePlan to collect
// SELECT / FROM / WHERE / ORDER BY / LIMIT clauses, then assembles the SQL.
//
// sourceType is mapped to the corresponding EExtSQLDialect internally so that
// callers never need to depend on EExtSQLDialect.
//
// Callers: Executor (federatedscanoperator.c), Connector (extConnectorQuery.c),
//          and EXPLAIN (explain.c).  The same function is used for EXPLAIN output
//          so the displayed Remote SQL exactly matches the SQL sent to the DB.
int32_t nodesRemotePlanToSQL(const SPhysiNode* pRemotePlan, int8_t sourceType,
                              const SNodesRemoteSQLCtx* pResolveCtx,
                              char** ppSQL) {
  if (!pRemotePlan || !ppSQL) {
    qError("FQ-SQL: nodesRemotePlanToSQL NULL input (pRemotePlan=%p ppSQL=%p)", pRemotePlan, ppSQL);
    return TSDB_CODE_INVALID_PARA;
  }

  EExtSQLDialect dialect;
  switch ((EExtSourceType)sourceType) {
    case EXT_SOURCE_MYSQL:      dialect = EXT_SQL_DIALECT_MYSQL;    break;
    case EXT_SOURCE_POSTGRESQL: dialect = EXT_SQL_DIALECT_POSTGRES; break;
    case EXT_SOURCE_INFLUXDB:   dialect = EXT_SQL_DIALECT_INFLUXQL; break;
    default:                    dialect = EXT_SQL_DIALECT_MYSQL;    break;
  }

  // ── UNION ALL: multi-child Project node ──
  // When the root is a SProjectPhysiNode with multiple children, each child
  // branch is an independent sub-query.  Generate SQL for each branch and
  // join them with " UNION ALL ".
  if (nodeType(pRemotePlan) == QUERY_NODE_PHYSICAL_PLAN_PROJECT &&
      pRemotePlan->pChildren != NULL &&
      LIST_LENGTH(pRemotePlan->pChildren) > 1) {
    SDynSQL unionSQL;
    dynSQLInit(&unionSQL);
    unionSQL.tzName = (pResolveCtx && pResolveCtx->tzName && pResolveCtx->tzName[0]) ? pResolveCtx->tzName : NULL;

    bool firstBranch = true;
    SNode* pChild = NULL;
    FOREACH(pChild, pRemotePlan->pChildren) {
      if (!firstBranch) {
        dynSQLAppendStr(&unionSQL, " UNION ALL ");
      }
      int32_t code = renderNode((const SPhysiNode*)pChild, dialect, pResolveCtx, &unionSQL);
      if (code) { qError("FQ-SQL: UNION ALL branch renderNode failed code=0x%x", code); dynSQLFree(&unionSQL); return code; }
      firstBranch = false;
    }

    if (unionSQL.err) {
      int32_t err = unionSQL.err;
      qError("FQ-SQL: UNION ALL dynSQL err=0x%x", err);
      dynSQLFree(&unionSQL);
      return err;
    }

    char* result = dynSQLDetach(&unionSQL);
    if (!result) {
      qError("FQ-SQL: UNION ALL OOM on dynSQLDetach");
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    *ppSQL = result;
    qDebug("FQ-SQL: generated UNION ALL sql len=%d sql=[%s]", (int)strlen(result), result);
    return TSDB_CODE_SUCCESS;
  }

  // ── Recursive rendering: handles arbitrary plan node combinations ──
  SDynSQL sql;
  dynSQLInit(&sql);
  sql.tzName = (pResolveCtx && pResolveCtx->tzName && pResolveCtx->tzName[0]) ? pResolveCtx->tzName : NULL;

  int32_t code = renderNode(pRemotePlan, dialect, pResolveCtx, &sql);
  if (code) {
    qError("FQ-SQL: renderNode failed code=0x%x %s", code, tstrerror(code));
    dynSQLFree(&sql);
    return code;
  }
  if (sql.err) {
    int32_t err = sql.err;
    qError("FQ-SQL: renderNode dynSQL err=0x%x", err);
    dynSQLFree(&sql);
    return err;
  }

  char* result = dynSQLDetach(&sql);
  if (!result) {
    qError("FQ-SQL: renderNode OOM on dynSQLDetach");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *ppSQL = result;
  qDebug("FQ-SQL: generated remote SQL dialect=%s len=%d sql=[%s]",
         dialectName(dialect), (int)strlen(result), result);
  return TSDB_CODE_SUCCESS;
}
