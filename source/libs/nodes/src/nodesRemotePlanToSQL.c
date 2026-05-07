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
#include "plannodes.h"
#include "querynodes.h"
#include "taoserror.h"
#include "osMemory.h"
#include "tdef.h"    // TSDB_DATA_TYPE_*
#include "thash.h"  // taosHashIterate / taosHashGetKey

// ---------------------------------------------------------------------------
// SDynSQL — growable SQL string buffer (no fixed size limit)
// ---------------------------------------------------------------------------
typedef struct {
  char*   buf;  // heap-allocated; NULL until first grow
  int32_t pos;  // bytes written so far
  int32_t cap;  // current capacity
  int32_t err;  // first error code encountered (0 = OK)
} SDynSQL;

#define DYN_SQL_INIT_CAP 512

static void dynSQLInit(SDynSQL* s) {
  s->buf = NULL;
  s->pos = 0;
  s->cap = 0;
  s->err = 0;
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
    case TSDB_DATA_TYPE_DOUBLE:
      dynSQLAppendf(s, "%.17g", pVal->datum.d);
      break;
    case TSDB_DATA_TYPE_BINARY:   // TSDB_DATA_TYPE_VARCHAR has the same integer value
    case TSDB_DATA_TYPE_NCHAR:
      dynAppendEscapedString(s, pVal->datum.p, dialect);
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
      gmtime_r(&sec, &tmBuf);
      dynSQLAppendf(s, "'%04d-%02d-%02d %02d:%02d:%02d.%03d'",
                    tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
                    tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, frac);
      break;
    }
    default:
      break;  // unsupported; skip silently
  }
}

// ---------------------------------------------------------------------------
// resolveExtColName — map TDengine column name to the remote source column name.
// If pExtMeta is NULL or the column has no remoteColName, returns tdColName as-is.
// ---------------------------------------------------------------------------
static const char* resolveExtColName(const SExtTableMeta* pExtMeta, const char* tdColName) {
  if (!pExtMeta) return tdColName;
  for (int32_t i = 0; i < pExtMeta->numOfCols; i++) {
    if (strcmp(pExtMeta->pCols[i].colName, tdColName) == 0 &&
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
static void dynAppendIntAsTimestamp(SDynSQL* s, int64_t ts, int8_t precision) {
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
  gmtime_r(&sec, &tmBuf);
  dynSQLAppendf(s, "'%04d-%02d-%02d %02d:%02d:%02d.%03d'",
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
    dynAppendIntAsTimestamp(s, pVal->datum.i, prec);
    return TSDB_CODE_SUCCESS;
  }
  return dynAppendExpr(s, pExpr, dialect, pExtMeta, pCtx);
}

// ---------------------------------------------------------------------------
// dynAppendRemoteValueList — resolve REMOTE_VALUE_LIST and emit IN (...) list
// ---------------------------------------------------------------------------
static int32_t dynAppendRemoteValueList(SDynSQL* s, const SRemoteValueListNode* pRemote,
                                         EExtSQLDialect dialect,
                                         const SNodesRemoteSQLCtx* pCtx) {
  if (!pCtx || !pCtx->fp) {
    // No resolve context — cannot expand; caller will skip WHERE clause.
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
      case TSDB_DATA_TYPE_TIMESTAMP:
        dynSQLAppendf(s, "%" PRId64, *(int64_t*)pKey);
        break;
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
        // Unsupported type — stop iteration and report error.
        taosHashCancelIterate(pRemote->pHashFilter, pVal);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }

    pVal = taosHashIterate(pRemote->pHashFilter, pVal);
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendOperatorExpr(SDynSQL* s, const SOperatorNode* pOp, EExtSQLDialect dialect,
                                      const SExtTableMeta* pExtMeta,
                                      const SNodesRemoteSQLCtx* pCtx) {
  const char* opStr = NULL;
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:         opStr = " = ";    break;
    case OP_TYPE_NOT_EQUAL:     opStr = " <> ";   break;
    case OP_TYPE_GREATER_THAN:  opStr = " > ";    break;
    case OP_TYPE_GREATER_EQUAL: opStr = " >= ";   break;
    case OP_TYPE_LOWER_THAN:    opStr = " < ";    break;
    case OP_TYPE_LOWER_EQUAL:   opStr = " <= ";   break;
    case OP_TYPE_LIKE:          opStr = " LIKE ";  break;
    case OP_TYPE_IN:
      // col IN REMOTE_VALUE_LIST(...) — resolve subquery values and emit inline list.
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IN ");
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_VALUE_LIST) {
        return dynAppendRemoteValueList(s, (const SRemoteValueListNode*)pOp->pRight, dialect, pCtx);
      }
      // pRight is something else (constant list, etc.) — not yet supported.
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    case OP_TYPE_NOT_IN: {
      // col NOT IN REMOTE_VALUE_LIST(...) — resolve subquery values and emit NOT IN (...) list.
      if (!pOp->pRight || nodeType(pOp->pRight) != QUERY_NODE_REMOTE_VALUE_LIST)
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      const SRemoteValueListNode* pRemote = (const SRemoteValueListNode*)pOp->pRight;
      if (!pCtx || !pCtx->fp)
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
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
        switch (pRemote->filterValueType) {
          case TSDB_DATA_TYPE_BOOL:
            dynSQLAppendStr(s, (*(int8_t*)pNotInKey) ? "TRUE" : "FALSE");
            break;
          case TSDB_DATA_TYPE_TINYINT:
            dynSQLAppendf(s, "%" PRId64, (int64_t)(*(int8_t*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            dynSQLAppendf(s, "%" PRId64, (int64_t)(*(int16_t*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_INT:
            dynSQLAppendf(s, "%" PRId64, (int64_t)(*(int32_t*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_TIMESTAMP:
            dynSQLAppendf(s, "%" PRId64, *(int64_t*)pNotInKey);
            break;
          case TSDB_DATA_TYPE_UTINYINT:
            dynSQLAppendf(s, "%" PRIu64, (uint64_t)(*(uint8_t*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_USMALLINT:
            dynSQLAppendf(s, "%" PRIu64, (uint64_t)(*(uint16_t*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_UINT:
            dynSQLAppendf(s, "%" PRIu64, (uint64_t)(*(uint32_t*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_UBIGINT:
            dynSQLAppendf(s, "%" PRIu64, *(uint64_t*)pNotInKey);
            break;
          case TSDB_DATA_TYPE_FLOAT:
            dynSQLAppendf(s, "%.9g", (double)(*(float*)pNotInKey));
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            dynSQLAppendf(s, "%.17g", *(double*)pNotInKey);
            break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR:
            dynAppendEscapedString(s, (const char*)pNotInKey, dialect);
            break;
          default:
            taosHashCancelIterate(pRemote->pHashFilter, pNotInVal);
            return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
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
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    case OP_TYPE_NOT_EXISTS:
      if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_VALUE &&
          (((const SValueNode*)pOp->pLeft)->flag & VALUE_FLAG_RAW_SQL_FRAG)) {
        dynSQLAppendStr(s, "NOT EXISTS (");
        dynSQLAppendStr(s, varDataVal(((const SValueNode*)pOp->pLeft)->datum.p));
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    default:
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
      return dynAppendRemoteValueList(s, (const SRemoteValueListNode*)pExpr, dialect, pCtx);
    case QUERY_NODE_REMOTE_VALUE: {
      // Scalar subquery result (single scalar) — resolve and emit as literal value.
      const SRemoteValueNode* pVal = (const SRemoteValueNode*)pExpr;
      if (IS_VAL_UNSET(pVal->val.flag)) {
        if (!pCtx || !pCtx->fp) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        int32_t valCode = pCtx->fp(pCtx->pCtx, pVal->subQIdx, (SNode*)pVal);
        if (valCode != TSDB_CODE_SUCCESS) return valCode;
      }
      dynAppendValueLiteral(s, &pVal->val, dialect);
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ROW: {
      // Scalar subquery result (ANY/ALL/scalar) — resolve and emit as literal value.
      const SRemoteRowNode* pRow = (const SRemoteRowNode*)pExpr;
      if (!pRow->valSet) {
        if (!pCtx || !pCtx->fp) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        int32_t rowCode = pCtx->fp(pCtx->pCtx, pRow->subQIdx, (SNode*)pRow);
        if (rowCode != TSDB_CODE_SUCCESS) return rowCode;
      }
      // Emit the resolved scalar value; NULL if subquery returned no rows.
      dynAppendValueLiteral(s, &pRow->val, dialect);
      return TSDB_CODE_SUCCESS;
    }
    default:
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

// ---------------------------------------------------------------------------
// SRemoteSQLParts — collected SQL clauses from the pRemotePlan tree
// pCtx is threaded through so that assembleRemoteSQL can pass it to dynAppendExpr.
// ---------------------------------------------------------------------------
// The tree walker fills this struct bottom-up; assembleRemoteSQL() then
// renders the final SQL string.
typedef struct SRemoteSQLParts {
  // FROM clause: provided by the leaf SFederatedScanPhysiNode (Mode 2)
  const SExtTableNode* pExtTable;    // table identity (database + schema + tableName)
  const SNodeList*     pScanCols;    // columns to SELECT when no explicit projection
  const SNodeList*     pOutputTargets; // scan node's pTargets (executor-expected columns)

  // WHERE clause: node.pConditions on the leaf scan node
  const SNode*         pConditions;  // may be NULL

  // SELECT clause: pProjections from SProjectPhysiNode (NULL → use pScanCols)
  const SNodeList*     pProjections; // SColumnNode / SExprNode list; NULL = SELECT pScanCols

  // ORDER BY clause: pSortKeys from SSortPhysiNode (NULL → no ORDER BY)
  const SNodeList*     pSortKeys;    // SOrderByExprNode list; NULL = no ORDER BY

  // LIMIT / OFFSET: node.pLimit on the leaf scan node (SLimitNode*)
  const SLimitNode*    pLimit;       // may be NULL
} SRemoteSQLParts;

// ---------------------------------------------------------------------------
// collectRemoteParts — depth-first tree walker
// ---------------------------------------------------------------------------
// Walk pRemotePlan downward collecting each clause type:
//   SProjectPhysiNode  → pProjections  (SELECT)
//   SSortPhysiNode     → pSortKeys     (ORDER BY)
//   SFederatedScanPhysiNode (Mode 2, pRemotePlan==NULL)
//                      → pExtTable, pScanCols, pConditions, pLimit
//
// Non-leaf SFederatedScanPhysiNode (Mode 1, pRemotePlan!=NULL) must not
// appear inside a pRemotePlan tree; callers pass the Mode 1 node's
// pRemotePlan field, not the Mode 1 node itself.
static int32_t collectRemoteParts(const SPhysiNode* pNode, SRemoteSQLParts* pParts) {
  if (!pNode) return TSDB_CODE_INVALID_PARA;

  switch (nodeType(pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN: {
      // Must be the Mode 2 leaf (pRemotePlan == NULL).
      const SFederatedScanPhysiNode* pScan = (const SFederatedScanPhysiNode*)pNode;
      if (pScan->pRemotePlan != NULL) {
        // Nested Mode 1 is not supported inside pRemotePlan.
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
      pParts->pExtTable      = (const SExtTableNode*)pScan->pExtTable;
      pParts->pScanCols      = pScan->pScanCols;
      pParts->pOutputTargets = pNode->pOutputDataBlockDesc ? pNode->pOutputDataBlockDesc->pSlots : NULL;
      pParts->pConditions    = pNode->pConditions;
      pParts->pLimit      = (const SLimitNode*)pNode->pLimit;
      return TSDB_CODE_SUCCESS;
    }

    case QUERY_NODE_PHYSICAL_PLAN_PROJECT: {
      const SProjectPhysiNode* pProj = (const SProjectPhysiNode*)pNode;
      pParts->pProjections = pProj->pProjections;
      // Recurse into single child
      if (!pNode->pChildren || LIST_LENGTH(pNode->pChildren) == 0)
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      return collectRemoteParts((const SPhysiNode*)nodesListGetNode(pNode->pChildren, 0), pParts);
    }

    case QUERY_NODE_PHYSICAL_PLAN_SORT: {
      const SSortPhysiNode* pSort = (const SSortPhysiNode*)pNode;
      pParts->pSortKeys = pSort->pSortKeys;
      // Recurse into single child
      if (!pNode->pChildren || LIST_LENGTH(pNode->pChildren) == 0)
        return TSDB_CODE_PLAN_INTERNAL_ERROR;
      return collectRemoteParts((const SPhysiNode*)nodesListGetNode(pNode->pChildren, 0), pParts);
    }

    default:
      // Unknown node type in pRemotePlan tree — skip and recurse into first child
      if (pNode->pChildren && LIST_LENGTH(pNode->pChildren) > 0)
        return collectRemoteParts((const SPhysiNode*)nodesListGetNode(pNode->pChildren, 0), pParts);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
}

// ---------------------------------------------------------------------------
// assembleRemoteSQL — render full SQL from collected parts using SDynSQL
// ---------------------------------------------------------------------------
static int32_t assembleRemoteSQL(const SRemoteSQLParts* pParts, EExtSQLDialect dialect,
                                  const SNodesRemoteSQLCtx* pCtx, char** ppSQL) {
  if (!pParts->pExtTable) return TSDB_CODE_PLAN_INTERNAL_ERROR;

  SDynSQL s;
  dynSQLInit(&s);

  // SELECT clause — always use pScanCols order to match pColTypeMappings and
  // slot descriptor order in the outer FederatedScan node.  pProjections may
  // reorder columns (e.g. when ORDER BY pushdown changes the column order),
  // which would cause a mismatch between the remote result and the executor's
  // slot-based column indexing.
  dynSQLAppendStr(&s, "SELECT ");
  // When a Project node exists, pProjections defines the SELECT column list.
  // When eliminateProjOptimize removed the Project, pProjections is NULL.
  // In that case, use pOutputTargets (the scan's output slot descriptors) which
  // reflects exactly the columns the executor expects. pScanCols is only used
  // as a last resort since it may list ALL table columns.
  const SNodeList* pCols = pParts->pProjections;
  if (!pCols) pCols = pParts->pScanCols;
  bool first = true;
  // Build a set of output column names from pOutputTargets for filtering.
  // When pOutputTargets has fewer columns than pCols, only emit the columns
  // that appear in pOutputTargets.
  SHashObj* pOutputSet = NULL;
  if (pParts->pOutputTargets && pCols == pParts->pScanCols &&
      LIST_LENGTH(pParts->pOutputTargets) < LIST_LENGTH(pCols)) {
    pOutputSet = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (pOutputSet) {
      SNode* pSlotNode = NULL;
      FOREACH(pSlotNode, pParts->pOutputTargets) {
        SSlotDescNode* pSlot = (SSlotDescNode*)pSlotNode;
        if (pSlot->output) {
          taosHashPut(pOutputSet, pSlot->name, strlen(pSlot->name), &pSlot, sizeof(void*));
        }
      }
    }
  }
  if (pCols) {
    SNode* pExpr = NULL;
    FOREACH(pExpr, pCols) {
      // Unwrap STargetNode wrapper if present (physical plan serialization wraps columns)
      const SNode* pCol = pExpr;
      if (nodeType(pCol) == QUERY_NODE_TARGET) {
        pCol = ((const STargetNode*)pCol)->pExpr;
      }
      if (NULL == pCol || nodeType(pCol) != QUERY_NODE_COLUMN) continue;
      // When filtering by output targets, skip columns not in the output set
      if (pOutputSet && !taosHashGet(pOutputSet, ((const SColumnNode*)pCol)->colName,
                                     strlen(((const SColumnNode*)pCol)->colName))) {
        continue;
      }
      if (!first) dynSQLAppendStr(&s, ", ");
      const char* colName = resolveExtColName(pParts->pExtTable->pExtMeta,
                                              ((const SColumnNode*)pCol)->colName);
      dynAppendQuotedId(&s, colName, dialect);
      first = false;
    }
  }
  if (pOutputSet) taosHashCleanup(pOutputSet);
  if (first) dynSQLAppendChar(&s, '*');

  // FROM clause
  dynSQLAppendStr(&s, " FROM ");
  dynAppendTablePath(&s, pParts->pExtTable, dialect);

  // WHERE clause (best-effort: skip on expression-render failure — local Filter handles it)
  if (pParts->pConditions) {
    SDynSQL cond;
    dynSQLInit(&cond);
    int32_t code = dynAppendExpr(&cond, pParts->pConditions, dialect,
                                  pParts->pExtTable->pExtMeta, pCtx);
    if (code == TSDB_CODE_SUCCESS && !cond.err && cond.pos > 0) {
      dynSQLAppendStr(&s, " WHERE ");
      dynSQLAppendLen(&s, cond.buf, cond.pos);
    }
    dynSQLFree(&cond);
  }

  // ORDER BY clause — only emit if ALL sort keys can be rendered.
  // If any key contains a non-renderable expression (e.g. length(name)),
  // skip the entire ORDER BY so the local Sort node handles the ordering.
  qError("FQ-DIAG-ORDERBY: pSortKeys=%p len=%d", pParts->pSortKeys,
         pParts->pSortKeys ? LIST_LENGTH(pParts->pSortKeys) : -1);
  if (pParts->pSortKeys && LIST_LENGTH(pParts->pSortKeys) > 0) {
    // First pass: render all sort key expressions into a temporary array.
    // If any key fails, we will drop the entire ORDER BY.
    int32_t  nKeys      = (int32_t)LIST_LENGTH(pParts->pSortKeys);
    SArray*  pRendered  = taosArrayInit(nKeys, sizeof(SDynSQL));
    bool     allOk      = (pRendered != NULL);
    SNode*   pKey       = NULL;
    FOREACH(pKey, pParts->pSortKeys) {
      if (!allOk) break;
      const SOrderByExprNode* pOrd = (const SOrderByExprNode*)pKey;
      SDynSQL expr;
      dynSQLInit(&expr);
      int32_t code = dynAppendExpr(&expr, pOrd->pExpr, dialect, pParts->pExtTable->pExtMeta, pCtx);
      qError("FQ-DIAG-ORDERBY: dynAppendExpr code=%d expr.err=%d expr.pos=%d exprType=%d colName=%s",
             code, expr.err, expr.pos,
             pOrd->pExpr ? nodeType(pOrd->pExpr) : -1,
             (pOrd->pExpr && nodeType(pOrd->pExpr) == QUERY_NODE_COLUMN) ? ((SColumnNode*)pOrd->pExpr)->colName : "?");
      if (code || expr.err) {
        dynSQLFree(&expr);
        allOk = false;  // un-renderable key — will skip entire ORDER BY
        break;
      }
      if (!taosArrayPush(pRendered, &expr)) {
        dynSQLFree(&expr);
        allOk = false;
        break;
      }
    }
    if (allOk && pRendered && taosArrayGetSize(pRendered) == nKeys) {
      // All keys rendered successfully — emit the full ORDER BY.
      bool firstKey = true;
      SNode* pKey2 = NULL;
      int32_t ki = 0;
      FOREACH(pKey2, pParts->pSortKeys) {
        const SOrderByExprNode* pOrd = (const SOrderByExprNode*)pKey2;
        SDynSQL* pExpr = (SDynSQL*)taosArrayGet(pRendered, ki++);
        dynSQLAppendStr(&s, firstKey ? " ORDER BY " : ", ");
        firstKey = false;
        dynSQLAppendLen(&s, pExpr->buf, pExpr->pos);
        dynSQLAppendStr(&s, (pOrd->order == ORDER_DESC) ? " DESC" : " ASC");
        if (dialect != EXT_SQL_DIALECT_MYSQL) {
          if (pOrd->nullOrder == NULL_ORDER_FIRST)
            dynSQLAppendStr(&s, " NULLS FIRST");
          else if (pOrd->nullOrder == NULL_ORDER_LAST)
            dynSQLAppendStr(&s, " NULLS LAST");
        }
      }
    } else {
      qError("FQ-DIAG-ORDERBY: dropping ORDER BY (un-renderable key); local Sort will handle ordering");
    }
    // Free all rendered expressions.
    if (pRendered) {
      for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pRendered); i++) {
        dynSQLFree((SDynSQL*)taosArrayGet(pRendered, i));
      }
      taosArrayDestroy(pRendered);
    }
  }

  // LIMIT / OFFSET clause
  if (pParts->pLimit && pParts->pLimit->limit) {
    dynSQLAppendf(&s, " LIMIT %" PRId64, pParts->pLimit->limit->datum.i);
    if (pParts->pLimit->offset && pParts->pLimit->offset->datum.i > 0)
      dynSQLAppendf(&s, " OFFSET %" PRId64, pParts->pLimit->offset->datum.i);
  }

  if (s.err) {
    int32_t err = s.err;
    dynSQLFree(&s);
    return err;
  }

  char* result = dynSQLDetach(&s);
  if (!result) return TSDB_CODE_OUT_OF_MEMORY;
  *ppSQL = result;
  qError("assembleRemoteSQL: generated SQL=[%s] pProjections=%p pScanCols=%p usedCols=%p",
         result, pParts->pProjections, pParts->pScanCols, pCols);
  return TSDB_CODE_SUCCESS;
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
  if (!pInnerSelect || !ppSQL) return TSDB_CODE_INVALID_PARA;

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
      // Render projection expression; fall back to literal 1 on failure.
      int32_t code = dynAppendExpr(&s, pProj, dialect, NULL, &fullCtx);
      if (code != TSDB_CODE_SUCCESS) {
        // Non-renderable projection (e.g. SELECT 1 constant is fine; complex
        // expressions may not be needed).  Emit "1" as a safe fallback.
        dynSQLAppendChar(&s, '1');
        s.err = 0;  // clear the error and continue
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
    if (code == TSDB_CODE_SUCCESS && !condSql.err && condSql.pos > 0) {
      dynSQLAppendStr(&s, " WHERE ");
      dynSQLAppendLen(&s, condSql.buf, condSql.pos);
    }
    dynSQLFree(&condSql);
  }

  if (s.err) {
    int32_t err = s.err;
    dynSQLFree(&s);
    return err;
  }

  char* plainSQL = dynSQLDetach(&s);
  if (!plainSQL) return TSDB_CODE_OUT_OF_MEMORY;
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
  if (!pRemotePlan || !ppSQL) return TSDB_CODE_INVALID_PARA;

  EExtSQLDialect dialect;
  switch ((EExtSourceType)sourceType) {
    case EXT_SOURCE_MYSQL:      dialect = EXT_SQL_DIALECT_MYSQL;    break;
    case EXT_SOURCE_POSTGRESQL: dialect = EXT_SQL_DIALECT_POSTGRES; break;
    case EXT_SOURCE_INFLUXDB:   dialect = EXT_SQL_DIALECT_INFLUXQL; break;
    default:                    dialect = EXT_SQL_DIALECT_MYSQL;    break;
  }

  SRemoteSQLParts parts = {0};
  int32_t code = collectRemoteParts(pRemotePlan, &parts);
  if (code) return code;

  return assembleRemoteSQL(&parts, dialect, pResolveCtx, ppSQL);
}
