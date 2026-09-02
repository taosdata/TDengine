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
#include "osTime.h"   // taosGetTimestampNs / taosGetTimestampMs
#include "tdef.h"    // TSDB_DATA_TYPE_*
#include "thash.h"  // taosHashIterate / taosHashGetKey
#include "tlog.h"   // qDebug / qError
#include "functionMgt.h"  // FUNCTION_TYPE_* constants
#include "libs/scalar/scalar.h"
#include "common/ttime.h"

static const char* dialectName(EExtSQLDialect d) {
  switch (d) {
    case EXT_SQL_DIALECT_MYSQL:    return "MySQL";
    case EXT_SQL_DIALECT_POSTGRES: return "PG";
    case EXT_SQL_DIALECT_INFLUXQL: return "InfluxDB";
    default:                      return "Unknown";
  }
}

static const char* fqDebugDataTypeName(int8_t type) {
  return (type >= 0 && type < TSDB_DATA_TYPE_MAX) ? tDataTypes[type].name : "unknown";
}

static const char* remoteSelectSourceName(ENodesRemoteSelectSource source) {
  switch (source) {
    case NODES_REMOTE_SELECT_SOURCE_PROJECT:
      return "project";
    case NODES_REMOTE_SELECT_SOURCE_AGG:
      return "agg";
    case NODES_REMOTE_SELECT_SOURCE_PARTITION:
      return "partition";
    case NODES_REMOTE_SELECT_SOURCE_INDEF:
      return "indef";
    case NODES_REMOTE_SELECT_SOURCE_SCAN:
      return "scan";
    case NODES_REMOTE_SELECT_SOURCE_FROM_CHILD:
      return "from-child";
    case NODES_REMOTE_SELECT_SOURCE_JOIN:
      return "join";
    case NODES_REMOTE_SELECT_SOURCE_SETOP:
      return "setop";
    default:
      return "unknown";
  }
}

static const SNode* fqDebugUnwrapExprNode(const SNode* pNode) {
  if (pNode == NULL) {
    return NULL;
  }

  if (nodeType((SNode*)pNode) == QUERY_NODE_TARGET) {
    return ((const STargetNode*)pNode)->pExpr;
  }

  return pNode;
}

static void fqDebugAppendIfPresent(char* pBuf, int32_t bufLen, int32_t* pPos,
                                   const char* pKey, const char* pVal) {
  if (pBuf == NULL || pPos == NULL || pKey == NULL || pVal == NULL || pVal[0] == '\0' ||
      *pPos >= bufLen - 1) {
    return;
  }

  int32_t written = snprintf(pBuf + *pPos, (size_t)(bufLen - *pPos), " %s=%s", pKey, pVal);
  if (written <= 0) {
    return;
  }

  *pPos += written;
  if (*pPos >= bufLen) {
    *pPos = bufLen - 1;
  }
}

static void fqDebugDescribeExprNode(const SNode* pNode, char* pBuf, int32_t bufLen) {
  if (pBuf == NULL || bufLen <= 0) {
    return;
  }

  if (pNode == NULL) {
    snprintf(pBuf, (size_t)bufLen, "(null)");
    return;
  }

  const SNode* pExpr = fqDebugUnwrapExprNode(pNode);
  if (pExpr == NULL) {
    snprintf(pBuf, (size_t)bufLen, "target expr=(null)");
    return;
  }

  int32_t pos = snprintf(pBuf, (size_t)bufLen, "nodeType=%s(%d)",
                         nodesNodeName(nodeType((SNode*)pExpr)), (int)nodeType((SNode*)pExpr));
  if (pos < 0) {
    pBuf[0] = '\0';
    return;
  }
  if (pos >= bufLen) {
    pos = bufLen - 1;
  }

  if (nodesIsExprNode((SNode*)pExpr)) {
    const SExprNode* pExprNode = (const SExprNode*)pExpr;
    fqDebugAppendIfPresent(pBuf, bufLen, &pos, "alias", pExprNode->aliasName);
    fqDebugAppendIfPresent(pBuf, bufLen, &pos, "userAlias", pExprNode->userAlias);
  }

  switch (nodeType((SNode*)pExpr)) {
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pExpr;
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "col", pCol->colName);
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "tblAlias", pCol->tableAlias);
      char idBuf[64] = {0};
      snprintf(idBuf, sizeof(idBuf), "%d", (int)pCol->colId);
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "colId", idBuf);
      break;
    }
    case QUERY_NODE_FUNCTION: {
      const SFunctionNode* pFunc = (const SFunctionNode*)pExpr;
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "func", pFunc->functionName);
      char argcBuf[64] = {0};
      snprintf(argcBuf, sizeof(argcBuf), "%d",
               pFunc->pParameterList ? LIST_LENGTH(pFunc->pParameterList) : 0);
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "argc", argcBuf);
      const SNode* pArg0 = (pFunc->pParameterList != NULL) ? nodesListGetNode(pFunc->pParameterList, 0) : NULL;
      pArg0 = fqDebugUnwrapExprNode(pArg0);
      if (pArg0 != NULL && nodesIsExprNode((SNode*)pArg0)) {
        const SExprNode* pArg0Expr = (const SExprNode*)pArg0;
        fqDebugAppendIfPresent(pBuf, bufLen, &pos, "arg0Alias", pArg0Expr->aliasName);
        fqDebugAppendIfPresent(pBuf, bufLen, &pos, "arg0UserAlias", pArg0Expr->userAlias);
        if (nodeType((SNode*)pArg0) == QUERY_NODE_COLUMN) {
          const SColumnNode* pArg0Col = (const SColumnNode*)pArg0;
          fqDebugAppendIfPresent(pBuf, bufLen, &pos, "arg0Col", pArg0Col->colName);
        }
      }
      break;
    }
    case QUERY_NODE_VALUE: {
      const SValueNode* pVal = (const SValueNode*)pExpr;
      char typeBuf[64] = {0};
      snprintf(typeBuf, sizeof(typeBuf), "%d", (int)pVal->node.resType.type);
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "valType", typeBuf);
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "isNull", pVal->isNull ? "1" : "0");
      break;
    }
    case QUERY_NODE_OPERATOR: {
      const SOperatorNode* pOp = (const SOperatorNode*)pExpr;
      char opBuf[64] = {0};
      snprintf(opBuf, sizeof(opBuf), "%d", (int)pOp->opType);
      fqDebugAppendIfPresent(pBuf, bufLen, &pos, "opType", opBuf);
      break;
    }
    default:
      break;
  }
}

static void fqDebugLogExprList(const char* pTag, const SNodeList* pExprs) {
  int32_t exprCnt = pExprs ? LIST_LENGTH((SNodeList*)pExprs) : 0;
  qDebug("FQ-SELECT: %s exprCount=%d", pTag ? pTag : "(null)", exprCnt);
  if (pExprs == NULL) {
    return;
  }

  int32_t idx = 0;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    char desc[512] = {0};
    fqDebugDescribeExprNode(pExpr, desc, sizeof(desc));
    qDebug("FQ-SELECT: %s[%d] %s", pTag ? pTag : "(null)", idx, desc);
    ++idx;
  }
}

static void fqDebugLogRenderCtx(const char* pTag, const SExtTableMeta* pExtMeta,
                                const SNodesRemoteSQLCtx* pCtx) {
  qDebug("FQ-SQL-CTX: %s meta=%p includeTable=%d joinAlias=%d emitInternalAlias=%d preserveOutputRefName=%d rewriteHaving=%d hasHavingAgg=%d hasResolver=%d tz=%s ctx=%p",
         pTag ? pTag : "(null)", pExtMeta,
         pCtx ? (int)pCtx->includeTableName : 0,
         pCtx ? (int)pCtx->useJoinLegAlias : 0,
         pCtx ? (int)pCtx->emitInternalAlias : 0,
         pCtx ? (int)pCtx->preserveOutputRefName : 0,
         pCtx ? (int)pCtx->rewriteAggHavingColumn : 0,
         (pCtx && pCtx->pHavingAggFuncs) ? 1 : 0,
         (pCtx && pCtx->fp) ? 1 : 0,
         (pCtx && pCtx->tzName && pCtx->tzName[0] != '\0') ? pCtx->tzName : "(global)",
         pCtx ? pCtx->pCtx : NULL);
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
  const char* value = name;
  size_t      len = strlen(name);
  char        inputQuote = '\0';

  // Strip surrounding backticks or double-quotes if the identifier is already quoted,
  // then decode its doubled input quotes before applying the target dialect's
  // identifier escaping. This preserves a parser-provided quoted identifier
  // without double-escaping it in the remote SQL.
  if (len >= 2 && (name[0] == '`' || name[0] == '"') && name[len-1] == name[0]) {
    inputQuote = name[0];
    value = name + 1;
    len -= 2;
  }

  dynSQLAppendChar(s, q);
  for (size_t i = 0; i < len; ++i) {
    char ch = value[i];
    if (inputQuote != '\0' && ch == inputQuote && i + 1 < len && value[i + 1] == inputQuote) {
      ++i;
    }
    if (ch == q) {
      dynSQLAppendChar(s, q);
    }
    dynSQLAppendChar(s, ch);
  }
  dynSQLAppendChar(s, q);
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
      if ('\0' == pExtTable->schemaName[0]) {
        qError("FQ-SQL: PostgreSQL table path missing schema source=%s table=%s srcSchema=%s",
               pExtTable->sourceName,
               pExtTable->table.tableName,
               '\0' != pExtTable->srcSchema[0] ? pExtTable->srcSchema : "(empty)");
        if (!s->err) {
          s->err = TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        return;
      }
      dynAppendQuotedId(s, pExtTable->schemaName, dialect);
      dynSQLAppendChar(s, '.');
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
static void dynAppendEscapedStringLen(SDynSQL* s, const char* str, int32_t len,
                                      EExtSQLDialect dialect) {
  dynSQLAppendChar(s, '\'');
  if (NULL != str && len > 0) {
    for (int32_t i = 0; i < len; ++i) {
      char ch = str[i];
      if (ch == '\'') {
        dynSQLAppendChar(s, '\'');
        dynSQLAppendChar(s, '\'');
      } else if (ch == '\\' && dialect == EXT_SQL_DIALECT_MYSQL) {
        dynSQLAppendChar(s, '\\');
        dynSQLAppendChar(s, '\\');
      } else {
        dynSQLAppendChar(s, ch);
      }
    }
  }
  dynSQLAppendChar(s, '\'');
}

static void dynAppendEscapedString(SDynSQL* s, const char* str, EExtSQLDialect dialect) {
  dynAppendEscapedStringLen(s, str, NULL != str ? (int32_t)strlen(str) : 0, dialect);
}

static int32_t dynAppendRemoteVarLiteral(SDynSQL* s, int32_t dataType, const void* pKey,
                                         size_t keyLen, void* charsetCxt,
                                         EExtSQLDialect dialect) {
  if (NULL == pKey) {
    dynAppendEscapedString(s, "", dialect);
    return TSDB_CODE_SUCCESS;
  }

  if (dataType == TSDB_DATA_TYPE_BINARY) {
    if (keyLen < VARSTR_HEADER_SIZE) {
      qError("FQ-SQL: invalid remote string key len=%zu type=%d", keyLen, dataType);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    int32_t strLen = (int32_t)varDataLen((void*)pKey);
    if ((size_t)varDataTLen((void*)pKey) > keyLen) {
      qError("FQ-SQL: remote string key truncated keyLen=%zu strLen=%d", keyLen, strLen);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    dynAppendEscapedStringLen(s, varDataVal((void*)pKey), strLen, dialect);
    return TSDB_CODE_SUCCESS;
  }

  if (dataType == TSDB_DATA_TYPE_NCHAR) {
    if (keyLen < VARSTR_HEADER_SIZE) {
      qError("FQ-SQL: invalid remote nchar key len=%zu", keyLen);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    int32_t ucs4Len = (int32_t)varDataLen((void*)pKey);
    if ((size_t)varDataTLen((void*)pKey) > keyLen) {
      qError("FQ-SQL: remote nchar key truncated keyLen=%zu ucs4Len=%d", keyLen, ucs4Len);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
    if (ucs4Len < 0 || (ucs4Len % TSDB_NCHAR_SIZE) != 0) {
      qError("FQ-SQL: invalid remote nchar byte len=%d", ucs4Len);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    char* pMbs = taosMemoryCalloc(1, (size_t)ucs4Len + 1);
    if (NULL == pMbs) {
      return terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    }

    int32_t mbsLen = taosUcs4ToMbs((TdUcs4*)varDataVal((void*)pKey), ucs4Len, pMbs, charsetCxt);
    if (mbsLen < 0) {
      qError("FQ-SQL: failed to convert remote nchar key len=%d code=%d", ucs4Len, mbsLen);
      taosMemoryFree(pMbs);
      return TSDB_CODE_FAILED;
    }

    dynAppendEscapedStringLen(s, pMbs, mbsLen, dialect);
    taosMemoryFree(pMbs);
    return TSDB_CODE_SUCCESS;
  }

  qError("FQ-SQL: dynAppendRemoteVarLiteral unsupported type=%d", dataType);
  return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
}

static int32_t dynTimestampFractionDigits(int8_t precision) {
  return precision == TSDB_TIME_PRECISION_NANO ? 9 :
         precision == TSDB_TIME_PRECISION_MICRO ? 6 : 3;
}

enum { EXT_SQL_MAX_TIMESTAMP_FRACTION_DIGITS = 6 };

static void dynAppendTypedTimestampLiteral(SDynSQL* s, const char* tsText, EExtSQLDialect dialect,
                                           int8_t precision) {
  if (NULL == tsText) {
    dynSQLAppendStr(s, "NULL");
    qError("FQ-SQL: typed timestamp literal got NULL text dialect=%s", dialectName(dialect));
    return;
  }

  char        normalizedTsText[64] = {0};
  const char* pLiteralText = tsText;
  if ((dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_POSTGRES) &&
      dynTimestampFractionDigits(precision) > EXT_SQL_MAX_TIMESTAMP_FRACTION_DIGITS) {
    tstrncpy(normalizedTsText, tsText, sizeof(normalizedTsText));
    char* pFraction = strrchr(normalizedTsText, '.');
    if (pFraction != NULL && strlen(pFraction + 1) > EXT_SQL_MAX_TIMESTAMP_FRACTION_DIGITS) {
      pFraction[EXT_SQL_MAX_TIMESTAMP_FRACTION_DIGITS + 1] = '\0';
    }
    pLiteralText = normalizedTsText;
  }

  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
      dynSQLAppendStr(s, "CAST(");
      dynAppendEscapedString(s, pLiteralText, dialect);
      dynSQLAppendf(s, " AS DATETIME(%d))",
                    TMIN(dynTimestampFractionDigits(precision), EXT_SQL_MAX_TIMESTAMP_FRACTION_DIGITS));
      break;
    case EXT_SQL_DIALECT_POSTGRES:
      dynSQLAppendStr(s, "TIMESTAMP ");
      dynAppendEscapedString(s, pLiteralText, dialect);
      break;
    case EXT_SQL_DIALECT_INFLUXQL:
      // Flight SQL otherwise infers a VARCHAR literal.  Folded NOW()/TODAY()
      // projections keep TIMESTAMP result metadata, so make the remote result
      // type explicit instead of passing a string to the timestamp decoder.
      dynSQLAppendStr(s, "CAST(");
      dynAppendEscapedString(s, tsText, dialect);
      dynSQLAppendStr(s, " AS TIMESTAMP)");
      break;
    default:
      dynAppendEscapedString(s, tsText, dialect);
      break;
  }

  qDebug("FQ-SQL: typed timestamp literal dialect=%s text=%s", dialectName(dialect), pLiteralText);
}

static void dynAppendTimestampLiteral(SDynSQL* s, int64_t ts, int8_t precision,
                                      EExtSQLDialect dialect) {
  if (precision != TSDB_TIME_PRECISION_MILLI && precision != TSDB_TIME_PRECISION_MICRO &&
      precision != TSDB_TIME_PRECISION_NANO) {
    precision = TSDB_TIME_PRECISION_MILLI;
  }

  int64_t ticksPerSecond = TSDB_TICK_PER_SECOND(precision);
  int64_t seconds = ts / ticksPerSecond;
  int64_t fraction = ts % ticksPerSecond;
  if (fraction < 0) {
    fraction += ticksPerSecond;
    --seconds;
  }

  time_t    sec = (time_t)seconds;
  struct tm tmBuf = {0};
  if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
    taosGmTimeR(&sec, &tmBuf);
  } else {
    timezone_t tz = NULL;
    if (s->tzName != NULL && s->tzName[0] != '\0') {
      int32_t code = taosValidateTimezone(s->tzName, &tz);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FQ-SQL: invalid timezone name=%s since %s", s->tzName, tstrerror(code));
        if (tz != NULL) tzfree(tz);
        s->err = code;
        return;
      }
    }
    taosLocalTime(&sec, &tmBuf, NULL, 0, tz);
    if (tz) tzfree(tz);
  }

  char tsText[64] = {0};
  (void)snprintf(tsText, sizeof(tsText), "%04d-%02d-%02d %02d:%02d:%02d.%0*" PRId64,
                 tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
                 tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec,
                 dynTimestampFractionDigits(precision), fraction);
  dynAppendTypedTimestampLiteral(s, tsText, dialect, precision);
  qDebug("FQ-SQL: ts->iso dialect=%s tz=%s epoch=%" PRId64 " prec=%d text=%s",
         dialectName(dialect), s->tzName ? s->tzName : "(global)", ts, (int)precision, tsText);
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
      // Some virtual-table SERIES/tag conditions keep the original string in
      // literal while leaving datum.p unset. Keep 3.0 behavior here so remote
      // SQL rendering does not collapse those values to an empty string.
      qDebug("FQ-SQL-VALUE: varchar literal=%s datum=%s datumPtr=%p translate=%d bytes=%d dialect=%s",
             pVal->literal ? pVal->literal : "(null)",
             pVal->datum.p ? varDataVal(pVal->datum.p) : "(null)",
             pVal->datum.p,
             (int)pVal->translate,
             (int)pVal->node.resType.bytes,
             dialectName(dialect));
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
      qDebug("FQ-SQL-VALUE: nchar literal=%s datumPtr=%p translate=%d bytes=%d dialect=%s",
             pVal->literal ? pVal->literal : "(null)",
             pVal->datum.p,
             (int)pVal->translate,
             (int)pVal->node.resType.bytes,
             dialectName(dialect));
      dynAppendEscapedString(s, pVal->literal ? pVal->literal : "", dialect);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      dynAppendTimestampLiteral(s, pVal->datum.i, pVal->node.resType.precision, dialect);
      break;
    }
    default:
      s->err = TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      break;
  }
}

// ---------------------------------------------------------------------------
// Date/time format-string translation (TDengine/Oracle tokens → remote dialect)
//
// TDengine uses Oracle-style tokens (yyyy, mm, dd, HH24, HH12, MI, SS, MS, US,
// NS, PM, AM).  Each remote dialect needs its own specifier:
//   - MySQL DATE_FORMAT/STR_TO_DATE: %Y %m %d %H %h %i %s %f %p
//   - DataFusion (InfluxDB v3) strftime: %Y %m %d %H %I %M %S %3f/%6f/%9f %p
//     (note: minute=%M, second=%S — differ from MySQL's %i/%s)
//
// One table drives both translations.  Entries are ordered longest-token-first
// so a single forward scan yields greedy longest-match without backtracking
// (e.g. HH24 before HH).  tokLen is precomputed so the per-character loop never
// recomputes strlen; the table is a compile-time constant in .rodata.
// ---------------------------------------------------------------------------
typedef struct {
  const char* tok;         // Oracle-style source token (matched case-insensitively)
  int32_t     tokLen;      // strlen(tok), precomputed
  const char* mysql;       // MySQL DATE_FORMAT specifier
  const char* datafusion;  // DataFusion (chrono strftime) specifier
} SFmtTokenMap;

static const SFmtTokenMap FMT_TOKENS[] = {
    {"yyyy", 4, "%Y", "%Y"},
    {"HH24", 4, "%H", "%H"},
    {"HH12", 4, "%h", "%I"},
    {"yy",   2, "%y", "%y"},
    {"mm",   2, "%m", "%m"},
    {"dd",   2, "%d", "%d"},
    {"HH",   2, "%H", "%H"},
    {"MI",   2, "%i", "%M"},
    {"SS",   2, "%s", "%S"},
    {"MS",   2, "%f", "%3f"},
    {"US",   2, "%f", "%6f"},
    {"NS",   2, "%f", "%9f"},
    {"PM",   2, "%p", "%p"},
    {"AM",   2, "%p", "%p"},
};

#define FMT_TOKENS_N ((int32_t)(sizeof(FMT_TOKENS) / sizeof(FMT_TOKENS[0])))

// Translate an Oracle-style format string and emit it as a quoted SQL literal.
// useDatafusion selects the DataFusion specifier column; otherwise MySQL's.
static void dynAppendFmtStr(SDynSQL* s, const char* fmt, bool useDatafusion) {
  dynSQLAppendChar(s, '\'');
  for (const char* p = fmt; *p;) {
    if (*p == '%') {  // escape a literal % in the output
      dynSQLAppendStr(s, "%%");
      p++;
      continue;
    }
    const SFmtTokenMap* pHit = NULL;
    for (int32_t i = 0; i < FMT_TOKENS_N; i++) {
      if (strncasecmp(p, FMT_TOKENS[i].tok, FMT_TOKENS[i].tokLen) == 0) {
        pHit = &FMT_TOKENS[i];
        break;
      }
    }
    if (pHit != NULL) {
      dynSQLAppendStr(s, useDatafusion ? pHit->datafusion : pHit->mysql);
      p += pHit->tokLen;
    } else {
      dynSQLAppendChar(s, *p);
      p++;
    }
  }
  dynSQLAppendChar(s, '\'');
}

static const SExtColumnDef* findExtColByTdName(const SExtTableMeta* pExtMeta, const char* tdColName) {
  if (pExtMeta == NULL || tdColName == NULL || tdColName[0] == '\0') {
    return NULL;
  }

  for (int32_t i = 0; i < pExtMeta->numOfCols; ++i) {
    if (0 == strcasecmp(pExtMeta->pCols[i].colName, tdColName)) {
      return &pExtMeta->pCols[i];
    }
  }

  return NULL;
}

static const SExtTimestampTypeHint* fqFindTimestampTypeHint(const SNodesRemoteSQLCtx* pCtx,
                                                            const char* pColName) {
  if (pCtx == NULL || pCtx->pTimestampTypeHints == NULL || pCtx->numTimestampTypeHints <= 0 ||
      pColName == NULL || pColName[0] == '\0') {
    return NULL;
  }

  for (int32_t i = 0; i < pCtx->numTimestampTypeHints; ++i) {
    if (0 == strcasecmp(pCtx->pTimestampTypeHints[i].colName, pColName)) {
      return &pCtx->pTimestampTypeHints[i];
    }
  }

  return NULL;
}

static const char* fqExtTimestampClassName(EExtTimestampTzClass cls) {
  switch (cls) {
    case EXT_TIMESTAMP_TZ_CLASS_AWARE:
      return "aware";
    case EXT_TIMESTAMP_TZ_CLASS_NAIVE:
      return "naive";
    default:
      return "unknown";
  }
}

static bool fqTypeStartsWithIgnoreCase(const char* pTypeName, const char* pPrefix) {
  if (pTypeName == NULL || pPrefix == NULL) {
    return false;
  }

  size_t prefixLen = strlen(pPrefix);
  return prefixLen > 0 && strncasecmp(pTypeName, pPrefix, prefixLen) == 0;
}

static bool fqTypeContainsIgnoreCase(const char* pTypeName, const char* pNeedle) {
  if (pTypeName == NULL || pNeedle == NULL || pNeedle[0] == '\0') {
    return false;
  }

  size_t hayLen = strlen(pTypeName);
  size_t needleLen = strlen(pNeedle);
  if (needleLen > hayLen) {
    return false;
  }

  for (size_t i = 0; i + needleLen <= hayLen; ++i) {
    if (0 == strncasecmp(pTypeName + i, pNeedle, needleLen)) {
      return true;
    }
  }

  return false;
}

static const SExprNode* fqFindSelectOutputByRefName(const char* pRefName,
                                                    const SNodeList* pSelectExprs);
static const SExprNode* fqFindSelectOutputByColumnRef(const SColumnNode* pCol,
                                                      const SNodeList* pSelectExprs);
static bool fqProjectColumnIsWindowStartPseudo(const SColumnNode* pProjCol);
static const SExprNode* fqFindAlignedSelectOutputByColumnRef(const SColumnNode* pCol,
                                                             const SNodeList* pSelectExprs,
                                                             const SNodeList* pRefExprs);
static const SNode* fqUnwrapSelectExpr(const SNode* pNode);
static int32_t fqFindUniqueProjectChildOutputIndexByExpr(const SNode* pProjExpr,
                                                         const SNodeList* pChildOutputs,
                                                         int32_t* pTargetIdx,
                                                         bool* pIsPureColumnRemap);
static int32_t fqFindUniqueProjectChildOutputIndex(const SColumnNode* pProjCol,
                                                   const SNodeList* pChildOutputs,
                                                   const SNodeList* pChildRefExprs,
                                                   int32_t* pTargetIdx);
static int32_t fqFindProjectChildOutputIndexByRefExprs(const SColumnNode* pProjCol,
                                                       const SNodeList* pChildRefExprs,
                                                       int32_t* pTargetIdx);

static bool fqIsValidTimePrecision(int8_t precision) {
  return precision == TSDB_TIME_PRECISION_MILLI ||
         precision == TSDB_TIME_PRECISION_MICRO ||
         precision == TSDB_TIME_PRECISION_NANO;
}

static int8_t fqNormalizeTimePrecision(int8_t precision, int8_t fallback) {
  if (fqIsValidTimePrecision(precision)) {
    return precision;
  }
  if (fqIsValidTimePrecision(fallback)) {
    return fallback;
  }
  return TSDB_TIME_PRECISION_MILLI;
}

static int8_t fqDefaultTimestampPrecisionForDialect(EExtSQLDialect dialect) {
  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
    case EXT_SQL_DIALECT_POSTGRES:
      return TSDB_TIME_PRECISION_MICRO;
    case EXT_SQL_DIALECT_INFLUXQL:
      return TSDB_TIME_PRECISION_NANO;
    default:
      return TSDB_TIME_PRECISION_MILLI;
  }
}

static int64_t fqTimestampTicksPerSecond(int8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      return 1000LL;
    case TSDB_TIME_PRECISION_MICRO:
      return 1000000LL;
    case TSDB_TIME_PRECISION_NANO:
      return 1000000000LL;
    default:
      return 1000LL;
  }
}

static EExtTimestampTzClass fqClassifyExtTimestampType(EExtSQLDialect dialect, const char* pExtTypeName) {
  if (pExtTypeName == NULL || pExtTypeName[0] == '\0') {
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamp")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "datetime") ||
          fqTypeStartsWithIgnoreCase(pExtTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SQL_DIALECT_POSTGRES:
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamptz") ||
          fqTypeContainsIgnoreCase(pExtTypeName, "with time zone")) {
        return EXT_TIMESTAMP_TZ_CLASS_AWARE;
      }
      if (fqTypeStartsWithIgnoreCase(pExtTypeName, "timestamp") ||
          fqTypeStartsWithIgnoreCase(pExtTypeName, "date")) {
        return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
      }
      break;
    case EXT_SQL_DIALECT_INFLUXQL:
      return EXT_TIMESTAMP_TZ_CLASS_AWARE;
    default:
      break;
  }

  return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
}

static const SExprNode* fqResolveCurrentScopeOutputExpr(const SColumnNode* pCol,
                                                        const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pCol || NULL == pCtx) {
    return NULL;
  }
  if (pCtx->renderingCurrentSelectExpr && !pCtx->inputSelectExprsFromChildSource) {
    return NULL;
  }
  if (pCtx->pInputSelectExprs == NULL && !pCtx->inputSelectExprsFromChildSource) {
    return NULL;
  }
  if ((pCtx->useJoinLegAlias || pCtx->includeTableName) &&
      ('\0' != pCol->tableAlias[0] || '\0' != pCol->tableName[0])) {
    return NULL;
  }

  if ('\0' == pCol->colName[0] && '\0' == pCol->node.aliasName[0] &&
      '\0' == pCol->node.userAlias[0]) {
    return NULL;
  }

  if (pCtx->inputSelectExprsFromChildSource && pCtx->pInputSelectExprs != NULL) {
    const SExprNode* pInExpr =
        fqFindAlignedSelectOutputByColumnRef(pCol, pCtx->pInputSelectExprs, pCtx->pInputRefExprs);
    if (pInExpr != NULL) {
      qDebug("FQ-SQL: current-scope expr resolved from from-child output first ref=%s",
             pCol->colName);
      return pInExpr;
    }
  }

  if (pCtx->pOutputSelectExprs != NULL) {
    const SExprNode* pOutExpr = fqFindSelectOutputByColumnRef(pCol, pCtx->pOutputSelectExprs);
    if (pOutExpr != NULL) {
      qDebug("FQ-SQL: current-scope expr resolved from output ref=%s", pCol->colName);
      return pOutExpr;
    }
  }

  if (pCtx->pInputSelectExprs != NULL) {
    const SExprNode* pInExpr = fqFindSelectOutputByColumnRef(pCol, pCtx->pInputSelectExprs);
    if (pInExpr != NULL) {
      qDebug("FQ-SQL: current-scope expr resolved from child output ref=%s", pCol->colName);
      return pInExpr;
    }
  }

  return NULL;
}

static EExtTimestampTzClass fqResolveExprTimestampClassDeep(const SNode* pExpr, EExtSQLDialect dialect,
                                                            const SExtTableMeta* pExtMeta,
                                                            const SNodesRemoteSQLCtx* pCtx,
                                                            const char** ppColName,
                                                            const char** ppExtTypeName,
                                                            int32_t depth) {
  if (ppColName != NULL) {
    *ppColName = NULL;
  }
  if (ppExtTypeName != NULL) {
    *ppExtTypeName = NULL;
  }

  if (depth > 8) {
    qDebug("FQ-SQL: timestamp class resolver hit depth limit dialect=%s expr=%p",
           dialectName(dialect), pExpr);
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  const SNode* pRealExpr = fqDebugUnwrapExprNode(pExpr);
  if (pRealExpr == NULL || !nodesIsExprNode((SNode*)pRealExpr)) {
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  switch (nodeType((SNode*)pRealExpr)) {
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pRealExpr;
      if (ppColName != NULL) {
        *ppColName = pCol->colName;
      }

      const SExprNode* pCurrentScopeExpr = fqResolveCurrentScopeOutputExpr(pCol, pCtx);
      if (NULL != pCurrentScopeExpr && (const SNode*)pCurrentScopeExpr != pRealExpr) {
        const char* pNestedColName = NULL;
        const char* pNestedExtTypeName = NULL;
        EExtTimestampTzClass nestedClass =
            fqResolveExprTimestampClassDeep((const SNode*)pCurrentScopeExpr, dialect, pExtMeta, pCtx,
                                            &pNestedColName, &pNestedExtTypeName, depth + 1);
        if (nestedClass != EXT_TIMESTAMP_TZ_CLASS_UNKNOWN) {
          if (ppExtTypeName != NULL) {
            *ppExtTypeName = pNestedExtTypeName;
          }
          qDebug("FQ-SQL: timestamp class resolved via current-scope ref=%s nestedCol=%s nestedClass=%s",
                 pCol->colName, pNestedColName ? pNestedColName : "(expr)",
                 fqExtTimestampClassName(nestedClass));
          return nestedClass;
        }
      }

      const SExtTimestampTypeHint* pHint = fqFindTimestampTypeHint(pCtx, pCol->colName);
      if (pHint != NULL) {
        if (ppExtTypeName != NULL) {
          *ppExtTypeName = pHint->extTypeName;
        }
        return (EExtTimestampTzClass)pHint->tzClass;
      }

      const SExtColumnDef* pExtCol = findExtColByTdName(pExtMeta, pCol->colName);
      if (pExtCol == NULL) {
        return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
      }

      if (ppExtTypeName != NULL) {
        *ppExtTypeName = pExtCol->extTypeName;
      }

      return fqClassifyExtTimestampType(dialect, pExtCol->extTypeName);
    }
    case QUERY_NODE_FUNCTION: {
      const SFunctionNode* pFunc = (const SFunctionNode*)pRealExpr;
      if (NULL == pFunc->pParameterList || LIST_LENGTH(pFunc->pParameterList) <= 0) {
        return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
      }

      switch ((EFunctionType)pFunc->funcType) {
        case FUNCTION_TYPE_CAST:
        case FUNCTION_TYPE_TIMETRUNCATE:
        case FUNCTION_TYPE_MIN:
        case FUNCTION_TYPE_MAX:
        case FUNCTION_TYPE_FIRST:
        case FUNCTION_TYPE_LAST:
        case FUNCTION_TYPE_FQ_TIME_BUCKET:
          return fqResolveExprTimestampClassDeep(nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta,
                                                 pCtx, ppColName, ppExtTypeName, depth + 1);
        default:
          return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
      }
    }
    default:
      return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }
}

static EExtTimestampTzClass fqResolveExprTimestampClass(const SNode* pExpr, EExtSQLDialect dialect,
                                                        const SExtTableMeta* pExtMeta,
                                                        const SNodesRemoteSQLCtx* pCtx,
                                                        const char** ppColName,
                                                        const char** ppExtTypeName) {
  return fqResolveExprTimestampClassDeep(pExpr, dialect, pExtMeta, pCtx, ppColName, ppExtTypeName, 0);
}

static const SExtColumnDef* findPrimaryKeyExtCol(const SExtTableMeta* pExtMeta) {
  if (pExtMeta == NULL || pExtMeta->pCols == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < pExtMeta->numOfCols; ++i) {
    if (pExtMeta->pCols[i].isPrimaryKey) {
      return &pExtMeta->pCols[i];
    }
  }

  return NULL;
}

// Forward declarations for mutual recursion
static int32_t dynAppendExpr(SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect,
                              const SExtTableMeta* pExtMeta,
                              const SNodesRemoteSQLCtx* pCtx);
static int32_t dynAppendExprTS(SDynSQL* s, const SNode* pExpr, const SNode* pOther,
                               EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                               const SNodesRemoteSQLCtx* pCtx);
static const char* dynSqlCastType(EExtSQLDialect dialect, uint8_t type);
static bool dynIsIntegerType(uint8_t type);

static int8_t fqResolveTimestampExprPrecision(const SNode* pExpr, int8_t fallback) {
  const SNode* pRealExpr = fqDebugUnwrapExprNode(pExpr);
  if (pRealExpr != NULL && nodesIsExprNode((SNode*)pRealExpr)) {
    const SExprNode* pExprNode = (const SExprNode*)pRealExpr;
    return fqNormalizeTimePrecision((int8_t)pExprNode->resType.precision, fallback);
  }
  return fqNormalizeTimePrecision(fallback, TSDB_TIME_PRECISION_MILLI);
}

static int8_t fqResolveCastEpochPrecision(const SFunctionNode* pFunc, const SNode* pArg0,
                                          EExtSQLDialect dialect) {
  int8_t fallback = fqDefaultTimestampPrecisionForDialect(dialect);
  if (NULL != pFunc && pFunc->pParameterList != NULL && LIST_LENGTH(pFunc->pParameterList) >= 2) {
    const SNode* pPrecArg = nodesListGetNode(pFunc->pParameterList, 1);
    if (pPrecArg != NULL && nodeType((SNode*)pPrecArg) == QUERY_NODE_VALUE) {
      fallback = fqNormalizeTimePrecision((int8_t)((const SValueNode*)pPrecArg)->datum.i, fallback);
    }
  }
  return fqResolveTimestampExprPrecision(pArg0, fallback);
}

static bool fqIsPgDateTypeName(const char* pExtTypeName) {
  return (pExtTypeName != NULL &&
          0 == strcasecmp(pExtTypeName, "date"));
}

static int32_t dynRenderPgEpochExtractExpr(SDynSQL* s, const SNode* pExpr,
                                           const SExtTableMeta* pExtMeta,
                                           const SNodesRemoteSQLCtx* pCtx,
                                           EExtTimestampTzClass tsClass,
                                           const char* pExtTypeName) {
  bool useClientTzWrap = (tsClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE &&
                          s->tzName != NULL && s->tzName[0] != '\0');
  bool isDateType = fqIsPgDateTypeName(pExtTypeName);

  qDebug("FQ-SQL: render PG epoch extract exprType=%s tsClass=%s extType=%s clientTz=%s useClientTzWrap=%d isDateType=%d",
         pExpr ? nodesNodeName(nodeType((SNode*)pExpr)) : "(null)",
         fqExtTimestampClassName(tsClass),
         pExtTypeName ? pExtTypeName : "(unknown)",
         (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)",
         (int)useClientTzWrap, (int)isDateType);

  dynSQLAppendStr(s, "EXTRACT(EPOCH FROM ");
  if (useClientTzWrap) {
    dynSQLAppendStr(s, "(");
    if (isDateType) {
      dynSQLAppendStr(s, "CAST(");
      int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      dynSQLAppendStr(s, " AS TIMESTAMP)");
    } else {
      int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    dynSQLAppendStr(s, " AT TIME ZONE '");
    dynSQLAppendStr(s, s->tzName);
    dynSQLAppendStr(s, "')");
  } else {
    int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynRenderPgEpochAsTimestampExpr(SDynSQL* s, const SNode* pEpochExpr,
                                               const SExtTableMeta* pExtMeta,
                                               const SNodesRemoteSQLCtx* pCtx,
                                               int8_t precision, const char* pClientTz) {
  int64_t ticksPerSecond = fqTimestampTicksPerSecond(precision);

  qDebug("FQ-SQL: render PG epoch->timestamp precision=%d ticksPerSecond=%" PRId64 " clientTz=%s exprType=%s",
         (int)precision, ticksPerSecond,
         (pClientTz != NULL && pClientTz[0] != '\0') ? pClientTz : "(global)",
         pEpochExpr ? nodesNodeName(nodeType((SNode*)pEpochExpr)) : "(null)");

  dynSQLAppendStr(s, "CAST(");
  if (pClientTz != NULL && pClientTz[0] != '\0') {
    dynSQLAppendStr(s, "(");
  }
  dynSQLAppendStr(s, "TO_TIMESTAMP(((");
  int32_t code = dynAppendExpr(s, pEpochExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  dynSQLAppendStr(s, ")::double precision) / ");
  dynSQLAppendf(s, "%" PRId64, ticksPerSecond);
  dynSQLAppendStr(s, ")");
  if (pClientTz != NULL && pClientTz[0] != '\0') {
    dynSQLAppendStr(s, " AT TIME ZONE '");
    dynSQLAppendStr(s, pClientTz);
    dynSQLAppendStr(s, "')");
  }
  dynSQLAppendStr(s, " AS TIMESTAMP)");
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynAppendPgEpochAsTimestamptzLiteral(SDynSQL* s, int64_t epoch, int8_t precision) {
  int64_t ticksPerSecond = fqTimestampTicksPerSecond(precision);

  qDebug("FQ-SQL: render PG epoch literal as timestamptz epoch=%" PRId64 " precision=%d ticksPerSecond=%" PRId64,
         epoch, (int)precision, ticksPerSecond);

  dynSQLAppendStr(s, "TO_TIMESTAMP((");
  dynSQLAppendf(s, "%" PRId64, epoch);
  dynSQLAppendStr(s, ")::double precision / ");
  dynSQLAppendf(s, "%" PRId64, ticksPerSecond);
  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynTryRenderPgTemporalDiffLiteralOperand(SDynSQL* s, const SNode* pExpr,
                                                        int8_t fallbackPrecision, bool* pHandled) {
  if (pHandled != NULL) {
    *pHandled = false;
  }

  const SNode* pRealExpr = fqDebugUnwrapExprNode(pExpr);
  if (NULL == pRealExpr || nodeType((SNode*)pRealExpr) != QUERY_NODE_VALUE) {
    return TSDB_CODE_SUCCESS;
  }

  const SValueNode* pVal = (const SValueNode*)pRealExpr;
  if (pVal->isNull) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t epoch = 0;
  int8_t  precision = fallbackPrecision;
  bool    canRender = false;

  if (pVal->node.resType.type == TSDB_DATA_TYPE_TIMESTAMP) {
    epoch = pVal->datum.i;
    precision = fqNormalizeTimePrecision((int8_t)pVal->node.resType.precision, fallbackPrecision);
    canRender = true;
    qDebug("FQ-SQL: TIMEDIFF PG literal timestamp node uses absolute epoch=%" PRId64 " precision=%d literal=%s",
           epoch, (int)precision, pVal->literal ? pVal->literal : "(null)");
  } else if (pVal->node.resType.type == TSDB_DATA_TYPE_BINARY ||
             pVal->node.resType.type == TSDB_DATA_TYPE_NCHAR) {
    timezone_t tz = NULL;
    if (s->tzName != NULL && s->tzName[0] != '\0') {
      int32_t code = taosValidateTimezone(s->tzName, &tz);
      if (code != TSDB_CODE_SUCCESS) {
        if (tz != NULL) tzfree(tz);
        return code;
      }
    }
    int32_t code = convertStringToTimestamp(pVal->node.resType.type, pVal->datum.p, fallbackPrecision,
                                            &epoch, tz, pVal->charsetCxt);
    if (NULL != tz) {
      tzfree(tz);
    }
    if (code == TSDB_CODE_SUCCESS) {
      canRender = true;
      qDebug("FQ-SQL: TIMEDIFF PG literal string parsed by TD semantics literal=%s epoch=%" PRId64
             " precision=%d clientTz=%s",
             pVal->literal ? pVal->literal : "(null)", epoch, (int)fallbackPrecision,
             (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)");
    } else {
      qDebug("FQ-SQL: TIMEDIFF PG literal string parse skipped literal=%s type=%d(%s) code=0x%x %s",
             pVal->literal ? pVal->literal : "(null)",
             (int)pVal->node.resType.type, fqDebugDataTypeName(pVal->node.resType.type),
             code, tstrerror(code));
    }
  }

  if (!canRender) {
    return TSDB_CODE_SUCCESS;
  }

  if (pHandled != NULL) {
    *pHandled = true;
  }
  return dynAppendPgEpochAsTimestamptzLiteral(s, epoch, precision);
}

static int32_t dynRenderPgTemporalDiffOperand(SDynSQL* s, const SNode* pExpr,
                                              const SExtTableMeta* pExtMeta,
                                              const SNodesRemoteSQLCtx* pCtx,
                                              int8_t precision) {
  bool isIntExpr = (pExpr != NULL && nodesIsExprNode((SNode*)pExpr) &&
                    dynIsIntegerType(((const SExprNode*)pExpr)->resType.type));
  const char* pExtColName = NULL;
  const char* pExtTypeName = NULL;
  EExtTimestampTzClass tsClass = EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;

  if (!isIntExpr && pExpr != NULL && nodesIsExprNode((SNode*)pExpr) &&
      ((const SExprNode*)pExpr)->resType.type == TSDB_DATA_TYPE_TIMESTAMP) {
    tsClass = fqResolveExprTimestampClass(pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx,
                                          &pExtColName, &pExtTypeName);
  }

  qDebug("FQ-SQL: render PG timediff operand exprType=%s isInt=%d precision=%d tsClass=%s col=%s extType=%s clientTz=%s",
         pExpr ? nodesNodeName(nodeType((SNode*)pExpr)) : "(null)", (int)isIntExpr, (int)precision,
         fqExtTimestampClassName(tsClass), pExtColName ? pExtColName : "(expr)",
         pExtTypeName ? pExtTypeName : "(unknown)",
         (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)");

  if (isIntExpr) {
    dynSQLAppendStr(s, "TO_TIMESTAMP(((");
    int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendStr(s, ")::double precision) / ");
    dynSQLAppendf(s, "%" PRId64, fqTimestampTicksPerSecond(precision));
    dynSQLAppendChar(s, ')');
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  bool literalHandled = false;
  int32_t literalCode = dynTryRenderPgTemporalDiffLiteralOperand(s, pExpr, precision, &literalHandled);
  if (literalCode != TSDB_CODE_SUCCESS) {
    return literalCode;
  }
  if (literalHandled) {
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  if (pExpr != NULL && nodesIsExprNode((SNode*)pExpr) &&
      ((const SExprNode*)pExpr)->resType.type == TSDB_DATA_TYPE_TIMESTAMP &&
      tsClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE &&
      s->tzName != NULL && s->tzName[0] != '\0') {
    bool isDateType = fqIsPgDateTypeName(pExtTypeName);
    dynSQLAppendChar(s, '(');
    if (isDateType) {
      dynSQLAppendStr(s, "CAST(");
      int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      dynSQLAppendStr(s, " AS TIMESTAMP)");
    } else {
      int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    dynSQLAppendStr(s, " AT TIME ZONE '");
    dynSQLAppendStr(s, s->tzName);
    dynSQLAppendStr(s, "')");
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  return dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
}

static int32_t dynAppendInfluxEpochAsBigintLiteral(SDynSQL* s, int64_t epoch,
                                                   int8_t precision) {
  dynSQLAppendf(s, "%" PRId64, epoch);
  qDebug("FQ-SQL: render Influx epoch literal as bigint epoch=%" PRId64
         " precision=%d",
         epoch, (int)precision);
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynTryRenderInfluxTemporalDiffEpochOperand(SDynSQL* s,
                                                          const SNode* pExpr,
                                                          int8_t fallbackPrecision,
                                                          bool* pHandled) {
  if (NULL != pHandled) {
    *pHandled = false;
  }

  const SNode* pRealExpr = fqDebugUnwrapExprNode(pExpr);
  if (NULL == pRealExpr || QUERY_NODE_VALUE != nodeType((SNode*)pRealExpr)) {
    return TSDB_CODE_SUCCESS;
  }

  const SValueNode* pVal = (const SValueNode*)pRealExpr;
  if (pVal->isNull) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t epoch = 0;
  int8_t precision = fallbackPrecision;
  bool canRender = false;

  if (TSDB_DATA_TYPE_TIMESTAMP == pVal->node.resType.type) {
    epoch = pVal->datum.i;
    precision = fqNormalizeTimePrecision((int8_t)pVal->node.resType.precision, fallbackPrecision);
    canRender = true;
    qDebug("FQ-SQL: TIMEDIFF Influx literal timestamp node uses absolute epoch=%" PRId64
           " precision=%d literal=%s",
           epoch, (int)precision, pVal->literal ? pVal->literal : "(null)");
  } else if (dynIsIntegerType(pVal->node.resType.type)) {
    epoch = pVal->datum.i;
    canRender = true;
    qDebug("FQ-SQL: TIMEDIFF Influx literal integer node uses epoch=%" PRId64
           " precision=%d literal=%s",
           epoch, (int)precision, pVal->literal ? pVal->literal : "(null)");
  }

  if (!canRender) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL != pHandled) {
    *pHandled = true;
  }
  return dynAppendInfluxEpochAsBigintLiteral(s, epoch, precision);
}

static int32_t dynRenderInfluxTemporalDiffEpochOperand(SDynSQL* s, const SNode* pExpr,
                                                       const SNode* pOther,
                                                       const SExtTableMeta* pExtMeta,
                                                       const SNodesRemoteSQLCtx* pCtx,
                                                       int8_t precision) {
  int8_t literalPrecision =
      fqResolveTimestampExprPrecision(pOther, fqNormalizeTimePrecision(precision, TSDB_TIME_PRECISION_NANO));
  bool literalHandled = false;
  int32_t literalCode =
      dynTryRenderInfluxTemporalDiffEpochOperand(s, pExpr, literalPrecision, &literalHandled);
  if (TSDB_CODE_SUCCESS != literalCode) {
    return literalCode;
  }
  if (literalHandled) {
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  dynSQLAppendStr(s, "CAST(");
  int32_t code = dynAppendExprTS(s, pExpr, pOther, EXT_SQL_DIALECT_INFLUXQL, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  dynSQLAppendStr(s, " AS BIGINT)");
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static bool fqTryResolvePgEpochSource(const SNode* pExpr, const SNodesRemoteSQLCtx* pCtx,
                                      const SNode** ppEpochArg, int8_t* pPrecision,
                                      EExtTimestampTzClass* pTsClass, const char** ppExtTypeName) {
  if (ppEpochArg != NULL) {
    *ppEpochArg = NULL;
  }
  if (pPrecision != NULL) {
    *pPrecision = TSDB_TIME_PRECISION_MICRO;
  }
  if (pTsClass != NULL) {
    *pTsClass = EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }
  if (ppExtTypeName != NULL) {
    *ppExtTypeName = NULL;
  }
  if (pExpr == NULL) {
    return false;
  }

  const SNode* pRealExpr = fqDebugUnwrapExprNode(pExpr);
  if (pRealExpr == NULL || nodeType((SNode*)pRealExpr) != QUERY_NODE_FUNCTION) {
    return false;
  }

  const SFunctionNode* pFunc = (const SFunctionNode*)pRealExpr;
  if ((EFunctionType)pFunc->funcType != FUNCTION_TYPE_CAST ||
      pFunc->pParameterList == NULL || LIST_LENGTH(pFunc->pParameterList) < 1) {
    return false;
  }

  const SNode* pArg0 = nodesListGetNode(pFunc->pParameterList, 0);
  if (pArg0 == NULL || !nodesIsExprNode((SNode*)pArg0) ||
      ((const SExprNode*)pArg0)->resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return false;
  }

  const char* pExtColName = NULL;
  const char* pExtTypeName = NULL;
  EExtTimestampTzClass tsClass =
      fqResolveExprTimestampClass(pArg0, EXT_SQL_DIALECT_POSTGRES, NULL, pCtx,
                                  &pExtColName, &pExtTypeName);
  int8_t precision = fqResolveCastEpochPrecision(pFunc, pArg0, EXT_SQL_DIALECT_POSTGRES);

  if (ppEpochArg != NULL) {
    *ppEpochArg = pArg0;
  }
  if (pPrecision != NULL) {
    *pPrecision = precision;
  }
  if (pTsClass != NULL) {
    *pTsClass = tsClass;
  }
  if (ppExtTypeName != NULL) {
    *ppExtTypeName = pExtTypeName;
  }

  qDebug("FQ-SQL: resolve PG epoch source success func=%s precision=%d tsClass=%s extType=%s srcCol=%s",
         pFunc->functionName, (int)precision, fqExtTimestampClassName(tsClass),
         pExtTypeName ? pExtTypeName : "(unknown)", pExtColName ? pExtColName : "(expr)");
  return true;
}

static int32_t dynRenderPgTimestampEpochCast(SDynSQL* s, const SFunctionNode* pFunc, const SNode* pArg0,
                                             const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  const char* pExtColName = NULL;
  const char* pExtTypeName = NULL;
  EExtTimestampTzClass tsClass = fqResolveExprTimestampClass(pArg0, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx,
                                                             &pExtColName, &pExtTypeName);
  int8_t precision = fqResolveCastEpochPrecision(pFunc, pArg0, EXT_SQL_DIALECT_POSTGRES);
  int64_t ticksPerSecond = fqTimestampTicksPerSecond(precision);
  const char* pTargetType = dynSqlCastType(EXT_SQL_DIALECT_POSTGRES, pFunc->node.resType.type);
  bool useClientTzWrap = (tsClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE &&
                          s->tzName != NULL && s->tzName[0] != '\0');

  qDebug("FQ-SQL: render PG CAST(ts->int) func=%s targetType=%u(%s) precision=%d ticksPerSecond=%" PRId64
         " tsClass=%s col=%s extType=%s clientTz=%s useClientTzWrap=%d",
         pFunc->functionName, (unsigned)pFunc->node.resType.type, pTargetType,
         (int)precision, ticksPerSecond, fqExtTimestampClassName(tsClass),
         pExtColName ? pExtColName : "(expr)",
         pExtTypeName ? pExtTypeName : "(unknown)",
         (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)",
         (int)useClientTzWrap);

  dynSQLAppendStr(s, "CAST(");
  int32_t code = dynRenderPgEpochExtractExpr(s, pArg0, pExtMeta, pCtx, tsClass, pExtTypeName);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  char factorBuf[32] = {0};
  (void)snprintf(factorBuf, sizeof(factorBuf), "%" PRId64, ticksPerSecond);
  dynSQLAppendStr(s, " * ");
  dynSQLAppendStr(s, factorBuf);
  dynSQLAppendStr(s, " AS ");
  dynSQLAppendStr(s, pTargetType);
  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static bool dynIsHiddenGroupConcatOrderArg(const SNode* pArg, const SExtTableMeta* pExtMeta) {
  if (pArg == NULL) {
    return false;
  }

  const SNode* pExpr = fqDebugUnwrapExprNode(pArg);
  if (pExpr == NULL || QUERY_NODE_COLUMN != nodeType((SNode*)pExpr)) {
    return false;
  }

  const SColumnNode* pCol = (const SColumnNode*)pExpr;
  const SExprNode* pExprNode = (const SExprNode*)pExpr;
  if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    return true;
  }

  if (pExprNode->resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return false;
  }

  const SExtColumnDef* pPkCol = findPrimaryKeyExtCol(pExtMeta);
  if (pPkCol != NULL && pPkCol->colName[0] != '\0' && 0 == strcmp(pCol->colName, pPkCol->colName)) {
    return true;
  }

  return false;
}

static int32_t dynRenderGroupConcatRowExpr(SDynSQL* s, const SFunctionNode* pFunc, int32_t valueArgCount,
                                           EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                                           const SNodesRemoteSQLCtx* pCtx) {
  if (pFunc == NULL || valueArgCount <= 0) {
    qError("FQ-SQL: GROUP_CONCAT row expr invalid func=%p valueArgCount=%d", pFunc, valueArgCount);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  if (1 == valueArgCount) {
    return dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
  }

  dynSQLAppendStr(s, "CONCAT(");
  for (int32_t i = 0; i < valueArgCount; ++i) {
    if (i > 0) {
      dynSQLAppendStr(s, ", ");
    }

    int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, i), dialect, pExtMeta, pCtx);
    if (code) {
      return code;
    }
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static bool dynFuncArgInt64Equals(const SFunctionNode* pFunc, int32_t argIdx, int64_t expected) {
  if (pFunc == NULL || pFunc->pParameterList == NULL || argIdx < 0 ||
      argIdx >= LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }

  SNode* pArg = nodesListGetNode(pFunc->pParameterList, argIdx);
  if (pArg == NULL || nodeType(pArg) != QUERY_NODE_VALUE) {
    return false;
  }

  const SValueNode* pVal = (const SValueNode*)pArg;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return false;
  }

  return pVal->datum.i == expected;
}

static const SFunctionNode* findHavingAggFuncBySlot(const SNodeList* pAggFuncs, const SColumnNode* pCol) {
  if (pAggFuncs == NULL || pCol == NULL) {
    return NULL;
  }

  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAggFuncs) {
    const SFunctionNode* pFunc = NULL;
    if (nodeType(pFuncNode) == QUERY_NODE_TARGET) {
      const STargetNode* pTarget = (const STargetNode*)pFuncNode;
      if (pTarget->pExpr != NULL && nodeType(pTarget->pExpr) == QUERY_NODE_FUNCTION) {
        pFunc = (const SFunctionNode*)pTarget->pExpr;
      }
    } else if (nodeType(pFuncNode) == QUERY_NODE_FUNCTION) {
      pFunc = (const SFunctionNode*)pFuncNode;
    }

    if (pFunc == NULL) {
      continue;
    }

    if (fmIsGroupKeyFunc(pFunc->funcId)) {
      continue;
    }

    if (strcmp(pCol->colName, pFunc->node.aliasName) == 0) {
      return pFunc;
    }
  }

  return NULL;
}
static int32_t dynAppendFunctionExpr(SDynSQL* s, const SFunctionNode* pFunc,
                                     EExtSQLDialect dialect,
                                     const SExtTableMeta* pExtMeta,
                                     const SNodesRemoteSQLCtx* pCtx);
static const char* fqSelectAlias(const SExprNode* pExpr, const SNodesRemoteSQLCtx* pCtx);
static const char* fqColumnOutputAlias(const SColumnNode* pCol, const SNodesRemoteSQLCtx* pCtx);
static int32_t fqTicksToMysqlMicros(int64_t ticks, int8_t precision, int64_t* pMicros);
static int32_t fqTicksToPgMicros(int64_t ticks, int8_t precision, int64_t* pMicros);
static int32_t fqTicksToInfluxNanos(int64_t ticks, int8_t precision, int64_t* pNanos);
static const char* fqFromChildSelectAlias(const SExprNode* pExpr);
static const char* fqFindEquivalentInputAlias(const SExprNode* pExpr,
                                              const SNodeList* pInputSelectExprs);
static const char* fqVisibleOutputAlias(const SExprNode* pExpr);
static bool fqShouldUseCurrentSelectRename(const SExprNode* pExpr,
                                           const SNodesRemoteSQLCtx* pCtx);
static const char* fqResolveOutputRefAlias(const SColumnNode* pCol,
                                           const SNodesRemoteSQLCtx* pCtx,
                                           const SExprNode** ppMatchedExpr);

static bool dynIsIntegerType(uint8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return true;
    default:
      return false;
  }
}

static bool dynIsFloatingType(uint8_t type) {
  return type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE;
}

static const char* dynFloatingCastType(EExtSQLDialect dialect, uint8_t type) {
  if (type == TSDB_DATA_TYPE_FLOAT) {
    return "FLOAT";
  }

  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
      return "DOUBLE";
    case EXT_SQL_DIALECT_POSTGRES:
    case EXT_SQL_DIALECT_INFLUXQL:
    default:
      return "DOUBLE PRECISION";
  }
}

static const char* dynSqlCastType(EExtSQLDialect dialect, uint8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR:
      return (dialect == EXT_SQL_DIALECT_POSTGRES) ? "TEXT" : "CHAR";
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return (dialect == EXT_SQL_DIALECT_MYSQL) ? "SIGNED" : "BIGINT";
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
      return (dialect == EXT_SQL_DIALECT_MYSQL) ? "SIGNED" : "INTEGER";
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      return "SMALLINT";
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      return (dialect == EXT_SQL_DIALECT_MYSQL) ? "SIGNED" : "SMALLINT";
    case TSDB_DATA_TYPE_FLOAT:
      return "FLOAT";
    case TSDB_DATA_TYPE_DOUBLE:
      return (dialect == EXT_SQL_DIALECT_MYSQL) ? "DOUBLE" : "DOUBLE PRECISION";
    case TSDB_DATA_TYPE_BOOL:
      return "BOOLEAN";
    case TSDB_DATA_TYPE_TIMESTAMP:
      return (dialect == EXT_SQL_DIALECT_MYSQL) ? "DATETIME(3)" : "TIMESTAMP";
    default:
      return "CHAR";
  }
}

static bool dynIsComparisonOp(EOperatorType opType) {
  switch (opType) {
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
      return true;
    default:
      return false;
  }
}

static bool dynIsPgRegexNumericComparableType(int32_t type) {
  return dynIsIntegerType(type) || dynIsFloatingType(type) || type == TSDB_DATA_TYPE_BOOL;
}

static bool dynIsVarLikeType(int32_t type) {
  return type == TSDB_DATA_TYPE_VARCHAR || type == TSDB_DATA_TYPE_NCHAR;
}

static bool dynNeedsPgComparisonConversion(int32_t leftType, int32_t rightType, int32_t convertType) {
  if (convertType <= 0 || leftType == rightType) {
    return false;
  }

  if (leftType == TSDB_DATA_TYPE_TIMESTAMP || rightType == TSDB_DATA_TYPE_TIMESTAMP) {
    // Keep using the dedicated timestamp literal/expr rendering path.
    return false;
  }

  if ((dynIsIntegerType(leftType) || dynIsFloatingType(leftType)) &&
      (dynIsIntegerType(rightType) || dynIsFloatingType(rightType))) {
    return false;
  }

  if (dynIsVarLikeType(leftType) && dynIsPgRegexNumericComparableType(rightType)) {
    return true;
  }

  if (dynIsVarLikeType(rightType) && dynIsPgRegexNumericComparableType(leftType)) {
    return true;
  }

  if ((leftType == TSDB_DATA_TYPE_BOOL &&
       (dynIsIntegerType(rightType) || dynIsFloatingType(rightType))) ||
      (rightType == TSDB_DATA_TYPE_BOOL &&
       (dynIsIntegerType(leftType) || dynIsFloatingType(leftType)))) {
    return true;
  }

  return false;
}

static const char* dynPgNumericPrefixRegex(int32_t targetType) {
  if (dynIsFloatingType(targetType)) {
    return "^\\s*[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?";
  }

  return "^\\s*[+-]?\\d+";
}

static int32_t dynAppendPgParsedNumericExpr(SDynSQL* s, const SNode* pExpr, int32_t targetType,
                                            const SExtTableMeta* pExtMeta,
                                            const SNodesRemoteSQLCtx* pCtx) {
  const char* pCastType = dynSqlCastType(EXT_SQL_DIALECT_POSTGRES, targetType);
  const char* pRegex = dynPgNumericPrefixRegex(targetType);

  dynSQLAppendStr(s, "COALESCE(CAST(NULLIF(SUBSTRING(");
  int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  dynSQLAppendStr(s, " FROM '");
  dynSQLAppendStr(s, pRegex);
  dynSQLAppendStr(s, "'), '') AS ");
  dynSQLAppendStr(s, pCastType);
  dynSQLAppendStr(s, "), 0");
  if (targetType == TSDB_DATA_TYPE_DOUBLE) {
    dynSQLAppendStr(s, ".0");
  }
  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynAppendPgComparisonOperand(SDynSQL* s, const SNode* pExpr, int32_t targetType,
                                            const SExtTableMeta* pExtMeta,
                                            const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pExpr) {
    qError("FQ-SQL: PG comparison operand got NULL targetType=%d", targetType);
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t exprType = ((const SExprNode*)pExpr)->resType.type;
  if (exprType == targetType || 0 == targetType) {
    return dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
  }

  if ((exprType == TSDB_DATA_TYPE_VARCHAR || exprType == TSDB_DATA_TYPE_NCHAR) &&
      dynIsPgRegexNumericComparableType(targetType)) {
    qDebug("FQ-SQL: PG comparison operand uses TD-compatible var->numeric parse exprType=%d targetType=%d nodeType=%d",
           exprType, targetType, (int)nodeType((SNode*)pExpr));
    return dynAppendPgParsedNumericExpr(s, pExpr, targetType, pExtMeta, pCtx);
  }

  if (exprType == TSDB_DATA_TYPE_BOOL && dynIsIntegerType(targetType)) {
    dynSQLAppendStr(s, "CAST(");
    int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendStr(s, " AS ");
    dynSQLAppendStr(s, dynSqlCastType(EXT_SQL_DIALECT_POSTGRES, targetType));
    dynSQLAppendChar(s, ')');
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  dynSQLAppendStr(s, "CAST(");
  int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  dynSQLAppendStr(s, " AS ");
  dynSQLAppendStr(s, dynSqlCastType(EXT_SQL_DIALECT_POSTGRES, targetType));
  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynAppendPgConvertedComparison(SDynSQL* s, const SOperatorNode* pOp, const char* opStr,
                                              const SExtTableMeta* pExtMeta,
                                              const SNodesRemoteSQLCtx* pCtx) {
  int32_t leftType = (NULL != pOp->pLeft) ? ((const SExprNode*)pOp->pLeft)->resType.type : 0;
  int32_t rightType = (NULL != pOp->pRight) ? ((const SExprNode*)pOp->pRight)->resType.type : 0;
  int32_t convertType = vectorGetConvertType(leftType, rightType);

  if (!dynNeedsPgComparisonConversion(leftType, rightType, convertType)) {
    return TSDB_CODE_SUCCESS;
  }

  qDebug("FQ-SQL: PG comparison uses vectorGetConvertType opType=%d leftType=%d rightType=%d convertType=%d",
         (int)pOp->opType, leftType, rightType, convertType);

  dynSQLAppendChar(s, '(');
  int32_t code = dynAppendPgComparisonOperand(s, pOp->pLeft, convertType, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  dynSQLAppendStr(s, opStr);
  code = dynAppendPgComparisonOperand(s, pOp->pRight, convertType, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static bool dynExprIsNcharType(const SNode* pNode) {
  return NULL != pNode && ((const SExprNode*)pNode)->resType.type == TSDB_DATA_TYPE_NCHAR;
}

static bool dynFuncArgIsNcharType(const SFunctionNode* pFunc, int32_t argIdx) {
  if (NULL == pFunc || NULL == pFunc->pParameterList || argIdx < 0 ||
      argIdx >= LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }

  return dynExprIsNcharType(nodesListGetNode(pFunc->pParameterList, argIdx));
}

static int32_t dynRejectFuncArg0NcharIfUnsupported(const SFunctionNode* pFunc, EExtSQLDialect dialect) {
  if (!dynFuncArgIsNcharType(pFunc, 0)) {
    return TSDB_CODE_SUCCESS;
  }

  if (fqFuncArg0NcharIsPushable(pFunc->funcId, dialect, NULL)) {
    return TSDB_CODE_SUCCESS;
  }

  qError("FQ-SQL: %s over NCHAR disabled by function capability matrix to preserve TDengine UCS4 byte semantics dialect=%s funcId=%d",
         pFunc->functionName, dialectName(dialect), pFunc->funcId);
  return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
}

static bool dynShouldCoerceDivisionToFloat(const SOperatorNode* pOp, EExtSQLDialect dialect) {
  if (NULL == pOp || NULL == pOp->pLeft || NULL == pOp->pRight) {
    return false;
  }

  if (dialect == EXT_SQL_DIALECT_MYSQL) {
    return false;
  }

  const SExprNode* pExpr = (const SExprNode*)pOp;
  const SExprNode* pLeft = (const SExprNode*)pOp->pLeft;
  const SExprNode* pRight = (const SExprNode*)pOp->pRight;

  return dynIsFloatingType(pExpr->resType.type) &&
         dynIsIntegerType(pLeft->resType.type) &&
         dynIsIntegerType(pRight->resType.type);
}

static int32_t dynAppendFloatCoercedDivisionExpr(SDynSQL* s, const SOperatorNode* pOp,
                                                 EExtSQLDialect dialect,
                                                 const SExtTableMeta* pExtMeta,
                                                 const SNodesRemoteSQLCtx* pCtx) {
  const SExprNode* pExpr = (const SExprNode*)pOp;

  qDebug("FQ-SQL: coerce integer division to floating semantics dialect=%s resultType=%d leftType=%d rightType=%d",
         dialectName(dialect), pExpr->resType.type,
         ((const SExprNode*)pOp->pLeft)->resType.type,
         ((const SExprNode*)pOp->pRight)->resType.type);

  dynSQLAppendStr(s, "(CAST(");
  int32_t code = dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  dynSQLAppendStr(s, " AS ");
  dynSQLAppendStr(s, dynFloatingCastType(dialect, pExpr->resType.type));
  dynSQLAppendStr(s, ") / ");

  code = dynAppendExpr(s, pOp->pRight, dialect, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

// Render an integer value as an ISO-8601 timestamp string literal.
// Used when an integer value is compared against a TIMESTAMP column in a WHERE clause.
static void dynAppendIntAsTimestamp(SDynSQL* s, int64_t ts, int8_t precision,
                                   EExtSQLDialect dialect) {
  dynAppendTimestampLiteral(s, ts, precision, dialect);
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
    if (prec == 0) {
      prec = TSDB_TIME_PRECISION_MILLI;
    }

    if (dialect == EXT_SQL_DIALECT_POSTGRES) {
      return dynRenderPgEpochAsTimestampExpr(s, pExpr, pExtMeta, pCtx, prec, s->tzName);
    }

    dynAppendIntAsTimestamp(s, pVal->datum.i, prec, dialect);
    return TSDB_CODE_SUCCESS;
  }
  return dynAppendExpr(s, pExpr, dialect, pExtMeta, pCtx);
}

// ---------------------------------------------------------------------------
// dynAppendRemoteValueList — resolve REMOTE_VALUE_LIST and emit IN (...) list
// ---------------------------------------------------------------------------
static int32_t dynAppendRemoteListLiteral(SDynSQL* s, const SRemoteValueListNode* pRemote,
                                          const void* pKey, size_t keyLen, void* charsetCxt,
                                          EExtSQLDialect dialect, int8_t tsPrecision) {
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
      return dynAppendRemoteVarLiteral(s, pRemote->filterValueType, pKey, keyLen, charsetCxt, dialect);
    default:
      qError("FQ-SQL: dynAppendRemoteListLiteral unsupported key type=%d", pRemote->filterValueType);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendResolvedRemoteValueList(SDynSQL* s, const SRemoteValueListNode* pRemote,
                                                EExtSQLDialect dialect, int8_t tsPrecision) {
  if (NULL == pRemote) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Empty set — "col IN ()" is invalid SQL; keep the historical FALSE
  // fallback for callers that render the list as a full standalone fragment.
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

    int32_t litCode = dynAppendRemoteListLiteral(s, pRemote, pKey, keyLen, NULL, dialect, tsPrecision);
    if (litCode != TSDB_CODE_SUCCESS) {
      qError("FQ-SQL: dynAppendResolvedRemoteValueList unsupported hash key type=%d",
             pRemote->filterValueType);
      taosHashCancelIterate(pRemote->pHashFilter, pVal);
      return litCode;
    }

    pVal = taosHashIterate(pRemote->pHashFilter, pVal);
  }
  dynSQLAppendChar(s, ')');
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

  return dynAppendResolvedRemoteValueList(s, pRemote, dialect, tsPrecision);
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
    qError("FQ-SQL: dynAppendInfluxRemoteInChain: no resolve ctx (pCtx=%p fp=%p)",
           pCtx, pCtx ? pCtx->fp : NULL);
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
    code = dynAppendRemoteListLiteral(s, pRemote, pKey, keyLen, pOp->charsetCxt, dialect, leftPrec);
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

// Map an SQL dialect back to the EExtSourceType expected by
// nodesRenderInSubqueryBody (used for v2 transform-mode inner-SELECT rendering).
static int8_t dialectToExtSourceType(EExtSQLDialect dialect) {
  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:    return EXT_SOURCE_MYSQL;
    case EXT_SQL_DIALECT_POSTGRES: return EXT_SOURCE_POSTGRESQL;
    case EXT_SQL_DIALECT_INFLUXQL: return EXT_SOURCE_INFLUXDB;
    default:                       return -1;
  }
}

// v2 two-state subquery body rendering.  Given a REMOTE_* node's render mode,
// its stored original-SQL string, and its attached inner logic plan, append the
// inner subquery SQL into `s` (without the surrounding parentheses / predicate
// keyword — the caller wraps it).
//   DIRECT_SQL    → append pOrigSql verbatim (user text; same-source, no xlate)
//   ORIG_TYPE_2B  → render pInnerPlan via nodesRemotePlanToSQL()
//                   (function mapping + schema/db/remoteTableName translation)
// Reaching toSQL means the collect phase decided this is pushable, so the
// required source (string or plan) MUST be present; missing it is a BUG.
static int32_t dynAppendSubqBodyByMode(SDynSQL* s, EFqSubqRenderMode mode,
                                       const char* pOrigSql, const SNode* pInnerPlan,
                                       EExtSQLDialect dialect, int32_t subQIdx) {
  qDebug("FQ-SQL-SUBQ: render body subQIdx=%d mode=%d dialect=%s hasOrigSql=%d innerPlan=%p innerType=%s",
         subQIdx, (int)mode, dialectName(dialect),
         (NULL != pOrigSql && pOrigSql[0] != '\0'), pInnerPlan,
         pInnerPlan ? nodesNodeName(nodeType((SNode*)pInnerPlan)) : "(null)");
  if (mode == FQ_SUBQ_RENDER_DIRECT_SQL) {
    if (NULL == pOrigSql) {
      qError("FQ-SQL: subquery node missing original SQL (DIRECT_SQL, subQIdx=%d)", subQIdx);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    qDebug("FQ-SQL-SUBQ: use original SQL subQIdx=%d len=%d sql=[%s]",
           subQIdx, (int32_t)strlen(pOrigSql), pOrigSql);
    dynSQLAppendStr(s, pOrigSql);
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }
  // Transform modes: render the inner LOGIC plan with dialect transforms (2b).
  if (NULL == pInnerPlan) {
    qError("FQ-SQL: subquery node missing inner logic plan (transform mode=%d, subQIdx=%d)",
           (int)mode, subQIdx);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  ENodeType innerType = nodeType(pInnerPlan);
  if (innerType < QUERY_NODE_LOGIC_PLAN_SCAN || innerType > QUERY_NODE_LOGIC_PLAN_TAG_REF_SOURCE) {
    qError("FQ-SQL: subquery node has invalid inner logic type=%d (mode=%d, subQIdx=%d)", (int)innerType,
           (int)mode, subQIdx);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  char*   pBody = NULL;
  int32_t code = nodesRemotePlanToSQL((const SLogicNode*)pInnerPlan, dialectToExtSourceType(dialect), NULL, &pBody);
  if (code != TSDB_CODE_SUCCESS || NULL == pBody) {
    qError("FQ-SQL: render inner subquery logic failed code=0x%x (mode=%d subQIdx=%d innerType=%d)",
           code, (int)mode, subQIdx, (int)innerType);
    return code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  qDebug("FQ-SQL-SUBQ: rendered logic body subQIdx=%d len=%d sql=[%s]",
         subQIdx, (int32_t)strlen(pBody), pBody);
  dynSQLAppendStr(s, pBody);
  taosMemoryFree(pBody);
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynAppendOperatorExpr(SDynSQL* s, const SOperatorNode* pOp, EExtSQLDialect dialect,
                                      const SExtTableMeta* pExtMeta,
                                      const SNodesRemoteSQLCtx* pCtx) {
  // Rewritten EXISTS/NOT EXISTS shape:
  //   EXISTS(subq)     -> REMOTE_ZERO_ROWS(subq) > 0
  //   NOT EXISTS(subq) -> REMOTE_ZERO_ROWS(subq) <= 0
  // The original inner subquery SQL is preserved on REMOTE_ZERO_ROWS.val.literal
  // at parse time.  Reaching toSQL means the collect phase decided EXISTS is
  // pushable, so the literal MUST be present; missing it is a planner BUG.
  if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_REMOTE_ZERO_ROWS &&
      dynIsZeroConstValue(pOp->pRight) &&
      (pOp->opType == OP_TYPE_GREATER_THAN || pOp->opType == OP_TYPE_LOWER_EQUAL)) {
    const SRemoteZeroRowsNode* pRZ = (const SRemoteZeroRowsNode*)pOp->pLeft;
    dynSQLAppendStr(s, (pOp->opType == OP_TYPE_GREATER_THAN) ? "EXISTS (" : "NOT EXISTS (");
    // v2 two-state: DIRECT_SQL inlines val.literal (original SQL); ORIG_TYPE_2B
    // renders the attached inner logic plan with dialect transforms (2b).
    int32_t ec = dynAppendSubqBodyByMode(s, pRZ->renderMode, pRZ->val.literal,
                                         pRZ->pRemotePlan, dialect, pRZ->subQIdx);
    if (ec != TSDB_CODE_SUCCESS) return ec;
    dynSQLAppendChar(s, ')');
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  // Quantified comparison ALL/ANY/SOME pushdown:
  //   left op ALL|ANY (subq)
  // Parser rewrites ALL/ANY into a MIN/MAX scalar subquery and stores the
  // ORIGINAL (un-wrapped) subquery SQL on REMOTE_ROW.pSubquerySql.  The original
  // quantifier is recovered from (opType, isMinVal):
  //   >,>=  MIN->ANY  MAX->ALL      <,<=  MIN->ALL  MAX->ANY
  // Reaching toSQL means the collect phase decided this is pushable, so the
  // original SQL MUST be present; missing it is a planner BUG.
  if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_ROW &&
      (pOp->opType == OP_TYPE_GREATER_THAN || pOp->opType == OP_TYPE_GREATER_EQUAL ||
       pOp->opType == OP_TYPE_LOWER_THAN || pOp->opType == OP_TYPE_LOWER_EQUAL)) {
    const SRemoteRowNode* pRow = (const SRemoteRowNode*)pOp->pRight;
    bool isGreater = (pOp->opType == OP_TYPE_GREATER_THAN || pOp->opType == OP_TYPE_GREATER_EQUAL);
    const char* quant = NULL;
    if (isGreater) {
      quant = pRow->isMinVal ? " ANY " : " ALL ";
    } else {
      quant = pRow->isMinVal ? " ALL " : " ANY ";
    }
    const char* opStr2 = NULL;
    switch (pOp->opType) {
      case OP_TYPE_GREATER_THAN:  opStr2 = ">";  break;
      case OP_TYPE_GREATER_EQUAL: opStr2 = ">="; break;
      case OP_TYPE_LOWER_THAN:    opStr2 = "<";  break;
      case OP_TYPE_LOWER_EQUAL:   opStr2 = "<="; break;
      default: break;
    }
    dynSQLAppendChar(s, '(');
    (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
    dynSQLAppendChar(s, ' ');
    dynSQLAppendStr(s, opStr2);
    dynSQLAppendStr(s, quant);
    dynSQLAppendChar(s, '(');
    // v2 two-state: DIRECT_SQL inlines pSubquerySql (original SQL); ORIG_TYPE_2B
    // renders the attached inner logic plan with dialect transforms (2b).
    // The quantified predicate form (op ALL|ANY) is always preserved here.
    int32_t ec = dynAppendSubqBodyByMode(s, pRow->renderMode, pRow->pSubquerySql,
                                         pRow->pRemotePlan, dialect, pRow->subQIdx);
    if (ec != TSDB_CODE_SUCCESS) return ec;
    dynSQLAppendStr(s, "))");
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  const char* opStr = NULL;
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:         opStr = " = ";    break;
    case OP_TYPE_NOT_EQUAL:
      opStr = (pOp->flag & OPERATOR_FLAG_BANG_NE) ? " != " : " <> ";
      break;
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
    case OP_TYPE_BIT_AND:       opStr = " & ";        break;
    case OP_TYPE_BIT_OR:        opStr = " | ";        break;
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
      // InfluxDB: DataFusion still rejects IN-subquery in physical planning, so
      // REMOTE_VALUE_LIST must always be expanded to an OR-chain. Constant lists
      // keep the legacy timestamp-only rewrite to avoid broad SQL churn.
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_VALUE_LIST) {
          return dynAppendInfluxRemoteInChain(s, pOp,
                                              (const SRemoteValueListNode*)pOp->pRight,
                                              false, dialect, pExtMeta, pCtx);
        }
        bool isTs = pOp->pLeft &&
                    ((const SExprNode*)pOp->pLeft)->resType.type == TSDB_DATA_TYPE_TIMESTAMP;
        if (isTs && pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_NODE_LIST) {
          return dynAppendInfluxNodeInChain(s, pOp,
                                            (const SNodeListNode*)pOp->pRight,
                                            false, dialect, pExtMeta, pCtx);
        }
      }
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      if ((pOp->flag & OPERATOR_FLAG_NEGATIVE_OP) &&
          pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_VALUE_LIST) {
        const SRemoteValueListNode* pRemote = (const SRemoteValueListNode*)pOp->pRight;
        // Parser rewrites  expr <> ANY|SOME (subq)  into
        //   NOT (expr IN REMOTE_VALUE_LIST(subq))
        // for local execution.  When the same-source quantified subquery is
        // fully pushable, remote SQL must restore the quantified predicate
        // form instead of leaking the local rewrite shape.
        dynSQLAppendStr(s, " <> ANY (");
        int32_t ec = dynAppendSubqBodyByMode(s, pRemote->renderMode, pRemote->inSubquerySql,
                                             pRemote->pRemotePlan, dialect, pRemote->subQIdx);
        if (ec != TSDB_CODE_SUCCESS) return ec;
        dynSQLAppendStr(s, ")");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      dynSQLAppendStr(s, " IN ");
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_NODE_LIST) {
        return dynAppendNodeList(s, (const SNodeListNode*)pOp->pRight, dialect, pExtMeta, pCtx);
      }
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_REMOTE_VALUE_LIST) {
        const SRemoteValueListNode* pRemote = (const SRemoteValueListNode*)pOp->pRight;
        // v2 two-state: DIRECT_SQL inlines inSubquerySql (original SQL);
        // ORIG_TYPE_2B renders the attached inner logic plan with dialect
        // transforms (2b: function mapping + schema/db/remoteTableName xlate).
        dynSQLAppendStr(s, "(");
        int32_t ec = dynAppendSubqBodyByMode(s, pRemote->renderMode, pRemote->inSubquerySql,
                                             pRemote->pRemotePlan, dialect, pRemote->subQIdx);
        if (ec != TSDB_CODE_SUCCESS) return ec;
        dynSQLAppendStr(s, ")");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      qError("FQ-SQL: OP_TYPE_IN unsupported right node type=%d", pOp->pRight ? nodeType(pOp->pRight) : -1);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    case OP_TYPE_NOT_IN: {
      // col NOT IN (constant list) or col NOT IN REMOTE_VALUE_LIST(subquery)
      if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_NODE_LIST) {
        bool isTs2 = dialect == EXT_SQL_DIALECT_INFLUXQL && pOp->pLeft &&
                     ((const SExprNode*)pOp->pLeft)->resType.type == TSDB_DATA_TYPE_TIMESTAMP;
        if (isTs2) {
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

      if (pOp->flag & OPERATOR_FLAG_NEGATIVE_OP) {
        const SRemoteValueListNode* pRemote = (const SRemoteValueListNode*)pOp->pRight;
        // Parser rewrites  expr = ALL (subq)  into
        //   NOT (expr NOT IN REMOTE_VALUE_LIST(subq))
        // for local execution.  When the quantified subquery is fully
        // pushable, restore the original quantified predicate in remote SQL.
        (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        dynSQLAppendStr(s, " = ALL (");
        int32_t ec = dynAppendSubqBodyByMode(s, pRemote->renderMode, pRemote->inSubquerySql,
                                             pRemote->pRemotePlan, dialect, pRemote->subQIdx);
        if (ec != TSDB_CODE_SUCCESS) return ec;
        dynSQLAppendStr(s, ")");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }

      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        return dynAppendInfluxRemoteInChain(s, pOp,
                    (const SRemoteValueListNode*)pOp->pRight,
                    true, dialect, pExtMeta, pCtx);
      }

      const SRemoteValueListNode* pRemote = (const SRemoteValueListNode*)pOp->pRight;
      // v2 two-state: DIRECT_SQL inlines inSubquerySql (original SQL);
      // ORIG_TYPE_2B renders the attached inner logic plan with dialect
      // transforms (2b).
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " NOT IN (");
      {
        int32_t ec = dynAppendSubqBodyByMode(s, pRemote->renderMode, pRemote->inSubquerySql,
                                             pRemote->pRemotePlan, dialect, pRemote->subQIdx);
        if (ec != TSDB_CODE_SUCCESS) return ec;
      }
      dynSQLAppendStr(s, ")");
      return s->err ? s->err : TSDB_CODE_SUCCESS;
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
    case OP_TYPE_IS_TRUE:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS TRUE)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_FALSE:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS FALSE)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_UNKNOWN:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS UNKNOWN)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_NOT_TRUE:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS NOT TRUE)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_NOT_FALSE:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS NOT FALSE)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_NOT_UNKNOWN:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
      dynSQLAppendStr(s, " IS NOT UNKNOWN)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_JSON_GET_VALUE: {
      // DS §5.3.3.1.5: MySQL → JSON_EXTRACT(col, '$.key'); PG → col->>'key'
      if (dialect == EXT_SQL_DIALECT_MYSQL) {
        dynSQLAppendStr(s, "JSON_EXTRACT(");
        (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        dynSQLAppendStr(s, ", '$.");
        // pRight is the key name (SValueNode string)
        if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_VALUE) {
          const SValueNode* pKey = (const SValueNode*)pOp->pRight;
          if (pKey->datum.p) {
            dynSQLAppendLen(s, varDataVal(pKey->datum.p), varDataLen(pKey->datum.p));
          }
        }
        dynSQLAppendStr(s, "')");
      } else {
        // PG: col->>'key'
        (void)dynAppendExpr(s, pOp->pLeft, dialect, pExtMeta, pCtx);
        dynSQLAppendStr(s, "->>'");
        if (pOp->pRight && nodeType(pOp->pRight) == QUERY_NODE_VALUE) {
          const SValueNode* pKey = (const SValueNode*)pOp->pRight;
          if (pKey->datum.p) {
            dynSQLAppendLen(s, varDataVal(pKey->datum.p), varDataLen(pKey->datum.p));
          }
        }
        dynSQLAppendStr(s, "'");
      }
      return TSDB_CODE_SUCCESS;
    }
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

  if (pOp->opType == OP_TYPE_DIV && dynShouldCoerceDivisionToFloat(pOp, dialect)) {
    return dynAppendFloatCoercedDivisionExpr(s, pOp, dialect, pExtMeta, pCtx);
  }

  if (NULL != opStr && dialect == EXT_SQL_DIALECT_POSTGRES &&
      dynIsComparisonOp(pOp->opType) && NULL != pOp->pLeft && NULL != pOp->pRight) {
    int32_t leftType = ((const SExprNode*)pOp->pLeft)->resType.type;
    int32_t rightType = ((const SExprNode*)pOp->pRight)->resType.type;
    int32_t convertType = vectorGetConvertType(leftType, rightType);
    if (dynNeedsPgComparisonConversion(leftType, rightType, convertType)) {
      int32_t code = dynAppendPgConvertedComparison(s, pOp, opStr, pExtMeta, pCtx);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FQ-SQL: PG comparison conversion failed opType=%d leftType=%d rightType=%d convertType=%d code=0x%x",
               (int)pOp->opType, leftType, rightType, convertType, code);
        return code;
      }
      return TSDB_CODE_SUCCESS;
    }
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

// ---------------------------------------------------------------------------
// tryRenderAsBetween — detect (col >= lb) AND (col <= ub) and render as BETWEEN
// ---------------------------------------------------------------------------
// Returns true and writes output when the 2-element AND list matches BETWEEN pattern.
static bool tryRenderAsBetween(SDynSQL* s, const SNodeList* pList, EExtSQLDialect dialect,
                                const SExtTableMeta* pExtMeta,
                                const SNodesRemoteSQLCtx* pCtx) {
  if (!pList || LIST_LENGTH(pList) != 2) return false;

  SNode* pN0 = nodesListGetNode((SNodeList*)pList, 0);
  SNode* pN1 = nodesListGetNode((SNodeList*)pList, 1);
  if (!pN0 || !pN1) return false;
  if (nodeType(pN0) != QUERY_NODE_OPERATOR || nodeType(pN1) != QUERY_NODE_OPERATOR) return false;

  const SOperatorNode* pOp0 = (const SOperatorNode*)pN0;
  const SOperatorNode* pOp1 = (const SOperatorNode*)pN1;

  const SOperatorNode* pGE = NULL;
  const SOperatorNode* pLE = NULL;
  if (pOp0->opType == OP_TYPE_GREATER_EQUAL && pOp1->opType == OP_TYPE_LOWER_EQUAL) {
    pGE = pOp0; pLE = pOp1;
  } else if (pOp0->opType == OP_TYPE_LOWER_EQUAL && pOp1->opType == OP_TYPE_GREATER_EQUAL) {
    pLE = pOp0; pGE = pOp1;
  } else {
    return false;
  }

  // Both sides must operate on the same expression
  if (!nodesEqualNode(pGE->pLeft, pLE->pLeft)) return false;

  // Render: col BETWEEN lb AND ub
  int32_t code = dynAppendExpr(s, pGE->pLeft, dialect, pExtMeta, pCtx);
  if (code) return false;
  dynSQLAppendStr(s, " BETWEEN ");
  code = dynAppendExpr(s, pGE->pRight, dialect, pExtMeta, pCtx);
  if (code) return false;
  dynSQLAppendStr(s, " AND ");
  code = dynAppendExpr(s, pLE->pRight, dialect, pExtMeta, pCtx);
  if (code) return false;

  return true;
}

// ---------------------------------------------------------------------------
// tryRenderAsNotBetween — detect (col < lb) OR (col > ub) → col NOT BETWEEN lb AND ub
// ---------------------------------------------------------------------------
static bool tryRenderAsNotBetween(SDynSQL* s, const SNodeList* pList, EExtSQLDialect dialect,
                                   const SExtTableMeta* pExtMeta,
                                   const SNodesRemoteSQLCtx* pCtx) {
  if (!pList || LIST_LENGTH(pList) != 2) return false;

  SNode* pN0 = nodesListGetNode((SNodeList*)pList, 0);
  SNode* pN1 = nodesListGetNode((SNodeList*)pList, 1);
  if (!pN0 || !pN1) return false;
  if (nodeType(pN0) != QUERY_NODE_OPERATOR || nodeType(pN1) != QUERY_NODE_OPERATOR) return false;

  const SOperatorNode* pOp0 = (const SOperatorNode*)pN0;
  const SOperatorNode* pOp1 = (const SOperatorNode*)pN1;

  // Pattern: (col < lb) OR (col > ub)
  const SOperatorNode* pLT = NULL;  // col < lb
  const SOperatorNode* pGT = NULL;  // col > ub
  if (pOp0->opType == OP_TYPE_LOWER_THAN && pOp1->opType == OP_TYPE_GREATER_THAN) {
    pLT = pOp0; pGT = pOp1;
  } else if (pOp0->opType == OP_TYPE_GREATER_THAN && pOp1->opType == OP_TYPE_LOWER_THAN) {
    pGT = pOp0; pLT = pOp1;
  } else {
    return false;
  }

  // Both sides must operate on the same expression
  if (!nodesEqualNode(pLT->pLeft, pGT->pLeft)) return false;

  // Render: col NOT BETWEEN lb AND ub
  int32_t code = dynAppendExpr(s, pLT->pLeft, dialect, pExtMeta, pCtx);
  if (code) return false;
  dynSQLAppendStr(s, " NOT BETWEEN ");
  code = dynAppendExpr(s, pLT->pRight, dialect, pExtMeta, pCtx);
  if (code) return false;
  dynSQLAppendStr(s, " AND ");
  code = dynAppendExpr(s, pGT->pRight, dialect, pExtMeta, pCtx);
  if (code) return false;

  return true;
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
      // NOT(REMOTE_ZERO_ROWS(...) > 0) — parser/planner may keep NOT EXISTS in
      // this normalized form.  Collapse it back so remote SQL preserves the
      // original NOT EXISTS predicate shape.
      if (pOp->pLeft && nodeType(pOp->pLeft) == QUERY_NODE_REMOTE_ZERO_ROWS &&
          dynIsZeroConstValue(pOp->pRight) &&
          pOp->opType == OP_TYPE_GREATER_THAN) {
        const SRemoteZeroRowsNode* pRZ = (const SRemoteZeroRowsNode*)pOp->pLeft;
        dynSQLAppendStr(s, "NOT EXISTS (");
        int32_t code = dynAppendSubqBodyByMode(s, pRZ->renderMode, pRZ->val.literal,
                                               pRZ->pRemotePlan, dialect, pRZ->subQIdx);
        if (code != TSDB_CODE_SUCCESS) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
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
  // AND with exactly 2 children: check for BETWEEN pattern
  if (pLogic->condType == LOGIC_COND_TYPE_AND) {
    SDynSQL tmp;
    dynSQLInit(&tmp);
    tmp.tzName = s->tzName;
    if (tryRenderAsBetween(&tmp, pLogic->pParameterList, dialect, pExtMeta, pCtx) && !tmp.err) {
      qDebug("FQ-SQL: render BETWEEN shortcut len=%d dialect=%s",
             (int)tmp.pos, dialectName(dialect));
      if (tmp.buf != NULL && tmp.pos > 0) {
        dynSQLAppendLen(s, tmp.buf, tmp.pos);
      }
      dynSQLFree(&tmp);
      return TSDB_CODE_SUCCESS;
    }
    dynSQLFree(&tmp);
  }
  // OR with exactly 2 children: check for NOT BETWEEN pattern
  if (pLogic->condType == LOGIC_COND_TYPE_OR) {
    SDynSQL tmp;
    dynSQLInit(&tmp);
    tmp.tzName = s->tzName;
    if (tryRenderAsNotBetween(&tmp, pLogic->pParameterList, dialect, pExtMeta, pCtx) && !tmp.err) {
      qDebug("FQ-SQL: render NOT BETWEEN shortcut len=%d dialect=%s",
             (int)tmp.pos, dialectName(dialect));
      if (tmp.buf != NULL && tmp.pos > 0) {
        dynSQLAppendLen(s, tmp.buf, tmp.pos);
      }
      dynSQLFree(&tmp);
      return TSDB_CODE_SUCCESS;
    }
    dynSQLFree(&tmp);
  }
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

// ---------------------------------------------------------------------------
// dynAppendFunctionExpr — render a SFunctionNode as remote SQL
// ---------------------------------------------------------------------------

// Helper: render function-name + '(' + comma-sep args + ')'
static int32_t dynRenderNamedFunc(SDynSQL* s, const char* funcName,
                                   const SFunctionNode* pFunc,
                                   EExtSQLDialect dialect,
                                   const SExtTableMeta* pExtMeta,
                                   const SNodesRemoteSQLCtx* pCtx) {
  dynSQLAppendStr(s, funcName);
  dynSQLAppendChar(s, '(');
  bool first = true;
  SNode* pArg = NULL;
  FOREACH(pArg, pFunc->pParameterList) {
    if (!first) dynSQLAppendStr(s, ", ");
    first = false;
    int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
    if (code) return code;
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

// Helper: render function-name(args) using only the first nKeep parameters.
// TDengine appends an internal database-precision parameter to several time
// functions (translateDate / translateAddPrec* in builtins.c); that trailing
// precision must NOT be forwarded to the remote SQL (e.g. MySQL DATE(expr)
// accepts one arg, so "DATE(time, 1)" is a syntax error).  Render user params only.
static int32_t dynRenderNamedFuncKeepN(SDynSQL* s, const char* funcName,
                                       const SFunctionNode* pFunc, int32_t nKeep,
                                       EExtSQLDialect dialect,
                                       const SExtTableMeta* pExtMeta,
                                       const SNodesRemoteSQLCtx* pCtx) {
  dynSQLAppendStr(s, funcName);
  dynSQLAppendChar(s, '(');
  for (int32_t i = 0; i < nKeep; i++) {
    if (i > 0) dynSQLAppendStr(s, ", ");
    int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, i), dialect, pExtMeta, pCtx);
    if (code) return code;
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t dynRenderTdLengthExpr(SDynSQL* s, const SFunctionNode* pFunc,
                                     EExtSQLDialect dialect,
                                     const SExtTableMeta* pExtMeta,
                                     const SNodesRemoteSQLCtx* pCtx) {
  const SNode* pArg0 = (pFunc->pParameterList != NULL) ? nodesListGetNode(pFunc->pParameterList, 0) : NULL;
  if (dynExprIsNcharType(pArg0)) {
    qDebug("FQ-SQL: LENGTH over NCHAR uses CHAR_LENGTH * %d dialect=%s argType=%d resultType=%d",
           (int)TSDB_NCHAR_SIZE, dialectName(dialect),
           ((const SExprNode*)pArg0)->resType.type, pFunc->node.resType.type);
    dynSQLAppendStr(s, "(CHAR_LENGTH(");
    int32_t code = dynAppendExpr(s, pArg0, dialect, pExtMeta, pCtx);
    if (code) {
      return code;
    }
    dynSQLAppendf(s, ") * %d)", (int)TSDB_NCHAR_SIZE);
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  if (dialect == EXT_SQL_DIALECT_MYSQL) {
    return dynRenderNamedFunc(s, "LENGTH", pFunc, dialect, pExtMeta, pCtx);
  }

  return dynRenderNamedFunc(s, "OCTET_LENGTH", pFunc, dialect, pExtMeta, pCtx);
}

static int32_t dynRenderTdSubstringExpr(SDynSQL* s, const SFunctionNode* pFunc,
                                        EExtSQLDialect dialect,
                                        const SExtTableMeta* pExtMeta,
                                        const SNodesRemoteSQLCtx* pCtx) {
  int32_t nParams = pFunc->pParameterList ? LIST_LENGTH(pFunc->pParameterList) : 0;

  if ((dialect != EXT_SQL_DIALECT_POSTGRES && dialect != EXT_SQL_DIALECT_INFLUXQL) || nParams < 2) {
    return dynRenderNamedFunc(s, "SUBSTRING", pFunc, dialect, pExtMeta, pCtx);
  }

  SNode* pStrArg = nodesListGetNode(pFunc->pParameterList, 0);
  SNode* pStartArg = nodesListGetNode(pFunc->pParameterList, 1);
  SNode* pLenArg = (nParams >= 3) ? nodesListGetNode(pFunc->pParameterList, 2) : NULL;

  qDebug("FQ-SQL: SUBSTRING negative-start compatibility rewrite enabled dialect=%s nParams=%d startType=%d startNodeType=%d",
         dialectName(dialect), nParams,
         (NULL != pStartArg) ? ((const SExprNode*)pStartArg)->resType.type : -1,
         (NULL != pStartArg) ? (int)nodeType(pStartArg) : -1);

  dynSQLAppendStr(s, "SUBSTRING(");
  int32_t code = dynAppendExpr(s, pStrArg, dialect, pExtMeta, pCtx);
  if (code) {
    return code;
  }

  dynSQLAppendStr(s, ", CASE WHEN ");
  code = dynAppendExpr(s, pStartArg, dialect, pExtMeta, pCtx);
  if (code) {
    return code;
  }

  dynSQLAppendStr(s, " < 0 THEN CHAR_LENGTH(");
  code = dynAppendExpr(s, pStrArg, dialect, pExtMeta, pCtx);
  if (code) {
    return code;
  }

  dynSQLAppendStr(s, ") + ");
  code = dynAppendExpr(s, pStartArg, dialect, pExtMeta, pCtx);
  if (code) {
    return code;
  }

  dynSQLAppendStr(s, " + 1 ELSE ");
  code = dynAppendExpr(s, pStartArg, dialect, pExtMeta, pCtx);
  if (code) {
    return code;
  }

  dynSQLAppendStr(s, " END");
  if (NULL != pLenArg) {
    dynSQLAppendStr(s, ", ");
    code = dynAppendExpr(s, pLenArg, dialect, pExtMeta, pCtx);
    if (code) {
      return code;
    }
  }

  dynSQLAppendChar(s, ')');
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t dynAppendFunctionExpr(SDynSQL* s, const SFunctionNode* pFunc,
                                     EExtSQLDialect dialect,
                                     const SExtTableMeta* pExtMeta,
                                     const SNodesRemoteSQLCtx* pCtx) {
  EFunctionType funcType = (EFunctionType)pFunc->funcType;
  int32_t nParams = pFunc->pParameterList ? LIST_LENGTH(pFunc->pParameterList) : 0;

  switch (funcType) {
    /* ── Math (all dialects) ─────────────────────────────────────────── */
    case FUNCTION_TYPE_ABS:     return dynRenderNamedFunc(s, "ABS",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CEIL:    return dynRenderNamedFunc(s, "CEIL",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_FLOOR:   return dynRenderNamedFunc(s, "FLOOR",   pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_ROUND:
      // MySQL ROUND(double, n) follows approximate-number semantics and may
      // disagree with TDengine on x.5 ties.  Cast floating inputs to DECIMAL
      // first so the remote result keeps exact half-up style semantics.
      if (dialect == EXT_SQL_DIALECT_MYSQL && nParams >= 2) {
        const SNode* pArg0 = nodesListGetNode(pFunc->pParameterList, 0);
        if (pArg0 != NULL && dynIsFloatingType(((const SExprNode*)pArg0)->resType.type)) {
          qDebug("FQ-SQL: MySQL ROUND uses DECIMAL coercion dialect=%s srcType=%d dstType=%d nParams=%d",
                 dialectName(dialect), ((const SExprNode*)pArg0)->resType.type, pFunc->node.resType.type, nParams);
          dynSQLAppendStr(s, "CAST(ROUND(CAST(");
          int32_t code = dynAppendExpr(s, pArg0, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, " AS DECIMAL(32, 16)), ");
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1), dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ") AS DOUBLE)");
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }
      }
      // PG 2-arg ROUND(value, scale) only exists for numeric, not double precision
      // (voltage is DOUBLE), so cast the first arg: ROUND(expr::numeric, n).
      // Single-arg ROUND(double) is fine.  MySQL/InfluxDB accept ROUND(double,n).
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 2) {
        dynSQLAppendStr(s, "ROUND(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, "::numeric, ");
        code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "ROUND",   pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_SQRT:    return dynRenderNamedFunc(s, "SQRT",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_POW:
      /* PostgreSQL only accepts POWER, not POW */
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL)
        return dynRenderNamedFunc(s, "POWER", pFunc, dialect, pExtMeta, pCtx);
      return dynRenderNamedFunc(s, "POW", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_EXP:     return dynRenderNamedFunc(s, "EXP",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_LN:      return dynRenderNamedFunc(s, "LN",      pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_LOG: {
      // DS §5.3.4.1.1: LOG() semantics differ by dialect.
      // Single-arg LOG(expr): TDengine means natural log; PG/Influx LOG() is base-10, must use LN().
      // Two-arg LOG(expr, base): TDengine means log_base(expr); others use LOG(base, expr) — swap args.
      if (nParams == 1 && (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL))
        return dynRenderNamedFunc(s, "LN", pFunc, dialect, pExtMeta, pCtx);
      if (nParams >= 2) {
        // Swap: LOG(expr, base) → LOG(base, expr)
        dynSQLAppendStr(s, "LOG(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", ");
        code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "LOG", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_SIN:     return dynRenderNamedFunc(s, "SIN",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_COS:     return dynRenderNamedFunc(s, "COS",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_TAN:     return dynRenderNamedFunc(s, "TAN",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_ASIN:    return dynRenderNamedFunc(s, "ASIN",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_ACOS:    return dynRenderNamedFunc(s, "ACOS",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_ATAN:    return dynRenderNamedFunc(s, "ATAN",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_PI:      return dynRenderNamedFunc(s, "PI",      pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_MOD:
      // DS §5.3.4.1.1: DataFusion has no MOD() function; use % operator.
      if (dialect == EXT_SQL_DIALECT_INFLUXQL && nParams >= 2) {
        dynSQLAppendChar(s, '(');
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " % ");
        code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "MOD", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_SIGN:
      // InfluxDB (DataFusion) names the sign function SIGNUM; MySQL/PG use SIGN.
      if (dialect == EXT_SQL_DIALECT_INFLUXQL)
        return dynRenderNamedFunc(s, "SIGNUM", pFunc, dialect, pExtMeta, pCtx);
      if (dialect == EXT_SQL_DIALECT_POSTGRES) {
        qDebug("FQ-SQL: PostgreSQL SIGN coerced to INTEGER to align remote result type");
        dynSQLAppendStr(s, "CAST(SIGN(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ") AS INTEGER)");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "SIGN",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_DEGREES: return dynRenderNamedFunc(s, "DEGREES", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_RADIANS: return dynRenderNamedFunc(s, "RADIANS", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_TRUNCATE:
      // PG 2-arg TRUNC(value, scale) only exists for numeric, not double precision;
      // cast the first arg: TRUNC(expr::numeric, n).  InfluxDB TRUNC accepts double.
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 2) {
        dynSQLAppendStr(s, "TRUNC(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, "::numeric, ");
        code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL)
        return dynRenderNamedFunc(s, "TRUNC", pFunc, dialect, pExtMeta, pCtx);
      return dynRenderNamedFunc(s, "TRUNCATE", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_GREATEST:
    case FUNCTION_TYPE_LEAST: {
      // TDengine appends an internal ignoreNull flag as the LAST param (see
      // sclfunc.c greatestLeastImpl); drop it — remote GREATEST/LEAST takes user args only.
      const char* funcName = pFunc->funcType == FUNCTION_TYPE_GREATEST ? "GREATEST" : "LEAST";
      bool hasTimestampArg = false;
      for (int32_t i = 0; i < nParams - 1; ++i) {
        SNode* pArg = nodesListGetNode(pFunc->pParameterList, i);
        if (pArg != NULL && nodesIsExprNode(pArg) &&
            ((SExprNode*)pArg)->resType.type == TSDB_DATA_TYPE_TIMESTAMP) {
          hasTimestampArg = true;
          break;
        }
      }

      if (dialect == EXT_SQL_DIALECT_POSTGRES &&
          pFunc->node.resType.type == TSDB_DATA_TYPE_BINARY && hasTimestampArg) {
        // Mixed timestamp/string inputs produce TDengine BINARY output. PG would
        // otherwise return a microsecond TIMESTAMP, which exceeds that BINARY's
        // millisecond text capacity in the external connector.
        dynSQLAppendStr(s, "TO_CHAR(");
        int32_t code = dynRenderNamedFuncKeepN(s, funcName, pFunc, nParams - 1, dialect, pExtMeta, pCtx);
        if (code != TSDB_CODE_SUCCESS) return code;
        dynSQLAppendStr(s, ", 'YYYY-MM-DD HH24:MI:SS.MS')");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFuncKeepN(s, funcName, pFunc, nParams - 1, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_RAND:
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL)
        return dynRenderNamedFunc(s, "RANDOM", pFunc, dialect, pExtMeta, pCtx);
      return dynRenderNamedFunc(s, "RAND", pFunc, dialect, pExtMeta, pCtx);

    /* ── String (all dialects unless noted) ─────────────────────────── */
    case FUNCTION_TYPE_UPPER:    return dynRenderNamedFunc(s, "UPPER",  pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_LOWER:    return dynRenderNamedFunc(s, "LOWER",  pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_LENGTH:
      return dynRenderTdLengthExpr(s, pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CHAR_LENGTH:  return dynRenderNamedFunc(s, "CHAR_LENGTH",  pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CONCAT:       return dynRenderNamedFunc(s, "CONCAT",       pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CONCAT_WS:    return dynRenderNamedFunc(s, "CONCAT_WS",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_SUBSTR:
      return dynRenderTdSubstringExpr(s, pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_ASCII:        return dynRenderNamedFunc(s, "ASCII",        pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_TRIM: {
      // TDengine appends an internal trimType (1=LEADING, 2=TRAILING, 3=BOTH) as
      // the LAST parameter (translateTrim).  Standard SQL TRIM does not take it as
      // a value arg; emit TRIM([LEADING|TRAILING] [remove_chars] FROM expr) or
      // plain TRIM(expr) for the common BOTH/no-remove-chars case.
      //   user args before trimType: [expr]  or  [remove_chars, expr]
      if (nParams < 2) {
        // No trimType appended (defensive) — render as-is.
        return dynRenderNamedFunc(s, "TRIM", pFunc, dialect, pExtMeta, pCtx);
      }
      int32_t nUser = nParams - 1;  // drop trailing trimType
      SNode* pTypeArg = nodesListGetNode(pFunc->pParameterList, nParams - 1);
      int64_t trimType = (pTypeArg && nodeType(pTypeArg) == QUERY_NODE_VALUE)
                             ? ((const SValueNode*)pTypeArg)->datum.i : 3;  // default BOTH
      const char* dir = (trimType == 1) ? "LEADING " : (trimType == 2) ? "TRAILING " : "";
      SNode* pExprArg   = nodesListGetNode(pFunc->pParameterList, nUser - 1);  // expr is last user arg
      SNode* pRemoveArg = (nUser >= 2) ? nodesListGetNode(pFunc->pParameterList, 0) : NULL;
      if (dir[0] == '\0' && pRemoveArg == NULL) {
        // Plain BOTH-trim of whitespace → TRIM(expr).
        dynSQLAppendStr(s, "TRIM(");
        int32_t code = dynAppendExpr(s, pExprArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      // TRIM([LEADING|TRAILING] [remove_chars] FROM expr)
      dynSQLAppendStr(s, "TRIM(");
      dynSQLAppendStr(s, dir);
      if (pRemoveArg != NULL) {
        int32_t code = dynAppendExpr(s, pRemoveArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ' ');
      }
      dynSQLAppendStr(s, "FROM ");
      int32_t code = dynAppendExpr(s, pExprArg, dialect, pExtMeta, pCtx);
      if (code) return code;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case FUNCTION_TYPE_LTRIM:        return dynRenderNamedFunc(s, "LTRIM",        pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_RTRIM:        return dynRenderNamedFunc(s, "RTRIM",        pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_REPLACE:      return dynRenderNamedFunc(s, "REPLACE",      pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_REPEAT:       return dynRenderNamedFunc(s, "REPEAT",       pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_SUBSTR_IDX:
      // DS §5.3.4.1.2: SUBSTRING_INDEX — MySQL only; PG/InfluxDB have no equivalent.
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: SUBSTRING_INDEX not supported by %s; deferring to local", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "SUBSTRING_INDEX", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_BASE64:
      // DS §5.3.4.1.3: PG uses ENCODE(expr::bytea, 'base64'); InfluxDB has no encoding function.
      {
        int32_t code = dynRejectFuncArg0NcharIfUnsupported(pFunc, dialect);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1) {
        dynSQLAppendStr(s, "ENCODE(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, "::bytea, 'base64')");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: TO_BASE64 not supported by InfluxDB; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "TO_BASE64",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_FROM_BASE64:
      // DS §5.3.4.1.3: PG uses DECODE(expr, 'base64'); InfluxDB has no decoding function.
      {
        int32_t code = dynRejectFuncArg0NcharIfUnsupported(pFunc, dialect);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1) {
        dynSQLAppendStr(s, "DECODE(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", 'base64')");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: FROM_BASE64 not supported by InfluxDB; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "FROM_BASE64",  pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_MD5: {
      int32_t code = dynRejectFuncArg0NcharIfUnsupported(pFunc, dialect);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      return dynRenderNamedFunc(s, "MD5", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_SHA1:
      // DS §5.3.4.1.4: PG needs pgcrypto ext; DataFusion has no SHA1. Local only for both.
      {
        int32_t code = dynRejectFuncArg0NcharIfUnsupported(pFunc, dialect);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: SHA1 not supported by %s; deferring to local", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "SHA1", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_SHA2: {
      {
        int32_t code = dynRejectFuncArg0NcharIfUnsupported(pFunc, dialect);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES) {
        // DS §5.3.4.1.4: PG needs pgcrypto; local only.
        qError("FQ-SQL: SHA2 not supported by PostgreSQL; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        int64_t bits = 256;
        if (nParams >= 2) {
          SNode* pBitsNode = nodesListGetNode(pFunc->pParameterList, 1);
          if (pBitsNode && nodeType(pBitsNode) == QUERY_NODE_VALUE) {
            bits = ((SValueNode*)pBitsNode)->datum.i;
          }
        }
        char shaName[16] = {0};
        (void)snprintf(shaName, sizeof(shaName), "SHA%" PRId64, bits);
        qDebug("FQ-SQL: SHA2 mapped to %s for InfluxDB dialect=%s", shaName, dialectName(dialect));
        dynSQLAppendStr(s, shaName);
        dynSQLAppendChar(s, '(');
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) {
          return code;
        }
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "SHA2", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_CRC32:
      // DS §5.3.4.1.5: PG/InfluxDB have no built-in CRC32.
      {
        int32_t code = dynRejectFuncArg0NcharIfUnsupported(pFunc, dialect);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: CRC32 not supported by %s; deferring to local", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "CRC32", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_FIND_IN_SET:
      // DS §5.3.4.1.2: PG/InfluxDB have no equivalent function.
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: FIND_IN_SET not supported by %s; deferring to local", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "FIND_IN_SET", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CHAR:
      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        // DS §5.3.4.1.2: PG/DataFusion CHR() accepts single arg only.
        // Multi-arg CHAR(e1, e2, ...) → CHR(e1) || CHR(e2) || ...
        if (nParams > 1) {
          bool first2 = true;
          SNode* pArg = NULL;
          FOREACH(pArg, pFunc->pParameterList) {
            if (!first2) dynSQLAppendStr(s, " || ");
            first2 = false;
            dynSQLAppendStr(s, "CHR(");
            int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
            if (code) return code;
            dynSQLAppendChar(s, ')');
          }
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }
        return dynRenderNamedFunc(s, "CHR",  pFunc, dialect, pExtMeta, pCtx);
      }
      return dynRenderNamedFunc(s, "CHAR", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_POSITION: {
      /* POSITION(needle IN haystack) — TDengine stores as POSITION(needle, haystack) */
      if (nParams >= 2) {
        if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
          // DS §5.3.4.1.2: DataFusion uses STRPOS(haystack, needle) — reversed args.
          dynSQLAppendStr(s, "STRPOS(");
          int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1),
                                       dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ", ");
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0),
                               dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendChar(s, ')');
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }
        dynSQLAppendStr(s, "POSITION(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0),
                                     dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " IN ");
        code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 1),
                             dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "POSITION", pFunc, dialect, pExtMeta, pCtx);
    }

    /* ── CAST ─────────────────────────────────────────────────────────── */
    case FUNCTION_TYPE_CAST: {
      const SNode* pArg0 = (nParams >= 1) ? nodesListGetNode(pFunc->pParameterList, 0) : NULL;
      bool castResultIsInt = (pFunc->node.resType.type == TSDB_DATA_TYPE_BIGINT ||
                              pFunc->node.resType.type == TSDB_DATA_TYPE_UBIGINT ||
                              pFunc->node.resType.type == TSDB_DATA_TYPE_INT    ||
                              pFunc->node.resType.type == TSDB_DATA_TYPE_UINT);
      bool castResultIsTimestamp = (pFunc->node.resType.type == TSDB_DATA_TYPE_TIMESTAMP);
      bool castArgIsTimestamp = (pArg0 != NULL && nodesIsExprNode((SNode*)pArg0) &&
                                 ((const SExprNode*)pArg0)->resType.type == TSDB_DATA_TYPE_TIMESTAMP);
      bool castArgIsInt = (pArg0 != NULL && nodesIsExprNode((SNode*)pArg0) &&
                           dynIsIntegerType(((const SExprNode*)pArg0)->resType.type));
      /* Keep generic Influx CAST(timestamp AS integer) local for now.
       * TIMEDIFF uses a dedicated exact-epoch renderer instead of widening
       * remote CAST pushdown semantics for unrelated queries. */
      if (dialect == EXT_SQL_DIALECT_INFLUXQL && nParams >= 1 && castResultIsInt) {
        if (castArgIsTimestamp) {
          qError("FQ-SQL: CAST(timestamp AS integer) not supported by InfluxDB; deferring to local");
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
      }
      /* MySQL/InfluxDB: CAST(timestamp AS integer) is deferred to local execution.
       * planOptimizer marks these as non-pushable; if this path is reached via
       * nodesRemotePlanToSQL directly, return unsupported so the caller falls back. */
      if ((dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_INFLUXQL) && nParams >= 1 &&
          castResultIsInt) {
        if (castArgIsTimestamp) {
          qError("FQ-SQL: CAST(timestamp AS integer) not supported for %s; deferring to local",
                 dialectName(dialect));
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1 && castResultIsInt && castArgIsTimestamp) {
        return dynRenderPgTimestampEpochCast(s, pFunc, pArg0, pExtMeta, pCtx);
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1 && castResultIsTimestamp && castArgIsInt) {
        const SNode* pEpochTsArg = NULL;
        int8_t precision = fqNormalizeTimePrecision((int8_t)pFunc->node.resType.precision,
                                                    TSDB_TIME_PRECISION_MICRO);
        EExtTimestampTzClass epochTsClass = EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
        const char* pEpochExtType = NULL;
        if (fqTryResolvePgEpochSource(pArg0, pCtx, &pEpochTsArg, &precision, &epochTsClass, &pEpochExtType)) {
          qDebug("FQ-SQL: PG CAST(int->timestamp) reuse nested epoch source precision=%d tsClass=%s extType=%s clientTz=%s",
                 (int)precision, fqExtTimestampClassName(epochTsClass),
                 pEpochExtType ? pEpochExtType : "(unknown)",
                 (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)");
        } else {
          qDebug("FQ-SQL: PG CAST(int->timestamp) fallback precision=%d exprType=%s clientTz=%s",
                 (int)precision, pArg0 ? nodesNodeName(nodeType((SNode*)pArg0)) : "(null)",
                 (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)");
        }
        return dynRenderPgEpochAsTimestampExpr(s, pArg0, pExtMeta, pCtx, precision, s->tzName);
      }

      if (pArg0 != NULL &&
          dynIsFloatingType(((const SExprNode*)pArg0)->resType.type) &&
          dynIsIntegerType(pFunc->node.resType.type) &&
          (dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_POSTGRES)) {
        qDebug("FQ-SQL: CAST float->int uses truncation semantics dialect=%s srcType=%d dstType=%d",
               dialectName(dialect), ((const SExprNode*)pArg0)->resType.type, pFunc->node.resType.type);
        dynSQLAppendStr(s, "CAST(");
        if (dialect == EXT_SQL_DIALECT_MYSQL) {
          dynSQLAppendStr(s, "TRUNCATE(");
          int32_t code = dynAppendExpr(s, pArg0, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ", 0)");
        } else {
          dynSQLAppendStr(s, "TRUNC(");
          int32_t code = dynAppendExpr(s, pArg0, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendChar(s, ')');
        }
      dynSQLAppendStr(s, " AS ");
      dynSQLAppendStr(s, dynSqlCastType(dialect, pFunc->node.resType.type));
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
      }

      dynSQLAppendStr(s, "CAST(");
      if (nParams >= 1) {
        int32_t code = dynAppendExpr(s, pArg0, dialect, pExtMeta, pCtx);
        if (code) return code;
      }
      dynSQLAppendStr(s, " AS ");
      dynSQLAppendStr(s, dynSqlCastType(dialect, pFunc->node.resType.type));
      dynSQLAppendChar(s, ')');
      return TSDB_CODE_SUCCESS;
    }
    case FUNCTION_TYPE_TO_UNIXTIMESTAMP: {
      // DS §5.2.6 / §5.3.4.1.8:
      //   - tz-aware external timestamp types must preserve their native UTC semantics.
      //   - tz-naive external timestamp/date types must be interpreted in the TDengine
      //     client timezone before converting to epoch.
      //   - string inputs continue to use the legacy remote parsing path.
      if (nParams < 1) {
        qError("FQ-SQL: TO_UNIXTIMESTAMP missing params");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      SNode* pArg = nodesListGetNode(pFunc->pParameterList, 0);
      uint8_t argType = nodesIsExprNode(pArg) ? ((const SExprNode*)pArg)->resType.type : TSDB_DATA_TYPE_NULL;
      const char* pExtColName = NULL;
      const char* pExtTypeName = NULL;
      EExtTimestampTzClass tsClass = EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
      if (argType == TSDB_DATA_TYPE_TIMESTAMP) {
        tsClass = fqResolveExprTimestampClass(pArg, dialect, pExtMeta, pCtx, &pExtColName, &pExtTypeName);
      }

      qDebug("FQ-SQL: TO_UNIXTIMESTAMP dialect=%s argType=%u(%s) tsClass=%s col=%s extType=%s clientTz=%s",
             dialectName(dialect), (unsigned)argType, fqDebugDataTypeName((int8_t)argType), fqExtTimestampClassName(tsClass),
             pExtColName ? pExtColName : "(expr)",
             pExtTypeName ? pExtTypeName : "(unknown)",
             (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)");

      if (dialect == EXT_SQL_DIALECT_MYSQL) {
        if (argType == TSDB_DATA_TYPE_TIMESTAMP && tsClass == EXT_TIMESTAMP_TZ_CLASS_AWARE) {
          qDebug("FQ-SQL: render TO_UNIXTIMESTAMP() MySQL aware-ts via UNIX_TIMESTAMP col=%s extType=%s",
                 pExtColName ? pExtColName : "(expr)", pExtTypeName ? pExtTypeName : "(unknown)");
          dynSQLAppendStr(s, "ROUND(UNIX_TIMESTAMP(");
          int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ") * 1000)");
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }

        if (s->tzName != NULL && s->tzName[0] != '\0') {
          qDebug("FQ-SQL: render TO_UNIXTIMESTAMP() MySQL via CONVERT_TZ+TIMESTAMPDIFF tsClass=%s col=%s extType=%s clientTz=%s",
                 fqExtTimestampClassName(tsClass), pExtColName ? pExtColName : "(expr)",
                 pExtTypeName ? pExtTypeName : "(unknown)", s->tzName);
          dynSQLAppendStr(s, "CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', CONVERT_TZ(");
          int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ", '");
          dynSQLAppendStr(s, s->tzName);
          dynSQLAppendStr(s, "', '+00:00')) / 1000 AS SIGNED)");
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }

        qDebug("FQ-SQL: render TO_UNIXTIMESTAMP() dialect=%s without clientTz; fallback UNIX_TIMESTAMP(expr) * 1000",
               dialectName(dialect));
        dynSQLAppendStr(s, "ROUND(UNIX_TIMESTAMP(");
        int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ") * 1000)");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES) {
        qDebug("FQ-SQL: render TO_UNIXTIMESTAMP() PG via epoch-extract tsClass=%s col=%s extType=%s clientTz=%s",
               fqExtTimestampClassName(tsClass), pExtColName ? pExtColName : "(expr)",
               pExtTypeName ? pExtTypeName : "(unknown)",
               (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)");
        dynSQLAppendStr(s, "CAST(");
        int32_t code = dynRenderPgEpochExtractExpr(s, pArg, pExtMeta, pCtx, tsClass, pExtTypeName);
        if (code) return code;
        dynSQLAppendStr(s, " * 1000 AS BIGINT)");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      // InfluxDB: DataFusion supports EXTRACT(EPOCH FROM ...) since PR #5555
      dynSQLAppendStr(s, "CAST(EXTRACT(EPOCH FROM ");
      int32_t code2 = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
      if (code2) return code2;
      dynSQLAppendStr(s, ") * 1000 AS BIGINT)");
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case FUNCTION_TYPE_TO_JSON: {
      qError("FQ-SQL: TO_JSON pushdown disabled — TDengine parses input text as JSON and maps invalid payloads to JSON null, which is not equivalent to external-source direct execution");
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }

    /* ── Time functions ──────────────────────────────────────────────── */
    case FUNCTION_TYPE_NOW:
      /* InfluxDB 3 NOW() comparison semantics differ from TDengine; compute locally. */
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      return dynRenderNamedFunc(s, "NOW", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_TODAY:
      // DS §5.3.4.1.9: MySQL uses CURDATE(); PG uses CURRENT_DATE; InfluxDB uses CURRENT_DATE.
      if (dialect == EXT_SQL_DIALECT_MYSQL)
        return dynRenderNamedFunc(s, "CURDATE", pFunc, dialect, pExtMeta, pCtx);
      // PG/InfluxDB: CURRENT_DATE (no parentheses — it's a keyword, not a function call)
      dynSQLAppendStr(s, "CURRENT_DATE");
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    case FUNCTION_TYPE_DATE: {
      // DATE() returns a formatted date string in TDengine.
      // Render string-producing dialect forms to avoid provider-specific
      // binary DATE decoding paths.
      if (dialect == EXT_SQL_DIALECT_MYSQL && nParams >= 1) {
        dynSQLAppendStr(s, "DATE_FORMAT(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", '%Y-%m-%d')");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1) {
        SNode* pArg = nodesListGetNode(pFunc->pParameterList, 0);
        const char* pExtColName = NULL;
        const char* pExtTypeName = NULL;
        EExtTimestampTzClass tsClass =
            fqResolveExprTimestampClass(pArg, EXT_SQL_DIALECT_POSTGRES, pExtMeta, pCtx,
                                        &pExtColName, &pExtTypeName);
        bool useClientTzWrap = (((const SExprNode*)pArg)->resType.type == TSDB_DATA_TYPE_TIMESTAMP &&
                                tsClass == EXT_TIMESTAMP_TZ_CLASS_AWARE &&
                                s->tzName != NULL && s->tzName[0] != '\0');
        qDebug("FQ-SQL: DATE PG render argType=%d(%s) tsClass=%s extType=%s col=%s clientTz=%s useClientTzWrap=%d",
               (int)((const SExprNode*)pArg)->resType.type,
               fqDebugDataTypeName(((const SExprNode*)pArg)->resType.type),
               fqExtTimestampClassName(tsClass),
               pExtTypeName ? pExtTypeName : "(unknown)",
               pExtColName ? pExtColName : "(expr)",
               (s->tzName != NULL && s->tzName[0] != '\0') ? s->tzName : "(global)",
               (int)useClientTzWrap);
        dynSQLAppendStr(s, "TO_CHAR(");
        if (useClientTzWrap) {
          dynSQLAppendChar(s, '(');
        }
        int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        if (useClientTzWrap) {
          dynSQLAppendStr(s, " AT TIME ZONE '");
          dynSQLAppendStr(s, s->tzName);
          dynSQLAppendStr(s, "')");
        }
        dynSQLAppendStr(s, ", 'YYYY-MM-DD')");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      // DS §5.3.4.1.9: InfluxDB → CAST(expr AS DATE)
      if (dialect == EXT_SQL_DIALECT_INFLUXQL && nParams >= 1) {
        dynSQLAppendStr(s, "CAST(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " AS DATE)");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFuncKeepN(s, "DATE", pFunc, nParams - 1, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_WEEK: {
      // PG / InfluxDB only expose ISO-week semantics (equivalent to
      // TDengine WEEK(..., 3) / WEEKOFYEAR). Default WEEK(expr) and other
      // modes must stay local to preserve TDengine semantics.
      if ((dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) && nParams >= 1) {
        bool isoWeek = (nParams >= 3) && dynFuncArgInt64Equals(pFunc, 1, 3);
        if (!isoWeek) {
          qError("FQ-SQL: WEEK non-ISO mode not supported by %s; keep local (nParams=%d)",
                 dialectName(dialect), nParams);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        dynSQLAppendStr(s, "EXTRACT(WEEK FROM ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFuncKeepN(s, "WEEK", pFunc, nParams - 1, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_WEEKDAY: {
      // DS §5.3.4.1.9: PG → EXTRACT(ISODOW FROM expr) - 1; InfluxDB → local.
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: WEEKDAY() not supported by InfluxDB; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1) {
        dynSQLAppendStr(s, "(EXTRACT(ISODOW FROM ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ") - 1)");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFuncKeepN(s, "WEEKDAY", pFunc, nParams - 1, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_WEEKOFYEAR: {
      // DS §5.3.4.1.9: PG/InfluxDB → EXTRACT(WEEK FROM expr)
      if ((dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) && nParams >= 1) {
        dynSQLAppendStr(s, "EXTRACT(WEEK FROM ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFuncKeepN(s, "WEEKOFYEAR", pFunc, nParams - 1, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_DAYOFWEEK: {
      // DS §5.3.4.1.9: PG/InfluxDB → EXTRACT(DOW FROM expr) + 1  [TDengine/MySQL: 1=Sunday]
      if ((dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) && nParams >= 1) {
        dynSQLAppendStr(s, "(EXTRACT(DOW FROM ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ") + 1)");
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFuncKeepN(s, "DAYOFWEEK", pFunc, nParams - 1, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_TO_CHAR: {
      // DS §5.3.4.1.8: MySQL → DATE_FORMAT(expr, mysql_fmt); PG/Influx → TO_CHAR(expr, fmt).
      // The 3rd param (timezone name) is TDengine-specific and not forwarded to remote.
      qDebug("FQ-SQL: TO_CHAR rendering dialect=%s nParams=%d", dialectName(dialect), nParams);
      if (dialect == EXT_SQL_DIALECT_MYSQL && nParams >= 2) {
        SNode* pTsArg  = nodesListGetNode(pFunc->pParameterList, 0);
        SNode* pFmtArg = nodesListGetNode(pFunc->pParameterList, 1);
        dynSQLAppendStr(s, "DATE_FORMAT(");
        int32_t code = dynAppendExpr(s, pTsArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", ");
        // Convert TDengine format string to MySQL format string.
        if (pFmtArg && nodeType(pFmtArg) == QUERY_NODE_VALUE) {
          const SValueNode* pFmtVal = (const SValueNode*)pFmtArg;
          const char* fmtStr = pFmtVal->datum.p ? varDataVal(pFmtVal->datum.p) : "";
          qDebug("FQ-SQL: TO_CHAR MySQL fmt_in='%s'", fmtStr);
          dynAppendFmtStr(s, fmtStr, false);
        } else {
          int32_t fmtCode = dynAppendExpr(s, pFmtArg, dialect, pExtMeta, pCtx);
          if (fmtCode) return fmtCode;
        }
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      // PG/Influx: TO_CHAR(expr, fmt) — pass first 2 params only (skip timezone arg).
      // PG accepts the Oracle-style format (yyyy-mm-dd) verbatim.  InfluxDB v3
      // (DataFusion) requires chrono strftime specifiers, so translate the format.
      if (nParams >= 2) {
        SNode* pTsArg  = nodesListGetNode(pFunc->pParameterList, 0);
        SNode* pFmtArg = nodesListGetNode(pFunc->pParameterList, 1);
        dynSQLAppendStr(s, "TO_CHAR(");
        int32_t code = dynAppendExpr(s, pTsArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", ");
        if (dialect == EXT_SQL_DIALECT_INFLUXQL && pFmtArg &&
            nodeType(pFmtArg) == QUERY_NODE_VALUE) {
          const SValueNode* pFmtVal = (const SValueNode*)pFmtArg;
          const char* fmtStr = pFmtVal->datum.p ? varDataVal(pFmtVal->datum.p) : "";
          qDebug("FQ-SQL: TO_CHAR DataFusion fmt_in='%s'", fmtStr);
          dynAppendFmtStr(s, fmtStr, true);
        } else {
          code = dynAppendExpr(s, pFmtArg, dialect, pExtMeta, pCtx);
          if (code) return code;
        }
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "TO_CHAR", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_TO_TIMESTAMP: {
      // DS §5.3.4.1.8: MySQL → STR_TO_DATE(expr, mysql_fmt); PG/Influx → TO_TIMESTAMP(expr, fmt).
      qDebug("FQ-SQL: TO_TIMESTAMP rendering dialect=%s nParams=%d", dialectName(dialect), nParams);
      if (dialect == EXT_SQL_DIALECT_MYSQL && nParams >= 2) {
        SNode* pStrArg = nodesListGetNode(pFunc->pParameterList, 0);
        SNode* pFmtArg = nodesListGetNode(pFunc->pParameterList, 1);
        dynSQLAppendStr(s, "STR_TO_DATE(");
        int32_t code = dynAppendExpr(s, pStrArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", ");
        if (pFmtArg && nodeType(pFmtArg) == QUERY_NODE_VALUE) {
          const SValueNode* pFmtVal = (const SValueNode*)pFmtArg;
          const char* fmtStr = pFmtVal->datum.p ? varDataVal(pFmtVal->datum.p) : "";
          qDebug("FQ-SQL: TO_TIMESTAMP MySQL fmt_in='%s'", fmtStr);
          dynAppendFmtStr(s, fmtStr, false);
        } else {
          int32_t fmtCode = dynAppendExpr(s, pFmtArg, dialect, pExtMeta, pCtx);
          if (fmtCode) return fmtCode;
        }
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      // PG/Influx: TO_TIMESTAMP(expr, fmt).  PG accepts the Oracle-style format
      // (yyyy-mm-dd) verbatim; InfluxDB v3 (DataFusion) requires chrono strftime
      // specifiers, so translate the format string.
      if (nParams >= 2) {
        SNode* pStrArg = nodesListGetNode(pFunc->pParameterList, 0);
        SNode* pFmtArg = nodesListGetNode(pFunc->pParameterList, 1);
        dynSQLAppendStr(s, "TO_TIMESTAMP(");
        int32_t code = dynAppendExpr(s, pStrArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", ");
        if (dialect == EXT_SQL_DIALECT_INFLUXQL && pFmtArg &&
            nodeType(pFmtArg) == QUERY_NODE_VALUE) {
          const SValueNode* pFmtVal = (const SValueNode*)pFmtArg;
          const char* fmtStr = pFmtVal->datum.p ? varDataVal(pFmtVal->datum.p) : "";
          qDebug("FQ-SQL: TO_TIMESTAMP DataFusion fmt_in='%s'", fmtStr);
          dynAppendFmtStr(s, fmtStr, true);
        } else {
          code = dynAppendExpr(s, pFmtArg, dialect, pExtMeta, pCtx);
          if (code) return code;
        }
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "TO_TIMESTAMP", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_TIMETRUNCATE: {
      // DS §5.3.4.1.9: MySQL local-only (no general timestamp truncation; disabled
      // via capability matrix).  PG / InfluxDB v3 → DATE_TRUNC('<unit>', expr).
      // TDengine param layout (translateTimeTruncate): [0]=time, [1]=interval
      // ticks, [2]=use_curr_tz, [3]=precision, [4]=tz, …  DATE_TRUNC takes a unit
      // KEYWORD, so the interval must equal exactly one supported calendar unit;
      // map interval(ticks,precision) → unit string, else defer to local.
      if (nParams < 4) {
        qError("FQ-SQL: TIMETRUNCATE missing params (nParams=%d)", nParams);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      SNode* pTsArg   = nodesListGetNode(pFunc->pParameterList, 0);
      SNode* pIvalArg = nodesListGetNode(pFunc->pParameterList, 1);
      SNode* pPrecArg = nodesListGetNode(pFunc->pParameterList, 3);
      if (!pIvalArg || nodeType(pIvalArg) != QUERY_NODE_VALUE ||
          !pPrecArg || nodeType(pPrecArg) != QUERY_NODE_VALUE) {
        qError("FQ-SQL: TIMETRUNCATE bad interval/precision param types");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      int64_t ticks = ((const SValueNode*)pIvalArg)->datum.i;
      int8_t  prec  = (int8_t)((const SValueNode*)pPrecArg)->datum.i;
      // Normalize the interval to nanoseconds using the source precision.
      int64_t ns;
      switch (prec) {
        case TSDB_TIME_PRECISION_MILLI: ns = ticks * 1000000LL; break;
        case TSDB_TIME_PRECISION_MICRO: ns = ticks * 1000LL;    break;
        case TSDB_TIME_PRECISION_NANO:  ns = ticks;             break;
        default:                        ns = ticks * 1000LL;    break;  // assume µs
      }
      const int64_t NS_SEC = 1000000000LL, NS_MIN = 60LL*1000000000LL,
                    NS_HOUR = 3600LL*1000000000LL, NS_DAY = 86400LL*1000000000LL,
                    NS_WEEK = 7LL*86400LL*1000000000LL;
      const char* unit = NULL;
      if      (ns == NS_SEC)  unit = "second";
      else if (ns == NS_MIN)  unit = "minute";
      else if (ns == NS_HOUR) unit = "hour";
      else if (ns == NS_DAY)  unit = "day";
      else if (ns == NS_WEEK) unit = "week";
      if (unit == NULL) {
        qError("FQ-SQL: TIMETRUNCATE interval=%" PRId64 "ns not a single DATE_TRUNC unit; defer local", ns);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      dynSQLAppendf(s, "DATE_TRUNC('%s', ", unit);
      int32_t code = dynAppendExpr(s, pTsArg, dialect, pExtMeta, pCtx);
      if (code) return code;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }

    /* ── Aggregate functions ─────────────────────────────────────────── */
    case FUNCTION_TYPE_COUNT: {
      /* COUNT(*) special case: single COLUMN param with colName="*" */
      bool isCountStar = (nParams == 0) ||
                         (nParams == 1 &&
                          nodeType(nodesListGetNode(pFunc->pParameterList, 0)) == QUERY_NODE_COLUMN &&
                          strcmp(((const SColumnNode*)
                                  nodesListGetNode(pFunc->pParameterList, 0))->colName, "*") == 0);
      if (!isCountStar && nParams == 1 &&
          nodeType(nodesListGetNode(pFunc->pParameterList, 0)) == QUERY_NODE_COLUMN) {
        /* The parser rewrites count(*) → count(<primary-key>) for scan optimisation.
         * InfluxDB's standard timestamp primary keys are named time/_time, and
         * COUNT(time) is not a valid field aggregate there. Do not rewrite an
         * ordinary field reference: COUNT(field) must retain NULL semantics. */
        if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
          const SColumnNode* pCol = (const SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          isCountStar = strcasecmp(pCol->colName, "time") == 0 ||
                        strcasecmp(pCol->colName, "_time") == 0;
        }
      }
      if (!isCountStar && nParams == 1 && pExtMeta &&
          nodeType(nodesListGetNode(pFunc->pParameterList, 0)) == QUERY_NODE_COLUMN) {
        /* Fallback: if pExtMeta is available (e.g. in EXPLAIN context), use the
         * isPrimaryKey flag to detect count(<pk>) → COUNT(*) for any dialect. */
        const SColumnNode* pCol = (const SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
        for (int32_t ci = 0; ci < pExtMeta->numOfCols; ci++) {
          if (pExtMeta->pCols[ci].isPrimaryKey &&
              strcasecmp(pExtMeta->pCols[ci].colName, pCol->colName) == 0) {
            isCountStar = true;
            break;
          }
        }
      }
      if (isCountStar) {
        dynSQLAppendStr(s, "COUNT(*)");
        return TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "COUNT", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_SUM:         return dynRenderNamedFunc(s, "SUM",         pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_AVG:         return dynRenderNamedFunc(s, "AVG",         pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_MIN:         return dynRenderNamedFunc(s, "MIN",         pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_MAX:         return dynRenderNamedFunc(s, "MAX",         pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_STDDEV:      return dynRenderNamedFunc(s, "STDDEV_POP",  pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_STDDEV_SAMP: return dynRenderNamedFunc(s, "STDDEV_SAMP", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_STDVAR:      return dynRenderNamedFunc(s, "VAR_POP",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_STDVAR_SAMP: return dynRenderNamedFunc(s, "VAR_SAMP",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_GROUP_CONCAT: {
      int32_t orderArgIdx = -1;
      if (nParams > 0 &&
          dynIsHiddenGroupConcatOrderArg(nodesListGetNode(pFunc->pParameterList, nParams - 1), pExtMeta)) {
        orderArgIdx = nParams - 1;
      }

      int32_t businessArgCount = (orderArgIdx >= 0) ? (nParams - 1) : nParams;
      if (businessArgCount <= 0) {
        qError("FQ-SQL: GROUP_CONCAT has no business args func=%p nParams=%d orderArgIdx=%d",
               pFunc, nParams, orderArgIdx);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }

      int32_t valueArgCount = (businessArgCount > 1) ? (businessArgCount - 1) : 1;
      int32_t sepArgIdx = (businessArgCount > 1) ? (businessArgCount - 1) : -1;
      const SExtColumnDef* pPkCol = findPrimaryKeyExtCol(pExtMeta);
      const char* pPkColName = (pPkCol && pPkCol->colName[0] != '\0')
                                   ? pPkCol->colName
                                   : NULL;

      qDebug("FQ-SQL: GROUP_CONCAT dialect=%s func=%p nParams=%d businessArgs=%d valueArgs=%d sepArgIdx=%d orderArgIdx=%d pk=%s",
             dialectName(dialect), pFunc, nParams, businessArgCount, valueArgCount, sepArgIdx,
             orderArgIdx, pPkColName ? pPkColName : "(null)");

      if (dialect == EXT_SQL_DIALECT_POSTGRES || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        dynSQLAppendStr(s, "STRING_AGG(");
        int32_t code = dynRenderGroupConcatRowExpr(s, pFunc, valueArgCount, dialect, pExtMeta, pCtx);
        if (code) {
          return code;
        }

        dynSQLAppendStr(s, ", ");
        if (sepArgIdx >= 0) {
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, sepArgIdx), dialect, pExtMeta, pCtx);
          if (code) {
            return code;
          }
        } else {
          dynAppendEscapedString(s, ",", dialect);
        }

        if (orderArgIdx >= 0) {
          dynSQLAppendStr(s, " ORDER BY ");
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, orderArgIdx), dialect, pExtMeta, pCtx);
          if (code) {
            return code;
          }
          qDebug("FQ-SQL: GROUP_CONCAT %s uses hidden order param idx=%d",
                 dialect == EXT_SQL_DIALECT_POSTGRES ? "PG" : "InfluxDB", orderArgIdx);
        } else if (pPkColName != NULL) {
          dynSQLAppendStr(s, " ORDER BY ");
          dynAppendQuotedId(s, pPkColName, dialect);
          qDebug("FQ-SQL: GROUP_CONCAT %s uses pk-ordered STRING_AGG pk=%s",
                 dialect == EXT_SQL_DIALECT_POSTGRES ? "PG" : "InfluxDB", pPkColName);
        } else {
          qDebug("FQ-SQL: GROUP_CONCAT %s has no primary key metadata; emit unordered STRING_AGG",
                 dialect == EXT_SQL_DIALECT_POSTGRES ? "PG" : "InfluxDB");
        }

        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }

      if (dialect == EXT_SQL_DIALECT_MYSQL) {
        dynSQLAppendStr(s, "GROUP_CONCAT(");
        int32_t code = dynRenderGroupConcatRowExpr(s, pFunc, valueArgCount, dialect, pExtMeta, pCtx);
        if (code) {
          return code;
        }

        if (orderArgIdx >= 0) {
          dynSQLAppendStr(s, " ORDER BY ");
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, orderArgIdx), dialect, pExtMeta, pCtx);
          if (code) {
            return code;
          }
          qDebug("FQ-SQL: GROUP_CONCAT MySQL uses hidden order param idx=%d", orderArgIdx);
        } else if (pPkColName != NULL) {
          dynSQLAppendStr(s, " ORDER BY ");
          dynAppendQuotedId(s, pPkColName, dialect);
          qDebug("FQ-SQL: GROUP_CONCAT MySQL uses pk-ordered GROUP_CONCAT pk=%s", pPkColName);
        } else {
          qDebug("FQ-SQL: GROUP_CONCAT MySQL has no primary key metadata; emit unordered GROUP_CONCAT");
        }

        dynSQLAppendStr(s, " SEPARATOR ");
        if (sepArgIdx >= 0) {
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, sepArgIdx), dialect, pExtMeta, pCtx);
          if (code) {
            return code;
          }
        } else {
          dynAppendEscapedString(s, ",", dialect);
        }
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }

      qError("FQ-SQL: GROUP_CONCAT() not supported by %s", dialectName(dialect));
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    case FUNCTION_TYPE_CORR:
      // DS §5.3.4.1.1: MySQL has no built-in CORR(); disabled via capability matrix.
      // PG and InfluxDB v3 both support CORR() natively.
      if (dialect == EXT_SQL_DIALECT_MYSQL) {
        qError("FQ-SQL: CORR() not supported by MySQL");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "CORR", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_SPREAD: {
      // DS §5.3.4.1.10: SPREAD(expr) → MAX(expr) - MIN(expr)
      if (nParams < 1) {
        qError("FQ-SQL: SPREAD missing params");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      SNode* pArg = nodesListGetNode(pFunc->pParameterList, 0);
      bool argIsTimestamp = (pArg != NULL && nodesIsExprNode((SNode*)pArg) &&
                             ((const SExprNode*)pArg)->resType.type == TSDB_DATA_TYPE_TIMESTAMP);
      if (argIsTimestamp) {
        int8_t precision = fqResolveTimestampExprPrecision(pArg,
                                                           fqDefaultTimestampPrecisionForDialect(dialect));
        int64_t ticksPerSecond = fqTimestampTicksPerSecond(precision);
        qDebug("FQ-SQL: SPREAD %s timestamp render precision=%d ticksPerSecond=%" PRId64,
               dialectName(dialect), (int)precision, ticksPerSecond);
        if (dialect == EXT_SQL_DIALECT_MYSQL) {
          dynSQLAppendStr(s, "((TIMESTAMPDIFF(MICROSECOND, MIN(");
          int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, "), MAX(");
          code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ")) / 1000000.0) * ");
          dynSQLAppendf(s, "%" PRId64, ticksPerSecond);
          dynSQLAppendChar(s, ')');
        } else {
          dynSQLAppendStr(s, "(EXTRACT(EPOCH FROM (MAX(");
          int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, ") - MIN(");
          code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
          if (code) return code;
          dynSQLAppendStr(s, "))) * ");
          dynSQLAppendf(s, "%" PRId64, ticksPerSecond);
          dynSQLAppendChar(s, ')');
        }
        return TSDB_CODE_SUCCESS;
      }
      dynSQLAppendStr(s, "(MAX(");
      int32_t code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
      if (code) return code;
      dynSQLAppendStr(s, ") - MIN(");
      code = dynAppendExpr(s, pArg, dialect, pExtMeta, pCtx);
      if (code) return code;
      dynSQLAppendStr(s, "))");
      return TSDB_CODE_SUCCESS;
    }
    case FUNCTION_TYPE_TIMEDIFF: {
      // DS §5.3.4.1.9: TIMEDIFF(e1, e2[, unit])
      //   MySQL → TIMESTAMPDIFF(unit, e2, e1)  (note: reversed parameter order)
      //   PG/Influx → EXTRACT(EPOCH FROM (e1 - e2))
      if (nParams < 2) {
        qError("FQ-SQL: TIMEDIFF missing params (nParams=%d)", nParams);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      SNode* pE1 = nodesListGetNode(pFunc->pParameterList, 0);
      SNode* pE2 = nodesListGetNode(pFunc->pParameterList, 1);
      SNode* pUnitArg = (nParams >= 4) ? nodesListGetNode(pFunc->pParameterList, 2) : NULL;
      SNode* pPrecArg = nodesListGetNode(pFunc->pParameterList, nParams - 1);
      const char* pUnitLiteral = NULL;
      int64_t unitValue = 1;
      int8_t unitToken = 0;
      int8_t unitType = TSDB_DATA_TYPE_NULL;
      int8_t precision = TSDB_TIME_PRECISION_MICRO;
      if (pUnitArg != NULL && nodeType(pUnitArg) == QUERY_NODE_VALUE) {
        const SValueNode* pUnitVal = (const SValueNode*)pUnitArg;
        pUnitLiteral = pUnitVal->literal;
        unitValue = pUnitVal->datum.i;
        unitToken = pUnitVal->unit;
        unitType = pUnitVal->node.resType.type;
      }
      if (pPrecArg != NULL && nodeType(pPrecArg) == QUERY_NODE_VALUE) {
        precision = fqNormalizeTimePrecision((int8_t)((const SValueNode*)pPrecArg)->datum.i,
                                             TSDB_TIME_PRECISION_MICRO);
      }
      qDebug("FQ-SQL: TIMEDIFF dialect=%s nParams=%d unitLiteral=%s unitValue=%" PRId64
             " unitToken=%d unitType=%d(%s) precision=%d",
             dialectName(dialect), nParams, pUnitLiteral ? pUnitLiteral : "(null)", unitValue,
             (int)unitToken, (int)unitType, fqDebugDataTypeName(unitType), (int)precision);
      if (dialect == EXT_SQL_DIALECT_MYSQL) {
        // TIMESTAMPDIFF(MICROSECOND, e2, e1)
        qDebug("FQ-SQL: TIMEDIFF MySQL render uses fixed unit MICROSECOND regardless of unitLiteral=%s",
               pUnitLiteral ? pUnitLiteral : "(null)");
        dynSQLAppendStr(s, "TIMESTAMPDIFF(MICROSECOND, ");
        int32_t code = dynAppendExpr(s, pE2, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", ");
        code = dynAppendExpr(s, pE1, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
      } else {
        if (dialect == EXT_SQL_DIALECT_POSTGRES) {
          int64_t ticksPerSecond = fqTimestampTicksPerSecond(precision);
          qDebug("FQ-SQL: TIMEDIFF PG render ticksPerSecond=%" PRId64 " unitValue=%" PRId64,
                 ticksPerSecond, unitValue);
          dynSQLAppendStr(s, "CAST((EXTRACT(EPOCH FROM (");
          int32_t code = dynRenderPgTemporalDiffOperand(s, pE1, pExtMeta, pCtx, precision);
          if (code) return code;
          dynSQLAppendStr(s, " - ");
          code = dynRenderPgTemporalDiffOperand(s, pE2, pExtMeta, pCtx, precision);
          if (code) return code;
          dynSQLAppendStr(s, ")) * ");
          dynSQLAppendf(s, "%" PRId64, ticksPerSecond);
          if (unitValue > 1) {
            dynSQLAppendStr(s, " / ");
            dynSQLAppendf(s, "%" PRId64, unitValue);
          }
          dynSQLAppendStr(s, ") AS BIGINT)");
        } else {
          qDebug("FQ-SQL: TIMEDIFF %s render epoch-diff unitValue=%" PRId64 " unitLiteral=%s",
                 dialectName(dialect), unitValue,
                 pUnitLiteral ? pUnitLiteral : "(null)");
          dynSQLAppendStr(s, "CAST(((");
          int32_t code = dynRenderInfluxTemporalDiffEpochOperand(s, pE1, pE2, pExtMeta, pCtx,
                                                                 precision);
          if (code) return code;
          dynSQLAppendStr(s, ") - (");
          code = dynRenderInfluxTemporalDiffEpochOperand(s, pE2, pE1, pExtMeta, pCtx, precision);
          if (code) return code;
          dynSQLAppendStr(s, "))");
          if (unitValue > 1) {
            dynSQLAppendStr(s, " / ");
            dynSQLAppendf(s, "%" PRId64, unitValue);
          }
          dynSQLAppendStr(s, " AS BIGINT)");
        }
      }
      return TSDB_CODE_SUCCESS;
    }
    case FUNCTION_TYPE_MODE: {
      // DS §5.3.4.1.13: PG uses ordered-set aggregate MODE() WITHIN GROUP (ORDER BY expr).
      // MySQL/InfluxDB have no built-in MODE; disabled via capability matrix.
      if (dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: MODE() not supported by %s", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1) {
        dynSQLAppendStr(s, "MODE() WITHIN GROUP (ORDER BY ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "MODE", pFunc, dialect, pExtMeta, pCtx);
    }
    case FUNCTION_TYPE_PERCENTILE: {
      // DS §5.3.4.1.11: PG → PERCENTILE_CONT(p/100.0) WITHIN GROUP (ORDER BY expr).
      // MySQL/InfluxDB: disabled via capability matrix.
      if (dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: PERCENTILE() not supported by %s", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 2) {
        // PERCENTILE(expr, p): pParameterList[0]=expr, [1]=p (0..100)
        SNode* pPNode = nodesListGetNode(pFunc->pParameterList, 1);
        dynSQLAppendStr(s, "PERCENTILE_CONT(");
        int32_t code = dynAppendExpr(s, pPNode, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, " / 100.0) WITHIN GROUP (ORDER BY ");
        code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      qError("FQ-SQL: PERCENTILE() unsupported dialect=%d nParams=%d; deferring to local", (int)dialect, nParams);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    case FUNCTION_TYPE_APERCENTILE: {
      qDebug("FQ-SQL: keep APERCENTILE() local dialect=%s nParams=%d", dialectName(dialect), nParams);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    case FUNCTION_TYPE_LAG: {
      // DS §5.3.4.1.13: LAG(expr, offset[, default]) — needs OVER (ORDER BY ts).
      // The TDengine parser appends the primary-key timestamp as the LAST
      // parameter for implicit-ts functions (appendTsForImplicitTsFuncImpl), so:
      //   LAG(voltage,1)          -> [voltage, 1, ts]
      //   LAG(voltage,1,default)  -> [voltage, 1, default, ts]
      // Render LAG(<all params except the last>) OVER (ORDER BY <last param>).
      if (nParams < 3) {
        qError("FQ-SQL: LAG() needs expr+offset+ts; got %d params", nParams);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      int32_t tsIdx = nParams - 1;  // last param = primary-key ts ordering column
      dynSQLAppendStr(s, "LAG(");
      for (int32_t i = 0; i < tsIdx; i++) {
        if (i > 0) dynSQLAppendStr(s, ", ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, i), dialect, pExtMeta, pCtx);
        if (code) return code;
      }
      dynSQLAppendStr(s, ") OVER (ORDER BY ");
      int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, tsIdx), dialect, pExtMeta, pCtx);
      if (code) return code;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case FUNCTION_TYPE_LEAD: {
      // DS §5.3.4.1.13: LEAD(expr, offset[, default]) — needs OVER (ORDER BY ts).
      // Parser appends the primary-key ts as the LAST param (see LAG above).
      if (nParams < 3) {
        qError("FQ-SQL: LEAD() needs expr+offset+ts; got %d params", nParams);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      int32_t leadTsIdx = nParams - 1;  // last param = primary-key ts ordering column
      dynSQLAppendStr(s, "LEAD(");
      for (int32_t i = 0; i < leadTsIdx; i++) {
        if (i > 0) dynSQLAppendStr(s, ", ");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, i), dialect, pExtMeta, pCtx);
        if (code) return code;
      }
      dynSQLAppendStr(s, ") OVER (ORDER BY ");
      int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, leadTsIdx), dialect, pExtMeta, pCtx);
      if (code) return code;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }

    /* ── Geospatial functions ─────────────────────────────────────────── */
    case FUNCTION_TYPE_GEOM_FROM_TEXT:
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_GeomFromText not supported by InfluxDB");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      if (dialect == EXT_SQL_DIALECT_POSTGRES && nParams >= 1) {
        dynSQLAppendStr(s, "ST_GeomFromText(");
        int32_t code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, 0), dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, "::text");
        for (int32_t i = 1; i < nParams; ++i) {
          dynSQLAppendStr(s, ", ");
          code = dynAppendExpr(s, nodesListGetNode(pFunc->pParameterList, i), dialect, pExtMeta, pCtx);
          if (code) return code;
        }
        dynSQLAppendChar(s, ')');
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }
      return dynRenderNamedFunc(s, "ST_GeomFromText",     pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_AS_TEXT:
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_AsText not supported by %s", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_AsText",           pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_MAKE_POINT:
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_MakePoint not supported by InfluxDB");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_MakePoint",        pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_COVERS:
      // DS §5.3.4.1.17: MySQL has no ST_Covers(); disabled via capability matrix.
      if (dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_Covers not supported by %s", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_Covers",           pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CONTAINS:
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_Contains not supported by InfluxDB");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_Contains",         pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_CONTAINS_PROPERLY:
      // DS §5.3.4.1.17: MySQL has no ST_ContainsProperly(); disabled via capability matrix.
      if (dialect == EXT_SQL_DIALECT_MYSQL || dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_ContainsProperly not supported by %s", dialectName(dialect));
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_ContainsProperly", pFunc, dialect, pExtMeta, pCtx);

    case FUNCTION_TYPE_EQUALS:
      // DS §5.3.4.1.17: MySQL/PG → ST_Equals(A, B); InfluxDB → local.
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_Equals not supported by InfluxDB; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_Equals",    pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_INTERSECTS:
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_Intersects not supported by InfluxDB; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_Intersects", pFunc, dialect, pExtMeta, pCtx);
    case FUNCTION_TYPE_TOUCHES:
      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        qError("FQ-SQL: ST_Touches not supported by InfluxDB; deferring to local");
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return dynRenderNamedFunc(s, "ST_Touches",   pFunc, dialect, pExtMeta, pCtx);

    /* ── FQ internal: time bucket (INTERVAL pushdown) ───────────────── */
    case FUNCTION_TYPE_FQ_TIME_BUCKET: {
      // pParameterList[0] = SColumnNode for the timestamp primary key column
      // pParameterList[1] = SValueNode (int64, ticks in srcPrecision) for bucket size
      // pParameterList[2] = SValueNode (int8,  TSDB_TIME_PRECISION_*) for source precision
      // pParameterList[3] = SValueNode (int64, optional origin offset in srcPrecision ticks)
      if (nParams < 3) {
        qError("FQ-SQL: FQ_TIME_BUCKET missing params (nParams=%d)", nParams);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      SNode* pTsArg   = nodesListGetNode(pFunc->pParameterList, 0);
      SNode* pTickArg = nodesListGetNode(pFunc->pParameterList, 1);
      SNode* pPrecArg = nodesListGetNode(pFunc->pParameterList, 2);
      SNode* pOffsetArg = (nParams >= 4) ? nodesListGetNode(pFunc->pParameterList, 3) : NULL;
      if (!pTsArg || !pTickArg || !pPrecArg ||
          nodeType(pTickArg) != QUERY_NODE_VALUE ||
          nodeType(pPrecArg) != QUERY_NODE_VALUE ||
          (pOffsetArg != NULL && nodeType(pOffsetArg) != QUERY_NODE_VALUE)) {
        qError("FQ-SQL: FQ_TIME_BUCKET bad param types (nParams=%d offsetType=%d)",
               nParams, pOffsetArg ? (int)nodeType(pOffsetArg) : -1);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      int64_t ticks = ((const SValueNode*)pTickArg)->datum.i;
      int64_t offsetTicks = (pOffsetArg != NULL) ? ((const SValueNode*)pOffsetArg)->datum.i : 0;
      int8_t  prec  = (int8_t)((const SValueNode*)pPrecArg)->datum.i;
      if (ticks <= 0) {
        qError("FQ-SQL: FQ_TIME_BUCKET ticks=%" PRId64 " invalid", ticks);
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }

      char tickBuf[32];
      char offsetBuf[32];

      if (dialect == EXT_SQL_DIALECT_MYSQL) {
        // MySQL uses µs precision (TSDB_TIME_PRECISION_MICRO).
        // Convert ticks to µs based on source precision, then generate:
        //   FLOOR((UNIX_TIMESTAMP(ts) * 1000000 - offset_us) / N_us) * N_us + offset_us
        int64_t n_us = 0;
        int64_t offset_us = 0;
        int32_t code = fqTicksToMysqlMicros(ticks, prec, &n_us);
        if (code != TSDB_CODE_SUCCESS) return code;
        code = fqTicksToMysqlMicros(offsetTicks, prec, &offset_us);
        if (code != TSDB_CODE_SUCCESS) return code;
        if (n_us <= 0) {
          qError("FQ-SQL: FQ_TIME_BUCKET MySQL: n_us=%" PRId64 " invalid offset=%" PRId64, n_us, offset_us);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        (void)snprintf(tickBuf, sizeof(tickBuf), "%" PRId64, n_us);
        (void)snprintf(offsetBuf, sizeof(offsetBuf), "%" PRId64, offset_us);
        // Wrap with FROM_UNIXTIME so MySQL returns DATETIME (not DECIMAL).
        // DATETIME is bound as MYSQL_TYPE_TIMESTAMP (MYSQL_TIME struct) and
        // converted to epoch µs by mysqlTimeToEpochUs in the connector.
        dynSQLAppendStr(s, "FROM_UNIXTIME((FLOOR((UNIX_TIMESTAMP(");
        code = dynAppendExpr(s, pTsArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ") * 1000000 - ");
        dynSQLAppendStr(s, offsetBuf);
        dynSQLAppendStr(s, ") / ");
        dynSQLAppendStr(s, tickBuf);
        dynSQLAppendStr(s, ") * ");
        dynSQLAppendStr(s, tickBuf);
        dynSQLAppendStr(s, " + ");
        dynSQLAppendStr(s, offsetBuf);
        dynSQLAppendStr(s, ") / 1000000)");
        return TSDB_CODE_SUCCESS;
      }

      if (dialect == EXT_SQL_DIALECT_POSTGRES) {
        // PostgreSQL uses µs precision.
        // DATE_BIN('N_us microseconds', ts, TIMESTAMP '1970-01-01 00:00:00' + origin_offset)
        int64_t n_us = 0;
        int64_t offset_us = 0;
        int32_t code = fqTicksToPgMicros(ticks, prec, &n_us);
        if (code != TSDB_CODE_SUCCESS) return code;
        code = fqTicksToPgMicros(offsetTicks, prec, &offset_us);
        if (code != TSDB_CODE_SUCCESS) return code;
        if (n_us <= 0) {
          qError("FQ-SQL: FQ_TIME_BUCKET PG: n_us=%" PRId64 " invalid offset=%" PRId64, n_us, offset_us);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        char buf[64];
        (void)snprintf(buf, sizeof(buf), "DATE_BIN('%" PRId64 " microseconds', ", n_us);
        dynSQLAppendStr(s, buf);
        code = dynAppendExpr(s, pTsArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", TIMESTAMP '1970-01-01 00:00:00'");
        if (offset_us != 0) {
          dynSQLAppendStr(s, " + (");
          dynSQLAppendf(s, "%" PRId64, offset_us);
          dynSQLAppendStr(s, " * INTERVAL '1 microsecond')");
        }
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }

      if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
        // InfluxDB v3 uses ns precision.
        // DATE_BIN(INTERVAL 'N_ns nanoseconds', time, TIMESTAMP '1970-01-01T00:00:00Z' + origin_offset)
        int64_t n_ns = 0;
        int64_t offset_ns = 0;
        int32_t code = fqTicksToInfluxNanos(ticks, prec, &n_ns);
        if (code != TSDB_CODE_SUCCESS) return code;
        code = fqTicksToInfluxNanos(offsetTicks, prec, &offset_ns);
        if (code != TSDB_CODE_SUCCESS) return code;
        if (n_ns <= 0) {
          qError("FQ-SQL: FQ_TIME_BUCKET InfluxDB: n_ns=%" PRId64 " invalid offset=%" PRId64, n_ns, offset_ns);
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        char buf[64];
        (void)snprintf(buf, sizeof(buf), "DATE_BIN(INTERVAL '%" PRId64 " nanoseconds', ", n_ns);
        dynSQLAppendStr(s, buf);
        code = dynAppendExpr(s, pTsArg, dialect, pExtMeta, pCtx);
        if (code) return code;
        dynSQLAppendStr(s, ", TIMESTAMP '1970-01-01T00:00:00Z'");
        if (offset_ns != 0) {
          dynSQLAppendStr(s, " + INTERVAL '");
          dynSQLAppendf(s, "%" PRId64, offset_ns);
          dynSQLAppendStr(s, " nanoseconds'");
        }
        dynSQLAppendChar(s, ')');
        return TSDB_CODE_SUCCESS;
      }

      qError("FQ-SQL: FQ_TIME_BUCKET unsupported dialect=%d", (int)dialect);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }

    default:
      qError("FQ-SQL: dynAppendFunctionExpr unsupported funcType=%d funcName=%s dialect=%d", (int)funcType, pFunc->functionName, (int)dialect);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
}

static int32_t dynAppendExpr(SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect,
                              const SExtTableMeta* pExtMeta,
                              const SNodesRemoteSQLCtx* pCtx) {
  if (!pExpr) return TSDB_CODE_SUCCESS;

  if (NULL == pExtMeta && NULL != pCtx && pCtx->inputSelectExprsFromChildSource &&
      QUERY_NODE_COLUMN != nodeType((SNode*)pExpr) && nodesIsExprNode((SNode*)pExpr)) {
    const char* pInputAlias =
        fqFindEquivalentInputAlias((const SExprNode*)pExpr, pCtx->pInputSelectExprs);
    if (NULL != pInputAlias && '\0' != pInputAlias[0]) {
      qDebug("FQ-SQL-REF: rewrite child-scope expr type=%s -> inputAlias=%s dialect=%s",
             nodesNodeName(nodeType((SNode*)pExpr)), pInputAlias, dialectName(dialect));
      dynAppendQuotedId(s, pInputAlias, dialect);
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
  }

  switch (nodeType(pExpr)) {
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pExpr;
      if (pCtx != NULL && pCtx->rewriteAggHavingColumn) {
        const SFunctionNode* pHavingFunc = findHavingAggFuncBySlot(pCtx->pHavingAggFuncs, pCol);
        if (pHavingFunc != NULL) {
          qDebug("FQ-SQL-HAVING: rewrite AGG slot column=%s alias=%s func=%s dialect=%s",
                 pCol->colName,
                 pHavingFunc->node.aliasName,
                 pHavingFunc->functionName,
                 dialectName(dialect));
          return dynAppendFunctionExpr(s, pHavingFunc, dialect, pExtMeta, pCtx);
        }

        qDebug("FQ-SQL-HAVING: keep column=%s as ordinary reference (no AGG slot match), table=%s alias=%s dialect=%s",
               pCol->colName,
               pCol->tableName,
               pCol->tableAlias,
               dialectName(dialect));
      }
      // JOIN leg disambiguation: prefix with the column's own tableAlias (s/r)
      // so same-named columns from the two legs are unambiguous in SELECT/ON/WHERE.
      if (pCtx && pCtx->useJoinLegAlias && pCol->tableAlias[0] != '\0') {
        dynAppendQuotedId(s, pCol->tableAlias, dialect);
        dynSQLAppendChar(s, '.');
      } else if (pCtx && pCtx->includeTableName && pCol->tableName[0] != '\0') {
        // When includeTableName is requested (e.g. for correlated EXISTS body),
        // prefix the column with its table name so the generated SQL is
        // self-contained when embedded as a subquery.
        dynAppendQuotedId(s, pCol->tableName, dialect);
        dynSQLAppendChar(s, '.');
      }
      // When pExtMeta is NULL the column references the CURRENT statement output
      // namespace (subquery / wrapper / derived table), not a real external
      // table column.  Those outputs must be referenced by the final emitted
      // remote SELECT alias (normally the internal slot aliasName like "expr_1"),
      // while planner-carried column refs may still arrive as colName, aliasName,
      // or userAlias depending on clause / rewrite path.  Normalize them here
      // before falling back to the raw column name.
      if (NULL == pExtMeta) {
        const SExprNode* pMatchedExpr = NULL;
        const char* pOutputAlias = fqResolveOutputRefAlias(pCol, pCtx, &pMatchedExpr);
        if (NULL != pOutputAlias && '\0' != pOutputAlias[0]) {
          qDebug("FQ-SQL-REF: rewrite output ref col=%s alias=%s userAlias=%s -> selectAlias=%s exprType=%s dialect=%s",
                 pCol->colName, pCol->node.aliasName, pCol->node.userAlias,
                 pOutputAlias, pMatchedExpr ? nodesNodeName(nodeType((SNode*)pMatchedExpr)) : "(unknown)",
                 dialectName(dialect));
          dynAppendQuotedId(s, pOutputAlias, dialect);
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }
        if (pCol->hasRef && pCol->refColName[0] != '\0') {
          qDebug("FQ-SQL-REF: fallback to ref column col=%s alias=%s userAlias=%s -> refCol=%s dialect=%s",
                 pCol->colName, pCol->node.aliasName, pCol->node.userAlias, pCol->refColName,
                 dialectName(dialect));
          dynAppendQuotedId(s, pCol->refColName, dialect);
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }
      }
      dynAppendQuotedId(s, pCol->colName, dialect);
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
      // Scalar subquery result (single scalar), e.g. col > (SELECT AVG(x) ...).
      // v2 two-state: DIRECT_SQL inlines val.literal (original SQL); ORIG_TYPE_2B
      // renders the attached inner logic plan with dialect transforms (2b).
      const SRemoteValueNode* pVal = (const SRemoteValueNode*)pExpr;
      dynSQLAppendChar(s, '(');
      int32_t ec = dynAppendSubqBodyByMode(s, pVal->renderMode, pVal->val.literal,
                                           pVal->pRemotePlan, dialect, pVal->subQIdx);
      if (ec != TSDB_CODE_SUCCESS) return ec;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ZERO_ROWS: {
      // Standalone EXISTS subquery row-count node.  EXISTS/NOT EXISTS are
      // normally rendered at the operator layer (REMOTE_ZERO_ROWS > 0).  If a
      // bare node reaches here it still means pushdown was chosen.
      const SRemoteZeroRowsNode* pRows = (const SRemoteZeroRowsNode*)pExpr;
      dynSQLAppendStr(s, "EXISTS (");
      int32_t ec = dynAppendSubqBodyByMode(s, pRows->renderMode, pRows->val.literal,
                                           pRows->pRemotePlan, dialect, pRows->subQIdx);
      if (ec != TSDB_CODE_SUCCESS) return ec;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ROW: {
      // Standalone quantified/scalar subquery node.  Quantified comparisons are
      // rendered at the operator layer (left op ALL|ANY (subq)).  A bare node
      // reaching here still means pushdown was chosen.
      const SRemoteRowNode* pRow = (const SRemoteRowNode*)pExpr;
      dynSQLAppendChar(s, '(');
      int32_t ec = dynAppendSubqBodyByMode(s, pRow->renderMode, pRow->pSubquerySql,
                                           pRow->pRemotePlan, dialect, pRow->subQIdx);
      if (ec != TSDB_CODE_SUCCESS) return ec;
      dynSQLAppendChar(s, ')');
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_CASE_WHEN: {
      // CASE [expr] WHEN cond THEN result [...] [ELSE result] END
      const SCaseWhenNode* pCW = (const SCaseWhenNode*)pExpr;

      // Detect COALESCE(a, b, ...) pattern:
      //   CASE WHEN a IS NOT NULL THEN a [WHEN b IS NOT NULL THEN b...] END (no pElse)
      // TDengine may constant-fold literal IS NOT NULL to TRUE.
      if (!pCW->pCase && !pCW->pElse && pCW->pWhenThenList &&
          LIST_LENGTH(pCW->pWhenThenList) >= 1) {
        bool isCoalesce = true;
        SNode* pCheckItem = NULL;
        FOREACH(pCheckItem, pCW->pWhenThenList) {
          const SWhenThenNode* pWT2 = (const SWhenThenNode*)pCheckItem;
          bool whenIsNotNull = (nodeType(pWT2->pWhen) == QUERY_NODE_OPERATOR &&
                                ((const SOperatorNode*)pWT2->pWhen)->opType == OP_TYPE_IS_NOT_NULL);
          bool whenIsTrue = false;
          if (nodeType(pWT2->pWhen) == QUERY_NODE_VALUE) {
            const SValueNode* pV = (const SValueNode*)pWT2->pWhen;
            if (pV->node.resType.type == TSDB_DATA_TYPE_BOOL && pV->datum.b) whenIsTrue = true;
          }
          if (!whenIsNotNull && !whenIsTrue) { isCoalesce = false; break; }
        }
        if (isCoalesce) {
          dynSQLAppendStr(s, "COALESCE(");
          bool first2 = true;
          SNode* pItem2 = NULL;
          FOREACH(pItem2, pCW->pWhenThenList) {
            if (!first2) dynSQLAppendStr(s, ", ");
            first2 = false;
            const SWhenThenNode* pWT2 = (const SWhenThenNode*)pItem2;
            int32_t code = dynAppendExpr(s, pWT2->pThen, dialect, pExtMeta, pCtx);
            if (code != TSDB_CODE_SUCCESS) return code;
          }
          dynSQLAppendChar(s, ')');
          return TSDB_CODE_SUCCESS;
        }
      }

      // Detect IFNULL(a, b) / NVL(a, b) pattern:
      //   CASE WHEN a IS NOT NULL THEN a ELSE b END  (1 WHEN, has pElse)
      // DS §5.3.4.1.14: MySQL → IFNULL(a,b); PG/Influx → COALESCE(a,b).
      if (!pCW->pCase && pCW->pElse && pCW->pWhenThenList &&
          LIST_LENGTH(pCW->pWhenThenList) == 1) {
        const SWhenThenNode* pWT2 = (const SWhenThenNode*)nodesListGetNode(pCW->pWhenThenList, 0);
        bool whenIsNotNull = (pWT2 && nodeType(pWT2->pWhen) == QUERY_NODE_OPERATOR &&
                              ((const SOperatorNode*)pWT2->pWhen)->opType == OP_TYPE_IS_NOT_NULL);
        if (whenIsNotNull) {
          // Check that THEN expr matches the operand of IS NOT NULL
          const SOperatorNode* pIsNotNull = (const SOperatorNode*)pWT2->pWhen;
          if (nodesEqualNode(pIsNotNull->pLeft, pWT2->pThen)) {
            const char* funcName = (dialect == EXT_SQL_DIALECT_MYSQL) ? "IFNULL" : "COALESCE";
            dynSQLAppendStr(s, funcName);
            dynSQLAppendChar(s, '(');
            int32_t code = dynAppendExpr(s, pWT2->pThen, dialect, pExtMeta, pCtx);
            if (code != TSDB_CODE_SUCCESS) return code;
            dynSQLAppendStr(s, ", ");
            code = dynAppendExpr(s, pCW->pElse, dialect, pExtMeta, pCtx);
            if (code != TSDB_CODE_SUCCESS) return code;
            dynSQLAppendChar(s, ')');
            return TSDB_CODE_SUCCESS;
          }
        }
      }

      // Detect NULLIF(expr1, expr2) pattern:
      //   CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END
      // Must be checked BEFORE the MySQL IF() pattern below, because NULLIF is
      // more specific: the THEN clause is always NULL.  If MySQL IF() were checked
      // first, "CASE WHEN a=b THEN NULL ELSE a" would be misrendered as IF(a=b,NULL,a).
      if (!pCW->pCase && pCW->pElse && pCW->pWhenThenList &&
          LIST_LENGTH(pCW->pWhenThenList) == 1) {
        const SWhenThenNode* pWT2 = (const SWhenThenNode*)nodesListGetNode(pCW->pWhenThenList, 0);
        bool whenIsEq = (pWT2 && nodeType(pWT2->pWhen) == QUERY_NODE_OPERATOR &&
                         ((const SOperatorNode*)pWT2->pWhen)->opType == OP_TYPE_EQUAL);
        bool thenIsNull = (pWT2 && nodeType(pWT2->pThen) == QUERY_NODE_VALUE &&
                           ((const SValueNode*)pWT2->pThen)->isNull);
        if (whenIsEq && thenIsNull) {
          const SOperatorNode* pEqOp = (const SOperatorNode*)pWT2->pWhen;
          dynSQLAppendStr(s, "NULLIF(");
          int32_t code = dynAppendExpr(s, pEqOp->pLeft, dialect, pExtMeta, pCtx);
          if (code != TSDB_CODE_SUCCESS) return code;
          dynSQLAppendStr(s, ", ");
          code = dynAppendExpr(s, pEqOp->pRight, dialect, pExtMeta, pCtx);
          if (code != TSDB_CODE_SUCCESS) return code;
          dynSQLAppendChar(s, ')');
          return TSDB_CODE_SUCCESS;
        }
      }

      // Detect MySQL IF(cond, then, else) pattern:
      //   CASE WHEN cond THEN then_val ELSE else_val END (1 WHEN, has pElse, no pCase)
      // DS §5.3.4.1.14: MySQL supports IF(cond, then, else) natively.
      // TDengine parser rewrites IF()/NVL2() to CASE WHEN; we convert back for MySQL.
      // Note: placed AFTER NULLIF detection so that NULLIF patterns are not misidentified.
      if (dialect == EXT_SQL_DIALECT_MYSQL && !pCW->pCase && pCW->pElse &&
          pCW->pWhenThenList && LIST_LENGTH(pCW->pWhenThenList) == 1) {
        const SWhenThenNode* pWT2 = (const SWhenThenNode*)nodesListGetNode(pCW->pWhenThenList, 0);
        if (pWT2) {
          dynSQLAppendStr(s, "IF(");
          int32_t code = dynAppendExpr(s, pWT2->pWhen, dialect, pExtMeta, pCtx);
          if (code != TSDB_CODE_SUCCESS) return code;
          dynSQLAppendStr(s, ", ");
          code = dynAppendExpr(s, pWT2->pThen, dialect, pExtMeta, pCtx);
          if (code != TSDB_CODE_SUCCESS) return code;
          dynSQLAppendStr(s, ", ");
          code = dynAppendExpr(s, pCW->pElse, dialect, pExtMeta, pCtx);
          if (code != TSDB_CODE_SUCCESS) return code;
          dynSQLAppendChar(s, ')');
          return TSDB_CODE_SUCCESS;
        }
      }

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
    case QUERY_NODE_FUNCTION:
      return dynAppendFunctionExpr(s, (const SFunctionNode*)pExpr, dialect, pExtMeta, pCtx);
    case QUERY_NODE_GROUPING_SET: {
      // SGroupingSetNode wraps one or more expressions in pParameterList.
      // For GROUP BY / DISTINCT, each grouping set holds exactly one expression.
      const SGroupingSetNode* pGS = (const SGroupingSetNode*)pExpr;
      if (pGS->pParameterList && LIST_LENGTH(pGS->pParameterList) > 0) {
        return dynAppendExpr(s, nodesListGetNode(pGS->pParameterList, 0), dialect, pExtMeta, pCtx);
      }
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
static int32_t renderNode(const SLogicNode* pNode, EExtSQLDialect dialect,
                           const SNodesRemoteSQLCtx* pCtx, SDynSQL* s);
static int32_t renderJoin(const SJoinLogicNode* pJoin, EExtSQLDialect dialect,
                          const SNodesRemoteSQLCtx* pCtx, SDynSQL* s);
static void renderTablePathWithAlias(SDynSQL* s, const SExtTableNode* pExt, EExtSQLDialect dialect);

static const SExtTableNode* renderJoinFindLeafExtSource(const SLogicNode* pSource) {
  if (NULL == pSource) {
    return NULL;
  }

  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pSource)) {
    const SScanLogicNode* pScan = (const SScanLogicNode*)pSource;
    if (NULL != pScan->pExtTableNode && QUERY_NODE_EXTERNAL_TABLE == nodeType(pScan->pExtTableNode)) {
      return (const SExtTableNode*)pScan->pExtTableNode;
    }
  }

  if (NULL == pSource->pChildren) {
    return NULL;
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pSource->pChildren) {
    const SExtTableNode* pExt = renderJoinFindLeafExtSource((const SLogicNode*)pChild);
    if (NULL != pExt) {
      return pExt;
    }
  }

  return NULL;
}

static int32_t renderJoinDerivedSource(const SLogicNode* pSource, EExtSQLDialect dialect,
                                       const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  const SExtTableNode* pLeafExt = renderJoinFindLeafExtSource(pSource);
  const char*          pAlias = (NULL != pLeafExt && pLeafExt->table.tableAlias[0] != '\0')
                                    ? pLeafExt->table.tableAlias
                                    : "_sq";

  qDebug("FQ-SQL: render join source derived nodeType=%s source=%p alias=%s leafTable=%s",
         nodesNodeName(nodeType((SNode*)pSource)), pSource, pAlias,
         (NULL != pLeafExt) ? pLeafExt->table.tableName : "(null)");

  dynSQLAppendChar(s, '(');
  int32_t code = renderNode(pSource, dialect, pCtx, s);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  dynSQLAppendStr(s, ") ");
  dynAppendQuotedId(s, pAlias, dialect);
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t renderJoinSource(const SLogicNode* pSource, EExtSQLDialect dialect,
                                const SNodesRemoteSQLCtx* pCtx, SDynSQL* s, bool wrapJoin) {
  if (NULL == pSource) {
    qError("FQ-SQL: renderJoinSource got NULL source");
    return TSDB_CODE_INVALID_PARA;
  }

  switch (nodeType(pSource)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      const SScanLogicNode* pScan = (const SScanLogicNode*)pSource;
      if (NULL == pScan->pExtTableNode) {
        qError("FQ-SQL: renderJoinSource scan missing pExtTableNode scan=%p", pScan);
        return TSDB_CODE_EXT_SQL_RENDER_ERROR;
      }
      qDebug("FQ-SQL: render join source scan=%p alias=%s table=%s",
             pScan, ((const SExtTableNode*)pScan->pExtTableNode)->table.tableAlias,
             ((const SExtTableNode*)pScan->pExtTableNode)->table.tableName);
      renderTablePathWithAlias(s, (const SExtTableNode*)pScan->pExtTableNode, dialect);
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      const SJoinLogicNode* pJoin = (const SJoinLogicNode*)pSource;
      if (pJoin->subType == JOIN_STYPE_SEMI || pJoin->subType == JOIN_STYPE_ANTI) {
        qError("FQ-SQL: renderJoinSource does not support SEMI/ANTI join leg join=%p subType=%d",
               pJoin, (int)pJoin->subType);
        return TSDB_CODE_EXT_SQL_RENDER_ERROR;
      }

      qDebug("FQ-SQL: render join source nested join=%p joinType=%d subType=%d wrap=%d",
             pJoin, (int)pJoin->joinType, (int)pJoin->subType, (int)wrapJoin);
      if (wrapJoin) {
        dynSQLAppendChar(s, '(');
      }
      int32_t code = renderJoin(pJoin, dialect, pCtx, s);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (wrapJoin) {
        dynSQLAppendChar(s, ')');
      }
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    default:
      qDebug("FQ-SQL: render join source fallback to derived table nodeType=%s source=%p",
             nodesNodeName(nodeType((SNode*)pSource)), pSource);
      return renderJoinDerivedSource(pSource, dialect, pCtx, s);
  }
}

static int32_t renderJoinTreeCondition(const SNode* pCond, const SExtTableMeta* pMeta,
                                       EExtSQLDialect dialect, const SNodesRemoteSQLCtx* pCtx,
                                       SDynSQL* s, bool* pWroteAny) {
  if (NULL == pCond) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pWroteAny) {
    qError("FQ-SQL: renderJoinTreeCondition got NULL wroteAny");
    return TSDB_CODE_INVALID_PARA;
  }

  if (*pWroteAny) {
    dynSQLAppendStr(s, " AND ");
  }
  dynSQLAppendChar(s, '(');
  int32_t code = dynAppendExpr(s, pCond, dialect, pMeta, pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: renderJoinTreeCondition failed code=0x%x meta=%p condType=%s",
           code, pMeta, nodesNodeName(nodeType((SNode*)pCond)));
    return code;
  }
  dynSQLAppendChar(s, ')');
  *pWroteAny = true;
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t renderJoinTreeWhere(const SLogicNode* pNode, EExtSQLDialect dialect,
                                   const SNodesRemoteSQLCtx* pCtx, SDynSQL* s, bool* pWroteAny) {
  if (NULL == pNode || NULL == pWroteAny) {
    qError("FQ-SQL: renderJoinTreeWhere invalid input node=%p wroteAny=%p", pNode, pWroteAny);
    return TSDB_CODE_INVALID_PARA;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      const SScanLogicNode* pScan = (const SScanLogicNode*)pNode;
      if (NULL == pScan->node.pConditions) {
        return TSDB_CODE_SUCCESS;
      }
      if (NULL == pScan->pExtTableNode) {
        qError("FQ-SQL: renderJoinTreeWhere scan missing pExtTableNode scan=%p", pScan);
        return TSDB_CODE_EXT_SQL_RENDER_ERROR;
      }
      const SExtTableNode* pExt = (const SExtTableNode*)pScan->pExtTableNode;
      qDebug("FQ-SQL: render join-tree scan WHERE scan=%p alias=%s hasCond=1",
             pScan, pExt->table.tableAlias);
      return renderJoinTreeCondition(pScan->node.pConditions, pExt->pExtMeta, dialect, pCtx, s, pWroteAny);
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      const SJoinLogicNode* pJoin = (const SJoinLogicNode*)pNode;
      if (NULL == pJoin->node.pChildren || LIST_LENGTH(pJoin->node.pChildren) != 2) {
        qError("FQ-SQL: renderJoinTreeWhere join has invalid children join=%p children=%d",
               pJoin, pJoin->node.pChildren ? (int)LIST_LENGTH(pJoin->node.pChildren) : 0);
        return TSDB_CODE_EXT_SQL_RENDER_ERROR;
      }

      const SLogicNode* pLeft = (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
      const SLogicNode* pRight = (const SLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
      qDebug("FQ-SQL: render join-tree WHERE join=%p joinType=%d subType=%d leftType=%s rightType=%s hasJoinCond=%d",
             pJoin, (int)pJoin->joinType, (int)pJoin->subType,
             pLeft ? nodesNodeName(nodeType((SNode*)pLeft)) : "(null)",
             pRight ? nodesNodeName(nodeType((SNode*)pRight)) : "(null)",
             NULL != pJoin->node.pConditions);

      int32_t code = renderJoinTreeWhere(pLeft, dialect, pCtx, s, pWroteAny);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      code = renderJoinTreeWhere(pRight, dialect, pCtx, s, pWroteAny);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      return renderJoinTreeCondition(pJoin->node.pConditions, NULL, dialect, pCtx, s, pWroteAny);
    }
    default:
      qError("FQ-SQL: renderJoinTreeWhere unsupported nodeType=%s node=%p",
             nodesNodeName(nodeType((SNode*)pNode)), pNode);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
  }
}

static bool fqColumnMatchesPseudoName(const SColumnNode* pCol, const char* pPseudoName) {
  if (pCol == NULL || pPseudoName == NULL) {
    return false;
  }
  return (0 == strcmp(pCol->colName, pPseudoName) ||
          0 == strcmp(pCol->node.userAlias, pPseudoName) ||
          0 == strcmp(pCol->node.aliasName, pPseudoName));
}

static const SExprNode* fqFindSelectOutputByRefName(const char* pRefName,
                                                    const SNodeList* pSelectExprs);
static const SExprNode* fqFindSelectOutputByColumnRef(const SColumnNode* pCol,
                                                      const SNodeList* pSelectExprs);

static const SExprNode* fqFindInputSelectExprByColumnRef(const SNodesRemoteSQLCtx* pCtx,
                                                         const SColumnNode* pCol) {
  if (pCtx == NULL || pCtx->pInputSelectExprs == NULL || pCol == NULL) {
    return NULL;
  }
  return fqFindAlignedSelectOutputByColumnRef(pCol, pCtx->pInputSelectExprs, pCtx->pInputRefExprs);
}

static bool fqTryFindWindowBucketExpr(const SNodesRemoteSQLCtx* pCtx, const SExprNode** ppBucketExpr) {
  if (ppBucketExpr != NULL) {
    *ppBucketExpr = NULL;
  }
  if (pCtx == NULL) {
    return false;
  }

  const SExprNode* pBucketExpr = fqFindSelectOutputByRefName("_wstart", pCtx->pInputSelectExprs);
  if (pBucketExpr == NULL) {
    pBucketExpr = fqFindSelectOutputByRefName("_wstart", pCtx->pOutputSelectExprs);
  }
  if (pBucketExpr == NULL || nodeType((SNode*)pBucketExpr) != QUERY_NODE_FUNCTION) {
    qDebug("FQ-SQL: current-scope pseudo cannot resolve _wstart from child/current outputs");
    return false;
  }

  const SFunctionNode* pBucketFunc = (const SFunctionNode*)pBucketExpr;
  if (0 != strcmp(pBucketFunc->functionName, "_fq_time_bucket") ||
      pBucketFunc->pParameterList == NULL || LIST_LENGTH(pBucketFunc->pParameterList) < 3) {
    qDebug("FQ-SQL: current-scope pseudo child/current _wstart is not _fq_time_bucket func=%s argc=%d",
           pBucketFunc->functionName,
           pBucketFunc->pParameterList ? LIST_LENGTH(pBucketFunc->pParameterList) : 0);
    return false;
  }

  if (ppBucketExpr != NULL) {
    *ppBucketExpr = pBucketExpr;
  }
  return true;
}

static int32_t fqTicksToMysqlMicros(int64_t ticks, int8_t precision, int64_t* pMicros) {
  if (pMicros == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (precision == TSDB_TIME_PRECISION_MILLI) {
    *pMicros = ticks * 1000LL;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    *pMicros = ticks;
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    if (ticks % 1000 != 0) {
      qError("FQ-SQL: ticks->mysql micros requires ns divisible by 1000 ticks=%" PRId64, ticks);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    *pMicros = ticks / 1000LL;
  } else {
    qError("FQ-SQL: ticks->mysql micros unsupported precision=%d ticks=%" PRId64,
           (int)precision, ticks);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t fqTicksToPgMicros(int64_t ticks, int8_t precision, int64_t* pMicros) {
  return fqTicksToMysqlMicros(ticks, precision, pMicros);
}

static int32_t fqTicksToInfluxNanos(int64_t ticks, int8_t precision, int64_t* pNanos) {
  if (pNanos == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (precision == TSDB_TIME_PRECISION_MILLI) {
    *pNanos = ticks * 1000000LL;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    *pNanos = ticks * 1000LL;
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    *pNanos = ticks;
  } else {
    qError("FQ-SQL: ticks->influx nanos unsupported precision=%d ticks=%" PRId64,
           (int)precision, ticks);
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return TSDB_CODE_SUCCESS;
}

static bool fqTryResolveWindowBucketStep(const SNodesRemoteSQLCtx* pCtx, int64_t* pStepTicks,
                                         int8_t* pPrecision, int64_t* pOffsetTicks,
                                         const char** ppBucketRefName) {
  if (pStepTicks != NULL) {
    *pStepTicks = 0;
  }
  if (pPrecision != NULL) {
    *pPrecision = TSDB_TIME_PRECISION_MICRO;
  }
  if (pOffsetTicks != NULL) {
    *pOffsetTicks = 0;
  }
  if (ppBucketRefName != NULL) {
    *ppBucketRefName = NULL;
  }
  const SExprNode* pBucketExpr = NULL;
  if (!fqTryFindWindowBucketExpr(pCtx, &pBucketExpr)) {
    return false;
  }

  const SFunctionNode* pBucketFunc = (const SFunctionNode*)pBucketExpr;
  const SNode* pStepArg = nodesListGetNode(pBucketFunc->pParameterList, 1);
  const SNode* pPrecArg = nodesListGetNode(pBucketFunc->pParameterList, 2);
  const SNode* pOffsetArg =
      (LIST_LENGTH(pBucketFunc->pParameterList) >= 4) ? nodesListGetNode(pBucketFunc->pParameterList, 3) : NULL;
  if (pStepArg == NULL || nodeType((SNode*)pStepArg) != QUERY_NODE_VALUE ||
      pPrecArg == NULL || nodeType((SNode*)pPrecArg) != QUERY_NODE_VALUE ||
      (pOffsetArg != NULL && nodeType((SNode*)pOffsetArg) != QUERY_NODE_VALUE)) {
    qError("FQ-SQL: current-scope pseudo _fq_time_bucket args invalid stepType=%d precType=%d offsetType=%d",
           pStepArg ? (int)nodeType((SNode*)pStepArg) : -1,
           pPrecArg ? (int)nodeType((SNode*)pPrecArg) : -1,
           pOffsetArg ? (int)nodeType((SNode*)pOffsetArg) : -1);
    return false;
  }

  if (pStepTicks != NULL) {
    *pStepTicks = ((const SValueNode*)pStepArg)->datum.i;
  }
  if (pPrecision != NULL) {
    *pPrecision = fqNormalizeTimePrecision((int8_t)((const SValueNode*)pPrecArg)->datum.i,
                                           TSDB_TIME_PRECISION_MICRO);
  }
  if (pOffsetTicks != NULL && pOffsetArg != NULL) {
    *pOffsetTicks = ((const SValueNode*)pOffsetArg)->datum.i;
  }
  if (ppBucketRefName != NULL) {
    *ppBucketRefName = fqSelectAlias(pBucketExpr, pCtx);
  }

  qDebug("FQ-SQL: current-scope pseudo resolved _wstart alias=%s step=%" PRId64 " offset=%" PRId64 " precision=%d",
         (ppBucketRefName != NULL && *ppBucketRefName != NULL) ? *ppBucketRefName : "(null)",
         pStepTicks ? *pStepTicks : 0, pOffsetTicks ? *pOffsetTicks : 0,
         pPrecision ? (int)*pPrecision : -1);
  return true;
}

static int32_t dynRenderPgIntervalFromTicks(SDynSQL* s, int64_t stepTicks, int8_t precision) {
  qDebug("FQ-SQL: render PG interval from ticks step=%" PRId64 " precision=%d", stepTicks, (int)precision);
  dynSQLAppendChar(s, '(');
  dynSQLAppendf(s, "%" PRId64, stepTicks);
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      dynSQLAppendStr(s, " * INTERVAL '1 millisecond')");
      break;
    case TSDB_TIME_PRECISION_MICRO:
      dynSQLAppendStr(s, " * INTERVAL '1 microsecond')");
      break;
    case TSDB_TIME_PRECISION_NANO:
      dynSQLAppendStr(s, " * INTERVAL '1 nanosecond')");
      break;
    default:
      qError("FQ-SQL: unsupported PG interval precision=%d step=%" PRId64, (int)precision, stepTicks);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t fqTryRenderCurrentScopePseudoColumn(SDynSQL* s, const SColumnNode* pCol,
                                                   EExtSQLDialect dialect,
                                                   const SNodesRemoteSQLCtx* pCtx,
                                                   bool* pRendered, const char** ppRenderedRef) {
  if (pRendered != NULL) {
    *pRendered = false;
  }
  if (ppRenderedRef != NULL) {
    *ppRenderedRef = NULL;
  }
  if (s == NULL || pCol == NULL || pCtx == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  bool wantWend = fqColumnMatchesPseudoName(pCol, "_wend");
  bool wantWduration = fqColumnMatchesPseudoName(pCol, "_wduration");
  if (!wantWend && !wantWduration) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t stepTicks = 0;
  int64_t offsetTicks = 0;
  int8_t precision = TSDB_TIME_PRECISION_MICRO;
  const char* pBucketRefName = NULL;
  if (!fqTryResolveWindowBucketStep(pCtx, &stepTicks, &precision, &offsetTicks, &pBucketRefName)) {
    qError("FQ-SQL: current-scope pseudo render failed name=%s childOutputs=%d",
           wantWend ? "_wend" : "_wduration",
           pCtx->pInputSelectExprs ? LIST_LENGTH((SNodeList*)pCtx->pInputSelectExprs) : 0);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  qDebug("FQ-SQL: render current-scope pseudo name=%s bucketRef=%s step=%" PRId64 " offset=%" PRId64 " precision=%d dialect=%s",
         wantWend ? "_wend" : "_wduration", pBucketRefName ? pBucketRefName : "(null)",
         stepTicks, offsetTicks, (int)precision, dialectName(dialect));

  if (wantWduration) {
    dynSQLAppendf(s, "%" PRId64, stepTicks);
  } else if (dialect == EXT_SQL_DIALECT_POSTGRES) {
    dynSQLAppendChar(s, '(');
    dynAppendQuotedId(s, pBucketRefName, dialect);
    dynSQLAppendStr(s, " + ");
    int32_t code = dynRenderPgIntervalFromTicks(s, stepTicks, precision);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendChar(s, ')');
  } else if (dialect == EXT_SQL_DIALECT_MYSQL) {
    int64_t stepMicros = 0;
    int32_t code = fqTicksToMysqlMicros(stepTicks, precision, &stepMicros);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendStr(s, "TIMESTAMPADD(MICROSECOND, ");
    dynSQLAppendf(s, "%" PRId64, stepMicros);
    dynSQLAppendStr(s, ", ");
    dynAppendQuotedId(s, pBucketRefName, dialect);
    dynSQLAppendChar(s, ')');
  } else if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
    int64_t stepNanos = 0;
    int32_t code = fqTicksToInfluxNanos(stepTicks, precision, &stepNanos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendChar(s, '(');
    dynAppendQuotedId(s, pBucketRefName, dialect);
    dynSQLAppendStr(s, " + INTERVAL '");
    dynSQLAppendf(s, "%" PRId64, stepNanos);
    dynSQLAppendStr(s, " nanoseconds')");
  } else {
    qError("FQ-SQL: current-scope pseudo %s not supported for dialect=%s",
           wantWend ? "_wend" : "_wduration", dialectName(dialect));
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  if (pRendered != NULL) {
    *pRendered = true;
  }
  if (ppRenderedRef != NULL) {
    *ppRenderedRef = wantWend ? "_wend" : "_wduration";
  }
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static const SNode* fqSelectListExprNode(const SNode* pNode) {
  const SNode* pExpr = pNode;
  if (NULL != pExpr && QUERY_NODE_TARGET == nodeType((SNode*)pExpr)) {
    pExpr = ((const STargetNode*)pExpr)->pExpr;
  }
  if (NULL != pExpr && QUERY_NODE_GROUPING_SET == nodeType((SNode*)pExpr)) {
    pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
  }
  return pExpr;
}

static bool fqVisibleNameMatchesExpr(const char* pName, const SNode* pExpr) {
  if (NULL == pName || '\0' == pName[0] || NULL == pExpr || !nodesIsExprNode((SNode*)pExpr)) {
    return false;
  }

  const SExprNode* pExprNode = (const SExprNode*)pExpr;
  if (('\0' != pExprNode->userAlias[0] && 0 == strcmp(pName, pExprNode->userAlias)) ||
      ('\0' != pExprNode->aliasName[0] && 0 == strcmp(pName, pExprNode->aliasName))) {
    return true;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    if ('\0' != pCol->colName[0] && 0 == strcmp(pName, pCol->colName)) {
      return true;
    }
  }

  return false;
}

static bool fqColumnHasPreviousVisibleCollision(const SNodeList* pCols, int32_t exprIdx,
                                                const SColumnNode* pCol,
                                                const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pCols || NULL == pCol || '\0' == pCol->colName[0]) {
    return false;
  }

  const char* pVisibleNames[2] = {fqColumnOutputAlias(pCol, pCtx), pCol->colName};
  int32_t idx = 0;
  SNode* pOtherNode = NULL;
  FOREACH(pOtherNode, (SNodeList*)pCols) {
    if (idx >= exprIdx) {
      break;
    }

    const SNode* pOtherExpr = fqSelectListExprNode(pOtherNode);
    if (NULL == pOtherExpr || !nodesIsExprNode((SNode*)pOtherExpr)) {
      ++idx;
      continue;
    }

    for (int32_t nameIdx = 0; nameIdx < 2; ++nameIdx) {
      if (fqVisibleNameMatchesExpr(pVisibleNames[nameIdx], pOtherExpr)) {
        return true;
      }
    }

    ++idx;
  }

  return false;
}

static bool fqColumnNeedsInternalAliasForVisibleCollision(const SNodeList* pCols, int32_t exprIdx,
                                                          const SColumnNode* pCol,
                                                          const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pCols || NULL == pCol || '\0' == pCol->node.aliasName[0] ||
      '\0' == pCol->colName[0] || 0 == strcmp(pCol->node.aliasName, pCol->colName)) {
    return false;
  }

  return fqColumnHasPreviousVisibleCollision(pCols, exprIdx, pCol, pCtx);
}

static const char* fqPickDuplicateSafeColumnAlias(const SNodeList* pCols, int32_t exprIdx,
                                                  const SColumnNode* pCol,
                                                  const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pCols || NULL == pCol) {
    return NULL;
  }

  if (NULL != pCtx && pCtx->emitInternalAlias) {
    return NULL;
  }

  const char* pVisibleAlias = fqColumnOutputAlias(pCol, pCtx);
  const char* pInternalAlias = ('\0' != pCol->node.aliasName[0]) ? pCol->node.aliasName : NULL;
  if (NULL == pVisibleAlias || '\0' == pVisibleAlias[0] ||
      NULL == pInternalAlias || '\0' == pInternalAlias[0] ||
      0 == strcmp(pVisibleAlias, pInternalAlias)) {
    return NULL;
  }

  int32_t idx = 0;
  SNode* pOtherNode = NULL;
  FOREACH(pOtherNode, (SNodeList*)pCols) {
    if (idx >= exprIdx) {
      break;
    }

    const SNode* pOtherExpr = fqSelectListExprNode(pOtherNode);
    if (NULL == pOtherExpr || !nodesIsExprNode((SNode*)pOtherExpr)) {
      ++idx;
      continue;
    }

    const char* pOtherAlias = fqSelectAlias((const SExprNode*)pOtherExpr, pCtx);
    if (NULL != pOtherAlias && '\0' != pOtherAlias[0] &&
        0 == strcmp(pVisibleAlias, pOtherAlias)) {
      qDebug("FQ-SQL-SELECT: expr[%d] duplicate visible alias=%s for col=%s, fallback internal alias=%s",
             exprIdx, pVisibleAlias, pCol->colName, pInternalAlias);
      return pInternalAlias;
    }

    ++idx;
  }

  return NULL;
}

static EExtTimestampTzClass fqTimestampEpochOutputClass(
    const SNode* pExpr, EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
    const SNodesRemoteSQLCtx* pCtx) {
  if ((dialect != EXT_SQL_DIALECT_MYSQL &&
       dialect != EXT_SQL_DIALECT_POSTGRES) ||
      pCtx == NULL ||
      !pCtx->convertTimestampOutputToEpoch || pCtx->emitInternalAlias ||
      pExpr == NULL || !nodesIsExprNode((SNode*)pExpr) ||
      ((const SExprNode*)pExpr)->resType.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  EExtTimestampTzClass tsClass = fqResolveExprTimestampClass(
      pExpr, dialect, pExtMeta, pCtx, NULL, NULL);
  if (tsClass == EXT_TIMESTAMP_TZ_CLASS_AWARE) {
    return tsClass;
  }
  if (tsClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE &&
      pCtx->timezoneConversionSupported && pCtx->tzName != NULL &&
      pCtx->tzName[0] != '\0') {
    return tsClass;
  }
  return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
}

static int32_t dynRenderTimestampEpochOutput(
    SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect,
    const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx,
    EExtTimestampTzClass tsClass) {
  if (dialect == EXT_SQL_DIALECT_MYSQL) {
    if (tsClass == EXT_TIMESTAMP_TZ_CLASS_AWARE) {
      dynSQLAppendStr(s, "CAST(UNIX_TIMESTAMP(");
      int32_t code = dynAppendExpr(s, pExpr, dialect, pExtMeta, pCtx);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      dynSQLAppendStr(s, ") * 1000000 AS SIGNED)");
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }

    dynSQLAppendStr(s,
                    "CAST(TIMESTAMPDIFF(MICROSECOND, "
                    "'1970-01-01 00:00:00', CONVERT_TZ(");
    int32_t code = dynAppendExpr(s, pExpr, EXT_SQL_DIALECT_MYSQL,
                                 pExtMeta, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dynSQLAppendStr(s, ", ");
    dynAppendEscapedString(s, pCtx->tzName, dialect);
    dynSQLAppendStr(s, ", '+00:00')) AS SIGNED)");
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  dynSQLAppendStr(s, "CAST(EXTRACT(EPOCH FROM ");
  if (tsClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE) {
    dynSQLAppendStr(s, "(CAST(");
  }
  int32_t code = dynAppendExpr(s, pExpr, dialect, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (tsClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE) {
    dynSQLAppendStr(s, " AS TIMESTAMP) AT TIME ZONE ");
    dynAppendEscapedString(s, pCtx->tzName, dialect);
    dynSQLAppendChar(s, ')');
  }
  dynSQLAppendStr(s, ") * 1000000 AS BIGINT)");
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

// Render SELECT column list from a SNodeList (pProjections or pScanCols).
static void renderSelectList(SDynSQL* s, const SNodeList* pCols, EExtSQLDialect dialect,
                              const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  SNodesRemoteSQLCtx selectExprCtxStore = {0};
  const SNodesRemoteSQLCtx* pSelectExprCtx = pCtx;
  if (pCtx != NULL) {
    selectExprCtxStore = *pCtx;
    selectExprCtxStore.renderingCurrentSelectExpr = true;
    pSelectExprCtx = &selectExprCtxStore;
  }

  qDebug("FQ-SQL-SELECT: render select list dialect=%s cols=%d meta=%p",
         dialectName(dialect), pCols ? LIST_LENGTH((SNodeList*)pCols) : 0, pExtMeta);
  fqDebugLogRenderCtx("select-list", pExtMeta, pSelectExprCtx);
  fqDebugLogExprList("render.selectList.input", pCols);
  if (!pCols || LIST_LENGTH(pCols) == 0) {
    if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
      dynSQLAppendChar(s, '*');
      qDebug("FQ-SQL-SELECT: empty select list uses '*' for dialect=%s", dialectName(dialect));
    } else {
      dynSQLAppendChar(s, '1');
      qDebug("FQ-SQL-SELECT: empty select list uses '1' for dialect=%s", dialectName(dialect));
    }
    return;
  }
  bool first = true;
  int32_t exprIdx = 0;
  SNode* pExpr = NULL;
  FOREACH(pExpr, pCols) {
    const SNode* pCol = pExpr;
    if (nodeType(pCol) == QUERY_NODE_TARGET) {
      pCol = ((const STargetNode*)pCol)->pExpr;
    }
    if (NULL == pCol) continue;
    char exprDesc[512] = {0};
    fqDebugDescribeExprNode(pCol, exprDesc, sizeof(exprDesc));
    qDebug("FQ-SQL-SELECT: expr[%d] %s", exprIdx, exprDesc);
    if (!first) dynSQLAppendStr(s, ", ");
    first = false;
    EExtTimestampTzClass epochOutputClass = fqTimestampEpochOutputClass(
        pCol, dialect, pExtMeta, pSelectExprCtx);
    bool renderedTimestampEpoch =
        epochOutputClass != EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
    if (renderedTimestampEpoch) {
      int32_t epochCode = dynRenderTimestampEpochOutput(
          s, pCol, dialect, pExtMeta, pSelectExprCtx, epochOutputClass);
      if (epochCode != TSDB_CODE_SUCCESS && !s->err) {
        s->err = epochCode;
      }
    }
    bool renderedCurrentScopeColumn = false;
    bool renderedCurrentScopePseudo = false;
    const char* pCurrentScopeRef = NULL;
    const char* pCurrentScopeAlias = NULL;
    if (!renderedTimestampEpoch && nodeType(pCol) == QUERY_NODE_COLUMN &&
        NULL == pExtMeta) {
      const SColumnNode* pColNode = (const SColumnNode*)pCol;
      bool allowCurrentScopePseudo = (pSelectExprCtx == NULL || !pSelectExprCtx->inputSelectExprsFromChildSource ||
                                      fqColumnMatchesPseudoName(pColNode, "_wend") ||
                                      fqColumnMatchesPseudoName(pColNode, "_wduration"));
      if ('\0' == pColNode->tableAlias[0] && '\0' == pColNode->tableName[0] && allowCurrentScopePseudo) {
        qDebug("FQ-SQL-SELECT: expr[%d] current-scope fast-path allow=%d fromChild=%d col=%s alias=%s userAlias=%s",
               exprIdx, (int)allowCurrentScopePseudo,
               (pSelectExprCtx != NULL) ? (int)pSelectExprCtx->inputSelectExprsFromChildSource : 0,
               pColNode->colName, pColNode->node.aliasName, pColNode->node.userAlias);
        int32_t pseudoCode = fqTryRenderCurrentScopePseudoColumn(s, pColNode, dialect, pSelectExprCtx,
                                                                 &renderedCurrentScopePseudo,
                                                                 &pCurrentScopeRef);
        if (pseudoCode != TSDB_CODE_SUCCESS && !s->err) {
          s->err = pseudoCode;
        }
        renderedCurrentScopeColumn = renderedCurrentScopePseudo;
        if (!renderedCurrentScopeColumn) {
          pCurrentScopeRef = pColNode->colName[0] != '\0' ? pColNode->colName
                                                          : fqColumnOutputAlias(pColNode, pSelectExprCtx);
        }
        pCurrentScopeAlias = fqColumnOutputAlias(pColNode, pSelectExprCtx);
        if (!renderedCurrentScopeColumn && pCurrentScopeRef != NULL && pCurrentScopeRef[0] != '\0') {
          qDebug("FQ-SQL-SELECT: expr[%d] current-scope column uses raw ref=%s alias=%s dialect=%s",
                 exprIdx, pCurrentScopeRef,
                 (pCurrentScopeAlias != NULL && pCurrentScopeAlias[0] != '\0') ? pCurrentScopeAlias : "(null)",
                 dialectName(dialect));
          dynAppendQuotedId(s, pCurrentScopeRef, dialect);
          renderedCurrentScopeColumn = true;
        } else if (!renderedCurrentScopeColumn) {
          qError("FQ-SQL-SELECT: expr[%d] current-scope column missing ref alias=%s userAlias=%s dialect=%s",
                 exprIdx, pColNode->node.aliasName, pColNode->node.userAlias, dialectName(dialect));
          if (!s->err) {
            s->err = TSDB_CODE_PLAN_INTERNAL_ERROR;
          }
        }
      }
    }
    // All expressions (columns and non-columns) go through dynAppendExpr for
    // uniform rendering.  dynAppendExpr handles the userAlias→colName resolution
    // for subquery-referencing columns (pExtMeta==NULL) so that every context
    // (SELECT, WHERE, HAVING, ORDER BY, GROUP BY, …) uses the same logic.
    if (!renderedTimestampEpoch && !renderedCurrentScopeColumn) {
      int32_t exprCode = dynAppendExpr(s, pCol, dialect, pExtMeta, pSelectExprCtx);
      if (exprCode != TSDB_CODE_SUCCESS && !s->err) s->err = exprCode;
    }
    // AS alias: for real external-table columns that have a different remote name,
    // add "AS tdName" so parent nodes can reference them by the TDengine name.
    // For other expressions, use aliasName.
    if (nodeType(pCol) == QUERY_NODE_COLUMN) {
      const SColumnNode* pColNode = (const SColumnNode*)pCol;
      if (renderedCurrentScopeColumn) {
        bool needAlias = false;
        if (pCurrentScopeAlias != NULL && pCurrentScopeAlias[0] != '\0') {
          needAlias = renderedCurrentScopePseudo ||
                      pCurrentScopeRef == NULL ||
                      0 != strcmp(pCurrentScopeRef, pCurrentScopeAlias);
        }
        if (needAlias) {
          dynSQLAppendStr(s, " AS ");
          dynAppendQuotedId(s, pCurrentScopeAlias, dialect);
          qDebug("FQ-SQL-SELECT: expr[%d] emit current-scope alias ref=%s alias=%s",
                 exprIdx, pCurrentScopeRef, pCurrentScopeAlias);
        }
      } else if (pExtMeta) {
        if (renderedTimestampEpoch) {
          dynSQLAppendStr(s, " AS ");
          dynAppendQuotedId(s, pColNode->colName, dialect);
          qDebug("FQ-SQL-SELECT: expr[%d] emit timestamp alias col=%s",
                 exprIdx, pColNode->colName);
        }
      } else {
        const char* alias = fqColumnOutputAlias(pColNode, pSelectExprCtx);
        bool hasVisibleCollision =
            fqColumnHasPreviousVisibleCollision(pCols, exprIdx, pColNode, pSelectExprCtx);
        bool forceCurrentSelectRename =
            fqShouldUseCurrentSelectRename(&pColNode->node, pSelectExprCtx);
        if (fqColumnNeedsInternalAliasForVisibleCollision(pCols, exprIdx, pColNode,
                                                          pSelectExprCtx)) {
          alias = pColNode->node.aliasName;
        } else {
          const char* pDuplicateSafeAlias =
              fqPickDuplicateSafeColumnAlias(pCols, exprIdx, pColNode, pSelectExprCtx);
          if (NULL != pDuplicateSafeAlias) {
            alias = pDuplicateSafeAlias;
          }
        }
        if (alias != NULL && alias[0] != '\0' &&
            (renderedTimestampEpoch ||
             (pColNode->colName[0] != '\0' &&
              0 != strcmp(alias, pColNode->colName)) ||
             forceCurrentSelectRename)) {
          dynSQLAppendStr(s, " AS ");
          dynAppendQuotedId(s, alias, dialect);
          qDebug("FQ-SQL-SELECT: expr[%d] emit column alias col=%s alias=%s forceRename=%d emitInternalAlias=%d preserveOutputRefName=%d",
                 exprIdx, pColNode->colName, alias, (int)forceCurrentSelectRename,
                 pSelectExprCtx ? (int)pSelectExprCtx->emitInternalAlias : 0,
                 pSelectExprCtx ? (int)pSelectExprCtx->preserveOutputRefName : 0);
        } else if (hasVisibleCollision) {
          char syntheticAlias[64] = {0};
          (void)snprintf(syntheticAlias, sizeof(syntheticAlias), "expr_dup_%d", exprIdx + 1);
          dynSQLAppendStr(s, " AS ");
          dynAppendQuotedId(s, syntheticAlias, dialect);
          qDebug("FQ-SQL-SELECT: expr[%d] emit synthetic alias col=%s alias=%s due to visible collision",
                 exprIdx, pColNode->colName, syntheticAlias);
        }
      }
    } else {
      const char* alias = fqSelectAlias((const SExprNode*)pCol, pSelectExprCtx);
      if (alias && alias[0] != '\0') {
        dynSQLAppendStr(s, " AS ");
        dynAppendQuotedId(s, alias, dialect);
        qDebug("FQ-SQL-SELECT: expr[%d] emit alias=%s", exprIdx, alias);
      }
    }
    ++exprIdx;
  }
  if (first) {
    if (dialect == EXT_SQL_DIALECT_INFLUXQL) {
      dynSQLAppendChar(s, '*');
      qDebug("FQ-SQL-SELECT: all select exprs skipped, fallback '*' dialect=%s", dialectName(dialect));
    } else {
      dynSQLAppendChar(s, '1');
      qDebug("FQ-SQL-SELECT: all select exprs skipped, fallback '1' dialect=%s", dialectName(dialect));
    }
  }
}

// Render WHERE clause from pConditions.
static bool renderWhere(SDynSQL* s, const SNode* pConditions, EExtSQLDialect dialect,
                        const SExtTableMeta* pExtMeta, const SNodesRemoteSQLCtx* pCtx) {
  if (!pConditions) {
    qDebug("FQ-SQL-WHERE: skip empty WHERE dialect=%s meta=%p ctx=%p", dialectName(dialect), pExtMeta, pCtx);
    return false;
  }
  fqDebugLogRenderCtx("where", pExtMeta, pCtx);
  char condDesc[512] = {0};
  fqDebugDescribeExprNode(pConditions, condDesc, sizeof(condDesc));
  qDebug("FQ-SQL-WHERE: begin dialect=%s cond=%s", dialectName(dialect), condDesc);
  SDynSQL cond;
  dynSQLInit(&cond);
  cond.tzName = s->tzName;
  int32_t code = dynAppendExpr(&cond, pConditions, dialect, pExtMeta, pCtx);
  if (code != TSDB_CODE_SUCCESS || cond.err) {
    if (!s->err) s->err = cond.err ? cond.err : code;
    dynSQLFree(&cond);
    return false;
  }
  if (cond.pos > 0) {
    qDebug("FQ-SQL-WHERE: emit WHERE len=%d dialect=%s meta=%p ctx=%p",
           cond.pos, dialectName(dialect), pExtMeta, pCtx);
    dynSQLAppendStr(s, " WHERE ");
    dynSQLAppendLen(s, cond.buf, cond.pos);
    dynSQLFree(&cond);
    return true;
  } else {
    qDebug("FQ-SQL-WHERE: rendered empty WHERE text dialect=%s meta=%p ctx=%p",
           dialectName(dialect), pExtMeta, pCtx);
  }
  dynSQLFree(&cond);
  return false;
}

static const SColumnNode* findRuntimePrimaryTsCol(const SScanLogicNode* pScan) {
  if (NULL == pScan) {
    return NULL;
  }
  SNode* pCol = NULL;
  FOREACH(pCol, pScan->pScanCols) {
    if (QUERY_NODE_COLUMN == nodeType(pCol) &&
        (((SColumnNode*)pCol)->isPrimTs || ((SColumnNode*)pCol)->colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
      return (const SColumnNode*)pCol;
    }
  }
  return NULL;
}

static int32_t renderRuntimeTimeRange(SDynSQL* s, const SScanLogicNode* pScan, bool wroteWhere,
                                      EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                                      const SNodesRemoteSQLCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL == pCtx || NULL == pCtx->pRuntimeTimeRange) {
    return TSDB_CODE_SUCCESS;
  }
  const SColumnNode* pTsCol = findRuntimePrimaryTsCol(pScan);
  QUERY_CHECK_NULL(pTsCol, code, lino, _return, TSDB_CODE_EXT_SQL_RENDER_ERROR);

  const STimeWindow* pRange = pCtx->pRuntimeTimeRange;
  bool hasSkey = pRange->skey != INT64_MIN;
  bool hasEkey = pRange->ekey != INT64_MAX;
  if (!hasSkey && !hasEkey) {
    return TSDB_CODE_SUCCESS;
  }
  if (hasEkey && INT64_MIN == pRange->ekey) {
    dynSQLAppendStr(s, wroteWhere ? " AND 1 = 0" : " WHERE 1 = 0");
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  dynSQLAppendStr(s, wroteWhere ? " AND (" : " WHERE (");
  bool wroteBound = false;
  if (hasSkey) {
    code = dynAppendExpr(s, (const SNode*)pTsCol, dialect, pExtMeta, pCtx);
    QUERY_CHECK_CODE(code, lino, _return);
    dynSQLAppendStr(s, " >= ");
    SValueNode value = {0};
    setNodeType(&value, QUERY_NODE_VALUE);
    value.node.resType.type = TSDB_DATA_TYPE_BIGINT;
    value.datum.i = pRange->skey;
    code = dynAppendExprTS(s, (const SNode*)&value, (const SNode*)pTsCol, dialect, pExtMeta, pCtx);
    QUERY_CHECK_CODE(code, lino, _return);
    wroteBound = true;
  }
  if (hasEkey) {
    if (wroteBound) {
      dynSQLAppendStr(s, " AND ");
    }
    code = dynAppendExpr(s, (const SNode*)pTsCol, dialect, pExtMeta, pCtx);
    QUERY_CHECK_CODE(code, lino, _return);
    // pRuntimeTimeRange is half-open in the virtual table's precision.  Use
    // the last included tick with an inclusive comparison so a source with a
    // coarser timestamp precision does not truncate ekey back onto the final
    // row and exclude it (for example, ns virtual time over MySQL DATETIME(6)).
    dynSQLAppendStr(s, " <= ");
    SValueNode value = {0};
    setNodeType(&value, QUERY_NODE_VALUE);
    value.node.resType.type = TSDB_DATA_TYPE_BIGINT;
    value.datum.i = pRange->ekey - 1;
    code = dynAppendExprTS(s, (const SNode*)&value, (const SNode*)pTsCol, dialect, pExtMeta, pCtx);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  dynSQLAppendChar(s, ')');
  code = s->err ? s->err : TSDB_CODE_SUCCESS;
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static const SExprNode* fqFindSelectOutputByRefName(const char* pRefName,
                                                    const SNodeList* pSelectExprs) {
  if (NULL == pRefName || '\0' == pRefName[0] || NULL == pSelectExprs) {
    return NULL;
  }

  SNode* pExpr = NULL;
  FOREACH(pExpr, (SNodeList*)pSelectExprs) {
    if (NULL == pExpr) {
      continue;
    }

    if (QUERY_NODE_TARGET == nodeType(pExpr)) {
      pExpr = ((STargetNode*)pExpr)->pExpr;
    }
    if (NULL == pExpr || !nodesIsExprNode(pExpr)) {
      continue;
    }

    const SExprNode* pOutExpr = (const SExprNode*)pExpr;
    if ('\0' != pOutExpr->aliasName[0] && 0 == strcmp(pRefName, pOutExpr->aliasName)) {
      return pOutExpr;
    }
  }

  FOREACH(pExpr, (SNodeList*)pSelectExprs) {
    if (NULL == pExpr) {
      continue;
    }

    if (QUERY_NODE_TARGET == nodeType(pExpr)) {
      pExpr = ((STargetNode*)pExpr)->pExpr;
    }
    if (NULL == pExpr || !nodesIsExprNode(pExpr)) {
      continue;
    }

    if (fqVisibleNameMatchesExpr(pRefName, pExpr)) {
      return (const SExprNode*)pExpr;
    }
  }

  return NULL;
}

static const SExprNode* fqFindSelectOutputByColumnRef(const SColumnNode* pCol,
                                                      const SNodeList* pSelectExprs) {
  if (NULL == pCol || NULL == pSelectExprs) {
    return NULL;
  }

  const char* pRefNames[3] = {pCol->node.aliasName, pCol->node.userAlias, pCol->colName};
  for (int32_t idx = 0; idx < 3; ++idx) {
    const char* pRefName = pRefNames[idx];
    if ('\0' == pRefName[0]) {
      continue;
    }

    bool duplicated = false;
    for (int32_t prev = 0; prev < idx; ++prev) {
      if ('\0' != pRefNames[prev][0] && 0 == strcmp(pRefNames[prev], pRefName)) {
        duplicated = true;
        break;
      }
    }
    if (duplicated) {
      continue;
    }

    const SExprNode* pMatched = fqFindSelectOutputByRefName(pRefName, pSelectExprs);
    if (NULL != pMatched) {
      return pMatched;
    }
  }

  return NULL;
}

static const SExprNode* fqFindAlignedSelectOutputByColumnRef(const SColumnNode* pCol,
                                                             const SNodeList* pSelectExprs,
                                                             const SNodeList* pRefExprs) {
  if (NULL == pCol || NULL == pSelectExprs) {
    return NULL;
  }

  int32_t targetIdx = -1;
  if (TSDB_CODE_SUCCESS ==
      fqFindUniqueProjectChildOutputIndex(pCol, pSelectExprs, pRefExprs, &targetIdx)) {
    const SNode* pMatched = nodesListGetNode((SNodeList*)pSelectExprs, targetIdx);
    pMatched = fqUnwrapSelectExpr(pMatched);
    if (NULL != pMatched && nodesIsExprNode((SNode*)pMatched)) {
      return (const SExprNode*)pMatched;
    }
  }

  if (!fqProjectColumnIsWindowStartPseudo(pCol)) {
    return fqFindSelectOutputByColumnRef(pCol, pSelectExprs);
  }

  return NULL;
}

static const char* fqResolveOutputRefAlias(const SColumnNode* pCol,
                                           const SNodesRemoteSQLCtx* pCtx,
                                           const SExprNode** ppMatchedExpr) {
  if (ppMatchedExpr != NULL) {
    *ppMatchedExpr = NULL;
  }
  if (NULL == pCol || NULL == pCtx || NULL == pCtx->pOutputSelectExprs) {
    return NULL;
  }
  if (pCtx->renderingCurrentSelectExpr && !pCtx->inputSelectExprsFromChildSource) {
    return NULL;
  }
  if (pCtx->pInputSelectExprs == NULL && !pCtx->inputSelectExprsFromChildSource) {
    return NULL;
  }
  if ((pCtx->useJoinLegAlias || pCtx->includeTableName) &&
      ('\0' != pCol->tableAlias[0] || '\0' != pCol->tableName[0])) {
    return NULL;
  }

  if (pCtx->inputSelectExprsFromChildSource && pCtx->pInputSelectExprs != NULL) {
    const SExprNode* pInExpr =
        fqFindAlignedSelectOutputByColumnRef(pCol, pCtx->pInputSelectExprs, pCtx->pInputRefExprs);
    if (NULL != pInExpr) {
      const char* pAlias = fqFromChildSelectAlias(pInExpr);
      if (NULL != pAlias && '\0' != pAlias[0]) {
        if (ppMatchedExpr != NULL) {
          *ppMatchedExpr = pInExpr;
        }
        qDebug("FQ-SQL-REF: prefer from-child emitted alias ref=%s alias=%s userAlias=%s childAlias=%s childExprType=%s",
               pCol->colName, pCol->node.aliasName, pCol->node.userAlias,
               pAlias, nodesNodeName(nodeType((SNode*)pInExpr)));
        return pAlias;
      }
    }
  }

  const SExprNode* pOutExpr = fqFindSelectOutputByColumnRef(pCol, pCtx->pOutputSelectExprs);
  if (NULL != pOutExpr) {
    const char* pAlias = fqSelectAlias(pOutExpr, pCtx);
    if (NULL != pAlias && '\0' != pAlias[0]) {
      if (ppMatchedExpr != NULL) {
        *ppMatchedExpr = pOutExpr;
      }
      return pAlias;
    }
  }

  return NULL;
}

static const SNode* fqUnwrapSelectExpr(const SNode* pNode);
static int32_t fqFindProjectChildOutputIndex(const SColumnNode* pProjCol,
                                             const SNodeList* pChildOutputs,
                                             const SNodeList* pChildRefExprs,
                                             int32_t* pTargetIdx);

static bool fqCanPassThroughChildOutputs(const SNodeList* pTargets,
                                         const SNodeList* pChildOutputs,
                                         const SNodeList* pChildRefExprs,
                                         const char* pTag) {
  if (pTargets == NULL || pChildOutputs == NULL) {
    qDebug("FQ-SQL: child-output passthrough rejected tag=%s targets=%p childOutputs=%p",
           pTag ? pTag : "(null)", pTargets, pChildOutputs);
    return false;
  }

  const int32_t targetCnt = LIST_LENGTH((SNodeList*)pTargets);
  const int32_t childCnt = LIST_LENGTH((SNodeList*)pChildOutputs);
  if (targetCnt <= 0 || childCnt <= 0 || targetCnt != childCnt) {
    qDebug("FQ-SQL: child-output passthrough rejected tag=%s targetCnt=%d childCnt=%d",
           pTag ? pTag : "(null)", targetCnt, childCnt);
    return false;
  }

  int32_t idx = 0;
  SNode* pTarget = NULL;
  FOREACH(pTarget, (SNodeList*)pTargets) {
    const SNode* pExpr = fqUnwrapSelectExpr(pTarget);
    if (pExpr == NULL || nodeType((SNode*)pExpr) != QUERY_NODE_COLUMN) {
      qDebug("FQ-SQL: child-output passthrough rejected tag=%s idx=%d exprType=%d",
             pTag ? pTag : "(null)", idx, pExpr ? (int)nodeType((SNode*)pExpr) : -1);
      return false;
    }

    int32_t childIdx = -1;
    if (TSDB_CODE_SUCCESS !=
        fqFindProjectChildOutputIndex((const SColumnNode*)pExpr, pChildOutputs,
                                      pChildRefExprs, &childIdx)) {
      qDebug("FQ-SQL: child-output passthrough rejected tag=%s idx=%d col=%s alias=%s userAlias=%s childCnt=%d",
             pTag ? pTag : "(null)", idx,
             ((const SColumnNode*)pExpr)->colName,
             ((const SColumnNode*)pExpr)->node.aliasName,
             ((const SColumnNode*)pExpr)->node.userAlias,
             childCnt);
      return false;
    }

    if (childIdx != idx) {
      qDebug("FQ-SQL: child-output passthrough rejected tag=%s idx=%d childIdx=%d",
             pTag ? pTag : "(null)", idx, childIdx);
      return false;
    }
    ++idx;
  }

  qDebug("FQ-SQL: child-output passthrough allowed tag=%s outputs=%d",
         pTag ? pTag : "(null)", targetCnt);
  return true;
}

static const char* fqFindEquivalentOutputAlias(const SExprNode* pInputExpr,
                                               const SNodeList* pOutputSelectExprs,
                                               const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pInputExpr || NULL == pOutputSelectExprs || NULL == pCtx) {
    return NULL;
  }

  SNode* pOutputNode = NULL;
  FOREACH(pOutputNode, (SNodeList*)pOutputSelectExprs) {
    const SNode* pOutputExpr = fqUnwrapSelectExpr(pOutputNode);
    if (NULL == pOutputExpr || !nodesIsExprNode((SNode*)pOutputExpr)) {
      continue;
    }

    if (!nodesEqualNode((SNode*)pInputExpr, (SNode*)pOutputExpr)) {
      continue;
    }

    const char* pAlias = fqSelectAlias((const SExprNode*)pOutputExpr, pCtx);
    if (NULL != pAlias && '\0' != pAlias[0]) {
      return pAlias;
    }
  }

  return NULL;
}

static const char* fqFindEquivalentInputAlias(const SExprNode* pExpr,
                                              const SNodeList* pInputSelectExprs) {
  if (NULL == pExpr || NULL == pInputSelectExprs) {
    return NULL;
  }

  SNode* pInputNode = NULL;
  FOREACH(pInputNode, (SNodeList*)pInputSelectExprs) {
    const SNode* pInputExpr = fqUnwrapSelectExpr(pInputNode);
    if (NULL == pInputExpr || !nodesIsExprNode((SNode*)pInputExpr)) {
      continue;
    }

    if (!nodesEqualNode((SNode*)pExpr, (SNode*)pInputExpr)) {
      continue;
    }

    return fqFromChildSelectAlias((const SExprNode*)pInputExpr);
  }

  return NULL;
}

static int32_t dynAppendOrderExpr(SDynSQL* s, const SNode* pExpr,
                                  const SNodeList* pSelectExprs,
                                  EExtSQLDialect dialect,
                                  const SExtTableMeta* pExtMeta,
                                  const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pExpr) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pOrderCol = (const SColumnNode*)pExpr;
    if ('\0' == pOrderCol->tableAlias[0] && '\0' == pOrderCol->tableName[0]) {
      const SExprNode* pOutExpr = NULL;
      const char* pAlias = fqResolveOutputRefAlias(pOrderCol, pCtx, &pOutExpr);
      if (NULL == pAlias && NULL != pSelectExprs) {
        pOutExpr = fqFindSelectOutputByColumnRef(pOrderCol, pSelectExprs);
        if (NULL != pOutExpr) {
          pAlias = fqSelectAlias(pOutExpr, pCtx);
        }
      }
      if (NULL != pAlias && '\0' != pAlias[0]) {
        qDebug("FQ-SQL-ORDER: rewrite output ref col=%s alias=%s userAlias=%s -> selectAlias=%s exprType=%s",
               pOrderCol->colName, pOrderCol->node.aliasName, pOrderCol->node.userAlias,
               pAlias, pOutExpr ? nodesNodeName(nodeType((SNode*)pOutExpr)) : "(unknown)");
        dynAppendQuotedId(s, pAlias, dialect);
        return s->err ? s->err : TSDB_CODE_SUCCESS;
      }

      const SExprNode* pInputExpr = fqFindInputSelectExprByColumnRef(pCtx, pOrderCol);
      if (NULL != pInputExpr) {
        const char* pOutputAlias =
            fqFindEquivalentOutputAlias(pInputExpr,
                                        (pCtx != NULL) ? pCtx->pOutputSelectExprs : NULL,
                                        pCtx);
        if (NULL != pOutputAlias && '\0' != pOutputAlias[0]) {
          qDebug("FQ-SQL-ORDER: rewrite merged-child ref col=%s alias=%s userAlias=%s -> outputAlias=%s exprType=%s",
                 pOrderCol->colName, pOrderCol->node.aliasName, pOrderCol->node.userAlias,
                 pOutputAlias, nodesNodeName(nodeType((SNode*)pInputExpr)));
          dynAppendQuotedId(s, pOutputAlias, dialect);
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }

        if (pCtx != NULL && !pCtx->inputSelectExprsFromChildSource &&
            dialect != EXT_SQL_DIALECT_MYSQL) {
          char inputDesc[512] = {0};
          fqDebugDescribeExprNode((const SNode*)pInputExpr, inputDesc, sizeof(inputDesc));
          qDebug("FQ-SQL-ORDER: rewrite hidden merged-child ref col=%s alias=%s userAlias=%s -> expr=%s",
                 pOrderCol->colName, pOrderCol->node.aliasName, pOrderCol->node.userAlias,
                 inputDesc);
          return dynAppendExpr(s, (const SNode*)pInputExpr, dialect, pExtMeta, pCtx);
        }

        const char* pInputAlias = fqSelectAlias(pInputExpr, pCtx);
        if (NULL != pInputAlias && '\0' != pInputAlias[0]) {
          qDebug("FQ-SQL-ORDER: rewrite child output ref col=%s alias=%s userAlias=%s -> childAlias=%s exprType=%s",
                 pOrderCol->colName, pOrderCol->node.aliasName, pOrderCol->node.userAlias,
                 pInputAlias, nodesNodeName(nodeType((SNode*)pInputExpr)));
          dynAppendQuotedId(s, pInputAlias, dialect);
          return s->err ? s->err : TSDB_CODE_SUCCESS;
        }
      }
    }
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pOrderCol = (const SColumnNode*)pExpr;
    if ('\0' == pOrderCol->tableAlias[0] && '\0' == pOrderCol->tableName[0]) {
      const SExprNode* pScopeExpr = fqResolveCurrentScopeOutputExpr(pOrderCol, pCtx);
      if (NULL != pScopeExpr && (const SNode*)pScopeExpr != pExpr) {
        char scopeDesc[512] = {0};
        fqDebugDescribeExprNode((const SNode*)pScopeExpr, scopeDesc, sizeof(scopeDesc));
        qDebug("FQ-SQL-ORDER: rewrite current-scope ref col=%s alias=%s userAlias=%s -> expr=%s directTable=%d",
               pOrderCol->colName, pOrderCol->node.aliasName, pOrderCol->node.userAlias,
               scopeDesc, NULL != pExtMeta);
        return dynAppendExpr(s, (const SNode*)pScopeExpr, dialect, pExtMeta, pCtx);
      }
    }
  }

  return dynAppendExpr(s, pExpr, dialect, pExtMeta, pCtx);
}

// Render ORDER BY clause from pSortKeys.
static void renderOrderBy(SDynSQL* s, const SNodeList* pSortKeys, const SNodeList* pSelectExprs,
                          EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                          const SNodesRemoteSQLCtx* pCtx) {
  if (!pSortKeys || LIST_LENGTH(pSortKeys) == 0) {
    qDebug("FQ-SQL-ORDER: skip empty ORDER BY dialect=%s", dialectName(dialect));
    return;
  }
  qDebug("FQ-SQL-ORDER: render ORDER BY dialect=%s keys=%d meta=%p",
         dialectName(dialect), LIST_LENGTH((SNodeList*)pSortKeys), pExtMeta);
  fqDebugLogRenderCtx("order-by", pExtMeta, pCtx);
  bool firstKey = true;
  int32_t keyIdx = 0;
  SNode* pKey = NULL;
  FOREACH(pKey, pSortKeys) {
    const SOrderByExprNode* pOrd = (const SOrderByExprNode*)pKey;
    char keyDesc[512] = {0};
    fqDebugDescribeExprNode(pOrd->pExpr, keyDesc, sizeof(keyDesc));
    qDebug("FQ-SQL-ORDER: key[%d] order=%d nullOrder=%d expr=%s",
           keyIdx, (int)pOrd->order, (int)pOrd->nullOrder, keyDesc);
    if (dialect == EXT_SQL_DIALECT_MYSQL) {
      bool needNullKey = false;
      bool nullKeyDesc = false;
      // MySQL has no NULLS FIRST/LAST syntax; emulate via ISNULL(col) ordering.
      // ISNULL(col) returns 1 for NULL, 0 for non-NULL.
      // ISNULL(col) DESC puts NULLs first; ISNULL(col) ASC puts NULLs last.
      if (pOrd->order == ORDER_ASC && pOrd->nullOrder == NULL_ORDER_FIRST) {
        // Want NULLs first in ASC: ISNULL(col) DESC, col ASC
        needNullKey = true; nullKeyDesc = true;
      } else if (pOrd->order == ORDER_ASC && pOrd->nullOrder == NULL_ORDER_LAST) {
        // Want NULLs last in ASC: ISNULL(col) ASC, col ASC
        needNullKey = true; nullKeyDesc = false;
      } else if (pOrd->order == ORDER_DESC && pOrd->nullOrder == NULL_ORDER_FIRST) {
        // Want NULLs first in DESC: ISNULL(col) DESC, col DESC
        needNullKey = true; nullKeyDesc = true;
      } else if (pOrd->order == ORDER_DESC && pOrd->nullOrder == NULL_ORDER_LAST) {
        // Want NULLs last in DESC: ISNULL(col) ASC, col DESC
        needNullKey = true; nullKeyDesc = false;
      }
      qDebug("FQ-SQL-ORDER: key[%d] mysql null emulation need=%d desc=%d",
             keyIdx, (int)needNullKey, (int)nullKeyDesc);
      if (needNullKey) {
        dynSQLAppendStr(s, firstKey ? " ORDER BY ISNULL(" : ", ISNULL(");
        (void)dynAppendOrderExpr(s, pOrd->pExpr, pSelectExprs, dialect, pExtMeta, pCtx);
        dynSQLAppendStr(s, nullKeyDesc ? ") DESC" : ") ASC");
        firstKey = false;
      }
    }
    dynSQLAppendStr(s, firstKey ? " ORDER BY " : ", ");
    firstKey = false;
    (void)dynAppendOrderExpr(s, pOrd->pExpr, pSelectExprs, dialect, pExtMeta, pCtx);
    dynSQLAppendStr(s, (pOrd->order == ORDER_DESC) ? " DESC" : " ASC");
    // DS §5.3.8.3: PG and InfluxDB v3 (DataFusion) both directly support NULLS FIRST/LAST.
    // MySQL does not support this syntax; handled above via ISNULL() conversion.
    qDebug("FQ-DBG renderOrderBy: dialect=%d nullOrder=%d NULL_ORDER_FIRST=%d NULL_ORDER_LAST=%d",
           (int)dialect, (int)pOrd->nullOrder, (int)NULL_ORDER_FIRST, (int)NULL_ORDER_LAST);
    if (dialect != EXT_SQL_DIALECT_MYSQL) {
      if (pOrd->nullOrder == NULL_ORDER_FIRST)
        dynSQLAppendStr(s, " NULLS FIRST");
      else if (pOrd->nullOrder == NULL_ORDER_LAST)
        dynSQLAppendStr(s, " NULLS LAST");
    }
    ++keyIdx;
  }
}

// Render LIMIT / OFFSET clause.
static void renderLimit(SDynSQL* s, const SLimitNode* pLimit) {
  if (!pLimit || !pLimit->limit) {
    qDebug("FQ-SQL-LIMIT: skip empty LIMIT node=%p", pLimit);
    return;
  }
  qDebug("FQ-SQL-LIMIT: render limit=%" PRId64 " offset=%" PRId64 " node=%p",
         pLimit->limit->datum.i,
         (pLimit->offset != NULL) ? pLimit->offset->datum.i : 0,
         pLimit);
  dynSQLAppendf(s, " LIMIT %" PRId64, pLimit->limit->datum.i);
  if (pLimit->offset && pLimit->offset->datum.i > 0)
    dynSQLAppendf(s, " OFFSET %" PRId64, pLimit->offset->datum.i);
}

// Get first child of a physi node.
static const SLogicNode* getFirstChild(const SLogicNode* pNode) {
  if (!pNode || !pNode->pChildren || LIST_LENGTH(pNode->pChildren) == 0) return NULL;
  return (const SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
}

// Walk down to find the FederatedScan leaf (used by Sort for ORDER BY meta).
static const SScanLogicNode* findLeafScan(const SLogicNode* pNode) {
  while (pNode) {
    if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_SCAN)
      return (const SScanLogicNode*)pNode;
    pNode = getFirstChild(pNode);
  }
  return NULL;
}

static const char* fqSelectAlias(const SExprNode* pExpr, const SNodesRemoteSQLCtx* pCtx);
static int32_t fqFindUniqueAggOutputTargetIndex(const SNode* pOutputExpr,
                                                const SNodeList* pTargets,
                                                int32_t* pTargetIdx);

static const char* fqTargetOutputAlias(const SNode* pTarget,
                                       const SNodesRemoteSQLCtx* pCtx) {
  if (NULL != pTarget && QUERY_NODE_TARGET == nodeType((SNode*)pTarget)) {
    pTarget = ((const STargetNode*)pTarget)->pExpr;
  }
  if (NULL != pTarget && QUERY_NODE_GROUPING_SET == nodeType((SNode*)pTarget)) {
    pTarget = nodesListGetNode(((const SGroupingSetNode*)pTarget)->pParameterList, 0);
  }
  if (NULL == pTarget || !nodesIsExprNode((SNode*)pTarget)) {
    return NULL;
  }

  const SExprNode* pTargetExpr = (const SExprNode*)pTarget;
  if ('\0' != pTargetExpr->aliasName[0]) {
    return pTargetExpr->aliasName;
  }
  if (QUERY_NODE_COLUMN == nodeType((SNode*)pTarget)) {
    const SColumnNode* pTargetCol = (const SColumnNode*)pTarget;
    if ('\0' != pTargetCol->colName[0]) {
      return pTargetCol->colName;
    }
  }

  return fqSelectAlias(pTargetExpr, pCtx);
}

static const char* fqGroupKeyOutputAlias(const SNode* pGroupKey,
                                         const SNodeList* pTargets,
                                         const SNodesRemoteSQLCtx* pCtx,
                                         int32_t outputIdx) {
  if (NULL != pCtx && NULL != pCtx->pOutputSelectExprs && outputIdx >= 0 &&
      outputIdx < LIST_LENGTH(pCtx->pOutputSelectExprs)) {
    const SNode* pOutput = nodesListGetNode((SNodeList*)pCtx->pOutputSelectExprs, outputIdx);
    if (NULL != pOutput && QUERY_NODE_TARGET == nodeType((SNode*)pOutput)) {
      pOutput = ((const STargetNode*)pOutput)->pExpr;
    }
    if (NULL != pOutput && QUERY_NODE_GROUPING_SET == nodeType((SNode*)pOutput)) {
      pOutput = nodesListGetNode(((const SGroupingSetNode*)pOutput)->pParameterList, 0);
    }
    if (NULL != pOutput && nodesIsExprNode((SNode*)pOutput) &&
        nodesEqualNode(pOutput, pGroupKey)) {
      const char* pAlias = fqSelectAlias((const SExprNode*)pOutput, pCtx);
      if (NULL != pAlias && '\0' != pAlias[0]) {
        return pAlias;
      }
    }
  }
  int32_t targetIdx = -1;
  if (NULL != pTargets &&
      TSDB_CODE_SUCCESS ==
          fqFindUniqueAggOutputTargetIndex(pGroupKey, pTargets, &targetIdx)) {
    const SNode* pTarget = nodesListGetNode((SNodeList*)pTargets, targetIdx);
    const char* pAlias = fqTargetOutputAlias(pTarget, pCtx);
    if (NULL != pAlias && '\0' != pAlias[0]) {
      return pAlias;
    }
  }

  return (NULL != pGroupKey && nodesIsExprNode((SNode*)pGroupKey))
             ? fqSelectAlias((const SExprNode*)pGroupKey, pCtx)
             : NULL;
}

static int32_t renderCollectedKeyExprs(SDynSQL* s, const SNodeList* pExprs, int32_t count,
                                       EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                                       const SNodesRemoteSQLCtx* pCtx, bool withAlias) {
  SNodesRemoteSQLCtx selectExprCtxStore = {0};
  const SNodesRemoteSQLCtx* pExprCtx = pCtx;
  if (withAlias && pCtx != NULL) {
    selectExprCtxStore = *pCtx;
    selectExprCtxStore.renderingCurrentSelectExpr = true;
    pExprCtx = &selectExprCtxStore;
  }

  if (pExprs == NULL || count <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  bool first = true;
  for (int32_t i = 0; i < count; ++i) {
    const SNode* pExprNode = nodesListGetNode((SNodeList*)pExprs, i);
    const SNode* pExpr = pExprNode;
    if (pExpr == NULL) {
      qError("FQ-SQL: renderCollectedKeyExprs missing expr at idx=%d count=%d", i, count);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }

    if (nodeType((SNode*)pExpr) == QUERY_NODE_TARGET) {
      pExpr = ((const STargetNode*)pExpr)->pExpr;
    }
    if (pExpr != NULL && nodeType((SNode*)pExpr) == QUERY_NODE_GROUPING_SET) {
      pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
    }
    if (pExpr == NULL) {
      qError("FQ-SQL: renderCollectedKeyExprs resolved expr NULL at idx=%d rawType=%d",
             i, pExprNode ? (int)nodeType((SNode*)pExprNode) : -1);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }

    if (!first) {
      dynSQLAppendStr(s, ", ");
    }
    first = false;

    int32_t code = dynAppendExpr(s, pExpr, dialect, pExtMeta, pExprCtx);
    if (code) {
      return code;
    }

    if (withAlias && nodesIsExprNode((SNode*)pExpr)) {
      const char* alias = fqSelectAlias((const SExprNode*)pExpr, pExprCtx);
      if (alias != NULL && alias[0] != '\0') {
        dynSQLAppendStr(s, " AS ");
        dynAppendQuotedId(s, alias, dialect);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t renderCollectedAggTailExprs(SDynSQL* s, const SNodeList* pExprs, int32_t fromIdx,
                                           EExtSQLDialect dialect, const SExtTableMeta* pExtMeta,
                                           const SNodesRemoteSQLCtx* pCtx, bool* pWroteAny) {
  SNodesRemoteSQLCtx selectExprCtxStore = {0};
  const SNodesRemoteSQLCtx* pExprCtx = pCtx;
  if (pCtx != NULL) {
    selectExprCtxStore = *pCtx;
    selectExprCtxStore.renderingCurrentSelectExpr = true;
    pExprCtx = &selectExprCtxStore;
  }

  if (pExprs == NULL || fromIdx < 0) {
    return TSDB_CODE_SUCCESS;
  }

  bool wroteAny = (pWroteAny != NULL) ? *pWroteAny : false;
  int32_t len = LIST_LENGTH((SNodeList*)pExprs);
  for (int32_t i = fromIdx; i < len; ++i) {
    const SNode* pExprNode = nodesListGetNode((SNodeList*)pExprs, i);
    const SNode* pExpr = pExprNode;
    if (pExpr == NULL) {
      qError("FQ-SQL: renderCollectedAggTailExprs missing expr at idx=%d len=%d", i, len);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }

    if (nodeType((SNode*)pExpr) == QUERY_NODE_TARGET) {
      pExpr = ((const STargetNode*)pExpr)->pExpr;
    }
    if (pExpr == NULL) {
      qError("FQ-SQL: renderCollectedAggTailExprs resolved expr NULL at idx=%d rawType=%d",
             i, pExprNode ? (int)nodeType((SNode*)pExprNode) : -1);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }

    if (wroteAny) {
      dynSQLAppendStr(s, ", ");
    }
    wroteAny = true;

    int32_t code = dynAppendExpr(s, pExpr, dialect, pExtMeta, pExprCtx);
    if (code) {
      return code;
    }

    if (nodesIsExprNode((SNode*)pExpr)) {
      const char* alias = fqSelectAlias((const SExprNode*)pExpr, pExprCtx);
      if (alias != NULL && alias[0] != '\0') {
        dynSQLAppendStr(s, " AS ");
        dynAppendQuotedId(s, alias, dialect);
      }
    }
  }

  if (pWroteAny != NULL) {
    *pWroteAny = wroteAny;
  }
  return TSDB_CODE_SUCCESS;
}


// Choose the output-column alias for a remote-SELECT expression.
// Derived-table / subquery bodies must expose INTERNAL slot names (aliasName,
// e.g. "expr_1"), because the enclosing remote statement resolves current-scope
// references against that internal output namespace.
//
// For a top-level remote statement, plain column passthrough should keep the
// user-facing / natural column name, while computed expressions should keep the
// internal alias so function names are not leaked into the remote SQL alias
// space and later clause rewrites stay stable.
static bool fqIsCurrentScopeOutputRef(const SExprNode* pExpr) {
  if (NULL == pExpr || QUERY_NODE_COLUMN != nodeType((SNode*)pExpr)) {
    return false;
  }

  const SColumnNode* pCol = (const SColumnNode*)pExpr;
  return '\0' == pCol->tableAlias[0] && '\0' == pCol->tableName[0];
}

static bool fqIsGeneratedCurrentScopeAlias(const char* pAlias) {
  if (NULL == pAlias || '\0' == pAlias[0]) {
    return false;
  }

  return (0 == strncmp(pAlias, "expr_", strlen("expr_")) ||
          0 == strncmp(pAlias, "#expr_", strlen("#expr_")));
}

static bool fqShouldUseCurrentSelectRename(const SExprNode* pExpr,
                                           const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pExpr || QUERY_NODE_COLUMN != nodeType((SNode*)pExpr) || NULL == pCtx ||
      pCtx->emitInternalAlias || !pCtx->renderingCurrentSelectExpr ||
      !pCtx->inputSelectExprsFromChildSource || '\0' == pExpr->aliasName[0] ||
      fqIsGeneratedCurrentScopeAlias(pExpr->aliasName)) {
    return false;
  }

  const SColumnNode* pCol = (const SColumnNode*)pExpr;
  if ('\0' == pCol->colName[0]) {
    return false;
  }

  // Some pure-column rename projections decode as:
  //   aliasName = visible SELECT alias
  //   colName   = current-scope output ref
  //   userAlias = child-scope emitted alias
  // Keep that visible rename even when aliasName == colName, as long as the
  // child alias remains different.
  if (0 == strcmp(pExpr->aliasName, pCol->colName) &&
      ('\0' == pExpr->userAlias[0] || 0 == strcmp(pExpr->aliasName, pExpr->userAlias))) {
    return false;
  }

  const SExprNode* pInputExpr = fqResolveCurrentScopeOutputExpr(pCol, pCtx);
  if (NULL == pInputExpr || pInputExpr == pExpr) {
    return false;
  }

  const char* pInputAlias = fqFromChildSelectAlias(pInputExpr);
  if (NULL != pInputAlias && '\0' != pInputAlias[0] &&
      0 == strcmp(pExpr->aliasName, pInputAlias)) {
    return false;
  }

  qDebug("FQ-SQL-SELECT: prefer current-select rename alias=%s col=%s userAlias=%s childAlias=%s",
         pExpr->aliasName, pCol->colName, pExpr->userAlias,
         (NULL != pInputAlias && '\0' != pInputAlias[0]) ? pInputAlias : "(null)");
  return true;
}

static const char* fqColumnOutputAlias(const SColumnNode* pCol, const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pCol) {
    return NULL;
  }

  if (pCtx != NULL && pCtx->preserveOutputRefName && '\0' != pCol->colName[0]) {
    return pCol->colName;
  }

  return fqSelectAlias(&pCol->node, pCtx);
}

static const char* fqFromChildSelectAlias(const SExprNode* pExpr) {
  if (NULL == pExpr) {
    return NULL;
  }

  if (fqIsCurrentScopeOutputRef(pExpr) && QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    if ('\0' != pCol->colName[0]) {
      return pCol->colName;
    }
  }

  static const SNodesRemoteSQLCtx kFromChildAliasCtx = {.emitInternalAlias = true};
  const char* pAlias = fqSelectAlias(pExpr, &kFromChildAliasCtx);
  if (NULL != pAlias && '\0' != pAlias[0]) {
    return pAlias;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    if ('\0' != pCol->colName[0]) {
      return pCol->colName;
    }
  }

  return fqVisibleOutputAlias(pExpr);
}

static const char* fqVisibleOutputAlias(const SExprNode* pExpr) {
  return (NULL != pExpr && '\0' != pExpr->userAlias[0]) ? pExpr->userAlias : NULL;
}

static const char* fqWrapperTargetOutputAlias(const SExprNode* pExpr,
                                              const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pExpr) {
    return NULL;
  }

  if (pCtx != NULL && pCtx->emitInternalAlias) {
    return fqFromChildSelectAlias(pExpr);
  }

  return fqVisibleOutputAlias(pExpr);
}

static const char* fqSelectAlias(const SExprNode* pExpr, const SNodesRemoteSQLCtx* pCtx) {
  if (NULL == pExpr) {
    return NULL;
  }

  if (pCtx != NULL && pCtx->emitInternalAlias) {
    return ('\0' != pExpr->aliasName[0]) ? pExpr->aliasName : NULL;
  }

  // Top-level computed outputs must keep the planner's internal slot alias so
  // later clause rewrites resolve against the same remote SELECT namespace.
  // Plain column outputs still use the visible alias path handled elsewhere.
  if (QUERY_NODE_COLUMN != nodeType((SNode*)pExpr) && '\0' != pExpr->aliasName[0]) {
    return pExpr->aliasName;
  }

  if (fqShouldUseCurrentSelectRename(pExpr, pCtx)) {
    return pExpr->aliasName;
  }

  if (fqIsCurrentScopeOutputRef(pExpr) && '\0' != pExpr->aliasName[0] &&
      !fqIsGeneratedCurrentScopeAlias(pExpr->aliasName)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    if ('\0' == pCol->colName[0] || 0 != strcmp(pExpr->aliasName, pCol->colName)) {
      return pExpr->aliasName;
    }
  }

  if ('\0' != pExpr->userAlias[0]) {
    return pExpr->userAlias;
  }

  return ('\0' != pExpr->aliasName[0]) ? pExpr->aliasName : NULL;
}

// Render comma-separated GROUP BY key expressions from pAgg->pGroupKeys.
// After rewritePrecalcExprs, these are raw expressions (not SGroupingSetNode).
// Render GROUP KEY expressions.
// withAlias=true: emit AS aliasName after each expression (for SELECT list).
// withAlias=false: render bare expression only (for GROUP BY clause).
static int32_t renderGroupKeyExprs(SDynSQL* s, const SNodeList* pGroupKeys,
                                    const SNodeList* pTargetList,
                                    EExtSQLDialect dialect,
                                    const SExtTableMeta* pExtMeta,
                                    const SNodesRemoteSQLCtx* pCtx,
                                    bool withAlias) {
  if (!pGroupKeys || LIST_LENGTH(pGroupKeys) == 0) return TSDB_CODE_SUCCESS;
  bool first = true;
  int32_t idx = 0;
  SNode* pKey = NULL;
  FOREACH(pKey, pGroupKeys) {
    if (!first) dynSQLAppendStr(s, ", ");
    first = false;
    const SNode* pExpr = pKey;
    if (nodeType((SNode*)pExpr) == QUERY_NODE_GROUPING_SET) {
      pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
    }

    bool renderedAlias = false;
    if (!withAlias && dialect == EXT_SQL_DIALECT_MYSQL &&
        pExpr != NULL && nodesIsExprNode((SNode*)pExpr) &&
        fqTimestampEpochOutputClass(pExpr, dialect, pExtMeta, pCtx) !=
            EXT_TIMESTAMP_TZ_CLASS_UNKNOWN) {
      const char* pAlias = fqGroupKeyOutputAlias(pExpr, pTargetList, pCtx, idx);
      if (pAlias != NULL && pAlias[0] != '\0') {
        // MySQL's ONLY_FULL_GROUP_BY does not infer that the epoch transport
        // expression in SELECT is functionally dependent on the raw timestamp
        // bucket. Group by that SELECT alias so both clauses use one expression.
        dynAppendQuotedId(s, pAlias, dialect);
        renderedAlias = true;
      }
    }

    if (!renderedAlias) {
      int32_t code = dynAppendExpr(s, pKey, dialect, pExtMeta, pCtx);
      if (code) return code;
    }
    if (withAlias) {
      // The key may be wrapped in a GroupingSet (parser-built / partitionColsOpt);
      // unwrap to the real key expression so the alias is read from the column
      // (e.g. slot name "expr_1"), not the GroupingSet shell.  The enclosing outer
      // query references this output column by that slot name.
      const SNode* pAliasNode = pKey;
      if (nodeType(pAliasNode) == QUERY_NODE_GROUPING_SET) {
        pAliasNode = nodesListGetNode(((const SGroupingSetNode*)pAliasNode)->pParameterList, 0);
      }
      if (pAliasNode && nodesIsExprNode(pAliasNode)) {
        const char* alias = fqSelectAlias((const SExprNode*)pAliasNode, pCtx);
        if (alias && alias[0] != '\0') {
          dynSQLAppendStr(s, " AS ");
          dynAppendQuotedId(s, alias, dialect);
        }
      }
    }
    ++idx;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t fqCountAggGroupKeyExprs(const SAggLogicNode* pAgg) {
  if (pAgg == NULL) {
    return 0;
  }

  if (pAgg->pGroupKeys != NULL && LIST_LENGTH(pAgg->pGroupKeys) > 0) {
    return LIST_LENGTH(pAgg->pGroupKeys);
  }

  int32_t keyCnt = 0;
  if (pAgg->pAggFuncs == NULL) {
    return 0;
  }

  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    const SNode* pInner = pFuncNode;
    if (nodeType((SNode*)pInner) == QUERY_NODE_TARGET) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }
    if (pInner != NULL && nodeType((SNode*)pInner) == QUERY_NODE_FUNCTION &&
        fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      ++keyCnt;
    }
  }

  return keyCnt;
}

static int32_t renderAggGroupKeyExprs(SDynSQL* s, const SAggLogicNode* pAgg,
                                      EExtSQLDialect dialect,
                                      const SExtTableMeta* pExtMeta,
                                      const SNodesRemoteSQLCtx* pCtx,
                                      bool withAlias) {
  if (pAgg == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pAgg->pGroupKeys != NULL && LIST_LENGTH(pAgg->pGroupKeys) > 0) {
    return renderGroupKeyExprs(s, pAgg->pGroupKeys, pAgg->node.pTargets,
                               dialect, pExtMeta, pCtx, withAlias);
  }

  if (pAgg->pAggFuncs == NULL || LIST_LENGTH(pAgg->pAggFuncs) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  bool first = true;
  int32_t funcIdx = 0;
  int32_t keyIdx = 0;
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    const SNode* pInner = pFuncNode;
    if (nodeType((SNode*)pInner) == QUERY_NODE_TARGET) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }

    if (pInner == NULL || nodeType((SNode*)pInner) != QUERY_NODE_FUNCTION ||
        !fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      ++funcIdx;
      continue;
    }

    const SFunctionNode* pGroupKeyFunc = (const SFunctionNode*)pInner;
    const SNode* pKeyExpr =
        (pGroupKeyFunc->pParameterList != NULL)
            ? nodesListGetNode(pGroupKeyFunc->pParameterList, 0)
            : NULL;
    if (pKeyExpr == NULL) {
      qError("FQ-SQL: agg derived group key missing parameter agg=%p funcIdx=%d",
             pAgg, funcIdx);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }

    if (!first) {
      dynSQLAppendStr(s, ", ");
    }
    first = false;

    qDebug("FQ-SQL: render derived agg group key agg=%p funcIdx=%d withAlias=%d",
           pAgg, funcIdx, withAlias ? 1 : 0);
    bool renderedAlias = false;
    if (!withAlias && dialect == EXT_SQL_DIALECT_MYSQL &&
        nodesIsExprNode((SNode*)pKeyExpr) &&
        fqTimestampEpochOutputClass(pKeyExpr, dialect, pExtMeta, pCtx) !=
            EXT_TIMESTAMP_TZ_CLASS_UNKNOWN) {
      const char* pAlias = fqGroupKeyOutputAlias(pKeyExpr, NULL, pCtx, keyIdx);
      if (pAlias != NULL && pAlias[0] != '\0') {
        dynAppendQuotedId(s, pAlias, dialect);
        renderedAlias = true;
      }
    }

    if (!renderedAlias) {
      int32_t code = dynAppendExpr(s, pKeyExpr, dialect, pExtMeta, pCtx);
      if (code) {
        return code;
      }
    }

    ++keyIdx;
    ++funcIdx;
  }

  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// renderAggSelectList — render SELECT clause for an Agg node.
// Emits: [group keys,] agg functions with aliases.
// ---------------------------------------------------------------------------
static int32_t renderAggSelectList(SDynSQL* s, const SNodesRemoteSelectOutputs* pOutputs,
                                    bool isDistinct,
                                    EExtSQLDialect dialect,
                                    const SExtTableMeta* pMeta,
                                    const SNodesRemoteSQLCtx* pCtx) {
  bool wroteAny = false;
  qDebug("FQ-SQL: renderAggSelectList source=%s keyExprCount=%d outputExprs=%d isDistinct=%d dialect=%s",
         pOutputs ? remoteSelectSourceName(pOutputs->source) : "unknown",
         pOutputs ? pOutputs->keyExprCount : -1,
         (pOutputs != NULL && pOutputs->pSelectExprs != NULL) ? LIST_LENGTH(pOutputs->pSelectExprs) : 0,
         isDistinct ? 1 : 0, dialectName(dialect));
  int32_t code = renderCollectedKeyExprs(s, pOutputs ? pOutputs->pSelectExprs : NULL,
                                         pOutputs ? pOutputs->keyExprCount : 0,
                                         dialect, pMeta, pCtx, /*withAlias=*/true);
  if (code) return code;
  if (pOutputs != NULL && pOutputs->keyExprCount > 0) {
    wroteAny = true;
  }

  if (!isDistinct && pOutputs != NULL) {
    code = renderCollectedAggTailExprs(s, pOutputs->pSelectExprs, pOutputs->keyExprCount,
                                       dialect, pMeta, pCtx, &wroteAny);
    if (code) return code;
  }

  if (!wroteAny) dynSQLAppendChar(s, '*');
  return TSDB_CODE_SUCCESS;
}

// ─────────────────────────────────────────────────────────────────────────────
// Combination-driven renderer (design tmp/fq_remoteplan_refactor_20260613.md §6.-3).
//
// Mirrors the optimizer's collect/commit two-step model, but for SQL generation:
//   Step 1 (collect): walk the logic tree TOP-DOWN, gathering consecutive nodes
//     that belong to ONE parent SELECT (one node per SQL clause slot).  A node
//     whose clause slot is already occupied starts a SUBQUERY boundary; SCAN/JOIN
//     are FROM-sources that terminate the group.
//   Step 2 (render): emit one
//     SELECT <select> FROM (<subquery>|<table>|<join>) [WHERE] [GROUP BY] [HAVING]
//                                                      [ORDER BY] [LIMIT]
//   The FROM child (subquery boundary node) recurses back to step 1.
//
// SQL clause execution-order ranks (one slot each per query group):
//   1 FROM  2 WHERE  3 PARTITION-BY  4 GROUP-BY  5 HAVING  6 SELECT
//   7 ORDER-BY  8 LIMIT
// PARTITION keys (rank 3) and AGG group keys (rank 4) are distinct slots but
// render as ONE combined "GROUP BY partKeys[, groupKeys]" (partition+agg cases 2/3);
// a PARTITION carrying its own aggFuncs claims the SELECT slot, so it conflicts
// with an AGG above and is pushed into a subquery (case 1).
// ─────────────────────────────────────────────────────────────────────────────

// Clause-slot bitmask for query-group collection.
#define FQ_SLOT_WHERE      (1u << 0)
#define FQ_SLOT_PARTITION  (1u << 1)
#define FQ_SLOT_GROUPBY    (1u << 2)
#define FQ_SLOT_HAVING     (1u << 3)
#define FQ_SLOT_SELECT     (1u << 4)
#define FQ_SLOT_ORDERBY    (1u << 5)
#define FQ_SLOT_LIMIT      (1u << 6)

// The clause sources gathered for one parent SELECT.
typedef struct SFqQueryGroup {
  const SAggLogicNode*           pAgg;        // GROUP BY (groupKeys) + HAVING + SELECT (aggFuncs)
  const SPartitionLogicNode*     pPartition;  // PARTITION keys → GROUP BY [+ SELECT aggFuncs]
  const SProjectLogicNode*       pProject;    // SELECT (pProjections)
  const SIndefRowsFuncLogicNode* pIndef;      // SELECT (window funcs)
  const SSortLogicNode*          pSort;       // ORDER BY
  const SScanLogicNode*          pScanWhere;  // WHERE source (the FROM scan, if any)
  const SLogicNode*              pWhereNode;  // intermediate node (PROJECT/SORT/…) carrying its own WHERE (node.pConditions)
  const SNode*                   pLimit;      // LIMIT (from whichever node carries it)
  const SLogicNode*              pFromSource; // SCAN or JOIN at the group bottom (FROM table/join)
  const SLogicNode*              pFromChild;  // non-FROM-source child → render as "(subquery) _sq"
  const SLogicNode*              pProjectSelectChild;  // child whose SELECT slot is folded into pProject
  uint32_t                       used;        // occupied FQ_SLOT_* bits
} SFqQueryGroup;

// True if the list has at least one aggregate function that is NOT a _group_key
// pass-through (i.e. a real SELECT-list aggregate the renderer would emit).
static bool fqAggFuncsHaveRealAgg(const SNodeList* pAggFuncs) {
  SNode* pF = NULL;
  FOREACH(pF, pAggFuncs) {
    const SNode* pInner = pF;
    if (nodeType(pInner) == QUERY_NODE_TARGET) pInner = ((const STargetNode*)pInner)->pExpr;
    if (pInner && nodeType(pInner) == QUERY_NODE_FUNCTION &&
        fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      continue;  // _group_key: rendered via GROUP BY, not a SELECT aggregate
    }
    return true;
  }
  return false;
}

// DISTINCT-only AGG is the planner shape for:
//   SELECT DISTINCT <group keys> ...
// It contributes GROUP BY semantics, but it does not own a standalone SELECT
// list the way real aggregate functions do.  Treat it specially so an outer
// pass-through PROJECT can stay in the same query group instead of being split
// into a derived table, which would otherwise lose DISTINCT during rendering.
static bool fqAggIsDistinctOnly(const SAggLogicNode* pAgg) {
  return (pAgg != NULL &&
          pAgg->pGroupKeys != NULL &&
          LIST_LENGTH(pAgg->pGroupKeys) > 0 &&
          (pAgg->pAggFuncs == NULL || LIST_LENGTH(pAgg->pAggFuncs) == 0));
}

// Clause slots a node contributes (0 for FROM-source nodes — handled separately).
static uint32_t fqNodeClauseSlots(const SLogicNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_PROJECT: {
      const SProjectLogicNode* pProj = (const SProjectLogicNode*)pNode;
      // A multi-child set-op (UNION) PROJECT is a FROM-source (its own statement).
      if (pNode->pChildren != NULL && LIST_LENGTH(pNode->pChildren) > 1) return 0;
      (void)pProj;
      uint32_t slots = FQ_SLOT_SELECT;
      // A PROJECT carrying its own pushable WHERE (post-projection filter) occupies
      // the WHERE slot too; a second WHERE-carrier forces a subquery boundary so
      // the filter renders in the correct clause position.
      if (pNode->pConditions) slots |= FQ_SLOT_WHERE;
      return slots;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      const SAggLogicNode* pAgg = (const SAggLogicNode*)pNode;
      uint32_t slots = fqAggIsDistinctOnly(pAgg) ? 0 : FQ_SLOT_SELECT;
      if (pAgg->pGroupKeys && LIST_LENGTH(pAgg->pGroupKeys) > 0) slots |= FQ_SLOT_GROUPBY;
      if (pAgg->node.pConditions) slots |= FQ_SLOT_HAVING;
      qDebug("FQ-SELECT: agg slots distinctOnly=%d groupKeys=%d aggFuncs=%d slots=0x%x",
             fqAggIsDistinctOnly(pAgg) ? 1 : 0,
             pAgg->pGroupKeys ? LIST_LENGTH(pAgg->pGroupKeys) : 0,
             pAgg->pAggFuncs ? LIST_LENGTH(pAgg->pAggFuncs) : 0,
             slots);
      return slots;
    }
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      const SPartitionLogicNode* pPart = (const SPartitionLogicNode*)pNode;
      uint32_t slots = FQ_SLOT_PARTITION;
      // Claim the SELECT slot only for REAL aggregates; a PARTITION whose pAggFuncs
      // are only _group_key pass-throughs contributes no SELECT-list aggregate (the
      // keys render via GROUP BY), so it must not conflict with an AGG above —
      // they merge into one "SELECT keys, aggs ... GROUP BY keys".
      if (fqAggFuncsHaveRealAgg(pPart->pAggFuncs)) slots |= FQ_SLOT_SELECT;
      if (pPart->node.pConditions) slots |= FQ_SLOT_WHERE;
      return slots;
    }
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      return FQ_SLOT_SELECT | (pNode->pConditions ? FQ_SLOT_WHERE : 0);
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return FQ_SLOT_ORDERBY | (pNode->pConditions ? FQ_SLOT_WHERE : 0);
    default:                                    return 0;
  }
}

static bool fqIsFromSource(const SLogicNode* pNode) {
  ENodeType t = nodeType(pNode);
  if (t == QUERY_NODE_LOGIC_PLAN_SCAN || t == QUERY_NODE_LOGIC_PLAN_JOIN) return true;
  // Multi-child set-op PROJECT (UNION/UNION ALL) is its own statement → FROM-source.
  if (t == QUERY_NODE_LOGIC_PLAN_PROJECT && pNode->pChildren != NULL &&
      LIST_LENGTH(pNode->pChildren) > 1) return true;
  return false;
}

static bool fqNodeRequiresWhereWrapper(const SLogicNode* pNode) {
  if (NULL == pNode || NULL == pNode->pConditions) {
    return false;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return !(pNode->pChildren != NULL && LIST_LENGTH(pNode->pChildren) > 1);
    case QUERY_NODE_LOGIC_PLAN_SORT:
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      return true;
    default:
      return false;
  }
}

static const char* fqSelectAlias(const SExprNode* pExpr, const SNodesRemoteSQLCtx* pCtx);
int32_t nodesCollectRemoteQuerySelectOutputs(const SLogicNode* pQueryRoot,
                                             SNodesRemoteSelectOutputs* pOutputs);
void nodesDestroyRemoteSelectOutputs(SNodesRemoteSelectOutputs* pOutputs);

static const SNode* fqUnwrapSelectExpr(const SNode* pNode) {
  if (pNode == NULL) {
    return NULL;
  }

  const SNode* pExpr = pNode;
  if (nodeType((SNode*)pExpr) == QUERY_NODE_TARGET) {
    pExpr = ((const STargetNode*)pExpr)->pExpr;
  }
  return pExpr;
}

static const char* fqChildOutputRefName(const SNode* pChildExpr) {
  if (pChildExpr == NULL) {
    return NULL;
  }

  return (nodesIsExprNode((SNode*)pChildExpr) && '\0' != ((const SExprNode*)pChildExpr)->aliasName[0])
             ? ((const SExprNode*)pChildExpr)->aliasName
             : NULL;
}

static const char* fqChildOutputVisibleName(const SNode* pChildExpr) {
  if (pChildExpr == NULL || !nodesIsExprNode((SNode*)pChildExpr)) {
    return NULL;
  }

  const SExprNode* pChildOut = (const SExprNode*)pChildExpr;
  if ('\0' != pChildOut->userAlias[0]) {
    return pChildOut->userAlias;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pChildExpr)) {
    const SColumnNode* pChildCol = (const SColumnNode*)pChildExpr;
    if ('\0' != pChildCol->colName[0]) {
      return pChildCol->colName;
    }
  }

  return fqChildOutputRefName(pChildExpr);
}

static bool fqNonEmptyNameEquals(const char* pLeft, const char* pRight) {
  return NULL != pLeft && NULL != pRight && '\0' != pLeft[0] && '\0' != pRight[0] && 0 == strcmp(pLeft, pRight);
}

static bool fqProjectColumnIsWindowStartPseudo(const SColumnNode* pProjCol) {
  if (pProjCol == NULL) {
    return false;
  }

  return COLUMN_TYPE_WINDOW_START == pProjCol->colType ||
         fqNonEmptyNameEquals(pProjCol->node.userAlias, "_wstart") ||
         fqNonEmptyNameEquals(pProjCol->node.aliasName, "_wstart") ||
         fqNonEmptyNameEquals(pProjCol->colName, "_wstart");
}

static bool fqChildOutputIsWindowStartBucket(const SNode* pChildOutputExpr) {
  const SNode* pChildExpr = fqUnwrapSelectExpr(pChildOutputExpr);
  if (pChildExpr == NULL) {
    return false;
  }

  if (QUERY_NODE_FUNCTION == nodeType((SNode*)pChildExpr)) {
    return FUNCTION_TYPE_FQ_TIME_BUCKET == ((const SFunctionNode*)pChildExpr)->funcType;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pChildExpr)) {
    const SColumnNode* pChildCol = (const SColumnNode*)pChildExpr;
    return COLUMN_TYPE_WINDOW_START == pChildCol->colType ||
           fqNonEmptyNameEquals(pChildCol->node.userAlias, "_wstart") ||
           fqNonEmptyNameEquals(pChildCol->node.aliasName, "_wstart") ||
           fqNonEmptyNameEquals(pChildCol->colName, "_wstart");
  }

  return false;
}

static bool fqProjectColumnMatchesChildOutput(const SColumnNode* pProjCol,
                                              const SNode* pChildOutputExpr) {
  const SNode* pChildExpr = fqUnwrapSelectExpr(pChildOutputExpr);
  if (pProjCol == NULL || pChildExpr == NULL) {
    return false;
  }

  if (fqProjectColumnIsWindowStartPseudo(pProjCol)) {
    if (fqChildOutputIsWindowStartBucket(pChildOutputExpr)) {
      return true;
    }
  }

  const char* pChildRefName = fqChildOutputRefName(pChildExpr);
  const char* pChildVisibleName = fqChildOutputVisibleName(pChildExpr);
  if (fqNonEmptyNameEquals(pProjCol->node.userAlias, pChildVisibleName) ||
      fqNonEmptyNameEquals(pProjCol->node.userAlias, pChildRefName)) {
    return true;
  }

  if (fqProjectColumnIsWindowStartPseudo(pProjCol)) {
    return false;
  }

  if (fqNonEmptyNameEquals(pProjCol->colName, pChildRefName) ||
      fqNonEmptyNameEquals(pProjCol->node.aliasName, pChildRefName)) {
    return true;
  }

  return false;
}

static bool fqProjectExprMatchesChildOutput(const SNode* pProjExpr,
                                            const SNode* pChildOutputExpr,
                                            bool* pIsPureColumnRemap) {
  const SNode* pProjInner = fqUnwrapSelectExpr(pProjExpr);
  const SNode* pChildExpr = fqUnwrapSelectExpr(pChildOutputExpr);
  if (pIsPureColumnRemap != NULL) {
    *pIsPureColumnRemap = false;
  }

  if (NULL == pProjInner || NULL == pChildExpr) {
    return false;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pProjInner)) {
    if (NULL != pIsPureColumnRemap) {
      *pIsPureColumnRemap = true;
    }
    return fqProjectColumnMatchesChildOutput((const SColumnNode*)pProjInner, pChildOutputExpr);
  }

  return nodesEqualNode(pProjInner, pChildExpr);
}

static bool fqCanReuseRootTargetsAsAlignedRefs(const SNodeList* pOutputs,
                                               const SNodeList* pRootTargets) {
  if (NULL == pOutputs || NULL == pRootTargets) {
    return false;
  }

  const int32_t outputCnt = LIST_LENGTH((SNodeList*)pOutputs);
  if (outputCnt <= 0 || outputCnt != LIST_LENGTH((SNodeList*)pRootTargets)) {
    return false;
  }

  for (int32_t idx = 0; idx < outputCnt; ++idx) {
    const SNode* pOutputExpr = nodesListGetNode((SNodeList*)pOutputs, idx);
    const SNode* pTargetExpr = nodesListGetNode((SNodeList*)pRootTargets, idx);
    if (!fqProjectExprMatchesChildOutput(pTargetExpr, pOutputExpr, NULL)) {
      qDebug("FQ-SELECT: root target ref fallback rejected outputIdx=%d targetType=%d outputType=%d",
             idx, pTargetExpr ? (int)nodeType((SNode*)pTargetExpr) : -1,
             pOutputExpr ? (int)nodeType((SNode*)pOutputExpr) : -1);
      return false;
    }
  }

  return true;
}

static int32_t fqFindUniqueProjectChildOutputIndexByExpr(const SNode* pProjExpr,
                                                         const SNodeList* pChildOutputs,
                                                         int32_t* pTargetIdx,
                                                         bool* pIsPureColumnRemap) {
  if (NULL == pProjExpr || NULL == pChildOutputs || NULL == pTargetIdx) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t idx = 0;
  int32_t matchedIdx = -1;
  int32_t matchedCnt = 0;
  bool    matchedPureColumnRemap = false;
  SNode*  pChildOutput = NULL;
  FOREACH(pChildOutput, pChildOutputs) {
    bool isPureColumnRemap = false;
    if (fqProjectExprMatchesChildOutput(pProjExpr, pChildOutput, &isPureColumnRemap)) {
      matchedIdx = idx;
      ++matchedCnt;
      matchedPureColumnRemap = isPureColumnRemap;
    }
    ++idx;
  }

  if (1 == matchedCnt) {
    *pTargetIdx = matchedIdx;
    if (NULL != pIsPureColumnRemap) {
      *pIsPureColumnRemap = matchedPureColumnRemap;
    }
    return TSDB_CODE_SUCCESS;
  }

  qError("FQ-SELECT: project expr mapping is not unique exprType=%d childOutputs=%d matchedCnt=%d",
         (int)nodeType((SNode*)pProjExpr), LIST_LENGTH((SNodeList*)pChildOutputs), matchedCnt);
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t fqFindUniqueProjectChildOutputIndex(const SColumnNode* pProjCol,
                                                   const SNodeList* pChildOutputs,
                                                   const SNodeList* pChildRefExprs,
                                                   int32_t* pTargetIdx) {
  if (NULL != pChildRefExprs &&
      LIST_LENGTH((SNodeList*)pChildRefExprs) == LIST_LENGTH((SNodeList*)pChildOutputs)) {
    int32_t code = fqFindProjectChildOutputIndexByRefExprs(pProjCol, pChildRefExprs, pTargetIdx);
    if (TSDB_CODE_SUCCESS == code) {
      return code;
    }
  }

  return fqFindUniqueProjectChildOutputIndexByExpr((const SNode*)pProjCol, pChildOutputs,
                                                   pTargetIdx, NULL);
}

static int32_t fqFindProjectChildOutputIndexByRefExprs(const SColumnNode* pProjCol,
                                                       const SNodeList* pChildRefExprs,
                                                       int32_t* pTargetIdx) {
  if (NULL == pProjCol || NULL == pChildRefExprs || NULL == pTargetIdx) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t idx = 0;
  int32_t primaryMatchedIdx = -1;
  int32_t primaryMatchedCnt = 0;
  int32_t fallbackMatchedIdx = -1;
  int32_t fallbackMatchedCnt = 0;
  const bool requireWindowStartSemanticMatch = fqProjectColumnIsWindowStartPseudo(pProjCol);
  SNode* pChildRefNode = NULL;
  FOREACH(pChildRefNode, pChildRefExprs) {
    const SNode* pChildRefExpr = fqUnwrapSelectExpr(pChildRefNode);
    if (NULL != pChildRefExpr && QUERY_NODE_COLUMN == nodeType((SNode*)pChildRefExpr)) {
      const SColumnNode* pChildRefCol = (const SColumnNode*)pChildRefExpr;
      if (fqNonEmptyNameEquals(pProjCol->node.userAlias, pChildRefCol->node.userAlias) ||
          fqNonEmptyNameEquals(pProjCol->node.userAlias, pChildRefCol->colName)) {
        primaryMatchedIdx = idx;
        ++primaryMatchedCnt;
      } else if (!requireWindowStartSemanticMatch &&
                 (fqNonEmptyNameEquals(pProjCol->colName, pChildRefCol->colName) ||
                  fqNonEmptyNameEquals(pProjCol->node.aliasName, pChildRefCol->colName))) {
        fallbackMatchedIdx = idx;
        ++fallbackMatchedCnt;
      }
    }
    ++idx;
  }

  if (1 == primaryMatchedCnt) {
    *pTargetIdx = primaryMatchedIdx;
    return TSDB_CODE_SUCCESS;
  }

  if (0 == primaryMatchedCnt && 1 == fallbackMatchedCnt) {
    *pTargetIdx = fallbackMatchedIdx;
    return TSDB_CODE_SUCCESS;
  }

  qError("FQ-SELECT: project ref mapping is not unique projCol=%s projAlias=%s projUserAlias=%s childRefs=%d primaryMatchedCnt=%d fallbackMatchedCnt=%d",
         pProjCol->colName, pProjCol->node.aliasName, pProjCol->node.userAlias,
         LIST_LENGTH((SNodeList*)pChildRefExprs), primaryMatchedCnt, fallbackMatchedCnt);
  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static bool fqProjectCanMergeChildSelect(const SProjectLogicNode* pProject,
                                         const SLogicNode* pChild) {
  if (pProject == NULL || pChild == NULL || pProject->isSetOpProj ||
      pProject->node.pConditions != NULL || pProject->pProjections == NULL ||
      pChild->pTargets == NULL || LIST_LENGTH(pChild->pTargets) == 0) {
    return false;
  }

  SNodesRemoteSelectOutputs childOutputs = {0};
  int32_t code = nodesCollectRemoteQuerySelectOutputs(pChild, &childOutputs);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: project merge validation cannot collect child outputs project=%p child=%p childType=%s code=0x%x",
           pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)), code);
    return false;
  }

  if (childOutputs.pSelectExprs == NULL || LIST_LENGTH(childOutputs.pSelectExprs) == 0) {
    qError("FQ-SELECT: project merge validation got empty child outputs project=%p child=%p childType=%s source=%s",
           pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)),
           remoteSelectSourceName(childOutputs.source));
    nodesDestroyRemoteSelectOutputs(&childOutputs);
    return false;
  }

  fqDebugLogExprList("project-merge.projectProjections", pProject->pProjections);
  fqDebugLogExprList("project-merge.childOutputs", childOutputs.pSelectExprs);

  const int32_t childOutputCnt = LIST_LENGTH(childOutputs.pSelectExprs);
  const int32_t projectOutputCnt = LIST_LENGTH(pProject->pProjections);
  const bool preservesAllChildOutputs = (projectOutputCnt == childOutputCnt);

  if (!preservesAllChildOutputs &&
      (nodeType((SNode*)pChild) == QUERY_NODE_LOGIC_PLAN_SORT ||
       nodeType((SNode*)pChild) == QUERY_NODE_LOGIC_PLAN_AGG)) {
    qDebug("FQ-SELECT: project merge rejected because project prunes child semantic outputs project=%p child=%p childType=%s projectOutputs=%d childOutputs=%d source=%s",
           pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)),
           projectOutputCnt, childOutputCnt, remoteSelectSourceName(childOutputs.source));
    nodesDestroyRemoteSelectOutputs(&childOutputs);
    return false;
  }

  bool* pUsedChildSlots = childOutputCnt > 0 ? taosMemoryCalloc(childOutputCnt, sizeof(bool)) : NULL;
  if (childOutputCnt > 0 && pUsedChildSlots == NULL) {
    qError("FQ-SELECT: project merge validation cannot allocate child slot map project=%p child=%p childType=%s childOutputs=%d",
           pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)), childOutputCnt);
    nodesDestroyRemoteSelectOutputs(&childOutputs);
    return false;
  }

  int32_t projIdx = 0;
  SNode* pProjExpr = NULL;
  FOREACH(pProjExpr, pProject->pProjections) {
    const SNode* pProjInner = fqUnwrapSelectExpr(pProjExpr);
    if (NULL == pProjInner) {
      qDebug("FQ-SELECT: project merge rejected because projection is empty project=%p child=%p projIdx=%d",
             pProject, pChild, projIdx);
      taosMemoryFree(pUsedChildSlots);
      nodesDestroyRemoteSelectOutputs(&childOutputs);
      return false;
    }

    bool isPureColumnRemap = false;
    int32_t targetIdx = -1;
    int32_t mapCode = TSDB_CODE_SUCCESS;
    if (QUERY_NODE_COLUMN == nodeType((SNode*)pProjInner)) {
      mapCode = fqFindUniqueProjectChildOutputIndex((const SColumnNode*)pProjInner,
                                                    childOutputs.pSelectExprs,
                                                    childOutputs.pRefExprs,
                                                    &targetIdx);
      isPureColumnRemap = (TSDB_CODE_SUCCESS == mapCode);
    } else {
      mapCode =
          fqFindUniqueProjectChildOutputIndexByExpr(pProjExpr, childOutputs.pSelectExprs, &targetIdx,
                                                    &isPureColumnRemap);
    }

    if (TSDB_CODE_SUCCESS != mapCode) {
      qDebug("FQ-SELECT: project merge rejected because projection cannot map uniquely to child output project=%p child=%p childType=%s projType=%s childOutputs=%d source=%s",
             pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)),
             nodesNodeName(nodeType((SNode*)pProjInner)),
             LIST_LENGTH(childOutputs.pSelectExprs), remoteSelectSourceName(childOutputs.source));
      taosMemoryFree(pUsedChildSlots);
      nodesDestroyRemoteSelectOutputs(&childOutputs);
      return false;
    }

    if (pUsedChildSlots[targetIdx]) {
      qDebug("FQ-SELECT: project merge rejected because projection reuses child output slot project=%p child=%p childType=%s projType=%s childSlot=%d childOutputs=%d source=%s",
             pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)),
             nodesNodeName(nodeType((SNode*)pProjInner)),
             targetIdx, LIST_LENGTH(childOutputs.pSelectExprs), remoteSelectSourceName(childOutputs.source));
      taosMemoryFree(pUsedChildSlots);
      nodesDestroyRemoteSelectOutputs(&childOutputs);
      return false;
    }

    const SExprNode* pChildOutExpr = (const SExprNode*)nodesListGetNode(childOutputs.pSelectExprs, targetIdx);
    const char* pChildAlias = (pChildOutExpr != NULL && pChildOutExpr->aliasName[0] != '\0') ? pChildOutExpr->aliasName : NULL;
    const char* pChildUserAlias = (pChildOutExpr != NULL && pChildOutExpr->userAlias[0] != '\0') ? pChildOutExpr->userAlias : NULL;
    const char* pChildColName = NULL;
    if (pChildOutExpr != NULL && QUERY_NODE_COLUMN == nodeType((SNode*)pChildOutExpr)) {
      const SColumnNode* pChildCol = (const SColumnNode*)pChildOutExpr;
      pChildColName = pChildCol->colName[0] != '\0' ? pChildCol->colName : NULL;
    }

    if (isPureColumnRemap) {
      const SColumnNode* pProjCol = (const SColumnNode*)pProjInner;
      const char* pProjColName = pProjCol->colName[0] != '\0' ? pProjCol->colName : NULL;
      const char* pProjAlias = pProjCol->node.aliasName[0] != '\0' ? pProjCol->node.aliasName : NULL;
      const char* pProjUserAlias = pProjCol->node.userAlias[0] != '\0' ? pProjCol->node.userAlias : NULL;
      qDebug("FQ-SELECT: project merge accepted pure-column remap project=%p child=%p childType=%s projIdx=%d childSlot=%d projCol=%s projAlias=%s projUserAlias=%s childAlias=%s childUserAlias=%s childCol=%s",
             pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)), projIdx, targetIdx,
             pProjColName ? pProjColName : "(null)",
             pProjAlias ? pProjAlias : "(null)",
             pProjUserAlias ? pProjUserAlias : "(null)",
             pChildAlias ? pChildAlias : "(null)",
             pChildUserAlias ? pChildUserAlias : "(null)",
             pChildColName ? pChildColName : "(null)");
    } else {
      qDebug("FQ-SELECT: project merge accepted equivalent expr passthrough project=%p child=%p childType=%s projIdx=%d projType=%s childSlot=%d childExprType=%s childAlias=%s childUserAlias=%s childCol=%s",
             pProject, pChild, nodesNodeName(nodeType((SNode*)pChild)), projIdx,
             nodesNodeName(nodeType((SNode*)pProjInner)), targetIdx,
             pChildOutExpr ? nodesNodeName(nodeType((SNode*)pChildOutExpr)) : "(null)",
             pChildAlias ? pChildAlias : "(null)",
             pChildUserAlias ? pChildUserAlias : "(null)",
             pChildColName ? pChildColName : "(null)");
    }

    pUsedChildSlots[targetIdx] = true;
    ++projIdx;
  }

  taosMemoryFree(pUsedChildSlots);
  nodesDestroyRemoteSelectOutputs(&childOutputs);
  return true;
}

static int32_t fqFindProjectChildOutputIndex(const SColumnNode* pProjCol,
                                             const SNodeList* pChildOutputs,
                                             const SNodeList* pChildRefExprs,
                                             int32_t* pTargetIdx) {
  return fqFindUniqueProjectChildOutputIndex(pProjCol, pChildOutputs, pChildRefExprs,
                                             pTargetIdx);
}

static bool fqJoinHasWhereCarrier(const SJoinLogicNode* pJoin) {
  if (NULL == pJoin) {
    return false;
  }

  if (NULL != pJoin->node.pConditions) {
    return true;
  }

  const SScanLogicNode* pLeft = (const SScanLogicNode*)nodesListGetNode(pJoin->node.pChildren, 0);
  if (NULL != pLeft && NULL != pLeft->node.pConditions) {
    return true;
  }

  const SScanLogicNode* pRight = (const SScanLogicNode*)nodesListGetNode(pJoin->node.pChildren, 1);
  if (NULL != pRight && NULL != pRight->node.pConditions) {
    return true;
  }

  return false;
}

static uint32_t fqFromSourceClauseSlots(const SLogicNode* pNode) {
  if (NULL == pNode) {
    return 0;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      uint32_t slots = (NULL != pNode->pLimit) ? FQ_SLOT_LIMIT : 0;
      if (pNode->pConditions) {
        slots |= FQ_SLOT_WHERE;
      }
      return slots;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      uint32_t slots = (NULL != pNode->pLimit) ? FQ_SLOT_LIMIT : 0;
      if (fqJoinHasWhereCarrier((const SJoinLogicNode*)pNode)) {
        slots |= FQ_SLOT_WHERE;
      }
      return slots;
    }
    default:
      return 0;
  }
}

static void fqInitQueryGroup(SFqQueryGroup* pGroup) {
  memset(pGroup, 0, sizeof(*pGroup));
}

static int32_t fqCollectQueryGroup(const SLogicNode* pRoot, SFqQueryGroup* pGroup, const char* pLogTag) {
  if (pRoot == NULL || pGroup == NULL) {
    qError("FQ-SELECT: collect query group invalid input root=%p group=%p", pRoot, pGroup);
    return TSDB_CODE_INVALID_PARA;
  }

  fqInitQueryGroup(pGroup);
  const SLogicNode* pCur = pRoot;
  qDebug("FQ-SELECT: [%s] collect query group rootType=%s root=%p",
         pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pRoot)), pRoot);

  while (pCur != NULL) {
    if (fqIsFromSource(pCur)) {
      uint32_t fromSlots = fqFromSourceClauseSlots(pCur);
      if (fromSlots & pGroup->used) {
        pGroup->pFromChild = pCur;
        qDebug("FQ-SELECT: [%s] from-source=%s extra-slot conflict slots=0x%x used=0x%x, use as from-child boundary node=%p",
               pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)),
               fromSlots, pGroup->used, pCur);
        break;
      }

      if ((nodeType(pCur) == QUERY_NODE_LOGIC_PLAN_SCAN || nodeType(pCur) == QUERY_NODE_LOGIC_PLAN_JOIN) &&
          NULL != pCur->pLimit) {
        pGroup->used |= FQ_SLOT_LIMIT;
        pGroup->pLimit = pCur->pLimit;
        qDebug("FQ-SELECT: [%s] harvest from-source LIMIT node=%s limit=%p used=0x%x fromNode=%p",
               pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)),
               pCur->pLimit, pGroup->used, pCur);
      }

      if (fromSlots & FQ_SLOT_WHERE) {
        pGroup->used |= FQ_SLOT_WHERE;
      }

      pGroup->pFromSource = pCur;
      qDebug("FQ-SELECT: [%s] group bottom from-source=%s node=%p extraSlots=0x%x",
             pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)), pCur, fromSlots);
      break;
    }

    if (pCur != pRoot && fqNodeRequiresWhereWrapper(pCur)) {
      pGroup->pFromChild = pCur;
      qDebug("FQ-SELECT: [%s] node=%s requires outer-WHERE wrapper, use as from-child boundary node=%p",
             pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)), pCur);
      break;
    }

    uint32_t slots = fqNodeClauseSlots(pCur);
    if (slots == 0) {
      pGroup->pFromChild = pCur;
      qDebug("FQ-SELECT: [%s] node=%s contributes no clause, use as from-child boundary node=%p",
             pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)), pCur);
      break;
    }

    bool allowProjectSelectMerge = false;
    uint32_t mergedSlots = slots;
    if (pGroup->pProject != NULL && fqProjectCanMergeChildSelect(pGroup->pProject, pCur)) {
      // Same remote SQL render scope: a pass-through Project must inherit the
      // child's REAL output expressions recursively, instead of keeping only
      // slot/alias references like expr_1 at the project layer.  The Project
      // still owns the final output order/aliases, but the child's SELECT slot
      // is folded into that same SELECT list.
      mergedSlots = slots & ~FQ_SLOT_SELECT;
      if ((mergedSlots & pGroup->used) == 0) {
        allowProjectSelectMerge = true;
        pGroup->pProjectSelectChild = pCur;
        qDebug("FQ-SELECT: [%s] allow pass-through project merge project=%p child=%s childSlots=0x%x mergedSlots=0x%x used=0x%x child=%p",
               pLogTag ? pLogTag : "collector", pGroup->pProject,
               nodesNodeName(nodeType(pCur)), slots, mergedSlots, pGroup->used, pCur);
      }
    }

    if ((allowProjectSelectMerge ? mergedSlots : slots) & pGroup->used) {
      pGroup->pFromChild = pCur;
      qDebug("FQ-SELECT: [%s] node=%s slot conflict slots=0x%x used=0x%x, use as from-child boundary node=%p",
             pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)), slots, pGroup->used, pCur);
      break;
    }

    if (pCur->pLimit != NULL) {
      if (pGroup->used & FQ_SLOT_LIMIT) {
        pGroup->pFromChild = pCur;
        qDebug("FQ-SELECT: [%s] node=%s carries second LIMIT, use as from-child boundary node=%p",
               pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)), pCur);
        break;
      }
      pGroup->used |= FQ_SLOT_LIMIT;
      pGroup->pLimit = pCur->pLimit;
    }

    pGroup->used |= allowProjectSelectMerge ? mergedSlots : slots;
    switch (nodeType(pCur)) {
      case QUERY_NODE_LOGIC_PLAN_PROJECT:
        if (!allowProjectSelectMerge || pGroup->pProject == NULL) {
          pGroup->pProject = (const SProjectLogicNode*)pCur;
        } else {
          qDebug("FQ-SELECT: [%s] preserve final project=%p while merging pass-through child project=%p",
                 pLogTag ? pLogTag : "collector", pGroup->pProject, pCur);
        }
        break;
      case QUERY_NODE_LOGIC_PLAN_AGG:
        pGroup->pAgg = (const SAggLogicNode*)pCur;
        break;
      case QUERY_NODE_LOGIC_PLAN_PARTITION:
        pGroup->pPartition = (const SPartitionLogicNode*)pCur;
        break;
      case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
        pGroup->pIndef = (const SIndefRowsFuncLogicNode*)pCur;
        break;
      case QUERY_NODE_LOGIC_PLAN_SORT:
        pGroup->pSort = (const SSortLogicNode*)pCur;
        break;
      default:
        break;
    }

    if (pCur->pConditions != NULL && nodeType(pCur) != QUERY_NODE_LOGIC_PLAN_AGG) {
      pGroup->pWhereNode = pCur;
    }

    qDebug("FQ-SELECT: [%s] include node=%s slots=0x%x used=0x%x node=%p",
           pLogTag ? pLogTag : "collector", nodesNodeName(nodeType(pCur)),
           allowProjectSelectMerge ? mergedSlots : slots, pGroup->used, pCur);
    pCur = getFirstChild(pCur);
  }

  if (pGroup->pFromSource != NULL && nodeType(pGroup->pFromSource) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    pGroup->pScanWhere = (const SScanLogicNode*)pGroup->pFromSource;
  }

  qDebug("FQ-SELECT: [%s] group summary root=%p proj=%c agg=%c part=%c indef=%c sort=%c scan=%c fromChild=%c fromSource=%c mergedChild=%p mergedChildType=%s",
         pLogTag ? pLogTag : "collector", pRoot,
         pGroup->pProject ? 'Y' : 'N', pGroup->pAgg ? 'Y' : 'N',
         pGroup->pPartition ? 'Y' : 'N', pGroup->pIndef ? 'Y' : 'N',
         pGroup->pSort ? 'Y' : 'N', pGroup->pScanWhere ? 'Y' : 'N',
         pGroup->pFromChild ? 'Y' : 'N', pGroup->pFromSource ? 'Y' : 'N',
         pGroup->pProjectSelectChild,
         pGroup->pProjectSelectChild ? nodesNodeName(nodeType((SNode*)pGroup->pProjectSelectChild)) : "(null)");
  return TSDB_CODE_SUCCESS;
}

static int32_t fqCloneSelectExprWithAlias(const SNode* pExprNode, const SNode* pTargetNode,
                                          int32_t outputIdx, const char* pTag, SNode** ppClone) {
  if (ppClone == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppClone = NULL;
  if (pExprNode == NULL) {
    qError("FQ-SELECT: clone expr got NULL input tag=%s outputIdx=%d",
           pTag ? pTag : "(null)", outputIdx);
    return TSDB_CODE_INVALID_PARA;
  }

  const SNode* pCloneSrc = pExprNode;
  if (nodeType((SNode*)pCloneSrc) == QUERY_NODE_TARGET) {
    const STargetNode* pTarget = (const STargetNode*)pCloneSrc;
    if (pTarget->pExpr != NULL && nodeType(pTarget->pExpr) == QUERY_NODE_GROUPING_SET) {
      pCloneSrc = nodesListGetNode(((const SGroupingSetNode*)pTarget->pExpr)->pParameterList, 0);
      qDebug("FQ-SELECT: normalize target-grouping-set output tag=%s outputIdx=%d",
             pTag ? pTag : "(null)", outputIdx);
    }
  } else if (nodeType((SNode*)pCloneSrc) == QUERY_NODE_GROUPING_SET) {
    pCloneSrc = nodesListGetNode(((const SGroupingSetNode*)pCloneSrc)->pParameterList, 0);
    qDebug("FQ-SELECT: normalize grouping-set output tag=%s outputIdx=%d",
           pTag ? pTag : "(null)", outputIdx);
  }

  if (pCloneSrc == NULL) {
    qError("FQ-SELECT: normalize expr got NULL source tag=%s outputIdx=%d rawType=%d",
           pTag ? pTag : "(null)", outputIdx, (int)nodeType((SNode*)pExprNode));
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  int32_t code = nodesCloneNode((SNode*)pCloneSrc, ppClone);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: clone expr failed tag=%s outputIdx=%d nodeType=%d code=0x%x",
           pTag ? pTag : "(null)", outputIdx, (int)nodeType((SNode*)pCloneSrc), code);
    return code;
  }

  SNode* pExpr = *ppClone;
  if (pExpr != NULL && nodeType(pExpr) == QUERY_NODE_TARGET) {
    pExpr = ((STargetNode*)pExpr)->pExpr;
  }

  if (pExpr != NULL && nodeType(pExpr) == QUERY_NODE_GROUPING_SET) {
    pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
  }

  if (pExpr != NULL && nodesIsExprNode(pExpr)) {
    SExprNode* pExprNodeClone = (SExprNode*)pExpr;
    if (pTargetNode != NULL) {
      const SNode* pTargetExpr = pTargetNode;
      if (nodeType((SNode*)pTargetExpr) == QUERY_NODE_TARGET) {
        pTargetExpr = ((const STargetNode*)pTargetExpr)->pExpr;
      }

      if (pTargetExpr != NULL && nodesIsExprNode((SNode*)pTargetExpr)) {
        const char* pAlias = ((const SExprNode*)pTargetExpr)->aliasName;
        const char* pUserAlias = ((const SExprNode*)pTargetExpr)->userAlias;
        if (nodeType((SNode*)pTargetExpr) == QUERY_NODE_COLUMN &&
            (pAlias == NULL || pAlias[0] == '\0')) {
          const SColumnNode* pTargetCol = (const SColumnNode*)pTargetExpr;
          if (pTargetCol->colName[0] != '\0') {
            pAlias = pTargetCol->colName;
          }
        }

        if (pAlias != NULL && pAlias[0] != '\0') {
          bool aliasChanged = (0 != strcmp(pExprNodeClone->aliasName, pAlias));
          tstrncpy(pExprNodeClone->aliasName, pAlias, sizeof(pExprNodeClone->aliasName));
          if (aliasChanged) {
            qDebug("FQ-SELECT: align alias from target tag=%s outputIdx=%d alias=%s targetType=%d",
                   pTag ? pTag : "(null)", outputIdx, pAlias, (int)nodeType((SNode*)pTargetExpr));
          }
        }

        const char* pChosenUserAlias = pUserAlias;
        if (pChosenUserAlias != NULL && pChosenUserAlias[0] != '\0') {
          bool userAliasChanged = (0 != strcmp(pExprNodeClone->userAlias, pChosenUserAlias));
          tstrncpy(pExprNodeClone->userAlias, pChosenUserAlias, sizeof(pExprNodeClone->userAlias));
          if (userAliasChanged) {
            qDebug("FQ-SELECT: align userAlias from target tag=%s outputIdx=%d userAlias=%s targetType=%d keepClone=%d",
                   pTag ? pTag : "(null)", outputIdx, pChosenUserAlias,
                   (int)nodeType((SNode*)pTargetExpr),
                   (pUserAlias == NULL || pUserAlias[0] == '\0') ? 1 : 0);
          }
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fqAppendCollectedSelectExpr(SNodeList** ppList, const SNode* pExprNode,
                                           const SNode* pTargetNode, int32_t outputIdx,
                                           const char* pTag) {
  SNode* pClone = NULL;
  int32_t code = fqCloneSelectExprWithAlias(pExprNode, pTargetNode, outputIdx, pTag, &pClone);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = nodesListMakeStrictAppend(ppList, pClone);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: append select expr failed tag=%s outputIdx=%d code=0x%x",
           pTag ? pTag : "(null)", outputIdx, code);
    nodesDestroyNode(pClone);
    return code;
  }

  qDebug("FQ-SELECT: append select expr tag=%s outputIdx=%d nodeType=%d targetType=%d",
         pTag ? pTag : "(null)", outputIdx, (int)nodeType((SNode*)pExprNode),
         pTargetNode ? (int)nodeType((SNode*)pTargetNode) : -1);
  return TSDB_CODE_SUCCESS;
}

static int32_t fqAppendCollectedRefExpr(SNodeList** ppList, const SNode* pRefNode,
                                        int32_t outputIdx, const char* pTag) {
  if (NULL == ppList || NULL == pRefNode) {
    return TSDB_CODE_INVALID_PARA;
  }

  SNode* pClone = NULL;
  int32_t code = nodesCloneNode((SNode*)pRefNode, &pClone);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: clone ref expr failed tag=%s outputIdx=%d nodeType=%d code=0x%x",
           pTag ? pTag : "(null)", outputIdx, (int)nodeType((SNode*)pRefNode), code);
    return code;
  }

  code = nodesListMakeStrictAppend(ppList, pClone);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: append ref expr failed tag=%s outputIdx=%d code=0x%x",
           pTag ? pTag : "(null)", outputIdx, code);
    nodesDestroyNode(pClone);
    return code;
  }

  qDebug("FQ-SELECT: append ref expr tag=%s outputIdx=%d nodeType=%d",
         pTag ? pTag : "(null)", outputIdx, (int)nodeType((SNode*)pRefNode));
  return TSDB_CODE_SUCCESS;
}

static int32_t fqAppendCollectedExprList(SNodeList* pExprs, const SNodeList* pTargets,
                                         int32_t* pOutputIdx, SNodeList** ppSelectExprs,
                                         const char* pTag) {
  if (pExprs == NULL || LIST_LENGTH(pExprs) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    const SNode* pTarget = (pTargets != NULL) ? nodesListGetNode((SNodeList*)pTargets, *pOutputIdx) : NULL;
    int32_t code = fqAppendCollectedSelectExpr(ppSelectExprs, pExpr, pTarget, *pOutputIdx, pTag);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    ++(*pOutputIdx);
  }
  return TSDB_CODE_SUCCESS;
}

static const SNode* fqNormalizeOutputAliasExpr(const SNode* pNode) {
  const SNode* pExpr = pNode;
  if (NULL != pExpr && QUERY_NODE_TARGET == nodeType((SNode*)pExpr)) {
    pExpr = ((const STargetNode*)pExpr)->pExpr;
  }
  if (NULL != pExpr && QUERY_NODE_GROUPING_SET == nodeType((SNode*)pExpr)) {
    pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
  }
  return pExpr;
}

static bool fqTargetExprMatchesName(const SNode* pTargetNode, const char* pName) {
  const SNode* pTargetExpr = fqNormalizeOutputAliasExpr(pTargetNode);
  if (NULL == pTargetExpr || NULL == pName || '\0' == pName[0] || !nodesIsExprNode((SNode*)pTargetExpr)) {
    return false;
  }

  const SExprNode* pTargetExprNode = (const SExprNode*)pTargetExpr;
  if (('\0' != pTargetExprNode->aliasName[0] && 0 == strcmp(pName, pTargetExprNode->aliasName)) ||
      ('\0' != pTargetExprNode->userAlias[0] && 0 == strcmp(pName, pTargetExprNode->userAlias))) {
    return true;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pTargetExpr)) {
    const char* pColName = ((const SColumnNode*)pTargetExpr)->colName;
    return '\0' != pColName[0] && 0 == strcmp(pName, pColName);
  }

  return false;
}

static bool fqOutputExprMatchesTargetExpr(const SNode* pOutputExpr, const SNode* pTargetNode) {
  const SNode* pExpr = fqNormalizeOutputAliasExpr(pOutputExpr);
  if (NULL == pExpr || !nodesIsExprNode((SNode*)pExpr)) {
    return false;
  }

  const SExprNode* pExprNode = (const SExprNode*)pExpr;
  if (QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    if (fqTargetExprMatchesName(pTargetNode, pCol->colName)) {
      return true;
    }

    return fqTargetExprMatchesName(pTargetNode, pExprNode->userAlias);
  }

  if (fqTargetExprMatchesName(pTargetNode, pExprNode->aliasName)) {
    return true;
  }

  return fqTargetExprMatchesName(pTargetNode, pExprNode->userAlias);
}

static int32_t fqFindUniqueAggOutputTargetIndex(const SNode* pOutputExpr,
                                                const SNodeList* pTargets,
                                                int32_t* pTargetIdx) {
  if (NULL == pOutputExpr || NULL == pTargets || NULL == pTargetIdx) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t idx = 0;
  int32_t matchedIdx = -1;
  int32_t matchedCnt = 0;
  SNode* pTargetNode = NULL;
  FOREACH(pTargetNode, pTargets) {
    if (fqOutputExprMatchesTargetExpr(pOutputExpr, pTargetNode)) {
      matchedIdx = idx;
      ++matchedCnt;
    }
    ++idx;
  }

  if (1 == matchedCnt) {
    *pTargetIdx = matchedIdx;
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_PLAN_INTERNAL_ERROR;
}

static int32_t fqAppendCollectedRealAggExprs(SNodeList* pAggFuncs, int32_t* pOutputIdx,
                                             SNodeList** ppSelectExprs, const char* pTag) {
  if (pAggFuncs == NULL || LIST_LENGTH(pAggFuncs) == 0) {
    qDebug("FQ-SELECT: skip agg expr append because list empty tag=%s", pTag ? pTag : "(null)");
    return TSDB_CODE_SUCCESS;
  }

  int32_t funcIdx = 0;
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAggFuncs) {
    const SNode* pInner = pFuncNode;
    if (nodeType((SNode*)pInner) == QUERY_NODE_TARGET) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }

    if (pInner != NULL && nodeType((SNode*)pInner) == QUERY_NODE_FUNCTION &&
        fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      // _group_key is a planner-internal wrapper for GROUP BY keys.  It must be
      // removed here, before the final remote output target list is materialized,
      // because downstream mapping/descriptor creation is a strict 1:1 walk over
      // that final target list with no additional filtering.
      qDebug("FQ-SELECT: skip _group_key agg output tag=%s funcIdx=%d outputIdx=%d",
             pTag ? pTag : "(null)", funcIdx, *pOutputIdx);
      ++funcIdx;
      continue;
    }

    int32_t code = fqAppendCollectedSelectExpr(ppSelectExprs, pFuncNode, NULL, *pOutputIdx, pTag);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    ++(*pOutputIdx);
    ++funcIdx;
  }
  return TSDB_CODE_SUCCESS;
}

static const SNode* fqFindAggGroupKeyTargetNode(const SAggLogicNode* pAgg,
                                                const SNode* pGroupKeyNode) {
  if (NULL == pAgg || NULL == pGroupKeyNode || NULL == pAgg->node.pTargets) {
    return NULL;
  }

  int32_t targetIdx = -1;
  if (TSDB_CODE_SUCCESS ==
      fqFindUniqueAggOutputTargetIndex(pGroupKeyNode, pAgg->node.pTargets, &targetIdx)) {
    return nodesListGetNode((SNodeList*)pAgg->node.pTargets, targetIdx);
  }

  const SNode* pNormGroupKey = fqNormalizeOutputAliasExpr(pGroupKeyNode);
  if (NULL == pNormGroupKey || NULL == pAgg->pAggFuncs) {
    return NULL;
  }

  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    const SNode* pInner = pFuncNode;
    if (QUERY_NODE_TARGET == nodeType((SNode*)pInner)) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }

    if (NULL == pInner || QUERY_NODE_FUNCTION != nodeType((SNode*)pInner) ||
        !fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      continue;
    }

    const SFunctionNode* pGroupKeyFunc = (const SFunctionNode*)pInner;
    const SNode* pKeyExpr =
        (NULL != pGroupKeyFunc->pParameterList)
            ? nodesListGetNode((SNodeList*)pGroupKeyFunc->pParameterList, 0)
            : NULL;
    if (NULL == pKeyExpr) {
      continue;
    }

    if (!nodesEqualNode((SNode*)pNormGroupKey, (SNode*)fqNormalizeOutputAliasExpr(pKeyExpr))) {
      continue;
    }

    targetIdx = -1;
    if (TSDB_CODE_SUCCESS ==
        fqFindUniqueAggOutputTargetIndex(pFuncNode, pAgg->node.pTargets, &targetIdx)) {
      return nodesListGetNode((SNodeList*)pAgg->node.pTargets, targetIdx);
    }
  }

  return NULL;
}

static int32_t fqAppendCollectedPartitionExprs(const SPartitionLogicNode* pPartition,
                                               int32_t* pOutputIdx,
                                               SNodesRemoteSelectOutputs* pOutputs) {
  if (NULL == pPartition || NULL == pOutputIdx || NULL == pOutputs) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int32_t keyCount =
      (pPartition->pPartitionKeys != NULL) ? LIST_LENGTH(pPartition->pPartitionKeys) : 0;
  pOutputs->keyExprCount = keyCount;

  SNode* pKeyNode = NULL;
  FOREACH(pKeyNode, pPartition->pPartitionKeys) {
    const SNode* pTargetNode = NULL;
    if (pPartition->node.pTargets != NULL) {
      int32_t matchedTargetIdx = -1;
      if (TSDB_CODE_SUCCESS ==
          fqFindUniqueAggOutputTargetIndex((const SNode*)pKeyNode, pPartition->node.pTargets,
                                           &matchedTargetIdx)) {
        pTargetNode = nodesListGetNode((SNodeList*)pPartition->node.pTargets, matchedTargetIdx);
      }
    }

    int32_t code = fqAppendCollectedSelectExpr(&pOutputs->pSelectExprs, pKeyNode,
                                               pTargetNode, *pOutputIdx, "partition-key");
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    code = fqAppendCollectedRefExpr(&pOutputs->pRefExprs,
                                    (pTargetNode != NULL) ? pTargetNode : (const SNode*)pKeyNode,
                                    *pOutputIdx, "partition-key-ref");
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    ++(*pOutputIdx);
  }

  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pPartition->pAggFuncs) {
    const SNode* pInner = pFuncNode;
    if (nodeType((SNode*)pInner) == QUERY_NODE_TARGET) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }

    if (pInner != NULL && nodeType((SNode*)pInner) == QUERY_NODE_FUNCTION &&
        fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      continue;
    }

    const SNode* pTargetNode = NULL;
    if (pPartition->node.pTargets != NULL) {
      int32_t matchedTargetIdx = -1;
      if (TSDB_CODE_SUCCESS ==
          fqFindUniqueAggOutputTargetIndex((const SNode*)pFuncNode, pPartition->node.pTargets,
                                           &matchedTargetIdx)) {
        pTargetNode = nodesListGetNode((SNodeList*)pPartition->node.pTargets, matchedTargetIdx);
      }
    }

    int32_t code = fqAppendCollectedSelectExpr(&pOutputs->pSelectExprs, pFuncNode,
                                               pTargetNode, *pOutputIdx, "partition-func");
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    code = fqAppendCollectedRefExpr(&pOutputs->pRefExprs,
                                    (pTargetNode != NULL) ? pTargetNode : pFuncNode,
                                    *pOutputIdx, "partition-func-ref");
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    ++(*pOutputIdx);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fqAppendCollectedAggKeyAndTailExprs(const SAggLogicNode* pAgg,
                                                   int32_t* pOutputIdx,
                                                   SNodesRemoteSelectOutputs* pOutputs) {
  if (pAgg == NULL || pOutputIdx == NULL || pOutputs == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int32_t aggTargetBase =
      (pAgg->pAggFuncs != NULL) ? LIST_LENGTH(pAgg->pAggFuncs) : 0;
  const int32_t aggTargetCount =
      (pAgg->node.pTargets != NULL) ? LIST_LENGTH(pAgg->node.pTargets) : 0;
  const int32_t explicitGroupKeyCount =
      (pAgg->pGroupKeys != NULL) ? LIST_LENGTH(pAgg->pGroupKeys) : 0;
  const bool aggTargetsCoverAllOutputs =
      (explicitGroupKeyCount > 0) ? (aggTargetCount >= aggTargetBase + explicitGroupKeyCount)
                                  : (aggTargetCount >= aggTargetBase);

  if (pAgg->pGroupKeys != NULL && LIST_LENGTH(pAgg->pGroupKeys) > 0) {
    pOutputs->keyExprCount = LIST_LENGTH(pAgg->pGroupKeys);
    const bool targetsCoverAllOutputs = aggTargetsCoverAllOutputs;
    // Shared AGG targets follow 3.0 order: aggregate outputs first, rewritten
    // group-key outputs after them when targets cover the full output shape.
    // Some branches only materialize explicit group-key targets; in that case
    // agg outputs must keep their own aliases and refs instead of borrowing a
    // group-key target by slot index. When the target list does not cover the
    // full output shape, only an unambiguous semantic match may reuse a target.
    int32_t groupKeyIdx = 0;
    SNode* pGroupKeyNode = NULL;
    FOREACH(pGroupKeyNode, pAgg->pGroupKeys) {
      const SNode* pTargetNode = fqFindAggGroupKeyTargetNode(pAgg, pGroupKeyNode);
      if (NULL == pTargetNode && pAgg->node.pTargets != NULL && targetsCoverAllOutputs) {
        const int32_t targetIdx = aggTargetBase + groupKeyIdx;
        if (targetIdx >= 0 && targetIdx < aggTargetCount) {
          pTargetNode = nodesListGetNode((SNodeList*)pAgg->node.pTargets, targetIdx);
        }
      }
      int32_t code = fqAppendCollectedSelectExpr(&pOutputs->pSelectExprs, pGroupKeyNode,
                                                 pTargetNode, *pOutputIdx, "agg-group-key");
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }

      code = fqAppendCollectedRefExpr(&pOutputs->pRefExprs,
                                      (pTargetNode != NULL) ? pTargetNode : (const SNode*)pGroupKeyNode,
                                      *pOutputIdx, "agg-group-key-ref");
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }

      ++groupKeyIdx;
      ++(*pOutputIdx);
    }
  }

  if (pAgg->pAggFuncs == NULL || LIST_LENGTH(pAgg->pAggFuncs) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t targetOutputIdx = 0;
  int32_t funcIdx = 0;
  SNode* pFuncNode = NULL;
  FOREACH(pFuncNode, pAgg->pAggFuncs) {
    const SNode* pInner = pFuncNode;
    if (nodeType((SNode*)pInner) == QUERY_NODE_TARGET) {
      pInner = ((const STargetNode*)pInner)->pExpr;
    }

    const SNode* pTargetNode = NULL;
    if (pAgg->node.pTargets != NULL) {
      int32_t matchedTargetIdx = -1;
      if (TSDB_CODE_SUCCESS ==
          fqFindUniqueAggOutputTargetIndex((const SNode*)pFuncNode, pAgg->node.pTargets, &matchedTargetIdx)) {
        pTargetNode = nodesListGetNode(pAgg->node.pTargets, matchedTargetIdx);
      } else if (targetOutputIdx < aggTargetCount && aggTargetsCoverAllOutputs) {
        pTargetNode = nodesListGetNode(pAgg->node.pTargets, targetOutputIdx);
      }
    }

    if (pInner != NULL && nodeType((SNode*)pInner) == QUERY_NODE_FUNCTION &&
        fmIsGroupKeyFunc(((const SFunctionNode*)pInner)->funcId)) {
      const SFunctionNode* pGroupKeyFunc = (const SFunctionNode*)pInner;
      const SNode* pKeyExpr =
          (pGroupKeyFunc->pParameterList != NULL)
              ? nodesListGetNode(pGroupKeyFunc->pParameterList, 0)
              : NULL;
      if (pAgg->pGroupKeys == NULL || LIST_LENGTH(pAgg->pGroupKeys) == 0) {
        if (pKeyExpr == NULL) {
          qError("FQ-SELECT: _group_key output missing parameter agg=%p funcIdx=%d outputIdx=%d",
                 pAgg, funcIdx, *pOutputIdx);
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }

        int32_t code = fqAppendCollectedSelectExpr(&pOutputs->pSelectExprs, pKeyExpr, pTargetNode,
                                                   *pOutputIdx, "agg-group-key-from-func");
        if (TSDB_CODE_SUCCESS != code) {
          qError("FQ-SELECT: append _group_key-derived key failed agg=%p funcIdx=%d outputIdx=%d code=0x%x",
                 pAgg, funcIdx, *pOutputIdx, code);
          return code;
        }

        code = fqAppendCollectedRefExpr(&pOutputs->pRefExprs,
                                        (pTargetNode != NULL) ? pTargetNode : pKeyExpr,
                                        *pOutputIdx, "agg-group-key-from-func-ref");
        if (TSDB_CODE_SUCCESS != code) {
          qError("FQ-SELECT: append _group_key-derived ref failed agg=%p funcIdx=%d outputIdx=%d code=0x%x",
                 pAgg, funcIdx, *pOutputIdx, code);
          return code;
        }

        ++pOutputs->keyExprCount;
        ++(*pOutputIdx);
        ++targetOutputIdx;
      } else {
        qDebug("FQ-SELECT: skip redundant _group_key agg output because explicit groupKeys already exist agg=%p funcIdx=%d outputIdx=%d",
               pAgg, funcIdx, *pOutputIdx);
      }

      ++funcIdx;
      continue;
    }

    int32_t code = fqAppendCollectedSelectExpr(&pOutputs->pSelectExprs, pFuncNode, pTargetNode,
                                               *pOutputIdx, "agg-func");
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: append agg func output failed agg=%p funcIdx=%d outputIdx=%d code=0x%x",
             pAgg, funcIdx, *pOutputIdx, code);
      return code;
    }

    code = fqAppendCollectedRefExpr(&pOutputs->pRefExprs,
                                    (pTargetNode != NULL) ? pTargetNode : pFuncNode,
                                    *pOutputIdx, "agg-func-ref");
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: append agg func ref failed agg=%p funcIdx=%d outputIdx=%d code=0x%x",
             pAgg, funcIdx, *pOutputIdx, code);
      return code;
    }

    ++targetOutputIdx;
    ++(*pOutputIdx);
    ++funcIdx;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fqBuildWrapperOutputRefs(const SNodeList* pTargets, const char* pTag,
                                        const SNodesRemoteSQLCtx* pAliasCtx,
                                        SNodeList** ppRefs) {
  if (ppRefs == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppRefs = NULL;
  if (pTargets == NULL || LIST_LENGTH((SNodeList*)pTargets) == 0) {
    qError("FQ-SELECT: wrapper output refs got empty targets tag=%s", pTag ? pTag : "(null)");
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  int32_t targetIdx = 0;
  SNode* pTargetNode = NULL;
  FOREACH(pTargetNode, pTargets) {
    const SNode* pExpr = pTargetNode;
    if (nodeType((SNode*)pExpr) == QUERY_NODE_TARGET) {
      pExpr = ((const STargetNode*)pExpr)->pExpr;
    }
    if (pExpr != NULL && nodeType((SNode*)pExpr) == QUERY_NODE_GROUPING_SET) {
      pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
    }
    if (pExpr == NULL || !nodesIsExprNode((SNode*)pExpr)) {
      qError("FQ-SELECT: wrapper output ref cannot resolve target expr tag=%s targetIdx=%d rawType=%d",
             pTag ? pTag : "(null)", targetIdx,
             pTargetNode ? (int)nodeType((SNode*)pTargetNode) : -1);
      nodesDestroyList(*ppRefs);
      *ppRefs = NULL;
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    const SExprNode* pExprNode = (const SExprNode*)pExpr;
    const char* pInnerRefName = fqFromChildSelectAlias(pExprNode);
    const char* pVisibleAlias = fqVisibleOutputAlias(pExprNode);
    const char* pOutputAlias = fqWrapperTargetOutputAlias(pExprNode, pAliasCtx);

    if (pInnerRefName == NULL || pInnerRefName[0] == '\0' ||
        pOutputAlias == NULL || pOutputAlias[0] == '\0') {
      qError("FQ-SELECT: wrapper output ref missing alias tag=%s targetIdx=%d exprType=%d innerRef=%s visible=%s output=%s",
             pTag ? pTag : "(null)", targetIdx, (int)nodeType((SNode*)pExpr),
             pInnerRefName ? pInnerRefName : "(null)",
             pVisibleAlias ? pVisibleAlias : "(null)",
             pOutputAlias ? pOutputAlias : "(null)");
      nodesDestroyList(*ppRefs);
      *ppRefs = NULL;
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    SColumnNode* pRefCol = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pRefCol);
    if (TSDB_CODE_SUCCESS != code || pRefCol == NULL) {
      qError("FQ-SELECT: wrapper output ref alloc failed tag=%s targetIdx=%d code=0x%x",
             pTag ? pTag : "(null)", targetIdx, code);
      nodesDestroyList(*ppRefs);
      *ppRefs = NULL;
      return (TSDB_CODE_SUCCESS == code) ? TSDB_CODE_OUT_OF_MEMORY : code;
    }

    pRefCol->node.resType = pExprNode->resType;
    tstrncpy(pRefCol->colName, pInnerRefName, sizeof(pRefCol->colName));
    tstrncpy(pRefCol->node.aliasName, pOutputAlias, sizeof(pRefCol->node.aliasName));
    tstrncpy(pRefCol->node.userAlias, pVisibleAlias, sizeof(pRefCol->node.userAlias));

    code = nodesListMakeStrictAppend(ppRefs, (SNode*)pRefCol);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: wrapper output ref append failed tag=%s targetIdx=%d ref=%s code=0x%x",
             pTag ? pTag : "(null)", targetIdx, pInnerRefName, code);
      nodesDestroyNode((SNode*)pRefCol);
      nodesDestroyList(*ppRefs);
      *ppRefs = NULL;
      return code;
    }

    qDebug("FQ-SELECT: wrapper output ref tag=%s targetIdx=%d exprType=%s innerRef=%s visible=%s output=%s",
           pTag ? pTag : "(null)", targetIdx, nodesNodeName(nodeType((SNode*)pExpr)),
           pInnerRefName, pVisibleAlias, pOutputAlias);
    ++targetIdx;
  }

  fqDebugLogExprList("wrapper-output-refs", *ppRefs);
  return TSDB_CODE_SUCCESS;
}

void nodesDestroyRemoteSelectOutputs(SNodesRemoteSelectOutputs* pOutputs) {
  if (pOutputs == NULL) {
    return;
  }

  nodesDestroyList(pOutputs->pSelectExprs);
  nodesDestroyList(pOutputs->pRefExprs);
  memset(pOutputs, 0, sizeof(*pOutputs));
}

static int32_t fqCollectMergedProjectSelectOutputs(const SFqQueryGroup* pGroup,
                                                   SNodesRemoteSelectOutputs* pOutputs) {
  if (pGroup == NULL || pOutputs == NULL || pGroup->pProject == NULL || pGroup->pProjectSelectChild == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SNodesRemoteSelectOutputs childOutputs = {0};
  int32_t code = nodesCollectRemoteQuerySelectOutputs(pGroup->pProjectSelectChild, &childOutputs);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: collect merged child outputs failed project=%p child=%p childType=%s code=0x%x",
           pGroup->pProject, pGroup->pProjectSelectChild,
           nodesNodeName(nodeType((SNode*)pGroup->pProjectSelectChild)), code);
    return code;
  }

  if (childOutputs.pSelectExprs == NULL || LIST_LENGTH(childOutputs.pSelectExprs) == 0) {
    qError("FQ-SELECT: merged project child has empty collected outputs project=%p child=%p childType=%s source=%s",
           pGroup->pProject, pGroup->pProjectSelectChild,
           nodesNodeName(nodeType((SNode*)pGroup->pProjectSelectChild)),
           remoteSelectSourceName(childOutputs.source));
    nodesDestroyRemoteSelectOutputs(&childOutputs);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  fqDebugLogExprList("project-merged.childOutputs", childOutputs.pSelectExprs);
  fqDebugLogExprList("project-merged.projectProjections", pGroup->pProject->pProjections);

  if (pGroup->pProjectSelectChild->pTargets == NULL ||
      LIST_LENGTH(pGroup->pProjectSelectChild->pTargets) == 0) {
    qError("FQ-SELECT: merged project child has empty targets project=%p child=%p childType=%s",
           pGroup->pProject, pGroup->pProjectSelectChild,
           nodesNodeName(nodeType((SNode*)pGroup->pProjectSelectChild)));
    nodesDestroyRemoteSelectOutputs(&childOutputs);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  pOutputs->source = NODES_REMOTE_SELECT_SOURCE_PROJECT;
  int32_t outputIdx = 0;
  SNode* pProjExpr = NULL;
  FOREACH(pProjExpr, pGroup->pProject->pProjections) {
    int32_t targetIdx = -1;
    if (QUERY_NODE_COLUMN == nodeType((SNode*)fqUnwrapSelectExpr(pProjExpr))) {
      code = fqFindProjectChildOutputIndex((const SColumnNode*)fqUnwrapSelectExpr(pProjExpr),
                                           childOutputs.pSelectExprs, childOutputs.pRefExprs,
                                           &targetIdx);
    } else {
      code = fqFindUniqueProjectChildOutputIndexByExpr(pProjExpr, childOutputs.pSelectExprs,
                                                       &targetIdx, NULL);
    }
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: merged project output cannot resolve child output project=%p child=%p outputIdx=%d code=0x%x",
             pGroup->pProject, pGroup->pProjectSelectChild, outputIdx, code);
      break;
    }

    const SNode* pChildExpr = nodesListGetNode(childOutputs.pSelectExprs, targetIdx);
    const SNode* pProjectAliasSource =
        (pGroup->pProject->node.pTargets != NULL)
            ? nodesListGetNode(pGroup->pProject->node.pTargets, outputIdx)
            : pProjExpr;
    code = fqAppendCollectedSelectExpr(&pOutputs->pSelectExprs, pChildExpr, pProjectAliasSource,
                                       outputIdx, "project-merged");
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: append merged project output failed project=%p child=%p childSlot=%d outputIdx=%d code=0x%x",
             pGroup->pProject, pGroup->pProjectSelectChild, targetIdx, outputIdx, code);
      break;
    }

    code = fqAppendCollectedRefExpr(&pOutputs->pRefExprs,
                                    (pProjectAliasSource != NULL) ? pProjectAliasSource : pProjExpr,
                                    outputIdx, "project-merged-ref");
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: append merged project ref failed project=%p child=%p childSlot=%d outputIdx=%d code=0x%x",
             pGroup->pProject, pGroup->pProjectSelectChild, targetIdx, outputIdx, code);
      break;
    }

    qDebug("FQ-SELECT: merged project output project=%p child=%p childType=%s outputIdx=%d childSlot=%d childSource=%s aliasSourceType=%d",
           pGroup->pProject, pGroup->pProjectSelectChild,
           nodesNodeName(nodeType((SNode*)pGroup->pProjectSelectChild)),
           outputIdx, targetIdx, remoteSelectSourceName(childOutputs.source),
           pProjectAliasSource ? (int)nodeType((SNode*)pProjectAliasSource) : -1);
    ++outputIdx;
  }

  fqDebugLogExprList("project-merged.finalOutputs", pOutputs->pSelectExprs);

  nodesDestroyRemoteSelectOutputs(&childOutputs);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyRemoteSelectOutputs(pOutputs);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static void fqResolveRemoteOutputShape(const SLogicNode* pQueryRoot,
                                       const SFqQueryGroup* pGroup,
                                       ENodesRemoteSelectSource* pSource,
                                       int32_t* pKeyExprCount) {
  if (pSource != NULL) {
    *pSource = NODES_REMOTE_SELECT_SOURCE_UNKNOWN;
  }
  if (pKeyExprCount != NULL) {
    *pKeyExprCount = 0;
  }
  if (pQueryRoot == NULL) {
    return;
  }

  if (nodeType((SNode*)pQueryRoot) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_JOIN;
    }
    return;
  }

  if (nodeType((SNode*)pQueryRoot) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
      pQueryRoot->pChildren != NULL && LIST_LENGTH(pQueryRoot->pChildren) > 1) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_SETOP;
    }
    return;
  }

  if (pGroup == NULL) {
    return;
  }

  if (pGroup->pProject != NULL) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_PROJECT;
    }
    return;
  }

  if (pGroup->pAgg != NULL) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_AGG;
    }
    if (pKeyExprCount != NULL) {
      *pKeyExprCount = pGroup->pAgg->pGroupKeys ? LIST_LENGTH(pGroup->pAgg->pGroupKeys) : 0;
    }
    return;
  }

  if (pGroup->pPartition != NULL && pGroup->pPartition->pAggFuncs != NULL &&
      LIST_LENGTH(pGroup->pPartition->pAggFuncs) > 0) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_PARTITION;
    }
    if (pKeyExprCount != NULL) {
      *pKeyExprCount = pGroup->pPartition->pPartitionKeys ? LIST_LENGTH(pGroup->pPartition->pPartitionKeys) : 0;
    }
    return;
  }

  if (pGroup->pIndef != NULL) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_INDEF;
    }
    return;
  }

  if (pGroup->pScanWhere != NULL) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_SCAN;
    }
    return;
  }

  if (pGroup->pFromChild != NULL && pGroup->pFromChild->pTargets != NULL) {
    if (pSource != NULL) {
      *pSource = NODES_REMOTE_SELECT_SOURCE_FROM_CHILD;
    }
  }
}

int32_t nodesCollectRemoteQuerySelectOutputs(const SLogicNode* pQueryRoot,
                                             SNodesRemoteSelectOutputs* pOutputs) {
  if (pQueryRoot == NULL || pOutputs == NULL) {
    qError("FQ-SELECT: collect outputs invalid input root=%p outputs=%p", pQueryRoot, pOutputs);
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pOutputs, 0, sizeof(*pOutputs));

  if (fqNodeRequiresWhereWrapper(pQueryRoot)) {
    if (pQueryRoot->pTargets == NULL || LIST_LENGTH(pQueryRoot->pTargets) == 0) {
      qError("FQ-SELECT: wrapper node has no targets root=%p rootType=%s",
             pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)));
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_FROM_CHILD;
    pOutputs->keyExprCount = 0;

    int32_t code = TSDB_CODE_SUCCESS;
    if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType((SNode*)pQueryRoot)) {
      const SProjectLogicNode* pProject = (const SProjectLogicNode*)pQueryRoot;
      if (pProject->pProjections != NULL && LIST_LENGTH(pProject->pProjections) > 0) {
        // A post-node WHERE wrapper does not change a Project's real SELECT
        // expressions. Preserve pProjections here so remote target alignment
        // and SQL rendering can still see expressions such as LOWER(col),
        // while pTargets continues to supply the current-scope output aliases.
        int32_t outputIdx = 0;
        code = fqAppendCollectedExprList(pProject->pProjections, pQueryRoot->pTargets,
                                         &outputIdx, &pOutputs->pSelectExprs, "wrapper-project");
      } else {
        code = nodesCloneList(pQueryRoot->pTargets, &pOutputs->pSelectExprs);
      }
    } else {
      code = nodesCloneList(pQueryRoot->pTargets, &pOutputs->pSelectExprs);
    }

    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: wrapper output collection failed root=%p rootType=%s targets=%d code=0x%x",
             pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
             LIST_LENGTH(pQueryRoot->pTargets), code);
      nodesDestroyRemoteSelectOutputs(pOutputs);
      return code;
    }

    code = nodesCloneList(pQueryRoot->pTargets, &pOutputs->pRefExprs);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: wrapper ref output clone failed root=%p rootType=%s targets=%d code=0x%x",
             pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
             LIST_LENGTH(pQueryRoot->pTargets), code);
      nodesDestroyRemoteSelectOutputs(pOutputs);
      return code;
    }

    qDebug("FQ-SELECT: collect outputs keep wrapper root expressions root=%p rootType=%s outputs=%d",
           pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
           pOutputs->pSelectExprs ? LIST_LENGTH(pOutputs->pSelectExprs) : 0);
    fqDebugLogExprList("collector.wrapperOutputs", pOutputs->pSelectExprs);
    fqDebugLogExprList("collector.wrapperRefOutputs", pOutputs->pRefExprs);
    return TSDB_CODE_SUCCESS;
  }

  SFqQueryGroup group;
  fqInitQueryGroup(&group);
  bool haveGroup = false;
  int32_t code = TSDB_CODE_SUCCESS;
  if (nodeType((SNode*)pQueryRoot) == QUERY_NODE_LOGIC_PLAN_JOIN) {
  } else if (!(nodeType((SNode*)pQueryRoot) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
               pQueryRoot->pChildren != NULL && LIST_LENGTH(pQueryRoot->pChildren) > 1)) {
    code = fqCollectQueryGroup(pQueryRoot, &group, "collector");
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    haveGroup = true;
  }

  fqResolveRemoteOutputShape(pQueryRoot, haveGroup ? &group : NULL, &pOutputs->source, &pOutputs->keyExprCount);

  if ((nodeType((SNode*)pQueryRoot) == QUERY_NODE_LOGIC_PLAN_JOIN ||
       (nodeType((SNode*)pQueryRoot) == QUERY_NODE_LOGIC_PLAN_PROJECT &&
        pQueryRoot->pChildren != NULL && LIST_LENGTH(pQueryRoot->pChildren) > 1)) &&
      pQueryRoot->pTargets != NULL && LIST_LENGTH(pQueryRoot->pTargets) > 0) {
    code = nodesCloneList(pQueryRoot->pTargets, &pOutputs->pSelectExprs);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: clone root targets failed root=%p rootType=%s source=%s targets=%d code=0x%x",
             pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
             remoteSelectSourceName(pOutputs->source), LIST_LENGTH(pQueryRoot->pTargets), code);
      return code;
    }

    code = nodesCloneList(pQueryRoot->pTargets, &pOutputs->pRefExprs);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: clone root ref targets failed root=%p rootType=%s source=%s targets=%d code=0x%x",
             pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
             remoteSelectSourceName(pOutputs->source), LIST_LENGTH(pQueryRoot->pTargets), code);
      nodesDestroyRemoteSelectOutputs(pOutputs);
      return code;
    }

    qDebug("FQ-SELECT: collect outputs use root targets root=%p rootType=%s source=%s keyExprs=%d outputs=%d",
           pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
           remoteSelectSourceName(pOutputs->source), pOutputs->keyExprCount,
           pOutputs->pSelectExprs ? LIST_LENGTH(pOutputs->pSelectExprs) : 0);
    fqDebugLogExprList("collector.rootRefOutputs", pOutputs->pRefExprs);
    return TSDB_CODE_SUCCESS;
  }

  qDebug("FQ-SELECT: use structural collection root=%p rootType=%s source=%s",
         pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
         remoteSelectSourceName(pOutputs->source));

  if (group.pProject != NULL) {
    fqDebugLogExprList("collector.group.projectProjections", group.pProject->pProjections);
    fqDebugLogExprList("collector.group.projectTargets", group.pProject->node.pTargets);
  }
  if (group.pAgg != NULL) {
    fqDebugLogExprList("collector.group.aggGroupKeys", group.pAgg->pGroupKeys);
    fqDebugLogExprList("collector.group.aggFuncs", group.pAgg->pAggFuncs);
    fqDebugLogExprList("collector.group.aggTargets", group.pAgg->node.pTargets);
  }
  if (group.pPartition != NULL) {
    fqDebugLogExprList("collector.group.partitionKeys", group.pPartition->pPartitionKeys);
    fqDebugLogExprList("collector.group.partitionFuncs", group.pPartition->pAggFuncs);
    fqDebugLogExprList("collector.group.partitionTargets", group.pPartition->node.pTargets);
  }
  if (group.pIndef != NULL) {
    fqDebugLogExprList("collector.group.indefFuncs", group.pIndef->pFuncs);
  }
  if (group.pScanWhere != NULL) {
    fqDebugLogExprList("collector.group.scanCols", group.pScanWhere->pScanCols);
    fqDebugLogExprList("collector.group.scanTargets", group.pScanWhere->node.pTargets);
  }
  if (group.pFromChild != NULL) {
    fqDebugLogExprList("collector.group.fromChildTargets", group.pFromChild->pTargets);
  }

  int32_t outputIdx = 0;
  if (group.pProject != NULL) {
    if (group.pProjectSelectChild != NULL) {
      qDebug("FQ-SELECT: collect outputs use merged project mapping root=%p project=%p child=%p childType=%s",
             pQueryRoot, group.pProject, group.pProjectSelectChild,
             nodesNodeName(nodeType((SNode*)group.pProjectSelectChild)));
      code = fqCollectMergedProjectSelectOutputs(&group, pOutputs);
    } else {
      pOutputs->source = NODES_REMOTE_SELECT_SOURCE_PROJECT;
      // Project outputs are the final remote SELECT list. Non-column items such
      // as constants must inherit the already-materialized target alias here so
      // downstream slot-key / descriptor rebuild walks the exact remote output
      // list without generating empty-key placeholder slots.
      code = fqAppendCollectedExprList(group.pProject->pProjections, group.pProject->node.pTargets, &outputIdx,
                                       &pOutputs->pSelectExprs, "project");
    }
  } else if (group.pAgg != NULL) {
    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_AGG;
    code = fqAppendCollectedAggKeyAndTailExprs(group.pAgg, &outputIdx, pOutputs);
  } else if (group.pPartition != NULL && group.pPartition->pAggFuncs != NULL &&
             LIST_LENGTH(group.pPartition->pAggFuncs) > 0) {
    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_PARTITION;
    code = fqAppendCollectedPartitionExprs(group.pPartition, &outputIdx, pOutputs);
  } else if (group.pIndef != NULL) {
    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_INDEF;
    code = fqAppendCollectedExprList(group.pIndef->pFuncs, NULL, &outputIdx,
                                     &pOutputs->pSelectExprs, "indef");
  } else if (group.pScanWhere != NULL) {
    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_SCAN;
    code = fqAppendCollectedExprList(group.pScanWhere->pScanCols, NULL, &outputIdx,
                                     &pOutputs->pSelectExprs, "scan");
  } else if (group.pFromChild != NULL && group.pFromChild->pTargets != NULL) {
    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_FROM_CHILD;
    code = nodesCloneList(group.pFromChild->pTargets, &pOutputs->pSelectExprs);
    if (TSDB_CODE_SUCCESS == code) {
      outputIdx = pOutputs->pSelectExprs ? LIST_LENGTH(pOutputs->pSelectExprs) : 0;
    }
  } else {
    pOutputs->source = NODES_REMOTE_SELECT_SOURCE_UNKNOWN;
    qDebug("FQ-SELECT: no explicit select-output source resolved rootType=%s root=%p",
           nodesNodeName(nodeType((SNode*)pQueryRoot)), pQueryRoot);
  }

  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SELECT: collect outputs failed root=%p rootType=%s source=%s code=0x%x",
           pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
           remoteSelectSourceName(pOutputs->source), code);
    nodesDestroyRemoteSelectOutputs(pOutputs);
    return code;
  }

  if (pOutputs->pSelectExprs == NULL || LIST_LENGTH(pOutputs->pSelectExprs) == 0) {
    qError("FQ-SELECT: collect outputs empty root=%p rootType=%s source=%s keyExprs=%d",
           pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
           remoteSelectSourceName(pOutputs->source), pOutputs->keyExprCount);
    nodesDestroyRemoteSelectOutputs(pOutputs);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  const int32_t outputCnt = LIST_LENGTH(pOutputs->pSelectExprs);
  if (NULL == pOutputs->pRefExprs) {
    const bool canReuseRootTargets =
        fqCanReuseRootTargetsAsAlignedRefs(pOutputs->pSelectExprs, pQueryRoot->pTargets);
    const SNodeList* pRefSource = canReuseRootTargets ? pQueryRoot->pTargets : pOutputs->pSelectExprs;
    qDebug("FQ-SELECT: choose fallback ref source root=%p rootType=%s source=%s outputs=%d reuseRootTargets=%d",
           pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
           remoteSelectSourceName(pOutputs->source), outputCnt, (int)canReuseRootTargets);
    code = nodesCloneList(pRefSource, &pOutputs->pRefExprs);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SELECT: clone output refs failed root=%p rootType=%s source=%s outputs=%d code=0x%x",
             pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
             remoteSelectSourceName(pOutputs->source), outputCnt, code);
      nodesDestroyRemoteSelectOutputs(pOutputs);
      return code;
    }
  } else if (LIST_LENGTH(pOutputs->pRefExprs) != outputCnt) {
    qError("FQ-SELECT: aligned output refs count mismatch root=%p rootType=%s source=%s outputs=%d refs=%d",
           pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
           remoteSelectSourceName(pOutputs->source), outputCnt,
           LIST_LENGTH(pOutputs->pRefExprs));
    nodesDestroyRemoteSelectOutputs(pOutputs);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  fqDebugLogExprList("collector.finalOutputs", pOutputs->pSelectExprs);
  fqDebugLogExprList("collector.finalRefOutputs", pOutputs->pRefExprs);

  qDebug("FQ-SELECT: collect outputs root=%p rootType=%s source=%s keyExprs=%d outputs=%d",
         pQueryRoot, nodesNodeName(nodeType((SNode*)pQueryRoot)),
         remoteSelectSourceName(pOutputs->source), pOutputs->keyExprCount,
         pOutputs->pSelectExprs ? LIST_LENGTH(pOutputs->pSelectExprs) : 0);
  return TSDB_CODE_SUCCESS;
}

static int32_t renderNode(const SLogicNode* pNode, EExtSQLDialect dialect,
                          const SNodesRemoteSQLCtx* pCtx, SDynSQL* s);

static void renderTablePathWithAlias(SDynSQL* s, const SExtTableNode* pExt, EExtSQLDialect dialect) {
  if (NULL == s || NULL == pExt) {
    return;
  }

  dynAppendTablePath(s, pExt, dialect);
  if (pExt->table.tableAlias[0] != '\0') {
    dynSQLAppendStr(s, " ");
    dynAppendQuotedId(s, pExt->table.tableAlias, dialect);
  }
}

static int32_t renderUnionBranch(const SProjectLogicNode* pUnion, const SLogicNode* pBranch,
                                 int32_t branchIdx, EExtSQLDialect dialect,
                                 const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  const SNodeList* pUnionExprs = (pUnion->pProjections != NULL) ? pUnion->pProjections : pUnion->node.pTargets;
  const int32_t unionTargetCount = pUnionExprs ? LIST_LENGTH((SNodeList*)pUnionExprs) : 0;
  bool needWrapper = (unionTargetCount > 0);
  SNodesRemoteSelectOutputs branchOutputs = {0};
  SNodeList* pWrapperOutputRefs = NULL;
  int32_t code = nodesCollectRemoteQuerySelectOutputs(pBranch, &branchOutputs);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: UNION branch collect outputs failed branchIdx=%d branchType=%s code=0x%x",
           branchIdx, nodesNodeName(nodeType((SNode*)pBranch)), code);
    return code;
  }

  if (0 == unionTargetCount) {
    needWrapper = false;
  }

  if (needWrapper &&
      fqCanPassThroughChildOutputs(pUnionExprs, branchOutputs.pSelectExprs,
                                   branchOutputs.pRefExprs, "union-branch")) {
    qDebug("FQ-SQL: UNION branch passthrough branchIdx=%d branchType=%s targetCnt=%d outputCnt=%d",
           branchIdx, nodesNodeName(nodeType((SNode*)pBranch)), unionTargetCount,
           branchOutputs.pSelectExprs ? LIST_LENGTH(branchOutputs.pSelectExprs) : 0);
    needWrapper = false;
  }

  qDebug("FQ-SQL: UNION branch plan branchIdx=%d branchType=%s rootTargets=%d branchOutputs=%d wrapped=%c sort=%c limit=%c",
         branchIdx, nodesNodeName(nodeType((SNode*)pBranch)), unionTargetCount,
         branchOutputs.pSelectExprs ? LIST_LENGTH(branchOutputs.pSelectExprs) : 0,
         needWrapper ? 'Y' : 'N',
         nodeType((SNode*)pBranch) == QUERY_NODE_LOGIC_PLAN_SORT ? 'Y' : 'N',
         pBranch->pLimit ? 'Y' : 'N');
  fqDebugLogExprList("render.union.branch.outputs", branchOutputs.pSelectExprs);

  if (!needWrapper) {
    code = renderNode(pBranch, dialect, pCtx, s);
    nodesDestroyRemoteSelectOutputs(&branchOutputs);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SQL: UNION branch raw render failed branchIdx=%d branchType=%s code=0x%x",
             branchIdx, nodesNodeName(nodeType((SNode*)pBranch)), code);
      return code;
    }
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  SNodesRemoteSQLCtx branchCtxStore = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  branchCtxStore.pOutputSelectExprs = branchOutputs.pSelectExprs;
  branchCtxStore.pInputSelectExprs = branchOutputs.pSelectExprs;
  branchCtxStore.pInputRefExprs = branchOutputs.pRefExprs;
  branchCtxStore.inputSelectExprsFromChildSource = true;
  // The per-branch wrapper feeds the UNION schema, not the user-visible final
  // output. Keep branch wrapper outputs on internal slot names so all branches
  // expose the same stable column namespace.
  branchCtxStore.emitInternalAlias = true;
  const SNodesRemoteSQLCtx* branchCtx = &branchCtxStore;
  fqDebugLogRenderCtx("union-branch-wrapper", NULL, branchCtx);

  code = fqBuildWrapperOutputRefs(pUnionExprs, "union-branch-wrapper", branchCtx,
                                  &pWrapperOutputRefs);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: UNION branch wrapper refs build failed branchIdx=%d branchType=%s code=0x%x",
           branchIdx, nodesNodeName(nodeType((SNode*)pBranch)), code);
    goto _return;
  }

  dynSQLAppendStr(s, "SELECT ");
  renderSelectList(s, pWrapperOutputRefs, dialect, NULL, branchCtx);
  if (s->err) {
    code = s->err;
    qError("FQ-SQL: UNION branch outer select render failed branchIdx=%d code=0x%x",
           branchIdx, code);
    goto _return;
  }

  dynSQLAppendStr(s, " FROM (");
  code = renderNode(pBranch, dialect, branchCtx, s);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: UNION branch inner render failed branchIdx=%d branchType=%s code=0x%x",
           branchIdx, nodesNodeName(nodeType((SNode*)pBranch)), code);
    goto _return;
  }
  dynSQLAppendf(s, ") _fq_set_%d", branchIdx);

  code = s->err ? s->err : TSDB_CODE_SUCCESS;

_return:
  nodesDestroyList(pWrapperOutputRefs);
  nodesDestroyRemoteSelectOutputs(&branchOutputs);
  return code;
}

// Render a UNION / UNION ALL set-op (multi-child isSetOpProj PROJECT).
static int32_t renderUnion(const SProjectLogicNode* pUnion, EExtSQLDialect dialect,
                           const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  const char* pKw = pUnion->isUnionDedup ? " UNION " : " UNION ALL ";
  bool first = true;
  int32_t branchIdx = 0;
  SNode* pBranch = NULL;
  FOREACH(pBranch, pUnion->node.pChildren) {
    if (!first) dynSQLAppendStr(s, pKw);
    int32_t code = renderUnionBranch(pUnion, (const SLogicNode*)pBranch, branchIdx, dialect, pCtx, s);
    if (code) {
      qError("FQ-SQL: UNION%s branch failed branchIdx=%d code=0x%x",
             pUnion->isUnionDedup ? "" : " ALL", branchIdx, code);
      return code;
    }
    first = false;
    ++branchIdx;
  }
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t renderSetOpStatement(const SProjectLogicNode* pUnion, EExtSQLDialect dialect,
                                    const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  if (NULL == pUnion) {
    qError("FQ-SQL: renderSetOpStatement got NULL union");
    return TSDB_CODE_INVALID_PARA;
  }

  const SNodeList* pUnionExprs = (pUnion->pProjections != NULL) ? pUnion->pProjections : pUnion->node.pTargets;
  bool needWrapper = (NULL != pUnion->node.pConditions);
  SNodesRemoteSQLCtx stmtCtxStore = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  stmtCtxStore.pOutputSelectExprs = pUnion->node.pTargets;
  const SNodesRemoteSQLCtx* stmtCtx = &stmtCtxStore;
  qDebug("FQ-SQL: render setop statement union=%p dedup=%d needWrapper=%d hasCond=%d hasLimit=%d targets=%d children=%d",
         pUnion, (int)pUnion->isUnionDedup, (int)needWrapper,
         NULL != pUnion->node.pConditions, NULL != pUnion->node.pLimit,
         pUnion->node.pTargets ? LIST_LENGTH(pUnion->node.pTargets) : 0,
         pUnion->node.pChildren ? LIST_LENGTH(pUnion->node.pChildren) : 0);
  fqDebugLogExprList("render.setop.targets", pUnion->node.pTargets);
  fqDebugLogRenderCtx("setop", NULL, pCtx);

  if (!needWrapper) {
    int32_t code = renderUnion(pUnion, dialect, pCtx, s);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (pUnion->node.pLimit != NULL) {
      qDebug("FQ-SQL: render setop root LIMIT directly union=%p", pUnion);
      renderLimit(s, (const SLimitNode*)pUnion->node.pLimit);
    }
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  dynSQLAppendStr(s, "SELECT ");
  renderSelectList(s, pUnionExprs, dialect, NULL, stmtCtx);
  if (s->err) {
    return s->err;
  }

  dynSQLAppendStr(s, " FROM (");
  int32_t code = renderUnion(pUnion, dialect, pCtx, s);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  dynSQLAppendStr(s, ") _sq");

  qDebug("FQ-SQL: render setop outer WHERE union=%p hasCond=%d", pUnion, NULL != pUnion->node.pConditions);
  renderWhere(s, pUnion->node.pConditions, dialect, NULL, stmtCtx);
  if (pUnion->node.pLimit != NULL) {
    qDebug("FQ-SQL: render setop outer LIMIT union=%p", pUnion);
    renderLimit(s, (const SLimitNode*)pUnion->node.pLimit);
  }

  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

static int32_t renderNodeWithOuterWhereWrapper(const SLogicNode* pNode, EExtSQLDialect dialect,
                                               const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  if (NULL == pNode || NULL == pNode->pConditions) {
    qError("FQ-SQL: renderNodeWithOuterWhereWrapper invalid input node=%p", pNode);
    return TSDB_CODE_INVALID_PARA;
  }
  if (NULL == pNode->pTargets) {
    qError("FQ-SQL: node=%s requires outer-WHERE wrapper but pTargets is NULL node=%p",
           nodesNodeName(nodeType(pNode)), pNode);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SLogicNode* pInnerNode = NULL;
  int32_t code = nodesCloneNode((SNode*)pNode, (SNode**)&pInnerNode);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: clone node for outer-WHERE wrapper failed type=%s node=%p code=0x%x",
           nodesNodeName(nodeType(pNode)), pNode, code);
    return code;
  }

  if (pInnerNode->pConditions != NULL) {
    nodesDestroyNode(pInnerNode->pConditions);
    pInnerNode->pConditions = NULL;
  }

  qDebug("FQ-SQL: render outer-WHERE wrapper type=%s node=%p innerClone=%p hasLimit=%d targets=%d",
         nodesNodeName(nodeType(pNode)), pNode, pInnerNode,
         NULL != pNode->pLimit, pNode->pTargets ? LIST_LENGTH(pNode->pTargets) : 0);
  fqDebugLogExprList("render.wrapper.targets", pNode->pTargets);
  fqDebugLogRenderCtx("outer-where-wrapper", NULL, pCtx);

  SNodesRemoteSelectOutputs outputs = {0};
  code = nodesCollectRemoteQuerySelectOutputs(pNode, &outputs);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: outer-WHERE wrapper collect outputs failed type=%s node=%p code=0x%x",
           nodesNodeName(nodeType(pNode)), pNode, code);
    goto _return;
  }
  fqDebugLogExprList("render.wrapper.outputs", outputs.pSelectExprs);

  SNodesRemoteSQLCtx wrapperCtxStore = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  wrapperCtxStore.pOutputSelectExprs = outputs.pSelectExprs;
  wrapperCtxStore.pInputSelectExprs = outputs.pSelectExprs;
  wrapperCtxStore.pInputRefExprs = outputs.pRefExprs;
  wrapperCtxStore.inputSelectExprsFromChildSource = true;
  const SNodesRemoteSQLCtx* wrapperCtx = &wrapperCtxStore;

  SNodeList* pWrapperOutputRefs = NULL;
  code = fqBuildWrapperOutputRefs(outputs.pSelectExprs, "wrapper-render", pCtx, &pWrapperOutputRefs);
  if (TSDB_CODE_SUCCESS != code) {
    goto _return;
  }

  dynSQLAppendStr(s, "SELECT ");
  renderSelectList(s, pWrapperOutputRefs, dialect, NULL, wrapperCtx);
  if (s->err) {
    code = s->err;
    goto _return;
  }

  dynSQLAppendStr(s, " FROM (");
  SNodesRemoteSQLCtx innerCtxStore = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  innerCtxStore.emitInternalAlias = true;
  code = renderNode(pInnerNode, dialect, &innerCtxStore, s);
  if (TSDB_CODE_SUCCESS != code) {
    goto _return;
  }
  dynSQLAppendStr(s, ") _sq");

  renderWhere(s, pNode->pConditions, dialect, NULL, wrapperCtx);
  if (s->err) {
    code = s->err;
    goto _return;
  }

  code = TSDB_CODE_SUCCESS;

_return:
  nodesDestroyList(pWrapperOutputRefs);
  nodesDestroyRemoteSelectOutputs(&outputs);
  nodesDestroyNode((SNode*)pInnerNode);
  return code;
}

// Render a JOIN as a FROM-source (INNER/LEFT/RIGHT/FULL ... ON; SEMI/ANTI → EXISTS).
// Emits a complete SELECT (it is its own statement / FROM-source).
static int32_t renderJoin(const SJoinLogicNode* pJoin, EExtSQLDialect dialect,
                          const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  const SLogicNode* pNode = (const SLogicNode*)pJoin;
  if (!pNode->pChildren || LIST_LENGTH(pNode->pChildren) != 2) {
    qError("FQ-SQL: JOIN node has %d children (expected 2)",
           pNode->pChildren ? (int)LIST_LENGTH(pNode->pChildren) : 0);
    return TSDB_CODE_EXT_SQL_RENDER_ERROR;
  }
  const SLogicNode* pLeft  = (const SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  const SLogicNode* pRight = (const SLogicNode*)nodesListGetNode(pNode->pChildren, 1);
  if (NULL == pLeft || NULL == pRight) {
    qError("FQ-SQL: JOIN child missing left=%p right=%p join=%p", pLeft, pRight, pJoin);
    return TSDB_CODE_EXT_SQL_RENDER_ERROR;
  }

  qDebug("FQ-SQL: render JOIN join=%p joinType=%d subType=%d leftType=%s rightType=%s hasFullOn=%d hasColEq=%d hasJoinCond=%d",
         pJoin, (int)pJoin->joinType, (int)pJoin->subType,
         nodesNodeName(nodeType((SNode*)pLeft)), nodesNodeName(nodeType((SNode*)pRight)),
         NULL != pJoin->pFullOnCond, NULL != pJoin->pColEqCond, NULL != pJoin->node.pConditions);

  if (pJoin->subType == JOIN_STYPE_SEMI || pJoin->subType == JOIN_STYPE_ANTI) {
    if (nodeType((SNode*)pLeft) != QUERY_NODE_LOGIC_PLAN_SCAN ||
        nodeType((SNode*)pRight) != QUERY_NODE_LOGIC_PLAN_SCAN) {
      qError("FQ-SQL: SEMI/ANTI JOIN expects scan children leftType=%s rightType=%s join=%p",
             nodesNodeName(nodeType((SNode*)pLeft)), nodesNodeName(nodeType((SNode*)pRight)), pJoin);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }

    const SScanLogicNode* pLeftScan = (const SScanLogicNode*)pLeft;
    const SScanLogicNode* pRightScan = (const SScanLogicNode*)pRight;
    if (!pLeftScan->pExtTableNode || !pRightScan->pExtTableNode) {
      qError("FQ-SQL: SEMI/ANTI JOIN scan missing pExtTable left=%p right=%p",
             pLeftScan, pRightScan);
      return TSDB_CODE_EXT_SQL_RENDER_ERROR;
    }
    const SExtTableNode* pLeftExt  = (const SExtTableNode*)pLeftScan->pExtTableNode;
    const SExtTableNode* pRightExt = (const SExtTableNode*)pRightScan->pExtTableNode;
    const SExtTableMeta* pLeftMeta  = pLeftExt->pExtMeta;
    const SExtTableMeta* pRightMeta = pRightExt->pExtMeta;
    const bool isRightVisible = (pJoin->joinType == JOIN_TYPE_RIGHT);
    const SScanLogicNode* pOuterScan = isRightVisible ? pRightScan : pLeftScan;
    const SScanLogicNode* pInnerScan = isRightVisible ? pLeftScan : pRightScan;
    const SExtTableNode*  pOuterExt = isRightVisible ? pRightExt : pLeftExt;
    const SExtTableNode*  pInnerExt = isRightVisible ? pLeftExt : pRightExt;
    const SExtTableMeta*  pOuterMeta = isRightVisible ? pRightMeta : pLeftMeta;
    const SExtTableMeta*  pInnerMeta = isRightVisible ? pLeftMeta : pRightMeta;
    SNodesRemoteSelectOutputs outputs = {0};
    int32_t code = nodesCollectRemoteQuerySelectOutputs(pNode, &outputs);
    if (code != TSDB_CODE_SUCCESS) {
      qError("FQ-SQL: SEMI/ANTI JOIN collect outputs failed joinType=%d subType=%d code=0x%x",
             (int)pJoin->joinType, (int)pJoin->subType, code);
      return code;
    }

    const char* existsKw = (pJoin->subType == JOIN_STYPE_SEMI) ? "EXISTS" : "NOT EXISTS";
    SNodesRemoteSQLCtx semiCtx = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
    semiCtx.useJoinLegAlias = true;
    qDebug("FQ-SQL: render SEMI/ANTI JOIN joinType=%d subType=%d visibleSide=%s outputs=%d outerAlias=%s innerAlias=%s outerCond=%c innerCond=%c joinCond=%c",
           (int)pJoin->joinType, (int)pJoin->subType, isRightVisible ? "right" : "left",
           outputs.pSelectExprs ? (int)LIST_LENGTH(outputs.pSelectExprs) : 0,
           pOuterExt->table.tableAlias, pInnerExt->table.tableAlias,
           pOuterScan->node.pConditions ? 'Y' : 'N',
           pInnerScan->node.pConditions ? 'Y' : 'N',
           pJoin->node.pConditions ? 'Y' : 'N');

    dynSQLAppendStr(s, "SELECT ");
    renderSelectList(s, outputs.pSelectExprs, dialect, pOuterMeta, &semiCtx);
    if (s->err) {
      int32_t err = s->err;
      nodesDestroyRemoteSelectOutputs(&outputs);
      return err;
    }

    dynSQLAppendStr(s, " FROM ");
    renderTablePathWithAlias(s, pOuterExt, dialect);

    bool wroteOuterWhere = false;
    if (pOuterScan->node.pConditions != NULL) {
      dynSQLAppendStr(s, " WHERE (");
      code = dynAppendExpr(s, pOuterScan->node.pConditions, dialect, pOuterMeta, &semiCtx);
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyRemoteSelectOutputs(&outputs);
        return code;
      }
      dynSQLAppendStr(s, ")");
      wroteOuterWhere = true;
    }

    dynSQLAppendStr(s, wroteOuterWhere ? " AND " : " WHERE ");
    dynSQLAppendStr(s, existsKw);
    dynSQLAppendStr(s, " (SELECT 1 FROM ");
    renderTablePathWithAlias(s, pInnerExt, dialect);

    const SNode* pOnCond = pJoin->pFullOnCond ? pJoin->pFullOnCond
                         : (pJoin->pColEqCond ? pJoin->pColEqCond : NULL);
    const SNode* pInnerConds[3] = {pOnCond, pInnerScan->node.pConditions, pJoin->node.pConditions};
    const SExtTableMeta* pInnerMetas[3] = {NULL, pInnerMeta, NULL};
    bool wroteInnerWhere = false;
    for (int32_t i = 0; i < 3; ++i) {
      if (pInnerConds[i] == NULL) {
        continue;
      }

      dynSQLAppendStr(s, wroteInnerWhere ? " AND (" : " WHERE (");
      code = dynAppendExpr(s, pInnerConds[i], dialect, pInnerMetas[i], &semiCtx);
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyRemoteSelectOutputs(&outputs);
        return code;
      }
      dynSQLAppendStr(s, ")");
      wroteInnerWhere = true;
    }
    dynSQLAppendStr(s, ")");
    nodesDestroyRemoteSelectOutputs(&outputs);
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }

  // Standard JOIN: render ONLY the FROM clause "t1 a1 [type] JOIN t2 a2 ON cond".
  // The SELECT list, WHERE and trailing clauses belong to the enclosing query
  // group (renderNode step 2), which renders them with useJoinLegAlias so the
  // two legs' columns are unambiguous.  This keeps Project→Join / Agg→Join in a
  // SINGLE SELECT (no subquery) per the combination model.
  SNodesRemoteSQLCtx joinCtx = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  joinCtx.useJoinLegAlias = true;

  int32_t code = renderJoinSource(pLeft, dialect, pCtx, s, nodeType((SNode*)pLeft) == QUERY_NODE_LOGIC_PLAN_JOIN);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: render JOIN left source failed join=%p code=0x%x", pJoin, code);
    return code;
  }
  switch (pJoin->joinType) {
    case JOIN_TYPE_LEFT:  dynSQLAppendStr(s, " LEFT JOIN ");  break;
    case JOIN_TYPE_RIGHT: dynSQLAppendStr(s, " RIGHT JOIN "); break;
    case JOIN_TYPE_FULL:  dynSQLAppendStr(s, " FULL JOIN ");  break;
    default:              dynSQLAppendStr(s, " INNER JOIN "); break;
  }
  code = renderJoinSource(pRight, dialect, pCtx, s, nodeType((SNode*)pRight) == QUERY_NODE_LOGIC_PLAN_JOIN);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: render JOIN right source failed join=%p code=0x%x", pJoin, code);
    return code;
  }
  const SNode* pOnCond = pJoin->pFullOnCond ? pJoin->pFullOnCond
                       : (pJoin->pColEqCond ? pJoin->pColEqCond : NULL);
  if (pOnCond) {
    qDebug("FQ-SQL: render JOIN ON join=%p condType=%s", pJoin, nodesNodeName(nodeType((SNode*)pOnCond)));
    dynSQLAppendStr(s, " ON ");
    code = dynAppendExpr(s, pOnCond, dialect, NULL, &joinCtx);
    if (TSDB_CODE_SUCCESS != code) {
      qError("FQ-SQL: render JOIN ON failed join=%p code=0x%x", pJoin, code);
      return code;
    }
  }
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}

// Render the JOIN legs' pushed WHERE conditions (combined) into the enclosing
// query group's WHERE clause.  Returns true if anything was written.
static bool renderJoinLegWhere(const SJoinLogicNode* pJoin, EExtSQLDialect dialect,
                               const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  SNodesRemoteSQLCtx joinCtx = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  joinCtx.useJoinLegAlias = true;
  bool wroteAny = false;
  int32_t code = renderJoinTreeWhere((const SLogicNode*)pJoin, dialect, &joinCtx, s, &wroteAny);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FQ-SQL: renderJoinLegWhere failed join=%p code=0x%x", pJoin, code);
    return false;
  }
  return wroteAny;
}

// renderNode — combination-driven: collect a query group top-down, emit one SELECT.
static int32_t renderNode(const SLogicNode* pNode, EExtSQLDialect dialect,
                          const SNodesRemoteSQLCtx* pCtx, SDynSQL* s) {
  if (!pNode) { qError("FQ-SQL: renderNode NULL"); return TSDB_CODE_INVALID_PARA; }

  // FROM-source nodes are their own statements.
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    const SJoinLogicNode* pJoin = (const SJoinLogicNode*)pNode;
    if (pJoin->subType == JOIN_STYPE_SEMI || pJoin->subType == JOIN_STYPE_ANTI) {
      qDebug("FQ-SQL-RENDER: node=%s joinSubType=%d → SEMI/ANTI JOIN own statement",
             nodesNodeName(nodeType(pNode)), (int)pJoin->subType);
      int32_t code = renderJoin(pJoin, dialect, pCtx, s);
      if (code) return code;
      if (pNode->pLimit) renderLimit(s, (const SLimitNode*)pNode->pLimit);
      return s->err ? s->err : TSDB_CODE_SUCCESS;
    }
    qDebug("FQ-SQL-RENDER: node=%s joinSubType=%d → standard JOIN uses shared collector path",
           nodesNodeName(nodeType(pNode)), (int)pJoin->subType);
  }
  if (nodeType(pNode) == QUERY_NODE_LOGIC_PLAN_PROJECT && pNode->pChildren != NULL &&
      LIST_LENGTH(pNode->pChildren) > 1) {
    qDebug("FQ-SQL-RENDER: node=%s nChildren=%d → UNION set-op (own statement)",
           nodesNodeName(nodeType(pNode)), (int)LIST_LENGTH(pNode->pChildren));
    int32_t code = renderSetOpStatement((const SProjectLogicNode*)pNode, dialect, pCtx, s);
    if (code) return code;
    return s->err ? s->err : TSDB_CODE_SUCCESS;
  }
  if (fqNodeRequiresWhereWrapper(pNode)) {
    qDebug("FQ-SQL-RENDER: node=%s has post-node WHERE → outer-WHERE wrapper statement",
           nodesNodeName(nodeType(pNode)));
    return renderNodeWithOuterWhereWrapper(pNode, dialect, pCtx, s);
  }

  SFqQueryGroup g;
  int32_t code = fqCollectQueryGroup(pNode, &g, "render");
  if (code) {
    return code;
  }

  SNodesRemoteSelectOutputs outputs = {0};
  code = nodesCollectRemoteQuerySelectOutputs(pNode, &outputs);
  if (code) {
    return code;
  }
  SNodesRemoteSelectOutputs inputOutputs = {0};
  bool inputSelectExprsFromChildSource = false;
  if (g.pFromChild != NULL) {
    code = nodesCollectRemoteQuerySelectOutputs(g.pFromChild, &inputOutputs);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&outputs);
      qError("FQ-SQL-RENDER: collect from-child input outputs failed root=%p rootType=%s child=%p childType=%s code=0x%x",
             pNode, nodesNodeName(nodeType(pNode)), g.pFromChild,
             nodesNodeName(nodeType((SNode*)g.pFromChild)), code);
      return code;
    }
    qDebug("FQ-SQL-RENDER: collected from-child input outputs root=%p child=%p childType=%s outputs=%d",
           pNode, g.pFromChild, nodesNodeName(nodeType((SNode*)g.pFromChild)),
           inputOutputs.pSelectExprs ? LIST_LENGTH(inputOutputs.pSelectExprs) : 0);
    fqDebugLogExprList("render.fromChild.inputOutputs", inputOutputs.pSelectExprs);
    inputSelectExprsFromChildSource = true;
  } else if (g.pProjectSelectChild != NULL) {
    code = nodesCollectRemoteQuerySelectOutputs(g.pProjectSelectChild, &inputOutputs);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&outputs);
      qError("FQ-SQL-RENDER: collect merged-child input outputs failed root=%p rootType=%s child=%p childType=%s code=0x%x",
             pNode, nodesNodeName(nodeType(pNode)), g.pProjectSelectChild,
             nodesNodeName(nodeType((SNode*)g.pProjectSelectChild)), code);
      return code;
    }
    qDebug("FQ-SQL-RENDER: collected merged-child input outputs root=%p child=%p childType=%s outputs=%d",
           pNode, g.pProjectSelectChild,
           nodesNodeName(nodeType((SNode*)g.pProjectSelectChild)),
           inputOutputs.pSelectExprs ? LIST_LENGTH(inputOutputs.pSelectExprs) : 0);
    fqDebugLogExprList("render.mergedChild.inputOutputs", inputOutputs.pSelectExprs);
  }
  qDebug("FQ-SQL-RENDER: shared select outputs root=%p rootType=%s source=%s keyExprs=%d outputs=%d",
         pNode, nodesNodeName(nodeType(pNode)), remoteSelectSourceName(outputs.source),
         outputs.keyExprCount, outputs.pSelectExprs ? LIST_LENGTH(outputs.pSelectExprs) : 0);
  fqDebugLogExprList("render.sharedOutputs", outputs.pSelectExprs);

  const SScanLogicNode* pLeaf = findLeafScan(pNode);
  const SExtTableMeta* pMeta = (pLeaf && pLeaf->pExtTableNode)
      ? ((const SExtTableNode*)pLeaf->pExtTableNode)->pExtMeta : NULL;
  fqDebugLogRenderCtx("render-node-input", pMeta, pCtx);

  // Outer-clause naming namespace: when FROM is a real single table, outer
  // SELECT/GROUP BY/HAVING/ORDER BY columns resolve against that table's remote
  // schema (pMeta → remote column-name mapping).  But when FROM is a SUBQUERY
  // (g.pFromChild), a JOIN, or a UNION, the outer clauses reference the current
  // statement's SELECT-list outputs.  Those output references must resolve to
  // the final emitted remote SELECT aliases (normally internal aliasName / slot
  // name), not to base-table remote names.
  bool isDirectTableFrom = (g.pFromChild == NULL && g.pFromSource != NULL &&
                            nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_SCAN);
  const SExtTableMeta* pOuterMeta = isDirectTableFrom ? pMeta : NULL;

  // Inline-JOIN FROM (standard JOIN, not SEMI/ANTI): outer SELECT/WHERE/ORDER
  // columns belong to the two legs and must be prefixed with the leg alias
  // (s./r.) and resolved to each leg's remote column name.  Use a ctx with
  // useJoinLegAlias enabled, and the leaf scan's pMeta for remote-name mapping.
  bool joinInlineFrom = (g.pFromChild == NULL && g.pFromSource != NULL &&
                         nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_JOIN &&
                         ((const SJoinLogicNode*)g.pFromSource)->subType != JOIN_STYPE_SEMI &&
                         ((const SJoinLogicNode*)g.pFromSource)->subType != JOIN_STYPE_ANTI);
  SNodesRemoteSQLCtx selCtxStore = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
  selCtxStore.pOutputSelectExprs = outputs.pSelectExprs;
  selCtxStore.pInputSelectExprs = inputOutputs.pSelectExprs;
  selCtxStore.pInputRefExprs = inputOutputs.pRefExprs;
  selCtxStore.inputSelectExprsFromChildSource = inputSelectExprsFromChildSource;
  const SNodesRemoteSQLCtx* selCtx = &selCtxStore;
  const SExtTableMeta*      selMeta = pOuterMeta;
  if (joinInlineFrom) {
    selCtxStore.useJoinLegAlias = true;
    selMeta = NULL;  // leg columns resolve via their own colName + leg alias
  }

  qDebug("FQ-SQL-RENDER: group routes rootType=%s directTable=%d joinInline=%d scanWhere=%p scanHasCond=%d whereNode=%p whereHasCond=%d limit=%p fromChild=%p fromSource=%p fromSourceType=%s outerMeta=%p selMeta=%p",
         nodesNodeName(nodeType(pNode)), (int)isDirectTableFrom, (int)joinInlineFrom,
         g.pScanWhere, (NULL != g.pScanWhere && NULL != g.pScanWhere->node.pConditions),
         g.pWhereNode, (NULL != g.pWhereNode && NULL != g.pWhereNode->pConditions),
         g.pLimit,
         g.pFromChild, g.pFromSource,
         g.pFromSource ? nodesNodeName(nodeType((SNode*)g.pFromSource)) : "(null)",
         pOuterMeta, selMeta);
  fqDebugLogRenderCtx("render-node-select", selMeta, selCtx);

  bool passThroughChildOutputs =
      (g.pFromChild != NULL && g.pAgg != NULL && g.pAgg->node.pConditions == NULL &&
       // The shared AGG node keeps 3.0 target order. Federated passthrough must
       // therefore compare and render against the collector's final output list,
       // which already reorders/aligned aliases to the actual remote SELECT
       // shape for this wrapper.
       fqCanPassThroughChildOutputs(outputs.pSelectExprs, inputOutputs.pSelectExprs,
                                    inputOutputs.pRefExprs,
                                    "agg-from-child"));
  if (passThroughChildOutputs) {
    qDebug("FQ-SQL-RENDER: pass through child outputs for agg wrapper root=%p rootType=%s child=%p childType=%s",
           pNode, nodesNodeName(nodeType(pNode)), g.pFromChild,
           nodesNodeName(nodeType((SNode*)g.pFromChild)));
  }
  bool passThroughProjectFromChildOutputs =
      (g.pFromChild != NULL && g.pProject != NULL &&
       fqCanPassThroughChildOutputs(g.pProject->node.pTargets, inputOutputs.pSelectExprs,
                                    inputOutputs.pRefExprs,
                                    "project-from-child"));
  if (passThroughProjectFromChildOutputs) {
    qDebug("FQ-SQL-RENDER: pass through child outputs for project wrapper root=%p rootType=%s child=%p childType=%s",
           pNode, nodesNodeName(nodeType(pNode)), g.pFromChild,
           nodesNodeName(nodeType((SNode*)g.pFromChild)));
  }

  // ── Step 2: render one SELECT from the collected group ──
  dynSQLAppendStr(s, "SELECT ");

  bool distinctOnlyAgg = fqAggIsDistinctOnly(g.pAgg);
  if (passThroughChildOutputs) {
    qDebug("FQ-SQL-RENDER: agg wrapper select list uses collected outputs as child output refs root=%p agg=%p child=%p",
           pNode, g.pAgg, g.pFromChild);
    renderSelectList(s, outputs.pSelectExprs, dialect, selMeta, selCtx);
  } else if (passThroughProjectFromChildOutputs) {
    qDebug("FQ-SQL-RENDER: project wrapper select list uses project targets as child output refs root=%p project=%p child=%p",
           pNode, g.pProject, g.pFromChild);
    renderSelectList(s, g.pProject->node.pTargets, dialect, selMeta, selCtx);
  } else if (outputs.source == NODES_REMOTE_SELECT_SOURCE_AGG) {
    bool isDistinct = distinctOnlyAgg;
    if (isDistinct) dynSQLAppendStr(s, "DISTINCT ");
    code = renderAggSelectList(s, &outputs, isDistinct, dialect, selMeta, selCtx);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
  } else if (outputs.source == NODES_REMOTE_SELECT_SOURCE_PARTITION) {
    bool wroteAny = false;
    code = renderCollectedKeyExprs(s, outputs.pSelectExprs, outputs.keyExprCount,
                                   dialect, selMeta, selCtx, /*withAlias=*/true);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
    if (outputs.keyExprCount > 0) {
      wroteAny = true;
    }
    code = renderCollectedAggTailExprs(s, outputs.pSelectExprs, outputs.keyExprCount,
                                       dialect, selMeta, selCtx, &wroteAny);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
    if (!wroteAny) {
      dynSQLAppendChar(s, '*');
    }
  } else if (g.pScanWhere != NULL) {
    if (distinctOnlyAgg) {
      qDebug("FQ-SQL-RENDER: prepend DISTINCT for project-over-distinct-agg root=%p outputs=%d",
             pNode, outputs.pSelectExprs ? LIST_LENGTH(outputs.pSelectExprs) : 0);
      dynSQLAppendStr(s, "DISTINCT ");
    }
    renderSelectList(s, outputs.pSelectExprs, dialect, pMeta, selCtx);
  } else {
    if (distinctOnlyAgg) {
      qDebug("FQ-SQL-RENDER: prepend DISTINCT for grouped outer select root=%p outputs=%d",
             pNode, outputs.pSelectExprs ? LIST_LENGTH(outputs.pSelectExprs) : 0);
      dynSQLAppendStr(s, "DISTINCT ");
    }
    renderSelectList(s, outputs.pSelectExprs, dialect, selMeta, selCtx);
  }

  // FROM: a real table/join (FROM-source) or a subquery (conflict child).
  // A standard JOIN renders INLINE ("t1 a1 JOIN t2 a2 ON cond") so Project→Join /
  // Agg→Join stay one SELECT; SEMI/ANTI JOIN is a complete EXISTS statement that
  // must be wrapped as a subquery.  (joinInlineFrom computed above.)
  dynSQLAppendStr(s, " FROM ");
  if (g.pFromChild != NULL) {
    // The child is a SUBQUERY: its SELECT-list output columns must be aliased by
    // their INTERNAL slot name so this outer query can reference them (the outer
    // columns address subquery outputs by colName == inner aliasName).
    SNodesRemoteSQLCtx subCtx = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
    subCtx.emitInternalAlias = true;
    qDebug("FQ-SQL-RENDER: render from-child rootType=%s childType=%s emitInternalAlias=%d child=%p",
           nodesNodeName(nodeType(pNode)), nodesNodeName(nodeType((SNode*)g.pFromChild)),
           (int)subCtx.emitInternalAlias, g.pFromChild);
    fqDebugLogExprList("render.fromChild.targets", g.pFromChild->pTargets);
    dynSQLAppendStr(s, "(");
    code = renderNode(g.pFromChild, dialect, &subCtx, s);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
    dynSQLAppendStr(s, ") _sq");
  } else if (joinInlineFrom) {
    // Inline JOIN FROM clause (no subquery).
    code = renderJoin((const SJoinLogicNode*)g.pFromSource, dialect, pCtx, s);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
  } else if (g.pFromSource != NULL && nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_JOIN) {
    // SEMI/ANTI JOIN: complete EXISTS SELECT → wrap as subquery.
    dynSQLAppendStr(s, "(");
    code = renderJoin((const SJoinLogicNode*)g.pFromSource, dialect, pCtx, s);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
    dynSQLAppendStr(s, ") _sq");
  } else if (g.pFromSource != NULL && fqIsFromSource(g.pFromSource) &&
             nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_PROJECT) {
    // A set-op used as a FROM source is an internal derived table. Timestamp
    // transport conversion belongs only to the enclosing, final SELECT;
    // applying it to UNION branches as well makes the outer PG expression run
    // EXTRACT(EPOCH) on an already-converted BIGINT.
    SNodesRemoteSQLCtx subCtx = pCtx ? *pCtx : (SNodesRemoteSQLCtx){0};
    subCtx.emitInternalAlias = true;
    dynSQLAppendStr(s, "(");
    code = renderSetOpStatement((const SProjectLogicNode*)g.pFromSource,
                                dialect, &subCtx, s);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
    dynSQLAppendStr(s, ") _sq");
  } else if (g.pScanWhere != NULL) {
    dynAppendTablePath(s, (const SExtTableNode*)g.pScanWhere->pExtTableNode, dialect);
  } else {
    qError("FQ-SQL: query group has no FROM source");
    nodesDestroyRemoteSelectOutputs(&inputOutputs);
    nodesDestroyRemoteSelectOutputs(&outputs);
    return TSDB_CODE_EXT_SQL_RENDER_ERROR;
  }

  // WHERE: real scan table → scan's own WHERE; inline JOIN → legs' + JOIN's WHERE.
  if (g.pScanWhere != NULL && g.pFromChild == NULL &&
      nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_SCAN) {
    // A remote-plan node carries its pushable WHERE in node.pConditions; the
    // renderer emits it directly (no separate pushed-conditions field).
    qDebug("FQ-SQL-RENDER: use scan WHERE rootType=%s scan=%p hasCond=%d source=%s table=%s",
           nodesNodeName(nodeType(pNode)), g.pScanWhere, NULL != g.pScanWhere->node.pConditions,
           g.pScanWhere->pExtTableNode ? ((const SExtTableNode*)g.pScanWhere->pExtTableNode)->sourceName : "(null)",
           g.pScanWhere->pExtTableNode ? ((const SExtTableNode*)g.pScanWhere->pExtTableNode)->table.tableName : "(null)");
    bool wroteWhere = renderWhere(s, g.pScanWhere->node.pConditions, dialect, pMeta, selCtx);
    code = renderRuntimeTimeRange(s, g.pScanWhere, wroteWhere, dialect, pMeta, selCtx);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
  } else if (joinInlineFrom) {
    SDynSQL tmp; dynSQLInit(&tmp); tmp.tzName = s->tzName;
    if (renderJoinLegWhere((const SJoinLogicNode*)g.pFromSource, dialect, selCtx, &tmp)) {
      dynSQLAppendStr(s, " WHERE ");
      char* w = dynSQLDetach(&tmp);
      if (w) { dynSQLAppendStr(s, w); taosMemoryFree(w); }
    } else {
      dynSQLFree(&tmp);
    }
  } else if (g.pWhereNode != NULL) {
    // Intermediate node (PROJECT/SORT/PARTITION/INDEF) carrying its own pushable
    // WHERE, rendered above a subquery / derived-table FROM.  selMeta/selCtx
    // resolve columns against the subquery's output namespace.
    qDebug("FQ-SQL-RENDER: use upper WHERE rootType=%s whereNodeType=%s whereNode=%p hasCond=%d",
           nodesNodeName(nodeType(pNode)), nodesNodeName(nodeType((SNode*)g.pWhereNode)),
           g.pWhereNode, NULL != g.pWhereNode->pConditions);
    renderWhere(s, g.pWhereNode->pConditions, dialect, selMeta, selCtx);
  } else {
    qDebug("FQ-SQL-RENDER: no WHERE rendered rootType=%s scanWhere=%p fromChild=%p fromSource=%p whereNode=%p",
           nodesNodeName(nodeType(pNode)), g.pScanWhere, g.pFromChild, g.pFromSource, g.pWhereNode);
  }

  // GROUP BY = partition keys [, agg group keys]  (cases 2/3 merge into one clause).
  // DISTINCT-only AGG is already rendered via SELECT DISTINCT above, so its
  // synthetic group keys must not be emitted again here in the same query level.
  bool hasPartGroup = (g.pPartition != NULL && g.pPartition->pPartitionKeys &&
                       LIST_LENGTH(g.pPartition->pPartitionKeys) > 0);
  int32_t aggGroupKeyCount = fqCountAggGroupKeyExprs(g.pAgg);
  bool hasAggGroup  = (g.pAgg != NULL && aggGroupKeyCount > 0 &&
                       !distinctOnlyAgg && !passThroughChildOutputs);
  if (distinctOnlyAgg && g.pAgg != NULL && aggGroupKeyCount > 0) {
    qDebug("FQ-SQL-RENDER: suppress distinct-only agg GROUP BY rootType=%s root=%p outputsSource=%d groupKeys=%d",
           nodesNodeName(nodeType(pNode)), pNode, (int)outputs.source, aggGroupKeyCount);
  }
  qDebug("FQ-SQL-RENDER: group by decision rootType=%s root=%p hasPartGroup=%d partKeys=%d partAggFuncs=%d hasAggGroup=%d aggKeys=%d distinctOnlyAgg=%d",
         nodesNodeName(nodeType(pNode)), pNode, hasPartGroup ? 1 : 0,
         (g.pPartition && g.pPartition->pPartitionKeys) ? LIST_LENGTH(g.pPartition->pPartitionKeys) : 0,
         (g.pPartition && g.pPartition->pAggFuncs) ? LIST_LENGTH(g.pPartition->pAggFuncs) : 0,
         hasAggGroup ? 1 : 0,
         aggGroupKeyCount,
         distinctOnlyAgg ? 1 : 0);
  if (hasPartGroup || hasAggGroup) {
    dynSQLAppendStr(s, " GROUP BY ");
    bool wrote = false;
    if (hasPartGroup) {
      code = renderGroupKeyExprs(s, g.pPartition->pPartitionKeys, g.pPartition->node.pTargets, dialect, selMeta, selCtx, /*withAlias=*/false);
      if (code) {
        nodesDestroyRemoteSelectOutputs(&inputOutputs);
        nodesDestroyRemoteSelectOutputs(&outputs);
        return code;
      }
      wrote = true;
    }
    if (hasAggGroup) {
      if (wrote) dynSQLAppendStr(s, ", ");
      code = renderAggGroupKeyExprs(s, g.pAgg, dialect, selMeta, selCtx, /*withAlias=*/false);
      if (code) {
        nodesDestroyRemoteSelectOutputs(&inputOutputs);
        nodesDestroyRemoteSelectOutputs(&outputs);
        return code;
      }
    }
  }

  // HAVING (AGG conditions).
  if (!passThroughChildOutputs && g.pAgg != NULL && g.pAgg->node.pConditions) {
    SNodesRemoteSQLCtx havingCtxStore = selCtx ? *selCtx : (SNodesRemoteSQLCtx){0};
    const SNodesRemoteSQLCtx* havingCtx = selCtx;
    havingCtxStore.rewriteAggHavingColumn = (g.pFromChild == NULL &&
                                             g.pFromSource != NULL &&
                                             nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_SCAN);
    havingCtxStore.pHavingAggFuncs = g.pAgg->pAggFuncs;
    havingCtx = &havingCtxStore;

    qDebug("FQ-SQL-HAVING: render start directTable=%d rewriteAggCols=%d aggFuncs=%d dialect=%s",
           (int)(g.pFromChild == NULL && g.pFromSource != NULL &&
                 nodeType(g.pFromSource) == QUERY_NODE_LOGIC_PLAN_SCAN),
           (int)havingCtxStore.rewriteAggHavingColumn,
           g.pAgg->pAggFuncs ? (int)LIST_LENGTH(g.pAgg->pAggFuncs) : 0,
           dialectName(dialect));
    dynSQLAppendStr(s, " HAVING ");
    code = dynAppendExpr(s, g.pAgg->node.pConditions, dialect, selMeta, havingCtx);
    if (code) {
      nodesDestroyRemoteSelectOutputs(&inputOutputs);
      nodesDestroyRemoteSelectOutputs(&outputs);
      return code;
    }
  }

  // ORDER BY (SORT).
  if (g.pSort != NULL) {
    renderOrderBy(s, g.pSort->pSortKeys, outputs.pSelectExprs, dialect, selMeta, selCtx);
  }

  // LIMIT (whichever node carried it).
  if (g.pLimit != NULL) {
    renderLimit(s, (const SLimitNode*)g.pLimit);
  }

  nodesDestroyRemoteSelectOutputs(&inputOutputs);
  nodesDestroyRemoteSelectOutputs(&outputs);
  return s->err ? s->err : TSDB_CODE_SUCCESS;
}


// ---------------------------------------------------------------------------
// nodesRenderInSubqueryBody — public API
// ---------------------------------------------------------------------------
// Renders the SELECT SQL of an uncorrelated IN (subquery) for same-source
// MySQL/PG pushdown.  The result is stored in pRemoteValueListNode->inSubquerySql
// by the parser so that nodesRemotePlanToSQL can emit col IN (SELECT ...) without
// needing a runtime resolve context.
//
// pInnerSelect : the inner SSelectStmt (single projection, FROM ext table).
// sourceType   : EExtSourceType (MySQL or PostgreSQL only; others not pushed).
// ppSQL        : OUT — heap-allocated SQL string; caller must taosMemoryFree().
int32_t nodesRenderInSubqueryBody(const SSelectStmt* pInnerSelect,
                                  int8_t             sourceType,
                                  char**             ppSQL) {
  if (!pInnerSelect || !ppSQL) {
    qError("FQ-SQL: nodesRenderInSubqueryBody NULL input");
    return TSDB_CODE_INVALID_PARA;
  }

  EExtSQLDialect dialect;
  switch ((EExtSourceType)sourceType) {
    case EXT_SOURCE_MYSQL:      dialect = EXT_SQL_DIALECT_MYSQL;    break;
    case EXT_SOURCE_POSTGRESQL: dialect = EXT_SQL_DIALECT_POSTGRES; break;
    default:
      // InfluxDB does not support IN (subquery); other sources unsupported.
      qError("FQ-SQL: nodesRenderInSubqueryBody unsupported sourceType=%d", (int)sourceType);
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  qDebug("FQ-SQL-SUBQ: nodesRenderInSubqueryBody begin sourceType=%d dialect=%s projections=%d hasWhere=%d fromType=%s",
         (int)sourceType, dialectName(dialect),
         pInnerSelect->pProjectionList ? LIST_LENGTH(pInnerSelect->pProjectionList) : 0,
         NULL != pInnerSelect->pWhere,
         pInnerSelect->pFromTable ? nodesNodeName(nodeType(pInnerSelect->pFromTable)) : "(null)");

  SDynSQL s;
  dynSQLInit(&s);

  // SELECT clause: render the single projection of the inner subquery.
  dynSQLAppendStr(&s, "SELECT ");
  bool first = true;
  if (pInnerSelect->pProjectionList) {
    SNode* pProj = NULL;
    FOREACH(pProj, pInnerSelect->pProjectionList) {
      if (!first) dynSQLAppendStr(&s, ", ");
      first = false;
      int32_t code = dynAppendExpr(&s, pProj, dialect, NULL, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FQ-SQL: nodesRenderInSubqueryBody SELECT expr failed code=0x%x", code);
        dynSQLFree(&s);
        return code;
      }
    }
  }
  if (first) dynSQLAppendChar(&s, '1');

  // FROM clause: must be an external real table.
  if (pInnerSelect->pFromTable &&
      QUERY_NODE_REAL_TABLE == nodeType(pInnerSelect->pFromTable)) {
    SRealTableNode* pReal = (SRealTableNode*)pInnerSelect->pFromTable;
    SExtTableNode*  pExt  = (SExtTableNode*)pReal->pExtTableNode;
    if (pExt) {
      dynSQLAppendStr(&s, " FROM ");
      dynAppendTablePath(&s, pExt, dialect);
    }
  }

  // WHERE clause (if any).
  if (pInnerSelect->pWhere) {
    SDynSQL condSql;
    dynSQLInit(&condSql);
    int32_t code = dynAppendExpr(&condSql, pInnerSelect->pWhere, dialect, NULL, NULL);
    if (code != TSDB_CODE_SUCCESS || condSql.err) {
      int32_t err = condSql.err ? condSql.err : code;
      qError("FQ-SQL: nodesRenderInSubqueryBody WHERE failed code=0x%x", err);
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
    dynSQLFree(&s);
    return err;
  }

  char* plainSQL = dynSQLDetach(&s);
  if (!plainSQL) {
    qError("FQ-SQL: nodesRenderInSubqueryBody OOM");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *ppSQL = plainSQL;
  qDebug("FQ-SQL-SUBQ: nodesRenderInSubqueryBody sql=[%s]", plainSQL);
  return TSDB_CODE_SUCCESS;
}


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

  qDebug("FQ-SQL-SUBQ: nodesRenderCorrelatedExistsBody begin sourceType=%d dialect=%s projections=%d hasWhere=%d fromType=%s",
         (int)sourceType, dialectName(dialect),
         pInnerSelect->pProjectionList ? LIST_LENGTH(pInnerSelect->pProjectionList) : 0,
         NULL != pInnerSelect->pWhere,
         pInnerSelect->pFromTable ? nodesNodeName(nodeType(pInnerSelect->pFromTable)) : "(null)");

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
  qDebug("FQ-SQL-SUBQ: nodesRenderCorrelatedExistsBody sql=[%s]", plainSQL);
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// nodesRemotePlanToSQL — public API
// ---------------------------------------------------------------------------
// pRemotePlan MUST be non-NULL (the Mode 1 outer node's .pRemotePlan field).
// The function walks the logic-plan tree rooted at pRemotePlan to collect
// SELECT / FROM / WHERE / ORDER BY / LIMIT clauses, then assembles the SQL.
//
// sourceType is mapped to the corresponding EExtSQLDialect internally so that
// callers never need to depend on EExtSQLDialect.
//
// Callers: Executor (federatedscanoperator.c), Connector (extConnectorQuery.c),
//          and EXPLAIN (explain.c).  The same function is used for EXPLAIN output
//          so the displayed Remote SQL exactly matches the SQL sent to the DB.
int32_t nodesRemotePlanToSQL(const SLogicNode* pRemotePlan, int8_t sourceType,
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

  // Dump the FULL remotePlan tree FIRST via the planner's own serializer
  // (nodesNodeToString) — this is the single most useful diagnostic: comparing
  // this complete plan dump against the generated SQL below tells you at a glance
  // whether a wrong remote SQL is a PLAN problem (optimizer built a wrong tree /
  // fields) or a RENDER problem (right tree, wrong SQL).
  qDebug("FQ-SQL: ===== nodesRemotePlanToSQL begin: pRemotePlan=%p dialect=%s rootType=%s =====",
         pRemotePlan, dialectName(dialect), nodesNodeName(nodeType(pRemotePlan)));
  qDebug("FQ-SQL: remote root detail type=%s nChildren=%d targets=%d limit=%p conditions=%p",
         nodesNodeName(nodeType(pRemotePlan)),
         pRemotePlan->pChildren ? LIST_LENGTH(pRemotePlan->pChildren) : 0,
         pRemotePlan->pTargets ? LIST_LENGTH(pRemotePlan->pTargets) : 0,
         pRemotePlan->pLimit, pRemotePlan->pConditions);
  fqDebugLogRenderCtx("nodesRemotePlanToSQL", NULL, pResolveCtx);
  fqDebugLogExprList("render.rootTargets", pRemotePlan->pTargets);
  if (qDebugFlag & DEBUG_DEBUG) {
    char*   pPlanStr = NULL;
    int32_t dumpCode = nodesNodeToString((const SNode*)pRemotePlan, false, &pPlanStr, NULL);
    if (dumpCode == TSDB_CODE_SUCCESS && pPlanStr != NULL) {
      qDebugL("FQ-SQL-PLAN: %s", pPlanStr);
      taosMemoryFree(pPlanStr);
    } else {
      qDebug("FQ-SQL-PLAN: dump failed code=0x%x", dumpCode);
    }
  }

  // Render the whole remote logic tree top-down.  renderNode handles every node
  // type (including a multi-child isSetOpProj PROJECT → UNION[ ALL], at any depth)
  // and wraps children in derived tables as SQL clause-ordering requires.
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
