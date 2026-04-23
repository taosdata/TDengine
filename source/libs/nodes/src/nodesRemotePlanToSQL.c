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
  dynSQLAppendChar(s, q);
  dynSQLAppendStr(s, name);
  dynSQLAppendChar(s, q);
}

static void dynAppendTablePath(SDynSQL* s, const SExtTableNode* pExtTable, EExtSQLDialect dialect) {
  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
      // `database`.`table`
      if (pExtTable->table.dbName[0]) {
        dynAppendQuotedId(s, pExtTable->table.dbName, dialect);
        dynSQLAppendChar(s, '.');
      }
      dynAppendQuotedId(s, pExtTable->table.tableName, dialect);
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
    case TSDB_DATA_TYPE_TIMESTAMP:
      dynSQLAppendf(s, "%" PRId64, pVal->datum.i);
      break;
    default:
      break;  // unsupported; skip silently
  }
}

// Forward declaration for mutual recursion
static int32_t dynAppendExpr(SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect);

static int32_t dynAppendOperatorExpr(SDynSQL* s, const SOperatorNode* pOp, EExtSQLDialect dialect) {
  const char* opStr = NULL;
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:         opStr = " = ";    break;
    case OP_TYPE_NOT_EQUAL:     opStr = " <> ";   break;
    case OP_TYPE_GREATER_THAN:  opStr = " > ";    break;
    case OP_TYPE_GREATER_EQUAL: opStr = " >= ";   break;
    case OP_TYPE_LOWER_THAN:    opStr = " < ";    break;
    case OP_TYPE_LOWER_EQUAL:   opStr = " <= ";   break;
    case OP_TYPE_LIKE:          opStr = " LIKE ";  break;
    case OP_TYPE_IS_NULL:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect);
      dynSQLAppendStr(s, " IS NULL)");
      return TSDB_CODE_SUCCESS;
    case OP_TYPE_IS_NOT_NULL:
      dynSQLAppendChar(s, '(');
      (void)dynAppendExpr(s, pOp->pLeft, dialect);
      dynSQLAppendStr(s, " IS NOT NULL)");
      return TSDB_CODE_SUCCESS;
    default:
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  dynSQLAppendChar(s, '(');
  int32_t code = dynAppendExpr(s, pOp->pLeft, dialect);
  if (code) return code;
  dynSQLAppendStr(s, opStr);
  code = dynAppendExpr(s, pOp->pRight, dialect);
  if (code) return code;
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendLogicCondition(SDynSQL* s, const SLogicConditionNode* pLogic,
                                        EExtSQLDialect dialect) {
  const char* sep = (pLogic->condType == LOGIC_COND_TYPE_AND) ? " AND " : " OR ";
  bool first = true;
  dynSQLAppendChar(s, '(');
  SNode* pNode = NULL;
  FOREACH(pNode, pLogic->pParameterList) {
    if (!first) dynSQLAppendStr(s, sep);
    int32_t code = dynAppendExpr(s, pNode, dialect);
    if (code) return code;
    first = false;
  }
  dynSQLAppendChar(s, ')');
  return TSDB_CODE_SUCCESS;
}

static int32_t dynAppendExpr(SDynSQL* s, const SNode* pExpr, EExtSQLDialect dialect) {
  if (!pExpr) return TSDB_CODE_SUCCESS;
  switch (nodeType(pExpr)) {
    case QUERY_NODE_COLUMN:
      dynAppendQuotedId(s, ((const SColumnNode*)pExpr)->colName, dialect);
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_VALUE:
      dynAppendValueLiteral(s, (const SValueNode*)pExpr, dialect);
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_OPERATOR:
      return dynAppendOperatorExpr(s, (const SOperatorNode*)pExpr, dialect);
    case QUERY_NODE_LOGIC_CONDITION:
      return dynAppendLogicCondition(s, (const SLogicConditionNode*)pExpr, dialect);
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
  int32_t code = dynAppendExpr(&s, pExpr, dialect);
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
// ---------------------------------------------------------------------------
// The tree walker fills this struct bottom-up; assembleRemoteSQL() then
// renders the final SQL string.
typedef struct SRemoteSQLParts {
  // FROM clause: provided by the leaf SFederatedScanPhysiNode (Mode 2)
  const SExtTableNode* pExtTable;    // table identity (database + schema + tableName)
  const SNodeList*     pScanCols;    // columns to SELECT when no explicit projection

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
      pParts->pExtTable   = (const SExtTableNode*)pScan->pExtTable;
      pParts->pScanCols   = pScan->pScanCols;
      pParts->pConditions = pNode->pConditions;
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
                                  char** ppSQL) {
  if (!pParts->pExtTable) return TSDB_CODE_PLAN_INTERNAL_ERROR;

  SDynSQL s;
  dynSQLInit(&s);

  // SELECT clause
  dynSQLAppendStr(&s, "SELECT ");
  const SNodeList* pCols = pParts->pProjections ? pParts->pProjections : pParts->pScanCols;
  bool first = true;
  if (pCols) {
    SNode* pExpr = NULL;
    FOREACH(pExpr, pCols) {
      if (nodeType(pExpr) != QUERY_NODE_COLUMN) continue;  // skip non-column; local Project handles
      if (!first) dynSQLAppendStr(&s, ", ");
      dynAppendQuotedId(&s, ((const SColumnNode*)pExpr)->colName, dialect);
      first = false;
    }
  }
  if (first) dynSQLAppendChar(&s, '*');

  // FROM clause
  dynSQLAppendStr(&s, " FROM ");
  dynAppendTablePath(&s, pParts->pExtTable, dialect);

  // WHERE clause (best-effort: skip on expression-render failure — local Filter handles it)
  if (pParts->pConditions) {
    SDynSQL cond;
    dynSQLInit(&cond);
    int32_t code = dynAppendExpr(&cond, pParts->pConditions, dialect);
    if (code == TSDB_CODE_SUCCESS && !cond.err && cond.pos > 0) {
      dynSQLAppendStr(&s, " WHERE ");
      dynSQLAppendLen(&s, cond.buf, cond.pos);
    }
    dynSQLFree(&cond);
  }

  // ORDER BY clause — emit header only after confirming at least one key renders.
  if (pParts->pSortKeys && LIST_LENGTH(pParts->pSortKeys) > 0) {
    bool firstKey = true;
    SNode* pKey = NULL;
    FOREACH(pKey, pParts->pSortKeys) {
      const SOrderByExprNode* pOrd = (const SOrderByExprNode*)pKey;
      SDynSQL expr;
      dynSQLInit(&expr);
      int32_t code = dynAppendExpr(&expr, pOrd->pExpr, dialect);
      if (code || expr.err) {
        dynSQLFree(&expr);
        continue;  // skip un-renderable expression; local Sort will handle it
      }
      dynSQLAppendStr(&s, firstKey ? " ORDER BY " : ", ");
      firstKey = false;
      dynSQLAppendLen(&s, expr.buf, expr.pos);
      dynSQLFree(&expr);
      dynSQLAppendStr(&s, (pOrd->order == ORDER_DESC) ? " DESC" : " ASC");
      if (dialect != EXT_SQL_DIALECT_MYSQL) {
        if (pOrd->nullOrder == NULL_ORDER_FIRST)
          dynSQLAppendStr(&s, " NULLS FIRST");
        else if (pOrd->nullOrder == NULL_ORDER_LAST)
          dynSQLAppendStr(&s, " NULLS LAST");
      }
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

  return assembleRemoteSQL(&parts, dialect, ppSQL);
}
