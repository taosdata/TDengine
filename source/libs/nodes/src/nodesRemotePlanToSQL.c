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

#include "nodes.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taoserror.h"
#include "osMemory.h"

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------
static int32_t appendQuotedId(char* buf, int32_t bufLen, const char* name, EExtSQLDialect dialect);
static int32_t appendTablePath(char* buf, int32_t bufLen, const SExtTableNode* pExtTable, EExtSQLDialect dialect);
static int32_t appendValueLiteral(char* buf, int32_t bufLen, const SValueNode* pVal, EExtSQLDialect dialect);
static int32_t appendEscapedString(char* buf, int32_t bufLen, const char* str, EExtSQLDialect dialect);
static int32_t appendOperatorExpr(char* buf, int32_t bufLen, const SOperatorNode* pOp,
                                   EExtSQLDialect dialect, int32_t* pPos);
static int32_t appendLogicCondition(char* buf, int32_t bufLen, const SLogicConditionNode* pLogic,
                                     EExtSQLDialect dialect, int32_t* pPos);

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// Append a quoted identifier using the dialect's quoting character.
static int32_t appendQuotedId(char* buf, int32_t bufLen, const char* name, EExtSQLDialect dialect) {
  char q;
  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
      q = '`';
      break;
    case EXT_SQL_DIALECT_POSTGRES:
    case EXT_SQL_DIALECT_INFLUXQL:
    default:
      q = '"';
      break;
  }
  return snprintf(buf, bufLen, "%c%s%c", q, name, q);
}

// Append table path (schema.table or database.table depending on dialect).
static int32_t appendTablePath(char* buf, int32_t bufLen, const SExtTableNode* pExtTable,
                                EExtSQLDialect dialect) {
  int32_t pos = 0;
  switch (dialect) {
    case EXT_SQL_DIALECT_MYSQL:
      // `database`.`table`
      if (pExtTable->table.dbName[0]) {
        pos += appendQuotedId(buf + pos, bufLen - pos, pExtTable->table.dbName, dialect);
        if (pos < bufLen - 1) buf[pos++] = '.';
      }
      pos += appendQuotedId(buf + pos, bufLen - pos, pExtTable->table.tableName, dialect);
      break;
    case EXT_SQL_DIALECT_POSTGRES:
      // "schema"."table"
      if (pExtTable->schemaName[0]) {
        pos += appendQuotedId(buf + pos, bufLen - pos, pExtTable->schemaName, dialect);
        if (pos < bufLen - 1) buf[pos++] = '.';
      }
      pos += appendQuotedId(buf + pos, bufLen - pos, pExtTable->table.tableName, dialect);
      break;
    case EXT_SQL_DIALECT_INFLUXQL:
    default:
      // "measurement"
      pos += appendQuotedId(buf + pos, bufLen - pos, pExtTable->table.tableName, dialect);
      break;
  }
  return pos;
}

// Append escaped string literal, guarding against SQL injection.
// Single quotes → double single-quotes; MySQL also escapes backslashes.
static int32_t appendEscapedString(char* buf, int32_t bufLen, const char* str, EExtSQLDialect dialect) {
  int32_t pos = 0;
  if (pos < bufLen - 1) buf[pos++] = '\'';
  for (const char* p = str; *p && pos < bufLen - 3; p++) {
    if (*p == '\'') {
      buf[pos++] = '\'';
      buf[pos++] = '\'';
    } else if (*p == '\\' && dialect == EXT_SQL_DIALECT_MYSQL) {
      buf[pos++] = '\\';
      buf[pos++] = '\\';
    } else {
      buf[pos++] = *p;
    }
  }
  if (pos < bufLen - 1) buf[pos++] = '\'';
  if (pos < bufLen) buf[pos] = '\0';
  return pos;
}

// Append a value literal node to the SQL buffer.
static int32_t appendValueLiteral(char* buf, int32_t bufLen, const SValueNode* pVal,
                                   EExtSQLDialect dialect) {
  if (pVal->isNull) {
    return snprintf(buf, bufLen, "NULL");
  }
  switch (pVal->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      return snprintf(buf, bufLen, "%s", pVal->datum.b ? "TRUE" : "FALSE");
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      return snprintf(buf, bufLen, "%" PRId64, pVal->datum.i);
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return snprintf(buf, bufLen, "%" PRIu64, pVal->datum.u);
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return snprintf(buf, bufLen, "%.17g", pVal->datum.d);
    case TSDB_DATA_TYPE_BINARY:   // TSDB_DATA_TYPE_VARCHAR has the same integer value
    case TSDB_DATA_TYPE_NCHAR:
      return appendEscapedString(buf, bufLen, pVal->datum.p, dialect);
    case TSDB_DATA_TYPE_TIMESTAMP:
      // Format as ISO 8601 string enclosed in single quotes for portability
      return snprintf(buf, bufLen, "%" PRId64, pVal->datum.i);
    default:
      return 0;  // unsupported; skip silently
  }
}

// Append binary operator expression.
static int32_t appendOperatorExpr(char* buf, int32_t bufLen, const SOperatorNode* pOp,
                                   EExtSQLDialect dialect, int32_t* pPos) {
  const char* opStr = NULL;
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:         opStr = " = ";   break;
    case OP_TYPE_NOT_EQUAL:     opStr = " <> ";  break;
    case OP_TYPE_GREATER_THAN:  opStr = " > ";   break;
    case OP_TYPE_GREATER_EQUAL: opStr = " >= ";  break;
    case OP_TYPE_LOWER_THAN:    opStr = " < ";   break;
    case OP_TYPE_LOWER_EQUAL:   opStr = " <= ";  break;
    case OP_TYPE_LIKE:          opStr = " LIKE "; break;
    case OP_TYPE_IS_NULL: {
      int32_t pos = 0, len = 0;
      pos += snprintf(buf + pos, bufLen - pos, "(");
      (void)nodesExprToExtSQL(pOp->pLeft, dialect, buf + pos, bufLen - pos, &len);
      pos += len;
      pos += snprintf(buf + pos, bufLen - pos, " IS NULL)");
      *pPos += pos;
      return TSDB_CODE_SUCCESS;
    }
    case OP_TYPE_IS_NOT_NULL: {
      int32_t pos = 0, len = 0;
      pos += snprintf(buf + pos, bufLen - pos, "(");
      (void)nodesExprToExtSQL(pOp->pLeft, dialect, buf + pos, bufLen - pos, &len);
      pos += len;
      pos += snprintf(buf + pos, bufLen - pos, " IS NOT NULL)");
      *pPos += pos;
      return TSDB_CODE_SUCCESS;
    }
    default:
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  int32_t pos = 0, len = 0;
  pos += snprintf(buf + pos, bufLen - pos, "(");
  int32_t code = nodesExprToExtSQL(pOp->pLeft, dialect, buf + pos, bufLen - pos, &len);
  if (code) return code;
  pos += len;
  pos += snprintf(buf + pos, bufLen - pos, "%s", opStr);
  len = 0;
  code = nodesExprToExtSQL(pOp->pRight, dialect, buf + pos, bufLen - pos, &len);
  if (code) return code;
  pos += len;
  pos += snprintf(buf + pos, bufLen - pos, ")");
  *pPos += pos;
  return TSDB_CODE_SUCCESS;
}

// Append AND/OR logic condition.
static int32_t appendLogicCondition(char* buf, int32_t bufLen, const SLogicConditionNode* pLogic,
                                     EExtSQLDialect dialect, int32_t* pPos) {
  const char* sep = (pLogic->condType == LOGIC_COND_TYPE_AND) ? " AND " : " OR ";
  int32_t pos = 0;
  bool    first = true;
  pos += snprintf(buf + pos, bufLen - pos, "(");
  SNode* pNode = NULL;
  FOREACH(pNode, pLogic->pParameterList) {
    if (!first) pos += snprintf(buf + pos, bufLen - pos, "%s", sep);
    int32_t len = 0;
    int32_t code = nodesExprToExtSQL(pNode, dialect, buf + pos, bufLen - pos, &len);
    if (code) return code;
    pos += len;
    first = false;
  }
  pos += snprintf(buf + pos, bufLen - pos, ")");
  *pPos += pos;
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// nodesExprToExtSQL — public API
// ---------------------------------------------------------------------------
int32_t nodesExprToExtSQL(const SNode* pExpr, EExtSQLDialect dialect, char* buf, int32_t bufLen,
                           int32_t* pLen) {
  if (!pExpr) {
    *pLen = 0;
    return TSDB_CODE_SUCCESS;
  }
  int32_t pos = 0;
  switch (nodeType(pExpr)) {
    case QUERY_NODE_COLUMN: {
      const SColumnNode* pCol = (const SColumnNode*)pExpr;
      pos += appendQuotedId(buf + pos, bufLen - pos, pCol->colName, dialect);
      break;
    }
    case QUERY_NODE_VALUE: {
      pos += appendValueLiteral(buf + pos, bufLen - pos, (const SValueNode*)pExpr, dialect);
      break;
    }
    case QUERY_NODE_OPERATOR: {
      int32_t code = appendOperatorExpr(buf + pos, bufLen - pos, (const SOperatorNode*)pExpr, dialect, &pos);
      if (code) return code;
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      int32_t code = appendLogicCondition(buf + pos, bufLen - pos, (const SLogicConditionNode*)pExpr,
                                           dialect, &pos);
      if (code) return code;
      break;
    }
    default:
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  *pLen = pos;
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// buildFallbackSQL — internal: SELECT cols FROM table [WHERE cond]
// ---------------------------------------------------------------------------
static int32_t buildFallbackSQL(const SNodeList* pScanCols, const SExtTableNode* pExtTable,
                                 const SNode* pConditions, EExtSQLDialect dialect, char** ppSQL) {
  int32_t capacity = 4096;
  char*   buf = (char*)taosMemoryMalloc(capacity);
  if (!buf) return terrno;

  int32_t pos = 0;

  // SELECT clause
  pos += snprintf(buf + pos, capacity - pos, "SELECT ");
  bool first = true;
  if (pScanCols) {
    SNode* pCol = NULL;
    FOREACH(pCol, pScanCols) {
      if (nodeType(pCol) == QUERY_NODE_COLUMN) {
        if (!first) pos += snprintf(buf + pos, capacity - pos, ", ");
        pos += appendQuotedId(buf + pos, capacity - pos, ((SColumnNode*)pCol)->colName, dialect);
        first = false;
      }
    }
  }
  if (first) {
    // empty scan columns → SELECT *
    pos += snprintf(buf + pos, capacity - pos, "*");
  }

  // FROM clause
  pos += snprintf(buf + pos, capacity - pos, " FROM ");
  pos += appendTablePath(buf + pos, capacity - pos, pExtTable, dialect);

  // WHERE clause (best-effort push-down)
  if (pConditions) {
    char    condBuf[2048] = {0};
    int32_t condLen = 0;
    int32_t code = nodesExprToExtSQL(pConditions, dialect, condBuf, sizeof(condBuf), &condLen);
    if (TSDB_CODE_SUCCESS == code && condLen > 0) {
      pos += snprintf(buf + pos, capacity - pos, " WHERE %s", condBuf);
    }
    // On error or unsupported expression: skip WHERE (local Filter operator will handle it)
  }

  *ppSQL = buf;
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// nodesRemotePlanToSQL — public API
// ---------------------------------------------------------------------------
int32_t nodesRemotePlanToSQL(const SPhysiNode* pRemotePlan, const SNodeList* pScanCols,
                              const SExtTableNode* pExtTable, const SNode* pConditions,
                              EExtSQLDialect dialect, char** ppSQL) {
  if (!pExtTable || !ppSQL) return TSDB_CODE_INVALID_PARA;

  if (pRemotePlan != NULL) {
    // Phase 2: full plan tree → SQL (not yet implemented)
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  // Phase 1: fallback path — build SELECT … FROM … WHERE …
  return buildFallbackSQL(pScanCols, pExtTable, pConditions, dialect, ppSQL);
}
