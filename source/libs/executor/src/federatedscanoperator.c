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

// federatedscanoperator.c — FederatedScan executor operator
//
// Responsibilities (DS §5.2.4):
//   - Lazy-connect to external data source on first getNext call
//   - Generate remote SQL via nodesRemotePlanToSQL and cache for EXPLAIN/log
//   - Execute query via extConnectorExecQuery(pHandle, pNode, ...)
//   - Fetch SSDataBlock results via extConnectorFetchBlock
//   - Propagate errors (including remote error strings) to pTaskInfo->extErrMsg
//   - Release all resources in close

#include "executorInt.h"
#include "extConnector.h"
#include "extTypeMap.h"
#include "filter.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "os.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "ttime.h"
#include "nodes.h"

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

// Map EExtSourceType to a human-readable string for logging and EXPLAIN output.
static const char* fedScanSourceTypeName(int8_t srcType) {
  switch ((EExtSourceType)srcType) {
    case EXT_SOURCE_MYSQL:      return "mysql";
    case EXT_SOURCE_POSTGRESQL: return "postgresql";
    case EXT_SOURCE_INFLUXDB:   return "influxdb";
    default:                    return "unknown";
  }
}

static bool fedScanTagCondMentionsColumn(const SNode* pCond, const char* colName) {
  if (pCond == NULL || colName == NULL || colName[0] == '\0') {
    return false;
  }

  if (nodeType(pCond) == QUERY_NODE_OPERATOR) {
    SOperatorNode* pOp = (SOperatorNode*)pCond;
    if (pOp->pLeft != NULL && nodeType(pOp->pLeft) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pOp->pLeft;
      return strcasecmp(pCol->colName, colName) == 0;
    }
    return false;
  }

  if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SNode* pChild = NULL;
    FOREACH(pChild, ((SLogicConditionNode*)pCond)->pParameterList) {
      if (fedScanTagCondMentionsColumn(pChild, colName)) {
        return true;
      }
    }
  }

  return false;
}

static int32_t fedScanBuildMissingTagFilter(
    const SNode*       pCond,
    const SExtTableMeta* pMeta,
    char               q,
    char**             ppSql,
    int32_t*           pSqlLen) {
  if (ppSql == NULL || pSqlLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppSql = NULL;
  *pSqlLen = 0;
  if (pCond == NULL || pMeta == NULL || pMeta->numOfCols <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t cap = pMeta->numOfCols * (TSDB_COL_NAME_LEN + 24) + 1;
  char* pSql = taosMemoryCalloc(1, cap);
  if (pSql == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t offset = 0;
  for (int32_t i = 0; i < pMeta->numOfCols; ++i) {
    const SExtColumnDef* pCol = &pMeta->pCols[i];
    if (!pCol->isTag || fedScanTagCondMentionsColumn(pCond, pCol->colName)) {
      continue;
    }

    const char* remoteName = pCol->remoteColName[0] ? pCol->remoteColName : pCol->colName;
    offset += snprintf(pSql + offset, cap - offset, "%s%c%s%c = ''",
                       offset > 0 ? " AND " : "", q, remoteName, q);
  }

  if (offset == 0) {
    taosMemoryFree(pSql);
    return TSDB_CODE_SUCCESS;
  }

  *ppSql = pSql;
  *pSqlLen = offset;
  return TSDB_CODE_SUCCESS;
}

typedef struct SVStbPushedCondSql {
  char*                            buf;
  int32_t                          cap;
  int32_t                          len;
  char                             q;
  const SExtTableMeta*             pMeta;
  const SForeignScanOperatorParam* pFsParam;
} SVStbPushedCondSql;

static int32_t fedScanCondSqlReserve(SVStbPushedCondSql* pSql, int32_t extra) {
  if (pSql == NULL || extra < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t need = pSql->len + extra + 1;
  if (need <= pSql->cap) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t newCap = pSql->cap > 0 ? pSql->cap : 256;
  while (newCap < need) {
    newCap *= 2;
  }

  char* pNew = taosMemoryRealloc(pSql->buf, newCap);
  if (pNew == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memset(pNew + pSql->cap, 0, newCap - pSql->cap);
  pSql->buf = pNew;
  pSql->cap = newCap;
  return TSDB_CODE_SUCCESS;
}

static int32_t fedScanCondSqlAppendN(SVStbPushedCondSql* pSql, const char* pText, int32_t n) {
  if (pText == NULL || n <= 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = fedScanCondSqlReserve(pSql, n);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  memcpy(pSql->buf + pSql->len, pText, n);
  pSql->len += n;
  pSql->buf[pSql->len] = '\0';
  return TSDB_CODE_SUCCESS;
}

static int32_t fedScanCondSqlAppend(SVStbPushedCondSql* pSql, const char* pText) {
  return fedScanCondSqlAppendN(pSql, pText, pText ? (int32_t)strlen(pText) : 0);
}

static int32_t fedScanCondSqlAppendChar(SVStbPushedCondSql* pSql, char c) {
  return fedScanCondSqlAppendN(pSql, &c, 1);
}

static int32_t fedScanCondSqlAppendQuotedIdent(SVStbPushedCondSql* pSql, const char* pName) {
  if (pName == NULL || pName[0] == '\0') {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  int32_t code = fedScanCondSqlAppendChar(pSql, pSql->q);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  for (const char* p = pName; *p != '\0'; ++p) {
    code = fedScanCondSqlAppendChar(pSql, *p);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (*p == pSql->q) {
      code = fedScanCondSqlAppendChar(pSql, *p);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }
  return fedScanCondSqlAppendChar(pSql, pSql->q);
}

static const char* fedScanFindRemoteCondColName(const SVStbPushedCondSql* pSql, col_id_t colId) {
  if (pSql == NULL || pSql->pFsParam == NULL || pSql->pFsParam->colMap == NULL || pSql->pMeta == NULL) {
    return NULL;
  }

  int32_t numDataCols = (int32_t)taosArrayGetSize(pSql->pFsParam->colMap);
  for (int32_t i = 0; i < numDataCols; ++i) {
    SColIdNameKV* pKV = (SColIdNameKV*)taosArrayGet(pSql->pFsParam->colMap, i);
    if (pKV == NULL || pKV->colId != colId) {
      continue;
    }

    for (int32_t j = 0; j < pSql->pMeta->numOfCols; ++j) {
      SExtColumnDef* pCol = &pSql->pMeta->pCols[j];
      if (strncasecmp(pCol->colName, pKV->colName, TSDB_COL_NAME_LEN) == 0) {
        return pCol->remoteColName[0] ? pCol->remoteColName : pCol->colName;
      }
    }
  }

  return NULL;
}

static bool fedScanPushedOpSupported(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
      return true;
    default:
      return false;
  }
}

static int32_t fedScanRenderPushedCondExpr(SVStbPushedCondSql* pSql, SNode* pNode);

static int32_t fedScanRenderPushedCondValue(SVStbPushedCondSql* pSql, SValueNode* pVal) {
  if (pSql == NULL || pVal == NULL) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  if (pVal->node.resType.type == TSDB_DATA_TYPE_FLOAT || pVal->node.resType.type == TSDB_DATA_TYPE_DOUBLE) {
    char buf[64] = {0};
    if (pVal->literal != NULL && pVal->literal[0] != '\0') {
      return fedScanCondSqlAppend(pSql, pVal->literal);
    }
    (void)snprintf(buf, sizeof(buf), pVal->node.resType.type == TSDB_DATA_TYPE_FLOAT ? "%.9g" : "%.17g",
                   pVal->datum.d);
    return fedScanCondSqlAppend(pSql, buf);
  }

  char* pValue = nodesGetStrValueFromNode(pVal);
  if (pValue == NULL) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
  int32_t code = fedScanCondSqlAppend(pSql, pValue);
  taosMemoryFree(pValue);
  return code;
}

static int32_t fedScanRenderPushedCondList(SVStbPushedCondSql* pSql, SNodeListNode* pList) {
  int32_t code = fedScanCondSqlAppendChar(pSql, '(');
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SNode* pChild = NULL;
  bool   first = true;
  FOREACH(pChild, pList->pNodeList) {
    if (!first) {
      code = fedScanCondSqlAppend(pSql, ", ");
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    code = fedScanRenderPushedCondExpr(pSql, pChild);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    first = false;
  }

  return fedScanCondSqlAppendChar(pSql, ')');
}

static int32_t fedScanRenderPushedCondLogic(SVStbPushedCondSql* pSql, SLogicConditionNode* pLogic) {
  if (pLogic->condType != LOGIC_COND_TYPE_AND && pLogic->condType != LOGIC_COND_TYPE_OR) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  int32_t code = fedScanCondSqlAppendChar(pSql, '(');
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SNode* pChild = NULL;
  bool   first = true;
  FOREACH(pChild, pLogic->pParameterList) {
    if (!first) {
      code = fedScanCondSqlAppend(pSql, pLogic->condType == LOGIC_COND_TYPE_AND ? " AND " : " OR ");
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    code = fedScanRenderPushedCondExpr(pSql, pChild);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    first = false;
  }

  return fedScanCondSqlAppendChar(pSql, ')');
}

static int32_t fedScanRenderPushedCondOperator(SVStbPushedCondSql* pSql, SOperatorNode* pOp) {
  if (!fedScanPushedOpSupported(pOp)) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  int32_t code = fedScanCondSqlAppendChar(pSql, '(');
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pOp->opType == OP_TYPE_IS_NULL || pOp->opType == OP_TYPE_IS_NOT_NULL) {
    code = fedScanRenderPushedCondExpr(pSql, pOp->pLeft);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = fedScanCondSqlAppend(pSql, pOp->opType == OP_TYPE_IS_NULL ? " IS NULL" : " IS NOT NULL");
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    return fedScanCondSqlAppendChar(pSql, ')');
  }

  if (pOp->opType == OP_TYPE_IN || pOp->opType == OP_TYPE_NOT_IN) {
    if (pOp->pRight == NULL || nodeType(pOp->pRight) != QUERY_NODE_NODE_LIST) {
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
    }
    code = fedScanRenderPushedCondExpr(pSql, pOp->pLeft);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = fedScanCondSqlAppend(pSql, pOp->opType == OP_TYPE_IN ? " IN " : " NOT IN ");
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = fedScanRenderPushedCondExpr(pSql, pOp->pRight);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    return fedScanCondSqlAppendChar(pSql, ')');
  }

  const char* opStr = NULL;
  switch (pOp->opType) {
    case OP_TYPE_EQUAL:         opStr = " = "; break;
    case OP_TYPE_NOT_EQUAL:     opStr = " <> "; break;
    case OP_TYPE_GREATER_THAN:  opStr = " > "; break;
    case OP_TYPE_GREATER_EQUAL: opStr = " >= "; break;
    case OP_TYPE_LOWER_THAN:    opStr = " < "; break;
    case OP_TYPE_LOWER_EQUAL:   opStr = " <= "; break;
    case OP_TYPE_LIKE:          opStr = " LIKE "; break;
    case OP_TYPE_NOT_LIKE:      opStr = " NOT LIKE "; break;
    default:
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  code = fedScanRenderPushedCondExpr(pSql, pOp->pLeft);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = fedScanCondSqlAppend(pSql, opStr);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = fedScanRenderPushedCondExpr(pSql, pOp->pRight);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  return fedScanCondSqlAppendChar(pSql, ')');
}

static int32_t fedScanRenderPushedCondExpr(SVStbPushedCondSql* pSql, SNode* pNode) {
  if (pNode == NULL) {
    return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SColumnNode* pCol = (SColumnNode*)pNode;
      const char* remoteName = fedScanFindRemoteCondColName(pSql, pCol->colId);
      if (remoteName == NULL) {
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return fedScanCondSqlAppendQuotedIdent(pSql, remoteName);
    }
    case QUERY_NODE_VALUE: {
      return fedScanRenderPushedCondValue(pSql, (SValueNode*)pNode);
    }
    case QUERY_NODE_NODE_LIST:
      return fedScanRenderPushedCondList(pSql, (SNodeListNode*)pNode);
    case QUERY_NODE_OPERATOR:
      return fedScanRenderPushedCondOperator(pSql, (SOperatorNode*)pNode);
    case QUERY_NODE_LOGIC_CONDITION:
      return fedScanRenderPushedCondLogic(pSql, (SLogicConditionNode*)pNode);
    default:
      return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
  }
}

static int32_t fedScanTryRenderPushedCond(SVStbPushedCondSql* pDst, SNode* pCond, bool* pRendered) {
  SVStbPushedCondSql tmp = {
      .q = pDst->q,
      .pMeta = pDst->pMeta,
      .pFsParam = pDst->pFsParam,
  };

  int32_t code = fedScanRenderPushedCondExpr(&tmp, pCond);
  if (code == TSDB_CODE_EXT_SYNTAX_UNSUPPORTED) {
    taosMemoryFree(tmp.buf);
    *pRendered = false;
    return TSDB_CODE_SUCCESS;
  }
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(tmp.buf);
    return code;
  }

  if (tmp.len > 0) {
    if (pDst->len > 0) {
      code = fedScanCondSqlAppend(pDst, " AND ");
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(tmp.buf);
        return code;
      }
    }
    code = fedScanCondSqlAppendN(pDst, tmp.buf, tmp.len);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(tmp.buf);
      return code;
    }
  }

  taosMemoryFree(tmp.buf);
  *pRendered = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t fedScanBuildVStbPushedCondSql(SForeignScanOperatorParam* pFsParam,
                                             const SExtTableMeta*        pMeta,
                                             char                        q,
                                             char**                      ppSql,
                                             int32_t*                    pSqlLen,
                                             bool*                       pFullyRendered) {
  if (ppSql == NULL || pSqlLen == NULL || pFullyRendered == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppSql = NULL;
  *pSqlLen = 0;
  *pFullyRendered = false;
  if (pFsParam == NULL || pFsParam->pPushedCond == NULL || pMeta == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SVStbPushedCondSql sql = {
      .q = q,
      .pMeta = pMeta,
      .pFsParam = pFsParam,
  };

  int32_t code = TSDB_CODE_SUCCESS;
  bool    allRendered = true;
  if (nodeType(pFsParam->pPushedCond) == QUERY_NODE_LOGIC_CONDITION &&
      ((SLogicConditionNode*)pFsParam->pPushedCond)->condType == LOGIC_COND_TYPE_AND) {
    SNode* pChild = NULL;
    FOREACH(pChild, ((SLogicConditionNode*)pFsParam->pPushedCond)->pParameterList) {
      bool rendered = false;
      code = fedScanTryRenderPushedCond(&sql, pChild, &rendered);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(sql.buf);
        return code;
      }
      if (!rendered) {
        allRendered = false;
      }
    }
  } else {
    bool rendered = false;
    code = fedScanTryRenderPushedCond(&sql, pFsParam->pPushedCond, &rendered);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(sql.buf);
      return code;
    }
    allRendered = rendered;
  }

  if (sql.len <= 0) {
    taosMemoryFree(sql.buf);
    return TSDB_CODE_SUCCESS;
  }

  *ppSql = sql.buf;
  *pSqlLen = sql.len;
  *pFullyRendered = allRendered;
  return TSDB_CODE_SUCCESS;
}

// Fill an appended extra output column with a constant VALUE target when possible.
// Returns true if the slot is filled, false if caller should apply other fallback logic.
static SNodeList* fedScanGetOutputExprList(const SFederatedScanPhysiNode* pFedScan) {
  if (NULL == pFedScan || NULL == pFedScan->pRemotePlan) {
    return NULL;
  }

  SNode* pCurNode = pFedScan->pRemotePlan;
  while (pCurNode != NULL) {
    ENodeType t = nodeType(pCurNode);
    if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == t) {
      SNodeList* pProj = ((SProjectPhysiNode*)pCurNode)->pProjections;
      if (pProj != NULL && LIST_LENGTH(pProj) > 0) {
        return pProj;
      }
    }

    SPhysiNode* pCur = (SPhysiNode*)pCurNode;

    if (NULL == pCur->pChildren || LIST_LENGTH(pCur->pChildren) != 1) {
      break;
    }
    pCurNode = nodesListGetNode(pCur->pChildren, 0);
  }

  return pFedScan->pScanCols;
}

static SNode* fedScanGetOutputExprBySlot(const SFederatedScanPhysiNode* pFedScan, int16_t slotId) {
  SNodeList* pTargets = fedScanGetOutputExprList(pFedScan);
  if (NULL == pTargets || slotId < 0 || slotId >= LIST_LENGTH(pTargets)) {
    return NULL;
  }

  SNode* pTargetNode = nodesListGetNode(pTargets, slotId);
  if (NULL == pTargetNode) {
    return NULL;
  }

  if (QUERY_NODE_TARGET == nodeType(pTargetNode)) {
    return ((STargetNode*)pTargetNode)->pExpr;
  }

  return pTargetNode;
}

static int32_t fedScanFillConstValueSlot(const SFederatedScanPhysiNode* pFedScan,
                                         const SSlotDescNode*           pSlot,
                                         SColumnInfoData*               pCol,
                                         int32_t                        rows,
                                         bool*                          pFilled) {
  if (pFilled != NULL) {
    *pFilled = false;
  }

  if (NULL == pFedScan || NULL == pSlot || NULL == pCol || rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pExpr = fedScanGetOutputExprBySlot(pFedScan, pSlot->slotId);
  if (NULL == pExpr || QUERY_NODE_VALUE != nodeType(pExpr)) {
    return TSDB_CODE_SUCCESS;
  }

  SValueNode* pVal = (SValueNode*)pExpr;
  void*       pRaw = nodesGetValueFromNode(pVal);
  for (int32_t r = 0; r < rows; ++r) {
    int32_t code = colDataSetVal(pCol, r, pRaw, pVal->isNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pFilled != NULL) {
    *pFilled = true;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t fedScanOverwriteConstSlots(const SFederatedScanPhysiNode* pFedScan,
                                          const SDataBlockDescNode*      pDesc,
                                          SSDataBlock*                   pBlock) {
  if (NULL == pFedScan || NULL == pDesc || NULL == pBlock || pBlock->pDataBlock == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t blockCols = taosArrayGetSize(pBlock->pDataBlock);
  if (blockCols <= 0 || pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t idx = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pDesc->pSlots) {
    if (idx >= blockCols) {
      break;
    }
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    SNode* pExpr = fedScanGetOutputExprBySlot(pFedScan, pSlot->slotId);
    if (pExpr != NULL && QUERY_NODE_VALUE == nodeType(pExpr)) {
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, idx);
      if (pCol != NULL) {
        SValueNode* pVal = (SValueNode*)pExpr;
        void*       pRaw = nodesGetValueFromNode(pVal);
        for (int32_t r = 0; r < (int32_t)pBlock->info.rows; ++r) {
          int32_t code = colDataSetVal(pCol, r, pRaw, pVal->isNull);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
        }
      }
    }
    idx++;
  }

  return TSDB_CODE_SUCCESS;
}

static bool fedScanIsInfluxHttpTokenOptions(int8_t sourceType, const char *options) {
  if ((EExtSourceType)sourceType != EXT_SOURCE_INFLUXDB) {
    return false;
  }
  if (options == NULL || options[0] == '\0') {
    return true;
  }
  const char *protoPos = taosStrCaseStr(options, "protocol");
  if (protoPos == NULL) {
    return true;
  }
  return taosStrCaseStr(protoPos, "http") != NULL;
}

static bool fedScanIsRemoteInternalCode(int32_t code) {
  return code == TSDB_CODE_EXT_REMOTE_INTERNAL ||
         (((uint32_t)code & 0xFFFFU) == ((uint32_t)TSDB_CODE_EXT_REMOTE_INTERNAL & 0xFFFFU));
}

static int32_t fedScanNormalizeInfluxAuthCode(int8_t sourceType, const char *options, int32_t code) {
  if (fedScanIsRemoteInternalCode(code) &&
      fedScanIsInfluxHttpTokenOptions(sourceType, options)) {
    return TSDB_CODE_EXT_AUTH_FAILED;
  }
  return code;
}

// Format timestamp as UTC string for InfluxDB (which stores epoch UTC).
// MySQL/PG use local time (formatTimestampLocal handles that).
static char* formatTimestampUTC(char* buf, int32_t cap, int64_t val, int precision) {
  time_t tt;
  int32_t frac;
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    frac = (int32_t)(val % 1000000);
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    frac = (int32_t)(val % 1000000000);
  } else {
    tt = (time_t)(val / 1000);
    frac = (int32_t)(val % 1000);
  }
  if (frac < 0) {
    if (precision == TSDB_TIME_PRECISION_MICRO) { frac += 1000000; tt -= 1; }
    else if (precision == TSDB_TIME_PRECISION_NANO) { frac += 1000000000; tt -= 1; }
    else { frac += 1000; tt -= 1; }
  }
  struct tm tm;
  taosGmTimeR(&tt, &tm);
  int pos = snprintf(buf, cap, "%04d-%02d-%02d %02d:%02d:%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                     tm.tm_hour, tm.tm_min, tm.tm_sec);
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    snprintf(buf + pos, cap - pos, ".%06d", frac);
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    snprintf(buf + pos, cap - pos, ".%09d", frac);
  } else {
    snprintf(buf + pos, cap - pos, ".%03d", frac);
  }
  return buf;
}

// Format a filled SExtConnectorError into pInfo->extErrMsg for later propagation.
static void fedScanFormatError(SFederatedScanOperatorInfo* pInfo,
                               const SExtConnectorError*   pErr) {
  if (!pErr || pErr->tdCode == 0) return;

  const char* tdErrStr  = tstrerror(pErr->tdCode);
  const char* typeName  = fedScanSourceTypeName(pErr->sourceType);
  int32_t     bufLen    = (int32_t)sizeof(pInfo->extErrMsg);
  int32_t     offset    = 0;

  offset = snprintf(pInfo->extErrMsg, bufLen, "%s [source=%s, type=%s",
                    tdErrStr, pErr->sourceName, typeName);

  if ((EExtSourceType)pErr->sourceType == EXT_SOURCE_MYSQL && pErr->remoteCode != 0) {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", remote_code=%d", pErr->remoteCode);
  }
  if ((EExtSourceType)pErr->sourceType == EXT_SOURCE_POSTGRESQL &&
      pErr->remoteSqlstate[0] != '\0') {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", remote_sqlstate=%s", pErr->remoteSqlstate);
  }
  if ((EExtSourceType)pErr->sourceType == EXT_SOURCE_INFLUXDB && pErr->httpStatus != 0) {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", http_status=%d", pErr->httpStatus);
  }
  if (pErr->remoteMessage[0] != '\0') {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", remote_message=%s", pErr->remoteMessage);
  }
  if (offset < bufLen - 1) {
    pInfo->extErrMsg[offset]     = ']';
    pInfo->extErrMsg[offset + 1] = '\0';
  }
}

// ---------------------------------------------------------------------------
// VStb dynamic query: build SQL + column type mappings at runtime
// ---------------------------------------------------------------------------

// Build SELECT SQL and column type mappings for a VStb child table query.
// Uses extConnectorGetTableSchema to resolve column types from the remote source.
// Always includes the remote table's primary timestamp column as column 0
// so the sort-merge in VTableScan can join on timestamp.
static int32_t fedScanBuildVStbQueryAndMappings(
    SFederatedScanOperatorInfo* pInfo,
    SForeignScanOperatorParam*  pFsParam,
    char**                      ppSQL,
    SExtColTypeMapping**        ppMappings,
    int32_t*                    pNumMappings) {
  int32_t code = TSDB_CODE_SUCCESS;
  SFederatedScanPhysiNode* pFedNode = pInfo->pFedScanNode;
  SExtTableNode* pExtTable = (SExtTableNode*)pFedNode->pExtTable;
  EExtSourceType srcType = (EExtSourceType)pFedNode->sourceType;

  // Determine SQL dialect: quote character and table path format
  char q = (srcType == EXT_SOURCE_MYSQL) ? '`' : '"';

  // Build a temporary SExtTableNode with the specific remote table name
  SExtTableNode tempExtTable = {0};
  if (pExtTable != NULL) {
    tempExtTable = *pExtTable;
  }
  tstrncpy(tempExtTable.table.tableName, pFsParam->tableName, sizeof(tempExtTable.table.tableName));
  // For PG: use srcSchema as schemaName (falls back to "public")
  // For MySQL: use srcDatabase as dbName (table lookup needs it)
  if (srcType == EXT_SOURCE_POSTGRESQL) {
    if (pFedNode->srcSchema[0] != '\0') {
      tstrncpy(tempExtTable.schemaName, pFedNode->srcSchema, sizeof(tempExtTable.schemaName));
    }
  } else if (srcType == EXT_SOURCE_MYSQL) {
    if (pFedNode->srcDatabase[0] != '\0') {
      tstrncpy(tempExtTable.table.dbName, pFedNode->srcDatabase, sizeof(tempExtTable.table.dbName));
    }
  }

  // Get ext table schema from remote source
  SExtTableMeta* pMeta = NULL;
  code = extConnectorGetTableSchema(pInfo->pConnHandle, &tempExtTable, &pMeta);
  if (code != TSDB_CODE_SUCCESS || pMeta == NULL) {
    qError("FederatedScan VStb: getTableSchema failed for %s.%s, code=0x%x",
           pFsParam->dbName, pFsParam->tableName, code);
    return code ? code : TSDB_CODE_EXT_TABLE_NOT_EXIST;
  }

  int32_t numDataCols = (int32_t)taosArrayGetSize(pFsParam->colMap);
  if (numDataCols <= 0) {
    extConnectorFreeTableSchema(pMeta);
    qError("FederatedScan VStb: empty colMap for %s.%s", pFsParam->dbName, pFsParam->tableName);
    return TSDB_CODE_INVALID_PARA;
  }

  // Find the primary key (first timestamp) column in the remote schema.
  // This is needed because the VTableScan sort-merge uses ts as the merge key.
  int32_t tsIdx = -1;
  for (int32_t j = 0; j < pMeta->numOfCols; j++) {
    SExtColumnDef* pCol = &pMeta->pCols[j];
    if (pCol->isPrimaryKey) {
      tsIdx = j;
      break;
    }
  }
  if (tsIdx < 0) {
    // Fallback: use the first column (usually ts for time-series external tables)
    tsIdx = 0;
  }

  // Total columns = 1 (ts) + numDataCols
  int32_t totalCols = 1 + numDataCols;

  // Pre-render the series tag filter (if any) so we can size the SQL buffer and
  // AND-merge it into the WHERE clause below.  tagCond is a serialized node tree
  // (nodesNodeToString output); parse it back and render to SQL text.
  char*   tagCondSql = NULL;
  int32_t tagCondSqlLen = 0;
  if (pFsParam->tagCond != NULL && pFsParam->tagCondLen > 0) {
    SNode* pCond = NULL;
    char*  missingTagSql = NULL;
    int32_t missingTagSqlLen = 0;
    code = nodesStringToNode(pFsParam->tagCond, &pCond);
    if (code != TSDB_CODE_SUCCESS || pCond == NULL) {
      qError("FederatedScan VStb: nodesStringToNode failed for tagCond, code=0x%x", code);
      nodesDestroyNode(pCond);
      extConnectorFreeTableSchema(pMeta);
      return code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_PAR_INTERNAL_ERROR;
    }
    if (srcType == EXT_SOURCE_INFLUXDB) {
      code = fedScanBuildMissingTagFilter(pCond, pMeta, q, &missingTagSql, &missingTagSqlLen);
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyNode(pCond);
        extConnectorFreeTableSchema(pMeta);
        return code;
      }
    }
    int32_t cap = pFsParam->tagCondLen * 4 + 256;
    tagCondSql = taosMemoryCalloc(1, cap);
    if (tagCondSql == NULL) {
      taosMemoryFree(missingTagSql);
      nodesDestroyNode(pCond);
      extConnectorFreeTableSchema(pMeta);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    int32_t outLen = 0;
    code = nodesNodeToSQL(pCond, tagCondSql, cap, &outLen);
    nodesDestroyNode(pCond);
    if (code != TSDB_CODE_SUCCESS) {
      qError("FederatedScan VStb: nodesNodeToSQL failed for tagCond, code=0x%x", code);
      taosMemoryFree(tagCondSql);
      taosMemoryFree(missingTagSql);
      extConnectorFreeTableSchema(pMeta);
      return code;
    }
    tagCondSqlLen = outLen;
    if (missingTagSqlLen > 0) {
      int32_t mergedCap = tagCondSqlLen + missingTagSqlLen + 8;
      char* mergedSql = taosMemoryCalloc(1, mergedCap);
      if (mergedSql == NULL) {
        taosMemoryFree(tagCondSql);
        taosMemoryFree(missingTagSql);
        extConnectorFreeTableSchema(pMeta);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      if (tagCondSqlLen > 0) {
        snprintf(mergedSql, mergedCap, "%s AND %s", tagCondSql, missingTagSql);
      } else {
        snprintf(mergedSql, mergedCap, "%s", missingTagSql);
      }
      taosMemoryFree(tagCondSql);
      taosMemoryFree(missingTagSql);
      tagCondSql = mergedSql;
      tagCondSqlLen = (int32_t)strlen(mergedSql);
    }
  }

  char*   pushedCondSql = NULL;
  int32_t pushedCondSqlLen = 0;
  bool    pushedCondFullyRendered = false;
  code = fedScanBuildVStbPushedCondSql(pFsParam, pMeta, q, &pushedCondSql, &pushedCondSqlLen,
                                       &pushedCondFullyRendered);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(tagCondSql);
    extConnectorFreeTableSchema(pMeta);
    return code;
  }

  SExtColTypeMapping* mappings = taosMemoryCalloc(totalCols, sizeof(SExtColTypeMapping));
  if (mappings == NULL) {
    taosMemoryFree(tagCondSql);
    taosMemoryFree(pushedCondSql);
    extConnectorFreeTableSchema(pMeta);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t sqlBufLen = 1024 + totalCols * (TSDB_COL_NAME_LEN + 4) + tagCondSqlLen + pushedCondSqlLen + 96;
  char* sqlBuf = taosMemoryCalloc(1, sqlBufLen);
  if (sqlBuf == NULL) {
    taosMemoryFree(tagCondSql);
    taosMemoryFree(pushedCondSql);
    taosMemoryFree(mappings);
    extConnectorFreeTableSchema(pMeta);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t offset = snprintf(sqlBuf, sqlBufLen, "SELECT ");

  // Column 0: the primary timestamp column
  char tsRemoteNameBuf[TSDB_COL_NAME_LEN] = {0};
  {
    SExtColumnDef* pTsCol = &pMeta->pCols[tsIdx];
    const char* remoteName = pTsCol->remoteColName[0] ? pTsCol->remoteColName : pTsCol->colName;
    tstrncpy(tsRemoteNameBuf, remoteName, sizeof(tsRemoteNameBuf));
    offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%c%s%c", q, remoteName, q);
    tstrncpy(mappings[0].extTypeName, pTsCol->extTypeName, sizeof(mappings[0].extTypeName));
    code = extTypeNameToTDengineType(
        srcType, pTsCol->extTypeName, pTsCol->extCharsetName, &mappings[0].tdType);
    if (code != TSDB_CODE_SUCCESS) {
      qError("FederatedScan VStb: type mapping failed for ts col %s type %s, code=0x%x",
             pTsCol->colName, pTsCol->extTypeName, code);
      taosMemoryFree(sqlBuf);
      taosMemoryFree(mappings);
      taosMemoryFree(tagCondSql);
      taosMemoryFree(pushedCondSql);
      extConnectorFreeTableSchema(pMeta);
      return code;
    }
  }

  // Columns 1..N: data columns from colMap
  for (int32_t i = 0; i < numDataCols; i++) {
    SColIdNameKV* pKV = (SColIdNameKV*)taosArrayGet(pFsParam->colMap, i);

    // Find matching column in ext table schema by TDengine-side name
    bool found = false;
    for (int32_t j = 0; j < pMeta->numOfCols; j++) {
      SExtColumnDef* pCol = &pMeta->pCols[j];
      if (strncasecmp(pCol->colName, pKV->colName, TSDB_COL_NAME_LEN) == 0) {
        const char* remoteName = pCol->remoteColName[0] ? pCol->remoteColName : pCol->colName;
        offset += snprintf(sqlBuf + offset, sqlBufLen - offset, ", %c%s%c", q, remoteName, q);

        int32_t mIdx = 1 + i;
        tstrncpy(mappings[mIdx].extTypeName, pCol->extTypeName, sizeof(mappings[mIdx].extTypeName));
        code = extTypeNameToTDengineType(
            srcType, pCol->extTypeName, pCol->extCharsetName, &mappings[mIdx].tdType);
        if (code != TSDB_CODE_SUCCESS) {
          qError("FederatedScan VStb: type mapping failed for col %s type %s, code=0x%x",
                 pKV->colName, pCol->extTypeName, code);
          taosMemoryFree(sqlBuf);
          taosMemoryFree(mappings);
          taosMemoryFree(tagCondSql);
          taosMemoryFree(pushedCondSql);
          extConnectorFreeTableSchema(pMeta);
          return code;
        }
        found = true;
        break;
      }
    }

    if (!found) {
      qError("FederatedScan VStb: column %s not found in remote schema for %s.%s",
             pKV->colName, pFsParam->dbName, pFsParam->tableName);
      taosMemoryFree(sqlBuf);
      taosMemoryFree(mappings);
      taosMemoryFree(tagCondSql);
      taosMemoryFree(pushedCondSql);
      extConnectorFreeTableSchema(pMeta);
      return TSDB_CODE_PAR_INVALID_COLUMN;
    }
  }

  // Build FROM clause — source-type-specific table path
  const char* remoteTable = pMeta->remoteTableName[0] ? pMeta->remoteTableName : pFsParam->tableName;
  switch (srcType) {
    case EXT_SOURCE_MYSQL: {
      // MySQL: `database`.`table`
      const char* db = pFedNode->srcDatabase[0] ? pFedNode->srcDatabase : NULL;
      if (db) {
        offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM `%s`.`%s`", db, remoteTable);
      } else {
        offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM `%s`", remoteTable);
      }
      break;
    }
    case EXT_SOURCE_POSTGRESQL: {
      // PG: "schema"."table"
      const char* schema = pFedNode->srcSchema[0] ? pFedNode->srcSchema : "public";
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM \"%s\".\"%s\"", schema, remoteTable);
      break;
    }
    case EXT_SOURCE_INFLUXDB:
    default: {
      // InfluxDB / others: "measurement"
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM \"%s\"", remoteTable);
      break;
    }
  }

  // Add WHERE clause for time range pushdown (from optimizer scanRange)
  if (pFedNode->scanRange.skey != INT64_MIN || pFedNode->scanRange.ekey != INT64_MAX) {
    bool hasSkey = (pFedNode->scanRange.skey != INT64_MIN);
    bool hasEkey = (pFedNode->scanRange.ekey != INT64_MAX);

    offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " WHERE ");
    if (hasSkey) {
      char skeyBuf[64];
      if (srcType == EXT_SOURCE_INFLUXDB)
        formatTimestampUTC(skeyBuf, sizeof(skeyBuf), pFedNode->scanRange.skey, TSDB_TIME_PRECISION_MILLI);
      else
        formatTimestampLocal(skeyBuf, sizeof(skeyBuf), pFedNode->scanRange.skey, TSDB_TIME_PRECISION_MILLI);
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%c%s%c >= '%s'", q, tsRemoteNameBuf, q, skeyBuf);
    }
    if (hasSkey && hasEkey) {
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " AND ");
    }
    if (hasEkey) {
      char ekeyBuf[64];
      if (srcType == EXT_SOURCE_INFLUXDB)
        formatTimestampUTC(ekeyBuf, sizeof(ekeyBuf), pFedNode->scanRange.ekey, TSDB_TIME_PRECISION_MILLI);
      else
        formatTimestampLocal(ekeyBuf, sizeof(ekeyBuf), pFedNode->scanRange.ekey, TSDB_TIME_PRECISION_MILLI);
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%c%s%c <= '%s'", q, tsRemoteNameBuf, q, ekeyBuf);
    }
  }

  bool hasWhere = (pFedNode->scanRange.skey != INT64_MIN || pFedNode->scanRange.ekey != INT64_MAX);

  // Append VStb data filter fragments that are safe for this foreign source.
  bool hasPushedDataCond = (pushedCondSql != NULL && pushedCondSqlLen > 0);
  if (hasPushedDataCond) {
    offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%s%s",
                       hasWhere ? " AND " : " WHERE ", pushedCondSql);
    hasWhere = true;
  }
  taosMemoryFree(pushedCondSql);
  pushedCondSql = NULL;

  // Append series tag filter (AND-merged with any earlier WHERE if present).
  if (tagCondSql != NULL && tagCondSqlLen > 0) {
    offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%s%s",
                       hasWhere ? " AND " : " WHERE ", tagCondSql);
    hasWhere = true;
  }
  taosMemoryFree(tagCondSql);
  tagCondSql = NULL;

  // ORDER BY ts ASC is required: the vstable sort-merge dedup assumes each
  // external source returns rows in ascending timestamp order.
  offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " ORDER BY %c%s%c ASC", q, tsRemoteNameBuf, q);
  if (pFsParam->rowLimit > 0 && (pFsParam->pPushedCond == NULL || pushedCondFullyRendered)) {
    offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " LIMIT %" PRId64, pFsParam->rowLimit);
  }

  extConnectorFreeTableSchema(pMeta);

  *ppSQL = sqlBuf;
  *ppMappings = mappings;
  *pNumMappings = totalCols;

  qDebug("FederatedScan VStb: generated remote SQL source=%s table=%s sql=[%s]",
         pFsParam->sourceName, pFsParam->tableName, sqlBuf);

  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// getNext — core execution
// ---------------------------------------------------------------------------

void fedScanReleaseFetchedBlock(SFederatedScanOperatorInfo* pInfo) {
  if (pInfo == NULL || pInfo->pFetchedBlock == NULL) {
    return;
  }

  blockDataDestroy(pInfo->pFetchedBlock);
  pInfo->pFetchedBlock = NULL;
}

static int32_t federatedScanGetNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  QRY_PARAM_CHECK(ppRes);

  SFederatedScanOperatorInfo* pInfo     = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  int32_t                     code      = TSDB_CODE_SUCCESS;
  int32_t                     lino      = 0;

  *ppRes = NULL;

  if (pInfo->queryFinished) {
    setOperatorCompleted(pOperator);
    return TSDB_CODE_SUCCESS;
  }

_restart:  // Entry point for phase-2 re-execution in twoPassMode
  // =========================================================================
  // Step 1: First call — connect + generate SQL + issue query
  // =========================================================================
  if (!pInfo->queryStarted) {
    SFederatedScanPhysiNode* pFedNode  = pInfo->pFedScanNode;
    SExtTableNode*           pExtTable = (SExtTableNode*)pFedNode->pExtTable;

    // 1.1 Build connection config from physi node (no Catalog access in taosd)
    SExtSourceCfg cfg = {0};
    if (pExtTable != NULL) {
      tstrncpy(cfg.source_name, pExtTable->sourceName, sizeof(cfg.source_name));
    }
    cfg.source_type = (EExtSourceType)pFedNode->sourceType;
    tstrncpy(cfg.host,             pFedNode->srcHost,     sizeof(cfg.host));
    cfg.port = pFedNode->srcPort;
    tstrncpy(cfg.user,             pFedNode->srcUser,     sizeof(cfg.user));
    tstrncpy(cfg.password,         pFedNode->srcPassword, sizeof(cfg.password));
    tstrncpy(cfg.default_database, pFedNode->srcDatabase, sizeof(cfg.default_database));
    tstrncpy(cfg.default_schema,   pFedNode->srcSchema,   sizeof(cfg.default_schema));
    tstrncpy(cfg.options,          pFedNode->srcOptions,  sizeof(cfg.options));
    cfg.meta_version = pFedNode->metaVersion;
    cfg.query_timeout_ms = tsFederatedQueryQueryTimeoutMs;

    qDebug("FederatedScan: connecting source=%s host=%s:%d user=%s type=%s",
           cfg.source_name, cfg.host, cfg.port, cfg.user,
           fedScanSourceTypeName(pFedNode->sourceType));
        qError("FederatedScan: source=%s options=%s", cfg.source_name,
          cfg.options[0] ? cfg.options : "<empty>");

    // 1.2 Open connection
    code = extConnectorOpen(&cfg, &pInfo->pConnHandle);
    code = fedScanNormalizeInfluxAuthCode(pFedNode->sourceType, cfg.options, code);
    if (code) {
      qError("FederatedScan: connect failed, source=%s host=%s:%d, code=0x%x %s",
             cfg.source_name, cfg.host, cfg.port, code, tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _return);
    }

    // 1.3 Generate remote SQL
    char* remoteSql = NULL;
    if (pFedNode->pRemotePlan == NULL && pInfo->pActiveVStbParam != NULL) {
      // VStb dynamic path: build SQL + col type mappings from ext table schema
      code = fedScanBuildVStbQueryAndMappings(
          pInfo, pInfo->pActiveVStbParam,
          &remoteSql, &pInfo->pDynColMappings, &pInfo->numDynColMappings);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FederatedScan VStb: build query failed, source=%s, code=0x%x %s",
               cfg.source_name, code, tstrerror(code));
        extConnectorClose(pInfo->pConnHandle);
        pInfo->pConnHandle = NULL;
        QUERY_CHECK_CODE(code, lino, _return);
      }
    } else if (pFedNode->pRemotePlan == NULL) {
      // Mode-2 leaf node has no pRemotePlan; SQL will be generated inside the connector.
      qDebug("FederatedScan: pRemotePlan is NULL (Mode-2 leaf), skipping SQL pre-generation, source=%s",
             cfg.source_name);
    } else {
      // Build resolve context from the thread-local scalar extra info so that
      // nodesRemotePlanToSQL can expand REMOTE_VALUE_LIST nodes (IN subquery pushdown).
      SNodesRemoteSQLCtx sqlCtx = {
        .pCtx   = gTaskScalarExtra.pSubJobCtx,
        .fp     = (FResolveRemoteForSQL)gTaskScalarExtra.fp,
        // DS §5.2.6: pass client timezone so timestamp filter values in WHERE
        // are formatted using the client TZ instead of the server-side global TZ.
        .tzName = (pFedNode->timezone[0] != '\0') ? pFedNode->timezone : NULL,
      };
      code = nodesRemotePlanToSQL(
          (const SPhysiNode*)pFedNode->pRemotePlan, pFedNode->sourceType,
          &sqlCtx, &remoteSql);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FederatedScan: nodesRemotePlanToSQL failed, source=%s, code=0x%x %s",
               cfg.source_name, code, tstrerror(code));
        extConnectorClose(pInfo->pConnHandle);
        pInfo->pConnHandle = NULL;
        QUERY_CHECK_CODE(code, lino, _return);
      }
    }

    // 1.4 Issue query — pass pre-computed SQL so the Connector doesn't
    // regenerate it (which would lose the REMOTE_VALUE_LIST resolution).
    SExtConnectorError extErr = {0};
    code = extConnectorExecQuery(pInfo->pConnHandle, pFedNode, remoteSql,
                                 &pInfo->pQueryHandle, &extErr);
    code = fedScanNormalizeInfluxAuthCode(pFedNode->sourceType, cfg.options, code);
    taosMemoryFree(remoteSql);
    remoteSql = NULL;
    if (code) {
      fedScanFormatError(pInfo, &extErr);
      tstrncpy(pTaskInfo->extErrMsg, pInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
      qError("FederatedScan: exec query failed, source=%s, code=0x%x %s",
             cfg.source_name, code, pInfo->extErrMsg[0] ? pInfo->extErrMsg : tstrerror(code));
      extConnectorClose(pInfo->pConnHandle);
      pInfo->pConnHandle = NULL;
      QUERY_CHECK_CODE(code, lino, _return);
    }

    pInfo->queryStarted = true;
    qDebug("FederatedScan: query started, source=%s", cfg.source_name);
  }

  // =========================================================================
  // Step 2: Fetch next data block
  // =========================================================================
_fetchNext:
  {
    SSDataBlock*        pBlock   = NULL;
    SExtConnectorError  fetchErr = {0};
    int64_t             startTs  = taosGetTimestampUs();

    fedScanReleaseFetchedBlock(pInfo);

    // Use dynamic col mappings (VStb) if available, otherwise static from physi node
    const SExtColTypeMapping* pUseMappings = pInfo->pDynColMappings
        ? pInfo->pDynColMappings : pInfo->pFedScanNode->pColTypeMappings;
    int32_t numUseMappings = pInfo->pDynColMappings
        ? pInfo->numDynColMappings : pInfo->pFedScanNode->numColTypeMappings;

    code = extConnectorFetchBlock(pInfo->pQueryHandle,
                                  pUseMappings,
                                  numUseMappings,
                                  &pBlock, &fetchErr);
    code = fedScanNormalizeInfluxAuthCode(
      pInfo->pFedScanNode->sourceType, pInfo->pFedScanNode->srcOptions, code);
    pInfo->elapsedTimeUs += (taosGetTimestampUs() - startTs);

    if (code) {
      fedScanFormatError(pInfo, &fetchErr);
      tstrncpy(pTaskInfo->extErrMsg, pInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
      qError("FederatedScan: fetch failed, code=0x%x %s", code,
             pInfo->extErrMsg[0] ? pInfo->extErrMsg : tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _return);
    }

    if (pBlock == NULL) {
      // EOF
      if (pInfo->twoPassMode && !pInfo->twoPassPhase1Done) {
        // Phase-1 (PRE_SCAN) exhausted.  Close the current connection and
        // immediately restart for phase-2 (MAIN_SCAN) without returning NULL
        // to the caller.  PERCENTILE needs a continuous stream of
        // PRE_SCAN blocks followed by MAIN_SCAN blocks within a single
        // nextGroupedResult() loop.
        pInfo->twoPassPhase1Done = true;
        if (pInfo->pQueryHandle) {
          extConnectorCloseQuery(pInfo->pQueryHandle);
          pInfo->pQueryHandle = NULL;
        }
        extConnectorClose(pInfo->pConnHandle);
        pInfo->pConnHandle  = NULL;
        pInfo->queryStarted = false;
        goto _restart;
      }
      // True EOF (phase-2 done, or non-twoPass mode)
      pInfo->queryFinished = true;
      setOperatorCompleted(pOperator);
      qDebug("FederatedScan: EOF, totalRows=%" PRId64 ", blocks=%" PRId64
             ", elapsed=%" PRId64 "us",
             pInfo->fetchedRows, pInfo->fetchBlockCount, pInfo->elapsedTimeUs);
      *ppRes = NULL;
      return TSDB_CODE_SUCCESS;
    }

    pInfo->pFetchedBlock = pBlock;

    // Set scan flag for PERCENTILE two-pass support.
    // Phase-1 returns PRE_SCAN (stage-0: collect min/max/count).
    // Phase-2 returns MAIN_SCAN (stage-1: fill tMemBucket).
    if (pInfo->twoPassMode) {
      pBlock->info.scanFlag = pInfo->twoPassPhase1Done ? MAIN_SCAN : PRE_SCAN;
    }

    // Connectors emit TIMESTAMP values in their source-native precision
    // (PG/MySQL = µs, InfluxDB = ns) and tag the block column accordingly.
    // The virtual table that consumes this block may declare a different
    // precision, so convert each TIMESTAMP cell here.  Limited to fed
    // scans feeding a VTable scan so direct external-table queries are
    // unaffected.
    //
    //   * VStb dispatch path: destination precision is propagated per call
    //     via pFsParam->dstPrecision (set by the upstream VTable scan from
    //     the input block's ts slot precision).
    //   * Static (non-VStb) path: the planner records the destination
    //     precision in pFedScanNode->pColTypeMappings[c].tdType.precision,
    //     which mirrors the vtable column's declared precision.
    if (pInfo->underVTableScan) {
      const SExtColTypeMapping* pConvMappings = pUseMappings;
      int32_t                   numConvMappings = numUseMappings;
      int8_t                    vstbDstPrec = -1;
      if (pInfo->pActiveVStbParam != NULL) {
        vstbDstPrec = pInfo->pActiveVStbParam->dstPrecision;
      }
      int32_t numCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
      for (int32_t c = 0; c < numCols; c++) {
        SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, c);
        if (pColData == NULL) continue;
        if (pColData->info.type != TSDB_DATA_TYPE_TIMESTAMP) continue;

        int8_t srcPrec = pColData->info.precision;
        int8_t dstPrec;
        if (vstbDstPrec == TSDB_TIME_PRECISION_MILLI ||
            vstbDstPrec == TSDB_TIME_PRECISION_MICRO ||
            vstbDstPrec == TSDB_TIME_PRECISION_NANO) {
          dstPrec = vstbDstPrec;
        } else if (c < numConvMappings) {
          dstPrec = pConvMappings[c].tdType.precision;
        } else {
          continue;
        }

        if (srcPrec == dstPrec) continue;
        if (srcPrec != TSDB_TIME_PRECISION_MILLI &&
            srcPrec != TSDB_TIME_PRECISION_MICRO &&
            srcPrec != TSDB_TIME_PRECISION_NANO) {
          continue;
        }
        if (dstPrec != TSDB_TIME_PRECISION_MILLI &&
            dstPrec != TSDB_TIME_PRECISION_MICRO &&
            dstPrec != TSDB_TIME_PRECISION_NANO) {
          continue;
        }

        for (int32_t r = 0; r < pBlock->info.rows; r++) {
          if (colDataIsNull_s(pColData, r)) continue;
          int64_t* p = (int64_t*)colDataGetNumData(pColData, r);
          *p = convertTimePrecision(*p, srcPrec, dstPrec);
        }
        pColData->info.precision = dstPrec;
      }
    }

    // Tag the output block with this operator's data-block ID so the
    // VirtualTableScan dataSlotMap can route columns to the correct output
    // slots when multiple FederatedScan operators coexist.
    pBlock->info.id.blockId = pOperator->resultDataBlockId;

    pInfo->fetchedRows += pBlock->info.rows;
    pInfo->fetchBlockCount++;

    qDebug("FQ-Fetch: source=%s block#=%" PRId64 " rows=%d totalRows=%" PRId64,
           pInfo->pFedScanNode->srcHost, pInfo->fetchBlockCount,
           (int)pBlock->info.rows, pInfo->fetchedRows);
    printDataBlock(pBlock, "FQ-ExtData", GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);

    // Extend block with extra columns for pushed-down expression slots
    // (e.g., CASE WHEN results needed by the parent Aggregate operator).
    // Pseudo-column slots (TBNAME) are filled with the external table name;
    // other extra slots are initialised to NULL.
    SDataBlockDescNode* pDesc = pInfo->pFedScanNode->node.pOutputDataBlockDesc;
    if (pDesc != NULL) {
      int32_t descSlots = LIST_LENGTH(pDesc->pSlots);
      int32_t blockCols = taosArrayGetSize(pBlock->pDataBlock);
      if (descSlots > blockCols) {
        // Pre-compute external table name for TBNAME pseudo-column fill.
        const char* extTableName = NULL;
        int32_t     extTableNameLen = 0;
        SExtTableNode* pExtTbl = (SExtTableNode*)pInfo->pFedScanNode->pExtTable;
        if (pExtTbl != NULL) {
          extTableName = (pExtTbl->remoteTableName[0] != '\0')
                           ? pExtTbl->remoteTableName
                           : pExtTbl->table.tableName;
          extTableNameLen = (int32_t)strlen(extTableName);
        }

        // Iterate over the extra slots in the descriptor and append empty columns
        int32_t idx = 0;
        SNode* pNode = NULL;
        FOREACH(pNode, pDesc->pSlots) {
          if (idx >= blockCols) {
            SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
            SColumnInfoData colInfo = createColumnInfoData(
                pSlot->dataType.type, pSlot->dataType.bytes, (int16_t)(idx + 1));
            code = blockDataAppendColInfo(pBlock, &colInfo);
            QUERY_CHECK_CODE(code, lino, _return);
            SColumnInfoData* pNewCol = taosArrayGetLast(pBlock->pDataBlock);
            if (pNewCol == NULL) { idx++; continue; }
            code = colInfoDataEnsureCapacity(pNewCol, pBlock->info.rows, true);
            QUERY_CHECK_CODE(code, lino, _return);

            bool slotFilled = false;
            code = fedScanFillConstValueSlot(pInfo->pFedScanNode, pSlot, pNewCol, (int32_t)pBlock->info.rows,
                                             &slotFilled);
            QUERY_CHECK_CODE(code, lino, _return);

            // Fill TBNAME pseudo-column with the external table name.
            if (!slotFilled && extTableName != NULL && strcasecmp(pSlot->name, "tbname") == 0 &&
                IS_VAR_DATA_TYPE(pSlot->dataType.type)) {
              for (int32_t r = 0; r < (int32_t)pBlock->info.rows; ++r) {
                code = varColSetVarData(pNewCol, r, extTableName, extTableNameLen, false);
                QUERY_CHECK_CODE(code, lino, _return);
              }
            }
          }
          idx++;
        }
      }

      // If a slot corresponds to a folded VALUE expression in the remote
      // output target list, overwrite fetched data with the constant so
      // expression-only queries keep TDengine local semantics.
      code = fedScanOverwriteConstSlots(pInfo->pFedScanNode, pDesc, pBlock);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    // Apply local filter for conditions that could not be pushed down
    // to the remote source (e.g., like_in_set, regexp_in_set).
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _return);
      if (pBlock->info.rows == 0) {
        // All rows filtered out — fetch next block
        goto _fetchNext;
      }
      qDebug("FQ-Fetch: after local filter rows=%d", (int)pBlock->info.rows);
      printDataBlock(pBlock, "FQ-Filtered", GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
    }

    SStreamRuntimeInfo* pStreamRuntimeInfo = pTaskInfo->pStreamRuntimeInfo;
    if (pStreamRuntimeInfo != NULL && pStreamRuntimeInfo->inputStatsFp != NULL) {
      pStreamRuntimeInfo->inputStatsFp(pStreamRuntimeInfo->pInputStatsParam, pBlock->info.rows, 1);
    }
    *ppRes = pBlock;
  }

  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

// ---------------------------------------------------------------------------
// getNextExtFn — VTable parameterized fetch (DS §5.5.6)
// ---------------------------------------------------------------------------

// Build a table key from SForeignScanOperatorParam for context lookup.
static void fedScanBuildTableKey(const SForeignScanOperatorParam* pFsParam, char* buf, int32_t bufLen) {
  uint32_t tagCondHash = 0;
  uint32_t pushedCondHash = 0;

  if (pFsParam->tagCond != NULL && pFsParam->tagCondLen > 0) {
    tagCondHash = MurmurHash3_32(pFsParam->tagCond, (uint32_t)pFsParam->tagCondLen);
  }

  if (pFsParam->pPushedCond != NULL) {
    char* pCondStr = NULL;
    if (nodesNodeToString(pFsParam->pPushedCond, false, &pCondStr, NULL) == TSDB_CODE_SUCCESS &&
        pCondStr != NULL) {
      pushedCondHash = MurmurHash3_32(pCondStr, (uint32_t)strlen(pCondStr));
      taosMemoryFree(pCondStr);
    }
  }

  snprintf(buf, bufLen, "%s.%s.%s.%u.%u.%" PRId64, pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName,
           tagCondHash, pushedCondHash, pFsParam->rowLimit > 0 ? pFsParam->rowLimit : 0);
}

#define FED_SCAN_TABLE_CTX_MAX_SIZE 64

static void fedScanCloseTableCtx(SFedScanTableCtx* pCtx) {
  if (pCtx == NULL) {
    return;
  }

  if (pCtx->pFetchedBlock) {
    blockDataDestroy(pCtx->pFetchedBlock);
    pCtx->pFetchedBlock = NULL;
  }
  if (pCtx->pQueryHandle) {
    extConnectorCloseQuery(pCtx->pQueryHandle);
    pCtx->pQueryHandle = NULL;
  }
  if (pCtx->pConnHandle) {
    extConnectorClose(pCtx->pConnHandle);
    pCtx->pConnHandle = NULL;
  }
  taosMemoryFreeClear(pCtx->pDynColMappings);
  pCtx->numDynColMappings = 0;
}

static void fedScanCloseActiveHandles(SFederatedScanOperatorInfo* pInfo) {
  if (pInfo == NULL) {
    return;
  }

  fedScanReleaseFetchedBlock(pInfo);

  SFedScanTableCtx ctx = {
      .pConnHandle = pInfo->pConnHandle,
      .pQueryHandle = pInfo->pQueryHandle,
      .pDynColMappings = pInfo->pDynColMappings,
      .numDynColMappings = pInfo->numDynColMappings,
  };
  fedScanCloseTableCtx(&ctx);

  pInfo->pConnHandle = NULL;
  pInfo->pQueryHandle = NULL;
  pInfo->pDynColMappings = NULL;
  pInfo->numDynColMappings = 0;
}

// Evict one entry from pTableCtxMap (closes its connections) to stay within limit.
static int32_t fedScanEvictOneCtx(SFederatedScanOperatorInfo* pInfo) {
  if (pInfo->pTableCtxMap == NULL) return TSDB_CODE_SUCCESS;
  void* pIter = taosHashIterate(pInfo->pTableCtxMap, NULL);
  if (pIter == NULL) return TSDB_CODE_SUCCESS;

  // Get key of first entry (arbitrary eviction — hash iteration order)
  size_t keyLen = 0;
  char*  key = taosHashGetKey(pIter, &keyLen);

  SFedScanTableCtx* pCtx = (SFedScanTableCtx*)pIter;
  fedScanCloseTableCtx(pCtx);

  // Must cancel iteration before removing
  taosHashCancelIterate(pInfo->pTableCtxMap, pIter);
  int32_t code = taosHashRemove(pInfo->pTableCtxMap, key, keyLen);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_NOT_FOUND) {
    qError("%s failed to remove evicted federated scan ctx since %s", __func__, tstrerror(code));
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

// Save current operator state into the active table's context entry.
int32_t fedScanSaveActiveCtx(SFederatedScanOperatorInfo* pInfo) {
  if (pInfo->pTableCtxMap == NULL || pInfo->activeTableKey[0] == '\0') return TSDB_CODE_SUCCESS;

  size_t             keyLen = strlen(pInfo->activeTableKey);
  SFedScanTableCtx*  pOld = taosHashGet(pInfo->pTableCtxMap, pInfo->activeTableKey, keyLen);
  SFedScanTableCtx   ctx = {
      .pConnHandle = pInfo->pConnHandle,
      .pQueryHandle = pInfo->pQueryHandle,
      .queryStarted = pInfo->queryStarted,
      .queryFinished = pInfo->queryFinished,
      .twoPassPhase1Done = pInfo->twoPassPhase1Done,
      .pDynColMappings = pInfo->pDynColMappings,
      .numDynColMappings = pInfo->numDynColMappings,
      .pFetchedBlock = pInfo->pFetchedBlock,
  };

  // Evict if at capacity and this is a new entry
  if (pOld == NULL && taosHashGetSize(pInfo->pTableCtxMap) >= FED_SCAN_TABLE_CTX_MAX_SIZE) {
    int32_t code = fedScanEvictOneCtx(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pOld != NULL) {
    *pOld = ctx;
  } else {
    int32_t code = taosHashPut(pInfo->pTableCtxMap, pInfo->activeTableKey, keyLen, &ctx, sizeof(ctx));
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed to save federated scan ctx for table %s since %s", __func__, pInfo->activeTableKey,
             tstrerror(code));
      return code;
    }
  }

  // Detach from operator so close doesn't double-free
  pInfo->pConnHandle = NULL;
  pInfo->pQueryHandle = NULL;
  pInfo->pDynColMappings = NULL;
  pInfo->numDynColMappings = 0;
  pInfo->pFetchedBlock = NULL;
  return TSDB_CODE_SUCCESS;
}

// Restore a table context into the operator state.
static void fedScanRestoreCtx(SFederatedScanOperatorInfo* pInfo, const char* tableKey) {
  if (pInfo->pTableCtxMap == NULL) return;
  SFedScanTableCtx* pCtx = taosHashGet(pInfo->pTableCtxMap, tableKey, strlen(tableKey));
  if (pCtx != NULL) {
    pInfo->pConnHandle      = pCtx->pConnHandle;
    pInfo->pQueryHandle     = pCtx->pQueryHandle;
    pInfo->queryStarted     = pCtx->queryStarted;
    pInfo->queryFinished    = pCtx->queryFinished;
    pInfo->twoPassPhase1Done = pCtx->twoPassPhase1Done;
    pInfo->pDynColMappings  = pCtx->pDynColMappings;
    pInfo->numDynColMappings = pCtx->numDynColMappings;
    pInfo->pFetchedBlock = pCtx->pFetchedBlock;
  } else {
    pInfo->pConnHandle   = NULL;
    pInfo->pQueryHandle  = NULL;
    pInfo->queryStarted  = false;
    pInfo->queryFinished = false;
    pInfo->twoPassPhase1Done = false;
    pInfo->pDynColMappings  = NULL;
    pInfo->numDynColMappings = 0;
    pInfo->pFetchedBlock = NULL;
  }
  tstrncpy(pInfo->activeTableKey, tableKey, sizeof(pInfo->activeTableKey));
}

static int32_t federatedScanGetNextExtFn(SOperatorInfo*  pOperator,
                                          SOperatorParam* pParam,
                                          SSDataBlock**   ppRes) {
  QRY_PARAM_CHECK(ppRes);

  SFederatedScanOperatorInfo* pInfo = pOperator->info;

  if (pParam != NULL && pParam->opType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    // Multi-table dispatch from VStb: use per-table context caching
    SForeignScanOperatorParam* pFsParam = (SForeignScanOperatorParam*)pParam->value;
    char tableKey[TSDB_EXT_SOURCE_NAME_LEN + TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 32] = {0};
    fedScanBuildTableKey(pFsParam, tableKey, sizeof(tableKey));

    if (pInfo->pTableCtxMap == NULL) {
      pInfo->pTableCtxMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
      if (pInfo->pTableCtxMap == NULL) return terrno;
    }

    if (strcmp(pInfo->activeTableKey, tableKey) != 0) {
      // Switching tables: save current context, restore target context
      int32_t code = fedScanSaveActiveCtx(pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      fedScanRestoreCtx(pInfo, tableKey);
    }

    // Store VStb param for dynamic SQL generation in getNext
    pInfo->pActiveVStbParam = pFsParam;

    return federatedScanGetNext(pOperator, ppRes);
  }

  // Legacy path: single-table or non-VStb param — tear down and restart
  if (pParam != NULL) {
    bool paramChanged = (pInfo->queryStarted);
    if (paramChanged) {
      if (pInfo->pQueryHandle) {
        extConnectorCloseQuery(pInfo->pQueryHandle);
        pInfo->pQueryHandle = NULL;
      }
      if (pInfo->pConnHandle) {
        extConnectorClose(pInfo->pConnHandle);
        pInfo->pConnHandle = NULL;
      }
      pInfo->queryStarted  = false;
      pInfo->queryFinished = false;
    }
  }

  return federatedScanGetNext(pOperator, ppRes);
}

// ---------------------------------------------------------------------------
// Reset FederatedScan state for new VStb child — closes cached connections
// ---------------------------------------------------------------------------

void federatedScanResetForNewChild(SOperatorInfo* pOperator) {
  SFederatedScanOperatorInfo* pInfo = (SFederatedScanOperatorInfo*)pOperator->info;
  if (!pInfo) return;

  // Save the active context into the map first so all handles are in one place
  int32_t code = fedScanSaveActiveCtx(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to save active federated scan ctx since %s", __func__, tstrerror(code));
  }
  fedScanReleaseFetchedBlock(pInfo);

  // Close all cached contexts
  if (pInfo->pTableCtxMap) {
    void* pIter = taosHashIterate(pInfo->pTableCtxMap, NULL);
    while (pIter != NULL) {
      SFedScanTableCtx* pCtx = (SFedScanTableCtx*)pIter;
      fedScanCloseTableCtx(pCtx);
      pIter = taosHashIterate(pInfo->pTableCtxMap, pIter);
    }
    taosHashClear(pInfo->pTableCtxMap);
  }
  fedScanCloseActiveHandles(pInfo);

  // Reset remaining state after cached and active handles have been closed.
  pInfo->queryStarted = false;
  pInfo->queryFinished = false;
  pInfo->activeTableKey[0] = '\0';
  pInfo->pActiveVStbParam = NULL;
}

// ---------------------------------------------------------------------------
// close — release all resources
// ---------------------------------------------------------------------------

static void federatedScanClose(void* param) {
  SFederatedScanOperatorInfo* pInfo = (SFederatedScanOperatorInfo*)param;
  if (!pInfo) return;

  if (pInfo->pTableCtxMap) {
    // Save the currently-active context into the map so every handle is
    // reachable through a single iteration.  Without this, the last child's
    // connection/query handles live only in pInfo and would be leaked.
    int32_t code = fedScanSaveActiveCtx(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed to save active federated scan ctx since %s", __func__, tstrerror(code));
    }

    void* pIter = taosHashIterate(pInfo->pTableCtxMap, NULL);
    while (pIter != NULL) {
      SFedScanTableCtx* pCtx = (SFedScanTableCtx*)pIter;
      fedScanCloseTableCtx(pCtx);
      pIter = taosHashIterate(pInfo->pTableCtxMap, pIter);
    }
    taosHashCleanup(pInfo->pTableCtxMap);
    pInfo->pTableCtxMap = NULL;
    fedScanCloseActiveHandles(pInfo);
  } else {
    fedScanCloseActiveHandles(pInfo);
  }

  taosMemoryFreeClear(pInfo);
}

// ---------------------------------------------------------------------------
// getExplainFn — verbose EXPLAIN ANALYZE output
// ---------------------------------------------------------------------------

static int32_t federatedScanGetExplainInfo(SOperatorInfo* pOperator,
                                           void**         ppOptrExplain,
                                           uint32_t*      pLen) {
  SFederatedScanOperatorInfo* pInfo = pOperator->info;

  SFederatedScanExplainInfo* pExInfo =
      taosMemoryCalloc(1, sizeof(SFederatedScanExplainInfo));
  if (!pExInfo) return terrno;

  pExInfo->fetchedRows     = pInfo->fetchedRows;
  pExInfo->fetchBlockCount = pInfo->fetchBlockCount;
  pExInfo->elapsedTimeUs   = pInfo->elapsedTimeUs;

  *ppOptrExplain = pExInfo;
  *pLen          = (uint32_t)sizeof(SFederatedScanExplainInfo);
  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// createFederatedScanOperatorInfo — public factory function
// ---------------------------------------------------------------------------

int32_t createFederatedScanOperatorInfo(SOperatorInfo*           pDownstream,
                                         SFederatedScanPhysiNode* pFedScanNode,
                                         SExecTaskInfo*           pTaskInfo,
                                         SOperatorInfo**          pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                     code  = TSDB_CODE_SUCCESS;
  int32_t                     lino  = 0;
  SFederatedScanOperatorInfo* pInfo = NULL;
  SOperatorInfo*              pOperator = NULL;

  pInfo = taosMemoryCalloc(1, sizeof(SFederatedScanOperatorInfo));
  QUERY_CHECK_NULL(pInfo, code, lino, _error, terrno);

  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  QUERY_CHECK_NULL(pOperator, code, lino, _error, terrno);

  initOperatorCostInfo(pOperator);

  // Store reference to physi node (not owned — lifetime managed by plan)
  pInfo->pFedScanNode = pFedScanNode;
  pInfo->underVTableScan = pFedScanNode->underVTableScan;

  qError("FqExec ENTRY: pColTypeMappings=%p, numColTypeMappings=%d, pRemotePlan=%p, pScanCols len=%d",
         (void*)pFedScanNode->pColTypeMappings,
         pFedScanNode->numColTypeMappings,
         (void*)pFedScanNode->pRemotePlan,
         pFedScanNode->pScanCols ? (int)LIST_LENGTH(pFedScanNode->pScanCols) : -1);

  // pColTypeMappings and pOutputDataBlockDesc are fully computed by the planner
  // and preserved through JSON serialization.  The executor uses them as-is.

  // Build pColTypeMappings if not already set.
  // The planner populates pColTypeMappings before serialization, but the JSON codec
  // does not serialize this raw C-array field, so it arrives as NULL after deserialization.
  //
  // When pRemotePlan is non-NULL, the remote connector executes the full pushed-down plan
  // and returns exactly the topmost operator's output columns.  Use the topmost physical
  // node's pTargets (for Sort) or pProjections (for Project) to determine output columns.
  // Do NOT use pScanCols or pOutputDataBlockDesc — they may include extra ORDER-BY columns.
  if (pFedScanNode->pColTypeMappings == NULL) {
    SNodeList* pOutputCols = NULL;

    if (pFedScanNode->pRemotePlan != NULL) {
      // Get output column list from the topmost remote physical node
      ENodeType remoteType = nodeType(pFedScanNode->pRemotePlan);
      if (remoteType == QUERY_NODE_PHYSICAL_PLAN_SORT) {
        SSortPhysiNode* pSort = (SSortPhysiNode*)pFedScanNode->pRemotePlan;
        pOutputCols = pSort->pTargets;
      } else if (remoteType == QUERY_NODE_PHYSICAL_PLAN_PROJECT) {
        SProjectPhysiNode* pProj = (SProjectPhysiNode*)pFedScanNode->pRemotePlan;
        pOutputCols = pProj->pProjections;
      }
    }

    if (pOutputCols != NULL && LIST_LENGTH(pOutputCols) > 0) {
      // Build pColTypeMappings from the remote plan's output column list
      int32_t numCols = LIST_LENGTH(pOutputCols);
      pFedScanNode->pColTypeMappings =
          (SExtColTypeMapping*)taosMemoryCalloc(numCols, sizeof(SExtColTypeMapping));
      QUERY_CHECK_NULL(pFedScanNode->pColTypeMappings, code, lino, _error, TSDB_CODE_OUT_OF_MEMORY);
      pFedScanNode->numColTypeMappings = numCols;
      int32_t colIdx = 0;
      SNode*  pNode = NULL;
      FOREACH(pNode, pOutputCols) {
        SNode* pExpr = pNode;
        if (QUERY_NODE_TARGET == nodeType(pNode)) {
          pExpr = ((STargetNode*)pNode)->pExpr;
        }
        if (pExpr != NULL) {
          pFedScanNode->pColTypeMappings[colIdx].tdType = ((SExprNode*)pExpr)->resType;
        }
        ++colIdx;
      }
    } else if (pFedScanNode->pScanCols != NULL) {
      // Fallback: no pRemotePlan — use pScanCols (plain scan without pushdown)
      int32_t numCols = LIST_LENGTH(pFedScanNode->pScanCols);
      if (numCols > 0) {
        pFedScanNode->pColTypeMappings =
            (SExtColTypeMapping*)taosMemoryCalloc(numCols, sizeof(SExtColTypeMapping));
        QUERY_CHECK_NULL(pFedScanNode->pColTypeMappings, code, lino, _error, TSDB_CODE_OUT_OF_MEMORY);
        pFedScanNode->numColTypeMappings = numCols;
        int32_t colIdx = 0;
        SNode*  pColNode = NULL;
        FOREACH(pColNode, pFedScanNode->pScanCols) {
          SNode* pExpr = pColNode;
          if (QUERY_NODE_TARGET == nodeType(pColNode)) {
            pExpr = ((STargetNode*)pColNode)->pExpr;
          }
          if (pExpr != NULL && QUERY_NODE_COLUMN == nodeType(pExpr)) {
            SColumnNode* pCol = (SColumnNode*)pExpr;
            pFedScanNode->pColTypeMappings[colIdx].tdType = pCol->node.resType;
          }
          ++colIdx;
        }
      }
    }
  }

  // When pRemotePlan exists, the remote query returns fewer columns than pScanCols.
  // Rebuild pOutputDataBlockDesc to match the actual output (pColTypeMappings) so
  // the data dispatcher's schema validation passes.
  // IMPORTANT: preserve any extra slots added by the planner's pushdownDataBlockSlots
  // (e.g., for pre-calculated expressions like CASE WHEN used in SUM).
  if (pFedScanNode->pRemotePlan != NULL && pFedScanNode->numColTypeMappings > 0) {
    SDataBlockDescNode* pDesc = pFedScanNode->node.pOutputDataBlockDesc;
    int32_t numColMappings = pFedScanNode->numColTypeMappings;
    if (pDesc != NULL && LIST_LENGTH(pDesc->pSlots) != numColMappings) {
      // Collect any pushed-down expression slots (slotId >= numColMappings)
      // that the planner added via pushdownDataBlockSlots.
      SNodeList* pExtraSlots = NULL;
      if ((int32_t)LIST_LENGTH(pDesc->pSlots) > numColMappings) {
        SNode* pNode = NULL;
        FOREACH(pNode, pDesc->pSlots) {
          SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
          if (pSlot->slotId >= numColMappings) {
            SNode* pCopy = NULL;
            code = nodesCloneNode((SNode*)pSlot, &pCopy);
            if (code == TSDB_CODE_SUCCESS && pCopy != NULL) {
              if (pExtraSlots == NULL) {
                code = nodesMakeList(&pExtraSlots);
                QUERY_CHECK_CODE(code, lino, _error);
              }
              code = nodesListStrictAppend(pExtraSlots, pCopy);
              QUERY_CHECK_CODE(code, lino, _error);
            }
          }
        }
      }

      nodesDestroyList(pDesc->pSlots);
      pDesc->pSlots = NULL;
      pDesc->totalRowSize = 0;
      pDesc->outputRowSize = 0;

      code = nodesMakeList(&pDesc->pSlots);
      QUERY_CHECK_NULL(pDesc->pSlots, code, lino, _error, terrno);

      for (int16_t si = 0; si < numColMappings; ++si) {
        SSlotDescNode* pSlot = NULL;
        code = nodesMakeNode(QUERY_NODE_SLOT_DESC, (SNode**)&pSlot);
        QUERY_CHECK_NULL(pSlot, code, lino, _error, terrno);
        pSlot->slotId = si;
        pSlot->dataType = pFedScanNode->pColTypeMappings[si].tdType;
        pSlot->output = true;
        pSlot->reserve = false;
        code = nodesListStrictAppend(pDesc->pSlots, (SNode*)pSlot);
        QUERY_CHECK_CODE(code, lino, _error);
        pDesc->totalRowSize += pSlot->dataType.bytes;
        pDesc->outputRowSize += pSlot->dataType.bytes;
      }

      // Restore pushed-down expression slots
      if (pExtraSlots != NULL) {
        SNode* pNode = NULL;
        FOREACH(pNode, pExtraSlots) {
          SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
          pDesc->totalRowSize += pSlot->dataType.bytes;
          pDesc->outputRowSize += pSlot->dataType.bytes;
        }
        code = nodesListStrictAppendList(pDesc->pSlots, pExtraSlots);
        QUERY_CHECK_CODE(code, lino, _error);
      }
    }
  }

  // FederatedScan is a leaf node — no downstream
  pInfo->twoPassMode = pFedScanNode->twoPassMode;
  pInfo->twoPassPhase1Done = false;

  setOperatorInfo(pOperator, "FederatedScanOperator",
                  QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN,
                  false, OP_NOT_OPENED, pInfo, pTaskInfo);

  // Initialize local filter from pConditions (for conditions like like_in_set
  // that cannot be pushed down to the remote source)
  if (pFedScanNode->node.pConditions != NULL) {
    code = filterInitFromNode((SNode*)pFedScanNode->node.pConditions,
                              &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pOperator->fpSet = createOperatorFpSet(
      optrDummyOpenFn,           // open: lazy — real connect happens in getNext
      federatedScanGetNext,      // getNext
      NULL,                      // cleanupFn: none
      federatedScanClose,        // close: release connector handles
      optrDefaultBufFn,          // reqBuf
      federatedScanGetExplainInfo, // explain ANALYZE
      federatedScanGetNextExtFn, // getNextExt: VTable parameterized fetch
      NULL                       // notify
  );

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  taosMemoryFree(pInfo);
  if (pOperator) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}
