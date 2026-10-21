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

// federatedscanoperator.c  -  FederatedScan executor operator
//
// Responsibilities (DS section 5.2.4):
//   - Lazy-connect to external data source on first getNext call
//   - Generate remote SQL via nodesRemotePlanToSQL and cache for EXPLAIN/log
//   - Execute query via extConnectorExecQuery(pHandle, pNode, ...)
//   - Fetch SSDataBlock results via extConnectorFetchBlock
//   - Propagate errors (including remote error strings) to pTaskInfo->extErrMsg
//   - Release all resources in close

#include <ctype.h>

#include "executorInt.h"
#include "extTypeMap.h"
#include "extError.h"
#include "filter.h"
#include "operator.h"
#include "osTimezone.h"
#include "query.h"
#include "querynodes.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tcompare.h"
#include "tglobal.h"
#include "ttime.h"
#include "nodes.h"

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

enum { FED_SCAN_RESULT_BLOCK_ROWS = 4096 };

// Map EExtSourceType to a human-readable string for logging and EXPLAIN output.
// Identifier quoting for the hand-built VStb remote SQL.  MySQL uses backticks,
// SQL Server brackets (a literal ']' is escaped by doubling), PostgreSQL and
// InfluxDB double quotes.
typedef struct SFedScanQuote {
  char open;
  char close;
} SFedScanQuote;

static SFedScanQuote fedScanQuoteFor(EExtSourceType srcType) {
  SFedScanQuote q = {'"', '"'};
  if (EXT_SOURCE_MYSQL == srcType) {
    q.open = '`';
    q.close = '`';
  } else if (EXT_SOURCE_SQLSERVER == srcType) {
    q.open = '[';
    q.close = ']';
  }
  return q;
}

// SQL dialect of a source type, for the renderers embedded in this file.
static EExtSQLDialect fedScanDialectFor(EExtSourceType srcType) {
  switch (srcType) {
    case EXT_SOURCE_MYSQL:      return EXT_SQL_DIALECT_MYSQL;
    case EXT_SOURCE_POSTGRESQL: return EXT_SQL_DIALECT_POSTGRES;
    case EXT_SOURCE_INFLUXDB:   return EXT_SQL_DIALECT_INFLUXQL;
    case EXT_SOURCE_SQLSERVER:  return EXT_SQL_DIALECT_SQLSERVER;
    default:                    return EXT_SQL_DIALECT_MYSQL;
  }
}

static const char* fedScanSourceTypeName(int8_t srcType) {
  switch ((EExtSourceType)srcType) {
    case EXT_SOURCE_MYSQL:      return "mysql";
    case EXT_SOURCE_POSTGRESQL: return "postgresql";
    case EXT_SOURCE_INFLUXDB:   return "influxdb";
    case EXT_SOURCE_SQLSERVER:  return "sqlserver";
    default:                    return "unknown";
  }
}

static int32_t fedScanFindVstbOutputIndex(const SFederatedScanPhysiNode* pScan, col_id_t colId,
                                          bool primaryTs) {
  int32_t index = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pScan->pScanCols) {
    const SNode* pExpr = pNode;
    if (QUERY_NODE_TARGET == nodeType((SNode*)pExpr)) {
      pExpr = ((STargetNode*)pExpr)->pExpr;
    }
    if (NULL != pExpr && QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
      const SColumnNode* pCol = (const SColumnNode*)pExpr;
      if ((primaryTs && (pCol->isPrimTs || PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId)) ||
          (!primaryTs && pCol->colId == colId)) {
        return index;
      }
    } else if (!primaryTs && 0 == colId && NULL != pExpr &&
               QUERY_NODE_FUNCTION == nodeType((SNode*)pExpr) &&
               FUNCTION_TYPE_TBNAME == ((const SFunctionNode*)pExpr)->funcType) {
      // VStableAgg preserves TBNAME as a fixed pseudo-function scan slot,
      // while the runtime tag payload addresses it with the logical cid 0.
      return index;
    }
    ++index;
  }
  return -1;
}

static int32_t fedScanGetVstbOutputType(const SFederatedScanPhysiNode* pScan, col_id_t colId,
                                        bool primaryTs, SDataType* pType) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pScan, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pType, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pScan->node.pOutputDataBlockDesc, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pScan->node.pOutputDataBlockDesc->pSlots, code, lino, _return, TSDB_CODE_INVALID_PARA);

  int32_t outputIdx = fedScanFindVstbOutputIndex(pScan, colId, primaryTs);
  if (outputIdx < 0 && primaryTs && LIST_LENGTH(pScan->node.pOutputDataBlockDesc->pSlots) > 0) {
    outputIdx = 0;
  }
  SSlotDescNode* pSlot = outputIdx >= 0
                             ? (SSlotDescNode*)nodesListGetNode(pScan->node.pOutputDataBlockDesc->pSlots, outputIdx)
                             : NULL;
  QUERY_CHECK_NULL(pSlot, code, lino, _return, TSDB_CODE_PLAN_SLOT_NOT_FOUND);
  *pType = pSlot->dataType;
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t fedScanNormalizeVstbBlock(SFederatedScanOperatorInfo* pInfo, SSDataBlock* pInput,
                                         SSDataBlock** ppOutput) {
  SForeignScanOperatorParam* pParam = pInfo->pActiveVStbParam;
  if (NULL == pParam || NULL == pParam->colMap || NULL == pInfo->pFedScanNode->pScanCols ||
      NULL == pInfo->pFedScanNode->node.pOutputDataBlockDesc) {
    *ppOutput = pInput;
    return TSDB_CODE_SUCCESS;
  }

  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  int32_t      rows = (int32_t)pInput->info.rows;
  char*        pVarValue = NULL;
  SSDataBlock* pOutput = createDataBlockFromDescNode(pInfo->pFedScanNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pOutput, code, lino, _return, terrno);
  QUERY_CHECK_CODE(blockDataEnsureCapacity(pOutput, rows), lino, _return);

  int32_t outputCols = (int32_t)taosArrayGetSize(pOutput->pDataBlock);
  for (int32_t i = 0; i < outputCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pOutput->pDataBlock, i);
    QUERY_CHECK_NULL(pCol, code, lino, _return, terrno);
    colDataSetNItemsNull(pCol, 0, rows);
  }

  int32_t tsOutput = fedScanFindVstbOutputIndex(pInfo->pFedScanNode, PRIMARYKEY_TIMESTAMP_COL_ID, true);
  if (tsOutput < 0 && outputCols > 0) {
    tsOutput = 0;
  }
  if (tsOutput >= 0 && taosArrayGetSize(pInput->pDataBlock) > 0) {
    SColumnInfoData* pSrc = taosArrayGet(pInput->pDataBlock, 0);
    SColumnInfoData* pDst = taosArrayGet(pOutput->pDataBlock, tsOutput);
    QUERY_CHECK_NULL(pSrc, code, lino, _return, terrno);
    QUERY_CHECK_NULL(pDst, code, lino, _return, terrno);
    QUERY_CHECK_CODE(colDataAssign(pDst, pSrc, rows, &pOutput->info), lino, _return);
  }

  int32_t mapSize = (int32_t)taosArrayGetSize(pParam->colMap);
  int32_t inputCols = (int32_t)taosArrayGetSize(pInput->pDataBlock);
  for (int32_t i = 0; i < mapSize && i + 1 < inputCols; ++i) {
    SColIdNameKV* pMap = taosArrayGet(pParam->colMap, i);
    QUERY_CHECK_NULL(pMap, code, lino, _return, terrno);
    int32_t outputIdx = fedScanFindVstbOutputIndex(pInfo->pFedScanNode, pMap->colId, false);
    if (outputIdx < 0 || outputIdx >= outputCols) {
      qError("%s cannot map virtual column id %d for source %s table %s", __func__, pMap->colId,
             pParam->sourceName, pParam->tableName);
    }
    QUERY_CHECK_CONDITION(outputIdx >= 0 && outputIdx < outputCols, code, lino, _return,
                          TSDB_CODE_PLAN_SLOT_NOT_FOUND);
    SColumnInfoData* pSrc = taosArrayGet(pInput->pDataBlock, i + 1);
    SColumnInfoData* pDst = taosArrayGet(pOutput->pDataBlock, outputIdx);
    QUERY_CHECK_NULL(pSrc, code, lino, _return, terrno);
    QUERY_CHECK_NULL(pDst, code, lino, _return, terrno);
    QUERY_CHECK_CODE(colDataAssign(pDst, pSrc, rows, &pOutput->info), lino, _return);
  }

  for (int32_t i = 0; NULL != pParam->pVtbTagList &&
                      i < taosArrayGetSize(pParam->pVtbTagList);
       ++i) {
    STagVal* pTag = taosArrayGet(pParam->pVtbTagList, i);
    QUERY_CHECK_NULL(pTag, code, lino, _return, terrno);
    int32_t outputIdx = fedScanFindVstbOutputIndex(pInfo->pFedScanNode, pTag->cid, false);
    if (outputIdx < 0 || outputIdx >= outputCols) {
      qError("%s cannot map virtual tag column id %d for source %s table %s", __func__, pTag->cid,
             pParam->sourceName, pParam->tableName);
    }
    QUERY_CHECK_CONDITION(outputIdx >= 0 && outputIdx < outputCols, code, lino, _return,
                          TSDB_CODE_PLAN_SLOT_NOT_FOUND);
    SColumnInfoData* pDst = taosArrayGet(pOutput->pDataBlock, outputIdx);
    QUERY_CHECK_NULL(pDst, code, lino, _return, terrno);

    const char* pValue = (const char*)&pTag->i64;
    bool        isNull = !IS_VAR_DATA_TYPE(pTag->type) && pTag->nData == (uint32_t)-1;
    if (IS_VAR_DATA_TYPE(pTag->type)) {
      isNull = NULL == pTag->pData;
      if (!isNull) {
        pVarValue = taosMemoryMalloc(VARSTR_HEADER_SIZE + pTag->nData);
        QUERY_CHECK_NULL(pVarValue, code, lino, _return, terrno);
        varDataSetLen(pVarValue, pTag->nData);
        memcpy(varDataVal(pVarValue), pTag->pData, pTag->nData);
        pValue = pVarValue;
      }
    }
    for (int32_t row = 0; row < rows; ++row) {
      code = colDataSetVal(pDst, row, pValue, isNull);
      QUERY_CHECK_CODE(code, lino, _return);
    }
    taosMemoryFreeClear(pVarValue);
  }

  // The normalized block owns buffers sized by blockDataEnsureCapacity above.
  // A connector block commonly advertises a much larger reusable capacity;
  // copying that value would make the next blockDataCleanup clear past the
  // normalized buffers and corrupt the heap.
  uint32_t    outputCapacity = pOutput->info.capacity;
  STimeWindow outputCalWin = pOutput->info.calWin;
  pOutput->info = pInput->info;
  pOutput->info.capacity = outputCapacity;
  // Connector blocks are not created from the physical output descriptor and
  // commonly leave calWin zero-initialized.  Keep the descriptor's full
  // calculation range; otherwise a sliding interval stops after its first
  // overlapping window because {0, 0} rejects every subsequent window.
  pOutput->info.calWin = outputCalWin;
  pOutput->info.id.blockId = pInfo->pFedScanNode->node.pOutputDataBlockDesc->dataBlockId;
  if (pParam->hasVtbGroupId) {
    pOutput->info.id.groupId = pParam->vtbGroupId;
  } else if (pParam->groupId >= 0) {
    pOutput->info.id.groupId = (uint64_t)pParam->groupId;
  }
  *ppOutput = pOutput;
  return code;

_return:
  taosMemoryFree(pVarValue);
  if (TSDB_CODE_SUCCESS != code) {
    blockDataDestroy(pOutput);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void fedScanLogBlockSummary(const SFederatedScanPhysiNode* pFedScan, const SSDataBlock* pBlock,
                                   const char* stage) {
  if (NULL == pFedScan || NULL == pBlock || NULL == pBlock->pDataBlock) {
    return;
  }

  const SExtTableNode* pExt = (const SExtTableNode*)pFedScan->pExtTable;
  const char* sourceName = (pExt != NULL && pExt->sourceName[0] != '\0') ? pExt->sourceName : "<unknown>";
  int32_t rows = (int32_t)pBlock->info.rows;
  int32_t cols = taosArrayGetSize(pBlock->pDataBlock);

  qDebug("FQ-BlockSummary: stage=%s source=%s rows=%d cols=%d groupId=%" PRIu64 " scanFlag=%d",
         stage ? stage : "<unknown>", sourceName, rows, cols, pBlock->info.id.groupId, (int)pBlock->info.scanFlag);

  for (int32_t c = 0; c < cols; ++c) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, c);
    if (NULL == pCol) {
      qDebug("FQ-BlockSummary: source=%s col=%d <null column ptr>", sourceName, c);
      continue;
    }

    int32_t firstRow = (rows > 0) ? 0 : -1;
    int32_t lastRow = rows - 1;
    bool firstNull = (firstRow >= 0) ? colDataIsNull(pCol, rows, firstRow, NULL) : true;
    bool lastNull = (lastRow >= 0) ? colDataIsNull(pCol, rows, lastRow, NULL) : true;

    switch (pCol->info.type) {
      case TSDB_DATA_TYPE_TIMESTAMP: {
        int64_t firstVal = firstNull ? 0 : *(int64_t*)colDataGetNumData(pCol, firstRow);
        int64_t lastVal = lastNull ? 0 : *(int64_t*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=TIMESTAMP prec=%d bytes=%d firstNull=%d first=%" PRId64
               " lastNull=%d last=%" PRId64,
               sourceName, c, pCol->info.precision, pCol->info.bytes, firstNull, firstVal, lastNull, lastVal);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT: {
        int64_t firstVal = firstNull ? 0 : *(int64_t*)colDataGetNumData(pCol, firstRow);
        int64_t lastVal = lastNull ? 0 : *(int64_t*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=%d bytes=%d firstNull=%d first=%" PRId64
               " lastNull=%d last=%" PRId64,
               sourceName, c, pCol->info.type, pCol->info.bytes, firstNull, firstVal, lastNull, lastVal);
        break;
      }
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT: {
        int32_t firstVal = firstNull ? 0 : *(int32_t*)colDataGetNumData(pCol, firstRow);
        int32_t lastVal = lastNull ? 0 : *(int32_t*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=%d bytes=%d firstNull=%d first=%d lastNull=%d last=%d",
               sourceName, c, pCol->info.type, pCol->info.bytes, firstNull, firstVal, lastNull, lastVal);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT: {
        int16_t firstVal = firstNull ? 0 : *(int16_t*)colDataGetNumData(pCol, firstRow);
        int16_t lastVal = lastNull ? 0 : *(int16_t*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=%d bytes=%d firstNull=%d first=%d lastNull=%d last=%d",
               sourceName, c, pCol->info.type, pCol->info.bytes, firstNull, (int)firstVal, lastNull, (int)lastVal);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_BOOL: {
        int8_t firstVal = firstNull ? 0 : *(int8_t*)colDataGetNumData(pCol, firstRow);
        int8_t lastVal = lastNull ? 0 : *(int8_t*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=%d bytes=%d firstNull=%d first=%d lastNull=%d last=%d",
               sourceName, c, pCol->info.type, pCol->info.bytes, firstNull, (int)firstVal, lastNull, (int)lastVal);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double firstVal = firstNull ? 0 : *(double*)colDataGetNumData(pCol, firstRow);
        double lastVal = lastNull ? 0 : *(double*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=DOUBLE bytes=%d firstNull=%d first=%.17g lastNull=%d last=%.17g",
               sourceName, c, pCol->info.bytes, firstNull, firstVal, lastNull, lastVal);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float firstVal = firstNull ? 0 : *(float*)colDataGetNumData(pCol, firstRow);
        float lastVal = lastNull ? 0 : *(float*)colDataGetNumData(pCol, lastRow);
        qDebug("FQ-BlockSummary: source=%s col=%d type=FLOAT bytes=%d firstNull=%d first=%.9g lastNull=%d last=%.9g",
               sourceName, c, pCol->info.bytes, firstNull, firstVal, lastNull, lastVal);
        break;
      }
      default:
        qDebug("FQ-BlockSummary: source=%s col=%d type=%d prec=%d bytes=%d firstNull=%d lastNull=%d",
               sourceName, c, pCol->info.type, pCol->info.precision, pCol->info.bytes, firstNull, lastNull);
        break;
    }
  }
}

static void fedScanRememberRemoteSql(SFederatedScanOperatorInfo* pInfo, const char* remoteSql,
                                     const char* sourceName) {
  if (NULL == pInfo) {
    return;
  }

  if (NULL == remoteSql || '\0' == remoteSql[0]) {
    pInfo->remoteSql[0] = '\0';
    qDebug("FederatedScan: runtime remote SQL is empty, source=%s",
           sourceName ? sourceName : "<unknown>");
    return;
  }

  tstrncpy(pInfo->remoteSql, remoteSql, sizeof(pInfo->remoteSql));
  qDebug("FederatedScan: remembered runtime remote SQL for explain, source=%s sql=%.512s",
         sourceName ? sourceName : "<unknown>", pInfo->remoteSql);
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

static int32_t fedScanBuildMissingTagFilter(const SNode* pCond, const SExtTableMeta* pMeta, SFedScanQuote q,
                                            char** ppSql, int32_t* pSqlLen) {
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

    offset += snprintf(pSql + offset, cap - offset, "%s%c%s%c = ''",
                       offset > 0 ? " AND " : "", q.open, pCol->colName, q.close);
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
  SFedScanQuote                    q;
  EExtSQLDialect                   dialect;
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

  int32_t code = fedScanCondSqlAppendChar(pSql, pSql->q.open);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (const char* p = pName; *p != '\0'; ++p) {
    code = fedScanCondSqlAppendChar(pSql, *p);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (*p == pSql->q.close) {
      code = fedScanCondSqlAppendChar(pSql, *p);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return fedScanCondSqlAppendChar(pSql, pSql->q.close);
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
        return pCol->colName;
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

  if (pSql->dialect == EXT_SQL_DIALECT_SQLSERVER) {
    // T-SQL renderings that nodesGetStrValueFromNode cannot express: the
    // boolean literals must be 1 / 0 (DS 5.3.12 R2) and a bare epoch integer
    // is not comparable with datetime2.  Values without an exact T-SQL form
    // stay local (the caller strips that fragment from the pushed WHERE).
    switch (pVal->node.resType.type) {
      case TSDB_DATA_TYPE_BOOL:
        return fedScanCondSqlAppend(pSql, pVal->datum.b ? "1" : "0");
      case TSDB_DATA_TYPE_TIMESTAMP:
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_GEOMETRY: {
        // The decoded value is authoritative; varDataLen bounds the loop so a
        // non-terminated buffer can never leak trailing bytes into the literal.
        const char* pText = NULL;
        int32_t     textLen = 0;
        if (pVal->datum.p != NULL) {
          pText = varDataVal(pVal->datum.p);
          textLen = (int32_t)varDataLen(pVal->datum.p);
        } else if (pVal->literal != NULL) {
          pText = pVal->literal;
          textLen = (int32_t)strlen(pText);
        } else {
          return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
        }
        int32_t code = fedScanCondSqlAppendChar(pSql, '\'');
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        for (int32_t i = 0; i < textLen; ++i) {
          // A T-SQL string literal escapes an embedded quote by doubling it.
          if (pText[i] == '\'') {
            code = fedScanCondSqlAppendChar(pSql, '\'');
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }
          }
          code = fedScanCondSqlAppendChar(pSql, pText[i]);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
        }
        return fedScanCondSqlAppendChar(pSql, '\'');
      }
      default:
        break;
    }
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
  bool first = true;
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
  bool first = true;
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
      const char* colName = fedScanFindRemoteCondColName(pSql, pCol->colId);
      if (colName == NULL) {
        return TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      }
      return fedScanCondSqlAppendQuotedIdent(pSql, colName);
    }
    case QUERY_NODE_VALUE:
      return fedScanRenderPushedCondValue(pSql, (SValueNode*)pNode);
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
      .dialect = pDst->dialect,
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

static int32_t fedScanBuildVStbPushedCondSql(SForeignScanOperatorParam* pFsParam, const SExtTableMeta* pMeta,
                                             SFedScanQuote q, EExtSQLDialect dialect, char** ppSql, int32_t* pSqlLen,
                                             bool* pFullyRendered) {
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
      .dialect = dialect,
      .pMeta = pMeta,
      .pFsParam = pFsParam,
  };

  int32_t code = TSDB_CODE_SUCCESS;
  bool allRendered = true;
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

static int32_t fedScanBuildVStbStoredFilterSql(const SForeignScanOperatorParam* pFsParam,
                                               EExtSourceType sourceType, char** ppSql, int32_t* pSqlLen) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  int32_t        sqlLen = 0;
  char*          pSql = NULL;
  SNode*         pCond = NULL;
  bool           renderingFilter = false;
  EExtSQLDialect dialect = EXT_SQL_DIALECT_MYSQL;

  QUERY_CHECK_NULL(ppSql, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSqlLen, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppSql = NULL;
  *pSqlLen = 0;
  QUERY_CHECK_NULL(pFsParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  if (pFsParam->filterCondLen == 0 && pFsParam->filterCondJson == NULL) {
    goto _return;
  }
  QUERY_CHECK_CONDITION(pFsParam->filterCondLen > 0, code, lino, _return, TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  QUERY_CHECK_NULL(pFsParam->filterCondJson, code, lino, _return, TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  QUERY_CHECK_CONDITION(
      strnlen(pFsParam->filterCondJson, (size_t)pFsParam->filterCondLen + 1) == pFsParam->filterCondLen, code, lino,
      _return, TSDB_CODE_VTABLE_INVALID_EXT_FILTER);

  switch (sourceType) {
    case EXT_SOURCE_MYSQL:
      dialect = EXT_SQL_DIALECT_MYSQL;
      break;
    case EXT_SOURCE_POSTGRESQL:
      dialect = EXT_SQL_DIALECT_POSTGRES;
      break;
    case EXT_SOURCE_SQLSERVER:
      // DS 5.3.9 / TS FQSS-VTBLP-001: the declared filter renders through the
      // shared renderer, so SQL Server reuses the T-SQL dialect branch there.
      dialect = EXT_SQL_DIALECT_SQLSERVER;
      break;
    default:
      code = TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED;
      QUERY_CHECK_CODE(code, lino, _return);
  }

  code = nodesStringToNode(pFsParam->filterCondJson, &pCond);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_NULL(pCond, code, lino, _return, TSDB_CODE_VTABLE_INVALID_EXT_FILTER);

  renderingFilter = true;
  code = nodesRestoreExternalFilterLiterals(&pCond, false);
  QUERY_CHECK_CODE(code, lino, _return);
  code = nodesExprToExtSQL(pCond, dialect, NULL, 0, &sqlLen);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_CONDITION(sqlLen > 0, code, lino, _return, TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED);

  pSql = taosMemoryMalloc((size_t)sqlLen + 1);
  QUERY_CHECK_NULL(pSql, code, lino, _return, terrno);
  code = nodesExprToExtSQL(pCond, dialect, pSql, sqlLen + 1, pSqlLen);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_CONDITION(*pSqlLen == sqlLen, code, lino, _return, TSDB_CODE_OUT_OF_BUFFER);

  *ppSql = pSql;
  pSql = NULL;

_return:
  nodesDestroyNode(pCond);
  taosMemoryFree(pSql);
  if (code != TSDB_CODE_SUCCESS) {
    int32_t cause = code;
    if (renderingFilter && code != TSDB_CODE_OUT_OF_MEMORY) {
      code = TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED;
    }
    qError("%s failed at line %d since %s, cause:0x%x %s", __func__, lino, tstrerror(code), cause, tstrerror(cause));
    terrno = code;
  }
  return code;
}

// Fill an appended extra output column with a constant VALUE target when possible.
// Returns true if the slot is filled, false if caller should apply other fallback logic.
static SNodeList* fedScanGetOutputExprList(const SFederatedScanPhysiNode* pFedScan) {
  if (NULL == pFedScan) {
    return NULL;
  }

  if (pFedScan->pRemotePlan != NULL) {
    SLogicNode* pRemoteRoot = (SLogicNode*)pFedScan->pRemotePlan;
    if (pRemoteRoot->pTargets != NULL && LIST_LENGTH(pRemoteRoot->pTargets) > 0) {
      qDebug("FederatedScan: use top remote targets for output expr list, remoteRootType=%d targets=%d",
             (int)nodeType((SNode*)pRemoteRoot), (int)LIST_LENGTH(pRemoteRoot->pTargets));
      return pRemoteRoot->pTargets;
    }

    qError("FederatedScan: remote plan exists but top remote targets are empty, remoteRootType=%d scanCols=%d",
           (int)nodeType((SNode*)pRemoteRoot),
           pFedScan->pScanCols ? (int)LIST_LENGTH(pFedScan->pScanCols) : 0);
    return NULL;
  }

  qDebug("FederatedScan: use local pScanCols for output expr list, remotePlan=%p scanCols=%d",
         (void*)pFedScan->pRemotePlan, pFedScan->pScanCols ? (int)LIST_LENGTH(pFedScan->pScanCols) : 0);
  return pFedScan->pScanCols;
}

static const SNode* fedScanUnwrapExpr(const SNode* pExprNode) {
  const SNode* pExpr = pExprNode;
  if (NULL == pExpr) {
    return NULL;
  }

  if (QUERY_NODE_TARGET == nodeType((SNode*)pExpr)) {
    pExpr = ((const STargetNode*)pExpr)->pExpr;
  }

  if (NULL != pExpr && QUERY_NODE_GROUPING_SET == nodeType((SNode*)pExpr)) {
    pExpr = nodesListGetNode(((const SGroupingSetNode*)pExpr)->pParameterList, 0);
  }

  return pExpr;
}

static bool fedScanNonEmptyNameEquals(const char* pLeft, const char* pRight) {
  return NULL != pLeft && NULL != pRight && '\0' != pLeft[0] && '\0' != pRight[0] &&
         0 == strcasecmp(pLeft, pRight);
}

static int32_t fedScanBuildHashedSlotKey(char* pBuf, int32_t cap, const char* pPrefix, const char* pName) {
  if (NULL == pBuf || cap <= 0 || NULL == pPrefix || NULL == pName || '\0' == pName[0]) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = snprintf(pBuf, cap, "%s.%s", pPrefix, pName);
  if (len < 0 || len >= cap) {
    return TSDB_CODE_FAILED;
  }

  (void)taosHashBinary(pBuf, len, cap);
  return TSDB_CODE_SUCCESS;
}

static int32_t fedScanBuildReserveExprSlotKey(const SNode* pExprNode, char* pBuf, int32_t cap) {
  const SNode* pExpr = fedScanUnwrapExpr(pExprNode);
  if (NULL == pExpr || NULL == pBuf || cap <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  const SExprNode* pResExpr = (const SExprNode*)pExpr;
  if (QUERY_NODE_COLUMN == nodeType((SNode*)pExpr)) {
    const SColumnNode* pCol = (const SColumnNode*)pExpr;
    if ('\0' == pCol->tableAlias[0]) {
      int32_t len = snprintf(pBuf, cap, "%s", pCol->colName);
      return (len >= 0 && len < cap) ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
    }

    if (pCol->hasRef) {
      if (COLUMN_TYPE_TAG == pCol->colType) {
        int32_t len = snprintf(pBuf, cap, "%s:%s.%s.%s", pCol->colName, pCol->refDbName, pCol->refTableName,
                               pCol->refColName);
        if (len < 0 || len >= cap) {
          return TSDB_CODE_FAILED;
        }
        (void)taosHashBinary(pBuf, len, cap);
        return TSDB_CODE_SUCCESS;
      }

      int32_t len = snprintf(pBuf, cap, "%s.%s.%s", pCol->refDbName, pCol->refTableName, pCol->refColName);
      if (len < 0 || len >= cap) {
        return TSDB_CODE_FAILED;
      }
      (void)taosHashBinary(pBuf, len, cap);
      return TSDB_CODE_SUCCESS;
    }

    if (pCol->hasDep) {
      char depPrefix[TSDB_TABLE_FNAME_LEN + 1] = {0};
      int32_t prefixLen = snprintf(depPrefix, sizeof(depPrefix), "%s.%s", pCol->dbName, pCol->tableAlias);
      if (prefixLen < 0 || prefixLen >= (int32_t)sizeof(depPrefix)) {
        return TSDB_CODE_FAILED;
      }
      return fedScanBuildHashedSlotKey(pBuf, cap, depPrefix, pCol->colName);
    }

    return fedScanBuildHashedSlotKey(pBuf, cap, pCol->tableAlias, pCol->colName);
  }

  int32_t len = snprintf(pBuf, cap, "%s", pResExpr->aliasName);
  return (len >= 0 && len < cap) ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}

static SNodeList* fedScanGetReserveExprList(const SFederatedScanPhysiNode* pFedScan) {
  if (NULL == pFedScan) {
    return NULL;
  }

  if (NULL != pFedScan->pScanCols) {
    return pFedScan->pScanCols;
  }

  if (NULL == pFedScan->pRemotePlan) {
    return NULL;
  }

  SLogicNode* pNode = (SLogicNode*)pFedScan->pRemotePlan;
  while (NULL != pNode) {
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType((SNode*)pNode)) {
      return ((SScanLogicNode*)pNode)->pScanCols;
    }

    if (NULL == pNode->pChildren || LIST_LENGTH(pNode->pChildren) != 1) {
      break;
    }
    pNode = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
  }

  return NULL;
}

static const SNode* fedScanFindReserveExprBySlot(const SFederatedScanPhysiNode* pFedScan,
                                                 const SDataBlockDescNode*      pDesc,
                                                 const SSlotDescNode*           pSlot,
                                                 int32_t                        descSlotIdx) {
  SNodeList* pScanCols = fedScanGetReserveExprList(pFedScan);
  if (NULL == pFedScan || NULL == pDesc || NULL == pSlot || NULL == pScanCols) {
    return NULL;
  }

  if ('\0' != pSlot->name[0]) {
    char  key[SLOT_NAME_LEN] = {0};
    int32_t scanIdx = 0;
    SNode* pNode = NULL;
    FOREACH(pNode, pScanCols) {
      if (TSDB_CODE_SUCCESS != fedScanBuildReserveExprSlotKey(pNode, key, sizeof(key))) {
        ++scanIdx;
        continue;
      }

      if (0 == strcmp(key, pSlot->name)) {
        qDebug("FQ-SlotMirror: reserve lookup hit slot=%d key=%s scanIdx=%d",
               (int)pSlot->slotId, pSlot->name, scanIdx);
        return fedScanUnwrapExpr(pNode);
      }

      ++scanIdx;
    }
  }

  int32_t reserveIdx = 0;
  for (int32_t i = 0; i < LIST_LENGTH(pDesc->pSlots); ++i) {
    SSlotDescNode* pCur = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, i);
    if (NULL == pCur || !pCur->reserve) {
      continue;
    }

    if (i == descSlotIdx) {
      if (reserveIdx < LIST_LENGTH(pScanCols)) {
        qDebug("FQ-SlotMirror: reserve ordinal fallback slot=%d descIdx=%d reserveIdx=%d",
               (int)pSlot->slotId, descSlotIdx, reserveIdx);
        return fedScanUnwrapExpr(nodesListGetNode(pScanCols, reserveIdx));
      }
      break;
    }

    ++reserveIdx;
  }

  qDebug("FQ-SlotMirror: reserve lookup miss slot=%d key=%s scanCols=%d descIdx=%d",
         (int)pSlot->slotId, pSlot->name, pScanCols ? (int)LIST_LENGTH(pScanCols) : 0, descSlotIdx);
  return NULL;
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

static int32_t fedScanGetOutputOrdinalByDescIdx(const SDataBlockDescNode* pDesc, int32_t descSlotIdx) {
  if (NULL == pDesc || descSlotIdx < 0 || descSlotIdx >= LIST_LENGTH(pDesc->pSlots)) {
    return -1;
  }

  int32_t ordinal = 0;
  for (int32_t i = 0; i <= descSlotIdx; ++i) {
    SSlotDescNode* pCur = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, i);
    if (NULL == pCur || !pCur->output || pCur->reserve) {
      continue;
    }

    if (i == descSlotIdx) {
      return ordinal;
    }

    ++ordinal;
  }

  return -1;
}

static const SNode* fedScanGetExprByOutputOrdinal(const SNodeList* pExprs, int32_t outputOrdinal) {
  if (NULL == pExprs || outputOrdinal < 0 || outputOrdinal >= LIST_LENGTH(pExprs)) {
    return NULL;
  }

  return fedScanUnwrapExpr(nodesListGetNode((SNodeList*)pExprs, outputOrdinal));
}

static int32_t fedScanFillConstExprValue(SColumnInfoData* pCol, int32_t rows, const SNode* pExpr, bool* pFilled) {
  if (NULL != pFilled) {
    *pFilled = false;
  }

  if (NULL == pCol || rows <= 0 || NULL == pExpr || QUERY_NODE_VALUE != nodeType((SNode*)pExpr)) {
    return TSDB_CODE_SUCCESS;
  }

  SValueNode* pVal = (SValueNode*)pExpr;
  void*       pRaw = nodesGetValueFromNode(pVal);
  for (int32_t r = 0; r < rows; ++r) {
    int32_t code = colDataSetVal(pCol, r, pRaw, pVal->isNull);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (NULL != pFilled) {
    *pFilled = true;
  }

  return TSDB_CODE_SUCCESS;
}

static bool fedScanBasicReserveColMatchesOutputExpr(const SNode* pReserveExprNode, const SNode* pOutputExprNode) {
  const SNode* pReserveExpr = fedScanUnwrapExpr(pReserveExprNode);
  const SNode* pOutputExpr = fedScanUnwrapExpr(pOutputExprNode);
  if (NULL == pReserveExpr || NULL == pOutputExpr || !nodesIsExprNode((SNode*)pReserveExpr) ||
      !nodesIsExprNode((SNode*)pOutputExpr)) {
    return false;
  }

  const SExprNode* pReserveResExpr = (const SExprNode*)pReserveExpr;
  const SExprNode* pOutputResExpr = (const SExprNode*)pOutputExpr;
  if (QUERY_NODE_COLUMN == nodeType((SNode*)pReserveExpr) && QUERY_NODE_COLUMN == nodeType((SNode*)pOutputExpr)) {
    const SColumnNode* pReserveCol = (const SColumnNode*)pReserveExpr;
    const SColumnNode* pOutputCol = (const SColumnNode*)pOutputExpr;
    if (pReserveCol->isPrimTs && pOutputCol->isPrimTs) {
      return true;
    }

    if (pReserveCol->colId > 0 && pOutputCol->colId > 0 && pReserveCol->colId == pOutputCol->colId) {
      return true;
    }

    if (pReserveCol->colType == pOutputCol->colType && COLUMN_TYPE_TBNAME == pReserveCol->colType) {
      return true;
    }
  }

  if (pReserveResExpr->resType.type != pOutputResExpr->resType.type) {
    return false;
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pReserveExpr)) {
    const SColumnNode* pReserveCol = (const SColumnNode*)pReserveExpr;
    if (fedScanNonEmptyNameEquals(pReserveCol->colName, ((const SExprNode*)pOutputExpr)->aliasName) ||
        fedScanNonEmptyNameEquals(pReserveCol->colName, ((const SExprNode*)pOutputExpr)->userAlias)) {
      return true;
    }
  }

  if (QUERY_NODE_COLUMN == nodeType((SNode*)pOutputExpr)) {
    const SColumnNode* pOutputCol = (const SColumnNode*)pOutputExpr;
    if (QUERY_NODE_COLUMN == nodeType((SNode*)pReserveExpr)) {
      const SColumnNode* pReserveCol = (const SColumnNode*)pReserveExpr;
      if (fedScanNonEmptyNameEquals(pReserveCol->colName, pOutputCol->colName) ||
          fedScanNonEmptyNameEquals(pReserveCol->colName, pOutputCol->node.aliasName) ||
          fedScanNonEmptyNameEquals(pReserveCol->node.aliasName, pOutputCol->colName) ||
          fedScanNonEmptyNameEquals(pReserveCol->node.aliasName, pOutputCol->node.aliasName) ||
          fedScanNonEmptyNameEquals(pReserveCol->node.userAlias, pOutputCol->node.userAlias)) {
        return true;
      }
    }

    if (fedScanNonEmptyNameEquals(pOutputCol->colName, pReserveResExpr->aliasName) ||
        fedScanNonEmptyNameEquals(pOutputCol->colName, pReserveResExpr->userAlias)) {
      return true;
    }
  }

  return fedScanNonEmptyNameEquals(pReserveResExpr->aliasName, pOutputResExpr->aliasName) ||
         fedScanNonEmptyNameEquals(pReserveResExpr->aliasName, pOutputResExpr->userAlias) ||
         fedScanNonEmptyNameEquals(pReserveResExpr->userAlias, pOutputResExpr->aliasName) ||
         fedScanNonEmptyNameEquals(pReserveResExpr->userAlias, pOutputResExpr->userAlias);
}

static bool fedScanReserveColMatchesOutputExpr(const SNode* pReserveExprNode, const SNode* pOutputExprNode) {
  if (fedScanBasicReserveColMatchesOutputExpr(pReserveExprNode, pOutputExprNode)) {
    return true;
  }

  const SNode* pReserveExpr = fedScanUnwrapExpr(pReserveExprNode);
  if (NULL == pReserveExpr || QUERY_NODE_COLUMN == nodeType((SNode*)pReserveExpr)) {
    return false;
  }

  SNodeList* pReserveCols = NULL;
  int32_t code = nodesCollectColumnsFromNode((SNode*)pReserveExpr, NULL, COLLECT_COL_TYPE_ALL, &pReserveCols);
  if (TSDB_CODE_SUCCESS != code || NULL == pReserveCols) {
    nodesDestroyList(pReserveCols);
    return false;
  }

  bool matched = false;
  SNode* pColNode = NULL;
  FOREACH(pColNode, pReserveCols) {
    if (fedScanBasicReserveColMatchesOutputExpr(pColNode, pOutputExprNode)) {
      matched = true;
      break;
    }
  }

  nodesDestroyList(pReserveCols);
  return matched;
}

static bool fedScanCanMirrorReserveColumn(const SColumnInfoData* pDstCol,
                                          const SColumnInfoData* pSrcCol) {
  if (NULL == pDstCol || NULL == pSrcCol) {
    return false;
  }

  if (pDstCol->info.type != pSrcCol->info.type || pDstCol->info.bytes != pSrcCol->info.bytes) {
    return false;
  }

  // Federated reserve timestamp slots may keep a placeholder precision in the
  // block desc. Mirror the real source column and preserve its precision.
  if (TSDB_DATA_TYPE_TIMESTAMP == pDstCol->info.type) {
    return true;
  }

  return pDstCol->info.precision == pSrcCol->info.precision &&
         pDstCol->info.scale == pSrcCol->info.scale;
}

static int32_t fedScanFillMirroredReserveSlot(const SFederatedScanPhysiNode* pFedScan,
                                              const SDataBlockDescNode*      pDesc,
                                              const SSlotDescNode*           pSlot,
                                              SSDataBlock*                   pBlock,
                                              int32_t                        remoteBlockCols,
                                              int32_t                        descSlotIdx,
                                              SColumnInfoData*               pCol,
                                              bool*                          pFilled) {
  if (NULL != pFilled) {
    *pFilled = false;
  }

  if (NULL == pFedScan || NULL == pDesc || NULL == pSlot || NULL == pBlock || NULL == pCol || remoteBlockCols <= 0 ||
      descSlotIdx < 0 || pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  const SNode* pReserveExpr = fedScanFindReserveExprBySlot(pFedScan, pDesc, pSlot, descSlotIdx);
  if (NULL == pReserveExpr) {
    return TSDB_CODE_SUCCESS;
  }

  SNodesRemoteSelectOutputs outputs = {0};
  bool                      hasRemoteOutputs = false;
  if (NULL != pFedScan && NULL != pFedScan->pRemotePlan) {
    hasRemoteOutputs = (TSDB_CODE_SUCCESS == nodesCollectRemoteQuerySelectOutputs(
                                                 (SLogicNode*)pFedScan->pRemotePlan, &outputs));
  }

  int32_t srcIdx = -1;
  for (int32_t i = 0; i < LIST_LENGTH(pDesc->pSlots); ++i) {
    SSlotDescNode* pOutputSlot = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, i);
    if (NULL == pOutputSlot || pOutputSlot->reserve || !pOutputSlot->output) {
      continue;
    }

    int32_t outputOrdinal = fedScanGetOutputOrdinalByDescIdx(pDesc, i);
    if (outputOrdinal < 0 || outputOrdinal >= remoteBlockCols) {
      continue;
    }

    const SNode* pOutputExpr = hasRemoteOutputs
                                   ? fedScanGetExprByOutputOrdinal(outputs.pSelectExprs, outputOrdinal)
                                   : NULL;
    if (NULL == pOutputExpr) {
      pOutputExpr = fedScanGetOutputExprBySlot(pFedScan, pOutputSlot->slotId);
    }

    if (fedScanReserveColMatchesOutputExpr(pReserveExpr, pOutputExpr)) {
      srcIdx = outputOrdinal;
      break;
    }
  }

  if (hasRemoteOutputs) {
    nodesDestroyRemoteSelectOutputs(&outputs);
  }

  if (srcIdx < 0) {
    qDebug("FQ-SlotMirror: output mapping miss reserveSlot=%d descIdx=%d remoteBlockCols=%d slotName=%s",
           (int)pSlot->slotId, descSlotIdx, remoteBlockCols, pSlot->name);
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData* pSrcCol = taosArrayGet(pBlock->pDataBlock, srcIdx);
  if (!fedScanCanMirrorReserveColumn(pCol, pSrcCol)) {
    if (NULL != pSrcCol) {
      qDebug("FQ-SlotMirror: metadata mismatch reserveSlot=%d descIdx=%d srcIdx=%d dstType=%d/%d/%d/%d srcType=%d/%d/%d/%d",
             (int)pSlot->slotId, descSlotIdx, srcIdx, (int)pCol->info.type, (int)pCol->info.bytes,
             (int)pCol->info.precision, (int)pCol->info.scale, (int)pSrcCol->info.type,
             (int)pSrcCol->info.bytes, (int)pSrcCol->info.precision, (int)pSrcCol->info.scale);
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = colDataAssign(pCol, pSrcCol, (int32_t)pBlock->info.rows, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (NULL != pFilled) {
    *pFilled = true;
  }

  qDebug("FQ-SlotMirror: reserveSlot=%d descIdx=%d srcIdx=%d rows=%d slotName=%s",
         (int)pSlot->slotId, descSlotIdx, srcIdx, (int)pBlock->info.rows, pSlot->name);
  return TSDB_CODE_SUCCESS;
}

static int32_t fedScanFillConstValueSlot(const SFederatedScanPhysiNode* pFedScan,
                                         const SDataBlockDescNode*      pDesc,
                                         const SSlotDescNode*           pSlot,
                                         int32_t                        descSlotIdx,
                                         SColumnInfoData*               pCol,
                                         int32_t                        rows,
                                         bool*                          pFilled) {
  if (NULL != pFilled) {
    *pFilled = false;
  }

  if (NULL == pFedScan || NULL == pSlot || NULL == pCol || rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  const SNode* pExpr = NULL;
  if (NULL != pDesc && pSlot->output && !pSlot->reserve) {
    int32_t outputOrdinal = fedScanGetOutputOrdinalByDescIdx(pDesc, descSlotIdx);
    if (outputOrdinal >= 0) {
      SNodesRemoteSelectOutputs outputs = {0};
      if (NULL != pFedScan->pRemotePlan &&
          TSDB_CODE_SUCCESS == nodesCollectRemoteQuerySelectOutputs((SLogicNode*)pFedScan->pRemotePlan, &outputs)) {
        pExpr = fedScanGetExprByOutputOrdinal(outputs.pSelectExprs, outputOrdinal);
        nodesDestroyRemoteSelectOutputs(&outputs);
      }
    }
  }

  if (NULL == pExpr) {
    pExpr = fedScanFindReserveExprBySlot(pFedScan, pDesc, pSlot, descSlotIdx);
  }

  if (NULL == pExpr) {
    pExpr = fedScanGetOutputExprBySlot(pFedScan, pSlot->slotId);
  }

  return fedScanFillConstExprValue(pCol, rows, pExpr, pFilled);
}

static int32_t fedScanCollectRemoteOutputExprs(const SFederatedScanPhysiNode* pFedScan,
                                               SNodesRemoteSelectOutputs*     pOutputs) {
  if (NULL == pOutputs) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pOutputs, 0, sizeof(*pOutputs));
  if (NULL == pFedScan || NULL == pFedScan->pRemotePlan) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = nodesCollectRemoteQuerySelectOutputs((SLogicNode*)pFedScan->pRemotePlan, pOutputs);
  if (TSDB_CODE_SUCCESS != code) {
    qError("FederatedScan: collect remote output exprs failed, remoteRootType=%d code=0x%x",
           (int)nodeType((SNode*)pFedScan->pRemotePlan), code);
  }
  return code;
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

  int32_t                    code = TSDB_CODE_SUCCESS;
  SNodesRemoteSelectOutputs  outputs = {0};
  code = fedScanCollectRemoteOutputExprs(pFedScan, &outputs);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  int32_t idx = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pDesc->pSlots) {
    if (idx >= blockCols) {
      break;
    }
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    int32_t outputOrdinal = fedScanGetOutputOrdinalByDescIdx(pDesc, idx);
    const SNode* pExpr = fedScanGetExprByOutputOrdinal(outputs.pSelectExprs, outputOrdinal);
    if (NULL == pExpr) {
      pExpr = fedScanGetOutputExprBySlot(pFedScan, pSlot->slotId);
    }

    if (NULL != pExpr) {
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, idx);
      code = fedScanFillConstExprValue(pCol, (int32_t)pBlock->info.rows, pExpr, NULL);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyRemoteSelectOutputs(&outputs);
        return code;
      }
    }
    idx++;
  }

  nodesDestroyRemoteSelectOutputs(&outputs);
  return TSDB_CODE_SUCCESS;
}

static bool fedScanIsInfluxHttpTokenOptions(int8_t sourceType, const char* options) {
  if ((EExtSourceType)sourceType != EXT_SOURCE_INFLUXDB) {
    return false;
  }
  if (options == NULL || options[0] == '\0') {
    return true;
  }
  const char* protoPos = taosStrCaseStr(options, "protocol");
  if (protoPos == NULL) {
    return true;
  }
  return taosStrCaseStr(protoPos, "http") != NULL;
}

static bool fedScanIsRemoteInternalCode(int32_t code) {
  return code == TSDB_CODE_EXT_REMOTE_INTERNAL ||
         (((uint32_t)code & 0xFFFFU) == ((uint32_t)TSDB_CODE_EXT_REMOTE_INTERNAL & 0xFFFFU));
}

static int32_t fedScanNormalizeInfluxAuthCode(int8_t sourceType, const char* options, int32_t code) {
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

static bool fedScanValidTimePrecision(int8_t precision) {
  return precision >= TSDB_TIME_PRECISION_MILLI && precision <= TSDB_TIME_PRECISION_NANO;
}

static int8_t fedScanGetCondPrecision(EExtSourceType srcType, const SExtTableMeta* pMeta, int32_t tsIdx) {
  if (pMeta == NULL || tsIdx < 0 || tsIdx >= pMeta->numOfCols) {
    return TSDB_TIME_PRECISION_MILLI;
  }
  return extTimestampTypePrecision(srcType, pMeta->pCols[tsIdx].extTypeName, pMeta->tsPrecision);
}

static bool fedScanTypeStartsWith(const char* typeName, const char* prefix) {
  if (typeName == NULL || prefix == NULL) {
    return false;
  }
  return strncasecmp(typeName, prefix, strlen(prefix)) == 0;
}

static EExtTimestampTzClass fedScanClassifyTimestampType(
    EExtSourceType srcType, const char* typeName) {
  if (srcType == EXT_SOURCE_MYSQL) {
    if (fedScanTypeStartsWith(typeName, "timestamp")) {
      return EXT_TIMESTAMP_TZ_CLASS_AWARE;
    }
    if (fedScanTypeStartsWith(typeName, "datetime") ||
        fedScanTypeStartsWith(typeName, "date")) {
      return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
    }
  } else if (srcType == EXT_SOURCE_POSTGRESQL) {
    if (fedScanTypeStartsWith(typeName, "timestamptz") ||
        taosStrCaseStr(typeName, "with time zone") != NULL) {
      return EXT_TIMESTAMP_TZ_CLASS_AWARE;
    }
    if (fedScanTypeStartsWith(typeName, "timestamp") ||
        fedScanTypeStartsWith(typeName, "date")) {
      return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
    }
  } else if (srcType == EXT_SOURCE_SQLSERVER) {
    // datetimeoffset carries an offset (absolute instant); datetime2 /
    // datetime / smalldatetime / date store the client's wall clock.  The
    // longer prefix must be tested first.
    if (fedScanTypeStartsWith(typeName, "datetimeoffset")) {
      return EXT_TIMESTAMP_TZ_CLASS_AWARE;
    }
    if (fedScanTypeStartsWith(typeName, "datetime") ||
        fedScanTypeStartsWith(typeName, "smalldatetime") ||
        fedScanTypeStartsWith(typeName, "date")) {
      return EXT_TIMESTAMP_TZ_CLASS_NAIVE;
    }
  }
  return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
}

static const char* fedScanNormalizeMySqlTimezone(const char* timezone) {
  if (timezone == NULL) {
    return NULL;
  }
  if (strcasecmp(timezone, "UTC") == 0 ||
      strcasecmp(timezone, "GMT") == 0 ||
      strcasecmp(timezone, "Etc/UTC") == 0 ||
      strcasecmp(timezone, "Etc/GMT") == 0) {
    return "+00:00";
  }
  return timezone;
}

static bool fedScanIsSafeTimezone(const char* timezone) {
  if (timezone == NULL || timezone[0] == '\0') {
    return false;
  }
  for (const unsigned char* p = (const unsigned char*)timezone; *p != '\0';
       ++p) {
    if (!(isalnum(*p) || *p == '/' || *p == '_' || *p == '+' ||
          *p == '-' || *p == ':' || *p == '.')) {
      return false;
    }
  }
  return true;
}

static bool fedScanHasNaiveTimestampHint(
    const SFederatedScanPhysiNode* pNode) {
  if (pNode == NULL || pNode->pTimestampTypeHints == NULL) {
    return false;
  }
  for (int32_t i = 0; i < pNode->numTimestampTypeHints; ++i) {
    if (pNode->pTimestampTypeHints[i].tzClass ==
        EXT_TIMESTAMP_TZ_CLASS_NAIVE) {
      return true;
    }
  }
  return false;
}

static int32_t fedScanAppendTimestampProjection(
    char* sql, int32_t sqlCapacity, int32_t* pOffset,
    EExtSourceType srcType, const char* colName, const char* extTypeName,
    const char* timezone,
    bool timezoneConversionSupported) {
  EExtTimestampTzClass tzClass =
      fedScanClassifyTimestampType(srcType, extTypeName);
  int32_t remaining = sqlCapacity - *pOffset;
  int32_t written = 0;

  if (srcType == EXT_SOURCE_MYSQL) {
    if (tzClass == EXT_TIMESTAMP_TZ_CLASS_AWARE) {
      written = snprintf(
          sql + *pOffset, remaining,
          "CAST(UNIX_TIMESTAMP(`%s`) * 1000000 AS SIGNED) AS `%s`",
          colName, colName);
    } else if (tzClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE &&
               timezoneConversionSupported) {
      written = snprintf(
          sql + *pOffset, remaining,
          "CAST(TIMESTAMPDIFF(MICROSECOND, "
          "'1970-01-01 00:00:00', CONVERT_TZ(`%s`, '%s', '+00:00')) "
          "AS SIGNED) AS `%s`",
          colName, timezone, colName);
    } else {
      written = snprintf(sql + *pOffset, remaining, "`%s`", colName);
    }
  } else if (srcType == EXT_SOURCE_POSTGRESQL) {
    if (tzClass == EXT_TIMESTAMP_TZ_CLASS_AWARE) {
      written = snprintf(
          sql + *pOffset, remaining,
          "CAST(EXTRACT(EPOCH FROM \"%s\") * 1000000 AS BIGINT) "
          "AS \"%s\"",
          colName, colName);
    } else if (tzClass == EXT_TIMESTAMP_TZ_CLASS_NAIVE &&
               timezoneConversionSupported) {
      written = snprintf(
          sql + *pOffset, remaining,
          "CAST(EXTRACT(EPOCH FROM (CAST(\"%s\" AS TIMESTAMP) "
          "AT TIME ZONE '%s')) * "
          "1000000 AS BIGINT) AS \"%s\"",
          colName, timezone, colName);
    } else {
      written = snprintf(sql + *pOffset, remaining, "\"%s\"", colName);
    }
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  if (written < 0 || written >= remaining) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  *pOffset += written;
  return TSDB_CODE_SUCCESS;
}

static int64_t fedScanConvertTimeBound(int64_t value, int8_t fromPrecision, int8_t toPrecision,
                                       bool lowerBound) {
  if (fromPrecision <= toPrecision) {
    return convertTimePrecision(value, fromPrecision, toPrecision);
  }

  int64_t factor = TSDB_TICK_PER_SECOND(fromPrecision) / TSDB_TICK_PER_SECOND(toPrecision);
  int64_t quotient = value / factor;
  int64_t remainder = value % factor;
  if (lowerBound && remainder > 0) {
    ++quotient;
  } else if (!lowerBound && remainder < 0) {
    --quotient;
  }
  return quotient;
}

// Format a filled SExtConnectorError into pInfo->extErrMsg for later propagation.
static void fedScanFormatError(SFederatedScanOperatorInfo* pInfo, int32_t code,
                               const SExtConnectorError* pErr,
                               const SExtPushdownDiagnostic* pDiag) {
  if (NULL == pInfo || 0 == code) {
    return;
  }

  const char* tdErrStr  = tstrerror(code);
  const char* typeName  = (NULL != pErr) ? fedScanSourceTypeName(pErr->sourceType) : "unknown";
  const char* source    = (NULL != pErr) ? pErr->sourceName : "";
  int32_t     bufLen    = (int32_t)sizeof(pInfo->extErrMsg);
  int32_t     offset    = 0;

  offset = snprintf(pInfo->extErrMsg, bufLen, "%s [source=%s, type=%s",
                    tdErrStr, source, typeName);

  if (NULL != pErr && (EExtSourceType)pErr->sourceType == EXT_SOURCE_MYSQL && pErr->remoteCode != 0) {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", remote_code=%d", pErr->remoteCode);
  }
  if (NULL != pErr && (EExtSourceType)pErr->sourceType == EXT_SOURCE_POSTGRESQL &&
      pErr->remoteSqlstate[0] != '\0') {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", remote_sqlstate=%s", pErr->remoteSqlstate);
  }
  if (NULL != pErr && (EExtSourceType)pErr->sourceType == EXT_SOURCE_SQLSERVER) {
    /* SQL Server carries both: the T-SQL native error number drives the
     * precise pushdown-demote classification (DS 5.3.8.1), the SQLSTATE
     * drives the coarse 42xxx fallback. */
    if (pErr->remoteCode != 0) {
      offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                         ", remote_code=%d", pErr->remoteCode);
    }
    if (pErr->remoteSqlstate[0] != '\0') {
      offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                         ", remote_sqlstate=%s", pErr->remoteSqlstate);
    }
  }
  if (NULL != pErr && (EExtSourceType)pErr->sourceType == EXT_SOURCE_INFLUXDB && pErr->httpStatus != 0) {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", http_status=%d", pErr->httpStatus);
  }
  if (NULL != pErr && pErr->remoteMessage[0] != '\0') {
    offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset,
                       ", remote_message=%s", pErr->remoteMessage);
  }

  if (NULL != pDiag && offset < bufLen - 1) {
    char diagHint[256] = {0};
    if (TSDB_CODE_SUCCESS == extFormatPushdownDiagnosticHint(pDiag, diagHint, sizeof(diagHint)) &&
        diagHint[0] != '\0') {
      offset += snprintf(pInfo->extErrMsg + offset, bufLen - offset, ", %s", diagHint);
    }
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
static STimeWindow fedScanGetVStbScanRange(
    const SFederatedScanOperatorInfo* pInfo,
    const SFederatedScanPhysiNode* pFedNode, bool* pEmptyRange) {
  STimeWindow scanRange = pFedNode->scanRange;
  *pEmptyRange = false;

  if (!pInfo->hasRuntimeTimeRange) {
    return scanRange;
  }

  const STimeWindow* pRuntimeRange = &pInfo->runtimeTimeRange;
  if (pRuntimeRange->skey >= pRuntimeRange->ekey) {
    *pEmptyRange = true;
    return scanRange;
  }

  scanRange.skey = TMAX(scanRange.skey, pRuntimeRange->skey);
  if (pRuntimeRange->ekey != INT64_MAX) {
    // Runtime windows are half-open, while scanRange uses an inclusive ekey.
    scanRange.ekey = TMIN(scanRange.ekey, pRuntimeRange->ekey - 1);
  }
  *pEmptyRange = scanRange.skey > scanRange.ekey;
  return scanRange;
}

static int32_t fedScanValidateVStbCurrentMeta(SFederatedScanOperatorInfo*      pInfo,
                                              const SForeignScanOperatorParam* pFsParam, const SExtTableMeta* pMeta,
                                              int32_t* pTsIdx) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  char       reason[256] = {0};
  timezone_t timezone = NULL;

  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pInfo->pFedScanNode, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pFsParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pMeta, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTsIdx, code, lino, _return, TSDB_CODE_INVALID_PARA);

  SFederatedScanPhysiNode* pFedNode = pInfo->pFedScanNode;
  if (pFsParam->filterCondLen > 0) {
    timezone = tzalloc('\0' != pFedNode->timezone[0] ? pFedNode->timezone : NULL);
    QUERY_CHECK_NULL(timezone, code, lino, _return, terrno);
  }

  code =
      nodesValidateExternalFilterCurrentMeta((EExtSourceType)pFedNode->sourceType, pMeta, pFsParam->filterCondJson,
                                             pFsParam->filterCondLen, timezone, NULL, reason, sizeof(reason), pTsIdx);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (NULL != timezone) {
    tzfree(timezone);
  }
  if (TSDB_CODE_SUCCESS != code) {
    if (NULL != pInfo && NULL != pFsParam) {
      snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
               "external column binding invalid [source=%s, table=%s.%s]: %s", pFsParam->sourceName, pFsParam->dbName,
               pFsParam->tableName, '\0' != reason[0] ? reason : tstrerror(code));
    }
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t fedScanBuildVStbQueryAndMappings(SFederatedScanOperatorInfo* pInfo, SForeignScanOperatorParam* pFsParam,
                                                char** ppSQL, SExtColTypeMapping** ppMappings, int32_t* pNumMappings) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SExtTableMeta*           pMeta = NULL;
  SExtColTypeMapping*      mappings = NULL;
  char*                    tagCondSql = NULL;
  char*                    pushedCondSql = NULL;
  char*                    storedFilterSql = NULL;
  char*                    sqlBuf = NULL;
  char*                    missingTagSql = NULL;
  SNode*                   pTagCond = NULL;
  timezone_t               timezone = NULL;
  SFederatedScanPhysiNode* pFedNode = pInfo->pFedScanNode;
  SExtTableNode*           pExtTable = (SExtTableNode*)pFedNode->pExtTable;
  EExtSourceType           srcType = (EExtSourceType)pFedNode->sourceType;
  // SQL Server is a relational source with a real schema: schema drift must be
  // reported as EXT_SCHEMA_CHANGED (DS 9.1.2) and tz-naive timestamp columns
  // take the client-timezone comparison path, exactly like MySQL / PostgreSQL.
  bool                     isRelationalSource = EXT_SOURCE_MYSQL == srcType || EXT_SOURCE_POSTGRESQL == srcType ||
                                               EXT_SOURCE_SQLSERVER == srcType;

  // Determine SQL dialect: quote characters and table path format
  SFedScanQuote q = fedScanQuoteFor(srcType);

  // Build a temporary SExtTableNode with the specific remote table name
  SExtTableNode tempExtTable = {0};
  if (pExtTable != NULL) {
    tempExtTable = *pExtTable;
  }
  tstrncpy(tempExtTable.table.tableName, pFsParam->tableName, sizeof(tempExtTable.table.tableName));
  // For PG: use srcSchema as schemaName (falls back to "public")
  // For MySQL: use srcDatabase as dbName (table lookup needs it)
  // For SQL Server: the runtime reference carries the exact database/schema of
  // the bound table (DS 5.3.6); prefer them over the plan-time defaults.
  if (srcType == EXT_SOURCE_POSTGRESQL) {
    if (pFedNode->srcSchema[0] != '\0') {
      tstrncpy(tempExtTable.schemaName, pFedNode->srcSchema, sizeof(tempExtTable.schemaName));
    }
  } else if (srcType == EXT_SOURCE_MYSQL) {
    if (pFedNode->srcDatabase[0] != '\0') {
      tstrncpy(tempExtTable.table.dbName, pFedNode->srcDatabase, sizeof(tempExtTable.table.dbName));
    }
  } else if (srcType == EXT_SOURCE_SQLSERVER) {
    if (pFsParam->dbName[0] != '\0') {
      tstrncpy(tempExtTable.table.dbName, pFsParam->dbName, sizeof(tempExtTable.table.dbName));
    }
    if (pFsParam->schemaName[0] != '\0') {
      tstrncpy(tempExtTable.schemaName, pFsParam->schemaName, sizeof(tempExtTable.schemaName));
    } else if (pFedNode->srcSchema[0] != '\0') {
      tstrncpy(tempExtTable.schemaName, pFedNode->srcSchema, sizeof(tempExtTable.schemaName));
    }
  }

  // Get ext table schema from remote source
  code = extConnectorGetTableSchema(pInfo->pConnHandle, &tempExtTable, &pMeta);
  if (code != TSDB_CODE_SUCCESS || pMeta == NULL) {
    qError("FederatedScan VStb: getTableSchema failed for %s.%s, code=0x%x",
           pFsParam->dbName, pFsParam->tableName, code);
    code = code ? code : TSDB_CODE_EXT_TABLE_NOT_EXIST;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  int32_t tsIdx = -1;
  code = fedScanValidateVStbCurrentMeta(pInfo, pFsParam, pMeta, &tsIdx);
  QUERY_CHECK_CODE(code, lino, _return);

  const char* clientTimezone =
      pFedNode->timezone[0] != '\0' ? pFedNode->timezone : NULL;
  const char* remoteTimezone = srcType == EXT_SOURCE_MYSQL
                                   ? fedScanNormalizeMySqlTimezone(clientTimezone)
                                   : clientTimezone;
  bool timezoneConversionSupported = false;
  if ((srcType == EXT_SOURCE_MYSQL || srcType == EXT_SOURCE_POSTGRESQL) &&
      fedScanIsSafeTimezone(remoteTimezone)) {
    int32_t probeCode = extConnectorSupportsTimezoneConversion(
        pInfo->pConnHandle, remoteTimezone,
        &timezoneConversionSupported);
    if (probeCode != TSDB_CODE_SUCCESS) {
      qWarn("FederatedScan VStb: remote timezone probe failed sourceType=%d "
            "timezone=%s, "
            "using local conversion, code=0x%x %s",
            srcType, remoteTimezone, probeCode, tstrerror(probeCode));
      timezoneConversionSupported = false;
    }
  }

  int32_t numDataCols = (int32_t)taosArrayGetSize(pFsParam->colMap);

  if (EXT_SOURCE_MYSQL != srcType && EXT_SOURCE_POSTGRESQL != srcType) {
    // Non-relational connectors retain their existing selected timestamp key.
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
  }

  QUERY_CHECK_CONDITION(tsIdx >= 0 && tsIdx < pMeta->numOfCols, code, lino, _return,
                        TSDB_CODE_EXT_NO_TS_PRIMARY_KEY);
  if ((EXT_SOURCE_MYSQL == srcType || EXT_SOURCE_POSTGRESQL == srcType) &&
      fedScanValidTimePrecision(pFsParam->dstPrecision) &&
      fedScanGetCondPrecision(srcType, pMeta, tsIdx) > pFsParam->dstPrecision) {
    snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
             "external column binding invalid [source=%s, table=%s.%s]: virtual table precision is lower than "
             "the remote timestamp primary-key precision",
             pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName);
    code = TSDB_CODE_EXT_SCHEMA_CHANGED;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  // Total columns = 1 (ts) + numDataCols
  int32_t totalCols = 1 + numDataCols;

  // Pre-render the series tag filter (if any) so we can size the SQL buffer and
  // AND-merge it into the WHERE clause below.  tagCond is a serialized node tree
  // (nodesNodeToString output); parse it back and render to SQL text.
  int32_t tagCondSqlLen = 0;
  if (pFsParam->tagCond != NULL && pFsParam->tagCondLen > 0) {
    int32_t missingTagSqlLen = 0;
    code = nodesStringToNode(pFsParam->tagCond, &pTagCond);
    QUERY_CHECK_CODE(code, lino, _return);
    QUERY_CHECK_NULL(pTagCond, code, lino, _return, TSDB_CODE_PAR_INTERNAL_ERROR);
    if (srcType == EXT_SOURCE_INFLUXDB) {
      code = fedScanBuildMissingTagFilter(pTagCond, pMeta, q, &missingTagSql, &missingTagSqlLen);
      QUERY_CHECK_CODE(code, lino, _return);
    }
    int32_t cap = pFsParam->tagCondLen * 4 + 256;
    tagCondSql = taosMemoryCalloc(1, cap);
    QUERY_CHECK_NULL(tagCondSql, code, lino, _return, terrno);
    int32_t outLen = 0;
    code = nodesNodeToSQL(pTagCond, tagCondSql, cap, &outLen);
    nodesDestroyNode(pTagCond);
    pTagCond = NULL;
    QUERY_CHECK_CODE(code, lino, _return);
    tagCondSqlLen = outLen;
    if (missingTagSqlLen > 0) {
      int32_t mergedCap = tagCondSqlLen + missingTagSqlLen + 8;
      char* mergedSql = taosMemoryCalloc(1, mergedCap);
      QUERY_CHECK_NULL(mergedSql, code, lino, _return, terrno);
      if (tagCondSqlLen > 0) {
        snprintf(mergedSql, mergedCap, "%s AND %s", tagCondSql, missingTagSql);
      } else {
        snprintf(mergedSql, mergedCap, "%s", missingTagSql);
      }
      taosMemoryFree(tagCondSql);
      taosMemoryFree(missingTagSql);
      tagCondSql = mergedSql;
      missingTagSql = NULL;
      tagCondSqlLen = (int32_t)strlen(mergedSql);
    }
  }

  int32_t pushedCondSqlLen = 0;
  bool    pushedCondFullyRendered = false;
  code = fedScanBuildVStbPushedCondSql(pFsParam, pMeta, q, fedScanDialectFor(srcType), &pushedCondSql,
                                       &pushedCondSqlLen, &pushedCondFullyRendered);
  QUERY_CHECK_CODE(code, lino, _return);

  mappings = taosMemoryCalloc(totalCols, sizeof(SExtColTypeMapping));
  QUERY_CHECK_NULL(mappings, code, lino, _return, terrno);

  int32_t projectionBytes = TSDB_COL_NAME_LEN + 4;
  if (srcType == EXT_SOURCE_MYSQL || srcType == EXT_SOURCE_POSTGRESQL) {
    projectionBytes = TSDB_COL_NAME_LEN * 2 + TD_TIMEZONE_LEN + 192;
  }
  int32_t sqlBufLen = 1024 + totalCols * projectionBytes + tagCondSqlLen +
                      pushedCondSqlLen + 96;
  sqlBuf = taosMemoryCalloc(1, sqlBufLen);
  QUERY_CHECK_NULL(sqlBuf, code, lino, _return, terrno);

  int32_t offset = snprintf(sqlBuf, sqlBufLen, "SELECT ");

  // Column 0: the primary timestamp column
  char tsRemoteNameBuf[TSDB_COL_NAME_LEN] = {0};
  {
    SExtColumnDef* pTsCol = &pMeta->pCols[tsIdx];
    const char* remoteName = pTsCol->colName;
    SDataType remoteType = {0};
    SDataType outputType = {0};
    tstrncpy(tsRemoteNameBuf, remoteName, sizeof(tsRemoteNameBuf));
    tstrncpy(mappings[0].extTypeName, pTsCol->extTypeName, sizeof(mappings[0].extTypeName));
    tstrncpy(mappings[0].colName, remoteName, sizeof(mappings[0].colName));
    code = extTypeNameToTDengineType(srcType, pTsCol->extTypeName, pTsCol->extCharsetName, &remoteType);
    if (isRelationalSource && TSDB_CODE_EXT_TYPE_NOT_MAPPABLE == code) {
      code = TSDB_CODE_EXT_SCHEMA_CHANGED;
    }
    if (code != TSDB_CODE_SUCCESS) {
      snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
               "external column binding invalid [source=%s, table=%s.%s]: timestamp column '%s' type '%s' is "
               "not supported",
               pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName, pTsCol->colName,
               pTsCol->extTypeName);
    }
    QUERY_CHECK_CODE(code, lino, _return);

    code = fedScanGetVstbOutputType(pFedNode, PRIMARYKEY_TIMESTAMP_COL_ID, true, &outputType);
    if (code != TSDB_CODE_SUCCESS) {
      snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
               "external column binding invalid [source=%s, table=%s.%s]: virtual timestamp output is missing",
               pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName);
    }
    QUERY_CHECK_CODE(code, lino, _return);
    if (!isSameRefDataType(&outputType, &remoteType)) {
      snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
               "external column binding invalid [source=%s, table=%s.%s]: timestamp column '%s' type changed",
               pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName, pTsCol->colName);
      code = isRelationalSource ? TSDB_CODE_EXT_SCHEMA_CHANGED : TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH;
      QUERY_CHECK_CODE(code, lino, _return);
    }
    mappings[0].tdType = outputType;
    mappings[0].tsPrecision = fedScanGetCondPrecision(srcType, pMeta, tsIdx);
    if (srcType == EXT_SOURCE_MYSQL || srcType == EXT_SOURCE_POSTGRESQL) {
      code = fedScanAppendTimestampProjection(
          sqlBuf, sqlBufLen, &offset, srcType, pTsCol->colName,
          pTsCol->extTypeName, remoteTimezone,
          timezoneConversionSupported);
    } else {
      int32_t written = snprintf(sqlBuf + offset, sqlBufLen - offset,
                                 "%c%s%c", q.open, pTsCol->colName, q.close);
      code = written < 0 || written >= sqlBufLen - offset
                 ? TSDB_CODE_OUT_OF_RANGE
                 : TSDB_CODE_SUCCESS;
      if (code == TSDB_CODE_SUCCESS) {
        offset += written;
      }
    }
    if (code != TSDB_CODE_SUCCESS) {
      qError("FederatedScan VStb: timestamp projection exceeds SQL buffer");
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }

  // Columns 1..N: data columns from colMap
  for (int32_t i = 0; i < numDataCols; i++) {
    SColIdNameKV* pKV = (SColIdNameKV*)taosArrayGet(pFsParam->colMap, i);

    // Find matching column in ext table schema by TDengine-side name
    bool found = false;
    for (int32_t j = 0; j < pMeta->numOfCols; j++) {
      SExtColumnDef* pCol = &pMeta->pCols[j];
      if (0 == strcasecmp(pCol->colName, pKV->colName)) {
        const char* remoteName = pCol->colName;
        int32_t   mIdx = 1 + i;
        SDataType remoteType = {0};
        SDataType outputType = {0};
        tstrncpy(mappings[mIdx].extTypeName, pCol->extTypeName, sizeof(mappings[mIdx].extTypeName));
        tstrncpy(mappings[mIdx].colName, remoteName, sizeof(mappings[mIdx].colName));
        code = extTypeNameToTDengineType(srcType, pCol->extTypeName, pCol->extCharsetName, &remoteType);
        if (isRelationalSource && TSDB_CODE_EXT_TYPE_NOT_MAPPABLE == code) {
          code = TSDB_CODE_EXT_SCHEMA_CHANGED;
        }
        if (code != TSDB_CODE_SUCCESS) {
          snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
                   "external column binding invalid [source=%s, table=%s.%s]: column '%s' type '%s' is not "
                   "supported",
                   pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName, pKV->colName,
                   pCol->extTypeName);
        }
        QUERY_CHECK_CODE(code, lino, _return);

        code = fedScanGetVstbOutputType(pFedNode, pKV->colId, false, &outputType);
        if (code != TSDB_CODE_SUCCESS) {
          snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
                   "external column binding invalid [source=%s, table=%s.%s]: virtual column id %d is missing",
                   pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName, pKV->colId);
        }
        QUERY_CHECK_CODE(code, lino, _return);
        if (!isSameRefDataType(&outputType, &remoteType)) {
          snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
                   "external column binding invalid [source=%s, table=%s.%s]: column '%s' type changed to '%s'",
                   pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName, pKV->colName,
                   pCol->extTypeName);
          code = isRelationalSource ? TSDB_CODE_EXT_SCHEMA_CHANGED : TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH;
          QUERY_CHECK_CODE(code, lino, _return);
        }
        mappings[mIdx].tdType = outputType;
        int32_t written = snprintf(sqlBuf + offset, sqlBufLen - offset, ", ");
        if (written < 0 || written >= sqlBufLen - offset) {
          code = TSDB_CODE_OUT_OF_RANGE;
        } else {
          offset += written;
          if ((srcType == EXT_SOURCE_MYSQL ||
               srcType == EXT_SOURCE_POSTGRESQL) &&
              mappings[mIdx].tdType.type == TSDB_DATA_TYPE_TIMESTAMP) {
            code = fedScanAppendTimestampProjection(
                sqlBuf, sqlBufLen, &offset, srcType, pCol->colName,
                pCol->extTypeName, remoteTimezone,
                timezoneConversionSupported);
          } else {
            written = snprintf(sqlBuf + offset, sqlBufLen - offset,
                               "%c%s%c", q.open, pCol->colName, q.close);
            if (written < 0 || written >= sqlBufLen - offset) {
              code = TSDB_CODE_OUT_OF_RANGE;
            } else {
              offset += written;
            }
          }
        }
        if (code != TSDB_CODE_SUCCESS) {
          qError("FederatedScan VStb: column projection exceeds SQL buffer col=%s",
                 pCol->colName);
          QUERY_CHECK_CODE(code, lino, _return);
        }
        found = true;
        break;
      }
    }

    if (!found) {
      snprintf(pInfo->extErrMsg, sizeof(pInfo->extErrMsg),
               "external column binding invalid [source=%s, table=%s.%s]: column '%s' no longer exists",
               pFsParam->sourceName, pFsParam->dbName, pFsParam->tableName, pKV->colName);
      code = isRelationalSource ? TSDB_CODE_EXT_SCHEMA_CHANGED : TSDB_CODE_EXT_COLUMN_NOT_EXIST;
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }

  // Build FROM clause  -  source-type-specific table path
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
    case EXT_SOURCE_SQLSERVER: {
      // SQL Server: [database].[schema].[table].  Both namespace segments are
      // carried explicitly (DS 5.3.6): the connector never injects a default
      // schema, so an unqualified name would resolve against the login's
      // defaults instead of the reference's namespace.  The runtime reference
      // namespace wins over the plan-time defaults when present.
      const char* db = pFsParam->dbName[0]
                           ? pFsParam->dbName
                           : (pFedNode->srcDatabase[0] ? pFedNode->srcDatabase : NULL);
      const char* schema = pFsParam->schemaName[0]
                               ? pFsParam->schemaName
                               : (pFedNode->srcSchema[0] ? pFedNode->srcSchema : NULL);
      if (db != NULL && schema != NULL) {
        offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM [%s].[%s].[%s]", db, schema, remoteTable);
      } else if (schema != NULL) {
        offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM [%s].[%s]", schema, remoteTable);
      } else {
        offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM [%s]", remoteTable);
      }
      break;
    }
    case EXT_SOURCE_INFLUXDB:
    default: {
      // InfluxDB / others: "measurement"
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " FROM \"%s\"", remoteTable);
      break;
    }
  }

  bool        hasWhere = false;
  int32_t     storedFilterSqlLen = 0;
  code = fedScanBuildVStbStoredFilterSql(pFsParam, srcType, &storedFilterSql, &storedFilterSqlLen);
  QUERY_CHECK_CODE(code, lino, _return);
  if (storedFilterSqlLen > 0) {
    int64_t expandedBufLen = (int64_t)sqlBufLen + storedFilterSqlLen + 16;
    QUERY_CHECK_CONDITION(expandedBufLen <= INT32_MAX, code, lino, _return, TSDB_CODE_OUT_OF_RANGE);
    char* pExpandedSqlBuf = taosMemoryRealloc(sqlBuf, (size_t)expandedBufLen);
    QUERY_CHECK_NULL(pExpandedSqlBuf, code, lino, _return, terrno);
    sqlBuf = pExpandedSqlBuf;
    sqlBufLen = (int32_t)expandedBufLen;

    int32_t written = snprintf(sqlBuf + offset, sqlBufLen - offset, " WHERE (%s)", storedFilterSql);
    QUERY_CHECK_CONDITION(written >= 0 && written < sqlBufLen - offset, code, lino, _return,
                          TSDB_CODE_OUT_OF_RANGE);
    offset += written;
    hasWhere = true;
  }

  bool        emptyTimeRange = false;
  STimeWindow scanRange =
      fedScanGetVStbScanRange(pInfo, pFedNode, &emptyTimeRange);

  // Merge the optimizer range with the dynamic external-window batch range.
  // The resulting inclusive bounds are converted to the source precision.
  if (emptyTimeRange || scanRange.skey != INT64_MIN ||
      scanRange.ekey != INT64_MAX) {
    bool hasSkey = !emptyTimeRange && scanRange.skey != INT64_MIN;
    bool hasEkey = !emptyTimeRange && scanRange.ekey != INT64_MAX;
    if (isRelationalSource && NULL != clientTimezone) {
      timezone = tzalloc(clientTimezone);
      QUERY_CHECK_NULL(timezone, code, lino, _return, terrno);
    }
    int8_t rangePrecision = fedScanValidTimePrecision(pFsParam->dstPrecision)
                                ? pFsParam->dstPrecision
                                : TSDB_TIME_PRECISION_MILLI;
    int8_t sourcePrecision = fedScanGetCondPrecision(srcType, pMeta, tsIdx);
    // Comparison-literal timezone: InfluxDB always takes UTC, and for SQL
    // Server a tz-aware column (datetimeoffset) holds an absolute instant, so
    // its literal is rendered as UTC while a tz-naive column
    // (datetime2/datetime/...) keeps the client's wall clock (DS 5.3.7).
    EExtTimestampTzClass tsTzClass =
        fedScanClassifyTimestampType(srcType, pMeta->pCols[tsIdx].extTypeName);
    bool boundAsUtc = (srcType == EXT_SOURCE_INFLUXDB) ||
                      (srcType == EXT_SOURCE_SQLSERVER &&
                       tsTzClass == EXT_TIMESTAMP_TZ_CLASS_AWARE);
    int64_t skey = hasSkey ? fedScanConvertTimeBound(scanRange.skey, rangePrecision,
                                                     sourcePrecision, true)
                           : INT64_MIN;
    int64_t ekey = hasEkey ? fedScanConvertTimeBound(scanRange.ekey, rangePrecision,
                                                     sourcePrecision, false)
                           : INT64_MAX;

    offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%s", hasWhere ? " AND " : " WHERE ");
    hasWhere = true;
    if (emptyTimeRange || (hasSkey && hasEkey && skey > ekey)) {
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "1 = 0");
      hasSkey = false;
      hasEkey = false;
    }
    if (hasSkey) {
      char  skeyBuf[64];
      char* pFormatted = NULL;
      if (boundAsUtc)
        pFormatted = formatTimestampUTC(skeyBuf, sizeof(skeyBuf), skey, sourcePrecision);
      else
        pFormatted = formatTimestampTz(skeyBuf, sizeof(skeyBuf), skey, sourcePrecision, timezone);
      QUERY_CHECK_NULL(pFormatted, code, lino, _return, TSDB_CODE_EXT_SQL_RENDER_ERROR);
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%c%s%c >= '%s'", q.open, tsRemoteNameBuf, q.close,
                         skeyBuf);
    }
    if (hasSkey && hasEkey) {
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " AND ");
    }
    if (hasEkey) {
      char  ekeyBuf[64];
      char* pFormatted = NULL;
      if (boundAsUtc)
        pFormatted = formatTimestampUTC(ekeyBuf, sizeof(ekeyBuf), ekey, sourcePrecision);
      else
        pFormatted = formatTimestampTz(ekeyBuf, sizeof(ekeyBuf), ekey, sourcePrecision, timezone);
      QUERY_CHECK_NULL(pFormatted, code, lino, _return, TSDB_CODE_EXT_SQL_RENDER_ERROR);
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, "%c%s%c <= '%s'", q.open, tsRemoteNameBuf, q.close,
                         ekeyBuf);
    }
  }

  // Append VStb data filter fragments that are safe for this foreign source.
  bool hasPushedDataCond = (pushedCondSql != NULL && pushedCondSqlLen > 0);
  if (hasPushedDataCond) {
    int32_t written =
        snprintf(sqlBuf + offset, sqlBufLen - offset, "%s(%s)", hasWhere ? " AND " : " WHERE ", pushedCondSql);
    QUERY_CHECK_CONDITION(written >= 0 && written < sqlBufLen - offset, code, lino, _return,
                          TSDB_CODE_OUT_OF_RANGE);
    offset += written;
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
  offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " ORDER BY %c%s%c ASC", q.open, tsRemoteNameBuf, q.close);
  if (pFsParam->rowLimit > 0 && (pFsParam->pPushedCond == NULL || pushedCondFullyRendered)) {
    if (EXT_SOURCE_SQLSERVER == srcType) {
      // T-SQL has no LIMIT clause.  The statement always carries ORDER BY ts ASC
      // here, so render the equivalent OFFSET/FETCH form (DS 5.3.6).
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset,
                         " OFFSET 0 ROWS FETCH NEXT %" PRId64 " ROWS ONLY", pFsParam->rowLimit);
    } else {
      offset += snprintf(sqlBuf + offset, sqlBufLen - offset, " LIMIT %" PRId64, pFsParam->rowLimit);
    }
  }

  qDebug("FederatedScan VStb: generated remote SQL source=%s table=%s sql=[%s]",
         pFsParam->sourceName, pFsParam->tableName, sqlBuf);

  *ppSQL = sqlBuf;
  sqlBuf = NULL;
  *ppMappings = mappings;
  mappings = NULL;
  *pNumMappings = totalCols;

_return:
  nodesDestroyNode(pTagCond);
  taosMemoryFree(missingTagSql);
  taosMemoryFree(storedFilterSql);
  taosMemoryFree(pushedCondSql);
  taosMemoryFree(tagCondSql);
  taosMemoryFree(sqlBuf);
  taosMemoryFree(mappings);
  extConnectorFreeTableSchema(pMeta);
  if (NULL != timezone) {
    tzfree(timezone);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// ---------------------------------------------------------------------------
// getNext  -  core execution
// ---------------------------------------------------------------------------

void fedScanReleaseFetchedBlock(SFederatedScanOperatorInfo* pInfo) {
  if (pInfo == NULL) {
    return;
  }

  if (pInfo->prefetchReady && pInfo->pPrefetchedBlock == pInfo->pFetchedBlock) {
    pInfo->pPrefetchParam = NULL;
    pInfo->pPrefetchedBlock = NULL;
    pInfo->prefetchReady = false;
  }
  if (pInfo->pFetchedBlock == NULL) {
    return;
  }
  blockDataDestroy(pInfo->pFetchedBlock);
  pInfo->pFetchedBlock = NULL;
}

static bool fedScanHasStoredFilter(const SFederatedScanOperatorInfo* pInfo) {
  return (pInfo->pFedScanNode->pushdownFlags & FQ_PUSHDOWN_REQUIRED_FILTER) != 0 ||
         (pInfo->pActiveVStbParam != NULL && pInfo->pActiveVStbParam->filterCondLen > 0);
}

static int32_t fedScanMapQueryError(const SFederatedScanOperatorInfo* pInfo, int32_t code,
                                    const SExtPushdownDiagnostic* pDiag) {
  bool hasStoredFilter = fedScanHasStoredFilter(pInfo);
  if (pInfo->pFedScanNode->pRemotePlan == NULL && !hasStoredFilter) {
    return code;
  }

  code = extMapDiagnosticToCode(code, pDiag);
  if (hasStoredFilter) {
    // A retry with fewer pushed expressions must never omit the saved binding filter.
    switch (code) {
      case TSDB_CODE_EXT_REMOTE_INTERNAL:
      case TSDB_CODE_EXT_PUSHDOWN_FAILED:
      case TSDB_CODE_EXT_SYNTAX_UNSUPPORTED:
      case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_FUNC:
      case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OP:
      case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_CLAUSE:
      case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_TYPE:
      case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OVERLAY:
        return TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED;
      default:
        break;
    }
  }
  return code;
}

static int32_t federatedScanGetNextSingle(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
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
  // Step 1: First call  -  connect + generate SQL + issue query
  // =========================================================================
  if (!pInfo->queryStarted) {
    SFederatedScanPhysiNode* pFedNode  = pInfo->pFedScanNode;
    SExtTableNode*           pExtTable = (SExtTableNode*)pFedNode->pExtTable;

    // Each new remote execution attempt must rebuild and refresh the cached SQL.
    // Clear any previous value first so early failures never leave a stale SQL
    // string behind in EXPLAIN or later diagnostics.
    pInfo->remoteSql[0] = '\0';
    pInfo->extErrMsg[0] = '\0';
    pInfo->hasPrefetchDiag = false;
    // 1.1 Build connection config from physi node (no Catalog access in taosd)
    SExtSourceCfg cfg = {0};
    if (pInfo->pActiveVStbParam != NULL) {
      tstrncpy(cfg.source_name, pInfo->pActiveVStbParam->sourceName, sizeof(cfg.source_name));
    } else if (pExtTable != NULL) {
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
    cfg.meta_version = pInfo->pActiveVStbParam != NULL && pInfo->pActiveVStbParam->sourceMetaVersion != 0
                           ? pInfo->pActiveVStbParam->sourceMetaVersion
                           : pFedNode->metaVersion;
    cfg.query_timeout_ms = tsFederatedQueryQueryTimeoutMs;

    qDebug("FederatedScan: connecting source=%s host=%s:%d user=%s type=%s",
           cfg.source_name, cfg.host, cfg.port, cfg.user,
           fedScanSourceTypeName(pFedNode->sourceType));

    // 1.2 Open connection
    code = extConnectorOpen(&cfg, &pInfo->pConnHandle);
    code = fedScanNormalizeInfluxAuthCode(pFedNode->sourceType, cfg.options, code);
    if (code) {
      qError("FederatedScan: connect failed, source=%s host=%s:%d, code=0x%x %s",
             cfg.source_name, cfg.host, cfg.port, code, tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _return);
    }

    // 1.3 Generate remote SQL; passed to extConnectorExecQuery so the
    // Connector uses the same SQL (with REMOTE_VALUE_LIST resolved) instead
    // of regenerating it without subquery resolve context.
    char* remoteSql = NULL;
    if (pFedNode->pRemotePlan == NULL && pInfo->pActiveVStbParam != NULL) {
      // VStb dynamic path: build SQL + col type mappings from ext table schema
      code = fedScanBuildVStbQueryAndMappings(
          pInfo, pInfo->pActiveVStbParam,
          &remoteSql, &pInfo->pDynColMappings, &pInfo->numDynColMappings);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FederatedScan VStb: build query failed, source=%s, code=0x%x %s",
               cfg.source_name, code, tstrerror(code));
        if (!pInfo->parallelPrefetch && '\0' != pInfo->extErrMsg[0]) {
          tstrncpy(pTaskInfo->extErrMsg, pInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
        }
        extConnectorClose(pInfo->pConnHandle);
        pInfo->pConnHandle = NULL;
        QUERY_CHECK_CODE(code, lino, _return);
      }
      qDebug("FederatedScan: VStb remote SQL: %.512s", remoteSql ? remoteSql : "(null)");
      fedScanRememberRemoteSql(pInfo, remoteSql, cfg.source_name);
    } else if (pFedNode->pRemotePlan == NULL) {
      // Mode-2 leaf node has no pRemotePlan; SQL will be generated inside the connector.
      qDebug("FederatedScan: pRemotePlan is NULL (Mode-2 leaf), skipping SQL pre-generation, source=%s",
             cfg.source_name);
      pInfo->remoteSql[0] = '\0';
    } else {
      EExtSourceType sourceType = (EExtSourceType)pFedNode->sourceType;
      const char* clientTimezone =
          pFedNode->timezone[0] != '\0' ? pFedNode->timezone : NULL;
      const char* remoteTimezone = sourceType == EXT_SOURCE_MYSQL
                                       ? fedScanNormalizeMySqlTimezone(
                                             clientTimezone)
                                       : clientTimezone;
      bool timezoneConversionSupported = false;
      if ((sourceType == EXT_SOURCE_MYSQL ||
           sourceType == EXT_SOURCE_POSTGRESQL) &&
          fedScanHasNaiveTimestampHint(pFedNode) &&
          fedScanIsSafeTimezone(remoteTimezone)) {
        int32_t probeCode = extConnectorSupportsTimezoneConversion(
            pInfo->pConnHandle, remoteTimezone,
            &timezoneConversionSupported);
        if (probeCode != TSDB_CODE_SUCCESS) {
          qWarn("FederatedScan: remote timezone probe failed sourceType=%d "
                "timezone=%s, "
                "using local conversion, code=0x%x %s",
                sourceType, remoteTimezone, probeCode,
                tstrerror(probeCode));
          timezoneConversionSupported = false;
        }
      }

      // Build resolve context from the thread-local scalar extra info so that
      // nodesRemotePlanToSQL can expand REMOTE_VALUE_LIST nodes (IN subquery pushdown).
      SNodesRemoteSQLCtx sqlCtx = {
        .pCtx   = gTaskScalarExtra.pSubJobCtx,
        .fp     = (FResolveRemoteForSQL)gTaskScalarExtra.fp,
        .pTimestampTypeHints = pFedNode->pTimestampTypeHints,
        .numTimestampTypeHints = pFedNode->numTimestampTypeHints,
        // DS section5.2.6: pass client timezone so timestamp filter values in WHERE
        // are formatted using the client TZ instead of the server-side global TZ.
        .tzName = remoteTimezone,
        .convertTimestampOutputToEpoch =
            sourceType == EXT_SOURCE_MYSQL ||
            sourceType == EXT_SOURCE_POSTGRESQL,
        .timezoneConversionSupported =
            timezoneConversionSupported,
        .pRuntimeTimeRange = pInfo->hasRuntimeTimeRange ? &pInfo->runtimeTimeRange : NULL,
      };
      code = nodesRemotePlanToSQL((const SLogicNode*)pFedNode->pRemotePlan, pFedNode->sourceType,
                                  &sqlCtx, &remoteSql);
      if (code != TSDB_CODE_SUCCESS) {
        qError("FederatedScan: nodesRemotePlanToSQL failed, source=%s, code=0x%x %s",
               cfg.source_name, code, tstrerror(code));
        if (fedScanHasStoredFilter(pInfo) && code != TSDB_CODE_OUT_OF_MEMORY) {
          code = TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED;
        }
        extConnectorClose(pInfo->pConnHandle);
        pInfo->pConnHandle = NULL;
        QUERY_CHECK_CODE(code, lino, _return);
      }
      qDebug("FederatedScan: remote SQL: %.512s", remoteSql ? remoteSql : "(null)");
      qError("FederatedScan: remote SQL(full) source=%s sql=%s",
             cfg.source_name, remoteSql ? remoteSql : "(null)");
      fedScanRememberRemoteSql(pInfo, remoteSql, cfg.source_name);
    }

    // 1.4 Issue query  -  pass pre-computed SQL so the Connector doesn't
    // regenerate it (which would lose the REMOTE_VALUE_LIST resolution).
    SExtConnectorError extErr = {0};
    code = extConnectorExecQuery(pInfo->pConnHandle, pFedNode, remoteSql,
                                 &pInfo->pQueryHandle, &extErr);
    code = fedScanNormalizeInfluxAuthCode(pFedNode->sourceType, cfg.options, code);
    if (code) {
      SExtPushdownDiagnostic diag = {0};
      (void)extBuildPushdownDiagnostic(code, &extErr, remoteSql, &diag);
      pInfo->hasPrefetchDiag = true;
      pInfo->prefetchDiag = diag;
      if (!pInfo->parallelPrefetch) {
        pTaskInfo->hasExtPushdownDiag = true;
        pTaskInfo->extPushdownDiag = diag;
      }
      code = fedScanMapQueryError(pInfo, code, &diag);
      fedScanFormatError(pInfo, code, &extErr, &diag);
      if (!pInfo->parallelPrefetch) {
        tstrncpy(pTaskInfo->extErrMsg, pInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
      }
      qError("FederatedScan: ext error detail source=%s tdCode=0x%x remoteCode=%d sqlstate=%s httpStatus=%d remoteMessage=%s sql=%s",
             cfg.source_name, extErr.tdCode, extErr.remoteCode,
             extErr.remoteSqlstate[0] != '\0' ? extErr.remoteSqlstate : "(empty)",
             extErr.httpStatus,
             extErr.remoteMessage[0] != '\0' ? extErr.remoteMessage : "(empty)",
             remoteSql ? remoteSql : "(null)");
      qError("FederatedScan: exec query failed, source=%s, code=0x%x %s",
             cfg.source_name, code, pInfo->extErrMsg[0] ? pInfo->extErrMsg : tstrerror(code));
      taosMemoryFree(remoteSql);
      remoteSql = NULL;
      extConnectorClose(pInfo->pConnHandle);
      pInfo->pConnHandle = NULL;
      QUERY_CHECK_CODE(code, lino, _return);
    }

    taosMemoryFree(remoteSql);
    remoteSql = NULL;
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
      SExtPushdownDiagnostic diag = {0};
      (void)extBuildPushdownDiagnostic(code, &fetchErr, NULL, &diag);
      pInfo->hasPrefetchDiag = true;
      pInfo->prefetchDiag = diag;
      if (!pInfo->parallelPrefetch) {
        pTaskInfo->hasExtPushdownDiag = true;
        pTaskInfo->extPushdownDiag = diag;
      }
      code = fedScanMapQueryError(pInfo, code, &diag);
      fedScanFormatError(pInfo, code, &fetchErr, &diag);
      if (!pInfo->parallelPrefetch) {
        tstrncpy(pTaskInfo->extErrMsg, pInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
      }
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
    // (PG/MySQL = micros, InfluxDB = ns) and tag the block column accordingly.
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
      int8_t staticVtableDstPrec = -1;
      if (pInfo->pActiveVStbParam == NULL) {
        for (int32_t c = 0; c < numConvMappings; ++c) {
          if (pConvMappings[c].tdType.type == TSDB_DATA_TYPE_TIMESTAMP) {
            staticVtableDstPrec = pConvMappings[c].tdType.precision;
            break;
          }
        }
        if (staticVtableDstPrec != TSDB_TIME_PRECISION_MILLI &&
            staticVtableDstPrec != TSDB_TIME_PRECISION_MICRO &&
            staticVtableDstPrec != TSDB_TIME_PRECISION_NANO &&
            pInfo->pFedScanNode->node.pOutputDataBlockDesc != NULL) {
          staticVtableDstPrec =
              pInfo->pFedScanNode->node.pOutputDataBlockDesc->precision;
        }
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
        } else if (staticVtableDstPrec == TSDB_TIME_PRECISION_MILLI ||
                   staticVtableDstPrec == TSDB_TIME_PRECISION_MICRO ||
                   staticVtableDstPrec == TSDB_TIME_PRECISION_NANO) {
          dstPrec = staticVtableDstPrec;
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

    if (pInfo->pActiveVStbParam != NULL) {
      SSDataBlock* pFetchedBlock = pBlock;
      code = fedScanNormalizeVstbBlock(pInfo, pBlock, &pBlock);
      QUERY_CHECK_CODE(code, lino, _return);
      if (pBlock != pFetchedBlock) {
        blockDataDestroy(pFetchedBlock);
        pInfo->pFetchedBlock = pBlock;
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
    fedScanLogBlockSummary(pInfo->pFedScanNode, pBlock, "remote-fetch");
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
        int32_t remoteBlockCols = blockCols;
        // Pre-compute external table name for TBNAME pseudo-column fill.
        const char* extTableName = NULL;
        int32_t     extTableNameLen = 0;
        if (pInfo->pActiveVStbParam != NULL && pInfo->pActiveVStbParam->tableName[0] != '\0') {
          extTableName = pInfo->pActiveVStbParam->tableName;
          extTableNameLen = (int32_t)strlen(extTableName);
        } else {
          SExtTableNode* pExtTbl = (SExtTableNode*)pInfo->pFedScanNode->pExtTable;
          if (pExtTbl != NULL) {
          extTableName = (pExtTbl->remoteTableName[0] != '\0')
                           ? pExtTbl->remoteTableName
                           : pExtTbl->table.tableName;
          extTableNameLen = (int32_t)strlen(extTableName);
          }
        }

        // Iterate over the extra slots in the descriptor and append empty columns
        int32_t idx = 0;
        SNode* pNode = NULL;
        FOREACH(pNode, pDesc->pSlots) {
          if (idx >= blockCols) {
            SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
            qDebug("FQ-ExtSlot: descIdx=%d slotId=%d reserve=%d output=%d name=%s rows=%d",
                   idx, (int)pSlot->slotId, (int)pSlot->reserve, (int)pSlot->output, pSlot->name,
                   (int)pBlock->info.rows);
            SColumnInfoData colInfo = createColumnInfoData(
                pSlot->dataType.type, pSlot->dataType.bytes, pSlot->slotId);
            colInfo.info.precision = pSlot->dataType.precision;
            colInfo.info.scale = pSlot->dataType.scale;
            colInfo.info.noData = pSlot->reserve;
            code = blockDataAppendColInfo(pBlock, &colInfo);
            QUERY_CHECK_CODE(code, lino, _return);
            SColumnInfoData* pNewCol = taosArrayGetLast(pBlock->pDataBlock);
            if (pNewCol == NULL) { idx++; continue; }
            code = colInfoDataEnsureCapacity(pNewCol, pBlock->info.rows, true);
            QUERY_CHECK_CODE(code, lino, _return);

            bool slotFilled = false;
            code = fedScanFillConstValueSlot(
                pInfo->pFedScanNode, pDesc, pSlot, idx, pNewCol, (int32_t)pBlock->info.rows, &slotFilled);
            QUERY_CHECK_CODE(code, lino, _return);

            if (!slotFilled) {
              code = fedScanFillMirroredReserveSlot(pInfo->pFedScanNode, pDesc, pSlot, pBlock, remoteBlockCols, idx,
                                                    pNewCol, &slotFilled);
              QUERY_CHECK_CODE(code, lino, _return);
            }

            // Fill TBNAME pseudo-column with the external table name.
            if (!slotFilled && extTableName != NULL && strcasecmp(pSlot->name, "tbname") == 0 &&
                IS_VAR_DATA_TYPE(pSlot->dataType.type)) {
              for (int32_t r = 0; r < (int32_t)pBlock->info.rows; ++r) {
                code = varColSetVarData(pNewCol, r, extTableName, extTableNameLen, false);
                QUERY_CHECK_CODE(code, lino, _return);
              }
              slotFilled = true;
            }

            if (!slotFilled) {
              // Reserve columns without a matching remote output must stay NULL.
              // Leaving their variable buffers uninitialized lets downstream sort
              // operators interpret arbitrary bytes as a var-data length.
              colDataSetNNULL(pNewCol, 0, pBlock->info.rows);
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
        // All rows filtered out  -  fetch next block
        goto _fetchNext;
      }
      qDebug("FQ-Fetch: after local filter rows=%d", (int)pBlock->info.rows);
      fedScanLogBlockSummary(pInfo->pFedScanNode, pBlock, "after-local-filter");
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
    if (!pInfo->parallelPrefetch) {
      pTaskInfo->code = code;
    }
  }
  return code;
}

// ---------------------------------------------------------------------------
// getNextExtFn  -  VTable parameterized fetch (DS section 5.5.6)
// ---------------------------------------------------------------------------

// Build a table key from SForeignScanOperatorParam for context lookup.
static int32_t fedScanBuildTableKey(const SForeignScanOperatorParam* pFsParam, char* buf, int32_t bufLen) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  len = 0;
  uint32_t tagCondHash = 0;
  uint32_t pushedCondHash = 0;
  uint32_t colMapHash = 0;
  uint64_t filterCondHash = 0;
  char*    pCondStr = NULL;

  QUERY_CHECK_NULL(pFsParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(buf, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(bufLen > 0, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pFsParam->tagCondLen >= 0 &&
                            (pFsParam->tagCondLen == 0 || pFsParam->tagCond != NULL) &&
                            pFsParam->filterCondLen >= 0 &&
                            (pFsParam->filterCondLen == 0 || pFsParam->filterCondJson != NULL),
                        code, lino, _return, TSDB_CODE_INVALID_MSG);

  if (pFsParam->tagCondLen > 0) {
    tagCondHash = MurmurHash3_32(pFsParam->tagCond, (uint32_t)pFsParam->tagCondLen);
  }
  if (pFsParam->filterCondLen > 0) {
    filterCondHash = MurmurHash3_64(pFsParam->filterCondJson, (uint32_t)pFsParam->filterCondLen);
  }

  if (pFsParam->pPushedCond != NULL) {
    code = nodesNodeToString(pFsParam->pPushedCond, false, &pCondStr, NULL);
    QUERY_CHECK_CODE(code, lino, _return);
    QUERY_CHECK_NULL(pCondStr, code, lino, _return, TSDB_CODE_APP_ERROR);
    pushedCondHash = MurmurHash3_32(pCondStr, (uint32_t)strlen(pCondStr));
  }

  if (pFsParam->colMap != NULL && taosArrayGetSize(pFsParam->colMap) > 0) {
    colMapHash = MurmurHash3_32(pFsParam->colMap->pData,
                                (uint32_t)(taosArrayGetSize(pFsParam->colMap) * sizeof(SColIdNameKV)));
  }

  len = snprintf(buf, bufLen,
                 "%s.%s.%s.%s.%" PRId64 ".%u.%u.%u.%" PRIu64 ".%d.%" PRIu64 ".%d.%" PRId64 ".%" PRIu64
                 ".%" PRId64 ".%" PRId64 ".%" PRIu64,
                 pFsParam->sourceName, pFsParam->dbName, pFsParam->schemaName, pFsParam->tableName,
                 pFsParam->sourceMetaVersion, colMapHash, tagCondHash, pushedCondHash,
                 pFsParam->scanParamId, pFsParam->filterCondLen, filterCondHash, pFsParam->dstPrecision,
                 pFsParam->rowLimit > 0 ? pFsParam->rowLimit : 0,
                 pFsParam->hasVtbGroupId ? pFsParam->vtbGroupId : (uint64_t)(uint32_t)pFsParam->groupId,
                 pFsParam->window.skey, pFsParam->window.ekey, pFsParam->virtualChildUid);
  QUERY_CHECK_CONDITION(len >= 0 && len < bufLen, code, lino, _return, TSDB_CODE_OUT_OF_RANGE);

_return:
  taosMemoryFree(pCondStr);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

static void fedScanDestroyBatchTable(void* pData) {
  SForeignTableInfo* pTable = pData;
  if (pTable == NULL) {
    return;
  }
  taosArrayDestroy(pTable->colMap);
  taosMemoryFreeClear(pTable->tagCond);
  taosMemoryFreeClear(pTable->filterCondJson);
  taosMemoryFreeClear(pTable->pPushedCondJson);
  nodesDestroyNode(pTable->pPushedCond);
  taosArrayDestroyEx(pTable->pVtbTagList, destroyTagVal);
}

static int32_t fedScanCloneTagList(const SArray* pSrc, SArray** ppDst) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pDst = NULL;

  QUERY_CHECK_NULL(ppDst, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppDst = NULL;
  if (NULL == pSrc) {
    return TSDB_CODE_SUCCESS;
  }
  pDst = taosArrayInit(TMAX(1, taosArrayGetSize(pSrc)), sizeof(STagVal));
  QUERY_CHECK_NULL(pDst, code, lino, _return, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(pSrc); ++i) {
    STagVal* pSrcTag = taosArrayGet(pSrc, i);
    QUERY_CHECK_NULL(pSrcTag, code, lino, _return, terrno);
    STagVal dstTag = *pSrcTag;
    if (IS_VAR_DATA_TYPE(dstTag.type)) {
      dstTag.pData = NULL;
      if (NULL != pSrcTag->pData) {
        dstTag.pData = taosMemoryMalloc(TMAX(1, pSrcTag->nData));
        QUERY_CHECK_NULL(dstTag.pData, code, lino, _return, terrno);
        if (pSrcTag->nData > 0) {
          memcpy(dstTag.pData, pSrcTag->pData, pSrcTag->nData);
        }
      }
    }
    if (NULL == taosArrayPush(pDst, &dstTag)) {
      destroyTagVal(&dstTag);
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }
  *ppDst = pDst;
  return code;

_return:
  taosArrayDestroyEx(pDst, destroyTagVal);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t fedScanCloneFilterCond(const char* pSrc, int32_t len, char** ppDst) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   pDst = NULL;

  QUERY_CHECK_NULL(ppDst, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(len >= 0 && (len == 0 || pSrc != NULL), code, lino, _return,
                        TSDB_CODE_INVALID_MSG);
  *ppDst = NULL;
  if (len == 0) {
    return code;
  }

  pDst = taosMemoryMalloc((size_t)len + 1);
  QUERY_CHECK_NULL(pDst, code, lino, _return, terrno);
  TAOS_MEMCPY(pDst, pSrc, len);
  pDst[len] = '\0';
  *ppDst = pDst;
  return code;

_return:
  taosMemoryFree(pDst);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void fedScanDestroyBatchParam(SForeignScanOperatorParam* pBatch) {
  if (pBatch == NULL) {
    return;
  }
  taosArrayDestroy(pBatch->colMap);
  taosMemoryFreeClear(pBatch->tagCond);
  taosMemoryFreeClear(pBatch->filterCondJson);
  nodesDestroyNode(pBatch->pPushedCond);
  taosArrayDestroyEx(pBatch->pBatchTables, fedScanDestroyBatchTable);
  taosMemoryFree(pBatch);
}

static int32_t fedScanCloneBatchTable(const SForeignTableInfo* pSrc, SForeignTableInfo* pDst) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pSrc, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pDst, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *pDst = *pSrc;
  pDst->colMap = NULL;
  pDst->tagCond = NULL;
  pDst->filterCondJson = NULL;
  pDst->pPushedCondJson = NULL;
  pDst->pPushedCond = NULL;
  pDst->pVtbTagList = NULL;

  if (pSrc->colMap != NULL) {
    pDst->colMap = taosArrayDup(pSrc->colMap, NULL);
    QUERY_CHECK_NULL(pDst->colMap, code, lino, _return, terrno);
  }
  if (pSrc->tagCond != NULL) {
    pDst->tagCond = taosStrdup(pSrc->tagCond);
    QUERY_CHECK_NULL(pDst->tagCond, code, lino, _return, terrno);
  }
  code = fedScanCloneFilterCond(pSrc->filterCondJson, pSrc->filterCondLen,
                                &pDst->filterCondJson);
  QUERY_CHECK_CODE(code, lino, _return);
  if (pSrc->pPushedCondJson != NULL) {
    pDst->pPushedCondJson = taosStrdup(pSrc->pPushedCondJson);
    QUERY_CHECK_NULL(pDst->pPushedCondJson, code, lino, _return, terrno);
  }
  if (pSrc->pPushedCond != NULL) {
    code = nodesCloneNode(pSrc->pPushedCond, &pDst->pPushedCond);
    QUERY_CHECK_CODE(code, lino, _return);
  } else if (pSrc->pPushedCondJson != NULL && pSrc->pPushedCondJson[0] != '\0') {
    code = nodesStringToNode(pSrc->pPushedCondJson, &pDst->pPushedCond);
    QUERY_CHECK_CODE(code, lino, _return);
  }
  code = fedScanCloneTagList(pSrc->pVtbTagList, &pDst->pVtbTagList);
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  if (NULL != pDst) {
    fedScanDestroyBatchTable(pDst);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t fedScanCloneBatchParam(const SForeignScanOperatorParam* pSrc,
                                      SForeignScanOperatorParam** ppDst) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SForeignScanOperatorParam* pDst = NULL;

  QUERY_CHECK_NULL(pSrc, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppDst, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppDst = NULL;
  pDst = taosMemoryCalloc(1, sizeof(*pDst));
  QUERY_CHECK_NULL(pDst, code, lino, _return, terrno);

  *pDst = *pSrc;
  pDst->colMap = NULL;
  pDst->tagCond = NULL;
  pDst->filterCondJson = NULL;
  pDst->pPushedCond = NULL;
  pDst->pBatchTables = NULL;
  pDst->isNewParam = false;

  if (pSrc->colMap != NULL) {
    pDst->colMap = taosArrayDup(pSrc->colMap, NULL);
    QUERY_CHECK_NULL(pDst->colMap, code, lino, _return, terrno);
  }
  if (pSrc->tagCond != NULL) {
    pDst->tagCond = taosStrdup(pSrc->tagCond);
    QUERY_CHECK_NULL(pDst->tagCond, code, lino, _return, terrno);
  }
  code = fedScanCloneFilterCond(pSrc->filterCondJson, pSrc->filterCondLen,
                                &pDst->filterCondJson);
  QUERY_CHECK_CODE(code, lino, _return);
  if (pSrc->pPushedCond != NULL) {
    code = nodesCloneNode(pSrc->pPushedCond, &pDst->pPushedCond);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  int32_t tableNum = taosArrayGetSize(pSrc->pBatchTables);
  pDst->pBatchTables = taosArrayInit(TMAX(1, tableNum), sizeof(SForeignTableInfo));
  QUERY_CHECK_NULL(pDst->pBatchTables, code, lino, _return, terrno);
  for (int32_t i = 0; i < tableNum; ++i) {
    SForeignTableInfo* pSrcTable = taosArrayGet(pSrc->pBatchTables, i);
    SForeignTableInfo  dstTable = {0};
    QUERY_CHECK_NULL(pSrcTable, code, lino, _return, TSDB_CODE_INVALID_PARA);
    code = fedScanCloneBatchTable(pSrcTable, &dstTable);
    QUERY_CHECK_CODE(code, lino, _return);
    if (taosArrayPush(pDst->pBatchTables, &dstTable) == NULL) {
      fedScanDestroyBatchTable(&dstTable);
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }

  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;

_return:
  fedScanDestroyBatchParam(pDst);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

// Evict one entry from pTableCtxMap (closes its connections) to stay within limit.
static int32_t fedScanEvictOneCtx(SFederatedScanOperatorInfo* pInfo) {
  if (pInfo->pTableCtxMap == NULL) return TSDB_CODE_SUCCESS;
  void* pIter = taosHashIterate(pInfo->pTableCtxMap, NULL);
  if (pIter == NULL) return TSDB_CODE_SUCCESS;

  // Get key of first entry (arbitrary eviction  -  hash iteration order)
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
  bool keepAllBatchMergeContexts = pInfo->pBatchMergeHandle != NULL;
  if (pOld == NULL && !keepAllBatchMergeContexts &&
      taosHashGetSize(pInfo->pTableCtxMap) >= FED_SCAN_TABLE_CTX_MAX_SIZE) {
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

/*
 * Activate one single-table parameter and restore its cached connector context.
 *
 * @param pInfo FederatedScan runtime state.
 * @param pFsParam Borrowed single-table parameter to activate.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code.
 */
static int32_t fedScanActivateTableParam(SFederatedScanOperatorInfo* pInfo,
                                         SForeignScanOperatorParam* pFsParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char tableKey[FED_SCAN_TABLE_KEY_LEN] = {0};

  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pFsParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CODE(fedScanBuildTableKey(pFsParam, tableKey, sizeof(tableKey)), lino, _return);

  if (pInfo->pTableCtxMap == NULL) {
    pInfo->pTableCtxMap =
        taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pInfo->pTableCtxMap, code, lino, _return, terrno);
  }

  if (strcmp(pInfo->activeTableKey, tableKey) != 0) {
    code = fedScanSaveActiveCtx(pInfo);
    QUERY_CHECK_CODE(code, lino, _return);
    fedScanRestoreCtx(pInfo, tableKey);
  }

  pInfo->pActiveVStbParam = pFsParam;
  return TSDB_CODE_SUCCESS;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * Build the borrowed single-table view used by the existing VStb SQL path.
 *
 * @param pInfo FederatedScan runtime state receiving the shallow view.
 * @param pBatch Borrowed batch parameter.
 * @param pTable Borrowed table entry inside pBatch.
 *
 * @return Pointer to the stable shallow view owned by pInfo.
 */
static SForeignScanOperatorParam* fedScanBuildBatchTableViewImpl(
    SForeignScanOperatorParam* pView, SFederatedScanOperatorInfo* pInfo,
    SForeignScanOperatorParam* pBatch, SForeignTableInfo* pTable) {
  memset(pView, 0, sizeof(*pView));
  pView->paramType = FOREIGN_SCAN_PARAM_SINGLE;
  tstrncpy(pView->sourceName, pBatch->sourceName, sizeof(pView->sourceName));
  pView->sourceMetaVersion = pBatch->sourceMetaVersion != 0
                                 ? pBatch->sourceMetaVersion
                                 : pInfo->pFedScanNode->metaVersion;
  tstrncpy(pView->dbName, pTable->dbName, sizeof(pView->dbName));
  tstrncpy(pView->schemaName, pTable->schemaName, sizeof(pView->schemaName));
  tstrncpy(pView->tableName, pTable->tableName, sizeof(pView->tableName));
  pView->colMap = pTable->colMap;
  pView->dstPrecision = (pTable->dstPrecision >= TSDB_TIME_PRECISION_MILLI &&
                         pTable->dstPrecision <= TSDB_TIME_PRECISION_NANO)
                            ? pTable->dstPrecision
                            : pBatch->dstPrecision;
  pView->tagCond = pTable->tagCond;
  pView->tagCondLen = pTable->tagCondLen;
  pView->scanParamId = pTable->scanParamId;
  pView->filterCondLen = pTable->filterCondLen;
  pView->filterCondJson = pTable->filterCondJson;
  pView->rowLimit = pTable->rowLimit;
  pView->pPushedCond = pTable->pPushedCond;
  pView->groupId = pBatch->groupId;
  pView->hasVtbGroupId = pBatch->hasVtbGroupId;
  pView->vtbGroupId = pBatch->vtbGroupId;
  pView->pVtbTagList = pTable->pVtbTagList;
  pView->window = pBatch->window;
  pView->virtualChildUid = pTable->virtualChildUid;
  return pView;
}

static SForeignScanOperatorParam* fedScanBuildBatchTableView(SFederatedScanOperatorInfo* pInfo,
                                                             SForeignScanOperatorParam* pBatch,
                                                             SForeignTableInfo* pTable) {
  return fedScanBuildBatchTableViewImpl(&pInfo->activeBatchTableParam, pInfo, pBatch, pTable);
}

typedef struct SFedBatchMergeSourceParam {
  SOperatorInfo*            pOperator;
  int32_t                   tableIndex;
  SForeignScanOperatorParam tableParam;
} SFedBatchMergeSourceParam;

static int32_t fedScanBatchMergeComparFn(const void* pLeft, const void* pRight,
                                         void* pParam) {
  int32_t             leftIdx = *(const int32_t*)pLeft;
  int32_t             rightIdx = *(const int32_t*)pRight;
  SMsortComparParam*   pCompar = pParam;
  SSortSource*         pLeftSource = pCompar->pSources[leftIdx];
  SSortSource*         pRightSource = pCompar->pSources[rightIdx];

  if (pLeftSource->src.rowIndex == -1) {
    return 1;
  }
  if (pRightSource->src.rowIndex == -1) {
    return -1;
  }

  SBlockOrderInfo* pOrder = taosArrayGet(pCompar->orderInfo, 0);
  SColumnInfoData* pLeftTsCol =
      taosArrayGet(pLeftSource->src.pBlock->pDataBlock, pOrder->slotId);
  SColumnInfoData* pRightTsCol =
      taosArrayGet(pRightSource->src.pBlock->pDataBlock, pOrder->slotId);
  int64_t* pLeftTs =
      (int64_t*)pLeftTsCol->pData + pLeftSource->src.rowIndex;
  int64_t* pRightTs =
      (int64_t*)pRightTsCol->pData + pRightSource->src.rowIndex;
  __compar_fn_t compFn = pOrder->compFn;
  int32_t       comp = compFn(pLeftTs, pRightTs);
  if (0 != comp) {
    return comp;
  }

  SFedBatchMergeSourceParam* pLeftParam = pLeftSource->param;
  SFedBatchMergeSourceParam* pRightParam = pRightSource->param;
  if (pLeftParam->tableIndex < pRightParam->tableIndex) {
    return -1;
  }
  return pLeftParam->tableIndex > pRightParam->tableIndex;
}

static void fedScanDestroyBatchMerge(SFederatedScanOperatorInfo* pInfo) {
  if (NULL == pInfo) {
    return;
  }

  tsortDestroySortHandle(pInfo->pBatchMergeHandle);
  pInfo->pBatchMergeHandle = NULL;
  taosArrayDestroy(pInfo->pBatchMergeSortInfo);
  pInfo->pBatchMergeSortInfo = NULL;

  if (pInfo->prefetchReady &&
      pInfo->pPrefetchedBlock == pInfo->pBatchMergeOutput) {
    pInfo->pPrefetchParam = NULL;
    pInfo->pPrefetchedBlock = NULL;
    pInfo->prefetchReady = false;
  }
  blockDataDestroy(pInfo->pBatchMergeOutput);
  pInfo->pBatchMergeOutput = NULL;

  for (int32_t i = 0;
       NULL != pInfo->pBatchMergeSourceParams &&
       i < taosArrayGetSize(pInfo->pBatchMergeSourceParams);
       ++i) {
    SFedBatchMergeSourceParam** ppParam =
        taosArrayGet(pInfo->pBatchMergeSourceParams, i);
    if (NULL != ppParam) {
      taosMemoryFree(*ppParam);
    }
  }
  taosArrayDestroy(pInfo->pBatchMergeSourceParams);
  pInfo->pBatchMergeSourceParams = NULL;
  pInfo->batchMergeTsSlot = -1;
}

/*
 * Fetch one timestamp-ordered block for a single source of the batch merge.
 * The table context retains block ownership until this source asks for its
 * next block, so tsort can safely reference one current block per source.
 */
static int32_t fedScanLoadBatchMergeBlock(void* param, SSDataBlock** ppBlock) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFedBatchMergeSourceParam*  pSource = param;
  SFederatedScanOperatorInfo* pInfo = NULL;
  SForeignScanOperatorParam*  pBatch = NULL;

  QUERY_CHECK_NULL(pSource, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOperator, code, lino, _return,
                   TSDB_CODE_INVALID_PARA);
  pInfo = pSource->pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pBatch = pInfo->pActiveBatchParam;
  QUERY_CHECK_NULL(pBatch, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(
      NULL != pBatch->pBatchTables && pSource->tableIndex >= 0 &&
          pSource->tableIndex < taosArrayGetSize(pBatch->pBatchTables),
      code, lino, _return, TSDB_CODE_INVALID_PARA);

  SForeignTableInfo* pTable =
      taosArrayGet(pBatch->pBatchTables, pSource->tableIndex);
  QUERY_CHECK_NULL(pTable, code, lino, _return, TSDB_CODE_INVALID_PARA);
  SForeignScanOperatorParam* pTableParam = fedScanBuildBatchTableViewImpl(
      &pSource->tableParam, pInfo, pBatch, pTable);
  code = fedScanActivateTableParam(pInfo, pTableParam);
  QUERY_CHECK_CODE(code, lino, _return);

  // A single table EOF must not complete the whole batch operator.
  pSource->pOperator->status = OP_NOT_OPENED;
  code = federatedScanGetNextSingle(pSource->pOperator, ppBlock);
  pSource->pOperator->status = OP_NOT_OPENED;
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  if (NULL != ppBlock) {
    *ppBlock = NULL;
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t fedScanCreateBatchMerge(SOperatorInfo* pOperator,
                                       SForeignScanOperatorParam* pBatch) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFederatedScanOperatorInfo* pInfo = NULL;
  SSDataBlock*                pTemplate = NULL;
  SSortSource*                pSortSource = NULL;
  SFedBatchMergeSourceParam*  pSourceParam = NULL;

  QUERY_CHECK_NULL(pOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBatch, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pInfo = pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBatch->pBatchTables, code, lino, _return,
                   TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pInfo->pFedScanNode->node.pOutputDataBlockDesc, code, lino,
                   _return, TSDB_CODE_PLAN_INTERNAL_ERROR);

  int32_t tableNum = (int32_t)taosArrayGetSize(pBatch->pBatchTables);
  QUERY_CHECK_CONDITION(tableNum > 1, code, lino, _return,
                        TSDB_CODE_INVALID_PARA);
  pInfo->batchMergeTsSlot = fedScanFindVstbOutputIndex(
      pInfo->pFedScanNode, PRIMARYKEY_TIMESTAMP_COL_ID, true);
  QUERY_CHECK_CONDITION(pInfo->batchMergeTsSlot >= 0, code, lino, _return,
                        TSDB_CODE_PLAN_SLOT_NOT_FOUND);

  pTemplate = createDataBlockFromDescNode(
      pInfo->pFedScanNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pTemplate, code, lino, _return, terrno);
  SColumnInfoData* pTsCol =
      taosArrayGet(pTemplate->pDataBlock, pInfo->batchMergeTsSlot);
  QUERY_CHECK_NULL(pTsCol, code, lino, _return, TSDB_CODE_PLAN_SLOT_NOT_FOUND);

  pInfo->pBatchMergeSortInfo =
      taosArrayInit(1, sizeof(SBlockOrderInfo));
  QUERY_CHECK_NULL(pInfo->pBatchMergeSortInfo, code, lino, _return, terrno);
  SBlockOrderInfo order = {
      .nullFirst = true,
      .order = TSDB_ORDER_ASC,
      .slotId = pInfo->batchMergeTsSlot,
      .compFn = getKeyComparFunc(pTsCol->info.type, TSDB_ORDER_ASC),
  };
  QUERY_CHECK_NULL(taosArrayPush(pInfo->pBatchMergeSortInfo, &order), code,
                   lino, _return, terrno);

  int32_t pageSize = getProperSortPageSize(
      blockDataGetRowSize(pTemplate),
      (uint32_t)taosArrayGetSize(pTemplate->pDataBlock));
  code = tsortCreateSortHandle(
      pInfo->pBatchMergeSortInfo, SORT_MULTISOURCE_TS_MERGE, pageSize,
      tableNum + 1,
      pTemplate, pOperator->pTaskInfo->id.str, 0, 0, 0,
      &pInfo->pBatchMergeHandle);
  QUERY_CHECK_CODE(code, lino, _return);
  tsortSetFetchRawDataFp(pInfo->pBatchMergeHandle,
                         fedScanLoadBatchMergeBlock, NULL, NULL);
  tsortSetComparFp(pInfo->pBatchMergeHandle,
                   fedScanBatchMergeComparFn);

  code = tsortGetSortedDataBlock(pInfo->pBatchMergeHandle,
                                 &pInfo->pBatchMergeOutput);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_NULL(pInfo->pBatchMergeOutput, code, lino, _return, terrno);
  pInfo->pBatchMergeSourceParams =
      taosArrayInit(tableNum, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pBatchMergeSourceParams, code, lino, _return,
                   terrno);

  for (int32_t i = 0; i < tableNum; ++i) {
    pSourceParam = taosMemoryCalloc(1, sizeof(*pSourceParam));
    QUERY_CHECK_NULL(pSourceParam, code, lino, _return, terrno);
    pSourceParam->pOperator = pOperator;
    pSourceParam->tableIndex = i;

    pSortSource = taosMemoryCalloc(1, sizeof(*pSortSource));
    QUERY_CHECK_NULL(pSortSource, code, lino, _return, terrno);
    pSortSource->param = pSourceParam;
    pSortSource->onlyRef = true;
    QUERY_CHECK_NULL(
        taosArrayPush(pInfo->pBatchMergeSourceParams, &pSourceParam), code,
        lino, _return, terrno);
    pSourceParam = NULL;
    code = tsortAddSource(pInfo->pBatchMergeHandle, pSortSource);
    QUERY_CHECK_CODE(code, lino, _return);
    pSortSource = NULL;
  }

  code = tsortOpen(pInfo->pBatchMergeHandle);
  QUERY_CHECK_CODE(code, lino, _return);
  blockDataDestroy(pTemplate);
  return code;

_return:
  taosMemoryFree(pSourceParam);
  taosMemoryFree(pSortSource);
  blockDataDestroy(pTemplate);
  fedScanDestroyBatchMerge(pInfo);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t federatedScanContinueMergedBatch(
    SOperatorInfo* pOperator, SForeignScanOperatorParam* pBatch,
    SSDataBlock** ppRes) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFederatedScanOperatorInfo* pInfo = NULL;

  QUERY_CHECK_NULL(pOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBatch, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppRes, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pInfo = pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppRes = NULL;

  if (OP_EXEC_DONE == pOperator->status) {
    return code;
  }
  if (NULL == pInfo->pBatchMergeHandle) {
    code = fedScanCreateBatchMerge(pOperator, pBatch);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  blockDataCleanup(pInfo->pBatchMergeOutput);
  int32_t capacity = TMAX(1, pOperator->resultInfo.capacity);
  code = blockDataEnsureCapacity(pInfo->pBatchMergeOutput, capacity);
  QUERY_CHECK_CODE(code, lino, _return);

  while (pInfo->pBatchMergeOutput->info.rows < capacity) {
    STupleHandle* pTuple = NULL;
    code = tsortNextTuple(pInfo->pBatchMergeHandle, &pTuple);
    QUERY_CHECK_CODE(code, lino, _return);
    if (NULL == pTuple) {
      break;
    }
    code = tsortAppendTupleToBlock(
        pInfo->pBatchMergeHandle, pInfo->pBatchMergeOutput, pTuple);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  if (0 == pInfo->pBatchMergeOutput->info.rows) {
    setOperatorCompleted(pOperator);
    return code;
  }

  pInfo->pBatchMergeOutput->info.id.blockId = pOperator->resultDataBlockId;
  if (pBatch->hasVtbGroupId) {
    pInfo->pBatchMergeOutput->info.id.groupId = pBatch->vtbGroupId;
  } else if (pBatch->groupId >= 0) {
    pInfo->pBatchMergeOutput->info.id.groupId =
        (uint64_t)pBatch->groupId;
  }
  code = blockDataUpdateTsWindow(pInfo->pBatchMergeOutput,
                                 pInfo->batchMergeTsSlot);
  QUERY_CHECK_CODE(code, lino, _return);
  pOperator->status = OP_NOT_OPENED;
  *ppRes = pInfo->pBatchMergeOutput;
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * Consume all remote tables in a source batch without exposing per-table EOF.
 *
 * @param pOperator FederatedScan operator.
 * @param pBatch Borrowed source batch parameter.
 * @param ppRes Output data block.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code.
 */
static int32_t federatedScanContinueBatch(SOperatorInfo* pOperator, SForeignScanOperatorParam* pBatch,
                                          SSDataBlock** ppRes) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFederatedScanOperatorInfo* pInfo = NULL;
  QUERY_CHECK_NULL(pOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBatch, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppRes, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pInfo = pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  int32_t                     tableNum = (int32_t)taosArrayGetSize(pBatch->pBatchTables);

  *ppRes = NULL;
  if (pOperator->status == OP_EXEC_DONE) {
    return TSDB_CODE_SUCCESS;
  }

  while (pInfo->currentBatchIdx < tableNum) {
    SForeignTableInfo* pTable = taosArrayGet(pBatch->pBatchTables, pInfo->currentBatchIdx);
    QUERY_CHECK_NULL(pTable, code, lino, _return, TSDB_CODE_INVALID_PARA);

    SForeignScanOperatorParam* pTableParam = fedScanBuildBatchTableView(pInfo, pBatch, pTable);
    code = fedScanActivateTableParam(pInfo, pTableParam);
    QUERY_CHECK_CODE(code, lino, _return);

    code = federatedScanGetNextSingle(pOperator, ppRes);
    QUERY_CHECK_CODE(code, lino, _return);
    if (*ppRes != NULL) {
      return TSDB_CODE_SUCCESS;
    }
    if (!pInfo->queryFinished) {
      return TSDB_CODE_SUCCESS;
    }

    ++pInfo->currentBatchIdx;
    if (pInfo->currentBatchIdx < tableNum) {
      pOperator->status = OP_NOT_OPENED;
    }
  }

  setOperatorCompleted(pOperator);
  return TSDB_CODE_SUCCESS;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t federatedScanGetNextBatch(SOperatorInfo* pOperator, SForeignScanOperatorParam* pInputBatch,
                                         SSDataBlock** ppRes) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFederatedScanOperatorInfo* pInfo = NULL;
  QUERY_CHECK_NULL(pOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pInputBatch, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppRes, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pInfo = pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);

  if (pOperator->status == OP_EXEC_DONE && pInfo->batchMode && !pInputBatch->isNewParam &&
      pInfo->pBatchInputIdentity == pInputBatch) {
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (pInfo->pActiveBatchParam == NULL || pInfo->pBatchInputIdentity != pInputBatch ||
      pInputBatch->isNewParam) {
    federatedScanResetForNewChild(pOperator);
    code = fedScanCloneBatchParam(pInputBatch, &pInfo->pActiveBatchParam);
    QUERY_CHECK_CODE(code, lino, _return);
    pInfo->pBatchInputIdentity = pInputBatch;
    pInfo->currentBatchIdx = 0;
    pInfo->activeBatchTableCount = taosArrayGetSize(pInfo->pActiveBatchParam->pBatchTables);
    pInfo->batchMode = true;
    pOperator->status = OP_NOT_OPENED;
    tstrncpy(pInfo->activeBatchSourceName, pInfo->pActiveBatchParam->sourceName,
             sizeof(pInfo->activeBatchSourceName));
    pInputBatch->isNewParam = false;
    pInfo->runtimeTimeRange = pInfo->pActiveBatchParam->window;
    pInfo->hasRuntimeTimeRange = pInfo->runtimeTimeRange.skey <= pInfo->runtimeTimeRange.ekey;
  }

  if (pInfo->activeBatchTableCount > 1 &&
      !pInfo->pActiveBatchParam->allowUnorderedBatch) {
    code = federatedScanContinueMergedBatch(
        pOperator, pInfo->pActiveBatchParam, ppRes);
  } else {
    code = federatedScanContinueBatch(
        pOperator, pInfo->pActiveBatchParam, ppRes);
  }
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t federatedScanGetNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SFederatedScanOperatorInfo* pInfo = NULL;
  QUERY_CHECK_NULL(pOperator, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppRes, code, lino, _return, TSDB_CODE_INVALID_PARA);
  pInfo = pOperator->info;
  QUERY_CHECK_NULL(pInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);

  SOperatorParam* pGetParam = pOperator->pOperatorGetParam;
  if (NULL != pGetParam &&
      QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN == pGetParam->opType &&
      -1 != pGetParam->downstreamIdx && NULL == pGetParam->pChildren &&
      NULL != pGetParam->value &&
      FOREIGN_SCAN_PARAM_BATCH ==
          ((SForeignScanOperatorParam*)pGetParam->value)->paramType) {
    code = federatedScanGetNextBatch(
        pOperator, (SForeignScanOperatorParam*)pGetParam->value, ppRes);
  } else if (pInfo->batchMode && pInfo->pActiveBatchParam != NULL) {
    if (pInfo->activeBatchTableCount > 1) {
      code = federatedScanContinueMergedBatch(
          pOperator, pInfo->pActiveBatchParam, ppRes);
    } else {
      code = federatedScanContinueBatch(
          pOperator, pInfo->pActiveBatchParam, ppRes);
    }
  } else {
    code = federatedScanGetNextSingle(pOperator, ppRes);
  }
  QUERY_CHECK_CODE(code, lino, _return);
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t federatedScanGetNextExtFn(SOperatorInfo*  pOperator,
                                          SOperatorParam* pParam,
                                          SSDataBlock**   ppRes) {
  QRY_PARAM_CHECK(ppRes);

  SFederatedScanOperatorInfo* pInfo = pOperator->info;

  if (pInfo->prefetchReady) {
    if (pInfo->pPrefetchParam == pParam) {
      SForeignScanOperatorParam* pFsParam = pParam->value;
      bool windowUnchanged = pFsParam != NULL && pFsParam->paramType == FOREIGN_SCAN_PARAM_BATCH &&
                             pFsParam->window.skey == pInfo->prefetchWindow.skey &&
                             pFsParam->window.ekey == pInfo->prefetchWindow.ekey;
      if (windowUnchanged) {
        pFsParam->isNewParam = false;
        *ppRes = pInfo->pPrefetchedBlock;
        pInfo->pPrefetchParam = NULL;
        pInfo->pPrefetchedBlock = NULL;
        pInfo->prefetchReady = false;
        return TSDB_CODE_SUCCESS;
      }
    }
    pInfo->pPrefetchParam = NULL;
    pInfo->pPrefetchedBlock = NULL;
    pInfo->prefetchReady = false;
  }

  if (pParam != NULL && pParam->opType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN &&
      pParam->downstreamIdx != -1 && pParam->pChildren == NULL && pParam->value != NULL &&
      ((SForeignScanOperatorParam*)pParam->value)->paramType == FOREIGN_SCAN_PARAM_BATCH) {
    SForeignScanOperatorParam* pBatch = pParam->value;
    if (pBatch->pBatchTables == NULL || taosArrayGetSize(pBatch->pBatchTables) == 0) {
      qError("%s received an empty federated scan batch for source %s", __func__, pBatch->sourceName);
      return TSDB_CODE_INVALID_PARA;
    }
    return federatedScanGetNextBatch(pOperator, pBatch, ppRes);
  }

  if (pParam != NULL && pParam->opType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN &&
      (pParam->downstreamIdx == -1 || pParam->pChildren != NULL) &&
      pParam->value != NULL) {
    pInfo->runtimeTimeRange = ((SDynQueryCtrlOperatorParam*)pParam->value)->window;
    pInfo->hasRuntimeTimeRange = true;
    qDebug("FederatedScan: apply dynamic VtableWindow range [%" PRId64 ", %" PRId64 ")",
           pInfo->runtimeTimeRange.skey, pInfo->runtimeTimeRange.ekey);
  }

  if (pParam != NULL && pParam->opType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN &&
      pParam->downstreamIdx != -1 && pParam->pChildren == NULL) {
    // Multi-table dispatch from VStb: use per-table context caching
    if (pOperator->pOperatorGetParam != pParam) {
      int32_t code = setOperatorParams(pOperator, pParam, OP_GET_PARAM);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    SForeignScanOperatorParam* pFsParam = (SForeignScanOperatorParam*)pParam->value;
    int32_t code = fedScanActivateTableParam(pInfo, pFsParam);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    return federatedScanGetNext(pOperator, ppRes);
  }

  // Legacy path: single-table or non-VStb param  -  tear down and restart
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

int32_t federatedScanPrefetch(SOperatorInfo* pOperator, SOperatorParam* pParam) {
  if (pOperator == NULL || pParam == NULL || pOperator->info == NULL ||
      pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    return TSDB_CODE_INVALID_PARA;
  }

  SFederatedScanOperatorInfo* pInfo = pOperator->info;
  if (pInfo->prefetchReady) {
    return pInfo->pPrefetchParam == pParam ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
  }

  SForeignScanOperatorParam* pFsParam = pParam->value;
  if (pParam->downstreamIdx == -1 || pParam->pChildren != NULL || pFsParam == NULL ||
      pFsParam->paramType != FOREIGN_SCAN_PARAM_BATCH) {
    return TSDB_CODE_INVALID_PARA;
  }

  SSDataBlock* pBlock = NULL;
  pInfo->parallelPrefetch = true;
  int32_t code = pOperator->fpSet.getNextExtFn(pOperator, pParam, &pBlock);
  pInfo->parallelPrefetch = false;
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->pPrefetchParam = pParam;
  pInfo->pPrefetchedBlock = pBlock;
  pInfo->prefetchWindow = pFsParam->window;
  pInfo->prefetchReady = true;
  return TSDB_CODE_SUCCESS;
}

void federatedScanPropagatePrefetchError(SOperatorInfo* pOperator, int32_t code) {
  if (pOperator == NULL || pOperator->info == NULL || pOperator->pTaskInfo == NULL) {
    return;
  }

  SFederatedScanOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->code = code;
  if (pInfo->hasPrefetchDiag) {
    pTaskInfo->hasExtPushdownDiag = true;
    pTaskInfo->extPushdownDiag = pInfo->prefetchDiag;
  }
  if (pInfo->extErrMsg[0] != '\0') {
    tstrncpy(pTaskInfo->extErrMsg, pInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
  }
}

// ---------------------------------------------------------------------------
// Reset FederatedScan state for new VStb child  -  closes cached connections
// ---------------------------------------------------------------------------

void federatedScanResetForNewChild(SOperatorInfo* pOperator) {
  SFederatedScanOperatorInfo* pInfo = (SFederatedScanOperatorInfo*)pOperator->info;
  if (!pInfo) return;

  fedScanDestroyBatchMerge(pInfo);

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
  fedScanDestroyBatchParam(pInfo->pActiveBatchParam);
  pInfo->pActiveBatchParam = NULL;
  pInfo->pBatchInputIdentity = NULL;
  pInfo->currentBatchIdx = 0;
  pInfo->activeBatchTableCount = 0;
  pInfo->activeBatchSourceName[0] = '\0';
  pInfo->batchMode = false;
  pInfo->runtimeTimeRange =
      (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pInfo->hasRuntimeTimeRange = false;
  memset(&pInfo->activeBatchTableParam, 0, sizeof(pInfo->activeBatchTableParam));
}

// ---------------------------------------------------------------------------
// close  -  release all resources
// ---------------------------------------------------------------------------

static void federatedScanClose(void* param) {
  SFederatedScanOperatorInfo* pInfo = (SFederatedScanOperatorInfo*)param;
  if (!pInfo) return;

  fedScanDestroyBatchMerge(pInfo);

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
  fedScanDestroyBatchParam(pInfo->pActiveBatchParam);
  pInfo->pActiveBatchParam = NULL;
  pInfo->pBatchInputIdentity = NULL;
  qDebug("FederatedScan closed: rows=%" PRId64 ", blocks=%" PRId64 ", elapsed=%" PRId64 "us",
         pInfo->fetchedRows, pInfo->fetchBlockCount, pInfo->elapsedTimeUs);

  taosMemoryFreeClear(pInfo);
}

// ---------------------------------------------------------------------------
// getExplainFn  -  verbose EXPLAIN ANALYZE output
// ---------------------------------------------------------------------------

static int32_t federatedScanGetExplainInfo(SOperatorInfo* pOperator,
                                           void**         ppOptrExplain,
                                           uint32_t*      pLen) {
  SFederatedScanOperatorInfo* pInfo = pOperator->info;

  SFederatedScanExplainInfo* pExInfo =
      taosMemoryCalloc(1, sizeof(SFederatedScanExplainInfo));
  if (!pExInfo) {
    qError("federatedScanGetExplainInfo: OOM allocating explain info");
    return terrno;
  }

  pExInfo->fetchedRows     = pInfo->fetchedRows;
  pExInfo->fetchBlockCount = pInfo->fetchBlockCount;
  pExInfo->elapsedTimeUs   = pInfo->elapsedTimeUs;
  tstrncpy(pExInfo->remoteSql, pInfo->remoteSql, sizeof(pExInfo->remoteSql));
  if (pInfo->batchMode) {
    pExInfo->batchMode = true;
    pExInfo->batchTables = pInfo->activeBatchTableCount;
    tstrncpy(pExInfo->sourceName, pInfo->activeBatchSourceName, sizeof(pExInfo->sourceName));
  } else if (pInfo->pActiveVStbParam != NULL) {
    tstrncpy(pExInfo->sourceName, pInfo->pActiveVStbParam->sourceName, sizeof(pExInfo->sourceName));
  }

  *ppOptrExplain = pExInfo;
  *pLen          = (uint32_t)sizeof(SFederatedScanExplainInfo);
  return TSDB_CODE_SUCCESS;
}

// The optimizer injects a virtual table's timestamp range into the external
// scan as a *source-precision* condition (it doubles as the remote condition).
// The scan converts every fetched timestamp to the virtual table's precision
// before applying its local filter, so the injected literal must be converted
// as well.  Without this, a fine local database (PRECISION 'ns') fed by a
// coarser source (SQL Server, us) compares microseconds against nanoseconds
// and silently drops the leg.  Direct external scans keep the scan precision
// equal to the source precision, so the conversion is a no-op for them.
static EDealRes fedScanNormalizeLocalTsLiteral(SNode* pNode, void* pContext) {
  if (QUERY_NODE_OPERATOR != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }
  SOperatorNode* pOp = (SOperatorNode*)pNode;
  if (pOp->opType < OP_TYPE_GREATER_THAN || pOp->opType >= OP_TYPE_IS_NOT_UNKNOWN) {
    return DEAL_RES_CONTINUE;
  }

  // Unary predicates (IS [NOT] NULL / TRUE / FALSE / UNKNOWN) carry a NULL
  // operand; nodeType() dereferences its argument.
  if (pOp->pLeft == NULL || pOp->pRight == NULL) {
    return DEAL_RES_CONTINUE;
  }

  SColumnNode* pCol = NULL;
  SValueNode*  pValue = NULL;
  if (QUERY_NODE_COLUMN == nodeType(pOp->pLeft) && QUERY_NODE_VALUE == nodeType(pOp->pRight)) {
    pCol = (SColumnNode*)pOp->pLeft;
    pValue = (SValueNode*)pOp->pRight;
  } else if (QUERY_NODE_VALUE == nodeType(pOp->pLeft) && QUERY_NODE_COLUMN == nodeType(pOp->pRight)) {
    pCol = (SColumnNode*)pOp->pRight;
    pValue = (SValueNode*)pOp->pLeft;
  } else {
    return DEAL_RES_CONTINUE;
  }

  if (pCol->colId != PRIMARYKEY_TIMESTAMP_COL_ID ||
      pValue->node.resType.type != TSDB_DATA_TYPE_TIMESTAMP ||
      pValue->placeholderNo > 0 || pValue->isNull) {
    return DEAL_RES_CONTINUE;
  }

  int8_t srcPrecision = (int8_t)pValue->node.resType.precision;
  int8_t dstPrecision = *(int8_t*)pContext;
  if (srcPrecision == dstPrecision ||
      srcPrecision < TSDB_TIME_PRECISION_MILLI || srcPrecision > TSDB_TIME_PRECISION_NANO ||
      dstPrecision < TSDB_TIME_PRECISION_MILLI || dstPrecision > TSDB_TIME_PRECISION_NANO) {
    return DEAL_RES_CONTINUE;
  }

  pValue->datum.i = convertTimePrecision(pValue->datum.i, srcPrecision, dstPrecision);
  *(int64_t*)&pValue->typeData = pValue->datum.i;
  pValue->node.resType.precision = dstPrecision;
  pCol->node.resType.precision = dstPrecision;
  return DEAL_RES_CONTINUE;
}

// ---------------------------------------------------------------------------
// createFederatedScanOperatorInfo  -  public factory function
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
  initResultSizeInfo(&pOperator->resultInfo, FED_SCAN_RESULT_BLOCK_ROWS);

  // Store reference to physi node (not owned  -  lifetime managed by plan)
  pInfo->pFedScanNode = pFedScanNode;
  pInfo->underVTableScan = pFedScanNode->underVTableScan;
  if (pFedScanNode->pRemotePlan != NULL) {
    SLogicNode* pRemoteRoot = (SLogicNode*)pFedScanNode->pRemotePlan;
    if (pRemoteRoot->pTargets == NULL || LIST_LENGTH(pRemoteRoot->pTargets) == 0) {
      qError("FederatedScan: remote plan exists but top remote targets are empty at operator init, remoteRootType=%d scanCols=%d",
             (int)nodeType((SNode*)pRemoteRoot),
             pFedScanNode->pScanCols ? (int)LIST_LENGTH(pFedScanNode->pScanCols) : 0);
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      goto _error;
    }
  }

  // pColTypeMappings and pOutputDataBlockDesc are fully computed by the planner
  // and preserved through JSON serialization.  The executor uses them as-is.

  // FederatedScan is a leaf node - no downstream
  pInfo->twoPassMode = pFedScanNode->twoPassMode;
  pInfo->twoPassPhase1Done = false;
  pOperator->pPhyNode = (SPhysiNode*)pFedScanNode;

  setOperatorInfo(pOperator, "FederatedScanOperator",
                  QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN,
                  false, OP_NOT_OPENED, pInfo, pTaskInfo);

  // Initialize local filter from pConditions (for conditions like like_in_set
  // that cannot be pushed down to the remote source).  Timestamp literals
  // injected by the planner carry the source precision; normalize them to the
  // precision of the blocks this operator emits (see
  // fedScanNormalizeLocalTsLiteral).
  if (pFedScanNode->node.pConditions != NULL) {
    int8_t localTsPrecision = -1;
    if (pFedScanNode->node.pOutputDataBlockDesc != NULL) {
      localTsPrecision = (int8_t)pFedScanNode->node.pOutputDataBlockDesc->precision;
    }
    if (localTsPrecision >= TSDB_TIME_PRECISION_MILLI &&
        localTsPrecision <= TSDB_TIME_PRECISION_NANO) {
      nodesWalkExpr((SNode*)pFedScanNode->node.pConditions, fedScanNormalizeLocalTsLiteral,
                    &localTsPrecision);
    }
    code = filterInitFromNode((SNode*)pFedScanNode->node.pConditions,
                              &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pOperator->fpSet = createOperatorFpSet(
      optrDummyOpenFn,           // open: lazy  -  real connect happens in getNext
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
