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
#include "filter.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tglobal.h"

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
// getNext — core execution
// ---------------------------------------------------------------------------

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

    // 1.2 Open connection
    // On TSDB_CODE_EXT_RESOURCE_EXHAUSTED the error is returned to the caller;
    // retry (if desired) must be done asynchronously by the client using a ref ID,
    // never by blocking the current thread.
    code = extConnectorOpen(&cfg, &pInfo->pConnHandle);
    if (code) {
      qError("FederatedScan: connect failed, source=%s host=%s:%d, code=0x%x %s",
             cfg.source_name, cfg.host, cfg.port, code, tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _return);
    }

    // 1.3 Generate remote SQL; passed to extConnectorExecQuery so the
    // Connector uses the same SQL (with REMOTE_VALUE_LIST resolved) instead
    // of regenerating it without subquery resolve context.
    char* remoteSql = NULL;
    if (pFedNode->pRemotePlan == NULL) {
      // Mode-2 leaf node has no pRemotePlan; SQL will be generated inside the connector.
      qDebug("FederatedScan: pRemotePlan is NULL (Mode-2 leaf), skipping SQL pre-generation, source=%s",
             cfg.source_name);
    } else {
      // Build resolve context from the thread-local scalar extra info so that
      // nodesRemotePlanToSQL can expand REMOTE_VALUE_LIST nodes (IN subquery pushdown).
      SNodesRemoteSQLCtx sqlCtx = {
        .pCtx = gTaskScalarExtra.pSubJobCtx,
        .fp   = (FResolveRemoteForSQL)gTaskScalarExtra.fp,
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
      qDebug("FederatedScan: remote SQL: %.512s", remoteSql ? remoteSql : "(null)");
    }

    // 1.4 Issue query — pass pre-computed SQL so the Connector doesn't
    // regenerate it (which would lose the REMOTE_VALUE_LIST resolution).
    SExtConnectorError extErr = {0};
    code = extConnectorExecQuery(pInfo->pConnHandle, pFedNode, remoteSql,
                                 &pInfo->pQueryHandle, &extErr);
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
  {
    SSDataBlock*        pBlock   = NULL;
    SExtConnectorError  fetchErr = {0};
    int64_t             startTs  = taosGetTimestampUs();

    code = extConnectorFetchBlock(pInfo->pQueryHandle,
                                  pInfo->pFedScanNode->pColTypeMappings,
                                  pInfo->pFedScanNode->numColTypeMappings,
                                  &pBlock, &fetchErr);
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
      pInfo->queryFinished = true;
      setOperatorCompleted(pOperator);
      qDebug("FederatedScan: EOF, totalRows=%" PRId64 ", blocks=%" PRId64
             ", elapsed=%" PRId64 "us",
             pInfo->fetchedRows, pInfo->fetchBlockCount, pInfo->elapsedTimeUs);
      *ppRes = NULL;
      return TSDB_CODE_SUCCESS;
    }

    pInfo->fetchedRows += pBlock->info.rows;
    pInfo->fetchBlockCount++;

    // Extend block with extra columns for pushed-down expression slots
    // (e.g., CASE WHEN results needed by the parent Aggregate operator).
    SDataBlockDescNode* pDesc = pInfo->pFedScanNode->node.pOutputDataBlockDesc;
    if (pDesc != NULL) {
      int32_t descSlots = LIST_LENGTH(pDesc->pSlots);
      int32_t blockCols = taosArrayGetSize(pBlock->pDataBlock);
      if (descSlots > blockCols) {
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
            // Allocate capacity and set all values to NULL for this column
            SColumnInfoData* pNewCol = taosArrayGetLast(pBlock->pDataBlock);
            if (pNewCol != NULL) {
              code = colInfoDataEnsureCapacity(pNewCol, pBlock->info.rows, true);
              QUERY_CHECK_CODE(code, lino, _return);
            }
          }
          idx++;
        }
      }
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

static int32_t federatedScanGetNextExtFn(SOperatorInfo*  pOperator,
                                          SOperatorParam* pParam,
                                          SSDataBlock**   ppRes) {
  QRY_PARAM_CHECK(ppRes);

  SFederatedScanOperatorInfo* pInfo = pOperator->info;

  // When called with a new param (sub-table switch), tear down the old connection.
  if (pParam != NULL) {
    bool paramChanged = (pInfo->queryStarted);  // any active connection = reset
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
// close — release all resources
// ---------------------------------------------------------------------------

static void federatedScanClose(void* param) {
  SFederatedScanOperatorInfo* pInfo = (SFederatedScanOperatorInfo*)param;
  if (!pInfo) return;

  // Close query handle before connection handle
  if (pInfo->pQueryHandle) {
    extConnectorCloseQuery(pInfo->pQueryHandle);
    pInfo->pQueryHandle = NULL;
  }
  if (pInfo->pConnHandle) {
    extConnectorClose(pInfo->pConnHandle);
    pInfo->pConnHandle = NULL;
  }

  qDebug("FederatedScan closed: rows=%" PRId64 ", blocks=%" PRId64
         ", elapsed=%" PRId64 "us",
         pInfo->fetchedRows, pInfo->fetchBlockCount, pInfo->elapsedTimeUs);

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

  qError("FqExec ENTRY: pColTypeMappings=%p, numColTypeMappings=%d, pRemotePlan=%p, pScanCols len=%d",
         (void*)pFedScanNode->pColTypeMappings,
         pFedScanNode->numColTypeMappings,
         (void*)pFedScanNode->pRemotePlan,
         pFedScanNode->pScanCols ? (int)LIST_LENGTH(pFedScanNode->pScanCols) : -1);

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
      qError("FqExec DIAG: pRemotePlan type=%d, pOutputCols len=%d, pScanCols len=%d",
             remoteType,
             pOutputCols ? (int)LIST_LENGTH(pOutputCols) : -1,
             pFedScanNode->pScanCols ? (int)LIST_LENGTH(pFedScanNode->pScanCols) : -1);
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
  setOperatorInfo(pOperator, "FederatedScanOperator",
                  QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN,
                  false, OP_NOT_OPENED, pInfo, pTaskInfo);

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
