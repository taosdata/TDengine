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
      qError("FQ-DIAG FederatedScan remoteSql=[%s] source=%s", remoteSql ? remoteSql : "(null)", cfg.source_name);
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
  // Step 2: Fetch next data block (loop until non-empty or EOF)
  // =========================================================================
  for (;;) {
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

    // Mark block as data-loaded so that downstream operators (e.g.
    // partition/groupby) process all rows rather than treating them as
    // "not-loaded" blocks (which only process the first row then break).
    pBlock->info.dataLoad = 1;

    // If the DataBlockDesc contains more slots than the remote query returned
    // (e.g. reserved constant-separator slots for group_concat, or extra precalc
    // slots added by pushdownDataBlockSlots for local Sort), expand the block
    // with pre-allocated placeholder columns.
    //
    // Always expand when numDescSlots > numBlockCols, even when a remote plan
    // was pushed.  The remote plan only returns table columns; locally-computed
    // columns (e.g. constant separators for group_concat) are never part of the
    // remote result and must be appended here for projectApplyFunctions to fill.
    {
      SDataBlockDescNode* pDesc = pInfo->pFedScanNode->node.pOutputDataBlockDesc;
      int32_t numDescSlots = (pDesc != NULL) ? (int32_t)LIST_LENGTH(pDesc->pSlots) : 0;
      int32_t numBlockCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
      if (numDescSlots > numBlockCols) {
        SNode*  pSlotNode = NULL;
        int32_t slotIdx   = 0;
        FOREACH(pSlotNode, pDesc->pSlots) {
          if (slotIdx >= numBlockCols) {
            SSlotDescNode*  pSlot = (SSlotDescNode*)pSlotNode;
            SColumnInfoData col   = {0};
            col.info.type         = pSlot->dataType.type;
            col.info.bytes        = pSlot->dataType.bytes;
            col.info.precision    = pSlot->dataType.precision;
            col.info.scale        = pSlot->dataType.scale;
            // Pre-allocate memory so projectApplyFunctions / Sort can write
            // into the column.  Zero-initialized (clearPayload=true).
            if (pBlock->info.rows > 0) {
              code = colInfoDataEnsureCapacity(&col, pBlock->info.rows, true);
              if (code != TSDB_CODE_SUCCESS) {
                colDataDestroy(&col);
                QUERY_CHECK_CODE(code, lino, _return);
              }
            }
            void* p = taosArrayPush(pBlock->pDataBlock, &col);
            if (p == NULL) {
              colDataDestroy(&col);
              code = terrno;
              QUERY_CHECK_CODE(code, lino, _return);
            }
          }
          slotIdx++;
        }
      }
    }

    // Apply local filter (handles TDengine-specific functions like like_in_set,
    // regexp_in_set that nodesRemotePlanToSQL could not push to the remote DB).
    if (pOperator->exprSupp.pFilterInfo != NULL && pBlock->info.rows > 0) {
      code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    if (pBlock->info.rows > 0) {
      *ppRes = pBlock;
      return TSDB_CODE_SUCCESS;
    }
    // Block became empty after local filter — fetch the next one.
  }

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

  // When a Sort or Project was pushed to the remote plan, the remote query returns
  // fewer columns than pScanCols.  Rebuild pOutputDataBlockDesc to match the actual
  // output (pColTypeMappings) so the data dispatcher's schema validation passes.
  // Do NOT rebuild when pRemotePlan is just the leaf FedScan node (nothing pushed):
  // in that case pOutputDataBlockDesc includes extra precalc slots (e.g. _length_name_)
  // added by pushdownDataBlockSlots, which must be preserved for the local Sort.
  //
  // Reserved slots (e.g. a constant separator pushed down from an Agg's scalar
  // exprSupp for group_concat) must be preserved at the end of the DataBlockDesc
  // so that the parent operator can write computed values into them.
  bool remotePlanIsPushed = (pFedScanNode->pRemotePlan != NULL &&
                             nodeType(pFedScanNode->pRemotePlan) != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN);
  if (remotePlanIsPushed && pFedScanNode->numColTypeMappings > 0) {
    SDataBlockDescNode* pDesc = pFedScanNode->node.pOutputDataBlockDesc;
    if (pDesc != NULL && LIST_LENGTH(pDesc->pSlots) != pFedScanNode->numColTypeMappings) {
      // Save reserved slots (struct copy — SSlotDescNode has no owned pointers)
      // BEFORE destroying the original list.
      int32_t       numReserved = 0;
      SSlotDescNode reservedSlotsBuf[16];
      SNode*        pSlotIter = NULL;
      FOREACH(pSlotIter, pDesc->pSlots) {
        SSlotDescNode* pSlot = (SSlotDescNode*)pSlotIter;
        if (pSlot->reserve && numReserved < 16) {
          reservedSlotsBuf[numReserved++] = *pSlot;
        }
      }

      nodesDestroyList(pDesc->pSlots);
      pDesc->pSlots = NULL;
      pDesc->totalRowSize = 0;
      pDesc->outputRowSize = 0;

      code = nodesMakeList(&pDesc->pSlots);
      QUERY_CHECK_NULL(pDesc->pSlots, code, lino, _error, terrno);

      for (int16_t si = 0; si < pFedScanNode->numColTypeMappings; ++si) {
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

      // Re-append reserved slots after MySQL columns so the parent operator's
      // scalar exprSupp can write computed values (e.g. group_concat separator)
      // into the placeholder columns added by federatedScanGetNext().
      for (int32_t ri = 0; ri < numReserved; ++ri) {
        SSlotDescNode* pSlot = NULL;
        code = nodesMakeNode(QUERY_NODE_SLOT_DESC, (SNode**)&pSlot);
        QUERY_CHECK_NULL(pSlot, code, lino, _error, terrno);
        *pSlot = reservedSlotsBuf[ri];
        pSlot->slotId = (uint16_t)(pFedScanNode->numColTypeMappings + ri);
        code = nodesListStrictAppend(pDesc->pSlots, (SNode*)pSlot);
        QUERY_CHECK_CODE(code, lino, _error);
        pDesc->totalRowSize += pSlot->dataType.bytes;
        // reserved slots intentionally NOT added to outputRowSize
      }
    }
  }

  // FederatedScan is a leaf node — no downstream
  setOperatorInfo(pOperator, "FederatedScanOperator",
                  QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN,
                  false, OP_NOT_OPENED, pInfo, pTaskInfo);

  // Initialize local filter for conditions that nodesRemotePlanToSQL could not
  // translate to the remote dialect (e.g., like_in_set, regexp_in_set).
  // pFedScanNode->node.pConditions has slot IDs set by the planner.
  // EXISTS/NOT_EXISTS conditions are fully pushed to the remote source and must
  // not appear here (fqHarvestConditions in the optimizer clears pConditions for
  // such nodes so pFedScanNode->node.pConditions is always NULL in that case).
  if (pFedScanNode->node.pConditions != NULL) {
    code = filterInitFromNode((SNode*)pFedScanNode->node.pConditions,
                              &pOperator->exprSupp.pFilterInfo, 0, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      // Non-fatal: log and continue without local filter
      qWarn("FederatedScan: failed to init local filter, code=0x%x %s; remote filter only",
            code, tstrerror(code));
      code = TSDB_CODE_SUCCESS;
    }
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
