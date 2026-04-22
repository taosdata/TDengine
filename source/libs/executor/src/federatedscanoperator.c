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

// Map EExtSourceType to the matching EExtSQLDialect.
// Mirrors extDialectFromSourceType() in extConnector.c; kept separate to avoid
// pulling extConnectorInt.h into the executor.
static EExtSQLDialect fedScanGetDialect(int8_t srcType) {
  switch ((EExtSourceType)srcType) {
    case EXT_SOURCE_MYSQL:      return EXT_SQL_DIALECT_MYSQL;
    case EXT_SOURCE_POSTGRESQL: return EXT_SQL_DIALECT_POSTGRES;
    case EXT_SOURCE_INFLUXDB:   return EXT_SQL_DIALECT_INFLUXQL;
    default:
      qError("FederatedScan: unexpected sourceType=%d in fedScanGetDialect, defaulting to MySQL dialect",
             (int32_t)srcType);
      return EXT_SQL_DIALECT_MYSQL;
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

    // 1.3 Generate remote SQL (for logging and EXPLAIN ANALYZE)
    {
      char*          remoteSql = NULL;
      EExtSQLDialect dialect   = fedScanGetDialect(pFedNode->sourceType);
      int32_t        sqlCode   = nodesRemotePlanToSQL(
          (const SPhysiNode*)pFedNode->pRemotePlan, dialect, &remoteSql);
      if (sqlCode == TSDB_CODE_SUCCESS && remoteSql != NULL) {
        tstrncpy(pInfo->remoteSql, remoteSql, sizeof(pInfo->remoteSql));
        taosMemoryFree(remoteSql);
      }
      // SQL generation failure is non-fatal for connection; Connector regenerates internally.
      qDebug("FederatedScan: remote SQL (cached): %.512s", pInfo->remoteSql);
    }

    // 1.4 Issue query — Connector uses pFedNode to build the actual SQL internally
    SExtConnectorError extErr = {0};
    code = extConnectorExecQuery(pInfo->pConnHandle, pFedNode,
                                 &pInfo->pQueryHandle, &extErr);
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

typedef struct SFederatedScanExplainInfo {
  int64_t fetchedRows;
  int64_t fetchBlockCount;
  int64_t elapsedTimeUs;
  char    remoteSql[4096];
} SFederatedScanExplainInfo;

static int32_t federatedScanGetExplainInfo(SOperatorInfo* pOperator,
                                           void**         ppOptrExplain,
                                           uint32_t*      pLen) {
  SFederatedScanOperatorInfo* pInfo = pOperator->info;

  SFederatedScanExplainInfo* pExInfo =
      taosMemoryCalloc(1, sizeof(SFederatedScanExplainInfo));
  if (!pExInfo) return terrno;

  pExInfo->fetchedRows    = pInfo->fetchedRows;
  pExInfo->fetchBlockCount = pInfo->fetchBlockCount;
  pExInfo->elapsedTimeUs  = pInfo->elapsedTimeUs;
  tstrncpy(pExInfo->remoteSql, pInfo->remoteSql, sizeof(pExInfo->remoteSql));

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
