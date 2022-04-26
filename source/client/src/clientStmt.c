
#include "clientInt.h"
#include "clientLog.h"
#include "clientStmt.h"
#include "tdef.h"

int32_t stmtSwitchStatus(STscStmt* pStmt, STMT_STATUS newStatus) {
  int32_t code = 0;
  
  switch (newStatus) {
    case STMT_PREPARE:
      break;
    case STMT_SETTBNAME:
      if (STMT_STATUS_EQ(INIT) || STMT_STATUS_EQ(BIND) || STMT_STATUS_EQ(BIND_COL)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_SETTAGS:
      if (STMT_STATUS_NE(SETTBNAME)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_FETCH_FIELDS:
      if (STMT_STATUS_EQ(INIT)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_BIND:
      if (STMT_STATUS_EQ(INIT) || STMT_STATUS_EQ(BIND_COL)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_BIND_COL:
      if (STMT_STATUS_EQ(INIT) || STMT_STATUS_EQ(BIND)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_ADD_BATCH:
      if (STMT_STATUS_NE(BIND) && STMT_STATUS_NE(BIND_COL) && STMT_STATUS_NE(FETCH_FIELDS)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_EXECUTE:
      if (STMT_STATUS_NE(ADD_BATCH) && STMT_STATUS_NE(FETCH_FIELDS)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    default:
      code = TSDB_CODE_TSC_APP_ERROR;
      break;
  }

  STMT_ERR_RET(code);

  pStmt->sql.status = newStatus;

  return TSDB_CODE_SUCCESS;
}


int32_t stmtGetTbName(TAOS_STMT *stmt, char **tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->sql.type = STMT_TYPE_MULTI_INSERT;
  
  if (NULL == pStmt->bInfo.tbName) {
    tscError("no table name set");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  *tbName = pStmt->bInfo.tbName;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtSetBindInfo(TAOS_STMT* stmt, STableMeta* pTableMeta, void* tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->bInfo.tbUid = pTableMeta->uid;
  pStmt->bInfo.tbSuid = pTableMeta->suid;
  pStmt->bInfo.tbType = pTableMeta->tableType;
  pStmt->bInfo.boundTags = tags;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtSetExecInfo(TAOS_STMT* stmt, SHashObj* pVgHash, SHashObj* pBlockHash) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->exec.pVgHash = pVgHash;
  pStmt->exec.pBlockHash = pBlockHash;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetExecInfo(TAOS_STMT* stmt, SHashObj** pVgHash, SHashObj** pBlockHash) {
  STscStmt* pStmt = (STscStmt*)stmt;

  *pVgHash = pStmt->exec.pVgHash;
  *pBlockHash = pStmt->exec.pBlockHash;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCacheBlock(STscStmt *pStmt) {
  if (pStmt->sql.type != STMT_TYPE_MULTI_INSERT) {
    return TSDB_CODE_SUCCESS;
  }

  uint64_t uid = pStmt->bInfo.tbUid; 
  uint64_t tuid = (TSDB_CHILD_TABLE == pStmt->bInfo.tbType) ? pStmt->bInfo.tbSuid : uid;

  if (taosHashGet(pStmt->sql.pTableCache, &tuid, sizeof(tuid))) {
    return TSDB_CODE_SUCCESS;
  }

  STableDataBlocks** pSrc = taosHashGet(pStmt->exec.pBlockHash, &uid, sizeof(uid));
  STableDataBlocks* pDst = NULL;
  
  STMT_ERR_RET(qCloneStmtDataBlock(&pDst, *pSrc));

  SStmtTableCache cache = {
    .pDataBlock = pDst,
    .boundTags = pStmt->bInfo.boundTags,
  };

  if (taosHashPut(pStmt->sql.pTableCache, &tuid, sizeof(tuid), &cache, sizeof(cache))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pStmt->bInfo.boundTags = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtParseSql(STscStmt* pStmt) {
  SStmtCallback stmtCb = {
    .pStmt = pStmt, 
    .getTbNameFn = stmtGetTbName, 
    .setBindInfoFn = stmtSetBindInfo,
    .setExecInfoFn = stmtSetExecInfo,
    .getExecInfoFn = stmtGetExecInfo,
  };

  if (NULL == pStmt->exec.pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql.sqlStr, pStmt->sql.sqlLen, &pStmt->exec.pRequest));
  }
  
  STMT_ERR_RET(parseSql(pStmt->exec.pRequest, false, &pStmt->sql.pQuery, &stmtCb));

  pStmt->bInfo.needParse = false;
  
  switch (nodeType(pStmt->sql.pQuery->pRoot)) {
    case QUERY_NODE_VNODE_MODIF_STMT:
      if (0 == pStmt->sql.type) {
        pStmt->sql.type = STMT_TYPE_INSERT;
      }
      break;
    case QUERY_NODE_SELECT_STMT:
      pStmt->sql.type = STMT_TYPE_QUERY;
      break;
    default:
      tscError("not supported stmt type %d", nodeType(pStmt->sql.pQuery->pRoot));
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_CLAUSE_ERROR);
  }

  STMT_ERR_RET(stmtCacheBlock(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanBindInfo(STscStmt* pStmt) {
  pStmt->bInfo.tbUid = 0;
  pStmt->bInfo.tbSuid = 0;
  pStmt->bInfo.tbType = 0;
  pStmt->bInfo.needParse = true;

  taosMemoryFreeClear(pStmt->bInfo.tbName);
  destroyBoundColumnInfo(pStmt->bInfo.boundTags);
  taosMemoryFreeClear(pStmt->bInfo.boundTags);

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanExecInfo(STscStmt* pStmt, bool keepTable, bool freeRequest) {
  if (STMT_TYPE_QUERY != pStmt->sql.type || freeRequest) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  void *pIter = taosHashIterate(pStmt->exec.pBlockHash, NULL);
  while (pIter) {
    STableDataBlocks* pBlocks = *(STableDataBlocks**)pIter;    
    uint64_t *key = taosHashGetKey(pIter, NULL);
    
    if (keepTable && (*key == pStmt->bInfo.tbUid)) {
      STMT_ERR_RET(qResetStmtDataBlock(pBlocks, true));
      
      pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
      continue;
    }

    qFreeStmtDataBlock(pBlocks);
    taosHashRemove(pStmt->exec.pBlockHash, key, sizeof(*key));

    pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
  }

  if (keepTable) {
    return TSDB_CODE_SUCCESS;
  }

  taosHashCleanup(pStmt->exec.pBlockHash);
  pStmt->exec.pBlockHash = NULL;

  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanSQLInfo(STscStmt* pStmt) {
  taosMemoryFree(pStmt->sql.sqlStr);
  qDestroyQuery(pStmt->sql.pQuery);
  qDestroyQueryPlan(pStmt->sql.pQueryPlan);
  taosArrayDestroy(pStmt->sql.nodeList);
  
  void *pIter = taosHashIterate(pStmt->sql.pTableCache, NULL);
  while (pIter) {
    SStmtTableCache* pCache = (SStmtTableCache*)pIter;    

    qDestroyStmtDataBlock(pCache->pDataBlock);
    destroyBoundColumnInfo(pCache->boundTags);
    
    pIter = taosHashIterate(pStmt->sql.pTableCache, pIter);
  }
  taosHashCleanup(pStmt->sql.pTableCache);
  pStmt->sql.pTableCache = NULL;

  memset(&pStmt->sql, 0, sizeof(pStmt->sql));

  STMT_ERR_RET(stmtCleanExecInfo(pStmt, false, true));
  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetFromCache(STscStmt* pStmt) {
  pStmt->bInfo.needParse = true;

  if (NULL == pStmt->sql.pTableCache || taosHashGetSize(pStmt->sql.pTableCache) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pStmt->pCatalog) {
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
  }

  STableMeta *pTableMeta = NULL;
  SEpSet ep = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
  STMT_ERR_RET(catalogGetTableMeta(pStmt->pCatalog, pStmt->taos->pAppInfo->pTransporter, &ep, &pStmt->bInfo.sname, &pTableMeta));

  if (pTableMeta->uid == pStmt->bInfo.tbUid) {
    pStmt->bInfo.needParse = false;
    
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashGet(pStmt->exec.pBlockHash, &pTableMeta->uid, sizeof(pTableMeta->uid))) {
    SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &pTableMeta->uid, sizeof(pTableMeta->uid));
    if (NULL == pCache) {
      tscError("table uid %" PRIx64 "found in exec blockHash, but not in sql blockHash", pTableMeta->uid);
      STMT_ERR_RET(TSDB_CODE_TSC_APP_ERROR);
    }
    
    pStmt->bInfo.needParse = false;
    
    pStmt->bInfo.tbUid = pTableMeta->uid;
    pStmt->bInfo.tbSuid = pTableMeta->suid;
    pStmt->bInfo.tbType = pTableMeta->tableType;
    pStmt->bInfo.boundTags = pCache->boundTags;
    
    return TSDB_CODE_SUCCESS;
  }

  SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &pTableMeta->uid, sizeof(pTableMeta->uid));
  if (pCache) {
    pStmt->bInfo.needParse = false;

    pStmt->bInfo.tbUid = pTableMeta->uid;
    pStmt->bInfo.tbSuid = pTableMeta->suid;
    pStmt->bInfo.tbType = pTableMeta->tableType;
    pStmt->bInfo.boundTags = pCache->boundTags;

    STableDataBlocks* pNewBlock = NULL;
    STMT_ERR_RET(qRebuildStmtDataBlock(&pNewBlock, pCache->pDataBlock));

    if (taosHashPut(pStmt->exec.pBlockHash, &pStmt->bInfo.tbUid, sizeof(pStmt->bInfo.tbUid), &pNewBlock, POINTER_BYTES)) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    return TSDB_CODE_SUCCESS;
  }

  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtResetStmt(STscStmt* pStmt) {
  STMT_ERR_RET(stmtCleanSQLInfo(pStmt));

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    STMT_ERR_RET(terrno);
  }

  pStmt->sql.status = STMT_INIT;

  return TSDB_CODE_SUCCESS;
}


TAOS_STMT *stmtInit(TAOS *taos) {
  STscObj* pObj = (STscObj*)taos;
  STscStmt* pStmt = NULL;

  pStmt = taosMemoryCalloc(1, sizeof(STscStmt));
  if (NULL == pStmt) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    taosMemoryFree(pStmt);
    return NULL;
  }

  pStmt->taos = pObj;
  pStmt->bInfo.needParse = true;
  pStmt->sql.status = STMT_INIT;
  
  return pStmt;
}

int stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (pStmt->sql.status >= STMT_PREPARE) {
    STMT_ERR_RET(stmtResetStmt(pStmt));
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_PREPARE));

  if (length <= 0) {
    length = strlen(sql);
  }
  
  pStmt->sql.sqlStr = strndup(sql, length);
  pStmt->sql.sqlLen = length;

  return TSDB_CODE_SUCCESS;
}


int stmtSetTbName(TAOS_STMT *stmt, const char *tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTBNAME));

  int32_t insert = 0;
  stmtIsInsert(stmt, &insert);
  if (0 == insert) {
    tscError("set tb name not available for none insert statement");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (NULL == pStmt->exec.pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql.sqlStr, pStmt->sql.sqlLen, &pStmt->exec.pRequest));
  }
  
  STMT_ERR_RET(qCreateSName(&pStmt->bInfo.sname, tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
    
  STMT_ERR_RET(stmtGetFromCache(pStmt));

  if (pStmt->bInfo.needParse) {
    taosMemoryFree(pStmt->bInfo.tbName);
    pStmt->bInfo.tbName = strdup(tbName);
  }

  return TSDB_CODE_SUCCESS;
}

int stmtSetTbTags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTAGS));

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  STableDataBlocks **pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bInfo.tbUid, sizeof(pStmt->bInfo.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bInfo.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  STMT_ERR_RET(qBindStmtTagsValue(*pDataBlock, pStmt->bInfo.boundTags, pStmt->bInfo.tbSuid, &pStmt->bInfo.sname, tags, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));

  return TSDB_CODE_SUCCESS;
}


int32_t stmtFetchTagFields(STscStmt* pStmt, int32_t *fieldNum, TAOS_FIELD** fields) {
  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query tag fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataBlocks **pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bInfo.tbUid, sizeof(pStmt->bInfo.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bInfo.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  STMT_ERR_RET(qBuildStmtTagFields(*pDataBlock, pStmt->bInfo.boundTags, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtFetchColFields(STscStmt* pStmt, int32_t *fieldNum, TAOS_FIELD** fields) {
  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query column fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataBlocks **pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bInfo.tbUid, sizeof(pStmt->bInfo.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bInfo.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  STMT_ERR_RET(qBuildStmtColFields(*pDataBlock, fieldNum, fields));

  return TSDB_CODE_SUCCESS;  
}

int stmtBindBatch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int32_t colIdx) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_BIND));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 && STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }
  
  if (NULL == pStmt->exec.pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql.sqlStr, pStmt->sql.sqlLen, &pStmt->exec.pRequest));
  }

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    if (NULL == pStmt->sql.pQueryPlan) {
      STMT_ERR_RET(getQueryPlan(pStmt->exec.pRequest, pStmt->sql.pQuery, &pStmt->sql.nodeList));
      pStmt->sql.pQueryPlan = pStmt->exec.pRequest->body.pDag;
      pStmt->exec.pRequest->body.pDag = NULL;
    }
    
    STMT_RET(qStmtBindParam(pStmt->sql.pQueryPlan, bind, colIdx));
  }
  
  STableDataBlocks **pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bInfo.tbUid, sizeof(pStmt->bInfo.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bInfo.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (colIdx < 0) {
    qBindStmtColsValue(*pDataBlock, bind, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen);
  } else {
    if (colIdx != (pStmt->bInfo.sBindLastIdx + 1) && colIdx != 0) {
      tscError("bind column index not in sequence");
      STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
    }

    pStmt->bInfo.sBindLastIdx = colIdx;
    
    if (0 == colIdx) {
      pStmt->bInfo.sBindRowNum = bind->num;
    }
    
    qBindStmtSingleColValue(*pDataBlock, bind, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen, colIdx, pStmt->bInfo.sBindRowNum);
  }
  
  return TSDB_CODE_SUCCESS;
}


int stmtAddBatch(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_ADD_BATCH));

  STMT_ERR_RET(stmtCacheBlock(pStmt));
  
  return TSDB_CODE_SUCCESS;
}

int stmtExec(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t code = 0;

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_EXECUTE));

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    scheduleQuery(pStmt->exec.pRequest, pStmt->sql.pQueryPlan, pStmt->sql.nodeList);
  } else {
    STMT_ERR_RET(qBuildStmtOutput(pStmt->sql.pQuery, pStmt->exec.pVgHash, pStmt->exec.pBlockHash));
    launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, TSDB_CODE_SUCCESS, true);
  }
  
  STMT_ERR_JRET(pStmt->exec.pRequest->code);

  pStmt->exec.affectedRows = taos_affected_rows(pStmt->exec.pRequest);
  pStmt->affectedRows += pStmt->exec.affectedRows;

_return:

  stmtCleanExecInfo(pStmt, (code ? false : true), false);
  
  ++pStmt->sql.runTimes;
  
  STMT_RET(code);
}


int stmtClose(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_RET(stmtCleanSQLInfo(pStmt));
}

const char *stmtErrstr(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL) {
    return (char*) tstrerror(terrno);
  }

  if (pStmt->exec.pRequest) {
    pStmt->exec.pRequest->code = terrno;
  }

  return taos_errstr(pStmt->exec.pRequest);
}

int stmtAffectedRows(TAOS_STMT *stmt) {
  return ((STscStmt*)stmt)->affectedRows;
}

int stmtAffectedRowsOnce(TAOS_STMT *stmt) {
  return ((STscStmt*)stmt)->exec.affectedRows;
}

int stmtIsInsert(TAOS_STMT *stmt, int *insert) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (pStmt->sql.type) {
    *insert = (STMT_TYPE_INSERT == pStmt->sql.type || STMT_TYPE_MULTI_INSERT == pStmt->sql.type);
  } else {
    *insert = isInsertSql(pStmt->sql.sqlStr, 0);
  }
  
  return TSDB_CODE_SUCCESS;
}

int stmtGetParamNum(TAOS_STMT *stmt, int *nums) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    if (NULL == pStmt->sql.pQueryPlan) {
      STMT_ERR_RET(getQueryPlan(pStmt->exec.pRequest, pStmt->sql.pQuery, &pStmt->sql.nodeList));
      pStmt->sql.pQueryPlan = pStmt->exec.pRequest->body.pDag;
      pStmt->exec.pRequest->body.pDag = NULL;
    }

    *nums = (pStmt->sql.pQueryPlan->pPlaceholderValues) ? pStmt->sql.pQueryPlan->pPlaceholderValues->length : 0;
  } else {
    STMT_ERR_RET(stmtFetchColFields(stmt, nums, NULL));
  }
  
  return TSDB_CODE_SUCCESS;
}

TAOS_RES *stmtUseResult(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (STMT_TYPE_QUERY != pStmt->sql.type) {
    tscError("useResult only for query statement");
    return NULL;
  }

  return pStmt->exec.pRequest;
}



