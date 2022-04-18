
#include "clientInt.h"
#include "clientLog.h"
#include "clientStmt.h"
#include "tdef.h"

int32_t stmtGetTbName(TAOS_STMT *stmt, char **tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->sql.type = STMT_TYPE_MULTI_INSERT;
  
  if (NULL == pStmt->bind.tbName) {
    tscError("no table name set");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  *tbName = pStmt->bind.tbName;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtSetBindInfo(TAOS_STMT* stmt, STableMeta* pTableMeta, void* tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->bind.tbUid = pTableMeta->uid;
  pStmt->bind.tbSuid = pTableMeta->suid;
  pStmt->bind.tbType = pTableMeta->tableType;
  pStmt->bind.boundTags = tags;

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

  uint64_t uid;
  if (TSDB_CHILD_TABLE == pStmt->bind.tbType) {
    uid = pStmt->bind.tbSuid;
  } else {
    ASSERT(TSDB_NORMAL_TABLE == pStmt->bind.tbType);
    uid = pStmt->bind.tbUid;
  }

  if (taosHashGet(pStmt->sql.pTableCache, &uid, sizeof(uid))) {
    return TSDB_CODE_SUCCESS;
  }

  STableDataBlocks** pSrc = taosHashGet(pStmt->exec.pBlockHash, &uid, sizeof(uid));
  STableDataBlocks* pDst = NULL;
  
  STMT_ERR_RET(qCloneStmtDataBlock(&pDst, *pSrc));

  SStmtTableCache cache = {
    .pDataBlock = pDst,
    .boundTags = pStmt->bind.boundTags,
  };

  if (taosHashPut(pStmt->sql.pTableCache, &uid, sizeof(uid), &cache, sizeof(cache))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pStmt->bind.boundTags = NULL;

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
  
  STMT_ERR_RET(parseSql(pStmt->exec.pRequest, false, &pStmt->sql.pQuery, &stmtCb));

  pStmt->bind.needParse = false;
  
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
  pStmt->bind.tbUid = 0;
  pStmt->bind.tbSuid = 0;
  pStmt->bind.tbType = 0;
  pStmt->bind.needParse = true;

  taosMemoryFreeClear(pStmt->bind.tbName);
  destroyBoundColumnInfo(pStmt->bind.boundTags);
  taosMemoryFreeClear(pStmt->bind.boundTags);

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanExecInfo(STscStmt* pStmt, bool keepTable) {
  taos_free_result(pStmt->exec.pRequest);
  pStmt->exec.pRequest = NULL;

  void *pIter = taosHashIterate(pStmt->exec.pBlockHash, NULL);
  while (pIter) {
    STableDataBlocks* pBlocks = *(STableDataBlocks**)pIter;    
    uint64_t *key = taosHashGetKey(pIter, NULL);
    
    if (keepTable && (*key == pStmt->bind.tbUid)) {
      qResetStmtDataBlock(pBlocks, true);
      
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

  void *pIter = taosHashIterate(pStmt->sql.pTableCache, NULL);
  while (pIter) {
    SStmtTableCache* pCache = *(SStmtTableCache**)pIter;    

    qDestroyStmtDataBlock(pCache->pDataBlock);
    destroyBoundColumnInfo(pCache->boundTags);
    
    pIter = taosHashIterate(pStmt->sql.pTableCache, pIter);
  }
  taosHashCleanup(pStmt->sql.pTableCache);
  pStmt->sql.pTableCache = NULL;

  memset(&pStmt->sql, 0, sizeof(pStmt->sql));

  STMT_ERR_RET(stmtCleanExecInfo(pStmt, false));
  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetFromCache(STscStmt* pStmt) {
  if (NULL == pStmt->sql.pTableCache || taosHashGetSize(pStmt->sql.pTableCache) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pStmt->pCatalog) {
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
  }

  STableMeta *pTableMeta = NULL;
  SEpSet ep = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
  STMT_ERR_RET(catalogGetTableMeta(pStmt->pCatalog, pStmt->taos->pAppInfo->pTransporter, &ep, &pStmt->bind.sname, &pTableMeta));

  if (pTableMeta->uid == pStmt->bind.tbUid) {
    pStmt->bind.needParse = false;
    
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashGet(pStmt->exec.pBlockHash, &pTableMeta->uid, sizeof(pTableMeta->uid))) {
    SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &pTableMeta->uid, sizeof(pTableMeta->uid));
    if (NULL == pCache) {
      tscError("table uid %" PRIx64 "found in exec blockHash, but not in sql blockHash", pTableMeta->uid);
      STMT_ERR_RET(TSDB_CODE_TSC_APP_ERROR);
    }
    
    pStmt->bind.needParse = false;
    
    pStmt->bind.tbUid = pTableMeta->uid;
    pStmt->bind.tbSuid = pTableMeta->suid;
    pStmt->bind.tbType = pTableMeta->tableType;
    pStmt->bind.boundTags = pCache->boundTags;
    
    return TSDB_CODE_SUCCESS;
  }

  SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &pTableMeta->uid, sizeof(pTableMeta->uid));
  if (pCache) {
    pStmt->bind.needParse = false;

    pStmt->bind.tbUid = pTableMeta->uid;
    pStmt->bind.tbSuid = pTableMeta->suid;
    pStmt->bind.tbType = pTableMeta->tableType;
    pStmt->bind.boundTags = pCache->boundTags;

    STableDataBlocks* pNewBlock = NULL;
    STMT_ERR_RET(qRebuildStmtDataBlock(&pNewBlock, pCache->pDataBlock));

    if (taosHashPut(pStmt->exec.pBlockHash, &pStmt->bind.tbUid, sizeof(pStmt->bind.tbUid), &pNewBlock, POINTER_BYTES)) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    return TSDB_CODE_SUCCESS;
  }

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
  pStmt->sql.status = STMT_INIT;
  pStmt->bind.needParse = true;

  return pStmt;
}

int stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (pStmt->sql.status >= STMT_PREPARE) {
    STMT_ERR_RET(stmtCleanSQLInfo(pStmt));
  }

  STMT_SWITCH_STATUS(pStmt, STMT_PREPARE, TSDB_CODE_TSC_STMT_API_ERROR);
  
  pStmt->sql.sqlStr = strndup(sql, length);
  pStmt->sql.sqlLen = length;

  return TSDB_CODE_SUCCESS;
}


int stmtSetTbName(TAOS_STMT *stmt, const char *tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_SWITCH_STATUS(pStmt, STMT_SETTBNAME, TSDB_CODE_TSC_STMT_API_ERROR);

  taosMemoryFree(pStmt->bind.tbName);

  if (NULL == pStmt->exec.pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql.sqlStr, pStmt->sql.sqlLen, &pStmt->exec.pRequest));
  }
  
  STMT_ERR_RET(qCreateSName(&pStmt->bind.sname, tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
  
  pStmt->bind.tbName = strdup(tbName);
  
  STMT_ERR_RET(stmtGetFromCache(pStmt));

  return TSDB_CODE_SUCCESS;
}

int stmtSetTbTags(TAOS_STMT *stmt, TAOS_BIND_v2 *tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_SWITCH_STATUS(pStmt, STMT_SETTBNAME, TSDB_CODE_TSC_STMT_API_ERROR);

  if (pStmt->bind.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bind.tbUid, sizeof(pStmt->bind.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bind.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  STMT_ERR_RET(qBindStmtTagsValue(pDataBlock, pStmt->bind.boundTags, pStmt->bind.tbSuid, &pStmt->bind.sname, tags, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));

  return TSDB_CODE_SUCCESS;
}


int32_t stmtFetchTagFields(TAOS_STMT *stmt, int32_t *fieldNum, TAOS_FIELD** fields) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_SWITCH_STATUS(pStmt, STMT_FETCH_TAG_FIELDS, TSDB_CODE_TSC_STMT_API_ERROR);

  if (pStmt->bind.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query tag fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bind.tbUid, sizeof(pStmt->bind.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bind.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  STMT_ERR_RET(qBuildStmtTagFields(pDataBlock, pStmt->bind.boundTags, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtFetchColFields(TAOS_STMT *stmt, int32_t *fieldNum, TAOS_FIELD** fields) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_SWITCH_STATUS(pStmt, STMT_FETCH_COL_FIELDS, TSDB_CODE_TSC_STMT_API_ERROR);

  if (pStmt->bind.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query column fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bind.tbUid, sizeof(pStmt->bind.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bind.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  STMT_ERR_RET(qBuildStmtColFields(pDataBlock, fieldNum, fields));

  return TSDB_CODE_SUCCESS;  
}

int stmtBindBatch(TAOS_STMT *stmt, TAOS_BIND_v2 *bind) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_SWITCH_STATUS(pStmt, STMT_BIND, TSDB_CODE_TSC_STMT_API_ERROR);

  if (pStmt->bind.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 && STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bind.needParse = false;
  }

  if (NULL == pStmt->exec.pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql.sqlStr, pStmt->sql.sqlLen, &pStmt->exec.pRequest));
  }

  if (pStmt->bind.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pStmt->exec.pBlockHash, (const char*)&pStmt->bind.tbUid, sizeof(pStmt->bind.tbUid));
  if (NULL == pDataBlock) {
    tscError("table uid %" PRIx64 "not found in exec blockHash", pStmt->bind.tbUid);
    STMT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  qBindStmtColsValue(pDataBlock, bind, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen);
  
  return TSDB_CODE_SUCCESS;
}


int stmtAddBatch(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_SWITCH_STATUS(pStmt, STMT_ADD_BATCH, TSDB_CODE_TSC_STMT_API_ERROR);

  STMT_ERR_RET(stmtCacheBlock(pStmt));
  
  return TSDB_CODE_SUCCESS;
}

int stmtExec(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t code = 0;

  STMT_SWITCH_STATUS(pStmt, STMT_EXECUTE, TSDB_CODE_TSC_STMT_API_ERROR);

  STMT_ERR_RET(qBuildStmtOutput(pStmt->sql.pQuery, pStmt->exec.pVgHash, pStmt->exec.pBlockHash));

  launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, TSDB_CODE_SUCCESS, true);

  STMT_ERR_JRET(pStmt->exec.pRequest->code);

_return:

  stmtCleanExecInfo(pStmt, (code ? false : true));
  
  ++pStmt->sql.runTimes;
  
  STMT_RET(code);
}


int stmtClose(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

const char *stmtErrstr(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL) {
    return (char*) tstrerror(terrno);
  }

  return taos_errstr(pStmt->exec.pRequest);
}

int stmtAffectedRows(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

int stmtIsInsert(TAOS_STMT *stmt, int *insert) {
  STscStmt* pStmt = (STscStmt*)stmt;

  *insert = (STMT_TYPE_INSERT == pStmt->sql.type || STMT_TYPE_MULTI_INSERT == pStmt->sql.type);
  
  return TSDB_CODE_SUCCESS;
}

int stmtGetParamNum(TAOS_STMT *stmt, int *nums) {
  return TSDB_CODE_SUCCESS;
}

TAOS_RES *stmtUseResult(TAOS_STMT *stmt) {
  return NULL;
}



