
#include "clientInt.h"
#include "clientLog.h"
#include "clientStmt.h"
#include "tdef.h"

int32_t stmtGetTbName(TAOS_STMT *stmt, char **tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->type = STMT_TYPE_MULTI_INSERT;
  
  if (NULL == pStmt->tbName) {
    tscError("no table name set");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  *tbName = pStmt->tbName;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtParseSql(STscStmt* pStmt) {
  SStmtCallback stmtCb = {.pStmt = pStmt, .getTbNameFn = stmtGetTbName};
  
  STMT_ERR_RET(parseSql(pStmt->pRequest, false, &pStmt->pQuery, &stmtCb));

  pStmt->tbNeedParse = false;
  
  switch (nodeType(pStmt->pQuery->pRoot)) {
    case QUERY_NODE_VNODE_MODIF_STMT:
      if (0 == pStmt->type) {
        pStmt->type = STMT_TYPE_INSERT;
      }
      break;
    case QUERY_NODE_SELECT_STMT:
      pStmt->type = STMT_TYPE_QUERY;
      break;
    default:
      tscError("not supported stmt type %d", nodeType(pStmt->pQuery->pRoot));
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_CLAUSE_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCloneBlock(STableDataBlocks** pDst, STableDataBlocks* pSrc) {
  *pDst = (STableDataBlocks*)taosMemoryMalloc(sizeof(STableDataBlocks));
  if (NULL == *pDst) {
    STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  memcpy(*pDst, pSrc, sizeof(STableDataBlocks));
  (*pDst)->cloned = true;
  
  (*pDst)->pData    = NULL;
  (*pDst)->ordered  = true;
  (*pDst)->prevTS   = INT64_MIN;
  (*pDst)->size     = sizeof(SSubmitBlk);
  (*pDst)->tsSource = -1;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtSaveTableDataBlock(STscStmt *pStmt) {
  if (pStmt->type != STMT_TYPE_MULTI_INSERT) {
    return TSDB_CODE_SUCCESS;
  }

  SVnodeModifOpStmt *modifyNode = (SVnodeModifOpStmt *)pStmt->pQuery->pRoot;
  SStmtDataCtx *pCtx = &modifyNode->stmtCtx;

  uint64_t uid;
  if (TSDB_CHILD_TABLE == pCtx->tbType) {
    uid = pCtx->tbSuid;
  } else {
    ASSERT(TSDB_NORMAL_TABLE == pCtx->tbType);
    uid = pCtx->tbUid;
  }

  if (taosHashGet(pStmt->pTableDataBlocks, &uid, sizeof(uid))) {
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(1 == taosHashGetSize(pStmt->pTableDataBlocks));

  STableDataBlocks** pSrc = taosHashIterate(pStmt->pTableDataBlocks, NULL);
  STableDataBlocks* pDst = NULL;
  
  STMT_ERR_RET(stmtCloneBlock(&pDst, *pSrc));

  taosHashPut(pStmt->pTableDataBlocks, &uid, sizeof(uid), &pDst, POINTER_BYTES);

  return TSDB_CODE_SUCCESS;
}

int32_t stmtHandleTbInCache(STscStmt* pStmt) {
  if (NULL == pStmt->pTableDataBlocks || taosHashGetSize(pStmt->pTableDataBlocks) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pStmt->pCatalog) {
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
  }

  STableMeta *pTableMeta = NULL;
  SEpSet ep = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
  STMT_ERR_RET(catalogGetTableMeta(pStmt->pCatalog, pStmt->taos->pAppInfo->pTransporter, &ep, &pStmt->sname, &pTableMeta));

  SVnodeModifOpStmt *modifyNode = (SVnodeModifOpStmt *)pStmt->pQuery->pRoot;
  SStmtDataCtx *pCtx = &modifyNode->stmtCtx;

  if (pTableMeta->uid == pCtx->tbUid) {
    pStmt->tbNeedParse = false;
    pStmt->tbReuse = false;
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashGet(pCtx->pTableBlockHashObj, &pTableMeta->uid, sizeof(pTableMeta->uid))) {
    pStmt->tbNeedParse = false;
    pStmt->tbReuse = true;
    pCtx->tbUid = pTableMeta->uid;
    pCtx->tbSuid = pTableMeta->suid;
    pCtx->tbType = pTableMeta->tableType;
    
    return TSDB_CODE_SUCCESS;
  }

  STableDataBlocks** pDataBlock = taosHashGet(pStmt->pTableBlockHashObj, &pTableMeta->uid, sizeof(pTableMeta->uid))
  if (pDataBlock && *pDataBlock) {
    pStmt->tbNeedParse = false;
    pStmt->tbReuse = true;

    pCtx->tbUid = pTableMeta->uid;
    pCtx->tbSuid = pTableMeta->suid;
    pCtx->tbType = pTableMeta->tableType;

    taosHashPut(pCtx->pTableBlockHashObj, &pCtx->tbUid, sizeof(pCtx->tbUid), pDataBlock, POINTER_BYTES);
    
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

  pStmt->pTableDataBlocks = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  pStmt->pVgList = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pStmt->pTableDataBlocks || NULL == pStmt->pVgList) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    taosMemoryFree(pStmt);
    return NULL;
  }
  
  pStmt->taos = pObj;
  pStmt->status = STMT_INIT;
  pStmt->tbNeedParse = true;

  return pStmt;
}

int stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_PREPARE, TSDB_CODE_TSC_STMT_STATUS_ERROR);
  
  pStmt->sql = strndup(sql, length);
  pStmt->sqlLen = length;

  return TSDB_CODE_SUCCESS;
}


int stmtSetTbName(TAOS_STMT *stmt, const char *tbName, TAOS_BIND *tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_SETTBNAME, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  taosMemoryFree(pStmt->tbName);

  if (NULL == pStmt->pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql, pStmt->sqlLen, &pStmt->pRequest));
  }
  
  STMT_ERR_RET(qCreateSName(&pStmt->sname, tbName, pStmt->taos->acctId, pStmt->pRequest->pDb, pStmt->pRequest->msgBuf, pStmt->pRequest->msgBufLen));
  
  pStmt->tbName = strdup(tbName);
  
  STMT_ERR_RET(stmtHandleTbInCache(pStmt));

  return TSDB_CODE_SUCCESS;
}

int stmtSetTbTags(TAOS_STMT *stmt, const char *tbName, TAOS_BIND *tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_SETTBNAME, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  if (pStmt->tbNeedParse) {
    taosMemoryFree(pStmt->bindTags);
    pStmt->bindTags = tags;
    
    STMT_ERR_RET(stmtParseSql(pStmt));
  } else {
    //TODO BIND TAG DATA
  }

  return TSDB_CODE_SUCCESS;
}


int32_t stmtFetchTagFields(TAOS_STMT *stmt, int32_t *fieldNum, TAOS_FIELD** fields) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_FETCH_TAG_FIELDS, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  if (pStmt->tbNeedParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  STMT_ERR_RET(qBuildStmtTagFields(pStmt->pQuery, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtFetchColFields(TAOS_STMT *stmt, int32_t *fieldNum, TAOS_FIELD* fields) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_FETCH_COL_FIELDS, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  if (pStmt->tbNeedParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  STMT_ERR_RET(qBuildStmtColFields(pStmt->pQuery, fieldNum, fields));

  return TSDB_CODE_SUCCESS;  
}

int stmtBindBatch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_BIND, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  if (pStmt->tbNeedParse && pStmt->runTimes && pStmt->type > 0 && STMT_TYPE_MULTI_INSERT != pStmt->type) {
    pStmt->tbNeedParse = false;
  }

  if (NULL == pStmt->pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos, pStmt->sql, pStmt->sqlLen, &pStmt->pRequest));
  }

  if (pStmt->tbNeedParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }
  
  qBindStmtData(pStmt->pQuery, bind, pStmt->pRequest->msgBuf, pStmt->pRequest->msgBufLen);
  
  return TSDB_CODE_SUCCESS;
}


int stmtAddBatch(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHK_STATUS(stmt, STMT_ADD_BATCH, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  STMT_ERR_RET(stmtSaveTableDataBlock(pStmt));
  
  return TSDB_CODE_SUCCESS;
}

int stmtExec(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t code = 0;

  STMT_CHK_STATUS(stmt, STMT_EXECUTE, TSDB_CODE_TSC_STMT_STATUS_ERROR);

  STMT_ERR_RET(qBuildStmtOutput(pStmt->pQuery));

  launchQueryImpl(pStmt->pRequest, pStmt->pQuery, TSDB_CODE_SUCCESS);

  STMT_ERR_JRET(pStmt->pRequest->code);

_return:

  //TODO RESET AND CLEAN PART TO DATABLOCK...
  
  taos_free_result(pStmt->pRequest);
  pStmt->pRequest = NULL;

  pStmt->tbNeedParse = true;
  ++pStmt->runTimes;
  
  STMT_RET(code);
}


int stmtClose(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

char *stmtErrstr(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL) {
    return (char*) tstrerror(terrno);
  }

  return taos_errstr(pStmt->pRequest);
}

int stmtAffectedRows(TAOS_STMT *stmt) {
  return TSDB_CODE_SUCCESS;
}

int stmtIsInsert(TAOS_STMT *stmt, int *insert) {
  return TSDB_CODE_SUCCESS;
}

int stmtGetParamNum(TAOS_STMT *stmt, int *nums) {
  return TSDB_CODE_SUCCESS;
}

TAOS_RES *stmtUseResult(TAOS_STMT *stmt) {
  return NULL;
}



