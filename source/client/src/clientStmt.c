
#include "clientInt.h"
#include "clientLog.h"
#include "tdef.h"

#include "clientStmt.h"

char* gStmtStatusStr[] = {"unknown",     "init", "prepare", "settbname", "settags",
                          "fetchFields", "bind", "bindCol", "addBatch",  "exec"};

static int32_t stmtCreateRequest(STscStmt* pStmt) {
  int32_t code = 0;

  if (pStmt->exec.pRequest == NULL) {
    code = buildRequest(pStmt->taos->id, pStmt->sql.sqlStr, pStmt->sql.sqlLen, NULL, false, &pStmt->exec.pRequest,
                        pStmt->reqid);
    if (pStmt->reqid != 0) {
      pStmt->reqid++;
    }
    if (TSDB_CODE_SUCCESS == code) {
      pStmt->exec.pRequest->syncQuery = true;
    }
  }

  return code;
}

int32_t stmtSwitchStatus(STscStmt* pStmt, STMT_STATUS newStatus) {
  int32_t code = 0;

  if (newStatus >= STMT_INIT && newStatus < STMT_MAX) {
    STMT_LOG_SEQ(newStatus);
  }

  if (pStmt->errCode && newStatus != STMT_PREPARE) {
    STMT_DLOG("stmt already failed with err: %s", tstrerror(pStmt->errCode));
    return pStmt->errCode;
  }

  switch (newStatus) {
    case STMT_PREPARE:
      pStmt->errCode = 0;    
      break;
    case STMT_SETTBNAME:
      if (STMT_STATUS_EQ(INIT) || STMT_STATUS_EQ(BIND) || STMT_STATUS_EQ(BIND_COL)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      break;
    case STMT_SETTAGS:
      if (STMT_STATUS_NE(SETTBNAME) && STMT_STATUS_NE(FETCH_FIELDS)) {
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
      /*
            if ((pStmt->sql.type == STMT_TYPE_MULTI_INSERT) && ()) {
              code = TSDB_CODE_TSC_STMT_API_ERROR;
            }
      */
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
      if (STMT_TYPE_QUERY == pStmt->sql.type) {
        if (STMT_STATUS_NE(ADD_BATCH) && STMT_STATUS_NE(FETCH_FIELDS) && STMT_STATUS_NE(BIND) &&
            STMT_STATUS_NE(BIND_COL)) {
          code = TSDB_CODE_TSC_STMT_API_ERROR;
        }
      } else {
        if (STMT_STATUS_NE(ADD_BATCH) && STMT_STATUS_NE(FETCH_FIELDS)) {
          code = TSDB_CODE_TSC_STMT_API_ERROR;
        }
      }
      break;
    default:
      code = TSDB_CODE_APP_ERROR;
      break;
  }

  STMT_ERR_RET(code);

  pStmt->sql.status = newStatus;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetTbName(TAOS_STMT* stmt, char** tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->sql.type = STMT_TYPE_MULTI_INSERT;

  if ('\0' == pStmt->bInfo.tbName[0]) {
    tscError("no table name set");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  *tbName = pStmt->bInfo.tbName;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtBackupQueryFields(STscStmt* pStmt) {
  SStmtQueryResInfo* pRes = &pStmt->sql.queryRes;
  pRes->numOfCols = pStmt->exec.pRequest->body.resInfo.numOfCols;
  pRes->precision = pStmt->exec.pRequest->body.resInfo.precision;

  int32_t size = pRes->numOfCols * sizeof(TAOS_FIELD);
  pRes->fields = taosMemoryMalloc(size);
  pRes->userFields = taosMemoryMalloc(size);
  if (NULL == pRes->fields || NULL == pRes->userFields) {
    STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  memcpy(pRes->fields, pStmt->exec.pRequest->body.resInfo.fields, size);
  memcpy(pRes->userFields, pStmt->exec.pRequest->body.resInfo.userFields, size);

  return TSDB_CODE_SUCCESS;
}

int32_t stmtRestoreQueryFields(STscStmt* pStmt) {
  SStmtQueryResInfo* pRes = &pStmt->sql.queryRes;
  int32_t            size = pRes->numOfCols * sizeof(TAOS_FIELD);

  pStmt->exec.pRequest->body.resInfo.numOfCols = pRes->numOfCols;
  pStmt->exec.pRequest->body.resInfo.precision = pRes->precision;

  if (NULL == pStmt->exec.pRequest->body.resInfo.fields) {
    pStmt->exec.pRequest->body.resInfo.fields = taosMemoryMalloc(size);
    if (NULL == pStmt->exec.pRequest->body.resInfo.fields) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    memcpy(pStmt->exec.pRequest->body.resInfo.fields, pRes->fields, size);
  }

  if (NULL == pStmt->exec.pRequest->body.resInfo.userFields) {
    pStmt->exec.pRequest->body.resInfo.userFields = taosMemoryMalloc(size);
    if (NULL == pStmt->exec.pRequest->body.resInfo.userFields) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    memcpy(pStmt->exec.pRequest->body.resInfo.userFields, pRes->userFields, size);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtUpdateBindInfo(TAOS_STMT* stmt, STableMeta* pTableMeta, void* tags, SName* tbName, const char* sTableName,
                           bool autoCreateTbl) {
  STscStmt* pStmt = (STscStmt*)stmt;
  char      tbFName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(tbName, tbFName);

  memcpy(&pStmt->bInfo.sname, tbName, sizeof(*tbName));
  strncpy(pStmt->bInfo.tbFName, tbFName, sizeof(pStmt->bInfo.tbFName) - 1);
  pStmt->bInfo.tbFName[sizeof(pStmt->bInfo.tbFName) - 1] = 0;

  pStmt->bInfo.tbUid = autoCreateTbl ? 0 : pTableMeta->uid;
  pStmt->bInfo.tbSuid = pTableMeta->suid;
  pStmt->bInfo.tbType = pTableMeta->tableType;
  pStmt->bInfo.boundTags = tags;
  pStmt->bInfo.tagsCached = false;
  tstrncpy(pStmt->bInfo.stbFName, sTableName, sizeof(pStmt->bInfo.stbFName));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtUpdateExecInfo(TAOS_STMT* stmt, SHashObj* pVgHash, SHashObj* pBlockHash) {
  STscStmt* pStmt = (STscStmt*)stmt;

  pStmt->sql.pVgHash = pVgHash;
  pStmt->exec.pBlockHash = pBlockHash;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtUpdateInfo(TAOS_STMT* stmt, STableMeta* pTableMeta, void* tags, SName* tbName, bool autoCreateTbl,
                       SHashObj* pVgHash, SHashObj* pBlockHash, const char* sTableName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_ERR_RET(stmtUpdateBindInfo(stmt, pTableMeta, tags, tbName, sTableName, autoCreateTbl));
  STMT_ERR_RET(stmtUpdateExecInfo(stmt, pVgHash, pBlockHash));

  pStmt->sql.autoCreateTbl = autoCreateTbl;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetExecInfo(TAOS_STMT* stmt, SHashObj** pVgHash, SHashObj** pBlockHash) {
  STscStmt* pStmt = (STscStmt*)stmt;

  *pVgHash = pStmt->sql.pVgHash;
  pStmt->sql.pVgHash = NULL;
  
  *pBlockHash = pStmt->exec.pBlockHash;
  pStmt->exec.pBlockHash = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCacheBlock(STscStmt* pStmt) {
  if (pStmt->sql.type != STMT_TYPE_MULTI_INSERT) {
    return TSDB_CODE_SUCCESS;
  }

  uint64_t uid = pStmt->bInfo.tbUid;
  uint64_t cacheUid = (TSDB_CHILD_TABLE == pStmt->bInfo.tbType) ? pStmt->bInfo.tbSuid : uid;

  if (taosHashGet(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid))) {
    return TSDB_CODE_SUCCESS;
  }

  STableDataCxt** pSrc = taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (!pSrc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  STableDataCxt* pDst = NULL;

  STMT_ERR_RET(qCloneStmtDataBlock(&pDst, *pSrc, true));

  SStmtTableCache cache = {
      .pDataCtx = pDst,
      .boundTags = pStmt->bInfo.boundTags,
  };

  if (taosHashPut(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid), &cache, sizeof(cache))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (pStmt->sql.autoCreateTbl) {
    pStmt->bInfo.tagsCached = true;
  } else {
    pStmt->bInfo.boundTags = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtParseSql(STscStmt* pStmt) {
  pStmt->exec.pCurrBlock = NULL;

  SStmtCallback stmtCb = {
      .pStmt = pStmt,
      .getTbNameFn = stmtGetTbName,
      .setInfoFn = stmtUpdateInfo,
      .getExecInfoFn = stmtGetExecInfo,
  };

  STMT_ERR_RET(stmtCreateRequest(pStmt));

  STMT_ERR_RET(parseSql(pStmt->exec.pRequest, false, &pStmt->sql.pQuery, &stmtCb));

  pStmt->bInfo.needParse = false;

  if (pStmt->sql.pQuery->pRoot && 0 == pStmt->sql.type) {
    pStmt->sql.type = STMT_TYPE_INSERT;
  } else if (pStmt->sql.pQuery->pPrepareRoot) {
    pStmt->sql.type = STMT_TYPE_QUERY;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanBindInfo(STscStmt* pStmt) {
  pStmt->bInfo.tbUid = 0;
  pStmt->bInfo.tbSuid = 0;
  pStmt->bInfo.tbType = 0;
  pStmt->bInfo.needParse = true;
  pStmt->bInfo.inExecCache = false;

  pStmt->bInfo.tbName[0] = 0;
  pStmt->bInfo.tbFName[0] = 0;
  if (!pStmt->bInfo.tagsCached) {
    qDestroyBoundColInfo(pStmt->bInfo.boundTags);
    taosMemoryFreeClear(pStmt->bInfo.boundTags);
  }
  memset(pStmt->bInfo.stbFName, 0, TSDB_TABLE_FNAME_LEN);
  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanExecInfo(STscStmt* pStmt, bool keepTable, bool deepClean) {
  if (STMT_TYPE_QUERY != pStmt->sql.type || deepClean) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  size_t keyLen = 0;
  void*  pIter = taosHashIterate(pStmt->exec.pBlockHash, NULL);
  while (pIter) {
    STableDataCxt* pBlocks = *(STableDataCxt**)pIter;
    char*          key = taosHashGetKey(pIter, &keyLen);
    STableMeta*    pMeta = qGetTableMetaInDataBlock(pBlocks);

    if (keepTable && pBlocks == pStmt->exec.pCurrBlock) {
      TSWAP(pBlocks->pData, pStmt->exec.pCurrTbData);
      STMT_ERR_RET(qResetStmtDataBlock(pBlocks, false));

      pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
      continue;
    }

    qDestroyStmtDataBlock(pBlocks);
    taosHashRemove(pStmt->exec.pBlockHash, key, keyLen);

    pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
  }

  if (keepTable) {
    return TSDB_CODE_SUCCESS;
  }

  taosHashCleanup(pStmt->exec.pBlockHash);
  pStmt->exec.pBlockHash = NULL;

  tDestroySubmitTbData(pStmt->exec.pCurrTbData, TSDB_MSG_FLG_ENCODE);
  taosMemoryFreeClear(pStmt->exec.pCurrTbData);

  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanSQLInfo(STscStmt* pStmt) {
  STMT_DLOG_E("start to free SQL info");
  
  taosMemoryFree(pStmt->sql.queryRes.fields);
  taosMemoryFree(pStmt->sql.queryRes.userFields);
  taosMemoryFree(pStmt->sql.sqlStr);
  qDestroyQuery(pStmt->sql.pQuery);
  taosArrayDestroy(pStmt->sql.nodeList);
  taosHashCleanup(pStmt->sql.pVgHash);
  pStmt->sql.pVgHash = NULL;

  void* pIter = taosHashIterate(pStmt->sql.pTableCache, NULL);
  while (pIter) {
    SStmtTableCache* pCache = (SStmtTableCache*)pIter;

    qDestroyStmtDataBlock(pCache->pDataCtx);
    qDestroyBoundColInfo(pCache->boundTags);
    taosMemoryFreeClear(pCache->boundTags);

    pIter = taosHashIterate(pStmt->sql.pTableCache, pIter);
  }
  taosHashCleanup(pStmt->sql.pTableCache);
  pStmt->sql.pTableCache = NULL;

  STMT_ERR_RET(stmtCleanExecInfo(pStmt, false, true));
  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  memset(&pStmt->sql, 0, sizeof(pStmt->sql));

  STMT_DLOG_E("end to free SQL info");

  return TSDB_CODE_SUCCESS;
}

int32_t stmtRebuildDataBlock(STscStmt* pStmt, STableDataCxt* pDataBlock, STableDataCxt** newBlock, uint64_t uid,
                             uint64_t suid) {
  SEpSet           ep = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
  SVgroupInfo      vgInfo = {0};
  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pStmt->exec.pRequest->requestId,
                           .requestObjRefId = pStmt->exec.pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};

  STMT_ERR_RET(catalogGetTableHashVgroup(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &vgInfo));
  STMT_ERR_RET(
      taosHashPut(pStmt->sql.pVgHash, (const char*)&vgInfo.vgId, sizeof(vgInfo.vgId), (char*)&vgInfo, sizeof(vgInfo)));

  STMT_ERR_RET(qRebuildStmtDataBlock(newBlock, pDataBlock, uid, suid, vgInfo.vgId, pStmt->sql.autoCreateTbl));

  STMT_DLOG("tableDataCxt rebuilt, uid:%" PRId64 ", vgId:%d", uid, vgInfo.vgId);

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetFromCache(STscStmt* pStmt) {
  pStmt->bInfo.needParse = true;
  pStmt->bInfo.inExecCache = false;

  STableDataCxt** pCxtInExec = taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (pCxtInExec) {
    pStmt->bInfo.needParse = false;
    pStmt->bInfo.inExecCache = true;

    pStmt->exec.pCurrBlock = *pCxtInExec;

    if (pStmt->sql.autoCreateTbl) {
      tscDebug("reuse stmt block for tb %s in execBlock", pStmt->bInfo.tbFName);
      return TSDB_CODE_SUCCESS;
    }
  }

  if (NULL == pStmt->sql.pTableCache || taosHashGetSize(pStmt->sql.pTableCache) <= 0) {
    if (pStmt->bInfo.inExecCache) {
      pStmt->bInfo.needParse = false;
      tscDebug("reuse stmt block for tb %s in execBlock", pStmt->bInfo.tbFName);
      return TSDB_CODE_SUCCESS;
    }

    tscDebug("no stmt block cache for tb %s", pStmt->bInfo.tbFName);
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pStmt->pCatalog) {
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
  }

  if (pStmt->sql.autoCreateTbl) {
    SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &pStmt->bInfo.tbSuid, sizeof(pStmt->bInfo.tbSuid));
    if (pCache) {
      pStmt->bInfo.needParse = false;
      pStmt->bInfo.tbUid = 0;

      STableDataCxt* pNewBlock = NULL;
      STMT_ERR_RET(stmtRebuildDataBlock(pStmt, pCache->pDataCtx, &pNewBlock, 0, pStmt->bInfo.tbSuid));

      if (taosHashPut(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName), &pNewBlock,
                      POINTER_BYTES)) {
        STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      pStmt->exec.pCurrBlock = pNewBlock;

      tscDebug("reuse stmt block for tb %s in sqlBlock, suid:0x%" PRIx64, pStmt->bInfo.tbFName, pStmt->bInfo.tbSuid);

      return TSDB_CODE_SUCCESS;
    }

    STMT_RET(stmtCleanBindInfo(pStmt));
  }

  STableMeta*      pTableMeta = NULL;
  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pStmt->exec.pRequest->requestId,
                           .requestObjRefId = pStmt->exec.pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};
  int32_t          code = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pTableMeta);
  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
    tscDebug("tb %s not exist", pStmt->bInfo.tbFName);
    stmtCleanBindInfo(pStmt);

    STMT_ERR_RET(code);
  }

  STMT_ERR_RET(code);

  uint64_t uid = pTableMeta->uid;
  uint64_t suid = pTableMeta->suid;
  int8_t   tableType = pTableMeta->tableType;
  taosMemoryFree(pTableMeta);
  uint64_t cacheUid = (TSDB_CHILD_TABLE == tableType) ? suid : uid;

  if (uid == pStmt->bInfo.tbUid) {
    pStmt->bInfo.needParse = false;

    tscDebug("tb %s is current table", pStmt->bInfo.tbFName);

    return TSDB_CODE_SUCCESS;
  }

  if (pStmt->bInfo.inExecCache) {
    SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid));
    if (NULL == pCache) {
      tscError("table [%s, %" PRIx64 ", %" PRIx64 "] found in exec blockHash, but not in sql blockHash",
               pStmt->bInfo.tbFName, uid, cacheUid);

      STMT_ERR_RET(TSDB_CODE_APP_ERROR);
    }

    pStmt->bInfo.needParse = false;

    pStmt->bInfo.tbUid = uid;
    pStmt->bInfo.tbSuid = suid;
    pStmt->bInfo.tbType = tableType;
    pStmt->bInfo.boundTags = pCache->boundTags;
    pStmt->bInfo.tagsCached = true;

    tscDebug("tb %s in execBlock list, set to current", pStmt->bInfo.tbFName);

    return TSDB_CODE_SUCCESS;
  }

  SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid));
  if (pCache) {
    pStmt->bInfo.needParse = false;

    pStmt->bInfo.tbUid = uid;
    pStmt->bInfo.tbSuid = suid;
    pStmt->bInfo.tbType = tableType;
    pStmt->bInfo.boundTags = pCache->boundTags;
    pStmt->bInfo.tagsCached = true;

    STableDataCxt* pNewBlock = NULL;
    STMT_ERR_RET(stmtRebuildDataBlock(pStmt, pCache->pDataCtx, &pNewBlock, uid, suid));

    if (taosHashPut(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName), &pNewBlock,
                    POINTER_BYTES)) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pStmt->exec.pCurrBlock = pNewBlock;

    tscDebug("tb %s in sqlBlock list, set to current", pStmt->bInfo.tbFName);

    return TSDB_CODE_SUCCESS;
  }

  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

int32_t stmtResetStmt(STscStmt* pStmt) {
  STMT_ERR_RET(stmtCleanSQLInfo(pStmt));

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    STMT_ERR_RET(terrno);
  }

  pStmt->sql.status = STMT_INIT;

  return TSDB_CODE_SUCCESS;
}

TAOS_STMT* stmtInit(STscObj* taos, int64_t reqid) {
  STscObj*  pObj = (STscObj*)taos;
  STscStmt* pStmt = NULL;

  pStmt = taosMemoryCalloc(1, sizeof(STscStmt));
  if (NULL == pStmt) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pStmt);
    return NULL;
  }

  pStmt->taos = pObj;
  pStmt->bInfo.needParse = true;
  pStmt->sql.status = STMT_INIT;
  pStmt->reqid = reqid;

  STMT_LOG_SEQ(STMT_INIT);

  tscDebug("stmt:%p initialized", pStmt);

  return pStmt;
}

int stmtPrepare(TAOS_STMT* stmt, const char* sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to prepare");

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

int stmtSetTbName(TAOS_STMT* stmt, const char* tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG("start to set tbName: %s", tbName);

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTBNAME));

  int32_t insert = 0;
  stmtIsInsert(stmt, &insert);
  if (0 == insert) {
    tscError("set tb name not available for none insert statement");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STMT_ERR_RET(stmtCreateRequest(pStmt));

  STMT_ERR_RET(qCreateSName(&pStmt->bInfo.sname, tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb,
                            pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
  tNameExtractFullName(&pStmt->bInfo.sname, pStmt->bInfo.tbFName);

  STMT_ERR_RET(stmtGetFromCache(pStmt));

  if (pStmt->bInfo.needParse) {
    strncpy(pStmt->bInfo.tbName, tbName, sizeof(pStmt->bInfo.tbName) - 1);
    pStmt->bInfo.tbName[sizeof(pStmt->bInfo.tbName) - 1] = 0;

    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  return TSDB_CODE_SUCCESS;
}

int stmtSetTbTags(TAOS_STMT* stmt, TAOS_MULTI_BIND* tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to set tbTags");

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTAGS));

  if (pStmt->bInfo.inExecCache) {
    return TSDB_CODE_SUCCESS;
  }

  STableDataCxt** pDataBlock =
      (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (NULL == pDataBlock) {
    tscError("table %s not found in exec blockHash", pStmt->bInfo.tbFName);
    STMT_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  tscDebug("start to bind stmt tag values");
  STMT_ERR_RET(qBindStmtTagsValue(*pDataBlock, pStmt->bInfo.boundTags, pStmt->bInfo.tbSuid, pStmt->bInfo.stbFName,
                                  pStmt->bInfo.sname.tname, tags, pStmt->exec.pRequest->msgBuf,
                                  pStmt->exec.pRequest->msgBufLen));

  return TSDB_CODE_SUCCESS;
}

int stmtFetchTagFields(STscStmt* pStmt, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query tag fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataCxt** pDataBlock =
      (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (NULL == pDataBlock) {
    tscError("table %s not found in exec blockHash", pStmt->bInfo.tbFName);
    STMT_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  STMT_ERR_RET(qBuildStmtTagFields(*pDataBlock, pStmt->bInfo.boundTags, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

int stmtFetchColFields(STscStmt* pStmt, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query column fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataCxt** pDataBlock =
      (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (NULL == pDataBlock) {
    tscError("table %s not found in exec blockHash", pStmt->bInfo.tbFName);
    STMT_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  STMT_ERR_RET(qBuildStmtColFields(*pDataBlock, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

int stmtBindBatch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind, int32_t colIdx) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG("start to bind stmt data, colIdx: %d", colIdx);

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_BIND));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  STMT_ERR_RET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    STMT_ERR_RET(qStmtBindParams(pStmt->sql.pQuery, bind, colIdx));

    SParseContext ctx = {.requestId = pStmt->exec.pRequest->requestId,
                         .acctId = pStmt->taos->acctId,
                         .db = pStmt->exec.pRequest->pDb,
                         .topicQuery = false,
                         .pSql = pStmt->sql.sqlStr,
                         .sqlLen = pStmt->sql.sqlLen,
                         .pMsg = pStmt->exec.pRequest->msgBuf,
                         .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                         .pTransporter = pStmt->taos->pAppInfo->pTransporter,
                         .pStmtCb = NULL,
                         .pUser = pStmt->taos->user};
    ctx.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &ctx.pCatalog));

    STMT_ERR_RET(qStmtParseQuerySql(&ctx, pStmt->sql.pQuery));

    if (pStmt->sql.pQuery->haveResultSet) {
      setResSchemaInfo(&pStmt->exec.pRequest->body.resInfo, pStmt->sql.pQuery->pResSchema,
                       pStmt->sql.pQuery->numOfResCols);
      taosMemoryFreeClear(pStmt->sql.pQuery->pResSchema);
      setResPrecision(&pStmt->exec.pRequest->body.resInfo, pStmt->sql.pQuery->precision);
    }

    TSWAP(pStmt->exec.pRequest->dbList, pStmt->sql.pQuery->pDbList);
    TSWAP(pStmt->exec.pRequest->tableList, pStmt->sql.pQuery->pTableList);
    TSWAP(pStmt->exec.pRequest->targetTableList, pStmt->sql.pQuery->pTargetTableList);

    // if (STMT_TYPE_QUERY == pStmt->sql.queryRes) {
    //   STMT_ERR_RET(stmtRestoreQueryFields(pStmt));
    // }

    // STMT_ERR_RET(stmtBackupQueryFields(pStmt));

    return TSDB_CODE_SUCCESS;
  }

  STableDataCxt** pDataBlock = NULL;

  if (pStmt->exec.pCurrBlock) {
    pDataBlock = &pStmt->exec.pCurrBlock;
  } else {
    pDataBlock =
        (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
    if (NULL == pDataBlock) {
      tscError("table %s not found in exec blockHash", pStmt->bInfo.tbFName);
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_CACHE_ERROR);
    }
    pStmt->exec.pCurrBlock = *pDataBlock;
  }

  if (colIdx < 0) {
    int32_t code = qBindStmtColsValue(*pDataBlock, bind, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen);
    if (code) {
      tscError("qBindStmtColsValue failed, error:%s", tstrerror(code));
      STMT_ERR_RET(code);
    }
  } else {
    if (colIdx != (pStmt->bInfo.sBindLastIdx + 1) && colIdx != 0) {
      tscError("bind column index not in sequence");
      STMT_ERR_RET(TSDB_CODE_APP_ERROR);
    }

    pStmt->bInfo.sBindLastIdx = colIdx;

    if (0 == colIdx) {
      pStmt->bInfo.sBindRowNum = bind->num;
    }

    qBindStmtSingleColValue(*pDataBlock, bind, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen, colIdx,
                            pStmt->bInfo.sBindRowNum);
  }

  return TSDB_CODE_SUCCESS;
}

int stmtAddBatch(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to add batch");

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_ADD_BATCH));

  STMT_ERR_RET(stmtCacheBlock(pStmt));

  return TSDB_CODE_SUCCESS;
}

int stmtUpdateTableUid(STscStmt* pStmt, SSubmitRsp* pRsp) {
  tscDebug("stmt start to update tbUid, blockNum: %d", pRsp->nBlocks);

  int32_t code = 0;
  int32_t finalCode = 0;
  size_t  keyLen = 0;
  void*   pIter = taosHashIterate(pStmt->exec.pBlockHash, NULL);
  while (pIter) {
    STableDataCxt* pBlock = *(STableDataCxt**)pIter;
    char*          key = taosHashGetKey(pIter, &keyLen);

    STableMeta* pMeta = qGetTableMetaInDataBlock(pBlock);
    if (pMeta->uid) {
      pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
      continue;
    }

    SSubmitBlkRsp* blkRsp = NULL;
    int32_t        i = 0;
    for (; i < pRsp->nBlocks; ++i) {
      blkRsp = pRsp->pBlocks + i;
      if (strlen(blkRsp->tblFName) != keyLen) {
        continue;
      }

      if (strncmp(blkRsp->tblFName, key, keyLen)) {
        continue;
      }

      break;
    }

    if (i < pRsp->nBlocks) {
      tscDebug("auto created table %s uid updated from %" PRIx64 " to %" PRIx64, blkRsp->tblFName, pMeta->uid,
               blkRsp->uid);

      pMeta->uid = blkRsp->uid;
      pStmt->bInfo.tbUid = blkRsp->uid;
    } else {
      tscDebug("table %s not found in submit rsp, will update from catalog", pStmt->bInfo.tbFName);
      if (NULL == pStmt->pCatalog) {
        code = catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog);
        if (code) {
          pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
          finalCode = code;
          continue;
        }
      }

      code = stmtCreateRequest(pStmt);
      if (code) {
        pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
        finalCode = code;
        continue;
      }

      STableMeta*      pTableMeta = NULL;
      SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                               .requestId = pStmt->exec.pRequest->requestId,
                               .requestObjRefId = pStmt->exec.pRequest->self,
                               .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};
      int32_t          code = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pTableMeta);

      taos_free_result(pStmt->exec.pRequest);
      pStmt->exec.pRequest = NULL;

      if (code || NULL == pTableMeta) {
        pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
        finalCode = code;
        taosMemoryFree(pTableMeta);
        continue;
      }

      pMeta->uid = pTableMeta->uid;
      pStmt->bInfo.tbUid = pTableMeta->uid;
      taosMemoryFree(pTableMeta);
    }

    pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
  }

  return finalCode;
}

int stmtExec(TAOS_STMT* stmt) {
  STscStmt*   pStmt = (STscStmt*)stmt;
  int32_t     code = 0;
  SSubmitRsp* pRsp = NULL;

  STMT_DLOG_E("start to exec");

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_EXECUTE));

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);
  } else {
    tDestroySubmitTbData(pStmt->exec.pCurrTbData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFreeClear(pStmt->exec.pCurrTbData);

    STMT_ERR_RET(qCloneCurrentTbData(pStmt->exec.pCurrBlock, &pStmt->exec.pCurrTbData));

    STMT_ERR_RET(qBuildStmtOutput(pStmt->sql.pQuery, pStmt->sql.pVgHash, pStmt->exec.pBlockHash));
    launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);
  }

  if (pStmt->exec.pRequest->code && NEED_CLIENT_HANDLE_ERROR(pStmt->exec.pRequest->code)) {
    code = refreshMeta(pStmt->exec.pRequest->pTscObj, pStmt->exec.pRequest);
    if (code) {
      pStmt->exec.pRequest->code = code;
    } else {
      tFreeSSubmitRsp(pRsp);
      STMT_ERR_RET(stmtResetStmt(pStmt));
      STMT_ERR_RET(TSDB_CODE_NEED_RETRY);
    }
  }

  STMT_ERR_JRET(pStmt->exec.pRequest->code);

  pStmt->exec.affectedRows = taos_affected_rows(pStmt->exec.pRequest);
  pStmt->affectedRows += pStmt->exec.affectedRows;

_return:

  stmtCleanExecInfo(pStmt, (code ? false : true), false);

  tFreeSSubmitRsp(pRsp);

  ++pStmt->sql.runTimes;

  STMT_RET(code);
}

int stmtClose(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to free stmt");

  stmtCleanSQLInfo(pStmt);
  taosMemoryFree(stmt);

  return TSDB_CODE_SUCCESS;
}

const char* stmtErrstr(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || NULL == pStmt->exec.pRequest) {
    return (char*)tstrerror(terrno);
  }

  pStmt->exec.pRequest->code = terrno;

  return taos_errstr(pStmt->exec.pRequest);
}

int stmtAffectedRows(TAOS_STMT* stmt) { return ((STscStmt*)stmt)->affectedRows; }

int stmtAffectedRowsOnce(TAOS_STMT* stmt) { return ((STscStmt*)stmt)->exec.affectedRows; }

int stmtIsInsert(TAOS_STMT* stmt, int* insert) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start is insert");

  if (pStmt->sql.type) {
    *insert = (STMT_TYPE_INSERT == pStmt->sql.type || STMT_TYPE_MULTI_INSERT == pStmt->sql.type);
  } else {
    *insert = qIsInsertValuesSql(pStmt->sql.sqlStr, pStmt->sql.sqlLen);
  }

  return TSDB_CODE_SUCCESS;
}

int stmtGetTagFields(TAOS_STMT* stmt, int* nums, TAOS_FIELD_E** fields) {
  int32_t code = 0;
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t preCode = pStmt->errCode;

  STMT_DLOG_E("start to get tag fields");

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    STMT_ERRI_JRET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STMT_ERRI_JRET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  STMT_ERRI_JRET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERRI_JRET(stmtParseSql(pStmt));
  }

  STMT_ERRI_JRET(stmtFetchTagFields(stmt, nums, fields));

_return:

  pStmt->errCode = preCode;
  
  return code;
}

int stmtGetColFields(TAOS_STMT* stmt, int* nums, TAOS_FIELD_E** fields) {
  int32_t code = 0;
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t preCode = pStmt->errCode;

  STMT_DLOG_E("start to get col fields");

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    STMT_ERRI_JRET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STMT_ERRI_JRET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  STMT_ERRI_JRET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERRI_JRET(stmtParseSql(pStmt));
  }

  STMT_ERRI_JRET(stmtFetchColFields(stmt, nums, fields));

_return:

  pStmt->errCode = preCode;

  return code;
}

int stmtGetParamNum(TAOS_STMT* stmt, int* nums) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to get param num");

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  STMT_ERR_RET(stmtCreateRequest(pStmt));
  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    *nums = taosArrayGetSize(pStmt->sql.pQuery->pPlaceholderValues);
  } else {
    STMT_ERR_RET(stmtFetchColFields(stmt, nums, NULL));
  }

  return TSDB_CODE_SUCCESS;
}

int stmtGetParam(TAOS_STMT* stmt, int idx, int* type, int* bytes) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to get param");

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    STMT_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }

  STMT_ERR_RET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }

  int32_t       nums = 0;
  TAOS_FIELD_E* pField = NULL;
  STMT_ERR_RET(stmtFetchColFields(stmt, &nums, &pField));
  if (idx >= nums) {
    tscError("idx %d is too big", idx);
    taosMemoryFree(pField);
    STMT_ERR_RET(TSDB_CODE_INVALID_PARA);
  }

  *type = pField[idx].type;
  *bytes = pField[idx].bytes;

  taosMemoryFree(pField);

  return TSDB_CODE_SUCCESS;
}

TAOS_RES* stmtUseResult(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to use result");

  if (STMT_TYPE_QUERY != pStmt->sql.type) {
    tscError("useResult only for query statement");
    return NULL;
  }

  return pStmt->exec.pRequest;
}
