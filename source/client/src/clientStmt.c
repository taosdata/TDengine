
#include "clientInt.h"
#include "clientLog.h"
#include "tdef.h"

#include "clientStmt.h"

char* gStmtStatusStr[] = {"unknown",     "init", "prepare", "settbname", "settags",
                          "fetchFields", "bind", "bindCol", "addBatch",  "exec"};

static FORCE_INLINE int32_t stmtAllocQNodeFromBuf(STableBufInfo* pTblBuf, void** pBuf) {
  if (pTblBuf->buffOffset < pTblBuf->buffSize) {
    *pBuf = (char*)pTblBuf->pCurBuff + pTblBuf->buffOffset;
    pTblBuf->buffOffset += pTblBuf->buffUnit;
  } else if (pTblBuf->buffIdx < taosArrayGetSize(pTblBuf->pBufList)) {
    pTblBuf->pCurBuff = taosArrayGetP(pTblBuf->pBufList, pTblBuf->buffIdx++);
    if (NULL == pTblBuf->pCurBuff) {
      return TAOS_GET_TERRNO(terrno);
    }
    *pBuf = pTblBuf->pCurBuff;
    pTblBuf->buffOffset = pTblBuf->buffUnit;
  } else {
    void* buff = taosMemoryMalloc(pTblBuf->buffSize);
    if (NULL == buff) {
      return terrno;
    }

    if (taosArrayPush(pTblBuf->pBufList, &buff) == NULL) {
      return terrno;
    }

    pTblBuf->buffIdx++;
    pTblBuf->pCurBuff = buff;
    *pBuf = buff;
    pTblBuf->buffOffset = pTblBuf->buffUnit;
  }

  return TSDB_CODE_SUCCESS;
}

bool stmtDequeue(STscStmt* pStmt, SStmtQNode** param) {
  while (0 == atomic_load_64(&pStmt->queue.qRemainNum)) {
    taosUsleep(1);
    return false;
  }

  SStmtQNode* orig = pStmt->queue.head;

  SStmtQNode* node = pStmt->queue.head->next;
  pStmt->queue.head = pStmt->queue.head->next;

  // taosMemoryFreeClear(orig);

  *param = node;

  (void)atomic_sub_fetch_64(&pStmt->queue.qRemainNum, 1);

  return true;
}

void stmtEnqueue(STscStmt* pStmt, SStmtQNode* param) {
  pStmt->queue.tail->next = param;
  pStmt->queue.tail = param;

  pStmt->stat.bindDataNum++;
  (void)atomic_add_fetch_64(&pStmt->queue.qRemainNum, 1);
}

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
      pStmt->exec.pRequest->isStmtBind = true;
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
      if (STMT_STATUS_EQ(INIT)) {
        code = TSDB_CODE_TSC_STMT_API_ERROR;
      }
      if (!pStmt->sql.stbInterlaceMode && (STMT_STATUS_EQ(BIND) || STMT_STATUS_EQ(BIND_COL))) {
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
    STMT_ERR_RET(terrno);
  }
  (void)memcpy(pRes->fields, pStmt->exec.pRequest->body.resInfo.fields, size);
  (void)memcpy(pRes->userFields, pStmt->exec.pRequest->body.resInfo.userFields, size);

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
      STMT_ERR_RET(terrno);
    }
    (void)memcpy(pStmt->exec.pRequest->body.resInfo.fields, pRes->fields, size);
  }

  if (NULL == pStmt->exec.pRequest->body.resInfo.userFields) {
    pStmt->exec.pRequest->body.resInfo.userFields = taosMemoryMalloc(size);
    if (NULL == pStmt->exec.pRequest->body.resInfo.userFields) {
      STMT_ERR_RET(terrno);
    }
    (void)memcpy(pStmt->exec.pRequest->body.resInfo.userFields, pRes->userFields, size);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtUpdateBindInfo(TAOS_STMT* stmt, STableMeta* pTableMeta, void* tags, SName* tbName, const char* sTableName,
                           bool autoCreateTbl) {
  STscStmt* pStmt = (STscStmt*)stmt;
  char      tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t   code = tNameExtractFullName(tbName, tbFName);
  if (code != 0) {
    return code;
  }

  (void)memcpy(&pStmt->bInfo.sname, tbName, sizeof(*tbName));
  (void)strncpy(pStmt->bInfo.tbFName, tbFName, sizeof(pStmt->bInfo.tbFName) - 1);
  pStmt->bInfo.tbFName[sizeof(pStmt->bInfo.tbFName) - 1] = 0;

  pStmt->bInfo.tbUid = autoCreateTbl ? 0 : pTableMeta->uid;
  pStmt->bInfo.tbSuid = pTableMeta->suid;
  pStmt->bInfo.tbVgId = pTableMeta->vgId;
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
  if (pStmt->sql.autoCreateTbl) {
    pStmt->sql.stbInterlaceMode = false;
  }

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
    return terrno;
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

  pStmt->stat.parseSqlNum++;
  STMT_ERR_RET(parseSql(pStmt->exec.pRequest, false, &pStmt->sql.pQuery, &stmtCb));
  pStmt->sql.siInfo.pQuery = pStmt->sql.pQuery;

  pStmt->bInfo.needParse = false;

  if (pStmt->sql.pQuery->pRoot && 0 == pStmt->sql.type) {
    pStmt->sql.type = STMT_TYPE_INSERT;
    pStmt->sql.stbInterlaceMode = false;
  } else if (pStmt->sql.pQuery->pPrepareRoot) {
    pStmt->sql.type = STMT_TYPE_QUERY;
    pStmt->sql.stbInterlaceMode = false;

    return TSDB_CODE_SUCCESS;
  }

  STableDataCxt** pSrc =
      (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (NULL == pSrc || NULL == *pSrc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  STableDataCxt* pTableCtx = *pSrc;
  if (pStmt->sql.stbInterlaceMode) {
    int16_t lastIdx = -1;

    for (int32_t i = 0; i < pTableCtx->boundColsInfo.numOfBound; ++i) {
      if (pTableCtx->boundColsInfo.pColIndex[i] < lastIdx) {
        pStmt->sql.stbInterlaceMode = false;
        break;
      }

      lastIdx = pTableCtx->boundColsInfo.pColIndex[i];
    }
  }

  if (NULL == pStmt->sql.pBindInfo) {
    pStmt->sql.pBindInfo = taosMemoryMalloc(pTableCtx->boundColsInfo.numOfBound * sizeof(*pStmt->sql.pBindInfo));
    if (NULL == pStmt->sql.pBindInfo) {
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtCleanBindInfo(STscStmt* pStmt) {
  pStmt->bInfo.tbUid = 0;
  pStmt->bInfo.tbSuid = 0;
  pStmt->bInfo.tbVgId = -1;
  pStmt->bInfo.tbType = 0;
  pStmt->bInfo.needParse = true;
  pStmt->bInfo.inExecCache = false;

  pStmt->bInfo.tbName[0] = 0;
  pStmt->bInfo.tbFName[0] = 0;
  if (!pStmt->bInfo.tagsCached) {
    qDestroyBoundColInfo(pStmt->bInfo.boundTags);
    taosMemoryFreeClear(pStmt->bInfo.boundTags);
  }
  pStmt->bInfo.stbFName[0] = 0;

  return TSDB_CODE_SUCCESS;
}

void stmtFreeTableBlkList(STableColsData* pTb) {
  (void)qResetStmtColumns(pTb->aCol, true);
  taosArrayDestroy(pTb->aCol);
}

void stmtResetQueueTableBuf(STableBufInfo* pTblBuf, SStmtQueue* pQueue) {
  pTblBuf->pCurBuff = taosArrayGetP(pTblBuf->pBufList, 0);
  if (NULL == pTblBuf->pCurBuff) {
    tscError("QInfo:%p, failed to get buffer from list", pTblBuf);
    return;
  }
  pTblBuf->buffIdx = 1;
  pTblBuf->buffOffset = sizeof(*pQueue->head);

  pQueue->head = pQueue->tail = pTblBuf->pCurBuff;
  pQueue->qRemainNum = 0;
  pQueue->head->next = NULL;
}

int32_t stmtCleanExecInfo(STscStmt* pStmt, bool keepTable, bool deepClean) {
  if (pStmt->sql.stbInterlaceMode) {
    if (deepClean) {
      taosHashCleanup(pStmt->exec.pBlockHash);
      pStmt->exec.pBlockHash = NULL;

      if (NULL != pStmt->exec.pCurrBlock) {
        taosMemoryFreeClear(pStmt->exec.pCurrBlock->pData);
        qDestroyStmtDataBlock(pStmt->exec.pCurrBlock);
      }
    } else {
      pStmt->sql.siInfo.pTableColsIdx = 0;
      stmtResetQueueTableBuf(&pStmt->sql.siInfo.tbBuf, &pStmt->queue);
    }
  } else {
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
      STMT_ERR_RET(taosHashRemove(pStmt->exec.pBlockHash, key, keyLen));

      pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
    }

    if (keepTable) {
      return TSDB_CODE_SUCCESS;
    }

    taosHashCleanup(pStmt->exec.pBlockHash);
    pStmt->exec.pBlockHash = NULL;

    tDestroySubmitTbData(pStmt->exec.pCurrTbData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFreeClear(pStmt->exec.pCurrTbData);
  }

  STMT_ERR_RET(stmtCleanBindInfo(pStmt));

  return TSDB_CODE_SUCCESS;
}

void stmtFreeTbBuf(void* buf) {
  void* pBuf = *(void**)buf;
  taosMemoryFree(pBuf);
}

void stmtFreeTbCols(void* buf) {
  SArray* pCols = *(SArray**)buf;
  taosArrayDestroy(pCols);
}

int32_t stmtCleanSQLInfo(STscStmt* pStmt) {
  STMT_DLOG_E("start to free SQL info");

  taosMemoryFree(pStmt->sql.pBindInfo);
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

  taos_free_result(pStmt->sql.siInfo.pRequest);
  taosHashCleanup(pStmt->sql.siInfo.pVgroupHash);
  tSimpleHashCleanup(pStmt->sql.siInfo.pTableHash);
  taosArrayDestroyEx(pStmt->sql.siInfo.tbBuf.pBufList, stmtFreeTbBuf);
  taosMemoryFree(pStmt->sql.siInfo.pTSchema);
  qDestroyStmtDataBlock(pStmt->sql.siInfo.pDataCtx);
  taosArrayDestroyEx(pStmt->sql.siInfo.pTableCols, stmtFreeTbCols);

  (void)memset(&pStmt->sql, 0, sizeof(pStmt->sql));
  pStmt->sql.siInfo.tableColsReady = true;

  STMT_DLOG_E("end to free SQL info");

  return TSDB_CODE_SUCCESS;
}

int32_t stmtTryAddTableVgroupInfo(STscStmt* pStmt, int32_t* vgId) {
  if (*vgId >= 0 && taosHashGet(pStmt->sql.pVgHash, (const char*)vgId, sizeof(*vgId))) {
    return TSDB_CODE_SUCCESS;
  }

  SVgroupInfo      vgInfo = {0};
  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pStmt->exec.pRequest->requestId,
                           .requestObjRefId = pStmt->exec.pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};

  int32_t code = catalogGetTableHashVgroup(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &vgInfo);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code =
      taosHashPut(pStmt->sql.pVgHash, (const char*)&vgInfo.vgId, sizeof(vgInfo.vgId), (char*)&vgInfo, sizeof(vgInfo));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *vgId = vgInfo.vgId;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtRebuildDataBlock(STscStmt* pStmt, STableDataCxt* pDataBlock, STableDataCxt** newBlock, uint64_t uid,
                             uint64_t suid, int32_t vgId) {
  STMT_ERR_RET(stmtTryAddTableVgroupInfo(pStmt, &vgId));
  STMT_ERR_RET(qRebuildStmtDataBlock(newBlock, pDataBlock, uid, suid, vgId, pStmt->sql.autoCreateTbl));

  STMT_DLOG("tableDataCxt rebuilt, uid:%" PRId64 ", vgId:%d", uid, vgId);

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetFromCache(STscStmt* pStmt) {
  if (pStmt->sql.stbInterlaceMode && pStmt->sql.siInfo.pDataCtx) {
    pStmt->bInfo.needParse = false;
    pStmt->bInfo.inExecCache = false;
    return TSDB_CODE_SUCCESS;
  }

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

  if (NULL == pStmt->pCatalog) {
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
    pStmt->sql.siInfo.pCatalog = pStmt->pCatalog;
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

  if (pStmt->sql.autoCreateTbl) {
    SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &pStmt->bInfo.tbSuid, sizeof(pStmt->bInfo.tbSuid));
    if (pCache) {
      pStmt->bInfo.needParse = false;
      pStmt->bInfo.tbUid = 0;

      STableDataCxt* pNewBlock = NULL;
      STMT_ERR_RET(stmtRebuildDataBlock(pStmt, pCache->pDataCtx, &pNewBlock, 0, pStmt->bInfo.tbSuid, -1));

      if (taosHashPut(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName), &pNewBlock,
                      POINTER_BYTES)) {
        STMT_ERR_RET(terrno);
      }

      pStmt->exec.pCurrBlock = pNewBlock;

      tscDebug("reuse stmt block for tb %s in sqlBlock, suid:0x%" PRIx64, pStmt->bInfo.tbFName, pStmt->bInfo.tbSuid);

      return TSDB_CODE_SUCCESS;
    }

    STMT_RET(stmtCleanBindInfo(pStmt));
  }

  uint64_t uid, suid;
  int32_t  vgId;
  int8_t   tableType;

  STableMeta*      pTableMeta = NULL;
  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pStmt->exec.pRequest->requestId,
                           .requestObjRefId = pStmt->exec.pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};
  int32_t          code = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pTableMeta);

  pStmt->stat.ctgGetTbMetaNum++;

  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
    tscDebug("tb %s not exist", pStmt->bInfo.tbFName);
    STMT_ERR_RET(stmtCleanBindInfo(pStmt));

    STMT_ERR_RET(code);
  }

  STMT_ERR_RET(code);

  uid = pTableMeta->uid;
  suid = pTableMeta->suid;
  tableType = pTableMeta->tableType;
  pStmt->bInfo.tbVgId = pTableMeta->vgId;
  vgId = pTableMeta->vgId;

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
    STMT_ERR_RET(stmtRebuildDataBlock(pStmt, pCache->pDataCtx, &pNewBlock, uid, suid, vgId));

    if (taosHashPut(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName), &pNewBlock,
                    POINTER_BYTES)) {
      STMT_ERR_RET(terrno);
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
    STMT_ERR_RET(terrno);
  }

  pStmt->sql.status = STMT_INIT;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtAsyncOutput(STscStmt* pStmt, void* param) {
  SStmtQNode* pParam = (SStmtQNode*)param;

  if (pParam->restoreTbCols) {
    for (int32_t i = 0; i < pStmt->sql.siInfo.pTableColsIdx; ++i) {
      SArray** p = (SArray**)TARRAY_GET_ELEM(pStmt->sql.siInfo.pTableCols, i);
      *p = taosArrayInit(20, POINTER_BYTES);
      if (*p == NULL) {
        STMT_ERR_RET(terrno);
      }
    }

    atomic_store_8((int8_t*)&pStmt->sql.siInfo.tableColsReady, true);
  } else {
    STMT_ERR_RET(qAppendStmtTableOutput(pStmt->sql.pQuery, pStmt->sql.pVgHash, &pParam->tblData, pStmt->exec.pCurrBlock,
                                        &pStmt->sql.siInfo));

    // taosMemoryFree(pParam->pTbData);

    (void)atomic_sub_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);
  }
  return TSDB_CODE_SUCCESS;
}

void* stmtBindThreadFunc(void* param) {
  setThreadName("stmtBind");

  qInfo("stmt bind thread started");

  STscStmt* pStmt = (STscStmt*)param;

  while (true) {
    if (atomic_load_8((int8_t*)&pStmt->queue.stopQueue)) {
      break;
    }

    SStmtQNode* asyncParam = NULL;
    if (!stmtDequeue(pStmt, &asyncParam)) {
      continue;
    }

    int ret = stmtAsyncOutput(pStmt, asyncParam);
    if (ret != 0) {
      qError("stmtAsyncOutput failed, reason:%s", tstrerror(ret));
    }
  }

  qInfo("stmt bind thread stopped");

  return NULL;
}

int32_t stmtStartBindThread(STscStmt* pStmt) {
  TdThreadAttr thAttr;
  if (taosThreadAttrInit(&thAttr) != 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE) != 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  if (taosThreadCreate(&pStmt->bindThread, &thAttr, stmtBindThreadFunc, pStmt) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    STMT_ERR_RET(terrno);
  }

  pStmt->bindThreadInUse = true;

  (void)taosThreadAttrDestroy(&thAttr);
  return TSDB_CODE_SUCCESS;
}

int32_t stmtInitQueue(STscStmt* pStmt) {
  STMT_ERR_RET(stmtAllocQNodeFromBuf(&pStmt->sql.siInfo.tbBuf, (void**)&pStmt->queue.head));
  pStmt->queue.tail = pStmt->queue.head;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtInitTableBuf(STableBufInfo* pTblBuf) {
  pTblBuf->buffUnit = sizeof(SStmtQNode);
  pTblBuf->buffSize = pTblBuf->buffUnit * 1000;
  pTblBuf->pBufList = taosArrayInit(100, POINTER_BYTES);
  if (NULL == pTblBuf->pBufList) {
    return terrno;
  }
  void* buff = taosMemoryMalloc(pTblBuf->buffSize);
  if (NULL == buff) {
    return terrno;
  }

  if (taosArrayPush(pTblBuf->pBufList, &buff) == NULL) {
    return terrno;
  }

  pTblBuf->pCurBuff = buff;
  pTblBuf->buffIdx = 1;
  pTblBuf->buffOffset = 0;

  return TSDB_CODE_SUCCESS;
}

TAOS_STMT* stmtInit(STscObj* taos, int64_t reqid, TAOS_STMT_OPTIONS* pOptions) {
  STscObj*  pObj = (STscObj*)taos;
  STscStmt* pStmt = NULL;
  int32_t   code = 0;

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
  pStmt->errCode = TSDB_CODE_SUCCESS;

  if (NULL != pOptions) {
    (void)memcpy(&pStmt->options, pOptions, sizeof(pStmt->options));
    if (pOptions->singleStbInsert && pOptions->singleTableBindOnce) {
      pStmt->stbInterlaceMode = true;
    }
  }

  if (pStmt->stbInterlaceMode) {
    pStmt->sql.siInfo.transport = taos->pAppInfo->pTransporter;
    pStmt->sql.siInfo.acctId = taos->acctId;
    pStmt->sql.siInfo.dbname = taos->db;
    pStmt->sql.siInfo.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
    pStmt->sql.siInfo.pTableHash = tSimpleHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (NULL == pStmt->sql.siInfo.pTableHash) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      (void)stmtClose(pStmt);
      return NULL;
    }
    pStmt->sql.siInfo.pTableCols = taosArrayInit(STMT_TABLE_COLS_NUM, POINTER_BYTES);
    if (NULL == pStmt->sql.siInfo.pTableCols) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      (void)stmtClose(pStmt);
      return NULL;
    }

    code = stmtInitTableBuf(&pStmt->sql.siInfo.tbBuf);
    if (TSDB_CODE_SUCCESS == code) {
      code = stmtInitQueue(pStmt);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = stmtStartBindThread(pStmt);
    }
    if (TSDB_CODE_SUCCESS != code) {
      terrno = code;
      (void)stmtClose(pStmt);
      return NULL;
    }
  }

  pStmt->sql.siInfo.tableColsReady = true;

  STMT_LOG_SEQ(STMT_INIT);

  tscDebug("stmt:%p initialized", pStmt);

  return pStmt;
}

int stmtPrepare(TAOS_STMT* stmt, const char* sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to prepare");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  if (pStmt->sql.status >= STMT_PREPARE) {
    STMT_ERR_RET(stmtResetStmt(pStmt));
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_PREPARE));

  if (length <= 0) {
    length = strlen(sql);
  }

  pStmt->sql.sqlStr = taosStrndup(sql, length);
  if (!pStmt->sql.sqlStr) {
    return terrno;
  }
  pStmt->sql.sqlLen = length;
  pStmt->sql.stbInterlaceMode = pStmt->stbInterlaceMode;

  char* dbName = NULL;
  if (qParseDbName(sql, length, &dbName)) {
    STMT_ERR_RET(stmtSetDbName(stmt, dbName));
    taosMemoryFreeClear(dbName);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stmtInitStbInterlaceTableInfo(STscStmt* pStmt) {
  STableDataCxt** pSrc = taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (!pSrc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  STableDataCxt* pDst = NULL;

  STMT_ERR_RET(qCloneStmtDataBlock(&pDst, *pSrc, true));
  pStmt->sql.siInfo.pDataCtx = pDst;

  SArray* pTblCols = NULL;
  for (int32_t i = 0; i < STMT_TABLE_COLS_NUM; i++) {
    pTblCols = taosArrayInit(20, POINTER_BYTES);
    if (NULL == pTblCols) {
      return terrno;
    }

    if (taosArrayPush(pStmt->sql.siInfo.pTableCols, &pTblCols) == NULL) {
      return terrno;
    }
  }

  pStmt->sql.siInfo.boundTags = pStmt->bInfo.boundTags;

  return TSDB_CODE_SUCCESS;
}

int stmtSetDbName(TAOS_STMT* stmt, const char* dbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG("start to set dbName: %s", dbName);

  STMT_ERR_RET(stmtCreateRequest(pStmt));

  // The SQL statement specifies a database name, overriding the previously specified database
  taosMemoryFreeClear(pStmt->exec.pRequest->pDb);
  pStmt->exec.pRequest->pDb = taosStrdup(dbName);
  if (pStmt->exec.pRequest->pDb == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

int stmtSetTbName(TAOS_STMT* stmt, const char* tbName) {
  STscStmt* pStmt = (STscStmt*)stmt;

  int64_t startUs = taosGetTimestampUs();

  STMT_DLOG("start to set tbName: %s", tbName);

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTBNAME));

  int32_t insert = 0;
  STMT_ERR_RET(stmtIsInsert(stmt, &insert));
  if (0 == insert) {
    tscError("set tb name not available for none insert statement");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (!pStmt->sql.stbInterlaceMode || NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtCreateRequest(pStmt));

    STMT_ERR_RET(qCreateSName(&pStmt->bInfo.sname, tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb,
                              pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
    STMT_ERR_RET(tNameExtractFullName(&pStmt->bInfo.sname, pStmt->bInfo.tbFName));

    STMT_ERR_RET(stmtGetFromCache(pStmt));

    if (pStmt->bInfo.needParse) {
      (void)strncpy(pStmt->bInfo.tbName, tbName, sizeof(pStmt->bInfo.tbName) - 1);
      pStmt->bInfo.tbName[sizeof(pStmt->bInfo.tbName) - 1] = 0;

      STMT_ERR_RET(stmtParseSql(pStmt));
    }
  } else {
    (void)strncpy(pStmt->bInfo.tbName, tbName, sizeof(pStmt->bInfo.tbName) - 1);
    pStmt->bInfo.tbName[sizeof(pStmt->bInfo.tbName) - 1] = 0;
    pStmt->exec.pRequest->requestId++;
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->sql.stbInterlaceMode && NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtInitStbInterlaceTableInfo(pStmt));
  }

  int64_t startUs2 = taosGetTimestampUs();
  pStmt->stat.setTbNameUs += startUs2 - startUs;

  return TSDB_CODE_SUCCESS;
}

int stmtSetTbTags(TAOS_STMT* stmt, TAOS_MULTI_BIND* tags) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to set tbTags");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTAGS));

  SBoundColInfo* tags_info = (SBoundColInfo*)pStmt->bInfo.boundTags;
  if (tags_info->numOfBound <= 0 || tags_info->numOfCols <= 0) {
    tscWarn("no tags bound in sql, will not bound tags");
    return TSDB_CODE_SUCCESS;
  }

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
  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

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
  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    tscError("invalid operation to get query column fileds");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataCxt** pDataBlock = NULL;

  if (pStmt->sql.stbInterlaceMode) {
    pDataBlock = &pStmt->sql.siInfo.pDataCtx;
  } else {
    pDataBlock =
        (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
    if (NULL == pDataBlock) {
      tscError("table %s not found in exec blockHash", pStmt->bInfo.tbFName);
      STMT_ERR_RET(TSDB_CODE_APP_ERROR);
    }
  }

  STMT_ERR_RET(qBuildStmtColFields(*pDataBlock, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

/*
SArray* stmtGetFreeCol(STscStmt* pStmt, int32_t* idx) {
  while (true) {
    if (pStmt->exec.smInfo.pColIdx >= STMT_COL_BUF_SIZE) {
      pStmt->exec.smInfo.pColIdx = 0;
    }

    if ((pStmt->exec.smInfo.pColIdx + 1) == atomic_load_32(&pStmt->exec.smInfo.pColFreeIdx)) {
      taosUsleep(1);
      continue;
    }

    *idx = pStmt->exec.smInfo.pColIdx;
    return pStmt->exec.smInfo.pCols[pStmt->exec.smInfo.pColIdx++];
  }
}
*/

int32_t stmtAppendTablePostHandle(STscStmt* pStmt, SStmtQNode* param) {
  if (NULL == pStmt->sql.siInfo.pVgroupHash) {
    pStmt->sql.siInfo.pVgroupHash =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  }
  if (NULL == pStmt->sql.siInfo.pVgroupList) {
    pStmt->sql.siInfo.pVgroupList = taosArrayInit(64, POINTER_BYTES);
  }

  if (NULL == pStmt->sql.siInfo.pRequest) {
    STMT_ERR_RET(buildRequest(pStmt->taos->id, pStmt->sql.sqlStr, pStmt->sql.sqlLen, NULL, false,
                              (SRequestObj**)&pStmt->sql.siInfo.pRequest, pStmt->reqid));

    if (pStmt->reqid != 0) {
      pStmt->reqid++;
    }
    pStmt->exec.pRequest->syncQuery = true;

    pStmt->sql.siInfo.requestId = ((SRequestObj*)pStmt->sql.siInfo.pRequest)->requestId;
    pStmt->sql.siInfo.requestSelf = ((SRequestObj*)pStmt->sql.siInfo.pRequest)->self;
  }

  if (!pStmt->sql.siInfo.tbFromHash && pStmt->sql.siInfo.firstName[0] &&
      0 == strcmp(pStmt->sql.siInfo.firstName, pStmt->bInfo.tbName)) {
    pStmt->sql.siInfo.tbFromHash = true;
  }

  if (0 == pStmt->sql.siInfo.firstName[0]) {
    (void)strcpy(pStmt->sql.siInfo.firstName, pStmt->bInfo.tbName);
  }

  param->tblData.getFromHash = pStmt->sql.siInfo.tbFromHash;
  param->next = NULL;

  (void)atomic_add_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);

  stmtEnqueue(pStmt, param);

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t stmtGetTableColsFromCache(STscStmt* pStmt, SArray** pTableCols) {
  while (true) {
    if (pStmt->sql.siInfo.pTableColsIdx < taosArrayGetSize(pStmt->sql.siInfo.pTableCols)) {
      *pTableCols = (SArray*)taosArrayGetP(pStmt->sql.siInfo.pTableCols, pStmt->sql.siInfo.pTableColsIdx++);
      break;
    } else {
      SArray* pTblCols = NULL;
      for (int32_t i = 0; i < 100; i++) {
        pTblCols = taosArrayInit(20, POINTER_BYTES);
        if (NULL == pTblCols) {
          return terrno;
        }

        if (taosArrayPush(pStmt->sql.siInfo.pTableCols, &pTblCols) == NULL) {
          return terrno;
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int stmtBindBatch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind, int32_t colIdx) {
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t   code = 0;

  int64_t startUs = taosGetTimestampUs();

  STMT_DLOG("start to bind stmt data, colIdx: %d", colIdx);

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

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
                         .pUser = pStmt->taos->user,
                         .setQueryFp = setQueryRequest};

    ctx.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
    STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &ctx.pCatalog));

    STMT_ERR_RET(qStmtParseQuerySql(&ctx, pStmt->sql.pQuery));

    if (pStmt->sql.pQuery->haveResultSet) {
      STMT_ERR_RET(setResSchemaInfo(&pStmt->exec.pRequest->body.resInfo, pStmt->sql.pQuery->pResSchema,
                                    pStmt->sql.pQuery->numOfResCols));
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

  if (pStmt->sql.stbInterlaceMode && NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtInitStbInterlaceTableInfo(pStmt));
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
    if (pStmt->sql.stbInterlaceMode) {
      taosArrayDestroy(pStmt->exec.pCurrBlock->pData->aCol);
      pStmt->exec.pCurrBlock->pData->aCol = NULL;
    }
  }

  int64_t startUs2 = taosGetTimestampUs();
  pStmt->stat.bindDataUs1 += startUs2 - startUs;

  SStmtQNode* param = NULL;
  if (pStmt->sql.stbInterlaceMode) {
    STMT_ERR_RET(stmtAllocQNodeFromBuf(&pStmt->sql.siInfo.tbBuf, (void**)&param));
    STMT_ERR_RET(stmtGetTableColsFromCache(pStmt, &param->tblData.aCol));
    taosArrayClear(param->tblData.aCol);

    // param->tblData.aCol = taosArrayInit(20, POINTER_BYTES);

    param->restoreTbCols = false;
    (void)strcpy(param->tblData.tbName, pStmt->bInfo.tbName);
  }

  int64_t startUs3 = taosGetTimestampUs();
  pStmt->stat.bindDataUs2 += startUs3 - startUs2;

  SArray* pCols = pStmt->sql.stbInterlaceMode ? param->tblData.aCol : (*pDataBlock)->pData->aCol;

  if (colIdx < 0) {
    if (pStmt->sql.stbInterlaceMode) {
      (*pDataBlock)->pData->flags = 0;
      code = qBindStmtStbColsValue(*pDataBlock, pCols, bind, pStmt->exec.pRequest->msgBuf,
                                   pStmt->exec.pRequest->msgBufLen, &pStmt->sql.siInfo.pTSchema, pStmt->sql.pBindInfo);
    } else {
      code =
          qBindStmtColsValue(*pDataBlock, pCols, bind, pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen);
    }

    if (code) {
      tscError("qBindStmtColsValue failed, error:%s", tstrerror(code));
      STMT_ERR_RET(code);
    }
  } else {
    if (pStmt->sql.stbInterlaceMode) {
      tscError("bind single column not allowed in stb insert mode");
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
    }

    if (colIdx != (pStmt->bInfo.sBindLastIdx + 1) && colIdx != 0) {
      tscError("bind column index not in sequence");
      STMT_ERR_RET(TSDB_CODE_APP_ERROR);
    }

    pStmt->bInfo.sBindLastIdx = colIdx;

    if (0 == colIdx) {
      pStmt->bInfo.sBindRowNum = bind->num;
    }

    code = qBindStmtSingleColValue(*pDataBlock, pCols, bind, pStmt->exec.pRequest->msgBuf,
                                   pStmt->exec.pRequest->msgBufLen, colIdx, pStmt->bInfo.sBindRowNum);
    if (code) {
      tscError("qBindStmtSingleColValue failed, error:%s", tstrerror(code));
      STMT_ERR_RET(code);
    }
  }

  int64_t startUs4 = taosGetTimestampUs();
  pStmt->stat.bindDataUs3 += startUs4 - startUs3;

  if (pStmt->sql.stbInterlaceMode) {
    STMT_ERR_RET(stmtAppendTablePostHandle(pStmt, param));
  }

  pStmt->stat.bindDataUs4 += taosGetTimestampUs() - startUs4;

  return TSDB_CODE_SUCCESS;
}

int stmtAddBatch(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  int64_t startUs = taosGetTimestampUs();

  STMT_DLOG_E("start to add batch");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_ADD_BATCH));

  if (pStmt->sql.stbInterlaceMode) {
    int64_t startUs2 = taosGetTimestampUs();
    pStmt->stat.addBatchUs += startUs2 - startUs;

    pStmt->sql.siInfo.tableColsReady = false;

    SStmtQNode* param = NULL;
    STMT_ERR_RET(stmtAllocQNodeFromBuf(&pStmt->sql.siInfo.tbBuf, (void**)&param));
    param->restoreTbCols = true;
    param->next = NULL;

    stmtEnqueue(pStmt, param);

    return TSDB_CODE_SUCCESS;
  }

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
      code = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pTableMeta);

      pStmt->stat.ctgGetTbMetaNum++;

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

/*
int stmtStaticModeExec(TAOS_STMT* stmt) {
  STscStmt*   pStmt = (STscStmt*)stmt;
  int32_t     code = 0;
  SSubmitRsp* pRsp = NULL;
  if (pStmt->sql.staticMode) {
    return TSDB_CODE_TSC_STMT_API_ERROR;
  }

  STMT_DLOG_E("start to exec");

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_EXECUTE));

  STMT_ERR_RET(qBuildStmtOutputFromTbList(pStmt->sql.pQuery, pStmt->sql.pVgHash, pStmt->exec.pTbBlkList,
pStmt->exec.pCurrBlock, pStmt->exec.tbBlkNum));

  launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);

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
*/

int stmtExec(TAOS_STMT* stmt) {
  STscStmt*   pStmt = (STscStmt*)stmt;
  int32_t     code = 0;
  SSubmitRsp* pRsp = NULL;

  int64_t startUs = taosGetTimestampUs();

  STMT_DLOG_E("start to exec");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_EXECUTE));

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);
  } else {
    if (pStmt->sql.stbInterlaceMode) {
      int64_t startTs = taosGetTimestampUs();
      while (atomic_load_64(&pStmt->sql.siInfo.tbRemainNum)) {
        taosUsleep(1);
      }
      pStmt->stat.execWaitUs += taosGetTimestampUs() - startTs;

      STMT_ERR_RET(qBuildStmtFinOutput(pStmt->sql.pQuery, pStmt->sql.pVgHash, pStmt->sql.siInfo.pVgroupList));
      taosHashCleanup(pStmt->sql.siInfo.pVgroupHash);
      pStmt->sql.siInfo.pVgroupHash = NULL;
      pStmt->sql.siInfo.pVgroupList = NULL;
    } else {
      tDestroySubmitTbData(pStmt->exec.pCurrTbData, TSDB_MSG_FLG_ENCODE);
      taosMemoryFreeClear(pStmt->exec.pCurrTbData);

      STMT_ERR_RET(qCloneCurrentTbData(pStmt->exec.pCurrBlock, &pStmt->exec.pCurrTbData));

      STMT_ERR_RET(qBuildStmtOutput(pStmt->sql.pQuery, pStmt->sql.pVgHash, pStmt->exec.pBlockHash));
    }

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

  while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
    taosUsleep(1);
  }

  STMT_ERR_RET(stmtCleanExecInfo(pStmt, (code ? false : true), false));

  tFreeSSubmitRsp(pRsp);

  ++pStmt->sql.runTimes;

  int64_t startUs2 = taosGetTimestampUs();
  pStmt->stat.execUseUs += startUs2 - startUs;

  STMT_RET(code);
}

int stmtClose(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_DLOG_E("start to free stmt");

  pStmt->queue.stopQueue = true;

  if (pStmt->bindThreadInUse) {
    (void)taosThreadJoin(pStmt->bindThread, NULL);
    pStmt->bindThreadInUse = false;
  }

  STMT_DLOG("stmt %p closed, stbInterlaceMode: %d, statInfo: ctgGetTbMetaNum=>%" PRId64 ", getCacheTbInfo=>%" PRId64
            ", parseSqlNum=>%" PRId64 ", pStmt->stat.bindDataNum=>%" PRId64
            ", settbnameAPI:%u, bindAPI:%u, addbatchAPI:%u, execAPI:%u"
            ", setTbNameUs:%" PRId64 ", bindDataUs:%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 " addBatchUs:%" PRId64
            ", execWaitUs:%" PRId64 ", execUseUs:%" PRId64,
            pStmt, pStmt->sql.stbInterlaceMode, pStmt->stat.ctgGetTbMetaNum, pStmt->stat.getCacheTbInfo,
            pStmt->stat.parseSqlNum, pStmt->stat.bindDataNum, pStmt->seqIds[STMT_SETTBNAME], pStmt->seqIds[STMT_BIND],
            pStmt->seqIds[STMT_ADD_BATCH], pStmt->seqIds[STMT_EXECUTE], pStmt->stat.setTbNameUs,
            pStmt->stat.bindDataUs1, pStmt->stat.bindDataUs2, pStmt->stat.bindDataUs3, pStmt->stat.bindDataUs4,
            pStmt->stat.addBatchUs, pStmt->stat.execWaitUs, pStmt->stat.execUseUs);

  STMT_ERR_RET(stmtCleanSQLInfo(pStmt));
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
  int32_t   code = 0;
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t   preCode = pStmt->errCode;

  STMT_DLOG_E("start to get tag fields");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

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
  int32_t   code = 0;
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t   preCode = pStmt->errCode;

  STMT_DLOG_E("start to get col fields");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

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
    STMT_ERR_RET(stmtCreateRequest(pStmt));
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

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
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

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

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
