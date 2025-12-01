#include "clientInt.h"
#include "clientLog.h"
#include "tdef.h"

#include "clientStmt.h"
#include "clientStmt2.h"

char* gStmt2StatusStr[] = {"unknown",     "init", "prepare", "settbname", "settags",
                           "fetchFields", "bind", "bindCol", "addBatch",  "exec"};

static FORCE_INLINE int32_t stmtAllocQNodeFromBuf(STableBufInfo* pTblBuf, void** pBuf) {
  if (pTblBuf->buffOffset < pTblBuf->buffSize) {
    *pBuf = (char*)pTblBuf->pCurBuff + pTblBuf->buffOffset;
    pTblBuf->buffOffset += pTblBuf->buffUnit;
  } else if (pTblBuf->buffIdx < taosArrayGetSize(pTblBuf->pBufList)) {
    pTblBuf->pCurBuff = taosArrayGetP(pTblBuf->pBufList, pTblBuf->buffIdx++);
    if (NULL == pTblBuf->pCurBuff) {
      return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
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

static bool stmtDequeue(STscStmt2* pStmt, SStmtQNode** param) {
  int i = 0;
  while (0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
    if (pStmt->queue.stopQueue) {
      return false;
    }
    if (i < 10) {
      taosUsleep(1);
      i++;
    } else {
      (void)taosThreadMutexLock(&pStmt->queue.mutex);
      if (pStmt->queue.stopQueue) {
        (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
        return false;
      }
      if (0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
        (void)taosThreadCondWait(&pStmt->queue.waitCond, &pStmt->queue.mutex);
      }
      (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
    }
  }

  if (pStmt->queue.stopQueue && 0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
    return false;
  }

  (void)taosThreadMutexLock(&pStmt->queue.mutex);
  if (pStmt->queue.head == pStmt->queue.tail) {
    pStmt->queue.qRemainNum = 0;
    (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
    STMT2_ELOG_E("interlace queue is empty, cannot dequeue");
    return false;
  }

  SStmtQNode* node = pStmt->queue.head->next;
  pStmt->queue.head->next = node->next;
  if (pStmt->queue.tail == node) {
    pStmt->queue.tail = pStmt->queue.head;
  }
  node->next = NULL;
  *param = node;

  (void)atomic_sub_fetch_64((int64_t*)&pStmt->queue.qRemainNum, 1);
  (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

  STMT2_TLOG("dequeue success, node:%p, remainNum:%" PRId64, node, pStmt->queue.qRemainNum);

  return true;
}

static void stmtEnqueue(STscStmt2* pStmt, SStmtQNode* param) {
  if (param == NULL) {
    STMT2_ELOG_E("enqueue param is NULL");
    return;
  }

  param->next = NULL;

  (void)taosThreadMutexLock(&pStmt->queue.mutex);

  pStmt->queue.tail->next = param;
  pStmt->queue.tail = param;
  pStmt->stat.bindDataNum++;

  (void)atomic_add_fetch_64(&pStmt->queue.qRemainNum, 1);
  (void)taosThreadCondSignal(&(pStmt->queue.waitCond));

  (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

  STMT2_TLOG("enqueue param:%p, remainNum:%" PRId64 ", restoreTbCols:%d", param, pStmt->queue.qRemainNum,
             param->restoreTbCols);
}

static int32_t stmtCreateRequest(STscStmt2* pStmt) {
  int32_t code = 0;

  if (pStmt->exec.pRequest == NULL) {
    code = buildRequest(pStmt->taos->id, pStmt->sql.sqlStr, pStmt->sql.sqlLen, NULL, false, &pStmt->exec.pRequest,
                        pStmt->reqid);
    if (pStmt->reqid != 0) {
      pStmt->reqid++;
    }
    pStmt->exec.pRequest->type = RES_TYPE__QUERY;
    if (pStmt->db != NULL) {
      taosMemoryFreeClear(pStmt->exec.pRequest->pDb);
      pStmt->exec.pRequest->pDb = taosStrdup(pStmt->db);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pStmt->exec.pRequest->syncQuery = true;
      pStmt->exec.pRequest->stmtBindVersion = 2;
    }
    STMT2_DLOG("create request:0x%" PRIx64 ", QID:0x%" PRIx64, pStmt->exec.pRequest->self,
               pStmt->exec.pRequest->requestId);
  }

  return code;
}

static int32_t stmtSwitchStatus(STscStmt2* pStmt, STMT_STATUS newStatus) {
  int32_t code = 0;

  if (newStatus >= STMT_INIT && newStatus < STMT_MAX) {
    STMT2_LOG_SEQ(newStatus);
  }

  if (pStmt->errCode && newStatus != STMT_PREPARE) {
    STMT2_ELOG("stmt already failed with err:%s, please use stmt prepare", tstrerror(pStmt->errCode));
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
      if (STMT_STATUS_EQ(INIT)) {
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

static int32_t stmtGetTbName(TAOS_STMT2* stmt, char** tbName) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  pStmt->sql.type = STMT_TYPE_MULTI_INSERT;

  if ('\0' == pStmt->bInfo.tbName[0]) {
    tscWarn("no table name set, OK if it is a stmt get fields");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  *tbName = pStmt->bInfo.tbName;

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtUpdateBindInfo(TAOS_STMT2* stmt, STableMeta* pTableMeta, void* tags, SArray* cols, SName* tbName,
                                  const char* sTableName, bool autoCreateTbl, int8_t tbNameFlag) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  char       tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t    code = tNameExtractFullName(tbName, tbFName);
  if (code != 0) {
    return code;
  }

  if ((tags != NULL && ((SBoundColInfo*)tags)->numOfCols == 0) || !autoCreateTbl) {
    pStmt->sql.autoCreateTbl = false;
  }

  (void)memcpy(&pStmt->bInfo.sname, tbName, sizeof(*tbName));
  tstrncpy(pStmt->bInfo.tbFName, tbFName, sizeof(pStmt->bInfo.tbFName));
  pStmt->bInfo.tbFName[sizeof(pStmt->bInfo.tbFName) - 1] = 0;

  pStmt->bInfo.tbUid = autoCreateTbl ? 0 : pTableMeta->uid;
  pStmt->bInfo.tbSuid = pTableMeta->suid;
  pStmt->bInfo.tbVgId = pTableMeta->vgId;
  pStmt->bInfo.tbType = pTableMeta->tableType;

  if (!pStmt->bInfo.tagsCached) {
    qDestroyBoundColInfo(pStmt->bInfo.boundTags);
    taosMemoryFreeClear(pStmt->bInfo.boundTags);
  }

  if (cols) {
    pStmt->bInfo.boundCols =
        tSimpleHashInit(taosArrayGetSize(cols), taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
    if (pStmt->bInfo.boundCols) {
      for (int32_t i = 0; i < taosArrayGetSize(cols); i++) {
        SColVal* pColVal = taosArrayGet(cols, i);
        if (pColVal) {
          code = tSimpleHashPut(pStmt->bInfo.boundCols, &pColVal->cid, sizeof(int16_t), pColVal, sizeof(SColVal));
          if (code != 0) {
            return code;
          }
        }
      }
    }
  } else {
    pStmt->bInfo.boundCols = NULL;
  }
  pStmt->bInfo.boundTags = tags;
  pStmt->bInfo.tagsCached = false;
  pStmt->bInfo.tbNameFlag = tbNameFlag;
  tstrncpy(pStmt->bInfo.stbFName, sTableName, sizeof(pStmt->bInfo.stbFName));

  if (pTableMeta->tableType != TSDB_CHILD_TABLE && pTableMeta->tableType != TSDB_SUPER_TABLE) {
    pStmt->sql.stbInterlaceMode = false;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtUpdateExecInfo(TAOS_STMT2* stmt, SHashObj* pVgHash, SHashObj* pBlockHash) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  pStmt->sql.pVgHash = pVgHash;
  pStmt->exec.pBlockHash = pBlockHash;

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtUpdateInfo(TAOS_STMT2* stmt, STableMeta* pTableMeta, void* tags, SArray* cols, SName* tbName,
                              bool autoCreateTbl, SHashObj* pVgHash, SHashObj* pBlockHash, const char* sTableName,
                              uint8_t tbNameFlag) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  STMT_ERR_RET(stmtUpdateBindInfo(stmt, pTableMeta, tags, cols, tbName, sTableName, autoCreateTbl, tbNameFlag));
  STMT_ERR_RET(stmtUpdateExecInfo(stmt, pVgHash, pBlockHash));

  pStmt->sql.autoCreateTbl = autoCreateTbl;

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtGetExecInfo(TAOS_STMT2* stmt, SHashObj** pVgHash, SHashObj** pBlockHash) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  *pVgHash = pStmt->sql.pVgHash;
  pStmt->sql.pVgHash = NULL;

  *pBlockHash = pStmt->exec.pBlockHash;
  pStmt->exec.pBlockHash = NULL;

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtParseSql(STscStmt2* pStmt) {
  pStmt->exec.pCurrBlock = NULL;

  SStmtCallback stmtCb = {
      .pStmt = pStmt,
      .getTbNameFn = stmtGetTbName,
      .setInfoFn = stmtUpdateInfo,
      .getExecInfoFn = stmtGetExecInfo,
  };

  STMT_ERR_RET(stmtCreateRequest(pStmt));
  pStmt->exec.pRequest->stmtBindVersion = 2;

  pStmt->stat.parseSqlNum++;

  STMT2_DLOG("start to parse, QID:0x%" PRIx64, pStmt->exec.pRequest->requestId);
  STMT_ERR_RET(parseSql(pStmt->exec.pRequest, false, &pStmt->sql.pQuery, &stmtCb));

  pStmt->sql.siInfo.pQuery = pStmt->sql.pQuery;

  pStmt->bInfo.needParse = false;

  if (pStmt->sql.type == 0) {
    if (pStmt->sql.pQuery->pRoot && LEGAL_INSERT(nodeType(pStmt->sql.pQuery->pRoot))) {
      pStmt->sql.type = STMT_TYPE_INSERT;
      pStmt->sql.stbInterlaceMode = false;
    } else if (pStmt->sql.pQuery->pPrepareRoot && LEGAL_SELECT(nodeType(pStmt->sql.pQuery->pPrepareRoot))) {
      pStmt->sql.type = STMT_TYPE_QUERY;
      pStmt->sql.stbInterlaceMode = false;

      return TSDB_CODE_SUCCESS;
    } else {
      STMT2_ELOG_E("only support select or insert sql");
      if (pStmt->exec.pRequest->msgBuf) {
        tstrncpy(pStmt->exec.pRequest->msgBuf, "stmt only support select or insert", pStmt->exec.pRequest->msgBufLen);
      }
      return TSDB_CODE_PAR_SYNTAX_ERROR;
    }
  } else if (pStmt->sql.type == STMT_TYPE_QUERY) {
    pStmt->sql.stbInterlaceMode = false;
    return TSDB_CODE_SUCCESS;
  } else if (pStmt->sql.type == STMT_TYPE_INSERT) {
    pStmt->sql.stbInterlaceMode = false;
  }

  STableDataCxt** pSrc =
      (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (NULL == pSrc || NULL == *pSrc) {
    STMT2_ELOG("fail to get exec.pBlockHash, maybe parse failed, tbFName:%s", pStmt->bInfo.tbFName);
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_CACHE_ERROR);
  }

  STableDataCxt* pTableCtx = *pSrc;
  if (pStmt->sql.stbInterlaceMode && pTableCtx->pData->pCreateTbReq && (pStmt->bInfo.tbNameFlag & USING_CLAUSE) == 0) {
    STMT2_TLOG("destroy pCreateTbReq for no-using insert, tbFName:%s", pStmt->bInfo.tbFName);
    tdDestroySVCreateTbReq(pTableCtx->pData->pCreateTbReq);
    taosMemoryFreeClear(pTableCtx->pData->pCreateTbReq);
    pTableCtx->pData->pCreateTbReq = NULL;
  }
  // if (pStmt->sql.stbInterlaceMode) {
  //   int16_t lastIdx = -1;

  //   for (int32_t i = 0; i < pTableCtx->boundColsInfo.numOfBound; ++i) {
  //     if (pTableCtx->boundColsInfo.pColIndex[i] < lastIdx) {
  //       pStmt->sql.stbInterlaceMode = false;
  //       break;
  //     }

  //     lastIdx = pTableCtx->boundColsInfo.pColIndex[i];
  //   }
  // }

  if (NULL == pStmt->sql.pBindInfo) {
    pStmt->sql.pBindInfo = taosMemoryMalloc(pTableCtx->boundColsInfo.numOfBound * sizeof(*pStmt->sql.pBindInfo));
    if (NULL == pStmt->sql.pBindInfo) {
      STMT2_ELOG_E("fail to malloc pBindInfo");
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void resetRequest(STscStmt2* pStmt) {
  if (pStmt->exec.pRequest) {
    taos_free_result(pStmt->exec.pRequest);
    pStmt->exec.pRequest = NULL;
  }
  pStmt->asyncResultAvailable = false;
}

static int32_t stmtCleanBindInfo(STscStmt2* pStmt) {
  pStmt->bInfo.tbUid = 0;
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

  if (!pStmt->bInfo.boundColsCached) {
    tSimpleHashCleanup(pStmt->bInfo.boundCols);
    pStmt->bInfo.boundCols = NULL;
  }

  if (!pStmt->sql.autoCreateTbl) {
    pStmt->bInfo.stbFName[0] = 0;
    pStmt->bInfo.tbSuid = 0;
  }

  STMT2_TLOG("finish clean bind info, tagsCached:%d, autoCreateTbl:%d", pStmt->bInfo.tagsCached,
             pStmt->sql.autoCreateTbl);

  return TSDB_CODE_SUCCESS;
}

static void stmtFreeTableBlkList(STableColsData* pTb) {
  (void)qResetStmtColumns(pTb->aCol, true);
  taosArrayDestroy(pTb->aCol);
}

static void stmtResetQueueTableBuf(STableBufInfo* pTblBuf, SStmtQueue* pQueue) {
  pTblBuf->pCurBuff = taosArrayGetP(pTblBuf->pBufList, 0);
  if (NULL == pTblBuf->pCurBuff) {
    tscError("QInfo:%p, fail to get buffer from list", pTblBuf);
    return;
  }
  pTblBuf->buffIdx = 1;
  pTblBuf->buffOffset = sizeof(*pQueue->head);

  pQueue->head = pQueue->tail = pTblBuf->pCurBuff;
  pQueue->qRemainNum = 0;
  pQueue->head->next = NULL;
}

static int32_t stmtCleanExecInfo(STscStmt2* pStmt, bool keepTable, bool deepClean) {
  if (pStmt->sql.stbInterlaceMode) {
    if (deepClean) {
      taosHashCleanup(pStmt->exec.pBlockHash);
      pStmt->exec.pBlockHash = NULL;

      if (NULL != pStmt->exec.pCurrBlock) {
        taosMemoryFreeClear(pStmt->exec.pCurrBlock->boundColsInfo.pColIndex);
        taosMemoryFreeClear(pStmt->exec.pCurrBlock->pData);
        qDestroyStmtDataBlock(pStmt->exec.pCurrBlock);
        pStmt->exec.pCurrBlock = NULL;
      }
      if (STMT_TYPE_QUERY != pStmt->sql.type) {
        resetRequest(pStmt);
      }
    } else {
      pStmt->sql.siInfo.pTableColsIdx = 0;
      stmtResetQueueTableBuf(&pStmt->sql.siInfo.tbBuf, &pStmt->queue);
      tSimpleHashClear(pStmt->sql.siInfo.pTableRowDataHash);
    }
    if (NULL != pStmt->exec.pRequest) {
      pStmt->exec.pRequest->body.resInfo.numOfRows = 0;
    }
  } else {
    if (STMT_TYPE_QUERY != pStmt->sql.type || deepClean) {
      resetRequest(pStmt);
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
      STMT2_TLOG("finish clean exec info, stbInterlaceMode:%d, keepTable:%d, deepClean:%d", pStmt->sql.stbInterlaceMode,
                 keepTable, deepClean);
      return TSDB_CODE_SUCCESS;
    }

    taosHashCleanup(pStmt->exec.pBlockHash);
    pStmt->exec.pBlockHash = NULL;

    tDestroySubmitTbData(pStmt->exec.pCurrTbData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFreeClear(pStmt->exec.pCurrTbData);
  }

  STMT_ERR_RET(stmtCleanBindInfo(pStmt));
  STMT2_TLOG("finish clean exec info, stbInterlaceMode:%d, keepTable:%d, deepClean:%d", pStmt->sql.stbInterlaceMode,
             keepTable, deepClean);

  return TSDB_CODE_SUCCESS;
}

static void stmtFreeTbBuf(void* buf) {
  void* pBuf = *(void**)buf;
  taosMemoryFree(pBuf);
}

static void stmtFreeTbCols(void* buf) {
  SArray* pCols = *(SArray**)buf;
  taosArrayDestroy(pCols);
}

static int32_t stmtCleanSQLInfo(STscStmt2* pStmt) {
  STMT2_TLOG_E("start to free SQL info");

  taosMemoryFree(pStmt->sql.pBindInfo);
  taosMemoryFree(pStmt->sql.queryRes.fields);
  taosMemoryFree(pStmt->sql.queryRes.userFields);
  taosMemoryFree(pStmt->sql.sqlStr);
  qDestroyQuery(pStmt->sql.pQuery);
  taosArrayDestroy(pStmt->sql.nodeList);
  taosHashCleanup(pStmt->sql.pVgHash);
  pStmt->sql.pVgHash = NULL;
  if (pStmt->sql.fixValueTags) {
    tdDestroySVCreateTbReq(pStmt->sql.fixValueTbReq);
  }

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
  tSimpleHashCleanup(pStmt->sql.siInfo.pTableRowDataHash);
  taosArrayDestroyEx(pStmt->sql.siInfo.tbBuf.pBufList, stmtFreeTbBuf);
  taosMemoryFree(pStmt->sql.siInfo.pTSchema);
  qDestroyStmtDataBlock(pStmt->sql.siInfo.pDataCtx);
  taosArrayDestroyEx(pStmt->sql.siInfo.pTableCols, stmtFreeTbCols);
  pStmt->sql.siInfo.pTableCols = NULL;

  (void)memset(&pStmt->sql, 0, sizeof(pStmt->sql));
  pStmt->sql.siInfo.tableColsReady = true;

  STMT2_TLOG_E("end to free SQL info");

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtTryAddTableVgroupInfo(STscStmt2* pStmt, int32_t* vgId) {
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
    STMT2_ELOG("fail to get vgroup info from catalog, code:%d", code);
    return code;
  }

  code =
      taosHashPut(pStmt->sql.pVgHash, (const char*)&vgInfo.vgId, sizeof(vgInfo.vgId), (char*)&vgInfo, sizeof(vgInfo));
  if (TSDB_CODE_SUCCESS != code) {
    STMT2_ELOG("fail to put vgroup info, code:%d", code);
    return code;
  }

  *vgId = vgInfo.vgId;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtGetTableMetaAndValidate(STscStmt2* pStmt, uint64_t* uid, uint64_t* suid, int32_t* vgId, int8_t* tableType) {
  STableMeta*      pTableMeta = NULL;
  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pStmt->exec.pRequest->requestId,
                           .requestObjRefId = pStmt->exec.pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};
  int32_t          code = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pTableMeta);

  pStmt->stat.ctgGetTbMetaNum++;

  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
    STMT2_ELOG("tb %s not exist", pStmt->bInfo.tbFName);
    (void)stmtCleanBindInfo(pStmt);

    if (!pStmt->sql.autoCreateTbl) {
      STMT2_ELOG("table %s does not exist and autoCreateTbl is disabled", pStmt->bInfo.tbFName);
      STMT_ERR_RET(TSDB_CODE_PAR_TABLE_NOT_EXIST);
    }

    STMT_ERR_RET(code);
  }

  STMT_ERR_RET(code);

  *uid = pTableMeta->uid;
  *suid = pTableMeta->suid;
  *tableType = pTableMeta->tableType;
  pStmt->bInfo.tbVgId = pTableMeta->vgId;
  *vgId = pTableMeta->vgId;

  taosMemoryFree(pTableMeta);

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtRebuildDataBlock(STscStmt2* pStmt, STableDataCxt* pDataBlock, STableDataCxt** newBlock, uint64_t uid,
                                    uint64_t suid, int32_t vgId) {
  STMT_ERR_RET(stmtTryAddTableVgroupInfo(pStmt, &vgId));
  STMT_ERR_RET(qRebuildStmtDataBlock(newBlock, pDataBlock, uid, suid, vgId, pStmt->sql.autoCreateTbl));

  STMT2_DLOG("uid:%" PRId64 ", rebuild table data context, vgId:%d", uid, vgId);

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtGetFromCache(STscStmt2* pStmt) {
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
      STMT2_DLOG("reuse stmt block for tb %s in execBlock", pStmt->bInfo.tbFName);
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
      STMT2_DLOG("reuse stmt block for tb %s in execBlock", pStmt->bInfo.tbFName);
      return TSDB_CODE_SUCCESS;
    }

    STMT2_DLOG("no stmt block cache for tb %s", pStmt->bInfo.tbFName);

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

      STMT2_DLOG("reuse stmt block for tb %s in sqlBlock, suid:0x%" PRIx64, pStmt->bInfo.tbFName, pStmt->bInfo.tbSuid);

      return TSDB_CODE_SUCCESS;
    }

    STMT_RET(stmtCleanBindInfo(pStmt));
  }

  uint64_t uid, suid;
  int32_t  vgId;
  int8_t   tableType;

  STMT_ERR_RET(stmtGetTableMetaAndValidate(pStmt, &uid, &suid, &vgId, &tableType));

  uint64_t cacheUid = (TSDB_CHILD_TABLE == tableType) ? suid : uid;

  if (uid == pStmt->bInfo.tbUid) {
    pStmt->bInfo.needParse = false;

    STMT2_DLOG("tb %s is current table", pStmt->bInfo.tbFName);

    return TSDB_CODE_SUCCESS;
  }

  if (pStmt->bInfo.inExecCache) {
    SStmtTableCache* pCache = taosHashGet(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid));
    if (NULL == pCache) {
      STMT2_ELOG("table [%s, %" PRIx64 ", %" PRIx64 "] found in exec blockHash, but not in sql blockHash",
                 pStmt->bInfo.tbFName, uid, cacheUid);

      STMT_ERR_RET(TSDB_CODE_APP_ERROR);
    }

    pStmt->bInfo.needParse = false;

    pStmt->bInfo.tbUid = uid;
    pStmt->bInfo.tbSuid = suid;
    pStmt->bInfo.tbType = tableType;
    pStmt->bInfo.boundTags = pCache->boundTags;
    pStmt->bInfo.tagsCached = true;

    STMT2_DLOG("tb %s in execBlock list, set to current", pStmt->bInfo.tbFName);

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

static int32_t stmtResetStmt(STscStmt2* pStmt) {
  STMT_ERR_RET(stmtCleanSQLInfo(pStmt));

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    STMT2_ELOG("fail to allocate memory for pTableCache in stmtResetStmt:%s", tstrerror(terrno));
    STMT_ERR_RET(terrno);
  }

  pStmt->sql.status = STMT_INIT;

  return TSDB_CODE_SUCCESS;
}

static void stmtAsyncOutput(STscStmt2* pStmt, void* param) {
  SStmtQNode* pParam = (SStmtQNode*)param;

  if (pParam->restoreTbCols) {
    for (int32_t i = 0; i < pStmt->sql.siInfo.pTableColsIdx; ++i) {
      SArray** p = (SArray**)TARRAY_GET_ELEM(pStmt->sql.siInfo.pTableCols, i);
      *p = taosArrayInit(20, POINTER_BYTES);
      if (*p == NULL) {
        pStmt->errCode = terrno;
      }
    }
    atomic_store_8((int8_t*)&pStmt->sql.siInfo.tableColsReady, true);
    STMT2_TLOG_E("restore pTableCols finished");
  } else {
    int code = qAppendStmt2TableOutput(pStmt->sql.pQuery, pStmt->sql.pVgHash, &pParam->tblData, pStmt->exec.pCurrBlock,
                                       &pStmt->sql.siInfo, pParam->pCreateTbReq);
    // taosMemoryFree(pParam->pTbData);
    if (code != TSDB_CODE_SUCCESS) {
      STMT2_ELOG("async append stmt output failed, tbname:%s, err:%s", pParam->tblData.tbName, tstrerror(code));
      pStmt->errCode = code;
    }
    (void)atomic_sub_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);
  }
}

static void* stmtBindThreadFunc(void* param) {
  setThreadName("stmt2Bind");

  STscStmt2* pStmt = (STscStmt2*)param;
  STMT2_ILOG_E("stmt2 bind thread started");

  while (true) {
    SStmtQNode* asyncParam = NULL;

    if (!stmtDequeue(pStmt, &asyncParam)) {
      if (pStmt->queue.stopQueue && 0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
        STMT2_DLOG_E("queue is empty and stopQueue is set, thread will exit");
        break;
      }
      continue;
    }

    stmtAsyncOutput(pStmt, asyncParam);
  }

  STMT2_ILOG_E("stmt2 bind thread stopped");
  return NULL;
}

static int32_t stmtStartBindThread(STscStmt2* pStmt) {
  TdThreadAttr thAttr;
  if (taosThreadAttrInit(&thAttr) != 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE) != 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  if (taosThreadCreate(&pStmt->bindThread, &thAttr, stmtBindThreadFunc, pStmt) != 0) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    STMT_ERR_RET(terrno);
  }

  pStmt->bindThreadInUse = true;

  (void)taosThreadAttrDestroy(&thAttr);
  return TSDB_CODE_SUCCESS;
}

static int32_t stmtInitQueue(STscStmt2* pStmt) {
  (void)taosThreadCondInit(&pStmt->queue.waitCond, NULL);
  (void)taosThreadMutexInit(&pStmt->queue.mutex, NULL);
  STMT_ERR_RET(stmtAllocQNodeFromBuf(&pStmt->sql.siInfo.tbBuf, (void**)&pStmt->queue.head));
  pStmt->queue.tail = pStmt->queue.head;

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtIniAsyncBind(STscStmt2* pStmt) {
  (void)taosThreadCondInit(&pStmt->asyncBindParam.waitCond, NULL);
  (void)taosThreadMutexInit(&pStmt->asyncBindParam.mutex, NULL);
  pStmt->asyncBindParam.asyncBindNum = 0;

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtInitTableBuf(STableBufInfo* pTblBuf) {
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

TAOS_STMT2* stmtInit2(STscObj* taos, TAOS_STMT2_OPTION* pOptions) {
  STscObj*   pObj = (STscObj*)taos;
  STscStmt2* pStmt = NULL;
  int32_t    code = 0;

  pStmt = taosMemoryCalloc(1, sizeof(STscStmt2));
  if (NULL == pStmt) {
    STMT2_ELOG_E("fail to allocate memory for pStmt");
    return NULL;
  }

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    STMT2_ELOG("fail to allocate memory for pTableCache in stmtInit2:%s", tstrerror(terrno));
    taosMemoryFree(pStmt);
    return NULL;
  }

  pStmt->taos = pObj;
  if (taos->db[0] != '\0') {
    pStmt->db = taosStrdup(taos->db);
  }
  pStmt->bInfo.needParse = true;
  pStmt->sql.status = STMT_INIT;
  pStmt->errCode = TSDB_CODE_SUCCESS;

  if (NULL != pOptions) {
    (void)memcpy(&pStmt->options, pOptions, sizeof(pStmt->options));
    if (pOptions->singleStbInsert && pOptions->singleTableBindOnce) {
      pStmt->stbInterlaceMode = true;
    }

    pStmt->reqid = pOptions->reqid;
  }

  if (pStmt->stbInterlaceMode) {
    pStmt->sql.siInfo.transport = taos->pAppInfo->pTransporter;
    pStmt->sql.siInfo.acctId = taos->acctId;
    pStmt->sql.siInfo.dbname = taos->db;
    pStmt->sql.siInfo.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);

    pStmt->sql.siInfo.pTableHash = tSimpleHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (NULL == pStmt->sql.siInfo.pTableHash) {
      STMT2_ELOG("fail to allocate memory for pTableHash:%s", tstrerror(terrno));
      (void)stmtClose2(pStmt);
      return NULL;
    }

    pStmt->sql.siInfo.pTableRowDataHash = tSimpleHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (NULL == pStmt->sql.siInfo.pTableRowDataHash) {
      STMT2_ELOG("fail to allocate memory for pTableRowDataHash:%s", tstrerror(terrno));
      (void)stmtClose2(pStmt);
      return NULL;
    }

    pStmt->sql.siInfo.pTableCols = taosArrayInit(STMT_TABLE_COLS_NUM, POINTER_BYTES);
    if (NULL == pStmt->sql.siInfo.pTableCols) {
      STMT2_ELOG("fail to allocate memory for pTableCols:%s", tstrerror(terrno));
      (void)stmtClose2(pStmt);
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
      STMT2_ELOG("fail to init stmt2 bind thread:%s", tstrerror(code));
      (void)stmtClose2(pStmt);
      return NULL;
    }
  }

  pStmt->sql.siInfo.tableColsReady = true;
  if (pStmt->options.asyncExecFn) {
    if (tsem_init(&pStmt->asyncExecSem, 0, 1) != 0) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      STMT2_ELOG("fail to init asyncExecSem:%s", tstrerror(terrno));
      (void)stmtClose2(pStmt);
      return NULL;
    }
  }
  code = stmtIniAsyncBind(pStmt);
  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
    STMT2_ELOG("fail to start init asyncExecSem:%s", tstrerror(code));

    (void)stmtClose2(pStmt);
    return NULL;
  }

  pStmt->execSemWaited = false;

  // STMT_LOG_SEQ(STMT_INIT);

  STMT2_DLOG("stmt2 initialize finished, seqId:%d, db:%s, interlaceMode:%d, asyncExec:%d", pStmt->seqId, pStmt->db,
             pStmt->stbInterlaceMode, pStmt->options.asyncExecFn != NULL);

  return pStmt;
}

static int stmtSetDbName2(TAOS_STMT2* stmt, const char* dbName) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  if (dbName == NULL || dbName[0] == '\0') {
    STMT2_ELOG_E("dbname in sql is illegal");
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  STMT2_DLOG("dbname is specified in sql:%s", dbName);
  if (pStmt->db == NULL || pStmt->db[0] == '\0') {
    taosMemoryFreeClear(pStmt->db);
    STMT2_DLOG("dbname:%s is by sql, not by taosconnect", dbName);
    pStmt->db = taosStrdup(dbName);
    (void)strdequote(pStmt->db);
  }
  STMT_ERR_RET(stmtCreateRequest(pStmt));

  // The SQL statement specifies a database name, overriding the previously specified database
  taosMemoryFreeClear(pStmt->exec.pRequest->pDb);
  pStmt->exec.pRequest->pDb = taosStrdup(dbName);
  (void)strdequote(pStmt->exec.pRequest->pDb);
  if (pStmt->exec.pRequest->pDb == NULL) {
    return terrno;
  }
  if (pStmt->sql.stbInterlaceMode) {
    pStmt->sql.siInfo.dbname = pStmt->exec.pRequest->pDb;
  }
  return TSDB_CODE_SUCCESS;
}
static int32_t stmtResetStbInterlaceCache(STscStmt2* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pStmt->bindThreadInUse) {
    while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
      taosUsleep(1);
    }
    (void)taosThreadMutexLock(&pStmt->queue.mutex);
    pStmt->queue.stopQueue = true;
    (void)taosThreadCondSignal(&(pStmt->queue.waitCond));
    (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

    (void)taosThreadJoin(pStmt->bindThread, NULL);
    pStmt->bindThreadInUse = false;
    pStmt->queue.head = NULL;
    pStmt->queue.tail = NULL;
    pStmt->queue.qRemainNum = 0;

    (void)taosThreadCondDestroy(&pStmt->queue.waitCond);
    (void)taosThreadMutexDestroy(&pStmt->queue.mutex);
  }

  pStmt->sql.siInfo.pTableHash = tSimpleHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pStmt->sql.siInfo.pTableHash) {
    return terrno;
  }

  pStmt->sql.siInfo.pTableRowDataHash = tSimpleHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pStmt->sql.siInfo.pTableRowDataHash) {
    return terrno;
  }

  pStmt->sql.siInfo.pTableCols = taosArrayInit(STMT_TABLE_COLS_NUM, POINTER_BYTES);
  if (NULL == pStmt->sql.siInfo.pTableCols) {
    return terrno;
  }

  code = stmtInitTableBuf(&pStmt->sql.siInfo.tbBuf);

  if (TSDB_CODE_SUCCESS == code) {
    code = stmtInitQueue(pStmt);
    pStmt->queue.stopQueue = false;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = stmtStartBindThread(pStmt);
  }
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t stmtDeepReset(STscStmt2* pStmt) {
  char*             db = pStmt->db;
  bool              stbInterlaceMode = pStmt->stbInterlaceMode;
  TAOS_STMT2_OPTION options = pStmt->options;
  uint32_t          reqid = pStmt->reqid;

  pStmt->errCode = 0;
  if (pStmt->options.asyncExecFn && !pStmt->execSemWaited) {
    if (tsem_wait(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("bind param wait asyncExecSem failed");
    }
    pStmt->execSemWaited = true;
  }
  pStmt->sql.autoCreateTbl = false;
  taosMemoryFree(pStmt->sql.pBindInfo);
  pStmt->sql.pBindInfo = NULL;

  taosMemoryFree(pStmt->sql.queryRes.fields);
  pStmt->sql.queryRes.fields = NULL;

  taosMemoryFree(pStmt->sql.queryRes.userFields);
  pStmt->sql.queryRes.userFields = NULL;

  pStmt->sql.type = 0;
  pStmt->sql.runTimes = 0;
  taosMemoryFree(pStmt->sql.sqlStr);
  pStmt->sql.sqlStr = NULL;

  qDestroyQuery(pStmt->sql.pQuery);
  pStmt->sql.pQuery = NULL;

  taosArrayDestroy(pStmt->sql.nodeList);
  pStmt->sql.nodeList = NULL;

  taosHashCleanup(pStmt->sql.pVgHash);
  pStmt->sql.pVgHash = NULL;

  if (pStmt->sql.fixValueTags) {
    tdDestroySVCreateTbReq(pStmt->sql.fixValueTbReq);
    pStmt->sql.fixValueTbReq = NULL;
  }
  pStmt->sql.fixValueTags = false;

  void* pIter = taosHashIterate(pStmt->sql.pTableCache, NULL);
  while (pIter) {
    SStmtTableCache* pCache = (SStmtTableCache*)pIter;

    qDestroyStmtDataBlock(pCache->pDataCtx);
    qDestroyBoundColInfo(pCache->boundTags);
    taosMemoryFreeClear(pCache->boundTags);

    pIter = taosHashIterate(pStmt->sql.pTableCache, pIter);
  }
  taosHashCleanup(pStmt->sql.pTableCache);

  if (pStmt->sql.stbInterlaceMode) {
    pStmt->bInfo.tagsCached = false;
  }
  STMT_ERR_RET(stmtCleanExecInfo(pStmt, false, true));

  resetRequest(pStmt);

  if (pStmt->sql.siInfo.pTableCols) {
    taosArrayDestroyEx(pStmt->sql.siInfo.pTableCols, stmtFreeTbCols);
    pStmt->sql.siInfo.pTableCols = NULL;
  }

  if (pStmt->sql.siInfo.tbBuf.pBufList) {
    taosArrayDestroyEx(pStmt->sql.siInfo.tbBuf.pBufList, stmtFreeTbBuf);
    pStmt->sql.siInfo.tbBuf.pBufList = NULL;
  }

  if (pStmt->sql.siInfo.pTableHash) {
    tSimpleHashCleanup(pStmt->sql.siInfo.pTableHash);
    pStmt->sql.siInfo.pTableHash = NULL;
  }

  if (pStmt->sql.siInfo.pTableRowDataHash) {
    tSimpleHashCleanup(pStmt->sql.siInfo.pTableRowDataHash);
    pStmt->sql.siInfo.pTableRowDataHash = NULL;
  }

  if (pStmt->sql.siInfo.pVgroupHash) {
    taosHashCleanup(pStmt->sql.siInfo.pVgroupHash);
    pStmt->sql.siInfo.pVgroupHash = NULL;
  }

  if (pStmt->sql.siInfo.pVgroupList) {
    taosArrayDestroy(pStmt->sql.siInfo.pVgroupList);
    pStmt->sql.siInfo.pVgroupList = NULL;
  }

  if (pStmt->sql.siInfo.pDataCtx) {
    qDestroyStmtDataBlock(pStmt->sql.siInfo.pDataCtx);
    pStmt->sql.siInfo.pDataCtx = NULL;
  }

  if (pStmt->sql.siInfo.pTSchema) {
    taosMemoryFree(pStmt->sql.siInfo.pTSchema);
    pStmt->sql.siInfo.pTSchema = NULL;
  }

  if (pStmt->sql.siInfo.pRequest) {
    taos_free_result(pStmt->sql.siInfo.pRequest);
    pStmt->sql.siInfo.pRequest = NULL;
  }

  if (stbInterlaceMode) {
    STMT_ERR_RET(stmtResetStbInterlaceCache(pStmt));
  }

  pStmt->db = db;
  pStmt->stbInterlaceMode = stbInterlaceMode;
  pStmt->options = options;
  pStmt->reqid = reqid;

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    STMT2_ELOG("fail to allocate memory for pTableCache in stmtResetStmt:%s", tstrerror(terrno));
    return terrno;
  }

  pStmt->sql.status = STMT_INIT;

  return TSDB_CODE_SUCCESS;
}

int stmtPrepare2(TAOS_STMT2* stmt, const char* sql, unsigned long length) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;

  STMT2_DLOG("start to prepare with sql:%s", sql);

  if (stmt == NULL || sql == NULL) {
    STMT2_ELOG_E("stmt or sql is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  if (pStmt->sql.status >= STMT_PREPARE) {
    STMT2_DLOG("stmt status is %d, need to reset stmt2 cache before prepare", pStmt->sql.status);
    STMT_ERR_RET(stmtDeepReset(pStmt));
  }

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    STMT2_ELOG("errCode is not success before, ErrCode: 0x%x, errorsyt: %s\n. ", pStmt->errCode,
               tstrerror(pStmt->errCode));
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_PREPARE));

  if (length <= 0) {
    length = strlen(sql);
  }
  pStmt->sql.sqlStr = taosStrndup(sql, length);
  if (!pStmt->sql.sqlStr) {
    STMT2_ELOG("fail to allocate memory for sqlStr:%s", tstrerror(terrno));
    STMT_ERR_RET(terrno);
  }
  pStmt->sql.sqlLen = length;
  STMT_ERR_RET(stmtCreateRequest(pStmt));

  if (stmt2IsInsert(pStmt)) {
    pStmt->sql.stbInterlaceMode = pStmt->stbInterlaceMode;
    char* dbName = NULL;
    if (qParseDbName(sql, length, &dbName)) {
      STMT_ERR_RET(stmtSetDbName2(stmt, dbName));
      taosMemoryFreeClear(dbName);
    } else if (pStmt->db != NULL && pStmt->db[0] != '\0') {
      taosMemoryFreeClear(pStmt->exec.pRequest->pDb);
      pStmt->exec.pRequest->pDb = taosStrdup(pStmt->db);
      if (pStmt->exec.pRequest->pDb == NULL) {
        STMT_ERR_RET(terrno);
      }
      (void)strdequote(pStmt->exec.pRequest->pDb);

      if (pStmt->sql.stbInterlaceMode) {
        pStmt->sql.siInfo.dbname = pStmt->exec.pRequest->pDb;
      }
    }

  } else if (stmt2IsSelect(pStmt)) {
    pStmt->sql.stbInterlaceMode = false;
    STMT_ERR_RET(stmtParseSql(pStmt));
  } else {
    return stmtBuildErrorMsgWithCode(pStmt, "stmt only support 'SELECT' or 'INSERT'", TSDB_CODE_PAR_SYNTAX_ERROR);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stmtInitStbInterlaceTableInfo(STscStmt2* pStmt) {
  STableDataCxt** pSrc = taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (!pSrc) {
    return terrno;
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

  STMT2_TLOG("init stb interlace table info, tbName:%s, pDataCtx:%p, boundTags:%p", pStmt->bInfo.tbFName,
             pStmt->sql.siInfo.pDataCtx, pStmt->sql.siInfo.boundTags);

  return TSDB_CODE_SUCCESS;
}

bool stmt2IsInsert(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  if (pStmt->sql.type) {
    return (STMT_TYPE_INSERT == pStmt->sql.type || STMT_TYPE_MULTI_INSERT == pStmt->sql.type);
  }

  return qIsInsertValuesSql(pStmt->sql.sqlStr, pStmt->sql.sqlLen);
}

bool stmt2IsSelect(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  if (pStmt->sql.type) {
    return STMT_TYPE_QUERY == pStmt->sql.type;
  }
  return qIsSelectFromSql(pStmt->sql.sqlStr, pStmt->sql.sqlLen);
}

int stmtSetTbName2(TAOS_STMT2* stmt, const char* tbName) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  int64_t startUs = taosGetTimestampUs();

  STMT2_TLOG("start to set tbName:%s", tbName);

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTBNAME));

  int32_t insert = 0;
  if (!stmt2IsInsert(stmt)) {
    STMT2_ELOG_E("set tb name not available for no-insert statement");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (!pStmt->sql.stbInterlaceMode || NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtCreateRequest(pStmt));

    STMT_ERR_RET(qCreateSName2(&pStmt->bInfo.sname, tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb,
                               pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
    STMT_ERR_RET(tNameExtractFullName(&pStmt->bInfo.sname, pStmt->bInfo.tbFName));

    STMT_ERR_RET(stmtGetFromCache(pStmt));

    if (pStmt->bInfo.needParse) {
      tstrncpy(pStmt->bInfo.tbName, tbName, sizeof(pStmt->bInfo.tbName));
      pStmt->bInfo.tbName[sizeof(pStmt->bInfo.tbName) - 1] = 0;

      STMT_ERR_RET(stmtParseSql(pStmt));
      if (!pStmt->sql.autoCreateTbl) {
        uint64_t uid, suid;
        int32_t  vgId;
        int8_t   tableType;

        int32_t code = stmtGetTableMetaAndValidate(pStmt, &uid, &suid, &vgId, &tableType);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }

  } else {
    tstrncpy(pStmt->bInfo.tbName, tbName, sizeof(pStmt->bInfo.tbName));
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

int stmtSetTbTags2(TAOS_STMT2* stmt, TAOS_STMT2_BIND* tags, SVCreateTbReq** pCreateTbReq) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  STMT2_TLOG_E("start to set tbTags");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTAGS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }
  STMT_ERR_RET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
  }
  if (pStmt->sql.stbInterlaceMode && NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtInitStbInterlaceTableInfo(pStmt));
  }

  SBoundColInfo* tags_info = (SBoundColInfo*)pStmt->bInfo.boundTags;
  // if (tags_info->numOfBound <= 0 || tags_info->numOfCols <= 0) {
  //   tscWarn("no tags or cols bound in sql, will not bound tags");
  //   return TSDB_CODE_SUCCESS;
  // }
  if (pStmt->sql.autoCreateTbl && pStmt->sql.stbInterlaceMode) {
    STMT_ERR_RET(qCreateSName2(&pStmt->bInfo.sname, pStmt->bInfo.tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb,
                               pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
    STMT_ERR_RET(tNameExtractFullName(&pStmt->bInfo.sname, pStmt->bInfo.tbFName));
  }

  STableDataCxt** pDataBlock = NULL;
  if (pStmt->exec.pCurrBlock) {
    pDataBlock = &pStmt->exec.pCurrBlock;
  } else {
    pDataBlock =
        (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
    if (NULL == pDataBlock) {
      STMT2_ELOG("table %s not found in exec blockHash:%p", pStmt->bInfo.tbFName, pStmt->exec.pBlockHash);
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_CACHE_ERROR);
    }
  }
  if (pStmt->bInfo.inExecCache && !pStmt->sql.autoCreateTbl) {
    return TSDB_CODE_SUCCESS;
  }

  STMT2_TLOG_E("start to bind stmt tag values");

  void* boundTags = NULL;
  if (pStmt->sql.stbInterlaceMode) {
    boundTags = pStmt->sql.siInfo.boundTags;
    *pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (NULL == pCreateTbReq) {
      return terrno;
    }
    int32_t vgId = -1;
    STMT_ERR_RET(stmtTryAddTableVgroupInfo(pStmt, &vgId));
    (*pCreateTbReq)->uid = vgId;
  } else {
    boundTags = pStmt->bInfo.boundTags;
  }

  STMT_ERR_RET(qBindStmtTagsValue2(*pDataBlock, boundTags, pStmt->bInfo.tbSuid, pStmt->bInfo.stbFName,
                                   pStmt->bInfo.sname.tname, tags, pStmt->exec.pRequest->msgBuf,
                                   pStmt->exec.pRequest->msgBufLen, pStmt->taos->optionInfo.charsetCxt, *pCreateTbReq));

  return TSDB_CODE_SUCCESS;
}

int stmtCheckTags2(TAOS_STMT2* stmt, SVCreateTbReq** pCreateTbReq) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  STMT2_TLOG_E("start to clone createTbRequest for fixed tags");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  if (!pStmt->sql.stbInterlaceMode) {
    return TSDB_CODE_SUCCESS;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_SETTAGS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }
  STMT_ERR_RET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERR_RET(stmtParseSql(pStmt));
    if (!pStmt->sql.autoCreateTbl) {
      STMT2_WLOG_E("don't need to create table, will not check tags");
      return TSDB_CODE_SUCCESS;
    }
  }

  if (pStmt->sql.stbInterlaceMode && NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtInitStbInterlaceTableInfo(pStmt));
  }

  STMT_ERR_RET(qCreateSName(&pStmt->bInfo.sname, pStmt->bInfo.tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb,
                            pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
  STMT_ERR_RET(tNameExtractFullName(&pStmt->bInfo.sname, pStmt->bInfo.tbFName));

  STableDataCxt** pDataBlock = NULL;
  if (pStmt->exec.pCurrBlock) {
    pDataBlock = &pStmt->exec.pCurrBlock;
  } else {
    pDataBlock =
        (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
    if (NULL == pDataBlock) {
      STMT2_ELOG("table %s not found in exec blockHash:%p", pStmt->bInfo.tbFName, pStmt->exec.pBlockHash);
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_CACHE_ERROR);
    }
  }

  if (!((*pDataBlock)->pData->flags & SUBMIT_REQ_AUTO_CREATE_TABLE)) {
    STMT2_DLOG_E("don't need to create, will not check tags");
    return TSDB_CODE_SUCCESS;
  }

  if (pStmt->sql.fixValueTags) {
    STMT2_TLOG_E("tags are fixed, use one createTbReq");
    STMT_ERR_RET(cloneSVreateTbReq(pStmt->sql.fixValueTbReq, pCreateTbReq));
    if ((*pCreateTbReq)->name) {
      taosMemoryFree((*pCreateTbReq)->name);
    }
    (*pCreateTbReq)->name = taosStrdup(pStmt->bInfo.tbName);
    int32_t vgId = -1;
    STMT_ERR_RET(stmtTryAddTableVgroupInfo(pStmt, &vgId));
    (*pCreateTbReq)->uid = vgId;
    return TSDB_CODE_SUCCESS;
  }

  if ((*pDataBlock)->pData->pCreateTbReq) {
    STMT2_TLOG_E("tags are fixed, set createTbReq first time");
    pStmt->sql.fixValueTags = true;
    STMT_ERR_RET(cloneSVreateTbReq((*pDataBlock)->pData->pCreateTbReq, &pStmt->sql.fixValueTbReq));
    STMT_ERR_RET(cloneSVreateTbReq(pStmt->sql.fixValueTbReq, pCreateTbReq));
    (*pCreateTbReq)->uid = (*pDataBlock)->pMeta->vgId;
  }

  return TSDB_CODE_SUCCESS;
}

static int stmtFetchColFields2(STscStmt2* pStmt, int32_t* fieldNum, TAOS_FIELD_E** fields) {
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
      STMT2_ELOG("table %s not found in exec blockHash:%p", pStmt->bInfo.tbFName, pStmt->exec.pBlockHash);
      STMT_ERR_RET(TSDB_CODE_APP_ERROR);
    }
  }

  STMT_ERR_RET(qBuildStmtColFields(*pDataBlock, fieldNum, fields));

  return TSDB_CODE_SUCCESS;
}

static int stmtFetchStbColFields2(STscStmt2* pStmt, int32_t* fieldNum, TAOS_FIELD_ALL** fields) {
  int32_t code = 0;
  int32_t preCode = pStmt->errCode;

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    STMT2_ELOG_E("stmtFetchStbColFields2 only for insert statement");
    STMT_ERRI_JRET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STableDataCxt** pDataBlock = NULL;
  bool            cleanStb = false;

  if (pStmt->sql.stbInterlaceMode && pStmt->sql.siInfo.pDataCtx != NULL) {
    pDataBlock = &pStmt->sql.siInfo.pDataCtx;
  } else {
    cleanStb = true;
    pDataBlock =
        (STableDataCxt**)taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  }

  if (NULL == pDataBlock || NULL == *pDataBlock) {
    STMT2_ELOG("table %s not found in exec blockHash:%p", pStmt->bInfo.tbFName, pStmt->exec.pBlockHash);
    STMT_ERRI_JRET(TSDB_CODE_APP_ERROR);
  }

  STMT_ERRI_JRET(qBuildStmtStbColFields(*pDataBlock, pStmt->bInfo.boundTags, pStmt->bInfo.boundCols,
                                        pStmt->bInfo.tbNameFlag, fieldNum, fields));

  if (pStmt->bInfo.tbType == TSDB_SUPER_TABLE && cleanStb) {
    taosMemoryFreeClear((*pDataBlock)->boundColsInfo.pColIndex);
    qDestroyStmtDataBlock(*pDataBlock);
    *pDataBlock = NULL;
    if (taosHashRemove(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName)) != 0) {
      STMT2_ELOG("fail to remove remove stb:%s exec blockHash", pStmt->bInfo.tbFName);
      STMT_ERRI_JRET(TSDB_CODE_APP_ERROR);
    }
    pStmt->sql.autoCreateTbl = false;
    pStmt->bInfo.tagsCached = false;
    pStmt->bInfo.sname = (SName){0};
    STMT_ERR_RET(stmtCleanBindInfo(pStmt));
  }

_return:

  pStmt->errCode = preCode;

  return code;
}
/*
SArray* stmtGetFreeCol(STscStmt2* pStmt, int32_t* idx) {
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
static int32_t stmtAppendTablePostHandle(STscStmt2* pStmt, SStmtQNode* param) {
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
    tstrncpy(pStmt->sql.siInfo.firstName, pStmt->bInfo.tbName, TSDB_TABLE_NAME_LEN);
  }

  param->tblData.getFromHash = pStmt->sql.siInfo.tbFromHash;
  param->next = NULL;

  (void)atomic_add_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);

  if (pStmt->queue.stopQueue) {
    STMT2_ELOG_E("bind thread already stopped, cannot enqueue");
    return TSDB_CODE_TSC_STMT_API_ERROR;
  }
  stmtEnqueue(pStmt, param);

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t stmtGetTableColsFromCache(STscStmt2* pStmt, SArray** pTableCols) {
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

static int32_t stmtCacheBlock(STscStmt2* pStmt) {
  if (pStmt->sql.type != STMT_TYPE_MULTI_INSERT) {
    return TSDB_CODE_SUCCESS;
  }

  uint64_t uid = pStmt->bInfo.tbUid;
  uint64_t cacheUid = (TSDB_CHILD_TABLE == pStmt->bInfo.tbType) ? pStmt->bInfo.tbSuid : uid;

  if (taosHashGet(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid))) {
    STMT2_TLOG("table %s already cached, no need to cache again", pStmt->bInfo.tbFName);
    return TSDB_CODE_SUCCESS;
  }

  STableDataCxt** pSrc = taosHashGet(pStmt->exec.pBlockHash, pStmt->bInfo.tbFName, strlen(pStmt->bInfo.tbFName));
  if (!pSrc) {
    STMT2_ELOG("table %s not found in exec blockHash:%p", pStmt->bInfo.tbFName, pStmt->exec.pBlockHash);
    return terrno;
  }
  STableDataCxt* pDst = NULL;

  STMT_ERR_RET(qCloneStmtDataBlock(&pDst, *pSrc, true));

  SStmtTableCache cache = {
      .pDataCtx = pDst,
      .boundTags = pStmt->bInfo.boundTags,
  };

  if (taosHashPut(pStmt->sql.pTableCache, &cacheUid, sizeof(cacheUid), &cache, sizeof(cache))) {
    STMT2_ELOG("fail to put table cache:%s", tstrerror(terrno));
    return terrno;
  }

  if (pStmt->sql.autoCreateTbl) {
    pStmt->bInfo.tagsCached = true;
  } else {
    pStmt->bInfo.boundTags = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

static int stmtAddBatch2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  int64_t startUs = taosGetTimestampUs();

  // STMT2_TLOG_E("start to add batch");

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

    if (pStmt->sql.autoCreateTbl) {
      pStmt->bInfo.tagsCached = true;
    }
    pStmt->bInfo.boundColsCached = true;

    if (pStmt->queue.stopQueue) {
      STMT2_ELOG_E("stmt bind thread is stopped,cannot enqueue bind request");
      return TSDB_CODE_TSC_STMT_API_ERROR;
    }

    stmtEnqueue(pStmt, param);

    return TSDB_CODE_SUCCESS;
  }

  STMT_ERR_RET(stmtCacheBlock(pStmt));

  return TSDB_CODE_SUCCESS;
}
/*
static int32_t stmtBackupQueryFields(STscStmt2* pStmt) {
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

static int32_t stmtRestoreQueryFields(STscStmt2* pStmt) {
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
*/

int stmtBindBatch2(TAOS_STMT2* stmt, TAOS_STMT2_BIND* bind, int32_t colIdx, SVCreateTbReq* pCreateTbReq) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;

  int64_t startUs = taosGetTimestampUs();

  STMT2_TLOG("start to bind data, colIdx:%d", colIdx);

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_BIND));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    resetRequest(pStmt);
  }

  STMT_ERR_RET(stmtCreateRequest(pStmt));
  if (pStmt->bInfo.needParse) {
    code = stmtParseSql(pStmt);
    if (code != TSDB_CODE_SUCCESS) {
      goto cleanup_root;
    }
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    code = qStmtBindParams2(pStmt->sql.pQuery, bind, colIdx, pStmt->taos->optionInfo.charsetCxt);
    if (code != TSDB_CODE_SUCCESS) {
      goto cleanup_root;
    }
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
                         .stmtBindVersion = pStmt->exec.pRequest->stmtBindVersion};
    ctx.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
    code = catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &ctx.pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      goto cleanup_root;
    }
    code = qStmtParseQuerySql(&ctx, pStmt->sql.pQuery);
    if (code != TSDB_CODE_SUCCESS) {
      goto cleanup_root;
    }

    if (pStmt->sql.pQuery->haveResultSet) {
      STMT_ERR_RET(setResSchemaInfo(&pStmt->exec.pRequest->body.resInfo, pStmt->sql.pQuery->pResSchema,
                                    pStmt->sql.pQuery->numOfResCols, pStmt->sql.pQuery->pResExtSchema, true));
      taosMemoryFreeClear(pStmt->sql.pQuery->pResSchema);
      taosMemoryFreeClear(pStmt->sql.pQuery->pResExtSchema);
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

  cleanup_root:
    STMT2_ELOG("parse query statment unexpected failed code:%d, need to clean node", code);
    if (pStmt->sql.pQuery && pStmt->sql.pQuery->pRoot) {
      nodesDestroyNode(pStmt->sql.pQuery->pRoot);
      pStmt->sql.pQuery->pRoot = NULL;
    }
    STMT_ERR_RET(code);
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
      STMT2_ELOG("table %s not found in exec blockHash:%p", pStmt->bInfo.tbFName, pStmt->exec.pBlockHash);
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_CACHE_ERROR);
    }
    pStmt->exec.pCurrBlock = *pDataBlock;
    if (pStmt->sql.stbInterlaceMode) {
      taosArrayDestroy(pStmt->exec.pCurrBlock->pData->aCol);
      (*pDataBlock)->pData->aCol = NULL;
    }
    if (colIdx < -1) {
      pStmt->sql.bindRowFormat = true;
      taosArrayDestroy((*pDataBlock)->pData->aCol);
      (*pDataBlock)->pData->aCol = taosArrayInit(20, POINTER_BYTES);
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
    param->tblData.isOrdered = true;
    param->tblData.isDuplicateTs = false;
    tstrncpy(param->tblData.tbName, pStmt->bInfo.tbName, TSDB_TABLE_NAME_LEN);

    param->pCreateTbReq = pCreateTbReq;
  }

  int64_t startUs3 = taosGetTimestampUs();
  pStmt->stat.bindDataUs2 += startUs3 - startUs2;

  SArray* pCols = pStmt->sql.stbInterlaceMode ? param->tblData.aCol : (*pDataBlock)->pData->aCol;
  SBlobSet* pBlob = NULL;
  if (colIdx < 0) {
    if (pStmt->sql.stbInterlaceMode) {
      (*pDataBlock)->pData->flags &= ~SUBMIT_REQ_COLUMN_DATA_FORMAT;
      code = qBindStmtStbColsValue2(*pDataBlock, pCols, pStmt->bInfo.boundCols, bind, pStmt->exec.pRequest->msgBuf,
                                    pStmt->exec.pRequest->msgBufLen, &pStmt->sql.siInfo.pTSchema, pStmt->sql.pBindInfo,
                                    pStmt->taos->optionInfo.charsetCxt, &pBlob);
      param->tblData.isOrdered = (*pDataBlock)->ordered;
      param->tblData.isDuplicateTs = (*pDataBlock)->duplicateTs;
    } else {
      if (colIdx == -1) {
        if (pStmt->sql.bindRowFormat) {
          STMT2_ELOG_E("can't mix bind row format and bind column format");
          STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
        }
        code = qBindStmtColsValue2(*pDataBlock, pCols, pStmt->bInfo.boundCols, bind, pStmt->exec.pRequest->msgBuf,
                                   pStmt->exec.pRequest->msgBufLen, pStmt->taos->optionInfo.charsetCxt);
      } else {
        code =
            qBindStmt2RowValue(*pDataBlock, (*pDataBlock)->pData->aRowP, pStmt->bInfo.boundCols, bind,
                               pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen,
                               &pStmt->sql.siInfo.pTSchema, pStmt->sql.pBindInfo, pStmt->taos->optionInfo.charsetCxt);
      }
    }

    if (code) {
      STMT2_ELOG("bind cols or rows failed, error:%s", tstrerror(code));
      STMT_ERR_RET(code);
    }
  } else {
    if (pStmt->sql.stbInterlaceMode) {
      STMT2_ELOG_E("bind single column not allowed in stb insert mode");
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
    }

    if (pStmt->sql.bindRowFormat) {
      STMT2_ELOG_E("can't mix bind row format and bind column format");
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
    }

    if (colIdx != (pStmt->bInfo.sBindLastIdx + 1) && colIdx != 0) {
      STMT2_ELOG_E("bind column index not in sequence");
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
    }

    pStmt->bInfo.sBindLastIdx = colIdx;

    if (0 == colIdx) {
      pStmt->bInfo.sBindRowNum = bind->num;
    }

    code = qBindStmtSingleColValue2(*pDataBlock, pCols, bind, pStmt->exec.pRequest->msgBuf,
                                    pStmt->exec.pRequest->msgBufLen, colIdx, pStmt->bInfo.sBindRowNum,
                                    pStmt->taos->optionInfo.charsetCxt);
    if (code) {
      STMT2_ELOG("bind single col failed, error:%s", tstrerror(code));
      STMT_ERR_RET(code);
    }
  }

  int64_t startUs4 = taosGetTimestampUs();
  pStmt->stat.bindDataUs3 += startUs4 - startUs3;

  if (pStmt->stbInterlaceMode) {
    if (param) param->tblData.pBlobSet = pBlob;
  }

  if (pStmt->sql.stbInterlaceMode) {
    STMT_ERR_RET(stmtAppendTablePostHandle(pStmt, param));
  } else {
    STMT_ERR_RET(stmtAddBatch2(pStmt));
  }

  pStmt->stat.bindDataUs4 += taosGetTimestampUs() - startUs4;
  return TSDB_CODE_SUCCESS;
}

/*
int stmtUpdateTableUid(STscStmt2* pStmt, SSubmitRsp* pRsp) {
  tscDebug("stmt start to update tbUid, blockNum:%d", pRsp->nBlocks);

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
*/
/*
int stmtStaticModeExec(TAOS_STMT* stmt) {
  STscStmt2*   pStmt = (STscStmt2*)stmt;
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

static int32_t createParseContext(const SRequestObj* pRequest, SParseContext** pCxt, SSqlCallbackWrapper* pWrapper) {
  const STscObj* pTscObj = pRequest->pTscObj;

  *pCxt = taosMemoryCalloc(1, sizeof(SParseContext));
  if (*pCxt == NULL) {
    return terrno;
  }

  **pCxt = (SParseContext){.requestId = pRequest->requestId,
                           .requestRid = pRequest->self,
                           .acctId = pTscObj->acctId,
                           .db = pRequest->pDb,
                           .topicQuery = false,
                           .pSql = pRequest->sqlstr,
                           .sqlLen = pRequest->sqlLen,
                           .pMsg = pRequest->msgBuf,
                           .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                           .pTransporter = pTscObj->pAppInfo->pTransporter,
                           .pStmtCb = NULL,
                           .pUser = pTscObj->user,
                           .pEffectiveUser = pRequest->effectiveUser,
                           .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER)),
                           .enableSysInfo = pTscObj->sysInfo,
                           .async = true,
                           .svrVer = pTscObj->sVer,
                           .nodeOffline = (pTscObj->pAppInfo->onlineDnodes < pTscObj->pAppInfo->totalDnodes),
                           .allocatorId = pRequest->allocatorRefId,
                           .parseSqlFp = clientParseSql,
                           .parseSqlParam = pWrapper};
  int8_t biMode = atomic_load_8(&((STscObj*)pTscObj)->biMode);
  (*pCxt)->biMode = biMode;
  return TSDB_CODE_SUCCESS;
}

static void asyncQueryCb(void* userdata, TAOS_RES* res, int code) {
  STscStmt2*        pStmt = userdata;
  __taos_async_fn_t fp = pStmt->options.asyncExecFn;
  pStmt->asyncResultAvailable = true;
  pStmt->exec.pRequest->inCallback = true;

  if (code == TSDB_CODE_SUCCESS) {
    pStmt->exec.affectedRows = taos_affected_rows(res);
    pStmt->affectedRows += pStmt->exec.affectedRows;
  }

  fp(pStmt->options.userdata, res, code);

  while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
    taosUsleep(1);
  }
  (void)stmtCleanExecInfo(pStmt, (code ? false : true), false);
  ++pStmt->sql.runTimes;
  if (pStmt->exec.pRequest != NULL) {
    pStmt->exec.pRequest->inCallback = false;
  }

  if (tsem_post(&pStmt->asyncExecSem) != 0) {
    STMT2_ELOG_E("fail to post asyncExecSem");
  }
}

int stmtExec2(TAOS_STMT2* stmt, int* affected_rows) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;
  int64_t    startUs = taosGetTimestampUs();

  STMT2_DLOG_E("start to exec");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERR_RET(taosThreadMutexLock(&pStmt->asyncBindParam.mutex));
  while (atomic_load_8((int8_t*)&pStmt->asyncBindParam.asyncBindNum) > 0) {
    (void)taosThreadCondWait(&pStmt->asyncBindParam.waitCond, &pStmt->asyncBindParam.mutex);
  }
  STMT_ERR_RET(taosThreadMutexUnlock(&pStmt->asyncBindParam.mutex));

  if (pStmt->sql.stbInterlaceMode) {
    STMT_ERR_RET(stmtAddBatch2(pStmt));
  }

  STMT_ERR_RET(stmtSwitchStatus(pStmt, STMT_EXECUTE));

  if (STMT_TYPE_QUERY != pStmt->sql.type) {
    if (pStmt->sql.stbInterlaceMode) {
      int64_t startTs = taosGetTimestampUs();
      // wait for stmt bind thread to finish
      while (atomic_load_64(&pStmt->sql.siInfo.tbRemainNum)) {
        taosUsleep(1);
      }

      if (pStmt->errCode != TSDB_CODE_SUCCESS) {
        return pStmt->errCode;
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
  }

  pStmt->asyncResultAvailable = false;
  SRequestObj*      pRequest = pStmt->exec.pRequest;
  __taos_async_fn_t fp = pStmt->options.asyncExecFn;
  STMT2_DLOG("EXEC INFO :req:0x%" PRIx64 ", QID:0x%" PRIx64 ", exec sql:%s,  conn:%" PRId64, pRequest->self,
             pRequest->requestId, pStmt->sql.sqlStr, pRequest->pTscObj->id);

  if (!fp) {
    launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);

    if (pStmt->exec.pRequest->code && NEED_CLIENT_HANDLE_ERROR(pStmt->exec.pRequest->code)) {
      STMT2_ELOG_E("exec failed errorcode:NEED_CLIENT_HANDLE_ERROR, need to refresh meta and retry");
      code = refreshMeta(pStmt->exec.pRequest->pTscObj, pStmt->exec.pRequest);
      if (code) {
        pStmt->exec.pRequest->code = code;

      } else {
        STMT_ERR_RET(stmtResetStmt(pStmt));
        STMT_ERR_RET(TSDB_CODE_NEED_RETRY);
      }
    }

    STMT_ERR_JRET(pStmt->exec.pRequest->code);

    pStmt->exec.affectedRows = taos_affected_rows(pStmt->exec.pRequest);
    if (affected_rows) {
      *affected_rows = pStmt->exec.affectedRows;
    }
    pStmt->affectedRows += pStmt->exec.affectedRows;

    // wait for stmt bind thread to finish
    while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
      taosUsleep(1);
    }

    STMT_ERR_RET(stmtCleanExecInfo(pStmt, (code ? false : true), false));

    ++pStmt->sql.runTimes;
  } else {
    SSqlCallbackWrapper* pWrapper = taosMemoryCalloc(1, sizeof(SSqlCallbackWrapper));
    if (pWrapper == NULL) {
      code = terrno;
    } else {
      pWrapper->pRequest = pRequest;
      pRequest->pWrapper = pWrapper;
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = createParseContext(pRequest, &pWrapper->pParseCtx, pWrapper);
    }
    pRequest->syncQuery = false;
    pRequest->body.queryFp = asyncQueryCb;
    ((SSyncQueryParam*)(pRequest)->body.interParam)->userParam = pStmt;

    pStmt->execSemWaited = false;
    launchAsyncQuery(pRequest, pStmt->sql.pQuery, NULL, pWrapper);
  }

_return:
  if (code) {
    STMT2_ELOG("exec failed, error:%s", tstrerror(code));
  }
  pStmt->stat.execUseUs += taosGetTimestampUs() - startUs;

  STMT_RET(code);
}

int stmtClose2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  STMT2_DLOG_E("start to close stmt");
  taosMemoryFreeClear(pStmt->db);

  if (pStmt->bindThreadInUse) {
    // wait for stmt bind thread to finish
    while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
      taosUsleep(1);
    }

    (void)taosThreadMutexLock(&pStmt->queue.mutex);
    pStmt->queue.stopQueue = true;
    (void)taosThreadCondSignal(&(pStmt->queue.waitCond));
    (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

    (void)taosThreadJoin(pStmt->bindThread, NULL);
    pStmt->bindThreadInUse = false;

    (void)taosThreadCondDestroy(&pStmt->queue.waitCond);
    (void)taosThreadMutexDestroy(&pStmt->queue.mutex);
  }

  TSC_ERR_RET(taosThreadMutexLock(&pStmt->asyncBindParam.mutex));
  while (atomic_load_8((int8_t*)&pStmt->asyncBindParam.asyncBindNum) > 0) {
    (void)taosThreadCondWait(&pStmt->asyncBindParam.waitCond, &pStmt->asyncBindParam.mutex);
  }
  TSC_ERR_RET(taosThreadMutexUnlock(&pStmt->asyncBindParam.mutex));

  (void)taosThreadCondDestroy(&pStmt->asyncBindParam.waitCond);
  (void)taosThreadMutexDestroy(&pStmt->asyncBindParam.mutex);

  if (pStmt->options.asyncExecFn && !pStmt->execSemWaited) {
    if (tsem_wait(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("fail to wait asyncExecSem");
    }
  }

  STMT2_DLOG("stmt %p closed, stbInterlaceMode:%d, statInfo: ctgGetTbMetaNum=>%" PRId64 ", getCacheTbInfo=>%" PRId64
             ", parseSqlNum=>%" PRId64 ", pStmt->stat.bindDataNum=>%" PRId64
             ", settbnameAPI:%u, bindAPI:%u, addbatchAPI:%u, execAPI:%u"
             ", setTbNameUs:%" PRId64 ", bindDataUs:%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 " addBatchUs:%" PRId64
             ", execWaitUs:%" PRId64 ", execUseUs:%" PRId64,
             pStmt, pStmt->sql.stbInterlaceMode, pStmt->stat.ctgGetTbMetaNum, pStmt->stat.getCacheTbInfo,
             pStmt->stat.parseSqlNum, pStmt->stat.bindDataNum, pStmt->seqIds[STMT_SETTBNAME], pStmt->seqIds[STMT_BIND],
             pStmt->seqIds[STMT_ADD_BATCH], pStmt->seqIds[STMT_EXECUTE], pStmt->stat.setTbNameUs,
             pStmt->stat.bindDataUs1, pStmt->stat.bindDataUs2, pStmt->stat.bindDataUs3, pStmt->stat.bindDataUs4,
             pStmt->stat.addBatchUs, pStmt->stat.execWaitUs, pStmt->stat.execUseUs);
  if (pStmt->sql.stbInterlaceMode) {
    pStmt->bInfo.tagsCached = false;
  }
  pStmt->bInfo.boundColsCached = false;

  STMT_ERR_RET(stmtCleanSQLInfo(pStmt));

  if (pStmt->options.asyncExecFn) {
    if (tsem_destroy(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("fail to destroy asyncExecSem");
    }
  }
  taosMemoryFree(stmt);

  return TSDB_CODE_SUCCESS;
}

const char* stmtErrstr2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  if (stmt == NULL || NULL == pStmt->exec.pRequest) {
    return (char*)tstrerror(terrno);
  }

  // if stmt async exec ,error code is pStmt->exec.pRequest->code
  if (!(pStmt->sql.status >= STMT_EXECUTE && pStmt->options.asyncExecFn != NULL && pStmt->asyncResultAvailable)) {
    pStmt->exec.pRequest->code = terrno;
  }

  SRequestObj* pRequest = pStmt->exec.pRequest;
  if (NULL != pRequest->msgBuf && (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR)) {
    return pRequest->msgBuf;
  }
  return (const char*)tstrerror(pRequest->code);
}
/*
int stmtAffectedRows(TAOS_STMT* stmt) { return ((STscStmt2*)stmt)->affectedRows; }

int stmtAffectedRowsOnce(TAOS_STMT* stmt) { return ((STscStmt2*)stmt)->exec.affectedRows; }
*/

int stmtParseColFields2(TAOS_STMT2* stmt) {
  int32_t    code = 0;
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    preCode = pStmt->errCode;

  STMT2_DLOG_E("start to get col fields for insert");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    STMT2_ELOG_E("stmtParseColFields2 only for insert");
    STMT_ERRI_JRET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  STMT_ERRI_JRET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }
  if (pStmt->sql.stbInterlaceMode && pStmt->sql.siInfo.pDataCtx != NULL) {
    pStmt->bInfo.needParse = false;
  }

  STMT_ERRI_JRET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERRI_JRET(stmtParseSql(pStmt));
  }

_return:
  // compatible with previous versions
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST && (pStmt->bInfo.tbNameFlag & NO_DATA_USING_CLAUSE) == 0x0) {
    code = TSDB_CODE_TSC_STMT_TBNAME_ERROR;
  }

  pStmt->errCode = preCode;

  return code;
}

int stmtGetStbColFields2(TAOS_STMT2* stmt, int* nums, TAOS_FIELD_ALL** fields) {
  int32_t code = stmtParseColFields2(stmt);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return stmtFetchStbColFields2(stmt, nums, fields);
}

int stmtGetParamNum2(TAOS_STMT2* stmt, int* nums) {
  int32_t    code = 0;
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    preCode = pStmt->errCode;

  STMT2_DLOG_E("start to get param num for query");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  STMT_ERRI_JRET(stmtSwitchStatus(pStmt, STMT_FETCH_FIELDS));

  if (pStmt->bInfo.needParse && pStmt->sql.runTimes && pStmt->sql.type > 0 &&
      STMT_TYPE_MULTI_INSERT != pStmt->sql.type) {
    pStmt->bInfo.needParse = false;
  }

  if (pStmt->exec.pRequest && STMT_TYPE_QUERY == pStmt->sql.type && pStmt->sql.runTimes) {
    resetRequest(pStmt);
  }

  STMT_ERRI_JRET(stmtCreateRequest(pStmt));

  if (pStmt->bInfo.needParse) {
    STMT_ERRI_JRET(stmtParseSql(pStmt));
  }

  if (STMT_TYPE_QUERY == pStmt->sql.type) {
    *nums = taosArrayGetSize(pStmt->sql.pQuery->pPlaceholderValues);
  } else {
    STMT_ERRI_JRET(stmtFetchColFields2(stmt, nums, NULL));
  }

  STMT2_TLOG("get param num success, nums:%d", *nums);

_return:

  pStmt->errCode = preCode;

  return code;
}

TAOS_RES* stmtUseResult2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  STMT2_TLOG_E("start to use result");

  if (STMT_TYPE_QUERY != pStmt->sql.type) {
    STMT2_ELOG_E("useResult only for query statement");
    return NULL;
  }

  if (pStmt->options.asyncExecFn != NULL && !pStmt->asyncResultAvailable) {
    STMT2_ELOG_E("use result after callBackFn return");
    return NULL;
  }

  return pStmt->exec.pRequest;
}

int32_t stmtAsyncBindThreadFunc(void* args) {
  qInfo("async stmt bind thread started");

  ThreadArgs* targs = (ThreadArgs*)args;
  STscStmt2*  pStmt = (STscStmt2*)targs->stmt;

  int code = taos_stmt2_bind_param(targs->stmt, targs->bindv, targs->col_idx);
  targs->fp(targs->param, NULL, code);
  (void)taosThreadMutexLock(&(pStmt->asyncBindParam.mutex));
  (void)atomic_sub_fetch_8(&pStmt->asyncBindParam.asyncBindNum, 1);
  (void)taosThreadCondSignal(&(pStmt->asyncBindParam.waitCond));
  (void)taosThreadMutexUnlock(&(pStmt->asyncBindParam.mutex));
  taosMemoryFree(args);

  qInfo("async stmt bind thread stopped");

  return code;
}

void stmtBuildErrorMsg(STscStmt2* pStmt, const char* msg) {
  if (pStmt == NULL || msg == NULL) {
    return;
  }

  if (pStmt->exec.pRequest == NULL) {
    return;
  }

  if (pStmt->exec.pRequest->msgBuf == NULL) {
    return;
  }

  size_t msgLen = strlen(msg);
  size_t bufLen = pStmt->exec.pRequest->msgBufLen;

  if (msgLen >= bufLen) {
    tstrncpy(pStmt->exec.pRequest->msgBuf, msg, bufLen - 1);
    pStmt->exec.pRequest->msgBuf[bufLen - 1] = '\0';
    pStmt->exec.pRequest->msgBufLen = bufLen - 1;
  } else {
    tstrncpy(pStmt->exec.pRequest->msgBuf, msg, bufLen);
    pStmt->exec.pRequest->msgBufLen = msgLen;
  }

  return;
}

int32_t stmtBuildErrorMsgWithCode(STscStmt2* pStmt, const char* msg, int32_t errorCode) {
  stmtBuildErrorMsg(pStmt, msg);
  pStmt->errCode = errorCode;

  return errorCode;
}