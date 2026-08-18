#include "clientInt.h"
#include "clientLog.h"
#include "taoserror.h"
#include "tdef.h"
#include "tglobal.h"
#include "clientStmt.h"
#include "clientStmt2.h"
#include "querynodes.h"
#include "tencode.h"
#include "tmsg.h"
#include "tname.h"
#include "trow.h"

#define STMT_ASYNC_BIND_QUEUE_CAPACITY 256
#define STMT_DEQUEUE_SPIN_ROUNDS       10

typedef struct SStmt2RetryTags {
  bool            fixedTags;
  int32_t         numOfTags;
  TAOS_STMT2_BIND binds[];
} SStmt2RetryTags;

static void stmtDestroyRetryTags(SStmt2RetryTags* pTags) {
  taosMemoryFree(pTags);
}

static void stmtFreeRetryTags(void* value) {
  SStmt2RetryTags* pTags = *(SStmt2RetryTags**)value;
  stmtDestroyRetryTags(pTags);
}

static void stmtClearRetryTags(STscStmt2* pStmt) {
  if (pStmt->pRetryTagHash != NULL) {
    taosHashClear(pStmt->pRetryTagHash);
  }
}

char* gStmt2StatusStr[] = {"unknown",     "init", "prepare", "settbname", "settags",
                           "fetchFields", "bind", "bindCol", "addBatch",  "exec"};

#define SET_ERR(fmt, ...) do {                                                 \
  char       *sbuf = pStmt->msgBuf;                                            \
  size_t      nlen = sizeof(pStmt->msgBuf);                                    \
  if (pStmt->exec.pRequest) {                                                  \
    sbuf = pStmt->exec.pRequest->msgBuf;                                       \
    nlen = pStmt->exec.pRequest->msgBufLen;                                    \
  }                                                                            \
  (void)snprintf(sbuf, nlen, "%s[%d]%s():" fmt "",                             \
      __FILE__, __LINE__, __func__, ##__VA_ARGS__);                            \
} while (0)

static inline void
stmt2LiteralCtxReset(SStmt2LiteralCtx *ctx) {
  ctx->code = 0;
  ctx->prepared       = 0;
  ctx->executing      = 0;
  ctx->executed       = 0;
  ctx->has_result_set = 0;
}

static inline void
stmt2LiteralCtxRelease(SStmt2LiteralCtx *ctx) {
  stmt2LiteralCtxReset(ctx);
  if (ctx->sem_valid) {
    tsem_destroy(&ctx->sem);
    ctx->sem_valid = 0;
  }
}

static inline int
stmt2LiteralCtxInit(SStmt2LiteralCtx *ctx) {
  if (ctx->sem_valid) return 0;
  if (tsem_init(&ctx->sem, 0, 0)) return -1;
  ctx->sem_valid = 1;
  return 0;
}

static inline int
stmt2LiteralCtxIsValid(SStmt2LiteralCtx *ctx) {
  return ctx && ctx->sem_valid;
}

int stmtIsLiteral(TAOS_STMT2 *stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  return pStmt && pStmt->literal;
}



/* Free any existing siInfo.dbname and replace with a heap copy of src.
 * src may be NULL or empty — in either case dbname is left NULL. */
static int32_t stmtDupSiInfoDbname(SStbInterlaceInfo* pSi, const char* src) {
  taosMemoryFreeClear(pSi->dbname);
  if (src != NULL && src[0] != '\0') {
    pSi->dbname = taosStrdup(src);
    if (pSi->dbname == NULL) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

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
    if (atomic_load_8((int8_t*)&pStmt->queue.stopQueue)) {
      return false;
    }
    if (i < STMT_DEQUEUE_SPIN_ROUNDS) {
      taosUsleep(1);
      i++;
    } else {
      (void)taosThreadMutexLock(&pStmt->queue.mutex);
      if (atomic_load_8((int8_t*)&pStmt->queue.stopQueue)) {
        (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
        return false;
      }
      if (0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
        (void)taosThreadCondWait(&pStmt->queue.waitCond, &pStmt->queue.mutex);
      }
      (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
    }
  }

  if (atomic_load_8((int8_t*)&pStmt->queue.stopQueue) &&
      0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
    return false;
  }

  (void)taosThreadMutexLock(&pStmt->queue.mutex);
  if (pStmt->queue.head == pStmt->queue.tail) {
    atomic_store_64((int64_t*)&pStmt->queue.qRemainNum, 0);
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

  int64_t remain = atomic_sub_fetch_64((int64_t*)&pStmt->queue.qRemainNum, 1);
  if (remain == STMT_ASYNC_BIND_QUEUE_CAPACITY - 1) {
    (void)taosThreadCondSignal(&pStmt->queue.waitCond);
  }
  (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

  STMT2_TLOG("dequeue success, node:%p, remainNum:%" PRId64, node, remain);

  return true;
}

static int32_t stmtEnqueue(STscStmt2* pStmt, SStmtQNode* param) {
  if (param == NULL) {
    STMT2_ELOG_E("enqueue param is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  param->next = NULL;

  int64_t waitStartUs = taosGetTimestampUs();
  (void)taosThreadMutexLock(&pStmt->queue.mutex);
  while (!atomic_load_8((int8_t*)&pStmt->queue.stopQueue) &&
         atomic_load_64((int64_t*)&pStmt->queue.qRemainNum) >= STMT_ASYNC_BIND_QUEUE_CAPACITY) {
    (void)taosThreadCondWait(&pStmt->queue.waitCond, &pStmt->queue.mutex);
  }
  if (atomic_load_8((int8_t*)&pStmt->queue.stopQueue)) {
    (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
    STMT2_ELOG_E("stmt bind thread is stopped, cannot enqueue");
    return TSDB_CODE_TSC_STMT_API_ERROR;
  }

  pStmt->queue.tail->next = param;
  pStmt->queue.tail = param;
  pStmt->stat.bindDataNum++;

  param->enqueueUs = taosGetTimestampUs();
  pStmt->stat.asyncBackpressureUs += param->enqueueUs - waitStartUs;
  int64_t remain = atomic_add_fetch_64((int64_t*)&pStmt->queue.qRemainNum, 1);
  if (remain > pStmt->stat.asyncQueueHighWater) {
    pStmt->stat.asyncQueueHighWater = remain;
  }
  if (remain == 1) {
    (void)taosThreadCondSignal(&(pStmt->queue.waitCond));
  }

  (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

  STMT2_TLOG("enqueue param:%p, remainNum:%" PRId64 ", restoreTbCols:%d", param, remain, param->restoreTbCols);
  return TSDB_CODE_SUCCESS;
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

static int32_t stmtUpdateBindInfo(TAOS_STMT2* stmt, STableMeta* pTableMeta, void* tags, SSHashObj** cols, SName* tbName,
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

  // transfer ownership of cols to stmt
  if (cols) {
    pStmt->bInfo.fixedValueCols = *cols;
    *cols = NULL;
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

static int32_t stmtUpdateInfo(TAOS_STMT2* stmt, STableMeta* pTableMeta, void* tags, SSHashObj** cols, SName* tbName,
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

static int32_t stmtPrintBindv(TAOS_STMT2* stmt, TAOS_STMT2_BIND* bindv, int32_t col_idx, bool isTags) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    count = 0;
  int32_t    code = 0;

  if (bindv == NULL) {
    STMT2_TLOG("bindv is NULL, col_idx:%d, isTags:%d", col_idx, isTags);
    return TSDB_CODE_SUCCESS;
  }

  if (col_idx >= 0) {
    count = 1;
    STMT2_TLOG("single col bind, col_idx:%d", col_idx);
  } else {
    if (STMT_TYPE_INSERT == pStmt->sql.type || STMT_TYPE_MULTI_INSERT == pStmt->sql.type ||
        (pStmt->sql.type == 0 && stmt2IsInsert(stmt))) {
      if (pStmt->sql.placeholderOfTags == 0 && pStmt->sql.placeholderOfCols == 0) {
        code = stmtGetStbColFields2(pStmt, NULL, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      if (isTags) {
        count = pStmt->sql.placeholderOfTags;
        STMT2_TLOG("print tags bindv, cols:%d", count);
      } else {
        count = pStmt->sql.placeholderOfCols;
        STMT2_TLOG("print cols bindv, cols:%d", count);
      }
    } else if (STMT_TYPE_QUERY == pStmt->sql.type || (pStmt->sql.type == 0 && stmt2IsSelect(stmt))) {
      count = taosArrayGetSize(pStmt->sql.pQuery->pPlaceholderValues);
      STMT2_TLOG("print query bindv, cols:%d", count);
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    STMT2_ELOG("failed to get param count, code:%d", code);
    return code;
  }

  for (int i = 0; i < count; i++) {
    int32_t type = bindv[i].buffer_type;
    int32_t num = bindv[i].num;
    char*   current_buf = (char*)bindv[i].buffer;

    for (int j = 0; j < num; j++) {
      char    buf[256] = {0};
      int32_t len = 0;
      bool    isNull = (bindv[i].is_null && bindv[i].is_null[j]);

      if (IS_VAR_DATA_TYPE(type) && bindv[i].length) {
        len = bindv[i].length[j];
      } else {
        len = tDataTypes[type].bytes;
      }

      if (isNull) {
        snprintf(buf, sizeof(buf), "NULL");
      } else {
        if (current_buf == NULL) {
          snprintf(buf, sizeof(buf), "NULL(Buf)");
        } else {
          switch (type) {
            case TSDB_DATA_TYPE_BOOL:
              snprintf(buf, sizeof(buf), "%d", *(int8_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_TINYINT:
              snprintf(buf, sizeof(buf), "%d", *(int8_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_SMALLINT:
              snprintf(buf, sizeof(buf), "%d", *(int16_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_INT:
              snprintf(buf, sizeof(buf), "%d", *(int32_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_BIGINT:
              snprintf(buf, sizeof(buf), "%" PRId64, *(int64_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_FLOAT:
              snprintf(buf, sizeof(buf), "%f", *(float*)current_buf);
              break;
            case TSDB_DATA_TYPE_DOUBLE:
              snprintf(buf, sizeof(buf), "%f", *(double*)current_buf);
              break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_VARBINARY:
              snprintf(buf, sizeof(buf), "len:%d, val:%.*s", len, len, current_buf);
              break;
            case TSDB_DATA_TYPE_TIMESTAMP:
              snprintf(buf, sizeof(buf), "%" PRId64, *(int64_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_UTINYINT:
              snprintf(buf, sizeof(buf), "%u", *(uint8_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_USMALLINT:
              snprintf(buf, sizeof(buf), "%u", *(uint16_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_UINT:
              snprintf(buf, sizeof(buf), "%u", *(uint32_t*)current_buf);
              break;
            case TSDB_DATA_TYPE_UBIGINT:
              snprintf(buf, sizeof(buf), "%" PRIu64, *(uint64_t*)current_buf);
              break;
            default:
              snprintf(buf, sizeof(buf), "UnknownType:%d", type);
              break;
          }
        }
      }

      STMT2_TLOG("bindv[%d] row[%d]: type:%s, val:%s", i, j, tDataTypes[type].name, buf);

      if (!isNull && current_buf) {
        current_buf += len;
      }
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

// Soft-reset for retry: keep the same SRequestObj (and therefore its tableList/dbList/pDb,
// which refreshMeta needs) and only clear the per-execution state set by the previous launch.
// Mirrors restartAsyncQuery + destroyCtxInRequest in clientMain.c.
static void stmtSoftResetRequestForRetry(STscStmt2* pStmt) {
  SRequestObj* pReq = pStmt->exec.pRequest;
  if (pReq == NULL) {
    return;
  }

  destroyCtxInRequest(pReq);

  pReq->code = 0;
  pReq->body.resInfo.numOfRows = 0;
  if (pReq->msgBuf != NULL) {
    pReq->msgBuf[0] = '\0';
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
    pStmt->bInfo.boundTags = NULL;
  }

  if (!pStmt->bInfo.boundColsCached) {
    tSimpleHashCleanup(pStmt->bInfo.fixedValueCols);
    pStmt->bInfo.fixedValueCols = NULL;
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
  stmtClearRetryTags(pStmt);
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
      resetRequest(pStmt);
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

        /* After TSWAP, for row-format data the retained pData's
         * aRowP is a shallow copy aliasing the original.  When
         * pCurrTbData is destroyed next cycle those pointers
         * dangle.  Break the alias by clearing aRowP.
         * For column-format data, qResetStmtDataBlock already
         * zeroes nVal on each aCol entry and aRowP is unused,
         * so the stale entry is harmlessly skipped by
         * insMergeTableDataCxt's nVal<=0 check. */
        if (!(pBlocks->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT)) {
          if (pBlocks->pData->aRowP) {
            taosArrayClear(pBlocks->pData->aRowP);
          }
        }

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

    pStmt->exec.pCurrBlock = NULL;
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

static void stmtFreeSingleVgDataBlock(void* p) {
  SVgDataBlocks* pVg = *(SVgDataBlocks**)p;
  if (pVg) {
    taosMemoryFree(pVg->pData);
    taosMemoryFree(pVg);
  }
}

static void stmtFreeVgDataBlocksForRetry(STscStmt2* pStmt) {
  if (pStmt->pVgDataBlocksForRetry) {
    taosArrayDestroyEx(pStmt->pVgDataBlocksForRetry, stmtFreeSingleVgDataBlock);
    pStmt->pVgDataBlocksForRetry = NULL;
  }
}

static int32_t stmtSaveVgDataBlocksForRetry(STscStmt2* pStmt) {
  stmtFreeVgDataBlocksForRetry(pStmt);

  SVnodeModifyOpStmt* pModif = (SVnodeModifyOpStmt*)pStmt->sql.pQuery->pRoot;
  if (!pModif || !pModif->pDataBlocks || taosArrayGetSize(pModif->pDataBlocks) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t num = taosArrayGetSize(pModif->pDataBlocks);
  pStmt->pVgDataBlocksForRetry = taosArrayInit(num, POINTER_BYTES);
  if (!pStmt->pVgDataBlocksForRetry) {
    return terrno;
  }

  for (int32_t i = 0; i < num; i++) {
    SVgDataBlocks* pSrc = taosArrayGetP(pModif->pDataBlocks, i);
    SVgDataBlocks* pDst = taosMemoryMalloc(sizeof(SVgDataBlocks));
    if (!pDst) {
      stmtFreeVgDataBlocksForRetry(pStmt);
      return terrno;
    }
    *pDst = *pSrc;
    pDst->pData = taosMemoryMalloc(pSrc->size);
    if (!pDst->pData) {
      taosMemoryFree(pDst);
      stmtFreeVgDataBlocksForRetry(pStmt);
      return terrno;
    }
    (void)memcpy(pDst->pData, pSrc->pData, pSrc->size);
    if (NULL == taosArrayPush(pStmt->pVgDataBlocksForRetry, &pDst)) {
      taosMemoryFree(pDst->pData);
      taosMemoryFree(pDst);
      stmtFreeVgDataBlocksForRetry(pStmt);
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stmtRestoreVgDataBlocksForRetry(STscStmt2* pStmt) {
  SVnodeModifyOpStmt* pModif = (SVnodeModifyOpStmt*)pStmt->sql.pQuery->pRoot;
  if (!pModif || !pStmt->pVgDataBlocksForRetry) {
    return TSDB_CODE_SUCCESS;
  }
  // The planner owns pDataBlocks after createQueryPlan (via TSWAP); it has already freed
  // the old array. We simply restore a new clone here.
  pModif->pDataBlocks = pStmt->pVgDataBlocksForRetry;
  pStmt->pVgDataBlocksForRetry = NULL;
  return TSDB_CODE_SUCCESS;
}

static STableMeta* stmtCloneTableMetaForRetry(const STableMeta* pSrc) {
  int32_t sz = (int32_t)TABLE_META_FULL_SIZE(pSrc);
  if (sz <= 0) {
    return NULL;
  }
  STableMeta* p = taosMemoryMalloc(sz);
  if (p == NULL) {
    return NULL;
  }
  (void)memcpy(p, pSrc, sz);
  tableMetaResetPointers(p);
  return p;
}

static void stmtFreeUidTableMetaHash(SHashObj* pHash) {
  if (pHash == NULL) {
    return;
  }
  void* pIter = NULL;
  while ((pIter = taosHashIterate(pHash, pIter)) != NULL) {
    STableMeta* pMeta = *(STableMeta**)pIter;
    taosMemoryFree(pMeta);
  }
  taosHashCleanup(pHash);
}

// tRowGet may succeed with a wrong prefix schema but leave VAR column pointers outside the SRow allocation;
// tRowBuild would then memcpy OOB. Require all VAR payloads to lie within [pRow, pRow + pRow->len).
static bool stmtScolValVarPayloadInRow(const SRow* pRow, const SColVal* pCv, int8_t colType) {
  if (!COL_VAL_IS_VALUE(pCv) || !IS_VAR_DATA_TYPE(colType)) {
    return true;
  }
  if (pCv->value.nData == 0) {
    return true;
  }
  if (pCv->value.pData == NULL) {
    return false;
  }
  const uint8_t* rbeg = (const uint8_t*)pRow;
  const uint8_t* rend = rbeg + pRow->len;
  const uint8_t* p = (const uint8_t*)pCv->value.pData;
  return (p >= rbeg) && (p + pCv->value.nData <= rend);
}

// Infer decode STSchema: full column count when row sver matches catalog; else try prefix column counts (ADD COLUMN).
static int32_t stmtFindDecodeSchemaForRow(SRow* pRow, const STableMeta* pMeta, STSchema** ppOld, int32_t* pnOldCols) {
  uint16_t oldSver = pRow->sver;
  int32_t  nMax = pMeta->tableInfo.numOfColumns;
  SSchema* base = (SSchema*)&pMeta->schema[0];

  if ((int32_t)oldSver == pMeta->sversion) {
    STSchema* p = tBuildTSchema(base, nMax, (int32_t)oldSver);
    if (p == NULL) {
      return terrno;
    }
    bool ok = true;
    for (int32_t i = 0; i < nMax; ++i) {
      SColVal cv = {0};
      if (tRowGet(pRow, p, i, &cv) != 0) {
        ok = false;
        break;
      }
      if (!stmtScolValVarPayloadInRow(pRow, &cv, p->columns[i].type)) {
        ok = false;
        break;
      }
    }
    if (ok) {
      *ppOld = p;
      *pnOldCols = nMax;
      return TSDB_CODE_SUCCESS;
    }
    tDestroyTSchema(p);
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t n = nMax; n >= 1; --n) {
    STSchema* pTry = tBuildTSchema(base, n, (int32_t)oldSver);
    if (pTry == NULL) {
      return terrno;
    }
    bool ok = true;
    for (int32_t i = 0; i < n; ++i) {
      SColVal cv = {0};
      if (tRowGet(pRow, pTry, i, &cv) != 0) {
        ok = false;
        break;
      }
      if (!stmtScolValVarPayloadInRow(pRow, &cv, pTry->columns[i].type)) {
        ok = false;
        break;
      }
    }
    if (ok) {
      *ppOld = pTry;
      *pnOldCols = n;
      return TSDB_CODE_SUCCESS;
    }
    tDestroyTSchema(pTry);
  }
  return TSDB_CODE_INVALID_PARA;
}

static int32_t stmtRebuildOneRowToLatestSchema(SRow* pOldRow, const STableMeta* pMeta, SRow** ppNewRow) {
  STSchema* pOldSch = NULL;
  int32_t   nOldCols = 0;
  int32_t   code = stmtFindDecodeSchemaForRow(pOldRow, pMeta, &pOldSch, &nOldCols);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t   nNewCols = pMeta->tableInfo.numOfColumns;
  STSchema* pNewSch = tBuildTSchema((SSchema*)&pMeta->schema[0], nNewCols, pMeta->sversion);
  if (pNewSch == NULL) {
    tDestroyTSchema(pOldSch);
    return terrno;
  }

  for (int32_t j = 0; j < nOldCols && j < nNewCols; ++j) {
    if (pOldSch->columns[j].colId != pNewSch->columns[j].colId) {
      tDestroyTSchema(pOldSch);
      tDestroyTSchema(pNewSch);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  SArray* aColVal = taosArrayInit(pNewSch->numOfCols, sizeof(SColVal));
  if (aColVal == NULL) {
    tDestroyTSchema(pOldSch);
    tDestroyTSchema(pNewSch);
    return terrno;
  }

  for (int32_t j = 0; j < pNewSch->numOfCols; ++j) {
    SColVal cv = {0};
    if (j < nOldCols) {
      code = tRowGet(pOldRow, pOldSch, j, &cv);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(aColVal);
        tDestroyTSchema(pOldSch);
        tDestroyTSchema(pNewSch);
        return code;
      }
    } else {
      STColumn* pc = &pNewSch->columns[j];
      cv = COL_VAL_NONE(pc->colId, pc->type);
    }
    if (taosArrayPush(aColVal, &cv) == NULL) {
      code = terrno;
      taosArrayDestroy(aColVal);
      tDestroyTSchema(pOldSch);
      tDestroyTSchema(pNewSch);
      return code;
    }
  }

  SRowBuildScanInfo sinfo = {0};
  code = tRowBuild(aColVal, pNewSch, ppNewRow, &sinfo);
  taosArrayDestroy(aColVal);
  tDestroyTSchema(pOldSch);
  tDestroyTSchema(pNewSch);
  return code;
}

static void stmtFreeHeapPatchRowsArray(SArray* aHeapRows) {
  if (aHeapRows == NULL) {
    return;
  }
  int32_t n = (int32_t)taosArrayGetSize(aHeapRows);
  for (int32_t i = 0; i < n; ++i) {
    SRow* p = taosArrayGetP(aHeapRows, i);
    tRowDestroy(p);
  }
  taosArrayDestroy(aHeapRows);
}

// After refreshMeta: set sver from catalog; decode each row with inferred old schema and tRowBuild with latest schema.
// aHeapRows: receives pointers from tRowBuild so they can be freed before tDestroySubmitReq (decode path does not free rows).
static void stmtPatchOneSubmitTbDataSchemaVer(SSubmitTbData* pTb, SHashObj* pUidMetaHash, SArray* aHeapRows) {
  if (pTb->uid == 0) {
    return;
  }
  void* pMv = taosHashGet(pUidMetaHash, &pTb->uid, sizeof(uint64_t));
  if (pMv == NULL) {
    return;
  }
  STableMeta* pMeta = *(STableMeta**)pMv;
  pTb->sver = pMeta->sversion;
  if (pTb->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    return;
  }
  if (pTb->aRowP == NULL) {
    return;
  }
  if (pTb->pBlobSet != NULL) {
    int32_t nRow = (int32_t)TARRAY_SIZE(pTb->aRowP);
    SRow**  rows = (SRow**)TARRAY_DATA(pTb->aRowP);
    for (int32_t i = 0; i < nRow; ++i) {
      if (rows[i] != NULL) {
        rows[i]->sver = (uint16_t)pMeta->sversion;
      }
    }
    return;
  }

  int32_t nRow = (int32_t)TARRAY_SIZE(pTb->aRowP);
  for (int32_t i = 0; i < nRow; ++i) {
    SRow* pRow = taosArrayGetP(pTb->aRowP, i);
    if (pRow == NULL) {
      continue;
    }
    if ((uint16_t)pMeta->sversion == pRow->sver) {
      continue;
    }
    if (pRow->flag & HAS_BLOB) {
      pRow->sver = (uint16_t)pMeta->sversion;
      continue;
    }
    SRow* pNew = NULL;
    if (stmtRebuildOneRowToLatestSchema(pRow, pMeta, &pNew) == TSDB_CODE_SUCCESS && pNew != NULL) {
      // pRow points into the decoded submit payload (tDecodeBinaryWithSize); do not tRowDestroy it.
      if (aHeapRows != NULL && taosArrayPush(aHeapRows, &pNew) == NULL) {
        tRowDestroy(pNew);
        // Cannot record heap row for destroy before tDestroySubmitReq; keep embedded row, bump sver only.
        pRow->sver = (uint16_t)pMeta->sversion;
      } else {
        (void)taosArraySet(pTb->aRowP, i, &pNew);
      }
    } else {
      pRow->sver = (uint16_t)pMeta->sversion;
    }
  }
}

static int32_t stmtBuildUidToTableMetaHash(STscStmt2* pStmt, SRequestObj* pRequest, SHashObj** ppHash) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppHash = NULL;

  if (NULL == pStmt->pCatalog) {
    code = catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  SHashObj* pHash =
      taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    return terrno;
  }

  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};

  int32_t tblNum = pRequest->tableList ? (int32_t)taosArrayGetSize(pRequest->tableList) : 0;
  for (int32_t i = 0; i < tblNum; ++i) {
    SName*      pName = taosArrayGet(pRequest->tableList, i);
    STableMeta* pMeta = NULL;
    int32_t     c = catalogGetTableMeta(pStmt->pCatalog, &conn, pName, &pMeta);
    if (c != TSDB_CODE_SUCCESS) {
      if (pMeta != NULL) {
        taosMemoryFree(pMeta);
      }
      taosHashCleanup(pHash);
      return c;
    }
    if (pMeta != NULL) {
      STableMeta* pDup = stmtCloneTableMetaForRetry(pMeta);
      taosMemoryFree(pMeta);
      pMeta = NULL;
      if (pDup != NULL) {
        int32_t putCode = taosHashPut(pHash, &pDup->uid, sizeof(uint64_t), &pDup, POINTER_BYTES);
        if (putCode != TSDB_CODE_SUCCESS) {
          STMT2_ELOG("stmtBuildUidToTableMetaHash taosHashPut failed uid:%" PRIu64 ", code:%s", (uint64_t)pDup->uid,
                     tstrerror(putCode));
          taosMemoryFree(pDup);
          taosHashCleanup(pHash);
          return putCode;
        }
      }
    }
  }

  if (taosHashGetSize(pHash) == 0 && pStmt->bInfo.sname.type != 0) {
    STableMeta* pMeta = NULL;
    code = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pMeta);
    if (code == TSDB_CODE_SUCCESS && pMeta != NULL) {
      STableMeta* pDup = stmtCloneTableMetaForRetry(pMeta);
      taosMemoryFree(pMeta);
      pMeta = NULL;
      if (pDup != NULL) {
        int32_t putCode = taosHashPut(pHash, &pDup->uid, sizeof(uint64_t), &pDup, POINTER_BYTES);
        if (putCode != TSDB_CODE_SUCCESS) {
          STMT2_ELOG("stmtBuildUidToTableMetaHash taosHashPut failed uid:%" PRIu64 ", code:%s", (uint64_t)pDup->uid,
                     tstrerror(putCode));
          taosMemoryFree(pDup);
          taosHashCleanup(pHash);
          return putCode;
        }
      }
    } else if (pMeta != NULL) {
      taosMemoryFree(pMeta);
    }
  }

  *ppHash = pHash;
  return TSDB_CODE_SUCCESS;
}

static int32_t stmtUpdateVgDataBlocksSchemaVer(STscStmt2* pStmt, SRequestObj* pRequest) {
  if (pStmt->pVgDataBlocksForRetry == NULL || taosArrayGetSize(pStmt->pVgDataBlocksForRetry) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SHashObj* pUidMetaHash = NULL;
  int32_t   code = stmtBuildUidToTableMetaHash(pStmt, pRequest, &pUidMetaHash);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pUidMetaHash == NULL || taosHashGetSize(pUidMetaHash) == 0) {
    if (pUidMetaHash != NULL) {
      stmtFreeUidTableMetaHash(pUidMetaHash);
    }
    return TSDB_CODE_SUCCESS;
  }

  const int32_t headSz = (int32_t)sizeof(SSubmitReq2Msg);
  int32_t       nBlk = (int32_t)taosArrayGetSize(pStmt->pVgDataBlocksForRetry);

  for (int32_t b = 0; b < nBlk; ++b) {
    SVgDataBlocks* pVg = *(SVgDataBlocks**)taosArrayGet(pStmt->pVgDataBlocksForRetry, b);
    if (pVg == NULL || pVg->pData == NULL || pVg->size <= headSz) {
      continue;
    }

    SDecoder     decoder = {0};
    int32_t      bodyLen = pVg->size - headSz;
    SSubmitReq2  req = {0};

    tDecoderInit(&decoder, (uint8_t*)pVg->pData + headSz, bodyLen);
    code = tDecodeSubmitReq(&decoder, &req, NULL);
    tDecoderClear(&decoder);
    if (code != TSDB_CODE_SUCCESS) {
      STMT2_ELOG("tDecodeSubmitReq failed when patching schema ver for retry, code:%s", tstrerror(code));
      stmtFreeUidTableMetaHash(pUidMetaHash);
      return code;
    }
    if (req.raw) {
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      continue;
    }

    SArray* aHeapRows = taosArrayInit(8, POINTER_BYTES);
    if (aHeapRows == NULL) {
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      stmtFreeUidTableMetaHash(pUidMetaHash);
      return terrno;
    }

    int32_t nTb = (int32_t)taosArrayGetSize(req.aSubmitTbData);
    for (int32_t t = 0; t < nTb; ++t) {
      stmtPatchOneSubmitTbDataSchemaVer(taosArrayGet(req.aSubmitTbData, t), pUidMetaHash, aHeapRows);
    }

    int32_t encCap = 0;
    int32_t szRet = 0;
    tEncodeSize(tEncodeSubmitReq, &req, encCap, szRet);
    if (szRet != 0) {
      stmtFreeHeapPatchRowsArray(aHeapRows);
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      stmtFreeUidTableMetaHash(pUidMetaHash);
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t allocLen = headSz + encCap;
    void*   pNew = taosMemoryMalloc(allocLen);
    if (pNew == NULL) {
      stmtFreeHeapPatchRowsArray(aHeapRows);
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      stmtFreeUidTableMetaHash(pUidMetaHash);
      return terrno;
    }

    (void)memcpy(pNew, pVg->pData, headSz);
    ((SSubmitReq2Msg*)pNew)->header.vgId = htonl(pVg->vg.vgId);
    ((SSubmitReq2Msg*)pNew)->version = htobe64(1);

    SEncoder encoder = {0};
    tEncoderInit(&encoder, (uint8_t*)pNew + headSz, encCap);
    code = tEncodeSubmitReq(&encoder, &req);
    int32_t bodyWritten = (int32_t)encoder.pos;
    tEncoderClear(&encoder);
    stmtFreeHeapPatchRowsArray(aHeapRows);
    tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);

    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pNew);
      stmtFreeUidTableMetaHash(pUidMetaHash);
      return code;
    }

    int32_t totalLen = headSz + bodyWritten;
    ((SSubmitReq2Msg*)pNew)->header.contLen = htonl(totalLen);

    taosMemoryFree(pVg->pData);
    pVg->pData = pNew;
    pVg->size = totalLen;
  }

  stmtFreeUidTableMetaHash(pUidMetaHash);
  return TSDB_CODE_SUCCESS;
}

typedef struct SStmtRetryTbPatch {
  uint64_t uid;
  uint64_t suid;
  int32_t  sver;
} SStmtRetryTbPatch;

// After refreshMeta, drop cached tbName->uid from stmt2 interlace bind so insGetStmtTableVgUid refetches from catalog.
static void stmtInvalidateStbInterlaceTableUidCache(STscStmt2* pStmt) {
  if (pStmt->sql.stbInterlaceMode && pStmt->sql.siInfo.pTableHash != NULL) {
    (void)taosThreadMutexLock(&pStmt->queue.mutex);
    tSimpleHashClear(pStmt->sql.siInfo.pTableHash);
    (void)taosThreadMutexUnlock(&pStmt->queue.mutex);
  }
}

// Super-table catalog meta uses uid == suid (see queryCreateTableMetaFromMsg); that must not be written onto
// child-table SSubmitTbData. Only use child/normal/virtual-child meta here.
static bool stmtRetryTbMetaIsSuperTable(const STableMeta* pMeta) {
  return (pMeta != NULL && pMeta->tableType == TSDB_SUPER_TABLE);
}

static int32_t stmtFetchOneRetryTbMetaPatch(STscStmt2* pStmt, SRequestObj* pRequest, SSubmitTbData* pTb, int32_t tbIdx,
                                            int32_t nSubmitTb, SStmtRetryTbPatch* pPatch, char* retryTbName) {
  if (NULL == pStmt->pCatalog) {
    int32_t c = catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog);
    if (c != TSDB_CODE_SUCCESS) {
      return c;
    }
  }

  SRequestConnInfo conn = {.pTrans = pStmt->taos->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp)};

  // 0) stb-interlace mode without USING: only the first child table is fully parsed into pRequest->tableList;
  // subsequent tables only exist in siInfo.pTableHash (key=name, value=STableVgUid). Iterate the hash to find
  // the table name whose cached (possibly stale) uid matches pTb->uid, then fetch fresh meta by name.
  // NOTE: stmtInvalidateStbInterlaceTableUidCache is called AFTER this function, so pTableHash is still intact.
  if (pStmt->sql.stbInterlaceMode && pStmt->sql.siInfo.pTableHash != NULL) {
    void*   pIter = NULL;
    int32_t iter = 0;
    while ((pIter = tSimpleHashIterate(pStmt->sql.siInfo.pTableHash, pIter, &iter)) != NULL) {
      STableVgUid* pVgUid = (STableVgUid*)pIter;
      if (pVgUid->uid != pTb->uid) {
        continue;
      }
      size_t keyLen = 0;
      char*  tbName = (char*)tSimpleHashGetKey(pIter, &keyLen);
      if (tbName == NULL || keyLen == 0 || keyLen >= TSDB_TABLE_NAME_LEN) {
        break;
      }
      char nameBuf[TSDB_TABLE_NAME_LEN] = {0};
      (void)memcpy(nameBuf, tbName, keyLen);
      if (retryTbName != NULL) {
        (void)memcpy(retryTbName, nameBuf, keyLen + 1);
      }
      SName       nm = {0};
      const char* dbname = (pRequest->pDb != NULL) ? pRequest->pDb : pStmt->taos->db;
      int32_t     nc = qCreateSName2(&nm, nameBuf, pStmt->taos->acctId, (char*)dbname, NULL, 0);
      if (nc != TSDB_CODE_SUCCESS) {
        break;
      }
      // Force-evict the stale catalog entry first: stb-interlace child tables are not in pRequest->tableList,
      // so refreshMeta does not refresh them. Without this, catalogGetTableMeta below would return the stale
      // cached uid and the retry would patch the submit with the same wrong uid.
      nc = catalogRemoveTableMeta(pStmt->pCatalog, &nm);
      if (nc != TSDB_CODE_SUCCESS) {
        return nc;
      }
      STableMeta* pMeta = NULL;
      nc = catalogGetTableMeta(pStmt->pCatalog, &conn, &nm, &pMeta);
      if (nc == TSDB_CODE_SUCCESS && pMeta != NULL) {
        if (!stmtRetryTbMetaIsSuperTable(pMeta)) {
          pPatch->uid = pMeta->uid;
          pPatch->suid = pMeta->suid;
          pPatch->sver = pMeta->sversion;
          taosMemoryFree(pMeta);
          return TSDB_CODE_SUCCESS;
        }
        taosMemoryFree(pMeta);
      } else {
        taosMemoryFreeClear(pMeta);
        if (nc != TSDB_CODE_SUCCESS) {
          return nc;
        }
      }
      break;
    }
  }

  // 0b) Non-interlace multi-VG TABLE_NOT_EXIST retry: scan exec.pBlockHash for the entry whose bind-time
  // pMeta->uid matches pTb->uid (the stale uid in the serialized submit block). exec.pBlockHash is built at
  // bind time and its pMeta->uid values are NOT changed by DROP+CREATE DDL between bind and exec, so they
  // correctly identify which table name belongs to each stale uid. Fetch fresh meta by name post-refresh.
  if (!pStmt->sql.stbInterlaceMode && pStmt->exec.pBlockHash != NULL) {
    void* pIter = taosHashIterate(pStmt->exec.pBlockHash, NULL);
    while (pIter) {
      STableDataCxt* pBlocks = *(STableDataCxt**)pIter;
      STableMeta*    pMeta2 = qGetTableMetaInDataBlock(pBlocks);
      if (pMeta2 != NULL && !stmtRetryTbMetaIsSuperTable(pMeta2) && (uint64_t)pMeta2->uid == (uint64_t)pTb->uid) {
        size_t      keyLen = 0;
        const char* key = taosHashGetKey(pIter, &keyLen);
        taosHashCancelIterate(pStmt->exec.pBlockHash, pIter);
        if (key == NULL || keyLen == 0) break;
        // key is "acctId.dbname.tname"; extract the short name after the last '.'.
        const char* tname = key;
        size_t      tnLen = keyLen;
        for (size_t i = 0; i < keyLen; ++i) {
          if (key[i] == '.') {
            tname = key + i + 1;
            tnLen = keyLen - i - 1;
          }
        }
        if (tnLen == 0 || tnLen >= TSDB_TABLE_NAME_LEN) break;
        char tnBuf[TSDB_TABLE_NAME_LEN] = {0};
        (void)memcpy(tnBuf, tname, tnLen);
        SName       nm = {0};
        const char* dbname = (pRequest->pDb != NULL) ? pRequest->pDb : pStmt->taos->db;
        int32_t     nc = qCreateSName2(&nm, tnBuf, pStmt->taos->acctId, (char*)dbname, NULL, 0);
        if (nc != TSDB_CODE_SUCCESS) break;
        // Force-evict the stale catalog entry: refreshMeta only refreshes pRequest->tableList which may
        // not cover every table touched by a multi-VG submit. Removing the cache entry guarantees the
        // fetch below goes to mnode and brings back the freshly created table's uid.
        nc = catalogRemoveTableMeta(pStmt->pCatalog, &nm);
        if (nc != TSDB_CODE_SUCCESS) {
          return nc;
        }
        STableMeta* pFresh = NULL;
        nc = catalogGetTableMeta(pStmt->pCatalog, &conn, &nm, &pFresh);
        if (nc == TSDB_CODE_SUCCESS && pFresh != NULL && !stmtRetryTbMetaIsSuperTable(pFresh)) {
          pPatch->uid = pFresh->uid;
          pPatch->suid = pFresh->suid;
          pPatch->sver = pFresh->sversion;
          taosMemoryFree(pFresh);
          return TSDB_CODE_SUCCESS;
        }
        taosMemoryFreeClear(pFresh);
        if (nc != TSDB_CODE_SUCCESS) return nc;
        break;
      }
      pIter = taosHashIterate(pStmt->exec.pBlockHash, pIter);
    }
  }

  // 1) Auto-create child: look up by child table name (never use STB-only name without child name).
  if (pTb->pCreateTbReq != NULL && pTb->pCreateTbReq->name != NULL) {
    SName         nm = {0};
    int32_t       nc = TSDB_CODE_SUCCESS;
    STableMeta*   pMeta = NULL;
    if (pStmt->bInfo.sname.type != 0) {
      tNameAssign(&nm, &pStmt->bInfo.sname);
      nc = tNameAddTbName(&nm, pTb->pCreateTbReq->name, strlen(pTb->pCreateTbReq->name));
    } else if (pRequest->tableList != NULL && taosArrayGetSize(pRequest->tableList) > 0) {
      SName* p0 = taosArrayGet(pRequest->tableList, 0);
      tNameAssign(&nm, p0);
      nc = tNameAddTbName(&nm, pTb->pCreateTbReq->name, strlen(pTb->pCreateTbReq->name));
    } else {
      STMT2_ELOG_E("retry patch: no db/sname context for createTbReq name");
      return TSDB_CODE_TDB_TABLE_NOT_EXIST;
    }
    if (nc != TSDB_CODE_SUCCESS) {
      return nc;
    }
    nc = catalogGetTableMeta(pStmt->pCatalog, &conn, &nm, &pMeta);
    if (nc != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(pMeta);
      return nc;
    }
    if (pMeta == NULL) {
      return TSDB_CODE_INTERNAL_ERROR;
    }
    if (stmtRetryTbMetaIsSuperTable(pMeta)) {
      taosMemoryFree(pMeta);
      STMT2_ELOG_E("retry patch: createTbReq resolved to super table meta (unexpected)");
      return TSDB_CODE_TDB_TABLE_NOT_EXIST;
    }
    pPatch->uid = pMeta->uid;
    pPatch->suid = pMeta->suid;
    pPatch->sver = pMeta->sversion;
    taosMemoryFree(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  // 2) request->tableList: align tbIdx with the tbIdx-th non-super-table entry (skip super table names).
  if (pRequest->tableList != NULL) {
    int32_t          nList = (int32_t)taosArrayGetSize(pRequest->tableList);
    int32_t          nonStbOrd = 0;
    for (int32_t li = 0; li < nList; ++li) {
      SName*      pName = taosArrayGet(pRequest->tableList, li);
      STableMeta* pMeta = NULL;
      int32_t     c = catalogGetTableMeta(pStmt->pCatalog, &conn, pName, &pMeta);
      if (c != TSDB_CODE_SUCCESS) {
        taosMemoryFreeClear(pMeta);
        return c;
      }
      if (pMeta == NULL) {
        return TSDB_CODE_INTERNAL_ERROR;
      }
      if (stmtRetryTbMetaIsSuperTable(pMeta)) {
        taosMemoryFree(pMeta);
        continue;
      }
      if (nonStbOrd == tbIdx) {
        pPatch->uid = pMeta->uid;
        pPatch->suid = pMeta->suid;
        pPatch->sver = pMeta->sversion;
        taosMemoryFree(pMeta);
        return TSDB_CODE_SUCCESS;
      }
      taosMemoryFree(pMeta);
      nonStbOrd++;
    }
  }

  // 3) Single-table statement: bInfo.sname
  if (nSubmitTb == 1 && pStmt->bInfo.sname.type != 0) {
    STableMeta* pMeta = NULL;
    int32_t     c = catalogGetTableMeta(pStmt->pCatalog, &conn, &pStmt->bInfo.sname, &pMeta);
    if (c != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(pMeta);
      return c;
    }
    if (pMeta == NULL) {
      return TSDB_CODE_INTERNAL_ERROR;
    }
    if (stmtRetryTbMetaIsSuperTable(pMeta)) {
      taosMemoryFree(pMeta);
      STMT2_ELOG_E("retry patch: bInfo.sname resolved to super table meta; need child table name");
      return TSDB_CODE_TDB_TABLE_NOT_EXIST;
    }
    pPatch->uid = pMeta->uid;
    pPatch->suid = pMeta->suid;
    pPatch->sver = pMeta->sversion;
    taosMemoryFree(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  STMT2_ELOG("retry patch: cannot resolve catalog meta for submit block (tb idx %d, uid %" PRId64 ")", tbIdx,
             (int64_t)pTb->uid);
  return TSDB_CODE_TDB_TABLE_NOT_EXIST;
}

static int32_t stmtBuildRetryCreateTbReq(STscStmt2* pStmt, SRequestObj* pRequest, const char* tbName,
                                          SSubmitTbData* pTb) {
  if (pStmt->pRetryTagHash == NULL || tbName == NULL || tbName[0] == '\0') {
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }

  SStmt2RetryTags** ppRetryTags =
      taosHashGet(pStmt->pRetryTagHash, tbName, strlen(tbName));
  if (ppRetryTags == NULL || *ppRetryTags == NULL) {
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }

  SStmt2RetryTags* pRetryTags = *ppRetryTags;
  SVCreateTbReq*   pCreateTbReq = NULL;
  int32_t          code = TSDB_CODE_SUCCESS;
  if (pRetryTags->fixedTags) {
    if (!pStmt->sql.fixValueTags || pStmt->sql.fixValueTbReq == NULL) {
      return TSDB_CODE_TSC_STMT_CACHE_ERROR;
    }
    code = cloneSVreateTbReq(pStmt->sql.fixValueTbReq, &pCreateTbReq);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    taosMemoryFree(pCreateTbReq->name);
    pCreateTbReq->name = taosStrdup(tbName);
    if (pCreateTbReq->name == NULL) {
      tdDestroySVCreateTbReq(pCreateTbReq);
      taosMemoryFree(pCreateTbReq);
      return terrno;
    }
  } else {
    if (pStmt->sql.siInfo.pDataCtx == NULL || pStmt->sql.siInfo.boundTags == NULL) {
      return TSDB_CODE_TSC_STMT_CACHE_ERROR;
    }
    pCreateTbReq = taosMemoryCalloc(1, sizeof(*pCreateTbReq));
    if (pCreateTbReq == NULL) {
      return terrno;
    }
    code = qBindStmtTagsValue2(pStmt->sql.siInfo.pDataCtx, pStmt->sql.siInfo.boundTags, pTb->suid,
                               pStmt->bInfo.stbFName, (char*)tbName, pRetryTags->binds, pRequest->msgBuf,
                               pRequest->msgBufLen, pStmt->taos->optionInfo.charsetCxt, pCreateTbReq);
    if (code != TSDB_CODE_SUCCESS) {
      tdDestroySVCreateTbReq(pCreateTbReq);
      taosMemoryFree(pCreateTbReq);
      return code;
    }
  }

  pCreateTbReq->uid = 0;
  pTb->uid = 0;
  pTb->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
  pTb->pCreateTbReq = pCreateTbReq;
  STMT2_DLOG("retry table %s with cached tags and auto-create request", tbName);
  return TSDB_CODE_SUCCESS;
}

// TSDB_CODE_TDB_TABLE_NOT_EXIST: refresh child table uid/suid/sver in serialized submit from catalog.
static int32_t stmtUpdateVgDataBlocksTbMetaFromCatalog(STscStmt2* pStmt, SRequestObj* pRequest) {
  if (pStmt->pVgDataBlocksForRetry == NULL || taosArrayGetSize(pStmt->pVgDataBlocksForRetry) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  const int32_t headSz = (int32_t)sizeof(SSubmitReq2Msg);
  int32_t       nBlk = (int32_t)taosArrayGetSize(pStmt->pVgDataBlocksForRetry);

  for (int32_t b = 0; b < nBlk; ++b) {
    SVgDataBlocks* pVg = *(SVgDataBlocks**)taosArrayGet(pStmt->pVgDataBlocksForRetry, b);
    if (pVg == NULL || pVg->pData == NULL || pVg->size <= headSz) {
      continue;
    }

    SDecoder     decoder = {0};
    int32_t      bodyLen = pVg->size - headSz;
    SSubmitReq2  req = {0};
    int32_t      code = 0;

    tDecoderInit(&decoder, (uint8_t*)pVg->pData + headSz, bodyLen);
    code = tDecodeSubmitReq(&decoder, &req, NULL);
    tDecoderClear(&decoder);
    if (code != TSDB_CODE_SUCCESS) {
      STMT2_ELOG("tDecodeSubmitReq failed when patching table meta for retry, code:%s", tstrerror(code));
      return code;
    }
    if (req.raw) {
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      continue;
    }

    int32_t nTb = (int32_t)taosArrayGetSize(req.aSubmitTbData);
    for (int32_t t = 0; t < nTb; ++t) {
      SStmtRetryTbPatch patch = {0};
      char              retryTbName[TSDB_TABLE_NAME_LEN] = {0};
      SSubmitTbData*    pRow = taosArrayGet(req.aSubmitTbData, t);
      code = stmtFetchOneRetryTbMetaPatch(pStmt, pRequest, pRow, t, nTb, &patch, retryTbName);
      if ((code == TSDB_CODE_TDB_TABLE_NOT_EXIST || code == TSDB_CODE_PAR_TABLE_NOT_EXIST) &&
          retryTbName[0] != '\0') {
        code = stmtBuildRetryCreateTbReq(pStmt, pRequest, retryTbName, pRow);
        if (code == TSDB_CODE_SUCCESS) {
          continue;
        }
      }
      if (code != TSDB_CODE_SUCCESS) {
        tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
        return code;
      }
      pRow->uid = (int64_t)patch.uid;
      pRow->suid = (int64_t)patch.suid;
      pRow->sver = patch.sver;
    }

    int32_t encCap = 0;
    int32_t szRet = 0;
    tEncodeSize(tEncodeSubmitReq, &req, encCap, szRet);
    if (szRet != 0) {
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t allocLen = headSz + encCap;
    void*   pNew = taosMemoryMalloc(allocLen);
    if (pNew == NULL) {
      tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);
      return terrno;
    }

    (void)memcpy(pNew, pVg->pData, headSz);
    ((SSubmitReq2Msg*)pNew)->header.vgId = htonl(pVg->vg.vgId);
    ((SSubmitReq2Msg*)pNew)->version = htobe64(1);

    SEncoder encoder = {0};
    tEncoderInit(&encoder, (uint8_t*)pNew + headSz, encCap);
    code = tEncodeSubmitReq(&encoder, &req);
    int32_t bodyWritten = (int32_t)encoder.pos;
    tEncoderClear(&encoder);
    tDestroySubmitReq(&req, TSDB_MSG_FLG_DECODE);

    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pNew);
      return code;
    }

    int32_t totalLen = headSz + bodyWritten;
    ((SSubmitReq2Msg*)pNew)->header.contLen = htonl(totalLen);

    taosMemoryFree(pVg->pData);
    pVg->pData = pNew;
    pVg->size = totalLen;
  }

  return TSDB_CODE_SUCCESS;
}

static bool stmtIsSchemaVersionRetryError(int32_t err) {
  return (bool)(NEED_CLIENT_REFRESH_TBLMETA_ERROR(err) || err == TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION);
}

static void stmtFreeTbBuf(void* buf) {
  void* pBuf = *(void**)buf;
  taosMemoryFree(pBuf);
}

static void stmtDestroyTableColArray(SArray* pCols) {
  if (pCols == NULL) {
    return;
  }
  int32_t n = (int32_t)taosArrayGetSize(pCols);
  for (int32_t i = 0; i < n; ++i) {
    SRow* p = taosArrayGetP(pCols, i);
    tRowDestroy(p);
  }
  taosArrayDestroy(pCols);
}

static void stmtFreeTbCols(void* buf) {
  SArray** p = (SArray**)buf;
  SArray*  pCols = *p;
  stmtDestroyTableColArray(pCols);
  *p = NULL;
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
  taosHashCleanup(pStmt->pRetryTagHash);
  pStmt->pRetryTagHash = NULL;
  if (pStmt->sql.fixValueTags) {
    pStmt->sql.fixValueTags = false;
    tdDestroySVCreateTbReq(pStmt->sql.fixValueTbReq);
    taosMemoryFreeClear(pStmt->sql.fixValueTbReq);
    pStmt->sql.fixValueTbReq = NULL;
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
  stmtFreeVgDataBlocksForRetry(pStmt);

  taos_free_result(pStmt->sql.siInfo.pRequest);
  if (pStmt->sql.siInfo.pVgroupList != NULL) {
    qDestroyStmtVgroupList(pStmt->sql.siInfo.pVgroupList);
    pStmt->sql.siInfo.pVgroupList = NULL;
  }
  taosHashCleanup(pStmt->sql.siInfo.pVgroupHash);
  tSimpleHashCleanup(pStmt->sql.siInfo.pTableHash);
  tSimpleHashCleanup(pStmt->sql.siInfo.pTableRowDataHash);
  taosArrayDestroyEx(pStmt->sql.siInfo.tbBuf.pBufList, stmtFreeTbBuf);
  taosMemoryFree(pStmt->sql.siInfo.pTSchema);
  qDestroyStmtDataBlock(pStmt->sql.siInfo.pDataCtx);
  taosArrayDestroyEx(pStmt->sql.siInfo.pTableCols, stmtFreeTbCols);
  pStmt->sql.siInfo.pTableCols = NULL;

  // Free field cache for columnar binding
  if (pStmt->sql.cachedFields != NULL) {
    taos_stmt2_free_fields((TAOS_STMT2*)pStmt, pStmt->sql.cachedFields);
    pStmt->sql.cachedFields = NULL;
  }
  taosMemoryFreeClear(pStmt->sql.siInfo.dbname);

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

  if (pStmt->bInfo.tbSuid != pTableMeta->suid) {
    STMT2_ELOG("table %s is in other stable, suid:0x%" PRIx64 " != 0x%" PRIx64, pStmt->bInfo.tbFName,
               pStmt->bInfo.tbSuid, pTableMeta->suid);
    taosMemoryFree(pTableMeta);
    STMT_ERR_RET(TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE);
  }

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
      if (*p != NULL) {
        stmtDestroyTableColArray(*p);
      }
      *p = taosArrayInit(20, POINTER_BYTES);
      if (*p == NULL) {
        atomic_store_32(&pStmt->errCode, terrno);
      }
    }
    pStmt->sql.siInfo.pTableColsIdx = 0;
    atomic_store_8((int8_t*)&pStmt->sql.siInfo.tableColsReady, true);
    STMT2_TLOG_E("restore pTableCols finished");
  } else {
    int64_t startUs = taosGetTimestampUs();
    pStmt->stat.asyncQueueWaitUs += startUs - pParam->enqueueUs;
    int code = qAppendStmt2TableOutput(pStmt->sql.pQuery, pStmt->sql.pVgHash, &pParam->tblData, pStmt->exec.pCurrBlock,
                                       &pStmt->sql.siInfo, pParam->pCreateTbReq);
    pStmt->stat.asyncAppendUs += taosGetTimestampUs() - startUs;
    pStmt->stat.asyncTaskNum++;
    if (code != TSDB_CODE_SUCCESS) {
      STMT2_ELOG("async append stmt output failed, tbname:%s, err:%s", pParam->tblData.tbName, tstrerror(code));
      atomic_store_32(&pStmt->errCode, code);
      if (pParam->tblData.aCol != NULL) {
        stmtDestroyTableColArray(pParam->tblData.aCol);
        pParam->tblData.aCol = NULL;
      }
    }
    (void)atomic_sub_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);
  }
}

static void* stmtBindThreadFunc(void* param) {
  setThreadName("stmt2Bind");

  STscStmt2* pStmt = (STscStmt2*)param;
  STMT2_DLOG_E("stmt2 bind thread started");

  while (true) {
    SStmtQNode* asyncParam = NULL;

    if (!stmtDequeue(pStmt, &asyncParam)) {
      if (atomic_load_8((int8_t*)&pStmt->queue.stopQueue) &&
          0 == atomic_load_64((int64_t*)&pStmt->queue.qRemainNum)) {
        STMT2_DLOG_E("queue is empty and stopQueue is set, thread will exit");
        break;
      }
      continue;
    }

    stmtAsyncOutput(pStmt, asyncParam);
  }

  STMT2_DLOG_E("stmt2 bind thread stopped");
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
  pStmt->sql.siInfo.pTableHashMutex = &pStmt->queue.mutex;
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
    const char* siDbSrc = (pStmt->db != NULL && pStmt->db[0] != '\0') ? pStmt->db : taos->db;
    code = stmtDupSiInfoDbname(&pStmt->sql.siInfo, siDbSrc);
    if (TSDB_CODE_SUCCESS != code) {
      STMT2_ELOG("fail to dup siInfo dbname in stmtInit2:%s", tstrerror(code));
      (void)stmtClose2(pStmt);
      return NULL;
    }
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
    STMT_ERR_RET(stmtDupSiInfoDbname(&pStmt->sql.siInfo, pStmt->exec.pRequest->pDb));
  }
  return TSDB_CODE_SUCCESS;
}
static int32_t stmtResetStbInterlaceCache(STscStmt2* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;

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
    atomic_store_8((int8_t*)&pStmt->queue.stopQueue, false);
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
  // Save state that needs to be preserved
  char*             db = pStmt->db;
  TAOS_STMT2_OPTION options = pStmt->options;
  uint32_t          reqid = pStmt->reqid;
  bool              stbInterlaceMode = pStmt->stbInterlaceMode;

  pStmt->errCode = 0;

  // Wait for async execution to complete
  if (pStmt->options.asyncExecFn && !pStmt->execSemWaited) {
    if (tsem_wait(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("bind param wait asyncExecSem failed");
    }
    pStmt->execSemWaited = true;
  }

  // Stop bind thread if in use (similar to stmtClose2)
  if (stbInterlaceMode && pStmt->bindThreadInUse) {
    while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
      taosUsleep(1);
    }
    (void)taosThreadMutexLock(&pStmt->queue.mutex);
    atomic_store_8((int8_t*)&pStmt->queue.stopQueue, true);
    (void)taosThreadCondBroadcast(&(pStmt->queue.waitCond));
    (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

    (void)taosThreadJoin(pStmt->bindThread, NULL);
    pStmt->bindThreadInUse = false;
    pStmt->queue.head = NULL;
    pStmt->queue.tail = NULL;
    pStmt->queue.qRemainNum = 0;

    (void)taosThreadCondDestroy(&pStmt->queue.waitCond);
    (void)taosThreadMutexDestroy(&pStmt->queue.mutex);
  }

  // NOTE: do NOT reset until asynchronous operations have completed
  stmt2LiteralCtxReset(&pStmt->ctx);
  pStmt->literal = 0;

  // Clean all SQL and execution info (stmtCleanSQLInfo already handles most cleanup)
  pStmt->bInfo.boundColsCached = false;
  pStmt->bInfo.tbNameFlag = 0; // NOTE:
  if (stbInterlaceMode) {
    pStmt->bInfo.tagsCached = false;
  }
  STMT_ERR_RET(stmtCleanSQLInfo(pStmt));

  // Reinitialize resources (similar to stmtInit2)
  if (stbInterlaceMode) {
    pStmt->sql.siInfo.transport = pStmt->taos->pAppInfo->pTransporter;
    pStmt->sql.siInfo.acctId = pStmt->taos->acctId;
    const char* siDbSrc = (db != NULL && db[0] != '\0') ? db : pStmt->taos->db;
    STMT_ERR_RET(stmtDupSiInfoDbname(&pStmt->sql.siInfo, siDbSrc));
    pStmt->sql.siInfo.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);

    if (NULL == pStmt->pCatalog) {
      STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
    }
    pStmt->sql.siInfo.pCatalog = pStmt->pCatalog;

    STMT_ERR_RET(stmtResetStbInterlaceCache(pStmt));

    int32_t code = stmtIniAsyncBind(pStmt);
    if (TSDB_CODE_SUCCESS != code) {
      STMT2_ELOG("fail to reinit async bind in stmtDeepReset:%s", tstrerror(code));
      return code;
    }
  }

  // Restore preserved state
  pStmt->db = db;
  pStmt->options = options;
  pStmt->reqid = reqid;
  pStmt->stbInterlaceMode = stbInterlaceMode;

  pStmt->sql.pTableCache = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (NULL == pStmt->sql.pTableCache) {
    STMT2_ELOG("fail to allocate memory for pTableCache in stmtDeepReset:%s", tstrerror(terrno));
    return terrno;
  }

  pStmt->bInfo.needParse = true;
  pStmt->sql.status = STMT_INIT;
  pStmt->sql.siInfo.tableColsReady = true;

  return TSDB_CODE_SUCCESS;
}

static void stmtLiteralCallback(void *param, TAOS_RES *res, int code) {
  TAOS_STMT2* stmt = (TAOS_STMT2*)param;
  STscStmt2* pStmt = (STscStmt2*)stmt;

  pStmt->ctx.code  = code; // NOTE: currently taos_stmt2_xxx is NOT thread-safe

  if (pStmt->exec.pRequest == NULL) {
    // NOTE: preparing stage for literal statement by stmt2
    //       transfer `res` which is created by buildRequest
    pStmt->exec.pRequest = res;
    // NOTE: wake up waiting thread
    tsem_post(&pStmt->ctx.sem);
  } else {
    // NOTE: executing stage for literal statement by stmt2
    if (pStmt->exec.pRequest != res) {
      // NOTE: internal logic error, not recoverable!!!
      STMT2_ELOG("%s[%d]:%s():internal logic error",
          __FILE__, __LINE__, __func__);
      abort();
    }

    int nr_fields = taos_num_fields(pStmt->exec.pRequest);
    if (nr_fields ||
        (pStmt->exec.pRequest &&
          pStmt->exec.pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT)) {
      // NOTE: literal sql statement generates a result set
      // 1. normal query with result set
      // 2. empty result when `QueryTbNotExistAsEmpty` is set
      //    and table not exists
      pStmt->ctx.has_result_set = 1;
    }

    if (pStmt->options.asyncExecFn) {
      // NOTE: user requires asynchronous execution via `taos_stmt2_init`
      // TODO: a well-defined reentrancy protection is desired, but ...

      // NOTE: `executing` and `executed` are mutually exclusive
      pStmt->ctx.executing = 0;
      pStmt->ctx.executed = 1;

      pStmt->options.asyncExecFn(pStmt->options.userdata,
          pStmt->exec.pRequest, pStmt->ctx.code);
    } else {
      // NOTE: wake up waiting thread
      tsem_post(&pStmt->ctx.sem);
    }
  }
}

static int stmtPrepareLiteral2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;

  pStmt->literal = 1;

  if (stmt2LiteralCtxInit(&pStmt->ctx)) {
    SET_ERR("out of memory");
    STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  uint64_t          connId = pStmt->taos->id;
  const char       *sql    = pStmt->sql.sqlStr;
  int64_t           reqid  = pStmt->options.reqid;

  STscObj *pObj = acquireTscObj(connId);
  if (pObj != pStmt->taos) {
    releaseTscObj(connId);
    SET_ERR("internal logic error");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR); // TODO: a new error code?
  }

  taosAsyncQueryImplWithReqid(stmt, connId, sql,
      stmtLiteralCallback, stmt, false, reqid);
  tsem_wait(&pStmt->ctx.sem);

  releaseTscObj(connId);

  if (pStmt->ctx.code == TSDB_CODE_SUCCESS) {
    pStmt->ctx.prepared = 1;
  }

  return pStmt->ctx.code;
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

  if (qIsLiteralSql(pStmt->sql.sqlStr)) {
    return stmtPrepareLiteral2(stmt);
  }

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
        STMT_ERR_RET(stmtDupSiInfoDbname(&pStmt->sql.siInfo, pStmt->exec.pRequest->pDb));
      }
    }

    int             count  = 0;
    TAOS_FIELD_ALL *fields = NULL;
    code = taos_stmt2_get_fields(stmt, &count, &fields);
    taos_stmt2_free_fields(stmt, fields);
    fields = NULL;
    STMT_ERR_RET(code);
  } else if (stmt2IsSelect(pStmt)) {
    pStmt->sql.stbInterlaceMode = false;
    STMT_ERR_RET(stmtParseSql(pStmt));
  } else {
    return stmtBuildErrorMsgWithCode(pStmt, "stmt only support 'SELECT' or 'INSERT'", TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

int stmtBindLiteral2(TAOS_STMT2 *stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  SET_ERR("no data binding required for literal sql statement");
  STMT_RET(TSDB_CODE_TSC_STMT_API_ERROR);
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
      taosArrayDestroy(pTblCols);
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
  // process tbname
  STMT_ERR_RET(stmtCreateRequest(pStmt));

  STMT_ERR_RET(qCreateSName2(&pStmt->bInfo.sname, tbName, pStmt->taos->acctId, pStmt->exec.pRequest->pDb,
                             pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen));
  STMT_ERR_RET(tNameExtractFullName(&pStmt->bInfo.sname, pStmt->bInfo.tbFName));
  tstrncpy(pStmt->bInfo.tbName, (char*)tNameGetTableName(&pStmt->bInfo.sname), TSDB_TABLE_NAME_LEN);

  if (!pStmt->sql.stbInterlaceMode || NULL == pStmt->sql.siInfo.pDataCtx) {
    STMT_ERR_RET(stmtGetFromCache(pStmt));

    if (pStmt->bInfo.needParse) {
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
  if (qDebugFlag & DEBUG_TRACE) {
    (void)stmtPrintBindv(stmt, tags, -1, true);
  }

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
    if (pStmt->sql.stbInterlaceMode && (*pDataBlock)->pData->pCreateTbReq) {
      tdDestroySVCreateTbReq((*pDataBlock)->pData->pCreateTbReq);
      taosMemoryFreeClear((*pDataBlock)->pData->pCreateTbReq);
      (*pDataBlock)->pData->pCreateTbReq = NULL;
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

bool stmt2TableExistsInCache(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  if (pStmt == NULL || !pStmt->sql.stbInterlaceMode || pStmt->sql.siInfo.pTableHash == NULL ||
      pStmt->bInfo.tbName[0] == '\0') {
    return false;
  }

  (void)taosThreadMutexLock(&pStmt->queue.mutex);
  STableVgUid* pInfo = (STableVgUid*)tSimpleHashGet(pStmt->sql.siInfo.pTableHash, pStmt->bInfo.tbName,
                                                   strlen(pStmt->bInfo.tbName));
  bool exists = pInfo != NULL && pInfo->uid != 0;
  (void)taosThreadMutexUnlock(&pStmt->queue.mutex);

  if (exists) {
    STMT2_TLOG("skip tag parsing for cached table, tbname:%s", pStmt->bInfo.tbName);
  }
  return exists;
}

int stmt2CacheRetryTags(TAOS_STMT2* stmt, TAOS_STMT2_BIND* tags, bool fixedTags) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  if (pStmt == NULL || !pStmt->sql.stbInterlaceMode || pStmt->bInfo.tbName[0] == '\0') {
    return TSDB_CODE_SUCCESS;
  }

  SStmt2RetryTags* pRetryTags = NULL;

  if (fixedTags) {
    pRetryTags = taosMemoryCalloc(1, sizeof(*pRetryTags));
    if (pRetryTags == NULL) {
      return terrno;
    }
    pRetryTags->fixedTags = true;
  } else {
    SBoundColInfo* pBoundTags = (SBoundColInfo*)pStmt->sql.siInfo.boundTags;
    if (pBoundTags == NULL || tags == NULL) {
      return TSDB_CODE_TSC_STMT_CACHE_ERROR;
    }

    int32_t numOfTags = pBoundTags->numOfBound;
    if (pBoundTags->parseredTags != NULL) {
      numOfTags -= pBoundTags->parseredTags->numOfTags;
    }
    if (numOfTags < 0) {
      return TSDB_CODE_INVALID_PARA;
    }

    size_t payloadSize = 0;
    for (int32_t i = 0; i < numOfTags; ++i) {
      TAOS_STMT2_BIND* pSrc = &tags[i];
      // Tag binding consumes one value at index 0 and historically permits num == 0.
      if (IS_INVALID_TYPE(pSrc->buffer_type)) {
        return TSDB_CODE_INVALID_PARA;
      }
      if (pSrc->is_null != NULL && pSrc->is_null[0]) {
        continue;
      }
      int32_t len = tDataTypes[pSrc->buffer_type].bytes;
      if (IS_VAR_DATA_TYPE(pSrc->buffer_type)) {
        if (pSrc->length == NULL || pSrc->length[0] < 0) {
          return TSDB_CODE_INVALID_PARA;
        }
        len = pSrc->length[0];
      }
      if (len > 0 && pSrc->buffer == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }
      payloadSize += len;
    }

    size_t bindSize = sizeof(TAOS_STMT2_BIND) * numOfTags;
    size_t dataOffset = sizeof(*pRetryTags) + bindSize + sizeof(int32_t) * numOfTags + sizeof(char) * numOfTags;
    dataOffset = (dataOffset + sizeof(int64_t) - 1) & ~(sizeof(int64_t) - 1);
    pRetryTags = taosMemoryCalloc(1, dataOffset + payloadSize);
    if (pRetryTags == NULL) {
      return terrno;
    }
    pRetryTags->numOfTags = numOfTags;

    int32_t* lengths = (int32_t*)((char*)pRetryTags->binds + bindSize);
    char*    nulls = (char*)(lengths + numOfTags);
    char*    payload = (char*)pRetryTags + dataOffset;
    for (int32_t i = 0; i < numOfTags; ++i) {
      TAOS_STMT2_BIND* pSrc = &tags[i];
      TAOS_STMT2_BIND* pDst = &pRetryTags->binds[i];
      *pDst = (TAOS_STMT2_BIND){.buffer_type = pSrc->buffer_type,
                                .length = &lengths[i],
                                .is_null = &nulls[i],
                                .num = 1};
      nulls[i] = (pSrc->is_null != NULL) ? pSrc->is_null[0] : 0;
      if (nulls[i]) {
        continue;
      }
      lengths[i] = IS_VAR_DATA_TYPE(pSrc->buffer_type) ? pSrc->length[0] : tDataTypes[pSrc->buffer_type].bytes;
      if (lengths[i] > 0) {
        pDst->buffer = payload;
        (void)memcpy(payload, pSrc->buffer, lengths[i]);
        payload += lengths[i];
      }
    }
  }

  if (pStmt->pRetryTagHash == NULL) {
    pStmt->pRetryTagHash =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (pStmt->pRetryTagHash == NULL) {
      stmtDestroyRetryTags(pRetryTags);
      return terrno;
    }
    taosHashSetFreeFp(pStmt->pRetryTagHash, stmtFreeRetryTags);
  }

  int32_t code = taosHashPut(pStmt->pRetryTagHash, pStmt->bInfo.tbName, strlen(pStmt->bInfo.tbName), &pRetryTags,
                             POINTER_BYTES);
  if (code != TSDB_CODE_SUCCESS) {
    stmtDestroyRetryTags(pRetryTags);
    return code;
  }
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
    if ((pStmt->bInfo.tbNameFlag & IS_FIXED_TAG) && !pStmt->sql.fixValueTags) {
      *pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
      if (*pCreateTbReq == NULL) {
        return terrno;
      }
      STMT_ERR_RET(qBindStmtTagsValue2(*pDataBlock, pStmt->sql.siInfo.boundTags, pStmt->bInfo.tbSuid,
                                       pStmt->bInfo.stbFName, pStmt->bInfo.sname.tname, NULL,
                                       pStmt->exec.pRequest->msgBuf, pStmt->exec.pRequest->msgBufLen,
                                       pStmt->taos->optionInfo.charsetCxt, *pCreateTbReq));
      STMT_ERR_RET(cloneSVreateTbReq(*pCreateTbReq, &pStmt->sql.fixValueTbReq));
      pStmt->sql.fixValueTags = true;
    }
    STMT2_DLOG_E("table exists; keep only the fixed-tag retry template");
    return TSDB_CODE_SUCCESS;
  }


  if ((*pDataBlock)->pData->pCreateTbReq) {
    STMT2_TLOG_E("tags are fixed, set createTbReq first time");
    STMT_ERR_RET(cloneSVreateTbReq((*pDataBlock)->pData->pCreateTbReq, &pStmt->sql.fixValueTbReq));
    pStmt->sql.fixValueTags = true;
    STMT_ERR_RET(cloneSVreateTbReq(pStmt->sql.fixValueTbReq, pCreateTbReq));
    (*pCreateTbReq)->uid = (*pDataBlock)->pMeta->vgId;

    // destroy the createTbReq in the data block
    tdDestroySVCreateTbReq((*pDataBlock)->pData->pCreateTbReq);
    taosMemoryFreeClear((*pDataBlock)->pData->pCreateTbReq);
    (*pDataBlock)->pData->pCreateTbReq = NULL;
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

  pStmt->sql.placeholderOfTags = 0;
  pStmt->sql.placeholderOfCols = 0;
  int32_t totalNum = 0;
  STMT_ERRI_JRET(qBuildStmtStbColFields(*pDataBlock, pStmt->bInfo.boundTags, pStmt->bInfo.fixedValueCols,
                                        pStmt->bInfo.tbNameFlag, &totalNum, fields, &pStmt->sql.placeholderOfTags,
                                        &pStmt->sql.placeholderOfCols));

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

  if (fieldNum != NULL) {
    *fieldNum = totalNum;
  }

  STMT2_DLOG("get insert fields totalNum:%d, tagsNum:%d, colsNum:%d", totalNum, pStmt->sql.placeholderOfTags,
             pStmt->sql.placeholderOfCols);

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

  SArray** pSlot = (SArray**)TARRAY_GET_ELEM(pStmt->sql.siInfo.pTableCols, param->tableColsIdx);
  if (pSlot == NULL || *pSlot != param->tblData.aCol) {
    STMT2_ELOG("invalid table cols slot, idx:%d, slot:%p, cols:%p", param->tableColsIdx,
               pSlot != NULL ? *pSlot : NULL, param->tblData.aCol);
    return TSDB_CODE_TSC_STMT_CACHE_ERROR;
  }

  // Transfer ownership before enqueueing. Only the producer mutates pTableCols,
  // so the worker no longer needs to scan or access a possibly reallocating array.
  *pSlot = NULL;
  (void)atomic_add_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);

  int32_t code = stmtEnqueue(pStmt, param);
  if (code != TSDB_CODE_SUCCESS) {
    (void)atomic_sub_fetch_64(&pStmt->sql.siInfo.tbRemainNum, 1);
    *pSlot = param->tblData.aCol;
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t stmtGetTableColsFromCache(STscStmt2* pStmt, SArray** pTableCols, int32_t* pTableColsIdx) {
  while (true) {
    if (pStmt->sql.siInfo.pTableColsIdx < taosArrayGetSize(pStmt->sql.siInfo.pTableCols)) {
      *pTableColsIdx = pStmt->sql.siInfo.pTableColsIdx++;
      *pTableCols = (SArray*)taosArrayGetP(pStmt->sql.siInfo.pTableCols, *pTableColsIdx);
      break;
    } else {
      SArray* pTblCols = NULL;
      for (int32_t i = 0; i < 100; i++) {
        pTblCols = taosArrayInit(20, POINTER_BYTES);
        if (NULL == pTblCols) {
          return terrno;
        }

        if (taosArrayPush(pStmt->sql.siInfo.pTableCols, &pTblCols) == NULL) {
          taosArrayDestroy(pTblCols);
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

    atomic_store_8((int8_t*)&pStmt->sql.siInfo.tableColsReady, false);

    SStmtQNode* param = NULL;
    STMT_ERR_RET(stmtAllocQNodeFromBuf(&pStmt->sql.siInfo.tbBuf, (void**)&param));
    param->restoreTbCols = true;
    param->tableColsIdx = -1;
    param->next = NULL;

    if (pStmt->sql.autoCreateTbl) {
      pStmt->bInfo.tagsCached = true;
    }
    pStmt->bInfo.boundColsCached = true;

    STMT_ERR_RET(stmtEnqueue(pStmt, param));

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

/**
 * Fetch metadata for query statement after parameter binding.
 * This function collects metadata requirements from the query (after binding),
 * fetches metadata synchronously from catalog, and returns it for parsing.
 *
 * Note: We fetch metadata on every bind because:
 * 1. Parameter values in WHERE conditions (e.g., dataname IN (?,?)) may change
 * 2. Different parameter values may require different vgroup lists for virtual tables
 * 3. Metadata requirements can only be determined after parameters are bound
 *
 * @param pStmt Statement handle
 * @param pCxt Parse context (must have catalog handle initialized)
 * @param pMetaData Output: Fetched metadata (caller responsible for cleanup)
 * @return TSDB_CODE_SUCCESS on success, error code otherwise
 */
// Callback parameter structure for synchronous catalog metadata fetch
typedef struct {
  SMetaData* pRsp;
  int32_t    code;
  tsem_t     sem;
} SCatalogSyncCbParam;

// Callback function for catalogAsyncGetAllMeta to make it synchronous
static void stmtCatalogSyncGetAllMetaCb(SMetaData* pResultMeta, void* param, int32_t code) {
  SCatalogSyncCbParam* pCbParam = (SCatalogSyncCbParam*)param;
  if (TSDB_CODE_SUCCESS == code && pResultMeta) {
    *pCbParam->pRsp = *pResultMeta;
    TAOS_MEMSET(pResultMeta, 0, sizeof(SMetaData));  // Clear to avoid double free
  }
  pCbParam->code = code;
  if (tsem_post(&pCbParam->sem) != 0) {
    tscError("failed to post semaphore");
  }
}

static int32_t stmtFetchMetadataForQuery(STscStmt2* pStmt, SParseContext* pCxt, SMetaData* pMetaData) {
  int32_t          code = 0;
  SParseMetaCache  metaCache = {0};
  SCatalogReq      catalogReq = {0};
  SRequestConnInfo conn = {.pTrans = pCxt->pTransporter,
                           .requestId = pCxt->requestId,
                           .requestObjRefId = pCxt->requestRid,
                           .mgmtEps = pCxt->mgmtEpSet};

  TAOS_MEMSET(pMetaData, 0, sizeof(SMetaData));

  code = collectMetaKey(pCxt, pStmt->sql.pQuery, &metaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCatalogReq(&metaCache, &catalogReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SCatalogSyncCbParam cbParam = {.pRsp = pMetaData, .code = TSDB_CODE_SUCCESS};
    if (tsem_init(&cbParam.sem, 0, 0) != 0) {
      code = TSDB_CODE_CTG_INTERNAL_ERROR;
    } else {
      code = catalogAsyncGetAllMeta(pCxt->pCatalog, &conn, &catalogReq, stmtCatalogSyncGetAllMetaCb, &cbParam, NULL);
      if (TSDB_CODE_SUCCESS == code) {
        code = tsem_wait(&cbParam.sem);
        if (code != TSDB_CODE_SUCCESS) {
          catalogFreeMetaData(pMetaData);
          TAOS_MEMSET(pMetaData, 0, sizeof(SMetaData));
        } else {
          code = cbParam.code;
        }
      }

      if (tsem_destroy(&cbParam.sem) != 0) {
        tscError("failed to destroy semaphore");
        code = TSDB_CODE_CTG_INTERNAL_ERROR;
        catalogFreeMetaData(pMetaData);
        TAOS_MEMSET(pMetaData, 0, sizeof(SMetaData));
      }
    }
  }

  // metaCache currently holds "reserved/request" structures built by collectMetaKey/buildCatalogReq.
  // It must be destroyed with request=true to release nested table-request hashes.
  destoryParseMetaCache(&metaCache, true);
  destoryCatalogReq(&catalogReq);

  if (TSDB_CODE_SUCCESS != code) {
    catalogFreeMetaData(pMetaData);
  }

  return code;
}

// qStmtBindParams2() clones the prepared AST before every execution. Some
// session-owned context pointers are intentionally not shared by node clone
// helpers, so restore them from the current STMT2 connection before semantic
// translation injects timezone/charset/first-day parameters.
typedef struct {
  timezone_t timezone;
  char       timezoneName[TD_TIMEZONE_LEN];
  void*      charsetCxt;
  int8_t     firstDayOfWeek;
} SStmt2QueryContext;

static void stmt2RestoreQueryContext(SNode* pRoot, SStmt2QueryContext* pCxt);

static EDealRes stmt2RestoreNodeContext(SNode* pNode, void* pContext) {
  SStmt2QueryContext* pCxt = pContext;

  switch (nodeType(pNode)) {
    case QUERY_NODE_VALUE: {
      SValueNode* pValue = (SValueNode*)pNode;
      pValue->tz = pCxt->timezone;
      pValue->charsetCxt = pCxt->charsetCxt;
      break;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOperator = (SOperatorNode*)pNode;
      if (pOperator->ownsTimezone && pOperator->tz != NULL) {
        tzfree(pOperator->tz);
      }
      pOperator->tz = pCxt->timezone;
      pOperator->ownsTimezone = false;
      tstrncpy(pOperator->timezoneName, pCxt->timezoneName, sizeof(pOperator->timezoneName));
      pOperator->charsetCxt = pCxt->charsetCxt;
      break;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pNode;
      if (pFunc->tzAllocated && pFunc->tz != NULL) {
        tzfree(pFunc->tz);
      }
      pFunc->tz = pCxt->timezone;
      pFunc->tzAllocated = false;
      tstrncpy(pFunc->tzName, pCxt->timezoneName, sizeof(pFunc->tzName));
      pFunc->charsetCxt = pCxt->charsetCxt;
      pFunc->firstDayOfWeek = pCxt->firstDayOfWeek;
      break;
    }
    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)pNode;
      pCaseWhen->tz = pCxt->timezone;
      pCaseWhen->charsetCxt = pCxt->charsetCxt;
      break;
    }
    case QUERY_NODE_TEMP_TABLE:
      stmt2RestoreQueryContext(((STempTableNode*)pNode)->pSubquery, pCxt);
      break;
    case QUERY_NODE_SELECT_STMT:
    case QUERY_NODE_SET_OPERATOR:
      stmt2RestoreQueryContext(pNode, pCxt);
      break;
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static void stmt2RestoreQueryContext(SNode* pRoot, SStmt2QueryContext* pCxt) {
  if (pRoot == NULL) {
    return;
  }

  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
      nodesWalkSelectStmt((SSelectStmt*)pRoot, SQL_CLAUSE_FROM, stmt2RestoreNodeContext, pCxt);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOper = (SSetOperator*)pRoot;
      nodesWalkExprs(pSetOper->pProjectionList, stmt2RestoreNodeContext, pCxt);
      nodesWalkExprs(pSetOper->pOrderByList, stmt2RestoreNodeContext, pCxt);
      stmt2RestoreQueryContext(pSetOper->pLeft, pCxt);
      stmt2RestoreQueryContext(pSetOper->pRight, pCxt);
      break;
    }
    default:
      break;
  }
}

int stmtBindBatch2(TAOS_STMT2* stmt, TAOS_STMT2_BIND* bind, int32_t colIdx, SVCreateTbReq* pCreateTbReq) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;

  int64_t startUs = taosGetTimestampUs();

  if (qDebugFlag & DEBUG_TRACE) {
    (void)stmtPrintBindv(stmt, bind, colIdx, false);
  }

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
    SStmt2QueryContext queryCxt = {.timezone = pStmt->taos->optionInfo.timezone,
                                   .charsetCxt = pStmt->taos->optionInfo.charsetCxt,
                                   .firstDayOfWeek = pStmt->taos->optionInfo.firstDayOfWeek};
    tstrncpy(queryCxt.timezoneName, pStmt->taos->optionInfo.timezoneName, sizeof(queryCxt.timezoneName));
    stmt2RestoreQueryContext(pStmt->sql.pQuery->pRoot, &queryCxt);

    SParseContext ctx = {.requestId = pStmt->exec.pRequest->requestId,
                         .acctId = pStmt->taos->acctId,
                         .minSecLevel = pStmt->taos->minSecLevel,
                         .maxSecLevel = pStmt->taos->maxSecLevel,
                         .db = pStmt->exec.pRequest->pDb,
                         .topicQuery = false,
                         .pSql = pStmt->sql.sqlStr,
                         .sqlLen = pStmt->sql.sqlLen,
                         .pMsg = pStmt->exec.pRequest->msgBuf,
                         .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                         .pTransporter = pStmt->taos->pAppInfo->pTransporter,
                         .pStmtCb = NULL,
                         .pUser = pStmt->taos->user,
                         .timezone = pStmt->taos->optionInfo.timezone,
                         .charsetCxt = pStmt->taos->optionInfo.charsetCxt,
                         .firstDayOfWeek = pStmt->taos->optionInfo.firstDayOfWeek,
                         .stmtBindVersion = pStmt->exec.pRequest->stmtBindVersion};
    tstrncpy(ctx.timezoneName, pStmt->taos->optionInfo.timezoneName, sizeof(ctx.timezoneName));
    ctx.mgmtEpSet = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
    code = catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &ctx.pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      goto cleanup_root;
    }

    // Fetch metadata for query(vtable need)
    SMetaData metaData = {0};
    code = stmtFetchMetadataForQuery(pStmt, &ctx, &metaData);
    if (TSDB_CODE_SUCCESS != code) {
      goto cleanup_root;
    }

    code = qStmtParseQuerySql(&ctx, pStmt->sql.pQuery, &metaData);
    if (TSDB_CODE_SUCCESS == code) {
      // Copy metaData to pRequest->parseMeta for potential future use
      // Similar to doAsyncQueryFromAnalyse when parseOnly is true
      (void)memcpy(&pStmt->exec.pRequest->parseMeta, &metaData, sizeof(SMetaData));
      (void)memset(&metaData, 0, sizeof(SMetaData));  // Clear to avoid double free
    } else {
      catalogFreeMetaData(&metaData);
      TAOS_MEMSET(&metaData, 0, sizeof(SMetaData));
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
    param->tableColsIdx = -1;
    STMT_ERR_RET(stmtGetTableColsFromCache(pStmt, &param->tblData.aCol, &param->tableColsIdx));
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

  SArray*   pCols = pStmt->sql.stbInterlaceMode ? param->tblData.aCol : (*pDataBlock)->pData->aCol;
  SBlobSet* pBlob = NULL;
  if (colIdx < 0) {
    if (pStmt->sql.stbInterlaceMode) {
      (*pDataBlock)->pData->flags &= ~SUBMIT_REQ_COLUMN_DATA_FORMAT;
      code = qBindStmtStbColsValue2(*pDataBlock, pCols, pStmt->bInfo.fixedValueCols, bind, pStmt->exec.pRequest->msgBuf,
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
        code = qBindStmtColsValue2(*pDataBlock, pCols, pStmt->bInfo.fixedValueCols, bind, pStmt->exec.pRequest->msgBuf,
                                   pStmt->exec.pRequest->msgBufLen, pStmt->taos->optionInfo.charsetCxt);
      } else {
        code =
            qBindStmt2RowValue(*pDataBlock, (*pDataBlock)->pData->aRowP, pStmt->bInfo.fixedValueCols, bind,
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
                           .userId = pTscObj->userId,
                           .pEffectiveUser = pRequest->effectiveUser,
                           .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER)),
                           .enableSysInfo = pTscObj->sysInfo,
                           .sodInitial = pTscObj->pAppInfo->serverCfg.sodInitial,
                           .privInfo = pWrapper->pParseCtx ? pWrapper->pParseCtx->privInfo : 0,
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

  // NEED_CLIENT_HANDLE_ERROR: retry internally without notifying user; retry completion uses this same cb + fp once.
  if (code != TSDB_CODE_SUCCESS && NEED_CLIENT_HANDLE_ERROR(code) && pStmt->pVgDataBlocksForRetry != NULL) {
    int32_t origExecCode = code;
    STMT2_WLOG("async exec got NEED_CLIENT_HANDLE_ERROR (code:%s), retrying internally", tstrerror(code));

    // Try to retry internally; completion uses asyncQueryCb so user fp runs once with the final result.
    int32_t retryCode = refreshMeta(pStmt->exec.pRequest->pTscObj, pStmt->exec.pRequest);
    if (origExecCode == TSDB_CODE_TDB_TABLE_NOT_EXIST || origExecCode == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      if (retryCode == TSDB_CODE_SUCCESS || retryCode == TSDB_CODE_PAR_TABLE_NOT_EXIST ||
          retryCode == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
        retryCode = stmtUpdateVgDataBlocksTbMetaFromCatalog(pStmt, pStmt->exec.pRequest);
      }
    } else if (retryCode == TSDB_CODE_SUCCESS && stmtIsSchemaVersionRetryError(origExecCode)) {
      retryCode = stmtUpdateVgDataBlocksSchemaVer(pStmt, pStmt->exec.pRequest);
    }
    stmtInvalidateStbInterlaceTableUidCache(pStmt);
    if (retryCode == TSDB_CODE_SUCCESS) {
      (void)stmtRestoreVgDataBlocksForRetry(pStmt);
      // Reuse the same pRequest so its tableList/dbList (set during initial parse) survive for
      // any subsequent refreshMeta calls. Building a fresh request here would leave those arrays
      // empty and break all future internal retries (refreshMeta returns TSDB_CODE_APP_ERROR
      // when both lists are empty).
      stmtSoftResetRequestForRetry(pStmt);
      retryCode = stmtCreateRequest(pStmt);
      if (retryCode == TSDB_CODE_SUCCESS) {
        SRequestObj*         pNewReq = pStmt->exec.pRequest;
        SSqlCallbackWrapper* pWrapper = taosMemoryCalloc(1, sizeof(SSqlCallbackWrapper));
        if (pWrapper == NULL) {
          retryCode = terrno;
          resetRequest(pStmt);
        } else {
          pWrapper->pRequest = pNewReq;
          pNewReq->pWrapper = pWrapper;
          retryCode = createParseContext(pNewReq, &pWrapper->pParseCtx, pWrapper);
          if (retryCode == TSDB_CODE_SUCCESS) {
            pNewReq->syncQuery = false;
            // Same as first exec: asyncQueryCb invokes user asyncExecFn once with userdata (not raw pStmt as fp's 1st arg).
            pNewReq->body.queryFp = asyncQueryCb;
            ((SSyncQueryParam*)(pNewReq)->body.interParam)->userParam = pStmt;
            launchAsyncQuery(pNewReq, pStmt->sql.pQuery, NULL, pWrapper);
            // Retry asyncQueryCb will call fp, stmtCleanExecInfo, and tsem_post(asyncExecSem).
            return;
          }
          // Do not taosMemoryFree(pWrapper): destroyRequest frees it via destorySqlCallbackWrapper.
          resetRequest(pStmt);
        }
      }
      if (retryCode != TSDB_CODE_SUCCESS) {
        STMT2_ELOG("retry failed, code:%d, will notify user with original error code:%d", retryCode, origExecCode);
      }
    }
    // Retry setup failed (did not return above): notify user once with the original error, then cleanup + post sem.
    if (fp) {
      fp(pStmt->options.userdata, res, code);
    }
  } else {
    if (code == TSDB_CODE_SUCCESS) {
      pStmt->exec.affectedRows = taos_affected_rows(res);
      pStmt->affectedRows += pStmt->exec.affectedRows;
    }

    if (fp) {
      fp(pStmt->options.userdata, res, code);
    }
  }

  while (0 == atomic_load_8((int8_t*)&pStmt->sql.siInfo.tableColsReady)) {
    taosUsleep(1);
  }
  (void)stmtCleanExecInfo(pStmt, (code ? false : true), false);
  ++pStmt->sql.runTimes;
  if (pStmt->exec.pRequest != NULL) {
    pStmt->exec.pRequest->inCallback = false;
  }

  if (code != TSDB_CODE_SUCCESS) {
    STMT2_ELOG("async exec failed, code:%d", code);
    pStmt->errCode = code;
  }

  if (tsem_post(&pStmt->asyncExecSem) != 0) {
    STMT2_ELOG_E("fail to post asyncExecSem");
  }
}

static int stmtExecLiteral2(TAOS_STMT2* stmt, int *affected_rows) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;

  if (pStmt->ctx.code) {
    return pStmt->ctx.code;
  }

  if (pStmt->ctx.prepared == 0) {
    SET_ERR("literal sql statement not fully prepared yet");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (pStmt->ctx.executing) {
    SET_ERR("previous execution of literal sql statement still in progress");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (pStmt->ctx.executed) {
    SET_ERR("multiple execution of a prepared literal sql statement "
        "not supported yet");
    STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  pStmt->ctx.executing = 1;

  // NOTE: triggering execution logic of a prepared literal sql statement
  taosAsyncExecLiteral(stmt);

  if (pStmt->options.asyncExecFn == NULL) {
    // NOTE: waiting for execution process to finish
    tsem_wait(&pStmt->ctx.sem);

    // NOTE: `executing` and `executed` are mutualy exclusive
    pStmt->ctx.executing = 0;
    pStmt->ctx.executed = 1;

    if (pStmt->ctx.code == TSDB_CODE_SUCCESS) {
      if (affected_rows) {
        if (pStmt->ctx.has_result_set) {
          // NOTE: literal sql statement generates a result set
          *affected_rows = 0;
        } else {
          // NOTE: literal sql statement does not generate any result set
          TAOS_RES *res = pStmt->exec.pRequest;
          *affected_rows = taos_affected_rows(res);
        }
      }
    }
    return pStmt->ctx.code;
  }

  // NOTE: what if taosAsyncExecLiteral failed prematurelly?
  return TSDB_CODE_SUCCESS;
}

int stmtExec2(TAOS_STMT2* stmt, int* affected_rows) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = 0;
  int64_t    startUs = taosGetTimestampUs();

  STMT2_DLOG_E("start to exec");

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    return pStmt->errCode;
  }

  if (stmtIsLiteral(pStmt)) {
    return stmtExecLiteral2(stmt, affected_rows);
  }

  STMT_ERR_RET(taosThreadMutexLock(&pStmt->asyncBindParam.mutex));
  while (atomic_load_8((int8_t*)&pStmt->asyncBindParam.asyncBindNum) > 0) {
    (void)taosThreadCondWait(&pStmt->asyncBindParam.waitCond, &pStmt->asyncBindParam.mutex);
  }
  STMT_ERR_RET(taosThreadMutexUnlock(&pStmt->asyncBindParam.mutex));

  if (pStmt->sql.stbInterlaceMode) {
    STMT_ERR_RET(stmtAddBatch2(pStmt));
  }

  code = stmtSwitchStatus(pStmt, STMT_EXECUTE);
  if (code != TSDB_CODE_SUCCESS) goto _return;

  if (STMT_TYPE_QUERY != pStmt->sql.type) {
    if (pStmt->sql.stbInterlaceMode) {
      int64_t startTs = taosGetTimestampUs();
      // wait for stmt bind thread to finish
      while (atomic_load_64(&pStmt->sql.siInfo.tbRemainNum)) {
        taosUsleep(1);
      }

      int32_t asyncCode = atomic_load_32(&pStmt->errCode);
      if (asyncCode != TSDB_CODE_SUCCESS) {
        return asyncCode;
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
    // Save serialized data blocks for potential NEED_CLIENT_HANDLE_ERROR retry before the planner
    // takes ownership of pDataBlocks during createQueryPlan.
    STMT_ERR_RET(stmtSaveVgDataBlocksForRetry(pStmt));
  }

  pStmt->asyncResultAvailable = false;
  SRequestObj*      pRequest = pStmt->exec.pRequest;
  __taos_async_fn_t fp = pStmt->options.asyncExecFn;
  STMT2_DLOG("EXEC INFO :req:0x%" PRIx64 ", QID:0x%" PRIx64 ", exec sql:%s,  conn:%" PRId64, pRequest->self,
             pRequest->requestId, pStmt->sql.sqlStr, pRequest->pTscObj->id);

  if (!fp) {
    launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);

    if (pStmt->exec.pRequest->code && NEED_CLIENT_HANDLE_ERROR(pStmt->exec.pRequest->code)) {
      int32_t origExecCode = pStmt->exec.pRequest->code;
      STMT2_WLOG_E("exec failed errorcode:NEED_CLIENT_HANDLE_ERROR, refresh meta and retry internally");
      code = refreshMeta(pStmt->exec.pRequest->pTscObj, pStmt->exec.pRequest);
      if (pStmt->pVgDataBlocksForRetry != NULL &&
          (origExecCode == TSDB_CODE_TDB_TABLE_NOT_EXIST || origExecCode == TSDB_CODE_PAR_TABLE_NOT_EXIST)) {
        if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_PAR_TABLE_NOT_EXIST ||
            code == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
          code = stmtUpdateVgDataBlocksTbMetaFromCatalog(pStmt, pStmt->exec.pRequest);
        }
      } else if (code == TSDB_CODE_SUCCESS && pStmt->pVgDataBlocksForRetry != NULL &&
                 stmtIsSchemaVersionRetryError(origExecCode)) {
        code = stmtUpdateVgDataBlocksSchemaVer(pStmt, pStmt->exec.pRequest);
      }
      stmtInvalidateStbInterlaceTableUidCache(pStmt);
      if (code == TSDB_CODE_SUCCESS && pStmt->pVgDataBlocksForRetry != NULL) {
        // Restore saved serialized data blocks and re-execute with refreshed meta.
        STMT_ERR_JRET(stmtRestoreVgDataBlocksForRetry(pStmt));
        // Reuse the same pRequest so its tableList/dbList survive for any subsequent
        // refreshMeta calls; building a fresh request would leave them empty.
        stmtSoftResetRequestForRetry(pStmt);
        STMT_ERR_JRET(stmtCreateRequest(pStmt));
        launchQueryImpl(pStmt->exec.pRequest, pStmt->sql.pQuery, true, NULL);
        code = pStmt->exec.pRequest->code;
      } else if (code == TSDB_CODE_SUCCESS) {
        code = pStmt->exec.pRequest->code;
      } else {
        pStmt->exec.pRequest->code = code;
        STMT2_ELOG("refresh meta and retry internally failed, code:%d, will notify user with original error code:%d",
                   code, origExecCode);
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
    stmtClearRetryTags(pStmt);
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
    atomic_store_8((int8_t*)&pStmt->queue.stopQueue, true);
    (void)taosThreadCondBroadcast(&(pStmt->queue.waitCond));
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

  /* On macOS dispatch_semaphore_dispose requires value >= orig (1). After tsem_wait above value is 0; post once before
   * destroy. */
  if (pStmt->options.asyncExecFn) {
    if (tsem_post(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("fail to post asyncExecSem");
    }
  }

  // NOTE: do NOT release until asynchronous operations have completed
  stmt2LiteralCtxRelease(&pStmt->ctx);

  STMT2_DLOG("stbInterlaceMode:%d, statInfo: ctgGetTbMetaNum=>%" PRId64 ", getCacheTbInfo=>%" PRId64
             ", parseSqlNum=>%" PRId64 ", pStmt->stat.bindDataNum=>%" PRId64
             ", settbnameAPI:%u, bindAPI:%u, addbatchAPI:%u, execAPI:%u"
             ", setTbNameUs:%" PRId64 ", bindDataUs:%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 " addBatchUs:%" PRId64
             ", execWaitUs:%" PRId64 ", execUseUs:%" PRId64 ", asyncQueueWaitUs:%" PRId64
             ", asyncBackpressureUs:%" PRId64 ", asyncAppendUs:%" PRId64 ", asyncTaskNum:%" PRId64
             ", asyncQueueHighWater:%" PRId64,
             pStmt->sql.stbInterlaceMode, pStmt->stat.ctgGetTbMetaNum, pStmt->stat.getCacheTbInfo,
             pStmt->stat.parseSqlNum, pStmt->stat.bindDataNum, pStmt->seqIds[STMT_SETTBNAME], pStmt->seqIds[STMT_BIND],
             pStmt->seqIds[STMT_ADD_BATCH], pStmt->seqIds[STMT_EXECUTE], pStmt->stat.setTbNameUs,
             pStmt->stat.bindDataUs1, pStmt->stat.bindDataUs2, pStmt->stat.bindDataUs3, pStmt->stat.bindDataUs4,
             pStmt->stat.addBatchUs, pStmt->stat.execWaitUs, pStmt->stat.execUseUs, pStmt->stat.asyncQueueWaitUs,
             pStmt->stat.asyncBackpressureUs, pStmt->stat.asyncAppendUs, pStmt->stat.asyncTaskNum,
             pStmt->stat.asyncQueueHighWater);
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

const char* stmt2Errstr(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  if (stmt && stmtIsLiteral(pStmt)) {
    if (NULL == pStmt->exec.pRequest) {
      // NOTE: since pStmt->exec.pRequest not fully prepared yet
      //       error msg is stored in pStmt->msgBuf via `SET_ERR`
      return pStmt->msgBuf;
    } else if (pStmt->exec.pRequest->msgBuf[0]) {
      // NOTE: reuse error msg stored in `msgBuf`
      return pStmt->exec.pRequest->msgBuf;
    }
    return pStmt->exec.pRequest->msgBuf;
  }

  if (stmt == NULL || NULL == pStmt->exec.pRequest) {
    return (char*)tstrerror(terrno);
  }

  // Async exec keeps request code set by callback; otherwise prefer stmt errCode over stale terrno.
  if (!(pStmt->sql.status >= STMT_EXECUTE && pStmt->options.asyncExecFn != NULL && pStmt->asyncResultAvailable)) {
    if (pStmt->errCode != TSDB_CODE_SUCCESS) {
      pStmt->exec.pRequest->code = pStmt->errCode;
    } else if (pStmt->exec.pRequest->code == TSDB_CODE_SUCCESS && terrno != TSDB_CODE_SUCCESS) {
      pStmt->exec.pRequest->code = terrno;
    }
  }

  SRequestObj* pRequest = pStmt->exec.pRequest;
  if (NULL != pRequest->msgBuf && (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR)) {
    return pRequest->msgBuf;
  }
  return (const char*)tstrerror(pRequest->code);
}

// Alias kept for compatibility with object files compiled against older headers.
const char* stmtErrstr2(TAOS_STMT2* stmt) { return stmt2Errstr(stmt); }
/*
int stmtAffectedRows(TAOS_STMT* stmt) { return ((STscStmt2*)stmt)->affectedRows; }

int stmtAffectedRowsOnce(TAOS_STMT* stmt) { return ((STscStmt2*)stmt)->exec.affectedRows; }
*/

int stmtParseColFields2(TAOS_STMT2* stmt) {
  int32_t    code = 0;
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    preCode = pStmt->errCode;

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
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t code = stmtParseColFields2(stmt);
  if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      SPureInsertParserCtx ctx = {0};
      const char *pStr   = pStmt->sql.sqlStr;
      code = qPureParseInsert(&ctx, pStr);
      if (code) {
        SET_ERR("%s", ctx.buf);
        return code;
      }
      if (nums) *nums = ctx.nr_params;
      return TSDB_CODE_SUCCESS;
    }
    return code;
  }

  return stmtFetchStbColFields2(stmt, nums, fields);
}

static void stmtClearColumnFieldCache2(STscStmt2* pStmt) {
  pStmt->sql.cachedFieldNum = 0;
  pStmt->sql.cachedIsInsert = false;
  pStmt->sql.cachedHasTbnameColumn = false;
  pStmt->sql.cachedTbnameColIdx = -1;
  if (pStmt->sql.cachedFields != NULL) {
    taos_stmt2_free_fields((TAOS_STMT2*)pStmt, pStmt->sql.cachedFields);
    pStmt->sql.cachedFields = NULL;
  }
}

int stmtEnsureColumnFieldCache2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    code = TSDB_CODE_SUCCESS;

  if (pStmt->sql.cachedFieldNum > 0 && pStmt->sql.cachedFields != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t savedStatus = pStmt->sql.status;
  int32_t savedErrCode = pStmt->errCode;
  STMT_TYPE savedType = pStmt->sql.type;
  bool savedStbInterlaceMode = pStmt->sql.stbInterlaceMode;
  bool savedAutoCreateTbl = pStmt->sql.autoCreateTbl;
  int32_t savedPlaceholderOfTags = pStmt->sql.placeholderOfTags;
  int32_t savedPlaceholderOfCols = pStmt->sql.placeholderOfCols;

  stmtClearColumnFieldCache2(pStmt);
  pStmt->sql.cachedIsInsert = stmt2IsInsert(stmt);

  if (pStmt->sql.cachedIsInsert) {
    code = stmtGetStbColFields2(stmt, &pStmt->sql.cachedFieldNum, &pStmt->sql.cachedFields);
    if (code == TSDB_CODE_SUCCESS) {
      for (int32_t i = 0; i < pStmt->sql.cachedFieldNum; ++i) {
        if (pStmt->sql.cachedFields[i].field_type == TAOS_FIELD_TBNAME) {
          pStmt->sql.cachedHasTbnameColumn = true;
          pStmt->sql.cachedTbnameColIdx = i;
          break;
        }
      }
    }

    if (pStmt->sql.siInfo.pDataCtx != NULL) {
      qDestroyStmtDataBlock(pStmt->sql.siInfo.pDataCtx);
      pStmt->sql.siInfo.pDataCtx = NULL;
    }
    pStmt->bInfo.tagsCached = false;
    pStmt->bInfo.boundColsCached = false;
    int32_t cleanCode = stmtCleanExecInfo(pStmt, false, true);
    if (code == TSDB_CODE_SUCCESS && cleanCode != TSDB_CODE_SUCCESS) {
      code = cleanCode;
    }
    qDestroyQuery(pStmt->sql.pQuery);
    pStmt->sql.pQuery = NULL;
    pStmt->sql.siInfo.pQuery = NULL;
    taosHashCleanup(pStmt->sql.pVgHash);
    pStmt->sql.pVgHash = NULL;
    taosMemoryFreeClear(pStmt->sql.pBindInfo);
  } else if (stmt2IsSelect(stmt)) {
    int32_t paramNum = 0;
    if (pStmt->sql.pQuery != NULL && pStmt->sql.pQuery->pPlaceholderValues != NULL) {
      paramNum = taosArrayGetSize(pStmt->sql.pQuery->pPlaceholderValues);
    }

    pStmt->sql.cachedFieldNum = paramNum;
    pStmt->sql.placeholderOfTags = 0;
    pStmt->sql.placeholderOfCols = paramNum;

    if (paramNum > 0) {
      pStmt->sql.cachedFields = (TAOS_FIELD_ALL*)taosMemoryCalloc(paramNum, sizeof(TAOS_FIELD_ALL));
      if (pStmt->sql.cachedFields == NULL) {
        code = terrno;
      } else {
        for (int32_t i = 0; i < paramNum; ++i) {
          SValueNode* pVal = (SValueNode*)taosArrayGetP(pStmt->sql.pQuery->pPlaceholderValues, i);
          TAOS_FIELD_ALL* pField = &pStmt->sql.cachedFields[i];
          snprintf(pField->name, sizeof(pField->name), "$%d", i + 1);
          pField->field_type = TAOS_FIELD_QUERY;
          if (pVal != NULL) {
            pField->type = pVal->node.resType.type;
            pField->precision = pVal->node.resType.precision;
            pField->scale = pVal->node.resType.scale;
            pField->bytes = pVal->node.resType.bytes;
          }
        }
      }
    }
  } else {
    code = TSDB_CODE_TSC_STMT_API_ERROR;
  }

  pStmt->sql.type = savedType;
  pStmt->sql.stbInterlaceMode = savedStbInterlaceMode;
  pStmt->sql.autoCreateTbl = savedAutoCreateTbl;
  pStmt->sql.placeholderOfTags = savedPlaceholderOfTags;
  pStmt->sql.placeholderOfCols = savedPlaceholderOfCols;
  pStmt->sql.status = savedStatus;
  pStmt->errCode = savedErrCode;

  if (code != TSDB_CODE_SUCCESS || pStmt->sql.cachedFieldNum <= 0 || pStmt->sql.cachedFields == NULL) {
    stmtClearColumnFieldCache2(pStmt);
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_INVALID_PARA : code;
  }

  STMT2_DLOG("cached column field info, fieldNum:%d, isInsert:%d, hasTbname:%d, tbnameColIdx:%d",
             pStmt->sql.cachedFieldNum, pStmt->sql.cachedIsInsert, pStmt->sql.cachedHasTbnameColumn,
             pStmt->sql.cachedTbnameColIdx);

  return TSDB_CODE_SUCCESS;
}

int stmtGetParamNum2(TAOS_STMT2* stmt, int* nums) {
  int32_t    code = 0;
  STscStmt2* pStmt = (STscStmt2*)stmt;
  int32_t    preCode = pStmt->errCode;

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

  STMT2_DLOG("get param num success, nums:%d", *nums);

_return:

  pStmt->errCode = preCode;

  return code;
}

TAOS_RES* stmtUseResult2(TAOS_STMT2* stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;

  STMT2_TLOG_E("start to use result");

  if (stmtIsLiteral(pStmt)) {
    if (pStmt->ctx.executing) {
      SET_ERR("literal sql statement still in progress");
      pStmt->errCode = TSDB_CODE_TSC_STMT_API_ERROR;
      return NULL;
    }
    if (pStmt->ctx.executed == 0) {
      SET_ERR("literal sql statement not executed yet");
      pStmt->errCode = TSDB_CODE_TSC_STMT_API_ERROR;
      return NULL;
    }
    if (!pStmt->ctx.has_result_set) {
      STMT2_ELOG_E("useResult only for query statement");
      return NULL;
    }
    return pStmt->exec.pRequest;
  }

  if (STMT_TYPE_QUERY != pStmt->sql.type) {
    STMT2_ELOG_E("useResult only for query statement");
    return NULL;
  }

  if (pStmt->options.asyncExecFn != NULL && !pStmt->asyncResultAvailable) {
    STMT2_ELOG_E("use result after callBackFn return");
    return NULL;
  }

  if (tsUseAdapter) {
    TAOS_RES* res = (TAOS_RES*)pStmt->exec.pRequest;
    pStmt->exec.pRequest = NULL;
    return res;
  }

  return pStmt->exec.pRequest;
}

int32_t stmtAsyncBindThreadFunc(void* args) {
  qInfo("async stmt bind thread started");

  ThreadArgs* targs = (ThreadArgs*)args;
  STscStmt2*  pStmt = (STscStmt2*)targs->stmt;

  int code;
  if (targs->is_columnar) {
    code = taos_stmt2_bind_param_column(targs->stmt, targs->column_bindv);
  } else {
    code = taos_stmt2_bind_param(targs->stmt, targs->bindv, targs->col_idx);
  }
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
