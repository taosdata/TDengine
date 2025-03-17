#include "qworker.h"

#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qwInt.h"
#include "qwMsg.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tname.h"

SQWorkerMgmt gQwMgmt = {
    .lock = 0,
    .qwRef = -1,
    .qwNum = 0,
};

TdThreadOnce gQueryPoolInit = PTHREAD_ONCE_INIT;
SQueryMgmt   gQueryMgmt = {0};


int32_t qwProcessHbLinkBroken(SQWorker *mgmt, SQWMsg *qwMsg, SSchedulerHbReq *req) {
  int32_t         code = 0;
  SSchedulerHbRsp rsp = {0};
  SQWSchStatus   *sch = NULL;

  QW_ERR_RET(qwAcquireScheduler(mgmt, req->clientId, QW_READ, &sch));

  QW_LOCK(QW_WRITE, &sch->hbConnLock);

  sch->hbBrokenTs = taosGetTimestampMs();

  if (qwMsg->connInfo.handle == sch->hbConnInfo.handle) {
    tmsgReleaseHandle(&sch->hbConnInfo, TAOS_CONN_SERVER, 0);
    sch->hbConnInfo.handle = NULL;
    sch->hbConnInfo.ahandle = NULL;

    QW_DLOG("release hb handle due to connection broken, handle:%p", qwMsg->connInfo.handle);
  } else {
    QW_DLOG("ignore hb connection broken, handle:%p, currentHandle:%p", qwMsg->connInfo.handle, sch->hbConnInfo.handle);
  }

  QW_UNLOCK(QW_WRITE, &sch->hbConnLock);

  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwHandleTaskComplete(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  qTaskInfo_t taskHandle = ctx->taskHandle;
  int32_t     code = TSDB_CODE_SUCCESS;

  ctx->queryExecDone = true;

  if (TASK_TYPE_TEMP == ctx->taskType && taskHandle) {
    if (ctx->explain && !ctx->explainRsped) {
      QW_ERR_JRET(qwSendExplainResponse(QW_FPARAMS(), ctx));
    }

    if (!ctx->needFetch) {
      QW_SINK_ENABLE_MEMPOOL(ctx);
      dsGetDataLength(ctx->sinkHandle, &ctx->affectedRows, NULL, NULL);
      QW_SINK_DISABLE_MEMPOOL();
    }
  }

_return:

  if ((!ctx->dynamicTask) && (!ctx->explain || ctx->explainRsped)) {
    qwFreeTaskHandle(ctx);
  }

  return code;
}

int32_t qwSendQueryRsp(QW_FPARAMS_DEF, int32_t msgType, SQWTaskCtx *ctx, int32_t rspCode, bool quickRsp) {
  if ((!quickRsp) || QUERY_RSP_POLICY_QUICK == tsQueryRspPolicy) {
    if (!ctx->localExec) {
      QW_ERR_RET(qwBuildAndSendQueryRsp(msgType, &ctx->ctrlConnInfo, rspCode, ctx));
      QW_TASK_DLOG("query msg rsped, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, rspCode, tstrerror(rspCode));
    }

    ctx->lastAckTs = taosGetTimestampSec();
    ctx->queryRsped = true;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwExecTask(QW_FPARAMS_DEF, SQWTaskCtx *ctx, bool *queryStop) {
  int32_t        code = 0;
  bool           qcontinue = true;
  uint64_t       useconds = 0;
  int32_t        i = 0;
  int32_t        execNum = 0;
  qTaskInfo_t    taskHandle = ctx->taskHandle;
  DataSinkHandle sinkHandle = ctx->sinkHandle;
  SLocalFetch    localFetch = {(void *)mgmt, ctx->localExec, qWorkerProcessLocalFetch, ctx->explainRes};

  if (ctx->queryExecDone) {
    if (queryStop) {
      *queryStop = true;
    }

    return TSDB_CODE_SUCCESS;
  }

  SArray *pResList = taosArrayInit(4, POINTER_BYTES);
  if (NULL == pResList) {
    QW_ERR_RET(terrno);
  }

  while (true) {
    QW_TASK_DLOG("start to execTask, loopIdx:%d", i++);

    // if *taskHandle is NULL, it's killed right now
    bool hasMore = false;

    if (taskHandle) {
      qwDbgSimulateSleep();

      taosEnableMemPoolUsage(ctx->memPoolSession);
      code = qExecTaskOpt(taskHandle, pResList, &useconds, &hasMore, &localFetch);
      taosDisableMemPoolUsage();

      if (code) {
        if (code != TSDB_CODE_OPS_NOT_SUPPORT) {
          QW_TASK_ELOG("qExecTask failed, code:%x - %s", code, tstrerror(code));
        } else {
          QW_TASK_DLOG("qExecTask failed, code:%x - %s", code, tstrerror(code));
        }
        QW_ERR_JRET(code);
      }
    }

    ++execNum;

    size_t numOfResBlock = taosArrayGetSize(pResList);
    for (int32_t j = 0; j < numOfResBlock; ++j) {
      SSDataBlock *pRes = taosArrayGetP(pResList, j);
      if (NULL == pRes) {
        QW_ERR_JRET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      }

      SInputData inputData = {.pData = pRes};
      QW_SINK_ENABLE_MEMPOOL(ctx);
      code = dsPutDataBlock(sinkHandle, &inputData, &qcontinue);
      QW_SINK_DISABLE_MEMPOOL();

      if (code) {
        QW_TASK_ELOG("dsPutDataBlock failed, code:%x - %s", code, tstrerror(code));
        QW_ERR_JRET(code);
      }

      QW_TASK_DLOG("data put into sink, rows:%" PRId64 ", continueExecTask:%d", pRes->info.rows, qcontinue);
    }

    if (numOfResBlock == 0 || (hasMore == false)) {
      if (!ctx->dynamicTask) {
        if (numOfResBlock == 0) {
          QW_TASK_DLOG("qExecTask end with empty res, useconds:%" PRIu64, useconds);
        } else {
          QW_TASK_DLOG("qExecTask done, useconds:%" PRIu64, useconds);
        }

        QW_ERR_JRET(qwHandleTaskComplete(QW_FPARAMS(), ctx));
      } else {
        if (numOfResBlock == 0) {
          QW_TASK_DLOG("dyn task qExecTask end with empty res, useconds:%" PRIu64, useconds);
        } else {
          QW_TASK_DLOG("dyn task qExecTask done, useconds:%" PRIu64, useconds);
        }

        ctx->queryExecDone = true;
      }

      QW_SINK_ENABLE_MEMPOOL(ctx);
      dsEndPut(sinkHandle, useconds);
      QW_SINK_DISABLE_MEMPOOL();

      if (queryStop) {
        *queryStop = true;
      }

      break;
    }

    if (!qcontinue) {
      if (queryStop) {
        *queryStop = true;
      }

      break;
    }

    if (ctx->needFetch && (!ctx->queryRsped) && execNum >= QW_DEFAULT_SHORT_RUN_TIMES) {
      break;
    }

    if (QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      break;
    }

    if (atomic_load_32(&ctx->rspCode)) {
      break;
    }
  }

_return:

  taosArrayDestroy(pResList);

  if (TSDB_CODE_SUCCESS != code) {
    qwFreeTaskHandle(ctx);
  }

  QW_RET(code);
}

bool qwTaskNotInExec(SQWTaskCtx *ctx) {
  qTaskInfo_t taskHandle = ctx->taskHandle;
  if (NULL == taskHandle || !qTaskIsExecuting(taskHandle)) {
    return true;
  }

  return false;
}

int32_t qwGenerateSchHbRsp(SQWorker *mgmt, SQWSchStatus *sch, SQWHbInfo *hbInfo) {
  int32_t taskNum = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  hbInfo->connInfo = sch->hbConnInfo;
  hbInfo->rsp.epId = sch->hbEpId;

  QW_LOCK(QW_READ, &sch->tasksLock);

  taskNum = taosHashGetSize(sch->tasksHash);

  hbInfo->rsp.taskStatus = taosArrayInit(taskNum, sizeof(STaskStatus));
  if (NULL == hbInfo->rsp.taskStatus) {
    QW_UNLOCK(QW_READ, &sch->tasksLock);
    QW_ELOG("taosArrayInit taskStatus failed, num:%d", taskNum);
    return terrno;
  }

  void       *key = NULL;
  size_t      keyLen = 0;
  int32_t     i = 0;
  STaskStatus status = {0};

  void *pIter = taosHashIterate(sch->tasksHash, NULL);
  while (pIter) {
    SQWTaskStatus *taskStatus = (SQWTaskStatus *)pIter;
    key = taosHashGetKey(pIter, &keyLen);

    // TODO GET EXECUTOR API TO GET MORE INFO

    QW_GET_QTID(key, status.queryId, status.clientId, status.taskId, status.execId);
    status.status = taskStatus->status;
    status.refId = taskStatus->refId;

    if (NULL == taosArrayPush(hbInfo->rsp.taskStatus, &status)) {
      taosHashCancelIterate(sch->tasksHash, pIter);
      code = terrno;
      break;
    }

    ++i;
    pIter = taosHashIterate(sch->tasksHash, pIter);
  }

  QW_UNLOCK(QW_READ, &sch->tasksLock);

  return code;
}

int32_t qwGetQueryResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, int32_t *dataLen, int32_t *pRawDataLen, void **rspMsg,
                              SOutputData *pOutput) {
  int64_t            len = 0;
  int64_t            rawLen = 0;
  SRetrieveTableRsp *pRsp = NULL;
  bool               queryEnd = false;
  int32_t            code = 0;
  SOutputData        output = {0};

  if (NULL == ctx->sinkHandle) {
    pOutput->queryEnd = true;
    return TSDB_CODE_SUCCESS;
  }

  *dataLen = 0;
  *pRawDataLen = 0;

  while (true) {
    QW_SINK_ENABLE_MEMPOOL(ctx);
    dsGetDataLength(ctx->sinkHandle, &len, &rawLen, &queryEnd);
    QW_SINK_DISABLE_MEMPOOL();

    if (len < 0) {
      QW_TASK_ELOG("invalid length from dsGetDataLength, length:%" PRId64 "", len);
      QW_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    if (len == 0) {
      if (queryEnd) {
        QW_SINK_ENABLE_MEMPOOL(ctx);
        code = dsGetDataBlock(ctx->sinkHandle, &output);
        QW_SINK_DISABLE_MEMPOOL();

        if (code) {
          QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
          QW_ERR_JRET(code);
        }

        QW_TASK_DLOG("no more data in sink and query end, fetched blocks %d rows %" PRId64, pOutput->numOfBlocks,
                     pOutput->numOfRows);

        if (!ctx->dynamicTask) {
          QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask));
        }

        if (NULL == pRsp) {
          QW_ERR_JRET(qwMallocFetchRsp(!ctx->localExec, len, &pRsp));
          *pOutput = output;
        } else {
          pOutput->queryEnd = output.queryEnd;
          pOutput->bufStatus = output.bufStatus;
          pOutput->useconds = output.useconds;
        }
        break;
      }

      pOutput->bufStatus = DS_BUF_EMPTY;
      break;
    }

    // Got data from sink
    QW_TASK_DLOG("there are data in sink, dataLength:%" PRId64 "", len);

    *dataLen += len + PAYLOAD_PREFIX_LEN;
    *pRawDataLen += rawLen + PAYLOAD_PREFIX_LEN;

    QW_ERR_JRET(qwMallocFetchRsp(!ctx->localExec, *dataLen, &pRsp));

    // set the serialize start position
    output.pData = pRsp->data + *dataLen - (len + PAYLOAD_PREFIX_LEN);

    ((int32_t *)output.pData)[0] = len;
    ((int32_t *)output.pData)[1] = rawLen;
    output.pData += sizeof(int32_t) * 2;

    QW_SINK_ENABLE_MEMPOOL(ctx);
    code = dsGetDataBlock(ctx->sinkHandle, &output);
    QW_SINK_DISABLE_MEMPOOL();

    if (code) {
      QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
      QW_ERR_JRET(code);
    }

    pOutput->queryEnd = output.queryEnd;
    pOutput->precision = output.precision;
    pOutput->bufStatus = output.bufStatus;
    pOutput->useconds = output.useconds;

    if (output.compressed) {
      pOutput->compressed = output.compressed;
    }

    pOutput->numOfCols = output.numOfCols;
    pOutput->numOfRows += output.numOfRows;
    pOutput->numOfBlocks++;

    if (DS_BUF_EMPTY == pOutput->bufStatus && pOutput->queryEnd) {
      QW_TASK_DLOG("task all data fetched and done, fetched blocks %d rows %" PRId64, pOutput->numOfBlocks,
                   pOutput->numOfRows);
      QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask));
      break;
    }

    if (0 == ctx->level) {
      QW_TASK_DLOG("task fetched blocks %d rows %" PRId64 ", level %d", pOutput->numOfBlocks, pOutput->numOfRows,
                   ctx->level);
      break;
    }

    if (pOutput->numOfRows >= QW_MIN_RES_ROWS) {
      QW_TASK_DLOG("task fetched blocks %d rows %" PRId64 " reaches the min rows", pOutput->numOfBlocks,
                   pOutput->numOfRows);
      break;
    }
  }

_return:

  *rspMsg = pRsp;

  return code;
}

int32_t qwGetDeleteResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, SDeleteRes *pRes) {
  int64_t     len = 0;
  int64_t     rawLen = 0;
  bool        queryEnd = false;
  int32_t     code = 0;
  SOutputData output = {0};

  QW_SINK_ENABLE_MEMPOOL(ctx);
  dsGetDataLength(ctx->sinkHandle, &len, &rawLen, &queryEnd);
  QW_SINK_DISABLE_MEMPOOL();

  if (len <= 0 || len != sizeof(SDeleterRes)) {
    QW_TASK_ELOG("invalid length from dsGetDataLength, length:%" PRId64, len);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  output.pData = taosMemoryCalloc(1, len);
  if (NULL == output.pData) {
    QW_ERR_RET(terrno);
  }

  QW_SINK_ENABLE_MEMPOOL(ctx);
  code = dsGetDataBlock(ctx->sinkHandle, &output);
  QW_SINK_DISABLE_MEMPOOL();

  if (code) {
    QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
    taosMemoryFree(output.pData);
    QW_ERR_RET(code);
  }

  SDeleterRes *pDelRes = (SDeleterRes *)output.pData;

  pRes->suid = pDelRes->suid;
  pRes->uidList = pDelRes->uidList;
  pRes->ctimeMs = taosGetTimestampMs();
  pRes->skey = pDelRes->skey;
  pRes->ekey = pDelRes->ekey;
  pRes->affectedRows = pDelRes->affectedRows;
  TAOS_STRCPY(pRes->tableFName, pDelRes->tableName);
  TAOS_STRCPY(pRes->tsColName, pDelRes->tsColName);
  taosMemoryFree(output.pData);

  return TSDB_CODE_SUCCESS;
}

int32_t qwQuickRspFetchReq(QW_FPARAMS_DEF, SQWMsg *qwMsg, int32_t code) {
  if (QUERY_RSP_POLICY_QUICK != tsQueryRspPolicy) {
    return TSDB_CODE_SUCCESS;
  }

  SQWTaskCtx *ctx = NULL;
  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  if (!QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
    goto _return;
  }

  void       *rsp = NULL;
  int32_t     dataLen = 0;
  int32_t     rawLen = 0;
  SOutputData sOutput = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rawLen, &rsp, &sOutput);
  }

  if (code) {
    qwFreeFetchRsp(rsp);
    rsp = NULL;
    dataLen = 0;
  }

  if (NULL == rsp && TSDB_CODE_SUCCESS == code) {
    goto _return;
  }

  if (NULL != rsp) {
    bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

    qwBuildFetchRsp(rsp, &sOutput, dataLen, rawLen, qComplete);
    if (qComplete) {
      atomic_store_8((int8_t *)&ctx->queryEnd, true);
      if (!ctx->dynamicTask) {
        qwFreeSinkHandle(ctx);
      }
    }
  }

  qwMsg->connInfo = ctx->dataConnInfo;
  QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);

  QW_ERR_JRET(qwBuildAndSendFetchRsp(ctx, ctx->fetchMsgType + 1, &qwMsg->connInfo, rsp, dataLen, code));
  rsp = NULL;

  QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code, tstrerror(code),
               dataLen);

_return:

  if (ctx) {
    qwReleaseTaskCtx(mgmt, ctx);
  }

  return code;
}

int32_t qwStartDynamicTaskNewExec(QW_FPARAMS_DEF, SQWTaskCtx *ctx, SQWMsg *qwMsg) {
#if 0
  if (!atomic_val_compare_exchange_8((int8_t*)&ctx->queryExecDone, true, false)) {
    QW_TASK_ELOG("dynamic task prev exec not finished, execDone:%d", ctx->queryExecDone);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }
  if (!atomic_val_compare_exchange_8((int8_t*)&ctx->queryEnd, true, false)) {
    QW_TASK_ELOG("dynamic task prev exec not finished, queryEnd:%d", ctx->queryEnd);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }
#else
  ctx->queryExecDone = false;
  ctx->queryEnd = false;
#endif

  QW_SINK_ENABLE_MEMPOOL(ctx);
  dsReset(ctx->sinkHandle);
  QW_SINK_DISABLE_MEMPOOL();

  qUpdateOperatorParam(ctx->taskHandle, qwMsg->msg);

  QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);

  if (QW_QUERY_RUNNING(ctx)) {
    atomic_store_8((int8_t *)&ctx->queryContinue, 1);
    QW_TASK_DLOG("the %dth dynamic task exec started, continue running", ctx->dynExecId++);
  } else if (0 == atomic_load_8((int8_t *)&ctx->queryInQueue)) {
    atomic_store_8((int8_t *)&ctx->queryInQueue, 1);
    QW_TASK_DLOG("the %dth dynamic task exec started", ctx->dynExecId++);
    QW_ERR_RET(qwBuildAndSendCQueryMsg(QW_FPARAMS(), &qwMsg->connInfo));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwHandlePrePhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t     code = 0;
  SQWTaskCtx *ctx = NULL;

  QW_TASK_DLOG("start to handle event at phase %s", qwPhaseStr(phase));

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (ctx->pJobInfo && (atomic_load_8(&ctx->pJobInfo->retired) || atomic_load_32(&ctx->pJobInfo->errCode))) {
    QW_TASK_ELOG("job already failed, error:%s", tstrerror(ctx->pJobInfo->errCode));
    QW_ERR_JRET(ctx->pJobInfo->errCode);
  }

  switch (phase) {
    case QW_PHASE_PRE_QUERY: {
      if (atomic_load_8((int8_t *)&ctx->queryEnd) && !ctx->dynamicTask) {
        QW_TASK_ELOG("query already end, phase:%d", phase);
        QW_ERR_JRET(TSDB_CODE_QW_MSG_ERROR);
      }
      
      if (QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_ELOG("task already dropped at phase %s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_STATUS_ERROR);
      }

      if (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        // qwBuildAndSendDropRsp(&ctx->ctrlConnInfo, code);
        // QW_TASK_DLOG("drop rsp send, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, code, tstrerror(code));

        QW_ERR_JRET(ctx->rspCode);
      }

      if (TSDB_CODE_SUCCESS != input->code) {
        QW_TASK_ELOG("task already failed at phase %s, code:0x%x", qwPhaseStr(phase), input->code);
        ctx->ctrlConnInfo.handle = NULL;
        (void)qwDropTask(QW_FPARAMS());

        QW_ERR_JRET(input->code);
      }

      QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXEC, ctx->dynamicTask));
      break;
    }
    case QW_PHASE_PRE_FETCH: {
      if (atomic_load_8((int8_t *)&ctx->queryEnd) && !ctx->dynamicTask) {
        QW_TASK_ELOG("query already end, phase:%d", phase);
        QW_ERR_JRET(TSDB_CODE_QW_MSG_ERROR);
      }
      
      if (QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP) || QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task dropping or already dropped, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(ctx->rspCode);
      }

      if (QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
        QW_TASK_WLOG("last fetch still not processed, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATED_OPERATION);
      }

      if (ctx->rspCode) {
        QW_TASK_ELOG("task already failed cause of %s, phase:%s", tstrerror(ctx->rspCode), qwPhaseStr(phase));
        QW_ERR_JRET(ctx->rspCode);
      }

      if (!ctx->queryRsped) {
        QW_TASK_ELOG("ready msg has not been processed, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_MSG_ERROR);
      }
      break;
    }
    case QW_PHASE_PRE_CQUERY: {
      if (atomic_load_8((int8_t *)&ctx->queryEnd) && !ctx->dynamicTask) {
        QW_TASK_ELOG("query already end, phase:%d", phase);
        code = ctx->rspCode;
        goto _return;
      }
      
      if (QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task already dropped, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(ctx->rspCode);
      }

      if (ctx->rspCode) {
        QW_TASK_ELOG("task already failed cause of %s, phase:%s", tstrerror(ctx->rspCode), qwPhaseStr(phase));
        QW_ERR_JRET(ctx->rspCode);
      }

      if (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        // qwBuildAndSendDropRsp(&ctx->ctrlConnInfo, code);
        // QW_TASK_DLOG("drop rsp send, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, code, tstrerror(code));

        QW_ERR_JRET(ctx->rspCode);
      }

      break;
    }
    default:
      QW_TASK_ELOG("invalid phase %s", qwPhaseStr(phase));
      QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  if (ctx->rspCode) {
    QW_TASK_ELOG("task already failed at phase %s, code:%s", qwPhaseStr(phase), tstrerror(ctx->rspCode));
    QW_ERR_JRET(ctx->rspCode);
  }

  QW_SET_PHASE(ctx, phase);

_return:

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);

    if (QW_PHASE_PRE_CQUERY == phase && code) {
      QW_SET_PHASE(ctx, QW_PHASE_POST_CQUERY);
    }

    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_QRY_TASK_CTX_NOT_EXIST) {
    QW_TASK_ELOG("end to handle event at phase %s, code:%s", qwPhaseStr(phase), tstrerror(code));
  } else {
    QW_TASK_DLOG("end to handle event at phase %s, code:%s", qwPhaseStr(phase), tstrerror(code));
  }

  QW_RET(code);
}

int32_t qwHandlePostPhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t        code = 0;
  SQWTaskCtx    *ctx = NULL;
  SRpcHandleInfo connInfo = {0};

  QW_TASK_DLOG("start to handle event at phase %s", qwPhaseStr(phase));

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already dropped, phase:%s", qwPhaseStr(phase));
    QW_ERR_JRET(ctx->rspCode);
  }

  if (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    if (QW_PHASE_POST_FETCH != phase || ((!QW_QUERY_RUNNING(ctx)) && qwTaskNotInExec(ctx))) {
      QW_ERR_JRET(qwDropTask(QW_FPARAMS()));
      QW_ERR_JRET(ctx->rspCode);
    }
  }

  if (ctx->rspCode) {
    QW_TASK_ELOG("task already failed, phase %s, error:%x - %s", qwPhaseStr(phase), ctx->rspCode,
                 tstrerror(ctx->rspCode));
    QW_ERR_JRET(ctx->rspCode);
  }

  QW_ERR_JRET(input->code);

_return:

  if (TSDB_CODE_SUCCESS == code && QW_PHASE_POST_QUERY == phase) {
    code = qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_PART_SUCC, ctx->dynamicTask);
    if (code == TSDB_CODE_QRY_TASK_SUCC_TO_PARTSUSS && ctx->queryRsped) {
      QW_TASK_DLOG("skip error: %s. ", tstrerror(code));
      code = TSDB_CODE_SUCCESS;
    }
    ctx->queryGotData = true;
  }

  if (QW_PHASE_POST_QUERY == phase && ctx && !ctx->queryRsped) {
    bool   rsped = false;
    SQWMsg qwMsg = {.msgType = ctx->queryMsgType, .connInfo = ctx->ctrlConnInfo};
    qwDbgSimulateRedirect(&qwMsg, ctx, &rsped);
    qwDbgSimulateDead(QW_FPARAMS(), ctx, &rsped);
    if (!rsped) {
      int32_t newCode = qwSendQueryRsp(QW_FPARAMS(), input->msgType + 1, ctx, code, false);
      if (TSDB_CODE_SUCCESS != newCode && TSDB_CODE_SUCCESS == code) {
        code = newCode;
      }
    }
  }

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);

    if (QW_PHASE_POST_CQUERY != phase) {
      QW_SET_PHASE(ctx, phase);
    }

    if (code) {
      (void)qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAIL,
                               ctx->dynamicTask);  // already in error, ignore new error
    }

    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_TASK_DLOG("end to handle event at phase %s, code:%x - %s", qwPhaseStr(phase), code, tstrerror(code));

  QW_RET(code);
}

int32_t qwAbortPrerocessQuery(QW_FPARAMS_DEF) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SQWTaskCtx *ctx = NULL;

  QW_ERR_RET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);
  QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

_return:

  QW_UNLOCK(QW_WRITE, &ctx->lock);    

  return code;
}

int32_t qwPreprocessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SQWTaskCtx *ctx = NULL;

  QW_ERR_JRET(qwRegisterQueryBrokenLinkArg(QW_FPARAMS(), &qwMsg->connInfo));

  QW_ERR_JRET(qwAddTaskCtx(QW_FPARAMS()));

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP) || QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task dropping or already dropped, drop event:%d", QW_GET_EVENT(ctx, QW_EVENT_DROP));
    QW_ERR_JRET(ctx->rspCode);
  }

  ctx->ctrlConnInfo = qwMsg->connInfo;
  ctx->sId = sId;
  ctx->phase = -1;

  if (NULL != gMemPoolHandle) {
    QW_ERR_JRET(qwInitSession(QW_FPARAMS(), ctx, &ctx->memPoolSession));
  }

  QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_INIT));

  QW_ERR_JRET(qwSendQueryRsp(QW_FPARAMS(), qwMsg->msgType + 1, ctx, code, true));

_return:

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);
    if (code) {
      (void)qwDropTask(QW_FPARAMS());
    }

    QW_UNLOCK(QW_WRITE, &ctx->lock);    
    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_TASK_DLOG("task preprocess %s, code:%s", code ? "failed": "succeed", tstrerror(code));

  return code;
}

int32_t qwProcessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg, char *sql) {
  int32_t        code = 0;
  SSubplan      *plan = NULL;
  SQWPhaseInput  input = {.code = qwMsg->code};
  qTaskInfo_t    pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx    *ctx = NULL;

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  ctx->taskType = qwMsg->msgInfo.taskType;
  ctx->explain = qwMsg->msgInfo.explain;
  ctx->needFetch = qwMsg->msgInfo.needFetch;
  ctx->queryMsgType = qwMsg->msgType;
  ctx->localExec = false;

  taosEnableMemPoolUsage(ctx->memPoolSession);
  code = qMsgToSubplan(qwMsg->msg, qwMsg->msgLen, &plan);
  taosDisableMemPoolUsage();

  if (TSDB_CODE_SUCCESS != code) {
    QW_TASK_ELOG("task physical plan to subplan failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  taosEnableMemPoolUsage(ctx->memPoolSession);
  code = qCreateExecTask(qwMsg->node, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, qwMsg->msgInfo.compressMsg, sql,
                         OPTR_EXEC_MODEL_BATCH);
  taosDisableMemPoolUsage();

  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    qDestroyTask(pTaskInfo);
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    qDestroyTask(pTaskInfo);
    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  (void)atomic_add_fetch_64(&gQueryMgmt.stat.taskRunNum, 1);

  uint64_t flags = 0;
  (void)dsGetSinkFlags(sinkHandle, &flags);

  ctx->level = plan->level;
  ctx->dynamicTask = qIsDynamicExecTask(pTaskInfo);
  ctx->sinkWithMemPool = flags & DS_FLAG_USE_MEMPOOL;
  atomic_store_ptr(&ctx->taskHandle, pTaskInfo);
  atomic_store_ptr(&ctx->sinkHandle, sinkHandle);

  QW_ERR_JRET(qwSaveTbVersionInfo(pTaskInfo, ctx));

  if (!ctx->dynamicTask) {
    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, NULL));
  } else {
    ctx->queryExecDone = true;
    ctx->queryEnd = true;
  }

_return:

  taosMemoryFree(sql);

  input.code = code;
  input.msgType = qwMsg->msgType;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_QUERY, &input, NULL);

  QW_ERR_RET(qwQuickRspFetchReq(QW_FPARAMS(), qwMsg, code));

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessCQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  SQWTaskCtx   *ctx = NULL;
  int32_t       code = 0;
  SQWPhaseInput input = {0};
  void         *rsp = NULL;
  int32_t       dataLen = 0;
  int32_t       rawLen = 0;
  bool          queryStop = false;
  bool          qComplete = false;

  do {
    ctx = NULL;

    QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_CQUERY, &input, NULL));

    QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

    atomic_store_8((int8_t *)&ctx->queryInQueue, 0);
    atomic_store_8((int8_t *)&ctx->queryContinue, 0);

    if (!queryStop) {
      QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, &queryStop));
    }

    if (QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      SOutputData sOutput = {0};
      QW_ERR_JRET(qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rawLen, &rsp, &sOutput));

      if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {
        QW_TASK_DLOG("task not end and buf is %s, need to continue query", qwBufStatusStr(sOutput.bufStatus));

        atomic_store_8((int8_t *)&ctx->queryContinue, 1);
      }

      if (rsp) {
        qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

        qwBuildFetchRsp(rsp, &sOutput, dataLen, rawLen, qComplete);
        if (qComplete) {
          atomic_store_8((int8_t *)&ctx->queryEnd, true);
          atomic_store_8((int8_t *)&ctx->queryContinue, 0);
          if (!ctx->dynamicTask) {
            qwFreeSinkHandle(ctx);
          }
        }

        qwMsg->connInfo = ctx->dataConnInfo;
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);

        QW_ERR_JRET(qwBuildAndSendFetchRsp(ctx, ctx->fetchMsgType + 1, &qwMsg->connInfo, rsp, dataLen, code));
        rsp = NULL;

        QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code,
                     tstrerror(code), dataLen);
      } else {
        atomic_store_8((int8_t *)&ctx->queryContinue, 1);
      }
    }

  _return:

    if (NULL == ctx) {
      break;
    }

    qwFreeFetchRsp(rsp);
    rsp = NULL;

    if (code && QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);

      qwMsg->connInfo = ctx->dataConnInfo;
      code = qwBuildAndSendFetchRsp(ctx, ctx->fetchMsgType + 1, &qwMsg->connInfo, NULL, 0, code);
      if (TSDB_CODE_SUCCESS != code) {
        QW_TASK_ELOG("fetch rsp send fail, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code,
                     tstrerror(code), 0);
      } else {
        QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code,
                     tstrerror(code), 0);
      }
    }

    QW_LOCK(QW_WRITE, &ctx->lock);
    if (atomic_load_8((int8_t *)&ctx->queryEnd) || (queryStop && (0 == atomic_load_8((int8_t *)&ctx->queryContinue))) ||
        code) {
      // Note: query is not running anymore
      QW_SET_PHASE(ctx, QW_PHASE_POST_CQUERY);
      QW_UNLOCK(QW_WRITE, &ctx->lock);
      break;
    }
    QW_UNLOCK(QW_WRITE, &ctx->lock);
    queryStop = false;
  } while (true);

  input.code = code;
  QW_ERR_RET(qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_CQUERY, &input, NULL));

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessFetch(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t code = 0;
  int32_t dataLen = 0;
  int32_t rawDataLen = 0;

  bool          locked = false;
  SQWTaskCtx   *ctx = NULL;
  void         *rsp = NULL;
  SQWPhaseInput input = {0};

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  ctx->fetchMsgType = qwMsg->msgType;
  ctx->dataConnInfo = qwMsg->connInfo;

  if (qwMsg->msg) {
    code = qwStartDynamicTaskNewExec(QW_FPARAMS(), ctx, qwMsg);
    goto _return;
  }

  SOutputData sOutput = {0};
  QW_ERR_JRET(qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rawDataLen, &rsp, &sOutput));

  if (NULL == rsp) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);
  } else {
    bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

    qwBuildFetchRsp(rsp, &sOutput, dataLen, rawDataLen, qComplete);
    if (qComplete) {
      atomic_store_8((int8_t *)&ctx->queryEnd, true);
      if (!ctx->dynamicTask) {
        qwFreeSinkHandle(ctx);
      }
    }
  }

  if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {
    QW_TASK_DLOG("task not end and buf is %s, need to continue query", qwBufStatusStr(sOutput.bufStatus));

    QW_LOCK(QW_WRITE, &ctx->lock);
    locked = true;

    // RC WARNING
    if (-1 == ctx->phase || false == ctx->queryGotData) {
      QW_TASK_DLOG_E("task query unfinished");
    } else if (QW_QUERY_RUNNING(ctx)) {
      atomic_store_8((int8_t *)&ctx->queryContinue, 1);
    } else if (0 == atomic_load_8((int8_t *)&ctx->queryInQueue)) {
      QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXEC, ctx->dynamicTask));
      atomic_store_8((int8_t *)&ctx->queryInQueue, 1);

      QW_ERR_JRET(qwBuildAndSendCQueryMsg(QW_FPARAMS(), &qwMsg->connInfo));
    }
  }

_return:

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  input.code = code;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_FETCH, &input, NULL);

  if (code) {
    qwFreeFetchRsp(rsp);
    rsp = NULL;
    dataLen = 0;
  }

  if (code || rsp) {
    bool rsped = false;

    ctx = NULL;
    (void)qwAcquireTaskCtx(QW_FPARAMS(), &ctx);

    if (ctx) {
      qwDbgSimulateRedirect(qwMsg, ctx, &rsped);
      qwDbgSimulateDead(QW_FPARAMS(), ctx, &rsped);
    }

    if (!rsped && ctx) {
      code = qwBuildAndSendFetchRsp(ctx, qwMsg->msgType + 1, &qwMsg->connInfo, rsp, dataLen, code);
      if (TSDB_CODE_SUCCESS != code) {
        QW_TASK_ELOG("fetch rsp send fail, msgType:%s, handle:%p, code:%x - %s, dataLen:%d",
                     TMSG_INFO(qwMsg->msgType + 1), qwMsg->connInfo.handle, code, tstrerror(code), dataLen);
      } else {
        QW_TASK_DLOG("fetch rsp send, msgType:%s, handle:%p, code:%x - %s, dataLen:%d", TMSG_INFO(qwMsg->msgType + 1),
                     qwMsg->connInfo.handle, code, tstrerror(code), dataLen);
      }
    } else {
      qwFreeFetchRsp(rsp);
      rsp = NULL;
    }

    qwReleaseTaskCtx(mgmt, ctx);    
  } else {
    // qwQuickRspFetchReq(QW_FPARAMS(), ctx, qwMsg, code);
  }

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessDrop(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t     code = 0;
  bool        dropped = false;
  SQWTaskCtx *ctx = NULL;
  bool        locked = false;

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  locked = true;

  if (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG_E("task already dropping");
    QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATED_OPERATION);
  }

  if (QW_QUERY_RUNNING(ctx)) {
    QW_ERR_JRET(qwKillTaskHandle(ctx, TSDB_CODE_TSC_QUERY_CANCELLED));
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_DROP, ctx->dynamicTask));
  } else {
    QW_ERR_JRET(qwDropTask(QW_FPARAMS()));
    dropped = true;
  }

  if (!dropped) {
    QW_UPDATE_RSP_CODE(ctx, TSDB_CODE_TSC_QUERY_CANCELLED);
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
  }

_return:

  if (code) {
    if (ctx) {
      QW_UPDATE_RSP_CODE(ctx, code);
      (void)qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAIL,
                               ctx->dynamicTask);  // task already failed, no more error handling
    } else {
      tmsgReleaseHandle(&qwMsg->connInfo, TAOS_CONN_SERVER, code);
    }
  }

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  if (ctx) {
    if (qwMsg->connInfo.handle != ctx->ctrlConnInfo.handle) {
      tmsgReleaseHandle(&qwMsg->connInfo, TAOS_CONN_SERVER, 0);
    }

    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessNotify(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t     code = 0;
  SQWTaskCtx *ctx = NULL;
  bool        locked = false;

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  locked = true;

  if (QW_QUERY_RUNNING(ctx)) {
    QW_ERR_JRET(qwKillTaskHandle(ctx, TSDB_CODE_TSC_QUERY_CANCELLED));
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask));
  }

  switch (qwMsg->msgType) {
    case TASK_NOTIFY_FINISHED:
      if (ctx->explain && !ctx->explainRsped) {
        QW_ERR_RET(qwSendExplainResponse(QW_FPARAMS(), ctx));
      }
      break;
    default:
      QW_ELOG("Invalid task notify type %d", qwMsg->msgType);
      QW_ERR_JRET(TSDB_CODE_INVALID_MSG);
      break;
  }

_return:

  if (code) {
    if (ctx) {
      QW_UPDATE_RSP_CODE(ctx, code);
      (void)qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAIL,
                               ctx->dynamicTask);  // task already failed, no more error handling
    }
  }

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  if (ctx) {
    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessHb(SQWorker *mgmt, SQWMsg *qwMsg, SSchedulerHbReq *req) {
  int32_t         code = 0;
  SSchedulerHbRsp rsp = {0};
  SQWSchStatus   *sch = NULL;

  if (qwMsg->code) {
    QW_RET(qwProcessHbLinkBroken(mgmt, qwMsg, req));
  }

  QW_ERR_JRET(qwAcquireAddScheduler(mgmt, req->clientId, QW_READ, &sch));
  QW_ERR_JRET(qwRegisterHbBrokenLinkArg(mgmt, req->clientId, &qwMsg->connInfo));

  sch->hbBrokenTs = 0;

  QW_LOCK(QW_WRITE, &sch->hbConnLock);

  if (sch->hbConnInfo.handle) {
    tmsgReleaseHandle(&sch->hbConnInfo, TAOS_CONN_SERVER, 0);
    sch->hbConnInfo.handle = NULL;
  }

  (void)memcpy(&sch->hbConnInfo, &qwMsg->connInfo, sizeof(qwMsg->connInfo));
  (void)memcpy(&sch->hbEpId, &req->epId, sizeof(req->epId));

  QW_UNLOCK(QW_WRITE, &sch->hbConnLock);

  QW_DLOG("hb connection updated, clientId:%" PRIx64 ", nodeId:%d, fqdn:%s, port:%d, handle:%p, ahandle:%p",
          req->clientId, req->epId.nodeId, req->epId.ep.fqdn, req->epId.ep.port, qwMsg->connInfo.handle,
          qwMsg->connInfo.ahandle);

  qwReleaseScheduler(QW_READ, mgmt);

_return:

  (void)memcpy(&rsp.epId, &req->epId, sizeof(req->epId));
  code = qwBuildAndSendHbRsp(&qwMsg->connInfo, &rsp, code);

  if (code) {
    tmsgReleaseHandle(&qwMsg->connInfo, TAOS_CONN_SERVER, 0);
    qwMsg->connInfo.handle = NULL;
  }

  /*QW_DLOG("hb rsp send, handle:%p, code:%x - %s", qwMsg->connInfo.handle, code, tstrerror(code));*/

  QW_RET(TSDB_CODE_SUCCESS);
}

void qwProcessHbTimerEvent(void *param, void *tmrId) {
  SQWHbParam *hbParam = (SQWHbParam *)param;
  if (hbParam->qwrId != atomic_load_32(&gQwMgmt.qwRef)) {
    return;
  }

  int64_t   refId = hbParam->refId;
  SQWorker *mgmt = qwAcquire(refId);
  if (NULL == mgmt) {
    QW_DLOG("qwAcquire %" PRIx64 "failed, code:0x%x", refId, terrno);
    return;
  }

  SQWSchStatus *sch = NULL;
  SQWHbInfo    *rspList = NULL;
  SArray       *pExpiredSch = NULL;
  int32_t       code = 0;
  int32_t       currTs = taosGetTimestampSec();

  qwDbgDumpMgmtInfo(mgmt);
  qwDbgDumpJobsInfo();

  if (gQWDebug.forceStop) {
    qwStopAllTasks(mgmt);
  }

  if (mgmt->lastChkTs > 0 && (currTs - mgmt->lastChkTs) >= QW_DEFAULT_TIMEOUT_INTERVAL_SECS) {
    qwChkDropTimeoutQuery(mgmt, currTs);
    mgmt->lastChkTs = currTs;
  }

  QW_LOCK(QW_READ, &mgmt->schLock);

  int32_t schNum = taosHashGetSize(mgmt->schHash);
  if (schNum <= 0) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    if (taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer)) {
      qError("reset qworker hb timer error, timer stoppped");
    }
    (void)qwRelease(refId);  // ignore error
    return;
  }

  rspList = taosMemoryCalloc(schNum, sizeof(SQWHbInfo));
  pExpiredSch = taosArrayInit(schNum, sizeof(uint64_t));
  if (NULL == rspList || NULL == pExpiredSch) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    taosMemoryFree(rspList);
    taosArrayDestroy(pExpiredSch);
    QW_ELOG("calloc %d SQWHbInfo failed, code:%x", schNum, terrno);
    if (taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer)) {
      qError("reset qworker hb timer error, timer stoppped");
    }
    (void)qwRelease(refId);  // ignore error
    return;
  }

  int32_t i = 0;
  int64_t currentMs = taosGetTimestampMs();

  void *pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    SQWSchStatus *sch1 = (SQWSchStatus *)pIter;
    if (NULL == sch1->hbConnInfo.handle) {
      uint64_t *clientId = taosHashGetKey(pIter, NULL);
      QW_TLOG("cancel send hb to client %" PRIx64 " cause of no connection handle", *clientId);

      if (sch1->hbBrokenTs > 0 && ((currentMs - sch1->hbBrokenTs) > QW_SCH_TIMEOUT_MSEC) &&
          taosHashGetSize(sch1->tasksHash) <= 0) {
        if (NULL == taosArrayPush(pExpiredSch, clientId)) {
          QW_ELOG("add clientId 0x%" PRIx64 " to expiredSch failed, code:%x", *clientId, terrno);
          taosHashCancelIterate(mgmt->schHash, pIter);
          break;
        }
      }

      pIter = taosHashIterate(mgmt->schHash, pIter);
      continue;
    }

    code = qwGenerateSchHbRsp(mgmt, (SQWSchStatus *)pIter, &rspList[i]);
    if (code) {
      taosHashCancelIterate(mgmt->schHash, pIter);
      QW_ERR_JRET(code);
    }

    ++i;
    pIter = taosHashIterate(mgmt->schHash, pIter);
  }

_return:

  QW_UNLOCK(QW_READ, &mgmt->schLock);

  for (int32_t j = 0; j < i; ++j) {
    (void)qwBuildAndSendHbRsp(&rspList[j].connInfo, &rspList[j].rsp, code);  // ignore error
    /*QW_DLOG("hb rsp send, handle:%p, code:%x - %s, taskNum:%d", rspList[j].connInfo.handle, code, tstrerror(code),*/
    /*(rspList[j].rsp.taskStatus ? (int32_t)taosArrayGetSize(rspList[j].rsp.taskStatus) : 0));*/
    tFreeSSchedulerHbRsp(&rspList[j].rsp);
  }

  if (taosArrayGetSize(pExpiredSch) > 0) {
    qwClearExpiredSch(mgmt, pExpiredSch);
  }

  taosMemoryFreeClear(rspList);
  taosArrayDestroy(pExpiredSch);

  if (taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer)) {
    qError("reset qworker hb timer error, timer stoppped");
  }

  (void)qwRelease(refId);  // ignore error
}

int32_t qwProcessDelete(QW_FPARAMS_DEF, SQWMsg *qwMsg, SDeleteRes *pRes) {
  int32_t        code = 0;
  SSubplan      *plan = NULL;
  qTaskInfo_t    pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx     ctx = {0};

  code = qMsgToSubplan(qwMsg->msg, qwMsg->msgLen, &plan);

  if (TSDB_CODE_SUCCESS != code) {
    code = TSDB_CODE_INVALID_MSG;
    QW_TASK_ELOG("task physical plan to subplan failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  code = qCreateExecTask(qwMsg->node, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, 0, NULL, OPTR_EXEC_MODEL_BATCH);
  
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    qDestroyTask(pTaskInfo);
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    qDestroyTask(pTaskInfo);
    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  ctx.taskHandle = pTaskInfo;
  ctx.sinkHandle = sinkHandle;

  uint64_t flags = 0;
  (void)dsGetSinkFlags(sinkHandle, &flags);

  ctx.sinkWithMemPool = flags & DS_FLAG_USE_MEMPOOL;

  QW_ERR_JRET(qwExecTask(QW_FPARAMS(), &ctx, NULL));

  QW_ERR_JRET(qwGetDeleteResFromSink(QW_FPARAMS(), &ctx, pRes));

_return:

  qwFreeTaskCtx(QW_FPARAMS(), &ctx);

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, void **qWorkerMgmt, const SMsgCb *pMsgCb) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == qWorkerMgmt || (pMsgCb && pMsgCb->mgmt == NULL)) {
    qError("invalid param to init qworker");
    QW_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t qwNum = atomic_add_fetch_32(&gQwMgmt.qwNum, 1);
  if (1 == qwNum) {
    TAOS_MEMSET(gQwMgmt.param, 0, sizeof(gQwMgmt.param));
  }

  code = qwOpenRef();
  if (code) {
    (void)atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);
    QW_RET(code);
  }

  SQWorker *mgmt = taosMemoryCalloc(1, sizeof(SQWorker));
  if (NULL == mgmt) {
    qError("calloc %d failed", (int32_t)sizeof(SQWorker));
    (void)atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);
    QW_RET(terrno);
  }

  mgmt->cfg.maxSchedulerNum = QW_DEFAULT_SCHEDULER_NUMBER;
  mgmt->cfg.maxTaskNum = QW_DEFAULT_TASK_NUMBER;
  mgmt->cfg.maxSchTaskNum = QW_DEFAULT_SCH_TASK_NUMBER;

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                               HASH_ENTRY_LOCK);
  if (NULL == mgmt->schHash) {
    taosMemoryFreeClear(mgmt);
    qError("init %d scheduler hash failed", mgmt->cfg.maxSchedulerNum);
    QW_ERR_JRET(terrno);
  }

  mgmt->ctxHash =
      taosHashInit(mgmt->cfg.maxTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->ctxHash) {
    qError("init %d task ctx hash failed", mgmt->cfg.maxTaskNum);
    QW_ERR_JRET(terrno);
  }

  mgmt->timer = taosTmrInit(0, 0, 0, "qworker");
  if (NULL == mgmt->timer) {
    qError("init timer failed, error:%s", tstrerror(terrno));
    QW_ERR_JRET(terrno);
  }

  mgmt->nodeType = nodeType;
  mgmt->nodeId = nodeId;
  if (pMsgCb) {
    mgmt->msgCb = *pMsgCb;
  } else {
    TAOS_MEMSET(&mgmt->msgCb, 0, sizeof(mgmt->msgCb));
  }

  mgmt->refId = taosAddRef(gQwMgmt.qwRef, mgmt);
  if (mgmt->refId < 0) {
    qError("taosAddRef qw failed, error:%s", tstrerror(terrno));
    QW_ERR_JRET(terrno);
  }

  SQWHbParam *param = NULL;
  qwSetHbParam(mgmt->refId, &param);

  mgmt->hbTimer = taosTmrStart(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, (void *)param, mgmt->timer);
  if (NULL == mgmt->hbTimer) {
    qError("start hb timer failed");
    QW_ERR_JRET(terrno);
  }

  QW_ERR_JRET(qExecutorInit());

  mgmt->lastChkTs = taosGetTimestampSec();
  
  *qWorkerMgmt = mgmt;

  qDebug("qworker initialized, type:%d, id:%d, handle:%p", mgmt->nodeType, mgmt->nodeId, mgmt);

  return TSDB_CODE_SUCCESS;

_return:

  if (mgmt->refId >= 0) {
    (void)qwRelease(mgmt->refId);  // ignore error
  } else {
    taosHashCleanup(mgmt->schHash);
    taosHashCleanup(mgmt->ctxHash);
    taosTmrCleanUp(mgmt->timer);
    taosMemoryFreeClear(mgmt);

    (void)atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);
  }

  QW_RET(code);
}

void qWorkerStopAllTasks(void *qWorkerMgmt) {
  SQWorker *mgmt = (SQWorker *)qWorkerMgmt;

  QW_DLOG("start to stop all tasks, taskNum:%d", taosHashGetSize(mgmt->ctxHash));

  atomic_store_8(&mgmt->nodeStopped, 1);

  qwStopAllTasks(mgmt);
}

void qWorkerDestroy(void **qWorkerMgmt) {
  if (NULL == qWorkerMgmt || NULL == *qWorkerMgmt) {
    return;
  }

  int32_t   destroyed = 0;
  SQWorker *mgmt = *qWorkerMgmt;
  mgmt->destroyed = &destroyed;

  if (taosRemoveRef(gQwMgmt.qwRef, mgmt->refId)) {
    qError("remove qw from ref list failed, refId:%" PRIx64, mgmt->refId);
    return;
  }

  qInfo("wait for destroyed");
  while (0 == destroyed) {
    taosMsleep(2);
  }

  *qWorkerMgmt = NULL;
}

int32_t qWorkerGetStat(SReadHandle *handle, void *qWorkerMgmt, SQWorkerStat *pStat) {
  if (NULL == handle || NULL == qWorkerMgmt || NULL == pStat) {
    QW_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;
  SDataSinkStat sinkStat = {0};

  QW_ERR_RET(dsDataSinkGetCacheSize(&sinkStat));
  pStat->cacheDataSize = sinkStat.cachedSize;

  pStat->queryProcessed = QW_STAT_GET(mgmt->stat.msgStat.queryProcessed);
  pStat->cqueryProcessed = QW_STAT_GET(mgmt->stat.msgStat.cqueryProcessed);
  pStat->fetchProcessed = QW_STAT_GET(mgmt->stat.msgStat.fetchProcessed);
  pStat->dropProcessed = QW_STAT_GET(mgmt->stat.msgStat.dropProcessed);
  pStat->notifyProcessed = QW_STAT_GET(mgmt->stat.msgStat.notifyProcessed);
  pStat->hbProcessed = QW_STAT_GET(mgmt->stat.msgStat.hbProcessed);
  pStat->deleteProcessed = QW_STAT_GET(mgmt->stat.msgStat.deleteProcessed);

  pStat->numOfQueryInQueue = handle->pMsgCb->qsizeFp(handle->pMsgCb->mgmt, mgmt->nodeId, QUERY_QUEUE);
  pStat->numOfFetchInQueue = handle->pMsgCb->qsizeFp(handle->pMsgCb->mgmt, mgmt->nodeId, FETCH_QUEUE);
  pStat->timeInQueryQueue = qwGetTimeInQueue((SQWorker *)qWorkerMgmt, QUERY_QUEUE);
  pStat->timeInFetchQueue = qwGetTimeInQueue((SQWorker *)qWorkerMgmt, FETCH_QUEUE);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessLocalQuery(void *pMgmt, uint64_t sId, uint64_t qId, uint64_t cId, uint64_t tId, int64_t rId,
                                 int32_t eId, SQWMsg *qwMsg, SArray *explainRes) {
  SQWorker      *mgmt = (SQWorker *)pMgmt;
  int32_t        code = 0;
  SQWTaskCtx    *ctx = NULL;
  SSubplan      *plan = (SSubplan *)qwMsg->msg;
  SQWPhaseInput  input = {0};
  qTaskInfo_t    pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SReadHandle    rHandle = {0};

  QW_ERR_JRET(qwAddTaskCtx(QW_FPARAMS()));
  QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_INIT));

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, NULL));
  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  ctx->taskType = qwMsg->msgInfo.taskType;
  ctx->explain = qwMsg->msgInfo.explain;
  ctx->needFetch = qwMsg->msgInfo.needFetch;
  ctx->queryMsgType = qwMsg->msgType;
  ctx->localExec = true;
  ctx->explainRes = explainRes;

  rHandle.pMsgCb = taosMemoryCalloc(1, sizeof(SMsgCb));
  rHandle.pWorkerCb = qwMsg->pWorkerCb;
  if (NULL == rHandle.pMsgCb) {
    QW_ERR_JRET(terrno);
  }

  rHandle.pMsgCb->clientRpc = qwMsg->connInfo.handle;
  rHandle.localExec = true;

  code = qCreateExecTask(&rHandle, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, 0, NULL, OPTR_EXEC_MODEL_BATCH);
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  ctx->level = plan->level;
  atomic_store_ptr(&ctx->taskHandle, pTaskInfo);
  atomic_store_ptr(&ctx->sinkHandle, sinkHandle);

  QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, NULL));

_return:

  taosMemoryFree(rHandle.pMsgCb);

  input.code = code;
  input.msgType = qwMsg->msgType;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_QUERY, &input, NULL);

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_RET(code);
}

int32_t qWorkerProcessLocalFetch(void *pMgmt, uint64_t sId, uint64_t qId, uint64_t cId, uint64_t tId, int64_t rId,
                                 int32_t eId, void **pRsp, SArray *explainRes) {
  SQWorker   *mgmt = (SQWorker *)pMgmt;
  int32_t     code = 0;
  int32_t     dataLen = 0;
  int32_t     rawLen = 0;
  SQWTaskCtx *ctx = NULL;
  void       *rsp = NULL;
  bool        queryStop = false;

  SQWPhaseInput input = {0};

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, NULL));
  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  ctx->fetchMsgType = TDMT_SCH_MERGE_FETCH;
  ctx->explainRes = explainRes;

  SOutputData sOutput = {0};

  while (true) {
    QW_ERR_JRET(qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rawLen, &rsp, &sOutput));

    if (NULL == rsp) {
      QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, &queryStop));

      continue;
    } else {
      bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

      qwBuildFetchRsp(rsp, &sOutput, dataLen, rawLen, qComplete);
      if (qComplete) {
        atomic_store_8((int8_t *)&ctx->queryEnd, true);
        if (!ctx->dynamicTask) {
          qwFreeSinkHandle(ctx);
        }
      }

      break;
    }
  }

_return:

  *pRsp = rsp;

  input.code = code;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_FETCH, &input, NULL);

  QW_RET(code);
}

void qWorkerRetireJob(uint64_t jobId, uint64_t clientId, int32_t errCode) {
  char id[sizeof(jobId) + sizeof(clientId) + 1] = {0};
  QW_SET_QCID(id, jobId, clientId);

  SQWJobInfo *pJob = (SQWJobInfo *)taosHashGet(gQueryMgmt.pJobInfo, id, sizeof(id));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 " CID:0x%" PRIx64 " fail to get job from job hash", jobId, clientId);
    return;
  }

  if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) &&
      0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
    qDebug("QID:0x%" PRIx64 " CID:0x%" PRIx64 " mark retired, errCode: 0x%x, allocSize:%" PRId64, jobId, clientId,
           errCode, atomic_load_64(&pJob->memInfo->allocMemSize));

    (void)qwRetireJob(pJob);
  } else {
    qDebug("QID:0x%" PRIx64 " already retired, retired: %d, errCode: 0x%x, allocSize:%" PRId64, jobId,
           atomic_load_8(&pJob->retired), atomic_load_32(&pJob->errCode), atomic_load_64(&pJob->memInfo->allocMemSize));
  }
}

void qWorkerRetireJobs(int64_t retireSize, int32_t errCode) {
  qDebug("need to retire jobs in batch, targetRetireSize:%" PRId64 ", remainJobNum:%d, task initNum:%" PRId64 " - %" PRId64 
      ", task destroyNum:%" PRId64 " - %" PRId64 " - %" PRId64, 
      retireSize, taosHashGetSize(gQueryMgmt.pJobInfo), atomic_load_64(&gQueryMgmt.stat.taskInitNum), atomic_load_64(&gQueryMgmt.stat.taskRunNum), 
      atomic_load_64(&gQueryMgmt.stat.taskExecDestroyNum), atomic_load_64(&gQueryMgmt.stat.taskSinkDestroyNum),
      atomic_load_64(&gQueryMgmt.stat.taskDestroyNum));

  SQWJobInfo* pJob = (SQWJobInfo*)taosHashIterate(gQueryMgmt.pJobInfo, NULL);
  int32_t jobNum = 0;
  int32_t alreadyJobNum = 0;
  int64_t retiredSize = 0;
  while (retiredSize < retireSize && NULL != pJob && jobNum < QW_RETIRE_JOB_BATCH_NUM) {
    if (atomic_load_8(&pJob->retired)) {
      pJob = (SQWJobInfo*)taosHashIterate(gQueryMgmt.pJobInfo, pJob);
      alreadyJobNum++;
      continue;
    }

    if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
      int64_t aSize = atomic_load_64(&pJob->memInfo->allocMemSize);
      bool retired = qwRetireJob(pJob);

      retiredSize += aSize;   
      
      jobNum++;

      qDebug("QID:0x%" PRIx64 " CID:0x%" PRIx64 " job mark retired in batch, retired:%d, usedSize:%" PRId64 ", retireSize:%" PRId64, 
      pJob->memInfo->jobId, pJob->memInfo->clientId, retired, aSize, retireSize);
    } else {
      qDebug("QID:0x%" PRIx64 " CID:0x%" PRIx64 " job may already failed, errCode:%s", pJob->memInfo->jobId, pJob->memInfo->clientId, tstrerror(pJob->errCode));
    }

    pJob = (SQWJobInfo *)taosHashIterate(gQueryMgmt.pJobInfo, pJob);
  }

  taosHashCancelIterate(gQueryMgmt.pJobInfo, pJob);

  qDebug("job retire in batch done, [prev:%d, curr:%d, total:%d] jobs, direct retiredSize:%" PRId64 " targetRetireSize:%" PRId64 
      ", task initNum:%" PRId64 " - %" PRId64 ", task destroyNum:%" PRId64 " - %" PRId64 " - %" PRId64, 
      alreadyJobNum, jobNum, taosHashGetSize(gQueryMgmt.pJobInfo), retiredSize, retireSize, 
      atomic_load_64(&gQueryMgmt.stat.taskInitNum), atomic_load_64(&gQueryMgmt.stat.taskRunNum), 
      atomic_load_64(&gQueryMgmt.stat.taskExecDestroyNum), atomic_load_64(&gQueryMgmt.stat.taskSinkDestroyNum),
      atomic_load_64(&gQueryMgmt.stat.taskDestroyNum));
}
