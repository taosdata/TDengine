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

int32_t qwStopAllTasks(SQWorker *mgmt) {
  uint64_t qId, tId, sId;
  int32_t  eId;
  int64_t  rId = 0;

  void *pIter = taosHashIterate(mgmt->ctxHash, NULL);
  while (pIter) {
    SQWTaskCtx *ctx = (SQWTaskCtx *)pIter;
    void *      key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, tId, eId);

    QW_LOCK(QW_WRITE, &ctx->lock);

    sId = ctx->sId;

    QW_TASK_DLOG_E("start to force stop task");

    if (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP) || QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
      QW_TASK_WLOG_E("task already dropping");
      QW_UNLOCK(QW_WRITE, &ctx->lock);

      pIter = taosHashIterate(mgmt->ctxHash, pIter);
      continue;
    }

    if (QW_QUERY_RUNNING(ctx)) {
      qwKillTaskHandle(ctx, TSDB_CODE_VND_STOPPED);
      QW_TASK_DLOG_E("task running, async killed");
    } else if (QW_FETCH_RUNNING(ctx)) {
      QW_UPDATE_RSP_CODE(ctx, TSDB_CODE_VND_STOPPED);
      QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
      QW_TASK_DLOG_E("task fetching, update drop received");
    } else {
      qwDropTask(QW_FPARAMS());
    }

    QW_UNLOCK(QW_WRITE, &ctx->lock);

    pIter = taosHashIterate(mgmt->ctxHash, pIter);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwProcessHbLinkBroken(SQWorker *mgmt, SQWMsg *qwMsg, SSchedulerHbReq *req) {
  int32_t         code = 0;
  SSchedulerHbRsp rsp = {0};
  SQWSchStatus *  sch = NULL;

  QW_ERR_RET(qwAcquireScheduler(mgmt, req->sId, QW_READ, &sch));

  QW_LOCK(QW_WRITE, &sch->hbConnLock);

  sch->hbBrokenTs = taosGetTimestampMs();

  if (qwMsg->connInfo.handle == sch->hbConnInfo.handle) {
    tmsgReleaseHandle(&sch->hbConnInfo, TAOS_CONN_SERVER);
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

  ctx->queryExecDone = true;

  if (TASK_TYPE_TEMP == ctx->taskType && taskHandle) {
    if (ctx->explain && !ctx->explainRsped) {
      QW_ERR_RET(qwSendExplainResponse(QW_FPARAMS(), ctx));
    }

    if (!ctx->needFetch) {
      dsGetDataLength(ctx->sinkHandle, &ctx->affectedRows, NULL);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwSendQueryRsp(QW_FPARAMS_DEF, int32_t msgType, SQWTaskCtx *ctx, int32_t rspCode, bool quickRsp) {
  if ((!quickRsp) || QUERY_RSP_POLICY_QUICK == tsQueryRspPolicy) {
    if (!ctx->localExec) {
      qwBuildAndSendQueryRsp(msgType, &ctx->ctrlConnInfo, rspCode, ctx);
      QW_TASK_DLOG("query msg rsped, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, rspCode, tstrerror(rspCode));
    }

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
  while (true) {
    QW_TASK_DLOG("start to execTask, loopIdx:%d", i++);

    // if *taskHandle is NULL, it's killed right now
    bool hasMore = false;

    if (taskHandle) {
      qwDbgSimulateSleep();

      code = qExecTaskOpt(taskHandle, pResList, &useconds, &hasMore, &localFetch);
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

      SInputData inputData = {.pData = pRes};
      code = dsPutDataBlock(sinkHandle, &inputData, &qcontinue);
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

      dsEndPut(sinkHandle, useconds);

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

  hbInfo->connInfo = sch->hbConnInfo;
  hbInfo->rsp.epId = sch->hbEpId;

  QW_LOCK(QW_READ, &sch->tasksLock);

  taskNum = taosHashGetSize(sch->tasksHash);

  hbInfo->rsp.taskStatus = taosArrayInit(taskNum, sizeof(STaskStatus));
  if (NULL == hbInfo->rsp.taskStatus) {
    QW_UNLOCK(QW_READ, &sch->tasksLock);
    QW_ELOG("taosArrayInit taskStatus failed, num:%d", taskNum);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  void *      key = NULL;
  size_t      keyLen = 0;
  int32_t     i = 0;
  STaskStatus status = {0};

  void *pIter = taosHashIterate(sch->tasksHash, NULL);
  while (pIter) {
    SQWTaskStatus *taskStatus = (SQWTaskStatus *)pIter;
    key = taosHashGetKey(pIter, &keyLen);

    // TODO GET EXECUTOR API TO GET MORE INFO

    QW_GET_QTID(key, status.queryId, status.taskId, status.execId);
    status.status = taskStatus->status;
    status.refId = taskStatus->refId;

    taosArrayPush(hbInfo->rsp.taskStatus, &status);

    ++i;
    pIter = taosHashIterate(sch->tasksHash, pIter);
  }

  QW_UNLOCK(QW_READ, &sch->tasksLock);

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetQueryResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, int32_t *dataLen, void **rspMsg, SOutputData *pOutput) {
  int64_t            len = 0;
  SRetrieveTableRsp *rsp = NULL;
  bool               queryEnd = false;
  int32_t            code = 0;
  SOutputData        output = {0};

  if (NULL == ctx->sinkHandle) {
    pOutput->queryEnd = true;
    return TSDB_CODE_SUCCESS;
  }

  *dataLen = 0;

  while (true) {
    dsGetDataLength(ctx->sinkHandle, &len, &queryEnd);

    if (len < 0) {
      QW_TASK_ELOG("invalid length from dsGetDataLength, length:%" PRId64 "", len);
      QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    if (len == 0) {
      if (queryEnd) {
        code = dsGetDataBlock(ctx->sinkHandle, &output);
        if (code) {
          QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
          QW_ERR_RET(code);
        }

        QW_TASK_DLOG("no more data in sink and query end, fetched blocks %d rows %" PRId64, pOutput->numOfBlocks,
                     pOutput->numOfRows);

        if (!ctx->dynamicTask) {
          qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask);
        }

        if (NULL == rsp) {
          QW_ERR_RET(qwMallocFetchRsp(!ctx->localExec, len, &rsp));
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

    *dataLen += len;

    QW_ERR_RET(qwMallocFetchRsp(!ctx->localExec, *dataLen, &rsp));

    output.pData = rsp->data + *dataLen - len;
    code = dsGetDataBlock(ctx->sinkHandle, &output);
    if (code) {
      QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
      QW_ERR_RET(code);
    }

    pOutput->queryEnd = output.queryEnd;
    pOutput->precision = output.precision;
    pOutput->bufStatus = output.bufStatus;
    pOutput->useconds = output.useconds;
    pOutput->compressed = output.compressed;
    pOutput->numOfCols = output.numOfCols;
    pOutput->numOfRows += output.numOfRows;
    pOutput->numOfBlocks++;

    if (DS_BUF_EMPTY == pOutput->bufStatus && pOutput->queryEnd) {
      QW_TASK_DLOG("task all data fetched and done, fetched blocks %d rows %" PRId64, pOutput->numOfBlocks,
                   pOutput->numOfRows);
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask);
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

  *rspMsg = rsp;

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetDeleteResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, SDeleteRes *pRes) {
  int64_t     len = 0;
  bool        queryEnd = false;
  int32_t     code = 0;
  SOutputData output = {0};

  dsGetDataLength(ctx->sinkHandle, &len, &queryEnd);

  if (len <= 0 || len != sizeof(SDeleterRes)) {
    QW_TASK_ELOG("invalid length from dsGetDataLength, length:%" PRId64, len);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  output.pData = taosMemoryCalloc(1, len);
  if (NULL == output.pData) {
    QW_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  code = dsGetDataBlock(ctx->sinkHandle, &output);
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
  strcpy(pRes->tableFName, pDelRes->tableName);
  strcpy(pRes->tsColName, pDelRes->tsColName);
  taosMemoryFree(output.pData);

  return TSDB_CODE_SUCCESS;
}

int32_t qwQuickRspFetchReq(QW_FPARAMS_DEF, SQWTaskCtx *ctx, SQWMsg *qwMsg, int32_t code) {
  if (QUERY_RSP_POLICY_QUICK == tsQueryRspPolicy && ctx != NULL) {
    if (QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      void *      rsp = NULL;
      int32_t     dataLen = 0;
      SOutputData sOutput = {0};
      if (TSDB_CODE_SUCCESS == code) {
        code = qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput);
      }

      if (NULL == rsp && TSDB_CODE_SUCCESS == code) {
        return TSDB_CODE_SUCCESS;
      }

      if (NULL != rsp) {
        bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

        qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
        if (qComplete) {
          atomic_store_8((int8_t *)&ctx->queryEnd, true);
        }
      }

      qwMsg->connInfo = ctx->dataConnInfo;
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);

      qwBuildAndSendFetchRsp(ctx->fetchMsgType + 1, &qwMsg->connInfo, rsp, dataLen, code);
      rsp = NULL;

      QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code, tstrerror(code),
                   dataLen);
    }
  }

  return TSDB_CODE_SUCCESS;
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

  dsReset(ctx->sinkHandle);

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

  QW_SET_PHASE(ctx, phase);

  if (atomic_load_8((int8_t *)&ctx->queryEnd) && !ctx->dynamicTask) {
    QW_TASK_ELOG_E("query already end");
    QW_ERR_JRET(TSDB_CODE_QW_MSG_ERROR);
  }

  switch (phase) {
    case QW_PHASE_PRE_QUERY: {
      if (QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_ELOG("task already dropped at wrong phase %s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_STATUS_ERROR);
      }

      if (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        // qwBuildAndSendDropRsp(&ctx->ctrlConnInfo, code);
        // QW_TASK_DLOG("drop rsp send, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, code, tstrerror(code));

        QW_ERR_JRET(ctx->rspCode);
      }

      QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXEC, ctx->dynamicTask));
      break;
    }
    case QW_PHASE_PRE_FETCH: {
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

_return:

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);

    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (code != TSDB_CODE_SUCCESS) {
    QW_TASK_ELOG("end to handle event at phase %s, code:%s", qwPhaseStr(phase), tstrerror(code));
  } else {
    QW_TASK_DLOG("end to handle event at phase %s, code:%s", qwPhaseStr(phase), tstrerror(code));
  }

  QW_RET(code);
}

int32_t qwHandlePostPhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t        code = 0;
  SQWTaskCtx *   ctx = NULL;
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
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_PART_SUCC, ctx->dynamicTask);
    ctx->queryGotData = true;
  }

  if (QW_PHASE_POST_QUERY == phase && ctx && !ctx->queryRsped) {
    bool   rsped = false;
    SQWMsg qwMsg = {.msgType = ctx->queryMsgType, .connInfo = ctx->ctrlConnInfo};
    qwDbgSimulateRedirect(&qwMsg, ctx, &rsped);
    qwDbgSimulateDead(QW_FPARAMS(), ctx, &rsped);
    if (!rsped) {
      qwSendQueryRsp(QW_FPARAMS(), input->msgType + 1, ctx, code, false);
    }
  }

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);

    if (QW_PHASE_POST_CQUERY != phase) {
      QW_SET_PHASE(ctx, phase);
    }

    if (code) {
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAIL, ctx->dynamicTask);
    }

    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_TASK_DLOG("end to handle event at phase %s, code:%x - %s", qwPhaseStr(phase), code, tstrerror(code));

  QW_RET(code);
}

int32_t qwAbortPrerocessQuery(QW_FPARAMS_DEF) {
  QW_ERR_RET(qwDropTask(QW_FPARAMS()));

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwPreprocessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t     code = 0;
  SQWTaskCtx *ctx = NULL;

  QW_ERR_JRET(qwRegisterQueryBrokenLinkArg(QW_FPARAMS(), &qwMsg->connInfo));

  QW_ERR_JRET(qwAddTaskCtx(QW_FPARAMS()));

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  ctx->ctrlConnInfo = qwMsg->connInfo;
  ctx->sId = sId;
  ctx->phase = -1;

  QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_INIT));

  qwSendQueryRsp(QW_FPARAMS(), qwMsg->msgType + 1, ctx, code, true);

_return:

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg, char *sql) {
  int32_t        code = 0;
  bool           queryRsped = false;
  SSubplan *     plan = NULL;
  SQWPhaseInput  input = {0};
  qTaskInfo_t    pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx *   ctx = NULL;

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  ctx->taskType = qwMsg->msgInfo.taskType;
  ctx->explain = qwMsg->msgInfo.explain;
  ctx->needFetch = qwMsg->msgInfo.needFetch;
  ctx->queryMsgType = qwMsg->msgType;
  ctx->localExec = false;

  // QW_TASK_DLOGL("subplan json string, len:%d, %s", qwMsg->msgLen, qwMsg->msg);

  code = qMsgToSubplan(qwMsg->msg, qwMsg->msgLen, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    code = TSDB_CODE_INVALID_MSG;
    QW_TASK_ELOG("task physical plan to subplan failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

#if 0
  SReadHandle* pReadHandle = qwMsg->node;
  int64_t delay = 0;
  bool fhFinish = false;
  pReadHandle->api.tqReaderFn.tqGetStreamExecProgress(pReadHandle->vnode, 0, &delay, &fhFinish);
#endif

  code = qCreateExecTask(qwMsg->node, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, sql, OPTR_EXEC_MODEL_BATCH);
  sql = NULL;
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  // qwSendQueryRsp(QW_FPARAMS(), qwMsg->msgType + 1, ctx, code, true);

  ctx->level = plan->level;
  ctx->dynamicTask = qIsDynamicExecTask(pTaskInfo);
  atomic_store_ptr(&ctx->taskHandle, pTaskInfo);
  atomic_store_ptr(&ctx->sinkHandle, sinkHandle);

  qwSaveTbVersionInfo(pTaskInfo, ctx);

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

  qwQuickRspFetchReq(QW_FPARAMS(), ctx, qwMsg, code);

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessCQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  SQWTaskCtx *  ctx = NULL;
  int32_t       code = 0;
  SQWPhaseInput input = {0};
  void *        rsp = NULL;
  int32_t       dataLen = 0;
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
      QW_ERR_JRET(qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

      if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {
        QW_TASK_DLOG("task not end and buf is %s, need to continue query", qwBufStatusStr(sOutput.bufStatus));

        atomic_store_8((int8_t *)&ctx->queryContinue, 1);
      }

      if (rsp) {
        qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

        qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
        if (qComplete) {
          atomic_store_8((int8_t *)&ctx->queryEnd, true);
          atomic_store_8((int8_t *)&ctx->queryContinue, 0);
        }

        qwMsg->connInfo = ctx->dataConnInfo;
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);

        qwBuildAndSendFetchRsp(ctx->fetchMsgType + 1, &qwMsg->connInfo, rsp, dataLen, code);
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

    if (code && QW_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);
      qwFreeFetchRsp(rsp);
      rsp = NULL;

      qwMsg->connInfo = ctx->dataConnInfo;
      qwBuildAndSendFetchRsp(ctx->fetchMsgType + 1, &qwMsg->connInfo, NULL, 0, code);
      QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code, tstrerror(code),
                   0);
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
  qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_CQUERY, &input, NULL);

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessFetch(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t       code = 0;
  int32_t       dataLen = 0;
  bool          locked = false;
  SQWTaskCtx *  ctx = NULL;
  void *        rsp = NULL;
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
  QW_ERR_JRET(qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

  if (NULL == rsp) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);
  } else {
    bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

    qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
    if (qComplete) {
      atomic_store_8((int8_t *)&ctx->queryEnd, true);
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
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXEC, ctx->dynamicTask);

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
    if (ctx) {
      qwDbgSimulateRedirect(qwMsg, ctx, &rsped);
      qwDbgSimulateDead(QW_FPARAMS(), ctx, &rsped);
    }
    if (!rsped) {
      qwBuildAndSendFetchRsp(qwMsg->msgType + 1, &qwMsg->connInfo, rsp, dataLen, code);
      QW_TASK_DLOG("fetch rsp send, msgType:%s, handle:%p, code:%x - %s, dataLen:%d", TMSG_INFO(qwMsg->msgType + 1),
                   qwMsg->connInfo.handle, code, tstrerror(code), dataLen);
    } else {
      qwFreeFetchRsp(rsp);
      rsp = NULL;
    }
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
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_DROP, ctx->dynamicTask);
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
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAIL, ctx->dynamicTask);
    } else {
      tmsgReleaseHandle(&qwMsg->connInfo, TAOS_CONN_SERVER);
    }
  }

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  if (ctx) {
    if (qwMsg->connInfo.handle != ctx->ctrlConnInfo.handle) {
      tmsgReleaseHandle(&qwMsg->connInfo, TAOS_CONN_SERVER);
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
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask);
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
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAIL, ctx->dynamicTask);
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
  SQWSchStatus *  sch = NULL;

  if (qwMsg->code) {
    QW_RET(qwProcessHbLinkBroken(mgmt, qwMsg, req));
  }

  QW_ERR_JRET(qwAcquireAddScheduler(mgmt, req->sId, QW_READ, &sch));
  QW_ERR_JRET(qwRegisterHbBrokenLinkArg(mgmt, req->sId, &qwMsg->connInfo));

  sch->hbBrokenTs = 0;

  QW_LOCK(QW_WRITE, &sch->hbConnLock);

  if (sch->hbConnInfo.handle) {
    tmsgReleaseHandle(&sch->hbConnInfo, TAOS_CONN_SERVER);
    sch->hbConnInfo.handle = NULL;
  }

  memcpy(&sch->hbConnInfo, &qwMsg->connInfo, sizeof(qwMsg->connInfo));
  memcpy(&sch->hbEpId, &req->epId, sizeof(req->epId));

  QW_UNLOCK(QW_WRITE, &sch->hbConnLock);

  QW_DLOG("hb connection updated, sId:%" PRIx64 ", nodeId:%d, fqdn:%s, port:%d, handle:%p, ahandle:%p", req->sId,
          req->epId.nodeId, req->epId.ep.fqdn, req->epId.ep.port, qwMsg->connInfo.handle, qwMsg->connInfo.ahandle);

  qwReleaseScheduler(QW_READ, mgmt);

_return:

  memcpy(&rsp.epId, &req->epId, sizeof(req->epId));

  qwBuildAndSendHbRsp(&qwMsg->connInfo, &rsp, code);

  if (code) {
    tmsgReleaseHandle(&qwMsg->connInfo, TAOS_CONN_SERVER);
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
    QW_DLOG("qwAcquire %" PRIx64 "failed", refId);
    return;
  }

  SQWSchStatus *sch = NULL;
  int32_t       taskNum = 0;
  SQWHbInfo *   rspList = NULL;
  SArray *      pExpiredSch = NULL;
  int32_t       code = 0;

  qwDbgDumpMgmtInfo(mgmt);

  if (gQWDebug.forceStop) {
    (void)qwStopAllTasks(mgmt);
  }

  QW_LOCK(QW_READ, &mgmt->schLock);

  int32_t schNum = taosHashGetSize(mgmt->schHash);
  if (schNum <= 0) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
    qwRelease(refId);
    return;
  }

  rspList = taosMemoryCalloc(schNum, sizeof(SQWHbInfo));
  pExpiredSch = taosArrayInit(schNum, sizeof(uint64_t));
  if (NULL == rspList || NULL == pExpiredSch) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    taosMemoryFree(rspList);
    taosArrayDestroy(pExpiredSch);
    QW_ELOG("calloc %d SQWHbInfo failed", schNum);
    taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
    qwRelease(refId);
    return;
  }

  void *  key = NULL;
  size_t  keyLen = 0;
  int32_t i = 0;
  int64_t currentMs = taosGetTimestampMs();

  void *pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    SQWSchStatus *sch1 = (SQWSchStatus *)pIter;
    if (NULL == sch1->hbConnInfo.handle) {
      uint64_t *sId = taosHashGetKey(pIter, NULL);
      QW_TLOG("cancel send hb to sch %" PRIx64 " cause of no connection handle", *sId);

      if (sch1->hbBrokenTs > 0 && ((currentMs - sch1->hbBrokenTs) > QW_SCH_TIMEOUT_MSEC) &&
          taosHashGetSize(sch1->tasksHash) <= 0) {
        taosArrayPush(pExpiredSch, sId);
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
    qwBuildAndSendHbRsp(&rspList[j].connInfo, &rspList[j].rsp, code);
    /*QW_DLOG("hb rsp send, handle:%p, code:%x - %s, taskNum:%d", rspList[j].connInfo.handle, code, tstrerror(code),*/
    /*(rspList[j].rsp.taskStatus ? (int32_t)taosArrayGetSize(rspList[j].rsp.taskStatus) : 0));*/
    tFreeSSchedulerHbRsp(&rspList[j].rsp);
  }

  if (taosArrayGetSize(pExpiredSch) > 0) {
    qwClearExpiredSch(mgmt, pExpiredSch);
  }

  taosMemoryFreeClear(rspList);
  taosArrayDestroy(pExpiredSch);

  taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
  qwRelease(refId);
}

int32_t qwProcessDelete(QW_FPARAMS_DEF, SQWMsg *qwMsg, SDeleteRes *pRes) {
  int32_t        code = 0;
  SSubplan *     plan = NULL;
  qTaskInfo_t    pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx     ctx = {0};

  code = qMsgToSubplan(qwMsg->msg, qwMsg->msgLen, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    code = TSDB_CODE_INVALID_MSG;
    QW_TASK_ELOG("task physical plan to subplan failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  code = qCreateExecTask(qwMsg->node, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, NULL, OPTR_EXEC_MODEL_BATCH);
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  ctx.taskHandle = pTaskInfo;
  ctx.sinkHandle = sinkHandle;

  QW_ERR_JRET(qwExecTask(QW_FPARAMS(), &ctx, NULL));

  QW_ERR_JRET(qwGetDeleteResFromSink(QW_FPARAMS(), &ctx, pRes));

_return:

  qwFreeTaskCtx(&ctx);

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, void **qWorkerMgmt, const SMsgCb *pMsgCb) {
  if (NULL == qWorkerMgmt || (pMsgCb && pMsgCb->mgmt == NULL)) {
    qError("invalid param to init qworker");
    QW_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t qwNum = atomic_add_fetch_32(&gQwMgmt.qwNum, 1);
  if (1 == qwNum) {
    memset(gQwMgmt.param, 0, sizeof(gQwMgmt.param));
  }

  int32_t code = qwOpenRef();
  if (code) {
    atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);
    QW_RET(code);
  }

  SQWorker *mgmt = taosMemoryCalloc(1, sizeof(SQWorker));
  if (NULL == mgmt) {
    qError("calloc %d failed", (int32_t)sizeof(SQWorker));
    atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);
    QW_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  mgmt->cfg.maxSchedulerNum = QW_DEFAULT_SCHEDULER_NUMBER;
  mgmt->cfg.maxTaskNum = QW_DEFAULT_TASK_NUMBER;
  mgmt->cfg.maxSchTaskNum = QW_DEFAULT_SCH_TASK_NUMBER;

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                               HASH_ENTRY_LOCK);
  if (NULL == mgmt->schHash) {
    taosMemoryFreeClear(mgmt);
    qError("init %d scheduler hash failed", mgmt->cfg.maxSchedulerNum);
    QW_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  mgmt->ctxHash =
      taosHashInit(mgmt->cfg.maxTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->ctxHash) {
    qError("init %d task ctx hash failed", mgmt->cfg.maxTaskNum);
    QW_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  mgmt->timer = taosTmrInit(0, 0, 0, "qworker");
  if (NULL == mgmt->timer) {
    qError("init timer failed, error:%s", tstrerror(terrno));
    QW_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  mgmt->nodeType = nodeType;
  mgmt->nodeId = nodeId;
  if (pMsgCb) {
    mgmt->msgCb = *pMsgCb;
  } else {
    memset(&mgmt->msgCb, 0, sizeof(mgmt->msgCb));
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
    QW_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  *qWorkerMgmt = mgmt;

  qDebug("qworker initialized, type:%d, id:%d, handle:%p", mgmt->nodeType, mgmt->nodeId, mgmt);

  return TSDB_CODE_SUCCESS;

_return:

  if (mgmt->refId >= 0) {
    qwRelease(mgmt->refId);
  } else {
    taosHashCleanup(mgmt->schHash);
    taosHashCleanup(mgmt->ctxHash);
    taosTmrCleanUp(mgmt->timer);
    taosMemoryFreeClear(mgmt);

    atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);
  }

  QW_RET(code);
}

void qWorkerStopAllTasks(void *qWorkerMgmt) {
  SQWorker *mgmt = (SQWorker *)qWorkerMgmt;

  QW_DLOG("start to stop all tasks, taskNum:%d", taosHashGetSize(mgmt->ctxHash));

  atomic_store_8(&mgmt->nodeStopped, 1);

  (void)qwStopAllTasks(mgmt);
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

  while (0 == destroyed) {
    taosMsleep(2);
  }
}

int32_t qWorkerGetStat(SReadHandle *handle, void *qWorkerMgmt, SQWorkerStat *pStat) {
  if (NULL == handle || NULL == qWorkerMgmt || NULL == pStat) {
    QW_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;
  SDataSinkStat sinkStat = {0};

  dsDataSinkGetCacheSize(&sinkStat);
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

int32_t qWorkerProcessLocalQuery(void *pMgmt, uint64_t sId, uint64_t qId, uint64_t tId, int64_t rId, int32_t eId,
                                 SQWMsg *qwMsg, SArray *explainRes) {
  SQWorker *     mgmt = (SQWorker *)pMgmt;
  int32_t        code = 0;
  SQWTaskCtx *   ctx = NULL;
  SSubplan *     plan = (SSubplan *)qwMsg->msg;
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
  rHandle.pMsgCb->clientRpc = qwMsg->connInfo.handle;

  code = qCreateExecTask(&rHandle, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, NULL, OPTR_EXEC_MODEL_BATCH);
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

int32_t qWorkerProcessLocalFetch(void *pMgmt, uint64_t sId, uint64_t qId, uint64_t tId, int64_t rId, int32_t eId,
                                 void **pRsp, SArray *explainRes) {
  SQWorker *  mgmt = (SQWorker *)pMgmt;
  int32_t     code = 0;
  int32_t     dataLen = 0;
  SQWTaskCtx *ctx = NULL;
  void *      rsp = NULL;
  bool        queryStop = false;

  SQWPhaseInput input = {0};

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  ctx->fetchMsgType = TDMT_SCH_MERGE_FETCH;
  ctx->explainRes = explainRes;

  SOutputData sOutput = {0};

  while (true) {
    QW_ERR_JRET(qwGetQueryResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

    if (NULL == rsp) {
      QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, &queryStop));

      continue;
    } else {
      bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

      qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
      if (qComplete) {
        atomic_store_8((int8_t *)&ctx->queryEnd, true);
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
