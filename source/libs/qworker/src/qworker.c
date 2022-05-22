#include "qworker.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qwInt.h"
#include "qwMsg.h"
#include "tcommon.h"
#include "tmsg.h"
#include "tname.h"

SQWorkerMgmt gQwMgmt = {
    .lock = 0,
    .qwRef = -1,
    .qwNum = 0,
};


int32_t qwProcessHbLinkBroken(SQWorker *mgmt, SQWMsg *qwMsg, SSchedulerHbReq *req) {
  int32_t         code = 0;
  SSchedulerHbRsp rsp = {0};
  SQWSchStatus   *sch = NULL;

  QW_ERR_RET(qwAcquireAddScheduler(mgmt, req->sId, QW_READ, &sch));

  QW_LOCK(QW_WRITE, &sch->hbConnLock);

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

  if (TASK_TYPE_TEMP == ctx->taskType && taskHandle) {
    if (ctx->explain) {
      SExplainExecInfo *execInfo = NULL;
      int32_t           resNum = 0;
      QW_ERR_RET(qGetExplainExecInfo(taskHandle, &resNum, &execInfo));

      SRpcHandleInfo connInfo = ctx->ctrlConnInfo;
      connInfo.ahandle = NULL;
      QW_ERR_RET(qwBuildAndSendExplainRsp(&connInfo, execInfo, resNum));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwExecTask(QW_FPARAMS_DEF, SQWTaskCtx *ctx, bool *queryEnd) {
  int32_t        code = 0;
  bool           qcontinue = true;
  SSDataBlock   *pRes = NULL;
  uint64_t       useconds = 0;
  int32_t        i = 0;
  int32_t        execNum = 0;
  qTaskInfo_t    taskHandle = ctx->taskHandle;
  DataSinkHandle sinkHandle = ctx->sinkHandle;

  while (true) {
    QW_TASK_DLOG("start to execTask, loopIdx:%d", i++);

    pRes = NULL;

    // if *taskHandle is NULL, it's killed right now
    if (taskHandle) {
      code = qExecTask(taskHandle, &pRes, &useconds);
      if (code) {
        QW_TASK_ELOG("qExecTask failed, code:%x - %s", code, tstrerror(code));
        QW_ERR_RET(code);
      }
    }

    ++execNum;

    if (NULL == pRes) {
      QW_TASK_DLOG("qExecTask end with empty res, useconds:%" PRIu64, useconds);

      dsEndPut(sinkHandle, useconds);

      QW_ERR_RET(qwHandleTaskComplete(QW_FPARAMS(), ctx));

      if (queryEnd) {
        *queryEnd = true;
      }

      break;
    }

    int32_t rows = pRes->info.rows;

    ASSERT(pRes->info.rows > 0);

    SInputData inputData = {.pData = pRes};
    code = dsPutDataBlock(sinkHandle, &inputData, &qcontinue);
    if (code) {
      QW_TASK_ELOG("dsPutDataBlock failed, code:%x - %s", code, tstrerror(code));
      QW_ERR_RET(code);
    }

    QW_TASK_DLOG("data put into sink, rows:%d, continueExecTask:%d", rows, qcontinue);

    if (!qcontinue) {
      break;
    }

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_READY) && execNum >= QW_DEFAULT_SHORT_RUN_TIMES) {
      break;
    }

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      break;
    }

    if (atomic_load_32(&ctx->rspCode)) {
      break;
    }
  }

  QW_RET(code);
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
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
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

    QW_GET_QTID(key, status.queryId, status.taskId);
    status.status = taskStatus->status;
    status.refId = taskStatus->refId;

    taosArrayPush(hbInfo->rsp.taskStatus, &status);

    ++i;
    pIter = taosHashIterate(sch->tasksHash, pIter);
  }

  QW_UNLOCK(QW_READ, &sch->tasksLock);

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, int32_t *dataLen, void **rspMsg, SOutputData *pOutput) {
  int32_t            len = 0;
  SRetrieveTableRsp *rsp = NULL;
  bool               queryEnd = false;
  int32_t            code = 0;

  dsGetDataLength(ctx->sinkHandle, &len, &queryEnd);

  if (len < 0) {
    QW_TASK_ELOG("invalid length from dsGetDataLength, length:%d", len);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (len == 0) {
    if (queryEnd) {
      code = dsGetDataBlock(ctx->sinkHandle, pOutput);
      if (code) {
        QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
        QW_ERR_RET(code);
      }

      QW_TASK_DLOG_E("no data in sink and query end");

      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED);
      QW_ERR_RET(qwMallocFetchRsp(len, &rsp));

      *rspMsg = rsp;
      *dataLen = 0;
      return TSDB_CODE_SUCCESS;
    }

    pOutput->bufStatus = DS_BUF_EMPTY;

    return TSDB_CODE_SUCCESS;
  }

  // Got data from sink
  QW_TASK_DLOG("there are data in sink, dataLength:%d", len);

  *dataLen = len;

  QW_ERR_RET(qwMallocFetchRsp(len, &rsp));
  *rspMsg = rsp;

  pOutput->pData = rsp->data;
  code = dsGetDataBlock(ctx->sinkHandle, pOutput);
  if (code) {
    QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_RET(code);
  }

  if (DS_BUF_EMPTY == pOutput->bufStatus && pOutput->queryEnd) {
    QW_TASK_DLOG_E("task all data fetched, done");
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qwHandlePrePhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t         code = 0;
  SQWTaskCtx     *ctx = NULL;
  SRpcHandleInfo *cancelConnection = NULL;

  QW_TASK_DLOG("start to handle event at phase %s", qwPhaseStr(phase));

  if (QW_PHASE_PRE_QUERY == phase) {
    QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), &ctx));
  } else {
    QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));
  }

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_PHASE_PRE_FETCH == phase) {
    atomic_store_8((int8_t *)&ctx->queryFetched, true);
  } else {
    atomic_store_8(&ctx->phase, phase);
  }

  if (atomic_load_8((int8_t *)&ctx->queryEnd)) {
    QW_TASK_ELOG_E("query already end");
    QW_ERR_JRET(TSDB_CODE_QW_MSG_ERROR);
  }

  switch (phase) {
    case QW_PHASE_PRE_QUERY: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_ELOG("task already dropped at wrong phase %s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_STATUS_ERROR);
        break;
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        //qwBuildAndSendDropRsp(&ctx->ctrlConnInfo, code);
        //QW_TASK_DLOG("drop rsp send, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, code, tstrerror(code));

        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
        break;
      }

      QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));
      break;
    }
    case QW_PHASE_PRE_FETCH: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP) || QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task dropping or already dropped, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
        QW_TASK_WLOG("last fetch still not processed, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
      }

      if (!QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_READY)) {
        QW_TASK_ELOG("ready msg has not been processed, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_MSG_ERROR);
      }
      break;
    }
    case QW_PHASE_PRE_CQUERY: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task already dropped, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        //qwBuildAndSendDropRsp(&ctx->ctrlConnInfo, code);
        //QW_TASK_DLOG("drop rsp send, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, code, tstrerror(code));

        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      break;
    }
    default:
      QW_TASK_ELOG("invalid phase %s", qwPhaseStr(phase));
      QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
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

  if (cancelConnection) {
    qwBuildAndSendCancelRsp(cancelConnection, code);
    QW_TASK_DLOG("cancel rsp send, handle:%p, code:%x - %s", cancelConnection->handle, code, tstrerror(code));
  }

  if (code != TSDB_CODE_SUCCESS) {
    QW_TASK_ELOG("end to handle event at phase %s, code:%s", qwPhaseStr(phase), tstrerror(code));
  } else {
    QW_TASK_DLOG("end to handle event at phase %s, code:%s", qwPhaseStr(phase), tstrerror(code));
  }

  QW_RET(code);
}

int32_t qwHandlePostPhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t         code = 0;
  SQWTaskCtx     *ctx = NULL;
  SRpcHandleInfo  connInfo = {0};
  SRpcHandleInfo *rspConnection = NULL;

  QW_TASK_DLOG("start to handle event at phase %s", qwPhaseStr(phase));

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already dropped, phase:%s", qwPhaseStr(phase));
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
  }

  if (QW_PHASE_POST_QUERY == phase) {
#if 0    
    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_READY)) {
      readyConnection = &ctx->connInfo;
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);
    }
#else
    connInfo = ctx->ctrlConnInfo;
    rspConnection = &connInfo;

    QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);
#endif
  }

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    if (QW_PHASE_POST_FETCH == phase) {
      QW_TASK_WLOG("drop received at wrong phase %s", qwPhaseStr(phase));
      QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
    }

    //qwBuildAndSendDropRsp(&ctx->ctrlConnInfo, code);
    //QW_TASK_DLOG("drop rsp send, handle:%p, code:%x - %s", ctx->ctrlConnInfo.handle, code, tstrerror(code));

    QW_ERR_JRET(qwDropTask(QW_FPARAMS()));
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
  }

  if (ctx->rspCode) {
    QW_TASK_ELOG("task already failed, phase %s, error:%x - %s", qwPhaseStr(phase), ctx->rspCode,
                 tstrerror(ctx->rspCode));
    QW_ERR_JRET(ctx->rspCode);
  }

  QW_ERR_JRET(input->code);

_return:

  if (TSDB_CODE_SUCCESS == code && QW_PHASE_POST_QUERY == phase) {
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_PARTIAL_SUCCEED);
  }

  if (rspConnection) {
    qwBuildAndSendQueryRsp(rspConnection, code, ctx ? &ctx->tbInfo : NULL);
    QW_TASK_DLOG("ready msg rsped, handle:%p, code:%x - %s", rspConnection->handle, code, tstrerror(code));
  }

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);

    if (QW_PHASE_POST_FETCH != phase) {
      atomic_store_8(&ctx->phase, phase);
    }

    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (code) {
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAILED);
  }

  QW_TASK_DLOG("end to handle event at phase %s, code:%x - %s", qwPhaseStr(phase), code, tstrerror(code));

  QW_RET(code);
}

int32_t qwProcessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg, int8_t taskType, int8_t explain) {
  int32_t        code = 0;
  bool           queryRsped = false;
  SSubplan      *plan = NULL;
  SQWPhaseInput  input = {0};
  qTaskInfo_t    pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx    *ctx = NULL;

  QW_ERR_JRET(qwRegisterQueryBrokenLinkArg(QW_FPARAMS(), &qwMsg->connInfo));

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  atomic_store_8(&ctx->taskType, taskType);
  atomic_store_8(&ctx->explain, explain);

  ctx->ctrlConnInfo = qwMsg->connInfo;

  /*QW_TASK_DLOGL("subplan json string, len:%d, %s", qwMsg->msgLen, qwMsg->msg);*/

  code = qStringToSubplan(qwMsg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    code = TSDB_CODE_INVALID_MSG;
    QW_TASK_ELOG("task physical plan to subplan failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  ctx->plan = plan;

  code = qCreateExecTask(qwMsg->node, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle, OPTR_EXEC_MODEL_BATCH);
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  // QW_ERR_JRET(qwBuildAndSendQueryRsp(&qwMsg->connInfo, code));
  // QW_TASK_DLOG("query msg rsped, handle:%p, code:%x - %s", qwMsg->connInfo.handle, code, tstrerror(code));

  // queryRsped = true;

  atomic_store_ptr(&ctx->taskHandle, pTaskInfo);
  atomic_store_ptr(&ctx->sinkHandle, sinkHandle);

  if (pTaskInfo && sinkHandle) {
    qwSaveTbVersionInfo(pTaskInfo, ctx);
    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, NULL));
  }

_return:

  input.code = code;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_QUERY, &input, NULL);

  // if (!queryRsped) {
  //  qwBuildAndSendQueryRsp(&qwMsg->connInfo, code);
  //  QW_TASK_DLOG("query msg rsped, handle:%p, code:%x - %s", qwMsg->connInfo.handle, code, tstrerror(code));
  //}

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessCQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  SQWTaskCtx   *ctx = NULL;
  int32_t       code = 0;
  SQWPhaseInput input = {0};
  void         *rsp = NULL;
  int32_t       dataLen = 0;
  bool          queryEnd = false;

  do {
    QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_CQUERY, &input, NULL));

    QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

    atomic_store_8((int8_t *)&ctx->queryInQueue, 0);
    atomic_store_8((int8_t *)&ctx->queryContinue, 0);

    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, &queryEnd));

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      SOutputData sOutput = {0};
      QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

      if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {
        QW_TASK_DLOG("task not end and buf is %s, need to continue query", qwBufStatusStr(sOutput.bufStatus));

        atomic_store_8((int8_t *)&ctx->queryContinue, 1);
      }

      if (rsp) {
        bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);

        qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
        if (qComplete) {
          atomic_store_8((int8_t *)&ctx->queryEnd, true);
        }

        qwMsg->connInfo = ctx->dataConnInfo;
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);

        qwBuildAndSendFetchRsp(&qwMsg->connInfo, rsp, dataLen, code);
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

    if (code && QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);
      qwFreeFetchRsp(rsp);
      rsp = NULL;

      qwMsg->connInfo = ctx->dataConnInfo;
      qwBuildAndSendFetchRsp(&qwMsg->connInfo, rsp, 0, code);
      QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code, tstrerror(code),
                   0);
    }

    QW_LOCK(QW_WRITE, &ctx->lock);
    if (queryEnd || code || 0 == atomic_load_8((int8_t *)&ctx->queryContinue)) {
      // Note: if necessary, fetch need to put cquery to queue again
      atomic_store_8(&ctx->phase, 0);
      QW_UNLOCK(QW_WRITE, &ctx->lock);
      break;
    }
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  } while (true);

  input.code = code;
  qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_CQUERY, &input, NULL);

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessFetch(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t       code = 0;
  int32_t       dataLen = 0;
  bool          locked = false;
  SQWTaskCtx   *ctx = NULL;
  void         *rsp = NULL;
  SQWPhaseInput input = {0};

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

  SOutputData sOutput = {0};
  QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

  if (NULL == rsp) {
    ctx->dataConnInfo = qwMsg->connInfo;

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
    if (QW_IS_QUERY_RUNNING(ctx)) {
      atomic_store_8((int8_t *)&ctx->queryContinue, 1);
    } else if (0 == atomic_load_8((int8_t *)&ctx->queryInQueue)) {
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING);

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
    qwBuildAndSendFetchRsp(&qwMsg->connInfo, rsp, dataLen, code);
    QW_TASK_DLOG("fetch rsp send, handle:%p, code:%x - %s, dataLen:%d", qwMsg->connInfo.handle, code, tstrerror(code),
                 dataLen);
  }

  QW_RET(TSDB_CODE_SUCCESS);
}

int32_t qwProcessDrop(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t     code = 0;
  bool        rsped = false;
  SQWTaskCtx *ctx = NULL;
  bool        locked = false;

  // TODO : TASK ALREADY REMOVED AND A NEW DROP MSG RECEIVED

  QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  locked = true;

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG_E("task already dropping");
    QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
  }

  if (QW_IS_QUERY_RUNNING(ctx)) {
    QW_ERR_JRET(qwKillTaskHandle(QW_FPARAMS(), ctx));
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_DROPPING);
  } else if (ctx->phase > 0) {
    QW_ERR_JRET(qwDropTask(QW_FPARAMS()));
    rsped = true;
  } else {
    // task not started
  }

  if (!rsped) {
    ctx->ctrlConnInfo = qwMsg->connInfo;

    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
  }

_return:

  if (code) {
    if (ctx) {
      QW_UPDATE_RSP_CODE(ctx, code);
    }

    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAILED);
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

  QW_ERR_JRET(qwAcquireAddScheduler(mgmt, req->sId, QW_READ, &sch));

  QW_ERR_JRET(qwRegisterHbBrokenLinkArg(mgmt, req->sId, &qwMsg->connInfo));

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
    taosMemoryFree(param);
    return;
  }

  SQWSchStatus *sch = NULL;
  int32_t       taskNum = 0;
  SQWHbInfo    *rspList = NULL;
  int32_t       code = 0;

  qwDbgDumpMgmtInfo(mgmt);

  QW_LOCK(QW_READ, &mgmt->schLock);

  int32_t schNum = taosHashGetSize(mgmt->schHash);
  if (schNum <= 0) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
    qwRelease(refId);
    return;
  }

  rspList = taosMemoryCalloc(schNum, sizeof(SQWHbInfo));
  if (NULL == rspList) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    QW_ELOG("calloc %d SQWHbInfo failed", schNum);
    taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
    qwRelease(refId);
    return;
  }

  void   *key = NULL;
  size_t  keyLen = 0;
  int32_t i = 0;

  void *pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    SQWSchStatus *sch = (SQWSchStatus *)pIter;
    if (NULL == sch->hbConnInfo.handle) {
      uint64_t *sId = taosHashGetKey(pIter, NULL);
      QW_TLOG("cancel send hb to sch %" PRIx64 " cause of no connection handle", *sId);
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

  taosMemoryFreeClear(rspList);

  taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
  qwRelease(refId);
}


int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, SQWorkerCfg *cfg, void **qWorkerMgmt, const SMsgCb *pMsgCb) {
  if (NULL == qWorkerMgmt || pMsgCb->mgmt == NULL) {
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
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (cfg) {
    mgmt->cfg = *cfg;
    if (0 == mgmt->cfg.maxSchedulerNum) {
      mgmt->cfg.maxSchedulerNum = QW_DEFAULT_SCHEDULER_NUMBER;
    }
    if (0 == mgmt->cfg.maxTaskNum) {
      mgmt->cfg.maxTaskNum = QW_DEFAULT_TASK_NUMBER;
    }
    if (0 == mgmt->cfg.maxSchTaskNum) {
      mgmt->cfg.maxSchTaskNum = QW_DEFAULT_SCH_TASK_NUMBER;
    }
  } else {
    mgmt->cfg.maxSchedulerNum = QW_DEFAULT_SCHEDULER_NUMBER;
    mgmt->cfg.maxTaskNum = QW_DEFAULT_TASK_NUMBER;
    mgmt->cfg.maxSchTaskNum = QW_DEFAULT_SCH_TASK_NUMBER;
  }

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                               HASH_ENTRY_LOCK);
  if (NULL == mgmt->schHash) {
    taosMemoryFreeClear(mgmt);
    qError("init %d scheduler hash failed", mgmt->cfg.maxSchedulerNum);
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->ctxHash =
      taosHashInit(mgmt->cfg.maxTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->ctxHash) {
    qError("init %d task ctx hash failed", mgmt->cfg.maxTaskNum);
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->timer = taosTmrInit(0, 0, 0, "qworker");
  if (NULL == mgmt->timer) {
    qError("init timer failed, error:%s", tstrerror(terrno));
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->nodeType = nodeType;
  mgmt->nodeId = nodeId;
  mgmt->msgCb = *pMsgCb;

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
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  *qWorkerMgmt = mgmt;

  qDebug("qworker initialized for node, type:%d, id:%d, handle:%p", mgmt->nodeType, mgmt->nodeId, mgmt);

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

void qWorkerDestroy(void **qWorkerMgmt) {
  if (NULL == qWorkerMgmt || NULL == *qWorkerMgmt) {
    return;
  }

  SQWorker *mgmt = *qWorkerMgmt;

  if (taosRemoveRef(gQwMgmt.qwRef, mgmt->refId)) {
    qError("remove qw from ref list failed, refId:%" PRIx64, mgmt->refId);
  }
}

int64_t qWorkerGetWaitTimeInQueue(void *qWorkerMgmt, EQueueType type) {
  return qwGetWaitTimeInQueue((SQWorker *)qWorkerMgmt, type);
}



