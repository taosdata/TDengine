#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qwInt.h"
#include "qwMsg.h"
#include "qworker.h"
#include "tcommon.h"
#include "tmsg.h"
#include "tname.h"

SQWDebug gQWDebug = {.lockEnable = false,
                     .statusEnable = true,
                     .dumpEnable = false,
                     .redirectSimulate = false,
                     .deadSimulate = false,
                     .sleepSimulate = false,
                     .forceStop = false};

int32_t qwDbgValidateStatus(QW_FPARAMS_DEF, int8_t oriStatus, int8_t newStatus, bool *ignore, bool dynamicTask) {
  if (!gQWDebug.statusEnable) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;

  if (oriStatus == newStatus) {
    if (dynamicTask || newStatus == JOB_TASK_STATUS_EXEC || newStatus == JOB_TASK_STATUS_FAIL) {
      *ignore = true;
      return TSDB_CODE_SUCCESS;
    }

    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  switch (oriStatus) {
    case JOB_TASK_STATUS_NULL:
      if (newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_INIT) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_INIT:
      if (newStatus != JOB_TASK_STATUS_DROP && newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_FAIL) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_EXEC:
      if (newStatus != JOB_TASK_STATUS_PART_SUCC && newStatus != JOB_TASK_STATUS_SUCC &&
          newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_DROP) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_PART_SUCC:
      if (newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_SUCC && newStatus != JOB_TASK_STATUS_FAIL &&
          newStatus != JOB_TASK_STATUS_DROP) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_SUCC:
      if (newStatus != JOB_TASK_STATUS_DROP && newStatus != JOB_TASK_STATUS_FAIL) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_FAIL:
      if (newStatus != JOB_TASK_STATUS_DROP) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }
      break;

    case JOB_TASK_STATUS_DROP:
      if (newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_PART_SUCC) {
        QW_ERR_JRET(TSDB_CODE_APP_ERROR);
      }
      break;

    default:
      QW_TASK_ELOG("invalid task origStatus:%s", jobTaskStatusStr(oriStatus));
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;

_return:

  QW_TASK_ELOG("invalid task status update from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));
  QW_RET(code);
}

void qwDbgDumpSchInfo(SQWorker *mgmt, SQWSchStatus *sch, int32_t i) {
  QW_LOCK(QW_READ, &sch->tasksLock);
  int32_t taskNum = taosHashGetSize(sch->tasksHash);
  QW_DLOG("***The %dth scheduler status, hbBrokenTs:%" PRId64 ",taskNum:%d", i, sch->hbBrokenTs, taskNum);

  uint64_t qId, tId;
  int32_t  eId;
  SQWTaskStatus *pTask = NULL;
  void *pIter = taosHashIterate(sch->tasksHash, NULL);
  while (pIter) {
    pTask = (SQWTaskStatus *)pIter;
    void       *key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, tId, eId);

    QW_TASK_DLOG("job refId:%" PRIx64 ", code:%x, task status:%d", pTask->refId, pTask->code, pTask->status);

    pIter = taosHashIterate(sch->tasksHash, pIter);
  }
  
  QW_UNLOCK(QW_READ, &sch->tasksLock);
}

void qwDbgDumpTasksInfo(SQWorker *mgmt) {
  QW_DUMP("***Total remain ctx num %d", taosHashGetSize(mgmt->ctxHash));

  int32_t i = 0;
  SQWTaskCtx *ctx = NULL;
  uint64_t qId, tId;
  int32_t  eId;
  void *pIter = taosHashIterate(mgmt->ctxHash, NULL);
  while (pIter) {
    ctx = (SQWTaskCtx *)pIter;
    void       *key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, tId, eId);
    
    QW_TASK_DLOG("%p lock:%x, phase:%d, type:%d, explain:%d, needFetch:%d, localExec:%d, queryMsgType:%d, "
      "sId:%" PRId64 ", level:%d, queryGotData:%d, queryRsped:%d, queryEnd:%d, queryContinue:%d, queryInQueue:%d, "
      "rspCode:%x, affectedRows:%" PRId64 ", taskHandle:%p, sinkHandle:%p, tbNum:%d, events:%d,%d,%d,%d,%d",
      ctx, ctx->lock, ctx->phase, ctx->taskType, ctx->explain, ctx->needFetch, ctx->localExec, ctx->queryMsgType,
      ctx->sId, ctx->level, ctx->queryGotData, ctx->queryRsped, ctx->queryEnd, ctx->queryContinue, 
      ctx->queryInQueue, ctx->rspCode, ctx->affectedRows, ctx->taskHandle, ctx->sinkHandle, (int32_t)taosArrayGetSize(ctx->tbInfo),
      ctx->events[QW_EVENT_CANCEL], ctx->events[QW_EVENT_READY], 
      ctx->events[QW_EVENT_FETCH], ctx->events[QW_EVENT_DROP], ctx->events[QW_EVENT_CQUERY]);
      
    pIter = taosHashIterate(mgmt->ctxHash, pIter);
  }

}

void qwDbgDumpMgmtInfo(SQWorker *mgmt) {
  if (!gQWDebug.dumpEnable) {
    return;
  }

  QW_LOCK(QW_READ, &mgmt->schLock);

  QW_DUMP("total remain scheduler num %d", taosHashGetSize(mgmt->schHash));

  void         *key = NULL;
  size_t        keyLen = 0;
  int32_t       i = 0;
  SQWSchStatus *sch = NULL;

  void *pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    sch = (SQWSchStatus *)pIter;
    qwDbgDumpSchInfo(mgmt, sch, i);
    ++i;
    pIter = taosHashIterate(mgmt->schHash, pIter);
  }

  QW_UNLOCK(QW_READ, &mgmt->schLock);

  qwDbgDumpTasksInfo(mgmt);
}

int32_t qwDbgBuildAndSendRedirectRsp(int32_t rspType, SRpcHandleInfo *pConn, int32_t code, SEpSet *pEpSet) {
  int32_t contLen = 0;
  char   *rsp = NULL;

  if (pEpSet) {
    contLen = tSerializeSEpSet(NULL, 0, pEpSet);
    rsp = rpcMallocCont(contLen);
    tSerializeSEpSet(rsp, contLen, pEpSet);
  }

  SRpcMsg rpcRsp = {
      .msgType = rspType,
      .pCont = rsp,
      .contLen = contLen,
      .code = code,
      .info = *pConn,
  };
  rpcRsp.info.hasEpSet = 1;

  tmsgSendRsp(&rpcRsp);

  qDebug("response %s msg, code: %s", TMSG_INFO(rspType), tstrerror(code));

  return TSDB_CODE_SUCCESS;
}

void qwDbgSimulateRedirect(SQWMsg *qwMsg, SQWTaskCtx *ctx, bool *rsped) {
  static int32_t ignoreTime = 0;
  if (*rsped) {
    return;
  }

  if (gQWDebug.redirectSimulate) {
    if (++ignoreTime <= 10) {
      return;
    }

    if (TDMT_SCH_QUERY == qwMsg->msgType && (0 == taosRand() % 3)) {
      SEpSet epSet = {0};
      epSet.inUse = 1;
      epSet.numOfEps = 3;
      strcpy(epSet.eps[0].fqdn, "localhost");
      epSet.eps[0].port = 7100;
      strcpy(epSet.eps[1].fqdn, "localhost");
      epSet.eps[1].port = 7200;
      strcpy(epSet.eps[2].fqdn, "localhost");
      epSet.eps[2].port = 7300;

      ctx->phase = QW_PHASE_POST_QUERY;
      qwDbgBuildAndSendRedirectRsp(qwMsg->msgType + 1, &qwMsg->connInfo, TSDB_CODE_SYN_NOT_LEADER, &epSet);
      *rsped = true;
      return;
    }

    if (TDMT_SCH_MERGE_QUERY == qwMsg->msgType && (0 == taosRand() % 3)) {
      QW_SET_PHASE(ctx, QW_PHASE_POST_QUERY);
      qwDbgBuildAndSendRedirectRsp(qwMsg->msgType + 1, &qwMsg->connInfo, TSDB_CODE_SYN_NOT_LEADER, NULL);
      *rsped = true;
      return;
    }

    if ((TDMT_SCH_FETCH == qwMsg->msgType) && (0 == taosRand() % 9)) {
      qwDbgBuildAndSendRedirectRsp(qwMsg->msgType + 1, &qwMsg->connInfo, TSDB_CODE_SYN_NOT_LEADER, NULL);
      *rsped = true;
      return;
    }
  }
}

void qwDbgSimulateSleep(void) {
  if (!gQWDebug.sleepSimulate) {
    return;
  }

  static int32_t ignoreTime = 0;
  if (++ignoreTime > 10) {
    taosSsleep(taosRand() % 20);
  }
}

void qwDbgSimulateDead(QW_FPARAMS_DEF, SQWTaskCtx *ctx, bool *rsped) {
  if (!gQWDebug.deadSimulate) {
    return;
  }

  if (*rsped) {
    return;
  }

  static int32_t ignoreTime = 0;

  if (++ignoreTime > 10 && 0 == taosRand() % 9) {
    if (ctx->fetchMsgType == TDMT_SCH_FETCH) {
      qwBuildAndSendErrorRsp(TDMT_SCH_LINK_BROKEN, &ctx->ctrlConnInfo, TSDB_CODE_RPC_BROKEN_LINK);
      qwBuildAndSendErrorRsp(ctx->fetchMsgType + 1, &ctx->dataConnInfo, TSDB_CODE_QRY_TASK_CTX_NOT_EXIST);
      *rsped = true;
      
      taosSsleep(3);
      return;
    }

#if 0
    SRpcHandleInfo *pConn =
        ((ctx->msgType == TDMT_SCH_FETCH || ctx->msgType == TDMT_SCH_MERGE_FETCH) ? &ctx->dataConnInfo
                                                                                  : &ctx->ctrlConnInfo);                                                                              
    qwBuildAndSendErrorRsp(ctx->msgType + 1, pConn, TSDB_CODE_RPC_BROKEN_LINK);
    
    qwBuildAndSendDropMsg(QW_FPARAMS(), pConn);
    *rsped = true;
    
    return;
#endif    
  }
}

int32_t qWorkerDbgEnableDebug(char *option) {
  if (0 == strcasecmp(option, "lock")) {
    gQWDebug.lockEnable = true;
    qError("qw lock debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "status")) {
    gQWDebug.statusEnable = true;
    qError("qw status debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "dump")) {
    gQWDebug.dumpEnable = true;
    qError("qw dump debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "sleep")) {
    gQWDebug.sleepSimulate = true;
    qError("qw sleep debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "dead")) {
    gQWDebug.deadSimulate = true;
    qError("qw dead debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "redirect")) {
    gQWDebug.redirectSimulate = true;
    qError("qw redirect debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "forceStop")) {
    gQWDebug.forceStop = true;
    qError("qw forceStop debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid qw debug option:%s", option);

  return TSDB_CODE_APP_ERROR;
}
