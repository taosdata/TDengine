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

SQWDebug     gQWDebug = {.statusEnable = true, .dumpEnable = true, .tmp = false};

int32_t qwDbgValidateStatus(QW_FPARAMS_DEF, int8_t oriStatus, int8_t newStatus, bool *ignore) {
  if (!gQWDebug.statusEnable) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;

  if (oriStatus == newStatus) {
    if (newStatus == JOB_TASK_STATUS_EXEC || newStatus == JOB_TASK_STATUS_FAIL) {
      *ignore = true;
      return TSDB_CODE_SUCCESS;
    }

    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  switch (oriStatus) {
    case JOB_TASK_STATUS_NULL:
      if (newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_FAIL &&
          newStatus != JOB_TASK_STATUS_INIT) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_INIT:
      if (newStatus != JOB_TASK_STATUS_DROP && newStatus != JOB_TASK_STATUS_EXEC
        && newStatus != JOB_TASK_STATUS_FAIL) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_EXEC:
      if (newStatus != JOB_TASK_STATUS_PART_SUCC && newStatus != JOB_TASK_STATUS_SUCC &&
          newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_DROP) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_PART_SUCC:
      if (newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_SUCC &&
          newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_DROP) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_SUCC:
      if (newStatus != JOB_TASK_STATUS_DROP && newStatus != JOB_TASK_STATUS_FAIL) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_FAIL:
      if (newStatus != JOB_TASK_STATUS_DROP) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      break;

    case JOB_TASK_STATUS_DROP:
      if (newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_PART_SUCC) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      break;

    default:
      QW_TASK_ELOG("invalid task origStatus:%s", jobTaskStatusStr(oriStatus));
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;

_return:

  QW_TASK_ELOG("invalid task status update from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));
  QW_RET(code);
}

void qwDbgDumpSchInfo(SQWorker *mgmt, SQWSchStatus *sch, int32_t i) {
  QW_LOCK(QW_READ, &sch->tasksLock);
  QW_DLOG("the %dth scheduler status, hbBrokenTs:%" PRId64 ",taskNum:%d", i, sch->hbBrokenTs, taosHashGetSize(sch->tasksHash));
  QW_UNLOCK(QW_READ, &sch->tasksLock);
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

  QW_DUMP("total remain ctx num %d", taosHashGetSize(mgmt->ctxHash));
}


int32_t qwDbgBuildAndSendRedirectRsp(int32_t rspType, SRpcHandleInfo *pConn, int32_t code, SEpSet *pEpSet) {
  int32_t contLen = 0;
  char* rsp = NULL;
  
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

int32_t qwDbgResponseRedirect(SQWMsg *qwMsg, SQWTaskCtx *ctx) {
  if (gQWDebug.tmp) {
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
      qwDbgBuildAndSendRedirectRsp(qwMsg->msgType + 1, &qwMsg->connInfo, TSDB_CODE_RPC_REDIRECT, &epSet);
      return TSDB_CODE_SUCCESS;
    }
    
    if (TDMT_SCH_MERGE_QUERY == qwMsg->msgType && (0 == taosRand() % 3)) {
      QW_SET_PHASE(ctx, QW_PHASE_POST_QUERY);
      qwDbgBuildAndSendRedirectRsp(qwMsg->msgType + 1, &qwMsg->connInfo, TSDB_CODE_RPC_REDIRECT, NULL);
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void qwDbgSimulateSleep() {
  if (!gQWDebug.sleepSimulate) {
    return;
  }

  taosSsleep(taosRand() % 10);
}

void qwDbgSimulateDead(QW_FPARAMS_DEF, SQWTaskCtx *ctx, int32_t msgType) {
  if (!gQWDebug.deadSimulate) {
    return;
  }

  SRpcHandleInfo *pConn = ((msgType == TDMT_SCH_FETCH || msgType == TDMT_SCH_MERGE_FETCH) ? &ctx->dataConnInfo : &ctx->ctrlConnInfo);
  qwBuildAndSendErrorRsp(msgType + 1, pConn, TSDB_CODE_RPC_BROKEN_LINK);

  qwDropTask(QW_FPARAMS());
}



int32_t qwDbgEnableDebug(char *option) {
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
    gQWDebug.sleepSimulate = true;
    qError("qw dead debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "tmp")) {
    gQWDebug.tmp = true;
    qError("qw tmp debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid qw debug option:%s", option);
  
  return TSDB_CODE_APP_ERROR;
}


