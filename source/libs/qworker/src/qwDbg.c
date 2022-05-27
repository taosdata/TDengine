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

SQWDebug     gQWDebug = {.statusEnable = true, .dumpEnable = true};

int32_t qwDbgValidateStatus(QW_FPARAMS_DEF, int8_t oriStatus, int8_t newStatus, bool *ignore) {
  if (!gQWDebug.statusEnable) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;

  if (oriStatus == newStatus) {
    if (newStatus == JOB_TASK_STATUS_EXECUTING || newStatus == JOB_TASK_STATUS_FAILED) {
      *ignore = true;
      return TSDB_CODE_SUCCESS;
    }

    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  switch (oriStatus) {
    case JOB_TASK_STATUS_NULL:
      if (newStatus != JOB_TASK_STATUS_EXECUTING && newStatus != JOB_TASK_STATUS_FAILED &&
          newStatus != JOB_TASK_STATUS_NOT_START) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_NOT_START:
      if (newStatus != JOB_TASK_STATUS_CANCELLED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_EXECUTING:
      if (newStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED && newStatus != JOB_TASK_STATUS_SUCCEED &&
          newStatus != JOB_TASK_STATUS_FAILED && newStatus != JOB_TASK_STATUS_CANCELLING &&
          newStatus != JOB_TASK_STATUS_CANCELLED && newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_PARTIAL_SUCCEED:
      if (newStatus != JOB_TASK_STATUS_EXECUTING && newStatus != JOB_TASK_STATUS_SUCCEED &&
          newStatus != JOB_TASK_STATUS_CANCELLED && newStatus != JOB_TASK_STATUS_FAILED &&
          newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_SUCCEED:
      if (newStatus != JOB_TASK_STATUS_CANCELLED && newStatus != JOB_TASK_STATUS_DROPPING &&
          newStatus != JOB_TASK_STATUS_FAILED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_FAILED:
      if (newStatus != JOB_TASK_STATUS_CANCELLED && newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      break;

    case JOB_TASK_STATUS_CANCELLING:
      if (newStatus != JOB_TASK_STATUS_CANCELLED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_CANCELLED:
    case JOB_TASK_STATUS_DROPPING:
      if (newStatus != JOB_TASK_STATUS_FAILED && newStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
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

void qwDbgDumpSchInfo(SQWSchStatus *sch, int32_t i) {}

void qwDbgDumpMgmtInfo(SQWorker *mgmt) {
  if (!gQWDebug.dumpEnable) {
    return;
  }

  QW_LOCK(QW_READ, &mgmt->schLock);

  /*QW_DUMP("total remain schduler num:%d", taosHashGetSize(mgmt->schHash));*/

  void         *key = NULL;
  size_t        keyLen = 0;
  int32_t       i = 0;
  SQWSchStatus *sch = NULL;

  void *pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    sch = (SQWSchStatus *)pIter;
    qwDbgDumpSchInfo(sch, i);
    ++i;
    pIter = taosHashIterate(mgmt->schHash, pIter);
  }

  QW_UNLOCK(QW_READ, &mgmt->schLock);

  /*QW_DUMP("total remain ctx num:%d", taosHashGetSize(mgmt->ctxHash));*/
}


