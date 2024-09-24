/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dmInt.h"
#include "tgrant.h"
#include "thttp.h"

static void *dmStatusThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-status");

  int32_t upTimeCount = 0;
  int64_t upTime = 0;

  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    if (curTime < lastTime) lastTime = curTime;
    float interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsStatusInterval) {
      dmSendStatusReq(pMgmt);
      lastTime = curTime;

      if ((upTimeCount = ((upTimeCount + 1) & 63)) == 0) {
        upTime = taosGetOsUptime() - tsDndStartOsUptime;
        tsDndUpTime = TMAX(tsDndUpTime, upTime);
      }
    }
  }

  return NULL;
}

SDmNotifyHandle dmNotifyHdl = {.state = 0};
#define TIMESERIES_STASH_NUM 5
static void *dmNotifyThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-notify");

  if (tsem_init(&dmNotifyHdl.sem, 0, 0) != 0) {
    return NULL;
  }

  // calculate approximate timeSeries per second
  int64_t  notifyTimeStamp[TIMESERIES_STASH_NUM];
  int64_t  notifyTimeSeries[TIMESERIES_STASH_NUM];
  int64_t  approximateTimeSeries = 0;
  uint64_t nTotalNotify = 0;
  int32_t  head, tail = 0;

  bool       wait = true;
  int32_t    nDnode = 0;
  int64_t    lastNotify = 0;
  int64_t    lastFetchDnode = 0;
  SNotifyReq req = {0};
  while (1) {
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;
    if (wait) tsem_wait(&dmNotifyHdl.sem);
    atomic_store_8(&dmNotifyHdl.state, 1);

    int64_t remainTimeSeries = grantRemain(TSDB_GRANT_TIMESERIES);
    if (remainTimeSeries == INT64_MAX || remainTimeSeries <= 0) {
      goto _skip;
    }
    int64_t current = taosGetTimestampMs();
    if (current - lastFetchDnode > 1000) {
      nDnode = dmGetDnodeSize(pMgmt->pData);
      if (nDnode < 1) nDnode = 1;
      lastFetchDnode = current;
    }
    if (req.dnodeId == 0 || req.clusterId == 0) {
      req.dnodeId = pMgmt->pData->dnodeId;
      req.clusterId = pMgmt->pData->clusterId;
    }

    if (current - lastNotify < 10) {
      int64_t nCmprTimeSeries = approximateTimeSeries / 100;
      if (nCmprTimeSeries < 1e5) nCmprTimeSeries = 1e5;
      if (remainTimeSeries > nCmprTimeSeries * 10) {
        taosMsleep(10);
      } else if (remainTimeSeries > nCmprTimeSeries * 5) {
        taosMsleep(5);
      } else {
        taosMsleep(2);
      }
    }

    SMonVloadInfo vinfo = {0};
    (*pMgmt->getVnodeLoadsLiteFp)(&vinfo);
    req.pVloads = vinfo.pVloads;
    int32_t nVgroup = taosArrayGetSize(req.pVloads);
    int64_t nTimeSeries = 0;
    for (int32_t i = 0; i < nVgroup; ++i) {
      SVnodeLoadLite *vload = TARRAY_GET_ELEM(req.pVloads, i);
      nTimeSeries += vload->nTimeSeries;
    }
    notifyTimeSeries[tail] = nTimeSeries;
    notifyTimeStamp[tail] = taosGetTimestampNs();
    ++nTotalNotify;

    approximateTimeSeries = 0;
    if (nTotalNotify >= TIMESERIES_STASH_NUM) {
      head = tail - TIMESERIES_STASH_NUM + 1;
      if (head < 0) head += TIMESERIES_STASH_NUM;
      int64_t timeDiff = notifyTimeStamp[tail] - notifyTimeStamp[head];
      int64_t tsDiff = notifyTimeSeries[tail] - notifyTimeSeries[head];
      if (tsDiff > 0) {
        if (timeDiff > 0 && timeDiff < 1e9) {
          approximateTimeSeries = (double)tsDiff * 1e9 / timeDiff;
          if ((approximateTimeSeries * nDnode) > remainTimeSeries) {
            dmSendNotifyReq(pMgmt, &req);
          }
        } else {
          dmSendNotifyReq(pMgmt, &req);
        }
      }
    } else {
      dmSendNotifyReq(pMgmt, &req);
    }
    if (++tail == TIMESERIES_STASH_NUM) tail = 0;

    tFreeSNotifyReq(&req);
    lastNotify = taosGetTimestampMs();
  _skip:
    if (1 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 0)) {
      wait = true;
      continue;
    }
    wait = false;
  }

  return NULL;
}

static void *dmMonitorThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  int64_t     lastTimeForBasic = taosGetTimestampMs();
  setThreadName("dnode-monitor");

  static int32_t TRIM_FREQ = 20;
  int32_t        trimCount = 0;

  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();

    if (curTime < lastTime) lastTime = curTime;
    float interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsMonitorInterval) {
      (*pMgmt->sendMonitorReportFp)();
      (*pMgmt->monitorCleanExpiredSamplesFp)();
      lastTime = curTime;

      trimCount = (trimCount + 1) % TRIM_FREQ;
      if (trimCount == 0) {
        taosMemoryTrim(0);
      }
    }
  }

  return NULL;
}

static void *dmAuditThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-audit");

  while (1) {
    taosMsleep(100);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    if (curTime < lastTime) lastTime = curTime;
    float interval = curTime - lastTime;
    if (interval >= tsAuditInterval) {
      (*pMgmt->sendAuditRecordsFp)();
      lastTime = curTime;
    }
  }

  return NULL;
}

static void *dmCrashReportThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-crashReport");
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.taosdCrashLog", tsLogDir, TD_DIRSEP);
  char     *pMsg = NULL;
  int64_t   msgLen = 0;
  TdFilePtr pFile = NULL;
  bool      truncateFile = false;
  int32_t   sleepTime = 200;
  int32_t   reportPeriodNum = 3600 * 1000 / sleepTime;
  ;
  int32_t loopTimes = reportPeriodNum;

  while (1) {
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;
    if (loopTimes++ < reportPeriodNum) {
      taosMsleep(sleepTime);
      continue;
    }

    taosReadCrashInfo(filepath, &pMsg, &msgLen, &pFile);
    if (pMsg && msgLen > 0) {
      if (taosSendHttpReport(tsTelemServer, tsSvrCrashReportUri, tsTelemPort, pMsg, msgLen, HTTP_FLAT) != 0) {
        dError("failed to send crash report");
        if (pFile) {
          taosReleaseCrashLogFile(pFile, false);
          pFile = NULL;

          taosMsleep(sleepTime);
          loopTimes = 0;
          continue;
        }
      } else {
        dInfo("succeed to send crash report");
        truncateFile = true;
      }
    } else {
      dDebug("no crash info");
    }

    taosMemoryFree(pMsg);

    if (pMsg && msgLen > 0) {
      pMsg = NULL;
      continue;
    }

    if (pFile) {
      taosReleaseCrashLogFile(pFile, truncateFile);
      pFile = NULL;
      truncateFile = false;
    }

    taosMsleep(sleepTime);
    loopTimes = 0;
  }

  return NULL;
}

int32_t dmStartStatusThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  TAOS_CHECK_GOTO(taosThreadAttrInit(&thAttr), NULL, _exception);

  TAOS_CHECK_GOTO(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE), NULL, _exception);
  if (taosThreadCreate(&pMgmt->statusThread, &thAttr, dmStatusThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create status thread since %s", tstrerror(code));
    return code;
  }

  TAOS_CHECK_GOTO(taosThreadAttrDestroy(&thAttr), NULL, _exception);
  tmsgReportStartup("dnode-status", "initialized");
  return 0;

_exception:
  dError("failed to create status thread since %s", tstrerror(code));
  return code;
}

void dmStopStatusThread(SDnodeMgmt *pMgmt) {
  int32_t code = 0;
  if (taosCheckPthreadValid(pMgmt->statusThread)) {
    code = taosThreadJoin(pMgmt->statusThread, NULL);
    if (code != 0) {
      dError("failed to stop status thread since %s", tstrerror(code));
    }
    taosThreadClear(&pMgmt->statusThread);
  }
}

int32_t dmStartNotifyThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  TAOS_CHECK_GOTO(taosThreadAttrInit(&thAttr), NULL, _exception);
  TAOS_CHECK_GOTO(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE), NULL, _exception);
  if (taosThreadCreate(&pMgmt->notifyThread, &thAttr, dmNotifyThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create notify thread since %s", tstrerror(code));
    return code;
  }

  TAOS_CHECK_GOTO(taosThreadAttrDestroy(&thAttr), NULL, _exception);
  tmsgReportStartup("dnode-notify", "initialized");
  return 0;
_exception:
  dError("failed to start notify thread since %s", tstrerror(code));
  return code;
}

void dmStopNotifyThread(SDnodeMgmt *pMgmt) {
  int32_t code = 0;
  if (taosCheckPthreadValid(pMgmt->notifyThread)) {
    TAOS_CHECK_GOTO(tsem_post(&dmNotifyHdl.sem), NULL, _exception);
    TAOS_CHECK_GOTO(taosThreadJoin(pMgmt->notifyThread, NULL), NULL, _exception);
    taosThreadClear(&pMgmt->notifyThread);
  }
  TAOS_CHECK_GOTO(tsem_destroy(&dmNotifyHdl.sem), NULL, _exception);
  return;
_exception:
  dError("failed to stop notify thread since %s", tstrerror(code));
  return;
}

int32_t dmStartMonitorThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  TAOS_CHECK_GOTO(taosThreadAttrInit(&thAttr), NULL, _exception);
  TAOS_CHECK_GOTO(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE), NULL, _exception);
  if (taosThreadCreate(&pMgmt->monitorThread, &thAttr, dmMonitorThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create monitor thread since %s", tstrerror(code));
    return code;
  }

  TAOS_CHECK_GOTO(taosThreadAttrDestroy(&thAttr), NULL, _exception);
  tmsgReportStartup("dnode-monitor", "initialized");
  return 0;
_exception:
  dError("failed to start monitor thread since %s", tstrerror(code));
  return code;
}

int32_t dmStartAuditThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  TAOS_CHECK_GOTO(taosThreadAttrInit(&thAttr), NULL, _exception);
  TAOS_CHECK_GOTO(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE), NULL, _exception);
  if (taosThreadCreate(&pMgmt->auditThread, &thAttr, dmAuditThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create audit thread since %s", tstrerror(code));
    return code;
  }

  TAOS_CHECK_GOTO(taosThreadAttrDestroy(&thAttr), NULL, _exception);
  tmsgReportStartup("dnode-audit", "initialized");
  return 0;
_exception:
  dError("failed to start audit thread since %s", tstrerror(code));
  return code;
}

void dmStopMonitorThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->monitorThread)) {
    (void)taosThreadJoin(pMgmt->monitorThread, NULL);
    taosThreadClear(&pMgmt->monitorThread);
  }
}

void dmStopAuditThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->auditThread)) {
    (void)taosThreadJoin(pMgmt->auditThread, NULL);
    taosThreadClear(&pMgmt->auditThread);
  }
}

int32_t dmStartCrashReportThread(SDnodeMgmt *pMgmt) {
  int32_t code = 0;
  if (!tsEnableCrashReport) {
    return 0;
  }

  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->crashReportThread, &thAttr, dmCrashReportThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create crashReport thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-crashReport", "initialized");
  return 0;
}

void dmStopCrashReportThread(SDnodeMgmt *pMgmt) {
  if (!tsEnableCrashReport) {
    return;
  }

  if (taosCheckPthreadValid(pMgmt->crashReportThread)) {
    (void)taosThreadJoin(pMgmt->crashReportThread, NULL);
    taosThreadClear(&pMgmt->crashReportThread);
  }
}

static void dmProcessMgmtQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SDnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  STraceId   *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, will be processed in dnode queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

  switch (pMsg->msgType) {
    case TDMT_DND_CONFIG_DNODE:
      code = dmProcessConfigReq(pMgmt, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      code = dmProcessAuthRsp(pMgmt, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      code = dmProcessGrantRsp(pMgmt, pMsg);
      break;
    case TDMT_DND_CREATE_MNODE:
      code = (*pMgmt->processCreateNodeFp)(MNODE, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = (*pMgmt->processDropNodeFp)(MNODE, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = (*pMgmt->processCreateNodeFp)(QNODE, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = (*pMgmt->processDropNodeFp)(QNODE, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = (*pMgmt->processCreateNodeFp)(SNODE, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = (*pMgmt->processDropNodeFp)(SNODE, pMsg);
      break;
    case TDMT_DND_ALTER_MNODE_TYPE:
      code = (*pMgmt->processAlterNodeTypeFp)(MNODE, pMsg);
      break;
    case TDMT_DND_SERVER_STATUS:
      code = dmProcessServerRunStatus(pMgmt, pMsg);
      break;
    case TDMT_DND_SYSTABLE_RETRIEVE:
      code = dmProcessRetrieve(pMgmt, pMsg);
      break;
    case TDMT_MND_GRANT:
      code = dmProcessGrantReq(&pMgmt->pData->clusterId, pMsg);
      break;
    case TDMT_MND_GRANT_NOTIFY:
      code = dmProcessGrantNotify(NULL, pMsg);
      break;
    case TDMT_DND_CREATE_ENCRYPT_KEY:
      code = dmProcessCreateEncryptKeyReq(pMgmt, pMsg);
      break;
    default:
      code = TSDB_CODE_MSG_NOT_PROCESSED;
      dGError("msg:%p, not processed in mgmt queue, reason:%s", pMsg, tstrerror(code));
      break;
  }

  if (IsReq(pMsg)) {
    if (code != 0 && terrno != 0) code = terrno;
    SRpcMsg rsp = {
        .code = code,
        .pCont = pMsg->info.rsp,
        .contLen = pMsg->info.rspLen,
        .info = pMsg->info,
    };

    code = rpcSendResponse(&rsp);
    if (code != 0) {
      dError("failed to send response since %s", tstrerror(code));
    }
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  int32_t          code = 0;
  SSingleWorkerCfg cfg = {
      .min = 1,
      .max = 1,
      .name = "dnode-mgmt",
      .fp = (FItem)dmProcessMgmtQueue,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->mgmtWorker, &cfg)) != 0) {
    dError("failed to start dnode-mgmt worker since %s", tstrerror(code));
    return code;
  }

  dDebug("dnode workers are initialized");
  return 0;
}

void dmStopWorker(SDnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  dDebug("dnode workers are closed");
}

int32_t dmPutNodeMsgToMgmtQueue(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->mgmtWorker;
  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}
