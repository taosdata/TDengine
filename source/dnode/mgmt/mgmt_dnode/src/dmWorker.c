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
#include "streamMsg.h"

static void *dmStatusThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-status");

  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    if (curTime < lastTime) lastTime = curTime;
    float interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsStatusInterval) {
      dmSendStatusReq(pMgmt);
      lastTime = curTime;
    }
  }

  return NULL;
}

static void *dmConfigThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-config");
  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped || tsConfigInited) break;

    int64_t curTime = taosGetTimestampMs();
    if (curTime < lastTime) lastTime = curTime;
    float interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsStatusInterval) {
      dmSendConfigReq(pMgmt);
      lastTime = curTime;
    }
  }
  return NULL;
}

static void *dmStatusInfoThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-status-info");

  int32_t upTimeCount = 0;
  int64_t upTime = 0;

  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    if (curTime < lastTime) lastTime = curTime;
    float interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsStatusInterval) {
      dmUpdateStatusInfo(pMgmt);
      lastTime = curTime;

      if ((upTimeCount = ((upTimeCount + 1) & 63)) == 0) {
        upTime = taosGetOsUptime() - tsDndStartOsUptime;
        if (upTime > 0) tsDndUpTime = upTime;
      }
    }
  }

  return NULL;
}

#if defined(TD_ENTERPRISE)
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
#endif

#ifdef USE_MONITOR
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
        taosMemoryTrim(0, NULL);
      }
    }
    if (atomic_val_compare_exchange_8(&tsNeedTrim, 1, 0)) {
      taosMemoryTrim(0, NULL);
    }
  }

  return NULL;
}
#endif
#ifdef USE_AUDIT
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
#endif
#ifdef USE_REPORT
static void *dmCrashReportThreadFp(void *param) {
  int32_t     code = 0;
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
  int32_t   loopTimes = reportPeriodNum;

  STelemAddrMgmt mgt = {0};
  code = taosTelemetryMgtInit(&mgt, tsTelemServer);
  if (code != 0) {
    dError("failed to init telemetry since %s", tstrerror(code));
    return NULL;
  }
  code = initCrashLogWriter();
  if (code != 0) {
    dError("failed to init crash log writer since %s", tstrerror(code));
    return NULL;
  }

  while (1) {
    checkAndPrepareCrashInfo();
    if ((pMgmt->pData->dropped || pMgmt->pData->stopped) && reportThreadSetQuit()) {
      break;
    }
    if (loopTimes++ < reportPeriodNum) {
      taosMsleep(sleepTime);
      if (loopTimes < 0) loopTimes = reportPeriodNum;
      continue;
    }
    taosReadCrashInfo(filepath, &pMsg, &msgLen, &pFile);
    if (pMsg && msgLen > 0) {
      if (taosSendTelemReport(&mgt, tsSvrCrashReportUri, tsTelemPort, pMsg, msgLen, HTTP_FLAT) != 0) {
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
      dInfo("no crash info was found");
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
  taosTelemetryDestroy(&mgt);

  return NULL;
}
#endif

static void *dmMetricsThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-metrics");
  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    if (curTime < lastTime) lastTime = curTime;
    float interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsMetricsInterval) {
      (*pMgmt->sendMetricsReportFp)();
      (*pMgmt->metricsCleanExpiredSamplesFp)();
      lastTime = curTime;
    }
  }
  return NULL;
}

int32_t dmStartStatusThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
  (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
  if (taosThreadCreate(&pMgmt->statusThread, &thAttr, dmStatusThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create status thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-status", "initialized");
  return 0;
}

int32_t dmStartConfigThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
  (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
  if (taosThreadCreate(&pMgmt->configThread, &thAttr, dmConfigThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create config thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("config-status", "initialized");
  return 0;
}

int32_t dmStartStatusInfoThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
  (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
  if (taosThreadCreate(&pMgmt->statusInfoThread, &thAttr, dmStatusInfoThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create status Info thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-status-info", "initialized");
  return 0;
}

void dmStopStatusThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->statusThread)) {
    (void)taosThreadJoin(pMgmt->statusThread, NULL);
    taosThreadClear(&pMgmt->statusThread);
  }
}

void dmStopConfigThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->configThread)) {
    (void)taosThreadJoin(pMgmt->configThread, NULL);
    taosThreadClear(&pMgmt->configThread);
  }
}

void dmStopStatusInfoThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->statusInfoThread)) {
    (void)taosThreadJoin(pMgmt->statusInfoThread, NULL);
    taosThreadClear(&pMgmt->statusInfoThread);
  }
}
#ifdef TD_ENTERPRISE
int32_t dmStartNotifyThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->notifyThread, &thAttr, dmNotifyThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create notify thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-notify", "initialized");
  return 0;
}

void dmStopNotifyThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->notifyThread)) {
    if (tsem_post(&dmNotifyHdl.sem) != 0) {
      dError("failed to post notify sem");
    }

    (void)taosThreadJoin(pMgmt->notifyThread, NULL);
    taosThreadClear(&pMgmt->notifyThread);
  }
  if (tsem_destroy(&dmNotifyHdl.sem) != 0) {
    dError("failed to destroy notify sem");
  }
}
#endif
int32_t dmStartMonitorThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
#ifdef USE_MONITOR
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->monitorThread, &thAttr, dmMonitorThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create monitor thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-monitor", "initialized");
#endif
  return 0;
}

int32_t dmStartAuditThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
#ifdef USE_AUDIT  
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->auditThread, &thAttr, dmAuditThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create audit thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-audit", "initialized");
#endif  
  return 0;
}

int32_t dmStartMetricsThread(SDnodeMgmt *pMgmt) {
  int32_t code = 0;
#ifdef USE_MONITOR
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->metricsThread, &thAttr, dmMetricsThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create metrics thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-metrics", "initialized");
#endif
  return 0;
}

void dmStopMonitorThread(SDnodeMgmt *pMgmt) {
#ifdef USE_MONITOR
  if (taosCheckPthreadValid(pMgmt->monitorThread)) {
    (void)taosThreadJoin(pMgmt->monitorThread, NULL);
    taosThreadClear(&pMgmt->monitorThread);
  }
#endif
}

void dmStopAuditThread(SDnodeMgmt *pMgmt) {
#ifdef USE_AUDIT
  if (taosCheckPthreadValid(pMgmt->auditThread)) {
    (void)taosThreadJoin(pMgmt->auditThread, NULL);
    taosThreadClear(&pMgmt->auditThread);
  }
#endif
}

int32_t dmStartCrashReportThread(SDnodeMgmt *pMgmt) {
  int32_t code = 0;
#ifdef USE_REPORT
  if (!tsEnableCrashReport) {
    return 0;
  }

  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->crashReportThread, &thAttr, dmCrashReportThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create crashReport thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-crashReport", "initialized");
#endif
  return 0;
}

void dmStopCrashReportThread(SDnodeMgmt *pMgmt) {
#ifdef USE_REPORT
  if (!tsEnableCrashReport) {
    return;
  }

  if (taosCheckPthreadValid(pMgmt->crashReportThread)) {
    (void)taosThreadJoin(pMgmt->crashReportThread, NULL);
    taosThreadClear(&pMgmt->crashReportThread);
  }
#endif
}

void dmStopMetricsThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->metricsThread)) {
    (void)taosThreadJoin(pMgmt->metricsThread, NULL);
    taosThreadClear(&pMgmt->metricsThread);
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
    case TDMT_DND_ALTER_SNODE:
      code = (*pMgmt->processAlterNodeFp)(SNODE, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = (*pMgmt->processDropNodeFp)(SNODE, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = (*pMgmt->processCreateNodeFp)(BNODE, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = (*pMgmt->processDropNodeFp)(BNODE, pMsg);
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

int32_t dmDispatchStreamHbMsg(struct SDispatchWorkerPool* pPool, void* pParam, int32_t *pWorkerIdx) {
  SRpcMsg* pMsg = (SRpcMsg*)pParam;
  if (pMsg->code) {
    *pWorkerIdx = 0;
    return TSDB_CODE_SUCCESS;
  }
  SStreamMsgGrpHeader* pHeader = (SStreamMsgGrpHeader*)pMsg->pCont;
  *pWorkerIdx = pHeader->streamGid % tsNumOfStreamMgmtThreads;
  return TSDB_CODE_SUCCESS;
}


static void dmProcessStreamMgmtQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SDnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  STraceId   *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, will be processed in dnode stream mgmt queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

  switch (pMsg->msgType) {
    case TDMT_MND_STREAM_HEARTBEAT_RSP:
      code = dmProcessStreamHbRsp(pMgmt, pMsg);
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

  SDispatchWorkerPool* pStMgmtpool = &pMgmt->streamMgmtWorker;
  pStMgmtpool->max = tsNumOfStreamMgmtThreads;
  pStMgmtpool->name = "dnode-stream-mgmt";
  code = tDispatchWorkerInit(pStMgmtpool);
  if (code != 0) {
    dError("failed to start dnode-stream-mgmt worker since %s", tstrerror(code));
    return code;
  }
  code = tDispatchWorkerAllocQueue(pStMgmtpool, pMgmt, (FItem)dmProcessStreamMgmtQueue, dmDispatchStreamHbMsg);
  if (code != 0) {
    dError("failed to allocate dnode-stream-mgmt worker queue since %s", tstrerror(code));
    return code;
  }

  dDebug("dnode workers are initialized");
  return 0;
}

void dmStopWorker(SDnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  tDispatchWorkerCleanup(&pMgmt->streamMgmtWorker);
  dDebug("dnode workers are closed");
}

int32_t dmPutNodeMsgToMgmtQueue(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->mgmtWorker;
  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t dmPutMsgToStreamMgmtQueue(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return tAddTaskIntoDispatchWorkerPool(&pMgmt->streamMgmtWorker, pMsg);
}
