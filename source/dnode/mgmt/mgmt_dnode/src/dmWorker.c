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
#include "thttp.h"

static void *dmStatusThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-status");

  const static int16_t TRIM_FREQ = 30;
  int32_t              trimCount = 0;
  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    float   interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsStatusInterval) {
      dmSendStatusReq(pMgmt);
      lastTime = curTime;

      trimCount = (trimCount + 1) % TRIM_FREQ;
      if (trimCount == 0) {
        taosMemoryTrim(0);
      }
    }
  }

  return NULL;
}

static void *dmMonitorThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();
  setThreadName("dnode-monitor");

  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t curTime = taosGetTimestampMs();
    float   interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsMonitorInterval) {
      (*pMgmt->sendMonitorReportFp)();
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
  char *pMsg = NULL;
  int64_t msgLen = 0;
  TdFilePtr pFile = NULL;
  bool truncateFile = false;
  int32_t sleepTime = 200;
  int32_t reportPeriodNum = 3600 * 1000 / sleepTime;;
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
      truncateFile = false;
    }
    
    taosMsleep(sleepTime);
    loopTimes = 0;
  }

  return NULL;
}


int32_t dmStartStatusThread(SDnodeMgmt *pMgmt) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->statusThread, &thAttr, dmStatusThreadFp, pMgmt) != 0) {
    dError("failed to create status thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-status", "initialized");
  return 0;
}

void dmStopStatusThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->statusThread)) {
    taosThreadJoin(pMgmt->statusThread, NULL);
    taosThreadClear(&pMgmt->statusThread);
  }
}

int32_t dmStartMonitorThread(SDnodeMgmt *pMgmt) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->monitorThread, &thAttr, dmMonitorThreadFp, pMgmt) != 0) {
    dError("failed to create monitor thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-monitor", "initialized");
  return 0;
}

void dmStopMonitorThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->monitorThread)) {
    taosThreadJoin(pMgmt->monitorThread, NULL);
    taosThreadClear(&pMgmt->monitorThread);
  }
}

int32_t dmStartCrashReportThread(SDnodeMgmt *pMgmt) {
  if (!tsEnableCrashReport) {
    return 0;
  }

  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->crashReportThread, &thAttr, dmCrashReportThreadFp, pMgmt) != 0) {
    dError("failed to create crashReport thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-crashReport", "initialized");
  return 0;
}

void dmStopCrashReportThread(SDnodeMgmt *pMgmt) {
  if (!tsEnableCrashReport) {
    return;
  }

  if (taosCheckPthreadValid(pMgmt->crashReportThread)) {
    taosThreadJoin(pMgmt->crashReportThread, NULL);
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
    case TDMT_DND_SERVER_STATUS:
      code = dmProcessServerRunStatus(pMgmt, pMsg);
      break;
    case TDMT_DND_SYSTABLE_RETRIEVE:
      code = dmProcessRetrieve(pMgmt, pMsg);
      break;
    case TDMT_MND_GRANT:
      code = dmProcessGrantReq(&pMgmt->pData->clusterId, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      dGError("msg:%p, not processed in mgmt queue", pMsg);
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
    rpcSendResponse(&rsp);
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  SSingleWorkerCfg cfg = {
      .min = 1,
      .max = 1,
      .name = "dnode-mgmt",
      .fp = (FItem)dmProcessMgmtQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &cfg) != 0) {
    dError("failed to start dnode-mgmt worker since %s", terrstr());
    return -1;
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
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}
