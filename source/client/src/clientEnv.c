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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalog.h"
#include "clientInt.h"
#include "clientLog.h"
#include "functionMgt.h"
#include "os.h"
#include "osSleep.h"
#include "query.h"
#include "qworker.h"
#include "scheduler.h"
#include "tcache.h"
#include "tglobal.h"
#include "thttp.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "tsched.h"
#include "ttime.h"
#include "tversion.h"

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include "cus_name.h"
#endif

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

STscDbg  tscDbg = {0};
SAppInfo appInfo;
int64_t  lastClusterId = 0;
int32_t  clientReqRefPool = -1;
int32_t  clientConnRefPool = -1;
int32_t  clientStop = -1;

int32_t timestampDeltaLimit = 900;  // s

static TdThreadOnce tscinit = PTHREAD_ONCE_INIT;
volatile int32_t    tscInitRes = 0;

static int32_t registerRequest(SRequestObj *pRequest, STscObj *pTscObj) {
  // connection has been released already, abort creating request.
  pRequest->self = taosAddRef(clientReqRefPool, pRequest);

  int32_t num = atomic_add_fetch_32(&pTscObj->numOfReqs, 1);

  if (pTscObj->pAppInfo) {
    SAppClusterSummary *pSummary = &pTscObj->pAppInfo->summary;

    int32_t total = atomic_add_fetch_64((int64_t *)&pSummary->totalRequests, 1);
    int32_t currentInst = atomic_add_fetch_64((int64_t *)&pSummary->currentRequests, 1);
    tscDebug("0x%" PRIx64 " new Request from connObj:0x%" PRIx64
             ", current:%d, app current:%d, total:%d, reqId:0x%" PRIx64,
             pRequest->self, pRequest->pTscObj->id, num, currentInst, total, pRequest->requestId);
  }

  return TSDB_CODE_SUCCESS;
}

static void deregisterRequest(SRequestObj *pRequest) {
  if (pRequest == NULL) {
    tscError("pRequest == NULL");
    return;
  }

  STscObj            *pTscObj = pRequest->pTscObj;
  SAppClusterSummary *pActivity = &pTscObj->pAppInfo->summary;

  int32_t currentInst = atomic_sub_fetch_64((int64_t *)&pActivity->currentRequests, 1);
  int32_t num = atomic_sub_fetch_32(&pTscObj->numOfReqs, 1);
  int32_t reqType = SLOW_LOG_TYPE_OTHERS;

  int64_t duration = taosGetTimestampUs() - pRequest->metric.start;
  tscDebug("0x%" PRIx64 " free Request from connObj: 0x%" PRIx64 ", reqId:0x%" PRIx64
           " elapsed:%.2f ms, "
           "current:%d, app current:%d",
           pRequest->self, pTscObj->id, pRequest->requestId, duration / 1000.0, num, currentInst);

  if (pRequest->pQuery && pRequest->pQuery->pRoot) {
    if (QUERY_NODE_VNODE_MODIFY_STMT == pRequest->pQuery->pRoot->type &&
        (0 == ((SVnodeModifyOpStmt *)pRequest->pQuery->pRoot)->sqlNodeType)) {
      tscDebug("insert duration %" PRId64 "us: parseCost:%" PRId64 "us, ctgCost:%" PRId64 "us, analyseCost:%" PRId64
               "us, planCost:%" PRId64 "us, exec:%" PRId64 "us",
               duration, pRequest->metric.parseCostUs, pRequest->metric.ctgCostUs, pRequest->metric.analyseCostUs,
               pRequest->metric.planCostUs, pRequest->metric.execCostUs);
      atomic_add_fetch_64((int64_t *)&pActivity->insertElapsedTime, duration);
      reqType = SLOW_LOG_TYPE_INSERT;
    } else if (QUERY_NODE_SELECT_STMT == pRequest->stmtType) {
      tscDebug("query duration %" PRId64 "us: parseCost:%" PRId64 "us, ctgCost:%" PRId64 "us, analyseCost:%" PRId64
               "us, planCost:%" PRId64 "us, exec:%" PRId64 "us",
               duration, pRequest->metric.parseCostUs, pRequest->metric.ctgCostUs, pRequest->metric.analyseCostUs,
               pRequest->metric.planCostUs, pRequest->metric.execCostUs);

      atomic_add_fetch_64((int64_t *)&pActivity->queryElapsedTime, duration);
      reqType = SLOW_LOG_TYPE_QUERY;
    } 
  }

  if (QUERY_NODE_VNODE_MODIFY_STMT == pRequest->stmtType || QUERY_NODE_INSERT_STMT == pRequest->stmtType) {
    sqlReqLog(pTscObj->id, pRequest->killed, pRequest->code, MONITORSQLTYPEINSERT);
  } else if (QUERY_NODE_SELECT_STMT == pRequest->stmtType) {
    sqlReqLog(pTscObj->id, pRequest->killed, pRequest->code, MONITORSQLTYPESELECT);
  } else if (QUERY_NODE_DELETE_STMT == pRequest->stmtType) {
    sqlReqLog(pTscObj->id, pRequest->killed, pRequest->code, MONITORSQLTYPEDELETE);
  }

  if (duration >= (tsSlowLogThreshold * 1000000UL)) {
    atomic_add_fetch_64((int64_t *)&pActivity->numOfSlowQueries, 1);
    if (tsSlowLogScope & reqType) {
      taosPrintSlowLog("PID:%d, Conn:%u, QID:0x%" PRIx64 ", Start:%" PRId64 ", Duration:%" PRId64 "us, SQL:%s",
                       taosGetPId(), pTscObj->connId, pRequest->requestId, pRequest->metric.start, duration,
                       pRequest->sqlstr);
      SlowQueryLog(pTscObj->id, pRequest->killed, pRequest->code, duration);
    }
  }

  releaseTscObj(pTscObj->id);
}

// todo close the transporter properly
void closeTransporter(SAppInstInfo *pAppInfo) {
  if (pAppInfo == NULL || pAppInfo->pTransporter == NULL) {
    return;
  }

  tscDebug("free transporter:%p in app inst %p", pAppInfo->pTransporter, pAppInfo);
  rpcClose(pAppInfo->pTransporter);
}

static bool clientRpcRfp(int32_t code, tmsg_t msgType) {
  if (NEED_REDIRECT_ERROR(code)) {
    if (msgType == TDMT_SCH_QUERY || msgType == TDMT_SCH_MERGE_QUERY || msgType == TDMT_SCH_FETCH ||
        msgType == TDMT_SCH_MERGE_FETCH || msgType == TDMT_SCH_QUERY_HEARTBEAT || msgType == TDMT_SCH_DROP_TASK ||
        msgType == TDMT_SCH_TASK_NOTIFY) {
      return false;
    }
    return true;
  } else {
    return false;
  }
}

// start timer for particular msgType
static bool clientRpcTfp(int32_t code, tmsg_t msgType) {
  if (msgType == TDMT_VND_SUBMIT || msgType == TDMT_VND_CREATE_TABLE) {
    return true;
  }
  return false;
}

// TODO refactor
void *openTransporter(const char *user, const char *auth, int32_t numOfThread) {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "TSC";
  rpcInit.numOfThreads = tsNumOfRpcThreads;
  rpcInit.cfp = processMsgFromServer;
  rpcInit.rfp = clientRpcRfp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = (char *)user;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.compressSize = tsCompressMsgSize;
  rpcInit.dfp = destroyAhandle;

  rpcInit.retryMinInterval = tsRedirectPeriod;
  rpcInit.retryStepFactor = tsRedirectFactor;
  rpcInit.retryMaxInterval = tsRedirectMaxPeriod;
  rpcInit.retryMaxTimeout = tsMaxRetryWaitTime;

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3);
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 1000);
  rpcInit.connLimitNum = connLimitNum;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;

  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));

  void *pDnodeConn = rpcOpen(&rpcInit);
  if (pDnodeConn == NULL) {
    tscError("failed to init connection to server");
    return NULL;
  }

  return pDnodeConn;
}

void destroyAllRequests(SHashObj *pRequests) {
  void *pIter = taosHashIterate(pRequests, NULL);
  while (pIter != NULL) {
    int64_t *rid = pIter;

    SRequestObj *pRequest = acquireRequest(*rid);
    if (pRequest) {
      destroyRequest(pRequest);
      releaseRequest(*rid);
    }

    pIter = taosHashIterate(pRequests, pIter);
  }
}

void stopAllRequests(SHashObj *pRequests) {
  void *pIter = taosHashIterate(pRequests, NULL);
  while (pIter != NULL) {
    int64_t *rid = pIter;

    SRequestObj *pRequest = acquireRequest(*rid);
    if (pRequest) {
      taos_stop_query(pRequest);
      releaseRequest(*rid);
    }

    pIter = taosHashIterate(pRequests, pIter);
  }
}

void destroyAppInst(SAppInstInfo *pAppInfo) {
  tscDebug("destroy app inst mgr %p", pAppInfo);

  taosThreadMutexLock(&appInfo.mutex);

  clientMonitorClose(pAppInfo->instKey);
  hbRemoveAppHbMrg(&pAppInfo->pAppHbMgr);
  taosHashRemove(appInfo.pInstMap, pAppInfo->instKey, strlen(pAppInfo->instKey));

  taosThreadMutexUnlock(&appInfo.mutex);

  taosMemoryFreeClear(pAppInfo->instKey);
  closeTransporter(pAppInfo);

  taosThreadMutexLock(&pAppInfo->qnodeMutex);
  taosArrayDestroy(pAppInfo->pQnodeList);
  taosThreadMutexUnlock(&pAppInfo->qnodeMutex);

  taosMemoryFree(pAppInfo);
}

void destroyTscObj(void *pObj) {
  if (NULL == pObj) {
    return;
  }

  STscObj *pTscObj = pObj;
  int64_t  tscId = pTscObj->id;
  tscTrace("begin to destroy tscObj %" PRIx64 " p:%p", tscId, pTscObj);

  SClientHbKey connKey = {.tscRid = pTscObj->id, .connType = pTscObj->connType};
  hbDeregisterConn(pTscObj, connKey);

  destroyAllRequests(pTscObj->pRequests);
  taosHashCleanup(pTscObj->pRequests);

  schedulerStopQueryHb(pTscObj->pAppInfo->pTransporter);
  tscDebug("connObj 0x%" PRIx64 " p:%p destroyed, remain inst totalConn:%" PRId64, pTscObj->id, pTscObj,
           pTscObj->pAppInfo->numOfConns);

  // In any cases, we should not free app inst here. Or an race condition rises.
  /*int64_t connNum = */ atomic_sub_fetch_64(&pTscObj->pAppInfo->numOfConns, 1);

  taosThreadMutexDestroy(&pTscObj->mutex);
  taosMemoryFree(pTscObj);

  tscTrace("end to destroy tscObj %" PRIx64 " p:%p", tscId, pTscObj);
}

void *createTscObj(const char *user, const char *auth, const char *db, int32_t connType, SAppInstInfo *pAppInfo) {
  STscObj *pObj = (STscObj *)taosMemoryCalloc(1, sizeof(STscObj));
  if (NULL == pObj) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pObj->pRequests = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pObj->pRequests) {
    taosMemoryFree(pObj);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pObj->connType = connType;
  pObj->pAppInfo = pAppInfo;
  pObj->appHbMgrIdx = pAppInfo->pAppHbMgr->idx;
  tstrncpy(pObj->user, user, sizeof(pObj->user));
  memcpy(pObj->pass, auth, TSDB_PASSWORD_LEN);

  if (db != NULL) {
    tstrncpy(pObj->db, db, tListLen(pObj->db));
  }

  taosThreadMutexInit(&pObj->mutex, NULL);
  pObj->id = taosAddRef(clientConnRefPool, pObj);

  atomic_add_fetch_64(&pObj->pAppInfo->numOfConns, 1);

  tscDebug("connObj created, 0x%" PRIx64 ",p:%p", pObj->id, pObj);
  return pObj;
}

STscObj *acquireTscObj(int64_t rid) { return (STscObj *)taosAcquireRef(clientConnRefPool, rid); }

int32_t releaseTscObj(int64_t rid) { return taosReleaseRef(clientConnRefPool, rid); }

void *createRequest(uint64_t connId, int32_t type, int64_t reqid) {
  SRequestObj *pRequest = (SRequestObj *)taosMemoryCalloc(1, sizeof(SRequestObj));
  if (NULL == pRequest) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  STscObj *pTscObj = acquireTscObj(connId);
  if (pTscObj == NULL) {
    taosMemoryFree(pRequest);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  SSyncQueryParam *interParam = taosMemoryCalloc(1, sizeof(SSyncQueryParam));
  if (interParam == NULL) {
    doDestroyRequest(pRequest);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  tsem_init(&interParam->sem, 0, 0);
  interParam->pRequest = pRequest;
  pRequest->body.interParam = interParam;

  pRequest->resType = RES_TYPE__QUERY;
  pRequest->requestId = reqid == 0 ? generateRequestId() : reqid;
  pRequest->metric.start = taosGetTimestampUs();

  pRequest->body.resInfo.convertUcs4 = true;  // convert ucs4 by default
  pRequest->type = type;
  pRequest->allocatorRefId = -1;

  pRequest->pDb = getDbOfConnection(pTscObj);
  pRequest->pTscObj = pTscObj;
  pRequest->inCallback = false;

  pRequest->msgBuf = taosMemoryCalloc(1, ERROR_MSG_BUF_DEFAULT_SIZE);
  pRequest->msgBufLen = ERROR_MSG_BUF_DEFAULT_SIZE;
  tsem_init(&pRequest->body.rspSem, 0, 0);

  if (registerRequest(pRequest, pTscObj)) {
    doDestroyRequest(pRequest);
    return NULL;
  }

  return pRequest;
}

void doFreeReqResultInfo(SReqResultInfo *pResInfo) {
  taosMemoryFreeClear(pResInfo->pRspMsg);
  taosMemoryFreeClear(pResInfo->length);
  taosMemoryFreeClear(pResInfo->row);
  taosMemoryFreeClear(pResInfo->pCol);
  taosMemoryFreeClear(pResInfo->fields);
  taosMemoryFreeClear(pResInfo->userFields);
  taosMemoryFreeClear(pResInfo->convertJson);

  if (pResInfo->convertBuf != NULL) {
    for (int32_t i = 0; i < pResInfo->numOfCols; ++i) {
      taosMemoryFreeClear(pResInfo->convertBuf[i]);
    }
    taosMemoryFreeClear(pResInfo->convertBuf);
  }
}

SRequestObj *acquireRequest(int64_t rid) { return (SRequestObj *)taosAcquireRef(clientReqRefPool, rid); }

int32_t releaseRequest(int64_t rid) { return taosReleaseRef(clientReqRefPool, rid); }

int32_t removeRequest(int64_t rid) { return taosRemoveRef(clientReqRefPool, rid); }

void destroySubRequests(SRequestObj *pRequest) {
  int32_t      reqIdx = -1;
  SRequestObj *pReqList[16] = {NULL};
  uint64_t     tmpRefId = 0;

  if (pRequest->relation.userRefId && pRequest->relation.userRefId != pRequest->self) {
    return;
  }

  SRequestObj *pTmp = pRequest;
  while (pTmp->relation.prevRefId) {
    tmpRefId = pTmp->relation.prevRefId;
    pTmp = acquireRequest(tmpRefId);
    if (pTmp) {
      pReqList[++reqIdx] = pTmp;
      releaseRequest(tmpRefId);
    } else {
      tscError("prev req ref 0x%" PRIx64 " is not there", tmpRefId);
      break;
    }
  }

  for (int32_t i = reqIdx; i >= 0; i--) {
    removeRequest(pReqList[i]->self);
  }

  tmpRefId = pRequest->relation.nextRefId;
  while (tmpRefId) {
    pTmp = acquireRequest(tmpRefId);
    if (pTmp) {
      tmpRefId = pTmp->relation.nextRefId;
      removeRequest(pTmp->self);
      releaseRequest(pTmp->self);
    } else {
      tscError("next req ref 0x%" PRIx64 " is not there", tmpRefId);
      break;
    }
  }
}

void doDestroyRequest(void *p) {
  if (NULL == p) {
    return;
  }

  SRequestObj *pRequest = (SRequestObj *)p;

  uint64_t reqId = pRequest->requestId;
  tscTrace("begin to destroy request %" PRIx64 " p:%p", reqId, pRequest);

  destroySubRequests(pRequest);

  taosHashRemove(pRequest->pTscObj->pRequests, &pRequest->self, sizeof(pRequest->self));

  schedulerFreeJob(&pRequest->body.queryJob, 0);

  destorySqlCallbackWrapper(pRequest->pWrapper);

  taosMemoryFreeClear(pRequest->msgBuf);
  taosMemoryFreeClear(pRequest->pDb);

  doFreeReqResultInfo(&pRequest->body.resInfo);
  tsem_destroy(&pRequest->body.rspSem);

  taosArrayDestroy(pRequest->tableList);
  taosArrayDestroy(pRequest->dbList);
  taosArrayDestroy(pRequest->targetTableList);

  destroyQueryExecRes(&pRequest->body.resInfo.execRes);

  if (pRequest->self) {
    deregisterRequest(pRequest);
  }

  if (pRequest->body.interParam) {
    tsem_destroy(&((SSyncQueryParam *)pRequest->body.interParam)->sem);
  }
  taosMemoryFree(pRequest->body.interParam);

  qDestroyQuery(pRequest->pQuery);
  nodesDestroyAllocator(pRequest->allocatorRefId);

  taosMemoryFreeClear(pRequest->effectiveUser);
  taosMemoryFreeClear(pRequest->sqlstr);
  taosMemoryFree(pRequest);
  tscTrace("end to destroy request %" PRIx64 " p:%p", reqId, pRequest);
}

void destroyRequest(SRequestObj *pRequest) {
  if (pRequest == NULL) {
    return;
  }

  taos_stop_query(pRequest);
  removeRequest(pRequest->self);
}

void taosStopQueryImpl(SRequestObj *pRequest) {
  pRequest->killed = true;

  // It is not a query, no need to stop.
  if (NULL == pRequest->pQuery || QUERY_EXEC_MODE_SCHEDULE != pRequest->pQuery->execMode) {
    tscDebug("request 0x%" PRIx64 " no need to be killed since not query", pRequest->requestId);
    return;
  }

  schedulerFreeJob(&pRequest->body.queryJob, TSDB_CODE_TSC_QUERY_KILLED);
  tscDebug("request %" PRIx64 " killed", pRequest->requestId);
}

void stopAllQueries(SRequestObj *pRequest) {
  int32_t      reqIdx = -1;
  SRequestObj *pReqList[16] = {NULL};
  uint64_t     tmpRefId = 0;

  if (pRequest->relation.userRefId && pRequest->relation.userRefId != pRequest->self) {
    return;
  }

  SRequestObj *pTmp = pRequest;
  while (pTmp->relation.prevRefId) {
    tmpRefId = pTmp->relation.prevRefId;
    pTmp = acquireRequest(tmpRefId);
    if (pTmp) {
      pReqList[++reqIdx] = pTmp;
      releaseRequest(tmpRefId);
    } else {
      tscError("prev req ref 0x%" PRIx64 " is not there", tmpRefId);
      break;
    }
  }

  for (int32_t i = reqIdx; i >= 0; i--) {
    taosStopQueryImpl(pReqList[i]);
  }

  taosStopQueryImpl(pRequest);

  tmpRefId = pRequest->relation.nextRefId;
  while (tmpRefId) {
    pTmp = acquireRequest(tmpRefId);
    if (pTmp) {
      tmpRefId = pTmp->relation.nextRefId;
      taosStopQueryImpl(pTmp);
      releaseRequest(pTmp->self);
    } else {
      tscError("next req ref 0x%" PRIx64 " is not there", tmpRefId);
      break;
    }
  }
}

void crashReportThreadFuncUnexpectedStopped(void) { atomic_store_32(&clientStop, -1); }

static void *tscCrashReportThreadFp(void *param) {
  setThreadName("client-crashReport");
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.taosCrashLog", tsLogDir, TD_DIRSEP);
  char     *pMsg = NULL;
  int64_t   msgLen = 0;
  TdFilePtr pFile = NULL;
  bool      truncateFile = false;
  int32_t   sleepTime = 200;
  int32_t   reportPeriodNum = 3600 * 1000 / sleepTime;
  int32_t   loopTimes = reportPeriodNum;

#ifdef WINDOWS
  if (taosCheckCurrentInDll()) {
    atexit(crashReportThreadFuncUnexpectedStopped);
  }
#endif

  if (-1 != atomic_val_compare_exchange_32(&clientStop, -1, 0)) {
    return NULL;
  }

  while (1) {
    if (clientStop > 0) break;
    if (loopTimes++ < reportPeriodNum) {
      taosMsleep(sleepTime);
      continue;
    }

    taosReadCrashInfo(filepath, &pMsg, &msgLen, &pFile);
    if (pMsg && msgLen > 0) {
      if (taosSendHttpReport(tsTelemServer, tsClientCrashReportUri, tsTelemPort, pMsg, msgLen, HTTP_FLAT) != 0) {
        tscError("failed to send crash report");
        if (pFile) {
          taosReleaseCrashLogFile(pFile, false);
          pFile = NULL;

          taosMsleep(sleepTime);
          loopTimes = 0;
          continue;
        }
      } else {
        tscInfo("succeed to send crash report");
        truncateFile = true;
      }
    } else {
      tscDebug("no crash info");
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

  clientStop = -2;
  return NULL;
}

int32_t tscCrashReportInit() {
  if (!tsEnableCrashReport) {
    return 0;
  }

  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  TdThread crashReportThread;
  if (taosThreadCreate(&crashReportThread, &thAttr, tscCrashReportThreadFp, NULL) != 0) {
    tscError("failed to create crashReport thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  return 0;
}

void tscStopCrashReport() {
  if (!tsEnableCrashReport) {
    return;
  }

  if (atomic_val_compare_exchange_32(&clientStop, 0, 1)) {
    tscDebug("hb thread already stopped");
    return;
  }

  while (atomic_load_32(&clientStop) > 0) {
    taosMsleep(100);
  }
}

void tscWriteCrashInfo(int signum, void *sigInfo, void *context) {
  char       *pMsg = NULL;
  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;
  int64_t     msgLen = -1;

  if (tsEnableCrashReport) {
    if (taosGenCrashJsonMsg(signum, &pMsg, lastClusterId, appInfo.startTime)) {
      taosPrintLog(flags, level, dflag, "failed to generate crash json msg");
    } else {
      msgLen = strlen(pMsg);
    }
  }

  taosLogCrashInfo("taos", pMsg, msgLen, signum, sigInfo);
}

void taos_init_imp(void) {
#if defined(LINUX)
  if (tscDbg.memEnable) {
    int32_t code = taosMemoryDbgInit();
    if (code) {
      printf("failed to init memory dbg, error:%s\n", tstrerror(code));
    } else {
      tsAsyncLog = false;
      printf("memory dbg enabled\n");
    }
  }
#endif

  // In the APIs of other program language, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated resource to suppress the valgrind warning.
  atexit(taos_cleanup);
  errno = TSDB_CODE_SUCCESS;
  taosSeedRand(taosGetTimestampSec());

  appInfo.pid = taosGetPId();
  appInfo.startTime = taosGetTimestampMs();
  appInfo.pInstMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

  deltaToUtcInitOnce();

  char logDirName[64] = {0};
#ifdef CUS_PROMPT
  snprintf(logDirName, 64, "%slog", CUS_PROMPT);
#else
  snprintf(logDirName, 64, "taoslog");
#endif
  if (taosCreateLog(logDirName, 10, configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    printf(" WARING: Create %s failed:%s. configDir=%s\n", logDirName, strerror(errno), configDir);
    tscInitRes = -1;
    return;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    tscInitRes = -1;
    return;
  }

  initQueryModuleMsgHandle();

  if (taosConvInit() != 0) {
    tscError("failed to init conv");
    return;
  }

  rpcInit();

  SCatalogCfg cfg = {.maxDBCacheNum = 100, .maxTblCacheNum = 100};
  catalogInit(&cfg);

  schedulerInit();
  tscDebug("starting to initialize TAOS driver");

#ifndef WINDOWS
  taosSetCoreDump(true);
#endif

  initTaskQueue();
  fmFuncMgtInit();
  nodesInitAllocatorSet();

  clientConnRefPool = taosOpenRef(200, destroyTscObj);
  clientReqRefPool = taosOpenRef(40960, doDestroyRequest);

  // transDestroyBuffer(&conn->readBuf);
  taosGetAppName(appInfo.appName, NULL);
  taosThreadMutexInit(&appInfo.mutex, NULL);

  tscCrashReportInit();

  tscDebug("client is initialized successfully");
}

int taos_init() {
  taosThreadOnce(&tscinit, taos_init_imp);
  return tscInitRes;
}

int taos_options_imp(TSDB_OPTION option, const char *str) {
  if (option == TSDB_OPTION_CONFIGDIR) {
#ifndef WINDOWS
    char newstr[PATH_MAX];
    int  len = strlen(str);
    if (len > 1 && str[0] != '"' && str[0] != '\'') {
        if (len + 2 >= PATH_MAX) {
        tscError("Too long path %s", str);
        return -1;
      }
      newstr[0] = '"';
      memcpy(newstr+1, str, len);
      newstr[len + 1] = '"';
      newstr[len + 2] = '\0';
      str = newstr;
    }
#endif
    tstrncpy(configDir, str, PATH_MAX);
    tscInfo("set cfg:%s to %s", configDir, str);
    return 0;
  }

  // initialize global config
  if (taos_init() != 0) {
    return -1;
  }

  SConfig     *pCfg = taosGetCfg();
  SConfigItem *pItem = NULL;

  switch (option) {
    case TSDB_OPTION_SHELL_ACTIVITY_TIMER:
      pItem = cfgGetItem(pCfg, "shellActivityTimer");
      break;
    case TSDB_OPTION_LOCALE:
      pItem = cfgGetItem(pCfg, "locale");
      break;
    case TSDB_OPTION_CHARSET:
      pItem = cfgGetItem(pCfg, "charset");
      break;
    case TSDB_OPTION_TIMEZONE:
      pItem = cfgGetItem(pCfg, "timezone");
      break;
    case TSDB_OPTION_USE_ADAPTER:
      pItem = cfgGetItem(pCfg, "useAdapter");
      break;
    default:
      break;
  }

  if (pItem == NULL) {
    tscError("Invalid option %d", option);
    return -1;
  }

  int code = cfgSetItem(pCfg, pItem->name, str, CFG_STYPE_TAOS_OPTIONS);
  if (code != 0) {
    tscError("failed to set cfg:%s to %s since %s", pItem->name, str, terrstr());
  } else {
    tscInfo("set cfg:%s to %s", pItem->name, str);
    if (TSDB_OPTION_SHELL_ACTIVITY_TIMER == option || TSDB_OPTION_USE_ADAPTER == option) {
      code = taosCfgDynamicOptions(pCfg, pItem->name, false);
    }
  }

  return code;
}

/**
 * The request id is an unsigned integer format of 64bit.
 *+------------+-----+-----------+---------------+
 *| uid|localIp| PId | timestamp | serial number |
 *+------------+-----+-----------+---------------+
 *| 12bit      |12bit|24bit      |16bit          |
 *+------------+-----+-----------+---------------+
 * @return
 */
uint64_t generateRequestId() {
  static uint64_t hashId = 0;
  static uint32_t requestSerialId = 0;

  if (hashId == 0) {
    char    uid[64] = {0};
    int32_t code = taosGetSystemUUID(uid, tListLen(uid));
    if (code != TSDB_CODE_SUCCESS) {
      tscError("Failed to get the system uid to generated request id, reason:%s. use ip address instead",
               tstrerror(TAOS_SYSTEM_ERROR(errno)));

    } else {
      hashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  uint64_t id = 0;

  while (true) {
    int64_t  ts = taosGetTimestampMs();
    uint64_t pid = taosGetPId();
    uint32_t val = atomic_add_fetch_32(&requestSerialId, 1);
    if (val >= 0xFFFF) atomic_store_32(&requestSerialId, 0);

    id = ((hashId & 0x0FFF) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
    if (id) {
      break;
    }
  }
  return id;
}

#if 0
#include "cJSON.h"
static setConfRet taos_set_config_imp(const char *config){
  setConfRet ret = {SET_CONF_RET_SUCC, {0}};
  static bool setConfFlag = false;
  if (setConfFlag) {
    ret.retCode = SET_CONF_RET_ERR_ONLY_ONCE;
    strcpy(ret.retMsg, "configuration can only set once");
    return ret;
  }
  taosInitGlobalCfg();
  cJSON *root = cJSON_Parse(config);
  if (root == NULL){
    ret.retCode = SET_CONF_RET_ERR_JSON_PARSE;
    strcpy(ret.retMsg, "parse json error");
    return ret;
  }

  int size = cJSON_GetArraySize(root);
  if(!cJSON_IsObject(root) || size == 0) {
    ret.retCode = SET_CONF_RET_ERR_JSON_INVALID;
    strcpy(ret.retMsg, "json content is invalid, must be not empty object");
    return ret;
  }

  if(size >= 1000) {
    ret.retCode = SET_CONF_RET_ERR_TOO_LONG;
    strcpy(ret.retMsg, "json object size is too long");
    return ret;
  }

  for(int i = 0; i < size; i++){
    cJSON *item = cJSON_GetArrayItem(root, i);
    if(!item) {
      ret.retCode = SET_CONF_RET_ERR_INNER;
      strcpy(ret.retMsg, "inner error");
      return ret;
    }
    if(!taosReadConfigOption(item->string, item->valuestring, NULL, NULL, TAOS_CFG_CSTATUS_OPTION, TSDB_CFG_CTYPE_B_CLIENT)){
      ret.retCode = SET_CONF_RET_ERR_PART;
      if (strlen(ret.retMsg) == 0){
        snprintf(ret.retMsg, RET_MSG_LENGTH, "part error|%s", item->string);
      }else{
        int tmp = RET_MSG_LENGTH - 1 - (int)strlen(ret.retMsg);
        size_t leftSize = tmp >= 0 ? tmp : 0;
        strncat(ret.retMsg, "|",  leftSize);
        tmp = RET_MSG_LENGTH - 1 - (int)strlen(ret.retMsg);
        leftSize = tmp >= 0 ? tmp : 0;
        strncat(ret.retMsg, item->string, leftSize);
      }
    }
  }
  cJSON_Delete(root);
  setConfFlag = true;
  return ret;
}

setConfRet taos_set_config(const char *config){
  taosThreadMutexLock(&setConfMutex);
  setConfRet ret = taos_set_config_imp(config);
  taosThreadMutexUnlock(&setConfMutex);
  return ret;
}
#endif
