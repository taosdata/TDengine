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

#include <ttimer.h>
#include "cJSON.h"
#include "catalog.h"
#include "clientInt.h"
#include "clientLog.h"
#include "clientMonitor.h"
#include "functionMgt.h"
#include "os.h"
#include "osSleep.h"
#include "query.h"
#include "qworker.h"
#include "scheduler.h"
#include "tcache.h"
#include "tcompare.h"
#include "tglobal.h"
#include "thttp.h"
#include "tmsg.h"
#include "tqueue.h"
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

#define ENV_JSON_FALSE_CHECK(c)                     \
  do {                                              \
    if (!c) {                                       \
      tscError("faild to add item to JSON object"); \
      code = TSDB_CODE_TSC_FAIL_GENERATE_JSON;      \
      goto _end;                                    \
    }                                               \
  } while (0)

#define ENV_ERR_RET(c, info)          \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      errno = _code;                  \
      tscInitRes = _code;             \
      tscError(info);                 \
      return;                         \
    }                                 \
  } while (0)

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
  int32_t code = TSDB_CODE_SUCCESS;
  // connection has been released already, abort creating request.
  pRequest->self = taosAddRef(clientReqRefPool, pRequest);
  if (pRequest->self < 0) {
    tscError("failed to add ref to request");
    code = terrno;
    return code;
  }

  int32_t num = atomic_add_fetch_32(&pTscObj->numOfReqs, 1);

  if (pTscObj->pAppInfo) {
    SAppClusterSummary *pSummary = &pTscObj->pAppInfo->summary;

    int32_t total = atomic_add_fetch_64((int64_t *)&pSummary->totalRequests, 1);
    int32_t currentInst = atomic_add_fetch_64((int64_t *)&pSummary->currentRequests, 1);
    tscDebug("0x%" PRIx64 " new Request from connObj:0x%" PRIx64
             ", current:%d, app current:%d, total:%d,QID:0x%" PRIx64,
             pRequest->self, pRequest->pTscObj->id, num, currentInst, total, pRequest->requestId);
  }

  return code;
}

static void concatStrings(SArray *list, char *buf, int size) {
  int len = 0;
  for (int i = 0; i < taosArrayGetSize(list); i++) {
    char *db = taosArrayGet(list, i);
    if (NULL == db) {
      tscError("get dbname failed, buf:%s", buf);
      break;
    }
    char *dot = strchr(db, '.');
    if (dot != NULL) {
      db = dot + 1;
    }
    if (i != 0) {
      (void)strcat(buf, ",");
      len += 1;
    }
    int ret = tsnprintf(buf + len, size - len, "%s", db);
    if (ret < 0) {
      tscError("snprintf failed, buf:%s, ret:%d", buf, ret);
      break;
    }
    len += ret;
    if (len >= size) {
      tscInfo("dbList is truncated, buf:%s, len:%d", buf, len);
      break;
    }
  }
}

static int32_t generateWriteSlowLog(STscObj *pTscObj, SRequestObj *pRequest, int32_t reqType, int64_t duration) {
  cJSON  *json = cJSON_CreateObject();
  int32_t code = TSDB_CODE_SUCCESS;
  if (json == NULL) {
    tscError("[monitor] cJSON_CreateObject failed");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  char clusterId[32] = {0};
  if (snprintf(clusterId, sizeof(clusterId), "%" PRId64, pTscObj->pAppInfo->clusterId) < 0) {
    tscError("failed to generate clusterId:%" PRId64, pTscObj->pAppInfo->clusterId);
    code = TSDB_CODE_FAILED;
    goto _end;
  }

  char startTs[32] = {0};
  if (snprintf(startTs, sizeof(startTs), "%" PRId64, pRequest->metric.start / 1000) < 0) {
    tscError("failed to generate startTs:%" PRId64, pRequest->metric.start / 1000);
    code = TSDB_CODE_FAILED;
    goto _end;
  }

  char requestId[32] = {0};
  if (snprintf(requestId, sizeof(requestId), "%" PRIu64, pRequest->requestId) < 0) {
    tscError("failed to generate requestId:%" PRIu64, pRequest->requestId);
    code = TSDB_CODE_FAILED;
    goto _end;
  }
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "cluster_id", cJSON_CreateString(clusterId)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "start_ts", cJSON_CreateString(startTs)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "request_id", cJSON_CreateString(requestId)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "query_time", cJSON_CreateNumber(duration / 1000)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "code", cJSON_CreateNumber(pRequest->code)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "error_info", cJSON_CreateString(tstrerror(pRequest->code))));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "type", cJSON_CreateNumber(reqType)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(
      json, "rows_num", cJSON_CreateNumber(pRequest->body.resInfo.numOfRows + pRequest->body.resInfo.totalRows)));
  if (pRequest->sqlstr != NULL && strlen(pRequest->sqlstr) > pTscObj->pAppInfo->monitorParas.tsSlowLogMaxLen) {
    char tmp = pRequest->sqlstr[pTscObj->pAppInfo->monitorParas.tsSlowLogMaxLen];
    pRequest->sqlstr[pTscObj->pAppInfo->monitorParas.tsSlowLogMaxLen] = '\0';
    ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "sql", cJSON_CreateString(pRequest->sqlstr)));
    pRequest->sqlstr[pTscObj->pAppInfo->monitorParas.tsSlowLogMaxLen] = tmp;
  } else {
    ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "sql", cJSON_CreateString(pRequest->sqlstr)));
  }

  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "user", cJSON_CreateString(pTscObj->user)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "process_name", cJSON_CreateString(appInfo.appName)));
  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "ip", cJSON_CreateString(tsLocalFqdn)));

  char pid[32] = {0};
  if (snprintf(pid, sizeof(pid), "%d", appInfo.pid) < 0) {
    tscError("failed to generate pid:%d", appInfo.pid);
    code = TSDB_CODE_FAILED;
    goto _end;
  }

  ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "process_id", cJSON_CreateString(pid)));
  if (pRequest->dbList != NULL) {
    char dbList[1024] = {0};
    concatStrings(pRequest->dbList, dbList, sizeof(dbList) - 1);
    ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "db", cJSON_CreateString(dbList)));
  } else if (pRequest->pDb != NULL) {
    ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "db", cJSON_CreateString(pRequest->pDb)));
  } else {
    ENV_JSON_FALSE_CHECK(cJSON_AddItemToObject(json, "db", cJSON_CreateString("")));
  }

  char *value = cJSON_PrintUnformatted(json);
  if (value == NULL) {
    tscError("failed to print json");
    code = TSDB_CODE_FAILED;
    goto _end;
  }
  MonitorSlowLogData data = {0};
  data.clusterId = pTscObj->pAppInfo->clusterId;
  data.type = SLOW_LOG_WRITE;
  data.data = value;
  code = monitorPutData2MonitorQueue(data);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(value);
    goto _end;
  }

_end:
  cJSON_Delete(json);
  return code;
}

static bool checkSlowLogExceptDb(SRequestObj *pRequest, char *exceptDb) {
  if (pRequest->pDb != NULL) {
    return strcmp(pRequest->pDb, exceptDb) != 0;
  }

  for (int i = 0; i < taosArrayGetSize(pRequest->dbList); i++) {
    char *db = taosArrayGet(pRequest->dbList, i);
    if (NULL == db) {
      tscError("get dbname failed, exceptDb:%s", exceptDb);
      return false;
    }
    char *dot = strchr(db, '.');
    if (dot != NULL) {
      db = dot + 1;
    }
    if (strcmp(db, exceptDb) == 0) {
      return false;
    }
  }
  return true;
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
  tscDebug("0x%" PRIx64 " free Request from connObj: 0x%" PRIx64 ",QID:0x%" PRIx64
           " elapsed:%.2f ms, "
           "current:%d, app current:%d",
           pRequest->self, pTscObj->id, pRequest->requestId, duration / 1000.0, num, currentInst);

  if (TSDB_CODE_SUCCESS == nodesSimAcquireAllocator(pRequest->allocatorRefId)) {
    if ((pRequest->pQuery && pRequest->pQuery->pRoot && QUERY_NODE_VNODE_MODIFY_STMT == pRequest->pQuery->pRoot->type &&
         (0 == ((SVnodeModifyOpStmt *)pRequest->pQuery->pRoot)->sqlNodeType)) ||
        QUERY_NODE_VNODE_MODIFY_STMT == pRequest->stmtType) {
      tscDebug("insert duration %" PRId64 "us: parseCost:%" PRId64 "us, ctgCost:%" PRId64 "us, analyseCost:%" PRId64
               "us, planCost:%" PRId64 "us, exec:%" PRId64 "us",
               duration, pRequest->metric.parseCostUs, pRequest->metric.ctgCostUs, pRequest->metric.analyseCostUs,
               pRequest->metric.planCostUs, pRequest->metric.execCostUs);
      (void)atomic_add_fetch_64((int64_t *)&pActivity->insertElapsedTime, duration);
      reqType = SLOW_LOG_TYPE_INSERT;
    } else if (QUERY_NODE_SELECT_STMT == pRequest->stmtType) {
      tscDebug("query duration %" PRId64 "us: parseCost:%" PRId64 "us, ctgCost:%" PRId64 "us, analyseCost:%" PRId64
               "us, planCost:%" PRId64 "us, exec:%" PRId64 "us",
               duration, pRequest->metric.parseCostUs, pRequest->metric.ctgCostUs, pRequest->metric.analyseCostUs,
               pRequest->metric.planCostUs, pRequest->metric.execCostUs);

      (void)atomic_add_fetch_64((int64_t *)&pActivity->queryElapsedTime, duration);
      reqType = SLOW_LOG_TYPE_QUERY;
    }

    if (TSDB_CODE_SUCCESS != nodesSimReleaseAllocator(pRequest->allocatorRefId)) {
      tscError("failed to release allocator");
    }
  }

  if (pTscObj->pAppInfo->monitorParas.tsEnableMonitor) {
    if (QUERY_NODE_VNODE_MODIFY_STMT == pRequest->stmtType || QUERY_NODE_INSERT_STMT == pRequest->stmtType) {
      sqlReqLog(pTscObj->id, pRequest->killed, pRequest->code, MONITORSQLTYPEINSERT);
    } else if (QUERY_NODE_SELECT_STMT == pRequest->stmtType) {
      sqlReqLog(pTscObj->id, pRequest->killed, pRequest->code, MONITORSQLTYPESELECT);
    } else if (QUERY_NODE_DELETE_STMT == pRequest->stmtType) {
      sqlReqLog(pTscObj->id, pRequest->killed, pRequest->code, MONITORSQLTYPEDELETE);
    }
  }

  if ((duration >= pTscObj->pAppInfo->monitorParas.tsSlowLogThreshold * 1000000UL ||
       duration >= pTscObj->pAppInfo->monitorParas.tsSlowLogThresholdTest * 1000000UL) &&
      checkSlowLogExceptDb(pRequest, pTscObj->pAppInfo->monitorParas.tsSlowLogExceptDb)) {
    (void)atomic_add_fetch_64((int64_t *)&pActivity->numOfSlowQueries, 1);
    if (pTscObj->pAppInfo->monitorParas.tsSlowLogScope & reqType) {
      taosPrintSlowLog("PID:%d, Conn:%u,QID:0x%" PRIx64 ", Start:%" PRId64 " us, Duration:%" PRId64 "us, SQL:%s",
                       taosGetPId(), pTscObj->connId, pRequest->requestId, pRequest->metric.start, duration,
                       pRequest->sqlstr);
      if (pTscObj->pAppInfo->monitorParas.tsEnableMonitor) {
        slowQueryLog(pTscObj->id, pRequest->killed, pRequest->code, duration);
        if (TSDB_CODE_SUCCESS != generateWriteSlowLog(pTscObj, pRequest, reqType, duration)) {
          tscError("failed to generate write slow log");
        }
      }
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
int32_t openTransporter(const char *user, const char *auth, int32_t numOfThread, void **pDnodeConn) {
  SRpcInit rpcInit;
  (void)memset(&rpcInit, 0, sizeof(rpcInit));
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

  int32_t code = taosVersionStrToInt(version, &(rpcInit.compatibilityVer));
  if (TSDB_CODE_SUCCESS != code) {
    tscError("invalid version string.");
    return code;
  }

  *pDnodeConn = rpcOpen(&rpcInit);
  if (*pDnodeConn == NULL) {
    tscError("failed to init connection to server since %s", tstrerror(terrno));
    code = terrno;
  }

  return code;
}

void destroyAllRequests(SHashObj *pRequests) {
  void *pIter = taosHashIterate(pRequests, NULL);
  while (pIter != NULL) {
    int64_t *rid = pIter;

    SRequestObj *pRequest = acquireRequest(*rid);
    if (pRequest) {
      destroyRequest(pRequest);
      (void)releaseRequest(*rid);  // ignore error
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
      (void)releaseRequest(*rid);  // ignore error
    }

    pIter = taosHashIterate(pRequests, pIter);
  }
}

void destroyAppInst(void *info) {
  SAppInstInfo *pAppInfo = *(SAppInstInfo **)info;
  tscDebug("destroy app inst mgr %p", pAppInfo);

  int32_t code = taosThreadMutexLock(&appInfo.mutex);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to lock app info, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }

  hbRemoveAppHbMrg(&pAppInfo->pAppHbMgr);

  code = taosThreadMutexUnlock(&appInfo.mutex);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to unlock app info, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }

  taosMemoryFreeClear(pAppInfo->instKey);
  closeTransporter(pAppInfo);

  code = taosThreadMutexLock(&pAppInfo->qnodeMutex);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to lock qnode mutex, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }

  taosArrayDestroy(pAppInfo->pQnodeList);
  code = taosThreadMutexUnlock(&pAppInfo->qnodeMutex);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to unlock qnode mutex, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }

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
  /*int64_t connNum = */ (void)atomic_sub_fetch_64(&pTscObj->pAppInfo->numOfConns, 1);

  (void)taosThreadMutexDestroy(&pTscObj->mutex);
  taosMemoryFree(pTscObj);

  tscTrace("end to destroy tscObj %" PRIx64 " p:%p", tscId, pTscObj);
}

int32_t createTscObj(const char *user, const char *auth, const char *db, int32_t connType, SAppInstInfo *pAppInfo,
                     STscObj **pObj) {
  *pObj = (STscObj *)taosMemoryCalloc(1, sizeof(STscObj));
  if (NULL == *pObj) {
    return terrno;
  }

  (*pObj)->pRequests = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == (*pObj)->pRequests) {
    taosMemoryFree(*pObj);
    return terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pObj)->connType = connType;
  (*pObj)->pAppInfo = pAppInfo;
  (*pObj)->appHbMgrIdx = pAppInfo->pAppHbMgr->idx;
  tstrncpy((*pObj)->user, user, sizeof((*pObj)->user));
  (void)memcpy((*pObj)->pass, auth, TSDB_PASSWORD_LEN);

  if (db != NULL) {
    tstrncpy((*pObj)->db, db, tListLen((*pObj)->db));
  }

  TSC_ERR_RET(taosThreadMutexInit(&(*pObj)->mutex, NULL));

  int32_t code = TSDB_CODE_SUCCESS;

  (*pObj)->id = taosAddRef(clientConnRefPool, *pObj);
  if ((*pObj)->id < 0) {
    tscError("failed to add object to clientConnRefPool");
    code = terrno;
    taosMemoryFree(*pObj);
    return code;
  }

  (void)atomic_add_fetch_64(&(*pObj)->pAppInfo->numOfConns, 1);

  tscDebug("connObj created, 0x%" PRIx64 ",p:%p", (*pObj)->id, *pObj);
  return code;
}

STscObj *acquireTscObj(int64_t rid) { return (STscObj *)taosAcquireRef(clientConnRefPool, rid); }

void releaseTscObj(int64_t rid) {
  int32_t code = taosReleaseRef(clientConnRefPool, rid);
  if (TSDB_CODE_SUCCESS != code) {
    tscWarn("failed to release TscObj, code:%s", tstrerror(code));
  }
}

int32_t createRequest(uint64_t connId, int32_t type, int64_t reqid, SRequestObj **pRequest) {
  int32_t code = TSDB_CODE_SUCCESS;
  *pRequest = (SRequestObj *)taosMemoryCalloc(1, sizeof(SRequestObj));
  if (NULL == *pRequest) {
    return terrno;
  }

  STscObj *pTscObj = acquireTscObj(connId);
  if (pTscObj == NULL) {
    TSC_ERR_JRET(TSDB_CODE_TSC_DISCONNECTED);
  }
  SSyncQueryParam *interParam = taosMemoryCalloc(1, sizeof(SSyncQueryParam));
  if (interParam == NULL) {
    releaseTscObj(connId);
    TSC_ERR_JRET(terrno);
  }
  TSC_ERR_JRET(tsem_init(&interParam->sem, 0, 0));
  interParam->pRequest = *pRequest;
  (*pRequest)->body.interParam = interParam;

  (*pRequest)->resType = RES_TYPE__QUERY;
  (*pRequest)->requestId = reqid == 0 ? generateRequestId() : reqid;
  (*pRequest)->metric.start = taosGetTimestampUs();

  (*pRequest)->body.resInfo.convertUcs4 = true;  // convert ucs4 by default
  (*pRequest)->type = type;
  (*pRequest)->allocatorRefId = -1;

  (*pRequest)->pDb = getDbOfConnection(pTscObj);
  if (NULL == (*pRequest)->pDb) {
    TSC_ERR_JRET(terrno);
  }
  (*pRequest)->pTscObj = pTscObj;
  (*pRequest)->inCallback = false;
  (*pRequest)->msgBuf = taosMemoryCalloc(1, ERROR_MSG_BUF_DEFAULT_SIZE);
  if (NULL == (*pRequest)->msgBuf) {
    code = terrno;
    goto _return;
  }
  (*pRequest)->msgBufLen = ERROR_MSG_BUF_DEFAULT_SIZE;
  TSC_ERR_JRET(tsem_init(&(*pRequest)->body.rspSem, 0, 0));
  TSC_ERR_JRET(registerRequest(*pRequest, pTscObj));

  return TSDB_CODE_SUCCESS;
_return:
  if ((*pRequest)->pTscObj) {
    doDestroyRequest(*pRequest);
  } else {
    taosMemoryFree(*pRequest);
  }
  return code;
}

void doFreeReqResultInfo(SReqResultInfo *pResInfo) {
  taosMemoryFreeClear(pResInfo->pRspMsg);
  taosMemoryFreeClear(pResInfo->length);
  taosMemoryFreeClear(pResInfo->row);
  taosMemoryFreeClear(pResInfo->pCol);
  taosMemoryFreeClear(pResInfo->fields);
  taosMemoryFreeClear(pResInfo->userFields);
  taosMemoryFreeClear(pResInfo->convertJson);
  taosMemoryFreeClear(pResInfo->decompBuf);

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

/// return the most previous req ref id
int64_t removeFromMostPrevReq(SRequestObj *pRequest) {
  int64_t      mostPrevReqRefId = pRequest->self;
  SRequestObj *pTmp = pRequest;
  while (pTmp->relation.prevRefId) {
    pTmp = acquireRequest(pTmp->relation.prevRefId);
    if (pTmp) {
      mostPrevReqRefId = pTmp->self;
      (void)releaseRequest(mostPrevReqRefId);  // ignore error
    } else {
      break;
    }
  }
  (void)removeRequest(mostPrevReqRefId);  // ignore error
  return mostPrevReqRefId;
}

void destroyNextReq(int64_t nextRefId) {
  if (nextRefId) {
    SRequestObj *pObj = acquireRequest(nextRefId);
    if (pObj) {
      (void)releaseRequest(nextRefId);  // ignore error
      (void)releaseRequest(nextRefId);  // ignore error
    }
  }
}

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
      (void)releaseRequest(tmpRefId);  // ignore error
    } else {
      tscError("prev req ref 0x%" PRIx64 " is not there", tmpRefId);
      break;
    }
  }

  for (int32_t i = reqIdx; i >= 0; i--) {
    (void)removeRequest(pReqList[i]->self);  // ignore error
  }

  tmpRefId = pRequest->relation.nextRefId;
  while (tmpRefId) {
    pTmp = acquireRequest(tmpRefId);
    if (pTmp) {
      tmpRefId = pTmp->relation.nextRefId;
      (void)removeRequest(pTmp->self);   // ignore error
      (void)releaseRequest(pTmp->self);  // ignore error
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

  int64_t nextReqRefId = pRequest->relation.nextRefId;

  int32_t code = taosHashRemove(pRequest->pTscObj->pRequests, &pRequest->self, sizeof(pRequest->self));
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to remove request from hash, code:%s", tstrerror(code));
  }
  schedulerFreeJob(&pRequest->body.queryJob, 0);

  destorySqlCallbackWrapper(pRequest->pWrapper);

  taosMemoryFreeClear(pRequest->msgBuf);

  doFreeReqResultInfo(&pRequest->body.resInfo);
  if (TSDB_CODE_SUCCESS != tsem_destroy(&pRequest->body.rspSem)) {
    tscError("failed to destroy semaphore");
  }

  taosArrayDestroy(pRequest->tableList);
  taosArrayDestroy(pRequest->targetTableList);
  destroyQueryExecRes(&pRequest->body.resInfo.execRes);

  if (pRequest->self) {
    deregisterRequest(pRequest);
  }

  taosMemoryFreeClear(pRequest->pDb);
  taosArrayDestroy(pRequest->dbList);
  if (pRequest->body.interParam) {
    if (TSDB_CODE_SUCCESS != tsem_destroy(&((SSyncQueryParam *)pRequest->body.interParam)->sem)) {
      tscError("failed to destroy semaphore in pRequest");
    }
  }
  taosMemoryFree(pRequest->body.interParam);

  qDestroyQuery(pRequest->pQuery);
  nodesDestroyAllocator(pRequest->allocatorRefId);

  taosMemoryFreeClear(pRequest->effectiveUser);
  taosMemoryFreeClear(pRequest->sqlstr);
  taosMemoryFree(pRequest);
  tscTrace("end to destroy request %" PRIx64 " p:%p", reqId, pRequest);
  destroyNextReq(nextReqRefId);
}

void destroyRequest(SRequestObj *pRequest) {
  if (pRequest == NULL) {
    return;
  }

  taos_stop_query(pRequest);
  (void)removeFromMostPrevReq(pRequest);
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
    } else {
      tscError("prev req ref 0x%" PRIx64 " is not there", tmpRefId);
      break;
    }
  }

  for (int32_t i = reqIdx; i >= 0; i--) {
    taosStopQueryImpl(pReqList[i]);
    (void)releaseRequest(pReqList[i]->self);  // ignore error
  }

  taosStopQueryImpl(pRequest);

  tmpRefId = pRequest->relation.nextRefId;
  while (tmpRefId) {
    pTmp = acquireRequest(tmpRefId);
    if (pTmp) {
      tmpRefId = pTmp->relation.nextRefId;
      taosStopQueryImpl(pTmp);
      (void)releaseRequest(pTmp->self);  // ignore error
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
  (void)snprintf(filepath, sizeof(filepath), "%s%s.taosCrashLog", tsLogDir, TD_DIRSEP);
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
    return TSDB_CODE_SUCCESS;
  }
  int32_t      code = TSDB_CODE_SUCCESS;
  TdThreadAttr thAttr;
  TSC_ERR_JRET(taosThreadAttrInit(&thAttr));
  TSC_ERR_JRET(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE));
  TdThread crashReportThread;
  if (taosThreadCreate(&crashReportThread, &thAttr, tscCrashReportThreadFp, NULL) != 0) {
    tscError("failed to create crashReport thread since %s", strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    TSC_ERR_RET(errno);
  }

  (void)taosThreadAttrDestroy(&thAttr);
_return:
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    TSC_ERR_RET(terrno);
  }

  return code;
}

void tscStopCrashReport() {
  if (!tsEnableCrashReport) {
    return;
  }

  if (atomic_val_compare_exchange_32(&clientStop, 0, 1)) {
    tscDebug("crash report thread already stopped");
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
      (void)printf("failed to init memory dbg, error:%s\n", tstrerror(code));
    } else {
      tsAsyncLog = false;
      (void)printf("memory dbg enabled\n");
    }
  }
#endif

  // In the APIs of other program language, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated resource to suppress the valgrind warning.
  (void)atexit(taos_cleanup);
  errno = TSDB_CODE_SUCCESS;
  taosSeedRand(taosGetTimestampSec());

  appInfo.pid = taosGetPId();
  appInfo.startTime = taosGetTimestampMs();
  appInfo.pInstMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  appInfo.pInstMapByClusterId =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (NULL == appInfo.pInstMap || NULL == appInfo.pInstMapByClusterId) {
    (void)printf("failed to allocate memory when init appInfo\n");
    tscInitRes = TSDB_CODE_OUT_OF_MEMORY;
    return;
  }
  taosHashSetFreeFp(appInfo.pInstMap, destroyAppInst);
  deltaToUtcInitOnce();

  char logDirName[64] = {0};
#ifdef CUS_PROMPT
  snprintf(logDirName, 64, "%slog", CUS_PROMPT);
#else
  (void)snprintf(logDirName, 64, "taoslog");
#endif
  if (taosCreateLog(logDirName, 10, configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    (void)printf(" WARING: Create %s failed:%s. configDir=%s\n", logDirName, strerror(errno), configDir);
    tscInitRes = -1;
    return;
  }

  ENV_ERR_RET(taosInitCfg(configDir, NULL, NULL, NULL, NULL, 1), "failed to init cfg");

  initQueryModuleMsgHandle();
  ENV_ERR_RET(taosConvInit(), "failed to init conv");
  ENV_ERR_RET(monitorInit(), "failed to init monitor");
  ENV_ERR_RET(rpcInit(), "failed to init rpc");

  if (InitRegexCache() != 0) {
    tscInitRes = -1;
    (void)printf("failed to init regex cache\n");
    return;
  }

  SCatalogCfg cfg = {.maxDBCacheNum = 100, .maxTblCacheNum = 100};
  ENV_ERR_RET(catalogInit(&cfg), "failed to init catalog");
  ENV_ERR_RET(schedulerInit(), "failed to init scheduler");

  tscDebug("starting to initialize TAOS driver");

  ENV_ERR_RET(initTaskQueue(), "failed to init task queue");
  ENV_ERR_RET(fmFuncMgtInit(), "failed to init funcMgt");
  ENV_ERR_RET(nodesInitAllocatorSet(), "failed to init allocator set");

  clientConnRefPool = taosOpenRef(200, destroyTscObj);
  clientReqRefPool = taosOpenRef(40960, doDestroyRequest);

  ENV_ERR_RET(taosGetAppName(appInfo.appName, NULL), "failed to get app name");
  ENV_ERR_RET(taosThreadMutexInit(&appInfo.mutex, NULL), "failed to init thread mutex");
  ENV_ERR_RET(tscCrashReportInit(), "failed to init crash report");
  ENV_ERR_RET(qInitKeywordsTable(), "failed to init parser keywords table");

  tscDebug("client is initialized successfully");
}

int taos_init() {
  (void)taosThreadOnce(&tscinit, taos_init_imp);
  return tscInitRes;
}

const char *getCfgName(TSDB_OPTION option) {
  const char *name = NULL;

  switch (option) {
    case TSDB_OPTION_SHELL_ACTIVITY_TIMER:
      name = "shellActivityTimer";
      break;
    case TSDB_OPTION_LOCALE:
      name = "locale";
      break;
    case TSDB_OPTION_CHARSET:
      name = "charset";
      break;
    case TSDB_OPTION_TIMEZONE:
      name = "timezone";
      break;
    case TSDB_OPTION_USE_ADAPTER:
      name = "useAdapter";
      break;
    default:
      break;
  }

  return name;
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
      (void)memcpy(newstr + 1, str, len);
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
  const char  *name = getCfgName(option);

  if (name == NULL) {
    tscError("Invalid option %d", option);
    return -1;
  }

  pItem = cfgGetItem(pCfg, name);
  if (pItem == NULL) {
    tscError("Invalid option %d", option);
    return -1;
  }

  int code = cfgSetItem(pCfg, name, str, CFG_STYPE_TAOS_OPTIONS, true);
  if (code != 0) {
    tscError("failed to set cfg:%s to %s since %s", name, str, terrstr());
  } else {
    tscInfo("set cfg:%s to %s", name, str);
    if (TSDB_OPTION_SHELL_ACTIVITY_TIMER == option || TSDB_OPTION_USE_ADAPTER == option) {
      code = taosCfgDynamicOptions(pCfg, name, false);
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
  static uint32_t hashId = 0;
  static int32_t requestSerialId = 0;

  if (hashId == 0) {
    int32_t code = taosGetSystemUUID32(&hashId);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("Failed to get the system uid to generated request id, reason:%s. use ip address instead",
               tstrerror(code));
    }
  }

  uint64_t id = 0;

  while (true) {
    int64_t  ts = taosGetTimestampMs();
    uint64_t pid = taosGetPId();
    uint32_t val = atomic_add_fetch_32(&requestSerialId, 1);
    if (val >= 0xFFFF) atomic_store_32(&requestSerialId, 0);

    id = (((uint64_t)(hashId & 0x0FFF)) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
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
