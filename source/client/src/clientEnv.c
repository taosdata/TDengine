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
#include "query.h"
#include "qworker.h"
#include "scheduler.h"
#include "tcache.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "tsched.h"
#include "ttime.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

SAppInfo appInfo;
int32_t  clientReqRefPool = -1;
int32_t  clientConnRefPool = -1;

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
  const static int64_t SLOW_QUERY_INTERVAL = 3000000L;  // todo configurable
  assert(pRequest != NULL);

  STscObj            *pTscObj = pRequest->pTscObj;
  SAppClusterSummary *pActivity = &pTscObj->pAppInfo->summary;

  int32_t currentInst = atomic_sub_fetch_64((int64_t *)&pActivity->currentRequests, 1);
  int32_t num = atomic_sub_fetch_32(&pTscObj->numOfReqs, 1);

  int64_t duration = taosGetTimestampUs() - pRequest->metric.start;
  tscDebug("0x%" PRIx64 " free Request from connObj: 0x%" PRIx64 ", reqId:0x%" PRIx64
           " elapsed:%.2f ms, "
           "current:%d, app current:%d",
           pRequest->self, pTscObj->id, pRequest->requestId, duration / 1000.0, num, currentInst);

  tscPerf("insert duration %" PRId64 "us: syntax:%" PRId64 "us, ctg:%" PRId64 "us, semantic:%" PRId64
          "us, exec:%" PRId64 "us, stmtType:%d",
          duration, pRequest->metric.syntaxEnd - pRequest->metric.syntaxStart,
          pRequest->metric.ctgEnd - pRequest->metric.ctgStart, pRequest->metric.semanticEnd - pRequest->metric.ctgEnd,
          pRequest->metric.execEnd - pRequest->metric.semanticEnd, pRequest->stmtType);

  if (QUERY_NODE_VNODE_MODIFY_STMT == pRequest->stmtType) {
    //        tscPerf("insert duration %" PRId64 "us: syntax:%" PRId64 "us, ctg:%" PRId64 "us, semantic:%" PRId64
    //                "us, exec:%" PRId64 "us",
    //                duration, pRequest->metric.syntaxEnd - pRequest->metric.syntaxStart,
    //                pRequest->metric.ctgEnd - pRequest->metric.ctgStart, pRequest->metric.semanticEnd -
    //                pRequest->metric.ctgEnd, pRequest->metric.execEnd - pRequest->metric.semanticEnd);
    //    atomic_add_fetch_64((int64_t *)&pActivity->insertElapsedTime, duration);
  } else if (QUERY_NODE_SELECT_STMT == pRequest->stmtType) {
    //    tscPerf("select duration %" PRId64 "us: syntax:%" PRId64 "us, ctg:%" PRId64 "us, semantic:%" PRId64
    //            "us, planner:%" PRId64 "us, exec:%" PRId64 "us, reqId:0x%" PRIx64,
    //            duration, pRequest->metric.syntaxEnd - pRequest->metric.syntaxStart,
    //            pRequest->metric.ctgEnd - pRequest->metric.ctgStart, pRequest->metric.semanticEnd -
    //            pRequest->metric.ctgEnd, pRequest->metric.planEnd - pRequest->metric.semanticEnd,
    //            pRequest->metric.resultReady - pRequest->metric.planEnd, pRequest->requestId);

    atomic_add_fetch_64((int64_t *)&pActivity->queryElapsedTime, duration);
  }

  if (duration >= SLOW_QUERY_INTERVAL) {
    atomic_add_fetch_64((int64_t *)&pActivity->numOfSlowQueries, 1);
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
        msgType == TDMT_SCH_MERGE_FETCH || msgType == TDMT_SCH_QUERY_HEARTBEAT || msgType == TDMT_SCH_DROP_TASK) {
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
  rpcInit.numOfThreads = numOfThread;
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
  rpcInit.retryMaxTimouet = tsMaxRetryWaitTime;

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
  hbDeregisterConn(pTscObj->pAppInfo->pAppHbMgr, connKey);

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

  pRequest->resType = RES_TYPE__QUERY;
  pRequest->requestId = reqid == 0 ? generateRequestId() : reqid;
  pRequest->metric.start = taosGetTimestampUs();

  pRequest->body.resInfo.convertUcs4 = true;  // convert ucs4 by default
  pRequest->type = type;
  pRequest->allocatorRefId = -1;

  pRequest->pDb = getDbOfConnection(pTscObj);
  pRequest->pTscObj = pTscObj;

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

void doDestroyRequest(void *p) {
  if (NULL == p) {
    return;
  }

  SRequestObj *pRequest = (SRequestObj *)p;

  uint64_t reqId = pRequest->requestId;
  tscTrace("begin to destroy request %" PRIx64 " p:%p", reqId, pRequest);

  taosHashRemove(pRequest->pTscObj->pRequests, &pRequest->self, sizeof(pRequest->self));

  schedulerFreeJob(&pRequest->body.queryJob, 0);

  taosMemoryFreeClear(pRequest->msgBuf);
  taosMemoryFreeClear(pRequest->pDb);

  doFreeReqResultInfo(&pRequest->body.resInfo);

  taosArrayDestroy(pRequest->tableList);
  taosArrayDestroy(pRequest->dbList);
  taosArrayDestroy(pRequest->targetTableList);
  qDestroyQuery(pRequest->pQuery);
  nodesDestroyAllocator(pRequest->allocatorRefId);

  destroyQueryExecRes(&pRequest->body.resInfo.execRes);

  if (pRequest->self) {
    deregisterRequest(pRequest);
  }

  if (pRequest->syncQuery) {
    taosMemoryFree(pRequest->body.param);
  }

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

void taos_init_imp(void) {
  // In the APIs of other program language, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated resource to suppress the valgrind warning.
  atexit(taos_cleanup);
  errno = TSDB_CODE_SUCCESS;
  taosSeedRand(taosGetTimestampSec());

  deltaToUtcInitOnce();

  if (taosCreateLog("taoslog", 10, configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    // ignore create log failed, only print
    printf(" WARING: Create taoslog failed. configDir=%s\n", configDir);
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    tscInitRes = -1;
    return;
  }

  initQueryModuleMsgHandle();

  if (taosConvInit() != 0) {
    ASSERTS(0, "failed to init conv");
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

  appInfo.pid = taosGetPId();
  appInfo.startTime = taosGetTimestampMs();
  appInfo.pInstMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  tscDebug("client is initialized successfully");
}

int taos_init() {
  taosThreadOnce(&tscinit, taos_init_imp);
  return tscInitRes;
}

int taos_options_imp(TSDB_OPTION option, const char *str) {
  if (option == TSDB_OPTION_CONFIGDIR) {
    tstrncpy(configDir, str, PATH_MAX);
    tscInfo("set cfg:%s to %s", configDir, str);
    return 0;
  } else {
    taos_init();  // initialize global config
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
