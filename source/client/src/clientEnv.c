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
#include "os.h"
#include "query.h"
#include "scheduler.h"
#include "tcache.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "ttime.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED 0

SAppInfo appInfo;
int32_t  clientReqRefPool = -1;
int32_t  clientConnRefPool = -1;

static TdThreadOnce tscinit = PTHREAD_ONCE_INIT;
volatile int32_t      tscInitRes = 0;

static void registerRequest(SRequestObj *pRequest) {
  STscObj *pTscObj = (STscObj *)taosAcquireRef(clientConnRefPool, pRequest->pTscObj->id);
  assert(pTscObj != NULL);

  // connection has been released already, abort creating request.
  pRequest->self = taosAddRef(clientReqRefPool, pRequest);

  int32_t num = atomic_add_fetch_32(&pTscObj->numOfReqs, 1);

  if (pTscObj->pAppInfo) {
    SInstanceSummary *pSummary = &pTscObj->pAppInfo->summary;

    int32_t total = atomic_add_fetch_64(&pSummary->totalRequests, 1);
    int32_t currentInst = atomic_add_fetch_64(&pSummary->currentRequests, 1);
    tscDebug("0x%" PRIx64 " new Request from connObj:0x%" PRIx64
             ", current:%d, app current:%d, total:%d, reqId:0x%" PRIx64,
             pRequest->self, pRequest->pTscObj->id, num, currentInst, total, pRequest->requestId);
  }
}

static void deregisterRequest(SRequestObj *pRequest) {
  assert(pRequest != NULL);

  STscObj *         pTscObj = pRequest->pTscObj;
  SInstanceSummary *pActivity = &pTscObj->pAppInfo->summary;

  int32_t currentInst = atomic_sub_fetch_64(&pActivity->currentRequests, 1);
  int32_t num = atomic_sub_fetch_32(&pTscObj->numOfReqs, 1);

  int64_t duration = taosGetTimestampMs() - pRequest->metric.start;
  tscDebug("0x%" PRIx64 " free Request from connObj: 0x%" PRIx64 ", reqId:0x%" PRIx64 " elapsed:%" PRIu64
           " ms, current:%d, app current:%d",
           pRequest->self, pTscObj->id, pRequest->requestId, duration, num, currentInst);
  taosReleaseRef(clientConnRefPool, pTscObj->id);
}

// todo close the transporter properly
void closeTransporter(STscObj *pTscObj) {
  if (pTscObj == NULL || pTscObj->pAppInfo->pTransporter == NULL) {
    return;
  }

  tscDebug("free transporter:%p in connObj: 0x%" PRIx64, pTscObj->pAppInfo->pTransporter, pTscObj->id);
  rpcClose(pTscObj->pAppInfo->pTransporter);
}

// TODO refactor
void *openTransporter(const char *user, const char *auth, int32_t numOfThread) {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "TSC";
  rpcInit.numOfThreads = numOfThread;
  rpcInit.cfp = processMsgFromServer;
  rpcInit.sessions = tsMaxConnections;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = (char *)user;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.ckey = "key";
  rpcInit.spi = 1;
  rpcInit.secret = (char *)auth;

  void *pDnodeConn = rpcOpen(&rpcInit);
  if (pDnodeConn == NULL) {
    tscError("failed to init connection to server");
    return NULL;
  }

  return pDnodeConn;
}

void destroyTscObj(void *pObj) {
  STscObj *pTscObj = pObj;

  SClientHbKey connKey = {.connId = pTscObj->connId, .hbType = pTscObj->connType};
  hbDeregisterConn(pTscObj->pAppInfo->pAppHbMgr, connKey);
  atomic_sub_fetch_64(&pTscObj->pAppInfo->numOfConns, 1);
  tscDebug("connObj 0x%" PRIx64 " destroyed, totalConn:%" PRId64, pTscObj->id, pTscObj->pAppInfo->numOfConns);
  taosThreadMutexDestroy(&pTscObj->mutex);
  tfree(pTscObj);
}

void *createTscObj(const char *user, const char *auth, const char *db, SAppInstInfo *pAppInfo) {
  STscObj *pObj = (STscObj *)calloc(1, sizeof(STscObj));
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pObj->pAppInfo = pAppInfo;
  tstrncpy(pObj->user, user, sizeof(pObj->user));
  memcpy(pObj->pass, auth, TSDB_PASSWORD_LEN);

  if (db != NULL) {
    tstrncpy(pObj->db, db, tListLen(pObj->db));
  }

  taosThreadMutexInit(&pObj->mutex, NULL);
  pObj->id = taosAddRef(clientConnRefPool, pObj);

  tscDebug("connObj created, 0x%" PRIx64, pObj->id);
  return pObj;
}

void *createRequest(STscObj *pObj, __taos_async_fn_t fp, void *param, int32_t type) {
  assert(pObj != NULL);

  SRequestObj *pRequest = (SRequestObj *)calloc(1, sizeof(SRequestObj));
  if (NULL == pRequest) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pRequest->pDb = getDbOfConnection(pObj);
  pRequest->requestId = generateRequestId();
  pRequest->metric.start = taosGetTimestampMs();

  pRequest->type = type;
  pRequest->pTscObj = pObj;
  pRequest->body.fp = fp;  // not used it yet
  pRequest->msgBuf = calloc(1, ERROR_MSG_BUF_DEFAULT_SIZE);
  tsem_init(&pRequest->body.rspSem, 0, 0);

  registerRequest(pRequest);
  return pRequest;
}

static void doFreeReqResultInfo(SReqResultInfo *pResInfo) {
  tfree(pResInfo->pRspMsg);
  tfree(pResInfo->length);
  tfree(pResInfo->row);
  tfree(pResInfo->pCol);
  tfree(pResInfo->fields);
}

static void doDestroyRequest(void *p) {
  assert(p != NULL);
  SRequestObj *pRequest = (SRequestObj *)p;

  assert(RID_VALID(pRequest->self));

  tfree(pRequest->msgBuf);
  tfree(pRequest->sqlstr);
  tfree(pRequest->pInfo);
  tfree(pRequest->pDb);

  doFreeReqResultInfo(&pRequest->body.resInfo);
  qDestroyQueryPlan(pRequest->body.pDag);

  if (pRequest->body.showInfo.pArray != NULL) {
    taosArrayDestroy(pRequest->body.showInfo.pArray);
  }

  deregisterRequest(pRequest);
  tfree(pRequest);
}

void destroyRequest(SRequestObj *pRequest) {
  if (pRequest == NULL) {
    return;
  }

  taosReleaseRef(clientReqRefPool, pRequest->self);
}

void taos_init_imp(void) {
  // In the APIs of other program language, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated resource to suppress the valgrind warning.
  atexit(taos_cleanup);

  errno = TSDB_CODE_SUCCESS;
  taosSeedRand(taosGetTimestampSec());

  deltaToUtcInitOnce();

  if (taosCreateLog("taoslog", 10, configDir, NULL, NULL, NULL, 1) != 0) {
    tscInitRes = -1;
    return;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, 1) != 0) {
    tscInitRes = -1;
    return;
  }

  initMsgHandleFp();
  initQueryModuleMsgHandle();

  rpcInit();

  SCatalogCfg cfg = {.maxDBCacheNum = 100, .maxTblCacheNum = 100};
  catalogInit(&cfg);

  SSchedulerCfg scfg = {.maxJobNum = 100};
  schedulerInit(&scfg);
  tscDebug("starting to initialize TAOS driver");

  taosSetCoreDump(true);

  initTaskQueue();

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
#if 0  
  SGlobalCfg *cfg = NULL;

  switch (option) {
    case TSDB_OPTION_CONFIGDIR:
      cfg = taosGetConfigOption("configDir");
      assert(cfg != NULL);

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tstrncpy(configDir, str, TSDB_FILENAME_LEN);
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscInfo("set config file directory:%s", str);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_SHELL_ACTIVITY_TIMER:
      cfg = taosGetConfigOption("shellActivityTimer");
      assert(cfg != NULL);

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tsShellActivityTimer = atoi(str);
        if (tsShellActivityTimer < 1) tsShellActivityTimer = 1;
        if (tsShellActivityTimer > 3600) tsShellActivityTimer = 3600;
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscInfo("set shellActivityTimer:%d", tsShellActivityTimer);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, str,
                tsCfgStatusStr[cfg->cfgStatus], *(int32_t *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_LOCALE: {  // set locale
      cfg = taosGetConfigOption("locale");
      assert(cfg != NULL);

      size_t len = strlen(str);
      if (len == 0 || len > TD_LOCALE_LEN) {
        tscInfo("Invalid locale:%s, use default", str);
        return -1;
      }

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        char sep = '.';

        if (strlen(tsLocale) == 0) {  // locale does not set yet
          char *defaultLocale = setlocale(LC_CTYPE, "");

          // The locale of the current OS does not be set correctly, so the default locale cannot be acquired.
          // The launch of current system will abort soon.
          if (defaultLocale == NULL) {
            tscError("failed to get default locale, please set the correct locale in current OS");
            return -1;
          }

          tstrncpy(tsLocale, defaultLocale, TD_LOCALE_LEN);
        }

        // set the user specified locale
        char *locale = setlocale(LC_CTYPE, str);

        if (locale != NULL) {  // failed to set the user specified locale
          tscInfo("locale set, prev locale:%s, new locale:%s", tsLocale, locale);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else {  // set the user specified locale failed, use default LC_CTYPE as current locale
          locale = setlocale(LC_CTYPE, tsLocale);
          tscInfo("failed to set locale:%s, current locale:%s", str, tsLocale);
        }

        tstrncpy(tsLocale, locale, TD_LOCALE_LEN);

        char *charset = strrchr(tsLocale, sep);
        if (charset != NULL) {
          charset += 1;

          charset = taosCharsetReplace(charset);

          if (taosValidateEncodec(charset)) {
            if (strlen(tsCharset) == 0) {
              tscInfo("charset set:%s", charset);
            } else {
              tscInfo("charset changed from %s to %s", tsCharset, charset);
            }

            tstrncpy(tsCharset, charset, TD_LOCALE_LEN);
            cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;

          } else {
            tscInfo("charset:%s is not valid in locale, charset remains:%s", charset, tsCharset);
          }

          free(charset);
        } else {  // it may be windows system
          tscInfo("charset remains:%s", tsCharset);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;
    }

    case TSDB_OPTION_CHARSET: {
      /* set charset will override the value of charset, assigned during system locale changed */
      cfg = taosGetConfigOption("charset");
      assert(cfg != NULL);

      size_t len = strlen(str);
      if (len == 0 || len > TD_LOCALE_LEN) {
        tscInfo("failed to set charset:%s", str);
        return -1;
      }

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        if (taosValidateEncodec(str)) {
          if (strlen(tsCharset) == 0) {
            tscInfo("charset is set:%s", str);
          } else {
            tscInfo("charset changed from %s to %s", tsCharset, str);
          }

          tstrncpy(tsCharset, str, TD_LOCALE_LEN);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else {
          tscInfo("charset:%s not valid", str);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }

      break;
    }

    case TSDB_OPTION_TIMEZONE:
      cfg = taosGetConfigOption("timezone");
      assert(cfg != NULL);

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tstrncpy(tsTimezone, str, TD_TIMEZONE_LEN);
        tsSetTimeZone();
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscDebug("timezone set:%s, input:%s by taos_options", tsTimezone, str);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    default:
      // TODO return the correct error code to client in the format for taos_errstr()
      tscError("Invalid option %d", option);
      return -1;
  }
#endif
  return 0;
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
  static int32_t  requestSerialId = 0;

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

  int64_t  ts = taosGetTimestampMs();
  uint64_t pid = taosGetPId();
  int32_t  val = atomic_add_fetch_32(&requestSerialId, 1);

  uint64_t id = ((hashId & 0x0FFF) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
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
