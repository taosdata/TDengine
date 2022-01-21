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

#include "os.h"
#include "taosmsg.h"
#include "tref.h"
#include "trpc.h"
#include "tnote.h"
#include "ttimer.h"
#include "tsched.h"
#include "tscLog.h"
#include "tsclient.h"
#include "tglobal.h"
#include "tconfig.h"
#include "ttimezone.h"
#include "qScript.h"

// global, not configurable
#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

int32_t    sentinel = TSC_VAR_NOT_RELEASE;

//SHashObj  *tscVgroupMap;         // hash map to keep the vgroup info from mnode
//SHashObj  *tscTableMetaMap;      // table meta info buffer
//SCacheObj *tscVgroupListBuf;     // super table vgroup list information, only survives 5 seconds for each super table vgroup list
SHashObj  *tscClusterMap = NULL;        // cluster obj
static pthread_mutex_t clusterMutex; // mutex to protect open the cluster obj

int32_t    tscObjRef = -1;
void      *tscTmr;
void      *tscQhandle;
int32_t    tscRefId = -1;
int32_t    tscNumOfObj = 0;         // number of sqlObj in current process.
static void  *tscCheckDiskUsageTmr;
void      *tscRpcCache;            // cache to keep rpc obj
int32_t    tscNumOfThreads = 1;     // num of rpc threads
char       tscLogFileName[12] = "taoslog";
int        tscLogFileNum = 10;

static pthread_mutex_t rpcObjMutex; // mutex to protect open the rpc obj concurrently
static pthread_once_t  tscinit = PTHREAD_ONCE_INIT;
static pthread_mutex_t setConfMutex = PTHREAD_MUTEX_INITIALIZER;

// pthread_once can not return result code, so result code is set to a global variable.
static volatile int tscInitRes = 0;

void tscCheckDiskUsage(void *UNUSED_PARAM(para), void *UNUSED_PARAM(param)) {
  taosGetDisk();
  taosTmrReset(tscCheckDiskUsage, 20 * 1000, NULL, tscTmr, &tscCheckDiskUsageTmr);
}

void tscFreeRpcObj(void *param) {
  assert(param);
  SRpcObj *pRpcObj = (SRpcObj *)(param);
  tscDebug("free rpcObj:%p and free pDnodeConn: %p", pRpcObj, pRpcObj->pDnodeConn);
  rpcClose(pRpcObj->pDnodeConn);
}

void tscReleaseRpc(void *param)  {
  if (param == NULL) {
    return;
  }

  taosCacheRelease(tscRpcCache, (void *)&param, false);
}

int32_t tscAcquireRpc(const char *key, const char *user, const char *secretEncrypt, void **ppRpcObj) {
  pthread_mutex_lock(&rpcObjMutex);

  SRpcObj *pRpcObj = (SRpcObj *)taosCacheAcquireByKey(tscRpcCache, key, strlen(key));
  if (pRpcObj != NULL) {
    *ppRpcObj = pRpcObj;   
    pthread_mutex_unlock(&rpcObjMutex);
    return 0;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "TSC";
  rpcInit.numOfThreads = tscNumOfThreads;    
  rpcInit.cfp = tscProcessMsgFromServer;
  rpcInit.sessions = tsMaxConnections;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = (char *)user;
  rpcInit.idleTime = tsShellActivityTimer * 1000; 
  rpcInit.ckey = "key"; 
  rpcInit.spi = 1; 
  rpcInit.secret = (char *)secretEncrypt;

  SRpcObj rpcObj;
  memset(&rpcObj, 0, sizeof(rpcObj));
  tstrncpy(rpcObj.key, key, sizeof(rpcObj.key));
  rpcObj.pDnodeConn = rpcOpen(&rpcInit);
  if (rpcObj.pDnodeConn == NULL) {
    pthread_mutex_unlock(&rpcObjMutex);
    tscError("failed to init connection to server");
    return -1;
  }

  pRpcObj = taosCachePut(tscRpcCache, rpcObj.key, strlen(rpcObj.key), &rpcObj, sizeof(rpcObj), 1000*5);   
  if (pRpcObj == NULL) {
    rpcClose(rpcObj.pDnodeConn);
    pthread_mutex_unlock(&rpcObjMutex);
    return -1;
  } 

  *ppRpcObj  = pRpcObj;
  pthread_mutex_unlock(&rpcObjMutex);
  return 0;
}

void tscClusterInfoDestroy(SClusterInfo *pObj) {
  if (pObj == NULL) { return; }
  taosHashCleanup(pObj->vgroupMap);
  taosHashCleanup(pObj->tableMetaMap);
  taosCacheCleanup(pObj->vgroupListBuf);
  tfree(pObj);
}

void *tscAcquireClusterInfo(const char *clusterId) {
  pthread_mutex_lock(&clusterMutex);
  size_t len = strlen(clusterId);

  SClusterInfo *pObj   = NULL;
  SClusterInfo **ppObj = taosHashGet(tscClusterMap, clusterId, len); 
  if (ppObj == NULL || *ppObj == NULL) {
    pObj = calloc(1, sizeof(SClusterInfo));
    if (pObj) {
      pObj->vgroupMap     = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
      pObj->tableMetaMap  = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK); //
      pObj->vgroupListBuf = taosCacheInit(TSDB_DATA_TYPE_BINARY, 5, false, NULL, "stable-vgroup-list");
      if (pObj->vgroupMap == NULL || pObj->tableMetaMap == NULL || pObj->vgroupListBuf == NULL) {
        tscClusterInfoDestroy(pObj);
        pObj = NULL;
      } else {
        taosHashPut(tscClusterMap, clusterId, len, &pObj, POINTER_BYTES);
      } 
    }
  } else {
    pObj = *ppObj;
  }

  if (pObj) { pObj->ref += 1; }
  
  pthread_mutex_unlock(&clusterMutex);
  return pObj;
}
void tscReleaseClusterInfo(const char *clusterId) {
  pthread_mutex_lock(&clusterMutex);

  size_t len = strlen(clusterId); 
  SClusterInfo *pObj = NULL;
  SClusterInfo **ppObj = taosHashGet(tscClusterMap, clusterId, len); 
  if (ppObj != NULL && *ppObj != NULL) {
    pObj = *ppObj;
  }
  if (pObj && --pObj->ref == 0) {
    taosHashRemove(tscClusterMap, clusterId, len);
    tscClusterInfoDestroy(pObj); 
  }
  pthread_mutex_unlock(&clusterMutex);
}
void taos_init_imp(void) {
  char temp[128] = {0};

  // In the APIs of other program language, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated resource to suppress the valgrind warning.
  atexit(taos_cleanup);
  
  errno = TSDB_CODE_SUCCESS;
  srand(taosGetTimestampSec());
  deltaToUtcInitOnce();

  if (tscEmbedded == 0) {

    // Read global configuration.
    taosInitGlobalCfg();
    taosReadGlobalLogCfg();

    // For log directory
    if (mkdir(tsLogDir, 0755) != 0 && errno != EEXIST) {
      printf("failed to create log dir:%s\n", tsLogDir);
    }

    sprintf(temp, "%s/%s", tsLogDir, tscLogFileName);
    if (taosInitLog(temp, tsNumOfLogLines, tscLogFileNum) < 0) {
      printf("failed to open log file in directory:%s\n", tsLogDir);
    }

    taosReadGlobalCfg();
    if (taosCheckGlobalCfg()) {
      tscInitRes = -1;
      return;
    }
    
    taosInitNotes();

    rpcInit();
#ifdef LUA_EMBEDDED
    scriptEnvPoolInit();
#endif
    tscDebug("starting to initialize client ...");
    tscDebug("Local End Point is:%s", tsLocalEp);
  }

  taosSetCoreDump();
  tscInitMsgsFp();

  double factor = (tscEmbedded == 0)? 2.0:4.0;
  tscNumOfThreads = (int)(tsNumOfCores * tsNumOfThreadsPerCore / factor);
  if (tscNumOfThreads < 2) {
    tscNumOfThreads = 2;
  }

  int32_t queueSize = tsMaxConnections*2;
  tscQhandle = taosInitScheduler(queueSize, tscNumOfThreads, "tsc");
  if (NULL == tscQhandle) {
    tscError("failed to init task queue");
    tscInitRes = -1;
    return;
  }

  tscDebug("client task queue is initialized, numOfWorkers: %d", tscNumOfThreads);

  tscTmr = taosTmrInit(tsMaxConnections * 2, 200, 60000, "TSC");
  if(0 == tscEmbedded){
    taosTmrReset(tscCheckDiskUsage, 20 * 1000, NULL, tscTmr, &tscCheckDiskUsageTmr);      
  }

  if (tscClusterMap == NULL) {
    tscObjRef        = taosOpenRef(40960, tscFreeRegisteredSqlObj);

    tscClusterMap    = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK); 
    pthread_mutex_init(&clusterMutex, NULL); 
    //tscVgroupMap     = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    //tscTableMetaMap  = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    //tscVgroupListBuf = taosCacheInit(TSDB_DATA_TYPE_BINARY, 5, false, NULL, "stable-vgroup-list");
    //tscDebug("TableMeta:%p, vgroup:%p is initialized", tscTableMetaMap, tscVgroupMap);

  }
   
  int refreshTime = 5;
  tscRpcCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, refreshTime, true, tscFreeRpcObj, "rpcObj");
  pthread_mutex_init(&rpcObjMutex, NULL);

  tscRefId = taosOpenRef(200, tscCloseTscObj);

  tscDebug("client is initialized successfully");
}

int taos_init() {
  pthread_once(&tscinit, taos_init_imp);
  return tscInitRes;
}

// this function may be called by user or system, or by both simultaneously.
void taos_cleanup(void) {
  tscDebug("start to cleanup client environment");

  if (atomic_val_compare_exchange_32(&sentinel, TSC_VAR_NOT_RELEASE, TSC_VAR_RELEASED) != TSC_VAR_NOT_RELEASE) {
    return;
  }

  if (tscEmbedded == 0) {
    #ifdef LUA_EMBEDDED
    scriptEnvPoolCleanup();
    #endif
  }

  int32_t id = tscObjRef;
  tscObjRef = -1;
  taosCloseRef(id);

  void* p = tscQhandle;
  tscQhandle = NULL;
  taosCleanUpScheduler(p);

  id = tscRefId;
  tscRefId = -1;
  taosCloseRef(id);

  taosCleanupKeywordsTable();

  p = tscRpcCache; 
  tscRpcCache = NULL;
  
  if (p != NULL) {
    taosCacheCleanup(p); 
    pthread_mutex_destroy(&rpcObjMutex);
  }

  pthread_mutex_destroy(&setConfMutex);

  if (tscEmbedded == 0) {
    rpcCleanup();
    taosCloseLog();
  };

  taosHashCleanup(tscClusterMap);
  tscClusterMap = NULL;
  pthread_mutex_destroy(&clusterMutex);

  p = tscTmr;
  tscTmr = NULL;
  taosTmrCleanUp(p);
}

static int taos_options_imp(TSDB_OPTION option, const char *pStr) {
  SGlobalCfg *cfg = NULL;

  switch (option) {
    case TSDB_OPTION_CONFIGDIR:
      cfg = taosGetConfigOption("configDir");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tstrncpy(configDir, pStr, TSDB_FILENAME_LEN);
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscInfo("set config file directory:%s", pStr);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_SHELL_ACTIVITY_TIMER:
      cfg = taosGetConfigOption("shellActivityTimer");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tsShellActivityTimer = atoi(pStr);
        if (tsShellActivityTimer < 1) tsShellActivityTimer = 1;
        if (tsShellActivityTimer > 3600) tsShellActivityTimer = 3600;
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscInfo("set shellActivityTimer:%d", tsShellActivityTimer);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], *(int32_t *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_LOCALE: {  // set locale
      cfg = taosGetConfigOption("locale");
      assert(cfg != NULL);
  
      size_t len = strlen(pStr);
      if (len == 0 || len > TSDB_LOCALE_LEN) {
        tscInfo("Invalid locale:%s, use default", pStr);
        return -1;
      }

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        char sep = '.';

        if (strlen(tsLocale) == 0) { // locale does not set yet
          char* defaultLocale = setlocale(LC_CTYPE, "");

          // The locale of the current OS does not be set correctly, so the default locale cannot be acquired.
          // The launch of current system will abort soon.
          if (defaultLocale == NULL) {
            tscError("failed to get default locale, please set the correct locale in current OS");
            return -1;
          }

          tstrncpy(tsLocale, defaultLocale, TSDB_LOCALE_LEN);
        }

        // set the user specified locale
        char *locale = setlocale(LC_CTYPE, pStr);

        if (locale != NULL) { // failed to set the user specified locale
          tscInfo("locale set, prev locale:%s, new locale:%s", tsLocale, locale);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else { // set the user specified locale failed, use default LC_CTYPE as current locale
          locale = setlocale(LC_CTYPE, tsLocale);
          if (locale == NULL) {
            tscError("failed to set locale:%s failed, neither default LC_CTYPE: %s", pStr, tsLocale);
            return -1;
          }
          tscInfo("failed to set locale:%s, current locale:%s", pStr, tsLocale);
        }

        tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);

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

            tstrncpy(tsCharset, charset, TSDB_LOCALE_LEN);
            cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;

          } else {
            tscInfo("charset:%s is not valid in locale, charset remains:%s", charset, tsCharset);
          }

          free(charset);
        } else { // it may be windows system
          tscInfo("charset remains:%s", tsCharset);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;
    }

    case TSDB_OPTION_CHARSET: {
      /* set charset will override the value of charset, assigned during system locale changed */
      cfg = taosGetConfigOption("charset");
      assert(cfg != NULL);
      
      size_t len = strlen(pStr);
      if (len == 0 || len > TSDB_LOCALE_LEN) {
        tscInfo("failed to set charset:%s", pStr);
        return -1;
      }

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        if (taosValidateEncodec(pStr)) {
          if (strlen(tsCharset) == 0) {
            tscInfo("charset is set:%s", pStr);
          } else {
            tscInfo("charset changed from %s to %s", tsCharset, pStr);
          }

          tstrncpy(tsCharset, pStr, TSDB_LOCALE_LEN);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else {
          tscInfo("charset:%s not valid", pStr);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }

      break;
    }

    case TSDB_OPTION_TIMEZONE:
      cfg = taosGetConfigOption("timezone");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tstrncpy(tsTimezone, pStr, TSDB_TIMEZONE_LEN);
        tsSetTimeZone();
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscDebug("timezone set:%s, input:%s by taos_options", tsTimezone, pStr);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    default:
      // TODO return the correct error code to client in the format for taos_errstr()
      tscError("Invalid option %d", option);
      return -1;
  }

  return 0;
}

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  static int32_t lock = 0;

  for (int i = 1; atomic_val_compare_exchange_32(&lock, 0, 1) != 0; ++i) {
    if (i % 1000 == 0) {
      tscInfo("haven't acquire lock after spin %d times.", i);
      sched_yield();
    }
  }

  int ret = taos_options_imp(option, (const char*)arg);

  atomic_store_32(&lock, 0);
  return ret;
}

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
  pthread_mutex_lock(&setConfMutex);
  setConfRet ret = taos_set_config_imp(config);
  pthread_mutex_unlock(&setConfMutex);
  return ret;
}
