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
#include "tsystem.h"
#include "ttimer.h"
#include "tutil.h"
#include "tsched.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tglobal.h"
#include "tconfig.h"
#include "ttimezone.h"
#include "tlocale.h"

// global, not configurable
SCacheObj *tscMetaCache;       // table meta cache
SHashObj  *tscHashMap;         // hash map to keep the global vgroup info
int     tscObjRef = -1;
void *  tscTmr;
void *  tscQhandle;
void *  tscCheckDiskUsageTmr;
void *  tscRpcCache;
int     tscRefId = -1;
int     tscNumOfObj = 0;       // number of sqlObj in current process.
int     tscNumOfThreads = 1;

static pthread_mutex_t rpcObjMutex;


static pthread_once_t tscinit = PTHREAD_ONCE_INIT;

void tscCheckDiskUsage(void *UNUSED_PARAM(para), void* UNUSED_PARAM(param)) {
  taosGetDisk();
  taosTmrReset(tscCheckDiskUsage, 1000, NULL, tscTmr, &tscCheckDiskUsageTmr);
}

void tscFreeRpcObj(void *param) {
  assert(param);
  SRpcObj *pRpcObj = (SRpcObj *)(param);
  rpcClose(pRpcObj->pDnodeConn);
}
void *tscAcquireRpc(const char *insKey) {
  SRpcObj *pRpcObj = taosCacheAcquireByKey(tscRpcCache, insKey, strlen(insKey)); 
  if (pRpcObj == NULL) {
    return NULL;
  }
  return pRpcObj; 
}

void tscReleaseRpc(void *param)  {
  if (param == NULL) {
    return;
  }
  pthread_mutex_lock(&rpcObjMutex);
  taosCacheRelease(tscRpcCache, (void *)&param, false); 
  pthread_mutex_unlock(&rpcObjMutex);
} 

int32_t tscInitRpc(const char *insKey, const char *user, const char *secretEncrypt, void **ppRpcObj, void **pDnodeConn) {
  pthread_mutex_lock(&rpcObjMutex);

  SRpcObj *pRpcObj = (SRpcObj *)tscAcquireRpc(insKey); 
  if (pRpcObj != NULL) {
    *ppRpcObj = pRpcObj;   
    *pDnodeConn = pRpcObj->pDnodeConn; 
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
  rpcInit.idleTime = 2000; 
  rpcInit.ckey = "key"; 
  rpcInit.spi = 1; 
  rpcInit.secret = (char *)secretEncrypt;

  SRpcObj rpcObj;
  memset(&rpcObj, 0, sizeof(rpcObj));
  strncpy(rpcObj.key, insKey, strlen(insKey));
  rpcObj.pDnodeConn = rpcOpen(&rpcInit);
  if (rpcObj.pDnodeConn == NULL) {
    pthread_mutex_unlock(&rpcObjMutex);
    tscError("failed to init connection to TDengine");
    return -1;
  } 
  pRpcObj = taosCachePut(tscRpcCache, rpcObj.key, strlen(rpcObj.key), &rpcObj, sizeof(rpcObj), 1000*10);   
  if (pRpcObj == NULL) {
    rpcClose(rpcObj.pDnodeConn);
    pthread_mutex_unlock(&rpcObjMutex);
    return -1;
  } 

  *ppRpcObj  = pRpcObj;
  *pDnodeConn = pRpcObj->pDnodeConn;
  pthread_mutex_unlock(&rpcObjMutex);
  return 0;
}


void taos_init_imp(void) {
  char temp[128]  = {0};
  
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

    sprintf(temp, "%s/taoslog", tsLogDir);
    if (taosInitLog(temp, tsNumOfLogLines, 10) < 0) {
      printf("failed to open log file in directory:%s\n", tsLogDir);
    }

    taosReadGlobalCfg();
    taosCheckGlobalCfg();
    taosInitNotes();

    rpcInit();
    tscDebug("starting to initialize TAOS client ...");
    tscDebug("Local End Point is:%s", tsLocalEp);
  }

  taosSetCoreDump();
  tscInitMsgsFp();
  int queueSize = tsMaxConnections*2;

  double factor = (tscEmbedded == 0)? 2.0:4.0;
  tscNumOfThreads = (int)(tsNumOfCores * tsNumOfThreadsPerCore / factor);
  if (tscNumOfThreads < 2) {
    tscNumOfThreads = 2;
  }

  tscQhandle = taosInitScheduler(queueSize, tscNumOfThreads, "tsc");
  if (NULL == tscQhandle) {
    tscError("failed to init scheduler");
    return;
  }

  tscTmr = taosTmrInit(tsMaxConnections * 2, 200, 60000, "TSC");
  if(0 == tscEmbedded){
    taosTmrReset(tscCheckDiskUsage, 10, NULL, tscTmr, &tscCheckDiskUsageTmr);      
  }

  int64_t refreshTime = 10; // 10 seconds by default
  if (tscMetaCache == NULL) {
    tscMetaCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, refreshTime, false, tscFreeTableMetaHelper, "tableMeta");
    tscObjRef  = taosOpenRef(40960, tscFreeRegisteredSqlObj);
    tscHashMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  }

  tscRpcCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, refreshTime, true, tscFreeRpcObj, "rpcObj");
  pthread_mutex_init(&rpcObjMutex, NULL);

  tscRefId = taosOpenRef(200, tscCloseTscObj);

  // in other language APIs, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated
  // resource to suppress the valgrind warning.
  atexit(taos_cleanup);
  tscDebug("client is initialized successfully");
}

void taos_init() { pthread_once(&tscinit, taos_init_imp); }

// this function may be called by user or system, or by both simultaneously.
void taos_cleanup(void) {
  tscDebug("start to cleanup client environment");

  void* m = tscMetaCache;
  if (m != NULL && atomic_val_compare_exchange_ptr(&tscMetaCache, m, 0) == m) {
    taosCacheCleanup(m);
  }

  int refId = atomic_exchange_32(&tscObjRef, -1);
  if (refId != -1) {
    taosCloseRef(refId);
  }

  m = tscQhandle;
  if (m != NULL && atomic_val_compare_exchange_ptr(&tscQhandle, m, 0) == m) {
    taosCleanUpScheduler(m);
  }

  taosCloseRef(tscRefId);
  taosCleanupKeywordsTable();
  taosCloseLog();

  m = tscRpcCache; 
  if (m != NULL && atomic_val_compare_exchange_ptr(&tscRpcCache, m, 0) == m) {
    pthread_mutex_lock(&rpcObjMutex);
    taosCacheCleanup(tscRpcCache); 
    tscRpcCache = NULL;
    pthread_mutex_unlock(&rpcObjMutex);
    pthread_mutex_destroy(&rpcObjMutex);
  }

  if (tscEmbedded == 0) rpcCleanup();
  m = tscTmr;
  if (m != NULL && atomic_val_compare_exchange_ptr(&tscTmr, m, 0) == m) {
    taosTmrCleanUp(m);
  }
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
          tstrncpy(tsLocale, defaultLocale, TSDB_LOCALE_LEN);
        }

        // set the user specified locale
        char *locale = setlocale(LC_CTYPE, pStr);

        if (locale != NULL) {
          tscInfo("locale set, prev locale:%s, new locale:%s", tsLocale, locale);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else { // set the user-specified localed failed, use default LC_CTYPE as current locale
          locale = setlocale(LC_CTYPE, tsLocale);
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
