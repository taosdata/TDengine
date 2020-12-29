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
#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

int32_t    sentinel = TSC_VAR_NOT_RELEASE;

SHashObj  *tscVgroupMap;         // hash map to keep the global vgroup info
SHashObj  *tscTableMetaInfo;     // table meta info
int32_t    tscObjRef = -1;
void      *tscTmr;
void      *tscQhandle;
int32_t    tscRefId = -1;
int32_t    tscNumOfObj = 0;         // number of sqlObj in current process.

static void  *tscCheckDiskUsageTmr;
static pthread_once_t tscinit = PTHREAD_ONCE_INIT;

void tscCheckDiskUsage(void *UNUSED_PARAM(para), void* UNUSED_PARAM(param)) {
  taosGetDisk();
  taosTmrReset(tscCheckDiskUsage, 1000, NULL, tscTmr, &tscCheckDiskUsageTmr);
}

int32_t tscInitRpc(const char *user, const char *secretEncrypt, void **pDnodeConn) {
  SRpcInit rpcInit;

  if (*pDnodeConn == NULL) {
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = "TSC";
    rpcInit.numOfThreads = 1;  // every DB connection has only one thread
    rpcInit.cfp = tscProcessMsgFromServer;
    rpcInit.sessions = tsMaxConnections;
    rpcInit.connType = TAOS_CONN_CLIENT;
    rpcInit.user = (char *)user;
    rpcInit.idleTime = 2000;
    rpcInit.ckey = "key";
    rpcInit.spi = 1;
    rpcInit.secret = (char *)secretEncrypt;

    *pDnodeConn = rpcOpen(&rpcInit);
    if (*pDnodeConn == NULL) {
      tscError("failed to init connection to TDengine");
      return -1;
    } else {
      tscDebug("dnodeConn:%p is created, user:%s", *pDnodeConn, user);
    }
  }

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
  int32_t tscNumOfThreads = (int)(tsNumOfCores * tsNumOfThreadsPerCore / factor);
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

  if (tscTableMetaInfo == NULL) {
    tscObjRef  = taosOpenRef(40960, tscFreeRegisteredSqlObj);
    tscVgroupMap = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    tscTableMetaInfo = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    tscDebug("TableMeta:%p", tscTableMetaInfo);
  }

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

  if (atomic_val_compare_exchange_32(&sentinel, TSC_VAR_NOT_RELEASE, TSC_VAR_RELEASED) != TSC_VAR_NOT_RELEASE) {
    return;
  }

  taosHashCleanup(tscTableMetaInfo);
  tscTableMetaInfo = NULL;

  taosHashCleanup(tscVgroupMap);
  tscVgroupMap = NULL;

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
  taosCloseLog();

  if (tscEmbedded == 0) {
    rpcCleanup();
  }

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
