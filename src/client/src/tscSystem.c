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
#include "tcache.h"
#include "tlog.h"
#include "trpc.h"
#include "tsdb.h"
#include "tsocket.h"
#include "tsystem.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#include "tsclient.h"
// global, not configurable
void *  pVnodeConn;
void *  pVMeterConn;
void *  pTscMgmtConn;
void *  pSlaveConn;
void *  tscCacheHandle;
uint8_t globalCode = 0;
int     initialized = 0;
int     slaveIndex;
void *  tscTmr;
void *  tscQhandle;
void *  tscConnCache;
void *  tscCheckDiskUsageTmr;
int     tsInsertHeadSize;

extern int            tscEmbedded;
int                   tscNumOfThreads;
static pthread_once_t tscinit = PTHREAD_ONCE_INIT;

extern int  tsTscEnableRecordSql;
extern int  tsNumOfLogLines;
void taosInitNote(int numOfNoteLines, int maxNotes, char* lable);
void deltaToUtcInitOnce();

void tscCheckDiskUsage(void *para, void *unused) {
  taosGetDisk();
  taosTmrReset(tscCheckDiskUsage, 1000, NULL, tscTmr, &tscCheckDiskUsageTmr);
}

void taos_init_imp() {
  char        temp[128];
  struct stat dirstat;
  SRpcInit    rpcInit;

  srand(taosGetTimestampSec());
  deltaToUtcInitOnce();

  if (tscEmbedded == 0) {
    /*
     * set localIp = 0
     * means unset tsLocalIp in client
     * except read from config file
     */
    strcpy(tsLocalIp, "0.0.0.0");

    // Read global configuration.
    tsReadGlobalLogConfig();

    // For log directory
    if (stat(logDir, &dirstat) < 0) mkdir(logDir, 0755);

    sprintf(temp, "%s/taoslog", logDir);
    if (taosInitLog(temp, tsNumOfLogLines, 10) < 0) {
      printf("failed to open log file in directory:%s\n", logDir);
    }

    tsReadGlobalConfig();
    tsPrintGlobalConfig();

    tscTrace("starting to initialize TAOS client ...");
    tscTrace("Local IP address is:%s", tsLocalIp);
  }

  taosSetCoreDump();

  if (tsTscEnableRecordSql != 0) {
    taosInitNote(tsNumOfLogLines / 10, 1, (char*)"tsc_note");
  }
  
  tscMgmtIpList.numOfIps = 2;
  strcpy(tscMgmtIpList.ipstr[0], tsMasterIp);
  tscMgmtIpList.ip[0] = inet_addr(tsMasterIp);

  strcpy(tscMgmtIpList.ipstr[1], tsMasterIp);
  tscMgmtIpList.ip[1] = inet_addr(tsMasterIp);

  if (tsSecondIp[0]) {
    tscMgmtIpList.numOfIps = 3;
    strcpy(tscMgmtIpList.ipstr[2], tsSecondIp);
    tscMgmtIpList.ip[2] = inet_addr(tsSecondIp);
  }

  tscInitMsgs();
  slaveIndex = rand();
  int queueSize = tsMaxVnodeConnections + tsMaxMeterConnections + tsMaxMgmtConnections + tsMaxMgmtConnections;

  if (tscEmbedded == 0) {
    tscNumOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  } else {
    tscNumOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  }

  if (tscNumOfThreads < 2) tscNumOfThreads = 2;

  tscQhandle = taosInitScheduler(queueSize, tscNumOfThreads, "tsc");
  if (NULL == tscQhandle) {
    tscError("failed to init scheduler");
    return;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsLocalIp;
  rpcInit.localPort = 0;
  rpcInit.label = "TSC-vnode";
  rpcInit.numOfThreads = tscNumOfThreads;
  rpcInit.fp = tscProcessMsgFromServer;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = tscNumOfThreads;
  rpcInit.sessionsPerChann = tsMaxVnodeConnections / tscNumOfThreads;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.noFree = 0;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_C();
  rpcInit.qhandle = tscQhandle;
  pVnodeConn = taosOpenRpc(&rpcInit);
  if (pVnodeConn == NULL) {
    tscError("failed to init connection to vnode");
    return;
  }

  for (int i = 0; i < tscNumOfThreads; ++i) {
    int retVal = taosOpenRpcChann(pVnodeConn, i, rpcInit.sessionsPerChann);
    if (0 != retVal) {
      tError("TSC-vnode, failed to open rpc chann");
      taosCloseRpc(pVnodeConn);
      return;
    }
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsLocalIp;
  rpcInit.localPort = 0;
  rpcInit.label = "TSC-mgmt";
  rpcInit.numOfThreads = 1;
  rpcInit.fp = tscProcessMsgFromServer;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = tsMaxMgmtConnections;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.noFree = 0;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_C();
  rpcInit.qhandle = tscQhandle;
  pTscMgmtConn = taosOpenRpc(&rpcInit);
  if (pTscMgmtConn == NULL) {
    tscError("failed to init connection to mgmt");
    return;
  }

  tscTmr = taosTmrInit(tsMaxMgmtConnections * 2, 200, 60000, "TSC");
  if(0 == tscEmbedded){
    taosTmrReset(tscCheckDiskUsage, 10, NULL, tscTmr, &tscCheckDiskUsageTmr);      
  }
  int64_t refreshTime = tsMetricMetaKeepTimer < tsMeterMetaKeepTimer ? tsMetricMetaKeepTimer : tsMeterMetaKeepTimer;
  refreshTime = refreshTime > 2 ? 2 : refreshTime;
  refreshTime = refreshTime < 1 ? 1 : refreshTime;

  if (tscCacheHandle == NULL) tscCacheHandle = taosInitDataCache(tsMaxMeterConnections / 2, tscTmr, refreshTime);

  tscConnCache = taosOpenConnCache(tsMaxMeterConnections * 2, taosCloseRpcConn, tscTmr, tsShellActivityTimer * 1000);

  initialized = 1;
  tscTrace("client is initialized successfully");
  tsInsertHeadSize = tsRpcHeadSize + sizeof(SShellSubmitMsg);
}

void taos_init() { pthread_once(&tscinit, taos_init_imp); }

static int taos_options_imp(TSDB_OPTION option, const char *pStr) {
  SGlobalConfig *cfg = NULL;

  switch (option) {
    case TSDB_OPTION_CONFIGDIR:
      cfg = tsGetConfigOption("configDir");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_OPTION) {
        strncpy(configDir, pStr, TSDB_FILENAME_LEN);
        cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;
        tscPrint("set config file directory:%s", pStr);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_SHELL_ACTIVITY_TIMER:
      cfg = tsGetConfigOption("shellActivityTimer");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_OPTION) {
        tsShellActivityTimer = atoi(pStr);
        if (tsShellActivityTimer < 1) tsShellActivityTimer = 1;
        if (tsShellActivityTimer > 3600) tsShellActivityTimer = 3600;
        cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;
        tscPrint("set shellActivityTimer:%d", tsShellActivityTimer);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (int32_t *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_LOCALE: {  // set locale
      cfg = tsGetConfigOption("locale");
      assert(cfg != NULL);
  
      size_t len = strlen(pStr);
      if (len == 0 || len > TSDB_LOCALE_LEN) {
        tscPrint("Invalid locale:%s, use default", pStr);
        return -1;
      }

      if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_OPTION) {
        char sep = '.';

        if (strlen(tsLocale) == 0) { // locale does not set yet
          char* defaultLocale = setlocale(LC_CTYPE, "");
          strcpy(tsLocale, defaultLocale);
        }

        // set the user specified locale
        char *locale = setlocale(LC_CTYPE, pStr);

        if (locale != NULL) {
          tscPrint("locale set, prev locale:%s, new locale:%s", tsLocale, locale);
          cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;
        } else { // set the user-specified localed failed, use default LC_CTYPE as current locale
          locale = setlocale(LC_CTYPE, tsLocale);
          tscPrint("failed to set locale:%s, current locale:%s", pStr, tsLocale);
        }

        strncpy(tsLocale, locale, tListLen(tsLocale));

        char *charset = strrchr(tsLocale, sep);
        if (charset != NULL) {
          charset += 1;

          charset = taosCharsetReplace(charset);

          if (taosValidateEncodec(charset)) {
            if (strlen(tsCharset) == 0) {
              tscPrint("charset set:%s", charset);
            } else {
              tscPrint("charset changed from %s to %s", tsCharset, charset);
            }

            strncpy(tsCharset, charset, tListLen(tsCharset));
            cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;

          } else {
            tscPrint("charset:%s is not valid in locale, charset remains:%s", charset, tsCharset);
          }

          free(charset);
        } else { // it may be windows system
          tscPrint("charset remains:%s", tsCharset);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;
    }

    case TSDB_OPTION_CHARSET: {
      /* set charset will override the value of charset, assigned during system locale changed */
      cfg = tsGetConfigOption("charset");
      assert(cfg != NULL);
      
      size_t len = strlen(pStr);
      if (len == 0 || len > TSDB_LOCALE_LEN) {
        tscPrint("failed to set charset:%s", pStr);
        return -1;
      }

      if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_OPTION) {
        if (taosValidateEncodec(pStr)) {
          if (strlen(tsCharset) == 0) {
            tscPrint("charset is set:%s", pStr);
          } else {
            tscPrint("charset changed from %s to %s", tsCharset, pStr);
          }

          strncpy(tsCharset, pStr, tListLen(tsCharset));
          cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;
        } else {
          tscPrint("charset:%s not valid", pStr);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }

      break;
    }

    case TSDB_OPTION_TIMEZONE:
      cfg = tsGetConfigOption("timezone");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_OPTION) {
        strcpy(tsTimezone, pStr);
        tsSetTimeZone();
        cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;
        tscTrace("timezone set:%s, input:%s by taos_options", tsTimezone, pStr);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, pStr,
                tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_SOCKET_TYPE:
      cfg = tsGetConfigOption("sockettype");
      assert(cfg != NULL);
    
      if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_OPTION) {
        if (strcasecmp(pStr, TAOS_SOCKET_TYPE_NAME_UDP) != 0 && strcasecmp(pStr, TAOS_SOCKET_TYPE_NAME_TCP) != 0) {
          tscError("only 'tcp' or 'udp' allowed for configuring the socket type");
          return -1;
        }

        strncpy(tsSocketType, pStr, tListLen(tsSocketType));
        cfg->cfgStatus = TSDB_CFG_CSTATUS_OPTION;
        tscPrint("socket type is set:%s", tsSocketType);
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
      tscPrint("haven't acquire lock after spin %d times.", i);
      sched_yield();
    }
  }

  int ret = taos_options_imp(option, (const char*)arg);

  atomic_store_32(&lock, 0);
  return ret;
}
