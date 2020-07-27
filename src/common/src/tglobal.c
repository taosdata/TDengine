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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tulog.h"
#include "tconfig.h"
#include "tglobal.h"
#include "monitor.h"
#include "tsocket.h"
#include "tutil.h"
#include "tlocale.h"
#include "ttimezone.h"
#include "tsync.h"

// cluster
char     tsFirst[TSDB_EP_LEN] = {0};
char     tsSecond[TSDB_EP_LEN] = {0};
char     tsArbitrator[TSDB_EP_LEN] = {0};
char     tsLocalFqdn[TSDB_FQDN_LEN] = {0};
char     tsLocalEp[TSDB_EP_LEN] = {0};  // Local End Point, hostname:port
uint16_t tsServerPort = 6030;
uint16_t tsDnodeShellPort = 6030;  // udp[6035-6039] tcp[6035]
uint16_t tsDnodeDnodePort = 6035;  // udp/tcp
uint16_t tsSyncPort = 6040;
int32_t  tsStatusInterval = 1;  // second
int32_t  tsNumOfMnodes = 3;
int32_t  tsEnableVnodeBak = 1;

// common
int32_t tsRpcTimer = 1000;
int32_t tsRpcMaxTime = 600;  // seconds;
int32_t tsMaxShellConns = 5000;
int32_t tsMaxConnections = 5000;
int32_t tsShellActivityTimer = 3;  // second
float   tsNumOfThreadsPerCore = 1.0;
float   tsRatioOfQueryThreads = 0.5;
int8_t  tsDaylight = 0;
char    tsTimezone[TSDB_TIMEZONE_LEN] = {0};
char    tsLocale[TSDB_LOCALE_LEN] = {0};
char    tsCharset[TSDB_LOCALE_LEN] = {0};  // default encode string
int32_t tsEnableCoreFile = 0;
int32_t tsMaxBinaryDisplayWidth = 30;

/*
 * denote if the server needs to compress response message at the application layer to client, including query rsp,
 * metricmeta rsp, and multi-meter query rsp message body. The client compress the submit message to server.
 *
 * 0: all data are compressed
 * -1: all data are not compressed
 * other values: if the message payload size is greater than the tsCompressMsgSize, the message will be compressed.
 */
int32_t tsCompressMsgSize = -1;

// client
int32_t tsTableMetaKeepTimer = 7200;  // second
int32_t tsMaxSQLStringLen = TSDB_MAX_SQL_LEN;
int32_t tsTscEnableRecordSql = 0;

// the maximum number of results for projection query on super table that are returned from
// one virtual node, to order according to timestamp
int32_t tsMaxNumOfOrderedResults = 100000;

// 10 ms for sliding time, the value will changed in case of time precision changed
int32_t tsMinSlidingTime = 10;

// 10 ms for interval time range, changed accordingly
int32_t tsMinIntervalTime = 10;

// 20sec, the maximum value of stream computing delay, changed accordingly
int32_t tsMaxStreamComputDelay = 20000;

// 10sec, the first stream computing delay time after system launched successfully, changed accordingly
int32_t tsStreamCompStartDelay = 10000;

// the stream computing delay time after executing failed, change accordingly
int32_t tsStreamCompRetryDelay = 10;

// The delayed computing ration. 10% of the whole computing time window by default.
float tsStreamComputDelayRatio = 0.1;

int32_t tsProjectExecInterval = 10000;   // every 10sec, the projection will be executed once
int64_t tsMaxRetentWindow = 24 * 3600L;  // maximum time window tolerance

// db parameters
int32_t tsCacheBlockSize = TSDB_DEFAULT_CACHE_BLOCK_SIZE;
int32_t tsBlocksPerVnode = TSDB_DEFAULT_TOTAL_BLOCKS;
int16_t tsDaysPerFile    = TSDB_DEFAULT_DAYS_PER_FILE;
int32_t tsDaysToKeep     = TSDB_DEFAULT_KEEP;
int32_t tsMinRowsInFileBlock = TSDB_DEFAULT_MIN_ROW_FBLOCK;
int32_t tsMaxRowsInFileBlock = TSDB_DEFAULT_MAX_ROW_FBLOCK;
int16_t tsCommitTime    = TSDB_DEFAULT_COMMIT_TIME;  // seconds
int32_t tsTimePrecision = TSDB_DEFAULT_PRECISION;
int16_t tsCompression   = TSDB_DEFAULT_COMP_LEVEL;
int16_t tsWAL           = TSDB_DEFAULT_WAL_LEVEL;
int32_t tsFsyncPeriod   = TSDB_DEFAULT_FSYNC_PERIOD;
int32_t tsReplications  = TSDB_DEFAULT_DB_REPLICA_OPTION;
int32_t tsMaxVgroupsPerDb  = 0;
int32_t tsMinTablePerVnode = 100;
int32_t tsMaxTablePerVnode = TSDB_DEFAULT_TABLES;
int32_t tsTableIncStepPerVnode = TSDB_TABLES_STEP;

// balance
int32_t tsEnableBalance = 1;
int32_t tsAlternativeRole = 0;
int32_t tsBalanceInterval = 300;  // seconds
int32_t tsOfflineThreshold = 86400*100;   // seconds 10days
int32_t tsMnodeEqualVnodeNum = 4;

// restful
int32_t  tsEnableHttpModule = 1;
int32_t  tsRestRowLimit = 10240;
uint16_t tsHttpPort = 6020;  // only tcp, range tcp[6020]
int32_t  tsHttpCacheSessions = 1000;
int32_t  tsHttpSessionExpire = 36000;
int32_t  tsHttpMaxThreads = 2;
int32_t  tsHttpEnableCompress = 0;
int32_t  tsHttpEnableRecordSql = 0;
int32_t  tsTelegrafUseFieldNum = 0;

// mqtt
int32_t tsEnableMqttModule = 0;  // not finished yet, not started it by default
char    tsMqttBrokerAddress[128] = {0};
char    tsMqttBrokerClientId[128] = {0};

// monitor
int32_t tsEnableMonitorModule = 0;
char    tsMonitorDbName[TSDB_DB_NAME_LEN] = "log";
char    tsInternalPass[] = "secretkey";
int32_t tsMonitorInterval = 30;  // seconds

// internal
int32_t tscEmbedded = 0;
char    configDir[TSDB_FILENAME_LEN] = "/etc/taos";
char    tsVnodeDir[TSDB_FILENAME_LEN] = {0};
char    tsDnodeDir[TSDB_FILENAME_LEN] = {0};
char    tsMnodeDir[TSDB_FILENAME_LEN] = {0};
char    tsDataDir[TSDB_FILENAME_LEN] = "/var/lib/taos";
char    tsScriptDir[TSDB_FILENAME_LEN] = "/etc/taos";
char    tsVnodeBakDir[TSDB_FILENAME_LEN] = {0};

/*
 * minimum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 86400000L
 *     TSDB_TIME_PRECISION_MICRO: 86400000000L
 *     TSDB_TIME_PRECISION_NANO:  86400000000000L
 */
int64_t tsMsPerDay[] = {86400000L, 86400000000L, 86400000000000L};

// system info
char    tsOsName[10] = "Linux";
int64_t tsPageSize;
int64_t tsOpenMax;
int64_t tsStreamMax;
int32_t tsNumOfCores = 1;
float   tsTotalTmpDirGB = 0;
float   tsTotalDataDirGB = 0;
float   tsAvailTmpDirectorySpace = 0;
float   tsAvailDataDirGB = 0;
float   tsReservedTmpDirectorySpace = 0.1;
float   tsMinimalDataDirGB = 0.5;
int32_t tsTotalMemoryMB = 0;
int32_t tsVersion = 0;

// log
int32_t tsNumOfLogLines = 10000000;
int32_t mDebugFlag = 135;
int32_t sdbDebugFlag = 135;
int32_t dDebugFlag = 135;
int32_t vDebugFlag = 131;
int32_t cDebugFlag = 131;
int32_t jniDebugFlag = 131;
int32_t odbcDebugFlag = 131;
int32_t httpDebugFlag = 131;
int32_t mqttDebugFlag = 131;
int32_t monitorDebugFlag = 131;
int32_t qDebugFlag = 131;
int32_t rpcDebugFlag = 131;
int32_t uDebugFlag = 131;
int32_t debugFlag = 0;
int32_t sDebugFlag = 135;
int32_t wDebugFlag = 135;
int32_t tsdbDebugFlag = 131;

int32_t (*monitorStartSystemFp)() = NULL;
void (*monitorStopSystemFp)() = NULL;
void (*monitorExecuteSQLFp)(char *sql) = NULL;

static pthread_once_t tsInitGlobalCfgOnce = PTHREAD_ONCE_INIT;

void taosSetAllDebugFlag() {
  if (debugFlag != 0) { 
    mDebugFlag = debugFlag;
    sdbDebugFlag = debugFlag;
    dDebugFlag = debugFlag;
    vDebugFlag = debugFlag;
    cDebugFlag = debugFlag;
    jniDebugFlag = debugFlag;
    odbcDebugFlag = debugFlag;
    httpDebugFlag = debugFlag;
    mqttDebugFlag = debugFlag;
    monitorDebugFlag = debugFlag;
    rpcDebugFlag = debugFlag;
    uDebugFlag = debugFlag;
    sDebugFlag = debugFlag;
    wDebugFlag = debugFlag;
    tsdbDebugFlag = debugFlag;
    qDebugFlag = debugFlag;    
    uInfo("all debug flag are set to %d", debugFlag);
  }
}

bool taosCfgDynamicOptions(char *msg) {
  char *option, *value;
  int32_t   olen, vlen;
  int32_t   vint = 0;

  paGetToken(msg, &option, &olen);
  if (olen == 0) return TSDB_CODE_COM_INVALID_CFG_MSG;

  paGetToken(option + olen + 1, &value, &vlen);
  if (vlen == 0)
    vint = 135;
  else {
    vint = atoi(value);
  }

  uInfo("change dynamic option: %s, value: %d", option, vint);

  for (int32_t i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_LOG)) continue;
    if (cfg->valType != TAOS_CFG_VTYPE_INT32) continue;
    if (strncasecmp(option, cfg->option, olen) != 0) continue;
    *((int32_t *)cfg->ptr) = vint;

    if (strncasecmp(cfg->option, "monitor", olen) == 0) {
      if (1 == vint) {
        if (monitorStartSystemFp) {
          (*monitorStartSystemFp)();
          uInfo("monitor is enabled");
        } else {
          uError("monitor can't be updated, for monitor not initialized");
        }
      } else {
        if (monitorStopSystemFp) {
          (*monitorStopSystemFp)();
          uInfo("monitor is disabled");
        } else {
          uError("monitor can't be updated, for monitor not initialized");
        }
      }
      return true;
    }

    if (strncasecmp(cfg->option, "debugFlag", olen) == 0) {
      taosSetAllDebugFlag();
    }
    
    return true;
  }

  if (strncasecmp(option, "resetlog", 8) == 0) {
    taosResetLog();
    taosPrintGlobalCfg();
    return true;
  }

  if (strncasecmp(option, "resetQueryCache", 15) == 0) {
    if (monitorExecuteSQLFp) {
      (*monitorExecuteSQLFp)("resetQueryCache");
      uInfo("resetquerycache is executed");
    } else {
      uError("resetquerycache can't be executed, for monitor not started");
    }
  }

  return false;
}

static void doInitGlobalConfig() {
  SGlobalCfg cfg = {0};
  
  // ip address
  cfg.option = "first";
  cfg.ptr = tsFirst;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_EP_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "second";
  cfg.ptr = tsSecond;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_EP_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "fqdn";
  cfg.ptr = tsLocalFqdn;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FQDN_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // port
  cfg.option = "serverPort";
  cfg.ptr = &tsServerPort;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 65535;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // directory
  cfg.option = "configDir";
  cfg.ptr = configDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "logDir";
  cfg.ptr = tsLogDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "scriptDir";
  cfg.ptr = tsScriptDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "dataDir";
  cfg.ptr = tsDataDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "arbitrator";
  cfg.ptr = tsArbitrator;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_EP_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // dnode configs
  cfg.option = "numOfThreadsPerCore";
  cfg.ptr = &tsNumOfThreadsPerCore;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 10;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "ratioOfQueryThreads";
  cfg.ptr = &tsRatioOfQueryThreads;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0.1;
  cfg.maxValue = 0.9;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "numOfMnodes";
  cfg.ptr = &tsNumOfMnodes;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 3;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "vnodeBak";
  cfg.ptr = &tsEnableVnodeBak;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "balance";
  cfg.ptr = &tsEnableBalance;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "balanceInterval";
  cfg.ptr = &tsBalanceInterval;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 30000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // 0-any; 1-mnode; 2-vnode
  cfg.option = "role";
  cfg.ptr = &tsAlternativeRole;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 2;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // timer
  cfg.option = "maxTmrCtrl";
  cfg.ptr = &tsMaxTmrCtrl;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 8;
  cfg.maxValue = 2048;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "monitorInterval";
  cfg.ptr = &tsMonitorInterval;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 1;
  cfg.maxValue = 600;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosInitConfigOption(cfg);

  cfg.option = "offlineThreshold";
  cfg.ptr = &tsOfflineThreshold;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 5;
  cfg.maxValue = 7200000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosInitConfigOption(cfg);

  cfg.option = "rpcTimer";
  cfg.ptr = &tsRpcTimer;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 100;
  cfg.maxValue = 3000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosInitConfigOption(cfg);

  cfg.option = "rpcMaxTime";
  cfg.ptr = &tsRpcMaxTime;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 100;
  cfg.maxValue = 7200;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosInitConfigOption(cfg);

  cfg.option = "statusInterval";
  cfg.ptr = &tsStatusInterval;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 10;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosInitConfigOption(cfg);

  cfg.option = "shellActivityTimer";
  cfg.ptr = &tsShellActivityTimer;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 120;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosInitConfigOption(cfg);

  cfg.option = "tableMetaKeepTimer";
  cfg.ptr = &tsTableMetaKeepTimer;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 8640000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosInitConfigOption(cfg);

  cfg.option = "minSlidingTime";
  cfg.ptr = &tsMinSlidingTime;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 1000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosInitConfigOption(cfg);

  cfg.option = "minIntervalTime";
  cfg.ptr = &tsMinIntervalTime;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 1000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosInitConfigOption(cfg);

  cfg.option = "maxStreamCompDelay";
  cfg.ptr = &tsMaxStreamComputDelay;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 1000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosInitConfigOption(cfg);

  cfg.option = "maxFirstStreamCompDelay";
  cfg.ptr = &tsStreamCompStartDelay;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1000;
  cfg.maxValue = 1000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosInitConfigOption(cfg);

  cfg.option = "retryStreamCompDelay";
  cfg.ptr = &tsStreamCompRetryDelay;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 1000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;

  taosInitConfigOption(cfg);
  cfg.option = "streamCompDelayRatio";
  cfg.ptr = &tsStreamComputDelayRatio;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.1;
  cfg.maxValue = 0.9;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxVgroupsPerDb";
  cfg.ptr = &tsMaxVgroupsPerDb;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 8192;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // database configs
  cfg.option = "maxTablesPerVnode";
  cfg.ptr = &tsMaxTablePerVnode;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_TABLES;
  cfg.maxValue = TSDB_MAX_TABLES;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "minTablesPerVnode";
  cfg.ptr = &tsMinTablePerVnode;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_TABLES;
  cfg.maxValue = TSDB_MAX_TABLES;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "tableIncStepPerVnode";
  cfg.ptr = &tsTableIncStepPerVnode;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_TABLES;
  cfg.maxValue = TSDB_MAX_TABLES;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "cache";
  cfg.ptr = &tsCacheBlockSize;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_CACHE_BLOCK_SIZE;
  cfg.maxValue = TSDB_MAX_CACHE_BLOCK_SIZE;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_Mb;
  taosInitConfigOption(cfg);

  cfg.option = "blocks";
  cfg.ptr = &tsBlocksPerVnode;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_TOTAL_BLOCKS;
  cfg.maxValue = TSDB_MAX_TOTAL_BLOCKS;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "days";
  cfg.ptr = &tsDaysPerFile;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_DAYS_PER_FILE;
  cfg.maxValue = TSDB_MAX_DAYS_PER_FILE;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "keep";
  cfg.ptr = &tsDaysToKeep;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_KEEP;
  cfg.maxValue = TSDB_MAX_KEEP;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "minRows";
  cfg.ptr = &tsMinRowsInFileBlock;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_MIN_ROW_FBLOCK;
  cfg.maxValue = TSDB_MAX_MIN_ROW_FBLOCK;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxRows";
  cfg.ptr = &tsMaxRowsInFileBlock;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_MAX_ROW_FBLOCK;
  cfg.maxValue = TSDB_MAX_MAX_ROW_FBLOCK;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "comp";
  cfg.ptr = &tsCompression;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_COMP_LEVEL;
  cfg.maxValue = TSDB_MAX_COMP_LEVEL;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "walLevel";
  cfg.ptr = &tsWAL;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_WAL_LEVEL;
  cfg.maxValue = TSDB_MAX_WAL_LEVEL;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "fsync";
  cfg.ptr = &tsFsyncPeriod;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_FSYNC_PERIOD;
  cfg.maxValue = TSDB_MAX_FSYNC_PERIOD;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "replica";
  cfg.ptr = &tsReplications;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_DB_REPLICA_OPTION;
  cfg.maxValue = TSDB_MAX_DB_REPLICA_OPTION;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "mqttBrokerAddress";
  cfg.ptr = tsMqttBrokerAddress;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_NOT_PRINT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 126;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "mqttBrokerClientId";
  cfg.ptr = tsMqttBrokerClientId;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_NOT_PRINT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 126;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);
 
  cfg.option = "compressMsgSize";
  cfg.ptr = &tsCompressMsgSize;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = -1;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxSQLLength";
  cfg.ptr = &tsMaxSQLStringLen;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MAX_SQL_LEN;
  cfg.maxValue = TSDB_MAX_ALLOWED_SQL_LEN;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
  taosInitConfigOption(cfg);

  cfg.option = "maxNumOfOrderedRes";
  cfg.ptr = &tsMaxNumOfOrderedResults;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MAX_SQL_LEN;
  cfg.maxValue = TSDB_MAX_ALLOWED_SQL_LEN;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // locale & charset
  cfg.option = "timezone";
  cfg.ptr = tsTimezone;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = tListLen(tsTimezone);
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "locale";
  cfg.ptr = tsLocale;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = tListLen(tsLocale);
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "charset";
  cfg.ptr = tsCharset;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = tListLen(tsCharset);
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // connect configs
  cfg.option = "maxShellConns";
  cfg.ptr = &tsMaxShellConns;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 50000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxConnections";
  cfg.ptr = &tsMaxConnections;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 100000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "minimalLogDirGB";
  cfg.ptr = &tsMinimalLogDirGB;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.001;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_GB;
  taosInitConfigOption(cfg);

  cfg.option = "minimalTmpDirGB";
  cfg.ptr = &tsReservedTmpDirectorySpace;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.001;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_GB;
  taosInitConfigOption(cfg);

  cfg.option = "minimalDataDirGB";
  cfg.ptr = &tsMinimalDataDirGB;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.001;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_GB;
  taosInitConfigOption(cfg);

  // module configs
  cfg.option = "mnodeEqualVnodeNum";
  cfg.ptr = &tsMnodeEqualVnodeNum;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "http";
  cfg.ptr = &tsEnableHttpModule;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "mqtt";
  cfg.ptr = &tsEnableMqttModule;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "monitor";
  cfg.ptr = &tsEnableMonitorModule;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // http configs
  cfg.option = "httpCacheSessions";
  cfg.ptr = &tsHttpCacheSessions;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 1;
  cfg.maxValue = 100000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "httpEnableRecordSql";
  cfg.ptr = &tsHttpEnableRecordSql;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "telegrafUseFieldNum";
  cfg.ptr = &tsTelegrafUseFieldNum;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "httpMaxThreads";
  cfg.ptr = &tsHttpMaxThreads;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 1;
  cfg.maxValue = 1000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "restfulRowLimit";
  cfg.ptr = &tsRestRowLimit;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 1;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // debug flag
  cfg.option = "numOfLogLines";
  cfg.ptr = &tsNumOfLogLines;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 10000;
  cfg.maxValue = 2000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "asyncLog";
  cfg.ptr = &tsAsyncLog;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "debugFlag";
  cfg.ptr = &debugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "mDebugFlag";
  cfg.ptr = &mDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "dDebugFlag";
  cfg.ptr = &dDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "sDebugFlag";
  cfg.ptr = &sDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "wDebugFlag";
  cfg.ptr = &wDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);


  cfg.option = "sdbDebugFlag";
  cfg.ptr = &sdbDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "rpcDebugFlag";
  cfg.ptr = &rpcDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "tmrDebugFlag";
  cfg.ptr = &tmrDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "cDebugFlag";
  cfg.ptr = &cDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "jniDebugFlag";
  cfg.ptr = &jniDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "odbcDebugFlag";
  cfg.ptr = &odbcDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "uDebugFlag";
  cfg.ptr = &uDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "httpDebugFlag";
  cfg.ptr = &httpDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "mqttDebugFlag";
  cfg.ptr = &mqttDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "monitorDebugFlag";
  cfg.ptr = &monitorDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "qDebugFlag";
  cfg.ptr = &qDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "vDebugFlag";
  cfg.ptr = &vDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "tsdbDebugFlag";
  cfg.ptr = &tsdbDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "tscEnableRecordSql";
  cfg.ptr = &tsTscEnableRecordSql;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "enableCoreFile";
  cfg.ptr = &tsEnableCoreFile;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // version info
  cfg.option = "gitinfo";
  cfg.ptr = gitinfo;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "gitinfoOfInternal";
  cfg.ptr = gitinfoOfInternal;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "buildinfo";
  cfg.ptr = buildinfo;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "version";
  cfg.ptr = version;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxBinaryDisplayWidth";
  cfg.ptr = &tsMaxBinaryDisplayWidth;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 0x7fffffff;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);
}

void taosInitGlobalCfg() {
  pthread_once(&tsInitGlobalCfgOnce, doInitGlobalConfig);
}

bool taosCheckGlobalCfg() {
  if (debugFlag & DEBUG_TRACE || debugFlag & DEBUG_DEBUG || debugFlag & DEBUG_DUMP) {
    taosSetAllDebugFlag();
  }
  
  if (tsLocalFqdn[0] == 0) {
    taosGetFqdn(tsLocalFqdn);
  }

  snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%d", tsLocalFqdn, tsServerPort);
  uInfo("localEp is: %s", tsLocalEp);

  if (tsFirst[0] == 0) {
    strcpy(tsFirst, tsLocalEp);
  }

  if (tsSecond[0] == 0) {
    strcpy(tsSecond, tsLocalEp);
  }
  
  taosGetSystemInfo();

  tsSetLocale();

  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus == TAOS_CFG_CSTATUS_FILE) {
    tsSetTimeZone();
  }

  if (tsNumOfCores <= 0) {
    tsNumOfCores = 1;
  }

  // todo refactor
  tsVersion = 0;
  for (int i = 0; i < 10; i++) {
    if (version[i] >= '0' && version[i] <= '9') {
      tsVersion = tsVersion * 10 + (version[i] - '0');
    } else if (version[i] == 0) {
      break;
    }
  }
  
  tsVersion = 10 * tsVersion;

  tsDnodeShellPort = tsServerPort + TSDB_PORT_DNODESHELL;  // udp[6035-6039] tcp[6035]
  tsDnodeDnodePort = tsServerPort + TSDB_PORT_DNODEDNODE;   // udp/tcp
  tsSyncPort = tsServerPort + TSDB_PORT_SYNC;

  return true;
}

int taosGetFqdnPortFromEp(const char *ep, char *fqdn, uint16_t *port) {
  *port = 0;
  strcpy(fqdn, ep);

  char *temp = strchr(fqdn, ':');
  if (temp) {   
    *temp = 0;
    *port = atoi(temp+1);
  } 
  
  if (*port == 0) *port = tsServerPort;

  return 0; 
}

/*
 * alter dnode 1 balance "vnode:1-dnode:2"
 */

bool taosCheckBalanceCfgOptions(const char *option, int32_t *vnodeId, int32_t *dnodeId) {
  int len = strlen(option);
  if (strncasecmp(option, "vnode:", 6) != 0) {
    return false;
  }

  int pos = 0;
  for (; pos < len; ++pos) {
    if (option[pos] == '-') break;
  }

  if (++pos >= len) return false;
  if (strncasecmp(option + pos, "dnode:", 6) != 0) {
    return false;
  }

  *vnodeId = strtol(option + 6, NULL, 10);
  *dnodeId = strtol(option + pos + 6, NULL, 10);
  if (*vnodeId <= 1 || *dnodeId <= 0) {
    return false;
  }

  return true;
}