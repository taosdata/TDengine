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

char configDir[TSDB_FILENAME_LEN] = "/etc/taos";
char tsVnodeDir[TSDB_FILENAME_LEN] = {0};
char tsDnodeDir[TSDB_FILENAME_LEN] = {0};
char tsMnodeDir[TSDB_FILENAME_LEN] = {0};
char tsDataDir[TSDB_FILENAME_LEN] = "/var/lib/taos";
char tsScriptDir[TSDB_FILENAME_LEN] = "/etc/taos";
char tsOsName[10] = "Linux";

// system info, not configurable
int64_t tsPageSize;
int64_t tsOpenMax;
int64_t tsStreamMax;
int32_t tsNumOfCores = 1;
int32_t tsAlternativeRole = 0;
float   tsTotalTmpDirGB = 0;
float   tsTotalDataDirGB = 0;
float   tsAvailTmpDirGB = 0;
float   tsAvailDataDirGB = 0;
float   tsMinimalTmpDirGB = 0.1;
float   tsMinimalDataDirGB = 0.5;
int32_t tsTotalMemoryMB = 0;
int32_t tsVersion = 0;
int32_t tsEnableCoreFile = 0;

// global, not configurable
int32_t tscEmbedded = 0;

/*
 * minmum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 86400000L
 *     TSDB_TIME_PRECISION_MICRO: 86400000000L
 */
int64_t tsMsPerDay[] = {86400000L, 86400000000L};

char  tsFirst[TSDB_EP_LEN] = {0};  
char  tsSecond[TSDB_EP_LEN] = {0};
char  tsArbitrator[TSDB_EP_LEN] = {0};
char  tsLocalEp[TSDB_EP_LEN] = {0};  // Local End Point, hostname:port
uint16_t tsServerPort = 6030;
uint16_t tsDnodeShellPort = 6030;  // udp[6035-6039] tcp[6035]
uint16_t tsDnodeDnodePort = 6035;   // udp/tcp
uint16_t tsSyncPort = 6040;

int32_t tsStatusInterval = 1;         // second
int32_t tsShellActivityTimer = 3;     // second
int32_t tsTableMetaKeepTimer = 7200;  // second
int32_t tsRpcTimer = 300;
int32_t tsRpcMaxTime = 600;      // seconds;

float   tsNumOfThreadsPerCore = 1.0;
float   tsRatioOfQueryThreads = 0.5;
int16_t tsNumOfVnodesPerCore = 8;
int16_t tsNumOfTotalVnodes = TSDB_INVALID_VNODE_NUM;

#ifdef _TD_ARM_32_
int32_t tsMaxTablePerVnode = 100;
#else
int32_t tsMaxTablePerVnode = TSDB_DEFAULT_TABLES;
#endif

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
int32_t tsReplications  = TSDB_DEFAULT_REPLICA_NUM;

/**
 * Change the meaning of affected rows:
 * 0: affected rows not include those duplicate records
 * 1: affected rows include those duplicate records
 */
int16_t tsAffectedRowsMod = 0;
int32_t tsNumOfMPeers = 3;
int32_t tsMaxShellConns = 2000;
int32_t tsMaxTables = 100000;

char    tsDefaultDB[TSDB_DB_NAME_LEN] = {0};
char    tsDefaultUser[64] = "root";
char    tsDefaultPass[64] = "taosdata";
int32_t tsMaxMeterConnections = 10000;
int32_t tsMaxMgmtConnections = 2000;
int32_t tsMaxVnodeConnections = 10000;

int32_t tsBalanceInterval = 300;  // seconds
int32_t tsOfflineThreshold = 86400*100;   // seconds 10days
int32_t tsMgmtEqualVnodeNum = 4;

int32_t tsEnableHttpModule = 1;
int32_t tsEnableMonitorModule = 0;

int32_t tsRestRowLimit = 10240;
int32_t tsMaxSQLStringLen = TSDB_MAX_SQL_LEN;

int32_t tsNumOfLogLines = 10000000;
int32_t mDebugFlag = 135;
int32_t sdbDebugFlag = 135;
int32_t dDebugFlag = 135;
int32_t vDebugFlag = 135;
int32_t cDebugFlag = 135;
int32_t jniDebugFlag = 131;
int32_t odbcDebugFlag = 131;
int32_t httpDebugFlag = 131;
int32_t monitorDebugFlag = 131;
int32_t qDebugFlag = 131;
int32_t rpcDebugFlag = 135;
int32_t uDebugFlag = 131;
int32_t debugFlag = 131;
int32_t sDebugFlag = 135;

// the maximum number of results for projection query on super table that are returned from
// one virtual node, to order according to timestamp
int32_t tsMaxNumOfOrderedResults = 100000;

/*
 * denote if the server needs to compress response message at the application layer to client, including query rsp,
 * metricmeta rsp, and multi-meter query rsp message body. The client compress the submit message to server.
 *
 * 0: all data are compressed
 * -1: all data are not compressed
 * other values: if the message payload size is greater than the tsCompressMsgSize, the message will be compressed.
 */
int32_t tsCompressMsgSize = -1;

// use UDP by default[option: udp, tcp]
char tsSocketType[4] = "udp";

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

uint16_t tsHttpPort = 6020;                 // only tcp, range tcp[6020]
// uint16_t tsNginxPort = 6060;             //only tcp, range tcp[6060]
int32_t tsHttpCacheSessions = 100;
int32_t tsHttpSessionExpire = 36000;
int32_t tsHttpMaxThreads = 2;
int32_t tsHttpEnableCompress = 0;
int32_t tsHttpEnableRecordSql = 0;
int32_t tsTelegrafUseFieldNum = 0;

int32_t  tsTscEnableRecordSql = 0;
uint32_t tsPublicIpInt = 0;

char tsMonitorDbName[TSDB_DB_NAME_LEN] = "log";
char tsInternalPass[] = "secretkey";
int32_t tsMonitorInterval = 30;  // seconds

char tsTimezone[64] = {0};
char tsLocale[TSDB_LOCALE_LEN] = {0};
char tsCharset[TSDB_LOCALE_LEN] = {0};  // default encode string

static pthread_once_t tsInitGlobalCfgOnce = PTHREAD_ONCE_INIT;

void taosSetAllDebugFlag() {
  for (int32_t i = 0; i < tsGlobalConfigNum; ++i) {
    mDebugFlag = debugFlag;
    sdbDebugFlag = debugFlag;
    dDebugFlag = debugFlag;
    vDebugFlag = debugFlag;
    cDebugFlag = debugFlag;
    jniDebugFlag = debugFlag;
    odbcDebugFlag = debugFlag;
    httpDebugFlag = debugFlag;
    monitorDebugFlag = debugFlag;
    rpcDebugFlag = debugFlag;
    uDebugFlag = debugFlag;
    sDebugFlag = debugFlag;
    //qDebugFlag = debugFlag;    
  }
  uPrint("all debug flag are set to %d", debugFlag);
}

bool taosCfgDynamicOptions(char *msg) {
  char *option, *value;
  int32_t   olen, vlen;
  int32_t   vint = 0;

  paGetToken(msg, &option, &olen);
  if (olen == 0) return TSDB_CODE_INVALID_MSG_CONTENT;

  paGetToken(option + olen + 1, &value, &vlen);
  if (vlen == 0)
    vint = 135;
  else {
    vint = atoi(value);
  }

  uPrint("change dynamic option: %s, value: %d", option, vint);

  for (int32_t i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_LOG)) continue;
    if (cfg->valType != TAOS_CFG_VTYPE_INT32) continue;
    if (strncasecmp(option, cfg->option, olen) != 0) continue;
    *((int32_t *)cfg->ptr) = vint;

    if (strncasecmp(cfg->option, "monitor", olen) == 0) {
      // if (0 == vint) {
      //   monitorStartSystem();
      // } else {
      //   monitorStopSystem();
      // }
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
    uError("reset query cache can't be executed, for monitor not initialized");
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

  cfg.option = "numOfVnodesPerCore";
  cfg.ptr = &tsNumOfVnodesPerCore;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 64;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "numOfTotalVnodes";
  cfg.ptr = &tsNumOfTotalVnodes;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = TSDB_MAX_VNODES;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "numOfMPeers";
  cfg.ptr = &tsNumOfMPeers;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 3;
  cfg.ptrLength = 0;
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

  // 0-any; 1-mgmt; 2-dnode
  cfg.option = "alternativeRole";
  cfg.ptr = &tsAlternativeRole;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 2;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "affectedRowsMod";
  cfg.ptr = &tsAffectedRowsMod;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // timer
  cfg.option = "maxTmrCtrl";
  cfg.ptr = &taosMaxTmrCtrl;
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
  cfg.minValue = 10;
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

  // database configs
  cfg.option = "maxtablesPerVnode";
  cfg.ptr = &tsMaxTablePerVnode;
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
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
  taosInitConfigOption(cfg);

  cfg.option = "blocks";
  cfg.ptr = &tsBlocksPerVnode;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_TOTAL_BLOCKS;
  cfg.maxValue = TSDB_MAX_TOTAL_BLOCKS;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
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

  cfg.option = "ctime";
  cfg.ptr = &tsCommitTime;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_COMMIT_TIME;
  cfg.maxValue = TSDB_MAX_COMMIT_TIME;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
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

  cfg.option = "wallevel";
  cfg.ptr = &tsWAL;
  cfg.valType = TAOS_CFG_VTYPE_INT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_WAL_LEVEL;
  cfg.maxValue = TSDB_MAX_WAL_LEVEL;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "replica";
  cfg.ptr = &tsReplications;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MIN_REPLICA_NUM;
  cfg.maxValue = TSDB_MAX_REPLICA_NUM;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // login configs
  cfg.option = "defaultDB";
  cfg.ptr = tsDefaultDB;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_DB_NAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "defaultUser";
  cfg.ptr = tsDefaultUser;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_USER_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "defaultPass";
  cfg.ptr = tsDefaultPass;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_NOT_PRINT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_PASSWORD_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  // socket type; udp by default
  cfg.option = "sockettype";
  cfg.ptr = tsSocketType;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 3;
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

  cfg.option = "maxMeterConnections";
  cfg.ptr = &tsMaxMeterConnections;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 50000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxMgmtConnections";
  cfg.ptr = &tsMaxMgmtConnections;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 50000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "maxVnodeConnections";
  cfg.ptr = &tsMaxVnodeConnections;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 50000000;
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
  cfg.ptr = &tsMinimalTmpDirGB;
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
  cfg.option = "mgmtEqualVnodeNum";
  cfg.ptr = &tsMgmtEqualVnodeNum;
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

  cfg.option = "monitor";
  cfg.ptr = &tsEnableMonitorModule;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosInitConfigOption(cfg);

  cfg.option = "monitorDbName";
  cfg.ptr = tsMonitorDbName;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_DB_NAME_LEN;
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

  cfg.option = "httpEnableCompress";
  cfg.ptr = &tsHttpEnableCompress;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
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
}

void taosInitGlobalCfg() {
  pthread_once(&tsInitGlobalCfgOnce, doInitGlobalConfig);
}

bool taosCheckGlobalCfg() {
  if (debugFlag == 135 || debugFlag == 199) {
    taosSetAllDebugFlag();
  }
  
  taosGetFqdn(tsLocalEp);
  sprintf(tsLocalEp + strlen(tsLocalEp), ":%d", tsServerPort);
  uPrint("localEp is %s", tsLocalEp);

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

  if (tsNumOfTotalVnodes == TSDB_INVALID_VNODE_NUM) {
    tsNumOfTotalVnodes = tsNumOfCores * tsNumOfVnodesPerCore;
    tsNumOfTotalVnodes = tsNumOfTotalVnodes > TSDB_MAX_VNODES ? TSDB_MAX_VNODES : tsNumOfTotalVnodes;
    tsNumOfTotalVnodes = tsNumOfTotalVnodes < TSDB_MIN_VNODES ? TSDB_MIN_VNODES : tsNumOfTotalVnodes;     
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
