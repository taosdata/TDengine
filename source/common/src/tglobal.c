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
#include "tcompare.h"
#include "tconfig.h"
#include "tep.h"
#include "tglobal.h"
#include "tlocale.h"
#include "tlog.h"
#include "ttimezone.h"
#include "tutil.h"
#include "ulog.h"

// cluster
char     tsFirst[TSDB_EP_LEN] = {0};
char     tsSecond[TSDB_EP_LEN] = {0};
char     tsLocalFqdn[TSDB_FQDN_LEN] = {0};
char     tsLocalEp[TSDB_EP_LEN] = {0};  // Local End Point, hostname:port
uint16_t tsServerPort = 6030;
int32_t  tsStatusInterval = 1;  // second
int8_t   tsEnableTelemetryReporting = 0;
char     tsEmail[TSDB_FQDN_LEN] = {0};
int32_t  tsNumOfSupportVnodes = 128;

// common
int32_t tsRpcTimer = 300;
int32_t tsRpcMaxTime = 600;  // seconds;
int32_t tsRpcForceTcp = 1;   // disable this, means query, show command use udp protocol as default
int32_t tsMaxShellConns = 50000;
int32_t tsMaxConnections = 50000;
int32_t tsShellActivityTimer = 3;  // second
float   tsNumOfThreadsPerCore = 1.0f;
int32_t tsNumOfCommitThreads = 4;
float   tsRatioOfQueryCores = 1.0f;
int8_t  tsDaylight = 0;
int8_t  tsEnableCoreFile = 0;
int32_t tsMaxBinaryDisplayWidth = 30;
int8_t  tsEnableSlaveQuery = 1;
int8_t  tsEnableAdjustMaster = 1;
int8_t  tsPrintAuth = 0;
/*
 * denote if the server needs to compress response message at the application layer to client, including query rsp,
 * metricmeta rsp, and multi-meter query rsp message body. The client compress the submit message to server.
 *
 * 0: all data are compressed
 * -1: all data are not compressed
 * other values: if the message payload size is greater than the tsCompressMsgSize, the message will be compressed.
 */
int32_t tsCompressMsgSize = -1;

/* denote if server needs to compress the retrieved column data before adding to the rpc response message body.
 * 0: all data are compressed
 * -1: all data are not compressed
 * other values: if any retrieved column size is greater than the tsCompressColData, all data will be compressed.
 */
int32_t tsCompressColData = -1;

/*
 * denote if 3.0 query pattern compatible for 2.0
 */
int32_t tsCompatibleModel = 1;

// client
int32_t tsMaxSQLStringLen = TSDB_MAX_ALLOWED_SQL_LEN;
int32_t tsMaxWildCardsLen = TSDB_PATTERN_STRING_DEFAULT_LEN;
int32_t tsMaxRegexStringLen = TSDB_REGEX_STRING_DEFAULT_LEN;

int8_t tsTscEnableRecordSql = 0;

// the maximum number of results for projection query on super table that are returned from
// one virtual node, to order according to timestamp
int32_t tsMaxNumOfOrderedResults = 100000;

// 10 ms for sliding time, the value will changed in case of time precision changed
int32_t tsMinSlidingTime = 10;

// the maxinum number of distict query result
int32_t tsMaxNumOfDistinctResults = 1000 * 10000;

// 1 us for interval time range, changed accordingly
int32_t tsMinIntervalTime = 1;

// 20sec, the maximum value of stream computing delay, changed accordingly
int32_t tsMaxStreamComputDelay = 20000;

// 10sec, the first stream computing delay time after system launched successfully, changed accordingly
int32_t tsStreamCompStartDelay = 10000;

// the stream computing delay time after executing failed, change accordingly
int32_t tsRetryStreamCompDelay = 10 * 1000;

// The delayed computing ration. 10% of the whole computing time window by default.
float tsStreamComputDelayRatio = 0.1f;

int32_t tsProjectExecInterval = 10000;   // every 10sec, the projection will be executed once
int64_t tsMaxRetentWindow = 24 * 3600L;  // maximum time window tolerance

// the maximum allowed query buffer size during query processing for each data node.
// -1 no limit (default)
// 0  no query allowed, queries are disabled
// positive value (in MB)
int32_t tsQueryBufferSize = -1;
int64_t tsQueryBufferSizeBytes = -1;

// in retrieve blocking model, the retrieve threads will wait for the completion of the query processing.
int32_t tsRetrieveBlockingModel = 0;

// last_row(*), first(*), last_row(ts, col1, col2) query, the result fields will be the original column name
int8_t tsKeepOriginalColumnName = 0;

// long query death-lock
int8_t tsDeadLockKillQuery = 0;

// tsdb config
// For backward compatibility
bool tsdbForceKeepFile = false;

int32_t tsDiskCfgNum = 0;

#ifndef _STORAGE
SDiskCfg tsDiskCfg[1];
#else
SDiskCfg tsDiskCfg[TFS_MAX_DISKS];
#endif

/*
 * minimum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 86400000L
 *     TSDB_TIME_PRECISION_MICRO: 86400000000L
 *     TSDB_TIME_PRECISION_NANO:  86400000000000L
 */
int64_t tsTickPerDay[] = {86400000L, 86400000000L, 86400000000000L};

// system info
float    tsTotalTmpDirGB = 0;
float    tsTotalDataDirGB = 0;
float    tsAvailTmpDirectorySpace = 0;
float    tsAvailDataDirGB = 0;
float    tsUsedDataDirGB = 0;
float    tsReservedTmpDirectorySpace = 1.0f;
float    tsMinimalDataDirGB = 2.0f;
int32_t  tsTotalMemoryMB = 0;
uint32_t tsVersion = 0;

//
// lossy compress 6
//
char tsLossyColumns[32] = "";  // "float|double" means all float and double columns can be lossy compressed.  set empty
                               // can close lossy compress.
// below option can take effect when tsLossyColumns not empty
double   tsFPrecision = 1E-8;                   // float column precision
double   tsDPrecision = 1E-16;                  // double column precision
uint32_t tsMaxRange = 500;                      // max range
uint32_t tsCurRange = 100;                      // range
char     tsCompressor[32] = "ZSTD_COMPRESSOR";  // ZSTD_COMPRESSOR or GZIP_COMPRESSOR


int32_t (*monStartSystemFp)() = NULL;
void (*monStopSystemFp)() = NULL;
void (*monExecuteSQLFp)(char *sql) = NULL;

char *qtypeStr[] = {"rpc", "fwd", "wal", "cq", "query"};

static pthread_once_t tsInitGlobalCfgOnce = PTHREAD_ONCE_INIT;

void taosSetAllDebugFlag() {
  if (debugFlag != 0) {
    mDebugFlag = debugFlag;
    dDebugFlag = debugFlag;
    vDebugFlag = debugFlag;
    jniDebugFlag = debugFlag;
    qDebugFlag = debugFlag;
    rpcDebugFlag = debugFlag;
    uDebugFlag = debugFlag;
    sDebugFlag = debugFlag;
    wDebugFlag = debugFlag;
    tsdbDebugFlag = debugFlag;
    cqDebugFlag = debugFlag;
    uInfo("all debug flag are set to %d", debugFlag);
  }
}

int32_t taosCfgDynamicOptions(char *msg) {
  char   *option, *value;
  int32_t olen, vlen;
  int32_t vint = 0;

  paGetToken(msg, &option, &olen);
  if (olen == 0) return -1;

  paGetToken(option + olen + 1, &value, &vlen);
  if (vlen == 0)
    vint = 135;
  else {
    vint = atoi(value);
  }

  uInfo("change dynamic option: %s, value: %d", option, vint);

  for (int32_t i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    // if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_LOG)) continue;
    if (cfg->valType != TAOS_CFG_VTYPE_INT32 && cfg->valType != TAOS_CFG_VTYPE_INT8) continue;

    int32_t cfgLen = (int32_t)strlen(cfg->option);
    if (cfgLen != olen) continue;
    if (strncasecmp(option, cfg->option, olen) != 0) continue;
    if (cfg->valType == TAOS_CFG_VTYPE_INT32) {
      *((int32_t *)cfg->ptr) = vint;
    } else {
      *((int8_t *)cfg->ptr) = (int8_t)vint;
    }

    if (strncasecmp(cfg->option, "monitor", olen) == 0) {
      if (1 == vint) {
        if (monStartSystemFp) {
          (*monStartSystemFp)();
          uInfo("monitor is enabled");
        } else {
          uError("monitor can't be updated, for monitor not initialized");
        }
      } else {
        if (monStopSystemFp) {
          (*monStopSystemFp)();
          uInfo("monitor is disabled");
        } else {
          uError("monitor can't be updated, for monitor not initialized");
        }
      }
      return 0;
    }
    if (strncasecmp(cfg->option, "debugFlag", olen) == 0) {
      taosSetAllDebugFlag();
    }
    return 0;
  }

  if (strncasecmp(option, "resetlog", 8) == 0) {
    taosResetLog();
    taosPrintCfg();
    return 0;
  }

  if (strncasecmp(option, "resetQueryCache", 15) == 0) {
    if (monExecuteSQLFp) {
      (*monExecuteSQLFp)("resetQueryCache");
      uInfo("resetquerycache is executed");
    } else {
      uError("resetquerycache can't be executed, for monitor not started");
    }
  }

  return false;
}

void taosAddDataDir(int index, char *v1, int level, int primary) {
  tstrncpy(tsDiskCfg[index].dir, v1, TSDB_FILENAME_LEN);
  tsDiskCfg[index].level = level;
  tsDiskCfg[index].primary = primary;
  uTrace("dataDir:%s, level:%d primary:%d is configured", v1, level, primary);
}

#ifndef _STORAGE
void taosReadDataDirCfg(char *v1, char *v2, char *v3) {
  if (tsDiskCfgNum == 1) {
    SDiskCfg *cfg = &tsDiskCfg[0];
    uInfo("dataDir:%s, level:%d primary:%d is replaced by %s", cfg->dir, cfg->level, cfg->primary, v1);
  }
  taosAddDataDir(0, v1, 0, 1);
  tsDiskCfgNum = 1;
}

void taosPrintDataDirCfg() {
  for (int i = 0; i < tsDiskCfgNum; ++i) {
    SDiskCfg *cfg = &tsDiskCfg[i];
    uInfo(" dataDir: %s", cfg->dir);
  }
}
#endif

static void taosCheckDataDirCfg() {
  if (tsDiskCfgNum <= 0) {
    taosAddDataDir(0, tsDataDir, 0, 1);
    tsDiskCfgNum = 1;
    uTrace("dataDir:%s, level:0 primary:1 is configured by default", tsDataDir);
  }
}

static void doInitGlobalConfig(void) {
  osInit();
  srand(taosSafeRand());

  SGlobalCfg cfg = {0};

  // ip address
  cfg.option = "firstEp";
  cfg.ptr = tsFirst;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_EP_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "secondEp";
  cfg.ptr = tsSecond;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_EP_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "fqdn";
  cfg.ptr = tsLocalFqdn;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FQDN_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // port
  cfg.option = "serverPort";
  cfg.ptr = &tsServerPort;
  cfg.valType = TAOS_CFG_VTYPE_UINT16;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 65056;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "supportVnodes";
  cfg.ptr = &tsNumOfSupportVnodes;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 65536;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // directory
  cfg.option = "configDir";
  cfg.ptr = configDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "logDir";
  cfg.ptr = tsLogDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "scriptDir";
  cfg.ptr = tsScriptDir;
  cfg.valType = TAOS_CFG_VTYPE_DIRECTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "dataDir";
  cfg.ptr = tsDataDir;
  cfg.valType = TAOS_CFG_VTYPE_DATA_DIRCTORY;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_FILENAME_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // dnode configs
  cfg.option = "numOfThreadsPerCore";
  cfg.ptr = &tsNumOfThreadsPerCore;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 10;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "numOfCommitThreads";
  cfg.ptr = &tsNumOfCommitThreads;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 1;
  cfg.maxValue = 100;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "ratioOfQueryCores";
  cfg.ptr = &tsRatioOfQueryCores;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0.0f;
  cfg.maxValue = 2.0f;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "maxNumOfDistinctRes";
  cfg.ptr = &tsMaxNumOfDistinctResults;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 10 * 10000;
  cfg.maxValue = 10000 * 10000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "telemetryReporting";
  cfg.ptr = &tsEnableTelemetryReporting;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // timer
  cfg.option = "maxTmrCtrl";
  cfg.ptr = &tsMaxTmrCtrl;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 8;
  cfg.maxValue = 2048;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "rpcTimer";
  cfg.ptr = &tsRpcTimer;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 100;
  cfg.maxValue = 3000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosAddConfigOption(cfg);

  cfg.option = "rpcForceTcp";
  cfg.ptr = &tsRpcForceTcp;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "rpcMaxTime";
  cfg.ptr = &tsRpcMaxTime;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 100;
  cfg.maxValue = 7200;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosAddConfigOption(cfg);

  cfg.option = "statusInterval";
  cfg.ptr = &tsStatusInterval;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 10;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosAddConfigOption(cfg);

  cfg.option = "shellActivityTimer";
  cfg.ptr = &tsShellActivityTimer;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 120;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_SECOND;
  taosAddConfigOption(cfg);

  cfg.option = "minSlidingTime";
  cfg.ptr = &tsMinSlidingTime;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 1000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosAddConfigOption(cfg);

  cfg.option = "minIntervalTime";
  cfg.ptr = &tsMinIntervalTime;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 1000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosAddConfigOption(cfg);

  cfg.option = "maxStreamCompDelay";
  cfg.ptr = &tsMaxStreamComputDelay;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 1000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosAddConfigOption(cfg);

  cfg.option = "maxFirstStreamCompDelay";
  cfg.ptr = &tsStreamCompStartDelay;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1000;
  cfg.maxValue = 1000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;
  taosAddConfigOption(cfg);

  cfg.option = "retryStreamCompDelay";
  cfg.ptr = &tsRetryStreamCompDelay;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 1000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_MS;

  taosAddConfigOption(cfg);
  cfg.option = "streamCompDelayRatio";
  cfg.ptr = &tsStreamComputDelayRatio;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.1f;
  cfg.maxValue = 0.9f;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "compressMsgSize";
  cfg.ptr = &tsCompressMsgSize;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = -1;
  cfg.maxValue = 100000000.0f;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "compressColData";
  cfg.ptr = &tsCompressColData;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = -1;
  cfg.maxValue = 100000000.0f;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "maxSQLLength";
  cfg.ptr = &tsMaxSQLStringLen;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MAX_SQL_LEN;
  cfg.maxValue = TSDB_MAX_ALLOWED_SQL_LEN;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
  taosAddConfigOption(cfg);

  cfg.option = "maxWildCardsLength";
  cfg.ptr = &tsMaxWildCardsLen;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = TSDB_MAX_FIELD_LEN;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
  taosAddConfigOption(cfg);

  cfg.option = "maxRegexStringLen";
  cfg.ptr = &tsMaxRegexStringLen;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = TSDB_MAX_FIELD_LEN;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
  taosAddConfigOption(cfg);

  cfg.option = "maxNumOfOrderedRes";
  cfg.ptr = &tsMaxNumOfOrderedResults;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = TSDB_MAX_SQL_LEN;
  cfg.maxValue = TSDB_MAX_ALLOWED_SQL_LEN;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "queryBufferSize";
  cfg.ptr = &tsQueryBufferSize;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = -1;
  cfg.maxValue = 500000000000.0f;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_BYTE;
  taosAddConfigOption(cfg);

  cfg.option = "retrieveBlockingModel";
  cfg.ptr = &tsRetrieveBlockingModel;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "keepColumnName";
  cfg.ptr = &tsKeepOriginalColumnName;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // locale & charset
  cfg.option = "timezone";
  cfg.ptr = tsTimezone;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_TIMEZONE_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "locale";
  cfg.ptr = tsLocale;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_LOCALE_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "charset";
  cfg.ptr = tsCharset;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = TSDB_LOCALE_LEN;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // connect configs
  cfg.option = "maxShellConns";
  cfg.ptr = &tsMaxShellConns;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 10;
  cfg.maxValue = 50000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "maxConnections";
  cfg.ptr = &tsMaxConnections;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 1;
  cfg.maxValue = 100000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "minimalLogDirGB";
  cfg.ptr = &tsMinimalLogDirGB;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.001f;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_GB;
  taosAddConfigOption(cfg);

  cfg.option = "minimalTmpDirGB";
  cfg.ptr = &tsReservedTmpDirectorySpace;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.001f;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_GB;
  taosAddConfigOption(cfg);

  cfg.option = "minimalDataDirGB";
  cfg.ptr = &tsMinimalDataDirGB;
  cfg.valType = TAOS_CFG_VTYPE_FLOAT;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0.001f;
  cfg.maxValue = 10000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_GB;
  taosAddConfigOption(cfg);

  cfg.option = "slaveQuery";
  cfg.ptr = &tsEnableSlaveQuery;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // debug flag
  cfg.option = "numOfLogLines";
  cfg.ptr = &tsNumOfLogLines;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1000;
  cfg.maxValue = 2000000000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "logKeepDays";
  cfg.ptr = &tsLogKeepDays;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = -365000;
  cfg.maxValue = 365000;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "asyncLog";
  cfg.ptr = &tsAsyncLog;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "debugFlag";
  cfg.ptr = &debugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "mDebugFlag";
  cfg.ptr = &mDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "dDebugFlag";
  cfg.ptr = &dDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "sDebugFlag";
  cfg.ptr = &sDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "wDebugFlag";
  cfg.ptr = &wDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "rpcDebugFlag";
  cfg.ptr = &rpcDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "tmrDebugFlag";
  cfg.ptr = &tmrDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "cDebugFlag";
  cfg.ptr = &cDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "jniDebugFlag";
  cfg.ptr = &jniDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "uDebugFlag";
  cfg.ptr = &uDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "qDebugFlag";
  cfg.ptr = &qDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "vDebugFlag";
  cfg.ptr = &vDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "tsdbDebugFlag";
  cfg.ptr = &tsdbDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "cqDebugFlag";
  cfg.ptr = &cqDebugFlag;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG;
  cfg.minValue = 0;
  cfg.maxValue = 255;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "enableRecordSql";
  cfg.ptr = &tsTscEnableRecordSql;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "enableCoreFile";
  cfg.ptr = &tsEnableCoreFile;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // version info
  cfg.option = "gitinfo";
  cfg.ptr = gitinfo;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "gitinfoOfInternal";
  cfg.ptr = gitinfoOfInternal;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "buildinfo";
  cfg.ptr = buildinfo;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "version";
  cfg.ptr = version;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "maxBinaryDisplayWidth";
  cfg.ptr = &tsMaxBinaryDisplayWidth;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 1;
  cfg.maxValue = 65536;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "tempDir";
  cfg.ptr = tsTempDir;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = PATH_MAX;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  // enable kill long query
  cfg.option = "deadLockKillQuery";
  cfg.ptr = &tsDeadLockKillQuery;
  cfg.valType = TAOS_CFG_VTYPE_INT8;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW;
  cfg.minValue = 0;
  cfg.maxValue = 1;
  cfg.ptrLength = 1;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

#ifdef TD_TSZ
  // lossy compress
  cfg.option = "lossyColumns";
  cfg.ptr = lossyColumns;
  cfg.valType = TAOS_CFG_VTYPE_STRING;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 0;
  cfg.ptrLength = tListLen(lossyColumns);
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "fPrecision";
  cfg.ptr = &fPrecision;
  cfg.valType = TAOS_CFG_VTYPE_DOUBLE;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = MIN_FLOAT;
  cfg.maxValue = MAX_FLOAT;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;

  taosAddConfigOption(cfg);

  cfg.option = "dPrecision";
  cfg.ptr = &dPrecision;
  cfg.valType = TAOS_CFG_VTYPE_DOUBLE;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = MIN_FLOAT;
  cfg.maxValue = MAX_FLOAT;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "maxRange";
  cfg.ptr = &maxRange;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 65536;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);

  cfg.option = "range";
  cfg.ptr = &curRange;
  cfg.valType = TAOS_CFG_VTYPE_INT32;
  cfg.cfgType = TSDB_CFG_CTYPE_B_CONFIG;
  cfg.minValue = 0;
  cfg.maxValue = 65536;
  cfg.ptrLength = 0;
  cfg.unitType = TAOS_CFG_UTYPE_NONE;
  taosAddConfigOption(cfg);
  assert(tsGlobalConfigNum == TSDB_CFG_MAX_NUM);
#else
  // assert(tsGlobalConfigNum == TSDB_CFG_MAX_NUM - 5);
#endif
}

void taosInitGlobalCfg() { pthread_once(&tsInitGlobalCfgOnce, doInitGlobalConfig); }

int32_t taosCheckAndPrintCfg() {
  SEp ep = {0};
  if (debugFlag & DEBUG_TRACE || debugFlag & DEBUG_DEBUG || debugFlag & DEBUG_DUMP) {
    taosSetAllDebugFlag();
  }

  if (tsLocalFqdn[0] == 0) {
    taosGetFqdn(tsLocalFqdn);
  }

  snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%u", tsLocalFqdn, tsServerPort);
  uInfo("localEp is: %s", tsLocalEp);

  if (tsFirst[0] == 0) {
    strcpy(tsFirst, tsLocalEp);
  } else {
    taosGetFqdnPortFromEp(tsFirst, &ep);
    snprintf(tsFirst, sizeof(tsFirst), "%s:%u", ep.fqdn, ep.port);
  }

  if (tsSecond[0] == 0) {
    strcpy(tsSecond, tsLocalEp);
  } else {
    taosGetFqdnPortFromEp(tsSecond, &ep);
    snprintf(tsSecond, sizeof(tsSecond), "%s:%u", ep.fqdn, ep.port);
  }

  taosCheckDataDirCfg();

  if (taosDirExist(tsTempDir) != 0) {
    return -1;
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

  if (tsQueryBufferSize >= 0) {
    tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
  }

  uInfo("   check global cfg completed");
  uInfo("==================================");
  taosPrintCfg();

  return 0;
}

/*
 * alter dnode 1 balance "vnode:1-dnode:2"
 */

bool taosCheckBalanceCfgOptions(const char *option, int32_t *vnodeId, int32_t *dnodeId) {
  int len = (int)strlen(option);
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
