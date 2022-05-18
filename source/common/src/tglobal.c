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
#include "tglobal.h"
#include "tcompare.h"
#include "tconfig.h"
#include "tdatablock.h"
#include "tlog.h"

SConfig *tsCfg = NULL;

// cluster
char     tsFirst[TSDB_EP_LEN] = {0};
char     tsSecond[TSDB_EP_LEN] = {0};
char     tsLocalFqdn[TSDB_FQDN_LEN] = {0};
char     tsLocalEp[TSDB_EP_LEN] = {0};  // Local End Point, hostname:port
uint16_t tsServerPort = 6030;
int32_t  tsVersion = 30000000;
int32_t  tsStatusInterval = 1;  // second
int32_t  tsNumOfSupportVnodes = 256;

// common
int32_t tsMaxShellConns = 50000;
int32_t tsShellActivityTimer = 3;  // second
bool    tsEnableSlaveQuery = true;
bool    tsPrintAuth = false;

// multi process
int32_t tsMultiProcess = 0;
int32_t tsMnodeShmSize = TSDB_MAX_WAL_SIZE * 2 + 128;
int32_t tsVnodeShmSize = TSDB_MAX_WAL_SIZE * 10 + 128;
int32_t tsQnodeShmSize = TSDB_MAX_WAL_SIZE * 4 + 128;
int32_t tsSnodeShmSize = TSDB_MAX_WAL_SIZE * 4 + 128;
int32_t tsBnodeShmSize = TSDB_MAX_WAL_SIZE * 4 + 128;
int32_t tsNumOfShmThreads = 1;

// queue & threads
int32_t tsNumOfRpcThreads = 1;
int32_t tsNumOfCommitThreads = 2;
int32_t tsNumOfTaskQueueThreads = 1;
int32_t tsNumOfMnodeQueryThreads = 1;
int32_t tsNumOfMnodeReadThreads = 1;
int32_t tsNumOfVnodeQueryThreads = 2;
int32_t tsNumOfVnodeFetchThreads = 2;
int32_t tsNumOfVnodeWriteThreads = 2;
int32_t tsNumOfVnodeSyncThreads = 2;
int32_t tsNumOfVnodeMergeThreads = 2;
int32_t tsNumOfQnodeQueryThreads = 2;
int32_t tsNumOfQnodeFetchThreads = 2;
int32_t tsNumOfSnodeSharedThreads = 2;
int32_t tsNumOfSnodeUniqueThreads = 2;

// monitor
bool     tsEnableMonitor = true;
int32_t  tsMonitorInterval = 30;
char     tsMonitorFqdn[TSDB_FQDN_LEN] = {0};
uint16_t tsMonitorPort = 6043;
int32_t  tsMonitorMaxLogs = 100;
bool     tsMonitorComp = false;

// telem
bool     tsEnableTelem = false;
int32_t  tsTelemInterval = 86400;
char     tsTelemServer[TSDB_FQDN_LEN] = "telemetry.taosdata.com";
uint16_t tsTelemPort = 80;

// schemaless
char tsSmlChildTableName[TSDB_TABLE_NAME_LEN] = ""; //user defined child table name can be specified in tag value.
                                                     //If set to empty system will generate table name using MD5 hash.
bool tsSmlDataFormat = true;  // true means that the name and order of cols in each line are the same(only for influx protocol)

// query
int32_t tsQueryPolicy = 1;

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
bool tsRetrieveBlockingModel = false;

// last_row(*), first(*), last_row(ts, col1, col2) query, the result fields will be the original column name
bool tsKeepOriginalColumnName = false;

// kill long query
bool tsDeadLockKillQuery = false;

// tsdb config
// For backward compatibility
bool tsdbForceKeepFile = false;

int32_t  tsDiskCfgNum = 0;
SDiskCfg tsDiskCfg[TFS_MAX_DISKS] = {0};

// stream scheduler
bool tsStreamSchedV = true;

/*
 * minimum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 60000L
 *     TSDB_TIME_PRECISION_MICRO: 60000000L
 *     TSDB_TIME_PRECISION_NANO:  60000000000L
 */
int64_t tsTickPerMin[] = {60000L, 60000000L, 60000000000L};

// lossy compress 6
char tsLossyColumns[32] = "";  // "float|double" means all float and double columns can be lossy compressed.  set empty
                               // can close lossy compress.
// below option can take effect when tsLossyColumns not empty
double   tsFPrecision = 1E-8;                   // float column precision
double   tsDPrecision = 1E-16;                  // double column precision
uint32_t tsMaxRange = 500;                      // max range
uint32_t tsCurRange = 100;                      // range
char     tsCompressor[32] = "ZSTD_COMPRESSOR";  // ZSTD_COMPRESSOR or GZIP_COMPRESSOR

// udf
bool tsStartUdfd = true;

// internal
int32_t tsTransPullupInterval = 6;
int32_t tsMqRebalanceInterval = 2;

void taosAddDataDir(int32_t index, char *v1, int32_t level, int32_t primary) {
  tstrncpy(tsDiskCfg[index].dir, v1, TSDB_FILENAME_LEN);
  tsDiskCfg[index].level = level;
  tsDiskCfg[index].primary = primary;
  uTrace("dataDir:%s, level:%d primary:%d is configured", v1, level, primary);
}

static int32_t taosSetTfsCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  memset(tsDataDir, 0, PATH_MAX);

  int32_t size = taosArrayGetSize(pItem->array);
  if (size <= 0) {
    tsDiskCfgNum = 1;
    taosAddDataDir(0, pItem->str, 0, 1);
    tstrncpy(tsDataDir, pItem->str, PATH_MAX);
    if (taosMulMkDir(tsDataDir) != 0) {
      uError("failed to create dataDir:%s since %s", tsDataDir, terrstr());
      return -1;
    }
  } else {
    tsDiskCfgNum = size < TFS_MAX_DISKS ? size : TFS_MAX_DISKS;
    for (int32_t index = 0; index < tsDiskCfgNum; ++index) {
      SDiskCfg *pCfg = taosArrayGet(pItem->array, index);
      memcpy(&tsDiskCfg[index], pCfg, sizeof(SDiskCfg));
      if (pCfg->level == 0 && pCfg->primary == 1) {
        tstrncpy(tsDataDir, pCfg->dir, PATH_MAX);
        if (taosMulMkDir(tsDataDir) != 0) {
          uError("failed to create dataDir:%s since %s", tsDataDir, terrstr());
          return -1;
        }
      }
      if (taosMulMkDir(pCfg->dir) != 0) {
        uError("failed to create tfsDir:%s since %s", tsDataDir, terrstr());
        return -1;
      }
    }
  }

  if (tsDataDir[0] == 0) {
    uError("datadir not set");
    return -1;
  }

  return 0;
}

struct SConfig *taosGetCfg() {
  return tsCfg;
}

static int32_t taosLoadCfg(SConfig *pCfg, const char **envCmd, const char *inputCfgDir, const char *envFile,
                           char *apolloUrl) {
  char cfgDir[PATH_MAX] = {0};
  char cfgFile[PATH_MAX + 100] = {0};

  taosExpandDir(inputCfgDir, cfgDir, PATH_MAX);
  if (taosIsDir(cfgDir)) {
    snprintf(cfgFile, sizeof(cfgFile), "%s" TD_DIRSEP "taos.cfg", cfgDir);
  } else {
    tstrncpy(cfgFile, cfgDir, sizeof(cfgDir));
  }

  if (apolloUrl == NULL || apolloUrl[0] == '\0') cfgGetApollUrl(envCmd, envFile, apolloUrl);

  if (cfgLoad(pCfg, CFG_STYPE_APOLLO_URL, apolloUrl) != 0) {
    uError("failed to load from apollo url:%s since %s", apolloUrl, terrstr());
    return -1;
  }

  if (cfgLoad(pCfg, CFG_STYPE_CFG_FILE, cfgFile) != 0) {
    uError("failed to load from cfg file:%s since %s", cfgFile, terrstr());
    return -1;
  }

  if (cfgLoad(pCfg, CFG_STYPE_ENV_FILE, envFile) != 0) {
    uError("failed to load from env file:%s since %s", envFile, terrstr());
    return -1;
  }

  if (cfgLoad(pCfg, CFG_STYPE_ENV_VAR, NULL) != 0) {
    uError("failed to load from global env variables since %s", terrstr());
    return -1;
  }

  if (cfgLoad(pCfg, CFG_STYPE_ENV_CMD, envCmd) != 0) {
    uError("failed to load from cmd env variables since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t taosAddClientLogCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "configDir", configDir, 1) != 0) return -1;
  if (cfgAddDir(pCfg, "scriptDir", configDir, 1) != 0) return -1;
  if (cfgAddDir(pCfg, "logDir", tsLogDir, 1) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalLogDirGB", 1.0f, 0.001f, 10000000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfLogLines", tsNumOfLogLines, 1000, 2000000000, 1) != 0) return -1;
  if (cfgAddBool(pCfg, "asyncLog", tsAsyncLog, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "logKeepDays", 0, -365000, 365000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "cDebugFlag", cDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "uDebugFlag", uDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcDebugFlag", rpcDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "qDebugFlag", qDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "tmrDebugFlag", tmrDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "jniDebugFlag", jniDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "simDebugFlag", 143, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "debugFlag", 0, 0, 255, 1) != 0) return -1;
  return 0;
}

static int32_t taosAddServerLogCfg(SConfig *pCfg) {
  if (cfgAddInt32(pCfg, "dDebugFlag", dDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "vDebugFlag", vDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "mDebugFlag", mDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "qDebugFlag", qDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "wDebugFlag", wDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "sDebugFlag", sDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "tsdbDebugFlag", tsdbDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "tqDebugFlag", tqDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "fsDebugFlag", fsDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "fnDebugFlag", fnDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "smaDebugFlag", smaDebugFlag, 0, 255, 0) != 0) return -1;
  return 0;
}

static int32_t taosAddClientCfg(SConfig *pCfg) {
  char    defaultFqdn[TSDB_FQDN_LEN] = {0};
  int32_t defaultServerPort = 6030;
  if (taosGetFqdn(defaultFqdn) != 0) return -1;

  if (cfgAddString(pCfg, "firstEp", "", 1) != 0) return -1;
  if (cfgAddString(pCfg, "secondEp", "", 1) != 0) return -1;
  if (cfgAddString(pCfg, "fqdn", defaultFqdn, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "serverPort", defaultServerPort, 1, 65056, 1) != 0) return -1;
  if (cfgAddDir(pCfg, "tempDir", tsTempDir, 1) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalTempDirGB", 1.0f, 0.001f, 10000000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "shellActivityTimer", tsShellActivityTimer, 1, 120, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "compressMsgSize", tsCompressMsgSize, -1, 100000000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "compressColData", tsCompressColData, -1, 100000000, 1) != 0) return -1;
  if (cfgAddBool(pCfg, "keepColumnName", tsKeepOriginalColumnName, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "queryPolicy", tsQueryPolicy, 1, 3, 1) != 0) return -1;
  if (cfgAddString(pCfg, "smlChildTableName", "", 1) != 0) return -1;
  if (cfgAddBool(pCfg, "smlDataFormat", tsSmlDataFormat, 1) != 0) return -1;

  tsNumOfTaskQueueThreads = tsNumOfCores / 4;
  tsNumOfTaskQueueThreads = TRANGE(tsNumOfTaskQueueThreads, 1, 2);
  if (cfgAddInt32(pCfg, "numOfTaskQueueThreads", tsNumOfTaskQueueThreads, 1, 1024, 0) != 0) return -1;

  return 0;
}

static int32_t taosAddSystemCfg(SConfig *pCfg) {
  SysNameInfo info = taosGetSysNameInfo();

  if (cfgAddTimezone(pCfg, "timezone", tsTimezoneStr) != 0) return -1;
  if (cfgAddLocale(pCfg, "locale", tsLocale) != 0) return -1;
  if (cfgAddCharset(pCfg, "charset", tsCharset) != 0) return -1;
  if (cfgAddBool(pCfg, "enableCoreFile", 1, 1) != 0) return -1;
  if (cfgAddFloat(pCfg, "numOfCores", tsNumOfCores, 0, 100000, 1) != 0) return -1;
  if (cfgAddInt64(pCfg, "openMax", tsOpenMax, 0, INT64_MAX, 1) != 0) return -1;
  if (cfgAddInt64(pCfg, "streamMax", tsStreamMax, 0, INT64_MAX, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "pageSizeKB", tsPageSizeKB, 0, INT64_MAX, 1) != 0) return -1;
  if (cfgAddInt64(pCfg, "totalMemoryKB", tsTotalMemoryKB, 0, INT64_MAX, 1) != 0) return -1;
  if (cfgAddString(pCfg, "os sysname", info.sysname, 1) != 0) return -1;
  if (cfgAddString(pCfg, "os nodename", info.nodename, 1) != 0) return -1;
  if (cfgAddString(pCfg, "os release", info.release, 1) != 0) return -1;
  if (cfgAddString(pCfg, "os version", info.version, 1) != 0) return -1;
  if (cfgAddString(pCfg, "os machine", info.machine, 1) != 0) return -1;

  if (cfgAddString(pCfg, "version", version, 1) != 0) return -1;
  if (cfgAddString(pCfg, "compatible_version", compatible_version, 1) != 0) return -1;
  if (cfgAddString(pCfg, "gitinfo", gitinfo, 1) != 0) return -1;
  if (cfgAddString(pCfg, "buildinfo", buildinfo, 1) != 0) return -1;
  return 0;
}

static int32_t taosAddServerCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "dataDir", tsDataDir, 0) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalDataDirGB", 2.0f, 0.001f, 10000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "supportVnodes", tsNumOfSupportVnodes, 0, 4096, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxShellConns", tsMaxShellConns, 10, 50000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "statusInterval", tsStatusInterval, 1, 30, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "minSlidingTime", tsMinSlidingTime, 10, 1000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "minIntervalTime", tsMinIntervalTime, 1, 1000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxNumOfDistinctRes", tsMaxNumOfDistinctResults, 10 * 10000, 10000 * 10000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxStreamCompDelay", tsMaxStreamComputDelay, 10, 1000000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxFirstStreamCompDelay", tsStreamCompStartDelay, 1000, 1000000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "retryStreamCompDelay", tsRetryStreamCompDelay, 10, 1000000000, 0) != 0) return -1;
  if (cfgAddFloat(pCfg, "streamCompDelayRatio", tsStreamComputDelayRatio, 0.1, 0.9, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "queryBufferSize", tsQueryBufferSize, -1, 500000000000, 0) != 0) return -1;
  if (cfgAddBool(pCfg, "retrieveBlockingModel", tsRetrieveBlockingModel, 0) != 0) return -1;
  if (cfgAddBool(pCfg, "printAuth", tsPrintAuth, 0) != 0) return -1;
  if (cfgAddBool(pCfg, "slaveQuery", tsEnableSlaveQuery, 0) != 0) return -1;
  if (cfgAddBool(pCfg, "deadLockKillQuery", tsDeadLockKillQuery, 0) != 0) return -1;

  if (cfgAddInt32(pCfg, "multiProcess", tsMultiProcess, 0, 2, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "mnodeShmSize", tsMnodeShmSize, TSDB_MAX_WAL_SIZE + 128, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "vnodeShmSize", tsVnodeShmSize, TSDB_MAX_WAL_SIZE + 128, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "qnodeShmSize", tsQnodeShmSize, TSDB_MAX_WAL_SIZE + 128, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "snodeShmSize", tsSnodeShmSize, TSDB_MAX_WAL_SIZE + 128, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "bnodeShmSize", tsBnodeShmSize, TSDB_MAX_WAL_SIZE + 128, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "mumOfShmThreads", tsNumOfShmThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfRpcThreads = tsNumOfCores / 2;
  tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 1, 4);
  if (cfgAddInt32(pCfg, "numOfRpcThreads", tsNumOfRpcThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfCommitThreads = tsNumOfCores / 2;
  tsNumOfCommitThreads = TRANGE(tsNumOfCommitThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfCommitThreads", tsNumOfCommitThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfMnodeQueryThreads = tsNumOfCores / 8;
  tsNumOfMnodeQueryThreads = TRANGE(tsNumOfMnodeQueryThreads, 1, 4);
  if (cfgAddInt32(pCfg, "numOfMnodeQueryThreads", tsNumOfMnodeQueryThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfMnodeReadThreads = tsNumOfCores / 8;
  tsNumOfMnodeReadThreads = TRANGE(tsNumOfMnodeReadThreads, 1, 4);
  if (cfgAddInt32(pCfg, "numOfMnodeReadThreads", tsNumOfMnodeReadThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfVnodeQueryThreads = tsNumOfCores / 2;
  tsNumOfVnodeQueryThreads = TMAX(tsNumOfVnodeQueryThreads, 1);
  if (cfgAddInt32(pCfg, "numOfVnodeQueryThreads", tsNumOfVnodeQueryThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfVnodeFetchThreads = tsNumOfCores / 2;
  tsNumOfVnodeFetchThreads = TRANGE(tsNumOfVnodeFetchThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeFetchThreads", tsNumOfVnodeFetchThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfVnodeWriteThreads = tsNumOfCores;
  tsNumOfVnodeWriteThreads = TMAX(tsNumOfVnodeWriteThreads, 1);
  if (cfgAddInt32(pCfg, "numOfVnodeWriteThreads", tsNumOfVnodeWriteThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfVnodeSyncThreads = tsNumOfCores / 2;
  tsNumOfVnodeSyncThreads = TMAX(tsNumOfVnodeSyncThreads, 1);
  if (cfgAddInt32(pCfg, "numOfVnodeSyncThreads", tsNumOfVnodeSyncThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfVnodeMergeThreads = tsNumOfCores / 8;
  tsNumOfVnodeMergeThreads = TRANGE(tsNumOfVnodeMergeThreads, 1, 1);
  if (cfgAddInt32(pCfg, "numOfVnodeMergeThreads", tsNumOfVnodeMergeThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfQnodeQueryThreads = tsNumOfCores / 2;
  tsNumOfQnodeQueryThreads = TMAX(tsNumOfQnodeQueryThreads, 1);
  if (cfgAddInt32(pCfg, "numOfQnodeQueryThreads", tsNumOfQnodeQueryThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfQnodeFetchThreads = tsNumOfCores / 2;
  tsNumOfQnodeFetchThreads = TRANGE(tsNumOfQnodeFetchThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfQnodeFetchThreads", tsNumOfQnodeFetchThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfSnodeSharedThreads = tsNumOfCores / 4;
  tsNumOfSnodeSharedThreads = TRANGE(tsNumOfSnodeSharedThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfSnodeSharedThreads", tsNumOfSnodeSharedThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfSnodeUniqueThreads = tsNumOfCores / 4;
  tsNumOfSnodeUniqueThreads = TRANGE(tsNumOfSnodeUniqueThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfSnodeUniqueThreads", tsNumOfSnodeUniqueThreads, 1, 1024, 0) != 0) return -1;

  tsRpcQueueMemoryAllowed = tsTotalMemoryKB * 1024 * 0.1;
  tsRpcQueueMemoryAllowed = TRANGE(tsRpcQueueMemoryAllowed, TSDB_MAX_WAL_SIZE * 10L, TSDB_MAX_WAL_SIZE * 10000L);
  if (cfgAddInt64(pCfg, "rpcQueueMemoryAllowed", tsRpcQueueMemoryAllowed, TSDB_MAX_WAL_SIZE * 10L, INT64_MAX, 0) != 0)
    return -1;

  if (cfgAddBool(pCfg, "monitor", tsEnableMonitor, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorInterval", tsMonitorInterval, 1, 200000, 0) != 0) return -1;
  if (cfgAddString(pCfg, "monitorFqdn", tsMonitorFqdn, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorPort", tsMonitorPort, 1, 65056, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorMaxLogs", tsMonitorMaxLogs, 1, 1000000, 0) != 0) return -1;
  if (cfgAddBool(pCfg, "monitorComp", tsMonitorComp, 0) != 0) return -1;

  if (cfgAddBool(pCfg, "telemetryReporting", tsEnableTelem, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "telemetryInterval", tsTelemInterval, 1, 200000, 0) != 0) return -1;
  if (cfgAddString(pCfg, "telemetryServer", tsTelemServer, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "telemetryPort", tsTelemPort, 1, 65056, 0) != 0) return -1;

  if (cfgAddInt32(pCfg, "transPullupInterval", tsTransPullupInterval, 1, 10000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "mqRebalanceInterval", tsMqRebalanceInterval, 1, 10000, 1) != 0) return -1;

  if (cfgAddBool(pCfg, "udf", tsStartUdfd, 0) != 0) return -1;
  return 0;
}

static void taosSetClientLogCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "logDir");
  tstrncpy(tsLogDir, cfgGetItem(pCfg, "logDir")->str, PATH_MAX);
  taosExpandDir(tsLogDir, tsLogDir, PATH_MAX);
  tsLogSpace.reserved = cfgGetItem(pCfg, "minimalLogDirGB")->fval;
  tsNumOfLogLines = cfgGetItem(pCfg, "numOfLogLines")->i32;
  tsAsyncLog = cfgGetItem(pCfg, "asyncLog")->bval;
  tsLogKeepDays = cfgGetItem(pCfg, "logKeepDays")->i32;
  cDebugFlag = cfgGetItem(pCfg, "cDebugFlag")->i32;
  uDebugFlag = cfgGetItem(pCfg, "uDebugFlag")->i32;
  qDebugFlag = cfgGetItem(pCfg, "qDebugFlag")->i32;
  rpcDebugFlag = cfgGetItem(pCfg, "rpcDebugFlag")->i32;
  tmrDebugFlag = cfgGetItem(pCfg, "tmrDebugFlag")->i32;
  jniDebugFlag = cfgGetItem(pCfg, "jniDebugFlag")->i32;
}

static void taosSetServerLogCfg(SConfig *pCfg) {
  dDebugFlag = cfgGetItem(pCfg, "dDebugFlag")->i32;
  vDebugFlag = cfgGetItem(pCfg, "vDebugFlag")->i32;
  mDebugFlag = cfgGetItem(pCfg, "mDebugFlag")->i32;
  qDebugFlag = cfgGetItem(pCfg, "qDebugFlag")->i32;
  wDebugFlag = cfgGetItem(pCfg, "wDebugFlag")->i32;
  sDebugFlag = cfgGetItem(pCfg, "sDebugFlag")->i32;
  tsdbDebugFlag = cfgGetItem(pCfg, "tsdbDebugFlag")->i32;
  tqDebugFlag = cfgGetItem(pCfg, "tqDebugFlag")->i32;
  fsDebugFlag = cfgGetItem(pCfg, "fsDebugFlag")->i32;
  fnDebugFlag = cfgGetItem(pCfg, "fnDebugFlag")->i32;
  smaDebugFlag = cfgGetItem(pCfg, "smaDebugFlag")->i32;
}

static int32_t taosSetClientCfg(SConfig *pCfg) {
  tstrncpy(tsLocalFqdn, cfgGetItem(pCfg, "fqdn")->str, TSDB_FQDN_LEN);
  tsServerPort = (uint16_t)cfgGetItem(pCfg, "serverPort")->i32;
  snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%u", tsLocalFqdn, tsServerPort);

  char defaultFirstEp[TSDB_EP_LEN] = {0};
  snprintf(defaultFirstEp, TSDB_EP_LEN, "%s:%u", tsLocalFqdn, tsServerPort);

  SConfigItem *pFirstEpItem = cfgGetItem(pCfg, "firstEp");
  SEp          firstEp = {0};
  taosGetFqdnPortFromEp(strlen(pFirstEpItem->str) == 0 ? defaultFirstEp : pFirstEpItem->str, &firstEp);
  snprintf(tsFirst, sizeof(tsFirst), "%s:%u", firstEp.fqdn, firstEp.port);
  cfgSetItem(pCfg, "firstEp", tsFirst, pFirstEpItem->stype);

  SConfigItem *pSecondpItem = cfgGetItem(pCfg, "secondEp");
  SEp          secondEp = {0};
  taosGetFqdnPortFromEp(strlen(pSecondpItem->str) == 0 ? defaultFirstEp : pSecondpItem->str, &secondEp);
  snprintf(tsSecond, sizeof(tsSecond), "%s:%u", secondEp.fqdn, secondEp.port);
  cfgSetItem(pCfg, "secondEp", tsSecond, pSecondpItem->stype);

  tstrncpy(tsTempDir, cfgGetItem(pCfg, "tempDir")->str, PATH_MAX);
  taosExpandDir(tsTempDir, tsTempDir, PATH_MAX);
  tsTempSpace.reserved = cfgGetItem(pCfg, "minimalTempDirGB")->fval;
  if (taosMulMkDir(tsTempDir) != 0) {
    uError("failed to create tempDir:%s since %s", tsTempDir, terrstr());
    return -1;
  }

  tstrncpy(tsSmlChildTableName, cfgGetItem(pCfg, "smlChildTableName")->str, TSDB_TABLE_NAME_LEN);
  tsSmlDataFormat = cfgGetItem(pCfg, "smlDataFormat")->bval;

  tsShellActivityTimer = cfgGetItem(pCfg, "shellActivityTimer")->i32;
  tsCompressMsgSize = cfgGetItem(pCfg, "compressMsgSize")->i32;
  tsCompressColData = cfgGetItem(pCfg, "compressColData")->i32;
  tsKeepOriginalColumnName = cfgGetItem(pCfg, "keepColumnName")->bval;
  tsNumOfTaskQueueThreads = cfgGetItem(pCfg, "numOfTaskQueueThreads")->i32;
  tsQueryPolicy = cfgGetItem(pCfg, "queryPolicy")->i32;
  return 0;
}

static void taosSetSystemCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "timezone");
  osSetTimezone(pItem->str);
  uDebug("timezone format changed from %s to %s", pItem->str, tsTimezoneStr);
  cfgSetItem(pCfg, "timezone", tsTimezoneStr, pItem->stype);

  const char *locale = cfgGetItem(pCfg, "locale")->str;
  const char *charset = cfgGetItem(pCfg, "charset")->str;
  taosSetSystemLocale(locale, charset);
  osSetSystemLocale(locale, charset);

  bool enableCore = cfgGetItem(pCfg, "enableCoreFile")->bval;
  taosSetConsoleEcho(enableCore);

  // todo
  tsVersion = 30000000;
}

static int32_t taosSetServerCfg(SConfig *pCfg) {
  tsDataSpace.reserved = cfgGetItem(pCfg, "minimalDataDirGB")->fval;
  tsNumOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  tsMaxShellConns = cfgGetItem(pCfg, "maxShellConns")->i32;
  tsStatusInterval = cfgGetItem(pCfg, "statusInterval")->i32;
  tsMinSlidingTime = cfgGetItem(pCfg, "minSlidingTime")->i32;
  tsMinIntervalTime = cfgGetItem(pCfg, "minIntervalTime")->i32;
  tsMaxNumOfDistinctResults = cfgGetItem(pCfg, "maxNumOfDistinctRes")->i32;
  tsMaxStreamComputDelay = cfgGetItem(pCfg, "maxStreamCompDelay")->i32;
  tsStreamCompStartDelay = cfgGetItem(pCfg, "maxFirstStreamCompDelay")->i32;
  tsRetryStreamCompDelay = cfgGetItem(pCfg, "retryStreamCompDelay")->i32;
  tsStreamComputDelayRatio = cfgGetItem(pCfg, "streamCompDelayRatio")->fval;
  tsQueryBufferSize = cfgGetItem(pCfg, "queryBufferSize")->i32;
  tsRetrieveBlockingModel = cfgGetItem(pCfg, "retrieveBlockingModel")->bval;
  tsPrintAuth = cfgGetItem(pCfg, "printAuth")->bval;
  tsEnableSlaveQuery = cfgGetItem(pCfg, "slaveQuery")->bval;
  tsDeadLockKillQuery = cfgGetItem(pCfg, "deadLockKillQuery")->i32;

  tsMultiProcess = cfgGetItem(pCfg, "multiProcess")->bval;
  tsMnodeShmSize = cfgGetItem(pCfg, "mnodeShmSize")->i32;
  tsVnodeShmSize = cfgGetItem(pCfg, "vnodeShmSize")->i32;
  tsQnodeShmSize = cfgGetItem(pCfg, "qnodeShmSize")->i32;
  tsSnodeShmSize = cfgGetItem(pCfg, "snodeShmSize")->i32;
  tsBnodeShmSize = cfgGetItem(pCfg, "bnodeShmSize")->i32;

  tsNumOfRpcThreads = cfgGetItem(pCfg, "numOfRpcThreads")->i32;
  tsNumOfCommitThreads = cfgGetItem(pCfg, "numOfCommitThreads")->i32;
  tsNumOfMnodeQueryThreads = cfgGetItem(pCfg, "numOfMnodeQueryThreads")->i32;
  tsNumOfMnodeReadThreads = cfgGetItem(pCfg, "numOfMnodeReadThreads")->i32;
  tsNumOfVnodeQueryThreads = cfgGetItem(pCfg, "numOfVnodeQueryThreads")->i32;
  tsNumOfVnodeFetchThreads = cfgGetItem(pCfg, "numOfVnodeFetchThreads")->i32;
  tsNumOfVnodeWriteThreads = cfgGetItem(pCfg, "numOfVnodeWriteThreads")->i32;
  tsNumOfVnodeSyncThreads = cfgGetItem(pCfg, "numOfVnodeSyncThreads")->i32;
  tsNumOfVnodeMergeThreads = cfgGetItem(pCfg, "numOfVnodeMergeThreads")->i32;
  tsNumOfQnodeQueryThreads = cfgGetItem(pCfg, "numOfQnodeQueryThreads")->i32;
  tsNumOfQnodeFetchThreads = cfgGetItem(pCfg, "numOfQnodeFetchThreads")->i32;
  tsNumOfSnodeSharedThreads = cfgGetItem(pCfg, "numOfSnodeSharedThreads")->i32;
  tsNumOfSnodeUniqueThreads = cfgGetItem(pCfg, "numOfSnodeUniqueThreads")->i32;
  tsRpcQueueMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64;

  tsEnableMonitor = cfgGetItem(pCfg, "monitor")->bval;
  tsMonitorInterval = cfgGetItem(pCfg, "monitorInterval")->i32;
  tstrncpy(tsMonitorFqdn, cfgGetItem(pCfg, "monitorFqdn")->str, TSDB_FQDN_LEN);
  tsMonitorPort = (uint16_t)cfgGetItem(pCfg, "monitorPort")->i32;
  tsMonitorMaxLogs = cfgGetItem(pCfg, "monitorMaxLogs")->i32;
  tsMonitorComp = cfgGetItem(pCfg, "monitorComp")->bval;

  tsEnableTelem = cfgGetItem(pCfg, "telemetryReporting")->bval;
  tsTelemInterval = cfgGetItem(pCfg, "telemetryInterval")->i32;
  tstrncpy(tsTelemServer, cfgGetItem(pCfg, "telemetryServer")->str, TSDB_FQDN_LEN);
  tsTelemPort = (uint16_t)cfgGetItem(pCfg, "telemetryPort")->i32;

  tsTransPullupInterval = cfgGetItem(pCfg, "transPullupInterval")->i32;
  tsMqRebalanceInterval = cfgGetItem(pCfg, "mqRebalanceInterval")->i32;

  tsStartUdfd = cfgGetItem(pCfg, "udf")->bval;

  if (tsQueryBufferSize >= 0) {
    tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
  }

  return 0;
}

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc) {
  osDefaultInit();

  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return -1;

  if (tsc) {
    tsLogEmbedded = 0;
    if (taosAddClientLogCfg(pCfg) != 0) return -1;
  } else {
    tsLogEmbedded = 1;
    if (taosAddClientLogCfg(pCfg) != 0) return -1;
    if (taosAddServerLogCfg(pCfg) != 0) return -1;
  }

  if (taosLoadCfg(pCfg, envCmd, cfgDir, envFile, apolloUrl) != 0) {
    uError("failed to load cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (cfgLoadFromArray(pCfg, pArgs) != 0) {
    uError("failed to load cfg from array since %s", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (tsc) {
    taosSetClientLogCfg(pCfg);
  } else {
    taosSetClientLogCfg(pCfg);
    taosSetServerLogCfg(pCfg);
  }

  taosSetAllDebugFlag(cfgGetItem(pCfg, "debugFlag")->i32);

  if (taosMulMkDir(tsLogDir) != 0) {
    uError("failed to create dir:%s since %s", tsLogDir, terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (taosInitLog(logname, logFileNum) != 0) {
    uError("failed to init log file since %s", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  cfgCleanup(pCfg);
  return 0;
}

static int32_t taosCheckGlobalCfg() {
  uint32_t ipv4 = taosGetIpv4FromFqdn(tsLocalFqdn);
  if (ipv4 == 0xffffffff) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get ip from fqdn:%s since %s, dnode can not be initialized", tsLocalFqdn, terrstr());
    return -1;
  }

  if (tsServerPort <= 0) {
    uError("invalid server port:%u, dnode can not be initialized", tsServerPort);
    return -1;
  }

  return 0;
}

int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc) {
  if (tsCfg != NULL) return 0;
  tsCfg = cfgInit();

  if (tsc) {
    if (taosAddClientCfg(tsCfg) != 0) return -1;
    if (taosAddClientLogCfg(tsCfg) != 0) return -1;
  } else {
    if (taosAddClientCfg(tsCfg) != 0) return -1;
    if (taosAddServerCfg(tsCfg) != 0) return -1;
    if (taosAddClientLogCfg(tsCfg) != 0) return -1;
    if (taosAddServerLogCfg(tsCfg) != 0) return -1;
  }
  taosAddSystemCfg(tsCfg);

  if (taosLoadCfg(tsCfg, envCmd, cfgDir, envFile, apolloUrl) != 0) {
    uError("failed to load cfg since %s", terrstr());
    cfgCleanup(tsCfg);
    tsCfg = NULL;
    return -1;
  }

  if (cfgLoadFromArray(tsCfg, pArgs) != 0) {
    uError("failed to load cfg from array since %s", terrstr());
    cfgCleanup(tsCfg);
    return -1;
  }

  if (tsc) {
    if (taosSetClientCfg(tsCfg)) return -1;
  } else {
    if (taosSetClientCfg(tsCfg)) return -1;
    if (taosSetServerCfg(tsCfg)) return -1;
    if (taosSetTfsCfg(tsCfg) != 0) return -1;
  }
  taosSetSystemCfg(tsCfg);

  cfgDumpCfg(tsCfg, tsc, false);

  if (taosCheckGlobalCfg() != 0) {
    return -1;
  }

  return 0;
}

void taosCleanupCfg() {
  if (tsCfg) {
    cfgCleanup(tsCfg);
    tsCfg = NULL;
  }
}

void taosCfgDynamicOptions(const char *option, const char *value) {
  if (strcasecmp(option, "debugFlag") == 0) {
    int32_t debugFlag = atoi(value);
    taosSetAllDebugFlag(debugFlag);
  }

  if (strcasecmp(option, "resetlog") == 0) {
    taosResetLog();
    cfgDumpCfg(tsCfg, 0, false);
  }
}
