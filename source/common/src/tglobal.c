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
#include "tgrant.h"
#include "tlog.h"

GRANT_CFG_DECLARE;

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
bool    tsPrintAuth = false;

// multi process
int32_t tsMultiProcess = 0;
int32_t tsMnodeShmSize = TSDB_MAX_MSG_SIZE * 2 + 1024;
int32_t tsVnodeShmSize = TSDB_MAX_MSG_SIZE * 10 + 1024;
int32_t tsQnodeShmSize = TSDB_MAX_MSG_SIZE * 4 + 1024;
int32_t tsSnodeShmSize = TSDB_MAX_MSG_SIZE * 4 + 1024;
int32_t tsBnodeShmSize = TSDB_MAX_MSG_SIZE * 4 + 1024;
int32_t tsNumOfShmThreads = 1;

// queue & threads
int32_t tsNumOfRpcThreads = 1;
int32_t tsNumOfCommitThreads = 2;
int32_t tsNumOfTaskQueueThreads = 4;
int32_t tsNumOfMnodeQueryThreads = 4;
int32_t tsNumOfMnodeFetchThreads = 1;
int32_t tsNumOfMnodeReadThreads = 1;
int32_t tsNumOfVnodeQueryThreads = 4;
int32_t tsNumOfVnodeStreamThreads = 2;
int32_t tsNumOfVnodeFetchThreads = 4;
int32_t tsNumOfVnodeWriteThreads = 2;
int32_t tsNumOfVnodeSyncThreads = 2;
int32_t tsNumOfQnodeQueryThreads = 4;
int32_t tsNumOfQnodeFetchThreads = 4;
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
char tsSmlTagName[TSDB_COL_NAME_LEN] = "_tag_null";
char tsSmlChildTableName[TSDB_TABLE_NAME_LEN] = "";  // user defined child table name can be specified in tag value.
                                                     // If set to empty system will generate table name using MD5 hash.
bool tsSmlDataFormat =
    true;  // true means that the name and order of cols in each line are the same(only for influx protocol)

// query
int32_t tsQueryPolicy = 1;
int32_t tsQuerySmaOptimize = 0;

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

// count/hyperloglog function always return values in case of all NULL data or Empty data set.
int32_t tsCountAlwaysReturnValue = 1;

// 10 ms for sliding time, the value will changed in case of time precision changed
int32_t tsMinSlidingTime = 10;

// the maxinum number of distict query result
int32_t tsMaxNumOfDistinctResults = 1000 * 10000;

// 1 database precision unit for interval time range, changed accordingly
int32_t tsMinIntervalTime = 1;

// the maximum allowed query buffer size during query processing for each data node.
// -1 no limit (default)
// 0  no query allowed, queries are disabled
// positive value (in MB)
int32_t tsQueryBufferSize = -1;
int64_t tsQueryBufferSizeBytes = -1;

// tsdb config
// For backward compatibility
bool tsdbForceKeepFile = false;

int32_t  tsDiskCfgNum = 0;
SDiskCfg tsDiskCfg[TFS_MAX_DISKS] = {0};

// stream scheduler
bool tsSchedStreamToSnode = true;

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
int32_t tsTransPullupInterval = 2;
int32_t tsMqRebalanceInterval = 2;
int32_t tsTtlUnit = 86400;
int32_t tsTtlPushInterval = 86400;
int32_t tsGrantHBInterval = 60;

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
      }
      if (taosMulMkDir(pCfg->dir) != 0) {
        uError("failed to create tfsDir:%s since %s", tsDataDir, terrstr());
        return -1;
      }
    }
  }

  if (tsDataDir[0] == 0) {
    if (pItem->str != NULL) {
      taosAddDataDir(tsDiskCfgNum, pItem->str, 0, 1);
      tstrncpy(tsDataDir, pItem->str, PATH_MAX);
      if (taosMulMkDir(tsDataDir) != 0) {
        uError("failed to create tfsDir:%s since %s", tsDataDir, terrstr());
        return -1;
      }
      tsDiskCfgNum++;
    } else {
      uError("datadir not set");
      return -1;
    }
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
  if (cfgAddInt32(pCfg, "debugFlag", 0, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "simDebugFlag", 143, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "tmrDebugFlag", tmrDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "uDebugFlag", uDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcDebugFlag", rpcDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "jniDebugFlag", jniDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "qDebugFlag", qDebugFlag, 0, 255, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "cDebugFlag", cDebugFlag, 0, 255, 1) != 0) return -1;
  return 0;
}

static int32_t taosAddServerLogCfg(SConfig *pCfg) {
  if (cfgAddInt32(pCfg, "dDebugFlag", dDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "vDebugFlag", vDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "mDebugFlag", mDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "wDebugFlag", wDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "sDebugFlag", sDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "tsdbDebugFlag", tsdbDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "tqDebugFlag", tqDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "fsDebugFlag", fsDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "udfDebugFlag", udfDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "smaDebugFlag", smaDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "idxDebugFlag", idxDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "tdbDebugFlag", tdbDebugFlag, 0, 255, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "metaDebugFlag", metaDebugFlag, 0, 255, 0) != 0) return -1;
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
  if (cfgAddFloat(pCfg, "minimalTmpDirGB", 1.0f, 0.001f, 10000000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "shellActivityTimer", tsShellActivityTimer, 1, 120, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "compressMsgSize", tsCompressMsgSize, -1, 100000000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "compressColData", tsCompressColData, -1, 100000000, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "queryPolicy", tsQueryPolicy, 1, 3, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "querySmaOptimize", tsQuerySmaOptimize, 0, 1, 1) != 0) return -1;
  if (cfgAddString(pCfg, "smlChildTableName", "", 1) != 0) return -1;
  if (cfgAddString(pCfg, "smlTagName", tsSmlTagName, 1) != 0) return -1;
  if (cfgAddBool(pCfg, "smlDataFormat", tsSmlDataFormat, 1) != 0) return -1;

  tsNumOfTaskQueueThreads = tsNumOfCores / 2;
  tsNumOfTaskQueueThreads = TMAX(tsNumOfTaskQueueThreads, 4);
  if (cfgAddInt32(pCfg, "numOfTaskQueueThreads", tsNumOfTaskQueueThreads, 4, 1024, 0) != 0) return -1;

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

  tsNumOfSupportVnodes = tsNumOfCores * 2;
  tsNumOfSupportVnodes = TMAX(tsNumOfSupportVnodes, 2);
  if (cfgAddInt32(pCfg, "supportVnodes", tsNumOfSupportVnodes, 0, 4096, 0) != 0) return -1;

  if (cfgAddInt32(pCfg, "maxShellConns", tsMaxShellConns, 10, 50000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "statusInterval", tsStatusInterval, 1, 30, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "minSlidingTime", tsMinSlidingTime, 10, 1000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "minIntervalTime", tsMinIntervalTime, 1, 1000000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxNumOfDistinctRes", tsMaxNumOfDistinctResults, 10 * 10000, 10000 * 10000, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "countAlwaysReturnValue", tsCountAlwaysReturnValue, 0, 1, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "queryBufferSize", tsQueryBufferSize, -1, 500000000000, 0) != 0) return -1;
  if (cfgAddBool(pCfg, "printAuth", tsPrintAuth, 0) != 0) return -1;

  if (cfgAddInt32(pCfg, "multiProcess", tsMultiProcess, 0, 2, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "mnodeShmSize", tsMnodeShmSize, TSDB_MAX_MSG_SIZE * 2 + 1024, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "vnodeShmSize", tsVnodeShmSize, TSDB_MAX_MSG_SIZE * 2 + 1024, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "qnodeShmSize", tsQnodeShmSize, TSDB_MAX_MSG_SIZE * 2 + 1024, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "snodeShmSize", tsSnodeShmSize, TSDB_MAX_MSG_SIZE * 2 + 1024, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "bnodeShmSize", tsBnodeShmSize, TSDB_MAX_MSG_SIZE * 2 + 1024, INT32_MAX, 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfShmThreads", tsNumOfShmThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfRpcThreads = tsNumOfCores / 2;
  tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 1, 4);
  if (cfgAddInt32(pCfg, "numOfRpcThreads", tsNumOfRpcThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfCommitThreads = tsNumOfCores / 2;
  tsNumOfCommitThreads = TRANGE(tsNumOfCommitThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfCommitThreads", tsNumOfCommitThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfMnodeReadThreads = tsNumOfCores / 8;
  tsNumOfMnodeReadThreads = TRANGE(tsNumOfMnodeReadThreads, 1, 4);
  if (cfgAddInt32(pCfg, "numOfMnodeReadThreads", tsNumOfMnodeReadThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfVnodeQueryThreads = tsNumOfCores * 2;
  tsNumOfVnodeQueryThreads = TMAX(tsNumOfVnodeQueryThreads, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeQueryThreads", tsNumOfVnodeQueryThreads, 4, 1024, 0) != 0) return -1;

  tsNumOfVnodeStreamThreads = tsNumOfCores / 4;
  tsNumOfVnodeStreamThreads = TMAX(tsNumOfVnodeStreamThreads, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeStreamThreads", tsNumOfVnodeStreamThreads, 4, 1024, 0) != 0) return -1;

  tsNumOfVnodeFetchThreads = tsNumOfCores / 4;
  tsNumOfVnodeFetchThreads = TMAX(tsNumOfVnodeFetchThreads, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeFetchThreads", tsNumOfVnodeFetchThreads, 4, 1024, 0) != 0) return -1;

  tsNumOfVnodeWriteThreads = tsNumOfCores;
  tsNumOfVnodeWriteThreads = TMAX(tsNumOfVnodeWriteThreads, 1);
  if (cfgAddInt32(pCfg, "numOfVnodeWriteThreads", tsNumOfVnodeWriteThreads, 1, 1024, 0) != 0) return -1;

  // tsNumOfVnodeSyncThreads = tsNumOfCores;
  tsNumOfVnodeSyncThreads = 32;
  tsNumOfVnodeSyncThreads = TMAX(tsNumOfVnodeSyncThreads, 1);
  if (cfgAddInt32(pCfg, "numOfVnodeSyncThreads", tsNumOfVnodeSyncThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfQnodeQueryThreads = tsNumOfCores * 2;
  tsNumOfQnodeQueryThreads = TMAX(tsNumOfQnodeQueryThreads, 4);
  if (cfgAddInt32(pCfg, "numOfQnodeQueryThreads", tsNumOfQnodeQueryThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfQnodeFetchThreads = tsNumOfCores / 2;
  tsNumOfQnodeFetchThreads = TMAX(tsNumOfQnodeFetchThreads, 4);
  if (cfgAddInt32(pCfg, "numOfQnodeFetchThreads", tsNumOfQnodeFetchThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfSnodeSharedThreads = tsNumOfCores / 4;
  tsNumOfSnodeSharedThreads = TRANGE(tsNumOfSnodeSharedThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfSnodeSharedThreads", tsNumOfSnodeSharedThreads, 2, 1024, 0) != 0) return -1;

  tsNumOfSnodeUniqueThreads = tsNumOfCores / 4;
  tsNumOfSnodeUniqueThreads = TRANGE(tsNumOfSnodeUniqueThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfSnodeUniqueThreads", tsNumOfSnodeUniqueThreads, 2, 1024, 0) != 0) return -1;

  tsRpcQueueMemoryAllowed = tsTotalMemoryKB * 1024 * 0.1;
  tsRpcQueueMemoryAllowed = TRANGE(tsRpcQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * 10LL, TSDB_MAX_MSG_SIZE * 10000LL);
  if (cfgAddInt64(pCfg, "rpcQueueMemoryAllowed", tsRpcQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * 10L, INT64_MAX, 0) != 0)
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
  if (cfgAddInt32(pCfg, "ttlUnit", tsTtlUnit, 1, 86400 * 365, 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "ttlPushInterval", tsTtlPushInterval, 1, 100000, 1) != 0) return -1;

  if (cfgAddBool(pCfg, "udf", tsStartUdfd, 0) != 0) return -1;
  GRANT_CFG_ADD;
  return 0;
}

static void taosSetClientLogCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "logDir");
  tstrncpy(tsLogDir, cfgGetItem(pCfg, "logDir")->str, PATH_MAX);
  taosExpandDir(tsLogDir, tsLogDir, PATH_MAX);
  tsLogSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalLogDirGB")->fval) * 1024 * 1024 * 1024);
  tsNumOfLogLines = cfgGetItem(pCfg, "numOfLogLines")->i32;
  tsAsyncLog = cfgGetItem(pCfg, "asyncLog")->bval;
  tsLogKeepDays = cfgGetItem(pCfg, "logKeepDays")->i32;
  tmrDebugFlag = cfgGetItem(pCfg, "tmrDebugFlag")->i32;
  uDebugFlag = cfgGetItem(pCfg, "uDebugFlag")->i32;
  jniDebugFlag = cfgGetItem(pCfg, "jniDebugFlag")->i32;
  rpcDebugFlag = cfgGetItem(pCfg, "rpcDebugFlag")->i32;
  qDebugFlag = cfgGetItem(pCfg, "qDebugFlag")->i32;
  cDebugFlag = cfgGetItem(pCfg, "cDebugFlag")->i32;
}

static void taosSetServerLogCfg(SConfig *pCfg) {
  dDebugFlag = cfgGetItem(pCfg, "dDebugFlag")->i32;
  vDebugFlag = cfgGetItem(pCfg, "vDebugFlag")->i32;
  mDebugFlag = cfgGetItem(pCfg, "mDebugFlag")->i32;
  wDebugFlag = cfgGetItem(pCfg, "wDebugFlag")->i32;
  sDebugFlag = cfgGetItem(pCfg, "sDebugFlag")->i32;
  tsdbDebugFlag = cfgGetItem(pCfg, "tsdbDebugFlag")->i32;
  tqDebugFlag = cfgGetItem(pCfg, "tqDebugFlag")->i32;
  fsDebugFlag = cfgGetItem(pCfg, "fsDebugFlag")->i32;
  udfDebugFlag = cfgGetItem(pCfg, "udfDebugFlag")->i32;
  smaDebugFlag = cfgGetItem(pCfg, "smaDebugFlag")->i32;
  idxDebugFlag = cfgGetItem(pCfg, "idxDebugFlag")->i32;
  tdbDebugFlag = cfgGetItem(pCfg, "tdbDebugFlag")->i32;
  metaDebugFlag = cfgGetItem(pCfg, "metaDebugFlag")->i32;
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
  tsTempSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalTmpDirGB")->fval) * 1024 * 1024 * 1024);
  if (taosMulMkDir(tsTempDir) != 0) {
    uError("failed to create tempDir:%s since %s", tsTempDir, terrstr());
    return -1;
  }

  tstrncpy(tsSmlChildTableName, cfgGetItem(pCfg, "smlChildTableName")->str, TSDB_TABLE_NAME_LEN);
  tstrncpy(tsSmlTagName, cfgGetItem(pCfg, "smlTagName")->str, TSDB_COL_NAME_LEN);
  tsSmlDataFormat = cfgGetItem(pCfg, "smlDataFormat")->bval;

  tsShellActivityTimer = cfgGetItem(pCfg, "shellActivityTimer")->i32;
  tsCompressMsgSize = cfgGetItem(pCfg, "compressMsgSize")->i32;
  tsCompressColData = cfgGetItem(pCfg, "compressColData")->i32;
  tsNumOfTaskQueueThreads = cfgGetItem(pCfg, "numOfTaskQueueThreads")->i32;
  tsQueryPolicy = cfgGetItem(pCfg, "queryPolicy")->i32;
  tsQuerySmaOptimize = cfgGetItem(pCfg, "querySmaOptimize")->i32;
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
  taosSetCoreDump(enableCore);

  // todo
  tsVersion = 30000000;
}

static int32_t taosSetServerCfg(SConfig *pCfg) {
  tsDataSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalDataDirGB")->fval) * 1024 * 1024 * 1024);
  tsNumOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  tsMaxShellConns = cfgGetItem(pCfg, "maxShellConns")->i32;
  tsStatusInterval = cfgGetItem(pCfg, "statusInterval")->i32;
  tsMinSlidingTime = cfgGetItem(pCfg, "minSlidingTime")->i32;
  tsMinIntervalTime = cfgGetItem(pCfg, "minIntervalTime")->i32;
  tsMaxNumOfDistinctResults = cfgGetItem(pCfg, "maxNumOfDistinctRes")->i32;
  tsCountAlwaysReturnValue = cfgGetItem(pCfg, "countAlwaysReturnValue")->i32;
  tsQueryBufferSize = cfgGetItem(pCfg, "queryBufferSize")->i32;
  tsPrintAuth = cfgGetItem(pCfg, "printAuth")->bval;

  tsMultiProcess = cfgGetItem(pCfg, "multiProcess")->bval;
  tsMnodeShmSize = cfgGetItem(pCfg, "mnodeShmSize")->i32;
  tsVnodeShmSize = cfgGetItem(pCfg, "vnodeShmSize")->i32;
  tsQnodeShmSize = cfgGetItem(pCfg, "qnodeShmSize")->i32;
  tsSnodeShmSize = cfgGetItem(pCfg, "snodeShmSize")->i32;
  tsBnodeShmSize = cfgGetItem(pCfg, "bnodeShmSize")->i32;

  tsNumOfRpcThreads = cfgGetItem(pCfg, "numOfRpcThreads")->i32;
  tsNumOfCommitThreads = cfgGetItem(pCfg, "numOfCommitThreads")->i32;
  tsNumOfMnodeReadThreads = cfgGetItem(pCfg, "numOfMnodeReadThreads")->i32;
  tsNumOfVnodeQueryThreads = cfgGetItem(pCfg, "numOfVnodeQueryThreads")->i32;
  tsNumOfVnodeStreamThreads = cfgGetItem(pCfg, "numOfVnodeStreamThreads")->i32;
  tsNumOfVnodeFetchThreads = cfgGetItem(pCfg, "numOfVnodeFetchThreads")->i32;
  tsNumOfVnodeWriteThreads = cfgGetItem(pCfg, "numOfVnodeWriteThreads")->i32;
  tsNumOfVnodeSyncThreads = cfgGetItem(pCfg, "numOfVnodeSyncThreads")->i32;
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
  tsTtlUnit = cfgGetItem(pCfg, "ttlUnit")->i32;
  tsTtlPushInterval = cfgGetItem(pCfg, "ttlPushInterval")->i32;

  tsStartUdfd = cfgGetItem(pCfg, "udf")->bval;

  if (tsQueryBufferSize >= 0) {
    tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
  }
  GRANT_CFG_GET;
  return 0;
}

void taosLocalCfgForbiddenToChange(char *name, bool *forbidden) {
  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));

  if (strcasecmp("charset", name) == 0) {
    *forbidden = true;
    return;
  }
  GRANT_CFG_CHECK;

  *forbidden = false;
}

int32_t taosSetCfg(SConfig *pCfg, char *name) {
  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));

  switch (lowcaseName[0]) {
    case 'a': {
      if (strcasecmp("asyncLog", name) == 0) {
        tsAsyncLog = cfgGetItem(pCfg, "asyncLog")->bval;
      }
      break;
    }
    case 'b': {
      if (strcasecmp("bnodeShmSize", name) == 0) {
        tsBnodeShmSize = cfgGetItem(pCfg, "bnodeShmSize")->i32;
      }
      break;
    }
    case 'c': {
      if (strcasecmp("charset", name) == 0) {
        const char *locale = cfgGetItem(pCfg, "locale")->str;
        const char *charset = cfgGetItem(pCfg, "charset")->str;
        taosSetSystemLocale(locale, charset);
        osSetSystemLocale(locale, charset);
      } else if (strcasecmp("compressMsgSize", name) == 0) {
        tsCompressMsgSize = cfgGetItem(pCfg, "compressMsgSize")->i32;
      } else if (strcasecmp("compressColData", name) == 0) {
        tsCompressColData = cfgGetItem(pCfg, "compressColData")->i32;
      } else if (strcasecmp("countAlwaysReturnValue", name) == 0) {
        tsCountAlwaysReturnValue = cfgGetItem(pCfg, "countAlwaysReturnValue")->i32;
      } else if (strcasecmp("cDebugFlag", name) == 0) {
        cDebugFlag = cfgGetItem(pCfg, "cDebugFlag")->i32;
      }
      break;
    }
    case 'd': {
      if (strcasecmp("dDebugFlag", name) == 0) {
        dDebugFlag = cfgGetItem(pCfg, "dDebugFlag")->i32;
      }
      break;
    }
    case 'e': {
      if (strcasecmp("enableCoreFile", name) == 0) {
        bool enableCore = cfgGetItem(pCfg, "enableCoreFile")->bval;
        taosSetCoreDump(enableCore);
      }
      break;
    }
    case 'f': {
      if (strcasecmp("fqdn", name) == 0) {
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
      } else if (strcasecmp("firstEp", name) == 0) {
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
      } else if (strcasecmp("fsDebugFlag", name) == 0) {
        fsDebugFlag = cfgGetItem(pCfg, "fsDebugFlag")->i32;
      }
      break;
    }
    case 'i': {
      if (strcasecmp("idxDebugFlag", name) == 0) {
        idxDebugFlag = cfgGetItem(pCfg, "idxDebugFlag")->i32;
      }
      break;
    }
    case 'j': {
      if (strcasecmp("jniDebugFlag", name) == 0) {
        jniDebugFlag = cfgGetItem(pCfg, "jniDebugFlag")->i32;
      }
      break;
    }
    case 'k': {
      break;
    }
    case 'l': {
      if (strcasecmp("locale", name) == 0) {
        const char *locale = cfgGetItem(pCfg, "locale")->str;
        const char *charset = cfgGetItem(pCfg, "charset")->str;
        taosSetSystemLocale(locale, charset);
        osSetSystemLocale(locale, charset);
      } else if (strcasecmp("logDir", name) == 0) {
        tstrncpy(tsLogDir, cfgGetItem(pCfg, "logDir")->str, PATH_MAX);
        taosExpandDir(tsLogDir, tsLogDir, PATH_MAX);
      } else if (strcasecmp("logKeepDays", name) == 0) {
        tsLogKeepDays = cfgGetItem(pCfg, "logKeepDays")->i32;
      }
      break;
    }
    case 'm': {
      switch (lowcaseName[1]) {
        case 'a': {
          if (strcasecmp("maxShellConns", name) == 0) {
            tsMaxShellConns = cfgGetItem(pCfg, "maxShellConns")->i32;
          } else if (strcasecmp("maxNumOfDistinctRes", name) == 0) {
            tsMaxNumOfDistinctResults = cfgGetItem(pCfg, "maxNumOfDistinctRes")->i32;
          }
          break;
        }
        case 'd': {
          if (strcasecmp("mDebugFlag", name) == 0) {
            mDebugFlag = cfgGetItem(pCfg, "mDebugFlag")->i32;
          }
          break;
        }
        case 'i': {
          if (strcasecmp("minimalTmpDirGB", name) == 0) {
            tsTempSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalTmpDirGB")->fval) * 1024 * 1024 * 1024);
          } else if (strcasecmp("minimalDataDirGB", name) == 0) {
            tsDataSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalDataDirGB")->fval) * 1024 * 1024 * 1024);
          } else if (strcasecmp("minSlidingTime", name) == 0) {
            tsMinSlidingTime = cfgGetItem(pCfg, "minSlidingTime")->i32;
          } else if (strcasecmp("minIntervalTime", name) == 0) {
            tsMinIntervalTime = cfgGetItem(pCfg, "minIntervalTime")->i32;
          } else if (strcasecmp("minimalLogDirGB", name) == 0) {
            tsLogSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalLogDirGB")->fval) * 1024 * 1024 * 1024);
          }
          break;
        }
        case 'n': {
          if (strcasecmp("mnodeShmSize", name) == 0) {
            tsMnodeShmSize = cfgGetItem(pCfg, "mnodeShmSize")->i32;
          }
          break;
        }
        case 'o': {
          if (strcasecmp("monitor", name) == 0) {
            tsEnableMonitor = cfgGetItem(pCfg, "monitor")->bval;
          } else if (strcasecmp("monitorInterval", name) == 0) {
            tsMonitorInterval = cfgGetItem(pCfg, "monitorInterval")->i32;
          } else if (strcasecmp("monitorFqdn", name) == 0) {
            tstrncpy(tsMonitorFqdn, cfgGetItem(pCfg, "monitorFqdn")->str, TSDB_FQDN_LEN);
          } else if (strcasecmp("monitorPort", name) == 0) {
            tsMonitorPort = (uint16_t)cfgGetItem(pCfg, "monitorPort")->i32;
          } else if (strcasecmp("monitorMaxLogs", name) == 0) {
            tsMonitorMaxLogs = cfgGetItem(pCfg, "monitorMaxLogs")->i32;
          } else if (strcasecmp("monitorComp", name) == 0) {
            tsMonitorComp = cfgGetItem(pCfg, "monitorComp")->bval;
          }
          break;
        }
        case 'q': {
          if (strcasecmp("mqRebalanceInterval", name) == 0) {
            tsMqRebalanceInterval = cfgGetItem(pCfg, "mqRebalanceInterval")->i32;
          }
          break;
        }
        case 'u': {
          if (strcasecmp("multiProcess", name) == 0) {
            tsMultiProcess = cfgGetItem(pCfg, "multiProcess")->bval;
          } else if (strcasecmp("udfDebugFlag", name) == 0) {
            udfDebugFlag = cfgGetItem(pCfg, "udfDebugFlag")->i32;
          }
          break;
        }
        default:
          terrno = TSDB_CODE_CFG_NOT_FOUND;
          return -1;
      }
      break;
    }
    case 'n': {
      if (strcasecmp("numOfTaskQueueThreads", name) == 0) {
        tsNumOfTaskQueueThreads = cfgGetItem(pCfg, "numOfTaskQueueThreads")->i32;
      } else if (strcasecmp("numOfRpcThreads", name) == 0) {
        tsNumOfRpcThreads = cfgGetItem(pCfg, "numOfRpcThreads")->i32;
      } else if (strcasecmp("numOfCommitThreads", name) == 0) {
        tsNumOfCommitThreads = cfgGetItem(pCfg, "numOfCommitThreads")->i32;
      } else if (strcasecmp("numOfMnodeReadThreads", name) == 0) {
        tsNumOfMnodeReadThreads = cfgGetItem(pCfg, "numOfMnodeReadThreads")->i32;
      } else if (strcasecmp("numOfVnodeQueryThreads", name) == 0) {
        tsNumOfVnodeQueryThreads = cfgGetItem(pCfg, "numOfVnodeQueryThreads")->i32;
      } else if (strcasecmp("numOfVnodeFetchThreads", name) == 0) {
        tsNumOfVnodeFetchThreads = cfgGetItem(pCfg, "numOfVnodeFetchThreads")->i32;
      } else if (strcasecmp("numOfVnodeWriteThreads", name) == 0) {
        tsNumOfVnodeWriteThreads = cfgGetItem(pCfg, "numOfVnodeWriteThreads")->i32;
      } else if (strcasecmp("numOfVnodeSyncThreads", name) == 0) {
        tsNumOfVnodeSyncThreads = cfgGetItem(pCfg, "numOfVnodeSyncThreads")->i32;
      } else if (strcasecmp("numOfQnodeQueryThreads", name) == 0) {
        tsNumOfQnodeQueryThreads = cfgGetItem(pCfg, "numOfQnodeQueryThreads")->i32;
      } else if (strcasecmp("numOfQnodeFetchThreads", name) == 0) {
        tsNumOfQnodeFetchThreads = cfgGetItem(pCfg, "numOfQnodeFetchThreads")->i32;
      } else if (strcasecmp("numOfSnodeSharedThreads", name) == 0) {
        tsNumOfSnodeSharedThreads = cfgGetItem(pCfg, "numOfSnodeSharedThreads")->i32;
      } else if (strcasecmp("numOfSnodeUniqueThreads", name) == 0) {
        tsNumOfSnodeUniqueThreads = cfgGetItem(pCfg, "numOfSnodeUniqueThreads")->i32;
      } else if (strcasecmp("numOfLogLines", name) == 0) {
        tsNumOfLogLines = cfgGetItem(pCfg, "numOfLogLines")->i32;
      }
      break;
    }
    case 'p': {
      if (strcasecmp("printAuth", name) == 0) {
        tsPrintAuth = cfgGetItem(pCfg, "printAuth")->bval;
      }
      break;
    }
    case 'q': {
      if (strcasecmp("queryPolicy", name) == 0) {
        tsQueryPolicy = cfgGetItem(pCfg, "queryPolicy")->i32;
      } else if (strcasecmp("querySmaOptimize", name) == 0) {
        tsQuerySmaOptimize = cfgGetItem(pCfg, "querySmaOptimize")->i32;
      } else if (strcasecmp("queryBufferSize", name) == 0) {
        tsQueryBufferSize = cfgGetItem(pCfg, "queryBufferSize")->i32;
        if (tsQueryBufferSize >= 0) {
          tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
        }
      } else if (strcasecmp("qnodeShmSize", name) == 0) {
        tsQnodeShmSize = cfgGetItem(pCfg, "qnodeShmSize")->i32;
      } else if (strcasecmp("qDebugFlag", name) == 0) {
        qDebugFlag = cfgGetItem(pCfg, "qDebugFlag")->i32;
      }
      break;
    }
    case 'r': {
      if (strcasecmp("rpcQueueMemoryAllowed", name) == 0) {
        tsRpcQueueMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64;
      } else if (strcasecmp("rpcDebugFlag", name) == 0) {
        rpcDebugFlag = cfgGetItem(pCfg, "rpcDebugFlag")->i32;
      }
      break;
    }
    case 's': {
      if (strcasecmp("secondEp", name) == 0) {
        SConfigItem *pSecondpItem = cfgGetItem(pCfg, "secondEp");
        SEp          secondEp = {0};
        taosGetFqdnPortFromEp(strlen(pSecondpItem->str) == 0 ? tsFirst : pSecondpItem->str, &secondEp);
        snprintf(tsSecond, sizeof(tsSecond), "%s:%u", secondEp.fqdn, secondEp.port);
        cfgSetItem(pCfg, "secondEp", tsSecond, pSecondpItem->stype);
      } else if (strcasecmp("smlChildTableName", name) == 0) {
        tstrncpy(tsSmlChildTableName, cfgGetItem(pCfg, "smlChildTableName")->str, TSDB_TABLE_NAME_LEN);
      } else if (strcasecmp("smlTagName", name) == 0) {
        tstrncpy(tsSmlTagName, cfgGetItem(pCfg, "smlTagName")->str, TSDB_COL_NAME_LEN);
      } else if (strcasecmp("smlDataFormat", name) == 0) {
        tsSmlDataFormat = cfgGetItem(pCfg, "smlDataFormat")->bval;
      } else if (strcasecmp("shellActivityTimer", name) == 0) {
        tsShellActivityTimer = cfgGetItem(pCfg, "shellActivityTimer")->i32;
      } else if (strcasecmp("supportVnodes", name) == 0) {
        tsNumOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
      } else if (strcasecmp("statusInterval", name) == 0) {
        tsStatusInterval = cfgGetItem(pCfg, "statusInterval")->i32;
      } else if (strcasecmp("snodeShmSize", name) == 0) {
        tsSnodeShmSize = cfgGetItem(pCfg, "snodeShmSize")->i32;
      } else if (strcasecmp("serverPort", name) == 0) {
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
      } else if (strcasecmp("sDebugFlag", name) == 0) {
        sDebugFlag = cfgGetItem(pCfg, "sDebugFlag")->i32;
      } else if (strcasecmp("smaDebugFlag", name) == 0) {
        smaDebugFlag = cfgGetItem(pCfg, "smaDebugFlag")->i32;
      }
      break;
    }
    case 't': {
      if (strcasecmp("timezone", name) == 0) {
        SConfigItem *pItem = cfgGetItem(pCfg, "timezone");
        osSetTimezone(pItem->str);
        uDebug("timezone format changed from %s to %s", pItem->str, tsTimezoneStr);
        cfgSetItem(pCfg, "timezone", tsTimezoneStr, pItem->stype);
      } else if (strcasecmp("tempDir", name) == 0) {
        tstrncpy(tsTempDir, cfgGetItem(pCfg, "tempDir")->str, PATH_MAX);
        taosExpandDir(tsTempDir, tsTempDir, PATH_MAX);
        if (taosMulMkDir(tsTempDir) != 0) {
          uError("failed to create tempDir:%s since %s", tsTempDir, terrstr());
          return -1;
        }
      } else if (strcasecmp("tdbDebugFlag", name) == 0) {
        tdbDebugFlag = cfgGetItem(pCfg, "tdbDebugFlag")->i32;
      } else if (strcasecmp("telemetryReporting", name) == 0) {
        tsEnableTelem = cfgGetItem(pCfg, "telemetryReporting")->bval;
      } else if (strcasecmp("telemetryInterval", name) == 0) {
        tsTelemInterval = cfgGetItem(pCfg, "telemetryInterval")->i32;
      } else if (strcasecmp("telemetryServer", name) == 0) {
        tstrncpy(tsTelemServer, cfgGetItem(pCfg, "telemetryServer")->str, TSDB_FQDN_LEN);
      } else if (strcasecmp("telemetryPort", name) == 0) {
        tsTelemPort = (uint16_t)cfgGetItem(pCfg, "telemetryPort")->i32;
      } else if (strcasecmp("transPullupInterval", name) == 0) {
        tsTransPullupInterval = cfgGetItem(pCfg, "transPullupInterval")->i32;
      } else if (strcasecmp("ttlUnit", name) == 0) {
        tsTtlUnit = cfgGetItem(pCfg, "ttlUnit")->i32;
      } else if (strcasecmp("ttlPushInterval", name) == 0) {
        tsTtlPushInterval = cfgGetItem(pCfg, "ttlPushInterval")->i32;
      } else if (strcasecmp("tmrDebugFlag", name) == 0) {
        tmrDebugFlag = cfgGetItem(pCfg, "tmrDebugFlag")->i32;
      } else if (strcasecmp("tsdbDebugFlag", name) == 0) {
        tsdbDebugFlag = cfgGetItem(pCfg, "tsdbDebugFlag")->i32;
      } else if (strcasecmp("tqDebugFlag", name) == 0) {
        tqDebugFlag = cfgGetItem(pCfg, "tqDebugFlag")->i32;
      }
      break;
    }
    case 'u': {
      if (strcasecmp("udf", name) == 0) {
        tsStartUdfd = cfgGetItem(pCfg, "udf")->bval;
      } else if (strcasecmp("uDebugFlag", name) == 0) {
        uDebugFlag = cfgGetItem(pCfg, "uDebugFlag")->i32;
      }
      break;
    }
    case 'v': {
      if (strcasecmp("vnodeShmSize", name) == 0) {
        tsVnodeShmSize = cfgGetItem(pCfg, "vnodeShmSize")->i32;
      } else if (strcasecmp("vDebugFlag", name) == 0) {
        vDebugFlag = cfgGetItem(pCfg, "vDebugFlag")->i32;
      }
      break;
    }
    case 'w': {
      if (strcasecmp("wDebugFlag", name) == 0) {
        wDebugFlag = cfgGetItem(pCfg, "wDebugFlag")->i32;
      }
      break;
    }
    default:
      terrno = TSDB_CODE_CFG_NOT_FOUND;
      return -1;
  }

  return 0;
}

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc) {
  if (tsCfg == NULL) osDefaultInit();

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
  if (strncasecmp(option, "debugFlag", 9) == 0) {
    int32_t flag = atoi(value);
    taosSetAllDebugFlag(flag);
    return;
  }

  if (strcasecmp(option, "resetlog") == 0) {
    taosResetLog();
    cfgDumpCfg(tsCfg, 0, false);
    return;
  }

  if (strcasecmp(option, "monitor") == 0) {
    int32_t monitor = atoi(value);
    uInfo("monitor set from %d to %d", tsEnableMonitor, monitor);
    tsEnableMonitor = monitor;
    SConfigItem *pItem = cfgGetItem(tsCfg, "monitor");
    if (pItem != NULL) {
      pItem->bval = tsEnableMonitor;
    }
    return;
  }

  const char *options[] = {
      "dDebugFlag",   "vDebugFlag",  "mDebugFlag",   "wDebugFlag",   "sDebugFlag",   "tsdbDebugFlag",
      "tqDebugFlag",  "fsDebugFlag", "udfDebugFlag", "smaDebugFlag", "idxDebugFlag", "tdbDebugFlag",
      "tmrDebugFlag", "uDebugFlag",  "smaDebugFlag", "rpcDebugFlag", "qDebugFlag",   "metaDebugFlag",
  };
  int32_t *optionVars[] = {
      &dDebugFlag,   &vDebugFlag,  &mDebugFlag,   &wDebugFlag,   &sDebugFlag,   &tsdbDebugFlag,
      &tqDebugFlag,  &fsDebugFlag, &udfDebugFlag, &smaDebugFlag, &idxDebugFlag, &tdbDebugFlag,
      &tmrDebugFlag, &uDebugFlag,  &smaDebugFlag, &rpcDebugFlag, &qDebugFlag,   &metaDebugFlag,
  };

  int32_t optionSize = tListLen(options);
  for (int32_t d = 0; d < optionSize; ++d) {
    const char *optName = options[d];
    int32_t     optLen = strlen(optName);
    if (strncasecmp(option, optName, optLen) != 0) continue;

    int32_t flag = atoi(value);
    uInfo("%s set from %d to %d", optName, *optionVars[d], flag);
    *optionVars[d] = flag;
    taosSetDebugFlag(optionVars[d], optName, flag);
    return;
  }

  uError("failed to cfg dynamic option:%s value:%s", option, value);
}

void taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal) {
  SConfigItem *pItem = cfgGetItem(tsCfg, flagName);
  if (pItem != NULL) {
    pItem->i32 = flagVal;
  }
  *pFlagPtr = flagVal;
}

void taosSetAllDebugFlag(int32_t flag) {
  if (flag <= 0) return;

  taosSetDebugFlag(&uDebugFlag, "uDebugFlag", flag);
  taosSetDebugFlag(&rpcDebugFlag, "rpcDebugFlag", flag);
  taosSetDebugFlag(&jniDebugFlag, "jniDebugFlag", flag);
  taosSetDebugFlag(&qDebugFlag, "qDebugFlag", flag);
  taosSetDebugFlag(&cDebugFlag, "cDebugFlag", flag);
  taosSetDebugFlag(&dDebugFlag, "dDebugFlag", flag);
  taosSetDebugFlag(&vDebugFlag, "vDebugFlag", flag);
  taosSetDebugFlag(&mDebugFlag, "mDebugFlag", flag);
  taosSetDebugFlag(&wDebugFlag, "wDebugFlag", flag);
  taosSetDebugFlag(&sDebugFlag, "sDebugFlag", flag);
  taosSetDebugFlag(&tsdbDebugFlag, "tsdbDebugFlag", flag);
  taosSetDebugFlag(&tqDebugFlag, "tqDebugFlag", flag);
  taosSetDebugFlag(&fsDebugFlag, "fsDebugFlag", flag);
  taosSetDebugFlag(&udfDebugFlag, "udfDebugFlag", flag);
  taosSetDebugFlag(&smaDebugFlag, "smaDebugFlag", flag);
  taosSetDebugFlag(&idxDebugFlag, "idxDebugFlag", flag);
  taosSetDebugFlag(&tdbDebugFlag, "tdbDebugFlag", flag);
  taosSetDebugFlag(&metaDebugFlag, "metaDebugFlag", flag);
  uInfo("all debug flag are set to %d", flag);
}
