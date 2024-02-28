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
#include "defines.h"
#include "os.h"
#include "tconfig.h"
#include "tgrant.h"
#include "tlog.h"
#include "tmisce.h"
#include "tunit.h"

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include "cus_name.h"
#endif

// GRANT_CFG_DECLARE;

SConfig *tsCfg = NULL;

// cluster
char     tsFirst[TSDB_EP_LEN] = {0};
char     tsSecond[TSDB_EP_LEN] = {0};
char     tsLocalFqdn[TSDB_FQDN_LEN] = {0};
char     tsLocalEp[TSDB_EP_LEN] = {0};  // Local End Point, hostname:port
char     tsVersionName[16] = "community";
uint16_t tsServerPort = 6030;
int32_t  tsVersion = 30000000;
int32_t  tsStatusInterval = 1;  // second
int32_t  tsNumOfSupportVnodes = 256;

// common
int32_t tsMaxShellConns = 50000;
int32_t tsShellActivityTimer = 3;  // second

// queue & threads
int32_t tsNumOfRpcThreads = 1;
int32_t tsNumOfRpcSessions = 30000;
int32_t tsTimeToGetAvailableConn = 500000;
int32_t tsKeepAliveIdle = 60;

int32_t tsNumOfCommitThreads = 2;
int32_t tsNumOfTaskQueueThreads = 4;
int32_t tsNumOfMnodeQueryThreads = 4;
int32_t tsNumOfMnodeFetchThreads = 1;
int32_t tsNumOfMnodeReadThreads = 1;
int32_t tsNumOfVnodeQueryThreads = 4;
float   tsRatioOfVnodeStreamThreads = 2.0;
int32_t tsNumOfVnodeFetchThreads = 4;
int32_t tsNumOfVnodeRsmaThreads = 2;
int32_t tsNumOfQnodeQueryThreads = 4;
int32_t tsNumOfQnodeFetchThreads = 1;
int32_t tsNumOfSnodeStreamThreads = 4;
int32_t tsNumOfSnodeWriteThreads = 1;
int32_t tsMaxStreamBackendCache = 128;  // M
int32_t tsPQSortMemThreshold = 16;      // M

// sync raft
int32_t tsElectInterval = 25 * 1000;
int32_t tsHeartbeatInterval = 1000;
int32_t tsHeartbeatTimeout = 20 * 1000;
int32_t tsSnapReplMaxWaitN = 128;

// mnode
int64_t tsMndSdbWriteDelta = 200;
int64_t tsMndLogRetention = 2000;
int8_t  tsGrant = 1;
int32_t tsMndGrantMode = 0;
bool    tsMndSkipGrant = false;
bool    tsEnableWhiteList = false;  // ip white list cfg

// dnode
int64_t tsDndStart = 0;
int64_t tsDndStartOsUptime = 0;
int64_t tsDndUpTime = 0;

// monitor
bool     tsEnableMonitor = true;
int32_t  tsMonitorInterval = 30;
char     tsMonitorFqdn[TSDB_FQDN_LEN] = {0};
uint16_t tsMonitorPort = 6043;
int32_t  tsMonitorMaxLogs = 100;
bool     tsMonitorComp = false;
bool     tsMonitorLogProtocol = false;
int32_t  tsMonitorIntervalForBasic = 30;
bool     tsMonitorForceV2 = true;

// audit
bool    tsEnableAudit = true;
bool    tsEnableAuditCreateTable = true;
int32_t tsAuditInterval = 5000;

// telem
#ifdef TD_ENTERPRISE
bool tsEnableTelem = false;
#else
bool    tsEnableTelem = true;
#endif
int32_t  tsTelemInterval = 43200;
char     tsTelemServer[TSDB_FQDN_LEN] = "telemetry.tdengine.com";
uint16_t tsTelemPort = 80;
char    *tsTelemUri = "/report";

#ifdef TD_ENTERPRISE
bool tsEnableCrashReport = false;
#else
bool    tsEnableCrashReport = true;
#endif
char *tsClientCrashReportUri = "/ccrashreport";
char *tsSvrCrashReportUri = "/dcrashreport";

// schemaless
bool tsSmlDot2Underline = true;
char tsSmlTsDefaultName[TSDB_COL_NAME_LEN] = "_ts";
char tsSmlTagName[TSDB_COL_NAME_LEN] = "_tag_null";
char tsSmlChildTableName[TSDB_TABLE_NAME_LEN] = "";  // user defined child table name can be specified in tag value.
char tsSmlAutoChildTableNameDelimiter[TSDB_TABLE_NAME_LEN] = "";
// If set to empty system will generate table name using MD5 hash.
// true means that the name and order of cols in each line are the same(only for influx protocol)
// bool    tsSmlDataFormat = false;
// int32_t tsSmlBatchSize = 10000;

// checkpoint backup
char    tsSnodeAddress[TSDB_FQDN_LEN] = {0};
int32_t tsRsyncPort = 873;
#ifdef WINDOWS
char tsCheckpointBackupDir[PATH_MAX] = "C:\\TDengine\\data\\backup\\checkpoint\\";
#else
char    tsCheckpointBackupDir[PATH_MAX] = "/var/lib/taos/backup/checkpoint/";
#endif

// tmq
int32_t tmqMaxTopicNum = 20;
// query
int32_t tsQueryPolicy = 1;
int32_t tsQueryRspPolicy = 0;
int64_t tsQueryMaxConcurrentTables = 200;  // unit is TSDB_TABLE_NUM_UNIT
bool    tsEnableQueryHb = true;
bool    tsEnableScience = false;  // on taos-cli show float and doulbe with scientific notation if true
int32_t tsQuerySmaOptimize = 0;
int32_t tsQueryRsmaTolerance = 1000;  // the tolerance time (ms) to judge from which level to query rsma data.
bool    tsQueryPlannerTrace = false;
int32_t tsQueryNodeChunkSize = 32 * 1024;
bool    tsQueryUseNodeAllocator = true;
bool    tsKeepColumnName = false;
int32_t tsRedirectPeriod = 10;
int32_t tsRedirectFactor = 2;
int32_t tsRedirectMaxPeriod = 1000;
int32_t tsMaxRetryWaitTime = 10000;
bool    tsUseAdapter = false;
int32_t tsMetaCacheMaxSize = -1;  // MB
int32_t tsSlowLogThreshold = 3;   // seconds
int32_t tsSlowLogScope = SLOW_LOG_TYPE_ALL;
int32_t tsTimeSeriesThreshold = 50;

/*
 * denote if the server needs to compress response message at the application layer to client, including query rsp,
 * metricmeta rsp, and multi-meter query rsp message body. The client compress the submit message to server.
 *
 * 0: all data are compressed
 * -1: all data are not compressed
 * other values: if the message payload size is greater than the tsCompressMsgSize, the message will be compressed.
 */
int32_t tsCompressMsgSize = -1;

// count/hyperloglog function always return values in case of all NULL data or Empty data set.
int32_t tsCountAlwaysReturnValue = 1;

// 1 ms for sliding time, the value will changed in case of time precision changed
int32_t tsMinSlidingTime = 1;

// 1 database precision unit for interval time range, changed accordingly
int32_t tsMinIntervalTime = 1;

// maximum batch rows numbers imported from a single csv load
int32_t tsMaxInsertBatchRows = 1000000;

float   tsSelectivityRatio = 1.0;
int32_t tsTagFilterResCacheSize = 1024 * 10;
char    tsTagFilterCache = 0;

// the maximum allowed query buffer size during query processing for each data node.
// -1 no limit (default)
// 0  no query allowed, queries are disabled
// positive value (in MB)
int32_t tsQueryBufferSize = -1;
int64_t tsQueryBufferSizeBytes = -1;
int32_t tsCacheLazyLoadThreshold = 500;

int32_t  tsDiskCfgNum = 0;
SDiskCfg tsDiskCfg[TFS_MAX_DISKS] = {0};
int64_t  tsMinDiskFreeSize = TFS_MIN_DISK_FREE_SIZE;

// stream scheduler
bool tsDeployOnSnode = true;

/*
 * minimum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 60000L
 *     TSDB_TIME_PRECISION_MICRO: 60000000L
 *     TSDB_TIME_PRECISION_NANO:  60000000000L
 */
int64_t tsTickPerMin[] = {60000L, 60000000L, 60000000000L};
/*
 * millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 3600000L
 *     TSDB_TIME_PRECISION_MICRO: 3600000000L
 *     TSDB_TIME_PRECISION_NANO:  3600000000000L
 */
int64_t tsTickPerHour[] = {3600000L, 3600000000L, 3600000000000L};

// lossy compress 7
char tsLossyColumns[32] = "";  // "float|double" means all float and double columns can be lossy compressed.  set empty
                               // can close lossy compress.
// below option can take effect when tsLossyColumns not empty
float    tsFPrecision = 1E-8;                   // float column precision
double   tsDPrecision = 1E-16;                  // double column precision
uint32_t tsMaxRange = 500;                      // max quantization intervals
uint32_t tsCurRange = 100;                      // current quantization intervals
bool     tsIfAdtFse = false;                    // ADT-FSE algorithom or original huffman algorithom
char     tsCompressor[32] = "ZSTD_COMPRESSOR";  // ZSTD_COMPRESSOR or GZIP_COMPRESSOR

// udf
#ifdef WINDOWS
bool tsStartUdfd = false;
#else
bool    tsStartUdfd = true;
#endif

// wal
int64_t tsWalFsyncDataSizeLimit = (100 * 1024 * 1024L);

// ttl
bool    tsTtlChangeOnWrite = false;  // if true, ttl delete time changes on last write
int32_t tsTtlFlushThreshold = 100;   /* maximum number of dirty items in memory.
                                      * if -1, flush will not be triggered by write-ops
                                      */
int32_t tsTtlBatchDropNum = 10000;   // number of tables dropped per batch

// internal
int32_t tsTransPullupInterval = 2;
int32_t tsCompactPullupInterval = 10;
int32_t tsMqRebalanceInterval = 2;
int32_t tsStreamCheckpointInterval = 300;    // 5min by default.
float   tsSinkDataRate = 2.0;
int32_t tsStreamNodeCheckInterval = 16;
int32_t tsTtlUnit = 86400;
int32_t tsTtlPushIntervalSec = 10;
int32_t tsTrimVDbIntervalSec = 60 * 60;  // interval of trimming db in all vgroups
int32_t tsGrantHBInterval = 60;
int32_t tsUptimeInterval = 300;    // seconds
char    tsUdfdResFuncs[512] = "";  // udfd resident funcs that teardown when udfd exits
char    tsUdfdLdLibPath[512] = "";
bool    tsDisableStream = false;
int64_t tsStreamBufferSize = 128 * 1024 * 1024;
bool    tsFilterScalarMode = false;
int     tsResolveFQDNRetryTime = 100;  // seconds
int     tsStreamAggCnt = 1000;
bool    tsDisableCount = true;

char   tsS3Endpoint[TSDB_FQDN_LEN] = "<endpoint>";
char   tsS3AccessKey[TSDB_FQDN_LEN] = "<accesskey>";
char   tsS3AccessKeyId[TSDB_FQDN_LEN] = "<accesskeyid>";
char   tsS3AccessKeySecret[TSDB_FQDN_LEN] = "<accesskeysecrect>";
char   tsS3BucketName[TSDB_FQDN_LEN] = "<bucketname>";
char   tsS3AppId[TSDB_FQDN_LEN] = "<appid>";
int8_t tsS3Enabled = false;
int8_t tsS3StreamEnabled = false;

int8_t tsS3Https = true;
char   tsS3Hostname[TSDB_FQDN_LEN] = "<hostname>";

int32_t tsS3BlockSize = -1;        // number of tsdb pages (4096)
int32_t tsS3BlockCacheSize = 16;   // number of blocks
int32_t tsS3PageCacheSize = 4096;  // number of pages
int32_t tsS3UploadDelaySec = 60 * 60 * 24;

bool tsExperimental = true;

#ifndef _STORAGE
int32_t taosSetTfsCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  memset(tsDataDir, 0, PATH_MAX);

  int32_t size = taosArrayGetSize(pItem->array);
  tsDiskCfgNum = 1;
  tstrncpy(tsDiskCfg[0].dir, pItem->str, TSDB_FILENAME_LEN);
  tsDiskCfg[0].level = 0;
  tsDiskCfg[0].primary = 1;
  tstrncpy(tsDataDir, pItem->str, PATH_MAX);
  if (taosMulMkDir(tsDataDir) != 0) {
    uError("failed to create dataDir:%s", tsDataDir);
    return -1;
  }
  return 0;
}
#else
int32_t taosSetTfsCfg(SConfig *pCfg);
#endif

int32_t taosSetS3Cfg(SConfig *pCfg) {
  tstrncpy(tsS3AccessKey, cfgGetItem(pCfg, "s3Accesskey")->str, TSDB_FQDN_LEN);
  if (tsS3AccessKey[0] == '<') {
    return 0;
  }
  char *colon = strchr(tsS3AccessKey, ':');
  if (!colon) {
    uError("invalid access key:%s", tsS3AccessKey);
    return -1;
  }
  *colon = '\0';
  tstrncpy(tsS3AccessKeyId, tsS3AccessKey, TSDB_FQDN_LEN);
  tstrncpy(tsS3AccessKeySecret, colon + 1, TSDB_FQDN_LEN);
  tstrncpy(tsS3Endpoint, cfgGetItem(pCfg, "s3Endpoint")->str, TSDB_FQDN_LEN);
  tstrncpy(tsS3BucketName, cfgGetItem(pCfg, "s3BucketName")->str, TSDB_FQDN_LEN);
  char *proto = strstr(tsS3Endpoint, "https://");
  if (!proto) {
    tsS3Https = false;
    tstrncpy(tsS3Hostname, tsS3Endpoint + 7, TSDB_FQDN_LEN);
  } else {
    tstrncpy(tsS3Hostname, tsS3Endpoint + 8, TSDB_FQDN_LEN);
  }

  char *cos = strstr(tsS3Endpoint, "cos.");
  if (cos) {
    char *appid = strrchr(tsS3BucketName, '-');
    if (!appid) {
      uError("failed to locate appid in bucket:%s", tsS3BucketName);
      return -1;
    } else {
      tstrncpy(tsS3AppId, appid + 1, TSDB_FQDN_LEN);
    }
  }
  if (tsS3BucketName[0] != '<') {
#if defined(USE_COS) || defined(USE_S3)
    if (tsDiskCfgNum > 1) tsS3Enabled = true;
    tsS3StreamEnabled = true;
#endif
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
  char  lastC = cfgDir[strlen(cfgDir) - 1];
  char *tdDirsep = TD_DIRSEP;
  if (lastC == '\\' || lastC == '/') {
    tdDirsep = "";
  }
  if (taosIsDir(cfgDir)) {
#ifdef CUS_PROMPT
    snprintf(cfgFile, sizeof(cfgFile),
             "%s"
             "%s"
             "%s.cfg",
             cfgDir, tdDirsep, CUS_PROMPT);
#else
    snprintf(cfgFile, sizeof(cfgFile),
             "%s"
             "%s"
             "taos.cfg",
             cfgDir, tdDirsep);
#endif
  } else {
    tstrncpy(cfgFile, cfgDir, sizeof(cfgDir));
  }

  if (apolloUrl != NULL && apolloUrl[0] == '\0') {
    cfgGetApollUrl(envCmd, envFile, apolloUrl);
  }

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
  if (cfgAddDir(pCfg, "configDir", configDir, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddDir(pCfg, "scriptDir", configDir, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddDir(pCfg, "logDir", tsLogDir, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalLogDirGB", 1.0f, 0.001f, 10000000, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfLogLines", tsNumOfLogLines, 1000, 2000000000, CFG_SCOPE_BOTH, CFG_DYN_ENT_BOTH) != 0)
    return -1;
  if (cfgAddBool(pCfg, "asyncLog", tsAsyncLog, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "logKeepDays", 0, -365000, 365000, CFG_SCOPE_BOTH, CFG_DYN_ENT_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "debugFlag", 0, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "simDebugFlag", simDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "tmrDebugFlag", tmrDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "uDebugFlag", uDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcDebugFlag", rpcDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "jniDebugFlag", jniDebugFlag, 0, 255, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddInt32(pCfg, "qDebugFlag", qDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "cDebugFlag", cDebugFlag, 0, 255, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  return 0;
}

static int32_t taosAddServerLogCfg(SConfig *pCfg) {
  if (cfgAddInt32(pCfg, "dDebugFlag", dDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "vDebugFlag", vDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "mDebugFlag", mDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "wDebugFlag", wDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "sDebugFlag", sDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "tsdbDebugFlag", tsdbDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "tqDebugFlag", tqDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "fsDebugFlag", fsDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "udfDebugFlag", udfDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "smaDebugFlag", smaDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "idxDebugFlag", idxDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "tdbDebugFlag", tdbDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "metaDebugFlag", metaDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "stDebugFlag", stDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "sndDebugFlag", sndDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  return 0;
}

static int32_t taosAddClientCfg(SConfig *pCfg) {
  char    defaultFqdn[TSDB_FQDN_LEN] = {0};
  int32_t defaultServerPort = 6030;
  if (taosGetFqdn(defaultFqdn) != 0) {
    strcpy(defaultFqdn, "localhost");
  }

  if (cfgAddString(pCfg, "firstEp", "", CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddString(pCfg, "secondEp", "", CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddString(pCfg, "fqdn", defaultFqdn, CFG_SCOPE_SERVER, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddInt32(pCfg, "serverPort", defaultServerPort, 1, 65056, CFG_SCOPE_SERVER, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddDir(pCfg, "tempDir", tsTempDir, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalTmpDirGB", 1.0f, 0.001f, 10000000, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddInt32(pCfg, "shellActivityTimer", tsShellActivityTimer, 1, 120, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "compressMsgSize", tsCompressMsgSize, -1, 100000000, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "queryPolicy", tsQueryPolicy, 1, 4, CFG_SCOPE_CLIENT, CFG_DYN_ENT_CLIENT) != 0) return -1;
  if (cfgAddBool(pCfg, "enableQueryHb", tsEnableQueryHb, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddBool(pCfg, "enableScience", tsEnableScience, CFG_SCOPE_CLIENT, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "querySmaOptimize", tsQuerySmaOptimize, 0, 1, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddBool(pCfg, "queryPlannerTrace", tsQueryPlannerTrace, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddInt32(pCfg, "queryNodeChunkSize", tsQueryNodeChunkSize, 1024, 128 * 1024, CFG_SCOPE_CLIENT,
                  CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddBool(pCfg, "queryUseNodeAllocator", tsQueryUseNodeAllocator, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddBool(pCfg, "keepColumnName", tsKeepColumnName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddString(pCfg, "smlChildTableName", tsSmlChildTableName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddString(pCfg, "smlAutoChildTableNameDelimiter", tsSmlAutoChildTableNameDelimiter, CFG_SCOPE_CLIENT,
                   CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddString(pCfg, "smlTagName", tsSmlTagName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddString(pCfg, "smlTsDefaultName", tsSmlTsDefaultName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddBool(pCfg, "smlDot2Underline", tsSmlDot2Underline, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  //  if (cfgAddBool(pCfg, "smlDataFormat", tsSmlDataFormat, CFG_SCOPE_CLIENT, CFG_DYN_NONE) != 0) return -1;
  //  if (cfgAddInt32(pCfg, "smlBatchSize", tsSmlBatchSize, 1, INT32_MAX, CFG_SCOPE_CLIENT, CFG_DYN_NONE) != 0)
  //  return -1;
  if (cfgAddInt32(pCfg, "maxShellConns", tsMaxShellConns, 10, 50000000, CFG_SCOPE_CLIENT, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxInsertBatchRows", tsMaxInsertBatchRows, 1, INT32_MAX, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "maxRetryWaitTime", tsMaxRetryWaitTime, 0, 86400000, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddBool(pCfg, "useAdapter", tsUseAdapter, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddBool(pCfg, "crashReporting", tsEnableCrashReport, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddInt64(pCfg, "queryMaxConcurrentTables", tsQueryMaxConcurrentTables, INT64_MIN, INT64_MAX, CFG_SCOPE_CLIENT,
                  CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "metaCacheMaxSize", tsMetaCacheMaxSize, -1, INT32_MAX, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "slowLogThreshold", tsSlowLogThreshold, 0, INT32_MAX, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddString(pCfg, "slowLogScope", "", CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;

  tsNumOfRpcThreads = tsNumOfCores / 2;
  tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 2, TSDB_MAX_RPC_THREADS);
  if (cfgAddInt32(pCfg, "numOfRpcThreads", tsNumOfRpcThreads, 1, 1024, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;

  tsNumOfRpcSessions = TRANGE(tsNumOfRpcSessions, 100, 100000);
  if (cfgAddInt32(pCfg, "numOfRpcSessions", tsNumOfRpcSessions, 1, 100000, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0)
    return -1;

  tsTimeToGetAvailableConn = TRANGE(tsTimeToGetAvailableConn, 20, 10000000);
  if (cfgAddInt32(pCfg, "timeToGetAvailableConn", tsTimeToGetAvailableConn, 20, 1000000, CFG_SCOPE_BOTH,
                  CFG_DYN_NONE) != 0)
    return -1;

  tsKeepAliveIdle = TRANGE(tsKeepAliveIdle, 1, 72000);
  if (cfgAddInt32(pCfg, "keepAliveIdle", tsKeepAliveIdle, 1, 7200000, CFG_SCOPE_BOTH, CFG_DYN_ENT_BOTH) != 0) return -1;

  tsNumOfTaskQueueThreads = tsNumOfCores / 2;
  tsNumOfTaskQueueThreads = TMAX(tsNumOfTaskQueueThreads, 4);

  if (tsNumOfTaskQueueThreads >= 50) {
    tsNumOfTaskQueueThreads = 50;
  }
  if (cfgAddInt32(pCfg, "numOfTaskQueueThreads", tsNumOfTaskQueueThreads, 4, 1024, CFG_SCOPE_CLIENT, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddBool(pCfg, "experimental", tsExperimental, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;

  if (cfgAddBool(pCfg, "monitor", tsEnableMonitor, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorInterval", tsMonitorInterval, 1, 200000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddBool(pCfg, "disableCount", tsDisableCount, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0) return -1;
  return 0;
}

static int32_t taosAddSystemCfg(SConfig *pCfg) {
  SysNameInfo info = taosGetSysNameInfo();

  if (cfgAddTimezone(pCfg, "timezone", tsTimezoneStr, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddLocale(pCfg, "locale", tsLocale, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddCharset(pCfg, "charset", tsCharset, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "assert", tsAssert, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddBool(pCfg, "enableCoreFile", 1, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0) return -1;
  if (cfgAddFloat(pCfg, "numOfCores", tsNumOfCores, 1, 100000, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddBool(pCfg, "ssd42", tsSSE42Enable, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "avx", tsAVXEnable, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "avx2", tsAVX2Enable, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "fma", tsFMAEnable, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "avx512", tsAVX512Enable, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "simdEnable", tsSIMDEnable, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "tagFilterCache", tsTagFilterCache, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddInt64(pCfg, "openMax", tsOpenMax, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
#if !defined(_ALPINE)
  if (cfgAddInt64(pCfg, "streamMax", tsStreamMax, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
#endif
  if (cfgAddInt32(pCfg, "pageSizeKB", tsPageSizeKB, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt64(pCfg, "totalMemoryKB", tsTotalMemoryKB, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "os sysname", info.sysname, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "os nodename", info.nodename, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "os release", info.release, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "os version", info.version, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "os machine", info.machine, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddString(pCfg, "version", version, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "compatible_version", compatible_version, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "gitinfo", gitinfo, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "buildinfo", buildinfo, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  return 0;
}

static int32_t taosAddServerCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "dataDir", tsDataDir, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalDataDirGB", 2.0f, 0.001f, 10000000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  tsNumOfSupportVnodes = tsNumOfCores * 2;
  tsNumOfSupportVnodes = TMAX(tsNumOfSupportVnodes, 2);
  if (cfgAddInt32(pCfg, "supportVnodes", tsNumOfSupportVnodes, 0, 4096, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddInt32(pCfg, "statusInterval", tsStatusInterval, 1, 30, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "minSlidingTime", tsMinSlidingTime, 1, 1000000, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "minIntervalTime", tsMinIntervalTime, 1, 1000000, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT) != 0)
    return -1;

  if (cfgAddInt32(pCfg, "countAlwaysReturnValue", tsCountAlwaysReturnValue, 0, 1, CFG_SCOPE_BOTH, CFG_DYN_CLIENT) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "queryBufferSize", tsQueryBufferSize, -1, 500000000000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "queryRspPolicy", tsQueryRspPolicy, 0, 1, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0) return -1;

  tsNumOfRpcThreads = tsNumOfCores / 2;
  tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 2, TSDB_MAX_RPC_THREADS);
  if (cfgAddInt32(pCfg, "numOfRpcThreads", tsNumOfRpcThreads, 1, 1024, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;

  tsNumOfRpcSessions = TRANGE(tsNumOfRpcSessions, 100, 10000);
  if (cfgAddInt32(pCfg, "numOfRpcSessions", tsNumOfRpcSessions, 1, 100000, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0)
    return -1;

  tsTimeToGetAvailableConn = TRANGE(tsTimeToGetAvailableConn, 20, 1000000);
  if (cfgAddInt32(pCfg, "timeToGetAvailableConn", tsNumOfRpcSessions, 20, 1000000, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0)
    return -1;

  tsNumOfCommitThreads = tsNumOfCores / 2;
  tsNumOfCommitThreads = TRANGE(tsNumOfCommitThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfCommitThreads", tsNumOfCommitThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  tsNumOfMnodeReadThreads = tsNumOfCores / 8;
  tsNumOfMnodeReadThreads = TRANGE(tsNumOfMnodeReadThreads, 1, 4);
  if (cfgAddInt32(pCfg, "numOfMnodeReadThreads", tsNumOfMnodeReadThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  tsNumOfVnodeQueryThreads = tsNumOfCores * 2;
  tsNumOfVnodeQueryThreads = TMAX(tsNumOfVnodeQueryThreads, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeQueryThreads", tsNumOfVnodeQueryThreads, 4, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) !=
      0)
    return -1;

  if (cfgAddFloat(pCfg, "ratioOfVnodeStreamThreads", tsRatioOfVnodeStreamThreads, 0.01, 4, CFG_SCOPE_SERVER,
                  CFG_DYN_NONE) != 0)
    return -1;

  tsNumOfVnodeFetchThreads = tsNumOfCores / 4;
  tsNumOfVnodeFetchThreads = TMAX(tsNumOfVnodeFetchThreads, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeFetchThreads", tsNumOfVnodeFetchThreads, 4, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) !=
      0)
    return -1;

  tsNumOfVnodeRsmaThreads = tsNumOfCores / 4;
  tsNumOfVnodeRsmaThreads = TMAX(tsNumOfVnodeRsmaThreads, 4);
  if (cfgAddInt32(pCfg, "numOfVnodeRsmaThreads", tsNumOfVnodeRsmaThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  tsNumOfQnodeQueryThreads = tsNumOfCores * 2;
  tsNumOfQnodeQueryThreads = TMAX(tsNumOfQnodeQueryThreads, 4);
  if (cfgAddInt32(pCfg, "numOfQnodeQueryThreads", tsNumOfQnodeQueryThreads, 4, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) !=
      0)
    return -1;

  //  tsNumOfQnodeFetchThreads = tsNumOfCores / 2;
  //  tsNumOfQnodeFetchThreads = TMAX(tsNumOfQnodeFetchThreads, 4);
  //  if (cfgAddInt32(pCfg, "numOfQnodeFetchThreads", tsNumOfQnodeFetchThreads, 1, 1024, 0) != 0) return -1;

  tsNumOfSnodeStreamThreads = tsNumOfCores / 4;
  tsNumOfSnodeStreamThreads = TRANGE(tsNumOfSnodeStreamThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfSnodeSharedThreads", tsNumOfSnodeStreamThreads, 2, 1024, CFG_SCOPE_SERVER,
                  CFG_DYN_NONE) != 0)
    return -1;

  tsNumOfSnodeWriteThreads = tsNumOfCores / 4;
  tsNumOfSnodeWriteThreads = TRANGE(tsNumOfSnodeWriteThreads, 2, 4);
  if (cfgAddInt32(pCfg, "numOfSnodeUniqueThreads", tsNumOfSnodeWriteThreads, 2, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE) !=
      0)
    return -1;

  tsRpcQueueMemoryAllowed = tsTotalMemoryKB * 1024 * 0.1;
  tsRpcQueueMemoryAllowed = TRANGE(tsRpcQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * 10LL, TSDB_MAX_MSG_SIZE * 10000LL);
  if (cfgAddInt64(pCfg, "rpcQueueMemoryAllowed", tsRpcQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * 10L, INT64_MAX,
                  CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0)
    return -1;

  if (cfgAddInt32(pCfg, "syncElectInterval", tsElectInterval, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_NONE) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "syncHeartbeatInterval", tsHeartbeatInterval, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER,
                  CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "syncHeartbeatTimeout", tsHeartbeatTimeout, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER,
                  CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "syncSnapReplMaxWaitN", tsSnapReplMaxWaitN, 16,
                  (TSDB_SYNC_SNAP_BUFFER_SIZE >> 2), CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  if (cfgAddInt64(pCfg, "mndSdbWriteDelta", tsMndSdbWriteDelta, 20, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddInt64(pCfg, "mndLogRetention", tsMndLogRetention, 500, 10000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "grantMode", tsMndGrantMode, 0, 10000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "skipGrant", tsMndSkipGrant, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddBool(pCfg, "monitor", tsEnableMonitor, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorInterval", tsMonitorInterval, 1, 200000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddString(pCfg, "monitorFqdn", tsMonitorFqdn, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorPort", tsMonitorPort, 1, 65056, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorMaxLogs", tsMonitorMaxLogs, 1, 1000000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "monitorComp", tsMonitorComp, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "monitorLogProtocol", tsMonitorLogProtocol, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "monitorIntervalForBasic", tsMonitorIntervalForBasic, 1, 200000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddBool(pCfg, "monitorForceV2", tsMonitorForceV2, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
    
  if (cfgAddBool(pCfg, "audit", tsEnableAudit, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0) return -1;
  if (cfgAddBool(pCfg, "auditCreateTable", tsEnableAuditCreateTable, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "auditInterval", tsAuditInterval, 500, 200000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddBool(pCfg, "crashReporting", tsEnableCrashReport, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "telemetryReporting", tsEnableTelem, CFG_SCOPE_BOTH, CFG_DYN_ENT_SERVER) != 0) return -1;
  if (cfgAddInt32(pCfg, "telemetryInterval", tsTelemInterval, 1, 200000, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "telemetryServer", tsTelemServer, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;
  if (cfgAddInt32(pCfg, "telemetryPort", tsTelemPort, 1, 65056, CFG_SCOPE_BOTH, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddInt32(pCfg, "rsyncPort", tsRsyncPort, 1, 65535, CFG_SCOPE_BOTH, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddString(pCfg, "snodeAddress", tsSnodeAddress, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0) return -1;
  if (cfgAddString(pCfg, "checkpointBackupDir", tsCheckpointBackupDir, CFG_SCOPE_SERVER, CFG_DYN_SERVER) != 0)
    return -1;

  if (cfgAddInt32(pCfg, "tmqMaxTopicNum", tmqMaxTopicNum, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0)
    return -1;

  if (cfgAddInt32(pCfg, "transPullupInterval", tsTransPullupInterval, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "compactPullupInterval", tsCompactPullupInterval, 1, 10000, CFG_SCOPE_SERVER,
                  CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "mqRebalanceInterval", tsMqRebalanceInterval, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "ttlUnit", tsTtlUnit, 1, 86400 * 365, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "ttlPushInterval", tsTtlPushIntervalSec, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "ttlBatchDropNum", tsTtlBatchDropNum, 0, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddBool(pCfg, "ttlChangeOnWrite", tsTtlChangeOnWrite, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "ttlFlushThreshold", tsTtlFlushThreshold, -1, 1000000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "trimVDbIntervalSec", tsTrimVDbIntervalSec, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "uptimeInterval", tsUptimeInterval, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "queryRsmaTolerance", tsQueryRsmaTolerance, 0, 900000, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "timeseriesThreshold", tsTimeSeriesThreshold, 0, 2000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) !=
      0)
    return -1;

  if (cfgAddInt64(pCfg, "walFsyncDataSizeLimit", tsWalFsyncDataSizeLimit, 100 * 1024 * 1024, INT64_MAX,
                  CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  if (cfgAddBool(pCfg, "udf", tsStartUdfd, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "udfdResFuncs", tsUdfdResFuncs, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "udfdLdLibPath", tsUdfdLdLibPath, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddBool(pCfg, "disableStream", tsDisableStream, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0) return -1;
  if (cfgAddInt64(pCfg, "streamBufferSize", tsStreamBufferSize, 0, INT64_MAX, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt64(pCfg, "streamAggCnt", tsStreamAggCnt, 2, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  if (cfgAddInt32(pCfg, "checkpointInterval", tsStreamCheckpointInterval, 60, 1200, CFG_SCOPE_SERVER,
                  CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddFloat(pCfg, "streamSinkDataRate", tsSinkDataRate, 0.1, 5, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddInt32(pCfg, "cacheLazyLoadThreshold", tsCacheLazyLoadThreshold, 0, 100000, CFG_SCOPE_SERVER,
                  CFG_DYN_ENT_SERVER) != 0)
    return -1;

  if (cfgAddString(pCfg, "lossyColumns", tsLossyColumns, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddFloat(pCfg, "fPrecision", tsFPrecision, 0.0f, 100000.0f, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddFloat(pCfg, "dPrecision", tsDPrecision, 0.0f, 1000000.0f, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxRange", tsMaxRange, 0, 65536, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "curRange", tsCurRange, 0, 65536, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddBool(pCfg, "ifAdtFse", tsIfAdtFse, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "compressor", tsCompressor, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;

  if (cfgAddBool(pCfg, "filterScalarMode", tsFilterScalarMode, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxStreamBackendCache", tsMaxStreamBackendCache, 16, 1024, CFG_SCOPE_SERVER,
                  CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "pqSortMemThreshold", tsPQSortMemThreshold, 1, 10240, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "resolveFQDNRetryTime", tsResolveFQDNRetryTime, 1, 10240, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0)
    return -1;

  if (cfgAddString(pCfg, "s3Accesskey", tsS3AccessKey, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "s3Endpoint", tsS3Endpoint, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddString(pCfg, "s3BucketName", tsS3BucketName, CFG_SCOPE_SERVER, CFG_DYN_NONE) != 0) return -1;
  if (cfgAddInt32(pCfg, "s3BlockSize", tsS3BlockSize, -1, 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (tsS3BlockSize > -1 && tsS3BlockSize < 1024) {
    uError("failed to config s3blocksize since value:%d. Valid range: -1 or [1024, 1024 * 1024]", tsS3BlockSize);
    return -1;
  }
  if (cfgAddInt32(pCfg, "s3BlockCacheSize", tsS3BlockCacheSize, 4, 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) !=
      0)
    return -1;
  if (cfgAddInt32(pCfg, "s3PageCacheSize", tsS3PageCacheSize, 4, 1024 * 1024 * 1024, CFG_SCOPE_SERVER,
                  CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddInt32(pCfg, "s3UploadDelaySec", tsS3UploadDelaySec, 60 * 1, 60 * 60 * 24 * 30, CFG_SCOPE_SERVER,
                  CFG_DYN_ENT_SERVER) != 0)
    return -1;

  // min free disk space used to check if the disk is full [50MB, 1GB]
  if (cfgAddInt64(pCfg, "minDiskFreeSize", tsMinDiskFreeSize, TFS_MIN_DISK_FREE_SIZE, 1024 * 1024 * 1024,
                  CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0)
    return -1;
  if (cfgAddBool(pCfg, "enableWhiteList", tsEnableWhiteList, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0) return -1;

  if (cfgAddBool(pCfg, "experimental", tsExperimental, CFG_SCOPE_BOTH, CFG_DYN_BOTH) != 0) return -1;

  // GRANT_CFG_ADD;
  return 0;
}

static int32_t taosUpdateServerCfg(SConfig *pCfg) {
  SConfigItem *pItem;
  ECfgSrcType  stype;
  int32_t      numOfCores;
  int64_t      totalMemoryKB;

  pItem = cfgGetItem(tsCfg, "numOfCores");
  if (pItem == NULL) {
    return -1;
  } else {
    stype = pItem->stype;
    numOfCores = pItem->fval;
  }

  pItem = cfgGetItem(tsCfg, "supportVnodes");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfSupportVnodes = numOfCores * 2;
    tsNumOfSupportVnodes = TMAX(tsNumOfSupportVnodes, 2);
    pItem->i32 = tsNumOfSupportVnodes;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfRpcThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfRpcThreads = numOfCores / 2;
    tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 2, TSDB_MAX_RPC_THREADS);
    pItem->i32 = tsNumOfRpcThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfRpcSessions");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfRpcSessions = TRANGE(tsNumOfRpcSessions, 100, 10000);
    pItem->i32 = tsNumOfRpcSessions;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "timeToGetAvailableConn");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsTimeToGetAvailableConn = TRANGE(tsTimeToGetAvailableConn, 20, 1000000);
    pItem->i32 = tsTimeToGetAvailableConn;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfCommitThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfCommitThreads = numOfCores / 2;
    tsNumOfCommitThreads = TRANGE(tsNumOfCommitThreads, 2, 4);
    pItem->i32 = tsNumOfCommitThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfMnodeReadThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfMnodeReadThreads = numOfCores / 8;
    tsNumOfMnodeReadThreads = TRANGE(tsNumOfMnodeReadThreads, 1, 4);
    pItem->i32 = tsNumOfMnodeReadThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfVnodeQueryThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfVnodeQueryThreads = numOfCores * 2;
    tsNumOfVnodeQueryThreads = TMAX(tsNumOfVnodeQueryThreads, 4);
    pItem->i32 = tsNumOfVnodeQueryThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "ratioOfVnodeStreamThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    pItem->fval = tsRatioOfVnodeStreamThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfVnodeFetchThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfVnodeFetchThreads = numOfCores / 4;
    tsNumOfVnodeFetchThreads = TMAX(tsNumOfVnodeFetchThreads, 4);
    pItem->i32 = tsNumOfVnodeFetchThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfVnodeRsmaThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfVnodeRsmaThreads = numOfCores;
    tsNumOfVnodeRsmaThreads = TMAX(tsNumOfVnodeRsmaThreads, 4);
    pItem->i32 = tsNumOfVnodeRsmaThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfQnodeQueryThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfQnodeQueryThreads = numOfCores * 2;
    tsNumOfQnodeQueryThreads = TMAX(tsNumOfQnodeQueryThreads, 4);
    pItem->i32 = tsNumOfQnodeQueryThreads;
    pItem->stype = stype;
  }

  /*
    pItem = cfgGetItem(tsCfg, "numOfQnodeFetchThreads");
    if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
      tsNumOfQnodeFetchThreads = numOfCores / 2;
      tsNumOfQnodeFetchThreads = TMAX(tsNumOfQnodeFetchThreads, 4);
      pItem->i32 = tsNumOfQnodeFetchThreads;
      pItem->stype = stype;
    }
  */

  pItem = cfgGetItem(tsCfg, "numOfSnodeSharedThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfSnodeStreamThreads = numOfCores / 4;
    tsNumOfSnodeStreamThreads = TRANGE(tsNumOfSnodeStreamThreads, 2, 4);
    pItem->i32 = tsNumOfSnodeStreamThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "numOfSnodeUniqueThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfSnodeWriteThreads = numOfCores / 4;
    tsNumOfSnodeWriteThreads = TRANGE(tsNumOfSnodeWriteThreads, 2, 4);
    pItem->i32 = tsNumOfSnodeWriteThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "totalMemoryKB");
  if (pItem == NULL) {
    return -1;
  } else {
    stype = pItem->stype;
    totalMemoryKB = pItem->i64;
  }

  pItem = cfgGetItem(tsCfg, "rpcQueueMemoryAllowed");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsRpcQueueMemoryAllowed = totalMemoryKB * 1024 * 0.1;
    tsRpcQueueMemoryAllowed = TRANGE(tsRpcQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * 10LL, TSDB_MAX_MSG_SIZE * 10000LL);
    pItem->i64 = tsRpcQueueMemoryAllowed;
    pItem->stype = stype;
  }

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
  simDebugFlag = cfgGetItem(pCfg, "simDebugFlag")->i32;
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
  stDebugFlag = cfgGetItem(pCfg, "stDebugFlag")->i32;
  sndDebugFlag = cfgGetItem(pCfg, "sndDebugFlag")->i32;
}

static int32_t taosSetSlowLogScope(char *pScope) {
  if (NULL == pScope || 0 == strlen(pScope)) {
    tsSlowLogScope = SLOW_LOG_TYPE_ALL;
    return 0;
  }

  if (0 == strcasecmp(pScope, "all")) {
    tsSlowLogScope = SLOW_LOG_TYPE_ALL;
    return 0;
  }

  if (0 == strcasecmp(pScope, "query")) {
    tsSlowLogScope = SLOW_LOG_TYPE_QUERY;
    return 0;
  }

  if (0 == strcasecmp(pScope, "insert")) {
    tsSlowLogScope = SLOW_LOG_TYPE_INSERT;
    return 0;
  }

  if (0 == strcasecmp(pScope, "others")) {
    tsSlowLogScope = SLOW_LOG_TYPE_OTHERS;
    return 0;
  }

  if (0 == strcasecmp(pScope, "none")) {
    tsSlowLogScope = 0;
    return 0;
  }

  uError("Invalid slowLog scope value:%s", pScope);
  terrno = TSDB_CODE_INVALID_CFG_VALUE;
  return -1;
}

// for common configs
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

  tstrncpy(tsSmlAutoChildTableNameDelimiter, cfgGetItem(pCfg, "smlAutoChildTableNameDelimiter")->str,
           TSDB_TABLE_NAME_LEN);
  tstrncpy(tsSmlChildTableName, cfgGetItem(pCfg, "smlChildTableName")->str, TSDB_TABLE_NAME_LEN);
  tstrncpy(tsSmlTagName, cfgGetItem(pCfg, "smlTagName")->str, TSDB_COL_NAME_LEN);
  tstrncpy(tsSmlTsDefaultName, cfgGetItem(pCfg, "smlTsDefaultName")->str, TSDB_COL_NAME_LEN);
  tsSmlDot2Underline = cfgGetItem(pCfg, "smlDot2Underline")->bval;
  //  tsSmlDataFormat = cfgGetItem(pCfg, "smlDataFormat")->bval;

  //  tsSmlBatchSize = cfgGetItem(pCfg, "smlBatchSize")->i32;
  tsMaxInsertBatchRows = cfgGetItem(pCfg, "maxInsertBatchRows")->i32;

  tsShellActivityTimer = cfgGetItem(pCfg, "shellActivityTimer")->i32;
  tsCompressMsgSize = cfgGetItem(pCfg, "compressMsgSize")->i32;
  tsNumOfTaskQueueThreads = cfgGetItem(pCfg, "numOfTaskQueueThreads")->i32;
  tsQueryPolicy = cfgGetItem(pCfg, "queryPolicy")->i32;
  tsEnableQueryHb = cfgGetItem(pCfg, "enableQueryHb")->bval;
  tsEnableScience = cfgGetItem(pCfg, "enableScience")->bval;
  tsQuerySmaOptimize = cfgGetItem(pCfg, "querySmaOptimize")->i32;
  tsQueryPlannerTrace = cfgGetItem(pCfg, "queryPlannerTrace")->bval;
  tsQueryNodeChunkSize = cfgGetItem(pCfg, "queryNodeChunkSize")->i32;
  tsQueryUseNodeAllocator = cfgGetItem(pCfg, "queryUseNodeAllocator")->bval;
  tsKeepColumnName = cfgGetItem(pCfg, "keepColumnName")->bval;
  tsUseAdapter = cfgGetItem(pCfg, "useAdapter")->bval;
  tsEnableCrashReport = cfgGetItem(pCfg, "crashReporting")->bval;
  tsQueryMaxConcurrentTables = cfgGetItem(pCfg, "queryMaxConcurrentTables")->i64;
  tsMetaCacheMaxSize = cfgGetItem(pCfg, "metaCacheMaxSize")->i32;
  tsSlowLogThreshold = cfgGetItem(pCfg, "slowLogThreshold")->i32;
  tsEnableMonitor = cfgGetItem(pCfg, "monitor")->bval;
  tsMonitorInterval = cfgGetItem(pCfg, "monitorInterval")->i32;
  if (taosSetSlowLogScope(cfgGetItem(pCfg, "slowLogScope")->str)) {
    return -1;
  }

  tsMaxRetryWaitTime = cfgGetItem(pCfg, "maxRetryWaitTime")->i32;

  tsNumOfRpcThreads = cfgGetItem(pCfg, "numOfRpcThreads")->i32;
  tsNumOfRpcSessions = cfgGetItem(pCfg, "numOfRpcSessions")->i32;

  tsTimeToGetAvailableConn = cfgGetItem(pCfg, "timeToGetAvailableConn")->i32;

  tsKeepAliveIdle = cfgGetItem(pCfg, "keepAliveIdle")->i32;

  tsExperimental = cfgGetItem(pCfg, "experimental")->bval;

  tsDisableCount = cfgGetItem(pCfg, "disableCount")->bval;
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

  tsAssert = cfgGetItem(pCfg, "assert")->bval;

  // todo
  tsVersion = 30000000;
}

// for server configs
static int32_t taosSetServerCfg(SConfig *pCfg) {
  tsDataSpace.reserved = (int64_t)(((double)cfgGetItem(pCfg, "minimalDataDirGB")->fval) * 1024 * 1024 * 1024);
  tsNumOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  tsMaxShellConns = cfgGetItem(pCfg, "maxShellConns")->i32;
  tsStatusInterval = cfgGetItem(pCfg, "statusInterval")->i32;
  tsMinSlidingTime = cfgGetItem(pCfg, "minSlidingTime")->i32;
  tsMinIntervalTime = cfgGetItem(pCfg, "minIntervalTime")->i32;
  tsCountAlwaysReturnValue = cfgGetItem(pCfg, "countAlwaysReturnValue")->i32;
  tsQueryBufferSize = cfgGetItem(pCfg, "queryBufferSize")->i32;

  tsNumOfRpcThreads = cfgGetItem(pCfg, "numOfRpcThreads")->i32;
  tsNumOfRpcSessions = cfgGetItem(pCfg, "numOfRpcSessions")->i32;
  tsTimeToGetAvailableConn = cfgGetItem(pCfg, "timeToGetAvailableConn")->i32;

  tsNumOfCommitThreads = cfgGetItem(pCfg, "numOfCommitThreads")->i32;
  tsNumOfMnodeReadThreads = cfgGetItem(pCfg, "numOfMnodeReadThreads")->i32;
  tsNumOfVnodeQueryThreads = cfgGetItem(pCfg, "numOfVnodeQueryThreads")->i32;
  tsRatioOfVnodeStreamThreads = cfgGetItem(pCfg, "ratioOfVnodeStreamThreads")->fval;
  tsNumOfVnodeFetchThreads = cfgGetItem(pCfg, "numOfVnodeFetchThreads")->i32;
  tsNumOfVnodeRsmaThreads = cfgGetItem(pCfg, "numOfVnodeRsmaThreads")->i32;
  tsNumOfQnodeQueryThreads = cfgGetItem(pCfg, "numOfQnodeQueryThreads")->i32;
  //  tsNumOfQnodeFetchThreads = cfgGetItem(pCfg, "numOfQnodeFetchTereads")->i32;
  tsNumOfSnodeStreamThreads = cfgGetItem(pCfg, "numOfSnodeSharedThreads")->i32;
  tsNumOfSnodeWriteThreads = cfgGetItem(pCfg, "numOfSnodeUniqueThreads")->i32;
  tsRpcQueueMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64;

  tsSIMDEnable = (bool)cfgGetItem(pCfg, "simdEnable")->bval;
  tsTagFilterCache = (bool)cfgGetItem(pCfg, "tagFilterCache")->bval;

  tsEnableMonitor = cfgGetItem(pCfg, "monitor")->bval;
  tsMonitorInterval = cfgGetItem(pCfg, "monitorInterval")->i32;
  tstrncpy(tsMonitorFqdn, cfgGetItem(pCfg, "monitorFqdn")->str, TSDB_FQDN_LEN);
  tsMonitorPort = (uint16_t)cfgGetItem(pCfg, "monitorPort")->i32;
  tsMonitorMaxLogs = cfgGetItem(pCfg, "monitorMaxLogs")->i32;
  tsMonitorComp = cfgGetItem(pCfg, "monitorComp")->bval;
  tsQueryRspPolicy = cfgGetItem(pCfg, "queryRspPolicy")->i32;
  tsMonitorLogProtocol = cfgGetItem(pCfg, "monitorLogProtocol")->bval;
  tsMonitorIntervalForBasic = cfgGetItem(pCfg, "monitorIntervalForBasic")->i32;
  tsMonitorForceV2 = cfgGetItem(pCfg, "monitorForceV2")->i32;
  
  tsEnableAudit = cfgGetItem(pCfg, "audit")->bval;
  tsEnableAuditCreateTable = cfgGetItem(pCfg, "auditCreateTable")->bval;
  tsAuditInterval = cfgGetItem(pCfg, "auditInterval")->i32;

  tsEnableTelem = cfgGetItem(pCfg, "telemetryReporting")->bval;
  tsEnableCrashReport = cfgGetItem(pCfg, "crashReporting")->bval;
  tsTtlChangeOnWrite = cfgGetItem(pCfg, "ttlChangeOnWrite")->bval;
  tsTtlFlushThreshold = cfgGetItem(pCfg, "ttlFlushThreshold")->i32;
  tsTelemInterval = cfgGetItem(pCfg, "telemetryInterval")->i32;
  tsRsyncPort = cfgGetItem(pCfg, "rsyncPort")->i32;
  tstrncpy(tsTelemServer, cfgGetItem(pCfg, "telemetryServer")->str, TSDB_FQDN_LEN);
  tstrncpy(tsSnodeAddress, cfgGetItem(pCfg, "snodeAddress")->str, TSDB_FQDN_LEN);
  tstrncpy(tsCheckpointBackupDir, cfgGetItem(pCfg, "checkpointBackupDir")->str, PATH_MAX);
  tsTelemPort = (uint16_t)cfgGetItem(pCfg, "telemetryPort")->i32;

  tmqMaxTopicNum = cfgGetItem(pCfg, "tmqMaxTopicNum")->i32;

  tsTransPullupInterval = cfgGetItem(pCfg, "transPullupInterval")->i32;
  tsCompactPullupInterval = cfgGetItem(pCfg, "compactPullupInterval")->i32;
  tsMqRebalanceInterval = cfgGetItem(pCfg, "mqRebalanceInterval")->i32;
  tsTtlUnit = cfgGetItem(pCfg, "ttlUnit")->i32;
  tsTtlPushIntervalSec = cfgGetItem(pCfg, "ttlPushInterval")->i32;
  tsTtlBatchDropNum = cfgGetItem(pCfg, "ttlBatchDropNum")->i32;
  tsTrimVDbIntervalSec = cfgGetItem(pCfg, "trimVDbIntervalSec")->i32;
  tsUptimeInterval = cfgGetItem(pCfg, "uptimeInterval")->i32;
  tsQueryRsmaTolerance = cfgGetItem(pCfg, "queryRsmaTolerance")->i32;
  tsTimeSeriesThreshold = cfgGetItem(pCfg, "timeseriesThreshold")->i32;

  tsWalFsyncDataSizeLimit = cfgGetItem(pCfg, "walFsyncDataSizeLimit")->i64;

  tsElectInterval = cfgGetItem(pCfg, "syncElectInterval")->i32;
  tsHeartbeatInterval = cfgGetItem(pCfg, "syncHeartbeatInterval")->i32;
  tsHeartbeatTimeout = cfgGetItem(pCfg, "syncHeartbeatTimeout")->i32;
  tsSnapReplMaxWaitN = cfgGetItem(pCfg, "syncSnapReplMaxWaitN")->i32;

  tsMndSdbWriteDelta = cfgGetItem(pCfg, "mndSdbWriteDelta")->i64;
  tsMndLogRetention = cfgGetItem(pCfg, "mndLogRetention")->i64;
  tsMndSkipGrant = cfgGetItem(pCfg, "skipGrant")->bval;
  tsMndGrantMode = cfgGetItem(pCfg, "grantMode")->i32;
  tsEnableWhiteList = cfgGetItem(pCfg, "enableWhiteList")->bval;

  tsStartUdfd = cfgGetItem(pCfg, "udf")->bval;
  tstrncpy(tsUdfdResFuncs, cfgGetItem(pCfg, "udfdResFuncs")->str, sizeof(tsUdfdResFuncs));
  tstrncpy(tsUdfdLdLibPath, cfgGetItem(pCfg, "udfdLdLibPath")->str, sizeof(tsUdfdLdLibPath));
  if (tsQueryBufferSize >= 0) {
    tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
  }

  tsCacheLazyLoadThreshold = cfgGetItem(pCfg, "cacheLazyLoadThreshold")->i32;

  tstrncpy(tsLossyColumns, cfgGetItem(pCfg, "lossyColumns")->str, sizeof(tsLossyColumns));
  tsFPrecision = cfgGetItem(pCfg, "fPrecision")->fval;
  tsDPrecision = cfgGetItem(pCfg, "dPrecision")->fval;
  tsMaxRange = cfgGetItem(pCfg, "maxRange")->i32;
  tsCurRange = cfgGetItem(pCfg, "curRange")->i32;
  tsIfAdtFse = cfgGetItem(pCfg, "ifAdtFse")->bval;
  tstrncpy(tsCompressor, cfgGetItem(pCfg, "compressor")->str, sizeof(tsCompressor));

  tsDisableStream = cfgGetItem(pCfg, "disableStream")->bval;
  tsStreamBufferSize = cfgGetItem(pCfg, "streamBufferSize")->i64;
  tsStreamAggCnt = cfgGetItem(pCfg, "streamAggCnt")->i32;
  tsStreamBufferSize = cfgGetItem(pCfg, "streamBufferSize")->i64;
  tsStreamCheckpointInterval = cfgGetItem(pCfg, "checkpointInterval")->i32;
  tsSinkDataRate = cfgGetItem(pCfg, "streamSinkDataRate")->fval;

  tsFilterScalarMode = cfgGetItem(pCfg, "filterScalarMode")->bval;
  tsMaxStreamBackendCache = cfgGetItem(pCfg, "maxStreamBackendCache")->i32;
  tsPQSortMemThreshold = cfgGetItem(pCfg, "pqSortMemThreshold")->i32;
  tsResolveFQDNRetryTime = cfgGetItem(pCfg, "resolveFQDNRetryTime")->i32;
  tsMinDiskFreeSize = cfgGetItem(pCfg, "minDiskFreeSize")->i64;

  tsS3BlockSize = cfgGetItem(pCfg, "s3BlockSize")->i32;
  tsS3BlockCacheSize = cfgGetItem(pCfg, "s3BlockCacheSize")->i32;
  tsS3PageCacheSize = cfgGetItem(pCfg, "s3PageCacheSize")->i32;
  tsS3UploadDelaySec = cfgGetItem(pCfg, "s3UploadDelaySec")->i32;

  tsExperimental = cfgGetItem(pCfg, "experimental")->bval;

  // GRANT_CFG_GET;
  return 0;
}

#ifndef TD_ENTERPRISE
static int32_t taosSetReleaseCfg(SConfig *pCfg) { return 0; }
#else
int32_t taosSetReleaseCfg(SConfig *pCfg);
#endif

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc) {
  if (tsCfg == NULL) osDefaultInit();

  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return -1;

  if (tsc) {
    tsLogEmbedded = 0;
    if (taosAddClientLogCfg(pCfg) != 0) {
      cfgCleanup(pCfg);
      return -1;
    }
  } else {
    tsLogEmbedded = 1;
    if (taosAddClientLogCfg(pCfg) != 0) {
      cfgCleanup(pCfg);
      return -1;
    }
    if (taosAddServerLogCfg(pCfg) != 0) {
      cfgCleanup(pCfg);
      return -1;
    }
  }

  if (taosLoadCfg(pCfg, envCmd, cfgDir, envFile, apolloUrl) != 0) {
    printf("failed to load cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (cfgLoadFromArray(pCfg, pArgs) != 0) {
    printf("failed to load cfg from array since %s", terrstr());
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

  if (taosMulModeMkDir(tsLogDir, 0777, true) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    printf("failed to create dir:%s since %s", tsLogDir, terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (taosInitLog(logname, logFileNum) != 0) {
    printf("failed to init log file since %s", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  cfgCleanup(pCfg);
  return 0;
}

static int32_t taosCheckGlobalCfg() {
  uint32_t ipv4 = taosGetIpv4FromFqdn(tsLocalFqdn);
  if (ipv4 == 0xffffffff) {
    terrno = TSDB_CODE_RPC_FQDN_ERROR;
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
    tsCfg = NULL;
    return -1;
  }

  if (tsc) {
    if (taosSetClientCfg(tsCfg)) return -1;
  } else {
    if (taosSetClientCfg(tsCfg)) return -1;
    if (taosUpdateServerCfg(tsCfg)) return -1;
    if (taosSetServerCfg(tsCfg)) return -1;
    if (taosSetReleaseCfg(tsCfg)) return -1;
    if (taosSetTfsCfg(tsCfg) != 0) return -1;
    if (taosSetS3Cfg(tsCfg) != 0) return -1;
  }
  taosSetSystemCfg(tsCfg);
  if (taosSetFileHandlesLimit() != 0) return -1;

  taosSetAllDebugFlag(cfgGetItem(tsCfg, "debugFlag")->i32);

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

typedef struct {
  const char *optionName;
  void       *optionVar;
} OptionNameAndVar;

static int32_t taosCfgSetOption(OptionNameAndVar *pOptions, int32_t optionSize, SConfigItem *pItem, bool isDebugflag) {
  terrno = TSDB_CODE_INVALID_CFG;
  char *name = pItem->name;
  for (int32_t d = 0; d < optionSize; ++d) {
    const char *optName = pOptions[d].optionName;
    int32_t     optLen = strlen(optName);
    if (strncasecmp(name, optName, optLen) != 0) continue;
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL: {
        int32_t flag = pItem->i32;
        bool   *pVar = pOptions[d].optionVar;
        uInfo("%s set from %d to %d", optName, *pVar, flag);
        *pVar = flag;
        terrno = TSDB_CODE_SUCCESS;
      } break;
      case CFG_DTYPE_INT32: {
        int32_t  flag = pItem->i32;
        int32_t *pVar = pOptions[d].optionVar;
        uInfo("%s set from %d to %d", optName, *pVar, flag);
        *pVar = flag;

        if (isDebugflag) {
          taosSetDebugFlag(pOptions[d].optionVar, optName, flag);
        }
        terrno = TSDB_CODE_SUCCESS;
      } break;
      case CFG_DTYPE_INT64: {
        int64_t  flag = pItem->i64;
        int64_t *pVar = pOptions[d].optionVar;
        uInfo("%s set from %" PRId64 " to %" PRId64, optName, *pVar, flag);
        *pVar = flag;
        terrno = TSDB_CODE_SUCCESS;
      } break;
      case CFG_DTYPE_FLOAT:
      case CFG_DTYPE_DOUBLE: {
        float  flag = pItem->fval;
        float *pVar = pOptions[d].optionVar;
        uInfo("%s set from %f to %f", optName, *pVar, flag);
        *pVar = flag;
        terrno = TSDB_CODE_SUCCESS;
      } break;
      default:
        terrno = TSDB_CODE_INVALID_CFG;
        break;
    }

    break;
  }

  return terrno == TSDB_CODE_SUCCESS ? 0 : -1;
}

static int32_t taosCfgDynamicOptionsForServer(SConfig *pCfg, char *name) {
  terrno = TSDB_CODE_SUCCESS;

  if (strcasecmp(name, "resetlog") == 0) {
    // trigger, no item in cfg
    taosResetLog();
    cfgDumpCfg(tsCfg, 0, false);
    return 0;
  }

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (!pItem || (pItem->dynScope & CFG_DYN_SERVER) == 0) {
    uError("failed to config:%s, not support", name);
    terrno = TSDB_CODE_INVALID_CFG;
    return -1;
  }

  if (strncasecmp(name, "debugFlag", 9) == 0) {
    taosSetAllDebugFlag(pItem->i32);
    return 0;
  }

  {  //  'bool/int32_t/int64_t/float/double' variables with general modification function
    static OptionNameAndVar debugOptions[] = {
        {"dDebugFlag", &dDebugFlag},     {"vDebugFlag", &vDebugFlag},     {"mDebugFlag", &mDebugFlag},
        {"wDebugFlag", &wDebugFlag},     {"sDebugFlag", &sDebugFlag},     {"tsdbDebugFlag", &tsdbDebugFlag},
        {"tqDebugFlag", &tqDebugFlag},   {"fsDebugFlag", &fsDebugFlag},   {"udfDebugFlag", &udfDebugFlag},
        {"smaDebugFlag", &smaDebugFlag}, {"idxDebugFlag", &idxDebugFlag}, {"tdbDebugFlag", &tdbDebugFlag},
        {"tmrDebugFlag", &tmrDebugFlag}, {"uDebugFlag", &uDebugFlag},     {"smaDebugFlag", &smaDebugFlag},
        {"rpcDebugFlag", &rpcDebugFlag}, {"qDebugFlag", &qDebugFlag},     {"metaDebugFlag", &metaDebugFlag},
        {"stDebugFlag", &stDebugFlag},   {"sndDebugFlag", &sndDebugFlag},
    };

    static OptionNameAndVar options[] = {{"audit", &tsEnableAudit},
                                         {"asynclog", &tsAsyncLog},
                                         {"disableStream", &tsDisableStream},
                                         {"enableWhiteList", &tsEnableWhiteList},
                                         {"telemetryReporting", &tsEnableTelem},
                                         {"monitor", &tsEnableMonitor},

                                         {"mndSdbWriteDelta", &tsMndSdbWriteDelta},
                                         {"minDiskFreeSize", &tsMinDiskFreeSize},

                                         {"cacheLazyLoadThreshold", &tsCacheLazyLoadThreshold},
                                         {"checkpointInterval", &tsStreamCheckpointInterval},
                                         {"keepAliveIdle", &tsKeepAliveIdle},
                                         {"logKeepDays", &tsLogKeepDays},
                                         {"maxStreamBackendCache", &tsMaxStreamBackendCache},
                                         {"mqRebalanceInterval", &tsMqRebalanceInterval},
                                         {"numOfLogLines", &tsNumOfLogLines},
                                         {"queryRspPolicy", &tsQueryRspPolicy},
                                         {"timeseriesThreshold", &tsTimeSeriesThreshold},
                                         {"tmqMaxTopicNum", &tmqMaxTopicNum},
                                         {"transPullupInterval", &tsTransPullupInterval},
                                         {"compactPullupInterval", &tsCompactPullupInterval},
                                         {"trimVDbIntervalSec", &tsTrimVDbIntervalSec},
                                         {"ttlBatchDropNum", &tsTtlBatchDropNum},
                                         {"ttlFlushThreshold", &tsTtlFlushThreshold},
                                         {"ttlPushInterval", &tsTtlPushIntervalSec},
                                         //{"s3BlockSize", &tsS3BlockSize},
                                         {"s3BlockCacheSize", &tsS3BlockCacheSize},
                                         {"s3PageCacheSize", &tsS3PageCacheSize},
                                         {"s3UploadDelaySec", &tsS3UploadDelaySec},
                                         {"supportVnodes", &tsNumOfSupportVnodes},
                                         {"experimental", &tsExperimental}};

    if (taosCfgSetOption(debugOptions, tListLen(debugOptions), pItem, true) != 0) {
      taosCfgSetOption(options, tListLen(options), pItem, false);
    }
  }

  return terrno == TSDB_CODE_SUCCESS ? 0 : -1;
}

static int32_t taosCfgDynamicOptionsForClient(SConfig *pCfg, char *name) {
  terrno = TSDB_CODE_SUCCESS;

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (!pItem || (pItem->dynScope & CFG_DYN_CLIENT) == 0) {
    uError("failed to config:%s, not support", name);
    terrno = TSDB_CODE_INVALID_CFG;
    return -1;
  }

  bool matched = false;

  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));
  switch (lowcaseName[0]) {
    case 'd': {
      if (strcasecmp("debugFlag", name) == 0) {
        taosSetAllDebugFlag(pItem->i32);
        matched = true;
      }
      break;
    }
    case 'e': {
      if (strcasecmp("enableCoreFile", name) == 0) {
        bool enableCore = pItem->bval;
        taosSetCoreDump(enableCore);
        uInfo("%s set to %d", name, enableCore);
        matched = true;
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
        uInfo("localEp set to '%s', tsFirst set to '%s'", tsLocalEp, tsFirst);
        matched = true;
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
        uInfo("localEp set to '%s', tsFirst set to '%s'", tsLocalEp, tsFirst);
        matched = true;
      }
      break;
    }
    case 'l': {
      if (strcasecmp("locale", name) == 0) {
        const char *locale = cfgGetItem(pCfg, "locale")->str;
        const char *charset = cfgGetItem(pCfg, "charset")->str;
        taosSetSystemLocale(locale, charset);
        osSetSystemLocale(locale, charset);
        uInfo("locale set to '%s', charset set to '%s'", locale, charset);
        matched = true;
      } else if (strcasecmp("logDir", name) == 0) {
        uInfo("%s set from '%s' to '%s'", name, tsLogDir, pItem->str);
        tstrncpy(tsLogDir, pItem->str, PATH_MAX);
        taosExpandDir(tsLogDir, tsLogDir, PATH_MAX);
        matched = true;
      }
      break;
    }
    case 'm': {
      if (strcasecmp("metaCacheMaxSize", name) == 0) {
        atomic_store_32(&tsMetaCacheMaxSize, pItem->i32);
        uInfo("%s set to %d", name, atomic_load_32(&tsMetaCacheMaxSize));
        matched = true;
      } else if (strcasecmp("minimalTmpDirGB", name) == 0) {
        tsTempSpace.reserved = (int64_t)(((double)pItem->fval) * 1024 * 1024 * 1024);
        uInfo("%s set to %" PRId64, name, tsTempSpace.reserved);
        matched = true;
      } else if (strcasecmp("minimalLogDirGB", name) == 0) {
        tsLogSpace.reserved = (int64_t)(((double)pItem->fval) * 1024 * 1024 * 1024);
        uInfo("%s set to %" PRId64, name, tsLogSpace.reserved);
        matched = true;
      }
      break;
    }
    case 's': {
      if (strcasecmp("secondEp", name) == 0) {
        SEp secondEp = {0};
        taosGetFqdnPortFromEp(strlen(pItem->str) == 0 ? tsFirst : pItem->str, &secondEp);
        snprintf(tsSecond, sizeof(tsSecond), "%s:%u", secondEp.fqdn, secondEp.port);
        cfgSetItem(pCfg, "secondEp", tsSecond, pItem->stype);
        uInfo("%s set to %s", name, tsSecond);
        matched = true;
      } else if (strcasecmp("smlChildTableName", name) == 0) {
        uInfo("%s set from %s to %s", name, tsSmlChildTableName, pItem->str);
        tstrncpy(tsSmlChildTableName, pItem->str, TSDB_TABLE_NAME_LEN);
        matched = true;
      } else if (strcasecmp("smlAutoChildTableNameDelimiter", name) == 0) {
        uInfo("%s set from %s to %s", name, tsSmlAutoChildTableNameDelimiter, pItem->str);
        tstrncpy(tsSmlAutoChildTableNameDelimiter, pItem->str, TSDB_TABLE_NAME_LEN);
        matched = true;
      } else if (strcasecmp("smlTagName", name) == 0) {
        uInfo("%s set from %s to %s", name, tsSmlTagName, pItem->str);
        tstrncpy(tsSmlTagName, pItem->str, TSDB_COL_NAME_LEN);
        matched = true;
      } else if (strcasecmp("smlTsDefaultName", name) == 0) {
        uInfo("%s set from %s to %s", name, tsSmlTsDefaultName, pItem->str);
        tstrncpy(tsSmlTsDefaultName, pItem->str, TSDB_COL_NAME_LEN);
        matched = true;
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
        uInfo("localEp set to '%s', tsFirst set to '%s'", tsLocalEp, tsFirst);
        matched = true;
      } else if (strcasecmp("slowLogScope", name) == 0) {
        if (taosSetSlowLogScope(pItem->str)) {
          return -1;
        }
        uInfo("%s set to %s", name, pItem->str);
        matched = true;
      }
      break;
    }
    case 't': {
      if (strcasecmp("timezone", name) == 0) {
        osSetTimezone(pItem->str);
        uInfo("%s set from %s to %s", name, tsTimezoneStr, pItem->str);
        cfgSetItem(pCfg, "timezone", tsTimezoneStr, pItem->stype);
        matched = true;
      } else if (strcasecmp("tempDir", name) == 0) {
        uInfo("%s set from %s to %s", name, tsTempDir, pItem->str);
        tstrncpy(tsTempDir, pItem->str, PATH_MAX);
        taosExpandDir(tsTempDir, tsTempDir, PATH_MAX);
        if (taosMulMkDir(tsTempDir) != 0) {
          uError("failed to create tempDir:%s since %s", tsTempDir, terrstr());
          return -1;
        }
        matched = true;
      }
      break;
    }
    default:
      terrno = TSDB_CODE_CFG_NOT_FOUND;
      break;
  }

  if (matched) goto _out;

  {  //  'bool/int32_t/int64_t/float/double' variables with general modification function
    static OptionNameAndVar debugOptions[] = {
        {"cDebugFlag", &cDebugFlag},     {"dDebugFlag", &dDebugFlag},     {"fsDebugFlag", &fsDebugFlag},
        {"idxDebugFlag", &idxDebugFlag}, {"jniDebugFlag", &jniDebugFlag}, {"qDebugFlag", &qDebugFlag},
        {"rpcDebugFlag", &rpcDebugFlag}, {"smaDebugFlag", &smaDebugFlag}, {"tmrDebugFlag", &tmrDebugFlag},
        {"uDebugFlag", &uDebugFlag},     {"simDebugFlag", &simDebugFlag},
    };

    static OptionNameAndVar options[] = {{"asyncLog", &tsAsyncLog},
                                         {"assert", &tsAssert},
                                         {"compressMsgSize", &tsCompressMsgSize},
                                         {"countAlwaysReturnValue", &tsCountAlwaysReturnValue},
                                         {"crashReporting", &tsEnableCrashReport},
                                         {"enableCoreFile", &tsAsyncLog},
                                         {"enableQueryHb", &tsEnableQueryHb},
                                         {"keepColumnName", &tsKeepColumnName},
                                         {"keepAliveIdle", &tsKeepAliveIdle},
                                         {"logKeepDays", &tsLogKeepDays},
                                         {"maxInsertBatchRows", &tsMaxInsertBatchRows},
                                         {"maxRetryWaitTime", &tsMaxRetryWaitTime},
                                         {"minSlidingTime", &tsMinSlidingTime},
                                         {"minIntervalTime", &tsMinIntervalTime},
                                         {"numOfLogLines", &tsNumOfLogLines},
                                         {"querySmaOptimize", &tsQuerySmaOptimize},
                                         {"queryPolicy", &tsQueryPolicy},
                                         {"queryPlannerTrace", &tsQueryPlannerTrace},
                                         {"queryNodeChunkSize", &tsQueryNodeChunkSize},
                                         {"queryUseNodeAllocator", &tsQueryUseNodeAllocator},
                                         {"smlDot2Underline", &tsSmlDot2Underline},
                                         {"shellActivityTimer", &tsShellActivityTimer},
                                         {"slowLogThreshold", &tsSlowLogThreshold},
                                         {"useAdapter", &tsUseAdapter},
                                         {"experimental", &tsExperimental},
                                         {"disableCount", &tsDisableCount}};

    if (taosCfgSetOption(debugOptions, tListLen(debugOptions), pItem, true) != 0) {
      taosCfgSetOption(options, tListLen(options), pItem, false);
    }
  }

_out:
  return terrno == TSDB_CODE_SUCCESS ? 0 : -1;
}

int32_t taosCfgDynamicOptions(SConfig *pCfg, char *name, bool forServer) {
  if (forServer) return taosCfgDynamicOptionsForServer(pCfg, name);
  return taosCfgDynamicOptionsForClient(pCfg, name);
}

void taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal) {
  SConfigItem *pItem = cfgGetItem(tsCfg, flagName);
  if (pItem != NULL) {
    pItem->i32 = flagVal;
  }
  if (pFlagPtr != NULL) {
    *pFlagPtr = flagVal;
  }
}

static int taosLogVarComp(void const *lp, void const *rp) {
  SLogVar *lpVar = (SLogVar *)lp;
  SLogVar *rpVar = (SLogVar *)rp;
  return strcasecmp(lpVar->name, rpVar->name);
}

static void taosCheckAndSetDebugFlag(int32_t *pFlagPtr, char *name, int32_t flag, SArray *noNeedToSetVars) {
  if (noNeedToSetVars != NULL && taosArraySearch(noNeedToSetVars, name, taosLogVarComp, TD_EQ) != NULL) {
    return;
  }
  taosSetDebugFlag(pFlagPtr, name, flag);
}

void taosSetAllDebugFlag(int32_t flag) {
  if (flag <= 0) return;

  SArray      *noNeedToSetVars = NULL;
  SConfigItem *pItem = cfgGetItem(tsCfg, "debugFlag");
  if (pItem != NULL) {
    pItem->i32 = flag;
    noNeedToSetVars = pItem->array;
  }

  taosCheckAndSetDebugFlag(&simDebugFlag, "simDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&tmrDebugFlag, "tmrDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&uDebugFlag, "uDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&rpcDebugFlag, "rpcDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&qDebugFlag, "qDebugFlag", flag, noNeedToSetVars);

  taosCheckAndSetDebugFlag(&jniDebugFlag, "jniDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&cDebugFlag, "cDebugFlag", flag, noNeedToSetVars);

  taosCheckAndSetDebugFlag(&dDebugFlag, "dDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&vDebugFlag, "vDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&mDebugFlag, "mDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&wDebugFlag, "wDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&sDebugFlag, "sDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&tsdbDebugFlag, "tsdbDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&tqDebugFlag, "tqDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&fsDebugFlag, "fsDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&udfDebugFlag, "udfDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&smaDebugFlag, "smaDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&idxDebugFlag, "idxDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&tdbDebugFlag, "tdbDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&metaDebugFlag, "metaDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&stDebugFlag, "stDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&sndDebugFlag, "sndDebugFlag", flag, noNeedToSetVars);

  taosArrayClear(noNeedToSetVars);  // reset array

  uInfo("all debug flag are set to %d", flag);
  if (terrno == TSDB_CODE_CFG_NOT_FOUND) terrno = TSDB_CODE_SUCCESS;  // ignore not exist
}

int8_t taosGranted(int8_t type) {
  switch (type) {
    case TSDB_GRANT_ALL:
      return atomic_load_8(&tsGrant) & GRANT_FLAG_ALL;
    case TSDB_GRANT_AUDIT:
      return atomic_load_8(&tsGrant) & GRANT_FLAG_AUDIT;
    case TSDB_GRANT_VIEW:
      return atomic_load_8(&tsGrant) & GRANT_FLAG_VIEW;
    default:
      ASSERTS(0, "undefined grant type:%" PRIi8, type);
      break;
  }
  return 0;
}