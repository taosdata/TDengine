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
#include "cJSON.h"
#include "defines.h"
#include "os.h"
#include "osString.h"
#include "tconfig.h"
#include "tgrant.h"
#include "tjson.h"
#include "tlog.h"
#include "tmisce.h"
#include "tunit.h"

#include "tutil.h"

#define CONFIG_PATH_LEN (TSDB_FILENAME_LEN + 12)
#define CONFIG_FILE_LEN (CONFIG_PATH_LEN + 32)

// GRANT_CFG_DECLARE;

SConfig *tsCfg = NULL;
// cluster
char          tsFirst[TSDB_EP_LEN] = {0};
char          tsSecond[TSDB_EP_LEN] = {0};
char          tsLocalFqdn[TSDB_FQDN_LEN] = {0};
char          tsLocalEp[TSDB_EP_LEN] = {0};  // Local End Point, hostname:port
char          tsVersionName[16] = "community";
uint16_t      tsServerPort = 6030;
int32_t       tsVersion = 30000000;
int32_t       tsForceReadConfig = 0;
int32_t       tsdmConfigVersion = -1;
int32_t       tsConfigInited = 0;
int32_t       tsStatusInterval = 1;  // second
int32_t       tsStatusIntervalMs = 1000;
int32_t       tsStatusSRTimeoutMs = 5000;
int32_t       tsStatusTimeoutMs = 5000;
int32_t       tsNumOfSupportVnodes = 256;
uint16_t      tsMqttPort = 6083;
char          tsEncryptAlgorithm[16] = {0};
char          tsEncryptScope[100] = {0};
EEncryptAlgor tsiEncryptAlgorithm = 0;
EEncryptScope tsiEncryptScope = 0;
// char     tsAuthCode[500] = {0};
// char     tsEncryptKey[17] = {0};
char          tsEncryptKey[17] = {0};
int8_t        tsEnableStrongPassword = 1;
char          tsEncryptPassAlgorithm[16] = {0};
EEncryptAlgor tsiEncryptPassAlgorithm = 0;

char tsTLSCaPath[PATH_MAX] = {0};
char tsTLSSvrCertPath[PATH_MAX] = {0};
char tsTLSSvrKeyPath[PATH_MAX] = {0};

char tsTLSCliCertPath[PATH_MAX] = {0};
char tsTLSCliKeyPath[PATH_MAX] = {0};

int8_t tsEnableTLS = 0;
// common
int32_t tsMaxShellConns = 50000;
int32_t tsShellActivityTimer = 3;  // second

// memory pool
int8_t tsMemPoolFullFunc = 0;
#ifndef TD_ASTRA
int8_t tsQueryUseMemoryPool = 1;
#else
int8_t  tsQueryUseMemoryPool = 0;
#endif
int32_t tsQueryBufferPoolSize = 0;       // MB
int32_t tsSingleQueryMaxMemorySize = 0;  // MB
int32_t tsMinReservedMemorySize = 0;     // MB
int64_t tsCurrentAvailMemorySize = 0;
int8_t  tsNeedTrim = 0;

// queue & threads
int32_t tsQueryMinConcurrentTaskNum = 1;
int32_t tsQueryMaxConcurrentTaskNum = 0;
int32_t tsQueryConcurrentTaskNum = 0;
int32_t tsQueryNoFetchTimeoutSec = 3600 * 5;

int32_t tsNumOfRpcThreads = 1;
int32_t tsNumOfRpcSessions = 30000;
#ifdef WINDOWS
int32_t tsShareConnLimit = 1;
#else
int32_t tsShareConnLimit = 10;
#endif

int32_t tsReadTimeout = 900;
int32_t tsTimeToGetAvailableConn = 500000;
int8_t  tsEnableIpv6 = 0;

#ifdef TD_ENTERPRISE
bool    tsAuthServer = 0;
bool    tsAuthReq = 0;
int32_t tsAuthReqInterval = 2592000;
int32_t tsAuthReqHBInterval = 5;
char    tsAuthReqUrl[TSDB_FQDN_LEN] = {0};
#endif

int32_t tsNumOfQueryThreads = 0;
int32_t tsNumOfCommitThreads = 2;
int32_t tsNumOfTaskQueueThreads = 16;
int32_t tsNumOfMnodeQueryThreads = 16;
int32_t tsNumOfMnodeFetchThreads = 1;
int32_t tsNumOfMnodeReadThreads = 1;
int32_t tsNumOfVnodeQueryThreads = 16;
int32_t tsNumOfVnodeFetchThreads = 4;
int32_t tsNumOfVnodeRsmaThreads = 2;
int32_t tsNumOfQnodeQueryThreads = 16;
int32_t tsNumOfQnodeFetchThreads = 1;
int32_t tsNumOfSnodeStreamThreads = 4;
int32_t tsNumOfSnodeWriteThreads = 1;
int32_t tsPQSortMemThreshold = 16;    // M
int32_t tsRetentionSpeedLimitMB = 0;  // unlimited
int32_t tsNumOfMnodeStreamMgmtThreads = 2;
int32_t tsNumOfStreamMgmtThreads = 2;
int32_t tsNumOfVnodeStreamReaderThreads = 16;
int32_t tsNumOfStreamTriggerThreads = 4;
int32_t tsNumOfStreamRunnerThreads = 4;

int32_t tsNumOfCompactThreads = 2;
int32_t tsNumOfRetentionThreads = 1;

// sync raft
int32_t tsElectInterval = 4000;
int32_t tsHeartbeatInterval = 1000;
int32_t tsVnodeElectIntervalMs = 4000;
int32_t tsVnodeHeartbeatIntervalMs = 1000;
int32_t tsMnodeElectIntervalMs = 3000;
int32_t tsMnodeHeartbeatIntervalMs = 500;
int32_t tsHeartbeatTimeout = 20 * 1000;
int32_t tsSnapReplMaxWaitN = 128;
int64_t tsLogBufferMemoryAllowed = 0;  // bytes
int64_t tsSyncApplyQueueSize = 512;
int32_t tsRoutineReportInterval = 300;
bool    tsSyncLogHeartbeat = false;
int32_t tsSyncTimeout = 0;

// mnode
int64_t tsMndSdbWriteDelta = 200;
int64_t tsMndLogRetention = 2000;
bool    tsMndSkipGrant = false;
bool    tsEnableWhiteList = false;  // ip white list cfg
bool    tsForceKillTrans = false;

// arbitrator
int32_t tsArbHeartBeatIntervalSec = 2;
int32_t tsArbCheckSyncIntervalSec = 3;
int32_t tsArbSetAssignedTimeoutSec = 14;
int32_t tsArbHeartBeatIntervalMs = 2000;
int32_t tsArbCheckSyncIntervalMs = 3000;
int32_t tsArbSetAssignedTimeoutMs = 14000;

// dnode
int64_t tsDndStart = 0;
int64_t tsDndStartOsUptime = 0;
int64_t tsDndUpTime = 0;

// dnode misc
uint32_t tsEncryptionKeyChksum = 0;
int8_t   tsEncryptionKeyStat = ENCRYPT_KEY_STAT_UNSET;
uint32_t tsGrant = 1;

bool tsCompareAsStrInGreatest = true;

// monitor
#ifdef USE_MONITOR
bool tsEnableMonitor = true;
#else
bool    tsEnableMonitor = false;
#endif
int32_t  tsMonitorInterval = 30;
char     tsMonitorFqdn[TSDB_FQDN_LEN] = {0};
uint16_t tsMonitorPort = 6043;
int32_t  tsMonitorMaxLogs = 100;
bool     tsMonitorComp = false;
bool     tsMonitorLogProtocol = false;
int32_t  tsEnableMetrics = 0;     // 0: disable, 1: enable
int32_t  tsMetricsLevel = 0;      // 0: only high level metrics, 1: full metrics
int32_t  tsMetricsInterval = 30;  // second
#ifdef USE_MONITOR
bool tsMonitorForceV2 = true;
#else
bool    tsMonitorForceV2 = false;
#endif

// audit
#ifdef USE_AUDIT
bool    tsEnableAudit = true;
bool    tsEnableAuditCreateTable = true;
bool    tsEnableAuditDelete = true;
int32_t tsAuditInterval = 5000;
#else
bool    tsEnableAudit = false;
bool    tsEnableAuditCreateTable = false;
bool    tsEnableAuditDelete = false;
int32_t tsAuditInterval = 200000;
#endif

// telem
#if defined(TD_ENTERPRISE) || !defined(USE_REPORT)
bool tsEnableTelem = false;
#else
bool    tsEnableTelem = true;
#endif
int32_t  tsTelemInterval = 86400;
char     tsTelemServer[TSDB_FQDN_LEN] = "telemetry.tdengine.com";
uint16_t tsTelemPort = 80;
char    *tsTelemUri = "/report";

#if defined(TD_ENTERPRISE) || !defined(USE_REPORT)
bool tsEnableCrashReport = false;
#else
bool    tsEnableCrashReport = true;
#endif
char   *tsClientCrashReportUri = "/ccrashreport";
char   *tsSvrCrashReportUri = "/dcrashreport";
int32_t tsSafetyCheckLevel = TSDB_SAFETY_CHECK_LEVELL_NORMAL;

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
int32_t tmqRowSize = 1000;
// query
int32_t tsQueryPolicy = 1;
bool    tsQueryTbNotExistAsEmpty = false;
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
int32_t tsMaxRetryWaitTime = 20000;
bool    tsUseAdapter = false;
int32_t tsMetaCacheMaxSize = -1;                   // MB
int32_t tsSlowLogThreshold = 10;                   // seconds
char    tsSlowLogExceptDb[TSDB_DB_NAME_LEN] = "";  // seconds
int32_t tsSlowLogScope = SLOW_LOG_TYPE_QUERY;
char   *tsSlowLogScopeString = "query";
int32_t tsSlowLogMaxLen = 4096;
int32_t tsTimeSeriesThreshold = 50;
bool    tsMultiResultFunctionStarReturnTags = false;

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
char    tsStableTagFilterCache = 0;

int32_t tsBypassFlag = 0;

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

int64_t tsSecTimes[] = {1e3L, 1e6L, 1e9L};  // for milli, micro, nano

// lossy compress 7
char tsLossyColumns[32] = "float|double";  // "float|double" means all float and double columns can be lossy compressed.
                                           // set empty can close lossy compress.
// below option can take effect when tsLossyColumns not empty
float    tsFPrecision = 1E-8;                   // float column precision
double   tsDPrecision = 1E-16;                  // double column precision
uint32_t tsMaxRange = 500;                      // max quantization intervals
uint32_t tsCurRange = 100;                      // current quantization intervals
bool     tsIfAdtFse = false;                    // ADT-FSE algorithom or original huffman algorithom
char     tsCompressor[32] = "ZSTD_COMPRESSOR";  // ZSTD_COMPRESSOR or GZIP_COMPRESSOR

// udf
#if defined(WINDOWS) || !defined(USE_UDF)
bool tsStartUdfd = false;
#else
bool    tsStartUdfd = true;
#endif

// wal
int64_t tsWalFsyncDataSizeLimit = (100 * 1024 * 1024L);
bool    tsWalForceRepair = 0;

// ttl
bool    tsTtlChangeOnWrite = false;  // if true, ttl delete time changes on last write
int32_t tsTtlFlushThreshold = 100;   /* maximum number of dirty items in memory.
                                      * if -1, flush will not be triggered by write-ops
                                      */
int32_t tsTtlBatchDropNum = 10000;   // number of tables dropped per batch

// internal
bool    tsDiskIDCheckEnabled = false;
int32_t tsTransPullupInterval = 2;
int32_t tsCompactPullupInterval = 10;
int32_t tsScanPullupInterval = 10;
int32_t tsMqRebalanceInterval = 2;
int32_t tsTtlUnit = 86400;
int32_t tsTtlPushIntervalSec = 10;
int32_t tsTrimVDbIntervalSec = 60 * 60;  // interval of trimming db in all vgroups
int32_t tsQueryTrimIntervalSec = 10;     // interval of query trim in all vgroups
int32_t tsGrantHBInterval = 60;
int32_t tsUptimeInterval = 300;    // seconds
char    tsUdfdResFuncs[512] = "";  // udfd resident funcs that teardown when udfd exits
char    tsUdfdLdLibPath[512] = "";
bool    tsDisableStream = false;
int32_t tsStreamBufferSize = 0;       // MB
int64_t tsStreamBufferSizeBytes = 0;  // bytes
bool    tsStreamPerfLogEnabled = true;
bool    tsFilterScalarMode = false;

bool tsUpdateCacheBatch = true;

int32_t tsSsEnabled = 0;  // enable shared storage, 0: disabled, 1: enabled, 2: enabled with auto migration
char    tsSsAccessString[1024] = "";
int32_t tsSsAutoMigrateIntervalSec = 60 * 60;  // auto migrate interval of shared storage migration
int32_t tsSsBlockSize = -1;        // number of tsdb pages (4096). note: some related (but unused) code hasn't check the
                                   // negative value, which is a bug.
int32_t tsSsBlockCacheSize = 16;   // number of blocks
int32_t tsSsPageCacheSize = 4096;  // number of pages
int32_t tsSsUploadDelaySec = 60;

bool tsExperimental = true;

int32_t tsMaxTsmaNum = 10;
int32_t tsMaxTsmaCalcDelay = 600;
int64_t tsmaDataDeleteMark = 1000 * 60 * 60 * 24;  // in ms, default to 1d
void   *pTimezoneNameMap = NULL;

int32_t tsStreamNotifyMessageSize = 8 * 1024;  // KB, default 8MB
int32_t tsStreamNotifyFrameSize = 256;         // KB, default 256KB
int32_t tsStreamBatchRequestWaitMs = 5000;     // ms, default 5s

bool    tsShowFullCreateTableColumn = 0;  // 0: show full create table, 1: show only table name and db name
int32_t tsRpcRecvLogThreshold = 3;        // in seconds, default 3s
int32_t taosCheckCfgStrValueLen(const char *name, const char *value, int32_t len);

#define TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, pName) \
  if ((pItem = cfgGetItem(pCfg, pName)) == NULL) {  \
    TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);           \
  }

#ifndef _STORAGE
int32_t taosSetTfsCfg(SConfig *pCfg) {
  SConfigItem *pItem = NULL;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "dataDir");
  (void)memset(tsDataDir, 0, PATH_MAX);

  int32_t size = taosArrayGetSize(pItem->array);
  tsDiskCfgNum = 1;
  tstrncpy(tsDiskCfg[0].dir, pItem->str, TSDB_FILENAME_LEN);
  tsDiskCfg[0].level = 0;
  tsDiskCfg[0].primary = 1;
  tsDiskCfg[0].disable = 0;
  tstrncpy(tsDataDir, pItem->str, PATH_MAX);
  if (taosMulMkDir(tsDataDir) != 0) {
    int32_t code = TAOS_SYSTEM_ERROR(ERRNO);
    uError("failed to create dataDir:%s, since:%s", tsDataDir, tstrerror(code));
    TAOS_RETURN(code);
  }
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}
#else
int32_t taosSetTfsCfg(SConfig *pCfg);
#endif

#ifndef _STORAGE
int32_t cfgUpdateTfsItemDisable(SConfig *pCfg, const char *value, void *pTfs) { return TSDB_CODE_INVALID_CFG; }
#else
int32_t cfgUpdateTfsItemDisable(SConfig *pCfg, const char *value, void *pTfs);
#endif

int32_t taosUpdateTfsItemDisable(SConfig *pCfg, const char *value, void *pTfs) {
  return cfgUpdateTfsItemDisable(pCfg, value, pTfs);
}

struct SConfig *taosGetCfg() { return tsCfg; }

static int32_t taosLoadCfg(SConfig *pCfg, const char **envCmd, const char *inputCfgDir, const char *envFile,
                           char *apolloUrl) {
  int32_t code = 0;
  char    cfgDir[PATH_MAX] = {0};
  char    cfgFile[PATH_MAX + 100] = {0};

  TAOS_CHECK_RETURN(taosExpandDir(inputCfgDir, cfgDir, PATH_MAX));
  int32_t pos = strlen(cfgDir);
  if (pos > 0) {
    pos -= 1;
  }
  char  lastC = cfgDir[pos];
  char *tdDirsep = TD_DIRSEP;
  if (lastC == '\\' || lastC == '/') {
    tdDirsep = "";
  }
  if (taosIsDir(cfgDir)) {
#ifdef CUS_PROMPT
    (void)snprintf(cfgFile, sizeof(cfgFile),
                   "%s"
                   "%s"
                   "%s.cfg",
                   cfgDir, tdDirsep, CUS_PROMPT);
#else
    (void)snprintf(cfgFile, sizeof(cfgFile),
                   "%s"
                   "%s"
                   "taos.cfg",
                   cfgDir, tdDirsep);
#endif
  } else {
    tstrncpy(cfgFile, cfgDir, sizeof(cfgDir));
  }

  if (apolloUrl != NULL && apolloUrl[0] == '\0') {
    (void)(cfgGetApollUrl(envCmd, envFile, apolloUrl));
  }

  if ((code = cfgLoad(pCfg, CFG_STYPE_APOLLO_URL, apolloUrl)) != 0) {
    (void)printf("failed to load from apollo url:%s since %s\n", apolloUrl, tstrerror(code));
    TAOS_RETURN(code);
  }

  if ((code = cfgLoad(pCfg, CFG_STYPE_CFG_FILE, cfgFile)) != 0) {
    (void)printf("failed to load from cfg file:%s since %s\n", cfgFile, tstrerror(code));
    TAOS_RETURN(code);
  }

  if ((code = cfgLoad(pCfg, CFG_STYPE_ENV_FILE, envFile)) != 0) {
    (void)printf("failed to load from env file:%s since %s\n", envFile, tstrerror(code));
    TAOS_RETURN(code);
  }

  if ((code = cfgLoad(pCfg, CFG_STYPE_ENV_VAR, NULL)) != 0) {
    (void)printf("failed to load from global env variables since %s\n", tstrerror(code));
    TAOS_RETURN(code);
  }

  if ((code = cfgLoad(pCfg, CFG_STYPE_ENV_CMD, envCmd)) != 0) {
    (void)printf("failed to load from cmd env variables since %s\n", tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t taosAddClientLogCfg(SConfig *pCfg) {
  TAOS_CHECK_RETURN(cfgAddDir(pCfg, "configDir", configDir, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddDir(pCfg, "scriptDir", configDir, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddDir(pCfg, "logDir", tsLogDir, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddFloat(pCfg, "minimalLogDirGB", 1.0f, 0.001f, 10000000, CFG_SCOPE_BOTH, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfLogLines", tsNumOfLogLines, 1000, 2000000000, CFG_SCOPE_BOTH,
                                CFG_DYN_ENT_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "asyncLog", tsAsyncLog, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "logKeepDays", 0, -365000, 365000, CFG_SCOPE_BOTH, CFG_DYN_ENT_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "debugFlag", 0, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "simDebugFlag", simDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "tmrDebugFlag", tmrDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "uDebugFlag", uDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "rpcDebugFlag", rpcDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "jniDebugFlag", jniDebugFlag, 0, 255, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "qDebugFlag", qDebugFlag, 0, 255, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "cDebugFlag", cDebugFlag, 0, 255, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "tqClientDebugFlag", tqClientDebugFlag, 0, 255, CFG_SCOPE_CLIENT, CFG_DYN_SERVER,
                                CFG_CATEGORY_LOCAL));
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosAddServerLogCfg(SConfig *pCfg) {
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "dDebugFlag", dDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "vDebugFlag", vDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "mDebugFlag", mDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "wDebugFlag", wDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "azDebugFlag", azDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "tssDebugFlag", tssDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "sDebugFlag", sDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "tsdbDebugFlag", tsdbDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "tqDebugFlag", tqDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "tqClientDebug", tqClientDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER,
                                CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "fsDebugFlag", fsDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "udfDebugFlag", udfDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "smaDebugFlag", smaDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "idxDebugFlag", idxDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "tdbDebugFlag", tdbDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "metaDebugFlag", metaDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "stDebugFlag", stDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "sndDebugFlag", sndDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "bseDebugFlag", bseDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "bndDebugFlag", bndDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosAddClientCfg(SConfig *pCfg) {
  char    defaultFqdn[TSDB_FQDN_LEN] = {0};
  int32_t defaultServerPort = 6030;
  int32_t defaultMqttPort = 6083;
  int64_t cost = 0;
  if (taosGetFqdnWithTimeCost(defaultFqdn, &cost) != 0) {
    tstrncpy(defaultFqdn, "localhost", TSDB_FQDN_LEN);
  }
  if (cost >= 1000) {
    printf("warning: get fqdn cost %" PRId64 " ms\n", cost);
  }

  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "forceReadConfig", tsForceReadConfig, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "firstEp", "", CFG_SCOPE_BOTH, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "secondEp", "", CFG_SCOPE_BOTH, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "fqdn", defaultFqdn, CFG_SCOPE_SERVER, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "serverPort", defaultServerPort, 1, 65056, CFG_SCOPE_SERVER, CFG_DYN_CLIENT,
                                CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "mqttPort", defaultMqttPort, 1, 65056, CFG_SCOPE_SERVER, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddDir(pCfg, "tempDir", tsTempDir, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddFloat(pCfg, "minimalTmpDirGB", 1.0f, 0.001f, 10000000, CFG_SCOPE_BOTH, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "shellActivityTimer", tsShellActivityTimer, 1, 120, CFG_SCOPE_BOTH, CFG_DYN_BOTH,
                                CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "compressMsgSize", tsCompressMsgSize, -1, 100000000, CFG_SCOPE_BOTH,
                                CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "queryPolicy", tsQueryPolicy, 1, 4, CFG_SCOPE_CLIENT, CFG_DYN_ENT_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "queryTableNotExistAsEmpty", tsQueryTbNotExistAsEmpty, CFG_SCOPE_CLIENT,
                               CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "enableQueryHb", tsEnableQueryHb, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "enableScience", tsEnableScience, CFG_SCOPE_CLIENT, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "querySmaOptimize", tsQuerySmaOptimize, 0, 1, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT,
                                CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "queryPlannerTrace", tsQueryPlannerTrace, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryNodeChunkSize", tsQueryNodeChunkSize, 1024, 128 * 1024, CFG_SCOPE_CLIENT,
                                CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "queryUseNodeAllocator", tsQueryUseNodeAllocator, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT,
                               CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "keepColumnName", tsKeepColumnName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "smlChildTableName", tsSmlChildTableName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT,
                                 CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "smlAutoChildTableNameDelimiter", tsSmlAutoChildTableNameDelimiter,
                                 CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddString(pCfg, "smlTagName", tsSmlTagName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddString(pCfg, "smlTsDefaultName", tsSmlTsDefaultName, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "smlDot2Underline", tsSmlDot2Underline, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "minSlidingTime", tsMinSlidingTime, 1, 1000000, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT,
                                CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "minIntervalTime", tsMinIntervalTime, 1, 1000000, CFG_SCOPE_CLIENT,
                                CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "maxInsertBatchRows", tsMaxInsertBatchRows, 1, INT32_MAX, CFG_SCOPE_CLIENT,
                                CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL) != 0);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "maxRetryWaitTime", tsMaxRetryWaitTime, 3000, 86400000, CFG_SCOPE_SERVER,
                                CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "useAdapter", tsUseAdapter, CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "crashReporting", tsEnableCrashReport, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "queryMaxConcurrentTables", tsQueryMaxConcurrentTables, INT64_MIN, INT64_MAX,
                                CFG_SCOPE_CLIENT, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "metaCacheMaxSize", tsMetaCacheMaxSize, -1, INT32_MAX, CFG_SCOPE_CLIENT,
                                CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "randErrorChance", tsRandErrChance, 0, 10000, CFG_SCOPE_BOTH, CFG_DYN_BOTH,
                                CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "randErrorDivisor", tsRandErrDivisor, 1, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_BOTH,
                                CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "randErrorScope", tsRandErrScope, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_BOTH,
                                CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "safetyCheckLevel", tsSafetyCheckLevel, 0, 5, CFG_SCOPE_BOTH, CFG_DYN_BOTH,
                                CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "bypassFlag", tsBypassFlag, 0, INT32_MAX, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  tsNumOfRpcThreads = tsNumOfCores / 2;
  tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 2, TSDB_MAX_RPC_THREADS);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfRpcThreads", tsNumOfRpcThreads, 1, 1024, CFG_SCOPE_BOTH, CFG_DYN_BOTH_LAZY,
                                CFG_CATEGORY_LOCAL));

  tsNumOfRpcSessions = TRANGE(tsNumOfRpcSessions, 100, 100000);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfRpcSessions", tsNumOfRpcSessions, 1, 100000, CFG_SCOPE_BOTH,
                                CFG_DYN_BOTH_LAZY, CFG_CATEGORY_LOCAL));

  tsShareConnLimit = TRANGE(tsShareConnLimit, 1, 512);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "shareConnLimit", tsShareConnLimit, 1, 512, CFG_SCOPE_BOTH, CFG_DYN_BOTH_LAZY,
                                CFG_CATEGORY_GLOBAL));

  tsReadTimeout = TRANGE(tsReadTimeout, 64, 24 * 3600 * 7);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "readTimeout", tsReadTimeout, 64, 24 * 3600 * 7, CFG_SCOPE_BOTH,
                                CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL));

  tsTimeToGetAvailableConn = TRANGE(tsTimeToGetAvailableConn, 20, 10000000);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "timeToGetAvailableConn", tsTimeToGetAvailableConn, 20, 1000000, CFG_SCOPE_BOTH,
                                CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL));
  tsNumOfTaskQueueThreads = tsNumOfCores * 2;
  tsNumOfTaskQueueThreads = TMAX(tsNumOfTaskQueueThreads, 16);

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfTaskQueueThreads", tsNumOfTaskQueueThreads, 4, 1024, CFG_SCOPE_CLIENT,
                                CFG_DYN_CLIENT_LAZY, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "experimental", tsExperimental, CFG_SCOPE_BOTH, CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "multiResultFunctionStarReturnTags", tsMultiResultFunctionStarReturnTags,
                               CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "countAlwaysReturnValue", tsCountAlwaysReturnValue, 0, 1, CFG_SCOPE_BOTH,
                                CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "maxTsmaCalcDelay", tsMaxTsmaCalcDelay, 600, 86400, CFG_SCOPE_CLIENT,
                                CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "tsmaDataDeleteMark", tsmaDataDeleteMark, 60 * 60 * 1000, INT64_MAX,
                                CFG_SCOPE_CLIENT, CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "compareAsStrInGreatest", tsCompareAsStrInGreatest, CFG_SCOPE_CLIENT,
                               CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "showFullCreateTableColumn", tsShowFullCreateTableColumn, CFG_SCOPE_CLIENT,
                               CFG_DYN_CLIENT, CFG_CATEGORY_LOCAL));
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosAddSystemCfg(SConfig *pCfg) {
  SysNameInfo info = taosGetSysNameInfo();

  TAOS_CHECK_RETURN(cfgAddTimezone(pCfg, "timezone", tsTimezoneStr, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddLocale(pCfg, "locale", tsLocale, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddCharset(pCfg, "charset", tsCharset, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "enableCoreFile", tsEnableCoreFile, CFG_SCOPE_BOTH, CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddFloat(pCfg, "numOfCores", tsNumOfCores, 1, 100000, CFG_SCOPE_BOTH, CFG_DYN_BOTH_LAZY, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "simdEnable", tsSIMDEnable, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "AVX512Enable", tsAVX512Enable, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "tagFilterCache", tsTagFilterCache, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(
      cfgAddBool(pCfg, "stableTagFilterCache", tsStableTagFilterCache,
        CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(
      cfgAddInt64(pCfg, "openMax", tsOpenMax, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
#if !defined(_ALPINE)
  TAOS_CHECK_RETURN(
      cfgAddInt64(pCfg, "streamMax", tsStreamMax, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
#endif
  TAOS_CHECK_RETURN(
      cfgAddInt32(pCfg, "pageSizeKB", tsPageSizeKB, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "totalMemoryKB", tsTotalMemoryKB, 0, INT64_MAX, CFG_SCOPE_BOTH, CFG_DYN_NONE,
                                CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "os sysname", info.sysname, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "os nodename", info.nodename, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "os release", info.release, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "os version", info.version, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "os machine", info.machine, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddString(pCfg, "version", td_version, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "compatible_version", td_compatible_version, CFG_SCOPE_BOTH, CFG_DYN_NONE,
                                 CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "gitinfo", td_gitinfo, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "buildinfo", td_buildinfo, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "enableIpv6", tsEnableIpv6, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddString(pCfg, "tlsCaPath", tsTLSCaPath, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddString(pCfg, "tlsSvrCertPath", tsTLSSvrCertPath, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddString(pCfg, "tlsSvrKeyPath", tsTLSSvrKeyPath, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddString(pCfg, "tlsCliCertPath", tsTLSCliCertPath, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(
      cfgAddString(pCfg, "tlsCliKeyPath", tsTLSCliKeyPath, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "enableTLS", tsEnableTLS, CFG_SCOPE_BOTH, CFG_DYN_BOTH, CFG_CATEGORY_GLOBAL));
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosAddServerCfg(SConfig *pCfg) {
  tsNumOfCommitThreads = tsNumOfCores / 2;
  tsNumOfCommitThreads = TRANGE(tsNumOfCommitThreads, 2, 4);

  tsNumOfSupportVnodes = tsNumOfCores * 2 + 5;
  tsNumOfSupportVnodes = TMAX(tsNumOfSupportVnodes, 2);

  tsNumOfMnodeReadThreads = tsNumOfCores / 8;
  tsNumOfMnodeReadThreads = TRANGE(tsNumOfMnodeReadThreads, 1, 4);

  tsNumOfVnodeQueryThreads = tsNumOfCores * 2;
  tsNumOfVnodeQueryThreads = TMAX(tsNumOfVnodeQueryThreads, 16);

  tsNumOfVnodeFetchThreads = tsNumOfCores / 4;
  tsNumOfVnodeFetchThreads = TMAX(tsNumOfVnodeFetchThreads, 4);

  tsNumOfVnodeRsmaThreads = tsNumOfCores / 4;
  tsNumOfVnodeRsmaThreads = TMAX(tsNumOfVnodeRsmaThreads, 4);

  tsNumOfQnodeQueryThreads = tsNumOfCores * 2;
  tsNumOfQnodeQueryThreads = TMAX(tsNumOfQnodeQueryThreads, 16);

  tsNumOfMnodeStreamMgmtThreads = tsNumOfCores / 4;
  tsNumOfMnodeStreamMgmtThreads = TRANGE(tsNumOfMnodeStreamMgmtThreads, 2, 5);

  tsNumOfStreamMgmtThreads = tsNumOfCores / 8;
  tsNumOfStreamMgmtThreads = TRANGE(tsNumOfStreamMgmtThreads, 2, 5);

  tsNumOfVnodeStreamReaderThreads = tsNumOfCores / 2;
  tsNumOfVnodeStreamReaderThreads = TMAX(tsNumOfVnodeStreamReaderThreads, 2);

  tsNumOfStreamTriggerThreads = tsNumOfCores;
  tsNumOfStreamTriggerThreads = TMAX(tsNumOfStreamTriggerThreads, 4);

  tsNumOfStreamRunnerThreads = tsNumOfCores;
  tsNumOfStreamRunnerThreads = TMAX(tsNumOfStreamRunnerThreads, 4);

  tsQueueMemoryAllowed = tsTotalMemoryKB * 1024 * RPC_MEMORY_USAGE_RATIO * QUEUE_MEMORY_USAGE_RATIO;
  tsQueueMemoryAllowed = TRANGE(tsQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * QUEUE_MEMORY_USAGE_RATIO * 10LL,
                                TSDB_MAX_MSG_SIZE * QUEUE_MEMORY_USAGE_RATIO * 10000LL);

  tsApplyMemoryAllowed = tsTotalMemoryKB * 1024 * RPC_MEMORY_USAGE_RATIO * (1 - QUEUE_MEMORY_USAGE_RATIO);
  tsApplyMemoryAllowed = TRANGE(tsApplyMemoryAllowed, TSDB_MAX_MSG_SIZE * (1 - QUEUE_MEMORY_USAGE_RATIO) * 10LL,
                                TSDB_MAX_MSG_SIZE * (1 - QUEUE_MEMORY_USAGE_RATIO) * 10000LL);

  tsLogBufferMemoryAllowed = tsTotalMemoryKB * 1024 * 0.1;
  tsLogBufferMemoryAllowed = TRANGE(tsLogBufferMemoryAllowed, TSDB_MAX_MSG_SIZE * 10LL, TSDB_MAX_MSG_SIZE * 10000LL);

  tsStreamBufferSize = tsTotalMemoryKB / 1024 * 0.3;
  tsStreamBufferSizeBytes = tsStreamBufferSize * 1048576L;

  // clang-format off
  TAOS_CHECK_RETURN(cfgAddDir(pCfg, "dataDir", tsDataDir, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddFloat(pCfg, "minimalDataDirGB", 2.0f, 0.001f, 10000000, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "supportVnodes", tsNumOfSupportVnodes, 0, 4096, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddString(pCfg, "encryptAlgorithm", tsEncryptAlgorithm, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "encryptScope", tsEncryptScope, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "enableStrongPassword", tsEnableStrongPassword, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "encryptPassAlgorithm", tsEncryptPassAlgorithm, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "statusInterval", tsStatusInterval, 1, 30, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "metricsInterval", tsMetricsInterval, 1, 3600, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "enableMetrics", tsEnableMetrics, 0, 1, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "metricsLevel", tsMetricsLevel, 0, 1, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "maxShellConns", tsMaxShellConns, 10, 50000000, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "statusIntervalMs", tsStatusIntervalMs, 50, 30000, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "statusSRTimeoutMs", tsStatusSRTimeoutMs, 50, 30000, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "statusTimeoutMs", tsStatusTimeoutMs, 50, 30000, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryBufferSize", tsQueryBufferSize, -1, 500000000000, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryRspPolicy", tsQueryRspPolicy, 0, 1, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfCommitThreads", tsNumOfCommitThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfCompactThreads", tsNumOfCompactThreads, 1, 16, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "retentionSpeedLimitMB", tsRetentionSpeedLimitMB, 0, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "queryUseMemoryPool", tsQueryUseMemoryPool, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL) != 0);
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "memPoolFullFunc", tsMemPoolFullFunc, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL) != 0);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "singleQueryMaxMemorySize", tsSingleQueryMaxMemorySize, 0, 1000000000, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL) != 0);
  //TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryBufferPoolSize", tsQueryBufferPoolSize, 0, 1000000000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER) != 0);
  TAOS_CHECK_RETURN(cfgAddInt32Ex(pCfg, "minReservedMemorySize", 0, 1024, 1000000000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL) != 0);
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryNoFetchTimeoutSec", tsQueryNoFetchTimeoutSec, 60, 1000000000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_LOCAL) != 0);

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfMnodeReadThreads", tsNumOfMnodeReadThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfVnodeQueryThreads", tsNumOfVnodeQueryThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfVnodeFetchThreads", tsNumOfVnodeFetchThreads, 4, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfVnodeRsmaThreads", tsNumOfVnodeRsmaThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfQnodeQueryThreads", tsNumOfQnodeQueryThreads, 1, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfMnodeStreamMgmtThreads", tsNumOfMnodeStreamMgmtThreads, 2, 5, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfStreamMgmtThreads", tsNumOfStreamMgmtThreads, 2, 5, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfVnodeStreamReaderThreads", tsNumOfVnodeStreamReaderThreads, 2, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfStreamTriggerThreads", tsNumOfStreamTriggerThreads, 4, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "numOfStreamRunnerThreads", tsNumOfStreamRunnerThreads, 4, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "rpcQueueMemoryAllowed", tsQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * RPC_MEMORY_USAGE_RATIO * 10L, INT64_MAX, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncElectInterval", tsElectInterval, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncHeartbeatInterval", tsHeartbeatInterval, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncVnodeElectIntervalMs", tsVnodeElectIntervalMs, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncVnodeHeartbeatIntervalMs", tsVnodeHeartbeatIntervalMs, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncMnodeElectIntervalMs", tsMnodeElectIntervalMs, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncMnodeHeartbeatIntervalMs", tsMnodeHeartbeatIntervalMs, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncHeartbeatTimeout", tsHeartbeatTimeout, 10, 1000 * 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncSnapReplMaxWaitN", tsSnapReplMaxWaitN, 16, (TSDB_SYNC_SNAP_BUFFER_SIZE >> 2), CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "syncLogBufferMemoryAllowed", tsLogBufferMemoryAllowed, TSDB_MAX_MSG_SIZE * 10L, INT64_MAX, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "syncApplyQueueSize", tsSyncApplyQueueSize, 32, 2048, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncRoutineReportInterval", tsRoutineReportInterval, 5, 600, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "syncLogHeartbeat", tsSyncLogHeartbeat, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "syncTimeout", tsSyncTimeout, 0, 60 * 24 * 2 * 1000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "arbHeartBeatIntervalSec", tsArbHeartBeatIntervalSec, 1, 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "arbCheckSyncIntervalSec", tsArbCheckSyncIntervalSec, 1, 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "arbSetAssignedTimeoutSec", tsArbSetAssignedTimeoutSec, 1, 60 * 24 * 2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "arbHeartBeatIntervalMs", tsArbHeartBeatIntervalMs, 100, 60 * 24 * 2 * 1000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "arbCheckSyncIntervalMs", tsArbCheckSyncIntervalMs, 100, 60 * 24 * 2 * 1000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "arbSetAssignedTimeoutMs", tsArbSetAssignedTimeoutMs, 100, 60 * 24 * 2 * 1000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "mndSdbWriteDelta", tsMndSdbWriteDelta, 20, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "mndLogRetention", tsMndLogRetention, 500, 10000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "skipGrant", tsMndSkipGrant, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "monitor", tsEnableMonitor, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "monitorInterval", tsMonitorInterval, 1, 86400, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "slowLogThreshold", tsSlowLogThreshold, 1, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "slowLogMaxLen", tsSlowLogMaxLen, 1, 16384, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "slowLogScope", tsSlowLogScopeString, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "slowLogExceptDb", tsSlowLogExceptDb, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddString(pCfg, "monitorFqdn", tsMonitorFqdn, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "monitorPort", tsMonitorPort, 1, 65056, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY, CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "monitorMaxLogs", tsMonitorMaxLogs, 1, 1000000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "monitorComp", tsMonitorComp, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "monitorLogProtocol", tsMonitorLogProtocol, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "monitorForceV2", tsMonitorForceV2, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "audit", tsEnableAudit, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "enableAuditDelete", tsEnableAuditDelete, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "auditCreateTable", tsEnableAuditCreateTable, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "auditInterval", tsAuditInterval, 500, 200000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "telemetryReporting", tsEnableTelem, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "telemetryInterval", tsTelemInterval, 1, 200000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "telemetryServer", tsTelemServer, CFG_SCOPE_SERVER, CFG_DYN_BOTH,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "telemetryPort", tsTelemPort, 1, 65056, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "rsyncPort", tsRsyncPort, 1, 65535, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "snodeAddress", tsSnodeAddress, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "checkpointBackupDir", tsCheckpointBackupDir, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "tmqMaxTopicNum", tmqMaxTopicNum, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "tmqRowSize", tmqRowSize, 1, 1000000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "maxTsmaNum", tsMaxTsmaNum, 0, 10, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "diskIDCheckEnabled", tsDiskIDCheckEnabled,  CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "transPullupInterval", tsTransPullupInterval, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "compactPullupInterval", tsCompactPullupInterval, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "mqRebalanceInterval", tsMqRebalanceInterval, 1, 10000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ttlUnit", tsTtlUnit, 1, 86400 * 365, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ttlPushInterval", tsTtlPushIntervalSec, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ttlBatchDropNum", tsTtlBatchDropNum, 0, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "ttlChangeOnWrite", tsTtlChangeOnWrite, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ttlFlushThreshold", tsTtlFlushThreshold, -1, 1000000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "trimVDbIntervalSec", tsTrimVDbIntervalSec, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryTrimIntervalSec", tsQueryTrimIntervalSec, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "uptimeInterval", tsUptimeInterval, 1, 100000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "queryRsmaTolerance", tsQueryRsmaTolerance, 0, 900000, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "timeseriesThreshold", tsTimeSeriesThreshold, 0, 2000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "walFsyncDataSizeLimit", tsWalFsyncDataSizeLimit, 100 * 1024 * 1024, INT64_MAX, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "walForceRepair", tsWalForceRepair, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "udf", tsStartUdfd, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "udfdResFuncs", tsUdfdResFuncs, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "udfdLdLibPath", tsUdfdLdLibPath, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "disableStream", tsDisableStream, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "streamBufferSize", tsStreamBufferSize, 128, INT32_MAX, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "cacheLazyLoadThreshold", tsCacheLazyLoadThreshold, 0, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddFloat(pCfg, "fPrecision", tsFPrecision, 0.0f, 100000.0f, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddFloat(pCfg, "dPrecision", tsDPrecision, 0.0f, 1000000.0f, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "maxRange", tsMaxRange, 0, 65536, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "curRange", tsCurRange, 0, 65536, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "ifAdtFse", tsIfAdtFse, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "compressor", tsCompressor, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "filterScalarMode", tsFilterScalarMode, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "pqSortMemThreshold", tsPQSortMemThreshold, 1, 10240, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ssEnabled", tsSsEnabled, 0, 2, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "ssAccessString", tsSsAccessString, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ssAutoMigrateIntervalSec", tsSsAutoMigrateIntervalSec, 600, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ssPageCacheSize", tsSsPageCacheSize, 4, 1024 * 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER_LAZY,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "ssUploadDelaySec", tsSsUploadDelaySec, 1, 60 * 60 * 24 * 30, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt64(pCfg, "minDiskFreeSize", tsMinDiskFreeSize, TFS_MIN_DISK_FREE_SIZE, TFS_MIN_DISK_FREE_SIZE_MAX, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "enableWhiteList", tsEnableWhiteList, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "forceKillTrans", tsForceKillTrans, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "streamNotifyMessageSize", tsStreamNotifyMessageSize, 8, 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "streamNotifyFrameSize", tsStreamNotifyFrameSize, 8, 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "streamBatchRequestWaitMs", tsStreamBatchRequestWaitMs, 0, 30 * 60 * 1000, CFG_SCOPE_SERVER, CFG_DYN_NONE,CFG_CATEGORY_LOCAL));

  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "rpcRecvLogThreshold", tsRpcRecvLogThreshold, 1, 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER,CFG_CATEGORY_LOCAL));

#ifdef TD_ENTERPRISE
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "authServer", tsAuthServer, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddBool(pCfg, "authReq", tsAuthReq, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddInt32(pCfg, "authReqInterval", tsAuthReqInterval, 1, 86400 * 30, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL));
  TAOS_CHECK_RETURN(cfgAddString(pCfg, "authReqUrl", tsAuthReqUrl, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL));
#endif
  // clang-format on

  // GRANT_CFG_ADD;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosUpdateServerCfg(SConfig *pCfg) {
  SConfigItem *pItem;
  ECfgSrcType  stype;
  int32_t      numOfCores;
  int64_t      totalMemoryKB;

  pItem = cfgGetItem(pCfg, "numOfCores");
  if (pItem == NULL) {
    TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);
  } else {
    stype = pItem->stype;
    numOfCores = pItem->fval;
  }

  pItem = cfgGetItem(pCfg, "supportVnodes");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfSupportVnodes = numOfCores * 2 + 5;
    tsNumOfSupportVnodes = TMAX(tsNumOfSupportVnodes, 2);
    pItem->i32 = tsNumOfSupportVnodes;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfRpcThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfRpcThreads = TRANGE(tsNumOfRpcThreads, 1, TSDB_MAX_RPC_THREADS);
    pItem->i32 = tsNumOfRpcThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfRpcSessions");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfRpcSessions = TRANGE(tsNumOfRpcSessions, 100, 10000);
    pItem->i32 = tsNumOfRpcSessions;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "shareConnLimit");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsShareConnLimit = TRANGE(tsShareConnLimit, 1, 512);
    pItem->i32 = tsShareConnLimit;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "readTimeout");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsReadTimeout = TRANGE(tsReadTimeout, 64, 24 * 3600 * 7);
    pItem->i32 = tsReadTimeout;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "timeToGetAvailableConn");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsTimeToGetAvailableConn = TRANGE(tsTimeToGetAvailableConn, 20, 1000000);
    pItem->i32 = tsTimeToGetAvailableConn;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfCommitThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfCommitThreads = numOfCores / 2;
    tsNumOfCommitThreads = TRANGE(tsNumOfCommitThreads, 2, 4);
    pItem->i32 = tsNumOfCommitThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfCompactThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    pItem->i32 = tsNumOfCompactThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfMnodeReadThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfMnodeReadThreads = numOfCores / 8;
    tsNumOfMnodeReadThreads = TRANGE(tsNumOfMnodeReadThreads, 1, 4);
    pItem->i32 = tsNumOfMnodeReadThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfVnodeQueryThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfVnodeQueryThreads = numOfCores * 2;
    tsNumOfVnodeQueryThreads = TMAX(tsNumOfVnodeQueryThreads, 16);
    pItem->i32 = tsNumOfVnodeQueryThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfVnodeFetchThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfVnodeFetchThreads = numOfCores / 4;
    tsNumOfVnodeFetchThreads = TMAX(tsNumOfVnodeFetchThreads, 4);
    pItem->i32 = tsNumOfVnodeFetchThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfVnodeRsmaThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfVnodeRsmaThreads = numOfCores;
    tsNumOfVnodeRsmaThreads = TMAX(tsNumOfVnodeRsmaThreads, 4);
    pItem->i32 = tsNumOfVnodeRsmaThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "numOfQnodeQueryThreads");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsNumOfQnodeQueryThreads = numOfCores * 2;
    tsNumOfQnodeQueryThreads = TMAX(tsNumOfQnodeQueryThreads, 16);
    pItem->i32 = tsNumOfQnodeQueryThreads;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(pCfg, "totalMemoryKB");
  if (pItem == NULL) {
    TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);
  } else {
    stype = pItem->stype;
    totalMemoryKB = pItem->i64;
  }

  pItem = cfgGetItem(pCfg, "rpcQueueMemoryAllowed");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsQueueMemoryAllowed = totalMemoryKB * 1024 * 0.1;
    tsQueueMemoryAllowed = TRANGE(tsQueueMemoryAllowed, TSDB_MAX_MSG_SIZE * 10LL, TSDB_MAX_MSG_SIZE * 10000LL);
    pItem->i64 = tsQueueMemoryAllowed;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "syncLogBufferMemoryAllowed");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    tsLogBufferMemoryAllowed = totalMemoryKB * 1024 * 0.1;
    tsLogBufferMemoryAllowed = TRANGE(tsLogBufferMemoryAllowed, TSDB_MAX_MSG_SIZE * 10LL, TSDB_MAX_MSG_SIZE * 10000LL);
    pItem->i64 = tsLogBufferMemoryAllowed;
    pItem->stype = stype;
  }

  pItem = cfgGetItem(tsCfg, "syncApplyQueueSize");
  if (pItem != NULL && pItem->stype == CFG_STYPE_DEFAULT) {
    pItem->i64 = tsSyncApplyQueueSize;
    pItem->stype = stype;
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosSetLogOutput(SConfig *pCfg) {
  if (tsLogOutput) {
    char *pLog = tsLogOutput;
    char *pEnd = NULL;
    if (strcasecmp(pLog, "stdout") && strcasecmp(pLog, "stderr") && strcasecmp(pLog, "/dev/null")) {
      if ((pEnd = strrchr(pLog, '/')) || (pEnd = strrchr(pLog, '\\'))) {
        int32_t pathLen = POINTER_DISTANCE(pEnd, pLog) + 1;
        if (*pLog == '/' || *pLog == '\\') {
          if (pathLen <= 0 || pathLen > PATH_MAX) TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
          tstrncpy(tsLogDir, pLog, pathLen);
        } else {
          int32_t len = strlen(tsLogDir);
          if (len < 0 || len >= (PATH_MAX - 1)) TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
          if (len == 0 || (tsLogDir[len - 1] != '/' && tsLogDir[len - 1] != '\\')) {
            tsLogDir[len++] = TD_DIRSEP_CHAR;
          }
          int32_t remain = PATH_MAX - len - 1;
          if (remain < pathLen) TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
          tstrncpy(tsLogDir + len, pLog, pathLen);
        }
        TAOS_CHECK_RETURN(cfgSetItem(pCfg, "logDir", tsLogDir, CFG_STYPE_DEFAULT, true));
      }
    } else {
      tstrncpy(tsLogDir, pLog, PATH_MAX);
      TAOS_CHECK_RETURN(cfgSetItem(pCfg, "logDir", tsLogDir, CFG_STYPE_DEFAULT, true));
    }
  }
  return 0;
}

static int32_t taosSetClientLogCfg(SConfig *pCfg) {
  SConfigItem *pItem = NULL;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "logDir");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsLogDir, pItem->str, PATH_MAX);
  TAOS_CHECK_RETURN(taosExpandDir(tsLogDir, tsLogDir, PATH_MAX));
  TAOS_CHECK_RETURN(taosSetLogOutput(pCfg));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minimalLogDirGB");
  tsLogSpace.reserved = (int64_t)(((double)pItem->fval) * 1024 * 1024 * 1024);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfLogLines");
  tsNumOfLogLines = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "asyncLog");
  tsAsyncLog = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "logKeepDays");
  tsLogKeepDays = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tmrDebugFlag");
  tmrDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "uDebugFlag");
  uDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "jniDebugFlag");
  jniDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "rpcDebugFlag");
  rpcDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "qDebugFlag");
  qDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "cDebugFlag");
  cDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "simDebugFlag");
  simDebugFlag = pItem->i32;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosSetServerLogCfg(SConfig *pCfg) {
  SConfigItem *pItem = NULL;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "dDebugFlag");
  dDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "vDebugFlag");
  vDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "mDebugFlag");
  mDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "wDebugFlag");
  wDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "azDebugFlag");
  azDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tssDebugFlag");
  tssDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "sDebugFlag");
  sDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tsdbDebugFlag");
  tsdbDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tqDebugFlag");
  tqDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "fsDebugFlag");
  fsDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "udfDebugFlag");
  udfDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "smaDebugFlag");
  smaDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "idxDebugFlag");
  idxDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tdbDebugFlag");
  tdbDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "metaDebugFlag");
  metaDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "stDebugFlag");
  stDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "sndDebugFlag");
  sndDebugFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "bseDebugFlag");
  bseDebugFlag = pItem->i32;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "bndDebugFlag");
  bndDebugFlag = pItem->i32;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t taosSetSlowLogScope(char *pScopeStr, int32_t *pScope) {
  if (NULL == pScopeStr || 0 == strlen(pScopeStr)) {
    *pScope = SLOW_LOG_TYPE_QUERY;
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  int32_t slowScope = 0;

  char *scope = NULL;
  char *tmp = NULL;
  while ((scope = strsep(&pScopeStr, "|")) != NULL) {
    taosMemoryFreeClear(tmp);
    tmp = taosStrdup(scope);
    if (tmp == NULL) {
      TAOS_RETURN(terrno);
    }
    (void)strtrim(tmp);
    if (0 == strcasecmp(tmp, "all")) {
      slowScope |= SLOW_LOG_TYPE_ALL;
      continue;
    }

    if (0 == strcasecmp(tmp, "query")) {
      slowScope |= SLOW_LOG_TYPE_QUERY;
      continue;
    }

    if (0 == strcasecmp(tmp, "insert")) {
      slowScope |= SLOW_LOG_TYPE_INSERT;
      continue;
    }

    if (0 == strcasecmp(tmp, "others")) {
      slowScope |= SLOW_LOG_TYPE_OTHERS;
      continue;
    }

    if (0 == strcasecmp(tmp, "none")) {
      slowScope |= SLOW_LOG_TYPE_NULL;
      continue;
    }

    taosMemoryFreeClear(tmp);
    uError("Invalid slowLog scope value:%s", pScopeStr);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG_VALUE);
  }

  *pScope = slowScope;
  taosMemoryFreeClear(tmp);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

// for common configs
static int32_t taosSetClientCfg(SConfig *pCfg) {
  SConfigItem *pItem = NULL;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "fqdn");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_FQDN_LEN));
  tstrncpy(tsLocalFqdn, pItem->str, TSDB_FQDN_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "forceReadConfig");
  tsForceReadConfig = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "serverPort");
  tsServerPort = (uint16_t)pItem->i32;
  (void)snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%u", tsLocalFqdn, tsServerPort);

  char defaultFirstEp[TSDB_EP_LEN] = {0};
  (void)snprintf(defaultFirstEp, TSDB_EP_LEN, "%s:%u", tsLocalFqdn, tsServerPort);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "mqttPort");
  tsMqttPort = (uint16_t)pItem->i32;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableIpv6");
  tsEnableIpv6 = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "firstEp");
  SEp firstEp = {0};
  TAOS_CHECK_RETURN(taosGetFqdnPortFromEp(strlen(pItem->str) == 0 ? defaultFirstEp : pItem->str, &firstEp));
  (void)snprintf(tsFirst, sizeof(tsFirst), "%s:%u", firstEp.fqdn, firstEp.port);
  TAOS_CHECK_RETURN(cfgSetItem(pCfg, "firstEp", tsFirst, pItem->stype, true));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "secondEp");
  SEp secondEp = {0};
  TAOS_CHECK_RETURN(taosGetFqdnPortFromEp(strlen(pItem->str) == 0 ? defaultFirstEp : pItem->str, &secondEp));
  (void)snprintf(tsSecond, sizeof(tsSecond), "%s:%u", secondEp.fqdn, secondEp.port);
  TAOS_CHECK_RETURN(cfgSetItem(pCfg, "secondEp", tsSecond, pItem->stype, true));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tempDir");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTempDir, pItem->str, PATH_MAX);
  TAOS_CHECK_RETURN(taosExpandDir(tsTempDir, tsTempDir, PATH_MAX));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minimalTmpDirGB");
  tsTempSpace.reserved = (int64_t)(((double)pItem->fval) * 1024 * 1024 * 1024);
  if (taosMulMkDir(tsTempDir) != 0) {
    int32_t code = TAOS_SYSTEM_ERROR(ERRNO);
    uError("failed to create tempDir:%s since %s", tsTempDir, tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "smlAutoChildTableNameDelimiter");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_TABLE_NAME_LEN));
  tstrncpy(tsSmlAutoChildTableNameDelimiter, pItem->str, TSDB_TABLE_NAME_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "smlChildTableName");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_TABLE_NAME_LEN));
  tstrncpy(tsSmlChildTableName, pItem->str, TSDB_TABLE_NAME_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "smlTagName");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_COL_NAME_LEN));
  tstrncpy(tsSmlTagName, pItem->str, TSDB_COL_NAME_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "smlTsDefaultName");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_COL_NAME_LEN));
  tstrncpy(tsSmlTsDefaultName, pItem->str, TSDB_COL_NAME_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "smlDot2Underline");
  tsSmlDot2Underline = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "maxInsertBatchRows");
  tsMaxInsertBatchRows = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "shellActivityTimer");
  tsShellActivityTimer = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "compressMsgSize");
  tsCompressMsgSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfTaskQueueThreads");
  tsNumOfTaskQueueThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryPolicy");
  tsQueryPolicy = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryTableNotExistAsEmpty");
  tsQueryTbNotExistAsEmpty = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableQueryHb");
  tsEnableQueryHb = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableScience");
  tsEnableScience = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "querySmaOptimize");
  tsQuerySmaOptimize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryPlannerTrace");
  tsQueryPlannerTrace = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryNodeChunkSize");
  tsQueryNodeChunkSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryUseNodeAllocator");
  tsQueryUseNodeAllocator = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "keepColumnName");
  tsKeepColumnName = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "useAdapter");
  tsUseAdapter = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "crashReporting");
  tsEnableCrashReport = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryMaxConcurrentTables");
  tsQueryMaxConcurrentTables = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "metaCacheMaxSize");
  tsMetaCacheMaxSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "randErrorChance");
  tsRandErrChance = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "randErrorDivisor");
  tsRandErrDivisor = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "randErrorScope");
  tsRandErrScope = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "countAlwaysReturnValue");
  tsCountAlwaysReturnValue = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "maxRetryWaitTime");
  tsMaxRetryWaitTime = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfRpcThreads");
  tsNumOfRpcThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfRpcSessions");
  tsNumOfRpcSessions = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "shareConnLimit");
  tsShareConnLimit = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "readTimeout");
  tsReadTimeout = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "timeToGetAvailableConn");
  tsTimeToGetAvailableConn = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "experimental");
  tsExperimental = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "multiResultFunctionStarReturnTags");
  tsMultiResultFunctionStarReturnTags = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "maxTsmaCalcDelay");
  tsMaxTsmaCalcDelay = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tsmaDataDeleteMark");
  tsmaDataDeleteMark = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "safetyCheckLevel");
  tsSafetyCheckLevel = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "bypassFlag");
  tsBypassFlag = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "compareAsStrInGreatest");
  tsCompareAsStrInGreatest = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "showFullCreateTableColumn");
  tsShowFullCreateTableColumn = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsCliKeyPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSCliKeyPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsCliCertPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSCliCertPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsCaPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSCaPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableTLS");
  tsEnableTLS = pItem->bval;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t taosSetSystemCfg(SConfig *pCfg) {
  SConfigItem *pItem = NULL;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableCoreFile");
  tsEnableCoreFile = pItem->bval;
  taosSetCoreDump(tsEnableCoreFile);

  // todo
  tsVersion = 30000000;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

// for server configs
static int32_t taosSetServerCfg(SConfig *pCfg) {
  SConfigItem *pItem = NULL;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minimalDataDirGB");
  tsDataSpace.reserved = (int64_t)(((double)pItem->fval) * 1024 * 1024 * 1024);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "supportVnodes");
  tsNumOfSupportVnodes = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "maxShellConns");
  tsMaxShellConns = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "statusInterval");
  tsStatusInterval = pItem->i32;
  tsStatusIntervalMs = pItem->i32 * 1000;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "statusIntervalMs");
  tsStatusIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "statusSRTimeoutMs");
  tsStatusSRTimeoutMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "statusTimeoutMs");
  tsStatusTimeoutMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableMetrics");
  tsEnableMetrics = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "metricsLevel");
  tsMetricsLevel = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "metricsInterval");
  tsMetricsInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minSlidingTime");
  tsMinSlidingTime = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minIntervalTime");
  tsMinIntervalTime = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryBufferSize");
  tsQueryBufferSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "encryptAlgorithm");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, 16));
  tstrncpy(tsEncryptAlgorithm, pItem->str, 16);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "encryptScope");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, 100));
  tstrncpy(tsEncryptScope, pItem->str, 100);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableStrongPassword");
  tsEnableStrongPassword = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "encryptPassAlgorithm");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, 16));
  tstrncpy(tsEncryptPassAlgorithm, pItem->str, 16);

  if (strlen(tsEncryptPassAlgorithm) > 0) {
    if (strcmp(tsEncryptPassAlgorithm, "sm4") == 0) {
      tsiEncryptPassAlgorithm = DND_CA_SM4;
    } else {
      uError("invalid tsEncryptAlgorithm:%s", tsEncryptPassAlgorithm);
    }
  }

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfRpcThreads");
  tsNumOfRpcThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfRpcSessions");
  tsNumOfRpcSessions = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "shareConnLimit");
  tsShareConnLimit = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "readTimeout");
  tsReadTimeout = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "timeToGetAvailableConn");
  tsTimeToGetAvailableConn = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfCommitThreads");
  tsNumOfCommitThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfCompactThreads");
  tsNumOfCompactThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableIpv6");
  tsEnableIpv6 = pItem->bval;

#ifdef TD_ENTERPRISE
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "authServer");
  tsAuthServer = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "authReq");
  tsAuthReq = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "authReqInterval");
  tsAuthReqInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "authReqUrl");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_FQDN_LEN));
  tstrncpy(tsAuthReqUrl, pItem->str, TSDB_FQDN_LEN);
#endif

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "retentionSpeedLimitMB");
  tsRetentionSpeedLimitMB = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfMnodeReadThreads");
  tsNumOfMnodeReadThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfVnodeQueryThreads");
  tsNumOfVnodeQueryThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfVnodeFetchThreads");
  tsNumOfVnodeFetchThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfVnodeRsmaThreads");
  tsNumOfVnodeRsmaThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfQnodeQueryThreads");
  tsNumOfQnodeQueryThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "rpcQueueMemoryAllowed");
  tsQueueMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64 * QUEUE_MEMORY_USAGE_RATIO;
  tsApplyMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64 * (1 - QUEUE_MEMORY_USAGE_RATIO);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "simdEnable");
  tsSIMDEnable = (bool)pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "AVX512Enable");
  tsAVX512Enable = (bool)pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tagFilterCache");
  tsTagFilterCache = (bool)pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "stableTagFilterCache");
  tsStableTagFilterCache = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "slowLogExceptDb");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_DB_NAME_LEN));
  tstrncpy(tsSlowLogExceptDb, pItem->str, TSDB_DB_NAME_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "slowLogThreshold");
  tsSlowLogThreshold = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "slowLogMaxLen");
  tsSlowLogMaxLen = pItem->i32;

  int32_t scope = 0;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "slowLogScope");
  TAOS_CHECK_RETURN(taosSetSlowLogScope(pItem->str, &scope));
  tsSlowLogScope = scope;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryRspPolicy");
  tsQueryRspPolicy = pItem->i32;
#ifdef USE_MONITOR
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitor");
  tsEnableMonitor = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorInterval");
  tsMonitorInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorFqdn");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_FQDN_LEN));
  tstrncpy(tsMonitorFqdn, pItem->str, TSDB_FQDN_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorPort");
  tsMonitorPort = (uint16_t)pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorMaxLogs");
  tsMonitorMaxLogs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorComp");
  tsMonitorComp = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorLogProtocol");
  tsMonitorLogProtocol = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "monitorForceV2");
  tsMonitorForceV2 = pItem->i32;
#endif
#ifdef USE_AUDIT
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "audit");
  tsEnableAudit = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "auditCreateTable");
  tsEnableAuditCreateTable = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableAuditDelete");
  tsEnableAuditDelete = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "auditInterval");
  tsAuditInterval = pItem->i32;
#endif
#ifdef USE_REPORT
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "telemetryReporting");
  tsEnableTelem = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "crashReporting");
  tsEnableCrashReport = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "telemetryInterval");
  tsTelemInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "telemetryServer");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_FQDN_LEN));
  tstrncpy(tsTelemServer, pItem->str, TSDB_FQDN_LEN);
#endif

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ttlChangeOnWrite");
  tsTtlChangeOnWrite = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ttlFlushThreshold");
  tsTtlFlushThreshold = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "rsyncPort");
  tsRsyncPort = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "snodeAddress");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_FQDN_LEN));
  tstrncpy(tsSnodeAddress, pItem->str, TSDB_FQDN_LEN);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "checkpointBackupDir");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsCheckpointBackupDir, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "telemetryPort");
  tsTelemPort = (uint16_t)pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tmqMaxTopicNum");
  tmqMaxTopicNum = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tmqRowSize");
  tmqRowSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "maxTsmaNum");
  tsMaxTsmaNum = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "diskIDCheckEnabled");
  tsDiskIDCheckEnabled = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "transPullupInterval");
  tsTransPullupInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "compactPullupInterval");
  tsCompactPullupInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "mqRebalanceInterval");
  tsMqRebalanceInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ttlUnit");
  tsTtlUnit = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ttlPushInterval");
  tsTtlPushIntervalSec = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ttlBatchDropNum");
  tsTtlBatchDropNum = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "trimVDbIntervalSec");
  tsTrimVDbIntervalSec = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryTrimIntervalSec");
  tsQueryTrimIntervalSec = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "uptimeInterval");
  tsUptimeInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryRsmaTolerance");
  tsQueryRsmaTolerance = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "timeseriesThreshold");
  tsTimeSeriesThreshold = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "walForceRepair");
  tsWalForceRepair = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "walFsyncDataSizeLimit");
  tsWalFsyncDataSizeLimit = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncElectInterval");
  tsElectInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncHeartbeatInterval");
  tsHeartbeatInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncVnodeElectIntervalMs");
  tsVnodeElectIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncVnodeHeartbeatIntervalMs");
  tsVnodeHeartbeatIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncMnodeElectIntervalMs");
  tsMnodeElectIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncMnodeHeartbeatIntervalMs");
  tsMnodeHeartbeatIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncHeartbeatTimeout");
  tsHeartbeatTimeout = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncSnapReplMaxWaitN");
  tsSnapReplMaxWaitN = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncLogBufferMemoryAllowed");
  tsLogBufferMemoryAllowed = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncApplyQueueSize");
  tsSyncApplyQueueSize = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncRoutineReportInterval");
  tsRoutineReportInterval = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncLogHeartbeat");
  tsSyncLogHeartbeat = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "arbHeartBeatIntervalSec");
  tsArbHeartBeatIntervalMs = pItem->i32 * 1000;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "arbCheckSyncIntervalSec");
  tsArbCheckSyncIntervalMs = pItem->i32 * 1000;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "arbSetAssignedTimeoutSec");
  tsArbSetAssignedTimeoutMs = pItem->i32 * 1000;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "arbHeartBeatIntervalMs");
  tsArbHeartBeatIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "arbCheckSyncIntervalMs");
  tsArbCheckSyncIntervalMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "arbSetAssignedTimeoutMs");
  tsArbSetAssignedTimeoutMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "syncTimeout");
  tsSyncTimeout = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "mndSdbWriteDelta");
  tsMndSdbWriteDelta = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "mndLogRetention");
  tsMndLogRetention = pItem->i64;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "skipGrant");
  tsMndSkipGrant = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableWhiteList");
  tsEnableWhiteList = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "forceKillTrans");
  tsForceKillTrans = pItem->bval;
#ifdef USE_UDF
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "udf");
  tsStartUdfd = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "udfdResFuncs");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, sizeof(tsUdfdResFuncs)));
  tstrncpy(tsUdfdResFuncs, pItem->str, sizeof(tsUdfdResFuncs));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "udfdLdLibPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, sizeof(tsUdfdLdLibPath)));
  tstrncpy(tsUdfdLdLibPath, pItem->str, sizeof(tsUdfdLdLibPath));
  if (tsQueryBufferSize >= 0) {
    tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
  }
#endif
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "cacheLazyLoadThreshold");
  tsCacheLazyLoadThreshold = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "fPrecision");
  tsFPrecision = pItem->fval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "dPrecision");
  tsDPrecision = pItem->fval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "maxRange");
  tsMaxRange = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "curRange");
  tsCurRange = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ifAdtFse");
  tsIfAdtFse = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "compressor");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, sizeof(tsCompressor)));
  tstrncpy(tsCompressor, pItem->str, sizeof(tsCompressor));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "disableStream");
  tsDisableStream = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "streamBufferSize");
  tsStreamBufferSize = pItem->i32;
  tsStreamBufferSizeBytes = tsStreamBufferSize * 1048576L;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfMnodeStreamMgmtThreads");
  tsNumOfMnodeStreamMgmtThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfStreamMgmtThreads");
  tsNumOfStreamMgmtThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfVnodeStreamReaderThreads");
  tsNumOfVnodeStreamReaderThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfStreamTriggerThreads");
  tsNumOfStreamTriggerThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "numOfStreamRunnerThreads");
  tsNumOfStreamRunnerThreads = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "filterScalarMode");
  tsFilterScalarMode = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "pqSortMemThreshold");
  tsPQSortMemThreshold = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minDiskFreeSize");
  tsMinDiskFreeSize = pItem->i64;

#ifdef USE_SHARED_STORAGE
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ssEnabled");
  tsSsEnabled = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ssAccessString");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, sizeof(tsSsAccessString)));
  tstrncpy(tsSsAccessString, pItem->str, sizeof(tsSsAccessString));

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ssAutoMigrateIntervalSec");
  tsSsAutoMigrateIntervalSec = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ssPageCacheSize");
  tsSsPageCacheSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "ssUploadDelaySec");
  tsSsUploadDelaySec = pItem->i32;
#endif

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "experimental");
  tsExperimental = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryUseMemoryPool");
  tsQueryUseMemoryPool = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "memPoolFullFunc");
  tsMemPoolFullFunc = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "singleQueryMaxMemorySize");
  tsSingleQueryMaxMemorySize = pItem->i32;

  // TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "queryBufferPoolSize");
  // tsQueryBufferPoolSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "minReservedMemorySize");
  tsMinReservedMemorySize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "streamNotifyMessageSize");
  tsStreamNotifyMessageSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "streamNotifyFrameSize");
  tsStreamNotifyFrameSize = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "streamBatchRequestWaitMs");
  tsStreamBatchRequestWaitMs = pItem->i32;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsCliKeyPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSCliKeyPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsCliCertPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSCliCertPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsCaPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSCaPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsSvrKeyPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSSvrKeyPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "tlsSvrCertPath");
  TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX));
  tstrncpy(tsTLSSvrCertPath, pItem->str, PATH_MAX);

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "enableTLS");
  tsEnableTLS = pItem->bval;

  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "rpcRecvLogThreshold");
  tsRpcRecvLogThreshold = pItem->i32;

  // GRANT_CFG_GET;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

#ifndef TD_ENTERPRISE
static int32_t taosSetReleaseCfg(SConfig *pCfg) { return 0; }
#else
int32_t taosSetReleaseCfg(SConfig *pCfg);
#endif

static int32_t taosSetAllDebugFlag(SConfig *pCfg, int32_t flag);

static int8_t tsLogCreated = 0;

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  mode = tsc ? LOG_MODE_TAOSC : LOG_MODE_TAOSD;
  SConfig *pCfg = NULL;

  if (atomic_val_compare_exchange_8(&tsLogCreated, 0, 1) != 0) return 0;

  if (tsCfg == NULL) {
    TAOS_CHECK_GOTO(osDefaultInit(), &lino, _exit);
  }

  TAOS_CHECK_GOTO(cfgInit(&pCfg), &lino, _exit);

#ifdef TAOSD_INTEGRATED
  mode |= LOG_MODE_TAOSD;
  tsLogEmbedded = 1;
#else
  tsLogEmbedded = (mode & LOG_MODE_TAOSC) ? 0 : 1;
#endif
  TAOS_CHECK_GOTO(taosAddClientLogCfg(pCfg), &lino, _exit);
  if (mode & LOG_MODE_TAOSD) {
    TAOS_CHECK_GOTO(taosAddServerLogCfg(pCfg), &lino, _exit);
  }

  if ((code = taosLoadCfg(pCfg, envCmd, cfgDir, envFile, apolloUrl)) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to load cfg since %s\n", tstrerror(code));
    goto _exit;
  }

  if ((code = cfgLoadFromArray(pCfg, pArgs)) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to load cfg from array since %s\n", tstrerror(code));
    goto _exit;
  }

  TAOS_CHECK_GOTO(taosSetClientLogCfg(pCfg), &lino, _exit);
  if (mode & LOG_MODE_TAOSD) {
    TAOS_CHECK_GOTO(taosSetServerLogCfg(pCfg), &lino, _exit);
  }

  SConfigItem *pDebugItem = NULL;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pDebugItem, "debugFlag");
  TAOS_CHECK_GOTO(taosSetAllDebugFlag(pCfg, pDebugItem->i32), &lino, _exit);

  if ((code = taosMulModeMkDir(tsLogDir, 0777, true)) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to create dir:%s since %s\n", tsLogDir, tstrerror(code));
    goto _exit;
  }

  if ((code = taosInitLog(logname, logFileNum, mode)) != 0) {
    (void)printf("failed to init log file since %s\n", tstrerror(code));
    goto _exit;
  }

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    (void)printf("failed to create log at %d since %s\n", lino, tstrerror(code));
  }

  cfgCleanup(pCfg);
  TAOS_RETURN(code);
}

int32_t taosReadDataFolder(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl,
                           SArray *pArgs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = -1;
  if (tsCfg == NULL) code = osDefaultInit();
  if (code != 0) {
    (void)printf("failed to init os since %s\n", tstrerror(code));
  }

  SConfig *pCfg = NULL;
  TAOS_CHECK_RETURN(cfgInit(&pCfg));
  TAOS_CHECK_GOTO(cfgAddDir(pCfg, "dataDir", tsDataDir, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL), &lino,
                  _exit);
  TAOS_CHECK_GOTO(
      cfgAddInt32(pCfg, "debugFlag", dDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL), &lino,
      _exit);
  TAOS_CHECK_GOTO(
      cfgAddInt32(pCfg, "dDebugFlag", dDebugFlag, 0, 255, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_LOCAL), &lino,
      _exit);

  if ((code = taosLoadCfg(pCfg, envCmd, cfgDir, envFile, apolloUrl)) != 0) {
    (void)printf("failed to load cfg since %s\n", tstrerror(code));
    goto _exit;
  }

  if ((code = cfgLoadFromArray(pCfg, pArgs)) != 0) {
    (void)printf("failed to load cfg from array since %s\n", tstrerror(code));
    goto _exit;
  }

  TAOS_CHECK_GOTO(taosSetTfsCfg(pCfg), &lino, _exit);

  SConfigItem *pItem = NULL;
  if ((pItem = cfgGetItem(pCfg, "dDebugFlag")) == NULL) {
    code = TSDB_CODE_CFG_NOT_FOUND;
    goto _exit;
  }

  dDebugFlag = pItem->i32;

_exit:
  cfgCleanup(pCfg);
  TAOS_RETURN(code);
}

static int32_t taosCheckGlobalCfg() {
  SIpAddr addr = {0};
  uInfo("check global fqdn:%s and port:%u", tsLocalFqdn, tsServerPort);
  int32_t code = taosGetIpFromFqdn(tsEnableIpv6, tsLocalFqdn, &addr);
  if (code) {
    uError("failed to get ip from fqdn:%s since %s, can not be initialized", tsLocalFqdn, tstrerror(code));
    TAOS_RETURN(TSDB_CODE_RPC_FQDN_ERROR);
  }

  if (tsServerPort <= 0) {
    uError("invalid server port:%u, can not be initialized", tsServerPort);
    TAOS_RETURN(TSDB_CODE_RPC_FQDN_ERROR);
  }

  if (tsMqttPort <= 0 || tsMqttPort == tsServerPort) {
    uError("invalid mqtt port:%u, can not be initialized", tsMqttPort);
    TAOS_RETURN(TSDB_CODE_RPC_FQDN_ERROR);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgInitWrapper(SConfig **pCfg) {
  if (*pCfg == NULL) {
    TAOS_CHECK_RETURN(cfgInit(pCfg));
  }
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t setAllConfigs(SConfig *pCfg) {
  int32_t code = 0;
  int32_t lino = -1;
  TAOS_CHECK_GOTO(taosSetClientCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosUpdateServerCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosSetServerCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosSetReleaseCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosSetTfsCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosSetSystemCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosSetFileHandlesLimit(), &lino, _exit);
_exit:
  TAOS_RETURN(code);
}

int32_t cfgDeserialize(SArray *array, char *buf, bool isGlobal) {
  int32_t code = TSDB_CODE_SUCCESS;

  cJSON *pRoot = cJSON_Parse(buf);
  if (pRoot == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (isGlobal) {
    cJSON *pItem = cJSON_GetObjectItem(pRoot, "version");
    if (pItem == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    tsdmConfigVersion = pItem->valueint;
  }

  int32_t sz = taosArrayGetSize(array);
  cJSON  *configs = cJSON_GetObjectItem(pRoot, "configs");
  if (configs == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  for (int i = 0; i < sz; i++) {
    SConfigItem *pItem = (SConfigItem *)taosArrayGet(array, i);
    cJSON       *pJson = cJSON_GetObjectItem(configs, pItem->name);
    if (pJson == NULL) {
      continue;
    }
    if (strcasecmp(pItem->name, "dataDir") == 0) {
      if (!tsDiskIDCheckEnabled) {
        continue;
      }
      int    sz = cJSON_GetArraySize(pJson);
      cJSON *filed = NULL;
      // check disk id for each dir
      for (int j = 0; j < sz; j++) {
        cJSON *diskCfgJson = cJSON_GetArrayItem(pJson, j);
        if (diskCfgJson == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }

        filed = cJSON_GetObjectItem(diskCfgJson, "dir");
        if (filed == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }

        char *dir = cJSON_GetStringValue(filed);
        filed = cJSON_GetObjectItem(diskCfgJson, "disk_id");
        if (filed == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }

        int64_t actDiskID = 0;
        int64_t expDiskID = taosStr2Int64(cJSON_GetStringValue(filed), NULL, 10);
        if ((code = taosGetFileDiskID(dir, &actDiskID)) != 0) {
          uError("failed to get disk id for dir:%s, since %s", dir, tstrerror(code));
          goto _exit;
        }
        if (actDiskID != expDiskID) {
          uError("failed to check disk id for dir:%s, actDiskID%" PRId64 ", expDiskID%" PRId64, dir, actDiskID,
                 expDiskID);
          code = TSDB_CODE_INVALID_DISK_ID;
          goto _exit;
        }
      }
      continue;
    }
    switch (pItem->dtype) {
      {
        case CFG_DTYPE_NONE:
          break;
        case CFG_DTYPE_BOOL:
          pItem->bval = cJSON_IsTrue(pJson);
          break;
        case CFG_DTYPE_INT32:
          pItem->i32 = pJson->valueint;
          break;
        case CFG_DTYPE_INT64:
          pItem->i64 = atoll(cJSON_GetStringValue(pJson));
          break;
        case CFG_DTYPE_FLOAT:
        case CFG_DTYPE_DOUBLE:
          pItem->fval = taosStr2Float(cJSON_GetStringValue(pJson), NULL);
          break;
        case CFG_DTYPE_STRING:
        case CFG_DTYPE_DIR:
        case CFG_DTYPE_LOCALE:
        case CFG_DTYPE_CHARSET:
        case CFG_DTYPE_TIMEZONE:
          taosMemoryFree(pItem->str);
          pItem->str = taosStrdup(pJson->valuestring);
          if (pItem->str == NULL) {
            code = terrno;
            goto _exit;
          }
          break;
      }
    }
  }
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to deserialize config since %s", tstrerror(code));
  }
  cJSON_Delete(pRoot);
  return code;
}

int32_t stypeConfigDeserialize(SArray *array, char *buf) {
  int32_t code = TSDB_CODE_SUCCESS;
  cJSON  *pRoot = cJSON_Parse(buf);
  if (pRoot == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  cJSON *stypes = cJSON_GetObjectItem(pRoot, "config_stypes");
  if (stypes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  int32_t sz = taosArrayGetSize(array);
  for (int i = 0; i < sz; i++) {
    SConfigItem *pItem = (SConfigItem *)taosArrayGet(array, i);
    cJSON       *pJson = cJSON_GetObjectItem(stypes, pItem->name);
    if (pJson == NULL) {
      continue;
    }
    pItem->stype = pJson->valueint;
  }
  cJSON_Delete(pRoot);
_exit:
  return code;
}

int32_t readStypeConfigFile(const char *path) {
  int32_t   code = 0;
  char      filename[CONFIG_FILE_LEN] = {0};
  SArray   *array = NULL;
  char     *buf = NULL;
  TdFilePtr pFile = NULL;

  array = taosGetLocalCfg(tsCfg);
  if (array == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  snprintf(filename, sizeof(filename), "%s%sdnode%sconfig%sstype.json", path, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  uInfo("start to read stype config file:%s", filename);
  if (!taosCheckExistFile(filename)) {
    uInfo("stype config file:%s does not exist", filename);
    goto _exit;
  }
  int64_t fileSize = 0;
  if (taosStatFile(filename, &fileSize, NULL, NULL) < 0) {
    code = terrno;
    goto _exit;
  }
  buf = (char *)taosMemoryMalloc(fileSize + 1);
  if (buf == NULL) {
    code = terrno;
    goto _exit;
  }
  pFile = taosOpenFile(filename, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    goto _exit;
  }
  if (taosReadFile(pFile, buf, fileSize) != fileSize) {
    code = terrno;
    goto _exit;
  }
  buf[fileSize] = '\0';

  code = stypeConfigDeserialize(array, buf);
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to deserialize stype config file:%s since %s", filename, tstrerror(code));
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to read stype config file:%s since %s", filename, tstrerror(code));
  }
  taosMemoryFree(buf);
  (void)taosCloseFile(&pFile);
  TAOS_RETURN(code);
}

int32_t readCfgFile(const char *path, bool isGlobal) {
  int32_t code = 0;
  char    filename[CONFIG_FILE_LEN] = {0};
  SArray *array = NULL;
  if (isGlobal) {
    array = taosGetGlobalCfg(tsCfg);
    snprintf(filename, sizeof(filename), "%s%sdnode%sconfig%sglobal.json", path, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  } else {
    array = taosGetLocalCfg(tsCfg);
    snprintf(filename, sizeof(filename), "%s%sdnode%sconfig%slocal.json", path, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  }
  uInfo("start to read config file:%s", filename);

  if (!taosCheckExistFile(filename)) {
    uInfo("config file:%s does not exist", filename);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }
  int64_t fileSize = 0;
  char   *buf = NULL;
  if (taosStatFile(filename, &fileSize, NULL, NULL) < 0) {
    code = terrno;
    uError("failed to stat file:%s , since %s", filename, tstrerror(code));
    TAOS_RETURN(code);
  }
  if (fileSize == 0) {
    uInfo("config file:%s is empty", filename);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }
  TdFilePtr pFile = taosOpenFile(filename, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    uError("failed to open file:%s , since %s", filename, tstrerror(code));
    goto _exit;
  }
  buf = (char *)taosMemoryMalloc(fileSize + 1);
  if (buf == NULL) {
    code = terrno;
    uError("failed to malloc memory for file:%s , since %s", filename, tstrerror(code));
    goto _exit;
  }
  if (taosReadFile(pFile, buf, fileSize) != fileSize) {
    code = terrno;
    uError("failed to read file:%s , config since %s", filename, tstrerror(code));
    goto _exit;
  }
  buf[fileSize] = '\0';  // 添加字符串结束符
  code = cfgDeserialize(array, buf, isGlobal);
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to deserialize config from %s since %s", filename, tstrerror(code));
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to read config from %s since %s", filename, tstrerror(code));
  }
  taosMemoryFree(buf);
  (void)taosCloseFile(&pFile);
  TAOS_RETURN(code);
}

int32_t tryLoadCfgFromDataDir(SConfig *pCfg) {
  int32_t      code = 0;
  SConfigItem *pItem = NULL;
  TAOS_CHECK_GET_CFG_ITEM(tsCfg, pItem, "forceReadConfig");
  tsForceReadConfig = pItem->i32;
  code = readCfgFile(tsDataDir, true);
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to read global config from %s since %s", tsDataDir, tstrerror(code));
    TAOS_RETURN(code);
  }
  if (!tsForceReadConfig) {
    uInfo("load config from tsDataDir:%s", tsDataDir);
    code = readCfgFile(tsDataDir, false);
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to read local config from %s since %s", tsDataDir, tstrerror(code));
      TAOS_RETURN(code);
    }
    code = readStypeConfigFile(tsDataDir);
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to read stype config from %s since %s", tsDataDir, tstrerror(code));
      TAOS_RETURN(code);
    }
  }
  TAOS_RETURN(code);
}

int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc) {
  if (tsCfg != NULL) TAOS_RETURN(TSDB_CODE_SUCCESS);

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = -1;

  TAOS_CHECK_GOTO(cfgInitWrapper(&tsCfg), &lino, _exit);

  if (tsc) {
    TAOS_CHECK_GOTO(taosAddClientCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosAddClientLogCfg(tsCfg), &lino, _exit);
  } else {
    TAOS_CHECK_GOTO(taosAddClientCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosAddServerCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosAddClientLogCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosAddServerLogCfg(tsCfg), &lino, _exit);
  }

  TAOS_CHECK_GOTO(taosAddSystemCfg(tsCfg), &lino, _exit);

  if ((code = taosLoadCfg(tsCfg, envCmd, cfgDir, envFile, apolloUrl)) != 0) {
    (void)printf("failed to load cfg since %s\n", tstrerror(code));
    cfgCleanup(tsCfg);
    tsCfg = NULL;
    TAOS_RETURN(code);
  }

  if ((code = cfgLoadFromArray(tsCfg, pArgs)) != 0) {
    (void)printf("failed to load cfg from array since %s\n", tstrerror(code));
    cfgCleanup(tsCfg);
    tsCfg = NULL;
    TAOS_RETURN(code);
  }

  if (!tsc) {
    TAOS_CHECK_GOTO(taosSetTfsCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(tryLoadCfgFromDataDir(tsCfg), &lino, _exit);
  }

  if (tsc) {
    TAOS_CHECK_GOTO(taosSetClientLogCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosSetClientCfg(tsCfg), &lino, _exit);
  } else {
    TAOS_CHECK_GOTO(taosSetClientLogCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosSetClientCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosUpdateServerCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosSetServerCfg(tsCfg), &lino, _exit);
    TAOS_CHECK_GOTO(taosSetReleaseCfg(tsCfg), &lino, _exit);
  }

  TAOS_CHECK_GOTO(taosSetSystemCfg(tsCfg), &lino, _exit);
  TAOS_CHECK_GOTO(taosSetFileHandlesLimit(), &lino, _exit);

  SConfigItem *pItem = cfgGetItem(tsCfg, "debugFlag");
  if (NULL == pItem) {
    (void)printf("debugFlag not found in cfg\n");
    TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);
  }
  TAOS_CHECK_GOTO(taosSetAllDebugFlag(tsCfg, pItem->i32), &lino, _exit);

  cfgDumpCfg(tsCfg, tsc, false);
  TAOS_CHECK_GOTO(taosCheckGlobalCfg(), &lino, _exit);

_exit:
  if (TSDB_CODE_SUCCESS != code) {
    cfgCleanup(tsCfg);
    tsCfg = NULL;
    (void)printf("failed to init cfg at %d since %s\n", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
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
  int32_t code = TSDB_CODE_CFG_NOT_FOUND;
  char   *name = pItem->name;
  for (int32_t d = 0; d < optionSize; ++d) {
    const char *optName = pOptions[d].optionName;
    if (taosStrcasecmp(name, optName) != 0) continue;
    code = TSDB_CODE_SUCCESS;
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL: {
        int32_t flag = pItem->i32;
        bool   *pVar = pOptions[d].optionVar;
        uInfo("%s set from %d to %d", optName, *pVar, flag);
        *pVar = flag;
      } break;
      case CFG_DTYPE_INT32: {
        int32_t  flag = pItem->i32;
        int32_t *pVar = pOptions[d].optionVar;
        uInfo("%s set from %d to %d", optName, *pVar, flag);
        *pVar = flag;

        if (isDebugflag) {
          TAOS_CHECK_RETURN(taosSetDebugFlag(pOptions[d].optionVar, optName, flag));
        }
      } break;
      case CFG_DTYPE_INT64: {
        int64_t  flag = pItem->i64;
        int64_t *pVar = pOptions[d].optionVar;
        uInfo("%s set from %" PRId64 " to %" PRId64, optName, *pVar, flag);
        *pVar = flag;
      } break;
      case CFG_DTYPE_FLOAT:
      case CFG_DTYPE_DOUBLE: {
        float  flag = pItem->fval;
        float *pVar = pOptions[d].optionVar;
        uInfo("%s set from %f to %f", optName, *pVar, flag);
        *pVar = flag;
      } break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE: {
        if (strcasecmp(pItem->name, "slowLogExceptDb") == 0) {
          TAOS_CHECK_RETURN(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_DB_NAME_LEN));
          tstrncpy(tsSlowLogExceptDb, pItem->str, TSDB_DB_NAME_LEN);
        } else {
          uError("not support string type for %s", optName);
          code = TSDB_CODE_INVALID_CFG;
          break;
        }
        uInfo("%s set to %s", optName, pItem->str);
      } break;
      default:
        code = TSDB_CODE_INVALID_CFG;
        break;
    }

    break;
  }

  TAOS_RETURN(code);
}

extern void tsdbAlterNumCompactThreads();

static int32_t taosCfgDynamicOptionsForServer(SConfig *pCfg, const char *name) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = -1;

  if (strcasecmp("timezone", name) == 0) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  if (strcasecmp(name, "resetlog") == 0) {
    // trigger, no item in cfg
    taosResetLog();
    cfgDumpCfg(tsCfg, 0, false);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  cfgLock(pCfg);

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (!pItem || (pItem->dynScope == CFG_DYN_CLIENT)) {
    uError("failed to config:%s, not support", name);
    code = TSDB_CODE_INVALID_CFG;
    goto _exit;
  }

  if (strncasecmp(name, "debugFlag", 9) == 0) {
    code = taosSetAllDebugFlag(pCfg, pItem->i32);
    goto _exit;
  }

  if (strncasecmp(name, "enableCoreFile", 9) == 0) {
    tsEnableCoreFile = pItem->bval;
    taosSetCoreDump(tsEnableCoreFile);
    uInfo("%s set to %d", name, tsEnableCoreFile);
    goto _exit;
  }

  if (strcasecmp("slowLogScope", name) == 0) {
    int32_t scope = 0;
    TAOS_CHECK_GOTO(taosSetSlowLogScope(pItem->str, &scope), &lino, _exit);
    tsSlowLogScope = scope;
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  if (strcasecmp("slowLogExceptDb", name) == 0) {
    tstrncpy(tsSlowLogExceptDb, pItem->str, TSDB_DB_NAME_LEN);
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }
  if (strcasecmp(name, "dataDir") == 0) {
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }
  if (strcasecmp("rpcQueueMemoryAllowed", name) == 0) {
    tsQueueMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64 * QUEUE_MEMORY_USAGE_RATIO;
    tsApplyMemoryAllowed = cfgGetItem(pCfg, "rpcQueueMemoryAllowed")->i64 * (1 - QUEUE_MEMORY_USAGE_RATIO);
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  if (strcasecmp(name, "numOfCompactThreads") == 0) {
#ifdef TD_ENTERPRISE
    tsNumOfCompactThreads = pItem->i32;
    code = TSDB_CODE_SUCCESS;
    // tsdbAlterNumCompactThreads();
#else
    code = TSDB_CODE_INVALID_CFG;
#endif
    goto _exit;
  }
#ifdef TD_ENTERPRISE
  if (strcasecmp(name, "authServer") == 0) {
    tsAuthServer = pItem->bval;
    goto _exit;
  }
  if (strcasecmp(name, "authReq") == 0) {
    tsAuthReq = pItem->bval;
    goto _exit;
  }
  if (strcasecmp(name, "authReqInterval") == 0) {
    tsAuthReqInterval = pItem->i32;
    goto _exit;
  }
  if (strcasecmp(name, "authReqUrl") == 0) {
    TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_FQDN_LEN), &lino, _exit);
    tstrncpy(tsAuthReqUrl, pItem->str, TSDB_FQDN_LEN);
    goto _exit;
  }
#endif

  if (strcasecmp(name, "minReservedMemorySize") == 0) {
    tsMinReservedMemorySize = pItem->i32;
    code = taosMemoryPoolCfgUpdateReservedSize(tsMinReservedMemorySize);
    goto _exit;
  }

  if (strcasecmp(name, "streamBufferSize") == 0) {
    tsStreamBufferSize = pItem->i32;
    atomic_store_64(&tsStreamBufferSizeBytes, tsStreamBufferSize * 1048576);
    goto _exit;
  }

  {  //  'bool/int32_t/int64_t/float/double' variables with general modification function
    static OptionNameAndVar debugOptions[] = {
        {"dDebugFlag", &dDebugFlag},       {"vDebugFlag", &vDebugFlag},     {"mDebugFlag", &mDebugFlag},
        {"wDebugFlag", &wDebugFlag},       {"azDebugFlag", &azDebugFlag},   {"sDebugFlag", &sDebugFlag},
        {"tsdbDebugFlag", &tsdbDebugFlag}, {"tqDebugFlag", &tqDebugFlag},   {"fsDebugFlag", &fsDebugFlag},
        {"udfDebugFlag", &udfDebugFlag},   {"smaDebugFlag", &smaDebugFlag}, {"idxDebugFlag", &idxDebugFlag},
        {"tdbDebugFlag", &tdbDebugFlag},   {"tmrDebugFlag", &tmrDebugFlag}, {"uDebugFlag", &uDebugFlag},
        {"smaDebugFlag", &smaDebugFlag},   {"rpcDebugFlag", &rpcDebugFlag}, {"qDebugFlag", &qDebugFlag},
        {"metaDebugFlag", &metaDebugFlag}, {"stDebugFlag", &stDebugFlag},   {"bseDebugFlag", &bseDebugFlag},
        {"sndDebugFlag", &sndDebugFlag},   {"bndDebugFlag", &bndDebugFlag}, {"tqClientDebugFlag", &tqClientDebugFlag},
        {"tssDebugFlag", &tssDebugFlag},
    };

    static OptionNameAndVar options[] = {{"audit", &tsEnableAudit},
                                         {"asynclog", &tsAsyncLog},
                                         {"enableWhiteList", &tsEnableWhiteList},
                                         {"statusInterval", &tsStatusInterval},
                                         {"statusIntervalMs", &tsStatusIntervalMs},
                                         {"statusSRTimeoutMs", &tsStatusSRTimeoutMs},
                                         {"statusTimeoutMs", &tsStatusTimeoutMs},
                                         {"telemetryReporting", &tsEnableTelem},
                                         {"monitor", &tsEnableMonitor},
                                         {"monitorInterval", &tsMonitorInterval},
                                         {"monitorComp", &tsMonitorComp},
                                         {"monitorForceV2", &tsMonitorForceV2},
                                         {"monitorLogProtocol", &tsMonitorLogProtocol},
                                         {"monitorMaxLogs", &tsMonitorMaxLogs},
                                         {"auditCreateTable", &tsEnableAuditCreateTable},
                                         {"auditInterval", &tsAuditInterval},
                                         {"slowLogThreshold", &tsSlowLogThreshold},
                                         {"compressMsgSize", &tsCompressMsgSize},
                                         {"compressor", &tsCompressor},
                                         {"dPrecision", &tsDPrecision},
                                         {"fPrecision", &tsFPrecision},
                                         {"maxRange", &tsMaxRange},
                                         {"maxTsmaNum", &tsMaxTsmaNum},
                                         {"queryRsmaTolerance", &tsQueryRsmaTolerance},
                                         {"uptimeInterval", &tsUptimeInterval},

                                         {"slowLogMaxLen", &tsSlowLogMaxLen},
                                         {"slowLogScope", &tsSlowLogScope},
                                         {"slowLogExceptDb", &tsSlowLogExceptDb},

                                         {"mndSdbWriteDelta", &tsMndSdbWriteDelta},
                                         {"minDiskFreeSize", &tsMinDiskFreeSize},
                                         {"randErrorChance", &tsRandErrChance},
                                         {"randErrorDivisor", &tsRandErrDivisor},
                                         {"randErrorScope", &tsRandErrScope},
                                         {"syncLogBufferMemoryAllowed", &tsLogBufferMemoryAllowed},
                                         {"syncApplyQueueSize", &tsSyncApplyQueueSize},
                                         {"syncHeartbeatInterval", &tsHeartbeatInterval},
                                         {"syncElectInterval", &tsElectInterval},
                                         {"syncVnodeHeartbeatIntervalMs", &tsVnodeHeartbeatIntervalMs},
                                         {"syncVnodeElectIntervalMs", &tsVnodeElectIntervalMs},
                                         {"syncMnodeHeartbeatIntervalMs", &tsMnodeHeartbeatIntervalMs},
                                         {"syncMnodeElectIntervalMs", &tsMnodeElectIntervalMs},
                                         {"syncHeartbeatTimeout", &tsHeartbeatTimeout},
                                         {"syncSnapReplMaxWaitN", &tsSnapReplMaxWaitN},
                                         {"syncRoutineReportInterval", &tsRoutineReportInterval},
                                         {"syncLogHeartbeat", &tsSyncLogHeartbeat},
                                         {"syncTimeout", &tsSyncTimeout},
                                         {"walFsyncDataSizeLimit", &tsWalFsyncDataSizeLimit},

                                         {"numOfCores", &tsNumOfCores},

                                         {"enableCoreFile", &tsEnableCoreFile},

                                         {"telemetryInterval", &tsTelemInterval},

                                         {"cacheLazyLoadThreshold", &tsCacheLazyLoadThreshold},

                                         {"retentionSpeedLimitMB", &tsRetentionSpeedLimitMB},
                                         {"ttlChangeOnWrite", &tsTtlChangeOnWrite},

                                         {"logKeepDays", &tsLogKeepDays},
                                         {"mqRebalanceInterval", &tsMqRebalanceInterval},
                                         {"numOfLogLines", &tsNumOfLogLines},
                                         {"queryRspPolicy", &tsQueryRspPolicy},
                                         {"timeseriesThreshold", &tsTimeSeriesThreshold},
                                         {"tmqMaxTopicNum", &tmqMaxTopicNum},
                                         {"tmqRowSize", &tmqRowSize},
                                         {"transPullupInterval", &tsTransPullupInterval},
                                         {"compactPullupInterval", &tsCompactPullupInterval},
                                         {"queryTrimIntervalSec", &tsQueryTrimIntervalSec},
                                         {"trimVDbIntervalSec", &tsTrimVDbIntervalSec},
                                         {"ttlBatchDropNum", &tsTtlBatchDropNum},
                                         {"ttlFlushThreshold", &tsTtlFlushThreshold},
                                         {"ttlPushInterval", &tsTtlPushIntervalSec},
                                         {"ttlUnit", &tsTtlUnit},
                                         {"ssAutoMigrateIntervalSec", &tsSsAutoMigrateIntervalSec},
                                         {"ssBlockCacheSize", &tsSsBlockCacheSize},
                                         {"ssPageCacheSize", &tsSsPageCacheSize},
                                         {"ssUploadDelaySec", &tsSsUploadDelaySec},
                                         {"mndLogRetention", &tsMndLogRetention},
                                         {"supportVnodes", &tsNumOfSupportVnodes},
                                         {"experimental", &tsExperimental},

                                         {"numOfRpcSessions", &tsNumOfRpcSessions},
                                         {"shellActivityTimer", &tsShellActivityTimer},
                                         {"readTimeout", &tsReadTimeout},
                                         {"safetyCheckLevel", &tsSafetyCheckLevel},
                                         {"bypassFlag", &tsBypassFlag},
                                         {"arbHeartBeatIntervalSec", &tsArbHeartBeatIntervalSec},
                                         {"arbCheckSyncIntervalSec", &tsArbCheckSyncIntervalSec},
                                         {"arbSetAssignedTimeoutSec", &tsArbSetAssignedTimeoutSec},
                                         {"arbHeartBeatIntervalMs", &tsArbHeartBeatIntervalMs},
                                         {"arbCheckSyncIntervalMs", &tsArbCheckSyncIntervalMs},
                                         {"arbSetAssignedTimeoutMs", &tsArbSetAssignedTimeoutMs},
                                         {"queryNoFetchTimeoutSec", &tsQueryNoFetchTimeoutSec},
                                         {"enableStrongPassword", &tsEnableStrongPassword},
                                         {"enableMetrics", &tsEnableMetrics},
                                         {"metricsInterval", &tsMetricsInterval},
                                         {"metricsLevel", &tsMetricsLevel},
                                         {"forceKillTrans", &tsForceKillTrans},
                                         {"enableTLS", &tsEnableTLS},
                                         {"rpcRecvLogThreshold", &tsRpcRecvLogThreshold},
                                         {"tagFilterCache", &tsTagFilterCache},
                                         {"stableTagFilterCache", &tsStableTagFilterCache}};

    if ((code = taosCfgSetOption(debugOptions, tListLen(debugOptions), pItem, true)) != TSDB_CODE_SUCCESS) {
      code = taosCfgSetOption(options, tListLen(options), pItem, false);
    }
  }

_exit:
  cfgUnLock(pCfg);
  TAOS_RETURN(code);
}

static int32_t taosCfgDynamicOptionsForClient(SConfig *pCfg, const char *name) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (strcasecmp("charset", name) == 0 || strcasecmp("timezone", name) == 0) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  cfgLock(pCfg);

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if ((pItem == NULL) || pItem->dynScope == CFG_DYN_SERVER) {
    uError("failed to config:%s, not support", name);
    code = TSDB_CODE_INVALID_CFG;
    goto _out;
  }

  bool matched = false;

  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  (void)strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));

  switch (lowcaseName[0]) {
    case 'd': {
      if (strcasecmp("debugFlag", name) == 0) {
        code = taosSetAllDebugFlag(pCfg, pItem->i32);
        matched = true;
      }
      break;
    }
    case 'e': {
      if (strcasecmp("enableCoreFile", name) == 0) {
        tsEnableCoreFile = pItem->bval;
        taosSetCoreDump(tsEnableCoreFile);
        uInfo("%s set to %d", name, tsEnableCoreFile);
        matched = true;
      }
      break;
    }
    case 'f': {
      if (strcasecmp("fqdn", name) == 0) {
        SConfigItem *pFqdnItem = cfgGetItem(pCfg, "fqdn");
        SConfigItem *pServerPortItem = cfgGetItem(pCfg, "serverPort");
        SConfigItem *pFirstEpItem = cfgGetItem(pCfg, "firstEp");
        if (pFqdnItem == NULL || pServerPortItem == NULL || pFirstEpItem == NULL) {
          uError("failed to get fqdn or serverPort or firstEp from cfg");
          code = TSDB_CODE_CFG_NOT_FOUND;
          goto _out;
        }

        tstrncpy(tsLocalFqdn, pFqdnItem->str, TSDB_FQDN_LEN);
        tsServerPort = (uint16_t)pServerPortItem->i32;
        (void)snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%u", tsLocalFqdn, tsServerPort);

        char defaultFirstEp[TSDB_EP_LEN] = {0};
        (void)snprintf(defaultFirstEp, TSDB_EP_LEN, "%s:%u", tsLocalFqdn, tsServerPort);

        SEp firstEp = {0};
        TAOS_CHECK_GOTO(
            taosGetFqdnPortFromEp(strlen(pFirstEpItem->str) == 0 ? defaultFirstEp : pFirstEpItem->str, &firstEp), &lino,
            _out);
        (void)snprintf(tsFirst, sizeof(tsFirst), "%s:%u", firstEp.fqdn, firstEp.port);

        TAOS_CHECK_GOTO(cfgSetItem(pCfg, "firstEp", tsFirst, pFirstEpItem->stype, false), &lino, _out);
        uInfo("localEp set to '%s', tsFirst set to '%s'", tsLocalEp, tsFirst);
        matched = true;
      } else if (strcasecmp("firstEp", name) == 0) {
        SConfigItem *pFqdnItem = cfgGetItem(pCfg, "fqdn");
        SConfigItem *pServerPortItem = cfgGetItem(pCfg, "serverPort");
        SConfigItem *pFirstEpItem = cfgGetItem(pCfg, "firstEp");
        if (pFqdnItem == NULL || pServerPortItem == NULL || pFirstEpItem == NULL) {
          uError("failed to get fqdn or serverPort or firstEp from cfg");
          code = TSDB_CODE_CFG_NOT_FOUND;
          goto _out;
        }

        tstrncpy(tsLocalFqdn, pFqdnItem->str, TSDB_FQDN_LEN);
        tsServerPort = (uint16_t)pServerPortItem->i32;
        (void)snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%u", tsLocalFqdn, tsServerPort);

        char defaultFirstEp[TSDB_EP_LEN] = {0};
        (void)snprintf(defaultFirstEp, TSDB_EP_LEN, "%s:%u", tsLocalFqdn, tsServerPort);

        SEp firstEp = {0};
        TAOS_CHECK_GOTO(
            taosGetFqdnPortFromEp(strlen(pFirstEpItem->str) == 0 ? defaultFirstEp : pFirstEpItem->str, &firstEp), &lino,
            _out);
        (void)snprintf(tsFirst, sizeof(tsFirst), "%s:%u", firstEp.fqdn, firstEp.port);

        TAOS_CHECK_GOTO(cfgSetItem(pCfg, "firstEp", tsFirst, pFirstEpItem->stype, false), &lino, _out);
        uInfo("localEp set to '%s', tsFirst set to '%s'", tsLocalEp, tsFirst);
        matched = true;
      }
      break;
    }
    case 'l': {
      if (strcasecmp("locale", name) == 0) {
        SConfigItem *pLocaleItem = cfgGetItem(pCfg, "locale");
        if (pLocaleItem == NULL) {
          uError("failed to get locale from cfg");
          code = TSDB_CODE_CFG_NOT_FOUND;
          goto _out;
        }

        const char *locale = pLocaleItem->str;
        TAOS_CHECK_GOTO(taosSetSystemLocale(locale), &lino, _out);
        uInfo("locale set to '%s'", locale);
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
        TAOS_CHECK_GOTO(taosGetFqdnPortFromEp(strlen(pItem->str) == 0 ? tsFirst : pItem->str, &secondEp), &lino, _out);
        (void)snprintf(tsSecond, sizeof(tsSecond), "%s:%u", secondEp.fqdn, secondEp.port);
        TAOS_CHECK_GOTO(cfgSetItem(pCfg, "secondEp", tsSecond, pItem->stype, false), &lino, _out);
        uInfo("%s set to %s", name, tsSecond);
        matched = true;
      } else if (strcasecmp("smlChildTableName", name) == 0) {
        TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_TABLE_NAME_LEN), &lino, _out);
        uInfo("%s set from %s to %s", name, tsSmlChildTableName, pItem->str);
        tstrncpy(tsSmlChildTableName, pItem->str, TSDB_TABLE_NAME_LEN);
        matched = true;
      } else if (strcasecmp("smlAutoChildTableNameDelimiter", name) == 0) {
        TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_TABLE_NAME_LEN), &lino, _out);
        uInfo("%s set from %s to %s", name, tsSmlAutoChildTableNameDelimiter, pItem->str);
        tstrncpy(tsSmlAutoChildTableNameDelimiter, pItem->str, TSDB_TABLE_NAME_LEN);
        matched = true;
      } else if (strcasecmp("smlTagName", name) == 0) {
        TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_COL_NAME_LEN), &lino, _out);
        uInfo("%s set from %s to %s", name, tsSmlTagName, pItem->str);
        tstrncpy(tsSmlTagName, pItem->str, TSDB_COL_NAME_LEN);
        matched = true;
      } else if (strcasecmp("smlTsDefaultName", name) == 0) {
        TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pItem->name, pItem->str, TSDB_COL_NAME_LEN), &lino, _out);
        uInfo("%s set from %s to %s", name, tsSmlTsDefaultName, pItem->str);
        tstrncpy(tsSmlTsDefaultName, pItem->str, TSDB_COL_NAME_LEN);
        matched = true;
      } else if (strcasecmp("serverPort", name) == 0) {
        SConfigItem *pFqdnItem = cfgGetItem(pCfg, "fqdn");
        SConfigItem *pServerPortItem = cfgGetItem(pCfg, "serverPort");
        SConfigItem *pFirstEpItem = cfgGetItem(pCfg, "firstEp");
        if (pFqdnItem == NULL || pServerPortItem == NULL || pFirstEpItem == NULL) {
          uError("failed to get fqdn or serverPort or firstEp from cfg");
          code = TSDB_CODE_CFG_NOT_FOUND;
          goto _out;
        }

        TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pFqdnItem->name, pFqdnItem->str, TSDB_FQDN_LEN), &lino, _out);
        tstrncpy(tsLocalFqdn, pFqdnItem->str, TSDB_FQDN_LEN);
        tsServerPort = (uint16_t)pServerPortItem->i32;
        (void)snprintf(tsLocalEp, sizeof(tsLocalEp), "%s:%u", tsLocalFqdn, tsServerPort);

        char defaultFirstEp[TSDB_EP_LEN] = {0};
        (void)snprintf(defaultFirstEp, TSDB_EP_LEN, "%s:%u", tsLocalFqdn, tsServerPort);

        SEp firstEp = {0};
        TAOS_CHECK_GOTO(
            taosGetFqdnPortFromEp(strlen(pFirstEpItem->str) == 0 ? defaultFirstEp : pFirstEpItem->str, &firstEp), &lino,
            _out);
        (void)snprintf(tsFirst, sizeof(tsFirst), "%s:%u", firstEp.fqdn, firstEp.port);

        TAOS_CHECK_GOTO(cfgSetItem(pCfg, "firstEp", tsFirst, pFirstEpItem->stype, false), &lino, _out);
        uInfo("localEp set to '%s', tsFirst set to '%s'", tsLocalEp, tsFirst);
        matched = true;
      }
      break;
    }
    case 't': {
      if (strcasecmp("tempDir", name) == 0) {
        TAOS_CHECK_GOTO(taosCheckCfgStrValueLen(pItem->name, pItem->str, PATH_MAX), &lino, _out);
        uInfo("%s set from %s to %s", name, tsTempDir, pItem->str);
        tstrncpy(tsTempDir, pItem->str, PATH_MAX);
        TAOS_CHECK_GOTO(taosExpandDir(tsTempDir, tsTempDir, PATH_MAX), &lino, _out);
        if (taosMulMkDir(tsTempDir) != 0) {
          code = TAOS_SYSTEM_ERROR(ERRNO);
          uError("failed to create tempDir:%s since %s", tsTempDir, tstrerror(code));
          goto _out;
        }
        matched = true;
      }
      break;
    }
    default:
      code = TSDB_CODE_CFG_NOT_FOUND;
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
                                         {"compressMsgSize", &tsCompressMsgSize},
                                         {"countAlwaysReturnValue", &tsCountAlwaysReturnValue},
                                         {"crashReporting", &tsEnableCrashReport},
                                         {"enableQueryHb", &tsEnableQueryHb},
                                         {"keepColumnName", &tsKeepColumnName},
                                         {"logKeepDays", &tsLogKeepDays},
                                         {"maxInsertBatchRows", &tsMaxInsertBatchRows},
                                         {"minSlidingTime", &tsMinSlidingTime},
                                         {"minIntervalTime", &tsMinIntervalTime},
                                         {"numOfLogLines", &tsNumOfLogLines},
                                         {"querySmaOptimize", &tsQuerySmaOptimize},
                                         {"queryPolicy", &tsQueryPolicy},
                                         {"queryTableNotExistAsEmpty", &tsQueryTbNotExistAsEmpty},
                                         {"queryPlannerTrace", &tsQueryPlannerTrace},
                                         {"queryNodeChunkSize", &tsQueryNodeChunkSize},
                                         {"queryUseNodeAllocator", &tsQueryUseNodeAllocator},
                                         {"smlDot2Underline", &tsSmlDot2Underline},
                                         {"useAdapter", &tsUseAdapter},
                                         {"multiResultFunctionStarReturnTags", &tsMultiResultFunctionStarReturnTags},
                                         {"maxTsmaCalcDelay", &tsMaxTsmaCalcDelay},
                                         {"tsmaDataDeleteMark", &tsmaDataDeleteMark},
                                         {"numOfRpcSessions", &tsNumOfRpcSessions},
                                         {"bypassFlag", &tsBypassFlag},
                                         {"safetyCheckLevel", &tsSafetyCheckLevel},
                                         {"compareAsStrInGreatest", &tsCompareAsStrInGreatest},
                                         {"showFullCreateTableColumn", &tsShowFullCreateTableColumn}};

    if ((code = taosCfgSetOption(debugOptions, tListLen(debugOptions), pItem, true)) != TSDB_CODE_SUCCESS) {
      code = taosCfgSetOption(options, tListLen(options), pItem, false);
    }
  }

_out:
  if (TSDB_CODE_SUCCESS != code) {
    uError("failed to set option:%s, lino:%d, since:%s", name, lino, tstrerror(code));
  }

  cfgUnLock(pCfg);
  TAOS_RETURN(code);
}

int32_t taosCfgDynamicOptions(SConfig *pCfg, const char *name, bool forServer) {
  if (forServer) {
    return taosCfgDynamicOptionsForServer(pCfg, name);
  } else {
    return taosCfgDynamicOptionsForClient(pCfg, name);
  }
}

int32_t taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal) {
  SConfigItem *pItem = NULL;
  TAOS_CHECK_GET_CFG_ITEM(tsCfg, pItem, flagName);
  pItem->i32 = flagVal;
  if (pFlagPtr != NULL) {
    *pFlagPtr = flagVal;
  }
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int taosLogVarComp(void const *lp, void const *rp) {
  SLogVar *lpVar = (SLogVar *)lp;
  SLogVar *rpVar = (SLogVar *)rp;
  return taosStrcasecmp(lpVar->name, rpVar->name);
}

static void taosCheckAndSetDebugFlag(int32_t *pFlagPtr, char *name, int32_t flag, SArray *noNeedToSetVars) {
  if (noNeedToSetVars != NULL && taosArraySearch(noNeedToSetVars, name, taosLogVarComp, TD_EQ) != NULL) {
    return;
  }
  int32_t code = 0;
  if ((code = taosSetDebugFlag(pFlagPtr, name, flag)) != 0) {
    if (code != TSDB_CODE_CFG_NOT_FOUND) {
      uError("failed to set flag %s to %d, since:%s", name, flag, tstrerror(code));
    } else {
      uTrace("failed to set flag %s to %d, since:%s", name, flag, tstrerror(code));
    }
  }
  return;
}

int32_t taosSetGlobalDebugFlag(int32_t flag) { return taosSetAllDebugFlag(tsCfg, flag); }

// NOTE: set all command does not change the tmrDebugFlag
static int32_t taosSetAllDebugFlag(SConfig *pCfg, int32_t flag) {
  if (flag < 0) TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  if (flag == 0) TAOS_RETURN(TSDB_CODE_SUCCESS);  // just ignore

  SArray      *noNeedToSetVars = NULL;
  SConfigItem *pItem = NULL;
  TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "debugFlag");

  pItem->i32 = flag;
  noNeedToSetVars = pItem->array;

  taosCheckAndSetDebugFlag(&simDebugFlag, "simDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&uDebugFlag, "uDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&rpcDebugFlag, "rpcDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&qDebugFlag, "qDebugFlag", flag, noNeedToSetVars);

  taosCheckAndSetDebugFlag(&jniDebugFlag, "jniDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&cDebugFlag, "cDebugFlag", flag, noNeedToSetVars);

  taosCheckAndSetDebugFlag(&dDebugFlag, "dDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&vDebugFlag, "vDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&mDebugFlag, "mDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&wDebugFlag, "wDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&azDebugFlag, "azDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&tssDebugFlag, "tssDebugFlag", flag, noNeedToSetVars);
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
  taosCheckAndSetDebugFlag(&bseDebugFlag, "bseDebugFlag", flag, noNeedToSetVars);
  taosCheckAndSetDebugFlag(&bndDebugFlag, "bndDebugFlag", flag, noNeedToSetVars);

  taosArrayClear(noNeedToSetVars);  // reset array

  uInfo("all debug flag are set to %d", flag);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t taosGranted(int8_t type) {
  switch (type) {
    case TSDB_GRANT_ALL: {
      if (atomic_load_32(&tsGrant) & GRANT_FLAG_ALL) {
        return 0;
      }
      int32_t grantVal = atomic_load_32(&tsGrant);
      if (grantVal & GRANT_FLAG_EX_MULTI_TIER) {
        return TSDB_CODE_GRANT_MULTI_STORAGE_EXPIRED;
      } else if (grantVal & GRANT_FLAG_EX_STORAGE) {
        return TSDB_CODE_GRANT_STORAGE_LIMITED;
      }
      return TSDB_CODE_GRANT_EXPIRED;
    }
    case TSDB_GRANT_AUDIT: {
      return (atomic_load_32(&tsGrant) & GRANT_FLAG_AUDIT) ? 0 : TSDB_CODE_GRANT_AUDIT_EXPIRED;
    }
    case TSDB_GRANT_VIEW:
      return (atomic_load_32(&tsGrant) & GRANT_FLAG_VIEW) ? 0 : TSDB_CODE_GRANT_VIEW_EXPIRED;
    default:
      uWarn("undefined grant type:%" PRIi8, type);
      break;
  }
  return 0;
}

int32_t globalConfigSerialize(int32_t version, SArray *array, char **serialized) {
  char   buf[30];
  cJSON *json = cJSON_CreateObject();
  if (json == NULL) goto _exit;
  if (cJSON_AddNumberToObject(json, "file_version", GLOBAL_CONFIG_FILE_VERSION) == NULL) goto _exit;
  if (cJSON_AddNumberToObject(json, "version", version) == NULL) goto _exit;
  int sz = taosArrayGetSize(array);

  cJSON *cField = cJSON_CreateObject();
  if (cField == NULL) goto _exit;

  if (!cJSON_AddItemToObject(json, "configs", cField)) goto _exit;

  // cjson only support int32_t or double
  // string are used to prohibit the loss of precision
  for (int i = 0; i < sz; i++) {
    SConfigItem *item = (SConfigItem *)taosArrayGet(array, i);
    switch (item->dtype) {
      {
        case CFG_DTYPE_NONE:
          break;
        case CFG_DTYPE_BOOL:
          if (cJSON_AddBoolToObject(cField, item->name, item->bval) == NULL) goto _exit;
          break;
        case CFG_DTYPE_INT32:
          if (cJSON_AddNumberToObject(cField, item->name, item->i32) == NULL) goto _exit;
          break;
        case CFG_DTYPE_INT64:
          (void)sprintf(buf, "%" PRId64, item->i64);
          if (cJSON_AddStringToObject(cField, item->name, buf) == NULL) goto _exit;
          break;
        case CFG_DTYPE_FLOAT:
        case CFG_DTYPE_DOUBLE:
          (void)sprintf(buf, "%f", item->fval);
          if (cJSON_AddStringToObject(cField, item->name, buf) == NULL) goto _exit;
          break;
        case CFG_DTYPE_STRING:
        case CFG_DTYPE_DIR:
        case CFG_DTYPE_LOCALE:
        case CFG_DTYPE_CHARSET:
        case CFG_DTYPE_TIMEZONE:
          if (cJSON_AddStringToObject(cField, item->name, item->str) == NULL) goto _exit;
          break;
      }
    }
  }
  char *pSerialized = tjsonToString(json);
_exit:
  if (terrno != TSDB_CODE_SUCCESS) {
    uError("failed to serialize global config since %s", tstrerror(terrno));
  }
  cJSON_Delete(json);
  *serialized = pSerialized;
  return terrno;
}

int32_t localConfigSerialize(SArray *array, char **serialized) {
  char   buf[30];
  cJSON *json = cJSON_CreateObject();
  if (json == NULL) goto _exit;

  int sz = taosArrayGetSize(array);

  cJSON *cField = cJSON_CreateObject();
  if (cField == NULL) goto _exit;
  if (cJSON_AddNumberToObject(json, "file_version", LOCAL_CONFIG_FILE_VERSION) == NULL) goto _exit;
  if (!cJSON_AddItemToObject(json, "configs", cField)) goto _exit;

  // cjson only support int32_t or double
  // string are used to prohibit the loss of precision
  for (int i = 0; i < sz; i++) {
    SConfigItem *item = (SConfigItem *)taosArrayGet(array, i);
    if (strcasecmp(item->name, "dataDir") == 0) {
      int32_t sz = taosArrayGetSize(item->array);
      cJSON  *dataDirs = cJSON_CreateArray();
      if (!cJSON_AddItemToObject(cField, item->name, dataDirs)) {
        uError("failed to serialize global config since %s", tstrerror(terrno));
        goto _exit;
      }
      for (int j = 0; j < sz; j++) {
        SDiskCfg *disk = (SDiskCfg *)taosArrayGet(item->array, j);
        cJSON    *dataDir = cJSON_CreateObject();
        if (dataDir == NULL) goto _exit;
        if (!cJSON_AddItemToArray(dataDirs, dataDir)) {
          uError("failed to serialize global config since %s", tstrerror(terrno));
          goto _exit;
        }
        if (cJSON_AddStringToObject(dataDir, "dir", disk->dir) == NULL) goto _exit;
        if (cJSON_AddNumberToObject(dataDir, "level", disk->level) == NULL) goto _exit;
        if (disk->diskId == 0) {
          if (taosGetFileDiskID(disk->dir, &disk->diskId) != 0) {
            uError("failed to get disk id for %s", disk->dir);
            goto _exit;
          }
        }
        (void)sprintf(buf, "%" PRId64, disk->diskId);
        if (cJSON_AddStringToObject(dataDir, "disk_id", buf) == NULL) goto _exit;
        if (cJSON_AddNumberToObject(dataDir, "primary", disk->primary) == NULL) goto _exit;
        if (cJSON_AddNumberToObject(dataDir, "disable", disk->disable) == NULL) goto _exit;
      }
      continue;
    }
    if (strcasecmp(item->name, "forceReadConfig") == 0) {
      continue;
    }
    switch (item->dtype) {
      {
        case CFG_DTYPE_NONE:
          break;
        case CFG_DTYPE_BOOL:
          if (cJSON_AddBoolToObject(cField, item->name, item->bval) == NULL) goto _exit;
          break;
        case CFG_DTYPE_INT32:
          if (cJSON_AddNumberToObject(cField, item->name, item->i32) == NULL) goto _exit;
          break;
        case CFG_DTYPE_INT64:
          (void)sprintf(buf, "%" PRId64, item->i64);
          if (cJSON_AddStringToObject(cField, item->name, buf) == NULL) goto _exit;
          break;
        case CFG_DTYPE_FLOAT:
        case CFG_DTYPE_DOUBLE:
          (void)sprintf(buf, "%f", item->fval);
          if (cJSON_AddStringToObject(cField, item->name, buf) == NULL) goto _exit;
          break;
        case CFG_DTYPE_STRING:
        case CFG_DTYPE_DIR:
        case CFG_DTYPE_LOCALE:
        case CFG_DTYPE_CHARSET:
        case CFG_DTYPE_TIMEZONE:
          if (cJSON_AddStringToObject(cField, item->name, item->str) == NULL) goto _exit;
          break;
      }
    }
  }
  char *pSerialized = tjsonToString(json);
_exit:
  if (terrno != TSDB_CODE_SUCCESS) {
    uError("failed to serialize local config since %s", tstrerror(terrno));
  }
  cJSON_Delete(json);
  *serialized = pSerialized;
  return terrno;
}

int32_t stypeConfigSerialize(SArray *array, char **serialized) {
  char   buf[30];
  cJSON *json = cJSON_CreateObject();
  if (json == NULL) goto _exit;

  int sz = taosArrayGetSize(array);

  cJSON *cField = cJSON_CreateObject();
  if (cField == NULL) goto _exit;
  if (cJSON_AddNumberToObject(json, "file_version", LOCAL_CONFIG_FILE_VERSION) == NULL) goto _exit;
  if (!cJSON_AddItemToObject(json, "config_stypes", cField)) goto _exit;

  // cjson only support int32_t or double
  // string are used to prohibit the loss of precision
  for (int i = 0; i < sz; i++) {
    SConfigItem *item = (SConfigItem *)taosArrayGet(array, i);
    if (cJSON_AddNumberToObject(cField, item->name, item->stype) == NULL) goto _exit;
  }
  char *pSerialized = tjsonToString(json);
_exit:
  if (terrno != TSDB_CODE_SUCCESS) {
    uError("failed to serialize local config since %s", tstrerror(terrno));
  }
  cJSON_Delete(json);
  *serialized = pSerialized;
  return terrno;
}

int32_t taosPersistGlobalConfig(SArray *array, const char *path, int32_t version) {
  int32_t   code = 0;
  int32_t   lino = 0;
  char     *buffer = NULL;
  TdFilePtr pFile = NULL;
  char     *serialized = NULL;
  char      filepath[CONFIG_FILE_LEN] = {0};
  char      filename[CONFIG_FILE_LEN] = {0};
  snprintf(filepath, sizeof(filepath), "%s%sconfig", path, TD_DIRSEP);
  snprintf(filename, sizeof(filename), "%s%sconfig%sglobal.json", path, TD_DIRSEP, TD_DIRSEP);

  TAOS_CHECK_GOTO(taosMkDir(filepath), &lino, _exit);

  pFile = taosOpenFile(filename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);

  if (pFile == NULL) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    uError("failed to open file:%s since %s", filename, tstrerror(code));
    TAOS_RETURN(code);
  }
  TAOS_CHECK_GOTO(globalConfigSerialize(version, array, &serialized), &lino, _exit);

  if (taosWriteFile(pFile, serialized, strlen(serialized)) < 0) {
    lino = __LINE__;
    code = TAOS_SYSTEM_ERROR(ERRNO);
    uError("failed to write file:%s since %s", filename, tstrerror(code));
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to persist global config at line:%d, since %s", lino, tstrerror(code));
  }
  (void)taosCloseFile(&pFile);
  taosMemoryFree(serialized);
  return code;
}

int32_t taosPersistLocalConfig(const char *path) {
  int32_t   code = 0;
  int32_t   lino = 0;
  char     *buffer = NULL;
  TdFilePtr pFile = NULL;
  char     *serializedCfg = NULL;
  char     *serializedStype = NULL;
  char      filepath[CONFIG_FILE_LEN] = {0};
  char      filename[CONFIG_FILE_LEN] = {0};
  char      stypeFilename[CONFIG_FILE_LEN] = {0};
  snprintf(filepath, sizeof(filepath), "%s%sconfig", path, TD_DIRSEP);
  snprintf(filename, sizeof(filename), "%s%sconfig%slocal.json", path, TD_DIRSEP, TD_DIRSEP);
  snprintf(stypeFilename, sizeof(stypeFilename), "%s%sconfig%sstype.json", path, TD_DIRSEP, TD_DIRSEP);

  // TODO(beryl) need to check if the file is existed
  TAOS_CHECK_GOTO(taosMkDir(filepath), &lino, _exit);

  TdFilePtr pConfigFile =
      taosOpenFile(filename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);

  if (pConfigFile == NULL) {
    code = TAOS_SYSTEM_ERROR(terrno);
    uError("failed to open file:%s since %s", filename, tstrerror(code));
    TAOS_RETURN(code);
  }

  TdFilePtr pStypeFile =
      taosOpenFile(stypeFilename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pStypeFile == NULL) {
    code = TAOS_SYSTEM_ERROR(terrno);
    uError("failed to open file:%s since %s", stypeFilename, tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(localConfigSerialize(taosGetLocalCfg(tsCfg), &serializedCfg), &lino, _exit);
  if (taosWriteFile(pConfigFile, serializedCfg, strlen(serializedCfg)) < 0) {
    lino = __LINE__;
    code = TAOS_SYSTEM_ERROR(terrno);
    uError("failed to write file:%s since %s", filename, tstrerror(code));
    goto _exit;
  }

  TAOS_CHECK_GOTO(stypeConfigSerialize(taosGetLocalCfg(tsCfg), &serializedStype), &lino, _exit);
  if (taosWriteFile(pStypeFile, serializedStype, strlen(serializedStype)) < 0) {
    lino = __LINE__;
    code = TAOS_SYSTEM_ERROR(terrno);
    uError("failed to write file:%s since %s", stypeFilename, tstrerror(code));
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to persist local config at line:%d, since %s", lino, tstrerror(code));
  }
  (void)taosCloseFile(&pConfigFile);
  (void)taosCloseFile(&pStypeFile);
  taosMemoryFree(serializedCfg);
  taosMemoryFree(serializedStype);
  return code;
}

int32_t tSerializeSConfigArray(SEncoder *pEncoder, SArray *array) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t sz = taosArrayGetSize(array);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, sz));
  for (int i = 0; i < sz; i++) {
    SConfigItem *item = (SConfigItem *)taosArrayGet(array, i);
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, item->name));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, item->dtype));
    switch (item->dtype) {
      {
        case CFG_DTYPE_NONE:
          break;
        case CFG_DTYPE_BOOL:
          TAOS_CHECK_EXIT(tEncodeBool(pEncoder, item->bval));
          break;
        case CFG_DTYPE_INT32:
          TAOS_CHECK_EXIT(tEncodeI32(pEncoder, item->i32));
          break;
        case CFG_DTYPE_INT64:
          TAOS_CHECK_EXIT(tEncodeI64(pEncoder, item->i64));
          break;
        case CFG_DTYPE_FLOAT:
        case CFG_DTYPE_DOUBLE:
          TAOS_CHECK_EXIT(tEncodeFloat(pEncoder, item->fval));
          break;
        case CFG_DTYPE_STRING:
        case CFG_DTYPE_DIR:
        case CFG_DTYPE_LOCALE:
        case CFG_DTYPE_CHARSET:
        case CFG_DTYPE_TIMEZONE:
          TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, item->str));
          break;
      }
    }
  }
_exit:
  return code;
}

int32_t tDeserializeSConfigArray(SDecoder *pDecoder, SArray *array) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t sz = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &sz));
  for (int i = 0; i < sz; i++) {
    SConfigItem item = {0};
    TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &item.name));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, (int32_t *)&item.dtype));
    switch (item.dtype) {
      {
        case CFG_DTYPE_NONE:
          break;
        case CFG_DTYPE_BOOL:
          TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &item.bval));
          break;
        case CFG_DTYPE_INT32:
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &item.i32));
          break;
        case CFG_DTYPE_INT64:
          TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &item.i64));
          break;
        case CFG_DTYPE_FLOAT:
        case CFG_DTYPE_DOUBLE:
          TAOS_CHECK_EXIT(tDecodeFloat(pDecoder, &item.fval));
          break;
        case CFG_DTYPE_STRING:
        case CFG_DTYPE_DIR:
        case CFG_DTYPE_LOCALE:
        case CFG_DTYPE_CHARSET:
        case CFG_DTYPE_TIMEZONE:
          TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &item.str));
          break;
      }
    }
    if (taosArrayPush(array, &item) == NULL) {
      code = terrno;
      goto _exit;
    }
  }
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to deserialize SConfigItem at line:%d, since %s", lino, tstrerror(code));
  }
  return code;
}

bool isConifgItemLazyMode(SConfigItem *item) {
  if (item->dynScope == CFG_DYN_CLIENT_LAZY || item->dynScope == CFG_DYN_SERVER_LAZY ||
      item->dynScope == CFG_DYN_BOTH_LAZY) {
    return true;
  }
  return false;
}

int32_t taosCheckCfgStrValueLen(const char *name, const char *value, int32_t len) {
  if (strlen(value) > len) {
    uError("invalid config:%s, value:%s, length should be less than %d", name, value, len);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG_VALUE);
  }
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}
