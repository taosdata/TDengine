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

#ifndef _TD_COMMON_GLOBAL_H_
#define _TD_COMMON_GLOBAL_H_

#include "tarray.h"
#include "tconfig.h"
#include "tdef.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SLOW_LOG_TYPE_NULL   0x0
#define SLOW_LOG_TYPE_QUERY  0x1
#define SLOW_LOG_TYPE_INSERT 0x2
#define SLOW_LOG_TYPE_OTHERS 0x4
#define SLOW_LOG_TYPE_ALL    0x7

#define GLOBAL_CONFIG_FILE_VERSION 1
#define LOCAL_CONFIG_FILE_VERSION  1

#define RPC_MEMORY_USAGE_RATIO   0.1
#define QUEUE_MEMORY_USAGE_RATIO 0.6

typedef enum {
  DND_CA_SM4 = 1,
} EEncryptAlgor;

typedef enum {
  DND_CS_TSDB = 1,
  DND_CS_VNODE_WAL = 2,
  DND_CS_SDB = 4,
  DND_CS_MNODE_WAL = 8,
} EEncryptScope;

extern SConfig *tsCfg;

// cluster
extern char          tsFirst[];
extern char          tsSecond[];
extern char          tsLocalFqdn[];
extern char          tsLocalEp[];
extern char          tsVersionName[];
extern uint16_t      tsServerPort;
extern int32_t       tsVersion;
extern int32_t       tsdmConfigVersion;
extern int32_t       tsConfigInited;
extern int32_t        tsEncryptKeysStatus;
extern int32_t       tsLocalKeyVersion;
extern int32_t       tsStatusInterval;
extern int32_t       tsStatusIntervalMs;
extern int32_t       tsStatusSRTimeoutMs;
extern int32_t       tsStatusTimeoutMs;
extern int32_t       tsNumOfSupportVnodes;
extern uint16_t      tsMqttPort;
extern char          tsEncryptAlgorithm[];
extern char          tsEncryptScope[];
extern EEncryptAlgor tsiEncryptAlgorithm;
extern EEncryptScope tsiEncryptScope;
// extern char     tsAuthCode[];
extern char          tsEncryptKey[];
extern int8_t        tsEnableStrongPassword;
extern int8_t        tsAllowDefaultPassword;
extern char          tsEncryptPassAlgorithm[];
extern EEncryptAlgor tsiEncryptPassAlgorithm;

extern char tsTLSCaPath[];
extern char tsTLSSvrCertPath[];
extern char tsTLSSvrKeyPath[];
extern char tsTLSCliCertPath[];
extern char tsTLSCliKeyPath[];
extern int8_t tsEnableTLS;
extern int8_t tsEnableSasl;

// common
extern int32_t tsMaxShellConns;
extern int32_t tsShellActivityTimer;
extern int32_t tsCompressMsgSize;
extern int64_t tsTickPerMin[3];
extern int64_t tsTickPerHour[3];
extern int64_t tsSecTimes[3];
extern int32_t tsCountAlwaysReturnValue;
extern float   tsSelectivityRatio;
extern int32_t tsTagFilterResCacheSize;
extern int32_t tsBypassFlag;
extern int64_t tsMetaEntryCacheSize;
extern char    tsMetaEntryCache;

// queue & threads
extern int32_t tsQueryMinConcurrentTaskNum;
extern int32_t tsQueryMaxConcurrentTaskNum;
extern int32_t tsQueryConcurrentTaskNum;
extern int32_t tsSingleQueryMaxMemorySize;
extern int8_t  tsQueryUseMemoryPool;
extern int8_t  tsMemPoolFullFunc;
// extern int32_t tsQueryBufferPoolSize;
extern int32_t tsMinReservedMemorySize;
extern int64_t tsCurrentAvailMemorySize;
extern int8_t  tsNeedTrim;
extern int32_t tsQueryNoFetchTimeoutSec;
extern int32_t tsNumOfQueryThreads;
extern int32_t tsNumOfRpcThreads;
extern int32_t tsNumOfRpcSessions;
extern int32_t tsShareConnLimit;
extern int32_t tsReadTimeout;
extern int8_t  tsEnableIpv6;
extern int32_t tsTimeToGetAvailableConn;
extern int32_t tsNumOfCommitThreads;
extern int32_t tsNumOfTaskQueueThreads;
extern int32_t tsNumOfMnodeQueryThreads;
extern int32_t tsNumOfMnodeFetchThreads;
extern int32_t tsNumOfMnodeReadThreads;
extern int32_t tsNumOfVnodeQueryThreads;
extern int32_t tsNumOfVnodeFetchThreads;
extern int32_t tsNumOfVnodeRsmaThreads;
extern int32_t tsNumOfQnodeQueryThreads;
extern int32_t tsNumOfQnodeFetchThreads;
extern int64_t tsQueueMemoryAllowed;
extern int64_t tsQueueMemoryUsed;
extern int64_t tsApplyMemoryAllowed;
extern int64_t tsApplyMemoryUsed;
extern int32_t tsRetentionSpeedLimitMB;
extern int32_t tsNumOfMnodeStreamMgmtThreads;
extern int32_t tsNumOfStreamMgmtThreads;
extern int32_t tsNumOfVnodeStreamReaderThreads;
extern int32_t tsNumOfStreamTriggerThreads;
extern int32_t tsNumOfStreamRunnerThreads;

extern int32_t tsNumOfCompactThreads;
extern int32_t tsNumOfRetentionThreads;

// sync raft
extern int32_t tsElectInterval;
extern int32_t tsHeartbeatInterval;
extern int32_t tsVnodeElectIntervalMs;
extern int32_t tsVnodeHeartbeatIntervalMs;
extern int32_t tsMnodeElectIntervalMs;
extern int32_t tsMnodeHeartbeatIntervalMs;
extern int32_t tsHeartbeatTimeout;
extern int32_t tsSnapReplMaxWaitN;
extern int64_t tsLogBufferMemoryAllowed;  // maximum allowed log buffer size in bytes for each dnode
extern int64_t tsSyncApplyQueueSize;
extern int32_t tsRoutineReportInterval;
extern bool    tsSyncLogHeartbeat;
extern int32_t tsSyncTimeout;

// arbitrator
extern int32_t tsArbHeartBeatIntervalSec;
extern int32_t tsArbCheckSyncIntervalSec;
extern int32_t tsArbSetAssignedTimeoutSec;
extern int32_t tsArbHeartBeatIntervalMs;
extern int32_t tsArbCheckSyncIntervalMs;
extern int32_t tsArbSetAssignedTimeoutMs;

// vnode
extern int64_t tsVndCommitMaxIntervalMs;

// snode
extern int32_t tsRsyncPort;
extern char    tsCheckpointBackupDir[];

// vnode checkpoint
extern char tsSnodeAddress[];  // 127.0.0.1:873

// mnode
extern int64_t tsMndSdbWriteDelta;
extern int64_t tsMndLogRetention;
extern bool    tsMndSkipGrant;
extern bool    tsEnableWhiteList;
extern bool    tsForceKillTrans;

// dnode
extern int64_t tsDndStart;
extern int64_t tsDndStartOsUptime;
extern int64_t tsDndUpTime;

// dnode misc
extern uint32_t tsEncryptionKeyChksum;
extern int8_t   tsEncryptionKeyStat;
extern uint32_t tsGrant;

// taosk encryption keys (multi-layer encryption)

enum {
  TSDB_ENCRYPT_KEY_STAT_UNLOADED = 0,
  TSDB_ENCRYPT_KEY_STAT_LOADED,
  TSDB_ENCRYPT_KEY_STAT_NOT_EXIST,
  TSDB_ENCRYPT_KEY_STAT_DISABLED,
  TSDB_ENCRYPT_KEY_STAT_MAX
};

extern bool     tsUseTaoskEncryption;      // Flag: using taosk encrypt.bin format
extern bool     tsSkipKeyCheckMode;        // Flag: skip key check mode
extern char     tsSvrKey[129];             // SVR_KEY (server master key)
extern char     tsDbKey[129];              // DB_KEY (database master key)
extern char     tsCfgKey[129];             // CFG_KEY (config encryption key)
extern char     tsMetaKey[129];            // META_KEY (metadata encryption key)
extern char     tsDataKey[129];            // DATA_KEY (data encryption key)
extern int32_t  tsEncryptAlgorithmType;    // Algorithm type for master keys (SVR_KEY, DB_KEY)
extern int32_t  tsCfgAlgorithm;            // Algorithm type for CFG_KEY
extern int32_t  tsMetaAlgorithm;           // Algorithm type for META_KEY
extern int32_t  tsEncryptFileVersion;      // File format version for compatibility
extern int32_t  tsEncryptKeyVersion;       // Key update version (starts from 1, increments on update)
extern int64_t  tsEncryptKeyCreateTime;    // Key creation timestamp
extern int64_t  tsSvrKeyUpdateTime;        // SVR_KEY last update timestamp
extern int64_t  tsDbKeyUpdateTime;         // DB_KEY last update timestamp

// monitor
extern bool     tsEnableMonitor;
extern int32_t  tsMonitorInterval;
extern char     tsMonitorFqdn[];
extern uint16_t tsMonitorPort;
extern int32_t  tsMonitorMaxLogs;
extern bool     tsMonitorComp;
extern bool     tsMonitorLogProtocol;
extern int32_t  tsEnableMetrics;
extern int32_t  tsMetricsInterval;
extern int32_t  tsMetricsLevel;
extern bool     tsMonitorForceV2;

// audit
extern bool    tsEnableAudit;
extern bool    tsEnableAuditCreateTable;
extern bool    tsEnableAuditDelete;
extern bool    tsEnableAuditSelect;
extern bool    tsEnableAuditInsert;
extern int32_t tsAuditLevel;
extern int32_t tsAuditInterval;
extern bool    tsAuditHttps;
extern bool    tsAuditUseToken;

// telem
extern bool     tsEnableTelem;
extern int32_t  tsTelemInterval;
extern char     tsTelemServer[];
extern uint16_t tsTelemPort;
extern bool     tsEnableCrashReport;
extern char    *tsTelemUri;
extern char    *tsClientCrashReportUri;
extern char    *tsSvrCrashReportUri;
extern int32_t  tsSafetyCheckLevel;
enum {
  TSDB_SAFETY_CHECK_LEVELL_NEVER = 0,
  TSDB_SAFETY_CHECK_LEVELL_NORMAL = 1,
  TSDB_SAFETY_CHECK_LEVELL_BYROW = 2,
};

// query buffer management
extern int32_t tsQueryBufferSize;  // maximum allowed usage buffer size in MB for each data node during query processing
extern int64_t tsQueryBufferSizeBytes;    // maximum allowed usage buffer size in byte for each data node
extern int32_t tsCacheLazyLoadThreshold;  // cost threshold for last/last_row loading cache as much as possible

// query client
extern int32_t tsQueryPolicy;
extern bool    tsQueryTbNotExistAsEmpty;
extern int32_t tsQueryRspPolicy;
extern int64_t tsQueryMaxConcurrentTables;
extern int32_t tsQuerySmaOptimize;
extern int32_t tsQueryRsmaTolerance;
extern bool    tsQueryPlannerTrace;
extern int32_t tsQueryNodeChunkSize;
extern bool    tsQueryUseNodeAllocator;
extern bool    tsKeepColumnName;
extern bool    tsEnableQueryHb;
extern bool    tsEnableScience;
extern bool    tsTtlChangeOnWrite;
extern int32_t tsTtlFlushThreshold;
extern int32_t tsRedirectPeriod;
extern int32_t tsRedirectFactor;
extern int32_t tsRedirectMaxPeriod;
extern int32_t tsMaxRetryWaitTime;
extern bool    tsUseAdapter;
extern int32_t tsMetaCacheMaxSize;
extern int32_t tsSlowLogThreshold;
extern char    tsSlowLogExceptDb[];
extern int32_t tsSlowLogScope;
extern int32_t tsSlowLogMaxLen;
extern int32_t tsTimeSeriesThreshold;
extern bool    tsMultiResultFunctionStarReturnTags;

// client
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxInsertBatchRows;
extern int32_t tsMaxSQLLength;

// build info
extern char td_version[];
extern char td_compatible_version[];
extern char td_gitinfo[];
extern char td_buildinfo[];
extern char td_edition_signature_salt[];

// lossy
extern char     tsLossyColumns[];
extern float    tsFPrecision;
extern double   tsDPrecision;
extern uint32_t tsMaxRange;
extern uint32_t tsCurRange;
extern bool     tsIfAdtFse;
extern char     tsCompressor[];

// tfs
extern int32_t  tsDiskCfgNum;
extern SDiskCfg tsDiskCfg[];
extern int64_t  tsMinDiskFreeSize;

// udf
extern bool tsStartUdfd;
extern char tsUdfdResFuncs[];
extern char tsUdfdLdLibPath[512];

// schemaless
extern char tsSmlChildTableName[];
extern char tsSmlAutoChildTableNameDelimiter[];
extern char tsSmlTagName[];
extern bool tsSmlDot2Underline;
extern char tsSmlTsDefaultName[];
// extern bool    tsSmlDataFormat;
// extern int32_t tsSmlBatchSize;

extern int32_t tmqMaxTopicNum;
extern int32_t tmqRowSize;
extern int32_t tsMaxTsmaNum;
extern int32_t tsMaxTsmaCalcDelay;
extern int64_t tsmaDataDeleteMark;

// wal
extern int64_t tsWalFsyncDataSizeLimit;
extern bool    tsWalForceRepair;
extern bool    tsWalDeleteOnCorruption;

// internal
extern bool    tsDiskIDCheckEnabled;
extern int32_t tsTransPullupInterval;
extern int32_t tsCompactPullupInterval;
extern int32_t tsScanPullupInterval;
extern int32_t tsInstancePullupInterval;
extern int32_t tsMqRebalanceInterval;
extern int32_t tsTtlUnit;
extern int32_t tsTtlPushIntervalSec;
extern int32_t tsTtlBatchDropNum;
extern int32_t tsTrimVDbIntervalSec;
extern int32_t tsQueryTrimIntervalSec;
extern int32_t tsGrantHBInterval;
extern int32_t tsUptimeInterval;
extern bool    tsUpdateCacheBatch;
extern bool    tsDisableStream;
extern int32_t tsStreamBufferSize;
extern int64_t tsStreamBufferSizeBytes;
extern bool    tsStreamPerfLogEnabled;
extern bool    tsFilterScalarMode;
extern int32_t tsPQSortMemThreshold;
extern int32_t tsStreamNotifyMessageSize;
extern int32_t tsStreamNotifyFrameSize;
extern int32_t tsStreamBatchRequestWaitMs;
extern bool    tsCompareAsStrInGreatest;
extern bool    tsShowFullCreateTableColumn;  // 0: show create table, and not include column compress info
extern int32_t tsRpcRecvLogThreshold;        // in seconds, default 3

// shared storage
extern int32_t tsSsEnabled;
extern int32_t tsSsAutoMigrateIntervalSec;
extern char    tsSsAccessString[];
extern int32_t tsSsUploadDelaySec;
extern int32_t tsSsBlockSize;
extern int32_t tsSsBlockCacheSize;
extern int32_t tsSsPageCacheSize;

extern int32_t sessionPerUser; 
extern int32_t sessionConnTime; 
extern int32_t sessionConnIdleTime;
extern int32_t sessionMaxConcurrency;  
extern int32_t sessionMaxCallVnodeNum;
// insert performance
extern bool tsInsertPerfEnabled;

extern bool tsExperimental;

extern int64_t tsTimestampDeltaLimit;  // s
// #define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

// auth
extern bool    tsAuthServer;
extern bool    tsAuthReq;
extern int32_t tsAuthReqInterval;
extern int32_t tsAuthReqHBInterval;
extern char    tsAuthReqUrl[];
extern bool    tsSessionControl;

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc);
int32_t taosReadDataFolder(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl,
                           SArray *pArgs);

int32_t taosPreLoadCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                       bool tsc);
int32_t taosApplyCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                     bool tsc);
int32_t tryLoadCfgFromDataDir(SConfig *pCfg);
int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc);
void    taosCleanupCfg();

int32_t taosCfgDynamicOptions(SConfig *pCfg, const char *name, bool forServer);

struct SConfig *taosGetCfg();

int32_t taosSetGlobalDebugFlag(int32_t flag);
int32_t taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal);
void    taosLocalCfgForbiddenToChange(char *name, bool *forbidden);
int32_t taosGranted(int8_t type);
int32_t taosSetSlowLogScope(char *pScopeStr, int32_t *pScope);

int32_t taosPersistGlobalConfig(SArray *array, const char *path, int32_t version);
int32_t taosPersistLocalConfig(const char *path);
int32_t localConfigSerialize(SArray *array, char **serialized);
int32_t tSerializeSConfigArray(SEncoder *pEncoder, SArray *array);
int32_t tDeserializeSConfigArray(SDecoder *pDecoder, SArray *array);
int32_t setAllConfigs(SConfig *pCfg);

bool    isConifgItemLazyMode(SConfigItem *item);
int32_t taosUpdateTfsItemDisable(SConfig *pCfg, const char *value, void *pTfs);
void    taosSetSkipKeyCheckMode(void);
#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GLOBAL_H_*/
