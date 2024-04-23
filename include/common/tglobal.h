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

#ifdef __cplusplus
extern "C" {
#endif

#define SLOW_LOG_TYPE_QUERY  0x1
#define SLOW_LOG_TYPE_INSERT 0x2
#define SLOW_LOG_TYPE_OTHERS 0x4
#define SLOW_LOG_TYPE_ALL    0xFFFFFFFF

// cluster
extern char     tsFirst[];
extern char     tsSecond[];
extern char     tsLocalFqdn[];
extern char     tsLocalEp[];
extern char     tsVersionName[];
extern uint16_t tsServerPort;
extern int32_t  tsVersion;
extern int32_t  tsStatusInterval;
extern int32_t  tsNumOfSupportVnodes;

// common
extern int32_t tsMaxShellConns;
extern int32_t tsShellActivityTimer;
extern int32_t tsCompressMsgSize;
extern int64_t tsTickPerMin[3];
extern int64_t tsTickPerHour[3];
extern int32_t tsCountAlwaysReturnValue;
extern float   tsSelectivityRatio;
extern int32_t tsTagFilterResCacheSize;

// queue & threads
extern int32_t tsNumOfRpcThreads;
extern int32_t tsNumOfRpcSessions;
extern int32_t tsTimeToGetAvailableConn;
extern int32_t tsKeepAliveIdle;
extern int32_t tsNumOfCommitThreads;
extern int32_t tsNumOfTaskQueueThreads;
extern int32_t tsNumOfMnodeQueryThreads;
extern int32_t tsNumOfMnodeFetchThreads;
extern int32_t tsNumOfMnodeReadThreads;
extern int32_t tsNumOfVnodeQueryThreads;
extern float   tsRatioOfVnodeStreamThreads;
extern int32_t tsNumOfVnodeFetchThreads;
extern int32_t tsNumOfVnodeRsmaThreads;
extern int32_t tsNumOfQnodeQueryThreads;
extern int32_t tsNumOfQnodeFetchThreads;
extern int32_t tsNumOfSnodeStreamThreads;
extern int32_t tsNumOfSnodeWriteThreads;
extern int64_t tsRpcQueueMemoryAllowed;

// sync raft
extern int32_t tsElectInterval;
extern int32_t tsHeartbeatInterval;
extern int32_t tsHeartbeatTimeout;
extern int32_t tsSnapReplMaxWaitN;

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
extern int8_t  tsGrant;
extern int32_t tsMndGrantMode;
extern bool    tsMndSkipGrant;
extern bool    tsEnableWhiteList;

// dnode
extern int64_t tsDndStart;
extern int64_t tsDndStartOsUptime;
extern int64_t tsDndUpTime;

// monitor
extern bool     tsEnableMonitor;
extern int32_t  tsMonitorInterval;
extern char     tsMonitorFqdn[];
extern uint16_t tsMonitorPort;
extern int32_t  tsMonitorMaxLogs;
extern bool     tsMonitorComp;
extern bool     tsMonitorLogProtocol;
extern int32_t  tsMonitorIntervalForBasic;
extern bool     tsMonitorForceV2;

// audit
extern bool     tsEnableAudit;
extern bool     tsEnableAuditCreateTable;
extern int32_t  tsAuditInterval;

// telem
extern bool     tsEnableTelem;
extern int32_t  tsTelemInterval;
extern char     tsTelemServer[];
extern uint16_t tsTelemPort;
extern bool     tsEnableCrashReport;
extern char *   tsTelemUri;
extern char *   tsClientCrashReportUri;
extern char *   tsSvrCrashReportUri;

// query buffer management
extern int32_t tsQueryBufferSize;  // maximum allowed usage buffer size in MB for each data node during query processing
extern int64_t tsQueryBufferSizeBytes;    // maximum allowed usage buffer size in byte for each data node
extern int32_t tsCacheLazyLoadThreshold;  // cost threshold for last/last_row loading cache as much as possible

// query client
extern int32_t tsQueryPolicy;
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
extern int32_t tsSlowLogScope;
extern int32_t tsTimeSeriesThreshold;

// client
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxInsertBatchRows;

// build info
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char buildinfo[];

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
extern char tsUdfdLdLibPath[];

// schemaless
extern char tsSmlChildTableName[];
extern char tsSmlAutoChildTableNameDelimiter[];
extern char tsSmlTagName[];
extern bool tsSmlDot2Underline;
extern char tsSmlTsDefaultName[];
// extern bool    tsSmlDataFormat;
// extern int32_t tsSmlBatchSize;

extern int32_t tmqMaxTopicNum;

// wal
extern int64_t tsWalFsyncDataSizeLimit;

// internal
extern int32_t tsTransPullupInterval;
extern int32_t tsCompactPullupInterval;
extern int32_t tsMqRebalanceInterval;
extern int32_t tsStreamCheckpointInterval;
extern float   tsSinkDataRate;
extern int32_t tsStreamNodeCheckInterval;
extern int32_t tsTtlUnit;
extern int32_t tsTtlPushIntervalSec;
extern int32_t tsTtlBatchDropNum;
extern int32_t tsTrimVDbIntervalSec;
extern int32_t tsGrantHBInterval;
extern int32_t tsUptimeInterval;

extern bool    tsDisableStream;
extern int64_t tsStreamBufferSize;
extern int     tsStreamAggCnt;
extern bool    tsFilterScalarMode;
extern int32_t tsMaxStreamBackendCache;
extern int32_t tsPQSortMemThreshold;
extern int32_t tsResolveFQDNRetryTime;
extern bool    tsDisableCount;

extern bool tsExperimental;
// #define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc);
int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc);
void    taosCleanupCfg();

int32_t taosCfgDynamicOptions(SConfig *pCfg, char *name, bool forServer);

struct SConfig *taosGetCfg();

void    taosSetAllDebugFlag(int32_t flag);
void    taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal);
void    taosLocalCfgForbiddenToChange(char *name, bool *forbidden);
int8_t  taosGranted(int8_t type);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GLOBAL_H_*/
