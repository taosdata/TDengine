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

// cluster
extern char     tsFirst[];
extern char     tsSecond[];
extern char     tsLocalFqdn[];
extern char     tsLocalEp[];
extern uint16_t tsServerPort;
extern int32_t  tsVersion;
extern int32_t  tsStatusInterval;
extern int32_t  tsNumOfSupportVnodes;

// common
extern int32_t tsMaxShellConns;
extern int32_t tsShellActivityTimer;
extern int32_t tsCompressMsgSize;
extern int32_t tsCompressColData;
extern int32_t tsMaxNumOfDistinctResults;
extern int32_t tsCompatibleModel;
extern bool    tsPrintAuth;
extern int64_t tsTickPerMin[3];
extern int32_t tsCountAlwaysReturnValue;
extern float   tsSelectivityRatio;
extern int32_t tsTagFilterResCacheSize;

// queue & threads
extern int32_t tsNumOfRpcThreads;
extern int32_t tsNumOfRpcSessions;
extern int32_t tsTimeToGetAvailableConn;
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

// vnode
extern int64_t tsVndCommitMaxIntervalMs;

// mnode
extern int64_t tsMndSdbWriteDelta;
extern int64_t tsMndLogRetention;

// monitor
extern bool     tsEnableMonitor;
extern int32_t  tsMonitorInterval;
extern char     tsMonitorFqdn[];
extern uint16_t tsMonitorPort;
extern int32_t  tsMonitorMaxLogs;
extern bool     tsMonitorComp;

// telem
extern bool     tsEnableTelem;
extern int32_t  tsTelemInterval;
extern char     tsTelemServer[];
extern uint16_t tsTelemPort;
extern bool     tsEnableCrashReport;
extern char    *tsTelemUri;
extern char    *tsClientCrashReportUri;
extern char    *tsSvrCrashReportUri;

// query buffer management
extern int32_t tsQueryBufferSize;  // maximum allowed usage buffer size in MB for each data node during query processing
extern int64_t tsQueryBufferSizeBytes;    // maximum allowed usage buffer size in byte for each data node
extern int32_t tsCacheLazyLoadThreshold;  // cost threshold for last/last_row loading cache as much as possible

// query client
extern int32_t tsQueryPolicy;
extern int32_t tsQueryRspPolicy;
extern int32_t tsQuerySmaOptimize;
extern int32_t tsQueryRsmaTolerance;
extern bool    tsQueryPlannerTrace;
extern int32_t tsQueryNodeChunkSize;
extern bool    tsQueryUseNodeAllocator;
extern bool    tsKeepColumnName;
extern bool    tsEnableQueryHb;
extern int32_t tsRedirectPeriod;
extern int32_t tsRedirectFactor;
extern int32_t tsRedirectMaxPeriod;
extern int32_t tsMaxRetryWaitTime;
extern bool    tsUseAdapter;

// client
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxMemUsedByInsert;

// build info
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char buildinfo[];

// lossy
extern char     tsLossyColumns[];
extern double   tsFPrecision;
extern double   tsDPrecision;
extern uint32_t tsMaxRange;
extern uint32_t tsCurRange;
extern char     tsCompressor[];

// tfs
extern int32_t  tsDiskCfgNum;
extern SDiskCfg tsDiskCfg[];

// udf
extern bool tsStartUdfd;
extern char tsUdfdResFuncs[];
extern char tsUdfdLdLibPath[];

// schemaless
extern char tsSmlChildTableName[];
extern char tsSmlTagName[];
// extern bool    tsSmlDataFormat;
// extern int32_t tsSmlBatchSize;

// wal
extern int64_t tsWalFsyncDataSizeLimit;

// internal
extern int32_t tsTransPullupInterval;
extern int32_t tsMqRebalanceInterval;
extern int32_t tsStreamCheckpointTickInterval;
extern int32_t tsTtlUnit;
extern int32_t tsTtlPushInterval;
extern int32_t tsGrantHBInterval;
extern int32_t tsUptimeInterval;

extern int32_t tsRpcRetryLimit;
extern int32_t tsRpcRetryInterval;

extern bool tsDisableStream;

// #define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc);
int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc);
void    taosCleanupCfg();
void    taosCfgDynamicOptions(const char *option, const char *value);

struct SConfig *taosGetCfg();

void    taosSetAllDebugFlag(int32_t flag, bool rewrite);
void    taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal, bool rewrite);
int32_t taosSetCfg(SConfig *pCfg, char *name);
void    taosLocalCfgForbiddenToChange(char *name, bool *forbidden);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GLOBAL_H_*/
