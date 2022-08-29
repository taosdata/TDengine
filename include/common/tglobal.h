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

// multi-process
extern int32_t tsMultiProcess;
extern int32_t tsMnodeShmSize;
extern int32_t tsVnodeShmSize;
extern int32_t tsQnodeShmSize;
extern int32_t tsSnodeShmSize;
extern int32_t tsBnodeShmSize;
extern int32_t tsNumOfShmThreads;

// queue & threads
extern int32_t tsNumOfRpcThreads;
extern int32_t tsNumOfCommitThreads;
extern int32_t tsNumOfTaskQueueThreads;
extern int32_t tsNumOfMnodeQueryThreads;
extern int32_t tsNumOfMnodeFetchThreads;
extern int32_t tsNumOfMnodeReadThreads;
extern int32_t tsNumOfVnodeQueryThreads;
extern int32_t tsNumOfVnodeStreamThreads;
extern int32_t tsNumOfVnodeFetchThreads;
extern int32_t tsNumOfVnodeWriteThreads;
extern int32_t tsNumOfVnodeSyncThreads;
extern int32_t tsNumOfVnodeRsmaThreads;
extern int32_t tsNumOfQnodeQueryThreads;
extern int32_t tsNumOfQnodeFetchThreads;
extern int32_t tsNumOfSnodeSharedThreads;
extern int32_t tsNumOfSnodeUniqueThreads;
extern int64_t tsRpcQueueMemoryAllowed;

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

// query buffer management
extern int32_t tsQueryBufferSize;  // maximum allowed usage buffer size in MB for each data node during query processing
extern int64_t tsQueryBufferSizeBytes;   // maximum allowed usage buffer size in byte for each data node

// query client
extern int32_t tsQueryPolicy;
extern int32_t tsQuerySmaOptimize;

// client
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;

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

// schemaless
extern char tsSmlChildTableName[];
extern char tsSmlTagName[];
extern bool tsSmlDataFormat;

// internal
extern int32_t tsTransPullupInterval;
extern int32_t tsMqRebalanceInterval;
extern int32_t tsTtlUnit;
extern int32_t tsTtlPushInterval;
extern int32_t tsGrantHBInterval;
extern int32_t tsUptimeInterval;

#define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc);
int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc);
void    taosCleanupCfg();
void    taosCfgDynamicOptions(const char *option, const char *value);

struct SConfig *taosGetCfg();

void    taosSetAllDebugFlag(int32_t flag);
void    taosSetDebugFlag(int32_t *pFlagPtr, const char *flagName, int32_t flagVal);
int32_t taosSetCfg(SConfig *pCfg, char *name);
void    taosLocalCfgForbiddenToChange(char* name, bool* forbidden);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GLOBAL_H_*/
