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

// common
extern int32_t tsMaxConnections;
extern int32_t tsMaxShellConns;
extern int32_t tsShellActivityTimer;
extern int32_t tsCompressMsgSize;
extern int32_t tsCompressColData;
extern int32_t tsMaxNumOfDistinctResults;
extern int32_t tsCompatibleModel;
extern bool    tsEnableSlaveQuery;
extern bool    tsPrintAuth;
extern int64_t tsTickPerDay[3];

// multi-process
extern bool    tsMultiProcess;
extern int32_t tsMnodeShmSize;
extern int32_t tsVnodeShmSize;
extern int32_t tsQnodeShmSize;
extern int32_t tsSnodeShmSize;
extern int32_t tsBnodeShmSize;

// queue & threads
extern int32_t tsNumOfRpcThreads;
extern int32_t tsNumOfCommitThreads;
extern int32_t tsNumOfTaskQueueThreads;
extern int32_t tsNumOfMnodeQueryThreads;
extern int32_t tsNumOfMnodeReadThreads;
extern int32_t tsNumOfVnodeQueryThreads;
extern int32_t tsNumOfVnodeFetchThreads;
extern int32_t tsNumOfVnodeWriteThreads;
extern int32_t tsNumOfVnodeSyncThreads;
extern int32_t tsNumOfVnodeMergeThreads;
extern int32_t tsNumOfQnodeQueryThreads;
extern int32_t tsNumOfQnodeFetchThreads;
extern int32_t tsNumOfSnodeSharedThreads;
extern int32_t tsNumOfSnodeUniqueThreads;

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
extern bool    tsRetrieveBlockingModel;  // retrieve threads will be blocked
extern bool    tsKeepOriginalColumnName;
extern bool    tsDeadLockKillQuery;

// query client
extern int32_t tsQueryPolicy;

// client
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxStreamComputDelay;
extern int32_t tsStreamCompStartDelay;
extern int32_t tsRetryStreamCompDelay;
extern float   tsStreamComputDelayRatio;  // the delayed computing ration of the whole time window
extern int32_t tsProjectExecInterval;
extern int64_t tsMaxRetentWindow;

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
extern bool  tsStartUdfd;

// internal
extern int32_t tsTransPullupInterval;
extern int32_t tsMqRebalanceInterval;

#define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

int32_t taosCreateLog(const char *logname, int32_t logFileNum, const char *cfgDir, const char **envCmd,
                      const char *envFile, char *apolloUrl, SArray *pArgs, bool tsc);
int32_t taosInitCfg(const char *cfgDir, const char **envCmd, const char *envFile, char *apolloUrl, SArray *pArgs,
                    bool tsc);
void    taosCleanupCfg();
void    taosCfgDynamicOptions(const char *option, const char *value);
void    taosAddDataDir(int32_t index, char *v1, int32_t level, int32_t primary);

struct SConfig *taosGetCfg();

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GLOBAL_H_*/
