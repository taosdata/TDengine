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

#ifndef TDENGINE_COMMON_GLOBAL_H
#define TDENGINE_COMMON_GLOBAL_H

#ifdef __cplusplus
extern "C" {
#endif

extern char configDir[];
extern char tsVnodeDir[];
extern char tsDnodeDir[];
extern char tsMnodeDir[];
extern char dataDir[];
extern char logDir[];
extern char scriptDir[];
extern char osName[];

// system info
extern int64_t tsPageSize;
extern int64_t tsOpenMax;
extern int64_t tsStreamMax;
extern int32_t tsNumOfCores;
extern int32_t tsAlternativeRole;
extern float   tsTotalLogDirGB;
extern float   tsTotalTmpDirGB;
extern float   tsTotalDataDirGB;
extern float   tsAvailLogDirGB;
extern float   tsAvailTmpDirGB;
extern float   tsAvailDataDirGB;
extern float   tsMinimalLogDirGB;
extern float   tsMinimalTmpDirGB;
extern float   tsMinimalDataDirGB;
extern int32_t tsEnableCoreFile;
extern int32_t tsTotalMemoryMB;
extern int32_t tsVersion;

extern int tscEmbedded;

extern int64_t tsMsPerDay[2];


extern char  tsMasterIp[];
extern char  tsSecondIp[];
extern uint16_t tsMnodeDnodePort;
extern uint16_t tsMnodeShellPort;
extern uint16_t tsDnodeShellPort;
extern uint16_t tsDnodeMnodePort;
extern uint16_t tsSyncPort;

extern int tsStatusInterval;
extern int tsShellActivityTimer;
extern int tsVnodePeerHBTimer;
extern int tsMgmtPeerHBTimer;
extern int tsMeterMetaKeepTimer;
extern int tsMetricMetaKeepTimer;

extern float tsNumOfThreadsPerCore;
extern float tsRatioOfQueryThreads;
extern char  tsPublicIp[];
extern char  tsPrivateIp[];
extern short tsNumOfVnodesPerCore;
extern short tsNumOfTotalVnodes;
extern short tsCheckHeaderFile;
extern uint32_t tsPublicIpInt;
extern short tsAffectedRowsMod;

extern int tsSessionsPerVnode;
extern int tsAverageCacheBlocks;
extern int tsCacheBlockSize;

extern int   tsRowsInFileBlock;
extern float tsFileBlockMinPercent;

extern short tsNumOfBlocksPerMeter;
extern short tsCommitTime;  // seconds
extern short tsCommitLog;
extern short tsAsyncLog;
extern short tsCompression;
extern short tsDaysPerFile;
extern int   tsDaysToKeep;
extern int   tsReplications;

extern int  tsNumOfMPeers;
extern int  tsMaxShellConns;
extern int  tsMaxTables;

extern char tsLocalIp[];
extern char tsDefaultDB[];
extern char tsDefaultUser[];
extern char tsDefaultPass[];
extern int  tsMaxMeterConnections;
extern int  tsMaxVnodeConnections;
extern int  tsMaxMgmtConnections;

extern int tsBalanceMonitorInterval;
extern int tsBalanceStartInterval;
extern int tsBalancePolicy;
extern int tsOfflineThreshold;
extern int tsMgmtEqualVnodeNum;

extern int tsEnableHttpModule;
extern int tsEnableMonitorModule;
extern int tsRestRowLimit;
extern int tsCompressMsgSize;
extern int tsMaxSQLStringLen;
extern int tsMaxNumOfOrderedResults;

extern char tsSocketType[4];

extern int tsTimePrecision;
extern int tsMinSlidingTime;
extern int tsMinIntervalTime;
extern int tsMaxStreamComputDelay;
extern int tsStreamCompStartDelay;
extern int tsStreamCompRetryDelay;
extern float tsStreamComputDelayRatio;   // the delayed computing ration of the whole time window

extern int     tsProjectExecInterval;
extern int64_t tsMaxRetentWindow;

extern char  tsHttpIp[];
extern uint16_t tsHttpPort;
extern int   tsHttpCacheSessions;
extern int   tsHttpSessionExpire;
extern int   tsHttpMaxThreads;
extern int   tsHttpEnableCompress;
extern int   tsHttpEnableRecordSql;
extern int   tsTelegrafUseFieldNum;

extern int   tsTscEnableRecordSql;
extern int   tsAnyIp;

extern char tsMonitorDbName[];
extern char tsInternalPass[];
extern int  tsMonitorInterval;

extern int tsNumOfLogLines;
extern int32_t ddebugFlag;
extern int32_t mdebugFlag;
extern int32_t cdebugFlag;
extern int32_t jnidebugFlag;
extern int32_t tmrDebugFlag;
extern int32_t sdbDebugFlag;
extern int32_t httpDebugFlag;
extern int32_t monitorDebugFlag;
extern int32_t uDebugFlag;
extern int32_t rpcDebugFlag;
extern int32_t debugFlag;
extern int32_t odbcdebugFlag;
extern int32_t qdebugFlag;

extern uint32_t taosMaxTmrCtrl;

extern int  tsRpcTimer;
extern int  tsRpcMaxTime;
extern int  tsUdpDelay;
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char gitinfoOfInternal[];
extern char buildinfo[];

extern char tsTimezone[64];
extern char tsLocale[64];
extern char tsCharset[64];  // default encode string

#define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

void taosInitGlobalCfg();
bool taosCheckGlobalCfg();
void taosSetAllDebugFlag();
bool taosCfgDynamicOptions(char *msg);

#ifdef __cplusplus
}
#endif

#endif
