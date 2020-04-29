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
extern char tsDataDir[];
extern char tsLogDir[];
extern char tsScriptDir[];
extern char tsOsName[];

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

extern int32_t tscEmbedded;
extern int64_t tsMsPerDay[2];

extern char  tsMaster[];
extern char  tsSecond[];
extern char  tsLocalEp[];
extern uint16_t tsServerPort;
extern uint16_t tsMnodeDnodePort;
extern uint16_t tsMnodeShellPort;
extern uint16_t tsDnodeShellPort;
extern uint16_t tsDnodeMnodePort;
extern uint16_t tsSyncPort;

extern int32_t tsStatusInterval;
extern int32_t tsShellActivityTimer;
extern int32_t tsVnodePeerHBTimer;
extern int32_t tsMgmtPeerHBTimer;
extern int32_t tsMeterMetaKeepTimer;
extern int32_t tsMetricMetaKeepTimer;

extern float    tsNumOfThreadsPerCore;
extern float    tsRatioOfQueryThreads;
extern char     tsPublicIp[];
extern char     tsPrivateIp[];
extern int16_t  tsNumOfVnodesPerCore;
extern int16_t  tsNumOfTotalVnodes;
extern uint32_t tsPublicIpInt;

extern int32_t tsCacheBlockSize;
extern int32_t tsTotalBlocks;
extern int32_t tsTablesPerVnode;
extern int16_t tsDaysPerFile;
extern int32_t tsDaysToKeep;
extern int32_t tsMinRowsInFileBlock;
extern int32_t tsMaxRowsInFileBlock;
extern int16_t tsCommitTime;  // seconds
extern int32_t tsTimePrecision;
extern int16_t tsCompression;
extern int16_t tsCommitLog;
extern int32_t tsReplications;

extern int16_t tsAffectedRowsMod;
extern int32_t tsNumOfMPeers;
extern int32_t tsMaxShellConns;
extern int32_t tsMaxTables;

extern char tsLocalIp[];
extern char tsDefaultDB[];
extern char tsDefaultUser[];
extern char tsDefaultPass[];
extern int32_t tsMaxMeterConnections;
extern int32_t tsMaxVnodeConnections;
extern int32_t tsMaxMgmtConnections;

extern int32_t tsBalanceMonitorInterval;
extern int32_t tsBalanceStartInterval;
extern int32_t tsOfflineThreshold;
extern int32_t tsMgmtEqualVnodeNum;

extern int32_t tsEnableHttpModule;
extern int32_t tsEnableMonitorModule;

extern int32_t tsRestRowLimit;
extern int32_t tsMaxSQLStringLen;
extern int32_t tsCompressMsgSize;
extern int32_t tsMaxNumOfOrderedResults;

extern char tsSocketType[4];

extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxStreamComputDelay;
extern int32_t tsStreamCompStartDelay;
extern int32_t tsStreamCompRetryDelay;
extern float tsStreamComputDelayRatio;   // the delayed computing ration of the whole time window

extern int     tsProjectExecInterval;
extern int64_t tsMaxRetentWindow;

extern char     tsHttpIp[];
extern uint16_t tsHttpPort;
extern int32_t  tsHttpCacheSessions;
extern int32_t  tsHttpSessionExpire;
extern int32_t  tsHttpMaxThreads;
extern int32_t  tsHttpEnableCompress;
extern int32_t  tsHttpEnableRecordSql;
extern int32_t  tsTelegrafUseFieldNum;

extern int32_t  tsTscEnableRecordSql;
extern int32_t  tsAnyIp;

extern char     tsMonitorDbName[];
extern char     tsInternalPass[];
extern int32_t  tsMonitorInterval;

extern int32_t tsAsyncLog;
extern int32_t tsNumOfLogLines;
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
int  taosGetFqdnPortFromEp(char *ep, char *fqdn, uint16_t *port);
 
#ifdef __cplusplus
}
#endif

#endif
