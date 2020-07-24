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

// cluster
extern char     tsFirst[];
extern char     tsSecond[];
extern char     tsLocalFqdn[];
extern char     tsLocalEp[];
extern uint16_t tsServerPort;
extern uint16_t tsDnodeShellPort;
extern uint16_t tsDnodeDnodePort;
extern uint16_t tsSyncPort;
extern int32_t  tsStatusInterval;
extern int32_t  tsNumOfMnodes;
extern int32_t  tsEnableVnodeBak;

// common
extern int      tsRpcTimer;
extern int      tsRpcMaxTime;
extern int32_t  tsMaxConnections;
extern int32_t  tsMaxShellConns;
extern int32_t  tsShellActivityTimer;
extern uint32_t tsMaxTmrCtrl;
extern float    tsNumOfThreadsPerCore;
extern float    tsRatioOfQueryThreads;
extern int8_t   tsDaylight;
extern char     tsTimezone[64];
extern char     tsLocale[64];
extern char     tsCharset[64];  // default encode string
extern int32_t  tsEnableCoreFile;
extern int32_t  tsCompressMsgSize;

// client
extern int32_t tsTableMetaKeepTimer;
extern int32_t tsMaxSQLStringLen;
extern int32_t tsTscEnableRecordSql;
extern int32_t tsMaxNumOfOrderedResults;
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxStreamComputDelay;
extern int32_t tsStreamCompStartDelay;
extern int32_t tsStreamCompRetryDelay;
extern float   tsStreamComputDelayRatio;  // the delayed computing ration of the whole time window
extern int32_t tsProjectExecInterval;
extern int64_t tsMaxRetentWindow;

// db parameters in client
extern int32_t tsCacheBlockSize;
extern int32_t tsBlocksPerVnode;
extern int32_t tsMinTablePerVnode;
extern int32_t tsMaxTablePerVnode;
extern int32_t tsTableIncStepPerVnode;
extern int32_t tsMaxVgroupsPerDb;
extern int16_t tsDaysPerFile;
extern int32_t tsDaysToKeep;
extern int32_t tsMinRowsInFileBlock;
extern int32_t tsMaxRowsInFileBlock;
extern int16_t tsCommitTime;  // seconds
extern int32_t tsTimePrecision;
extern int16_t tsCompression;
extern int16_t tsWAL;
extern int32_t tsFsyncPeriod;
extern int32_t tsReplications;

// balance
extern int32_t tsEnableBalance;
extern int32_t tsAlternativeRole;
extern int32_t tsBalanceInterval;
extern int32_t tsOfflineThreshold;
extern int32_t tsMnodeEqualVnodeNum;

// restful
extern int32_t  tsEnableHttpModule;
extern int32_t  tsRestRowLimit;
extern uint16_t tsHttpPort;
extern int32_t  tsHttpCacheSessions;
extern int32_t  tsHttpSessionExpire;
extern int32_t  tsHttpMaxThreads;
extern int32_t  tsHttpEnableCompress;
extern int32_t  tsHttpEnableRecordSql;
extern int32_t  tsTelegrafUseFieldNum;

// mqtt
extern int32_t tsEnableMqttModule;
extern char    tsMqttBrokerAddress[];
extern char    tsMqttBrokerClientId[];

// monitor
extern int32_t tsEnableMonitorModule;
extern char    tsMonitorDbName[];
extern char    tsInternalPass[];
extern int32_t tsMonitorInterval;

// internal
extern int32_t tscEmbedded;
extern char    configDir[];
extern char    tsVnodeDir[];
extern char    tsDnodeDir[];
extern char    tsMnodeDir[];
extern char    tsDataDir[];
extern char    tsLogDir[];
extern char    tsScriptDir[];
extern int64_t tsMsPerDay[3];
extern char    tsVnodeBakDir[];

// system info
extern char    tsOsName[];
extern int64_t tsPageSize;
extern int64_t tsOpenMax;
extern int64_t tsStreamMax;
extern int32_t tsNumOfCores;
extern float   tsTotalLogDirGB;
extern float   tsTotalTmpDirGB;
extern float   tsTotalDataDirGB;
extern float   tsAvailLogDirGB;
extern float   tsAvailTmpDirectorySpace;
extern float   tsAvailDataDirGB;
extern float   tsMinimalLogDirGB;
extern float   tsReservedTmpDirectorySpace;
extern float   tsMinimalDataDirGB;
extern int32_t tsTotalMemoryMB;
extern int32_t tsVersion;

// build info
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char gitinfoOfInternal[];
extern char buildinfo[];

// log
extern int32_t tsAsyncLog;
extern int32_t tsNumOfLogLines;
extern int32_t dDebugFlag;
extern int32_t vDebugFlag;
extern int32_t mDebugFlag;
extern int32_t cDebugFlag;
extern int32_t jniDebugFlag;
extern int32_t tmrDebugFlag;
extern int32_t sdbDebugFlag;
extern int32_t httpDebugFlag;
extern int32_t mqttDebugFlag;
extern int32_t monitorDebugFlag;
extern int32_t uDebugFlag;
extern int32_t rpcDebugFlag;
extern int32_t odbcDebugFlag;
extern int32_t qDebugFlag;
extern int32_t wDebugFlag;
extern int32_t debugFlag;

#define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

void taosInitGlobalCfg();
bool taosCheckGlobalCfg();
void taosSetAllDebugFlag();
bool taosCfgDynamicOptions(char *msg);
int  taosGetFqdnPortFromEp(const char *ep, char *fqdn, uint16_t *port);
bool taosCheckBalanceCfgOptions(const char *option, int32_t *vnodeId, int32_t *dnodeId);
 
#ifdef __cplusplus
}
#endif

#endif
