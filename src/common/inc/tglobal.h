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

#include "taosdef.h"

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
extern uint16_t tsArbitratorPort;
extern int32_t  tsStatusInterval;
extern int32_t  tsNumOfMnodes;
extern int8_t   tsEnableVnodeBak;
extern int8_t   tsEnableTelemetryReporting;
extern char     tsEmail[];
extern char     tsArbitrator[];
extern int8_t   tsArbOnline;

// common
extern int      tsRpcTimer;
extern int      tsRpcMaxTime;
extern int32_t  tsMaxConnections;
extern int32_t  tsMaxShellConns;
extern int32_t  tsShellActivityTimer;
extern uint32_t tsMaxTmrCtrl;
extern float    tsNumOfThreadsPerCore;
extern int32_t  tsNumOfCommitThreads;
extern float    tsRatioOfQueryCores;
extern int8_t   tsDaylight;
extern char     tsTimezone[];
extern char     tsLocale[];
extern char     tsCharset[];            // default encode string
extern int8_t   tsEnableCoreFile;
extern int32_t  tsCompressMsgSize;
extern char     tsTempDir[];

//query buffer management
extern int32_t  tsQueryBufferSize;      // maximum allowed usage buffer for each data node during query processing
extern int32_t  tsRetrieveBlockingModel;// retrieve threads will be blocked

extern int8_t   tsKeepOriginalColumnName;

// client
extern int32_t tsMaxSQLStringLen;
extern int8_t  tsTscEnableRecordSql;
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
extern int8_t  tsCompression;
extern int8_t  tsWAL;
extern int32_t tsFsyncPeriod;
extern int32_t tsReplications;
extern int32_t tsQuorum;
extern int8_t  tsUpdate;
extern int8_t  tsCacheLastRow;

// balance
extern int8_t  tsEnableBalance;
extern int8_t  tsAlternativeRole;
extern int32_t tsBalanceInterval;
extern int32_t tsOfflineThreshold;
extern int32_t tsMnodeEqualVnodeNum;
extern int8_t  tsEnableFlowCtrl;
extern int8_t  tsEnableSlaveQuery;
extern int8_t  tsEnableAdjustMaster;

// restful
extern int8_t   tsEnableHttpModule;
extern int32_t  tsRestRowLimit;
extern uint16_t tsHttpPort;
extern int32_t  tsHttpCacheSessions;
extern int32_t  tsHttpSessionExpire;
extern int32_t  tsHttpMaxThreads;
extern int8_t   tsHttpEnableCompress;
extern int8_t   tsHttpEnableRecordSql;
extern int8_t   tsTelegrafUseFieldNum;

// mqtt
extern int8_t tsEnableMqttModule;
extern char   tsMqttHostName[];
extern char   tsMqttPort[];
extern char   tsMqttUser[];
extern char   tsMqttPass[];
extern char   tsMqttClientId[];
extern char   tsMqttTopic[];

// monitor
extern int8_t  tsEnableMonitorModule;
extern char    tsMonitorDbName[];
extern char    tsInternalPass[];
extern int32_t tsMonitorInterval;

// stream
extern int8_t tsEnableStream;

// internal
extern int8_t  tsPrintAuth;
extern int8_t  tscEmbedded;
extern char    configDir[];
extern char    tsVnodeDir[];
extern char    tsDnodeDir[];
extern char    tsMnodeDir[];
extern char    tsDataDir[];
extern char    tsLogDir[];
extern char    tsScriptDir[];
extern int64_t tsMsPerDay[3];

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
extern uint32_t tsVersion;

// build info
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char gitinfoOfInternal[];
extern char buildinfo[];

// log
extern int8_t  tsAsyncLog;
extern int32_t tsNumOfLogLines;
extern int32_t tsLogKeepDays;
extern int32_t dDebugFlag;
extern int32_t vDebugFlag;
extern int32_t mDebugFlag;
extern uint32_t cDebugFlag;
extern int32_t jniDebugFlag;
extern int32_t tmrDebugFlag;
extern int32_t sdbDebugFlag;
extern int32_t httpDebugFlag;
extern int32_t mqttDebugFlag;
extern int32_t monDebugFlag;
extern int32_t uDebugFlag;
extern int32_t rpcDebugFlag;
extern int32_t odbcDebugFlag;
extern uint32_t qDebugFlag;
extern int32_t wDebugFlag;
extern int32_t cqDebugFlag;
extern int32_t debugFlag;

typedef struct {
  char dir[TSDB_FILENAME_LEN];
  int  level;
  int  primary;
} SDiskCfg;
extern int32_t  tsDiskCfgNum;
extern SDiskCfg tsDiskCfg[];

#define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

void    taosInitGlobalCfg();
int32_t taosCheckGlobalCfg();
void    taosSetAllDebugFlag();
bool    taosCfgDynamicOptions(char *msg);
int     taosGetFqdnPortFromEp(const char *ep, char *fqdn, uint16_t *port);
bool    taosCheckBalanceCfgOptions(const char *option, int32_t *vnodeId, int32_t *dnodeId);
void    taosAddDataDir(int index, char *v1, int level, int primary);
void    taosReadDataDirCfg(char *v1, char *v2, char *v3);
void    taosPrintDataDirCfg();

#ifdef __cplusplus
}
#endif

#endif
