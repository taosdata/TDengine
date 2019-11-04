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

#ifndef TDENGINE_TGLOBALCFG_H
#define TDENGINE_TGLOBALCFG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include "tsdb.h"

// system info
extern int64_t tsPageSize;
extern int64_t tsOpenMax;
extern int64_t tsStreamMax;
extern int32_t tsNumOfCores;
extern float   tsTotalLogDirGB;
extern float   tsTotalTmpDirGB;
extern float   tsTotalDataDirGB;
extern float   tsAvailLogDirGB;
extern float   tsAvailTmpDirGB;
extern float   tsAvailDataDirGB;
extern float   tsMinimalLogDirGB;
extern float   tsMinimalTmpDirGB;
extern float   tsMinimalDataDirGB;
extern int32_t tsTotalMemoryMB;
extern int32_t tsVersion;

extern int tscEmbedded;

extern int64_t tsMsPerDay[2];

extern char configDir[];
extern char tsDirectory[];
extern char dataDir[];
extern char logDir[];
extern char scriptDir[];

extern char  tsMasterIp[];
extern char  tsSecondIp[];
extern short tsMgmtVnodePort;
extern short tsMgmtShellPort;
extern short tsVnodeShellPort;
extern short tsVnodeVnodePort;
extern short tsMgmtMgmtPort;
extern short tsVnodeSyncPort;
extern short tsMgmtSyncPort;

extern int tsStatusInterval;
extern int tsShellActivityTimer;
extern int tsVnodePeerHBTimer;
extern int tsMgmtPeerHBTimer;
extern int tsMeterMetaKeepTimer;
extern int tsMetricMetaKeepTimer;

extern float tsNumOfThreadsPerCore;
extern float tsRatioOfQueryThreads;
extern char  tsInternalIp[];
extern char  tsServerIpStr[];
extern short tsNumOfVnodesPerCore;
extern short tsNumOfTotalVnodes;
extern int   tsShellsPerVnode;
extern short tsCheckHeaderFile;

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
extern int  tsMaxAccounts;
extern int  tsMaxUsers;
extern int  tsMaxDbs;
extern int  tsMaxTables;
extern int  tsMaxDnodes;
extern int  tsMaxVGroups;
extern int  tsShellActivityTimer;
extern char tsMgmtZone[];

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

extern int tsTimePrecision;
extern int tsMinSlidingTime;
extern int tsMinIntervalTime;
extern int tsMaxStreamComputDelay;
extern int tsStreamCompStartDelay;
extern int tsStreamCompRetryDelay;

extern int     tsProjectExecInterval;
extern int64_t tsMaxRetentWindow;

extern char tsSocketType[4];

extern char  tsHttpIp[];
extern short tsHttpPort;
extern int   tsHttpCacheSessions;
extern int   tsHttpSessionExpire;
extern int   tsHttpMaxThreads;
extern int   tsHttpEnableCompress;
extern int   tsTelegrafUseFieldNum;
extern int   tsAdminRowLimit;

extern char tsMonitorDbName[];
extern char tsInternalPass[];
extern int  tsMonitorInterval;

extern int tsNumOfLogLines;
extern uint32_t ddebugFlag;
extern uint32_t mdebugFlag;
extern uint32_t cdebugFlag;
extern uint32_t jnidebugFlag;
extern uint32_t tmrDebugFlag;
extern uint32_t sdbDebugFlag;
extern uint32_t httpDebugFlag;
extern uint32_t monitorDebugFlag;
extern uint32_t uDebugFlag;
extern uint32_t rpcDebugFlag;
extern uint32_t debugFlag;
extern uint32_t odbcdebugFlag;
extern uint32_t qdebugFlag;

extern int  tsRpcTimer;
extern int  tsRpcMaxTime;
extern int  tsUdpDelay;
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char buildinfo[];

extern char tsTimezone[64];
extern char tsLocale[64];
extern char tsCharset[64];  // default encode string

//
void tsReadGlobalLogConfig();
bool tsReadGlobalConfig();
int tsCfgDynamicOptions(char *msg);
void tsPrintGlobalConfig();
void tsSetAllDebugFlag();
void tsSetTimeZone();
void tsSetLocale();
void tsInitGlobalConfig();

#define TSDB_CFG_CTYPE_B_CONFIG     1U   // can be configured from file
#define TSDB_CFG_CTYPE_B_SHOW       2U   // can displayed by "show configs" commands
#define TSDB_CFG_CTYPE_B_LOG        4U   // is a log type configuration
#define TSDB_CFG_CTYPE_B_CLIENT     8U   // can be displayed in the client log
#define TSDB_CFG_CTYPE_B_OPTION     16U  // can be configured by taos_options function
#define TSDB_CFG_CTYPE_B_NOT_PRINT  32U

#define TSDB_CFG_CSTATUS_NONE 0     // not configured
#define TSDB_CFG_CSTATUS_DEFAULT 1  // use system default value
#define TSDB_CFG_CSTATUS_FILE 2     // configured from file
#define TSDB_CFG_CSTATUS_OPTION 3   // configured by taos_options function
#define TSDB_CFG_CSTATUS_ARG 4      // configured by program argument

enum {
  TSDB_CFG_VTYPE_SHORT,
  TSDB_CFG_VTYPE_INT,
  TSDB_CFG_VTYPE_UINT,
  TSDB_CFG_VTYPE_FLOAT,
  TSDB_CFG_VTYPE_STRING,
  TSDB_CFG_VTYPE_IPSTR,
  TSDB_CFG_VTYPE_DIRECTORY,
};

enum {
  TSDB_CFG_UTYPE_NONE,
  TSDB_CFG_UTYPE_PERCENT,
  TSDB_CFG_UTYPE_GB,
  TSDB_CFG_UTYPE_MB,
  TSDB_CFG_UTYPE_Mb,
  TSDB_CFG_UTYPE_BYTE,
  TSDB_CFG_UTYPE_SECOND,
  TSDB_CFG_UTYPE_MS
};

typedef struct {
  char *   option;
  void *   ptr;
  float    minValue;
  float    maxValue;
  int8_t   cfgType;
  int8_t   cfgStatus;
  int8_t   unitType;
  int8_t   valType;
  uint32_t ptrLength;
} SGlobalConfig;

extern SGlobalConfig *tsGlobalConfig;
extern int            tsGlobalConfigNum;
extern char *         tsCfgStatusStr[];
SGlobalConfig *tsGetConfigOption(char *option);

#define TSDB_CFG_MAX_NUM    110
#define TSDB_CFG_PRINT_LEN  23
#define TSDB_CFG_OPTION_LEN 24
#define TSDB_CFG_VALUE_LEN  41

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TGLOBALCFG_H
