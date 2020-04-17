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

#include "os.h"

#include "taosdef.h"
#include "taoserror.h"
#include "tglobalcfg.h"
#include "tkey.h"
#include "tlog.h"
#include "tsocket.h"
#include "tsystem.h"
#include "tutil.h"

void (*tsReadStorageConfig)() = NULL;
void (*tsPrintStorageConfig)() = NULL;

// monitor module api
int  (*startMonitor)() = NULL;
void (*stopMonitor)()  = NULL;

// system info, not configurable
int64_t tsPageSize;
int64_t tsOpenMax;
int64_t tsStreamMax;
int32_t tsNumOfCores = 1;
int32_t tsAlternativeRole = 0;
float tsTotalLogDirGB = 0;
float tsTotalTmpDirGB = 0;
float tsTotalDataDirGB = 0;
float tsAvailLogDirGB = 0;
float tsAvailTmpDirGB = 0;
float tsAvailDataDirGB = 0;
float tsMinimalLogDirGB = 0.1;
float tsMinimalTmpDirGB = 0.1;
float tsMinimalDataDirGB = 0.5;
int32_t tsTotalMemoryMB = 0;
int32_t tsVersion = 0;

// global, not configurable
int tscEmbedded = 0;

/*
 * minmum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 86400000L
 *     TSDB_TIME_PRECISION_MICRO: 86400000000L
 */
int64_t tsMsPerDay[] = {86400000L, 86400000000L};

char  tsMasterIp[TSDB_IPv4ADDR_LEN] = {0};
char  tsSecondIp[TSDB_IPv4ADDR_LEN] = {0};
uint16_t tsMnodeShellPort = 6030;   // udp[6030-6034] tcp[6030]
uint16_t tsDnodeShellPort = 6035;  // udp[6035-6039] tcp[6035]
uint16_t tsMnodeDnodePort = 6040;   // udp/tcp
uint16_t tsDnodeMnodePort = 6041;   // udp/tcp
uint16_t tsVnodeVnodePort = 6045;  // tcp[6045]
uint16_t tsMgmtMgmtPort = 6050;    // udp, numOfVnodes fixed to 1, range udp[6050]
uint16_t tsMgmtSyncPort = 6050;    // tcp, range tcp[6050]

int tsStatusInterval = 1;         // second
int tsShellActivityTimer = 3;     // second
int tsVnodePeerHBTimer = 1;       // second
int tsMgmtPeerHBTimer = 1;        // second
int tsMeterMetaKeepTimer = 7200;  // second
int tsMetricMetaKeepTimer = 600;  // second

float tsNumOfThreadsPerCore = 1.0;
float tsRatioOfQueryThreads = 0.5;
char  tsPublicIp[TSDB_IPv4ADDR_LEN] = {0};
char  tsPrivateIp[TSDB_IPv4ADDR_LEN] = {0};
short tsNumOfVnodesPerCore = 8;
short tsNumOfTotalVnodes = TSDB_INVALID_VNODE_NUM;
short tsCheckHeaderFile = 0;

#ifdef _TD_ARM_32_
int tsSessionsPerVnode = 100;
#else
int tsSessionsPerVnode = 1000;
#endif

int tsCacheBlockSize = 16384;  // 256 columns
int tsAverageCacheBlocks = TSDB_DEFAULT_AVG_BLOCKS;
/**
 * Change the meaning of affected rows:
 * 0: affected rows not include those duplicate records
 * 1: affected rows include those duplicate records
 */
short tsAffectedRowsMod = 0;

int   tsRowsInFileBlock = 4096;
float tsFileBlockMinPercent = 0.05;

short tsNumOfBlocksPerMeter = 100;
short tsCommitTime = 3600;  // seconds
short tsCommitLog = 1;
short tsCompression = TSDB_MAX_COMPRESSION_LEVEL;
short tsDaysPerFile = 10;
int   tsDaysToKeep = 3650;
int   tsReplications = TSDB_REPLICA_MIN_NUM;

int  tsNumOfMPeers = 3;
int  tsMaxShellConns = 2000;
int  tsMaxTables = 100000;

char tsLocalIp[TSDB_IPv4ADDR_LEN] = {0};
char tsDefaultDB[TSDB_DB_NAME_LEN] = {0};
char tsDefaultUser[64] = "root";
char tsDefaultPass[64] = "taosdata";
int  tsMaxMeterConnections = 10000;
int  tsMaxMgmtConnections = 2000;
int  tsMaxVnodeConnections = 10000;

int tsBalanceMonitorInterval = 2;  // seconds
int tsBalanceStartInterval = 300;  // seconds
int tsBalancePolicy = 0;           // 1-use sys.montor
int tsOfflineThreshold = 864000;   // seconds 10days
int tsMgmtEqualVnodeNum = 4;

int tsEnableHttpModule = 1;
int tsEnableMonitorModule = 0;
int tsRestRowLimit = 10240;
int tsMaxSQLStringLen = TSDB_MAX_SQL_LEN;

// the maximum number of results for projection query on super table that are returned from
// one virtual node, to order according to timestamp
int tsMaxNumOfOrderedResults = 100000;

/*
 * denote if the server needs to compress response message at the application layer to client, including query rsp,
 * metricmeta rsp, and multi-meter query rsp message body. The client compress the submit message to server.
 *
 * 0: all data are compressed
 * -1: all data are not compressed
 * other values: if the message payload size is greater than the tsCompressMsgSize, the message will be compressed.
 */
int tsCompressMsgSize = -1;

// use UDP by default[option: udp, tcp]
char tsSocketType[4] = "udp";

// time precision, millisecond by default
int tsTimePrecision = TSDB_TIME_PRECISION_MILLI;

// 10 ms for sliding time, the value will changed in case of time precision changed
int tsMinSlidingTime = 10;

// 10 ms for interval time range, changed accordingly
int tsMinIntervalTime = 10;

// 20sec, the maximum value of stream computing delay, changed accordingly
int tsMaxStreamComputDelay = 20000;

// 10sec, the first stream computing delay time after system launched successfully, changed accordingly
int tsStreamCompStartDelay = 10000;

// the stream computing delay time after executing failed, change accordingly
int tsStreamCompRetryDelay = 10;

// The delayed computing ration. 10% of the whole computing time window by default.
float tsStreamComputDelayRatio = 0.1;

int     tsProjectExecInterval = 10000;   // every 10sec, the projection will be executed once
int64_t tsMaxRetentWindow = 24 * 3600L;  // maximum time window tolerance

char  tsHttpIp[TSDB_IPv4ADDR_LEN] = "0.0.0.0";
uint16_t tsHttpPort = 6020;                 // only tcp, range tcp[6020]
// uint16_t tsNginxPort = 6060;             //only tcp, range tcp[6060]
int tsHttpCacheSessions = 100;
int tsHttpSessionExpire = 36000;
int tsHttpMaxThreads = 2;
int tsHttpEnableCompress = 0;
int tsHttpEnableRecordSql = 0;
int tsTelegrafUseFieldNum = 0;

int   tsTscEnableRecordSql = 0;
int   tsEnableCoreFile = 0;
int   tsAnyIp = 1;
uint32_t tsPublicIpInt = 0;

#ifdef _CLUSTER
int   tsIsCluster = 1;
#else
int   tsIsCluster = 0;
#endif

int tsRpcTimer = 300;
int tsRpcMaxTime = 600;      // seconds;
int tsRpcMaxUdpSize = 15000;  // bytes

char tsMonitorDbName[TSDB_DB_NAME_LEN] = "log";
int  tsMonitorInterval = 30;  // seconds
char tsInternalPass[] = "secretkey";

char tsTimezone[64] = {0};
char tsLocale[TSDB_LOCALE_LEN] = {0};
char tsCharset[TSDB_LOCALE_LEN] = {0};  // default encode string

int tsNumOfLogLines = 10000000;
uint32_t rpcDebugFlag = 135;
uint32_t ddebugFlag = 131;
uint32_t mdebugFlag = 135;
uint32_t sdbDebugFlag = 135;
uint32_t cdebugFlag = 131;
uint32_t jnidebugFlag = 131;
uint32_t httpDebugFlag = 131;
uint32_t monitorDebugFlag = 131;
uint32_t debugFlag = 131;
uint32_t odbcdebugFlag = 131;
uint32_t qdebugFlag = 131;

SGlobalConfig *tsGlobalConfig = NULL;
int            tsGlobalConfigNum = 0;

char *tsGlobalUnit[] = {
    " ", "(%)", "(GB)", "(MB)", "(Mb)", "(byte)", "(s)", "(ms)",
};

char *tsCfgStatusStr[] = {"none", "system default", "config file", "taos_options", "program argument list"};

void tsReadFloatConfig(SGlobalConfig *cfg, char *input_value) {
  float  value = (float)atof(input_value);
  float *option = (float *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    pError("config option:%s, input value:%s, out of range[%f, %f], use default value:%f",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %f", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

void tsReadIntConfig(SGlobalConfig *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  int32_t *option = (int32_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    pError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

void tsReadUIntConfig(SGlobalConfig *cfg, char *input_value) {
  uint32_t  value = (uint32_t)atoi(input_value);
  uint32_t *option = (uint32_t *)cfg->ptr;
  if (value < (uint32_t)cfg->minValue || value > (uint32_t)cfg->maxValue) {
    pError("config option:%s, input value:%s, out of range[%f, %f], use default value:%u",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %u", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

void tsReadShortConfig(SGlobalConfig *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  int16_t *option = (int16_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    pError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      *option = (int16_t)value;
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

void tsReadFilePathConfig(SGlobalConfig *cfg, char *input_value) {
  int   length = strlen(input_value);
  char *option = (char *)cfg->ptr;
  if (length <= 0 || length > cfg->ptrLength) {
    pError("config option:%s, input value:%s, length out of range[0, %d], use default value:%s",
           cfg->option, input_value, cfg->ptrLength, option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      wordexp_t full_path;
      wordexp(input_value, &full_path, 0);
      if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
        strcpy(option, full_path.we_wordv[0]);
      }
      wordfree(&full_path);

      struct stat dirstat;
      if (stat(option, &dirstat) < 0) {
        int code = mkdir(option, 0755);
        pPrint("config option:%s, input value:%s, directory not exist, create with return code:%d",
               cfg->option, input_value, code);
      }
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }
}

void tsExpandFilePath(char* option_name, char* input_value) {
  wordexp_t full_path;
  wordexp(input_value, &full_path, 0);
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strcpy(input_value, full_path.we_wordv[0]);
  }
  wordfree(&full_path);

  struct stat dirstat;
  if (stat(input_value, &dirstat) < 0) {
    int code = mkdir(input_value, 0755);
    pPrint("config option:%s, input value:%s, directory not exist, create with return code:%d", option_name, input_value, code);
  }
}

void tsReadIpConfig(SGlobalConfig *cfg, char *input_value) {
  uint32_t value = inet_addr(input_value);
  char *   option = (char *)cfg->ptr;
  if (value == INADDR_NONE) {
    pError("config option:%s, input value:%s, is not a valid ip address, use default value:%s",
           cfg->option, input_value, option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      strncpy(option, input_value, cfg->ptrLength);
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }
}

void tsReadStrConfig(SGlobalConfig *cfg, char *input_value) {
  int   length = strlen(input_value);
  char *option = (char *)cfg->ptr;
  if (length <= 0 || length > cfg->ptrLength) {
    pError("config option:%s, input value:%s, length out of range[0, %d], use default value:%s",
           cfg->option, input_value, cfg->ptrLength, option);
  } else {
    if (cfg->cfgStatus <= TSDB_CFG_CSTATUS_FILE) {
      strncpy(option, input_value, cfg->ptrLength);
      cfg->cfgStatus = TSDB_CFG_CSTATUS_FILE;
    } else {
      pWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }
}

void tsReadLogOption(char *option, char *value) {
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_CONFIG) || !(cfg->cfgType & TSDB_CFG_CTYPE_B_LOG)) continue;
    if (strcasecmp(cfg->option, option) != 0) continue;

    switch (cfg->valType) {
      case TSDB_CFG_VTYPE_INT:
        tsReadIntConfig(cfg, value);
        if (strcasecmp(cfg->option, "debugFlag") == 0) {
          tsSetAllDebugFlag();
        }
        break;
      case TSDB_CFG_VTYPE_DIRECTORY:
        tsReadFilePathConfig(cfg, value);
        break;
      default:
        break;
    }
    break;
  }
}

SGlobalConfig *tsGetConfigOption(const char *option) {
  tsInitGlobalConfig();
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (strcasecmp(cfg->option, option) != 0) continue;
    return cfg;
  }
  return NULL;
}

void tsReadConfigOption(const char *option, char *value) {
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_CONFIG)) continue;
    if (strcasecmp(cfg->option, option) != 0) continue;

    switch (cfg->valType) {
      case TSDB_CFG_VTYPE_SHORT:
        tsReadShortConfig(cfg, value);
        break;
      case TSDB_CFG_VTYPE_INT:
        tsReadIntConfig(cfg, value);
        break;
      case TSDB_CFG_VTYPE_UINT:
        tsReadUIntConfig(cfg, value);
        break;
      case TSDB_CFG_VTYPE_FLOAT:
        tsReadFloatConfig(cfg, value);
        break;
      case TSDB_CFG_VTYPE_STRING:
        tsReadStrConfig(cfg, value);
        break;
      case TSDB_CFG_VTYPE_IPSTR:
        tsReadIpConfig(cfg, value);
        break;
      case TSDB_CFG_VTYPE_DIRECTORY:
        tsReadFilePathConfig(cfg, value);
        break;
      default:
        pError("config option:%s, input value:%s, can't be recognized", option, value);
        break;
    }
    break;
  }
}

void tsInitConfigOption(SGlobalConfig *cfg, char *name, void *ptr, int8_t valType, int8_t cfgType, float minVal,
                        float maxVal, uint8_t ptrLength, int8_t unitType) {
  cfg->option = name;
  cfg->ptr = ptr;
  cfg->valType = valType;
  cfg->cfgType = cfgType;
  cfg->minValue = minVal;
  cfg->maxValue = maxVal;
  cfg->ptrLength = ptrLength;
  cfg->unitType = unitType;
  cfg->cfgStatus = TSDB_CFG_CSTATUS_NONE;
}

static void doInitGlobalConfig() {
  tsGlobalConfig = (SGlobalConfig *) malloc(sizeof(SGlobalConfig) * TSDB_CFG_MAX_NUM);
  memset(tsGlobalConfig, 0, sizeof(SGlobalConfig) * TSDB_CFG_MAX_NUM);

  SGlobalConfig *cfg = tsGlobalConfig;

  // ip address
  tsInitConfigOption(cfg++, "masterIp", tsMasterIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "secondIp", tsSecondIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_CLUSTER,
                     0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "publicIp", tsPublicIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLUSTER,
                     0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "privateIp", tsPrivateIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLUSTER,
                     0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "localIp", tsLocalIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpIp", tsHttpIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);

  // port
  tsInitConfigOption(cfg++, "httpPort", &tsHttpPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mgmtShellPort", &tsMnodeShellPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "vnodeShellPort", &tsDnodeShellPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mgmtVnodePort", &tsMnodeDnodePort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "vnodeVnodePort", &tsVnodeVnodePort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mgmtMgmtPort", &tsMgmtMgmtPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mgmtSyncPort", &tsMgmtSyncPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 65535, 0, TSDB_CFG_UTYPE_NONE);

  // directory
  tsInitConfigOption(cfg++, "configDir", configDir, TSDB_CFG_VTYPE_DIRECTORY,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "logDir", logDir, TSDB_CFG_VTYPE_DIRECTORY,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_LOG,
                     0, 0, TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "scriptDir", scriptDir, TSDB_CFG_VTYPE_DIRECTORY,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "dataDir", dataDir, TSDB_CFG_VTYPE_DIRECTORY,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 0, TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);

  // dnode configs
  tsInitConfigOption(cfg++, "numOfThreadsPerCore", &tsNumOfThreadsPerCore, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 10, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "ratioOfQueryThreads", &tsRatioOfQueryThreads, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0.1, 0.9, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "numOfVnodesPerCore", &tsNumOfVnodesPerCore, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1, 64, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "numOfTotalVnodes", &tsNumOfTotalVnodes, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, TSDB_MAX_VNODES, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "tables", &tsSessionsPerVnode, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     TSDB_MIN_TABLES_PER_VNODE, TSDB_MAX_TABLES_PER_VNODE, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "cache", &tsCacheBlockSize, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     100, 1048576, 0, TSDB_CFG_UTYPE_BYTE);
  tsInitConfigOption(cfg++, "rows", &tsRowsInFileBlock, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     200, 1048576, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "fileBlockMinPercent", &tsFileBlockMinPercent, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 1.0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "ablocks", &tsAverageCacheBlocks, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     TSDB_MIN_AVG_BLOCKS, TSDB_MAX_AVG_BLOCKS, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "tblocks", &tsNumOfBlocksPerMeter, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     32, 4096, 0, TSDB_CFG_UTYPE_NONE);
#ifdef _SYNC                    
  tsInitConfigOption(cfg++, "numOfMPeers", &tsNumOfMPeers, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 3, 0, TSDB_CFG_UTYPE_NONE);
#endif 
  tsInitConfigOption(cfg++, "balanceInterval", &tsBalanceStartInterval, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 30000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "alternativeRole", &tsAlternativeRole, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLUSTER,
                     0, 2, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "affectedRowsMod", &tsAffectedRowsMod, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 1, 0, TSDB_CFG_UTYPE_NONE);
  // 0-any, 1-mgmt, 2-dnode

  // timer
  tsInitConfigOption(cfg++, "maxTmrCtrl", &taosMaxTmrCtrl, TSDB_CFG_VTYPE_INT,
                    TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                    8, 2048, 0, TSDB_CFG_UTYPE_NONE);

  // time
  tsInitConfigOption(cfg++, "monitorInterval", &tsMonitorInterval, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     1, 600, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "offlineThreshold", &tsOfflineThreshold, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     5, 7200000, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "rpcTimer", &tsRpcTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     100, 3000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "rpcMaxTime", &tsRpcMaxTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     100, 7200, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "ctime", &tsCommitTime, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     30, 40960, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "statusInterval", &tsStatusInterval, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1, 10, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "shellActivityTimer", &tsShellActivityTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     1, 120, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "vnodePeerHBTimer", &tsVnodePeerHBTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 10, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "mgmtPeerHBTimer", &tsMgmtPeerHBTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER, 1, 10, 0,
                     TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "meterMetaKeepTimer", &tsMeterMetaKeepTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     1, 8640000, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "metricMetaKeepTimer", &tsMetricMetaKeepTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     1, 8640000, 0, TSDB_CFG_UTYPE_SECOND);

  // mgmt configs
  tsInitConfigOption(cfg++, "maxTables", &tsMaxTables, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1, 100000000, 0, TSDB_CFG_UTYPE_NONE);
  
  tsInitConfigOption(cfg++, "minSlidingTime", &tsMinSlidingTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 1000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "minIntervalTime", &tsMinIntervalTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 1000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "maxStreamCompDelay", &tsMaxStreamComputDelay, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 1000000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "maxFirstStreamCompDelay", &tsStreamCompStartDelay, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1000, 1000000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "retryStreamCompDelay", &tsStreamCompRetryDelay, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 1000000000, 0, TSDB_CFG_UTYPE_MS);
  
  
  tsInitConfigOption(cfg++, "streamCompDelayRatio", &tsStreamComputDelayRatio, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 0.1, 0.9, 0, TSDB_CFG_UTYPE_NONE);
  
  tsInitConfigOption(cfg++, "clog", &tsCommitLog, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0, 2, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "comp", &tsCompression, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0, 2, 0, TSDB_CFG_UTYPE_NONE);

  // database configs
  tsInitConfigOption(cfg++, "days", &tsDaysPerFile, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1, 365, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "keep", &tsDaysToKeep, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     1, 365000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "replica", &tsReplications, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     1, 3, 0, TSDB_CFG_UTYPE_NONE);

  // login configs
  tsInitConfigOption(cfg++, "defaultDB", tsDefaultDB, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, TSDB_DB_NAME_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "defaultUser", tsDefaultUser, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, TSDB_USER_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "defaultPass", tsDefaultPass, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_NOT_PRINT,
                     0, 0, TSDB_PASSWORD_LEN, TSDB_CFG_UTYPE_NONE);
  
  // socket type, udp by default
  tsInitConfigOption(cfg++, "sockettype", tsSocketType, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW,
                     0, 0, 3, TSDB_CFG_UTYPE_NONE);

  tsInitConfigOption(cfg++, "compressMsgSize", &tsCompressMsgSize, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW,
                     -1, 10000000, 0, TSDB_CFG_UTYPE_NONE);
  
  tsInitConfigOption(cfg++, "maxSQLLength", &tsMaxSQLStringLen, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW,
                     TSDB_MAX_SQL_LEN, TSDB_MAX_ALLOWED_SQL_LEN, 0, TSDB_CFG_UTYPE_BYTE);
  
  tsInitConfigOption(cfg++, "maxNumOfOrderedRes", &tsMaxNumOfOrderedResults, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT | TSDB_CFG_CTYPE_B_SHOW,
                     TSDB_MAX_SQL_LEN, TSDB_MAX_ALLOWED_SQL_LEN, 0, TSDB_CFG_UTYPE_NONE);
  
  // locale & charset
  tsInitConfigOption(cfg++, "timezone", tsTimezone, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, tListLen(tsTimezone), TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "locale", tsLocale, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, tListLen(tsLocale), TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "charset", tsCharset, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, tListLen(tsCharset), TSDB_CFG_UTYPE_NONE);

  // connect configs
  tsInitConfigOption(cfg++, "maxShellConns", &tsMaxShellConns, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 50000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxMeterConnections", &tsMaxMeterConnections, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 50000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxMgmtConnections", &tsMaxMgmtConnections, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 50000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxVnodeConnections", &tsMaxVnodeConnections, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     10, 50000000, 0, TSDB_CFG_UTYPE_NONE);

  tsInitConfigOption(cfg++, "minimalLogDirGB", &tsMinimalLogDirGB, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0.001, 10000000, 0, TSDB_CFG_UTYPE_GB);
  tsInitConfigOption(cfg++, "minimalTmpDirGB", &tsMinimalTmpDirGB, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0.001, 10000000, 0, TSDB_CFG_UTYPE_GB);
  tsInitConfigOption(cfg++, "minimalDataDirGB", &tsMinimalDataDirGB, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0.001, 10000000, 0, TSDB_CFG_UTYPE_GB);

  // module configs
  tsInitConfigOption(cfg++, "mgmtEqualVnodeNum", &tsMgmtEqualVnodeNum, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLUSTER,
                     0, 1000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "http", &tsEnableHttpModule, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0, 1, 1, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "monitor", &tsEnableMonitorModule, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0, 1, 1, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "monitorDbName", tsMonitorDbName, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0, 0, TSDB_DB_NAME_LEN, TSDB_CFG_UTYPE_NONE);

  // http configs
  tsInitConfigOption(cfg++, "httpCacheSessions", &tsHttpCacheSessions, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     1, 100000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpEnableRecordSql", &tsHttpEnableRecordSql, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 1, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "telegrafUseFieldNum", &tsTelegrafUseFieldNum, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW,
                     0, 1, 1, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpMaxThreads", &tsHttpMaxThreads, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     1, 1000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "restfulRowLimit", &tsRestRowLimit, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     1, 10000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpEnableCompress", &tsHttpEnableCompress, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 1, 1, TSDB_CFG_UTYPE_NONE);

  // debug flag
  tsInitConfigOption(cfg++, "numOfLogLines", &tsNumOfLogLines, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     10000, 2000000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "asyncLog", &tsAsyncLog, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 1, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "debugFlag", &debugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mDebugFlag", &mdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "dDebugFlag", &ddebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "sdbDebugFlag", &sdbDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "rpcDebugFlag", &rpcDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "tmrDebugFlag", &tmrDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "cDebugFlag", &cdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "jniDebugFlag", &jnidebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "odbcDebugFlag", &odbcdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "uDebugFlag", &uDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpDebugFlag", &httpDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "monitorDebugFlag", &monitorDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG,
                     0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "qDebugFlag", &qdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);

  tsInitConfigOption(cfg++, "tscEnableRecordSql", &tsTscEnableRecordSql, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 1, 0, TSDB_CFG_UTYPE_NONE);

  tsInitConfigOption(cfg++, "enableCoreFile", &tsEnableCoreFile, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG,
                     0, 1, 0, TSDB_CFG_UTYPE_NONE);

  tsInitConfigOption(cfg++, "anyIp", &tsAnyIp, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLUSTER,
                     0, 1, 0, TSDB_CFG_UTYPE_NONE);

  // version info
  tsInitConfigOption(cfg++, "gitinfo", gitinfo, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "gitinfoOfInternal", gitinfoOfInternal, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "buildinfo", buildinfo, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "version", version, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, 0, TSDB_CFG_UTYPE_NONE);

  tsGlobalConfigNum = (int)(cfg - tsGlobalConfig);
  assert(tsGlobalConfigNum <= TSDB_CFG_MAX_NUM);
}

static pthread_once_t initGlobalConfig = PTHREAD_ONCE_INIT;
void tsInitGlobalConfig() {
  pthread_once(&initGlobalConfig, doInitGlobalConfig);
}

void tsReadGlobalLogConfig() {
  tsInitGlobalConfig();

  FILE * fp;
  char * line, *option, *value;
  int    olen, vlen;
  char   fileName[PATH_MAX] = {0};

  mdebugFlag = 135;
  sdbDebugFlag = 135;

  wordexp_t full_path;
  wordexp(configDir, &full_path, 0);
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strcpy(configDir, full_path.we_wordv[0]);
  } else {
    printf("configDir:%s not there, use default value: /etc/taos", configDir);
    strcpy(configDir, "/etc/taos");
  }
  wordfree(&full_path);

  tsReadLogOption("logDir", logDir);
  
  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    printf("\nconfig file:%s not found, all variables are set to default\n", fileName);
    return;
  }
  
  size_t len = 1024;
  line = calloc(1, len);
  
  while (!feof(fp)) {
    memset(line, 0, len);
    
    option = value = NULL;
    olen = vlen = 0;

    getline(&line, &len, fp);
    line[len - 1] = 0;
    
    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    tsReadLogOption(option, value);
  }

  tfree(line);
  fclose(fp);
}

bool tsReadGlobalConfig() {
  tsInitGlobalConfig();

  char * line, *option, *value, *value1;
  int    olen, vlen, vlen1;
  char   fileName[PATH_MAX] = {0};

  sprintf(fileName, "%s/taos.cfg", configDir);
  FILE* fp = fopen(fileName, "r");
  
  size_t len = 1024;
  line = calloc(1, len);
  
  if (fp != NULL) {
    while (!feof(fp)) {
      memset(line, 0, len);

      option = value = NULL;
      olen = vlen = 0;

      getline(&line, &len, fp);
      line[len - 1] = 0;
      
      paGetToken(line, &option, &olen);
      if (olen == 0) continue;
      option[olen] = 0;

      paGetToken(option + olen + 1, &value, &vlen);
      if (vlen == 0) continue;
      value[vlen] = 0;

      // For dataDir, the format is:
      // dataDir    /mnt/disk1    0
      paGetToken(value + vlen + 1, &value1, &vlen1);
      
      tsReadConfigOption(option, value);
    }

    fclose(fp);
  }

  tfree(line);
  
  if (tsReadStorageConfig) {
    tsReadStorageConfig();
  }

  if (tsPrivateIp[0] == 0) {
    taosGetPrivateIp(tsPrivateIp);
  }

  if (tsPublicIp[0] == 0) {
    strcpy(tsPublicIp, tsPrivateIp);
  }
  tsPublicIpInt = inet_addr(tsPublicIp);

  if (tsLocalIp[0] == 0) {
    strcpy(tsLocalIp, tsPrivateIp);
  }

  if (tsMasterIp[0] == 0) {
    strcpy(tsMasterIp, tsPrivateIp);
  }

  if (tsSecondIp[0] == 0) {
    strcpy(tsSecondIp, tsMasterIp);
  }
  
  taosGetSystemInfo();

  tsSetLocale();

  SGlobalConfig *cfg_timezone = tsGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus == TSDB_CFG_CSTATUS_FILE) {
    tsSetTimeZone();
  }

  if (tsNumOfCores <= 0) {
    tsNumOfCores = 1;
  }

  if (tsNumOfTotalVnodes == TSDB_INVALID_VNODE_NUM) {
    tsNumOfTotalVnodes = tsNumOfCores * tsNumOfVnodesPerCore;
    tsNumOfTotalVnodes = tsNumOfTotalVnodes > TSDB_MAX_VNODES ? TSDB_MAX_VNODES : tsNumOfTotalVnodes;
    tsNumOfTotalVnodes = tsNumOfTotalVnodes < TSDB_MIN_VNODES ? TSDB_MIN_VNODES : tsNumOfTotalVnodes;     
  }

  if (strlen(tsPrivateIp) == 0) {
    pError("privateIp is null");
    return false;
  }

  if (tscEmbedded) {
    strcpy(tsLocalIp, tsPrivateIp);
  }

  // todo refactor
  tsVersion = 0;
  for (int i = 0; i < 10; i++) {
    if (version[i] >= '0' && version[i] <= '9') {
      tsVersion = tsVersion * 10 + (version[i] - '0');
    } else if (version[i] == 0) {
      break;
    }
  }
  
  tsVersion = 10 * tsVersion;

  return true;
}

int tsCfgDynamicOptions(char *msg) {
  char *option, *value;
  int   olen, vlen, code = 0;
  int   vint = 0;

  paGetToken(msg, &option, &olen);
  if (olen == 0) return TSDB_CODE_INVALID_MSG_CONTENT;

  paGetToken(option + olen + 1, &value, &vlen);
  if (vlen == 0)
    vint = 135;
  else {
    vint = atoi(value);
  }

  pPrint("change dynamic option: %s, value: %d", option, vint);

  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_LOG)) continue;
    if (cfg->valType != TSDB_CFG_VTYPE_INT) continue;
    if (strncasecmp(option, cfg->option, olen) != 0) continue;
    *((int *)cfg->ptr) = vint;

    if (strncasecmp(cfg->option, "monitor", olen) == 0) {
      if (0 == vint) {
        if(stopMonitor) (void)(*stopMonitor)();
      } else {
        if(startMonitor) (*startMonitor)();
      }
      return code;
    }

    if (strncasecmp(cfg->option, "debugFlag", olen) == 0) {
      tsSetAllDebugFlag();
    }
    
    return code;
  }

  if (strncasecmp(option, "resetlog", 8) == 0) {
    taosResetLogFile();
    tsPrintGlobalConfig();
    return code;
  }

  if (strncasecmp(option, "resetQueryCache", 15) == 0) {
    if (taosLogSqlFp) {
      pPrint("the query cache of internal client will reset");
      taosLogSqlFp("reset query cache");
    } else {
      pError("reset query cache can't be executed, for monitor not initialized");
      code = 169;
    }
  } else {
    code = 169;  // INVALID_OPTION
  }

  return code;
}

void tsPrintGlobalConfig() {
  pPrint("   taos config & system info:");
  pPrint("==================================");

  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (tscEmbedded == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_LITE) {
#ifdef _CLUSTER
      continue;
#endif
    }
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_CLUSTER) {
#ifndef _CLUSTER
      continue;
#endif
    }
    
    int optionLen = (int)strlen(cfg->option);
    int blankLen = TSDB_CFG_PRINT_LEN - optionLen;
    blankLen = blankLen < 0 ? 0 : blankLen;

    char blank[TSDB_CFG_PRINT_LEN];
    memset(blank, ' ', TSDB_CFG_PRINT_LEN);
    blank[blankLen] = 0;

    switch (cfg->valType) {
      case TSDB_CFG_VTYPE_SHORT:
        pPrint(" %s:%s%d%s", cfg->option, blank, *((int16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TSDB_CFG_VTYPE_INT:
        pPrint(" %s:%s%d%s", cfg->option, blank, *((int32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TSDB_CFG_VTYPE_UINT:
        pPrint(" %s:%s%d%s", cfg->option, blank, *((uint32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TSDB_CFG_VTYPE_FLOAT:
        pPrint(" %s:%s%f%s", cfg->option, blank, *((float *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TSDB_CFG_VTYPE_STRING:
      case TSDB_CFG_VTYPE_IPSTR:
      case TSDB_CFG_VTYPE_DIRECTORY:
        pPrint(" %s:%s%s%s", cfg->option, blank, (char *)cfg->ptr, tsGlobalUnit[cfg->unitType]);
        break;
      default:
        break;
    }
  }

  if (tsPrintStorageConfig) {
    tsPrintStorageConfig();
  } else {
    pPrint(" dataDir:                %s", dataDir);
  }

  tsPrintOsInfo();
}

void tsSetAllDebugFlag() {
  if (mdebugFlag != debugFlag) mdebugFlag = debugFlag;
  if (ddebugFlag != debugFlag) ddebugFlag = debugFlag;
  if (sdbDebugFlag != debugFlag) sdbDebugFlag = debugFlag;
  if (rpcDebugFlag != debugFlag) rpcDebugFlag = debugFlag;
  if (cdebugFlag != debugFlag) cdebugFlag = debugFlag;
  if (jnidebugFlag != debugFlag) jnidebugFlag = debugFlag;
  if (uDebugFlag != debugFlag) uDebugFlag = debugFlag;
  if (httpDebugFlag != debugFlag) httpDebugFlag = debugFlag;
  if (monitorDebugFlag != debugFlag) monitorDebugFlag = debugFlag;
  if (odbcdebugFlag != debugFlag) odbcdebugFlag = debugFlag;
  pPrint("all debug flag are set to %d", debugFlag);
}

/**
 * In some Linux systems, setLocale(LC_CTYPE, "") may return NULL, in which case the launch of
 * both the TDengine Server and the Client may be interrupted.
 *
 * In case that the setLocale failed to be executed, the right charset needs to be set.
 */
void tsSetLocale() {
  char msgLocale[] = "Invalid locale:%s, please set the valid locale in config file\n";
  char msgCharset[] = "Invalid charset:%s, please set the valid charset in config file\n";
  char msgCharset1[] = "failed to get charset, please set the valid charset in config file\n";

  char *locale = setlocale(LC_CTYPE, tsLocale);

  // default locale or user specified locale is not valid, abort launch
  if (locale == NULL) {
    printf(msgLocale, tsLocale);
    pPrint(msgLocale, tsLocale);
  }

  if (strlen(tsCharset) == 0) {
    printf("%s\n", msgCharset1);
    pPrint(msgCharset1);
    exit(-1);
  }

  if (!taosValidateEncodec(tsCharset)) {
    printf(msgCharset, tsCharset);
    pPrint(msgCharset, tsCharset);
    exit(-1);
  }
}

void tsSetTimeZone() {
  SGlobalConfig *cfg_timezone = tsGetConfigOption("timezone");
  pPrint("timezone is set to %s by %s", tsTimezone, tsCfgStatusStr[cfg_timezone->cfgStatus]);

#ifdef WINDOWS
  char winStr[TSDB_LOCALE_LEN * 2];
  sprintf(winStr, "TZ=%s", tsTimezone);
  putenv(winStr);
#else
  setenv("TZ", tsTimezone, 1);
#endif
  tzset();

  /*
  * get CURRENT time zone.
  * system current time zone is affected by daylight saving time(DST)
  *
  * e.g., the local time zone of London in DST is GMT+01:00,
  * otherwise is GMT+00:00
  */
#ifdef _MSC_VER
#if _MSC_VER >= 1900
  // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
  int64_t timezone = _timezone;
  int32_t daylight = _daylight;
  char **tzname = _tzname;
#endif
#endif

  int32_t tz = (-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR;
  tz += daylight;

  /*
  * format:
  * (CST, +0800)
  * (BST, +0100)
  */
  sprintf(tsTimezone, "(%s, %s%02d00)", tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));

  pPrint("timezone format changed to %s", tsTimezone);
}

