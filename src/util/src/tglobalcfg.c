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

#include <locale.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "os.h"
#include "tglobalcfg.h"
#include "tkey.h"
#include "tlog.h"
#include "tsdb.h"
#include "tsocket.h"
#include "tsystem.h"
#include "tutil.h"

// system info, not configurable
int64_t tsPageSize;
int64_t tsOpenMax;
int64_t tsStreamMax;
int32_t tsNumOfCores;
int32_t tsTotalDiskGB;
int32_t tsTotalMemoryMB;
int32_t tsVersion = 0;

// global, not configurable
int tscEmbedded = 0;

/*
 * minmum scale for whole system, millisecond by default
 * for TSDB_TIME_PRECISION_MILLI: 86400000L
 *     TSDB_TIME_PRECISION_MICRO: 86400000000L
 */
int64_t tsMsPerDay[] = {86400000L, 86400000000L};

short tsMgmtShellPort = 6030;   // udp[6030-6034] tcp[6030]
short tsVnodeShellPort = 6035;  // udp[6035-6039] tcp[6035]

int tsStatusInterval = 1;         // second
int tsShellActivityTimer = 3;     // second
int tsVnodePeerHBTimer = 1;       // second
int tsMgmtPeerHBTimer = 1;        // second
int tsMeterMetaKeepTimer = 7200;  // second
int tsMetricMetaKeepTimer = 600;  // second

float tsNumOfThreadsPerCore = 1.0;
float tsRatioOfQueryThreads = 0.5;
char  tsInternalIp[TSDB_IPv4ADDR_LEN] = {0};
char  tsServerIpStr[TSDB_IPv4ADDR_LEN] = "0.0.0.0";
int   tsNumOfVnodesPerCore = 8;
int   tsNumOfTotalVnodes = 0;

int tsSessionsPerVnode = 1000;
int tsCacheBlockSize = 16384;  // 256 columns
int tsAverageCacheBlocks = 4;

int   tsRowsInFileBlock = 4096;
float tsFileBlockMinPercent = 0.05;

short tsNumOfBlocksPerMeter = 100;
int   tsCommitTime = 3600;  // seconds
int   tsCommitLog = 1;
int   tsCompression = 2;
int   tsDaysPerFile = 10;
int   tsDaysToKeep = 3650;

int  tsMaxShellConns = 2000;
int  tsMaxUsers = 1000;
int  tsMaxDbs = 1000;
int  tsMaxTables = 650000;
int  tsMaxDnodes = 1000;
int  tsMaxVGroups = 1000;

char tsLocalIp[TSDB_IPv4ADDR_LEN] = "0.0.0.0";
char tsDefaultDB[TSDB_DB_NAME_LEN] = {0};
char tsDefaultUser[64] = "root";
char tsDefaultPass[64] = "taosdata";
int  tsMaxMeterConnections = 10000;
int  tsMaxMgmtConnections = 2000;
int  tsMaxVnodeConnections = 10000;

int tsEnableHttpModule = 1;
int tsEnableMonitorModule = 1;
int tsRestRowLimit = 10240;

int tsTimePrecision = TSDB_TIME_PRECISION_MILLI;  // time precision, millisecond by default
int tsMinSlidingTime = 10;                        // 10 ms for sliding time, the value will changed in
                                                  // case of time precision changed
int tsMinIntervalTime = 10;                       // 10 ms for interval time range, changed accordingly
int tsMaxStreamComputDelay = 20000;               // 20sec, the maximum value of stream
                                                  // computing delay, changed accordingly
int tsStreamCompStartDelay = 10000;               // 10sec, the first stream computing delay
                                                  // time after system launched successfully,
                                                  // changed accordingly
int tsStreamCompRetryDelay = 10;                  // the stream computing delay time after
                                                  // executing failed, change accordingly

int     tsProjectExecInterval = 10000;   // every 10sec, the projection will be executed once
int64_t tsMaxRetentWindow = 24 * 3600L;  // maximum time window tolerance

char  tsHttpIp[TSDB_IPv4ADDR_LEN] = "0.0.0.0";
short tsHttpPort = 6020;                 // only tcp, range tcp[6020]
// short tsNginxPort = 6060;             //only tcp, range tcp[6060]
int tsHttpCacheSessions = 100;
int tsHttpSessionExpire = 36000;
int tsHttpMaxThreads = 2;
int tsHttpEnableCompress = 0;
int tsTelegrafUseFieldNum = 0;

char tsMonitorDbName[] = "log";
int  tsMonitorInterval = 30;  // seconds
char tsInternalPass[] = "secretkey";

char tsTimezone[64] = {0};
char tsLocale[TSDB_LOCALE_LEN] = {0};
char tsCharset[TSDB_LOCALE_LEN] = {0};  // default encode string

int tsNumOfLogLines = 10000000;
int ddebugFlag = 131;
int mdebugFlag = 135;
int sdbDebugFlag = 135;
int cdebugFlag = 131;
int jnidebugFlag = 131;
int httpDebugFlag = 131;
int monitorDebugFlag = 131;
int debugFlag = 131;
int odbcdebugFlag = 131;
int qdebugFlag = 131;

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

SGlobalConfig *tsGetConfigOption(char *option) {
  tsInitGlobalConfig();
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (strcasecmp(cfg->option, option) != 0) continue;
    return cfg;
  }
  return NULL;
}

void tsReadConfigOption(char *option, char *value) {
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

void tsInitGlobalConfig() {
  if (tsGlobalConfig != NULL) return;

  tsGlobalConfig = (SGlobalConfig *)malloc(sizeof(SGlobalConfig) * TSDB_CFG_MAX_NUM);
  memset(tsGlobalConfig, 0, sizeof(SGlobalConfig) * TSDB_CFG_MAX_NUM);

  SGlobalConfig *cfg = tsGlobalConfig;

  // ip address
  tsInitConfigOption(cfg++, "internalIp", tsInternalIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG, 0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "serverIp", tsServerIpStr, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "localIp", tsLocalIp, TSDB_CFG_VTYPE_IPSTR,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, TSDB_IPv4ADDR_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpIp", tsHttpIp, TSDB_CFG_VTYPE_IPSTR, TSDB_CFG_CTYPE_B_CONFIG, 0, 0, TSDB_IPv4ADDR_LEN,
                     TSDB_CFG_UTYPE_NONE);

  // port
  tsInitConfigOption(cfg++, "httpPort", &tsHttpPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 65535, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mgmtShellPort", &tsMgmtShellPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT, 1, 65535, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "vnodeShellPort", &tsVnodeShellPort, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT, 1, 65535, 0,
                     TSDB_CFG_UTYPE_NONE);

  // directory
  tsInitConfigOption(cfg++, "configDir", configDir, TSDB_CFG_VTYPE_DIRECTORY, TSDB_CFG_CTYPE_B_CLIENT, 0, 0,
                       TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);
#ifdef LINUX
  tsInitConfigOption(cfg++, "dataDir", dataDir, TSDB_CFG_VTYPE_DIRECTORY, TSDB_CFG_CTYPE_B_CONFIG, 0, 0,
                     TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);
#endif
  tsInitConfigOption(cfg++, "logDir", logDir, TSDB_CFG_VTYPE_DIRECTORY,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, TSDB_FILENAME_LEN,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "scriptDir", scriptDir, TSDB_CFG_VTYPE_DIRECTORY, TSDB_CFG_CTYPE_B_CONFIG, 0, 0,
                     TSDB_FILENAME_LEN, TSDB_CFG_UTYPE_NONE);

  // dnode configs
  tsInitConfigOption(cfg++, "numOfThreadsPerCore", &tsNumOfThreadsPerCore, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 10, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "ratioOfQueryThreads", &tsRatioOfQueryThreads, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG, 0.1, 0.9, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "numOfVnodesPerCore", &tsNumOfVnodesPerCore, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 64, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "numOfTotalVnodes", &tsNumOfTotalVnodes, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG, 0,
                     TSDB_MAX_VNODES, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "tables", &tsSessionsPerVnode, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 4, 220000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "cache", &tsCacheBlockSize, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 100, 1048576, 0, TSDB_CFG_UTYPE_BYTE);
  tsInitConfigOption(cfg++, "rows", &tsRowsInFileBlock, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 200, 1048576, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "fileBlockMinPercent", &tsFileBlockMinPercent, TSDB_CFG_VTYPE_FLOAT,
                     TSDB_CFG_CTYPE_B_CONFIG, 0, 1.0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "ablocks", &tsAverageCacheBlocks, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 2, 128, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "tblocks", &tsNumOfBlocksPerMeter, TSDB_CFG_VTYPE_SHORT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 32, 4096, 0, TSDB_CFG_UTYPE_NONE);

  // time
  tsInitConfigOption(cfg++, "monitorInterval", &tsMonitorInterval, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG, 1, 600,
                     0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "rpcTimer", &tsRpcTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 100, 3000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "rpcMaxTime", &tsRpcMaxTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 100, 7200, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "ctime", &tsCommitTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 30, 40960, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "statusInterval", &tsStatusInterval, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 10, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "shellActivityTimer", &tsShellActivityTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 1, 120, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "meterMetaKeepTimer", &tsMeterMetaKeepTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 1, 36000, 0, TSDB_CFG_UTYPE_SECOND);
  tsInitConfigOption(cfg++, "metricMetaKeepTimer", &tsMetricMetaKeepTimer, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 1, 36000, 0, TSDB_CFG_UTYPE_SECOND);

  // mgmt configs
  tsInitConfigOption(cfg++, "maxUsers", &tsMaxUsers, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 1000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxDbs", &tsMaxDbs, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1,
                     10000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxTables", &tsMaxTables, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 100000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxVGroups", &tsMaxVGroups, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 1000000, 0, TSDB_CFG_UTYPE_NONE);

  tsInitConfigOption(cfg++, "minSlidingTime", &tsMinSlidingTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 1000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "minIntervalTime", &tsMinIntervalTime, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 1000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "maxStreamCompDelay", &tsMaxStreamComputDelay, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 1000000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "maxFirstStreamCompDelay", &tsStreamCompStartDelay, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1000, 1000000000, 0, TSDB_CFG_UTYPE_MS);
  tsInitConfigOption(cfg++, "retryStreamCompDelay", &tsStreamCompRetryDelay, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 1000000000, 0, TSDB_CFG_UTYPE_MS);

  tsInitConfigOption(cfg++, "clog", &tsCommitLog, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 0, 1, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "comp", &tsCompression, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 0, 2, 0, TSDB_CFG_UTYPE_NONE);

  // database configs
  tsInitConfigOption(cfg++, "days", &tsDaysPerFile, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 365, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "keep", &tsDaysToKeep, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 1, 365000, 0, TSDB_CFG_UTYPE_NONE);

  // login configs
  tsInitConfigOption(cfg++, "defaultDB", tsDefaultDB, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, TSDB_DB_NAME_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "defaultUser", tsDefaultUser, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, TSDB_USER_LEN, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "defaultPass", tsDefaultPass, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, TSDB_PASSWORD_LEN, TSDB_CFG_UTYPE_NONE);

  // locale & charset
  tsInitConfigOption(cfg++, "timezone", tsTimezone, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, tListLen(tsTimezone),
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "locale", tsLocale, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, tListLen(tsLocale), TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "charset", tsCharset, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, tListLen(tsCharset), TSDB_CFG_UTYPE_NONE);

  // connect configs
  tsInitConfigOption(cfg++, "maxShellConns", &tsMaxShellConns, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 50000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxMeterConnections", &tsMaxMeterConnections, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 50000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxMgmtConnections", &tsMaxMgmtConnections, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 50000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "maxVnodeConnections", &tsMaxVnodeConnections, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 10, 50000000, 0, TSDB_CFG_UTYPE_NONE);

  // module configs
  tsInitConfigOption(cfg++, "enableHttp", &tsEnableHttpModule, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 0, 1, 1, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "enableMonitor", &tsEnableMonitorModule, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 0, 1, 1, TSDB_CFG_UTYPE_NONE);

  // http configs
  tsInitConfigOption(cfg++, "httpCacheSessions", &tsHttpCacheSessions, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG, 1,
                     100000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "telegrafUseFieldNum", &tsTelegrafUseFieldNum, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_SHOW, 0,
                     1, 1, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpMaxThreads", &tsHttpMaxThreads, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG, 1,
                     1000000, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpEnableCompress", &tsHttpEnableCompress, TSDB_CFG_VTYPE_INT, TSDB_CFG_CTYPE_B_CONFIG, 0,
                     1, 1, TSDB_CFG_UTYPE_NONE);

  // debug flag
  tsInitConfigOption(cfg++, "numOfLogLines", &tsNumOfLogLines, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 10000, 2000000000, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "asyncLog", &tsAsyncLog, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 1, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "debugFlag", &debugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "mDebugFlag", &mdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG, 0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "dDebugFlag", &ddebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG, 0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "sdbDebugFlag", &sdbDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG, 0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "taosDebugFlag", &taosDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "tmrDebugFlag", &tmrDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "cDebugFlag", &cdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "jniDebugFlag", &jnidebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "odbcDebugFlag", &odbcdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "uDebugFlag", &uDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "httpDebugFlag", &httpDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG, 0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "monitorDebugFlag", &monitorDebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG, 0, 255, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "qDebugFlag", &qdebugFlag, TSDB_CFG_VTYPE_INT,
                     TSDB_CFG_CTYPE_B_CONFIG | TSDB_CFG_CTYPE_B_LOG | TSDB_CFG_CTYPE_B_CLIENT, 0, 255, 0,
                     TSDB_CFG_UTYPE_NONE);

  // version info
  tsInitConfigOption(cfg++, "gitinfo", gitinfo, TSDB_CFG_VTYPE_STRING, TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "buildinfo", buildinfo, TSDB_CFG_VTYPE_STRING,
                     TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT, 0, 0, 0, TSDB_CFG_UTYPE_NONE);
  tsInitConfigOption(cfg++, "version", version, TSDB_CFG_VTYPE_STRING, TSDB_CFG_CTYPE_B_SHOW | TSDB_CFG_CTYPE_B_CLIENT,
                     0, 0, 0, TSDB_CFG_UTYPE_NONE);

  tsGlobalConfigNum = (int)(cfg - tsGlobalConfig);
}

void tsReadGlobalLogConfig() {
  tsInitGlobalConfig();

  FILE * fp;
  char * line, *option, *value;
  size_t len;
  int    olen, vlen;
  char   fileName[128];

  mdebugFlag = 135;
  sdbDebugFlag = 135;

  wordexp_t full_path;
  wordexp(configDir, &full_path, 0);
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strcpy(configDir, full_path.we_wordv[0]);
  } else {
    strcpy(configDir, "/etc/taos");
    printf("configDir:%s not there, use default value: /etc/taos", configDir);
  }
  wordfree(&full_path);

  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    printf("option file:%s not found, all options are set to system default\n", fileName);
    tsReadLogOption("logDir", logDir);
    return;
  }

  line = NULL;
  while (!feof(fp)) {
    tfree(line);
    line = option = value = NULL;
    len = olen = vlen = 0;

    getline(&line, &len, fp);
    if (line == NULL) break;

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

  FILE * fp;
  char * line, *option, *value, *value1;
  size_t len;
  int    olen, vlen, vlen1;
  char   fileName[128];

  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    // printf("option file:%s not there, all options are set to system default\n", fileName);
    // return;
  } else {
    line = NULL;
    while (!feof(fp)) {
      tfree(line);
      line = option = value = NULL;
      len = olen = vlen = 0;

      getline(&line, &len, fp);
      if (line == NULL) break;

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

    tfree(line);
    fclose(fp);
  }

  if (tsInternalIp[0] == 0) {
    taosGetPrivateIp(tsInternalIp);
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
  if (olen == 0) return 0;

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

    if (strncasecmp(cfg->option, "debugFlag", olen) == 0) tsSetAllDebugFlag();
    return code;
  }

  if (strncasecmp(option, "resetlog", 8) == 0) {
    taosResetLogFile();
    tsPrintGlobalConfig();
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
#ifdef LINUX
  pPrint(" dataDir:                %s", dataDir);
#endif

  tsPrintOsInfo();

  pPrint("==================================");
}

void tsSetAllDebugFlag() {
  if (mdebugFlag != debugFlag) mdebugFlag = debugFlag;
  if (ddebugFlag != debugFlag) ddebugFlag = debugFlag;
  if (sdbDebugFlag != debugFlag) sdbDebugFlag = debugFlag;
  if (taosDebugFlag != debugFlag) taosDebugFlag = debugFlag;
  if (cdebugFlag != debugFlag) cdebugFlag = debugFlag;
  if (jnidebugFlag != debugFlag) jnidebugFlag = debugFlag;
  if (uDebugFlag != debugFlag) uDebugFlag = debugFlag;
  if (httpDebugFlag != debugFlag) httpDebugFlag = debugFlag;
  if (monitorDebugFlag != debugFlag) monitorDebugFlag = debugFlag;
  if (odbcdebugFlag != debugFlag) odbcdebugFlag = debugFlag;
  pPrint("all debug flag are set to %d", debugFlag);
}

void tsSetLocale() {
  char msgLocale[] = "Invalid locale:%s, please set the valid locale in config file";
  char msgCharset[] = "Invalid charset:%s, please set the valid charset in config file";
  char msgCharset1[] = "failed to get charset, please set the valid charset in config file";

  char *locale = setlocale(LC_CTYPE, tsLocale);

  /* default locale or user specified locale is not valid, abort launch */
  if (locale == NULL) {
    printf(msgLocale, tsLocale);
    pPrint(msgLocale, tsLocale);
    exit(-1);
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
