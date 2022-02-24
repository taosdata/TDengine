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

#define _DEFAULT_SOURCE
#include "clientInt.h"
#include "ulog.h"

// todo refact
SConfig *tscCfg;

static int32_t tscLoadCfg(SConfig *pConfig, const char *inputCfgDir, const char *envFile, const char *apolloUrl) {
  char cfgDir[PATH_MAX] = {0};
  char cfgFile[PATH_MAX + 100] = {0};

  taosExpandDir(inputCfgDir, cfgDir, PATH_MAX);
  snprintf(cfgFile, sizeof(cfgFile), "%s" TD_DIRSEP "taos.cfg", cfgDir);

  if (cfgLoad(pConfig, CFG_STYPE_APOLLO_URL, apolloUrl) != 0) {
    uError("failed to load from apollo url:%s since %s\n", apolloUrl, terrstr());
    return -1;
  }

  if (cfgLoad(pConfig, CFG_STYPE_CFG_FILE, cfgFile) != 0) {
    if (cfgLoad(pConfig, CFG_STYPE_CFG_FILE, cfgDir) != 0) {
      uError("failed to load from config file:%s since %s\n", cfgFile, terrstr());
      return -1;
    }
  }

  if (cfgLoad(pConfig, CFG_STYPE_ENV_FILE, envFile) != 0) {
    uError("failed to load from env file:%s since %s\n", envFile, terrstr());
    return -1;
  }

  if (cfgLoad(pConfig, CFG_STYPE_ENV_VAR, NULL) != 0) {
    uError("failed to load from global env variables since %s\n", terrstr());
    return -1;
  }

  return 0;
}

static int32_t tscAddLogCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "logDir", "/var/log/taos") != 0) return -1;
  if (cfgAddBool(pCfg, "asyncLog", 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfLogLines", 10000000, 1000, 2000000000) != 0) return -1;
  if (cfgAddInt32(pCfg, "logKeepDays", 0, -365000, 365000) != 0) return -1;
  if (cfgAddInt32(pCfg, "debugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "cDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "jniDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "tmrDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "uDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcDebugFlag", 0, 0, 255) != 0) return -1;
  return 0;
}

static int32_t tscSetLogCfg(SConfig *pCfg) {
  osSetLogDir(cfgGetItem(pCfg, "logDir")->str);
  tsAsyncLog = cfgGetItem(pCfg, "asyncLog")->bval;
  tsNumOfLogLines = cfgGetItem(pCfg, "numOfLogLines")->i32;
  tsLogKeepDays = cfgGetItem(pCfg, "logKeepDays")->i32;
  cDebugFlag = cfgGetItem(pCfg, "cDebugFlag")->i32;
  jniDebugFlag = cfgGetItem(pCfg, "jniDebugFlag")->i32;
  tmrDebugFlag = cfgGetItem(pCfg, "tmrDebugFlag")->i32;
  uDebugFlag = cfgGetItem(pCfg, "uDebugFlag")->i32;
  rpcDebugFlag = cfgGetItem(pCfg, "rpcDebugFlag")->i32;

  int32_t debugFlag = cfgGetItem(pCfg, "debugFlag")->i32;
  taosSetAllDebugFlag(debugFlag);
  return 0;
}

int32_t tscInitLog(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  if (tsLogInited) return 0;

  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return -1;

  if (tscAddLogCfg(pCfg) != 0) {
    printf("failed to add log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (tscLoadCfg(pCfg, cfgDir, envFile, apolloUrl) != 0) {
    printf("failed to load log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (tscSetLogCfg(pCfg) != 0) {
    printf("failed to set log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  const int32_t maxLogFileNum = 10;
  if (taosInitLog("taoslog", maxLogFileNum) != 0) {
    printf("failed to init log file since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  cfgDumpCfg(pCfg);
  cfgCleanup(pCfg);
  return 0;
}

static int32_t tscAddEpCfg(SConfig *pCfg) {
  char defaultFqdn[TSDB_FQDN_LEN] = {0};
  if (taosGetFqdn(defaultFqdn) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (cfgAddString(pCfg, "fqdn", defaultFqdn) != 0) return -1;

  int32_t defaultServerPort = 6030;
  if (cfgAddInt32(pCfg, "serverPort", defaultServerPort, 1, 65056) != 0) return -1;

  char defaultFirstEp[TSDB_EP_LEN] = {0};
  char defaultSecondEp[TSDB_EP_LEN] = {0};
  snprintf(defaultFirstEp, TSDB_EP_LEN, "%s:%d", defaultFqdn, defaultServerPort);
  snprintf(defaultSecondEp, TSDB_EP_LEN, "%s:%d", defaultFqdn, defaultServerPort);
  if (cfgAddString(pCfg, "firstEp", defaultFirstEp) != 0) return -1;
  if (cfgAddString(pCfg, "secondEp", defaultSecondEp) != 0) return -1;

  return 0;
}

static int32_t tscAddCfg(SConfig *pCfg) {
  if (tscAddEpCfg(pCfg) != 0) return -1;

  // if (cfgAddString(pCfg, "buildinfo", buildinfo) != 0) return -1;
  // if (cfgAddString(pCfg, "gitinfo", gitinfo) != 0) return -1;
  // if (cfgAddString(pCfg, "version", version) != 0) return -1;

  // if (cfgAddDir(pCfg, "dataDir", osDataDir()) != 0) return -1;
  if (cfgAddTimezone(pCfg, "timezone", "") != 0) return -1;
  if (cfgAddLocale(pCfg, "locale", "") != 0) return -1;
  if (cfgAddCharset(pCfg, "charset", "") != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfCores", 1, 1, 100000) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfCommitThreads", 4, 1, 1000) != 0) return -1;
  // if (cfgAddBool(pCfg, "telemetryReporting", 0) != 0) return -1;
  if (cfgAddBool(pCfg, "enableCoreFile", 0) != 0) return -1;
  // if (cfgAddInt32(pCfg, "supportVnodes", 256, 0, 65536) != 0) return -1;
  if (cfgAddInt32(pCfg, "statusInterval", 1, 1, 30) != 0) return -1;
  if (cfgAddFloat(pCfg, "numOfThreadsPerCore", 1, 0, 10) != 0) return -1;
  if (cfgAddFloat(pCfg, "ratioOfQueryCores", 1, 0, 5) != 0) return -1;
  if (cfgAddInt32(pCfg, "shellActivityTimer", 3, 1, 120) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcTimer", 300, 100, 3000) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcMaxTime", 600, 100, 7200) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxConnections", 50000, 1, 100000) != 0) return -1;
  return 0;
}

int32_t tscCheckCfg(SConfig *pCfg) {
  bool enableCore = cfgGetItem(pCfg, "enableCoreFile")->bval;
  taosSetCoreDump(enableCore);

  return 0;
}

SConfig *tscInitCfgImp(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return NULL;

  if (tscAddCfg(pCfg) != 0) {
    uError("failed to init tsc cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (tscLoadCfg(pCfg, cfgDir, envFile, apolloUrl) != 0) {
    printf("failed to load tsc cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (tscCheckCfg(pCfg) != 0) {
    uError("failed to check cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  cfgDumpCfg(pCfg);
  return pCfg;
}

int32_t tscInitCfg(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  tscCfg = tscInitCfgImp(cfgDir, envFile, apolloUrl);
  if (tscCfg == NULL) return -1;

  return 0;
}