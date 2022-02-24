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
#include "dmnInt.h"

int32_t dmnAddLogCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "logDir", osLogDir()) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalLogDirGB", 1.0f, 0.001f, 10000000) != 0) return -1;
  if (cfgAddBool(pCfg, "asyncLog", 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfLogLines", 10000000, 1000, 2000000000) != 0) return -1;
  if (cfgAddInt32(pCfg, "logKeepDays", 0, -365000, 365000) != 0) return -1;
  if (cfgAddInt32(pCfg, "debugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "dDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "vDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "mDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "cDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "jniDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "tmrDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "uDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "qDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "wDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "sDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "tsdbDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "tqDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "fsDebugFlag", 0, 0, 255) != 0) return -1;
  return 0;
}

int32_t dmnSetLogCfg(SConfig *pCfg) {
  osSetLogDir(cfgGetItem(pCfg, "logDir")->str);
  osSetLogReservedSpace(cfgGetItem(pCfg, "minimalLogDirGB")->fval);
  tsAsyncLog = cfgGetItem(pCfg, "asyncLog")->bval;
  tsNumOfLogLines = cfgGetItem(pCfg, "numOfLogLines")->i32;
  tsLogKeepDays = cfgGetItem(pCfg, "logKeepDays")->i32;
  dDebugFlag = cfgGetItem(pCfg, "dDebugFlag")->i32;
  vDebugFlag = cfgGetItem(pCfg, "vDebugFlag")->i32;
  mDebugFlag = cfgGetItem(pCfg, "mDebugFlag")->i32;
  cDebugFlag = cfgGetItem(pCfg, "cDebugFlag")->i32;
  jniDebugFlag = cfgGetItem(pCfg, "jniDebugFlag")->i32;
  tmrDebugFlag = cfgGetItem(pCfg, "tmrDebugFlag")->i32;
  uDebugFlag = cfgGetItem(pCfg, "uDebugFlag")->i32;
  rpcDebugFlag = cfgGetItem(pCfg, "rpcDebugFlag")->i32;
  qDebugFlag = cfgGetItem(pCfg, "qDebugFlag")->i32;
  wDebugFlag = cfgGetItem(pCfg, "wDebugFlag")->i32;
  sDebugFlag = cfgGetItem(pCfg, "sDebugFlag")->i32;
  tsdbDebugFlag = cfgGetItem(pCfg, "tsdbDebugFlag")->i32;
  tqDebugFlag = cfgGetItem(pCfg, "tqDebugFlag")->i32;
  fsDebugFlag = cfgGetItem(pCfg, "fsDebugFlag")->i32;

  int32_t debugFlag = cfgGetItem(pCfg, "debugFlag")->i32;
  taosSetAllDebugFlag(debugFlag);

  return 0;
}

int32_t dmnInitLog(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return -1;

  if (dmnAddLogCfg(pCfg) != 0) {
    printf("failed to add log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (dmnLoadCfg(pCfg, cfgDir, envFile, apolloUrl) != 0) {
    printf("failed to load log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (dmnSetLogCfg(pCfg) != 0) {
    printf("failed to set log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (taosInitLog("taosdlog", 1) != 0) {
    printf("failed to init log file since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  cfgCleanup(pCfg);
  return 0;
}

int32_t dmnLoadCfg(SConfig *pConfig, const char *inputCfgDir, const char *envFile, const char *apolloUrl) {
  char configDir[PATH_MAX] = {0};
  char configFile[PATH_MAX + 100] = {0};

  taosExpandDir(inputCfgDir, configDir, PATH_MAX);
  snprintf(configFile, sizeof(configFile), "%s" TD_DIRSEP "taos.cfg", configDir);

  if (cfgLoad(pConfig, CFG_STYPE_APOLLO_URL, apolloUrl) != 0) {
    uError("failed to load from apollo url:%s since %s\n", apolloUrl, terrstr());
    return -1;
  }

  if (cfgLoad(pConfig, CFG_STYPE_CFG_FILE, configFile) != 0) {
    if (cfgLoad(pConfig, CFG_STYPE_CFG_FILE, configDir) != 0) {
      uError("failed to load from config file:%s since %s\n", configFile, terrstr());
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
