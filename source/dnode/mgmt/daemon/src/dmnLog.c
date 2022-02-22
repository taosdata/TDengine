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

int32_t dmnInitLogCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "logDir", "/var/log/taos") != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfLogLines", 10000000, 1000, 2000000000) != 0) return -1;
  if (cfgAddInt32(pCfg, "logKeepDays", 0, -365000, 365000) != 0) return -1;
  if (cfgAddBool(pCfg, "asyncLog", 1) != 0) return -1;
  if (cfgAddInt32(pCfg, "debugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "mDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "dDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "sDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "wDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "tmrDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "cDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "jniDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "uDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "qDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "vDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "tsdbDebugFlag", 0, 0, 255) != 0) return -1;
  if (cfgAddInt32(pCfg, "cqDebugFlag", 0, 0, 255) != 0) return -1;
  return 0;
}

int32_t dmnInitLog(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return -1;

  if (dmnInitLogCfg(pCfg) != 0) {
    uError("failed to init log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  if (dmnLoadCfg(pCfg, cfgDir, envFile, apolloUrl) != 0) {
    uError("failed to load log cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  char temp[PATH_MAX] = {0};
  snprintf(temp, PATH_MAX, "%s" TD_DIRSEP "taosdlog", cfgGetItem(pCfg, "logDir")->str);
  if (taosInitLog(temp, cfgGetItem(pCfg, "numOfLogLines")->i32, 1) != 0) {
    uError("failed to init log file since %s\n", terrstr());
    cfgCleanup(pCfg);
    return -1;
  }

  cfgCleanup(pCfg);
  return 0;
}
