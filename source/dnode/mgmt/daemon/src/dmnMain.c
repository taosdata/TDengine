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

static struct {
  bool stop;
  bool dumpConfig;
  bool generateGrant;
  bool printAuth;
  bool printVersion;
  char envFile[PATH_MAX];
  char apolloUrl[PATH_MAX];
} dmn = {0};

static void dmnSigintHandle(int signum, void *info, void *ctx) {
  uInfo("signal:%d is received", signum);
  dmn.stop = true;
}

static void dmnSetSignalHandle() {
  taosSetSignal(SIGTERM, dmnSigintHandle);
  taosSetSignal(SIGHUP, dmnSigintHandle);
  taosSetSignal(SIGINT, dmnSigintHandle);
  taosSetSignal(SIGABRT, dmnSigintHandle);
  taosSetSignal(SIGBREAK, dmnSigintHandle);
}

static void dmnWaitSignal() {
  dmnSetSignalHandle();
  while (!dmn.stop) {
    taosMsleep(100);
  }
}

static int32_t dmnParseOption(int32_t argc, char const *argv[]) {
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      dmn.dumpConfig = true;
    } else if (strcmp(argv[i], "-k") == 0) {
      dmn.generateGrant = true;
    } else if (strcmp(argv[i], "-V") == 0) {
      dmn.printVersion = true;
    } else {
    }
  }

  return 0;
}

int32_t dmnRunDnode() {
  if (dndInit() != 0) {
    uInfo("Failed to start TDengine, please check the log");
    return -1;
  }

  SDnodeObjCfg objCfg = dmnGetObjCfg();
  SDnode      *pDnode = dndCreate(&objCfg);
  if (pDnode == NULL) {
    uInfo("Failed to start TDengine, please check the log");
    return -1;
  }

  uInfo("Started TDengine service successfully.");
  dmnWaitSignal();
  uInfo("TDengine is shut down!");

  dndClose(pDnode);
  dndCleanup();
  taosCloseLog();
  taosCleanupCfg();
  return 0;
}

int main(int argc, char const *argv[]) {
  if (!taosCheckSystemIsSmallEnd()) {
    uError("TDengine does not run on non-small-end machines.");
    return -1;
  }

  if (dmnParseOption(argc, argv) != 0) {
    return -1;
  }

  if (dmn.generateGrant) {
    dmnGenerateGrant();
    return 0;
  }

  if (dmn.printVersion) {
    dmnPrintVersion();
    return 0;
  }

  if (taosCreateLog("taosdlog", 1, configDir, dmn.envFile, dmn.apolloUrl, NULL, 0) != 0) {
    uInfo("Failed to start TDengine since read config error");
    return -1;
  }

  if (taosInitCfg(configDir, dmn.envFile, dmn.apolloUrl, NULL, 0) != 0) {
    uInfo("Failed to start TDengine since read config error");
    return -1;
  }

  if (dmn.dumpConfig) {
    dmnDumpCfg();
    taosCleanupCfg();
    return 0;
  }

  return dmnRunDnode();
}
