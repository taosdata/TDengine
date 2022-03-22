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
#include "dndMain.h"

static struct {
  bool    dumpConfig;
  bool    generateGrant;
  bool    printAuth;
  bool    printVersion;
  char    envFile[PATH_MAX];
  char    apolloUrl[PATH_MAX];
  SDnode *pDnode;
} global = {0};

static void dndSigintHandle(int signum, void *info, void *ctx) {
  dInfo("signal:%d is received", signum);
  SDnode *pDnode = atomic_val_compare_exchange_ptr(&global.pDnode, 0, global.pDnode);
  if (pDnode != NULL) {
    dndHandleEvent(pDnode, DND_EVENT_STOP);
  }
}

static void dndSetSignalHandle() {
  taosSetSignal(SIGTERM, dndSigintHandle);
  taosSetSignal(SIGHUP, dndSigintHandle);
  taosSetSignal(SIGINT, dndSigintHandle);
  taosSetSignal(SIGABRT, dndSigintHandle);
  taosSetSignal(SIGBREAK, dndSigintHandle);
}

static int32_t dndParseOption(int32_t argc, char const *argv[]) {
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
      global.dumpConfig = true;
    } else if (strcmp(argv[i], "-k") == 0) {
      global.generateGrant = true;
    } else if (strcmp(argv[i], "-V") == 0) {
      global.printVersion = true;
    } else {
    }
  }

  return 0;
}

static int32_t dndRunDnode() {
  if (dndInit() != 0) {
    dInfo("failed to initialize dnode environment since %s", terrstr());
    return -1;
  }

  SDnodeOpt option = dndGetOpt();

  SDnode *pDnode = dndCreate(&option);
  if (pDnode == NULL) {
    dError("failed to to create dnode object since %s", terrstr());
    return -1;
  } else {
    global.pDnode = pDnode;
    dndSetSignalHandle();
  }

  dInfo("start the TDengine service");
  int32_t code = dndRun(pDnode);
  dInfo("start shutting down the TDengine service");

  global.pDnode = NULL;
  dndClose(pDnode);
  dndCleanup();
  taosCloseLog();
  taosCleanupCfg();
  return code;
}

int main(int argc, char const *argv[]) {
  if (!taosCheckSystemIsSmallEnd()) {
    dError("failed to start TDengine since on non-small-end machines");
    return -1;
  }

  if (dndParseOption(argc, argv) != 0) {
    return -1;
  }

  if (global.generateGrant) {
    dndGenerateGrant();
    return 0;
  }

  if (global.printVersion) {
    dndPrintVersion();
    return 0;
  }

  if (taosCreateLog("taosdlog", 1, configDir, global.envFile, global.apolloUrl, NULL, 0) != 0) {
    dError("failed to start TDengine since read log config error");
    return -1;
  }

  if (taosInitCfg(configDir, global.envFile, global.apolloUrl, NULL, 0) != 0) {
    dError("failed to start TDengine since read config error");
    return -1;
  }

  if (global.dumpConfig) {
    dndDumpCfg();
    taosCleanupCfg();
    taosCloseLog();
    return 0;
  }

  return dndRunDnode();
}
