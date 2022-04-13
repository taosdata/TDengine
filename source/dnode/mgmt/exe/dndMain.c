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
#include "dndInt.h"
#include "tconfig.h"
#include "tgrant.h"

static struct {
  bool     dumpConfig;
  bool     generateGrant;
  bool     printAuth;
  bool     printVersion;
  char     envFile[PATH_MAX];
  char     apolloUrl[PATH_MAX];
  SArray  *pArgs;  // SConfigPair
  SDnode  *pDnode;
  EDndType ntype;
} global = {0};

static void dndStopDnode(int signum, void *info, void *ctx) {
  SDnode *pDnode = atomic_val_compare_exchange_ptr(&global.pDnode, 0, global.pDnode);
  if (pDnode != NULL) {
    dndHandleEvent(pDnode, DND_EVENT_STOP);
  }
}

static void dndSetSignalHandle() {
  taosSetSignal(SIGTERM, dndStopDnode);
  taosSetSignal(SIGHUP, dndStopDnode);
  taosSetSignal(SIGINT, dndStopDnode);
  taosSetSignal(SIGTSTP, dndStopDnode);
  taosSetSignal(SIGABRT, dndStopDnode);
  taosSetSignal(SIGBREAK, dndStopDnode);
  taosSetSignal(SIGQUIT, dndStopDnode);

  if (!tsMultiProcess) {
  } else if (global.ntype == DNODE || global.ntype == NODE_MAX) {
    taosIgnSignal(SIGCHLD);
  } else {
    taosKillChildOnParentStopped();
  }
}

static int32_t dndParseArgs(int32_t argc, char const *argv[]) {
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
    } else if (strcmp(argv[i], "-a") == 0) {
      tstrncpy(global.apolloUrl, argv[++i], PATH_MAX);
    } else if (strcmp(argv[i], "-e") == 0) {
      tstrncpy(global.envFile, argv[++i], PATH_MAX);
    } else if (strcmp(argv[i], "-n") == 0) {
      global.ntype = atoi(argv[++i]);
      if (global.ntype <= DNODE || global.ntype > NODE_MAX) {
        printf("'-n' range is [1 - %d], default is 0\n", NODE_MAX - 1);
        return -1;
      }
    } else if (strcmp(argv[i], "-k") == 0) {
      global.generateGrant = true;
    } else if (strcmp(argv[i], "-C") == 0) {
      global.dumpConfig = true;
    } else if (strcmp(argv[i], "-V") == 0) {
      global.printVersion = true;
    } else {
    }
  }

  return 0;
}

static void dndGenerateGrant() {
  grantParseParameter();
}

static void dndPrintVersion() {
#ifdef TD_ENTERPRISE
  char *releaseName = "enterprise";
#else
  char *releaseName = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", releaseName, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
  printf("buildInfo: %s\n", buildinfo);
}

static void dndDumpCfg() {
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfg(pCfg, 0, 1);
}

static SDnodeOpt dndGetOpt() {
  SConfig  *pCfg = taosGetCfg();
  SDnodeOpt option = {0};

  option.numOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  tstrncpy(option.dataDir, tsDataDir, sizeof(option.dataDir));
  tstrncpy(option.firstEp, tsFirst, sizeof(option.firstEp));
  tstrncpy(option.secondEp, tsSecond, sizeof(option.firstEp));
  option.serverPort = tsServerPort;
  tstrncpy(option.localFqdn, tsLocalFqdn, sizeof(option.localFqdn));
  snprintf(option.localEp, sizeof(option.localEp), "%s:%u", option.localFqdn, option.serverPort);
  option.disks = tsDiskCfg;
  option.numOfDisks = tsDiskCfgNum;
  option.ntype = global.ntype;
  return option;
}

static int32_t dndInitLog() {
  char logName[12] = {0};
  snprintf(logName, sizeof(logName), "%slog", dndNodeLogStr(global.ntype));
  return taosCreateLog(logName, 1, configDir, global.envFile, global.apolloUrl, global.pArgs, 0);
}

static void dndSetProcInfo(int32_t argc, char **argv) {
  taosSetProcPath(argc, argv);
  if (global.ntype != DNODE && global.ntype != NODE_MAX) {
    const char *name = dndNodeProcStr(global.ntype);
    taosSetProcName(argc, argv, name);
  }
}

static int32_t dndRunDnode() {
  if (dndInit() != 0) {
    dError("failed to init environment since %s", terrstr());
    return -1;
  }

  SDnodeOpt option = dndGetOpt();
  SDnode   *pDnode = dndCreate(&option);
  if (pDnode == NULL) {
    dError("failed to to create dnode since %s", terrstr());
    return -1;
  } else {
    global.pDnode = pDnode;
    dndSetSignalHandle();
  }

  dInfo("start the service");
  int32_t code = dndRun(pDnode);
  dInfo("start shutting down the service");

  global.pDnode = NULL;
  dndClose(pDnode);
  dndCleanup();
  taosCloseLog();
  taosCleanupCfg();
  return code;
}

int main(int argc, char const *argv[]) {
  if (!taosCheckSystemIsSmallEnd()) {
    printf("failed to start since on non-small-end machines\n");
    return -1;
  }

  if (dndParseArgs(argc, argv) != 0) {
    printf("failed to start since parse args error\n");
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

  if (dndInitLog() != 0) {
    printf("failed to start since init log error\n");
    return -1;
  }

  if (taosInitCfg(configDir, global.envFile, global.apolloUrl, global.pArgs, 0) != 0) {
    dError("failed to start since read config error");
    return -1;
  }

  if (global.dumpConfig) {
    dndDumpCfg();
    taosCleanupCfg();
    taosCloseLog();
    return 0;
  }

  dndSetProcInfo(argc, (char **)argv);
  return dndRunDnode();
}
