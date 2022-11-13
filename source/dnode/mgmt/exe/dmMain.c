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
#include "dmMgmt.h"
#include "mnode.h"
#include "tconfig.h"

// clang-format off
#define DM_APOLLO_URL    "The apollo string to use when configuring the server, such as: -a 'jsonFile:./tests/cfg.json', cfg.json text can be '{\"fqdn\":\"td1\"}'."
#define DM_CFG_DIR       "Configuration directory."
#define DM_DMP_CFG       "Dump configuration."
#define DM_SDB_INFO      "Dump sdb info."
#define DM_ENV_CMD       "The env cmd variable string to use when configuring the server, such as: -e 'TAOS_FQDN=td1'."
#define DM_ENV_FILE      "The env variable file path to use when configuring the server, default is './.env', .env text can be 'TAOS_FQDN=td1'."
#define DM_MACHINE_CODE  "Get machine code."
#define DM_VERSION       "Print program version."
#define DM_EMAIL         "<support@taosdata.com>"
// clang-format on
static struct {
#ifdef WINDOWS
  bool winServiceMode;
#endif
  bool         dumpConfig;
  bool         dumpSdb;
  bool         generateGrant;
  bool         printAuth;
  bool         printVersion;
  bool         printHelp;
  char         envFile[PATH_MAX];
  char         apolloUrl[PATH_MAX];
  const char **envCmd;
  SArray      *pArgs;  // SConfigPair
} global = {0};

static void dmStopDnode(int signum, void *info, void *ctx) { dmStop(); }

static void dmSetSignalHandle() {
  taosSetSignal(SIGTERM, dmStopDnode);
  taosSetSignal(SIGHUP, dmStopDnode);
  taosSetSignal(SIGINT, dmStopDnode);
  taosSetSignal(SIGABRT, dmStopDnode);
  taosSetSignal(SIGBREAK, dmStopDnode);
#ifndef WINDOWS
  taosSetSignal(SIGTSTP, dmStopDnode);
  taosSetSignal(SIGQUIT, dmStopDnode);
#endif
}

static int32_t dmParseArgs(int32_t argc, char const *argv[]) {
  int32_t cmdEnvIndex = 0;
  if (argc < 2) return 0;
  global.envCmd = taosMemoryMalloc((argc - 1) * sizeof(char *));
  memset(global.envCmd, 0, (argc - 1) * sizeof(char *));
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
    } else if (strcmp(argv[i], "-s") == 0) {
      global.dumpSdb = true;
    } else if (strcmp(argv[i], "-E") == 0) {
      tstrncpy(global.envFile, argv[++i], PATH_MAX);
    } else if (strcmp(argv[i], "-k") == 0) {
      global.generateGrant = true;
    } else if (strcmp(argv[i], "-C") == 0) {
      global.dumpConfig = true;
    } else if (strcmp(argv[i], "-V") == 0) {
      global.printVersion = true;
#ifdef WINDOWS
    } else if (strcmp(argv[i], "--win_service") == 0) {
      global.winServiceMode = true;
#endif
    } else if (strcmp(argv[i], "-e") == 0) {
      global.envCmd[cmdEnvIndex] = argv[++i];
      cmdEnvIndex++;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0 ||
               strcmp(argv[i], "-?")) {
      global.printHelp = true;
    } else {
    }
  }

  return 0;
}

static void dmGenerateGrant() { mndGenerateMachineCode(); }

static void dmPrintVersion() {
#ifdef TD_ENTERPRISE
  char *releaseName = "enterprise";
#else
  char *releaseName = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", releaseName, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
  printf("buildInfo: %s\n", buildinfo);
}

static void dmPrintHelp() {
  char indent[] = "  ";
  printf("Usage: taosd [OPTION...] \n\n");
  printf("%s%s%s%s\n", indent, "-a,", indent, DM_APOLLO_URL);
  printf("%s%s%s%s\n", indent, "-c,", indent, DM_CFG_DIR);
  printf("%s%s%s%s\n", indent, "-s,", indent, DM_SDB_INFO);
  printf("%s%s%s%s\n", indent, "-C,", indent, DM_DMP_CFG);
  printf("%s%s%s%s\n", indent, "-e,", indent, DM_ENV_CMD);
  printf("%s%s%s%s\n", indent, "-E,", indent, DM_ENV_FILE);
  printf("%s%s%s%s\n", indent, "-k,", indent, DM_MACHINE_CODE);
  printf("%s%s%s%s\n", indent, "-V,", indent, DM_VERSION);

  printf("\n\nReport bugs to %s.\n", DM_EMAIL);
}

static void dmDumpCfg() {
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfg(pCfg, 0, true);
}

static int32_t dmInitLog() {
  return taosCreateLog("taosdlog", 1, configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0);
}

static void taosCleanupArgs() {
  if (global.envCmd != NULL) taosMemoryFree(global.envCmd);
}

int main(int argc, char const *argv[]) {
  if (!taosCheckSystemIsLittleEnd()) {
    printf("failed to start since on non-little-end machines\n");
    return -1;
  }

  if (dmParseArgs(argc, argv) != 0) {
    printf("failed to start since parse args error\n");
    taosCleanupArgs();
    return -1;
  }

#ifdef WINDOWS
  int mainWindows(int argc, char **argv);
  if (global.winServiceMode) {
    stratWindowsService(mainWindows);
  } else {
    return mainWindows(argc, argv);
  }
  return 0;
}
int mainWindows(int argc, char **argv) {
#endif

  if (global.generateGrant) {
    dmGenerateGrant();
    taosCleanupArgs();
    return 0;
  }

  if (global.printHelp) {
    dmPrintHelp();
    taosCleanupArgs();
    return 0;
  }

  if (global.printVersion) {
    dmPrintVersion();
    taosCleanupArgs();
    return 0;
  }

  if (dmInitLog() != 0) {
    printf("failed to start since init log error\n");
    taosCleanupArgs();
    return -1;
  }

  if (taosInitCfg(configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0) != 0) {
    dError("failed to start since read config error");
    taosCloseLog();
    taosCleanupArgs();
    return -1;
  }

  taosConvInit();

  if (global.dumpConfig) {
    dmDumpCfg();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  if (global.dumpSdb) {
    mndDumpSdb();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  osSetProcPath(argc, (char **)argv);
  taosCleanupArgs();

  if (dmInit() != 0) {
    dError("failed to init dnode since %s", terrstr());
    return -1;
  }

  dInfo("start to init service");
  dmSetSignalHandle();
  int32_t code = dmRun();
  dInfo("shutting down the service");

  dmCleanup();
  return code;
}
