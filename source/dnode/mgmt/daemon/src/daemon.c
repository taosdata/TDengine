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
#include "dnode.h"
#include "os.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tnote.h"
#include "ulog.h"

static struct {
  bool stop;
  bool dumpConfig;
  bool generateGrant;
  bool printAuth;
  bool printVersion;
  char configDir[PATH_MAX];
} dmn = {0};

void dmnSigintHandle(int signum, void *info, void *ctx) {
  uInfo("singal:%d is received", signum);
  dmn.stop = true;
}

void dmnSetSignalHandle() {
  taosSetSignal(SIGTERM, dmnSigintHandle);
  taosSetSignal(SIGHUP, dmnSigintHandle);
  taosSetSignal(SIGINT, dmnSigintHandle);
  taosSetSignal(SIGABRT, dmnSigintHandle);
  taosSetSignal(SIGBREAK, dmnSigintHandle);
}

int dmnParseOption(int argc, char const *argv[]) {
  tstrncpy(dmn.configDir, "/etc/taos", PATH_MAX);

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(dmn.configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      dmn.dumpConfig = true;
    } else if (strcmp(argv[i], "-k") == 0) {
      dmn.generateGrant = true;
    } else if (strcmp(argv[i], "-A") == 0) {
      dmn.printAuth = true;
    } else if (strcmp(argv[i], "-V") == 0) {
      dmn.printVersion = true;
    } else {
    }
  }

  return 0;
}

void dmnGenerateGrant() {
#if 0
  grantParseParameter();
#endif
}

void dmnPrintVersion() {
#ifdef TD_ENTERPRISE
  char *releaseName = "enterprise";
#else
  char *releaseName = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", releaseName, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
  printf("gitinfoI: %s\n", gitinfoOfInternal);
  printf("builuInfo: %s\n", buildinfo);
}

int dmnReadConfig(const char *path) {
  tstrncpy(configDir, dmn.configDir, PATH_MAX);
  taosInitGlobalCfg();
  taosReadGlobalLogCfg();

  if (taosMkDir(tsLogDir) != 0) {
    printf("failed to create dir: %s, reason: %s\n", tsLogDir, strerror(errno));
    return -1;
  }

  char temp[PATH_MAX];
  snprintf(temp, PATH_MAX, "%s/taosdlog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) != 0) {
    printf("failed to init log file\n");
    return -1;
  }

  if (taosInitNotes() != 0) {
    printf("failed to init log file\n");
    return -1;
  }

  if (taosReadCfgFromFile() != 0) {
    uError("failed to read config");
    return -1;
  }

  if (taosCheckAndPrintCfg() != 0) {
    uError("failed to check config");
    return -1;
  }

  taosSetCoreDump(tsEnableCoreFile);
  return 0;
}

void dmnDumpConfig() { taosDumpGlobalCfg(); }

void dmnWaitSignal() {
  dmnSetSignalHandle();
  while (!dmn.stop) {
    taosMsleep(100);
  }
}

void dnmInitEnvCfg(SDnodeEnvCfg *pCfg) {
  pCfg->sver = 30000000;  // 3.0.0.0
  pCfg->numOfCores = tsNumOfCores;
  pCfg->numOfCommitThreads = tsNumOfCommitThreads;
  pCfg->enableTelem = 0;
  tstrncpy(pCfg->timezone, tsTimezone, TSDB_TIMEZONE_LEN);
  tstrncpy(pCfg->locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pCfg->charset, tsCharset, TSDB_LOCALE_LEN);
  tstrncpy(pCfg->buildinfo, buildinfo, 64);
  tstrncpy(pCfg->gitinfo, gitinfo, 48);
}

void dmnInitObjCfg(SDnodeObjCfg *pCfg) {
  pCfg->numOfSupportVnodes = tsNumOfSupportVnodes;
  pCfg->statusInterval = tsStatusInterval;
  pCfg->numOfThreadsPerCore = tsNumOfThreadsPerCore;
  pCfg->ratioOfQueryCores = tsRatioOfQueryCores;
  pCfg->maxShellConns = tsMaxShellConns;
  pCfg->shellActivityTimer = tsShellActivityTimer;
  pCfg->serverPort = tsServerPort;
  tstrncpy(pCfg->dataDir, tsDataDir, TSDB_FILENAME_LEN);
  tstrncpy(pCfg->localEp, tsLocalEp, TSDB_EP_LEN);
  tstrncpy(pCfg->localFqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  tstrncpy(pCfg->firstEp, tsFirst, TSDB_EP_LEN);
}

int dmnRunDnode() {
  SDnodeEnvCfg envCfg = {0};
  SDnodeObjCfg objCfg = {0};

  dnmInitEnvCfg(&envCfg);
  dmnInitObjCfg(&objCfg);

  if (dndInit(&envCfg) != 0) {
    uInfo("Failed to start TDengine, please check the log at %s", tsLogDir);
    return -1;
  }

  SDnode *pDnode = dndCreate(&objCfg);
  if (pDnode == NULL) {
    uInfo("Failed to start TDengine, please check the log at %s", tsLogDir);
    return -1;
  }

  uInfo("Started TDengine service successfully.");
  dmnWaitSignal();
  uInfo("TDengine is shut down!");

  dndClose(pDnode);
  dndCleanup();
  taosCloseLog();
  return 0;
}

int main(int argc, char const *argv[]) {
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

  if (dmnReadConfig(dmn.configDir) != 0) {
    return -1;
  }

  if (dmn.dumpConfig) {
    dmnDumpConfig();
    return 0;
  }

  return dmnRunDnode();
}
