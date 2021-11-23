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
} global = {0};

void dmnSigintHandle(int signum, void *info, void *ctx) { global.stop = true; }

void dmnSetSignalHandle() {
  taosSetSignal(SIGTERM, dmnSigintHandle);
  taosSetSignal(SIGHUP, dmnSigintHandle);
  taosSetSignal(SIGINT, dmnSigintHandle);
  taosSetSignal(SIGABRT, dmnSigintHandle);
  taosSetSignal(SIGBREAK, dmnSigintHandle);
}

int dmnParseOpts(int argc, char const *argv[]) {
  tstrncpy(global.configDir, "/etc/taos", PATH_MAX);

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(global.configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default:%s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      global.dumpConfig = true;
    } else if (strcmp(argv[i], "-k") == 0) {
      global.generateGrant = true;
    } else if (strcmp(argv[i], "-A") == 0) {
      global.printAuth = true;
    } else if (strcmp(argv[i], "-V") == 0) {
      global.printVersion = true;
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
  char *versionStr = "enterprise";
#else
  char *versionStr = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", versionStr, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
  printf("gitinfoI: %s\n", gitinfoOfInternal);
  printf("builuInfo: %s\n", buildinfo);
}

int dmnReadConfig(const char *path) {
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

  if (taosReadGlobalCfg() != 0) {
    uError("failed to read global config");
    return -1;
  }

  if (taosCheckGlobalCfg() != 0) {
    uError("failed to check global config");
    return -1;
  }

  taosSetCoreDump(tsEnableCoreFile);
  return 0;
}

void dmnDumpConfig() { taosDumpGlobalCfg(); }

void dmnWaitSignal() {
  dmnSetSignalHandle();
  while (!global.stop) {
    taosMsleep(100);
  }
}

void dmnInitOption(SDnodeOpt *pOpt) {
  pOpt->sver = tsVersion;
  pOpt->numOfCores = tsNumOfCores;
  pOpt->statusInterval = tsStatusInterval;
  pOpt->serverPort = tsServerPort;
  tstrncpy(pOpt->localEp, tsLocalEp, TSDB_EP_LEN);
  tstrncpy(pOpt->localFqdn, tsLocalEp, TSDB_FQDN_LEN);
  tstrncpy(pOpt->timezone, tsLocalEp, TSDB_TIMEZONE_LEN);
  tstrncpy(pOpt->locale, tsLocalEp, TSDB_LOCALE_LEN);
  tstrncpy(pOpt->charset, tsLocalEp, TSDB_LOCALE_LEN);
}

int dmnRunDnode() {
  SDnodeOpt opt = {0};
  dmnInitOption(&opt);

  SDnode *pDnd = dndInit(&opt);
  if (pDnd == NULL) {
    uInfo("Failed to start TDengine, please check the log at %s", tsLogDir);
    return -1;
  }

  uInfo("Started TDengine service successfully.");
  dmnWaitSignal();
  uInfo("TDengine is shut down!");

  dndCleanup(pDnd);
  taosCloseLog();
  return 0;
}

int main(int argc, char const *argv[]) {
  if (dmnParseOpts(argc, argv) != 0) {
    return -1;
  }

  if (global.generateGrant) {
    dmnGenerateGrant();
    return 0;
  }

  if (global.printVersion) {
    dmnPrintVersion();
    return 0;
  }

  if (dmnReadConfig(global.configDir) != 0) {
    return -1;
  }

  if (global.dumpConfig) {
    dmnDumpConfig();
    return 0;
  }

  return dmnRunDnode();
}
