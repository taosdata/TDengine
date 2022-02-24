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
#include "tlocale.h"

static int32_t dmnAddEpCfg(SConfig *pCfg) {
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

static int32_t dmnAddDirCfg(SConfig *pCfg) {
  if (cfgAddDir(pCfg, "dataDir", osDataDir()) != 0) return -1;
  if (cfgAddDir(pCfg, "tempDir", osTempDir()) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalDataDirGB", 2.0f, 0.001f, 10000000) != 0) return -1;
  if (cfgAddFloat(pCfg, "minimalTempDirGB", 1.0f, 0.001f, 10000000) != 0) return -1;
  return 0;
}

static int32_t dmnCheckDirCfg(SConfig *pCfg) {
  SEnvVar *pEnv = osEnv();
  tstrncpy(pEnv->dataDir, cfgGetItem(pCfg, "dataDir")->str, PATH_MAX);
  tstrncpy(pEnv->tempDir, cfgGetItem(pCfg, "tempDir")->str, PATH_MAX);
  pEnv->dataSpace.reserved = cfgGetItem(pCfg, "minimalDataDirGB")->fval;
  pEnv->tempSpace.reserved = cfgGetItem(pCfg, "minimalTempDirGB")->fval;
  return 0;
}

static int32_t dmnAddVersionCfg(SConfig *pCfg) {
  if (cfgAddString(pCfg, "buildinfo", buildinfo) != 0) return -1;
  if (cfgAddString(pCfg, "gitinfo", gitinfo) != 0) return -1;
  if (cfgAddString(pCfg, "version", version) != 0) return -1;
  return 0;
}

static int32_t dmnAddDnodeCfg(SConfig *pCfg) {
  if (dmnAddEpCfg(pCfg) != 0) return -1;
  if (dmnAddDirCfg(pCfg) != 0) return -1;
  if (dmnAddVersionCfg(pCfg) != 0) return -1;

  if (cfgAddTimezone(pCfg, "timezone", "") != 0) return -1;
  if (cfgAddLocale(pCfg, "locale", "") != 0) return -1;
  if (cfgAddCharset(pCfg, "charset", "") != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfCores", 2, 1, 100000) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfCommitThreads", 4, 1, 1000) != 0) return -1;
  if (cfgAddBool(pCfg, "telemetryReporting", 0) != 0) return -1;
  if (cfgAddBool(pCfg, "enableCoreFile", 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "supportVnodes", 256, 0, 65536) != 0) return -1;
  if (cfgAddInt32(pCfg, "statusInterval", 1, 1, 30) != 0) return -1;
  if (cfgAddFloat(pCfg, "numOfThreadsPerCore", 1, 0, 10) != 0) return -1;
  if (cfgAddFloat(pCfg, "ratioOfQueryCores", 1, 0, 5) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxShellConns", 50000, 10, 50000000) != 0) return -1;
  if (cfgAddInt32(pCfg, "shellActivityTimer", 3, 1, 120) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcTimer", 300, 100, 3000) != 0) return -1;
  if (cfgAddInt32(pCfg, "rpcMaxTime", 600, 100, 7200) != 0) return -1;
  
  return 0;
}

static void dmnSetDnodeCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "timezone");
  osSetTimezone(pItem->str);
  uDebug("timezone format changed from %s to %s", pItem->str, osTimezone());
  cfgSetItem(pCfg, "timezone", osTimezone(), pItem->stype);
}

static int32_t dmnCheckCfg(SConfig *pCfg) {
  bool enableCore = cfgGetItem(pCfg, "enableCoreFile")->bval;
  taosSetCoreDump(enableCore);

  dmnSetDnodeCfg(pCfg);

  if (dmnCheckDirCfg(pCfg) != 0) {
    return -1;
  }

  taosGetSystemInfo();

  tsSetLocale();

  if (tsNumOfCores <= 0) {
    tsNumOfCores = 1;
  }

  if (tsQueryBufferSize >= 0) {
    tsQueryBufferSizeBytes = tsQueryBufferSize * 1048576UL;
  }

  return 0;
}

SConfig *dmnReadCfg(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return NULL;

  if (dmnAddLogCfg(pCfg) != 0) {
    uError("failed to add log cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (dmnAddDnodeCfg(pCfg) != 0) {
    uError("failed to init dnode cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (dmnLoadCfg(pCfg, cfgDir, envFile, apolloUrl) != 0) {
    uError("failed to load cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (dmnCheckCfg(pCfg) != 0) {
    uError("failed to check cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  cfgDumpCfg(pCfg);
  return pCfg;
}

void dmnDumpCfg(SConfig *pCfg) {
  printf("taos global config:\n");
  printf("==================================\n");

  SConfigItem *pItem = cfgIterate(pCfg, NULL);
  while (pItem != NULL) {
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        printf("cfg:%s, value:%u src:%s\n", pItem->name, pItem->bval, cfgStypeStr(pItem->stype));
        break;
      case CFG_DTYPE_INT32:
        printf("cfg:%s, value:%d src:%s\n", pItem->name, pItem->i32, cfgStypeStr(pItem->stype));
        break;
      case CFG_DTYPE_INT64:
        printf("cfg:%s, value:%" PRId64 " src:%s\n", pItem->name, pItem->i64, cfgStypeStr(pItem->stype));
        break;
      case CFG_DTYPE_FLOAT:
        printf("cfg:%s, value:%f src:%s\n", pItem->name, pItem->fval, cfgStypeStr(pItem->stype));
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_IPSTR:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE:
        printf("cfg:%s, value:%s src:%s\n", pItem->name, pItem->str, cfgStypeStr(pItem->stype));
        break;
    }
    pItem = cfgIterate(pCfg, pItem);
  }
}

SDnodeEnvCfg dmnGetEnvCfg(SConfig *pCfg) {
  SDnodeEnvCfg envCfg = {0};

  const char *vstr = cfgGetItem(pCfg, "version")->str;
  envCfg.sver = 30000000;
  tstrncpy(envCfg.buildinfo, cfgGetItem(pCfg, "buildinfo")->str, sizeof(envCfg.buildinfo));
  tstrncpy(envCfg.gitinfo, cfgGetItem(pCfg, "gitinfo")->str, sizeof(envCfg.gitinfo));
  tstrncpy(envCfg.timezone, cfgGetItem(pCfg, "timezone")->str, sizeof(envCfg.timezone));
  tstrncpy(envCfg.locale, cfgGetItem(pCfg, "locale")->str, sizeof(envCfg.locale));
  tstrncpy(envCfg.charset, cfgGetItem(pCfg, "charset")->str, sizeof(envCfg.charset));
  envCfg.numOfCores = cfgGetItem(pCfg, "numOfCores")->i32;
  envCfg.numOfCommitThreads = (uint16_t)cfgGetItem(pCfg, "numOfCommitThreads")->i32;
  envCfg.enableTelem = cfgGetItem(pCfg, "telemetryReporting")->bval;
  envCfg.rpcMaxTime = cfgGetItem(pCfg, "rpcMaxTime")->i32;
  envCfg.rpcTimer = cfgGetItem(pCfg, "rpcTimer")->i32;

  return envCfg;
}

SDnodeObjCfg dmnGetObjCfg(SConfig *pCfg) {
  SDnodeObjCfg objCfg = {0};

  objCfg.numOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  objCfg.statusInterval = cfgGetItem(pCfg, "statusInterval")->i32;
  objCfg.numOfThreadsPerCore = cfgGetItem(pCfg, "numOfThreadsPerCore")->fval;
  objCfg.ratioOfQueryCores = cfgGetItem(pCfg, "ratioOfQueryCores")->fval;
  objCfg.maxShellConns = cfgGetItem(pCfg, "maxShellConns")->i32;
  objCfg.shellActivityTimer = cfgGetItem(pCfg, "shellActivityTimer")->i32;
  tstrncpy(objCfg.dataDir, cfgGetItem(pCfg, "dataDir")->str, sizeof(objCfg.dataDir));

  tstrncpy(objCfg.firstEp, cfgGetItem(pCfg, "firstEp")->str, sizeof(objCfg.firstEp));
  tstrncpy(objCfg.secondEp, cfgGetItem(pCfg, "secondEp")->str, sizeof(objCfg.firstEp));
  objCfg.serverPort = (uint16_t)cfgGetItem(pCfg, "serverPort")->i32;
  tstrncpy(objCfg.localFqdn, cfgGetItem(pCfg, "fqdn")->str, sizeof(objCfg.localFqdn));
  snprintf(objCfg.localEp, sizeof(objCfg.localEp), "%s:%u", objCfg.localFqdn, objCfg.serverPort);
  return objCfg;
}