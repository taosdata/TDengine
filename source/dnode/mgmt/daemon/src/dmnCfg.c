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

static int32_t dmnAddDnodeCfg(SConfig *pCfg) {
  if (cfgAddString(pCfg, "buildinfo", buildinfo) != 0) return -1;
  if (cfgAddString(pCfg, "gitinfo", gitinfo) != 0) return -1;
  if (cfgAddString(pCfg, "version", version) != 0) return -1;
  if (cfgAddString(pCfg, "firstEp", "") != 0) return -1;
  if (cfgAddString(pCfg, "secondEp", "") != 0) return -1;
  if (cfgAddString(pCfg, "fqdn", "") != 0) return -1;
  if (cfgAddInt32(pCfg, "serverPort", 6030, 1, 65056) != 0) return -1;
  if (cfgAddDir(pCfg, "dataDir", tsDataDir) != 0) return -1;
  if (cfgAddTimezone(pCfg, "timezone", "") != 0) return -1;
  if (cfgAddLocale(pCfg, "locale", "") != 0) return -1;
  if (cfgAddCharset(pCfg, "charset", "") != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfCores", 1, 1, 100000) != 0) return -1;
  if (cfgAddInt32(pCfg, "numOfCommitThreads", 4, 1, 1000) != 0) return -1;
  if (cfgAddBool(pCfg, "telemetryReporting", 0) != 0) return -1;
  if (cfgAddBool(pCfg, "enableCoreFile", 0) != 0) return -1;
  if (cfgAddInt32(pCfg, "supportVnodes", 256, 0, 65536) != 0) return -1;
  if (cfgAddInt32(pCfg, "statusInterval", 1, 1, 30) != 0) return -1;
  if (cfgAddFloat(pCfg, "numOfThreadsPerCore", 1, 0, 10) != 0) return -1;
  if (cfgAddFloat(pCfg, "ratioOfQueryCores", 1, 0, 5) != 0) return -1;
  if (cfgAddInt32(pCfg, "maxShellConns", 50000, 10, 50000000) != 0) return -1;
  if (cfgAddInt32(pCfg, "shellActivityTimer", 3, 1, 120) != 0) return -1;
  return 0;
}

int32_t dmnCheckCfg(SConfig *pCfg) {
  bool enableCore = cfgGetItem(pCfg, "enableCoreFile")->bval;
  taosSetCoreDump(enableCore);

  

  return 0;
}

SConfig *dmnReadCfg(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return NULL;

  if (dmnAddLogCfg(pCfg) != 0) {
    printf("failed to add log cfg since %s\n", terrstr());
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
  }

  if (taosCheckAndPrintCfg() != 0) {
    uError("failed to check config");
    return NULL;
  }

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
  objCfg.serverPort = (uint16_t)cfgGetItem(pCfg, "serverPort")->i32;
  tstrncpy(objCfg.dataDir, cfgGetItem(pCfg, "dataDir")->str, sizeof(objCfg.dataDir));
  tstrncpy(objCfg.localEp, cfgGetItem(pCfg, "localEp")->str, sizeof(objCfg.localEp));
  tstrncpy(objCfg.localFqdn, cfgGetItem(pCfg, "localFqdn")->str, sizeof(objCfg.localFqdn, cfgGetItem));
  tstrncpy(objCfg.firstEp, cfgGetItem(pCfg, "firstEp")->str, sizeof(objCfg.firstEp));
  tstrncpy(objCfg.secondEp, cfgGetItem(pCfg, "secondEp")->str, sizeof(objCfg.firstEp));

  return objCfg;
}