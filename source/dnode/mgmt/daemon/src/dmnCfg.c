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

static void dmnInitEnvCfg(SDnodeEnvCfg *pCfg) {
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

static void dmnInitObjCfg(SDnodeObjCfg *pCfg) {
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

int32_t dnmInitCfg(SDnodeEnvCfg *pEnvCfg, SDnodeObjCfg *pObjCfg, const char *configFile, const char *envFile,
                   const char *apolloUrl) {
  dmnInitEnvCfg(pEnvCfg);
  dmnInitObjCfg(pObjCfg);
  return 0;
}