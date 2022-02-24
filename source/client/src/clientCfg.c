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
#include "clientInt.h"
#include "ulog.h"


int32_t tscCheckCfg(SConfig *pCfg) {
  bool enableCore = cfgGetItem(pCfg, "enableCoreFile")->bval;
  taosSetCoreDump(enableCore);

  return 0;
}

SConfig *tscInitCfgImp(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  SConfig *pCfg = cfgInit();
  if (pCfg == NULL) return NULL;

  if (tscAddCfg(pCfg) != 0) {
    uError("failed to init tsc cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (tscLoadCfg(pCfg, cfgDir, envFile, apolloUrl) != 0) {
    printf("failed to load tsc cfg since %s\n", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  if (tscCheckCfg(pCfg) != 0) {
    uError("failed to check cfg since %s", terrstr());
    cfgCleanup(pCfg);
    return NULL;
  }

  cfgDumpCfg(pCfg);
  return pCfg;
}

int32_t tscInitCfg(const char *cfgDir, const char *envFile, const char *apolloUrl) {
  tscCfg = tscInitCfgImp(cfgDir, envFile, apolloUrl);
  if (tscCfg == NULL) return -1;

  return 0;
}