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
#include "cfgInt.h"

SConfig *cfgInit() {
  SConfig *pConfig = calloc(1, sizeof(SConfig));
  return pConfig;
}

int32_t cfgLoad(SConfig *pConfig, ECfgType cfgType, const char *sourceStr) {
  switch (cfgType) {
    case CFG_TYPE_TAOS_CFG:
      return cfgLoadFromTaosFile(pConfig, sourceStr);
    case CFG_TYPE_DOT_ENV:
      return cfgLoadFromDotEnvFile(pConfig, sourceStr);
    case CFG_TYPE_ENV_VAR:
      return cfgLoadFromGlobalEnvVariable(pConfig);
    case CFG_TYPE_APOLLO_URL:
      return cfgLoadFromApollUrl(pConfig, sourceStr);
    default:
      return -1;
  }
}

void cfgCleanup(SConfig *pConfig) { free(pConfig); }

int32_t cfgGetSize(SConfig *pConfig) { return 0; }

void *cfgIterate(SConfig *pConfig, void *p) { return NULL; }

void cfgCancelIterate(SConfig *pConfig, void *p);
