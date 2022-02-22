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

#if 0
void taosReadGlobalLogCfg() {
  FILE * fp;
  char * line, *option, *value;
  int    olen, vlen;
  char   fileName[PATH_MAX] = {0};

  taosExpandDir(configDir, configDir, PATH_MAX);
  taosReadLogOption("logDir", tsLogDir);
  
  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    printf("\nconfig file:%s not found, all variables are set to default\n", fileName);
    return;
  }

  ssize_t _bytes = 0;
  size_t len = 1024;
  line = calloc(1, len);
  
  while (!feof(fp)) {
    memset(line, 0, len);
    
    option = value = NULL;
    olen = vlen = 0;

    _bytes = tgetline(&line, &len, fp);
    if (_bytes < 0)
    {
      break;
    }

    line[len - 1] = 0;

    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    taosReadLogOption(option, value);
  }

  tfree(line);
  fclose(fp);
}


void taosPrintCfg() {
  uInfo("   taos config & system info:");
  uInfo("==================================");

  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (tscEmbeddedInUtil == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    
    int optionLen = (int)strlen(cfg->option);
    int blankLen = TSDB_CFG_PRINT_LEN - optionLen;
    blankLen = blankLen < 0 ? 0 : blankLen;

    char blank[TSDB_CFG_PRINT_LEN];
    memset(blank, ' ', TSDB_CFG_PRINT_LEN);
    blank[blankLen] = 0;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT8:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((int8_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT16:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((int16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT32:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((int32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_UINT16:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((uint16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        uInfo(" %s:%s%f%s", cfg->option, blank, *((float *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_DOUBLE:
        uInfo(" %s:%s%f%s", cfg->option, blank, *((double *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        uInfo(" %s:%s%s%s", cfg->option, blank, (char *)cfg->ptr, tsGlobalUnit[cfg->unitType]);
        break;
      default:
        break;
    }
  }

  taosPrintOsInfo();
  uInfo("==================================");
}

#if 0
static void taosDumpCfg(SGlobalCfg *cfg) {
    int optionLen = (int)strlen(cfg->option);
    int blankLen = TSDB_CFG_PRINT_LEN - optionLen;
    blankLen = blankLen < 0 ? 0 : blankLen;

    char blank[TSDB_CFG_PRINT_LEN];
    memset(blank, ' ', TSDB_CFG_PRINT_LEN);
    blank[blankLen] = 0;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT8:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((int8_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT16:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((int16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT32:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((int32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_UINT16:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((uint16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        printf(" %s:%s%f%s\n", cfg->option, blank, *((float *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        printf(" %s:%s%s%s\n", cfg->option, blank, (char *)cfg->ptr, tsGlobalUnit[cfg->unitType]);
        break;
      default:
        break;
    }
}

void taosDumpGlobalCfg() {
  printf("taos global config:\n");
  printf("==================================\n");
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (tscEmbeddedInUtil == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW)) continue;

    taosDumpCfg(cfg);
  }

  printf("\ntaos local config:\n");
  printf("==================================\n");

  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (tscEmbeddedInUtil == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW) continue;

    taosDumpCfg(cfg);
  }
}

#endif

#endif

static int32_t dmnInitLog() {

}

int32_t dnmInitCfg(SDnodeEnvCfg *pEnvCfg, SDnodeObjCfg *pObjCfg, const char *configFile, const char *envFile,
                   const char *apolloUrl) {
  dmnInitEnvCfg(pEnvCfg);
  dmnInitObjCfg(pObjCfg);
  return 0;
}