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
#include "dndMain.h"

void dndGenerateGrant() {
#if 0
  grantParseParameter();
#endif
}

void dndPrintVersion() {
#ifdef TD_ENTERPRISE
  char *releaseName = "enterprise";
#else
  char *releaseName = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", releaseName, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
  printf("builuInfo: %s\n", buildinfo);
}

void dndDumpCfg() {
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfg(pCfg, 0, 1);
}

SDnodeOpt dndGetOpt() {
  SConfig  *pCfg = taosGetCfg();
  SDnodeOpt option = {0};

  option.numOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  tstrncpy(option.dataDir, tsDataDir, sizeof(option.dataDir));
  tstrncpy(option.firstEp, tsFirst, sizeof(option.firstEp));
  tstrncpy(option.secondEp, tsSecond, sizeof(option.firstEp));
  option.serverPort = tsServerPort;
  tstrncpy(option.localFqdn, tsLocalFqdn, sizeof(option.localFqdn));
  snprintf(option.localEp, sizeof(option.localEp), "%s:%u", option.localFqdn, option.serverPort);
  option.pDisks = tsDiskCfg;
  option.numOfDisks = tsDiskCfgNum;
  return option;
}
