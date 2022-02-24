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
#include "tconfig.h"

SDnodeObjCfg dmnGetObjCfg() {
  SConfig *pCfg = taosGetCfg();
  SDnodeObjCfg objCfg = {0};

  objCfg.numOfSupportVnodes = cfgGetItem(pCfg, "supportVnodes")->i32;
  tstrncpy(objCfg.dataDir, cfgGetItem(pCfg, "dataDir")->str, sizeof(objCfg.dataDir));
  tstrncpy(objCfg.firstEp, cfgGetItem(pCfg, "firstEp")->str, sizeof(objCfg.firstEp));
  tstrncpy(objCfg.secondEp, cfgGetItem(pCfg, "secondEp")->str, sizeof(objCfg.firstEp));
  objCfg.serverPort = (uint16_t)cfgGetItem(pCfg, "serverPort")->i32;
  tstrncpy(objCfg.localFqdn, cfgGetItem(pCfg, "fqdn")->str, sizeof(objCfg.localFqdn));
  snprintf(objCfg.localEp, sizeof(objCfg.localEp), "%s:%u", objCfg.localFqdn, objCfg.serverPort);
  return objCfg;
}

void dmnDumpCfg() {
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfg(pCfg, 0, 1);
}