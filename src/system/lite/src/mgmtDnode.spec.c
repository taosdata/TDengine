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
#include "mgmt.h"
#include "vnodeStatus.h"

SDnodeObj       dnodeObj;
extern uint32_t tsRebootTime;

SDnodeObj *mgmtGetDnode(uint32_t ip) { return &dnodeObj; }

int mgmtUpdateDnode(SDnodeObj *pDnode) { return 0; }

void mgmtCleanUpDnodes() {}

int mgmtInitDnodes() {
  dnodeObj.privateIp = inet_addr(tsPrivateIp);;
  dnodeObj.createdTime = (int64_t)tsRebootTime * 1000;
  dnodeObj.lastReboot = tsRebootTime;
  dnodeObj.numOfCores = (uint16_t)tsNumOfCores;
  dnodeObj.status = TSDB_DN_STATUS_READY;
  dnodeObj.alternativeRole = TSDB_DNODE_ROLE_ANY;
  dnodeObj.numOfTotalVnodes = tsNumOfTotalVnodes;
  dnodeObj.thandle = (void*)(1);  //hack way
  if (dnodeObj.numOfVnodes == TSDB_INVALID_VNODE_NUM) {
    mgmtSetDnodeMaxVnodes(&dnodeObj);
    mPrint("dnode first access, set total vnodes:%d", dnodeObj.numOfVnodes);
  }
  return  0;
}

int mgmtGetDnodesNum() { return 1; }

void *mgmtGetNextDnode(SShowObj *pShow, SDnodeObj **pDnode) {
  if (*pDnode == NULL) {
    *pDnode = &dnodeObj;
  } else {
    *pDnode = NULL;
  }

  return *pDnode;
}

int mgmtGetScoresMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) { return TSDB_CODE_OPS_NOT_SUPPORT; }

int mgmtRetrieveScores(SShowObj *pShow, char *data, int rows, SConnObj *pConn) { return 0; }

void mgmtSetDnodeUnRemove(SDnodeObj *pDnode) {}

bool mgmtCheckConfigShow(SGlobalConfig *cfg) {
  if (cfg->cfgType & TSDB_CFG_CTYPE_B_CLUSTER)
    return false;
  if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT)
    return false;
  return true;
}