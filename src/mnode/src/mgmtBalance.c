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
#include "mgmtBalance.h"
#include "mgmtDnode.h"
#include "dnodeModule.h"
#include "tstatus.h"
#include "tglobalcfg.h"
#include "ttime.h"

void mgmtStartBalanceTimerImp(int64_t mseconds) {}
void (*mgmtStartBalanceTimer)(int64_t mseconds) = mgmtStartBalanceTimerImp;

int32_t mgmtInitBalanceImp() { return 0; }
int32_t (*mgmtInitBalance)() = mgmtInitBalanceImp;

void mgmtCleanupBalanceImp() {}
void (*mgmtCleanupBalance)() = mgmtCleanupBalanceImp;

int32_t mgmtAllocVnodesImp(SVgObj *pVgroup) {
  int        selectedVnode = -1;
  SDnodeObj *pDnode = &dnodeObj;
  int        lastAllocVode = pDnode->lastAllocVnode;

  for (int i = 0; i < pDnode->numOfVnodes; i++) {
    int vnode = (i + lastAllocVode) % pDnode->numOfVnodes;
    if (pDnode->vload[vnode].vgId == 0 && pDnode->vload[vnode].status == TSDB_VN_STATUS_OFFLINE) {
      selectedVnode = vnode;
      break;
    }
  }

  if (selectedVnode == -1) {
    mError("vgroup:%d alloc vnode failed, free vnodes:%d", pVgroup->vgId, pDnode->numOfFreeVnodes);
    return -1;
  } else {
    mTrace("vgroup:%d allocate vnode:%d, last allocated vnode:%d", pVgroup->vgId, selectedVnode, lastAllocVode);
    pVgroup->vnodeGid[0].vnode = selectedVnode;
    pDnode->lastAllocVnode = selectedVnode + 1;
    if (pDnode->lastAllocVnode >= pDnode->numOfVnodes) pDnode->lastAllocVnode = 0;
    return 0;
  }
}
int32_t (*mgmtAllocVnodes)(SVgObj *pVgroup) = mgmtAllocVnodesImp;

bool mgmtCheckModuleInDnodeImp(SDnodeObj *pDnode, int moduleType) {
  return tsModule[moduleType].num != 0;
}
bool (*mgmtCheckModuleInDnode)(SDnodeObj *pDnode, int moduleType) = mgmtCheckModuleInDnodeImp;

char *mgmtGetVnodeStatusImp(SVgObj *pVgroup, SVnodeGid *pVnode) { return "master"; }
char *(*mgmtGetVnodeStatus)(SVgObj *pVgroup, SVnodeGid *pVnode) = mgmtGetVnodeStatusImp;

bool mgmtCheckVnodeReadyImp(SDnodeObj *pDnode, SVgObj *pVgroup, SVnodeGid *pVnode) { return true; }
bool (*mgmtCheckVnodeReady)(SDnodeObj *pDnode, SVgObj *pVgroup, SVnodeGid *pVnode) = mgmtCheckVnodeReadyImp;


void mgmtUpdateDnodeStateImp(SDnodeObj *pDnode, int lbStatus) {}
void (*mgmtUpdateDnodeState)(SDnodeObj *pDnode, int lbStatus) = mgmtUpdateDnodeStateImp;

void mgmtUpdateVgroupStateImp(SVgObj *pVgroup, int lbStatus, int srcIp) {}
void (*mgmtUpdateVgroupState)(SVgObj *pVgroup, int lbStatus, int srcIp) = mgmtUpdateVgroupStateImp;

bool mgmtAddVnodeImp(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode) { return false; }
bool (*mgmtAddVnode)(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode) = mgmtAddVnodeImp;

