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
#include "tstatus.h"
#include "mgmtBalance.h"
#include "mgmtDnode.h"

int32_t (*mgmtInitBalanceFp)() = NULL;
void    (*mgmtCleanupBalanceFp)() = NULL;
int32_t (*mgmtAllocVnodesFp)(SVgObj *pVgroup) = NULL;

int32_t mgmtInitBalance() {
  if (mgmtInitBalanceFp) {
    return (*mgmtInitBalanceFp)();
  } else {
    return 0;
  }
}

void mgmtCleanupBalance() {
  if (mgmtCleanupBalanceFp) {
    (*mgmtCleanupBalanceFp)();
  }
}

int32_t mgmtAllocVnodes(SVgObj *pVgroup) {
  if (mgmtAllocVnodesFp) {
    return mgmtAllocVnodesFp(pVgroup);
  }

  SDnodeObj *pDnode = mgmtGetDnode(0);
  if (pDnode == NULL) return TSDB_CODE_OTHERS;

  int32_t selectedVnode = -1;
  int32_t lastAllocVode = pDnode->lastAllocVnode;

  for (int32_t i = 0; i < pDnode->numOfVnodes; i++) {
    int32_t vnode = (i + lastAllocVode) % pDnode->numOfVnodes;
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
    pDnode->lastAllocVnode     = selectedVnode + 1;
    if (pDnode->lastAllocVnode >= pDnode->numOfVnodes) pDnode->lastAllocVnode = 0;
    return 0;
  }
}
