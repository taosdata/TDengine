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
void    (*mgmtStartBalanceFp)(int32_t afterMs) = NULL;
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

void mgmtStartBalance(int32_t afterMs) {
  if (mgmtStartBalanceFp) {
    (*mgmtStartBalanceFp)(afterMs);
  }
}

int32_t mgmtAllocVnodes(SVgObj *pVgroup) {
  if (mgmtAllocVnodesFp) {
    return (*mgmtAllocVnodesFp)(pVgroup);
  }

  SDnodeObj *pDnode = mgmtGetDnode(1);
  if (pDnode == NULL) return TSDB_CODE_OTHERS;

  if (pDnode->openVnodes < pDnode->numOfTotalVnodes) {
    pVgroup->vnodeGid[0].dnodeId   = pDnode->dnodeId;
    pVgroup->vnodeGid[0].privateIp = pDnode->privateIp;
    pVgroup->vnodeGid[0].publicIp  = pDnode->publicIp;
    mTrace("dnode:%d, alloc one vnode to vgroup", pDnode->dnodeId);
    return TSDB_CODE_SUCCESS;
  } else {
    mError("dnode:%d, failed to alloc vnode to vgroup", pDnode->dnodeId);
    return TSDB_CODE_NO_ENOUGH_DNODES;
  }
}
