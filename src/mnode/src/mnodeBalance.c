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
#include "os.h"
#include "tglobal.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDnode.h"
#include "mnodeSdb.h"

#ifndef _SYNC

int32_t balanceInit() { return TSDB_CODE_SUCCESS; }
void    balanceCleanUp() {}
void    balanceAsyncNotify() {}
void    balanceSyncNotify() {}
void    balanceReset() {}
int32_t balanceAlterDnode(struct SDnodeObj *pDnode, int32_t vnodeId, int32_t dnodeId) { return TSDB_CODE_SYN_NOT_ENABLED; }

int32_t balanceAllocVnodes(SVgObj *pVgroup) {
  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;
  SDnodeObj *pSelDnode = NULL;
  float      vnodeUsage = 1000.0;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;

    if (pDnode->numOfCores > 0 && pDnode->openVnodes < TSDB_MAX_VNODES) {
      float openVnodes = pDnode->openVnodes;
      if (pDnode->isMgmt) openVnodes += tsMnodeEqualVnodeNum;

      float usage = openVnodes / pDnode->numOfCores;
      if (usage <= vnodeUsage) {
        pSelDnode = pDnode;
        vnodeUsage = usage;
      }
    }
    mnodeDecDnodeRef(pDnode);
  }

  sdbFreeIter(pIter);

  if (pSelDnode == NULL) {
    mError("failed to alloc vnode to vgroup");
    return TSDB_CODE_MND_NO_ENOUGH_DNODES;
  }

  pVgroup->vnodeGid[0].dnodeId = pSelDnode->dnodeId;
  pVgroup->vnodeGid[0].pDnode = pSelDnode;

  mDebug("dnode:%d, alloc one vnode to vgroup, openVnodes:%d", pSelDnode->dnodeId, pSelDnode->openVnodes);
  return TSDB_CODE_SUCCESS;
}

#endif 
