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
#include "trpc.h"
#include "tbalance.h"
#include "tglobal.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtMnode.h"
#include "mgmtDnode.h"
#include "mgmtVgroup.h"

#ifndef _SYNC

int32_t balanceInit() { return TSDB_CODE_SUCCESS; }
void    balanceCleanUp() {}
void    balanceNotify() {}
void    balanceUpdateMgmt() {}
void    balanceReset() {}

int32_t balanceAllocVnodes(SVgObj *pVgroup) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  SDnodeObj *pSelDnode = NULL;
  float      vnodeUsage = 1000.0;

  while (1) {
    pNode = mgmtGetNextDnode(pNode, &pDnode);
    if (pDnode == NULL) break;

    if (pDnode->totalVnodes > 0 && pDnode->openVnodes < pDnode->totalVnodes) {
      float openVnodes = pDnode->openVnodes;
      if (pDnode->isMgmt) openVnodes += tsMgmtEqualVnodeNum;

      float usage = openVnodes / pDnode->totalVnodes;
      if (usage <= vnodeUsage) {
        pSelDnode = pDnode;
        vnodeUsage = usage;
      }
    }
    mgmtDecDnodeRef(pDnode);
  }

  if (pSelDnode == NULL) {
    mError("failed to alloc vnode to vgroup");
    return TSDB_CODE_NO_ENOUGH_DNODES;
  }

  pVgroup->vnodeGid[0].dnodeId = pSelDnode->dnodeId;
  pVgroup->vnodeGid[0].pDnode = pSelDnode;

  mTrace("dnode:%d, alloc one vnode to vgroup, openVnodes:%d", pSelDnode->dnodeId, pSelDnode->openVnodes);
  return TSDB_CODE_SUCCESS;
}

#endif 
