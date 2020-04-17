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
#include "treplica.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtMnode.h"
#include "mgmtDnode.h"
#include "mgmtVgroup.h"

#ifndef _SYNC

int32_t replicaInit() { return TSDB_CODE_SUCCESS; }
void    replicaCleanUp() {}
void    replicaNotify() {}
void    replicaReset() {}
int32_t replicaForwardReqToPeer(void *pHead) { return TSDB_CODE_SUCCESS; }

int32_t replicaAllocVnodes(SVgObj *pVgroup) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  SDnodeObj *pSelDnode = NULL;
  float      vnodeUsage = 1.0;

  while (1) {
    pNode = mgmtGetNextDnode(pNode, &pDnode);
    if (pDnode == NULL) break;

    if (pDnode->totalVnodes > 0 && pDnode->openVnodes < pDnode->totalVnodes) {
      float usage = (float)pDnode->openVnodes / pDnode->totalVnodes;
      if (usage <= vnodeUsage) {
        pSelDnode = pDnode;
        vnodeUsage = usage;
      }
    }
    mgmtReleaseDnode(pDnode);
  }

  if (pSelDnode == NULL) {
    mError("failed to alloc vnode to vgroup");
    return TSDB_CODE_NO_ENOUGH_DNODES;
  }

  pVgroup->vnodeGid[0].dnodeId = pSelDnode->dnodeId;
  pVgroup->vnodeGid[0].privateIp = pSelDnode->privateIp;
  pVgroup->vnodeGid[0].publicIp = pSelDnode->publicIp;

  mTrace("dnode:%d, alloc one vnode to vgroup, openVnodes:%d", pSelDnode->dnodeId, pSelDnode->openVnodes);
  return TSDB_CODE_SUCCESS;
}

#endif 
