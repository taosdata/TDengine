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
#include "vnode.h"

int vnodeInitPeer(int numOfThreads) { return 0; }

void vnodeCleanUpPeer(int vnode) {}

int vnodeForwardToPeer(SMeterObj *pObj, char *cont, int contLen, char action, int sversion) { return 0; }

int vnodeRecoverFromPeer(SVnodeObj *pVnode, int fileId) { return -TSDB_CODE_FILE_CORRUPTED; }

void  vnodeCloseAllSyncFds(int vnode) {}

void vnodeBroadcastStatusToUnsyncedPeer(SVnodeObj *pVnode) {}

int vnodeOpenPeerVnode(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;
  pVnode->status = (pVnode->cfg.replications > 1) ? TSDB_STATUS_UNSYNCED : TSDB_STATUS_MASTER;
  dTrace("vid:%d, vnode status:%d numOfPeers:%d", vnode, pVnode->status, pVnode->cfg.replications-1);
  vnodeUpdateStreamRole(pVnode);
  return 0;
}

void vnodeClosePeerVnode(int vnode) {}

void vnodeConfigVPeers(int vnode, int numOfPeers, SVPeerDesc peerDesc[]) {}