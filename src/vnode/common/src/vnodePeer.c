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
#include "vnodeStatus.h"

int vnodeInitPeer(int numOfThreads) { return 0; }

void vnodeCleanUpPeer(int vnode) {}

int vnodeForwardToPeer(SMeterObj *pObj, char *cont, int contLen, char action, int sversion) { return 0; }

int vnodeRecoverFromPeer(SVnodeObj *pVnode, int fileId) { return -TSDB_CODE_FILE_CORRUPTED; }

void  vnodeCloseAllSyncFds(int vnode) {}

void vnodeBroadcastStatusToUnsyncedPeer(SVnodeObj *pVnode) {}

int vnodeOpenPeerVnode(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;
  pVnode->vnodeStatus = (pVnode->cfg.replications > 1) ? TSDB_VN_STATUS_UNSYNCED : TSDB_VN_STATUS_MASTER;
  dPrint("vid:%d, status:%s numOfPeers:%d", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus), pVnode->cfg.replications - 1);
  vnodeUpdateStreamRole(pVnode);
  return 0;
}

void vnodeClosePeerVnode(int vnode) {}

void vnodeConfigVPeers(int vnode, int numOfPeers, SVPeerDesc peerDesc[]) {}