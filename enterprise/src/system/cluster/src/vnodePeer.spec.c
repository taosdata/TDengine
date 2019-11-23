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
#include <endian.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netdb.h>
#include <sys/epoll.h>

#include "vnode.h"
#include "vnodeUtil.h"
#include "vnodePeer.h"
#include "trpc.h"
#include "dnodeSystem.h"
#include "tstatus.h"

uint32_t    tsPrivateIp4;
int         tsSyncNum;    // number of sync in process in whole system
SThreadPool tsPeerThreadPool;

int   vnodeProcessSyncRequest(char *pMsg, SVnodePeer *pVPeer);
void *vnodeSyncRetrieveData(void *param);
void  vnodeSyncWithPeer(void *, void *);
void  vnodeCheckPeerConnection(void *param, void *tmrId);
void *vnodeAcceptPeerTcpConnection(void *argv);
void  vnodeRestartConnection(SVnodePeer *pVPeer);
int   vnodeSendStatusMsgToPeer(SVnodePeer *pVPeer, char ack);

int   vnodeOpenThreadPool(SThreadPool *pPool, int numOfThreads);
void  vnodeCloseThreadPool(SThreadPool *pPool);
int   vnodeAddPeerFd(SThreadPool *pPool, SVnodePeer *pVPeer, int connFd);
void  vnodeClosePeerFd(SVnodePeer *pVPeer);
void  vnodeBroadcastStatus(SVnodeObj *pVnode);

void taosBlockSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    pError("failed to block SIGPIPE");
  }
}

int vnodeOpenPeerVnode(int vnode) 
{
  SVnodePeer    *pVPeer;
  STranQueue    *pQueue;;
  SVnodeObj     *pVnode;

  tsPrivateIp4 = inet_addr(tsPrivateIp);
  pVnode = vnodeList + vnode;

  pVnode->pQueue = malloc(sizeof(STranQueue));
  memset(pVnode->pQueue, 0, sizeof(STranQueue));
  pQueue = (STranQueue *) pVnode->pQueue;
  pthread_mutex_init ( &(pQueue->qmutex), NULL);

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = (SVnodePeer *) calloc(1, sizeof(SVnodePeer));
    pVPeer->signature = pVPeer;
    pVPeer->ip = pVnode->vpeers[i].ip;
    tinet_ntoa(pVPeer->ipstr, pVPeer->ip);
    pVPeer->vid = pVnode->vpeers[i].vnode;
    pVPeer->ownId = vnode;
    pVPeer->status = TSDB_VN_STATUS_OFFLINE;
    pVPeer->syncFd = -1;
    pVPeer->peerFd = -1;
    pVnode->peerInfo[i] = pVPeer;
    if (pVPeer->ip == tsPrivateIp4) {
      pVnode->selfIndex = i;
    }
  }

  pVnode->vnodeStatus = (pVnode->cfg.replications > 1) ? TSDB_VN_STATUS_UNSYNCED : TSDB_VN_STATUS_MASTER;
  dPrint("vid:%d, open peers, status:%s numOfPeers:%d",
          vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus), pVnode->cfg.replications - 1);

  vnodeUpdateStreamRole(pVnode);

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = pVnode->peerInfo[i];
    if (pVPeer->ip) {
      dPrint("vid:%d, peer:%s:%d is configured by open msg", vnode, pVPeer->ipstr, pVPeer->vid);
      if (tsPrivateIp4 < pVPeer->ip) {
        dTrace("vid:%d, peer:%s:%d start check peer:%p connection", vnode, pVPeer->ipstr, pVPeer->vid, pVPeer);
        taosTmrReset(vnodeCheckPeerConnection, 0, pVPeer, vnodeTmrCtrl, &pVPeer->hbTimer);
      }
    }
  }

  return 0;
}

void vnodeClosePeerVnode(int vnode) 
{
  SVnodePeer *pVPeer;
  STranQueue *pQueue;
  SVnodeObj  *pVnode = vnodeList + vnode;

  dPrint("vid:%d, close vpeer", vnode);

  pthread_mutex_lock (&pVnode->vmutex);

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = pVnode->peerInfo[i];
    if (pVPeer == NULL) continue;
    taosTmrStopA(&pVPeer->hbTimer);
    taosTmrStopA(&pVPeer->syncTimer);
    pVPeer->ip = 0;
    if (pVPeer->syncFd >= 0) tclose(pVPeer->syncFd);
    if (pVPeer->peerFd >= 0) vnodeClosePeerFd(pVPeer);
    // do not free if connection is there
    if (pVPeer->peerFd < 0) {
      pVPeer->signature = NULL;
      tfree(pVPeer);
    }
  }

  pQueue = (STranQueue *) pVnode->pQueue;
  if (pQueue) {
    pthread_mutex_destroy(&(pQueue->qmutex));
    tfree(pQueue->buffer);
    tfree(pQueue);
    pVnode->pQueue = NULL;
  }

  pVnode->vnodeStatus = TSDB_VN_STATUS_OFFLINE;

  pthread_mutex_unlock (&pVnode->vmutex);
}

int vnodeInitPeer(int numOfThreads)
{
  int code = 0;
  
  code = vnodeOpenThreadPool(&tsPeerThreadPool, numOfThreads);

  return code;
}

void vnodeCleanUpPeer() 
{
  vnodeCloseThreadPool(&tsPeerThreadPool);

  for (int vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode)
    vnodeClosePeerVnode(vnode);
}

int vnodeRemoveOneVPeer(SVnodePeer *pVPeer)
{
  if (pVPeer == NULL) {
    dError("failed to remove peer for peer is null");
    return 0;
  }

  if (pVPeer->ip == 0) {
    dError("failed to remove peer:%p for ip is 0", pVPeer);
    return 0;
  }

  dPrint("vid:%d, peer:%s:%d is removed", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);

  pVPeer->ip = 0;
  taosTmrStopA(&pVPeer->hbTimer);
  if (pVPeer->syncFd >= 0) tclose (pVPeer->syncFd);
  if (pVPeer->peerFd >= 0) vnodeClosePeerFd(pVPeer);

  // if connection is there, dont free. It will be freed when read error happen
  // if ( pVPeer->peerFd < 0 ) { pVPeer->signature = NULL; tfree(pVPeer); }; 
  if (pVPeer->peerFd < 0) { pVPeer->signature = NULL; };

  return 0;
}

void vnodeConfigVPeers(int vnode, int numOfPeers, SVPeerDesc peerDesc[])
{
  SVnodeObj  *pVnode = vnodeList + vnode;
  int         i, j;

  if (vnodeList[vnode].vnodeStatus == TSDB_VN_STATUS_CREATING) {
    dPrint("vid:%d, vnode is still under creating", vnode);
    return;
  }

  dPrint("vid:%d, config vpeer, status:%s numOfPeers:%d", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus), numOfPeers);

  pthread_mutex_lock (&dmutex);
  pthread_mutex_lock (&(pVnode->vmutex));

  for (i = 0; i < pVnode->cfg.replications; ++i) {
    for (j = 0; j < numOfPeers; ++j) {
      if (pVnode->peerInfo[i] && pVnode->peerInfo[i]->ip == peerDesc[j].ip &&
          pVnode->peerInfo[i]->vid == peerDesc[j].vnode)
        break;
    }

    if (j >= numOfPeers) {
      vnodeRemoveOneVPeer(pVnode->peerInfo[i]);
      pVnode->peerInfo[i] = NULL;
    }
  }

  SVnodePeer *newPeers[10];
  for (i = 0; i < numOfPeers; ++i) {
    for (j = 0; j < pVnode->cfg.replications; ++j) {
      if (pVnode->peerInfo[j] && pVnode->peerInfo[j]->ip == peerDesc[i].ip &&
          pVnode->peerInfo[j]->vid == peerDesc[i].vnode)
        break;
    }

    if (j >= pVnode->cfg.replications) {
      // add a new peer
      SVnodePeer *pVPeer = (SVnodePeer *) calloc(1, sizeof(SVnodePeer));
      pVPeer->signature = pVPeer;
      pVPeer->ip = peerDesc[i].ip;
      tinet_ntoa(pVPeer->ipstr, pVPeer->ip);
      pVPeer->vid = peerDesc[i].vnode;
      pVPeer->ownId = vnode;
      pVPeer->peerFd = -1;
      pVPeer->syncFd = -1;
      pVPeer->status = TSDB_VN_STATUS_OFFLINE;
      dPrint("vid:%d, peer:%s:%d is configured by config msg", vnode, pVPeer->ipstr, pVPeer->vid);
      if (pVPeer->ip > tsPrivateIp4) {
        dTrace("vid:%d, peer:%s:%d start check peer:%p connection", vnode, pVPeer->ipstr, pVPeer->vid, pVPeer);
        taosTmrReset(vnodeCheckPeerConnection, 0, pVPeer, vnodeTmrCtrl, &pVPeer->hbTimer);
      }
      newPeers[i] = pVPeer;
    } else {
      newPeers[i] = pVnode->peerInfo[j];
    }

    if (peerDesc[i].ip == tsPrivateIp4) pVnode->selfIndex = i;
  }

  memcpy(pVnode->peerInfo, newPeers, sizeof(SVnodePeer *) * numOfPeers);
  for (i = numOfPeers; i < TSDB_VNODES_SUPPORT; ++i)
    pVnode->peerInfo[i] = NULL;

  if (numOfPeers <= 1) {
    dPrint("vid:%d, no peers are configured, work as master!", vnode);
    pVnode->vnodeStatus = TSDB_VN_STATUS_MASTER;
  }

  pVnode->cfg.replications = numOfPeers;
  pthread_mutex_unlock (&(pVnode->vmutex));

  vnodeBroadcastStatus(pVnode);

  pthread_mutex_unlock (&dmutex);

  vnodeUpdateStreamRole(pVnode);
}

int vnodeCleanUpVPeers(int vnode) 
{ 
  SVnodeObj *pVnode = vnodeList + vnode;

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    vnodeRemoveOneVPeer(pVnode->peerInfo[i]);
    pVnode->peerInfo[i] = NULL;
  }
 
  return 0;
}

void vnodeBroadcastStatus(SVnodeObj *pVnode)
{
  SVnodePeer *pVPeer;

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = pVnode->peerInfo[i];
    if (pVPeer == NULL || pVPeer->ip == 0) continue;
    if (pVPeer->peerFd >= 0)
      vnodeSendStatusMsgToPeer(pVPeer, 1);
  }
} 

void vnodeBroadcastStatusToUnsyncedPeer(SVnodeObj *pVnode)
{
  SVnodePeer *pVPeer;

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = pVnode->peerInfo[i];
    if (pVPeer == NULL || pVPeer->ip == 0) continue;
    if ((pVPeer->peerFd >= 0) && (pVPeer->status == TSDB_VN_STATUS_UNSYNCED))
      vnodeSendStatusMsgToPeer(pVPeer, 1);
  }
}

void vnodeChooseMaster(SVnodeObj *pVnode)
{
  SVnodePeer *pVPeer;
  int         unsyncNum = 0;
  char        index = -1;

  dPrint("vid:%d, choose master", pVnode->vnode);

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = pVnode->peerInfo[i];
    if (pVPeer->status == TSDB_VN_STATUS_UNSYNCED)
      unsyncNum++;

    if (pVPeer->status == TSDB_VN_STATUS_SLAVE) {
      //slave with highest version shall be master
      if (index < 0 || pVPeer->version > pVnode->peerInfo[index]->version)
        index = i;
    }
  }

  if (index < 0 && unsyncNum == pVnode->cfg.replications) {
    // if all peers are unsynced, peer with highest version shall be master
    index = -1;
    for (int i = 0; i < pVnode->cfg.replications; ++i) {
      if (pVnode->peerInfo[i]->fileId != 0) continue;
      if (index < 0) index = i;

      if (pVnode->peerInfo[i]->version > pVnode->peerInfo[index]->version)
        index = i;
    }

    if (index < 0) {
      dError("vid:%d, all peers have corrupted files", pVnode->vnode);
    }
  }

  if (index >= 0) {
    if (index == pVnode->selfIndex) {
      dPrint("vid:%d, start to work as master", pVnode->vnode);
      pVnode->vnodeStatus = TSDB_VN_STATUS_MASTER;
    } else {
      dPrint("vid:%d, peer:%s:%d shall work as master", pVnode->vnode, pVnode->peerInfo[index]->ipstr,
             pVnode->peerInfo[index]->vid);
    }
  } else {
    dPrint("vid:%d, failed to choose master", pVnode->vnode);
  }
} 
 
int vnodeRecoverFromPeer(SVnodeObj *pVnode, int fileId) 
{
  int offlineNum = 0;
  int code = -TSDB_CODE_FILE_CORRUPTED;
  SVnodePeer *pVPeer;

  int slot = fileId % pVnode->maxFiles;
  pVnode->fmagic[slot] = 0;
  pVnode->badFileId = fileId;

  if (pVnode->cfg.replications <= 1 || pVnode->peerInfo[0] == NULL) return code;

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    if (pVnode->peerInfo[i]->status == TSDB_VN_STATUS_OFFLINE)
      offlineNum++;
  }

  if (offlineNum < pVnode->cfg.replications * 0.5) {
    dPrint("vid:%d, try to recover fileId:%d from peer", pVnode->vnode, fileId);
    pVnode->vnodeStatus = TSDB_VN_STATUS_UNSYNCED;

    for (int i = 0; i < pVnode->cfg.replications; ++i) {
      pVPeer = (SVnodePeer *) pVnode->peerInfo[i];
      if (pVPeer->peerFd >= 0)
        vnodeRestartConnection(pVPeer);
    }

    code = -TSDB_CODE_ACTION_IN_PROGRESS;
  } else {
    dError("vid:%d, fileId:%d could not be recovered, offlineNum:%d", pVnode->vnode, fileId, offlineNum);
  }

  return code;
}

void vnodeCheckStatus(SVnodePeer *pVPeer, SPeerState peerStates[], char newState)
{
  if (pVPeer->signature != pVPeer) {
    dError("failed to check vpeer:%p status, sig:%p", pVPeer, pVPeer->signature);
    return;
  }

  if (pVPeer->ownId < 0 || pVPeer->ownId >= TSDB_MAX_VNODES) {
    dError("failed to check vpeer:%p status, invalid ownId:%d", pVPeer, pVPeer->ownId);
    return;
  }

  SVnodeObj *pVnode = vnodeList + pVPeer->ownId;
  char peerOldState = pVPeer->status;
  char selfOldState = pVnode->vnodeStatus;
  int i, offlineNum = 0, syncRequired = 0;

  pthread_mutex_lock(&(pVnode->vmutex));

  if (pVPeer->ip == 0) {
    pthread_mutex_unlock(&(pVnode->vmutex));
    dError("vid:%d, failed to check status for ip is 0", pVnode->vnode);
    return;
  }

  pVnode->peerInfo[pVnode->selfIndex]->version = pVnode->version;
  pVnode->peerInfo[pVnode->selfIndex]->status = pVnode->vnodeStatus;
  pVnode->peerInfo[pVnode->selfIndex]->fileId = pVnode->badFileId;
  pVPeer->status = newState;

  dTrace("vid:%d, status:%s, peer:%s:%d received new status:%s",
          pVnode->vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus),
          pVPeer->ipstr, pVPeer->vid, taosGetVnodeStatusStr(newState));

  if (newState == TSDB_VN_STATUS_OFFLINE) {
    for (i = 0; i < pVnode->cfg.replications; ++i) {
      if (pVnode->peerInfo[i]->status == TSDB_VN_STATUS_OFFLINE) {
        offlineNum++;
      }
    }

    if (offlineNum > pVnode->cfg.replications * 0.5 && pVnode->vnodeStatus != TSDB_VN_STATUS_UNSYNCED) {
      pVnode->vnodeStatus = TSDB_VN_STATUS_UNSYNCED;
      pVnode->peerInfo[pVnode->selfIndex]->status = pVnode->vnodeStatus;
      dPrint("vid:%d, offline:%d replica:%d, change to status:%s",
             pVnode->vnode, offlineNum, pVnode->cfg.replications, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    }
  }

  int index = -1;
  for (i = 0; i < pVnode->cfg.replications; ++i) {
    SVnodePeer *pTemp = pVnode->peerInfo[i];
    dTrace("vid:%d, peer:%s:%d status:%s", pVnode->vnode, pTemp->ipstr, pTemp->vid, taosGetVnodeStatusStr(pTemp->status));

    if (pTemp->status == TSDB_VN_STATUS_MASTER) {
      if (index < 0) {
        index = i;
      } else { // multiple masters 
        if (i == pVnode->selfIndex) {
          dPrint("vid:%d, peer:%s:%d is master, work as slave instead", pTemp->ownId, pTemp->ipstr, pTemp->vid);
          pVnode->vnodeStatus = TSDB_VN_STATUS_SLAVE;
        }
      }
    }
  }

  SVnodePeer *pMaster = (index >= 0) ? pVnode->peerInfo[index] : NULL;
  if (pMaster) {
    // master is there
    dPrint("vid:%d, peer:%s:%d is master", pVnode->vnode, pMaster->ipstr, pMaster->vid);

    if (pVnode->vnodeStatus == TSDB_VN_STATUS_UNSYNCED) {
      if (pVnode->version < pMaster->version || pVnode->badFileId > 0) {
        syncRequired = 1;
        dPrint("vid:%d, need to sync, self version:%d master version:%d badFileId:%d",
               pVnode->vnode, pVnode->version, pMaster->version, pVnode->badFileId);
      } else {
        dPrint("vid:%d, work as slave, peer:%s:%d is master", pVnode->vnode, pMaster->ipstr, pMaster->vid, pMaster->version);
        pVnode->vnodeStatus = TSDB_VN_STATUS_SLAVE;
      }
    } else if (pVnode->vnodeStatus == TSDB_VN_STATUS_SLAVE && pMaster == pVPeer) {
      dPrint("vid:%d, set self version from %d to %d", pVnode->vnode, pVnode->version, pMaster->version);
      pVnode->version = pMaster->version;
    }
  } else {
    // master not there, if all peer's state and version are consistent, choose the master
    int consistent = 0;
    if (peerStates) {
      for (i = 0; i < pVnode->cfg.replications; ++i) {
        SVnodePeer *pTemp = pVnode->peerInfo[i];
        if (pTemp->status != peerStates[i].status) {
          dPrint("vid:%d, index:%d peer:%s:%d status:%s not equal with input status:%s",
                  pVnode->vnode, i, pTemp->ipstr, pTemp->vid,
                  taosGetVnodeStatusStr(pTemp->status), taosGetVnodeStatusStr(peerStates[i].status));
          break;
        }
        if ((pTemp->status != TSDB_VN_STATUS_OFFLINE) && (pTemp->version != peerStates[i].version)) {
          dPrint("vid:%d, index:%d peer:%s:%d status:%s version:%d not equal with input verison:%d",
                  pVnode->vnode, i, pTemp->ipstr, pTemp->vid,
                  taosGetVnodeStatusStr(pTemp->status), pTemp->version, peerStates[i].version);
          break;
        }
        dPrint("vid:%d, index:%d peer:%s:%d status:%s version:%d",
               pVnode->vnode, i, pTemp->ipstr, pTemp->vid, taosGetVnodeStatusStr(pTemp->status), pTemp->version);
      }

      if (i >= pVnode->cfg.replications) {
        dPrint("vid:%d, master not there, peernum:%d >= replica:%d, choose master at once",
               pVnode->vnode, i, pVnode->cfg.replications);
        consistent = 1;
      } else {
        dPrint("vid:%d, master not there, peernum:%d < replica:%d, need additional info to choose master",
               pVnode->vnode, i, pVnode->cfg.replications);
      }
    } else {
      if (pVnode->cfg.replications < 3) {
        consistent = 1;
        dPrint("vid:%d, master not there, peerStates is null, replica:%d, choose master at once",
                pVnode->vnode, pVnode->cfg.replications);
      } else {
        dPrint("vid:%d, master not there, peerStates is null, replica:%d, need additional info to choose master",
                pVnode->vnode, pVnode->cfg.replications);
      }
    }

    if (consistent)
      vnodeChooseMaster(pVnode);
  }

  pthread_mutex_unlock(&(pVnode->vmutex));

  if (syncRequired) {
    vnodeSyncWithPeer(pMaster, NULL);
  }

  if (pVnode->vnodeStatus != selfOldState)
    vnodeUpdateStreamRole(pVnode);

  if (peerOldState != newState || pVnode->vnodeStatus != selfOldState)
    vnodeBroadcastStatus(pVnode);
}

void vnodeRestartConnection(SVnodePeer *pVPeer)
{
  SVnodeObj *pVnode;

  if (pVPeer->signature != pVPeer) {
    dError("vpeer:%p, failed to restart connection, invalid signature:%p", pVPeer, pVPeer->signature);
    return;
  }

  if (pVPeer->ownId < 0 || pVPeer->ownId >= TSDB_MAX_VNODES) {
    dError("vid:%d, peer:%s:%d failed to restart connection, invalid vnode", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    return;
  }

  pVnode = &vnodeList[pVPeer->ownId];
  if (pVnode->pQueue == NULL) {
    dError("vid:%d, peer:%s:%d failed to restart connection, pQuery is null", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    return;
  }

  if (pVPeer->ip == 0) {
    dError("vid:%d, peer:%s:%d failed to restart connection, ip is 0", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    return;
  }

  dTrace("vid:%d, peer:%s:%d restart connection", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);

  pthread_mutex_lock(&(pVnode->vmutex));

  if (pVPeer->peerFd >= 0) vnodeClosePeerFd(pVPeer);
  pVPeer->peerFd = -1;

  if (pVPeer->syncFd >= 0) {
    taosCloseTcpSocket(pVPeer->syncFd);
    STranQueue *pQueue = (STranQueue *) pVnode->pQueue;
    pQueue->trans = 0;
    tfree(pQueue->buffer);
  }

  pVnode->syncStatus = 0;
  pVPeer->syncFd = -1;
  taosTmrStopA(&pVPeer->syncTimer);

  if (pVPeer->ip > tsPrivateIp4)
    taosTmrReset(vnodeCheckPeerConnection, tsVnodePeerHBTimer * 1000, pVPeer, vnodeTmrCtrl, &pVPeer->hbTimer);

  pthread_mutex_unlock(&(pVnode->vmutex));

  vnodeCheckStatus(pVPeer, NULL, TSDB_VN_STATUS_OFFLINE);
}

int vnodeProcessSyncRequest(char *msg, SVnodePeer *pVPeer)
{
  SSyncMsg       *pMsg = (SSyncMsg *)msg;
  SSyncCmd       *pSync; 
  SVnodeObj      *pVnode = vnodeList + pVPeer->ownId;
  pthread_attr_t  thattr;
  pthread_t       thread;
  int             code = 0;

  int fsize = pVnode->maxFiles * sizeof(uint64_t);
  pSync = (SSyncCmd *) malloc(sizeof(SSyncCmd) + fsize);
  pSync->pVPeer = pVPeer;
  pSync->lastCreate = htobe64(pMsg->lastCreate);
  pSync->lastRemove = htobe64(pMsg->lastRemove);
  pSync->fileId = pMsg->fileId;
  memcpy(pSync->fmagic, pMsg->fmagic, fsize);

  if (pVPeer->syncFd >= 0) taosCloseTcpSocket(pVPeer->syncFd);
  pVPeer->syncFd = -1;

  // set up tcp socket
  pVPeer->syncFd = taosOpenTcpClientSocket(pVPeer->ipstr, tsVnodeVnodePort, tsPrivateIp);
  if (pVPeer->syncFd < 0) {
    dError("vid:%d, peer:%s:%d failed to open socket to sync, ip:%s vid:%d", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    code = TSDB_CODE_APP_ERROR;
    goto _sync_create_over;
  }

  dPrint("vid:%d, peer:%s:%d sync tcp socket is setup", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);

  // start a new thread to transfer the cache
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &thattr, vnodeSyncRetrieveData, pSync) != 0) {
    dError("vid:%d, peer:%s:%d failed to create sync thread, reason:%s", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    code = TSDB_CODE_APP_ERROR;
  }

_sync_create_over:

  if (code != 0) {
    tfree(pSync);
  }

  return code;
}

void vnodeSyncNotStarted(void *param, void *tmrId)
{
  SVnodePeer *pVPeer = (SVnodePeer *)param;
  int         vid;

  if (pVPeer == NULL) return;
  if (pVPeer->ip == 0) return;

  vid = pVPeer->ownId;

  dPrint("vid:%d, peer:%s:%d sync connection is still not up", vid, pVPeer->ipstr, pVPeer->vid);

  vnodeRestartConnection(pVPeer);
}

void vnodeSyncWithPeer(void *param, void *tmrId)
{
  SVnodePeer  *pVPeer = (SVnodePeer *)param;
  STranQueue  *pQueue;
  SSyncMsg    *pSync;
  SVnodeObj   *pVnode;
  SVMsgHeader *pHeader;
  char         eflag = 0;

  pVnode = &vnodeList[pVPeer->ownId];
  pQueue = (STranQueue *) pVnode->pQueue;

  taosTmrStopA(&pVPeer->hbTimer);
  dPrint("vid:%d, peer:%s:%d try to sync", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);

  if (pVnode->syncStatus > 0) {
    dPrint("vid:%d, syncstatus:%s is not init status, stop sync", pVnode->vnode, taosGetVnodeSyncStatusStr(pVnode->syncStatus));
    return;
  }
  if (pVnode->vnodeStatus == TSDB_VN_STATUS_SLAVE) {
    dPrint("vid:%d, status:%s is slave, stop sync", pVnode->vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    return;
  }
  if (pVPeer->status != TSDB_VN_STATUS_MASTER) {
    dPrint("vid:%d, peer:%s:%d status:%s is not master, stop sync",
           pVnode->vnode, pVPeer->ipstr, pVPeer->vid, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    return;
  }
  if (pVPeer->commitInProcess) {
    dPrint("vid:%d, peer:%s:%d status:%s is in commit process, stop sync",
           pVnode->vnode, pVPeer->ipstr, pVPeer->vid, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    return;
  }

  if (pVnode->commitInProcess) { // if commiting in process, try to start again later
    dPrint("vid:%d, peer:%s:%d commit in process, try later", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    taosTmrReset(vnodeSyncWithPeer, 500, pVPeer, vnodeTmrCtrl, &pVPeer->hbTimer);
    return;
  }

  if (tsSyncNum >= tsPeerThreadPool.numOfThreads) {
    dPrint("vid:%d, peer:%s:%d too many sync in process, try later, tsSyncNum:%d", pVPeer->ownId, pVPeer->ipstr,
           pVPeer->vid, tsSyncNum);
    taosTmrReset(vnodeSyncWithPeer, 500, pVPeer, vnodeTmrCtrl, &pVPeer->hbTimer);
    return;
  }

  pthread_mutex_lock(&pQueue->qmutex);
  if (pVnode->syncStatus < TSDB_VN_SYNC_STATUS_SYNCING)
    pVnode->syncStatus = TSDB_VN_SYNC_STATUS_SYNCING;
  else
    eflag = 1;
  pthread_mutex_unlock(&pQueue->qmutex);

  if (eflag) {
    dPrint("vid:%d, peer:%s:%d is syncing, break current schedule", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, tsSyncNum);
    return;
  }

  int fsize = pVnode->maxFiles * sizeof(uint64_t);
  int msgLen = sizeof(SVMsgHeader) + sizeof(SSyncMsg) + fsize;
  char *buffer = malloc(msgLen);
  pHeader = (SVMsgHeader *) buffer;
  pHeader->type = TSDB_VMSG_SYNC_REQ;
  pHeader->sid = 0;
  pHeader->len = sizeof(SSyncMsg) + fsize;

  pSync = (SSyncMsg *) pHeader->cont;
  pSync->lastCreate = htobe64(pVnode->lastCreate);
  pSync->lastRemove = htobe64(pVnode->lastRemove);
  pSync->fileId = pVnode->fileId;
  memcpy(pSync->fmagic, pVnode->fmagic, fsize);

  if (write(pVPeer->peerFd, buffer, msgLen) != msgLen) {
    dError("vid:%d, peer:%s:%d failed to send sync req to peer", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    taosTmrStart(vnodeSyncNotStarted, 0, pVPeer, vnodeTmrCtrl);
  } else {
    dPrint("vid:%d, peer:%s:%d sync req is sent", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);

    if (pVPeer->syncFd < 0) {
      dPrint("vid:%d, peer:%s:%d syncFd:%d < 0, try sync latter", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, pVPeer->syncFd);
      taosTmrReset(vnodeSyncNotStarted, tsVnodePeerHBTimer * 1000, pVPeer, vnodeTmrCtrl, &pVPeer->syncTimer);
    }
  }

  free(buffer);

  return;
}

char *vnodeProcessOneBufferedFwd(int vid, char *offset)
{
  SVMsgHeader *pHeader = (SVMsgHeader *) offset;
  int contLen = pHeader->len;
  char *cont = offset + sizeof(SVMsgHeader);
  int insertPoints;

  if (vid < TSDB_MAX_VNODES) {
    SVnodeObj *pVnode = vnodeList + vid;
    if (pVnode->meterList && pHeader->sid < pVnode->cfg.maxSessions) {
      SMeterObj *pObj = pVnode->meterList[pHeader->sid];
      TSKEY now = taosGetTimestamp(pVnode->cfg.precision);
      (*vnodeProcessAction[pHeader->action])(pObj, cont, contLen, TSDB_DATA_SOURCE_QUEUE, NULL, pHeader->sversion,
                                             &insertPoints, now);
    } else {
      dError("vid:%d, invalid sid:%d max:%d", vid, pHeader->sid, pVnode->cfg.maxSessions);
    }
  } else {
    dError("vid:%d, invalid vnode", vid);
  }

  offset += contLen + sizeof(SVMsgHeader);

  return offset;
}

int vnodeProcessBufferedFwd(int vnode)
{
  STranQueue *pQueue;
  int submits = 0;
  char *offset;

  pQueue = (STranQueue *) vnodeList[vnode].pQueue;
  offset = pQueue->buffer;

  while (submits < pQueue->trans) {
    offset = vnodeProcessOneBufferedFwd(vnode, offset);
    submits++;
  }

  pthread_mutex_lock(&pQueue->qmutex);

  if (offset == NULL) offset = pQueue->buffer;
  while (submits < pQueue->trans) {
    offset = vnodeProcessOneBufferedFwd(vnode, offset);
    submits++;
  }

  vnodeList[vnode].syncStatus = 0;
  vnodeList[vnode].vnodeStatus = TSDB_VN_STATUS_SLAVE;
  tfree (pQueue->buffer);
  pQueue->trans = 0;

  pthread_mutex_unlock(&pQueue->qmutex);
  dPrint("vid:%d, moved to slave state since sync is over", vnode);

  return 0;
}

int vnodeForwardToPeer(SMeterObj *pObj, char *cont, int contLen, char action, int sversion)
{
  SVnodePeer  *pVPeer;
  SVMsgHeader *pHeader;
  int          fwdLen;
  SVnodeObj   *pVnode = vnodeList + pObj->vnode;

  // a hacker way to improve the performance 
  pHeader = (SVMsgHeader *) (cont - sizeof(SVMsgHeader));
  pHeader->type = TSDB_VMSG_FORWARD;
  pHeader->pversion = 0;
  pHeader->action = action;
  pHeader->sid = pObj->sid;
  pHeader->len = contLen;
  pHeader->sversion = sversion;
  pHeader->lastVersion = pVnode->version;
  fwdLen = contLen + sizeof(SVMsgHeader);

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = (SVnodePeer *) vnodeList[pObj->vnode].peerInfo[i];
    if (pVPeer->peerFd >= 0) {
      pthread_mutex_lock(&(pVnode->vmutex));
      int retLen = write(pVPeer->peerFd, (char *) pHeader, fwdLen);
      pthread_mutex_unlock(&(pVnode->vmutex));
      if (retLen == fwdLen) {
        dTrace("vid:%d sid:%d, peer:%s:%d forward is sent, contLen:%ld", pVPeer->ownId, pObj->sid, pVPeer->ipstr,
               pVPeer->vid, contLen);
      } else {
        dError("vid:%d sid:%d, peer:%s:%d failed to send forward", pVPeer->ownId, pObj->sid, pVPeer->ipstr,
               pVPeer->vid);
        vnodeRestartConnection(pVPeer);
        return TSDB_CODE_ACTION_IN_PROGRESS;
      }
    }
  }

  return 0;
}

int vnodeProcessForwardFromVMeter(int vid, SVMsgHeader *pHeader, char *cont, SVnodePeer *pVPeer) 
{
  STranQueue *pQueue;
  SMeterObj  *pObj;
  SVnodeObj  *pVnode;
  int         code = TSDB_CODE_SUCCESS;
  int         contLen = pHeader->len;
  int         sid = pHeader->sid;
  int         insertPoints;

  pVnode = vnodeList + vid;
  pObj = vnodeList[vid].meterList[sid];
  pQueue = (STranQueue *)pVnode->pQueue;

  if (pObj == NULL) {
    dError("vid:%d sid:%d, meter is not there, contact mgmt node", vid, sid);
    vnodeSendMeterCfgMsg(vid, sid);
    return TSDB_CODE_SUCCESS;
  }

  if (pVnode->vnodeStatus >= TSDB_VN_STATUS_SLAVE) {
    pVnode->version = pHeader->lastVersion;
    TSKEY now = taosGetTimestamp(pVnode->cfg.precision);
    (*vnodeProcessAction[pHeader->action])(pObj, cont, contLen, TSDB_DATA_SOURCE_QUEUE, NULL, pHeader->sversion,
                                           &insertPoints, now);
    return code;
  }

  if (pVnode->vnodeStatus == TSDB_VN_STATUS_OFFLINE ||
      (pVnode->vnodeStatus == TSDB_VN_STATUS_UNSYNCED && pVnode->syncStatus < TSDB_VN_SYNC_STATUS_SYNC_CACHE)) {
    dTrace("vid:%d sid:%d id:%s, forward not processed since in state:%d", vid, sid, pObj->meterId,
           pVnode->vnodeStatus);
    return code;
  }

  pthread_mutex_lock(&pQueue->qmutex);

  if (pVnode->syncStatus == TSDB_VN_SYNC_STATUS_SYNC_CACHE) {
    if (pQueue->bufferSize - (pQueue->offset - pQueue->buffer) < contLen + 100) {
      dError("vid:%d sid:%d id:%s, sync queue size:%d is small, sync shall restart", vid, sid, pObj->meterId,
             pQueue->bufferSize);
      vnodeCancelSync(vid);
    } else {
      memcpy(pQueue->offset, pHeader, sizeof(SVMsgHeader));
      pQueue->offset += sizeof(SVMsgHeader);
      memcpy(pQueue->offset, cont, contLen);
      pQueue->offset += contLen;
      pQueue->trans++;
      dTrace("vid:%d sid:%d id:%s, forward is saved into sync queue", vid, sid, pObj->meterId);
    }
  } else if (pVnode->vnodeStatus >= TSDB_VN_STATUS_SLAVE) {
    dTrace("vid:%d sid:%d id:%s, forward is processed since sync is over ", vid, sid, pObj->meterId);
    pVnode->version = pHeader->lastVersion;
    TSKEY now = taosGetTimestamp(pVnode->cfg.precision);
    (*vnodeProcessAction[pHeader->action])(pObj, cont, contLen, TSDB_DATA_SOURCE_QUEUE, NULL, pHeader->sversion,
                                           &insertPoints, now);
  } else {
    dTrace("vid:%d sid:%d id:%s, forward is thrown away during sync, status:%d", vid, sid, pObj->meterId,
           pVnode->vnodeStatus);
  }

  pthread_mutex_unlock(&pQueue->qmutex);
  
  return code;
}

int vnodeProcessPeerStatusMsg(char *cont, SVnodePeer *pVPeer)
{
  SVnodeObj   *pVnode = &vnodeList[pVPeer->ownId];
  SPeerStatus *pStatus = (SPeerStatus *)cont;
  int  code = 0;
  int  vid = pVPeer->ownId;

  dTrace("vid:%d, peer:%s:%d status received, self:%s version:%d peer:%s version:%d ack:%d",
         vid, pVPeer->ipstr, pVPeer->vid, taosGetVnodeStatusStr(pVnode->vnodeStatus), pVnode->version,
         taosGetVnodeStatusStr(pStatus->status), pStatus->version, pStatus->ack);

  pVPeer->commitInProcess = pStatus->commitInProcess;
  pVPeer->version = pStatus->version;
  pVPeer->fileId = htonl(pStatus->fileId);
  vnodeCheckStatus(pVPeer, pStatus->peerStates, pStatus->status);

  if (pStatus->ack)
    vnodeSendStatusMsgToPeer(pVPeer, 0); 

  return code;
}

int vnodeSendStatusMsgToPeer(SVnodePeer *pVPeer, char ack)
{
  SVnodeObj *pVnode = &vnodeList[pVPeer->ownId];
  int        msgLen, code = -1;

  int size = sizeof(SVMsgHeader) + sizeof(SPeerStatus) + sizeof(SPeerState) * 10;
  char *msg = (char *) calloc(1, size);

  SVMsgHeader *pHeader = (SVMsgHeader *) msg;
  SPeerStatus *pStatus = (SPeerStatus *) pHeader->cont;

  pHeader->type = TSDB_VMSG_STATUS;
  pHeader->len = size - sizeof(SVMsgHeader);

  pStatus->version = pVnode->version;
  pStatus->status = pVnode->vnodeStatus;
  pStatus->fileId = htonl(pVnode->badFileId);
  pStatus->commitInProcess = pVnode->commitInProcess;
  pStatus->ack = ack;

  pVnode->peerInfo[pVnode->selfIndex]->version = pVnode->version;
  pVnode->peerInfo[pVnode->selfIndex]->status = pVnode->vnodeStatus;
  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pStatus->peerStates[i].status = pVnode->peerInfo[i]->status;
    pStatus->peerStates[i].version = pVnode->peerInfo[i]->version;
  }

  msgLen = size;

  dTrace("vid:%d, peer:%s:%d status is sent, ack:%d", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, ack);
  pthread_mutex_lock(&(pVnode->vmutex));
  int retLen = write(pVPeer->peerFd, msg, msgLen);
  pthread_mutex_unlock(&(pVnode->vmutex));
  if (retLen == msgLen) {
    code = 0;
  } else {
    dTrace("vid:%d, peer:%s:%d failed to send status", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    vnodeRestartConnection(pVPeer);
  }

  free(msg);
  return code;
}

void vnodeProcessVPeerMsg(SVnodePeer *pVPeer, void *buffer)
{
  SVMsgHeader header;
  int         vid = pVPeer->ownId;
  int         bytes = 0;
  char       *cont = (char *)buffer;

  int hlen = taosReadMsg(pVPeer->peerFd, &header, sizeof(header));
  if (hlen != sizeof(header)) {
    dTrace("vid:%d, peer:%s:%d failed to read msg, hlen:%d size:%d", vid, pVPeer->ipstr, pVPeer->vid, hlen,
           sizeof(header));
    goto _error;
  }

  if (header.len > TSDB_DEFAULT_PKT_SIZE) {
    dError("vid:%d, peer:%s:%d data packet is too long, len:%d", vid, pVPeer->ipstr, pVPeer->vid, header.len);
    goto _error;
  } else if (header.len > 0) {
    bytes = taosReadMsg(pVPeer->peerFd, cont, header.len);
    if (bytes != header.len) {
      dError("vid:%d, peer:%s:%d failed to read cont, bytes:%d len:%d", vid, pVPeer->ipstr, pVPeer->vid, bytes,
             header.len);
      goto _error;
    }
  }

  if (header.type == TSDB_VMSG_FORWARD) {
    dTrace("vid:%d sid:%d, peer:%s:%d forward received, contLen:%d", vid, header.sid, pVPeer->ipstr, pVPeer->vid,
           header.len);
    vnodeProcessForwardFromVMeter(vid, &header, cont, pVPeer);
  } else if (header.type == TSDB_VMSG_SYNC_REQ) {
    dTrace("vid:%d, peer:%s:%d sync req received", vid, pVPeer->ipstr, pVPeer->vid);
    vnodeProcessSyncRequest(cont, pVPeer);
  } else if (header.type == TSDB_VMSG_STATUS) {
    vnodeProcessPeerStatusMsg(cont, pVPeer);
  }

  return;

_error:
  if (pVPeer->ip) {
    dPrint("vid:%d, peer:%s:%d tcp connection is broken", vid, pVPeer->ipstr, pVPeer->vid);
    vnodeRestartConnection(pVPeer);
  } else {
    dTrace("vid:%d, peer:%s:%d is removed, close connection", vid, pVPeer->ipstr, pVPeer->vid);
    pVPeer->signature = NULL;
    tfree(pVPeer);
  }

  return;
}

void vnodeCheckPeerConnection(void *param, void *tmrId) 
{
  SVnodePeer *pVPeer = (SVnodePeer *)param;
  int         vid;  
  int         connFd;
  SFirstPkt   firstPkt;

  if (pVPeer == NULL) {
    dError("check peer:%p connection, vpeer is null", pVPeer);
    return;
  }

  if (pVPeer->signature != pVPeer) {
    dError("check peer:%p connection, invalid signature:%p", pVPeer, pVPeer->signature);
    return;
  }

  if (pVPeer->ip == 0) {
    dError("check peer:%p connection, invalid ip:%d", pVPeer, pVPeer->ip);
    return;
  }

  if (pVPeer->ownId < 0 || pVPeer->ownId >= TSDB_MAX_VNODES) {
    dError("check peer:%p connection, invalid ownId:%d", pVPeer, pVPeer->ownId);
    return;
  }

  dTrace("vid:%d, peer:%s:%d check peer:%p connection", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, pVPeer);

  SVnodeObj *pVnode = vnodeList + pVPeer->ownId;
  if (pVnode->peerInfo[pVnode->selfIndex] == NULL) {
    dError("vid:%d, peer:%s:%d self info is empty", vid, pVPeer->ipstr, pVPeer->vid);
    return;
  }

  taosTmrStopA(&pVPeer->hbTimer);

  if (pVPeer->peerFd >= 0) {
    dTrace("vid:%d, peer:%s:%d send status to peer, peerFd:%d", vid, pVPeer->ipstr, pVPeer->vid, pVPeer->peerFd);
    vnodeSendStatusMsgToPeer(pVPeer, 1);
    return;
  }

  vid = pVPeer->ownId;
  connFd = taosOpenTcpClientSocket(pVPeer->ipstr, tsVnodeVnodePort, tsPrivateIp);
  if (connFd < 0) {
    dTrace("vid:%d, failed to open tcp socket to peer:%s:%d, retry after %d mseconds ",
            vid, pVPeer->ipstr, pVPeer->vid, tsVnodePeerHBTimer * 1000);
    taosTmrReset(vnodeCheckPeerConnection, tsVnodePeerHBTimer * 1000, pVPeer, vnodeTmrCtrl, &pVPeer->hbTimer);
    return;
  }

  taosKeepTcpAlive(connFd);
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.sourceVid = pVPeer->ownId;
  firstPkt.destVid = pVPeer->vid;
  firstPkt.type = TSDB_VMSG_STATUS;

  int len = write(connFd, &firstPkt, sizeof(firstPkt));
  if (len != sizeof(firstPkt)) {
    dError("vid:%d, peer:%s:%d failed to send firstPkt size:%d, ret:%d",
            vid, pVPeer->ipstr, pVPeer->vid, connFd, sizeof(firstPkt), len);
  }

  dTrace("vid:%d, peer:%s:%d connection to peer server is setup, connFd:%d", vid, pVPeer->ipstr, pVPeer->vid, connFd);

  vnodeAddPeerFd(&tsPeerThreadPool, pVPeer, connFd);

  return;
}

void *vnodeSyncRestoreData(void *param)
{
  SVnodePeer *pVPeer = (SVnodePeer *)param;
  int         vnode;
  SVnodeObj  *pVnode;
  uint32_t    startCache = 1;
  STranQueue *pQueue;

  tsSyncNum++;
  vnode = pVPeer->ownId;
  pVnode = &vnodeList[vnode];
  pQueue = (STranQueue *) pVnode->pQueue;
  SVnodeCfg *pCfg = &vnodeList[vnode].cfg;

  pthread_mutex_lock(&pQueue->qmutex);
  tfree(pQueue->buffer);
  pQueue->bufferSize = pCfg->cacheBlockSize * pCfg->cacheNumOfBlocks.totalBlocks;
  pQueue->buffer = malloc(pQueue->bufferSize);
  pQueue->offset = pQueue->buffer;
  pQueue->trans = 0;
  pthread_mutex_unlock(&pQueue->qmutex);
  if (pQueue->buffer == NULL) {
    dError("vid:%d, peer:%s:%d failed to allocate sync buffer", vnode, pVPeer->ipstr, pVPeer->vid);
    goto _sync_req_over;
  }

  taosBlockSIGPIPE();
  
  dTrace("vid:%d, peer:%s:%d start to restore, buffer:%p size:%d", vnode, pVPeer->ipstr, pVPeer->vid, pQueue->buffer,
         pQueue->bufferSize);
  dTrace("vid:%d, peer:%s:%d start to restore missed create", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeRestoreMissedCreateMsg(vnode, pVPeer->syncFd) < 0) {
    dError("vid:%d, peer:%s:%d failed to restore missed create, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _sync_req_over;
  }

  dTrace("vid:%d, peer:%s:%d start to restore missed remove", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeRestoreMissedRemoveMsg(vnode, pVPeer->syncFd) < 0) {
    dError("vid:%d, peer:%s:%d failed to restore missed remove, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _sync_req_over;
  }

  dTrace("vid:%d, peer:%s:%d start to process buffered create msgs", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeProcessBufferedCreateMsgs(vnode) < 0) {
    dError("vid:%d, peer:%s:%d failed to process buffered create msgs, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _sync_req_over;
  }

  vnodeList[vnode].syncStatus = TSDB_VN_SYNC_STATUS_SYNC_FILE;
  dTrace("vid:%d, peer:%s:%d start to restore file", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeSyncRestoreFile(vnode, pVPeer->syncFd) < 0) {
    dError("vid:%d, peer:%s:%d failed to restore file, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid, strerror(errno));
    goto _sync_req_over;
  }

  vnodeList[vnode].syncStatus = TSDB_VN_SYNC_STATUS_SYNC_CACHE;
  dTrace("vid:%d, peer:%s:%d send sync cache signal", vnode, pVPeer->ipstr, pVPeer->vid);
  if (write(pVPeer->syncFd, &startCache, sizeof(startCache)) < 0) {
    dError("vid:%d, peer:%s:%d failed to send sync cache signal, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _sync_req_over;
  }

  dTrace("vid:%d, peer:%s:%d start to restore cache", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeSyncRestoreCache(vnode, pVPeer->syncFd) < 0) {
    dError("vid:%d, peer:%s:%d failed to restore cache, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid, strerror(errno));
    goto _sync_req_over;
  }

  dTrace("vid:%d, peer:%s:%d start to insert buffered points", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeProcessBufferedFwd(vnode) < 0) {
    dError("vid:%d, peer:%s:%d failed to insert buffered points", vnode, pVPeer->ipstr, pVPeer->vid);
    goto _sync_req_over;
  }

  tclose(pVPeer->syncFd);
  dPrint("vid:%d, peer:%s:%d it is synced successfully", vnode, pVPeer->ipstr, pVPeer->vid);
  vnodeBroadcastStatus(pVnode);

  taosTmrStopA(&pVPeer->syncTimer);
  tsSyncNum--;
  return NULL;

_sync_req_over:
  tfree (pQueue->buffer);
  pVnode->syncStatus = 0;
  pVnode->vnodeStatus = TSDB_VN_STATUS_UNSYNCED;
  vnodeRestartConnection(pVPeer);
  tsSyncNum--;
  return NULL;
}

void *vnodeSyncRetrieveData(void *param)
{
  SSyncCmd    *pSync = (SSyncCmd *)param;
  SVnodePeer  *pVPeer = pSync->pVPeer;
  int          vnode = pVPeer->ownId;
  SFirstPkt    firstPkt;
  uint32_t     startCache;

  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.type = TSDB_VMSG_SYNC_DATA;
  firstPkt.sourceVid = pVPeer->ownId;
  firstPkt.destVid = pVPeer->vid;

  if (vnodeList[vnode].commitInProcess) {
    dTrace("vid:%d, peer:%s:%d sync retrieve shall stop since in commit process", vnode, pVPeer->ipstr, pVPeer->vid);
    goto _over;
  }

  taosBlockSIGPIPE();

  pVPeer->syncStatus = TSDB_VN_SYNC_STATUS_SYNCING;
  dTrace("vid:%d, peer:%s:%d start to retrieve data", vnode, pVPeer->ipstr, pVPeer->vid);
  if (write(pVPeer->syncFd, (char *) &firstPkt, sizeof(firstPkt)) < 0) {
    dError("vid:%d, peer:%s:%d failed to send syncCmd, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid, strerror(errno));
    goto _over;
  }

  dTrace("vid:%d, peer:%s:%d start to retrieve missed create", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeRetrieveMissedCreateMsg(vnode, pVPeer->syncFd, pSync->lastCreate) < 0) {
    dError("vid:%d, peer:%s:%d failed to retrieve missed create, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _over;
  }

  dTrace("vid:%d, peer:%s:%d start to retrieve missed remove", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeRetrieveMissedRemoveMsg(vnode, pVPeer->syncFd, pSync->lastRemove) < 0) {
    dError("vid:%d, peer:%s:%d failed to retrieve missed remove, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _over;
  }

  pVPeer->syncStatus = TSDB_VN_SYNC_STATUS_SYNC_FILE;
  dTrace("vid:%d, peer:%s:%d start to retrieve file", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeSyncRetrieveFile(vnode, pVPeer->syncFd, pSync->fileId, pSync->fmagic) < 0) {
    dError("vid:%d, peer:%s:%d failed to retrieve file, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid, strerror(errno));
    goto _over;
  }

  dTrace("vid:%d, peer:%s:%d start to receive sync cache signal", vnode, pVPeer->ipstr, pVPeer->vid);
  if (taosReadMsg(pVPeer->syncFd, &startCache, sizeof(startCache)) < 0) {
    dError("vid:%d, peer:%s:%d failed to get sync cache signal, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _over;
  }

  pVPeer->syncStatus = TSDB_VN_SYNC_STATUS_SYNC_CACHE;
  dTrace("vid:%d, peer:%s:%d start to retrieve cache", vnode, pVPeer->ipstr, pVPeer->vid);
  if (vnodeSyncRetrieveCache(vnode, pVPeer->syncFd) < 0) {
    dError("vid:%d, peer:%s:%d failed to retrieve cache, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
           strerror(errno));
    goto _over;
  }

  dTrace("vid:%d, peer:%s:%d sync retrieve process is successful", vnode, pVPeer->ipstr, pVPeer->vid);

  tclose(pVPeer->syncFd);
  pVPeer->syncStatus = 0;
  tfree(pSync);
  return NULL;

_over:
  pVPeer->syncStatus = 0;
  vnodeRestartConnection(pVPeer);
  tfree(pSync);
  return NULL;
}

void  vnodeCloseAllSyncFds(int vnode) {
  SVnodePeer *pVPeer;
  SVnodeObj *pVnode = vnodeList + vnode;

  for (int i = 0; i < pVnode->cfg.replications; ++i) {
    pVPeer = pVnode->peerInfo[i];
    if (pVPeer && pVPeer->syncFd >= 0) {
      taosCloseTcpSocket(pVPeer->syncFd);
      pVPeer->syncFd = -1;
      pVPeer->syncStatus = 0;
      dTrace("vid:%d, peer:%s:%d sync connection is closed", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    }
  }
}

void vnodeCancelSync(int vnode)
{
  STranQueue *pQueue;

  vnodeCloseAllSyncFds(vnode);
  pQueue = (STranQueue *) vnodeList[vnode].pQueue;

  pthread_mutex_lock(&pQueue->qmutex);
  pQueue->trans = 0;
  tfree(pQueue->buffer);
  pthread_mutex_unlock(&pQueue->qmutex);
}

void vnodeClosePeerFd(SVnodePeer *pVPeer)
{
  SThreadObj *pThread = pVPeer->pThread;

  if (pThread == NULL) {
    dTrace("vid:%d, peer:%s:%d thread is null", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
    return;
  }

  if (pVPeer->peerFd >= 0) {
    epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pVPeer->peerFd, NULL);
    taosCloseTcpSocket(pVPeer->peerFd);
    pThread->numOfFds--;
  }

  dTrace("vid:%d, peer:%s:%d connection closed, threadId:%d numOfFds:%d",
          pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, pThread->threadId, pThread->numOfFds);
}

void vnodeCloseThreadPool(SThreadPool *pPool)
{
  SThreadObj *pThread;
  if (pPool == NULL) return;

  pthread_cancel(pPool->thread);
  pthread_join(pPool->thread, NULL);

  for (int i = 0; i < pPool->numOfThreads; ++i) {
    pThread = pPool->pThread[i];
    if (pThread) {
      close(pThread->pollFd);
      pthread_cancel(pThread->thread);
      pthread_join(pThread->thread, NULL);
      tfree(pThread);
    }
  }

  tfree(pPool->pThread);
  dPrint("peer TCP server is cleaned up");
}

#define maxEvents 10

static void vnodeProcessTcpData(void *param) 
{
  SThreadObj *pThread = (SThreadObj *) param;
  int fdNum;
  struct epoll_event events[maxEvents];

  void *buffer = malloc(64000);

  taosBlockSIGPIPE();

  while (1) {
    fdNum = epoll_wait(pThread->pollFd, events, maxEvents, -1);
    if (fdNum < 0) continue;

    for (int i = 0; i < fdNum; ++i) {
      SVnodePeer *pVPeer = events[i].data.ptr;
      if (pVPeer->ip == 0 && pVPeer->peerFd >= 0) {
        // it is already removed
        dError("vid:%d, peer:%s:%d is already removed, signature:%p", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, pVPeer->signature);
        pVPeer->signature = NULL;
        tfree(pVPeer);
        continue;
      }

      if (events[i].events & EPOLLERR) {
        dTrace("vid:%d, peer:%s:%d error happened on FD, threadId:%d", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, pThread->threadId);
        vnodeRestartConnection(pVPeer);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        dTrace("vid:%d, peer:%s:%d FD hang up, threadId:%d", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, pThread->threadId);
        vnodeRestartConnection(pVPeer);
        continue;
      }

      vnodeProcessVPeerMsg(pVPeer, buffer);
    }
  }

  free (buffer);
}

void *vnodeAcceptPeerTcpConnection(void *argv)
{
  SThreadPool   *pPool = (SThreadPool *)argv;
  int            tcpFd, i, vnode;
  int            connFd = -1;
  SFirstPkt      firstPkt;
  SVnodePeer    *pVPeer;
  SVnodeObj     *pVnode;
  uint32_t       sourceIp;
  char           ipstr[24];
  struct sockaddr_in clientAddr;

  taosBlockSIGPIPE();

  tcpFd = taosOpenTcpServerSocket(tsPrivateIp, tsVnodeVnodePort);
  if (tcpFd < 0) {
    dError("failed to create peer TCP socket, reason:%s", strerror(errno));
    return NULL;
  }

  dTrace("peer TCP server is created, ip:%s port:%hu", tsPrivateIp, tsVnodeVnodePort);

  while (1) {
    socklen_t addrlen = sizeof(clientAddr);
    connFd = accept(tcpFd, (struct sockaddr *) &clientAddr, &addrlen);

    if (connFd < 0) {
      dError("peer TCP accept failure, reason:%s", strerror(errno));
      continue;
    }

    taosKeepTcpAlive(connFd);
    sourceIp = clientAddr.sin_addr.s_addr;
    tinet_ntoa(ipstr, sourceIp);
    dTrace("peer TCP connection from ip:%s port:%hu", ipstr, htons(clientAddr.sin_port));

    if (taosReadMsg(connFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt)) {
      dError("failed to read peer first pkt from ip:%s, reason:%s", ipstr, strerror(errno));
      taosCloseTcpSocket(connFd);
      continue;
    }

    vnode = firstPkt.destVid;
    if (vnode < 0 || vnode >= TSDB_MAX_VNODES) {
      dError("vid:%d, vnode from peer is out of range", vnode);
      taosCloseTcpSocket(connFd);
      continue;
    }

    pVnode = &vnodeList[vnode];
    pthread_mutex_lock(&pVnode->vmutex);
    for (i = 0; i < pVnode->cfg.replications; ++i) {
      pVPeer = pVnode->peerInfo[i];
      if (pVPeer && pVPeer->ip == sourceIp && pVPeer->vid == firstPkt.sourceVid)
        break;
    }

    pVPeer = (i < pVnode->cfg.replications) ? pVnode->peerInfo[i] : NULL;
    pthread_mutex_unlock(&pVnode->vmutex);

    if (pVPeer == NULL) {
      dError("vid:%d, peer:%s:%d not configured", vnode, ipstr, firstPkt.sourceVid);
      vnodeSendVpeerCfgMsg(vnode);
      taosCloseTcpSocket(connFd);
      continue;
    }

    if (firstPkt.type == TSDB_VMSG_SYNC_DATA) {
      pthread_attr_t thattr;
      pthread_t thread;
      pthread_attr_init(&thattr);
      pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

      pVPeer->syncFd = connFd;
      taosTmrStopA(&pVPeer->syncTimer);
      if (pthread_create(&(thread), &thattr, (void *) vnodeSyncRestoreData, (void *) pVPeer) < 0) {
        dError("vid:%d, peer:%s:%d failed to create peer sync thread, reason:%s", vnode, pVPeer->ipstr, pVPeer->vid,
               strerror(errno));
        taosCloseTcpSocket(connFd);
        continue;
      }

      pthread_attr_destroy(&thattr);
      dPrint("vid:%d, peer:%s:%d sync connection is up", vnode, pVPeer->ipstr, pVPeer->vid);
      continue;
    }

    if (pVPeer->peerFd >= 0) {
      dTrace("vid:%d, peer:%s:%d TCP connection is already up, close current one", vnode, pVPeer->ipstr, pVPeer->vid);
      vnodeClosePeerFd(pVPeer);
      pVPeer->peerFd = -1;
    }

    vnodeAddPeerFd(pPool, pVPeer, connFd);
  }
 
  return NULL;
}

int vnodeAddPeerFd(SThreadPool *pPool, SVnodePeer *pVPeer, int connFd)
{
  struct epoll_event event;
  pthread_attr_t thattr;

  event.events = EPOLLIN | EPOLLPRI;
  event.data.ptr = pVPeer;

  pthread_mutex_lock(&dmutex);

  SThreadObj *pThread = pPool->pThread[pPool->threadId];

  if (pThread == NULL) {
    pThread = (SThreadObj *) calloc(1, sizeof(SThreadObj));
    if (pThread == NULL) {
      dError("vid:%d, peer:%s:%d failed to create thread", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);
      goto _over;
    }

    pThread->threadId = pPool->threadId;
    pThread->pollFd = epoll_create(10);  // size does not matter
    if (pThread->pollFd < 0) {
      dError("vid:%d, peer:%s:%d failed to create tcp epoll, reason:%s",
              pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, strerror(errno));
      goto _over;
    }

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(pThread->thread), &thattr, (void *) vnodeProcessTcpData, (void *) (pThread)) != 0) {
      dError("vid:%d, peer:%s:%d failed to create tcp thread, reason:%s",
              pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, strerror(errno));
      goto _over;
    }

    pPool->pThread[pPool->threadId] = pThread;
  }

  if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
    dError("vid:%d, peer:%s:%d failed to add fd:%d to tcp epoll, reason:%s",
           pVPeer->ownId, pVPeer->ipstr, pVPeer->vid, connFd, strerror(errno));
    close(connFd);
    goto _over;
  }

  pVPeer->peerFd = connFd;
  pVPeer->pThread = pThread;
  vnodeSendStatusMsgToPeer(pVPeer, 0);
  dTrace("vid:%d, peer:%s:%d ready to exchange data", pVPeer->ownId, pVPeer->ipstr, pVPeer->vid);

  pThread->numOfFds++;
  pPool->threadId++;
  pPool->threadId = pPool->threadId % pPool->numOfThreads;

_over:
  pthread_mutex_unlock(&dmutex);
  return 0;
}

int vnodeOpenThreadPool(SThreadPool *pPool, int numOfThreads)
{
  pthread_attr_t thattr;

  pPool->threadId = 0;
  pPool->numOfThreads = numOfThreads;
  pPool->pThread = (SThreadObj **) calloc(1, sizeof(SThreadObj *) * (size_t) numOfThreads);
  if (pPool->pThread == NULL) {
    dError("peer TCP, no enough memory");
    return -1;
  }

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pPool->thread), &thattr, (void *) vnodeAcceptPeerTcpConnection, (void *) (pPool)) != 0) {
    dError("peer TCP, failed to create accept thread, reason:%s", strerror(errno));
    return -1;
  }

  pthread_attr_destroy(&thattr);
  dPrint ("peer TCP, server is initialized, numOfThreads:%d", numOfThreads);

  return 0;
}


