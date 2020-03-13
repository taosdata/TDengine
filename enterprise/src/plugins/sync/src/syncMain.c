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

#include <stdint.h>
#include <stdbool.h>
#include "os.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "tsocket.h"
#include "taosTcpPool.h"
#include "taosHashId.h"
#include "tsync.h"
#include "syncInt.h"

// global configurable
uint32_t  tsPrivateIpv4;
int       tsMaxSyncNum = 4;
int       tsSyncTcpThreads = 2;
int       tsMaxWatchFiles = 100;

// module global
int         tsSyncNum;    // number of sync in process in whole system
int         tsNodeNum;    // number of nodes in system

static void  syncProcessSyncRequest(char *pMsg, SSyncPeer *pPeer);
static void  syncSyncWithMaster(void *, void *);
static void  syncCheckPeerConnection(void *param, void *tmrId);
static void  syncSendStatusMsgToPeer(SSyncPeer *pPeer, char ack);
static void  syncProcessBrokenLink(void *param);
static void  syncProcessPeerMsg(void *param, void *buffer);
static void  syncProcessIncommingConnection(int connFd, uint32_t sourceIp); 
static void  syncRemovePeer(SSyncPeer *pPeer);
static void  syncAddNodeRef(SSyncNode *pNode);
static void  syncAddPeerRef(SSyncPeer *pPeer);
static int   syncDecNodeRef(SSyncNode *pNode);
static int   syncDecPeerRef(SSyncPeer *pPeer);
static SSyncPeer *syncAddPeer(SSyncNode *pNode, SNodeInfo *pInfo);

static ttpool_h       tsTcpPool;
static void          *syncTmrCtrl = NULL;
static void          *vgIdHash;
static pthread_once_t syncModuleInit = PTHREAD_ONCE_INIT;

static void syncModuleInitFunc() {
  SPoolInfo info;

  info.numOfThreads = tsSyncTcpThreads;
  info.serverIp = tsPrivateIpv4;
  info.port = tsVnodeVnodePort;
  info.bufferSize = 640000;
  info.processBrokenLink = syncProcessBrokenLink;
  info.processIncomingMsg = syncProcessPeerMsg;
  info.processIncomingConn = syncProcessIncommingConnection;
  tsTcpPool = taosOpenTcpThreadPool(&info);

  syncTmrCtrl = taosTmrInit(1000, 50, 10000, "SYNC");
  vgIdHash = taosOpenIdHash(100000);
}

void *syncStart(SSyncInfo *pInfo) 
{
  pthread_once(&syncModuleInit, syncModuleInitFunc); 

  if (tsTcpPool == NULL) {
    dError("failed to init TCP thread pool, reason:%s", strerror(errno));
    return NULL;
  }
    
  SSyncNode *pNode = (SSyncNode *) calloc(sizeof(SSyncNode), 1);
  
  pNode->replica = pInfo->replica;
  pNode->quorum = pInfo->quorum;
  strcpy(pNode->label, pInfo->label);
  pNode->vgId = pInfo->vgId;
  pNode->getFileInfo = pInfo->getFileInfo;
  pNode->getWalInfo = pInfo->getWalInfo;
  pNode->writeToCache = pInfo->writeToCache;
  pNode->status = (pInfo->replica > 1) ? TAOS_SYNC_STATUS_UNSYNCED : TAOS_SYNC_STATUS_MASTER;
  pthread_mutex_init(&pNode->mutex, NULL);

  for (int i = 0; i < pInfo->replica; ++i) {
    pNode->peerInfo[i] = syncAddPeer(pNode, &pInfo->nodeInfo[i]);
    if (pInfo->nodeInfo[i].nodeIp == tsPrivateIpv4) pNode->selfIndex = i;
  }

  dPrint("%s, %d replicas are configured, status:%s", pNode->label, pNode->replica, syncStatus[pNode->status]);

  atomic_add_fetch_32(&tsNodeNum, 1);
  syncAddNodeRef(pNode);
  taosAddIdHash(vgIdHash, pNode, pInfo->vgId);

  (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
  return pNode;
}

void syncStop(void *param) 
{
  SSyncNode  *pNode = (SSyncNode *)param;
  SSyncPeer  *pPeer;

  dPrint("%s, cleanup sync", pNode->label);

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer) syncRemovePeer(pPeer); 
  }

  taosDeleteIdHash(vgIdHash, pNode->vgId);
  syncDecNodeRef(pNode);
  atomic_sub_fetch_32(&tsNodeNum, 1);

  if (tsNodeNum <=0) {
    taosCloseTcpThreadPool(tsTcpPool);
    taosCloseIdHash(vgIdHash);
  }
}

int syncReconfig(void *param, SSyncInfo *pNewInfo) 
{
  SSyncNode  *pNode = (SSyncNode *)param;
  int         i, j;

  dPrint("%s, reconfig, status:%s replica:%d old:%d", pNode->label, syncStatus[pNode->status], 
         pNewInfo->replica, pNode->replica);

  for (i = 0; i < pNode->replica; ++i) {
    for (j = 0; j < pNewInfo->replica; ++j) {
      if (pNode->peerInfo[i]->ip == pNewInfo->nodeInfo[j].nodeIp) 
        break;
    }

    if (j >= pNewInfo->replica) {
      syncRemovePeer(pNode->peerInfo[i]);
      pNode->peerInfo[i] = NULL;
    }
  }

  SSyncPeer *newPeers[TAOS_SYNC_MAX_REPLICA];
  for (i = 0; i < pNewInfo->replica; ++i) {
    SNodeInfo *pNewNode = &pNewInfo->nodeInfo[i];

    for (j = 0; j < pNode->replica; ++j) {
      if (pNode->peerInfo[j]->ip == pNewNode->nodeIp)
        break;
    }

    if (j >= pNode->replica) {
      newPeers[i] = syncAddPeer(pNode, pNewNode);
    } else {
      newPeers[i] = pNode->peerInfo[j];
    }

    if (pNewNode->nodeIp == tsPrivateIpv4) pNode->selfIndex = i;
  }

  pNode->replica = pNewInfo->replica;
  pNode->quorum = pNewInfo->quorum;
  memcpy(pNode->peerInfo, newPeers, sizeof(SSyncPeer *) * pNewInfo->replica);

  for (i = pNewInfo->replica; i < TAOS_SYNC_MAX_REPLICA; ++i)
    pNode->peerInfo[i] = NULL;

  if (pNewInfo->replica <= 1) {
    dPrint("%s, no peers are configured, work as master!", pNode->label);
    pNode->status = TAOS_SYNC_STATUS_MASTER;
    (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
  }

  syncBroadcastStatus(pNode);

  return 0;
}

int syncForwardToPeer(void *param, uint64_t version, char *cont, int contLen)
{
  SSyncNode  *pNode = (SSyncNode *)param;
  SSyncPeer  *pPeer;
  SSyncHead  *pHead;
  int         fwdLen;

  if (pNode->status != TAOS_SYNC_STATUS_MASTER) return -1;

  // always update version
  pNode->version = version;
 
  // a hacker way to improve the performance
  pHead = (SSyncHead *) (cont - sizeof(SSyncHead));
  pHead->type = TAOS_SMSG_FORWARD;
  pHead->pversion = 0;
  pHead->len = contLen;
  pHead->version = pNode->version;
  fwdLen = contLen + sizeof(SSyncHead);

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer == NULL || pPeer->peerFd <0) continue;
    if (pPeer->status != TAOS_SYNC_STATUS_SLAVE && 
        pPeer->status != TAOS_SYNC_STATUS_CACHE ) continue;
      
    int retLen = write(pPeer->peerFd, pHead, fwdLen);
    if (retLen == fwdLen) {
      dTrace("%s peer:%s, forward is sent, contLen:%d", pNode->label, pPeer->ipstr, contLen);
    } else {
      dError("%s peer:%s, failed to forward, retLen:%d", pNode->label, pPeer->ipstr, retLen);
      syncRestartConnection(pPeer);
    }
  }

  return 0;
}

void syncRecover(void *param) {
  SSyncNode *pNode = (SSyncNode *)param;
  SSyncPeer *pPeer;

  // to do: add a few lines to check if recover is OK 
  // if take this node to unsync state, the whole system may not work

  pNode->status = TAOS_SYNC_STATUS_UNSYNCED;
  (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
  pNode->version = 0;

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = (SSyncPeer *) pNode->peerInfo[i];
    if (pPeer->peerFd >= 0) {
      syncRestartConnection(pPeer);
    }
  }
}

int syncGetStatus(void *param, SSyncStatus *pStatus)
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncNode *pNode = pPeer->pSyncNode;
  
  pStatus->selfIndex = pNode->selfIndex;
  for (int i=0; i<pNode->replica; ++i) {
    pStatus->nodeId[i] = pNode->peerInfo[i]->nodeId;
    pStatus->status[i] = pNode->peerInfo[i]->status;
  }

  return 0;
}

static void syncAddNodeRef(SSyncNode *pNode)
{
   atomic_add_fetch_8(&pNode->refCount, 1);
}

static int syncDecNodeRef(SSyncNode *pNode)
{
  if (atomic_sub_fetch_8(&pNode->refCount, 1) == 0) {
    pthread_mutex_destroy(&pNode->mutex);
    free (pNode);
    return 0;
  }

  return 1;
}

static void syncAddPeerRef(SSyncPeer *pPeer)
{
   atomic_add_fetch_8(&pPeer->refCount, 1);
}

static int syncDecPeerRef(SSyncPeer *pPeer)
{
  if (atomic_sub_fetch_8(&pPeer->refCount, 1) == 0) {
    syncDecNodeRef(pPeer->pSyncNode);
    free (pPeer);
    return 0;
  }

  return 1;
}

static void syncRemovePeer(SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncNode  *pNode = pPeer->pSyncNode;

  dPrint("%s peer:%s, it is removed", pNode->label, pPeer->ipstr);

  pPeer->ip = 0;
  taosTmrStopA(&pPeer->hbTimer);
  taosTmrStopA(&pPeer->syncTimer);
  tclose(pPeer->syncFd);
  tclose(pPeer->peerFd);
  tfree(pPeer->watchFd);

  syncDecPeerRef(pPeer);
}

static SSyncPeer *syncAddPeer(SSyncNode *pNode, SNodeInfo *pInfo) 
{
  SSyncPeer *pPeer = (SSyncPeer *) calloc(1, sizeof(SSyncPeer));

  pPeer->ip = pInfo->nodeIp;
  tinet_ntoa(pPeer->ipstr, pInfo->nodeIp);
  pPeer->nodeId = pInfo->nodeId;

  pPeer->peerFd = -1;
  pPeer->syncFd = -1;
  pPeer->status = TAOS_SYNC_STATUS_OFFLINE;
  pPeer->pSyncNode = pNode;
  pPeer->refCount = 1;
  dPrint("%s peer:%s, %s is configured", pNode->label, pPeer->ipstr, pInfo->name);
  if (pInfo->nodeIp > tsPrivateIpv4) {
    dTrace("%s peer:%s, start to check peer connection", pNode->label, pPeer->ipstr);
    taosTmrReset(syncCheckPeerConnection, 0, pPeer, syncTmrCtrl, &pPeer->hbTimer);
  }

  syncAddPeerRef(pPeer);

  return pPeer;
}

void syncBroadcastStatus(SSyncNode *pNode)
{
  SSyncPeer *pPeer;

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    syncSendStatusMsgToPeer(pPeer, 1);
  }
} 

static void syncChooseMaster(SSyncNode *pNode) {
  SSyncPeer *pPeer;
  int8_t     onlineNum = 0;
  int8_t     index = -1;

  dPrint("%p, choose master", pNode->label);

  for (int i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->status != TAOS_SYNC_STATUS_OFFLINE)
      onlineNum++;
  }

  if ( onlineNum >= (pNode->replica+0.5)/2 ) {
    // over half of nodes are online
    for (int i = 0; i < pNode->replica; ++i) {
      //slave with highest version shall be master
      pPeer = pNode->peerInfo[i];
      if (pPeer->status == TAOS_SYNC_STATUS_SLAVE || pPeer->status == TAOS_SYNC_STATUS_MASTER) {
        if (index < 0 || pPeer->version > pNode->peerInfo[index]->version)
          index = i;
      }
    }
  }

  if (index < 0 && onlineNum == pNode->replica) {
    // if all peers are online, peer with highest version shall be master
    index = 0;
    for (int i = 1; i < pNode->replica; ++i) {
      if (pNode->peerInfo[i]->version > pNode->peerInfo[index]->version)
        index = i;
    }
  }

  if (index >= 0) {
    if (index == pNode->selfIndex) {
      dPrint("%s, start to work as master", pNode->label);
      pNode->status = TAOS_SYNC_STATUS_MASTER;
      (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
    } else {
      pPeer = pNode->peerInfo[index];
      dPrint("%s peer:%s, it shall work as master", pNode->label, pPeer->ipstr);
    }
  } else {
    dPrint("%s, failed to choose master", pNode->label);
  }
} 
 
static SSyncPeer *syncCheckMaster(SSyncNode *pNode ) {
  int offlineNum = 0;
  int index = -1;

  for (int i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->status == TAOS_SYNC_STATUS_OFFLINE) 
      offlineNum++;
  }

  if (offlineNum > pNode->replica * 0.5 ) {
    if (pNode->status != TAOS_SYNC_STATUS_UNSYNCED) {
      pNode->status = TAOS_SYNC_STATUS_UNSYNCED;
      (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
      pNode->peerInfo[pNode->selfIndex]->status = pNode->status;
      dPrint("%s, offline:%d replica:%d, change to unsynced state", pNode->label, offlineNum, pNode->replica);
    }
  } else {
    for (int i=0; i<pNode->replica; ++i) {
      SSyncPeer *pTemp = pNode->peerInfo[i];
      if ( pTemp->status != TAOS_SYNC_STATUS_MASTER ) continue;
      if ( index < 0 ) {
        index = i;
      } else { // multiple masters, it shall not happen 
        if ( i == pNode->selfIndex ) {
          dError("%s, peer:%s: is master, work as slave instead", pNode->label, pTemp->ipstr);
          pNode->status = TAOS_SYNC_STATUS_SLAVE;
          (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
        }
      }
    }
  }

  SSyncPeer *pMaster = (index>=0) ? pNode->peerInfo[index]:NULL;
  return pMaster;
}

static void syncCheckStatus(SSyncPeer *pPeer, SPeerState peerStates[], int8_t newState)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  int8_t     peerOldState = pPeer->status;
  int8_t     selfOldState = pNode->status;
  int8_t     i, syncRequired = 0;

  pthread_mutex_lock(&(pNode->mutex));

  pNode->peerInfo[pNode->selfIndex]->version = pNode->version;
  pNode->peerInfo[pNode->selfIndex]->status = pNode->status;
  pPeer->status = newState;

  dTrace("%s peer:%s, own status:%s, new peer status:%s", pNode->label, pPeer->ipstr, 
          syncStatus[pNode->status], syncStatus[pPeer->status]);  

  SSyncPeer *pMaster = syncCheckMaster(pNode);

  if ( pMaster ) {
    // master is there
    if ( pNode->status == TAOS_SYNC_STATUS_UNSYNCED ) {
      if ( pNode->version < pMaster->version) {
        syncRequired = 1;
      } else {
        dPrint("%s, peer:%s is master, work as slave, version:%d", pNode->label, pMaster->ipstr, pMaster->version);
        pNode->status = TAOS_SYNC_STATUS_SLAVE;
        (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
      }
    } else if ( pNode->status == TAOS_SYNC_STATUS_SLAVE && pMaster == pPeer) {
      pNode->version = pMaster->version;
    }
  } else {
    // master not there, if all peer's state and version are consistent, choose the master
    int consistent = 0;
    if (peerStates) {
      for (i = 0; i < pNode->replica; ++i) {
        SSyncPeer *pTemp = pNode->peerInfo[i];
        if (pTemp->status != peerStates[i].status) break;
        if ((pTemp->status != TAOS_SYNC_STATUS_OFFLINE) && (pTemp->version != peerStates[i].version)) break; 
      }
 
      if (i >= pNode->replica) consistent = 1;
    } 

    if (consistent || pNode->replica < 3)
      syncChooseMaster(pNode);
  }

  pthread_mutex_unlock(&(pNode->mutex));

  if (syncRequired) {
    syncSyncWithMaster(pMaster, NULL);
  }

  if (peerOldState != newState || pNode->status != selfOldState)
    syncBroadcastStatus(pNode);
}

void syncRestartConnection(SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncNode *pNode = pPeer->pSyncNode;

  dTrace("%s peer:%s, restart connection", pNode->label, pPeer->ipstr);
  tclose(pPeer->peerFd);
  tclose(pPeer->syncFd);
  taosTmrStopA(&pPeer->hbTimer);
  taosTmrStopA(&pPeer->syncTimer);

  if (pPeer->ip > tsPrivateIpv4)
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer*1000, pPeer, syncTmrCtrl, &pPeer->hbTimer);

  syncCheckStatus(pPeer, NULL, TAOS_SYNC_STATUS_OFFLINE);
}

static void syncProcessSyncRequest(char *msg, SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncNode *pNode = pPeer->pSyncNode;

  // start a new thread to retrieve the data
 
  pthread_attr_t  thattr;
  pthread_t       thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &thattr, syncRetrieveData, pPeer) != 0) {
    dError("%s peer:%s, failed to create sync thread, reason:%s", pNode->label, pPeer->ipstr, strerror(errno));
  }
}

static void syncNotStarted(void *param, void *tmrId)
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0) return;
  SSyncNode *pNode = pPeer->pSyncNode;

  dPrint("%s peer:%s, sync connection is still not up, restart", pNode->label, pPeer->ipstr);
  syncRestartConnection(pPeer);
}

static void syncSyncWithMaster(void *param, void *tmrId)
{
  SSyncPeer   *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0) return;
  SSyncNode   *pNode = pPeer->pSyncNode;

  taosTmrStopA(&pPeer->hbTimer);
  dPrint("%s peer:%s, try to sync", pNode->label, pPeer->ipstr)

  if (tsSyncNum >= tsMaxSyncNum) {
    dPrint("%s peer:%s, %d syncs are in process, try later", pNode->label, pPeer->ipstr, tsSyncNum);
    pPeer->hbTimer = taosTmrStart(syncSyncWithMaster, 500, pPeer, syncTmrCtrl);
    return;
  }

  SSyncHead firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.type = TAOS_SMSG_SYNC_REQ;
  pPeer->syncTimer = taosTmrStart(syncNotStarted, tsVnodePeerHBTimer*1000, pPeer, syncTmrCtrl);

  if (write(pPeer->peerFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt) ) {
    dError("%s peer:%s, failed to send sync req to peer", pNode->label, pPeer->ipstr);
  } else {
    dPrint("%s peer:%s, sync req is sent", pNode->label, pPeer->ipstr);
  }

  return;
}

static void syncProcessForwardFromPeer(SSyncHead *pHead, SSyncPeer *pPeer)
{
  SSyncNode   *pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;

  if (pNode->status == TAOS_SYNC_STATUS_SLAVE) {
    pNode->version = pHead->version;
    (*pNode->writeToCache)(pNode->ahandle, pHead->version, pHead->cont, pHead->len);
    return;
  }

  if (pRecv == NULL) return;

  pthread_mutex_lock(&pNode->mutex);

  if (pNode->status == TAOS_SYNC_STATUS_SLAVE) {
    pNode->version = pHead->version;
    (*pNode->writeToCache)(pNode->ahandle, pHead->version, pHead->cont, pHead->len);
    return;
  } else if (pNode->status == TAOS_SYNC_STATUS_CACHE) {
    if (syncSaveIntoBuffer(pRecv, pHead) == 0) {
      dTrace("%s peer:%s, forward is saved into sync queue", pNode->label, pPeer->ipstr);
    } else {
      dError("%s peer:%s, failed to save into sync queue", pNode->label, pPeer->ipstr);
    }
  } else {
    dError("%s peer:%s, forward is thrown away", pNode->label, pPeer->ipstr);
  }

  pthread_mutex_lock(&pNode->mutex);

  return;
}

static void syncProcessPeerStatusMsg(char *cont, SSyncPeer *pPeer)
{
  SSyncNode   *pNode = pPeer->pSyncNode;
  SPeerStatus *pStatus = (SPeerStatus *)cont;

  dTrace("%s peer:%s, status received, self:%s version:%d peer:%s version:%d",
         pNode->label, pPeer->ipstr, syncStatus[pNode->status], pNode->version,
         syncStatus[pStatus->status], pStatus->version, pStatus->ack);

  pPeer->version = pStatus->version;
  syncCheckStatus(pPeer, pStatus->peerStates, pStatus->status);

  if (pStatus->ack)
    syncSendStatusMsgToPeer(pPeer, 0);
}

static void syncProcessPeerMsg(void *param, void *buffer)
{
  SSyncPeer  *pPeer = (SSyncPeer *)param;
  SSyncHead   header;
  SSyncNode  *pNode = pPeer->pSyncNode;
  int         bytes = 0;
  char       *cont = (char *)buffer;

  if (pPeer->ip == 0) return;

  int hlen = taosReadMsg(pPeer->peerFd, &header, sizeof(header));
  if (hlen != sizeof(header)) {
    dTrace("%s peer:%s, failed to read msg, hlen:%d", pNode->label, pPeer->ipstr, hlen);
    syncRestartConnection(pPeer);
    return;
  }

  header.len = htonl(header.len);
  if (header.len > TSDB_DEFAULT_PKT_SIZE || header.len <0) {
    dError("%s peer:%s, invalid pkt length, len:%d", pNode->label, pPeer->ipstr, header.len);
    syncRestartConnection(pPeer);
    return;
  } 

  bytes = taosReadMsg(pPeer->peerFd, cont, header.len);
  if (bytes != header.len) {
    dError("%s peer:%s, failed to read, bytes:%d len:%d", pNode->label, pPeer->ipstr, bytes, header.len);
    syncRestartConnection(pPeer);
    return;
  }

  if (header.type == TAOS_SMSG_FORWARD) {
    dTrace("%s peer:%s, forward received, contLen:%d", pNode->label, pPeer->ipstr, header.len);
    syncProcessForwardFromPeer(&header, pPeer);
  } else if (header.type == TAOS_SMSG_SYNC_REQ) {
    dTrace("%s peer:%s, sync req received", pNode->label, pPeer->ipstr);
    syncProcessSyncRequest(cont, pPeer);
  } else if (header.type == TAOS_SMSG_STATUS) {
    syncProcessPeerStatusMsg(cont, pPeer);
  }

  return;
}

static void syncSendStatusMsgToPeer(SSyncPeer *pPeer, char ack)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  int        msgLen;

  int size = sizeof(SSyncHead)+sizeof(SPeerStatus)+sizeof(SPeerState)*TAOS_SYNC_MAX_REPLICA;
  char *msg = (char *) calloc(1, size);

  SSyncHead   *pHead = (SSyncHead *) msg;
  SPeerStatus *pStatus = (SPeerStatus *) pHead->cont;

  pHead->type = TAOS_SMSG_STATUS;
  pHead->len = size - sizeof(SSyncHead);

  pStatus->version = pNode->version;
  pStatus->status = pNode->status;
  pStatus->ack = ack;

  pNode->peerInfo[pNode->selfIndex]->version = pNode->version;
  pNode->peerInfo[pNode->selfIndex]->status = pNode->status;
  for (int i = 0; i < pNode->replica; ++i) {
    pStatus->peerStates[i].status = pNode->peerInfo[i]->status;
    pStatus->peerStates[i].version = pNode->peerInfo[i]->version;
  }

  msgLen = size;

  pthread_mutex_lock(&(pNode->mutex));
  int retLen = write(pPeer->peerFd, msg, msgLen);
  pthread_mutex_unlock(&(pNode->mutex));
  if (retLen == msgLen) {
    dTrace("%s peer:%s, status is sent", pNode->label, pPeer->ipstr);
  } else {
    dTrace("%s peer:%s, failed to send status, restart connection", pNode->label, pPeer->ipstr);
    syncRestartConnection(pPeer);
  }

  free(msg);
  return;
}

static void syncCheckPeerConnection(void *param, void *tmrId) 
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0 ) return;

  SSyncNode *pNode = pPeer->pSyncNode;
  dTrace("%s peer:%s, check peer connection", pNode->label, pPeer->ipstr);

  taosTmrStopA(&pPeer->hbTimer);
  if (pPeer->peerFd >= 0) {
    dTrace("%s peer:%s, send status to peer", pNode->label,  pPeer->ipstr);
    syncSendStatusMsgToPeer(pPeer, 1);
    return;
  }

  int connFd = taosOpenTcpClientSocket(pPeer->ipstr, tsVnodeVnodePort, tsPrivateIp);
  if (connFd < 0) {
    dTrace("%s peer:%s, failed to open tcp socket, retry later", pNode->label, pPeer->ipstr);
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer *1000, pPeer, syncTmrCtrl, &pPeer->hbTimer);
    return;
  }

  taosKeepTcpAlive(connFd);

  SSyncHead firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.vgId = pNode->vgId;
  firstPkt.type = TAOS_SMSG_STATUS;

  if ( write(connFd, &firstPkt, sizeof(firstPkt)) == sizeof(firstPkt)) {
    dTrace("%s peer:%s, connection to peer server is setup", pNode->label, pPeer->ipstr);
    pPeer->peerFd = connFd;
    pPeer->pThread = taosAllocateTcpThread(&tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
  } else {
    close(connFd);
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer *1000, pPeer, syncTmrCtrl, &pPeer->hbTimer);
  }
}

static void syncCreateRestoreDataThread(SSyncPeer *pPeer) 
{
  SSyncNode *pNode = pPeer->pSyncNode;

  taosTmrStopA(&pPeer->syncTimer);

  pthread_attr_t thattr;
  pthread_t thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

  if (pthread_create(&(thread), &thattr, (void *)syncRestoreData, pPeer) < 0) {
    dError("%s peer:%s, failed to create sync thread, reason:%s", pNode->label, pPeer->ipstr);
    tclose(pPeer->syncFd);
  } else { 
    pthread_attr_destroy(&thattr);
    dPrint("%s peer:%s, sync connection is up", pNode->label, pPeer->ipstr);
  }
}

static void syncProcessIncommingConnection(int connFd, uint32_t sourceIp) 
{
  char  ipstr[24];
  int   i;
   
  tinet_ntoa(ipstr, sourceIp);
  dTrace("peer TCP connection from ip:%s", ipstr);

  SSyncHead firstPkt;
  if (taosReadMsg(connFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt)) {
    dError("failed to read peer first pkt from ip:%s, reason:%s", ipstr, strerror(errno));
    taosCloseTcpSocket(connFd);
    return;;
  }

  int32_t vgId = htonl(firstPkt.vgId);
  SSyncNode *pNode = taosGetIdHash(vgIdHash, vgId); 
  if (pNode == NULL) {
    dError("vgId:%d, vgId could not be found", vgId);
    taosCloseTcpSocket(connFd);
    return;
  }

  SSyncPeer *pPeer;
  for (i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer && pPeer->ip == sourceIp)
      break;
  }

  pPeer = (i < pNode->replica) ? pNode->peerInfo[i] : NULL;
  if (pPeer == NULL) {
    dError("%s, peer:%s not configured", pNode->label, ipstr);
    // syncSendVpeerCfgMsg(sync);
    taosCloseTcpSocket(connFd);
    return; 
  }

  // first packet tells what kind of link
  if (firstPkt.type == TAOS_SMSG_SYNC_DATA) {
    pPeer->syncFd = connFd;
    syncCreateRestoreDataThread(pPeer);
  } else {
    if (pPeer->peerFd >= 0) {
      dTrace("%s peer:%s, TCP connection is already up, close current one", pNode->label, pPeer->ipstr);
      taosFreeTcpThread(pPeer->pThread, &pPeer->peerFd);
      syncDecPeerRef(pPeer);
    }
 
    pPeer->peerFd = connFd;
    pPeer->pThread = taosAllocateTcpThread(tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
    dTrace("%s peer:%s, ready to exchange data", pNode->label, pPeer->ipstr);
    syncSendStatusMsgToPeer(pPeer, 0);
  }

  return;
}

static void syncProcessBrokenLink(void *param) {
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncNode *pNode = pPeer->pSyncNode;

  dTrace("%s peer:%s, TCP link is broken, reason:%s", pNode->label, pPeer->ipstr, strerror(errno));

  tclose(pPeer->peerFd);

  if (syncDecPeerRef(pPeer) != 0) 
    syncRestartConnection(pPeer);
}


