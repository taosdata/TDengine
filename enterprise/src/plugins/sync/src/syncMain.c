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
#include "ttime.h"
#include "tsocket.h"
#include "taoserror.h"
#include "taosTcpPool.h"
#include "taosHashId.h"
#include "tsync.h"
#include "syncInt.h"

// global configurable
int       tsMaxSyncNum = 4;
int       tsSyncTcpThreads = 2;
int       tsMaxWatchFiles = 100;
int       tsMaxFwdInfo = 200;
short     tsSyncPort = 6045;

// module global, not configurable
int       tsSyncNum;    // number of sync in process in whole system
int       tsNodeNum;    // number of nodes in system
uint32_t  tsSyncServerIp;
static ttpool_h       tsTcpPool;
static void          *syncTmrCtrl = NULL;
static void          *vgIdHash;
static pthread_once_t syncModuleInit = PTHREAD_ONCE_INIT;

// local functions
static void  syncProcessSyncRequest(char *pMsg, SSyncPeer *pPeer);
static void  syncSyncWithMaster(void *, void *);
static void  syncCheckPeerConnection(void *param, void *tmrId);
static void  syncSendPeersStatusMsgToPeer(SSyncPeer *pPeer, char ack);
static void  syncProcessBrokenLink(void *param);
static void  syncProcessPeerMsg(void *param, void *buffer);
static void  syncProcessIncommingConnection(int connFd, uint32_t sourceIp); 
static void  syncRemovePeer(SSyncPeer *pPeer);
static void  syncAddNodeRef(SSyncNode *pNode);
static void  syncAddPeerRef(SSyncPeer *pPeer);
static int   syncDecNodeRef(SSyncNode *pNode);
static int   syncDecPeerRef(SSyncPeer *pPeer);
static void  syncRemoveConfirmedFwdInfo(SSyncNode *pNode);
static void  syncMonitorFwdInfos(void *param, void *tmrId);
static void  syncProcessFwdAck(SSyncNode *pNode, SFwdInfo *pFwdInfo, int32_t code);
static void  syncSaveFwdInfo(SSyncNode *pNode, uint64_t version, void *mhandle); 
static SSyncPeer *syncAddPeer(SSyncNode *pNode, SNodeInfo *pInfo);

char* syncRole[] = {
  "offline",
  "unsynced",
  "slave",
  "master"
};

static void syncModuleInitFunc() {
  SPoolInfo info;

  tsSyncServerIp = inet_addr(tsPrivateIp);
  info.numOfThreads = tsSyncTcpThreads;
  info.serverIp = tsSyncServerIp;
  info.port = tsSyncPort;
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
    dError("failed to init TCP thread pool(%s)", strerror(errno));
    return NULL;
  }
    
  SSyncNode *pNode = (SSyncNode *) calloc(sizeof(SSyncNode), 1);
  
  pNode->selfIndex = -1;
  pNode->replica = pInfo->replica;
  pNode->quorum = pInfo->quorum;
  strcpy(pNode->label, pInfo->label);
  strcpy(pNode->path, pInfo->path);
  pNode->vgId = pInfo->vgId;
  pNode->getFileInfo = pInfo->getFileInfo;
  pNode->getWalInfo = pInfo->getWalInfo;
  pNode->writeToCache = pInfo->writeToCache;
  pNode->notifyRole = pInfo->notifyRole;
  pNode->confirmForward = pInfo->confirmForward;
  pNode->ahandle = pInfo->ahandle;
  pthread_mutex_init(&pNode->mutex, NULL);

  for (int i = 0; i < pInfo->replica; ++i) {
    pNode->peerInfo[i] = syncAddPeer(pNode, &pInfo->nodeInfo[i]);
    if (pInfo->nodeInfo[i].nodeIp == tsSyncServerIp) pNode->selfIndex = i;
  }

  if (pNode->selfIndex < 0) {
    dPrint("%s, this node is not configured", pNode->label);
    free (pNode);
    return NULL;
  }

  pNode->pSyncFwds = calloc(sizeof(SSyncFwds) + tsMaxFwdInfo*sizeof(SFwdInfo), 1);
  pNode->pFwdTimer = taosTmrStart(syncMonitorFwdInfos, 300, pNode, syncTmrCtrl);
  nodeVersion = pInfo->version;    // set the initial version
  nodeRole = (pInfo->replica > 1) ? TAOS_SYNC_ROLE_UNSYNCED : TAOS_SYNC_ROLE_MASTER;
  dPrint("%s, %d replicas are configured, role:%s", pNode->label, pNode->replica, syncRole[nodeRole]);

  atomic_add_fetch_32(&tsNodeNum, 1);
  syncAddNodeRef(pNode);
  taosAddIdHash(vgIdHash, pNode, pInfo->vgId);

  (*pNode->notifyRole)(pNode->ahandle, nodeRole);
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
  taosTmrStop(pNode->pFwdTimer);

  if (tsNodeNum <=0) {
    taosCloseTcpThreadPool(tsTcpPool);
    taosCloseIdHash(vgIdHash);
  }
}

int syncReconfig(void *param, SSyncInfo *pNewInfo) 
{
  SSyncNode  *pNode = (SSyncNode *)param;
  int         i, j;

  dPrint("%s, reconfig, role:%s replica:%d old:%d", pNode->label, syncRole[nodeRole], 
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

    if (pNewNode->nodeIp == tsSyncServerIp) pNode->selfIndex = i;
  }

  pNode->replica = pNewInfo->replica;
  pNode->quorum = pNewInfo->quorum;
  memcpy(pNode->peerInfo, newPeers, sizeof(SSyncPeer *) * pNewInfo->replica);

  for (i = pNewInfo->replica; i < TAOS_SYNC_MAX_REPLICA; ++i)
    pNode->peerInfo[i] = NULL;

  pNode->selfIndex = -1;
  for (i=0; i<pNode->replica; ++i) {
    if (pNewInfo->nodeInfo[i].nodeIp == tsSyncServerIp) pNode->selfIndex = i;
  }
    
  if (pNode->selfIndex <0) {
    dPrint("%s, this node is not configured", pNode->label);
    syncStop(pNode);
    return -1;
  }  

  if (pNewInfo->replica <= 1) {
    dPrint("%s, no peers are configured, work as master!", pNode->label);
    nodeRole = TAOS_SYNC_ROLE_MASTER;
    (*pNode->notifyRole)(pNode->ahandle, nodeRole);
  }

  syncBroadcastStatus(pNode);

  return 0;
}

int syncForwardToPeer(void *param, SWalHead *pWalHead, void *mhandle)
{
  SSyncNode  *pNode = (SSyncNode *)param;
  SSyncPeer  *pPeer;
  SSyncHead  *pSyncHead;
  int         fwdLen;
  int         code = 0;

  if (nodeRole != TAOS_SYNC_ROLE_MASTER) return -1;

  // always update version
  nodeVersion = pWalHead->version;

  // a hacker way to improve the performance
  pSyncHead = (SSyncHead *) ( ((char *)pWalHead) - sizeof(SSyncHead));
  pSyncHead->type = TAOS_SMSG_FORWARD;
  pSyncHead->pversion = 0;
  pSyncHead->len = sizeof(SWalHead) + pWalHead->len;
  fwdLen = pSyncHead->len + sizeof(SSyncHead);

  if (pNode->quorum > 1) syncSaveFwdInfo(pNode, pWalHead->version, mhandle);

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer == NULL || pPeer->peerFd <0) continue; 
    if (pPeer->role != TAOS_SYNC_ROLE_SLAVE && pPeer->sstatus != TAOS_SYNC_STATUS_CACHE) continue; 
  
    int retLen = write(pPeer->peerFd, pSyncHead, fwdLen);
    if (retLen == fwdLen) {
      dTrace("%s peer:%s, forward is sent, ver:%d len:%d", 
              pNode->label, pPeer->ipstr, pWalHead->version, pWalHead->len);
    } else {
      dError("%s peer:%s, failed to forward, ver:%d retLen:%d", 
              pNode->label, pPeer->ipstr, pWalHead->version, retLen);
      syncRestartConnection(pPeer);
    }
  }

  return code;
}

void syncConfirmForward(void *param, uint64_t version, int32_t code)
{
  SSyncNode  *pNode = (SSyncNode *)param;
  SSyncPeer  *pPeer = pNode->pMaster;
  char        msg[sizeof(SSyncHead) + sizeof(SFwdRsp)];

  if (pNode->quorum <= 1) return;
  if (pPeer == NULL) return;

  SSyncHead   *pHead = (SSyncHead *) msg;
  pHead->type = TAOS_SMSG_FORWARD_RSP;
  pHead->len = sizeof(SFwdRsp);

  SFwdRsp *pFwdRsp = (SFwdRsp *)pHead->cont;
  pFwdRsp->version = version;
  pFwdRsp->code = code;

  int msgLen = sizeof(SSyncHead) + sizeof(SFwdRsp);
  int retLen = write(pPeer->peerFd, msg, msgLen);

  if (retLen == msgLen) {
    dTrace("%s peer:%s, forward-rsp is sent, ver:%d ", pNode->label, pPeer->ipstr, version);
  } else {
    dTrace("%s peer:%s, failed to send forward ack, restart", pNode->label, pPeer->ipstr);
    syncRestartConnection(pPeer);
  }
}

void syncRecover(void *param) {
  SSyncNode *pNode = (SSyncNode *)param;
  SSyncPeer *pPeer;

  // to do: add a few lines to check if recover is OK 
  // if take this node to unsync state, the whole system may not work

  nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
  (*pNode->notifyRole)(pNode->ahandle, nodeRole);
  nodeVersion = 0;

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = (SSyncPeer *) pNode->peerInfo[i];
    if (pPeer->peerFd >= 0) {
      syncRestartConnection(pPeer);
    }
  }
}

int syncGetNodesRole(void *param, SNodesRole *pNodesRole)
{
  SSyncNode *pNode = (SSyncNode *)param;
  
  pNodesRole->selfIndex = pNode->selfIndex;
  for (int i=0; i<pNode->replica; ++i) {
    pNodesRole->nodeId[i] = pNode->peerInfo[i]->nodeId;
    pNodesRole->role[i] = pNode->peerInfo[i]->role;
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
    free (pNode->pSyncFwds);
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

    tfree(pPeer->watchFd);
    tfree(pPeer);
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
  taosTmrStopA(&pPeer->timer);
  tclose(pPeer->syncFd);
  tclose(pPeer->peerFd);

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
  pPeer->role = TAOS_SYNC_ROLE_OFFLINE;
  pPeer->pSyncNode = pNode;
  pPeer->refCount = 1;

  dPrint("%s peer:%s, %s is configured", pNode->label, pPeer->ipstr, pInfo->name);
  if (pInfo->nodeIp > tsSyncServerIp) {
    dTrace("%s peer:%s, start to check peer connection", pNode->label, pPeer->ipstr);
    taosTmrReset(syncCheckPeerConnection, 100, pPeer, syncTmrCtrl, &pPeer->timer);
  }

  syncAddPeerRef(pPeer);

  return pPeer;
}

void syncBroadcastStatus(SSyncNode *pNode)
{
  SSyncPeer *pPeer;

  for (int i = 0; i < pNode->replica; ++i) {
    if ( i == pNode->selfIndex ) continue;
    pPeer = pNode->peerInfo[i];
    syncSendPeersStatusMsgToPeer(pPeer, 1);
  }
} 

static void syncChooseMaster(SSyncNode *pNode) {
  SSyncPeer *pPeer;
  int8_t     onlineNum = 0;
  int8_t     index = -1;

  dTrace("%s, choose master", pNode->label);

  for (int i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->role != TAOS_SYNC_ROLE_OFFLINE)
      onlineNum++;
  }

  if ( onlineNum >= pNode->replica/2.0 ) {
    // over half of nodes are online
    for (int i = 0; i < pNode->replica; ++i) {
      //slave with highest version shall be master
      pPeer = pNode->peerInfo[i];
      if (pPeer->role == TAOS_SYNC_ROLE_SLAVE || pPeer->role == TAOS_SYNC_ROLE_MASTER) {
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
      nodeRole = TAOS_SYNC_ROLE_MASTER;
      (*pNode->notifyRole)(pNode->ahandle, nodeRole);
    } else {
      pPeer = pNode->peerInfo[index];
      dPrint("%s peer:%s, it shall work as master", pNode->label, pPeer->ipstr);
    }
  } else {
    dTrace("%s, failed to choose master", pNode->label);
  }
} 
 
static SSyncPeer *syncCheckMaster(SSyncNode *pNode ) {
  int offlineNum = 0;
  int index = -1;

  for (int i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->role == TAOS_SYNC_ROLE_OFFLINE) 
      offlineNum++;
  }

  if (offlineNum > pNode->replica * 0.5 ) {
    if (nodeRole != TAOS_SYNC_ROLE_UNSYNCED) {
      nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
      (*pNode->notifyRole)(pNode->ahandle, nodeRole);
      pNode->peerInfo[pNode->selfIndex]->role = nodeRole;
      dPrint("%s, offline:%d replica:%d, change to unsynced state", pNode->label, offlineNum, pNode->replica);
    }
  } else {
    for (int i=0; i<pNode->replica; ++i) {
      SSyncPeer *pTemp = pNode->peerInfo[i];
      if ( pTemp->role != TAOS_SYNC_ROLE_MASTER ) continue;
      if ( index < 0 ) {
        index = i;
      } else { // multiple masters, it shall not happen 
        if ( i == pNode->selfIndex ) {
          dError("%s, peer:%s: is master, work as slave instead", pNode->label, pTemp->ipstr);
          nodeRole = TAOS_SYNC_ROLE_SLAVE;
          (*pNode->notifyRole)(pNode->ahandle, nodeRole);
        }
      }
    }
  }

  SSyncPeer *pMaster = (index>=0) ? pNode->peerInfo[index]:NULL;
  return pMaster;
}

static void syncCheckRole(SSyncPeer *pPeer, SPeerStatus peersStatus[], int8_t newRole)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  int8_t     peerOldRole = pPeer->role;
  int8_t     selfOldRole = nodeRole;
  int8_t     i, syncRequired = 0;

  pthread_mutex_lock(&(pNode->mutex));

  pNode->peerInfo[pNode->selfIndex]->version = nodeVersion;
  pPeer->role = newRole;

  dTrace("%s peer:%s, own role:%s, new peer role:%s", pNode->label, pPeer->ipstr, 
          syncRole[nodeRole], syncRole[pPeer->role]);  

  SSyncPeer *pMaster = syncCheckMaster(pNode);

  if ( pMaster ) {
    // master is there
    if ( nodeRole == TAOS_SYNC_ROLE_UNSYNCED ) {
      if ( nodeVersion < pMaster->version) {
        syncRequired = 1;
      } else {
        dPrint("%s, peer:%s is master, work as slave, ver:%d", pNode->label, pMaster->ipstr, pMaster->version);
        nodeRole = TAOS_SYNC_ROLE_SLAVE;
        (*pNode->notifyRole)(pNode->ahandle, nodeRole);
      }
    } else if ( nodeRole == TAOS_SYNC_ROLE_SLAVE && pMaster == pPeer) {
      nodeVersion = pMaster->version;
    }
  } else {
    // master not there, if all peer's state and version are consistent, choose the master
    int consistent = 0;
    if (peersStatus) {
      for (i = 0; i < pNode->replica; ++i) {
        SSyncPeer *pTemp = pNode->peerInfo[i];
        if (pTemp->role != peersStatus[i].role) break;
        if ((pTemp->role != TAOS_SYNC_ROLE_OFFLINE) && (pTemp->version != peersStatus[i].version)) break; 
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

  if (peerOldRole != newRole || nodeRole != selfOldRole)
    syncBroadcastStatus(pNode);
}

void syncRestartConnection(SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncNode *pNode = pPeer->pSyncNode;

  dTrace("%s peer:%s, restart connection", pNode->label, pPeer->ipstr);
  tclose(pPeer->peerFd);
  tclose(pPeer->syncFd);
  taosTmrStopA(&pPeer->timer);

  pPeer->sstatus = TAOS_SYNC_STATUS_INIT;

  if (pPeer->ip > tsSyncServerIp)
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer*1000, pPeer, syncTmrCtrl, &pPeer->timer);

  syncCheckRole(pPeer, NULL, TAOS_SYNC_ROLE_OFFLINE);
}

static void syncProcessSyncRequest(char *msg, SSyncPeer *pPeer)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  dTrace("%s peer:%s, sync-req is received", pNode->label, pPeer->ipstr);

  if (pPeer->ip == 0) return;

  if (nodeRole != TAOS_SYNC_ROLE_MASTER) {
    dError("%s peer:%s, I am not master anymore", pNode->label, pPeer->ipstr);
    tclose(pPeer->syncFd);
    return;
  }

  if (pPeer->sstatus != TAOS_SYNC_STATUS_INIT) {
    dTrace("%s peer:%s, sync is already started", pNode->label, pPeer->ipstr);
    return; // already started
  }

  // start a new thread to retrieve the data
  pthread_attr_t  thattr;
  pthread_t       thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &thattr, syncRetrieveData, pPeer) != 0) {
    dError("%s peer:%s, failed to create sync thread(%s)", pNode->label, pPeer->ipstr, strerror(errno));
  } else {
    pPeer->sstatus = TAOS_SYNC_STATUS_START;
    dTrace("%s peer:%s, thread is created to retrieve data", pNode->label, pPeer->ipstr);
  }
}

static void syncNotStarted(void *param, void *tmrId)
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0) return;
  SSyncNode *pNode = pPeer->pSyncNode;

  pPeer->timer = NULL;
  dPrint("%s peer:%s, sync connection is still not up, restart", pNode->label, pPeer->ipstr);
  syncRestartConnection(pPeer);
}

static void syncSyncWithMaster(void *param, void *tmrId)
{
  SSyncPeer   *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0) return;
  SSyncNode   *pNode = pPeer->pSyncNode;

  if ( nodeSStatus != TAOS_SYNC_STATUS_INIT) {
    dTrace("%s peer:%s, sync is already started, status:%d", pNode->label, pPeer->ipstr, nodeSStatus);
    return;
  } 

  taosTmrStopA(&pPeer->timer);
  if (tsSyncNum >= tsMaxSyncNum) {
    dPrint("%s peer:%s, %d syncs are in process, try later", pNode->label, pPeer->ipstr, tsSyncNum);
    taosTmrReset(syncSyncWithMaster, 500, pPeer, syncTmrCtrl, &pPeer->timer);
    return;
  }

  dTrace("%s peer:%s, try to sync", pNode->label, pPeer->ipstr)

  SSyncHead firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.type = TAOS_SMSG_SYNC_REQ;
  firstPkt.vgId = pNode->vgId;
  taosTmrReset(syncNotStarted, tsVnodePeerHBTimer*1000, pPeer, syncTmrCtrl, &pPeer->timer);

  if (write(pPeer->peerFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt) ) {
    dError("%s peer:%s, failed to send sync-req to peer", pNode->label, pPeer->ipstr);
  } else {
    nodeSStatus = TAOS_SYNC_STATUS_START;
    dPrint("%s peer:%s, sync-req is sent", pNode->label, pPeer->ipstr);
  }

  return;
}

static void syncProcessFwdResponse(char *cont, SSyncPeer *pPeer) 
{
  SSyncNode  *pNode = pPeer->pSyncNode;
  SFwdRsp    *pFwdRsp = (SFwdRsp *) cont;
  SSyncFwds  *pSyncFwds = pNode->pSyncFwds;
  SFwdInfo   *pFwdInfo;


  pthread_mutex_lock(&(pNode->mutex));
  
  dTrace("%s peer:%s, forward-rsp is received, ver:%d ", pNode->label, pPeer->ipstr, pFwdRsp->version);

  SFwdInfo *pFirst = pSyncFwds->fwdInfo + pSyncFwds->first;

  if (pFirst->version <= pFwdRsp->version && pSyncFwds->fwds > 0) {
    // find the forwardInfo from first
    for (int i=0; i<pSyncFwds->fwds; ++i) {
      pFwdInfo = pSyncFwds->fwdInfo + (i+pSyncFwds->first)%tsMaxFwdInfo;
      if (pFwdRsp->version == pFwdInfo->version) break;
    }
 
    syncProcessFwdAck(pNode, pFwdInfo, pFwdRsp->code);
    syncRemoveConfirmedFwdInfo(pNode);
  }

  pthread_mutex_unlock(&(pNode->mutex));
}


static void syncProcessForwardFromPeer(char *cont, SSyncPeer *pPeer)
{
  SSyncNode   *pNode = pPeer->pSyncNode;
  SWalHead    *pHead = (SWalHead *)cont;

  dTrace("%s peer:%s, forward is received, ver:%d ", pNode->label, pPeer->ipstr, pHead->version);

  if (nodeRole == TAOS_SYNC_ROLE_SLAVE) {
    nodeVersion = pHead->version;
    pNode->pMaster = pPeer;
    (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_FWD);
    return;
  }

  pthread_mutex_lock(&pNode->mutex);

  if (nodeRole == TAOS_SYNC_ROLE_SLAVE) {
    nodeVersion = pHead->version;
    pNode->pMaster = pPeer;
    (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_FWD);
    return;
  } else { 
    if (nodeSStatus != TAOS_SYNC_STATUS_INIT) {
      syncSaveIntoBuffer(pPeer, pHead);
    } else {
      dError("%s peer:%s, forward discarded, ver:%d", pNode->label, pPeer->ipstr, pHead->version);
    }
  }

  pthread_mutex_unlock(&pNode->mutex);

  return;
}

static void syncProcessPeersStatusMsg(char *cont, SSyncPeer *pPeer)
{
  SSyncNode    *pNode = pPeer->pSyncNode;
  SPeersStatus *pPeersStatus = (SPeersStatus *)cont;

  dTrace("%s peer:%s, status msg received, self:%s ver:%d peer:%s ver:%d",
         pNode->label, pPeer->ipstr, syncRole[nodeRole], nodeVersion,
         syncRole[pPeersStatus->role], pPeersStatus->version, pPeersStatus->ack);

  pPeer->version = pPeersStatus->version;
  syncCheckRole(pPeer, pPeersStatus->peersStatus, pPeersStatus->role);

  if (pPeersStatus->ack)
    syncSendPeersStatusMsgToPeer(pPeer, 0);
}

static void syncProcessPeerMsg(void *param, void *buffer)
{
  SSyncPeer  *pPeer = (SSyncPeer *)param;
  SSyncHead   head;
  SSyncNode  *pNode = pPeer->pSyncNode;
  int         bytes = 0;
  char       *cont = (char *)buffer;

  if (pPeer->ip == 0) return;

  int hlen = taosReadMsg(pPeer->peerFd, &head, sizeof(head));
  if (hlen != sizeof(head)) {
    dTrace("%s peer:%s, failed to read msg, hlen:%d", pNode->label, pPeer->ipstr, hlen);
    syncRestartConnection(pPeer);
    return;
  }

  // head.len = htonl(head.len);
  if (head.len > TSDB_DEFAULT_PKT_SIZE || head.len <0) {
    dError("%s peer:%s, invalid pkt length, len:%d", pNode->label, pPeer->ipstr, head.len);
    syncRestartConnection(pPeer);
    return;
  } 

  bytes = taosReadMsg(pPeer->peerFd, cont, head.len);
  if (bytes != head.len) {
    dError("%s peer:%s, failed to read, bytes:%d len:%d", pNode->label, pPeer->ipstr, bytes, head.len);
    syncRestartConnection(pPeer);
    return;
  }

  if (head.type == TAOS_SMSG_FORWARD) {
    syncProcessForwardFromPeer(cont, pPeer);
  } else if (head.type == TAOS_SMSG_FORWARD_RSP) {
    syncProcessFwdResponse(cont, pPeer);
  } else if (head.type == TAOS_SMSG_SYNC_REQ) {
    syncProcessSyncRequest(cont, pPeer);
  } else if (head.type == TAOS_SMSG_STATUS) {
    syncProcessPeersStatusMsg(cont, pPeer);
  }

  return;
}

#define statusMsgLen sizeof(SSyncHead)+sizeof(SPeersStatus)+sizeof(SPeerStatus)*TAOS_SYNC_MAX_REPLICA

static void syncSendPeersStatusMsgToPeer(SSyncPeer *pPeer, char ack)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  char       msg[statusMsgLen];

  if (pPeer->peerFd <0 || pPeer->ip ==0) return;

  SSyncHead    *pHead = (SSyncHead *) msg;
  SPeersStatus *pPeersStatus = (SPeersStatus *) pHead->cont;

  pHead->type = TAOS_SMSG_STATUS;
  pHead->len = statusMsgLen - sizeof(SSyncHead);

  pPeersStatus->version = nodeVersion;
  pPeersStatus->role = nodeRole;
  pPeersStatus->ack = ack;

  for (int i = 0; i < pNode->replica; ++i) {
    pPeersStatus->peersStatus[i].role = pNode->peerInfo[i]->role;
    pPeersStatus->peersStatus[i].version = pNode->peerInfo[i]->version;
  }

  int retLen = write(pPeer->peerFd, msg, statusMsgLen);
  if (retLen == statusMsgLen) {
    dTrace("%s peer:%s, status msg is sent", pNode->label, pPeer->ipstr);
  } else {
    dTrace("%s peer:%s, failed to send status msg, restart", pNode->label, pPeer->ipstr);
    syncRestartConnection(pPeer);
  }

  return;
}

static void syncCheckPeerConnection(void *param, void *tmrId) 
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0 ) return;

  SSyncNode *pNode = pPeer->pSyncNode;
  dTrace("%s peer:%s, check peer connection", pNode->label, pPeer->ipstr);

  taosTmrStopA(&pPeer->timer);
  if (pPeer->peerFd >= 0) {
    dTrace("%s peer:%s, send role version to peer", pNode->label, pPeer->ipstr);
    syncSendPeersStatusMsgToPeer(pPeer, 1);
    return;
  }

  int connFd = taosOpenTcpClientSocket(pPeer->ipstr, tsVnodeVnodePort, tsPrivateIp);
  if (connFd < 0) {
    dTrace("%s peer:%s, failed to open tcp socket(%s)", pNode->label, pPeer->ipstr, strerror(errno));
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer *1000, pPeer, syncTmrCtrl, &pPeer->timer);
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
    pPeer->pThread = taosAllocateTcpThread(tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
  } else {
    close(connFd);
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer *1000, pPeer, syncTmrCtrl, &pPeer->timer);
  }
}

static void syncCreateRestoreDataThread(SSyncPeer *pPeer) 
{
  SSyncNode *pNode = pPeer->pSyncNode;

  taosTmrStopA(&pPeer->timer);

  pthread_attr_t thattr;
  pthread_t thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

  if (pthread_create(&(thread), &thattr, (void *)syncRestoreData, pPeer) < 0) {
    dError("%s peer:%s, failed to create sync thread(%s)", pNode->label, pPeer->ipstr);
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
    dError("failed to read peer first pkt from ip:%s(%s)", ipstr, strerror(errno));
    taosCloseTcpSocket(connFd);
    return;;
  }

  int32_t vgId = firstPkt.vgId;
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
      dTrace("%s peer:%s, TCP connection is already up, close one", pNode->label, pPeer->ipstr);
      taosFreeTcpThread(pPeer->pThread, &pPeer->peerFd);
      syncDecPeerRef(pPeer);
    }
 
    pPeer->peerFd = connFd;
    pPeer->pThread = taosAllocateTcpThread(tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
    dTrace("%s peer:%s, ready to exchange data", pNode->label, pPeer->ipstr);
    syncSendPeersStatusMsgToPeer(pPeer, 0);
  }

  return;
}

static void syncProcessBrokenLink(void *param) {
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncNode *pNode = pPeer->pSyncNode;

  dTrace("%s peer:%s, TCP link is broken(%s)", pNode->label, pPeer->ipstr, strerror(errno));

  tclose(pPeer->peerFd);

  if (syncDecPeerRef(pPeer) != 0) 
    syncRestartConnection(pPeer);
}

static void syncSaveFwdInfo(SSyncNode *pNode, uint64_t version, void *mhandle) 
{
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;
  uint64_t   time = taosGetTimestampMs();

  pthread_mutex_lock(&(pNode->mutex));

  if (pSyncFwds->fwds >= tsMaxFwdInfo) {
    pSyncFwds->first = (pSyncFwds->first + 1) % tsMaxFwdInfo;
    pSyncFwds->fwds--;
  } 

  if (pSyncFwds->fwds > 0) 
    pSyncFwds->last = (pSyncFwds->last+1) % tsMaxFwdInfo;
  SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + pSyncFwds->last;
  pFwdInfo->version = version;
  pFwdInfo->mhandle = mhandle;
  pFwdInfo->acks = 0;
  pFwdInfo->confirmed = 0;
  pFwdInfo->time = time;

  pSyncFwds->fwds++;
  dTrace("%s, fwd info is saved, ver:%d fwds:%d ", pNode->label, version, pSyncFwds->fwds);

  pthread_mutex_unlock(&(pNode->mutex));
}

static void syncRemoveConfirmedFwdInfo(SSyncNode *pNode)
{
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;

  int fwds = pSyncFwds->fwds;
  for (int i=0; i<fwds; ++i) {
    SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + pSyncFwds->first; 
    if (pFwdInfo->confirmed == 0) break;

    pSyncFwds->first = (pSyncFwds->first+1) % tsMaxFwdInfo;
    pSyncFwds->fwds--;
    if (pSyncFwds->fwds == 0) pSyncFwds->first = pSyncFwds->last;
    //dTrace("%s, fwd info is removed, ver:%d, fwds:%d", 
    //        pNode->label, pFwdInfo->version, pSyncFwds->fwds);
    memset(pFwdInfo, 0, sizeof(SFwdInfo));
  }
}

static void syncProcessFwdAck(SSyncNode *pNode, SFwdInfo *pFwdInfo, int32_t code) 
{
  int confirm = 0;
  if (pFwdInfo->code == 0) pFwdInfo->code = code;

  if (code == 0) {
    pFwdInfo->acks++;
    if (pFwdInfo->acks >= pNode->quorum-1) 
      confirm = 1;
  } else {
    pFwdInfo->nacks++;
    if (pFwdInfo->nacks > pNode->replica-pNode->quorum) 
      confirm = 1;
  }

  if (confirm && pFwdInfo->confirmed ==0) {
    dTrace("%s, forward is confirmed, ver:%d code:%x", pNode->label, pFwdInfo->version, pFwdInfo->code);
    (*pNode->confirmForward)(pNode->ahandle, pFwdInfo->mhandle, pFwdInfo->code);
    pFwdInfo->confirmed = 1;
  }
}

static void syncMonitorFwdInfos(void *param, void *tmrId)
{
  SSyncNode *pNode = (SSyncNode *)param;
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;
  uint64_t   time = taosGetTimestampMs();

  pthread_mutex_lock(&(pNode->mutex));

  if (pSyncFwds->fwds > 0) {

    for (int i=0; i<pSyncFwds->fwds; ++i) {
      SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + (pSyncFwds->first+i) % tsMaxFwdInfo; 
      if (time - pFwdInfo->time < 2000) break;
      syncProcessFwdAck(pNode, pFwdInfo, TSDB_CODE_NETWORK_UNAVAIL);
    }

    syncRemoveConfirmedFwdInfo(pNode);
  } 
  
  pthread_mutex_unlock(&(pNode->mutex));

  pNode->pFwdTimer = taosTmrStart(syncMonitorFwdInfos, 300, pNode, syncTmrCtrl);
}
 


