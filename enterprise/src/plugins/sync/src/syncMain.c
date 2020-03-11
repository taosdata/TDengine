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
#include "ttcpPool.h"
#include "tsync.h"
#include "syncInt.h"

// global configurable
uint32_t    tsPrivateIpv4;
int         tsMaxSyncNum = 4;
int         tsSyncTcpThreads = 2;

static void  syncProcessSyncRequest(char *pMsg, SSyncPeer *pPeer);
static void *syncRetrieveData(void *param);
static void  syncSyncWithMaster(void *, void *);
static void  syncCheckPeerConnection(void *param, void *tmrId);
static void  syncRestartConnection(SSyncPeer *pPeer);
static void  syncSendStatusMsgToPeer(SSyncPeer *pPeer, char ack);
static void  syncBroadcastStatus(SSyncObj *pObj);
static void  syncProcessBrokenLink(void *param);
static void  syncProcessPeerMsg(void *param, void *buffer);
static void  syncProcessIncommingConnection(int connFd, uint32_t sourceIp); 
static void  syncRemovePeer(SSyncPeer *pPeer);
static void  syncAddObjRef(SSyncObj *pObj);
static void  syncAddPeerRef(SSyncPeer *pPeer);
static int   syncDecObjRef(SSyncObj *pObj);
static int   syncDecPeerRef(SSyncPeer *pPeer);
static SSyncObj  *syncGetSyncObj(int32_t vgId);
static SSyncPeer *syncAddPeer(SSyncObj *pObj, SNodeInfo *pNode);

static pthread_once_t syncModuleInit = PTHREAD_ONCE_INIT;
static ttpool_h       tsTcpPool;
static void          *syncTmrCtrl = NULL;
static int            tsSyncNum;    // number of sync in process in whole system

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
}

void *syncStart(SSyncInfo *pInfo) 
{
  pthread_once(&syncModuleInit, syncModuleInitFunc); 

  if (tsTcpPool == NULL) {
    dError("failed to init TCP thread pool, reason:%s", strerror(errno));
    return NULL;
  }
    
  SSyncObj *pObj = (SSyncObj *) calloc(sizeof(SSyncObj), 1);
  
  pObj->replica = pInfo->replica;
  pObj->quorum = pInfo->quorum;
  strcpy(pObj->label, pInfo->label);
  pObj->vgId = pInfo->vgId;
  pObj->getFileInfo = pInfo->getFileInfo;
  pObj->getWalInfo = pInfo->getWalInfo;
  pObj->writeToCache = pInfo->writeToCache;
  pObj->status = (pInfo->replica > 1) ? TAOS_SYNC_STATUS_UNSYNCED : TAOS_SYNC_STATUS_MASTER;

  for (int i = 0; i < pInfo->replica; ++i) {
    pObj->peerInfo[i] = syncAddPeer(pObj, &pInfo->nodeInfo[i]);
  }

  dPrint("%s, %d replicas are configured, status:%s", pObj->label, pObj->replica, syncStatus[pObj->status]);

  atomic_add_fetch_32(&tsSyncNum, 1);
  syncAddObjRef(pObj);

  return pObj;
}

void syncStop(void *param) 
{
  SSyncObj   *pObj = (SSyncObj *)param;
  SSyncPeer  *pPeer;

  dPrint("%s, cleanup sync", pObj->label);

  for (int i = 0; i < pObj->replica; ++i) {
    pPeer = pObj->peerInfo[i];
    if (pPeer) syncRemovePeer(pPeer); 
  }

  syncDecObjRef(pObj);
  atomic_sub_fetch_32(&tsSyncNum, 1);
  if (tsSyncNum <0)
    taosCloseTcpThreadPool(tsTcpPool);
}

int syncReconfig(void *param, SSyncInfo *pNewInfo) 
{
  SSyncObj   *pObj = (SSyncObj *)param;
  int         i, j;

  dPrint("%s, reconfig, status:%s replica:%d old:%d", pObj->label, syncStatus[pObj->status], 
         pNewInfo->replica, pObj->replica);

  for (i = 0; i < pObj->replica; ++i) {
    for (j = 0; j < pNewInfo->replica; ++j) {
      if (pObj->peerInfo[i]->ip == pNewInfo->nodeInfo[j].nodeIp) 
        break;
    }

    if (j >= pNewInfo->replica) {
      syncRemovePeer(pObj->peerInfo[i]);
      pObj->peerInfo[i] = NULL;
    }
  }

  SSyncPeer *newPeers[TAOS_SYNC_MAX_REPLICA];
  for (i = 0; i < pNewInfo->replica; ++i) {
    SNodeInfo *pNewNode = &pNewInfo->nodeInfo[i];

    for (j = 0; j < pObj->replica; ++j) {
      if (pObj->peerInfo[j]->ip == pNewNode->nodeIp)
        break;
    }

    if (j >= pObj->replica) {
      newPeers[i] = syncAddPeer(pObj, pNewNode);
    } else {
      newPeers[i] = pObj->peerInfo[j];
    }

    if (pNewNode->nodeIp == tsPrivateIpv4) pObj->selfIndex = i;
  }

  pObj->replica = pNewInfo->replica;
  pObj->quorum = pNewInfo->quorum;
  memcpy(pObj->peerInfo, newPeers, sizeof(SSyncPeer *) * pNewInfo->replica);

  for (i = pNewInfo->replica; i < TAOS_SYNC_MAX_REPLICA; ++i)
    pObj->peerInfo[i] = NULL;

  if (pNewInfo->replica <= 1) {
    dPrint("%s, no peers are configured, work as master!", pObj->label);
    pObj->status = TAOS_SYNC_STATUS_MASTER;
  }

  syncBroadcastStatus(pObj);

  return 0;
}

int syncForwardToPeer(void *param, uint64_t version, char *cont, int contLen)
{
  SSyncObj   *pObj = (SSyncObj *)param;
  SSyncPeer  *pPeer;
  SSyncHead  *pHead;
  int         fwdLen;

  if (pObj->status != TAOS_SYNC_STATUS_MASTER) return -1;
 
  // a hacker way to improve the performance
  pHead = (SSyncHead *) (cont - sizeof(SSyncHead));
  pHead->type = TAOS_SMSG_FORWARD;
  pHead->pversion = 0;
  pHead->len = contLen;
  pHead->version = pObj->version;
  fwdLen = contLen + sizeof(SSyncHead);

  for (int i = 0; i < pObj->replica; ++i) {
    pPeer = pObj->peerInfo[i];
    if (pPeer == NULL || pPeer->peerFd <0) continue;
      
    pthread_mutex_lock(&(pObj->vmutex));
    int retLen = write(pPeer->peerFd, pHead, fwdLen);
    pthread_mutex_unlock(&(pObj->vmutex));
    if (retLen == fwdLen) {
      dTrace("%s peer:%s, forward is sent, contLen:%d", pObj->label, pPeer->ipstr, contLen);
    } else {
      dError("%s peer:%s, failed to forward, retLen:%d", pObj->label, pPeer->ipstr, retLen);
      syncRestartConnection(pPeer);
    }
  }

  return 0;
}


void syncRecover(void *param) {
  SSyncObj  *pObj = (SSyncObj *)param;
  SSyncPeer *pPeer;

  pObj->status = TAOS_SYNC_STATUS_UNSYNCED;
  pObj->version = 0;

  for (int i = 0; i < pObj->replica; ++i) {
    pPeer = (SSyncPeer *) pObj->peerInfo[i];
    if (pPeer->peerFd >= 0) {
      syncRestartConnection(pPeer);
    }
  }
}

int syncGetStatus(void *param, SSyncStatus *pStatus)
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncObj  *pObj = pPeer->pSyncObj;
  
  pStatus->selfIndex = pObj->selfIndex;
  for (int i=0; i<pObj->replica; ++i) {
    pStatus->nodeId[i] = pObj->peerInfo[i]->nodeId;
    pStatus->status[i] = pObj->peerInfo[i]->status;
  }

  return 0;
}

static int syncRetrieveFile(SSyncPeer *pPeer)
{
  SSyncObj   *pObj = pPeer->pSyncObj;
  int32_t     size, ret;
  SFileInfo   fileInfo;
  SFileAck    fileAck;
  int         code = -1;

  fileInfo.index = 0;

  while (1) {
    // retrieve file info
    fileInfo.name[0] = 0;
    fileInfo.magic = (*pObj->getFileInfo)(fileInfo.name, &fileInfo.index, &size);   
    fileInfo.index = htonl(fileInfo.index);
    fileInfo.size = htonl(size);

    // send the file info
    ret = taosWriteMsg(pPeer->syncFd, &(fileInfo), sizeof(fileInfo));
    if (ret < 0 ) break;

    // if no file anymore, break
    if (fileInfo.name[0] == 0) { code = 0; break; }

    // wait for the ack from peer
    ret = taosReadMsg(pPeer->syncFd, &(fileAck), sizeof(fileAck));
    if (ret <0)  break;

    // if sync is not required, continue
    if (fileAck.sync == 0) continue; 

    // send the file to peer
    int sfd = open(fileInfo.name, O_RDONLY);
    if ( sfd < 0 ) break;

    ret = tsendfile(pPeer->syncFd, sfd, NULL, size); 
    close(sfd); 
    if (ret <0) break;

    dTrace("%s peer:%s, %s is sent, size:%d", pObj->label, pPeer->ipstr, fileInfo.name, size);    
    fileInfo.index++; 
  }

  if (code<0) {
    dError("%s peer:%s, failed to send %s, reason:%s", pObj->label, pPeer->ipstr, strerror(errno));
  }

  return code;
}

static int syncRestoreFile(SSyncPeer *pPeer) 
{
  SSyncObj  *pObj = pPeer->pSyncObj;
  SFileInfo  minfo, sinfo;
  SFileAck   fileAck;
  int        code = -1;

  while (1) {
    // read file info
    int ret = taosReadMsg(pPeer->syncFd, &(minfo), sizeof(minfo));
    if (ret < 0 ) break;

    // if no more file, break;
    if (minfo.name[0] == 0) {code = 0; break;}
   
    fileAck.sync = 0;
    minfo.index = htonl(minfo.index);
    minfo.size = htonl(minfo.size);

    // check the file info
    strcpy(sinfo.name, minfo.name);
    sinfo.magic = (*pObj->getFileInfo)(sinfo.name, &sinfo.index, &sinfo.size);
    if (sinfo.magic != minfo.magic || sinfo.name[0] == 0) fileAck.sync =1;

    // send file ack
    ret = taosWriteMsg(pPeer->syncFd, &(fileAck), sizeof(fileAck));
    if (ret <0)  break;
 
    // if sync is not required, continue
    if (fileAck.sync == 0) continue;

    // if sync is requred, open file, receive from master, and write to file
    int dfd = open(sinfo.name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if ( dfd < 0 ) {
      dError("%s peer:%s, failed to open file:%s", pObj->label, pPeer->ipstr, minfo.name);
      break;
    }

    ret = taosCopyFds(pPeer->syncFd, dfd, minfo.size);
    close(dfd);
    if (ret<0) break;

    dTrace("%s peer:%s, %s is received, size:%d", pObj->label, pPeer->ipstr, minfo.name, minfo.size);
  }

  if (code<0) {
    dError("%s peer:%s, failed to recv %s, reason:%s", pObj->label, pPeer->ipstr, strerror(errno));
  }

  return code;
}

static int syncRetrieveWal(SSyncPeer *pPeer)
{
  SSyncObj   *pObj = pPeer->pSyncObj;
  char        name[TSDB_FILENAME_LEN];
  int32_t     size, ret;
  struct stat fstat;
  int         code = -1;
  int         index = 0;

  while (1) {
    // retrieve wal info
    name[0] = 0;
    ret = (*pObj->getWalInfo)(name, &index);   

    // if no wal file anymore, break
    if (name[0] == 0) { 
      SWalHead walHead;
      memset(&walHead, 0, sizeof(walHead));
      code = taosWriteMsg(pPeer->syncFd, &walHead, sizeof(walHead));
      break; 
    }

    // send WAL file
    if ( stat(name, &fstat) < 0 ) break;
    size = fstat.st_size;

    int sfd = open(name, O_RDONLY);
    if (sfd < 0) break;

    ret = tsendfile(pPeer->syncFd, sfd, NULL, size); 
    close(sfd); 
    if (ret <0) break;

    dTrace("%s peer:%s, wal:%s is sent, size:%d", pObj->label, pPeer->ipstr, name, size);    
    index++; 
  }

  if (code < 0) {
    dError("%s peer:%s, failed to send %s, reason:%s", pObj->label, pPeer->ipstr, strerror(errno));
  }

  return code;
}

static int syncRestoreWal(SSyncPeer *pPeer)
{
  SSyncObj   *pObj = pPeer->pSyncObj;
  int         ret, code = -1;
  SWalHead    walHead;

  void *buffer = malloc(1024000);
  if (buffer == NULL) return -1;

  while (1) {
    ret = taosReadMsg(pPeer->syncFd, &(walHead), sizeof(walHead));
    if (ret <0)  break;

    if (walHead.len == 0) {code = 0; break;}  // wal sync over
    
    ret = taosReadMsg(pPeer->syncFd, buffer, walHead.len);
    if (ret <0)  break;

    (*pObj->writeToCache)(pObj->ahandle, walHead.version, walHead.cont, walHead.len);
  }

  if (code<0) {
    dError("%s peer:%s, failed to read WAL, reason:%s", pObj->label, pPeer->ipstr, strerror(errno));
  }

  free(buffer);
  return code;
}

static void syncAddObjRef(SSyncObj *pObj)
{
   atomic_add_fetch_8(&pObj->refCount, 1);
}

static int syncDecObjRef(SSyncObj *pObj)
{
  if (atomic_sub_fetch_8(&pObj->refCount, 1) == 0) {
    free (pObj);
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
    syncDecObjRef(pPeer->pSyncObj);
    free (pPeer);
    return 0;
  }

  return 1;
}

static void syncRemovePeer(SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncObj  *pObj = pPeer->pSyncObj;

  dPrint("%s peer:%s, it is removed", pObj->label, pPeer->ipstr);

  pPeer->ip = 0;
  taosTmrStopA(&pPeer->hbTimer);
  taosTmrStopA(&pPeer->syncTimer);
  tclose(pPeer->syncFd);
  tclose(pPeer->peerFd);

  syncDecPeerRef(pPeer);
}

static SSyncPeer *syncAddPeer(SSyncObj *pObj, SNodeInfo *pNode) 
{
  SSyncPeer *pPeer = (SSyncPeer *) calloc(1, sizeof(SSyncPeer));

  pPeer->ip = pNode->nodeIp;
  tinet_ntoa(pPeer->ipstr, pNode->nodeIp);
  pPeer->nodeId = pNode->nodeId;

  pPeer->peerFd = -1;
  pPeer->syncFd = -1;
  pPeer->status = TAOS_SYNC_STATUS_OFFLINE;
  pPeer->pSyncObj = pObj;
  pPeer->refCount = 1;
  dPrint("%s peer:%s, %s is configured", pObj->label, pPeer->ipstr, pNode->name);
  if (pNode->nodeIp > tsPrivateIpv4) {
    dTrace("%s peer:%s, start to check peer connection", pObj->label, pPeer->ipstr);
    taosTmrReset(syncCheckPeerConnection, 0, pPeer, syncTmrCtrl, &pPeer->hbTimer);
  }

  syncAddPeerRef(pPeer);

  return pPeer;
}

static void syncBroadcastStatus(SSyncObj *pObj)
{
  SSyncPeer *pPeer;

  for (int i = 0; i < pObj->replica; ++i) {
    pPeer = pObj->peerInfo[i];
    syncSendStatusMsgToPeer(pPeer, 1);
  }
} 

static void syncChooseMaster(SSyncObj *pObj) {
  SSyncPeer *pPeer;
  int8_t     onlineNum = 0;
  int8_t     index = -1;

  dPrint("%p, choose master", pObj->label);

  for (int i = 0; i < pObj->replica; ++i) {
    if (pObj->peerInfo[i]->status != TAOS_SYNC_STATUS_OFFLINE)
      onlineNum++;
  }

  if ( onlineNum >= (pObj->replica+0.5)/2 ) {
    // over half of nodes are online
    for (int i = 0; i < pObj->replica; ++i) {
      //slave with highest version shall be master
      pPeer = pObj->peerInfo[i];
      if (pPeer->status == TAOS_SYNC_STATUS_SLAVE || pPeer->status == TAOS_SYNC_STATUS_MASTER) {
        if (index < 0 || pPeer->version > pObj->peerInfo[index]->version)
          index = i;
      }
    }
  }

  if (index < 0 && onlineNum == pObj->replica) {
    // if all peers are online, peer with highest version shall be master
    index = 0;
    for (int i = 1; i < pObj->replica; ++i) {
      if (pObj->peerInfo[i]->version > pObj->peerInfo[index]->version)
        index = i;
    }
  }

  if (index >= 0) {
    if (index == pObj->selfIndex) {
      dPrint("%s, start to work as master", pObj->label);
      pObj->status = TAOS_SYNC_STATUS_MASTER;
    } else {
      pPeer = pObj->peerInfo[index];
      dPrint("%s peer:%s, it shall work as master", pObj->label, pPeer->ipstr);
    }
  } else {
    dPrint("%s, failed to choose master", pObj->label);
  }
} 
 
static SSyncPeer *syncCheckMaster(SSyncObj *pObj ) {
  int offlineNum = 0;
  int index = -1;

  for (int i = 0; i < pObj->replica; ++i) {
    if (pObj->peerInfo[i]->status == TAOS_SYNC_STATUS_OFFLINE) 
      offlineNum++;
  }

  if (offlineNum > pObj->replica * 0.5 ) {
    if (pObj->status != TAOS_SYNC_STATUS_UNSYNCED) {
      pObj->status = TAOS_SYNC_STATUS_UNSYNCED;
      pObj->peerInfo[pObj->selfIndex]->status = pObj->status;
      dPrint("%s, offline:%d replica:%d, change to unsynced state", pObj->label, offlineNum, pObj->replica);
    }
  } else {
    for (int i=0; i<pObj->replica; ++i) {
      SSyncPeer *pTemp = pObj->peerInfo[i];
      if ( pTemp->status != TAOS_SYNC_STATUS_MASTER ) continue;
      if ( index < 0 ) {
        index = i;
      } else { // multiple masters, it shall not happen 
        if ( i == pObj->selfIndex ) {
          dError("%s, peer:%s: is master, work as slave instead", pObj->label, pTemp->ipstr);
          pObj->status = TAOS_SYNC_STATUS_SLAVE;
        }
      }
    }
  }

  SSyncPeer *pMaster = (index>=0) ? pObj->peerInfo[index]:NULL;
  return pMaster;
}

static void syncCheckStatus(SSyncPeer *pPeer, SPeerState peerStates[], int8_t newState)
{
  SSyncObj *pObj = pPeer->pSyncObj;
  int8_t    peerOldState = pPeer->status;
  int8_t    selfOldState = pObj->status;
  int8_t    i, syncRequired = 0;

  pthread_mutex_lock(&(pObj->vmutex));

  pObj->peerInfo[pObj->selfIndex]->version = pObj->version;
  pObj->peerInfo[pObj->selfIndex]->status = pObj->status;
  pPeer->status = newState;

  dTrace("%s peer:%s, own status:%s, new peer status:%s", pObj->label, pPeer->ipstr, 
          syncStatus[pObj->status], syncStatus[pPeer->status]);  

  SSyncPeer *pMaster = syncCheckMaster(pObj);

  if ( pMaster ) {
    // master is there
    if ( pObj->status == TAOS_SYNC_STATUS_UNSYNCED ) {
      if ( pObj->version < pMaster->version) {
        syncRequired = 1;
      } else {
        dPrint("%s, peer:%s is master, work as slave, version:%d", pObj->label, pMaster->ipstr, pMaster->version);
        pObj->status = TAOS_SYNC_STATUS_SLAVE;
      }
    } else if ( pObj->status == TAOS_SYNC_STATUS_SLAVE && pMaster == pPeer) {
      pObj->version = pMaster->version;
    }
  } else {
    // master not there, if all peer's state and version are consistent, choose the master
    int consistent = 0;
    if (peerStates) {
      for (i = 0; i < pObj->replica; ++i) {
        SSyncPeer *pTemp = pObj->peerInfo[i];
        if (pTemp->status != peerStates[i].status) break;
        if ((pTemp->status != TAOS_SYNC_STATUS_OFFLINE) && (pTemp->version != peerStates[i].version)) break; 
      }
 
      if (i >= pObj->replica) consistent = 1;
    } 

    if (consistent || pObj->replica < 3)
      syncChooseMaster(pObj);
  }

  pthread_mutex_unlock(&(pObj->vmutex));

  if (syncRequired) {
    syncSyncWithMaster(pMaster, NULL);
  }

  if (peerOldState != newState || pObj->status != selfOldState)
    syncBroadcastStatus(pObj);
}

static void syncRestartConnection(SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncObj *pObj = pPeer->pSyncObj;

  dTrace("%s peer:%s, restart connection", pObj->label, pPeer->ipstr);
  tclose(pPeer->peerFd);

  if (pPeer->ip > tsPrivateIpv4)
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer*1000, pPeer, syncTmrCtrl, &pPeer->hbTimer);

  syncCheckStatus(pPeer, NULL, TAOS_SYNC_STATUS_OFFLINE);
}

static void syncProcessSyncRequest(char *msg, SSyncPeer *pPeer)
{
  if (pPeer->ip == 0) return;
  SSyncObj *pObj = pPeer->pSyncObj;

  // start a new thread to retrieve the data
 
  pthread_attr_t  thattr;
  pthread_t       thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &thattr, syncRetrieveData, pPeer) != 0) {
    dError("%s peer:%s, failed to create sync thread, reason:%s", pObj->label, pPeer->ipstr, strerror(errno));
  }
}

static void syncNotStarted(void *param, void *tmrId)
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0) return;
  SSyncObj  *pObj = pPeer->pSyncObj;

  dPrint("%s peer:%s, sync connection is still not up, restart", pObj->label, pPeer->ipstr);
  syncRestartConnection(pPeer);
}

static void syncSyncWithMaster(void *param, void *tmrId)
{
  SSyncPeer   *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0) return;
  SSyncObj    *pObj = pPeer->pSyncObj;

  taosTmrStopA(&pPeer->hbTimer);
  dPrint("%s peer:%s, try to sync", pObj->label, pPeer->ipstr)

  if (tsSyncNum >= tsMaxSyncNum) {
    dPrint("%s peer:%s, %d syncs are in process, try later", pObj->label, pPeer->ipstr, tsSyncNum);
    pPeer->hbTimer = taosTmrStart(syncSyncWithMaster, 500, pPeer, syncTmrCtrl);
    return;
  }

  SSyncHead firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.type = TAOS_SMSG_SYNC_REQ;
  pPeer->syncTimer = taosTmrStart(syncNotStarted, tsVnodePeerHBTimer*1000, pPeer, syncTmrCtrl);

  if (write(pPeer->peerFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt) ) {
    dError("%s peer:%s, failed to send sync req to peer", pObj->label, pPeer->ipstr);
  } else {
    dPrint("%s peer:%s, sync req is sent", pObj->label, pPeer->ipstr);
  }

  return;
}

static char *syncProcessOneBufferedFwd(SSyncObj *pObj, char *offset)
{
  SSyncHead *pHead = (SSyncHead *) offset;
  int        contLen = pHead->len;

  (*pObj->writeToCache)(pObj->ahandle, pHead->version, pHead->cont, pHead->len);
  offset += contLen + sizeof(SSyncHead);

  return offset;
}

static int syncProcessBufferedFwd(SSyncObj *pObj)
{
  SRecvBuffer *pRecv = pObj->pRecv;
  int          forwards = 0;
  char        *offset = NULL;

  offset = pRecv->buffer;
  while (forwards < pRecv->forwards) {
    offset = syncProcessOneBufferedFwd(pObj, offset);
    forwards++;
  }
  
  pthread_mutex_lock(&pRecv->mutex);

  while (forwards < pRecv->forwards && pRecv->code == 0) {
    offset = syncProcessOneBufferedFwd(pObj, offset);
    forwards++;
  }

  pthread_mutex_unlock(&pRecv->mutex);

  return pRecv->code;
}

static int syncSaveIntoBuffer(SRecvBuffer *pRecv, SSyncHead *pHead)
{
  int contLen = pHead->len;

  pthread_mutex_lock(&pRecv->mutex);

  if (pRecv->bufferSize - (pRecv->offset - pRecv->buffer) > contLen + 100) {
    memcpy(pRecv->offset, pHead, sizeof(SSyncHead));
    pRecv->offset += sizeof(SSyncHead);
    memcpy(pRecv->offset, pHead->cont, contLen);
    pRecv->offset += contLen;
    pRecv->forwards++;
  } else {
    pRecv->code = -1;  // set error code
  }

  pthread_mutex_unlock(&pRecv->mutex);

  return pRecv->code;
}

static void syncProcessForwardFromPeer(SSyncHead *pHead, SSyncPeer *pPeer)
{
  SSyncObj    *pObj = pPeer->pSyncObj;
  SRecvBuffer *pRecv = pObj->pRecv;

  if (pObj->status == TAOS_SYNC_STATUS_SLAVE) {
    pObj->version = pHead->version;
    (*pObj->writeToCache)(pObj->ahandle, pHead->version, pHead->cont, pHead->len);
    return;
  }

  if (pObj->status == TAOS_SYNC_STATUS_CACHE) {
    if (syncSaveIntoBuffer(pRecv, pHead) == 0) {
      dTrace("%s peer:%s, forward is saved into sync queue", pObj->label, pPeer->ipstr);
    } else {
      dError("%s peer:%s, failed to save into sync queue", pObj->label, pPeer->ipstr);
    }
  } else {
    dTrace("%s peer:%s, forward not processed, state:%s", pObj->label, pPeer->ipstr, syncStatus[pObj->status]);
    return;
  }

}

static void syncProcessPeerStatusMsg(char *cont, SSyncPeer *pPeer)
{
  SSyncObj    *pObj = pPeer->pSyncObj;
  SPeerStatus *pStatus = (SPeerStatus *)cont;

  dTrace("%s peer:%s, status received, self:%s version:%d peer:%s version:%d",
         pObj->label, pPeer->ipstr, syncStatus[pObj->status], pObj->version,
         syncStatus[pStatus->status], pStatus->version, pStatus->ack);

  pPeer->version = pStatus->version;
  syncCheckStatus(pPeer, pStatus->peerStates, pStatus->status);

  if (pStatus->ack)
    syncSendStatusMsgToPeer(pPeer, 0);
}

static void syncSendStatusMsgToPeer(SSyncPeer *pPeer, char ack)
{
  SSyncObj *pObj = pPeer->pSyncObj;
  int       msgLen;

  int size = sizeof(SSyncHead)+sizeof(SPeerStatus)+sizeof(SPeerState)*TAOS_SYNC_MAX_REPLICA;
  char *msg = (char *) calloc(1, size);

  SSyncHead   *pHead = (SSyncHead *) msg;
  SPeerStatus *pStatus = (SPeerStatus *) pHead->cont;

  pHead->type = TAOS_SMSG_STATUS;
  pHead->len = size - sizeof(SSyncHead);

  pStatus->version = pObj->version;
  pStatus->status = pObj->status;
  pStatus->ack = ack;

  pObj->peerInfo[pObj->selfIndex]->version = pObj->version;
  pObj->peerInfo[pObj->selfIndex]->status = pObj->status;
  for (int i = 0; i < pObj->replica; ++i) {
    pStatus->peerStates[i].status = pObj->peerInfo[i]->status;
    pStatus->peerStates[i].version = pObj->peerInfo[i]->version;
  }

  msgLen = size;

  pthread_mutex_lock(&(pObj->vmutex));
  int retLen = write(pPeer->peerFd, msg, msgLen);
  pthread_mutex_unlock(&(pObj->vmutex));
  if (retLen == msgLen) {
    dTrace("%s peer:%s, status is sent", pObj->label, pPeer->ipstr);
  } else {
    dTrace("%s peer:%s, failed to send status, restart connection", pObj->label, pPeer->ipstr);
    syncRestartConnection(pPeer);
  }

  free(msg);
  return;
}

static void syncProcessPeerMsg(void *param, void *buffer)
{
  SSyncPeer  *pPeer = (SSyncPeer *)param;
  SSyncHead   header;
  SSyncObj   *pObj = pPeer->pSyncObj;
  int         bytes = 0;
  char       *cont = (char *)buffer;

  if (pPeer->ip == 0) return;

  int hlen = taosReadMsg(pPeer->peerFd, &header, sizeof(header));
  if (hlen != sizeof(header)) {
    dTrace("%s peer:%s, failed to read msg, hlen:%d", pObj->label, pPeer->ipstr, hlen);
    syncRestartConnection(pPeer);
    return;
  }

  header.len = htonl(header.len);
  if (header.len > TSDB_DEFAULT_PKT_SIZE || header.len <0) {
    dError("%s peer:%s, invalid pkt length, len:%d", pObj->label, pPeer->ipstr, header.len);
    syncRestartConnection(pPeer);
    return;
  } 

  bytes = taosReadMsg(pPeer->peerFd, cont, header.len);
  if (bytes != header.len) {
    dError("%s peer:%s, failed to read, bytes:%d len:%d", pObj->label, pPeer->ipstr, bytes, header.len);
    syncRestartConnection(pPeer);
    return;
  }

  if (header.type == TAOS_SMSG_FORWARD) {
    dTrace("%s peer:%s, forward received, contLen:%d", pObj->label, pPeer->ipstr, header.len);
    syncProcessForwardFromPeer(&header, pPeer);
  } else if (header.type == TAOS_SMSG_SYNC_REQ) {
    dTrace("%s peer:%s, sync req received", pObj->label, pPeer->ipstr);
    syncProcessSyncRequest(cont, pPeer);
  } else if (header.type == TAOS_SMSG_STATUS) {
    syncProcessPeerStatusMsg(cont, pPeer);
  }

  return;
}

static void syncCheckPeerConnection(void *param, void *tmrId) 
{
  SSyncPeer *pPeer = (SSyncPeer *)param;
  if (pPeer->ip == 0 ) return;

  SSyncObj  *pObj = pPeer->pSyncObj;
  dTrace("%s peer:%s, check peer connection", pObj->label, pPeer->ipstr);

  taosTmrStopA(&pPeer->hbTimer);
  if (pPeer->peerFd >= 0) {
    dTrace("%s peer:%s, send status to peer", pObj->label,  pPeer->ipstr);
    syncSendStatusMsgToPeer(pPeer, 1);
    return;
  }

  int connFd = taosOpenTcpClientSocket(pPeer->ipstr, tsVnodeVnodePort, tsPrivateIp);
  if (connFd < 0) {
    dTrace("%s peer:%s, failed to open tcp socket, retry later", pObj->label, pPeer->ipstr);
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer *1000, pPeer, syncTmrCtrl, &pPeer->hbTimer);
    return;
  }

  taosKeepTcpAlive(connFd);

  SSyncHead firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.vgId = pObj->vgId;
  firstPkt.type = TAOS_SMSG_STATUS;

  if ( write(connFd, &firstPkt, sizeof(firstPkt)) == sizeof(firstPkt)) {
    dTrace("%s peer:%s, connection to peer server is setup", pObj->label, pPeer->ipstr);
    pPeer->peerFd = connFd;
    pPeer->pThread = taosAllocateTcpThread(&tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
  } else {
    close(connFd);
    taosTmrReset(syncCheckPeerConnection, tsVnodePeerHBTimer *1000, pPeer, syncTmrCtrl, &pPeer->hbTimer);
  }
}

static void syncCloseRecvBuffer(SRecvBuffer *pRecv)
{
  if (pRecv) {
    free(pRecv->buffer);
    pthread_mutex_destroy(&pRecv->mutex);
  }
}

static int syncOpenRecvBuffer(SSyncObj *pObj) 
{
  syncCloseRecvBuffer(pObj->pRecv);

  SRecvBuffer *pRecv = calloc(sizeof(SRecvBuffer), 1);
  if (pRecv == NULL) return -1;

  pRecv->bufferSize = 1024000;
  pRecv->buffer = malloc(pRecv->bufferSize);
  if (pRecv->buffer == NULL) return -1;

  pRecv->offset = pRecv->buffer;
  pRecv->forwards = 0;
  pthread_mutex_init(&pRecv->mutex, NULL);

  pObj->pRecv = pRecv;

  return 0;
}

static int syncRestoreDataStepByStep(SSyncPeer *pPeer)
{
  SSyncObj *pObj = pPeer->pSyncObj;

  dTrace("%s peer:%s, start to restore", pObj->label, pPeer->ipstr);

  pObj->status = TAOS_SYNC_STATUS_FILE;
  dTrace("%s peer:%s, start to restore file", pObj->label, pPeer->ipstr);
  if (syncRestoreFile(pPeer) < 0) {
    dError("%s peer:%s, failed to restore file", pObj->label, pPeer->ipstr);
    return -1;
  }

  dTrace("%s peer:%s, start to restore WAL", pObj->label, pPeer->ipstr);
  if (syncRestoreWal(pPeer) < 0) {
    dError("%s peer:%s, failed to restore WAL", pObj->label, pPeer->ipstr);
    return -1;
  }

  pObj->status = TAOS_SYNC_STATUS_CACHE;
  dTrace("%s peer:%s, start to insert buffered points", pObj->label, pPeer->ipstr);
  if (syncProcessBufferedFwd(pObj) < 0) {
    dError("%s peer:%s, failed to insert buffered points", pObj->label, pPeer->ipstr);
    return -1;
  }

  return 0;
}

static void *syncRestoreData(void *param)
{
  SSyncPeer  *pPeer = (SSyncPeer *)param;
  SSyncObj   *pObj = pPeer->pSyncObj;

  if (syncOpenRecvBuffer(pObj) < 0) {
    dError("%s peer:%s, failed to allocate recv buffer", pObj->label, pPeer->ipstr);
    tclose(pPeer->syncFd)
    return NULL;
  } 

  taosBlockSIGPIPE();
  __sync_fetch_and_add(&tsSyncNum, 1);

  if ( syncRestoreDataStepByStep(pPeer) == 0) {
    dPrint("%s peer:%s, it is synced successfully", pObj->label, pPeer->ipstr);
    pObj->status = TAOS_SYNC_STATUS_SLAVE;
    syncBroadcastStatus(pObj);
  } else {
    dError("%s peer:%s, failed to restore data, restart connection", pObj->label, pPeer->ipstr);
    pObj->status = TAOS_SYNC_STATUS_UNSYNCED;
    syncRestartConnection(pPeer);
  }

  tclose(pPeer->syncFd)
  syncCloseRecvBuffer(pObj->pRecv);

  __sync_fetch_and_sub(&tsSyncNum, 1);

  return NULL;
}

static int syncRetrieveDataStepByStep(SSyncPeer *pPeer)
{
  SSyncObj  *pObj = pPeer->pSyncObj;
  SSyncHead  firstPkt;

  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.type = TAOS_SMSG_SYNC_DATA;
  firstPkt.vgId = pObj->vgId;

  dTrace("%s peer:%s, start to retrieve data", pObj->label, pPeer->ipstr);
  if (write(pPeer->syncFd, (char *) &firstPkt, sizeof(firstPkt)) < 0) {
    dError("%s peer:%s, failed to send syncCmd", pObj->label, pPeer->ipstr);
    return -1;
  }

  pPeer->status = TAOS_SYNC_STATUS_FILE;
  dTrace("%s peer:%s, start to retrieve file", pObj->label, pPeer->ipstr);
  if (syncRetrieveFile(pPeer) < 0) {
    dError("%s peer:%s, failed to retrieve file", pObj->label, pPeer->ipstr);
    return -1;
  }

  dTrace("%s peer:%s, start to retrieve WAL", pObj->label, pPeer->ipstr);
  if (syncRetrieveWal(pPeer) < 0) {
    dError("%s peer:%s, failed to retrieve WAL", pObj->label, pPeer->ipstr);
    return -1;
  }

  return 0;
}

static void *syncRetrieveData(void *param)
{
  SSyncPeer   *pPeer = (SSyncPeer *)param;
  SSyncObj    *pObj = pPeer->pSyncObj;

  assert(pPeer->syncFd >=0);
  taosBlockSIGPIPE();

  pPeer->syncFd = taosOpenTcpClientSocket(pPeer->ipstr, tsVnodeVnodePort, tsPrivateIp);
  if (pPeer->syncFd < 0) {
    dError("%s peer:%s, failed to open socket to sync", pObj->label, pPeer->ipstr);
    return NULL;    
  } 
  
  if (syncRetrieveDataStepByStep(pPeer) == 0) {
    dTrace("%s peer:%s, sync retrieve process is successful", pObj->label, pPeer->ipstr);
  } else {
    dError("%s peer:%s, failed to sync retrieve data, restart connection", pObj->label, pPeer->ipstr);
    syncRestartConnection(pPeer);
  }

  tclose(pPeer->syncFd);

  return NULL;
}

static void syncCreateRestoreDataThread(SSyncPeer *pPeer) 
{
  SSyncObj *pObj = pPeer->pSyncObj;

  taosTmrStopA(&pPeer->syncTimer);

  pthread_attr_t thattr;
  pthread_t thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

  if (pthread_create(&(thread), &thattr, (void *)syncRestoreData, pPeer) < 0) {
    dError("%s peer:%s, failed to create sync thread, reason:%s", pObj->label, pPeer->ipstr);
    taosCloseTcpSocket(pPeer->syncFd);
  } else { 
    pthread_attr_destroy(&thattr);
    dPrint("%s peer:%s, sync connection is up", pObj->label, pPeer->ipstr);
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
  SSyncObj *pObj = syncGetSyncObj(vgId);
  if (pObj == NULL) {
    dError("vgId:%d, vgId could not be found", vgId);
    taosCloseTcpSocket(connFd);
    return;
  }

  SSyncPeer *pPeer;
  for (i = 0; i < pObj->replica; ++i) {
    pPeer = pObj->peerInfo[i];
    if (pPeer && pPeer->ip == sourceIp)
      break;
  }

  pPeer = (i < pObj->replica) ? pObj->peerInfo[i] : NULL;
  if (pPeer == NULL) {
    dError("%s, peer:%s not configured", pObj->label, ipstr);
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
      dTrace("%s peer:%s, TCP connection is already up, close current one", pObj->label, pPeer->ipstr);
      taosFreeTcpThread(pPeer->pThread, &pPeer->peerFd);
      syncDecPeerRef(pPeer);
    }
 
    pPeer->peerFd = connFd;
    pPeer->pThread = taosAllocateTcpThread(tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
    dTrace("%s peer:%s, ready to exchange data", pObj->label, pPeer->ipstr);
    syncSendStatusMsgToPeer(pPeer, 0);
  }

  return;
}

static void syncProcessBrokenLink(void *param) {
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncObj  *pObj = pPeer->pSyncObj;

  dTrace("%s peer:%s, TCP link is broken, reason:%s", pObj->label, pPeer->ipstr, strerror(errno));

  tclose(pPeer->peerFd);

  if (syncDecPeerRef(pPeer) != 0) 
    syncRestartConnection(pPeer);
}

static SSyncObj *syncGetSyncObj(int32_t vgId) 
{
  SSyncObj *pObj = NULL;

  return pObj;
}
