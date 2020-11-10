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

//#include <stdint.h>
//#include <stdbool.h>
#include "os.h"
#include "hash.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "tref.h"
#include "tsocket.h"
#include "tglobal.h"
#include "taoserror.h"
#include "taosTcpPool.h"
#include "tqueue.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

// global configurable
int tsMaxSyncNum = 2;
int tsSyncTcpThreads = 2;
int tsMaxWatchFiles = 500;
int tsMaxFwdInfo = 200;
int tsSyncTimer = 1;

// module global, not configurable
int  tsSyncNum;  // number of sync in process in whole system
char tsNodeFqdn[TSDB_FQDN_LEN];

static ttpool_h tsTcpPool;
static void *   syncTmrCtrl = NULL;
static void *   vgIdHash;
static int      tsSyncRefId = -1;

// local functions
static void  syncProcessSyncRequest(char *pMsg, SSyncPeer *pPeer);
static void  syncRecoverFromMaster(SSyncPeer *pPeer);
static void  syncCheckPeerConnection(void *param, void *tmrId);
static void  syncSendPeersStatusMsgToPeer(SSyncPeer *pPeer, char ack);
static void  syncProcessBrokenLink(void *param);
static int   syncProcessPeerMsg(void *param, void *buffer);
static void  syncProcessIncommingConnection(int connFd, uint32_t sourceIp); 
static void  syncRemovePeer(SSyncPeer *pPeer);
static void  syncAddArbitrator(SSyncNode *pNode);
static void  syncFreeNode(void *);
static void  syncRemoveConfirmedFwdInfo(SSyncNode *pNode);
static void  syncMonitorFwdInfos(void *param, void *tmrId);
static void  syncProcessFwdAck(SSyncNode *pNode, SFwdInfo *pFwdInfo, int32_t code);
static void  syncSaveFwdInfo(SSyncNode *pNode, uint64_t version, void *mhandle); 
static void  syncRestartPeer(SSyncPeer *pPeer);
static int32_t syncForwardToPeerImpl(SSyncNode *pNode, void *data, void *mhandle, int qtyp);
static SSyncPeer *syncAddPeer(SSyncNode *pNode, const SNodeInfo *pInfo);

char* syncRole[] = {
  "offline",
  "unsynced",
  "syncing",
  "slave",
  "master"
};

int32_t syncInit() {
  SPoolInfo info;

  info.numOfThreads = tsSyncTcpThreads;
  info.serverIp = 0;
  info.port = tsSyncPort;
  info.bufferSize = 640000;
  info.processBrokenLink = syncProcessBrokenLink;
  info.processIncomingMsg = syncProcessPeerMsg;
  info.processIncomingConn = syncProcessIncommingConnection;

  tsTcpPool = taosOpenTcpThreadPool(&info);
  if (tsTcpPool == NULL) {
    sError("failed to init tcpPool");
    return -1;
  }

  syncTmrCtrl = taosTmrInit(1000, 50, 10000, "SYNC");
  if (syncTmrCtrl == NULL) {
    sError("failed to init tmrCtrl");
    taosCloseTcpThreadPool(tsTcpPool);
    tsTcpPool = NULL;
    return -1;
  }

  vgIdHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, true);
  if (vgIdHash == NULL) {
    sError("failed to init vgIdHash");
    taosTmrCleanUp(syncTmrCtrl);
    taosCloseTcpThreadPool(tsTcpPool);
    tsTcpPool = NULL;
    syncTmrCtrl = NULL;
    return -1;
  }

  tsSyncRefId = taosOpenRef(200, syncFreeNode);
  if (tsSyncRefId < 0) {
    syncCleanUp();
    return -1;
  }

  tstrncpy(tsNodeFqdn, tsLocalFqdn, sizeof(tsNodeFqdn));
  sInfo("sync module initialized successfully");

  return 0;
}

void syncCleanUp() {
  if (tsTcpPool) {
    taosCloseTcpThreadPool(tsTcpPool);
    tsTcpPool = NULL;
  }

  if (syncTmrCtrl) {
    taosTmrCleanUp(syncTmrCtrl);
    syncTmrCtrl = NULL;
  }

  if (vgIdHash) {
    taosHashCleanup(vgIdHash);
    vgIdHash = NULL;
  }

  taosCloseRef(tsSyncRefId);
  tsSyncRefId = -1;

  sInfo("sync module is cleaned up");
}

int64_t syncStart(const SSyncInfo *pInfo) {
  const SSyncCfg *pCfg = &pInfo->syncCfg;

  SSyncNode *pNode = (SSyncNode *)calloc(sizeof(SSyncNode), 1);
  if (pNode == NULL) {
    sError("no memory to allocate syncNode");
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tstrncpy(pNode->path, pInfo->path, sizeof(pNode->path));
  pthread_mutex_init(&pNode->mutex, NULL);

  pNode->ahandle = pInfo->ahandle;
  pNode->getFileInfo = pInfo->getFileInfo;
  pNode->getWalInfo = pInfo->getWalInfo;
  pNode->writeToCache = pInfo->writeToCache;
  pNode->notifyRole = pInfo->notifyRole;
  pNode->confirmForward = pInfo->confirmForward;
  pNode->notifyFlowCtrl = pInfo->notifyFlowCtrl;
  pNode->notifyFileSynced = pInfo->notifyFileSynced;

  pNode->selfIndex = -1;
  pNode->vgId = pInfo->vgId;
  pNode->replica = pCfg->replica;
  pNode->quorum = pCfg->quorum;
  if (pNode->quorum > pNode->replica) pNode->quorum = pNode->replica;

  pNode->rid = taosAddRef(tsSyncRefId, pNode);
  if (pNode->rid < 0) {
    syncFreeNode(pNode);
    return -1;
  }

  for (int i = 0; i < pCfg->replica; ++i) {
    const SNodeInfo *pNodeInfo = pCfg->nodeInfo + i;
    pNode->peerInfo[i] = syncAddPeer(pNode, pNodeInfo);
    if ((strcmp(pNodeInfo->nodeFqdn, tsNodeFqdn) == 0) && (pNodeInfo->nodePort == tsSyncPort)) {
      pNode->selfIndex = i;
    }
  }

  if (pNode->selfIndex < 0) {
    sInfo("vgId:%d, this node is not configured", pNode->vgId);
    terrno = TSDB_CODE_SYN_INVALID_CONFIG;
    syncStop(pNode->rid);
    return -1;
  }

  nodeVersion = pInfo->version;  // set the initial version
  nodeRole = (pNode->replica > 1) ? TAOS_SYNC_ROLE_UNSYNCED : TAOS_SYNC_ROLE_MASTER;
  sInfo("vgId:%d, %d replicas are configured, quorum:%d role:%s", pNode->vgId, pNode->replica, pNode->quorum,
        syncRole[nodeRole]);

  pNode->pSyncFwds = calloc(sizeof(SSyncFwds) + tsMaxFwdInfo * sizeof(SFwdInfo), 1);
  if (pNode->pSyncFwds == NULL) {
    sError("vgId:%d, no memory to allocate syncFwds", pNode->vgId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    syncStop(pNode->rid);
    return -1;
  }

  pNode->pFwdTimer = taosTmrStart(syncMonitorFwdInfos, 300, (void *)pNode->rid, syncTmrCtrl);
  if (pNode->pFwdTimer == NULL) {
    sError("vgId:%d, failed to allocate timer", pNode->vgId);
    syncStop(pNode->rid);
    return -1;
  }

  syncAddArbitrator(pNode);
  taosHashPut(vgIdHash, (const char *)&pNode->vgId, sizeof(int32_t), (char *)(&pNode), sizeof(SSyncNode *));

  if (pNode->notifyRole) {
    (*pNode->notifyRole)(pNode->ahandle, nodeRole);
  }

  return pNode->rid;
}

void syncStop(int64_t rid) {
  SSyncPeer *pPeer;

  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return;

  sInfo("vgId:%d, cleanup sync", pNode->vgId);

  pthread_mutex_lock(&(pNode->mutex));

  if (vgIdHash) taosHashRemove(vgIdHash, (const char *)&pNode->vgId, sizeof(int32_t));
  if (pNode->pFwdTimer) taosTmrStop(pNode->pFwdTimer);

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer) syncRemovePeer(pPeer);
  }

  pPeer = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pPeer) syncRemovePeer(pPeer);

  pthread_mutex_unlock(&(pNode->mutex));

  taosReleaseRef(tsSyncRefId, rid);
  taosRemoveRef(tsSyncRefId, rid);
}

int32_t syncReconfig(int64_t rid, const SSyncCfg *pNewCfg) {
  int        i, j;

  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return TSDB_CODE_SYN_INVALID_CONFIG;

  sInfo("vgId:%d, reconfig, role:%s replica:%d old:%d", pNode->vgId, syncRole[nodeRole], pNewCfg->replica,
        pNode->replica);

  pthread_mutex_lock(&(pNode->mutex));

  for (i = 0; i < pNode->replica; ++i) {
    for (j = 0; j < pNewCfg->replica; ++j) {
      if ((strcmp(pNode->peerInfo[i]->fqdn, pNewCfg->nodeInfo[j].nodeFqdn) == 0) &&
          (pNode->peerInfo[i]->port == pNewCfg->nodeInfo[j].nodePort))
        break;
    }

    if (j >= pNewCfg->replica) {
      syncRemovePeer(pNode->peerInfo[i]);
      pNode->peerInfo[i] = NULL;
    }
  }

  SSyncPeer *newPeers[TAOS_SYNC_MAX_REPLICA];
  for (i = 0; i < pNewCfg->replica; ++i) {
    const SNodeInfo *pNewNode = &pNewCfg->nodeInfo[i];

    for (j = 0; j < pNode->replica; ++j) {
      if (pNode->peerInfo[j] && (strcmp(pNode->peerInfo[j]->fqdn, pNewNode->nodeFqdn) == 0) &&
          (pNode->peerInfo[j]->port == pNewNode->nodePort))
        break;
    }

    if (j >= pNode->replica) {
      newPeers[i] = syncAddPeer(pNode, pNewNode);
    } else {
      newPeers[i] = pNode->peerInfo[j];
    }

    if ((strcmp(pNewNode->nodeFqdn, tsNodeFqdn) == 0) && (pNewNode->nodePort == tsSyncPort)) {
      pNode->selfIndex = i;
    }
  }

  pNode->replica = pNewCfg->replica;
  pNode->quorum = pNewCfg->quorum;
  if (pNode->quorum > pNode->replica) pNode->quorum = pNode->replica;
  memcpy(pNode->peerInfo, newPeers, sizeof(SSyncPeer *) * pNewCfg->replica);

  for (i = pNewCfg->replica; i < TAOS_SYNC_MAX_REPLICA; ++i) {
    pNode->peerInfo[i] = NULL;
  }

  syncAddArbitrator(pNode);

  if (pNewCfg->replica <= 1) {
    sInfo("vgId:%d, no peers are configured, work as master!", pNode->vgId);
    nodeRole = TAOS_SYNC_ROLE_MASTER;
    (*pNode->notifyRole)(pNode->ahandle, nodeRole);
  }

  pthread_mutex_unlock(&(pNode->mutex));

  sInfo("vgId:%d, %d replicas are configured, quorum:%d role:%s", pNode->vgId, pNode->replica, pNode->quorum,
        syncRole[nodeRole]);
  syncBroadcastStatus(pNode);

  taosReleaseRef(tsSyncRefId, rid);

  return 0;
}

int32_t syncForwardToPeer(int64_t rid, void *data, void *mhandle, int qtype) {
  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return 0; 

  int32_t code = syncForwardToPeerImpl(pNode, data, mhandle, qtype);

  taosReleaseRef(tsSyncRefId, rid);

  return code;
}

void syncConfirmForward(int64_t rid, uint64_t version, int32_t code) {
  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return;

  SSyncPeer *pPeer = pNode->pMaster;
  if (pPeer && pNode->quorum > 1) {
    char msg[sizeof(SSyncHead) + sizeof(SFwdRsp)] = {0};

    SSyncHead *pHead = (SSyncHead *)msg;
    pHead->type = TAOS_SMSG_FORWARD_RSP;
    pHead->len = sizeof(SFwdRsp);

    SFwdRsp *pFwdRsp = (SFwdRsp *)(msg + sizeof(SSyncHead));
    pFwdRsp->version = version;
    pFwdRsp->code = code;

    int msgLen = sizeof(SSyncHead) + sizeof(SFwdRsp);
    int retLen = write(pPeer->peerFd, msg, msgLen);

    if (retLen == msgLen) {
      sDebug("%s, forward-rsp is sent, ver:%" PRIu64, pPeer->id, version);
    } else {
      sDebug("%s, failed to send forward ack, restart", pPeer->id);
      syncRestartConnection(pPeer);
    }
  }

  taosReleaseRef(tsSyncRefId, rid);
}

void syncRecover(int64_t rid) {
  SSyncPeer *pPeer;

  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return;

  // to do: add a few lines to check if recover is OK
  // if take this node to unsync state, the whole system may not work

  nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
  (*pNode->notifyRole)(pNode->ahandle, nodeRole);
  nodeVersion = 0;

  pthread_mutex_lock(&(pNode->mutex));

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = (SSyncPeer *)pNode->peerInfo[i];
    if (pPeer->peerFd >= 0) {
      syncRestartConnection(pPeer);
    }
  }

  pthread_mutex_unlock(&(pNode->mutex));

  taosReleaseRef(tsSyncRefId, rid);
}

int syncGetNodesRole(int64_t rid, SNodesRole *pNodesRole) {
  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return -1;

  pNodesRole->selfIndex = pNode->selfIndex;
  for (int i = 0; i < pNode->replica; ++i) {
    pNodesRole->nodeId[i] = pNode->peerInfo[i]->nodeId;
    pNodesRole->role[i] = pNode->peerInfo[i]->role;
  }

  taosReleaseRef(tsSyncRefId, rid);

  return 0;
}

static void syncAddArbitrator(SSyncNode *pNode) {
  SSyncPeer *pPeer = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];

  // if not configured, return right away
  if (tsArbitrator[0] == 0) {
    if (pPeer) syncRemovePeer(pPeer);
    pNode->peerInfo[TAOS_SYNC_MAX_REPLICA] = NULL;
    return;
  }

  SNodeInfo nodeInfo;
  nodeInfo.nodeId = 0;
  int ret = taosGetFqdnPortFromEp(tsArbitrator, nodeInfo.nodeFqdn, &nodeInfo.nodePort);
  if (-1 == ret) {
    nodeInfo.nodePort = tsArbitratorPort;
  }

  if (pPeer) {
    if ((strcmp(nodeInfo.nodeFqdn, pPeer->fqdn) == 0) && (nodeInfo.nodePort == pPeer->port)) {
      return;
    } else {
      syncRemovePeer(pPeer);
      pNode->peerInfo[TAOS_SYNC_MAX_REPLICA] = NULL;
    }
  }

  pNode->peerInfo[TAOS_SYNC_MAX_REPLICA] = syncAddPeer(pNode, &nodeInfo);
}

static void syncFreeNode(void *param) {
  SSyncNode *pNode = param;

  pthread_mutex_destroy(&pNode->mutex);
  tfree(pNode->pRecv);
  tfree(pNode->pSyncFwds);
  tfree(pNode);
}

void syncAddPeerRef(SSyncPeer *pPeer) { atomic_add_fetch_8(&pPeer->refCount, 1); }

int syncDecPeerRef(SSyncPeer *pPeer) {
  if (atomic_sub_fetch_8(&pPeer->refCount, 1) == 0) {
    taosReleaseRef(tsSyncRefId, pPeer->pSyncNode->rid);

    sDebug("%s, resource is freed", pPeer->id);
    tfree(pPeer->watchFd);
    tfree(pPeer);
    return 0;
  }

  return 1;
}

static void syncClosePeerConn(SSyncPeer *pPeer) {
  taosTmrStopA(&pPeer->timer);
  taosClose(pPeer->syncFd);
  if (pPeer->peerFd >= 0) {
    pPeer->peerFd = -1;
    taosFreeTcpConn(pPeer->pConn);
  }
}

static void syncRemovePeer(SSyncPeer *pPeer) {
  sInfo("%s, it is removed", pPeer->id);

  pPeer->ip = 0;
  syncClosePeerConn(pPeer);
  syncDecPeerRef(pPeer);
}

static SSyncPeer *syncAddPeer(SSyncNode *pNode, const SNodeInfo *pInfo) {
  uint32_t ip = taosGetIpFromFqdn(pInfo->nodeFqdn);
  if (ip == -1) return NULL;

  SSyncPeer *pPeer = calloc(1, sizeof(SSyncPeer));
  if (pPeer == NULL) return NULL;

  pPeer->nodeId = pInfo->nodeId;
  tstrncpy(pPeer->fqdn, pInfo->nodeFqdn, sizeof(pPeer->fqdn));
  pPeer->ip = ip;
  pPeer->port = pInfo->nodePort;
  pPeer->fqdn[sizeof(pPeer->fqdn) - 1] = 0;
  snprintf(pPeer->id, sizeof(pPeer->id), "vgId:%d peer:%s:%u", pNode->vgId, pPeer->fqdn, pPeer->port);

  pPeer->peerFd = -1;
  pPeer->syncFd = -1;
  pPeer->role = TAOS_SYNC_ROLE_OFFLINE;
  pPeer->pSyncNode = pNode;
  pPeer->refCount = 1;

  sInfo("%s, it is configured", pPeer->id);
  int ret = strcmp(pPeer->fqdn, tsNodeFqdn);
  if (pPeer->nodeId == 0 || (ret > 0) || (ret == 0 && pPeer->port > tsSyncPort)) {
    int32_t checkMs = 100 + (pNode->vgId * 10) % 100;
    if (pNode->vgId > 1) checkMs = tsStatusInterval * 2000 + checkMs;
    sDebug("%s, start to check peer connection after %d ms", pPeer->id, checkMs);
    taosTmrReset(syncCheckPeerConnection, checkMs, pPeer, syncTmrCtrl, &pPeer->timer);
  }

  taosAcquireRef(tsSyncRefId, pNode->rid);  
  return pPeer;
}

void syncBroadcastStatus(SSyncNode *pNode) {
  SSyncPeer *pPeer;

  for (int i = 0; i < pNode->replica; ++i) {
    if (i == pNode->selfIndex) continue;
    pPeer = pNode->peerInfo[i];
    syncSendPeersStatusMsgToPeer(pPeer, 1);
  }
}

static void syncResetFlowCtrl(SSyncNode *pNode) {
  for (int i = 0; i < pNode->replica; ++i) {
    pNode->peerInfo[i]->numOfRetrieves = 0;
  }

  if (pNode->notifyFlowCtrl) {
    (*pNode->notifyFlowCtrl)(pNode->ahandle, 0);
  }
}

static void syncChooseMaster(SSyncNode *pNode) {
  SSyncPeer *pPeer;
  int        onlineNum = 0;
  int        index = -1;
  int        replica = pNode->replica;

  sDebug("vgId:%d, choose master", pNode->vgId);

  for (int i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
    }
  }

  if (onlineNum == pNode->replica) {
    // if all peers are online, peer with highest version shall be master
    index = 0;
    for (int i = 1; i < pNode->replica; ++i) {
      if (pNode->peerInfo[i]->version > pNode->peerInfo[index]->version) {
        index = i;
      }
    }
  }

  // add arbitrator connection
  SSyncPeer *pArb = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pArb && pArb->role != TAOS_SYNC_ROLE_OFFLINE) {
    onlineNum++;
    replica = pNode->replica + 1;
  }

  if (index < 0 && onlineNum > replica / 2.0) {
    // over half of nodes are online
    for (int i = 0; i < pNode->replica; ++i) {
      // slave with highest version shall be master
      pPeer = pNode->peerInfo[i];
      if (pPeer->role == TAOS_SYNC_ROLE_SLAVE || pPeer->role == TAOS_SYNC_ROLE_MASTER) {
        if (index < 0 || pPeer->version > pNode->peerInfo[index]->version) {
          index = i;
        }
      }
    }
  }

  if (index >= 0) {
    if (index == pNode->selfIndex) {
      sInfo("vgId:%d, start to work as master", pNode->vgId);
      nodeRole = TAOS_SYNC_ROLE_MASTER;
      syncResetFlowCtrl(pNode);
      (*pNode->notifyRole)(pNode->ahandle, nodeRole);
    } else {
      pPeer = pNode->peerInfo[index];
      sInfo("%s, it shall work as master", pPeer->id);
    }
  } else {
    sDebug("vgId:%d, failed to choose master", pNode->vgId);
  }
}

static SSyncPeer *syncCheckMaster(SSyncNode *pNode) {
  int onlineNum = 0;
  int index = -1;
  int replica = pNode->replica;

  for (int i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
    }
  }

  // add arbitrator connection
  SSyncPeer *pArb = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pArb && pArb->role != TAOS_SYNC_ROLE_OFFLINE) {
    onlineNum++;
    replica = pNode->replica + 1;
  }

  if (onlineNum <= replica * 0.5) {
    if (nodeRole != TAOS_SYNC_ROLE_UNSYNCED) {
      nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
      // pNode->peerInfo[pNode->selfIndex]->role = nodeRole;
      (*pNode->notifyRole)(pNode->ahandle, nodeRole);
      sInfo("vgId:%d, change to unsynced state, online:%d replica:%d", pNode->vgId, onlineNum, replica);
    }
  } else {
    for (int i = 0; i < pNode->replica; ++i) {
      SSyncPeer *pTemp = pNode->peerInfo[i];
      if (pTemp->role != TAOS_SYNC_ROLE_MASTER) continue;
      if (index < 0) {
        index = i;
      } else {  // multiple masters, it shall not happen
        if (i == pNode->selfIndex) {
          sError("%s, peer is master, work as slave instead", pTemp->id);
          nodeRole = TAOS_SYNC_ROLE_SLAVE;
          (*pNode->notifyRole)(pNode->ahandle, nodeRole);
        }
      }
    }
  }

  SSyncPeer *pMaster = (index >= 0) ? pNode->peerInfo[index] : NULL;
  return pMaster;
}

static int syncValidateMaster(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int        code = 0;

  if (nodeRole == TAOS_SYNC_ROLE_MASTER && nodeVersion < pPeer->version) {
    sDebug("%s, slave has higher version, restart all connections!!!", pPeer->id);
    nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
    (*pNode->notifyRole)(pNode->ahandle, nodeRole);
    code = -1;

    for (int i = 0; i < pNode->replica; ++i) {
      if (i == pNode->selfIndex) continue;
      syncRestartPeer(pNode->peerInfo[i]);
    }
  }

  return code;
}

static void syncCheckRole(SSyncPeer *pPeer, SPeerStatus peersStatus[], int8_t newRole) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int8_t     peerOldRole = pPeer->role;
  int8_t     selfOldRole = nodeRole;
  int8_t     i, syncRequired = 0;

  // pNode->peerInfo[pNode->selfIndex]->version = nodeVersion;
  pPeer->role = newRole;

  sDebug("%s, own role:%s, new peer role:%s", pPeer->id, syncRole[nodeRole], syncRole[pPeer->role]);

  SSyncPeer *pMaster = syncCheckMaster(pNode);

  if (pMaster) {
    // master is there
    pNode->pMaster = pMaster;
    sDebug("%s, it is the master, ver:%" PRIu64, pMaster->id, pMaster->version);

    if (syncValidateMaster(pPeer) < 0) return;

    if (nodeRole == TAOS_SYNC_ROLE_UNSYNCED) {
      if (nodeVersion < pMaster->version) {
        syncRequired = 1;
      } else {
        sInfo("%s is master, work as slave, ver:%" PRIu64, pMaster->id, pMaster->version);
        nodeRole = TAOS_SYNC_ROLE_SLAVE;
        (*pNode->notifyRole)(pNode->ahandle, nodeRole);
      }
    } else if (nodeRole == TAOS_SYNC_ROLE_SLAVE && pMaster == pPeer) {
      // nodeVersion = pMaster->version;
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
    } else {
      if (pNode->replica == 2) consistent = 1;
    }

    if (consistent) {
      syncChooseMaster(pNode);
    }
  }

  if (syncRequired) {
    syncRecoverFromMaster(pMaster);
  }

  if (peerOldRole != newRole || nodeRole != selfOldRole) {
    syncBroadcastStatus(pNode);
  }

  if (nodeRole != TAOS_SYNC_ROLE_MASTER) {
    syncResetFlowCtrl(pNode);
  }
}

static void syncRestartPeer(SSyncPeer *pPeer) {
  sDebug("%s, restart connection", pPeer->id);

  syncClosePeerConn(pPeer);

  pPeer->sstatus = TAOS_SYNC_STATUS_INIT;

  int ret = strcmp(pPeer->fqdn, tsNodeFqdn);
  if (ret > 0 || (ret == 0 && pPeer->port > tsSyncPort)) {
    taosTmrReset(syncCheckPeerConnection, tsSyncTimer * 1000, pPeer, syncTmrCtrl, &pPeer->timer);
  }
}

void syncRestartConnection(SSyncPeer *pPeer) {
  if (pPeer->ip == 0) return;

  syncRestartPeer(pPeer);
  syncCheckRole(pPeer, NULL, TAOS_SYNC_ROLE_OFFLINE);
}

static void syncProcessSyncRequest(char *msg, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  sDebug("%s, sync-req is received", pPeer->id);

  if (pPeer->ip == 0) return;

  if (nodeRole != TAOS_SYNC_ROLE_MASTER) {
    sError("%s, I am not master anymore", pPeer->id);
    taosClose(pPeer->syncFd);
    return;
  }

  if (pPeer->sstatus != TAOS_SYNC_STATUS_INIT) {
    sDebug("%s, sync is already started", pPeer->id);
    return;  // already started
  }

  // start a new thread to retrieve the data
  syncAddPeerRef(pPeer);
  pthread_attr_t thattr;
  pthread_t      thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  int ret = pthread_create(&thread, &thattr, syncRetrieveData, pPeer);
  pthread_attr_destroy(&thattr);

  if (ret != 0) {
    sError("%s, failed to create sync thread(%s)", pPeer->id, strerror(errno));
    syncDecPeerRef(pPeer);
  } else {
    pPeer->sstatus = TAOS_SYNC_STATUS_START;
    sDebug("%s, thread is created to retrieve data", pPeer->id);
  }
}

static void syncNotStarted(void *param, void *tmrId) {
  SSyncPeer *pPeer = param;
  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&(pNode->mutex));
  pPeer->timer = NULL;
  sInfo("%s, sync connection is still not up, restart", pPeer->id);
  syncRestartConnection(pPeer);
  pthread_mutex_unlock(&(pNode->mutex));
}

static void syncTryRecoverFromMaster(void *param, void *tmrId) {
  SSyncPeer *pPeer = param;
  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&(pNode->mutex));
  syncRecoverFromMaster(pPeer);
  pthread_mutex_unlock(&(pNode->mutex));
}

static void syncRecoverFromMaster(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  if (nodeSStatus != TAOS_SYNC_STATUS_INIT) {
    sDebug("%s, sync is already started, status:%d", pPeer->id, nodeSStatus);
    return;
  }

  taosTmrStopA(&pPeer->timer);

  // Ensure the sync of mnode not interrupted
  if (pNode->vgId != 1 && tsSyncNum >= tsMaxSyncNum) {
    sInfo("%s, %d syncs are in process, try later", pPeer->id, tsSyncNum);
    taosTmrReset(syncTryRecoverFromMaster, 500 + (pNode->vgId * 10) % 200, pPeer, syncTmrCtrl, &pPeer->timer);
    return;
  }

  sDebug("%s, try to sync", pPeer->id);

  SFirstPkt firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.syncHead.type = TAOS_SMSG_SYNC_REQ;
  firstPkt.syncHead.vgId = pNode->vgId;
  firstPkt.syncHead.len = sizeof(firstPkt) - sizeof(SSyncHead);
  tstrncpy(firstPkt.fqdn, tsNodeFqdn, sizeof(firstPkt.fqdn));
  firstPkt.port = tsSyncPort;
  taosTmrReset(syncNotStarted, tsSyncTimer * 1000, pPeer, syncTmrCtrl, &pPeer->timer);

  if (write(pPeer->peerFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt)) {
    sError("%s, failed to send sync-req to peer", pPeer->id);
  } else {
    nodeSStatus = TAOS_SYNC_STATUS_START;
    sInfo("%s, sync-req is sent", pPeer->id);
  }
}

static void syncProcessFwdResponse(char *cont, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SFwdRsp *  pFwdRsp = (SFwdRsp *)cont;
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;
  SFwdInfo * pFwdInfo;

  sDebug("%s, forward-rsp is received, ver:%" PRIu64, pPeer->id, pFwdRsp->version);
  SFwdInfo *pFirst = pSyncFwds->fwdInfo + pSyncFwds->first;

  if (pFirst->version <= pFwdRsp->version && pSyncFwds->fwds > 0) {
    // find the forwardInfo from first
    for (int i = 0; i < pSyncFwds->fwds; ++i) {
      pFwdInfo = pSyncFwds->fwdInfo + (i + pSyncFwds->first) % tsMaxFwdInfo;
      if (pFwdRsp->version == pFwdInfo->version) break;
    }

    syncProcessFwdAck(pNode, pFwdInfo, pFwdRsp->code);
    syncRemoveConfirmedFwdInfo(pNode);
  }
}

static void syncProcessForwardFromPeer(char *cont, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SWalHead * pHead = (SWalHead *)cont;

  sDebug("%s, forward is received, ver:%" PRIu64, pPeer->id, pHead->version);

  if (nodeRole == TAOS_SYNC_ROLE_SLAVE) {
    // nodeVersion = pHead->version;
    (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_FWD, NULL);
  } else {
    if (nodeSStatus != TAOS_SYNC_STATUS_INIT) {
      syncSaveIntoBuffer(pPeer, pHead);
    } else {
      sError("%s, forward discarded, ver:%" PRIu64, pPeer->id, pHead->version);
    }
  }
}

static void syncProcessPeersStatusMsg(char *cont, SSyncPeer *pPeer) {
  SSyncNode *   pNode = pPeer->pSyncNode;
  SPeersStatus *pPeersStatus = (SPeersStatus *)cont;

  sDebug("%s, status msg is received, self:%s ver:%" PRIu64 " peer:%s ver:%" PRIu64 ", ack:%d", pPeer->id,
         syncRole[nodeRole], nodeVersion, syncRole[pPeersStatus->role], pPeersStatus->version, pPeersStatus->ack);

  pPeer->version = pPeersStatus->version;
  syncCheckRole(pPeer, pPeersStatus->peersStatus, pPeersStatus->role);

  if (pPeersStatus->ack) {
    syncSendPeersStatusMsgToPeer(pPeer, 0);
  }
}

static int syncReadPeerMsg(SSyncPeer *pPeer, SSyncHead *pHead, char *cont) {
  if (pPeer->peerFd < 0) return -1;

  int hlen = taosReadMsg(pPeer->peerFd, pHead, sizeof(SSyncHead));
  if (hlen != sizeof(SSyncHead)) {
    sDebug("%s, failed to read msg, hlen:%d", pPeer->id, hlen);
    return -1;
  }

  // head.len = htonl(head.len);
  if (pHead->len < 0) {
    sError("%s, invalid pkt length, len:%d", pPeer->id, pHead->len);
    return -1;
  }

  int bytes = taosReadMsg(pPeer->peerFd, cont, pHead->len);
  if (bytes != pHead->len) {
    sError("%s, failed to read, bytes:%d len:%d", pPeer->id, bytes, pHead->len);
    return -1;
  }

  return 0;
}

static int syncProcessPeerMsg(void *param, void *buffer) {
  SSyncPeer *pPeer = param;
  SSyncHead  head;
  char *     cont = buffer;

  SSyncNode *pNode = pPeer->pSyncNode;
  pthread_mutex_lock(&(pNode->mutex));

  int code = syncReadPeerMsg(pPeer, &head, cont);

  if (code == 0) {
    if (head.type == TAOS_SMSG_FORWARD) {
      syncProcessForwardFromPeer(cont, pPeer);
    } else if (head.type == TAOS_SMSG_FORWARD_RSP) {
      syncProcessFwdResponse(cont, pPeer);
    } else if (head.type == TAOS_SMSG_SYNC_REQ) {
      syncProcessSyncRequest(cont, pPeer);
    } else if (head.type == TAOS_SMSG_STATUS) {
      syncProcessPeersStatusMsg(cont, pPeer);
    }
  }

  pthread_mutex_unlock(&(pNode->mutex));

  return code;
}

#define statusMsgLen sizeof(SSyncHead) + sizeof(SPeersStatus) + sizeof(SPeerStatus) * TAOS_SYNC_MAX_REPLICA

static void syncSendPeersStatusMsgToPeer(SSyncPeer *pPeer, char ack) {
  SSyncNode *pNode = pPeer->pSyncNode;
  char       msg[statusMsgLen] = {0};

  if (pPeer->peerFd < 0 || pPeer->ip == 0) return;

  SSyncHead *   pHead = (SSyncHead *)msg;
  SPeersStatus *pPeersStatus = (SPeersStatus *)(msg + sizeof(SSyncHead));

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
    sDebug("%s, status msg is sent, self:%s ver:%" PRIu64 ", ack:%d", pPeer->id, syncRole[pPeersStatus->role],
           pPeersStatus->version, pPeersStatus->ack);
  } else {
    sDebug("%s, failed to send status msg, restart", pPeer->id);
    syncRestartConnection(pPeer);
  }

  return;
}

static void syncSetupPeerConnection(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  taosTmrStopA(&pPeer->timer);
  if (pPeer->peerFd >= 0) {
    sDebug("%s, send role version to peer", pPeer->id);
    syncSendPeersStatusMsgToPeer(pPeer, 1);
    return;
  }

  int connFd = taosOpenTcpClientSocket(pPeer->ip, pPeer->port, 0);
  if (connFd < 0) {
    sDebug("%s, failed to open tcp socket(%s)", pPeer->id, strerror(errno));
    taosTmrReset(syncCheckPeerConnection, tsSyncTimer * 1000, pPeer, syncTmrCtrl, &pPeer->timer);
    return;
  }

  SFirstPkt firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.syncHead.vgId = pPeer->nodeId ? pNode->vgId : 0;
  firstPkt.syncHead.type = TAOS_SMSG_STATUS;
  tstrncpy(firstPkt.fqdn, tsNodeFqdn, sizeof(firstPkt.fqdn));
  firstPkt.port = tsSyncPort;
  firstPkt.sourceId = pNode->vgId;  // tell arbitrator its vgId

  if (write(connFd, &firstPkt, sizeof(firstPkt)) == sizeof(firstPkt)) {
    sDebug("%s, connection to peer server is setup", pPeer->id);
    pPeer->peerFd = connFd;
    pPeer->role = TAOS_SYNC_ROLE_UNSYNCED;
    pPeer->pConn = taosAllocateTcpConn(tsTcpPool, pPeer, connFd);
    syncAddPeerRef(pPeer);
  } else {
    sDebug("try later");
    close(connFd);
    taosTmrReset(syncCheckPeerConnection, tsSyncTimer * 1000, pPeer, syncTmrCtrl, &pPeer->timer);
  }
}

static void syncCheckPeerConnection(void *param, void *tmrId) {
  SSyncPeer *pPeer = param;
  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&(pNode->mutex));

  sDebug("%s, check peer connection", pPeer->id);
  syncSetupPeerConnection(pPeer);

  pthread_mutex_unlock(&(pNode->mutex));
}

static void syncCreateRestoreDataThread(SSyncPeer *pPeer) {
  taosTmrStopA(&pPeer->timer);

  pthread_attr_t thattr;
  pthread_t      thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

  syncAddPeerRef(pPeer);
  int ret = pthread_create(&(thread), &thattr, (void *)syncRestoreData, pPeer);
  pthread_attr_destroy(&thattr);

  if (ret < 0) {
    sError("%s, failed to create sync thread", pPeer->id);
    taosClose(pPeer->syncFd);
    syncDecPeerRef(pPeer);
  } else {
    sInfo("%s, sync connection is up", pPeer->id);
  }
}

static void syncProcessIncommingConnection(int connFd, uint32_t sourceIp) {
  char ipstr[24];
  int  i;

  tinet_ntoa(ipstr, sourceIp);
  sDebug("peer TCP connection from ip:%s", ipstr);

  SFirstPkt firstPkt;
  if (taosReadMsg(connFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt)) {
    sError("failed to read peer first pkt from ip:%s(%s)", ipstr, strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  int32_t     vgId = firstPkt.syncHead.vgId;
  SSyncNode **ppNode = (SSyncNode **)taosHashGet(vgIdHash, (const char *)&vgId, sizeof(int32_t));
  if (ppNode == NULL || *ppNode == NULL) {
    sError("vgId:%d, vgId could not be found", vgId);
    taosCloseSocket(connFd);
    return;
  }

  SSyncNode *pNode = *ppNode;
  pthread_mutex_lock(&(pNode->mutex));

  SSyncPeer *pPeer;
  for (i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer && (strcmp(pPeer->fqdn, firstPkt.fqdn) == 0) && (pPeer->port == firstPkt.port)) break;
  }

  pPeer = (i < pNode->replica) ? pNode->peerInfo[i] : NULL;
  if (pPeer == NULL) {
    sError("vgId:%d, peer:%s not configured", pNode->vgId, firstPkt.fqdn);
    taosCloseSocket(connFd);
    // syncSendVpeerCfgMsg(sync);
  } else {
    // first packet tells what kind of link
    if (firstPkt.syncHead.type == TAOS_SMSG_SYNC_DATA) {
      pPeer->syncFd = connFd;
      syncCreateRestoreDataThread(pPeer);
    } else {
      sDebug("%s, TCP connection is already up, close one", pPeer->id);
      syncClosePeerConn(pPeer);
      pPeer->peerFd = connFd;
      pPeer->pConn = taosAllocateTcpConn(tsTcpPool, pPeer, connFd);
      syncAddPeerRef(pPeer);
      sDebug("%s, ready to exchange data", pPeer->id);
      syncSendPeersStatusMsgToPeer(pPeer, 1);
    }
  }

  pthread_mutex_unlock(&(pNode->mutex));
}

static void syncProcessBrokenLink(void *param) {
  if (param == NULL) return;  // the connection for arbitrator
  SSyncPeer *pPeer = param;
  SSyncNode *pNode = pPeer->pSyncNode;

  if (taosAcquireRef(tsSyncRefId, pNode->rid) < 0) return;
  pthread_mutex_lock(&(pNode->mutex));

  sDebug("%s, TCP link is broken(%s)", pPeer->id, strerror(errno));
  pPeer->peerFd = -1;

  if (syncDecPeerRef(pPeer) != 0) {
    syncRestartConnection(pPeer);
  }

  pthread_mutex_unlock(&(pNode->mutex));
  taosReleaseRef(tsSyncRefId, pNode->rid);
}

static void syncSaveFwdInfo(SSyncNode *pNode, uint64_t version, void *mhandle) {
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;
  uint64_t   time = taosGetTimestampMs();

  if (pSyncFwds->fwds >= tsMaxFwdInfo) {
    pSyncFwds->first = (pSyncFwds->first + 1) % tsMaxFwdInfo;
    pSyncFwds->fwds--;
  }

  if (pSyncFwds->fwds > 0) {
    pSyncFwds->last = (pSyncFwds->last + 1) % tsMaxFwdInfo;
  }

  SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + pSyncFwds->last;
  pFwdInfo->version = version;
  pFwdInfo->mhandle = mhandle;
  pFwdInfo->acks = 0;
  pFwdInfo->confirmed = 0;
  pFwdInfo->time = time;

  pSyncFwds->fwds++;
  sDebug("vgId:%d, fwd info is saved, ver:%" PRIu64 " fwds:%d ", pNode->vgId, version, pSyncFwds->fwds);
}

static void syncRemoveConfirmedFwdInfo(SSyncNode *pNode) {
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;

  int fwds = pSyncFwds->fwds;
  for (int i = 0; i < fwds; ++i) {
    SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + pSyncFwds->first;
    if (pFwdInfo->confirmed == 0) break;

    pSyncFwds->first = (pSyncFwds->first + 1) % tsMaxFwdInfo;
    pSyncFwds->fwds--;
    if (pSyncFwds->fwds == 0) pSyncFwds->first = pSyncFwds->last;
    // sDebug("vgId:%d, fwd info is removed, ver:%d, fwds:%d",
    //        pNode->vgId, pFwdInfo->version, pSyncFwds->fwds);
    memset(pFwdInfo, 0, sizeof(SFwdInfo));
  }
}

static void syncProcessFwdAck(SSyncNode *pNode, SFwdInfo *pFwdInfo, int32_t code) {
  int confirm = 0;
  if (pFwdInfo->code == 0) pFwdInfo->code = code;

  if (code == 0) {
    pFwdInfo->acks++;
    if (pFwdInfo->acks >= pNode->quorum - 1) {
      confirm = 1;
    }
  } else {
    pFwdInfo->nacks++;
    if (pFwdInfo->nacks > pNode->replica - pNode->quorum) {
      confirm = 1;
    }
  }

  if (confirm && pFwdInfo->confirmed == 0) {
    sDebug("vgId:%d, forward is confirmed, ver:%" PRIu64 " code:%x", pNode->vgId, pFwdInfo->version, pFwdInfo->code);
    (*pNode->confirmForward)(pNode->ahandle, pFwdInfo->mhandle, pFwdInfo->code);
    pFwdInfo->confirmed = 1;
  }
}

static void syncMonitorFwdInfos(void *param, void *tmrId) {
  int64_t rid = (int64_t) param;
  SSyncNode *pNode = taosAcquireRef(tsSyncRefId, rid);
  if (pNode == NULL) return;

  SSyncFwds *pSyncFwds = pNode->pSyncFwds;

  if (pSyncFwds) {;
    uint64_t   time = taosGetTimestampMs();

    if (pSyncFwds->fwds > 0) {
      pthread_mutex_lock(&(pNode->mutex));
      for (int i = 0; i < pSyncFwds->fwds; ++i) {
        SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + (pSyncFwds->first + i) % tsMaxFwdInfo;
        if (time - pFwdInfo->time < 2000) break;
        syncProcessFwdAck(pNode, pFwdInfo, TSDB_CODE_RPC_NETWORK_UNAVAIL);
      }

      syncRemoveConfirmedFwdInfo(pNode);
      pthread_mutex_unlock(&(pNode->mutex));
    }

    pNode->pFwdTimer = taosTmrStart(syncMonitorFwdInfos, 300, (void *)pNode->rid, syncTmrCtrl);
  }

  taosReleaseRef(tsSyncRefId, rid);
}

static int32_t syncForwardToPeerImpl(SSyncNode *pNode, void *data, void *mhandle, int qtype) {
  SSyncPeer *pPeer;
  SSyncHead *pSyncHead;
  SWalHead * pWalHead = data;
  int        fwdLen;
  int32_t    code = 0;

  if (nodeRole == TAOS_SYNC_ROLE_SLAVE && pWalHead->version != nodeVersion + 1) {
    sError("vgId:%d, received ver:%" PRIu64 ", inconsistent with last ver:%" PRIu64 ", restart connection", pNode->vgId,
           pWalHead->version, nodeVersion);
    for (int i = 0; i < pNode->replica; ++i) {
      pPeer = pNode->peerInfo[i];
      syncRestartConnection(pPeer);
    }
    return TSDB_CODE_SYN_INVALID_VERSION;
  }

  // always update version
  nodeVersion = pWalHead->version;
  sDebug("vgId:%d, forward to peer, replica:%d role:%s qtype:%s hver:%" PRIu64, pNode->vgId, pNode->replica,
         syncRole[nodeRole], qtypeStr[qtype], pWalHead->version);

  if (pNode->replica == 1 || nodeRole != TAOS_SYNC_ROLE_MASTER) return 0;

  // only pkt from RPC or CQ can be forwarded
  if (qtype != TAOS_QTYPE_RPC && qtype != TAOS_QTYPE_CQ) return 0;

  // a hacker way to improve the performance
  pSyncHead = (SSyncHead *)(((char *)pWalHead) - sizeof(SSyncHead));
  pSyncHead->type = TAOS_SMSG_FORWARD;
  pSyncHead->pversion = 0;
  pSyncHead->len = sizeof(SWalHead) + pWalHead->len;
  fwdLen = pSyncHead->len + sizeof(SSyncHead);  // include the WAL and SYNC head

  pthread_mutex_lock(&(pNode->mutex));

  for (int i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer == NULL || pPeer->peerFd < 0) continue;
    if (pPeer->role != TAOS_SYNC_ROLE_SLAVE && pPeer->sstatus != TAOS_SYNC_STATUS_CACHE) continue;

    if (pNode->quorum > 1 && code == 0) {
      syncSaveFwdInfo(pNode, pWalHead->version, mhandle);
      code = 1;
    }

    int retLen = write(pPeer->peerFd, pSyncHead, fwdLen);
    if (retLen == fwdLen) {
      sDebug("%s, forward is sent, ver:%" PRIu64 " contLen:%d", pPeer->id, pWalHead->version, pWalHead->len);
    } else {
      sError("%s, failed to forward, ver:%" PRIu64 " retLen:%d", pPeer->id, pWalHead->version, retLen);
      syncRestartConnection(pPeer);
    }
  }

  pthread_mutex_unlock(&(pNode->mutex));

  return code;
}

