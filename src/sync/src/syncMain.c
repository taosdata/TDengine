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
#include "hash.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "tref.h"
#include "tsocket.h"
#include "tglobal.h"
#include "taoserror.h"
#include "tqueue.h"
#include "twal.h"
#include "tsync.h"
#include "syncTcp.h"
#include "syncInt.h"

int32_t tsSyncNum = 0;  // number of sync in process in whole system
char    tsNodeFqdn[TSDB_FQDN_LEN] = {0};

static void *  tsTcpPool = NULL;
static void *  tsSyncTmrCtrl = NULL;
static void *  tsVgIdHash = NULL;
static int32_t tsNodeRefId = -1;
static int32_t tsPeerRefId = -1;

// local functions
static void    syncProcessSyncRequest(char *pMsg, SSyncPeer *pPeer);
static void    syncRecoverFromMaster(SSyncPeer *pPeer);
static void    syncCheckPeerConnection(void *param, void *tmrId);
static int32_t syncSendPeersStatusMsgToPeer(SSyncPeer *pPeer, char ack, int8_t type, uint16_t tranId);
static void    syncProcessBrokenLink(int64_t rid);
static int32_t syncProcessPeerMsg(int64_t rid, void *buffer);
static void    syncProcessIncommingConnection(SOCKET connFd, uint32_t sourceIp);
static void    syncRemovePeer(SSyncPeer *pPeer);
static void    syncAddArbitrator(SSyncNode *pNode);
static void    syncFreeNode(void *);
static void    syncFreePeer(void *);
static void    syncRemoveConfirmedFwdInfo(SSyncNode *pNode);
static void    syncMonitorFwdInfos(void *param, void *tmrId);
static void    syncMonitorNodeRole(void *param, void *tmrId);
static void    syncProcessFwdAck(SSyncNode *pNode, SFwdInfo *pFwdInfo, int32_t code);
static int32_t syncSaveFwdInfo(SSyncNode *pNode, uint64_t version, void *mhandle);
static void    syncRestartPeer(SSyncPeer *pPeer);
static int32_t syncForwardToPeerImpl(SSyncNode *pNode, void *data, void *mhandle, int32_t qtyp);

static SSyncPeer *syncAddPeer(SSyncNode *pNode, const SNodeInfo *pInfo);
static void       syncStartCheckPeerConn(SSyncPeer *pPeer);
static void       syncStopCheckPeerConn(SSyncPeer *pPeer);
static SSyncNode *syncAcquireNode(int64_t rid);
static void       syncReleaseNode(SSyncNode *pNode);

char* syncRole[] = {
  "offline",
  "unsynced",
  "syncing",
  "slave",
  "master"
};

char *syncStatus[] = {
  "init",
  "start",
  "file",
  "cache",
  "invalid"
};

int32_t syncInit() {
  SPoolInfo info = {0};

  info.numOfThreads = SYNC_TCP_THREADS;
  info.serverIp = 0;
  info.port = tsSyncPort;
  info.bufferSize = SYNC_MAX_SIZE;
  info.processBrokenLink = syncProcessBrokenLink;
  info.processIncomingMsg = syncProcessPeerMsg;
  info.processIncomingConn = syncProcessIncommingConnection;

  tsTcpPool = syncOpenTcpThreadPool(&info);
  if (tsTcpPool == NULL) {
    sError("failed to init tcpPool");
    syncCleanUp();
    return -1;
  }

  tsSyncTmrCtrl = taosTmrInit(1000, 50, 10000, "SYNC");
  if (tsSyncTmrCtrl == NULL) {
    sError("failed to init tmrCtrl");
    syncCleanUp();
    return -1;
  }

  tsVgIdHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVgIdHash == NULL) {
    sError("failed to init vgIdHash");
    syncCleanUp();
    return -1;
  }

  tsNodeRefId = taosOpenRef(200, syncFreeNode);
  if (tsNodeRefId < 0) {
    sError("failed to init node ref");
    syncCleanUp();
    return -1;
  }

  tsPeerRefId = taosOpenRef(1000, syncFreePeer);
  if (tsPeerRefId < 0) {
    sError("failed to init peer ref");
    syncCleanUp();
    return -1;
  }

  tstrncpy(tsNodeFqdn, tsLocalFqdn, sizeof(tsNodeFqdn));
  sInfo("sync module initialized successfully");

  return 0;
}

void syncCleanUp() {
  if (tsTcpPool != NULL) {
    syncCloseTcpThreadPool(tsTcpPool);
    tsTcpPool = NULL;
  }

  if (tsSyncTmrCtrl != NULL) {
    taosTmrCleanUp(tsSyncTmrCtrl);
    tsSyncTmrCtrl = NULL;
  }

  if (tsVgIdHash) {
    taosHashCleanup(tsVgIdHash);
    tsVgIdHash = NULL;
  }

  if (tsNodeRefId != -1) {
    taosCloseRef(tsNodeRefId);
    tsNodeRefId = -1;
  }

  if (tsPeerRefId != -1) {
    taosCloseRef(tsPeerRefId);
    tsPeerRefId = -1;
  }

  sInfo("sync module is cleaned up");
}

int64_t syncStart(const SSyncInfo *pInfo) {
  const SSyncCfg *pCfg = &pInfo->syncCfg;

  SSyncNode *pNode = calloc(sizeof(SSyncNode), 1);
  if (pNode == NULL) {
    sError("no memory to allocate syncNode");
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tstrncpy(pNode->path, pInfo->path, sizeof(pNode->path));
  pthread_mutex_init(&pNode->mutex, NULL);

  pNode->getWalInfoFp = pInfo->getWalInfoFp;
  pNode->writeToCacheFp = pInfo->writeToCacheFp;
  pNode->notifyRoleFp = pInfo->notifyRoleFp;
  pNode->confirmForward = pInfo->confirmForward;
  pNode->notifyFlowCtrlFp = pInfo->notifyFlowCtrlFp;
  pNode->startSyncFileFp = pInfo->startSyncFileFp;
  pNode->stopSyncFileFp = pInfo->stopSyncFileFp;
  pNode->getVersionFp = pInfo->getVersionFp;
  pNode->sendFileFp = pInfo->sendFileFp;
  pNode->recvFileFp = pInfo->recvFileFp;

  pNode->selfIndex = -1;
  pNode->vgId = pInfo->vgId;
  pNode->replica = pCfg->replica;
  pNode->quorum = pCfg->quorum;
  pNode->pTsdb = pInfo->pTsdb;
  if (pNode->quorum > pNode->replica) pNode->quorum = pNode->replica;

  pNode->refCount = 1;
  pNode->rid = taosAddRef(tsNodeRefId, pNode);
  if (pNode->rid < 0) {
    syncFreeNode(pNode);
    return -1;
  }

  for (int32_t index = 0; index < pCfg->replica; ++index) {
    const SNodeInfo *pNodeInfo = pCfg->nodeInfo + index;
    pNode->peerInfo[index] = syncAddPeer(pNode, pNodeInfo);
    if (pNode->peerInfo[index] == NULL) {
      sError("vgId:%d, node:%d fqdn:%s port:%u is not configured, stop taosd", pNode->vgId, pNodeInfo->nodeId,
             pNodeInfo->nodeFqdn, pNodeInfo->nodePort);
      syncStop(pNode->rid);
      exit(1);
    }

    if ((strcmp(pNodeInfo->nodeFqdn, tsNodeFqdn) == 0) && (pNodeInfo->nodePort == tsSyncPort)) {
      pNode->selfIndex = index;
    }
  }

  if (pNode->selfIndex < 0) {
    sError("vgId:%d, this node is not configured", pNode->vgId);
    terrno = TSDB_CODE_SYN_INVALID_CONFIG;
    syncStop(pNode->rid);
    return -1;
  }

  nodeVersion = pInfo->version;  // set the initial version
  nodeRole = (pNode->replica > 1) ? TAOS_SYNC_ROLE_UNSYNCED : TAOS_SYNC_ROLE_MASTER;
  sInfo("vgId:%d, %d replicas are configured, quorum:%d role:%s", pNode->vgId, pNode->replica, pNode->quorum,
        syncRole[nodeRole]);

  pNode->pSyncFwds = calloc(sizeof(SSyncFwds) + SYNC_MAX_FWDS * sizeof(SFwdInfo), 1);
  if (pNode->pSyncFwds == NULL) {
    sError("vgId:%d, no memory to allocate syncFwds", pNode->vgId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    syncStop(pNode->rid);
    return -1;
  }

  pNode->pFwdTimer = taosTmrStart(syncMonitorFwdInfos, SYNC_FWD_TIMER, (void *)pNode->rid, tsSyncTmrCtrl);
  if (pNode->pFwdTimer == NULL) {
    sError("vgId:%d, failed to allocate fwd timer", pNode->vgId);
    syncStop(pNode->rid);
    return -1;
  }

  pNode->pRoleTimer = taosTmrStart(syncMonitorNodeRole, SYNC_ROLE_TIMER, (void *)pNode->rid, tsSyncTmrCtrl);
  if (pNode->pRoleTimer == NULL) {
    sError("vgId:%d, failed to allocate role timer", pNode->vgId);
    syncStop(pNode->rid);
    return -1;
  }

  syncAddArbitrator(pNode);
  taosHashPut(tsVgIdHash, &pNode->vgId, sizeof(int32_t), &pNode, sizeof(SSyncNode *));

  if (pNode->notifyRoleFp) {
    (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
  }

  syncStartCheckPeerConn(pNode->peerInfo[TAOS_SYNC_MAX_REPLICA]);  // arb
  for (int32_t index = 0; index < pNode->replica; ++index) {
    syncStartCheckPeerConn(pNode->peerInfo[index]);
  }

  return pNode->rid;
}

void syncStop(int64_t rid) {
  SSyncPeer *pPeer;

  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return;

  sInfo("vgId:%d, cleanup sync", pNode->vgId);

  pthread_mutex_lock(&pNode->mutex);

  if (tsVgIdHash) taosHashRemove(tsVgIdHash, &pNode->vgId, sizeof(int32_t));
  if (pNode->pFwdTimer) taosTmrStop(pNode->pFwdTimer);
  if (pNode->pRoleTimer) taosTmrStop(pNode->pRoleTimer);

  for (int32_t index = 0; index < pNode->replica; ++index) {
    pPeer = pNode->peerInfo[index];
    if (pPeer) syncRemovePeer(pPeer);
  }

  pPeer = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pPeer) syncRemovePeer(pPeer);

  pthread_mutex_unlock(&pNode->mutex);

  syncReleaseNode(pNode);
  taosRemoveRef(tsNodeRefId, rid);
}

int32_t syncReconfig(int64_t rid, const SSyncCfg *pNewCfg) {
  int32_t i, j;

  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return TSDB_CODE_SYN_INVALID_CONFIG;

  sInfo("vgId:%d, reconfig, role:%s replica:%d old:%d", pNode->vgId, syncRole[nodeRole], pNewCfg->replica,
        pNode->replica);

  pthread_mutex_lock(&pNode->mutex);

  syncStopCheckPeerConn(pNode->peerInfo[TAOS_SYNC_MAX_REPLICA]);  // arb
  for (int32_t index = 0; index < pNode->replica; ++index) {
    syncStopCheckPeerConn(pNode->peerInfo[index]);
  }

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

    if (newPeers[i] == NULL) {
      sError("vgId:%d, failed to reconfig", pNode->vgId);
      return TSDB_CODE_SYN_INVALID_CONFIG;
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
    (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
  }

  syncStartCheckPeerConn(pNode->peerInfo[TAOS_SYNC_MAX_REPLICA]);  // arb
  for (int32_t index = 0; index < pNode->replica; ++index) {
    syncStartCheckPeerConn(pNode->peerInfo[index]);
  }

  pthread_mutex_unlock(&pNode->mutex);

  sInfo("vgId:%d, %d replicas are configured, quorum:%d", pNode->vgId, pNode->replica, pNode->quorum);
  syncBroadcastStatus(pNode);

  syncReleaseNode(pNode);
  return 0;
}

int32_t syncForwardToPeer(int64_t rid, void *data, void *mhandle, int32_t qtype) {
  if (rid <= 0) return 0;

  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return 0;

  int32_t code = syncForwardToPeerImpl(pNode, data, mhandle, qtype);

  syncReleaseNode(pNode);
  return code;
}

void syncConfirmForward(int64_t rid, uint64_t version, int32_t code) {
  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return;

  SSyncPeer *pPeer = pNode->pMaster;
  if (pPeer && pNode->quorum > 1) {
    SFwdRsp rsp;
    syncBuildSyncFwdRsp(&rsp, pNode->vgId, version, code);

    if (taosWriteMsg(pPeer->peerFd, &rsp, sizeof(SFwdRsp)) == sizeof(SFwdRsp)) {
      sTrace("%s, forward-rsp is sent, code:0x%x hver:%" PRIu64, pPeer->id, code, version);
    } else {
      sDebug("%s, failed to send forward-rsp, restart", pPeer->id);
      syncRestartConnection(pPeer);
    }
  }

  syncReleaseNode(pNode);
}

#if 0
void syncRecover(int64_t rid) {
  SSyncPeer *pPeer;

  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return;

  // to do: add a few lines to check if recover is OK
  // if take this node to unsync state, the whole system may not work

  nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
  (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
  nodeVersion = 0;

  pthread_mutex_lock(&pNode->mutex);

  for (int32_t i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer->peerFd >= 0) {
      syncRestartConnection(pPeer);
    }
  }

  pthread_mutex_unlock(&pNode->mutex);

  syncReleaseNode(pNode);
}
#endif

int32_t syncGetNodesRole(int64_t rid, SNodesRole *pNodesRole) {
  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return -1;

  pNodesRole->selfIndex = pNode->selfIndex;
  for (int32_t i = 0; i < pNode->replica; ++i) {
    pNodesRole->nodeId[i] = pNode->peerInfo[i]->nodeId;
    pNodesRole->role[i] = pNode->peerInfo[i]->role;
  }

  syncReleaseNode(pNode);
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
  int32_t ret = taosGetFqdnPortFromEp(tsArbitrator, nodeInfo.nodeFqdn, &nodeInfo.nodePort);
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

  pPeer = syncAddPeer(pNode, &nodeInfo);
  if (pPeer != NULL) {
    pPeer->isArb = 1;
    sInfo("%s, is added as arbitrator", pPeer->id);
  }

  pNode->peerInfo[TAOS_SYNC_MAX_REPLICA] = pPeer;
}

static void syncFreeNode(void *param) {
  SSyncNode *pNode = param;
  sDebug("vgId:%d, node is freed, refCount:%d", pNode->vgId, pNode->refCount);

  pthread_mutex_destroy(&pNode->mutex);
  tfree(pNode->pRecv);
  tfree(pNode->pSyncFwds);
  tfree(pNode);
}

SSyncNode *syncAcquireNode(int64_t rid) {
  SSyncNode *pNode = taosAcquireRef(tsNodeRefId, rid);
  if (pNode == NULL) {
    sDebug("failed to acquire node from refId:%" PRId64, rid);
  } else {
    int32_t refCount = atomic_add_fetch_32(&pNode->refCount, 1);
    sTrace("vgId:%d, acquire node refId:%" PRId64 ", refCount:%d", pNode->vgId, rid, refCount);
  }

  return pNode;
}

void syncReleaseNode(SSyncNode *pNode) {
  int32_t refCount = atomic_sub_fetch_32(&pNode->refCount, 1);
  sTrace("vgId:%d, release node refId:%" PRId64 ", refCount:%d", pNode->vgId, pNode->rid, refCount);

  taosReleaseRef(tsNodeRefId, pNode->rid);
}

static void syncFreePeer(void *param) {
  SSyncPeer *pPeer = param;
  sDebug("%s, peer is freed, refCount:%d", pPeer->id, pPeer->refCount);

  syncReleaseNode(pPeer->pSyncNode);
  tfree(pPeer);
}

SSyncPeer *syncAcquirePeer(int64_t rid) {
  SSyncPeer *pPeer = taosAcquireRef(tsPeerRefId, rid);
  if (pPeer == NULL) {
    sDebug("failed to acquire peer from refId:%" PRId64, rid);
  } else {
    int32_t refCount = atomic_add_fetch_32(&pPeer->refCount, 1);
    sTrace("%s, acquire peer refId:%" PRId64 ", refCount:%d", pPeer->id, rid, refCount);
  }

  return pPeer;
}

void syncReleasePeer(SSyncPeer *pPeer) {
  int32_t refCount = atomic_sub_fetch_32(&pPeer->refCount, 1);
  sTrace("%s, release peer refId:%" PRId64 ", refCount:%d", pPeer->id, pPeer->rid, refCount);

  taosReleaseRef(tsPeerRefId, pPeer->rid);
}

static void syncClosePeerConn(SSyncPeer *pPeer) {
  sDebug("%s, pfd:%d sfd:%d will be closed", pPeer->id, pPeer->peerFd, pPeer->syncFd);

  taosTmrStopA(&pPeer->timer);
  taosCloseSocket(pPeer->syncFd);
  if (pPeer->peerFd >= 0) {
    pPeer->peerFd = -1;
    void *pConn = pPeer->pConn;
    if (pConn != NULL) syncFreeTcpConn(pPeer->pConn);
  }
}

static void syncRemovePeer(SSyncPeer *pPeer) {
  sInfo("%s, it is removed", pPeer->id);

  pPeer->ip = 0;
  syncClosePeerConn(pPeer);
  //taosRemoveRef(tsPeerRefId, pPeer->rid);
  syncReleasePeer(pPeer);
}

static void syncStartCheckPeerConn(SSyncPeer *pPeer) {
  if (pPeer == NULL) return;
  SSyncNode *pNode = pPeer->pSyncNode;

  int32_t ret = strcmp(pPeer->fqdn, tsNodeFqdn);
  if (pPeer->nodeId == 0 || (ret > 0) || (ret == 0 && pPeer->port > tsSyncPort)) {
    int32_t checkMs = 100 + (pNode->vgId * 10) % 100;

    sDebug("%s, check peer connection after %d ms", pPeer->id, checkMs);
    taosTmrReset(syncCheckPeerConnection, checkMs, (void *)pPeer->rid, tsSyncTmrCtrl, &pPeer->timer);
  }
}

static void syncStopCheckPeerConn(SSyncPeer *pPeer) {
  if (pPeer == NULL) return;

  taosTmrStopA(&pPeer->timer);
  sDebug("%s, stop check peer connection", pPeer->id);
}

static SSyncPeer *syncAddPeer(SSyncNode *pNode, const SNodeInfo *pInfo) {
  uint32_t ip = taosGetIpv4FromFqdn(pInfo->nodeFqdn);
  if (ip == 0xFFFFFFFF) {
    sError("failed to add peer, can resolve fqdn:%s since %s", pInfo->nodeFqdn, strerror(errno));
    terrno = TSDB_CODE_RPC_FQDN_ERROR;
    return NULL;
  }

  SSyncPeer *pPeer = calloc(1, sizeof(SSyncPeer));
  if (pPeer == NULL) return NULL;

  pPeer->nodeId = pInfo->nodeId;
  tstrncpy(pPeer->fqdn, pInfo->nodeFqdn, sizeof(pPeer->fqdn));
  pPeer->ip = ip;
  pPeer->port = pInfo->nodePort;
  pPeer->fqdn[sizeof(pPeer->fqdn) - 1] = 0;
  snprintf(pPeer->id, sizeof(pPeer->id), "vgId:%d, nodeId:%d", pNode->vgId, pPeer->nodeId);

  pPeer->peerFd = -1;
  pPeer->syncFd = -1;
  pPeer->role = TAOS_SYNC_ROLE_OFFLINE;
  pPeer->pSyncNode = pNode;
  pPeer->refCount = 1;
  pPeer->rid = taosAddRef(tsPeerRefId, pPeer);

  sInfo("%s, %p it is configured, ep:%s:%u rid:%" PRId64, pPeer->id, pPeer, pPeer->fqdn, pPeer->port, pPeer->rid);

  (void)syncAcquireNode(pNode->rid);
  return pPeer;
}

void syncBroadcastStatus(SSyncNode *pNode) {
  for (int32_t index = 0; index < pNode->replica; ++index) {
    if (index == pNode->selfIndex) continue;
    SSyncPeer *pPeer = pNode->peerInfo[index];
    syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_BROADCAST, syncGenTranId());
  }
}

static void syncResetFlowCtrl(SSyncNode *pNode) {
  for (int32_t index = 0; index < pNode->replica; ++index) {
    pNode->peerInfo[index]->numOfRetrieves = 0;
  }

  if (pNode->notifyFlowCtrlFp) {
    (*pNode->notifyFlowCtrlFp)(pNode->vgId, 0);
  }
}

static void syncChooseMaster(SSyncNode *pNode) {
  SSyncPeer *pPeer;
  int32_t    onlineNum = 0;
  int32_t    index = -1;
  int32_t    replica = pNode->replica;

  for (int32_t i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
    }
  }

  if (onlineNum == pNode->replica) {
    // if all peers are online, peer with highest version shall be master
    index = 0;
    for (int32_t i = 1; i < pNode->replica; ++i) {
      if (pNode->peerInfo[i]->version > pNode->peerInfo[index]->version) {
        index = i;
      }
    }
    sDebug("vgId:%d, master:%s may be choosed, index:%d", pNode->vgId, pNode->peerInfo[index]->id, index);
  } else {
    sDebug("vgId:%d, no master election since onlineNum:%d replica:%d", pNode->vgId, onlineNum, pNode->replica);
  }

  // add arbitrator connection
  SSyncPeer *pArb = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pArb) {
    if (pArb->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
      replica = pNode->replica + 1;
      sDebug("vgId:%d, arb:%s is used while choose master", pNode->vgId, pArb->id);
    } else {
      sError("vgId:%d, arb:%s is not used while choose master for its offline", pNode->vgId, pArb->id);
    }
  }

  if (index < 0 && onlineNum > replica / 2.0) {
    // over half of nodes are online
    for (int32_t i = 0; i < pNode->replica; ++i) {
      // slave with highest version shall be master
      pPeer = pNode->peerInfo[i];
      if (pPeer->role == TAOS_SYNC_ROLE_SLAVE || pPeer->role == TAOS_SYNC_ROLE_MASTER) {
        if (index < 0 || pPeer->version > pNode->peerInfo[index]->version) {
          index = i;
        }
      }
    }

    if (index >= 0) {
      sDebug("vgId:%d, master:%s may be choosed, index:%d onlineNum(arb):%d replica:%d", pNode->vgId,
             pNode->peerInfo[index]->id, index, onlineNum, replica);
    }
  }

  if (index >= 0) {
    if (index == pNode->selfIndex) {
      sInfo("vgId:%d, start to work as master", pNode->vgId);
      nodeRole = TAOS_SYNC_ROLE_MASTER;

      // Wait for other nodes to receive status to avoid version inconsistency
      taosMsleep(SYNC_WAIT_AFTER_CHOOSE_MASTER);

      syncResetFlowCtrl(pNode);
      (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
    } else {
      pPeer = pNode->peerInfo[index];
      sInfo("%s, it shall work as master", pPeer->id);
    }
  } else {
    sDebug("vgId:%d, failed to choose master", pNode->vgId);
  }
}

static SSyncPeer *syncCheckMaster(SSyncNode *pNode) {
  int32_t onlineNum = 0;
  int32_t masterIndex = -1;
  int32_t replica = pNode->replica;

  for (int32_t index = 0; index < pNode->replica; ++index) {
    if (pNode->peerInfo[index]->role != TAOS_SYNC_ROLE_OFFLINE) {
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
       if (nodeRole == TAOS_SYNC_ROLE_MASTER && onlineNum == replica * 0.5 && onlineNum >= 1) {
         sInfo("vgId:%d, self keep work as master, online:%d replica:%d", pNode->vgId, onlineNum, replica);
       } else {
        nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
        sInfo("vgId:%d, self change to unsynced state, online:%d replica:%d", pNode->vgId, onlineNum, replica);
       }
      (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
    }
  } else {
    for (int32_t index = 0; index < pNode->replica; ++index) {
      SSyncPeer *pTemp = pNode->peerInfo[index];
      if (pTemp->role != TAOS_SYNC_ROLE_MASTER) continue;
      if (masterIndex < 0) {
        masterIndex = index;
      } else {  // multiple masters, it shall not happen
        if (masterIndex == pNode->selfIndex) {
          sError("%s, peer is master, work as slave instead", pTemp->id);
          nodeRole = TAOS_SYNC_ROLE_SLAVE;
          (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
        }
      }
    }
  }

  SSyncPeer *pMaster = (masterIndex >= 0) ? pNode->peerInfo[masterIndex] : NULL;
  return pMaster;
}

static int32_t syncValidateMaster(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int32_t code = 0;

  if (nodeRole == TAOS_SYNC_ROLE_MASTER && nodeVersion < pPeer->version) {
    sDebug("%s, peer has higher sver:%" PRIu64 ", restart all peer connections", pPeer->id, pPeer->version);
    nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
    (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
    code = -1;

    for (int32_t index = 0; index < pNode->replica; ++index) {
      if (index == pNode->selfIndex) continue;
      syncRestartPeer(pNode->peerInfo[index]);
    }
  }

  return code;
}

static void syncCheckRole(SSyncPeer *pPeer, SPeerStatus* peersStatus, int8_t newPeerRole) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int8_t oldPeerRole = pPeer->role;
  int8_t oldSelfRole = nodeRole;
  int8_t syncRequired = 0;

  pPeer->role = newPeerRole;
  sDebug("%s, peer role:%s change to %s", pPeer->id, syncRole[oldPeerRole], syncRole[newPeerRole]);

  SSyncPeer *pMaster = syncCheckMaster(pNode);

  if (pMaster) {
    // master is there
    pNode->pMaster = pMaster;
    sDebug("%s, it is the master, replica:%d sver:%" PRIu64, pMaster->id, pNode->replica, pMaster->version);

    if (syncValidateMaster(pPeer) < 0) return;

    if (nodeRole == TAOS_SYNC_ROLE_UNSYNCED) {
      if (nodeVersion < pMaster->version) {
        sDebug("%s, is master, sync required, self sver:%" PRIu64, pMaster->id, nodeVersion);
        syncRequired = 1;
      } else {
        sInfo("%s, is master, work as slave, self sver:%" PRIu64, pMaster->id, nodeVersion);
        nodeRole = TAOS_SYNC_ROLE_SLAVE;
        (*pNode->notifyRoleFp)(pNode->vgId, nodeRole);
      }
    } else if (nodeRole == TAOS_SYNC_ROLE_SLAVE && pMaster == pPeer) {
      sDebug("%s, is master, continue work as slave, self sver:%" PRIu64, pMaster->id, nodeVersion);
    }
  } else {
    // master not there, if all peer's state and version are consistent, choose the master
    int32_t consistent = 0;
    int32_t index = 0;
    if (peersStatus != NULL) {
      for (index = 0; index < pNode->replica; ++index) {
        SSyncPeer *pTemp = pNode->peerInfo[index];
        if (pTemp->role != peersStatus[index].role) break;
        if ((pTemp->role != TAOS_SYNC_ROLE_OFFLINE) && (pTemp->version != peersStatus[index].version)) break;
      }

      if (index >= pNode->replica) consistent = 1;
    } else {
      if (pNode->replica == 2) consistent = 1;
    }

    if (consistent) {
      sDebug("vgId:%d, choose master, replica:%d", pNode->vgId, pNode->replica);
      syncChooseMaster(pNode);
    } else {
      sDebug("vgId:%d, cannot choose master since roles inequality, replica:%d", pNode->vgId, pNode->replica);
    }
  }

  if (syncRequired) {
    syncRecoverFromMaster(pMaster);
  }

  if (oldPeerRole != newPeerRole || nodeRole != oldSelfRole) {
    sDebug("vgId:%d, roles changed, broadcast status", pNode->vgId);
    syncBroadcastStatus(pNode);
  }

  if (nodeRole != TAOS_SYNC_ROLE_MASTER) {
    syncResetFlowCtrl(pNode);
  }
}

static void syncRestartPeer(SSyncPeer *pPeer) {
  sDebug("%s, restart peer connection, last sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);

  syncClosePeerConn(pPeer);

  pPeer->sstatus = TAOS_SYNC_STATUS_INIT;
  sDebug("%s, peer conn is restart and set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);

  int32_t ret = strcmp(pPeer->fqdn, tsNodeFqdn);
  if (ret > 0 || (ret == 0 && pPeer->port > tsSyncPort)) {
    sDebug("%s, check peer connection in 1000 ms", pPeer->id);
    taosTmrReset(syncCheckPeerConnection, SYNC_CHECK_INTERVAL, (void *)pPeer->rid, tsSyncTmrCtrl, &pPeer->timer);
  }
}

void syncRestartConnection(SSyncPeer *pPeer) {
  if (pPeer->ip == 0) return;

  syncRestartPeer(pPeer);
  syncCheckRole(pPeer, NULL, TAOS_SYNC_ROLE_OFFLINE);
}

static void syncProcessSyncRequest(char *msg, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  sInfo("%s, sync-req is received", pPeer->id);

  if (pPeer->ip == 0) return;

  if (nodeRole != TAOS_SYNC_ROLE_MASTER) {
    sError("%s, I am not master anymore", pPeer->id);
    taosCloseSocket(pPeer->syncFd);
    return;
  }

  if (pPeer->sstatus != TAOS_SYNC_STATUS_INIT) {
    sDebug("%s, sync is already started for sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
    return;  // already started
  }

  // start a new thread to retrieve the data
  (void)syncAcquirePeer(pPeer->rid);

  pthread_attr_t thattr;
  pthread_t      thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  int32_t ret = pthread_create(&thread, &thattr, syncRetrieveData, (void *)pPeer->rid);
  pthread_attr_destroy(&thattr);

  if (ret < 0) {
    sError("%s, failed to create sync retrieve thread since %s", pPeer->id, strerror(errno));
    syncReleasePeer(pPeer);
  } else {
    pPeer->sstatus = TAOS_SYNC_STATUS_START;
    sDebug("%s, sync retrieve thread:0x%08" PRIx64 " create successfully, rid:%" PRId64 ", set sstatus:%s", pPeer->id,
           taosGetPthreadId(thread), pPeer->rid, syncStatus[pPeer->sstatus]);
  }
}

static void syncNotStarted(void *param, void *tmrId) {
  int64_t    rid = (int64_t)param;
  SSyncPeer *pPeer = syncAcquirePeer(rid);
  if (pPeer == NULL) return;

  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&pNode->mutex);
  pPeer->timer = NULL;
  pPeer->sstatus = TAOS_SYNC_STATUS_INIT;
  sInfo("%s, sync conn is still not up, restart and set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
  syncRestartConnection(pPeer);
  pthread_mutex_unlock(&pNode->mutex);

  syncReleasePeer(pPeer);
}

static void syncTryRecoverFromMaster(void *param, void *tmrId) {
  int64_t    rid = (int64_t)param;
  SSyncPeer *pPeer = syncAcquirePeer(rid);
  if (pPeer == NULL) return;

  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&pNode->mutex);
  syncRecoverFromMaster(pPeer);
  pthread_mutex_unlock(&pNode->mutex);

  syncReleasePeer(pPeer);
}

static void syncRecoverFromMaster(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  if (nodeSStatus != TAOS_SYNC_STATUS_INIT) {
    sDebug("%s, sync is already started since sstatus:%s", pPeer->id, syncStatus[nodeSStatus]);
    return;
  }

  taosTmrStopA(&pPeer->timer);

  // Ensure the sync of mnode not interrupted
  if (pNode->vgId != 1 && tsSyncNum >= SYNC_MAX_NUM) {
    sInfo("%s, %d syncs are in process, try later", pPeer->id, tsSyncNum);
    taosTmrReset(syncTryRecoverFromMaster, 500 + (pNode->vgId * 10) % 200, (void *)pPeer->rid, tsSyncTmrCtrl, &pPeer->timer);
    return;
  }

  sDebug("%s, try to sync", pPeer->id);

  SSyncMsg msg;
  syncBuildSyncReqMsg(&msg, pNode->vgId);

  taosTmrReset(syncNotStarted, SYNC_CHECK_INTERVAL, (void *)pPeer->rid, tsSyncTmrCtrl, &pPeer->timer);

  if (taosWriteMsg(pPeer->peerFd, &msg, sizeof(SSyncMsg)) != sizeof(SSyncMsg)) {
    sError("%s, failed to send sync-req to peer", pPeer->id);
  } else {
    sInfo("%s, sync-req is sent to peer, tranId:%u, sstatus:%s", pPeer->id, msg.tranId, syncStatus[nodeSStatus]);
  }
}

static void syncProcessFwdResponse(SFwdRsp *pFwdRsp, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;
  SFwdInfo * pFwdInfo;

  sTrace("%s, forward-rsp is received, code:%x hver:%" PRIu64, pPeer->id, pFwdRsp->code, pFwdRsp->version);
  SFwdInfo *pFirst = pSyncFwds->fwdInfo + pSyncFwds->first;

  if (pFirst->version <= pFwdRsp->version && pSyncFwds->fwds > 0) {
    // find the forwardInfo from first
    for (int32_t i = 0; i < pSyncFwds->fwds; ++i) {
      pFwdInfo = pSyncFwds->fwdInfo + (i + pSyncFwds->first) % SYNC_MAX_FWDS;
      if (pFwdRsp->version == pFwdInfo->version) {
        syncProcessFwdAck(pNode, pFwdInfo, pFwdRsp->code);
        syncRemoveConfirmedFwdInfo(pNode);
        return;
      }
    }
  }
}

static void syncProcessForwardFromPeer(char *cont, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SWalHead * pHead = (SWalHead *)(cont + sizeof(SSyncHead));

  sTrace("%s, forward is received, hver:%" PRIu64 ", len:%d", pPeer->id, pHead->version, pHead->len);

  if (nodeRole == TAOS_SYNC_ROLE_SLAVE) {
    // nodeVersion = pHead->version;
    (*pNode->writeToCacheFp)(pNode->vgId, pHead, TAOS_QTYPE_FWD, NULL);
  } else {
    if (nodeSStatus != TAOS_SYNC_STATUS_INIT) {
      syncSaveIntoBuffer(pPeer, pHead);
    } else {
      sError("%s, forward discarded since sstatus:%s, hver:%" PRIu64, pPeer->id, syncStatus[nodeSStatus],
             pHead->version);
    }
  }
}

static void syncProcessPeersStatusMsg(SPeersStatus *pPeersStatus, SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  sDebug("%s, status is received, self:%s:%s:%" PRIu64 ", peer:%s:%" PRIu64 ", ack:%d tranId:%u type:%s pfd:%d",
         pPeer->id, syncRole[nodeRole], syncStatus[nodeSStatus], nodeVersion, syncRole[pPeersStatus->role],
         pPeersStatus->version, pPeersStatus->ack, pPeersStatus->tranId, statusType[pPeersStatus->type], pPeer->peerFd);

  pPeer->version = pPeersStatus->version;
  syncCheckRole(pPeer, pPeersStatus->peersStatus, pPeersStatus->role);

  if (pPeersStatus->ack) {
    syncSendPeersStatusMsgToPeer(pPeer, 0, pPeersStatus->type + 1, pPeersStatus->tranId);
  }
}

static int32_t syncReadPeerMsg(SSyncPeer *pPeer, SSyncHead *pHead) {
  if (pPeer->peerFd < 0) return -1;

  int32_t hlen = taosReadMsg(pPeer->peerFd, pHead, sizeof(SSyncHead));
  if (hlen != sizeof(SSyncHead)) {
    sDebug("%s, failed to read msg since %s, hlen:%d", pPeer->id, tstrerror(errno), hlen);
    return -1;
  }

  int32_t code = syncCheckHead(pHead);
  if (code != 0) {
    sError("%s, failed to check msg head since %s, type:%d", pPeer->id, tstrerror(code), pHead->type);
    return -1;
  }

  int32_t bytes = taosReadMsg(pPeer->peerFd, (char *)pHead + sizeof(SSyncHead), pHead->len);
  if (bytes != pHead->len) {
    sError("%s, failed to read, bytes:%d len:%d", pPeer->id, bytes, pHead->len);
    return -1;
  }

  return 0;
}

static int32_t syncProcessPeerMsg(int64_t rid, void *buffer) {
  SSyncPeer *pPeer = syncAcquirePeer(rid);
  if (pPeer == NULL) return -1;

  SSyncHead *pHead = buffer;
  SSyncNode *pNode = pPeer->pSyncNode;
  
  pthread_mutex_lock(&pNode->mutex);

  int32_t code = syncReadPeerMsg(pPeer, pHead);

  if (code == 0) {
    if (pHead->type == TAOS_SMSG_SYNC_FWD) {
      syncProcessForwardFromPeer(buffer, pPeer);
    } else if (pHead->type == TAOS_SMSG_SYNC_FWD_RSP) {
      syncProcessFwdResponse(buffer, pPeer);
    } else if (pHead->type == TAOS_SMSG_SYNC_REQ) {
      syncProcessSyncRequest(buffer, pPeer);
    } else if (pHead->type == TAOS_SMSG_STATUS) {
      syncProcessPeersStatusMsg(buffer, pPeer);
    }
  }

  pthread_mutex_unlock(&pNode->mutex);
  syncReleasePeer(pPeer);

  return code;
}

static int32_t syncSendPeersStatusMsgToPeer(SSyncPeer *pPeer, char ack, int8_t type, uint16_t tranId) {
  if (pPeer->peerFd < 0 || pPeer->ip == 0) {
    sDebug("%s, failed to send status msg, restart fd:%d", pPeer->id, pPeer->peerFd);
    syncRestartConnection(pPeer);
    return -1;
  }

  SSyncNode *  pNode = pPeer->pSyncNode;
  SPeersStatus msg;

  memset(&msg, 0, sizeof(SPeersStatus));
  syncBuildPeersStatus(&msg, pNode->vgId);

  msg.role = nodeRole;
  msg.ack = ack;
  msg.type = type;
  msg.tranId = tranId;
  msg.version = nodeVersion;

  for (int32_t i = 0; i < pNode->replica; ++i) {
    msg.peersStatus[i].role = pNode->peerInfo[i]->role;
    msg.peersStatus[i].version = pNode->peerInfo[i]->version;
  }

  if (taosWriteMsg(pPeer->peerFd, &msg, sizeof(SPeersStatus)) == sizeof(SPeersStatus)) {
    sDebug("%s, status is sent, self:%s:%s:%" PRIu64 ", peer:%s:%s:%" PRIu64 ", ack:%d tranId:%u type:%s pfd:%d",
           pPeer->id, syncRole[nodeRole], syncStatus[nodeSStatus], nodeVersion, syncRole[pPeer->role],
           syncStatus[pPeer->sstatus], pPeer->version, ack, tranId, statusType[type], pPeer->peerFd);
    return 0;
  } else {
    sDebug("%s, failed to send status msg, restart", pPeer->id);
    syncRestartConnection(pPeer);
    return -1;
  }
}

static void syncSetupPeerConnection(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  taosTmrStopA(&pPeer->timer);
  if (pPeer->peerFd >= 0) {
    sDebug("%s, send role version to peer", pPeer->id);
    syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_SETUP_CONN, syncGenTranId());
    return;
  }

  SOCKET connFd = taosOpenTcpClientSocket(pPeer->ip, pPeer->port, 0);
  if (connFd <= 0) {
    sDebug("%s, failed to open tcp socket since %s", pPeer->id, strerror(errno));
    taosTmrReset(syncCheckPeerConnection, SYNC_CHECK_INTERVAL, (void *)pPeer->rid, tsSyncTmrCtrl, &pPeer->timer);
    return;
  }

  SSyncMsg msg;
  syncBuildSyncSetupMsg(&msg, pPeer->nodeId ? pNode->vgId : 0);

  if (taosWriteMsg(connFd, &msg, sizeof(SSyncMsg)) == sizeof(SSyncMsg)) {
    sDebug("%s, connection to peer server is setup, pfd:%d sfd:%d tranId:%u", pPeer->id, connFd, pPeer->syncFd, msg.tranId);
    pPeer->peerFd = connFd;
    pPeer->role = TAOS_SYNC_ROLE_UNSYNCED;
    pPeer->pConn = syncAllocateTcpConn(tsTcpPool, pPeer->rid, connFd);
    if (pPeer->isArb) tsArbOnline = 1;
  } else {
    sDebug("%s, failed to setup peer connection to server since %s, try later", pPeer->id, strerror(errno));
    taosCloseSocket(connFd);
    taosTmrReset(syncCheckPeerConnection, SYNC_CHECK_INTERVAL, (void *)pPeer->rid, tsSyncTmrCtrl, &pPeer->timer);
  }
}

static void syncCheckPeerConnection(void *param, void *tmrId) {
  int64_t    rid = (int64_t)param;
  SSyncPeer *pPeer = syncAcquirePeer(rid);
  if (pPeer == NULL) return;

  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&pNode->mutex);

  sDebug("%s, check peer connection", pPeer->id);
  syncSetupPeerConnection(pPeer);

  pthread_mutex_unlock(&pNode->mutex);

  syncReleasePeer(pPeer);
}

static void syncCreateRestoreDataThread(SSyncPeer *pPeer) {
  taosTmrStopA(&pPeer->timer);

  pthread_attr_t thattr;
  pthread_t      thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

  (void)syncAcquirePeer(pPeer->rid);

  int32_t ret = pthread_create(&thread, &thattr, (void *)syncRestoreData, (void *)pPeer->rid);
  pthread_attr_destroy(&thattr);

  if (ret < 0) {
    SSyncNode *pNode = pPeer->pSyncNode;
    nodeSStatus = TAOS_SYNC_STATUS_INIT;
    sError("%s, failed to create sync restore thread, set sstatus:%s", pPeer->id, syncStatus[nodeSStatus]);
    taosCloseSocket(pPeer->syncFd);
    syncReleasePeer(pPeer);
  } else {
    sInfo("%s, sync restore thread:0x%08" PRIx64 " create successfully, rid:%" PRId64, pPeer->id,
          taosGetPthreadId(thread), pPeer->rid);
  }
}

void syncProcessTestMsg(SSyncMsg *pMsg, SOCKET connFd) {
  sInfo("recv sync test msg");

  SSyncMsg rsp;
  syncBuildSyncTestMsg(&rsp, -1);
  if (taosWriteMsg(connFd, &rsp, sizeof(SSyncMsg)) != sizeof(SSyncMsg)) {
    sInfo("failed to send sync test rsp since %s", strerror(errno));
  }

  sInfo("send sync test rsp");
  taosMsleep(1000);
  taosCloseSocket(connFd);
}

static void syncProcessIncommingConnection(SOCKET connFd, uint32_t sourceIp) {
  char    ipstr[24];
  int32_t i;

  tinet_ntoa(ipstr, sourceIp);
  sDebug("peer TCP connection from ip:%s", ipstr);

  SSyncMsg msg;
  if (taosReadMsg(connFd, &msg, sizeof(SSyncMsg)) != sizeof(SSyncMsg)) {
    sError("failed to read peer sync msg from ip:%s since %s", ipstr, strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  int32_t code = syncCheckHead((SSyncHead *)(&msg));
  if (code != 0) {
    sError("failed to check peer sync msg from ip:%s since %s", ipstr, strerror(code));
    taosCloseSocket(connFd);
    return;
  }

  if (msg.head.type == TAOS_SMSG_TEST) {
    syncProcessTestMsg(&msg, connFd);
    return;
  }

  int32_t     vgId = msg.head.vgId;
  SSyncNode **ppNode = taosHashGet(tsVgIdHash, &vgId, sizeof(int32_t));
  if (ppNode == NULL || *ppNode == NULL) {
    sError("vgId:%d, vgId could not be found", vgId);
    taosCloseSocket(connFd);
    return;
  }

  sDebug("vgId:%d, sync connection is incoming, tranId:%u", vgId, msg.tranId);

  SSyncNode *pNode = *ppNode;
  pthread_mutex_lock(&pNode->mutex);

  SSyncPeer *pPeer;
  for (i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer && (strcmp(pPeer->fqdn, msg.fqdn) == 0) && (pPeer->port == msg.port)) break;
  }

  pPeer = (i < pNode->replica) ? pNode->peerInfo[i] : NULL;
  if (pPeer == NULL) {
    sError("vgId:%d, peer:%s:%u not configured", pNode->vgId, msg.fqdn, msg.port);
    taosCloseSocket(connFd);
    // syncSendVpeerCfgMsg(sync);
  } else {
    // first packet tells what kind of link
    if (msg.head.type == TAOS_SMSG_SYNC_DATA) {
      pPeer->syncFd = connFd;
      nodeSStatus = TAOS_SYNC_STATUS_START;
      sInfo("%s, sync-data msg from master is received, tranId:%u, set sstatus:%s", pPeer->id, msg.tranId,
            syncStatus[nodeSStatus]);
      syncCreateRestoreDataThread(pPeer);
    } else {
      sDebug("%s, TCP connection is up, pfd:%d sfd:%d, old pfd:%d", pPeer->id, connFd, pPeer->syncFd, pPeer->peerFd);
      syncClosePeerConn(pPeer);
      pPeer->peerFd = connFd;
      pPeer->pConn = syncAllocateTcpConn(tsTcpPool, pPeer->rid, connFd);
      sDebug("%s, ready to exchange data", pPeer->id);
      syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_EXCHANGE_DATA, syncGenTranId());
    }
  }

  pthread_mutex_unlock(&pNode->mutex);
}

static void syncProcessBrokenLink(int64_t rid) {
  SSyncPeer *pPeer = syncAcquirePeer(rid);
  if (pPeer == NULL) return;

  SSyncNode *pNode = pPeer->pSyncNode;

  pthread_mutex_lock(&pNode->mutex);

  sDebug("%s, TCP link is broken since %s, pfd:%d sfd:%d", pPeer->id, strerror(errno), pPeer->peerFd, pPeer->syncFd);
  pPeer->peerFd = -1;
  if (pPeer->isArb) {
    tsArbOnline = 0;
  }

  syncRestartConnection(pPeer);
  pthread_mutex_unlock(&pNode->mutex);

  syncReleasePeer(pPeer);
}

static int32_t syncSaveFwdInfo(SSyncNode *pNode, uint64_t version, void *mhandle) {
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;
  int64_t    time = taosGetTimestampMs();

  if (pSyncFwds->fwds >= SYNC_MAX_FWDS) {
    // pSyncFwds->first = (pSyncFwds->first + 1) % SYNC_MAX_FWDS;
    // pSyncFwds->fwds--;
    sError("vgId:%d, failed to save fwd info, hver:%" PRIu64 " fwds:%d", pNode->vgId, version, pSyncFwds->fwds);
    return TSDB_CODE_SYN_TOO_MANY_FWDINFO;
  }

  if (pSyncFwds->fwds > 0) {
    pSyncFwds->last = (pSyncFwds->last + 1) % SYNC_MAX_FWDS;
  }

  SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + pSyncFwds->last;
  memset(pFwdInfo, 0, sizeof(SFwdInfo));
  pFwdInfo->version = version;
  pFwdInfo->mhandle = mhandle;
  pFwdInfo->time = time;

  pSyncFwds->fwds++;
  sTrace("vgId:%d, fwd info is saved, hver:%" PRIu64 " fwds:%d ", pNode->vgId, version, pSyncFwds->fwds);

  return 0;
}

static void syncRemoveConfirmedFwdInfo(SSyncNode *pNode) {
  SSyncFwds *pSyncFwds = pNode->pSyncFwds;

  int32_t fwds = pSyncFwds->fwds;
  for (int32_t i = 0; i < fwds; ++i) {
    SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + pSyncFwds->first;
    if (pFwdInfo->confirmed == 0) break;

    pSyncFwds->first = (pSyncFwds->first + 1) % SYNC_MAX_FWDS;
    pSyncFwds->fwds--;
    if (pSyncFwds->fwds == 0) pSyncFwds->first = pSyncFwds->last;
    sTrace("vgId:%d, fwd info is removed, hver:%" PRIu64 " fwds:%d", pNode->vgId, pFwdInfo->version, pSyncFwds->fwds);
    memset(pFwdInfo, 0, sizeof(SFwdInfo));
  }
}

static void syncProcessFwdAck(SSyncNode *pNode, SFwdInfo *pFwdInfo, int32_t code) {
  int32_t confirm = 0;
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
    sTrace("vgId:%d, forward is confirmed, hver:%" PRIu64 " code:0x%x", pNode->vgId, pFwdInfo->version, pFwdInfo->code);
    (*pNode->confirmForward)(pNode->vgId, pFwdInfo->mhandle, pFwdInfo->code);
    pFwdInfo->confirmed = 1;
  }
}

static void syncMonitorNodeRole(void *param, void *tmrId) {
  int64_t    rid = (int64_t)param;
  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return;

  for (int32_t index = 0; index < pNode->replica; index++) {
    if (index == pNode->selfIndex) continue;

    SSyncPeer *pPeer = pNode->peerInfo[index];
    if (/*pPeer->role > TAOS_SYNC_ROLE_UNSYNCED && */ nodeRole > TAOS_SYNC_ROLE_UNSYNCED) continue;
    if (/*pPeer->sstatus > TAOS_SYNC_STATUS_INIT || */ nodeSStatus > TAOS_SYNC_STATUS_INIT) continue;

    sDebug("%s, check roles since self:%s sstatus:%s, peer:%s sstatus:%s", pPeer->id, syncRole[pPeer->role],
           syncStatus[pPeer->sstatus], syncRole[nodeRole], syncStatus[nodeSStatus]);
    syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_CHECK_ROLE, syncGenTranId());
    break;
  }

  pNode->pRoleTimer = taosTmrStart(syncMonitorNodeRole, SYNC_ROLE_TIMER, (void *)pNode->rid, tsSyncTmrCtrl);
  syncReleaseNode(pNode);
}

static void syncMonitorFwdInfos(void *param, void *tmrId) {
  int64_t    rid = (int64_t)param;
  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return;

  SSyncFwds *pSyncFwds = pNode->pSyncFwds;

  if (pSyncFwds) {
    int64_t time = taosGetTimestampMs();

    if (pSyncFwds->fwds > 0) {
      pthread_mutex_lock(&pNode->mutex);
      for (int32_t i = 0; i < pSyncFwds->fwds; ++i) {
        SFwdInfo *pFwdInfo = pSyncFwds->fwdInfo + (pSyncFwds->first + i) % SYNC_MAX_FWDS;
        if (ABS(time - pFwdInfo->time) < 2000) break;

        sDebug("vgId:%d, forward info expired, hver:%" PRIu64 " curtime:%" PRIu64 " savetime:%" PRIu64, pNode->vgId,
               pFwdInfo->version, time, pFwdInfo->time);
        syncProcessFwdAck(pNode, pFwdInfo, TSDB_CODE_SYN_CONFIRM_EXPIRED);
      }

      syncRemoveConfirmedFwdInfo(pNode);
      pthread_mutex_unlock(&pNode->mutex);
    }

    pNode->pFwdTimer = taosTmrStart(syncMonitorFwdInfos, SYNC_FWD_TIMER, (void *)pNode->rid, tsSyncTmrCtrl);
  }

  syncReleaseNode(pNode);
}

static int32_t syncForwardToPeerImpl(SSyncNode *pNode, void *data, void *mhandle, int32_t qtype) {
  SSyncPeer *pPeer;
  SSyncHead *pSyncHead;
  SWalHead * pWalHead = data;
  int32_t    fwdLen;
  int32_t    code = 0;

  if (pWalHead->version > nodeVersion + 1) {
    sError("vgId:%d, hver:%" PRIu64 ", inconsistent with sver:%" PRIu64, pNode->vgId, pWalHead->version, nodeVersion);
    if (nodeRole == TAOS_SYNC_ROLE_SLAVE) {
      sInfo("vgId:%d, restart connection", pNode->vgId);
      for (int32_t i = 0; i < pNode->replica; ++i) {
        pPeer = pNode->peerInfo[i];
        syncRestartConnection(pPeer);
      }
    }

    if (pNode->replica != 1) {
      return TSDB_CODE_SYN_INVALID_VERSION;
    }
  }

  // always update version
  sTrace("vgId:%d, update version, replica:%d role:%s qtype:%s hver:%" PRIu64, pNode->vgId, pNode->replica,
         syncRole[nodeRole], qtypeStr[qtype], pWalHead->version);
  nodeVersion = pWalHead->version;

  if (pNode->replica == 1 || nodeRole != TAOS_SYNC_ROLE_MASTER) return 0;

  // only msg from RPC or CQ can be forwarded
  if (qtype != TAOS_QTYPE_RPC && qtype != TAOS_QTYPE_CQ) return 0;

    // a hacker way to improve the performance
  pSyncHead = (SSyncHead *)(((char *)pWalHead) - sizeof(SSyncHead));
  syncBuildSyncFwdMsg(pSyncHead, pNode->vgId, sizeof(SWalHead) + pWalHead->len);
  fwdLen = pSyncHead->len + sizeof(SSyncHead);  // include the WAL and SYNC head

  pthread_mutex_lock(&pNode->mutex);

  for (int32_t i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer == NULL || pPeer->peerFd < 0) continue;
    if (pPeer->role != TAOS_SYNC_ROLE_SLAVE && pPeer->sstatus != TAOS_SYNC_STATUS_CACHE) continue;

    if (pNode->quorum > 1 && code == 0) {
      code = syncSaveFwdInfo(pNode, pWalHead->version, mhandle);
      if (code >= 0) code = 1;
    }

    int32_t retLen = taosWriteMsg(pPeer->peerFd, pSyncHead, fwdLen);
    if (retLen == fwdLen) {
      sTrace("%s, forward is sent, role:%s sstatus:%s hver:%" PRIu64 " contLen:%d", pPeer->id, syncRole[pPeer->role],
             syncStatus[pPeer->sstatus], pWalHead->version, pWalHead->len);
    } else {
      sError("%s, failed to forward, role:%s sstatus:%s hver:%" PRIu64 " retLen:%d", pPeer->id, syncRole[pPeer->role],
             syncStatus[pPeer->sstatus], pWalHead->version, retLen);
      syncRestartConnection(pPeer);
    }
  }

  pthread_mutex_unlock(&pNode->mutex);

  return code;
}
