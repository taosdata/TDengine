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

#include "syncInt.h"
#include "trpc.h"
#include "ttimer.h"

SSyncManager* gSyncManager = NULL;

#define SYNC_TICK_TIMER 50
#define SYNC_ACTIVITY_TIMER 5
#define SYNC_SERVER_WORKER 2

static void syncProcessRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static void syncProcessReqMsg(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);

static int syncInitRpcServer(SSyncManager* syncManager, const SSyncCluster* pSyncCfg);
static int syncInitRpcClient(SSyncManager* syncManager);
static int syncOpenWorkerPool(SSyncManager* syncManager);
static int syncCloseWorkerPool(SSyncManager* syncManager);
static void *syncWorkerMain(void *argv);
static void syncNodeTick(void *param, void *tmrId);

int32_t syncInit() { 
  if (gSyncManager != NULL) {
    return 0;
  }

  gSyncManager = (SSyncManager*)calloc(sizeof(SSyncManager), 0);
  if (gSyncManager == NULL) {
    syncError("malloc SSyncManager fail");
    return -1;
  }

  pthread_mutex_init(&gSyncManager->mutex, NULL);

  // init client rpc
  if (syncInitRpcClient(gSyncManager) != 0) {
    syncCleanUp();
    return -1;
  }

  // init sync timer manager
  gSyncManager->syncTimerManager = taosTmrInit(1000, 50, 10000, "SYNC");
  if (gSyncManager->syncTimerManager == NULL) {
    syncCleanUp();
    return -1;
  }

  // init worker pool
  if (syncOpenWorkerPool(gSyncManager) != 0) {
    syncCleanUp();
    return -1;
  }

  // init vgroup hash table
  gSyncManager->vgroupTable = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (gSyncManager->vgroupTable == NULL) {
    syncCleanUp();
    return -1;
  }
  return 0; 
}

void syncCleanUp() {
  if (gSyncManager == NULL) {
    return;
  }
  pthread_mutex_lock(&gSyncManager->mutex);
  if (gSyncManager->vgroupTable) {
    taosHashCleanup(gSyncManager->vgroupTable);
  }
  if (gSyncManager->clientRpc) {
    rpcClose(gSyncManager->clientRpc);
    syncInfo("sync inter-sync rpc client is closed");
  }
  if (gSyncManager->syncTimerManager) {
    taosTmrCleanUp(gSyncManager->syncTimerManager);
  }
  syncCloseWorkerPool(gSyncManager);
  pthread_mutex_unlock(&gSyncManager->mutex);
  pthread_mutex_destroy(&gSyncManager->mutex);
  free(gSyncManager);
  gSyncManager = NULL;
}

SSyncNode* syncStart(const SSyncInfo* pInfo) {
  pthread_mutex_lock(&gSyncManager->mutex);

  SSyncNode **ppNode = taosHashGet(gSyncManager->vgroupTable, &pInfo->vgId, sizeof(SyncGroupId*));
  if (ppNode != NULL) {
    syncInfo("vgroup %d already exist", pInfo->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return *ppNode;
  }

  // init rpc server
  if (syncInitRpcServer(gSyncManager, &pInfo->syncCfg) != 0) {
    pthread_mutex_unlock(&gSyncManager->mutex);
    return NULL;
  }

  SSyncNode *pNode = (SSyncNode*)malloc(sizeof(SSyncNode));
  if (pNode == NULL) {
    syncError("malloc vgroup %d node fail", pInfo->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return NULL;
  }

  pNode->syncTimer = taosTmrStart(syncNodeTick, SYNC_TICK_TIMER, (void*)((int64_t)pInfo->vgId), gSyncManager->syncTimerManager);

  // start raft
  pNode->raft.pNode = pNode;
  if (syncRaftStart(&pNode->raft, pInfo) != 0) {
    syncError("raft start at %d node fail", pInfo->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return NULL;
  }

  pthread_mutex_init(&pNode->mutex, NULL);

  taosHashPut(gSyncManager->vgroupTable, &pInfo->vgId, sizeof(SyncGroupId), &pNode, sizeof(SSyncNode *));

  pthread_mutex_unlock(&gSyncManager->mutex);
  return NULL;
}

void syncStop(const SSyncNode* pNode) {
  pthread_mutex_lock(&gSyncManager->mutex);

  SSyncNode **ppNode = taosHashGet(gSyncManager->vgroupTable, &pNode->vgId, sizeof(SyncGroupId*));
  if (ppNode == NULL) {
    syncInfo("vgroup %d not exist", pNode->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return;
  }
  assert(*ppNode == pNode);
  taosTmrStop(pNode->syncTimer);

  taosHashRemove(gSyncManager->vgroupTable, &pNode->vgId, sizeof(SyncGroupId));
  pthread_mutex_unlock(&gSyncManager->mutex);

  pthread_mutex_destroy(&((*ppNode)->mutex));
  free(*ppNode);
}

int32_t syncPropose(SSyncNode* syncNode, const SSyncBuffer* pBuf, void* pData, bool isWeak) {
  SSyncMessage msg;

  pthread_mutex_lock(&syncNode->mutex);
  int32_t ret = syncRaftStep(&syncNode->raft, syncInitPropMsg(&msg, pBuf, pData, isWeak));
  pthread_mutex_unlock(&syncNode->mutex);
  return ret;
}

void syncReconfig(const SSyncNode* pNode, const SSyncCluster* pCfg) {}

int32_t syncAddNode(SSyncNode syncNode, const SNodeInfo *pNode) {
  return 0;
}

int32_t syncRemoveNode(SSyncNode syncNode, const SNodeInfo *pNode) {
  return 0;
}

// process rpc rsp message from other sync server
static void syncProcessRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {

}

// process rpc message from other sync server
static void syncProcessReqMsg(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {

}

static int syncInitRpcServer(SSyncManager* syncManager, const SSyncCluster* pSyncCfg) {
  if (gSyncManager->rpcServerTable == NULL) {
    gSyncManager->rpcServerTable = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (gSyncManager->rpcServerTable == NULL) {
      syncError("init sync rpc server hash table error");
      return -1;
    }
  }
  assert(pSyncCfg->selfIndex < pSyncCfg->replica && pSyncCfg->selfIndex >= 0);
  const SNodeInfo* pNode = &(pSyncCfg->nodeInfo[pSyncCfg->replica]);
  char buffer[156] = {'\0'};
  snprintf(buffer, sizeof(buffer), "%s:%d", &(pNode->nodeFqdn[0]), pNode->nodePort);
  size_t len = strlen(buffer);
  void** ppRpcServer = taosHashGet(gSyncManager->rpcServerTable, buffer, len);
  if (ppRpcServer != NULL) {
    // already inited
    syncInfo("sync rpc server for %s already exist", buffer);
    return 0;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = pNode->nodePort;
  rpcInit.label        = "sync-server";
  rpcInit.numOfThreads = SYNC_SERVER_WORKER;
  rpcInit.cfp          = syncProcessReqMsg;
  rpcInit.sessions     = TSDB_MAX_VNODES << 4;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = SYNC_ACTIVITY_TIMER * 1000;

  void* rpcServer = rpcOpen(&rpcInit);
  if (rpcServer == NULL) {
    syncInfo("rpcOpen for sync rpc server for %s fail", buffer);
    return -1;
  }

  taosHashPut(gSyncManager->rpcServerTable, buffer, strlen(buffer), rpcServer, len);
  syncInfo("sync rpc server for %s init success", buffer);

  return 0;
}

static int syncInitRpcClient(SSyncManager* syncManager) {
  char secret[TSDB_PASSWORD_LEN] = "secret";
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label        = "sync-client";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = syncProcessRsp;
  rpcInit.sessions     = TSDB_MAX_VNODES << 4;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = SYNC_ACTIVITY_TIMER * 1000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = secret;

  syncManager->clientRpc = rpcOpen(&rpcInit);
  if (syncManager->clientRpc == NULL) {
    syncError("failed to init sync rpc client");
    return -1;
  }

  syncInfo("sync inter-sync rpc client is initialized");
  return 0;
}

static int syncOpenWorkerPool(SSyncManager* syncManager) {
  int i;
  pthread_attr_t thattr;

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE); 

  for (i = 0; i < TAOS_SYNC_MAX_WORKER; ++i) {
    SSyncWorker* pWorker = &(syncManager->worker[i]);

    if (pthread_create(&(pWorker->thread), &thattr, (void *)syncWorkerMain, pWorker) != 0) {
      syncError("failed to create sync worker since %s", strerror(errno));

      return -1;
    }
  }

  pthread_attr_destroy(&thattr);

  return 0;
}

static int syncCloseWorkerPool(SSyncManager* syncManager) {
  return 0;
}

static void *syncWorkerMain(void *argv) {
  SSyncWorker* pWorker = (SSyncWorker *)argv;

  taosBlockSIGPIPE();
  setThreadName("syncWorker");

  return NULL;
}

static void syncNodeTick(void *param, void *tmrId) {
  SyncGroupId vgId = (SyncGroupId)((int64_t)param);
  SSyncNode **ppNode = taosHashGet(gSyncManager->vgroupTable, &vgId, sizeof(SyncGroupId*));
  if (ppNode == NULL) {
    return;
  }
  SSyncNode *pNode = *ppNode;

  pthread_mutex_lock(&pNode->mutex);
  syncRaftTick(&pNode->raft);
  pthread_mutex_unlock(&pNode->mutex);

  pNode->syncTimer = taosTmrStart(syncNodeTick, SYNC_TICK_TIMER, (void*)(int64_t)pNode->vgId, gSyncManager->syncTimerManager);
}