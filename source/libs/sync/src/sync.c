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

SSyncManager* gSyncManager = NULL;

static int syncOpenWorkerPool(SSyncManager* syncManager);
static int syncCloseWorkerPool(SSyncManager* syncManager);
static void *syncWorkerMain(void *argv);

int32_t syncInit() { 
  if (gSyncManager != NULL) {
    return 0;
  }

  gSyncManager = (SSyncManager*)malloc(sizeof(SSyncManager));
  if (gSyncManager == NULL) {
    syncError("malloc SSyncManager fail");
    return -1;
  }

  pthread_mutex_init(&gSyncManager->mutex, NULL);
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
  syncCloseWorkerPool(gSyncManager);
  pthread_mutex_unlock(&gSyncManager->mutex);
  pthread_mutex_destroy(&gSyncManager->mutex);
  free(gSyncManager);
  gSyncManager = NULL;
}

SSyncNode* syncStart(const SSyncInfo* pInfo) {
  pthread_mutex_lock(&gSyncManager->mutex);

  SSyncNode **ppNode = taosHashGet(gSyncManager->vgroupTable, &pInfo->vgId, sizeof(SyncGroupId));
  if (ppNode != NULL) {
    syncInfo("vgroup %d already exist", pInfo->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return *ppNode;
  }

  SSyncNode *pNode = (SSyncNode*)malloc(sizeof(SSyncNode));
  if (pNode == NULL) {
    syncError("malloc vgroup %d node fail", pInfo->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return NULL;
  }

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

  SSyncNode **ppNode = taosHashGet(gSyncManager->vgroupTable, &pNode->vgId, sizeof(SyncGroupId));
  if (ppNode == NULL) {
    syncInfo("vgroup %d not exist", pNode->vgId);
    pthread_mutex_unlock(&gSyncManager->mutex);
    return;
  }
  assert(*ppNode == pNode);
  
  taosHashRemove(gSyncManager->vgroupTable, &pNode->vgId, sizeof(SyncGroupId));
  pthread_mutex_unlock(&gSyncManager->mutex);

  pthread_mutex_destroy(&((*ppNode)->mutex));
  free(*ppNode);
}

int32_t syncPropose(SSyncNode* syncNode, const SSyncBuffer* pBuf, void* pData, bool isWeak) {
  RaftMessage msg;

  pthread_mutex_lock(&syncNode->mutex);
  int32_t ret = syncRaftStep(&syncNode->raft, syncInitPropMsg(&msg, pBuf, pData, isWeak));
  pthread_mutex_unlock(&syncNode->mutex);
  return ret;
}

void syncReconfig(const SSyncNode* pNode, const SSyncCluster* pCfg) {}

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