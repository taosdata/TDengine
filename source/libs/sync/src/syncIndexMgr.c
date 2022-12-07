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
#include "syncIndexMgr.h"
#include "syncUtil.h"

SSyncIndexMgr *syncIndexMgrCreate(SSyncNode *pSyncNode) {
  SSyncIndexMgr *pSyncIndexMgr = taosMemoryCalloc(1, sizeof(SSyncIndexMgr));
  if (pSyncIndexMgr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pSyncIndexMgr->replicas = &(pSyncNode->replicasId);
  pSyncIndexMgr->replicaNum = pSyncNode->replicaNum;
  pSyncIndexMgr->pSyncNode = pSyncNode;
  syncIndexMgrClear(pSyncIndexMgr);

  return pSyncIndexMgr;
}

void syncIndexMgrUpdate(SSyncIndexMgr *pSyncIndexMgr, SSyncNode *pSyncNode) {
  pSyncIndexMgr->replicas = &(pSyncNode->replicasId);
  pSyncIndexMgr->replicaNum = pSyncNode->replicaNum;
  pSyncIndexMgr->pSyncNode = pSyncNode;
  syncIndexMgrClear(pSyncIndexMgr);
}

void syncIndexMgrDestroy(SSyncIndexMgr *pSyncIndexMgr) {
  if (pSyncIndexMgr != NULL) {
    taosMemoryFree(pSyncIndexMgr);
  }
}

void syncIndexMgrClear(SSyncIndexMgr *pSyncIndexMgr) {
  memset(pSyncIndexMgr->index, 0, sizeof(pSyncIndexMgr->index));
  memset(pSyncIndexMgr->privateTerm, 0, sizeof(pSyncIndexMgr->privateTerm));

  // int64_t timeNow = taosGetMonotonicMs();
  int64_t timeNow = taosGetTimestampMs();
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    pSyncIndexMgr->startTimeArr[i] = 0;
    pSyncIndexMgr->recvTimeArr[i] = timeNow;
  }

  /*
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    pSyncIndexMgr->index[i] = 0;
  }
  */
}

void syncIndexMgrSetIndex(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, SyncIndex index) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      (pSyncIndexMgr->index)[i] = index;
      return;
    }
  }

  // maybe config change
  // tAssert(0);

  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, index:%" PRId64 " error", pSyncIndexMgr->pSyncNode->vgId, host, port,
         index);
}

SSyncLogReplMgr *syncNodeGetLogReplMgr(SSyncNode *pNode, SRaftId *pDestId) {
  for (int i = 0; i < pNode->replicaNum; i++) {
    if (syncUtilSameId(&(pNode->replicasId[i]), pDestId)) {
      return pNode->logReplMgrs[i];
    }
  }
  return NULL;
}

SyncIndex syncIndexMgrGetIndex(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId) {
  if (pSyncIndexMgr == NULL) {
    return SYNC_INDEX_INVALID;
  }

  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      SyncIndex idx = (pSyncIndexMgr->index)[i];
      return idx;
    }
  }

  return SYNC_INDEX_INVALID;
}

void syncIndexMgrSetStartTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, int64_t startTime) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      (pSyncIndexMgr->startTimeArr)[i] = startTime;
      return;
    }
  }

  // maybe config change
  // tAssert(0);
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, start-time:%" PRId64 " error", pSyncIndexMgr->pSyncNode->vgId, host, port,
         startTime);
}

int64_t syncIndexMgrGetStartTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      int64_t startTime = (pSyncIndexMgr->startTimeArr)[i];
      return startTime;
    }
  }
  tAssert(0);
  return -1;
}

void syncIndexMgrSetRecvTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, int64_t recvTime) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      (pSyncIndexMgr->recvTimeArr)[i] = recvTime;
      return;
    }
  }

  // maybe config change
  // tAssert(0);
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, recv-time:%" PRId64 " error", pSyncIndexMgr->pSyncNode->vgId, host, port,
         recvTime);
}

int64_t syncIndexMgrGetRecvTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      int64_t recvTime = (pSyncIndexMgr->recvTimeArr)[i];
      return recvTime;
    }
  }

  return -1;
}

void syncIndexMgrSetTerm(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, SyncTerm term) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      (pSyncIndexMgr->privateTerm)[i] = term;
      return;
    }
  }

  // maybe config change
  // tAssert(0);
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, term:%" PRIu64 " error", pSyncIndexMgr->pSyncNode->vgId, host, port, term);
}

SyncTerm syncIndexMgrGetTerm(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pSyncIndexMgr->replicas))[i]), pRaftId)) {
      SyncTerm term = (pSyncIndexMgr->privateTerm)[i];
      return term;
    }
  }
  tAssert(0);
  return -1;
}
