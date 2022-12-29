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

SSyncIndexMgr *syncIndexMgrCreate(SSyncNode *pNode) {
  SSyncIndexMgr *pIndexMgr = taosMemoryCalloc(1, sizeof(SSyncIndexMgr));
  if (pIndexMgr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pIndexMgr->replicas = &(pNode->replicasId);
  pIndexMgr->replicaNum = pNode->replicaNum;
  pIndexMgr->pNode = pNode;
  syncIndexMgrClear(pIndexMgr);

  return pIndexMgr;
}

void syncIndexMgrUpdate(SSyncIndexMgr *pIndexMgr, SSyncNode *pNode) {
  pIndexMgr->replicas = &(pNode->replicasId);
  pIndexMgr->replicaNum = pNode->replicaNum;
  pIndexMgr->pNode = pNode;
  syncIndexMgrClear(pIndexMgr);
}

void syncIndexMgrDestroy(SSyncIndexMgr *pIndexMgr) {
  if (pIndexMgr != NULL) {
    taosMemoryFree(pIndexMgr);
  }
}

void syncIndexMgrClear(SSyncIndexMgr *pIndexMgr) {
  memset(pIndexMgr->index, 0, sizeof(pIndexMgr->index));
  memset(pIndexMgr->privateTerm, 0, sizeof(pIndexMgr->privateTerm));

  // int64_t timeNow = taosGetMonotonicMs();
  int64_t timeNow = taosGetTimestampMs();
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    pIndexMgr->startTimeArr[i] = 0;
    pIndexMgr->recvTimeArr[i] = timeNow;
  }

  /*
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    pIndexMgr->index[i] = 0;
  }
  */
}

void syncIndexMgrSetIndex(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, SyncIndex index) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->index)[i] = index;
      return;
    }
  }

  // maybe config change
  // ASSERT(0);

  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, index:%" PRId64 " error", pIndexMgr->pNode->vgId, host, port, index);
}

SSyncLogReplMgr *syncNodeGetLogReplMgr(SSyncNode *pNode, SRaftId *pDestId) {
  for (int i = 0; i < pNode->replicaNum; i++) {
    if (syncUtilSameId(&(pNode->replicasId[i]), pDestId)) {
      return pNode->logReplMgrs[i];
    }
  }
  return NULL;
}

SyncIndex syncIndexMgrGetIndex(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  if (pIndexMgr == NULL) {
    return SYNC_INDEX_INVALID;
  }

  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      SyncIndex idx = (pIndexMgr->index)[i];
      return idx;
    }
  }

  return SYNC_INDEX_INVALID;
}

void syncIndexMgrSetStartTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, int64_t startTime) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->startTimeArr)[i] = startTime;
      return;
    }
  }

  // maybe config change
  // ASSERT(0);
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, start-time:%" PRId64 " error", pIndexMgr->pNode->vgId, host, port,
         startTime);
}

int64_t syncIndexMgrGetStartTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      int64_t startTime = (pIndexMgr->startTimeArr)[i];
      return startTime;
    }
  }
  ASSERT(0);
  return -1;
}

void syncIndexMgrSetRecvTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, int64_t recvTime) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->recvTimeArr)[i] = recvTime;
      return;
    }
  }

  // maybe config change
  // ASSERT(0);
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, recv-time:%" PRId64 " error", pIndexMgr->pNode->vgId, host, port, recvTime);
}

int64_t syncIndexMgrGetRecvTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      int64_t recvTime = (pIndexMgr->recvTimeArr)[i];
      return recvTime;
    }
  }

  return -1;
}

void syncIndexMgrSetTerm(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, SyncTerm term) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->privateTerm)[i] = term;
      return;
    }
  }

  // maybe config change
  // ASSERT(0);
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pRaftId->addr, host, sizeof(host), &port);
  sError("vgId:%d, index mgr set for %s:%d, term:%" PRIu64 " error", pIndexMgr->pNode->vgId, host, port, term);
}

SyncTerm syncIndexMgrGetTerm(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      SyncTerm term = (pIndexMgr->privateTerm)[i];
      return term;
    }
  }
  ASSERT(0);
  return -1;
}
