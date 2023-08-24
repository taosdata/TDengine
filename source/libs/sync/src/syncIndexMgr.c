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

  pIndexMgr->replicas = &pNode->replicasId;
  pIndexMgr->replicaNum = pNode->replicaNum;
  pIndexMgr->totalReplicaNum = pNode->totalReplicaNum;
  pIndexMgr->pNode = pNode;
  syncIndexMgrClear(pIndexMgr);

  return pIndexMgr;
}

void syncIndexMgrUpdate(SSyncIndexMgr *pIndexMgr, SSyncNode *pNode) {
  pIndexMgr->replicas = &pNode->replicasId;
  pIndexMgr->replicaNum = pNode->replicaNum;
  pIndexMgr->totalReplicaNum = pNode->totalReplicaNum;
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

  int64_t timeNow = taosGetTimestampMs();
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    pIndexMgr->startTimeArr[i] = 0;
    pIndexMgr->recvTimeArr[i] = timeNow;
  }
}

void syncIndexMgrSetIndex(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, SyncIndex index) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->index)[i] = index;
      return;
    }
  }

  sError("vgId:%d, indexmgr set index:%" PRId64 " for dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, index,
         DID(pRaftId), CID(pRaftId));
}

void syncIndexMgrCopyIfExist(SSyncIndexMgr * pNewIndex, SSyncIndexMgr * pOldIndex, SRaftId *oldReplicasId){
  for(int j = 0; j < pOldIndex->totalReplicaNum; ++j){
    sDebug("old Index j:%d, index:%"PRId64, j, pOldIndex->index[j]);
  }
  
  for (int i = 0; i < pNewIndex->totalReplicaNum; ++i) {
    for(int j = 0; j < pOldIndex->totalReplicaNum; ++j){
      if (syncUtilSameId(/*(const SRaftId*)*/&((oldReplicasId[j])), &((*(pNewIndex->replicas))[i]))) {
        pNewIndex->index[i] = pOldIndex->index[j];
        pNewIndex->privateTerm[i] = pOldIndex->privateTerm[j];
        pNewIndex->startTimeArr[i] = pOldIndex->startTimeArr[j];
        pNewIndex->recvTimeArr[i] = pOldIndex->recvTimeArr[j];   
      }
    }
  }

  for (int i = 0; i < pNewIndex->totalReplicaNum; ++i){
    sDebug("new index i:%d, index:%"PRId64, i, pNewIndex->index[i]);
  }
}

SSyncLogReplMgr *syncNodeGetLogReplMgr(SSyncNode *pNode, SRaftId *pRaftId) {
  for (int i = 0; i < pNode->totalReplicaNum; i++) {
    if (syncUtilSameId(&pNode->replicasId[i], pRaftId)) {
      return pNode->logReplMgrs[i];
    }
  }

  sError("vgId:%d, indexmgr get replmgr from dnode:%d cluster:%d failed", pNode->vgId, DID(pRaftId), CID(pRaftId));
  return NULL;
}

SyncIndex syncIndexMgrGetIndex(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      SyncIndex idx = (pIndexMgr->index)[i];
      return idx;
    }
  }

  sError("vgId:%d, indexmgr get index from dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, DID(pRaftId),
         CID(pRaftId));
  return SYNC_INDEX_INVALID;
}

void syncIndexMgrSetStartTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, int64_t startTime) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->startTimeArr)[i] = startTime;
      return;
    }
  }

  sError("vgId:%d, indexmgr set start-time:%" PRId64 " for dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId,
         startTime, DID(pRaftId), CID(pRaftId));
}

int64_t syncIndexMgrGetStartTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      int64_t startTime = (pIndexMgr->startTimeArr)[i];
      return startTime;
    }
  }

  sError("vgId:%d, indexmgr get start-time from dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, DID(pRaftId),
         CID(pRaftId));
  return -1;
}

void syncIndexMgrSetRecvTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, int64_t recvTime) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->recvTimeArr)[i] = recvTime;
      return;
    }
  }

  sError("vgId:%d, indexmgr set recv-time:%" PRId64 " for dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, recvTime,
         DID(pRaftId), CID(pRaftId));
}

int64_t syncIndexMgrGetRecvTime(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      int64_t recvTime = (pIndexMgr->recvTimeArr)[i];
      return recvTime;
    }
  }

  sError("vgId:%d, indexmgr get recv-time from dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, DID(pRaftId),
         CID(pRaftId));
  return -1;
}

void syncIndexMgrSetTerm(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId, SyncTerm term) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      (pIndexMgr->privateTerm)[i] = term;
      return;
    }
  }

  sError("vgId:%d, indexmgr set term:%" PRId64 " for dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, term,
         DID(pRaftId), CID(pRaftId));
}

SyncTerm syncIndexMgrGetTerm(SSyncIndexMgr *pIndexMgr, const SRaftId *pRaftId) {
  for (int i = 0; i < pIndexMgr->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((*(pIndexMgr->replicas))[i]), pRaftId)) {
      SyncTerm term = (pIndexMgr->privateTerm)[i];
      return term;
    }
  }

  sError("vgId:%d, indexmgr get term from dnode:%d cluster:%d failed", pIndexMgr->pNode->vgId, DID(pRaftId),
         CID(pRaftId));
  return -1;
}
