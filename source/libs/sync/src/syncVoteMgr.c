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
#include "syncVoteMgr.h"
#include "syncMessage.h"
#include "syncUtil.h"

static void voteGrantedClearVotes(SVotesGranted *pVotesGranted) {
  memset(pVotesGranted->isGranted, 0, sizeof(pVotesGranted->isGranted));
  pVotesGranted->votes = 0;
}

SVotesGranted *voteGrantedCreate(SSyncNode *pNode) {
  SVotesGranted *pVotesGranted = taosMemoryCalloc(1, sizeof(SVotesGranted));
  if (pVotesGranted == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pVotesGranted->replicas = (void*)&pNode->replicasId;
  pVotesGranted->replicaNum = pNode->replicaNum;
  voteGrantedClearVotes(pVotesGranted);

  pVotesGranted->term = 0;
  pVotesGranted->quorum = pNode->quorum;
  pVotesGranted->toLeader = false;
  pVotesGranted->pNode = pNode;

  return pVotesGranted;
}

void voteGrantedDestroy(SVotesGranted *pVotesGranted) {
  if (pVotesGranted != NULL) {
    taosMemoryFree(pVotesGranted);
  }
}

void voteGrantedUpdate(SVotesGranted *pVotesGranted, SSyncNode *pNode) {
  pVotesGranted->replicas = (void*)&pNode->replicasId;
  pVotesGranted->replicaNum = pNode->replicaNum;
  voteGrantedClearVotes(pVotesGranted);

  pVotesGranted->term = 0;
  pVotesGranted->quorum = pNode->quorum;
  pVotesGranted->toLeader = false;
  pVotesGranted->pNode = pNode;
}

bool voteGrantedMajority(SVotesGranted *pVotesGranted) { return pVotesGranted->votes >= pVotesGranted->quorum; }

void voteGrantedVote(SVotesGranted *pVotesGranted, SyncRequestVoteReply *pMsg) {
  if (!pMsg->voteGranted) {
    sNFatal(pVotesGranted->pNode, "vote granted should be true");
    return;
  }

  if (pMsg->term != pVotesGranted->term) {
    sNTrace(pVotesGranted->pNode, "vote grant term:%" PRId64 " not matched with msg term:%" PRId64, pVotesGranted->term,
            pMsg->term);
    return;
  }

  if (!syncUtilSameId(&pVotesGranted->pNode->myRaftId, &pMsg->destId)) {
    sNFatal(pVotesGranted->pNode, "vote granted raftId not matched with msg");
    return;
  }

  int32_t j = -1;
  for (int32_t i = 0; i < pVotesGranted->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pVotesGranted->replicas))[i]), &(pMsg->srcId))) {
      j = i;
      break;
    }
  }
  if ((j == -1) || !(j >= 0 && j < pVotesGranted->replicaNum)) {
    sNFatal(pVotesGranted->pNode, "invalid msg srcId, index:%d", j);
    return;
  }

  if (pVotesGranted->isGranted[j] != true) {
    ++(pVotesGranted->votes);
    pVotesGranted->isGranted[j] = true;
  }

  if (pVotesGranted->votes > pVotesGranted->replicaNum) {
    sNFatal(pVotesGranted->pNode, "votes:%d not matched with replicaNum:%d", pVotesGranted->votes,
            pVotesGranted->replicaNum);
    return;
  }
}

void voteGrantedReset(SVotesGranted *pVotesGranted, SyncTerm term) {
  pVotesGranted->term = term;
  voteGrantedClearVotes(pVotesGranted);
  pVotesGranted->toLeader = false;
}

SVotesRespond *votesRespondCreate(SSyncNode *pNode) {
  SVotesRespond *pVotesRespond = taosMemoryCalloc(1, sizeof(SVotesRespond));
  if (pVotesRespond == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pVotesRespond->replicas = (void*)&pNode->replicasId;
  pVotesRespond->replicaNum = pNode->replicaNum;
  pVotesRespond->term = 0;
  pVotesRespond->pNode = pNode;

  return pVotesRespond;
}

void votesRespondDestory(SVotesRespond *pVotesRespond) {
  if (pVotesRespond != NULL) {
    taosMemoryFree(pVotesRespond);
  }
}

void votesRespondUpdate(SVotesRespond *pVotesRespond, SSyncNode *pNode) {
  pVotesRespond->replicas = (void*)&pNode->replicasId;
  pVotesRespond->replicaNum = pNode->replicaNum;
  pVotesRespond->term = 0;
  pVotesRespond->pNode = pNode;
}

bool votesResponded(SVotesRespond *pVotesRespond, const SRaftId *pRaftId) {
  bool ret = false;
  for (int32_t i = 0; i < pVotesRespond->replicaNum; ++i) {
    if (syncUtilSameId(&(*pVotesRespond->replicas)[i], pRaftId) && pVotesRespond->isRespond[i]) {
      ret = true;
      break;
    }
  }
  return ret;
}

void votesRespondAdd(SVotesRespond *pVotesRespond, const SyncRequestVoteReply *pMsg) {
  if (pVotesRespond->term != pMsg->term) {
    sNTrace(pVotesRespond->pNode, "vote respond add error");
    return;
  }

  for (int32_t i = 0; i < pVotesRespond->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pVotesRespond->replicas))[i]), &pMsg->srcId)) {
      pVotesRespond->isRespond[i] = true;
      return;
    }
  }

  sNFatal(pVotesRespond->pNode, "votes respond not found");
}

void votesRespondReset(SVotesRespond *pVotesRespond, SyncTerm term) {
  pVotesRespond->term = term;
  memset(pVotesRespond->isRespond, 0, sizeof(pVotesRespond->isRespond));
}
