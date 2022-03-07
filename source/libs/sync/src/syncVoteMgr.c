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

#include "syncVoteMgr.h"

SVotesGranted *voteGrantedCreate(SSyncNode *pSyncNode) {
  SVotesGranted *pVotesGranted = malloc(sizeof(SVotesGranted));
  assert(pVotesGranted != NULL);
  memset(pVotesGranted, 0, sizeof(SVotesGranted));

  pVotesGranted->quorum = pSyncNode->quorum;
  pVotesGranted->term = 0;
  pVotesGranted->votes = 0;
  pVotesGranted->toLeader = false;
  pVotesGranted->pSyncNode = pSyncNode;

  return pVotesGranted;
}

void voteGrantedDestroy(SVotesGranted *pVotesGranted) {
  if (pVotesGranted != NULL) {
    free(pVotesGranted);
  }
}

bool voteGrantedMajority(SVotesGranted *pVotesGranted) {
  bool ret = pVotesGranted->votes >= pVotesGranted->quorum;
  return ret;
}

void voteGrantedVote(SVotesGranted *pVotesGranted, SyncRequestVoteReply *pMsg) {
  assert(pMsg->voteGranted == true);
  assert(pMsg->term == pVotesGranted->term);
  pVotesGranted->votes++;
}

void voteGrantedReset(SVotesGranted *pVotesGranted, SyncTerm term) {
  pVotesGranted->term = term;
  pVotesGranted->votes = 0;
  pVotesGranted->toLeader = false;
}

SVotesRespond *votesRespondCreate(SSyncNode *pSyncNode) {
  SVotesRespond *pVotesRespond = malloc(sizeof(SVotesRespond));
  assert(pVotesRespond != NULL);
  memset(pVotesRespond, 0, sizeof(SVotesRespond));

  pVotesRespond->replicas = &(pSyncNode->replicasId);
  pVotesRespond->replicaNum = pSyncNode->replicaNum;
  pVotesRespond->term = 0;
  pVotesRespond->pSyncNode = pSyncNode;

  return pVotesRespond;
}

bool votesResponded(SVotesRespond *pVotesRespond, const SRaftId *pRaftId) {
  bool ret = false;
  for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
    if (syncUtilSameId(&(*pVotesRespond->replicas)[i], pRaftId) && pVotesRespond->isRespond[i]) {
      ret = true;
      break;
    }
  }
  return ret;
}

void votesRespondAdd(SVotesRespond *pVotesRespond, const SyncRequestVoteReply *pMsg) {
  assert(pVotesRespond->term == pMsg->term);
  for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
    if (syncUtilSameId(&(*pVotesRespond->replicas)[i], &pMsg->srcId)) {
      assert(pVotesRespond->isRespond[i] == false);
      pVotesRespond->isRespond[i] = true;
      return;
    }
  }
  assert(0);
}

void Reset(SVotesRespond *pVotesRespond, SyncTerm term) {
  pVotesRespond->term = term;
  for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
    pVotesRespond->isRespond[i] = false;
  }
}