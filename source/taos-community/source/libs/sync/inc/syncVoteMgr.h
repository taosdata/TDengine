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

#ifndef _TD_LIBS_SYNC_VOTG_MGR_H
#define _TD_LIBS_SYNC_VOTG_MGR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

typedef struct SVotesGranted {
  SRaftId (*replicas)[TSDB_MAX_REPLICA];
  bool       isGranted[TSDB_MAX_REPLICA];
  int32_t    replicaNum;
  int32_t    votes;
  SyncTerm   term;
  int32_t    quorum;
  bool       toLeader;
  SSyncNode *pNode;
} SVotesGranted;

SVotesGranted *voteGrantedCreate(SSyncNode *pNode);
void           voteGrantedDestroy(SVotesGranted *pVotesGranted);
void           voteGrantedUpdate(SVotesGranted *pVotesGranted, SSyncNode *pNode);
bool           voteGrantedMajority(SVotesGranted *pVotesGranted);
void           voteGrantedVote(SVotesGranted *pVotesGranted, SyncRequestVoteReply *pMsg);
void           voteGrantedReset(SVotesGranted *pVotesGranted, SyncTerm term);

typedef struct SVotesRespond {
  SRaftId (*replicas)[TSDB_MAX_REPLICA];
  bool       isRespond[TSDB_MAX_REPLICA];
  int32_t    replicaNum;
  SyncTerm   term;
  SSyncNode *pNode;
} SVotesRespond;

SVotesRespond *votesRespondCreate(SSyncNode *pNode);
void           votesRespondDestory(SVotesRespond *pVotesRespond);
void           votesRespondUpdate(SVotesRespond *pVotesRespond, SSyncNode *pNode);
bool           votesResponded(SVotesRespond *pVotesRespond, const SRaftId *pRaftId);
void           votesRespondAdd(SVotesRespond *pVotesRespond, const SyncRequestVoteReply *pMsg);
void           votesRespondReset(SVotesRespond *pVotesRespond, SyncTerm term);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_VOTG_MGR_H*/
