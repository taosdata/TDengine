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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "syncMessage.h"
#include "syncUtil.h"
#include "taosdef.h"

// SVotesGranted -----------------------------
typedef struct SVotesGranted {
  SRaftId (*replicas)[TSDB_MAX_REPLICA];
  bool       isGranted[TSDB_MAX_REPLICA];
  int32_t    replicaNum;
  int32_t    votes;
  SyncTerm   term;
  int32_t    quorum;
  bool       toLeader;
  SSyncNode *pSyncNode;
} SVotesGranted;

SVotesGranted *voteGrantedCreate(SSyncNode *pSyncNode);
void           voteGrantedDestroy(SVotesGranted *pVotesGranted);
bool           voteGrantedMajority(SVotesGranted *pVotesGranted);
void           voteGrantedVote(SVotesGranted *pVotesGranted, SyncRequestVoteReply *pMsg);
void           voteGrantedReset(SVotesGranted *pVotesGranted, SyncTerm term);
cJSON *        voteGranted2Json(SVotesGranted *pVotesGranted);
char *         voteGranted2Str(SVotesGranted *pVotesGranted);

// for debug -------------------
void voteGrantedPrint(SVotesGranted *pObj);
void voteGrantedPrint2(char *s, SVotesGranted *pObj);
void voteGrantedLog(SVotesGranted *pObj);
void voteGrantedLog2(char *s, SVotesGranted *pObj);

// SVotesRespond -----------------------------
typedef struct SVotesRespond {
  SRaftId (*replicas)[TSDB_MAX_REPLICA];
  bool       isRespond[TSDB_MAX_REPLICA];
  int32_t    replicaNum;
  SyncTerm   term;
  SSyncNode *pSyncNode;
} SVotesRespond;

SVotesRespond *votesRespondCreate(SSyncNode *pSyncNode);
void           votesRespondDestory(SVotesRespond *pVotesRespond);
bool           votesResponded(SVotesRespond *pVotesRespond, const SRaftId *pRaftId);
void           votesRespondAdd(SVotesRespond *pVotesRespond, const SyncRequestVoteReply *pMsg);
void           votesRespondReset(SVotesRespond *pVotesRespond, SyncTerm term);
cJSON *        votesRespond2Json(SVotesRespond *pVotesRespond);
char *         votesRespond2Str(SVotesRespond *pVotesRespond);

// for debug -------------------
void votesRespondPrint(SVotesRespond *pObj);
void votesRespondPrint2(char *s, SVotesRespond *pObj);
void votesRespondLog(SVotesRespond *pObj);
void votesRespondLog2(char *s, SVotesRespond *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_VOTG_MGR_H*/
