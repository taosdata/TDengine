/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <gtest/gtest.h>
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 3;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM*  pFsm;
SSyncNode* pSyncNode;

SSyncNode* syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.msgcb = &gSyncIO->msgcb;
  syncInfo.syncSendMSg = syncIOSendMsg;
  syncInfo.syncEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./");

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
  pCfg->replicaNum = replicaNum;

  for (int i = 0; i < replicaNum; ++i) {
    pCfg->nodeInfo[i].nodePort = ports[i];
    snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    // taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);
  }

  pSyncNode = syncNodeOpen(&syncInfo);
  TD_ALWAYS_ASSERT(pSyncNode != NULL);

  // gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  // gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  // gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  // gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  // gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  // gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  // gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  // gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  gSyncIO->pSyncNode = pSyncNode;

  return pSyncNode;
}

SSyncNode* syncInitTest() { return syncNodeInit(); }

void initRaftId(SSyncNode* pSyncNode) {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i] = pSyncNode->replicasId[i];
    char* s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    taosMemoryFree(s);
  }
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  int32_t ret = syncIOStart((char*)"127.0.0.1", ports[myIndex]);
  TD_ALWAYS_ASSERT(ret == 0);

  ret = syncInit();
  TD_ALWAYS_ASSERT(ret == 0);

  SSyncNode* pSyncNode = syncInitTest();
  TD_ALWAYS_ASSERT(pSyncNode != NULL);

  char* serialized = syncNode2Str(pSyncNode);
  printf("%s\n", serialized);
  taosMemoryFree(serialized);

  initRaftId(pSyncNode);

  SVotesRespond* pVotesRespond = votesRespondCreate(pSyncNode);
  TD_ALWAYS_ASSERT(pVotesRespond != NULL);

  printf("---------------------------------------\n");
  {
    char* serialized = votesRespond2Str(pVotesRespond);
    TD_ALWAYS_ASSERT(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  SyncTerm term = 1234;
  printf("---------------------------------------\n");
  votesRespondReset(pVotesRespond, term);
  {
    char* serialized = votesRespond2Str(pVotesRespond);
    TD_ALWAYS_ASSERT(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  for (int i = 0; i < replicaNum; ++i) {
    SyncRequestVoteReply* reply = syncRequestVoteReplyBuild(1000);
    reply->destId = pSyncNode->myRaftId;
    reply->srcId = ids[i];
    reply->term = term;
    reply->voteGranted = true;

    votesRespondAdd(pVotesRespond, reply);
    {
      char* serialized = votesRespond2Str(pVotesRespond);
      TD_ALWAYS_ASSERT(serialized != NULL);
      printf("%s\n", serialized);
      taosMemoryFree(serialized);
    }

    votesRespondAdd(pVotesRespond, reply);
    {
      char* serialized = votesRespond2Str(pVotesRespond);
      TD_ALWAYS_ASSERT(serialized != NULL);
      printf("%s\n", serialized);
      taosMemoryFree(serialized);
    }
  }

  printf("---------------------------------------\n");
  votesRespondReset(pVotesRespond, 123456789);
  {
    char* serialized = votesRespond2Str(pVotesRespond);
    TD_ALWAYS_ASSERT(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  votesRespondDestory(pVotesRespond);
  return 0;
}
