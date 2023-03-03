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
  assert(pSyncNode != NULL);

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
  assert(ret == 0);

  ret = syncInit();
  assert(ret == 0);

  SSyncNode* pSyncNode = syncInitTest();
  assert(pSyncNode != NULL);

  char* serialized = syncNode2Str(pSyncNode);
  printf("%s\n", serialized);
  taosMemoryFree(serialized);

  initRaftId(pSyncNode);

  SVotesRespond* pVotesRespond = votesRespondCreate(pSyncNode);
  assert(pVotesRespond != NULL);

  printf("---------------------------------------\n");
  {
    char* serialized = votesRespond2Str(pVotesRespond);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  SyncTerm term = 1234;
  printf("---------------------------------------\n");
  votesRespondReset(pVotesRespond, term);
  {
    char* serialized = votesRespond2Str(pVotesRespond);
    assert(serialized != NULL);
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
      assert(serialized != NULL);
      printf("%s\n", serialized);
      taosMemoryFree(serialized);
    }

    votesRespondAdd(pVotesRespond, reply);
    {
      char* serialized = votesRespond2Str(pVotesRespond);
      assert(serialized != NULL);
      printf("%s\n", serialized);
      taosMemoryFree(serialized);
    }
  }

  printf("---------------------------------------\n");
  votesRespondReset(pVotesRespond, 123456789);
  {
    char* serialized = votesRespond2Str(pVotesRespond);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  votesRespondDestory(pVotesRespond);
  return 0;
}
