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

SRaftId   ids[TSDB_MAX_REPLICA];
SSyncInfo syncInfo;
SSyncFSM* pFsm;

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

  SSyncNode* pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  // gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  // gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  // gSyncIO->FpOnSyncClientRequest = pSyncNode->FpOnClientRequest;
  // gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  // gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  // gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  // gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  // gSyncIO->FpOnSyncTimeout = pSyncNode->FpOnTimeout;
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
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

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
  sNTrace(pSyncNode, "----1");

  initRaftId(pSyncNode);

  //---------------------------

  sTrace("syncNodeStartPingTimer ...");
  ret = syncNodeStartPingTimer(pSyncNode);
  assert(ret == 0);
  sNTrace(pSyncNode, "----2");

  sTrace("sleep ...");
  taosMsleep(10000);

  sTrace("syncNodeStopPingTimer ...");
  ret = syncNodeStopPingTimer(pSyncNode);
  assert(ret == 0);
  sNTrace(pSyncNode, "----3");

  sTrace("sleep ...");
  taosMsleep(5000);

  sTrace("syncNodeStartPingTimer ...");
  ret = syncNodeStartPingTimer(pSyncNode);
  assert(ret == 0);
  sNTrace(pSyncNode, "----4");

  sTrace("sleep ...");
  taosMsleep(10000);

  sTrace("syncNodeStopPingTimer ...");
  ret = syncNodeStopPingTimer(pSyncNode);
  assert(ret == 0);
  sNTrace(pSyncNode, "----5");

  while (1) {
    sTrace("while 1 sleep ...");
    taosMsleep(1000);
  }

  return 0;
}
