#include "syncRaftStore.h"
//#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 5;
int32_t  myIndex = 0;
SRaftId  ids[TSDB_MAX_REPLICA];

void initRaftId() {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i].addr = syncUtilAddr2U64("127.0.0.1", ports[i]);
    ids[i].vgId = 1234;
    char* s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    free(s);
  }
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  initRaftId();

  SRaftStore* pRaftStore = raftStoreOpen("./test_raft_store.json");
  assert(pRaftStore != NULL);
  raftStorePrint2((char*)"==raftStoreOpen==", pRaftStore);

  raftStoreSetTerm(pRaftStore, 100);
  raftStorePrint2((char*)"==raftStoreSetTerm==", pRaftStore);

  raftStoreVote(pRaftStore, &ids[0]);
  raftStorePrint2((char*)"==raftStoreVote==", pRaftStore);

  raftStoreClearVote(pRaftStore);
  raftStorePrint2((char*)"==raftStoreClearVote==", pRaftStore);

  raftStoreVote(pRaftStore, &ids[1]);
  raftStorePrint2((char*)"==raftStoreVote==", pRaftStore);

  raftStoreNextTerm(pRaftStore);
  raftStorePrint2((char*)"==raftStoreNextTerm==", pRaftStore);

  raftStoreNextTerm(pRaftStore);
  raftStorePrint2((char*)"==raftStoreNextTerm==", pRaftStore);

  raftStoreNextTerm(pRaftStore);
  raftStorePrint2((char*)"==raftStoreNextTerm==", pRaftStore);

  raftStoreNextTerm(pRaftStore);
  raftStorePrint2((char*)"==raftStoreNextTerm==", pRaftStore);

  return 0;
}
