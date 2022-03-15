#include "syncRaftStore.h"
//#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  SRaftStore *pRaftStore = raftStoreOpen("./raft_store.json");
  assert(pRaftStore != NULL);
  raftStorePrint(pRaftStore);

#if 0
  pRaftStore->currentTerm = 100;
  pRaftStore->voteFor.addr = 200;
  pRaftStore->voteFor.vgId = 300;
  raftStorePersist(pRaftStore);
  raftStorePrint(pRaftStore);
#endif

  ++(pRaftStore->currentTerm);
  ++(pRaftStore->voteFor.addr);
  ++(pRaftStore->voteFor.vgId);
  raftStorePersist(pRaftStore);
  raftStorePrint(pRaftStore);

  return 0;
}
